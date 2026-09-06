use super::*;
impl FollowSelection {
    pub(super) const fn replication(selection: ReplicationSelection) -> Self {
        Self::Selected(selection)
    }
}

pub fn replication_cursor_key(
    peer_id: EndpointId,
    selection: &ReplicationSelection,
) -> ReplicationCursorKey {
    let peer_id = peer_id.to_string();
    let peer = match selection {
        ReplicationSelection::All => format!("{peer_id}|all|v2"),
        ReplicationSelection::Service(service_id) => {
            format!(
                "{peer_id}|service|{}:{}",
                service_id.as_str().len(),
                service_id
            )
        }
        ReplicationSelection::ServiceScope {
            service_id,
            scope_id,
        } => format!(
            "{peer_id}|service_scope|{}:{}|{}:{}",
            service_id.as_str().len(),
            service_id,
            scope_id.as_str().len(),
            scope_id
        ),
        ReplicationSelection::Scopes(selections) => {
            let mut key = format!("{peer_id}|scopes");
            let mut components = selections
                .iter()
                .map(|selection| match selection {
                    myko_federation::ScopeSelection::Exact(scope_id) => {
                        ("exact", scope_id.as_str())
                    }
                    myko_federation::ScopeSelection::Subtree(scope_id) => {
                        ("subtree", scope_id.as_str())
                    }
                })
                .collect::<Vec<_>>();
            components.sort_unstable();
            components.dedup();
            for (kind, scope_id) in components {
                let _ = write!(key, "|{kind}|{}:{scope_id}", scope_id.len());
            }
            key
        }
        ReplicationSelection::Intersection { requested, scopes } => {
            let requested = serde_json::to_string(requested).unwrap_or_default();
            let mut key = format!("{peer_id}|intersection|{}:{requested}", requested.len());
            let mut components = scopes
                .iter()
                .map(|selection| match selection {
                    myko_federation::ScopeSelection::Exact(scope_id) => {
                        ("exact", scope_id.as_str())
                    }
                    myko_federation::ScopeSelection::Subtree(scope_id) => {
                        ("subtree", scope_id.as_str())
                    }
                })
                .collect::<Vec<_>>();
            components.sort_unstable();
            components.dedup();
            for (kind, scope_id) in components {
                let _ = write!(key, "|{kind}|{}:{scope_id}", scope_id.len());
            }
            key
        }
    };
    ReplicationCursorKey::new("iroh", peer)
}

fn selected_response_matches_request(
    response: &ReplicationSelection,
    requested: &ReplicationSelection,
) -> bool {
    response == requested
        || matches!(
            response,
            ReplicationSelection::Intersection {
                requested: original,
                scopes,
            } if original.as_ref() == requested && !scopes.is_empty()
        )
}

struct FollowCursorState {
    expected_source_node: Option<NodeId>,
    source_node: Option<NodeId>,
    cursor: Option<LogPosition>,
    effective_selection: Option<ReplicationSelection>,
}

struct FollowerConfig {
    cursor: FollowCursorState,
    retry_interval: Duration,
    persistence: Option<CursorPersistence>,
    selection: FollowSelection,
}

impl FollowerConfig {
    const fn new(
        expected_source_node: Option<NodeId>,
        source_node: Option<NodeId>,
        cursor: Option<LogPosition>,
        retry_interval: Duration,
        persistence: Option<CursorPersistence>,
        selection: FollowSelection,
    ) -> Self {
        Self {
            cursor: FollowCursorState {
                expected_source_node,
                source_node,
                cursor,
                effective_selection: None,
            },
            retry_interval,
            persistence,
            selection,
        }
    }

    fn with_effective_selection(mut self, selection: Option<ReplicationSelection>) -> Self {
        self.cursor.effective_selection = selection;
        self
    }
}

async fn write_request(
    send: &mut SendStream,
    request: &ReplicationRequest,
) -> Result<(), IrohReplicationError> {
    write_request_envelope(send, &NodeRequestEnvelope::connected(request.clone())).await
}

pub async fn write_request_with_authority(
    send: &mut SendStream,
    request: &ReplicationRequest,
    authority: Option<AuthorityPresentation>,
) -> Result<(), IrohReplicationError> {
    match authority {
        Some(authority) => {
            write_request_envelope(
                send,
                &NodeRequestEnvelope {
                    destination: None,
                    authority: Some(authority),
                    forwarding_provenance: Vec::new(),
                    request: request.clone(),
                },
            )
            .await
        }
        None => write_request(send, request).await,
    }
}

pub async fn write_request_envelope(
    send: &mut SendStream,
    request: &NodeRequestEnvelope,
) -> Result<(), IrohReplicationError> {
    let encoded = serde_json::to_vec(&WireEnvelope::new(request.clone()))?;
    send.write_all(&encoded)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
    send.finish()
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))
}

fn validate_live_topics(topics: &[String]) -> Result<(), String> {
    if topics.len() > MAX_LIVE_TOPICS {
        return Err(format!(
            "live subscription exceeds {MAX_LIVE_TOPICS} topics"
        ));
    }
    if let Some(topic) = topics
        .iter()
        .find(|topic| topic.is_empty() || topic.len() > MAX_LIVE_TOPIC_BYTES)
    {
        return Err(format!(
            "live topic must contain 1..={MAX_LIVE_TOPIC_BYTES} bytes, got {}",
            topic.len()
        ));
    }
    Ok(())
}

async fn write_frame(
    send: &mut SendStream,
    frame: &ReplicationFrame,
) -> Result<(), IrohReplicationError> {
    let encoded = serde_json::to_vec(&WireEnvelope::new(frame.clone()))?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(IrohReplicationError::Stream(format!(
            "replication frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    let length = u32::try_from(encoded.len()).map_err(|error| {
        IrohReplicationError::Stream(format!("replication batch length is invalid: {error}"))
    })?;
    send.write_all(&length.to_be_bytes())
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
    send.write_all(&encoded)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))
}

pub async fn read_frame(
    receive: &mut RecvStream,
) -> Result<ReplicationFrame, IrohReplicationError> {
    loop {
        let frame = read_frame_optional(receive).await?.ok_or_else(|| {
            IrohReplicationError::Stream("peer closed before sending a response frame".to_owned())
        })?;
        match frame {
            ReplicationFrame::Authorization { decision }
                if matches!(
                    decision.as_ref(),
                    myko_federation::AuthorizationDecision::Permit(_)
                ) => {}
            ReplicationFrame::Authorization { decision } => {
                return Err(authorization_error(decision));
            }
            ReplicationFrame::AuthorityUnavailable { reason } => {
                return Err(IrohReplicationError::AuthorityUnavailable(reason));
            }
            frame => return Ok(frame),
        }
    }
}

async fn read_frame_optional(
    receive: &mut RecvStream,
) -> Result<Option<ReplicationFrame>, IrohReplicationError> {
    let mut header = [0_u8; size_of::<u32>()];
    let Some(read) = receive
        .read(&mut header)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?
    else {
        return Ok(None);
    };
    let remainder = header.get_mut(read..).ok_or_else(|| {
        IrohReplicationError::Stream("peer returned an invalid frame header length".to_owned())
    })?;
    receive
        .read_exact(remainder)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
    let length = usize::try_from(u32::from_be_bytes(header)).map_err(|error| {
        IrohReplicationError::Stream(format!("replication frame length is invalid: {error}"))
    })?;
    if length > MAX_FRAME_BYTES {
        return Err(IrohReplicationError::Stream(format!(
            "replication frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    let mut encoded = vec![0_u8; length];
    receive
        .read_exact(&mut encoded)
        .await
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
    let envelope: WireEnvelope<ReplicationFrame> =
        serde_json::from_slice(&encoded).map_err(IrohReplicationError::from)?;
    envelope
        .into_current()
        .map(Some)
        .map_err(|error| IrohReplicationError::Stream(error.to_string()))
}

pub async fn read_command_frame(
    receive: &mut RecvStream,
    command_id: CommandId,
) -> Result<CommandResponse, IrohReplicationError> {
    match read_frame(receive).await? {
        ReplicationFrame::Command { response }
            if response
                .command
                .as_ref()
                .is_none_or(|command| command.request.id == command_id) =>
        {
            Ok(*response)
        }
        ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
            "remote command watch failed: {message}"
        ))),
        _ => Err(IrohReplicationError::Stream(
            "peer sent a mismatched frame for a command watch".to_owned(),
        )),
    }
}

async fn read_request(receive: &mut RecvStream) -> Result<NodeRequestEnvelope, AcceptError> {
    let encoded = receive
        .read_to_end(MAX_REQUEST_BYTES)
        .await
        .map_err(AcceptError::from_err)?;
    let envelope: WireEnvelope<NodeRequestEnvelope> =
        serde_json::from_slice(&encoded).map_err(AcceptError::from_err)?;
    envelope.into_current().map_err(AcceptError::from_err)
}

fn persist_cursor(
    persistence: Option<&CursorPersistence>,
    checkpoint: ReplicationCheckpoint,
) -> Result<(), IrohReplicationError> {
    let Some(persistence) = persistence else {
        return Ok(());
    };
    persistence
        .store
        .save_checkpoint(&persistence.key, checkpoint)
        .map_err(|error| IrohReplicationError::Cursor(error.to_string()))
}

fn follower_checkpoint(
    source_node: NodeId,
    position: Option<LogPosition>,
    selection: &FollowSelection,
    effective_selection: Option<&ReplicationSelection>,
) -> ReplicationCheckpoint {
    match selection {
        FollowSelection::Selected(_) => ReplicationCheckpoint::selected(
            source_node,
            position,
            effective_selection
                .cloned()
                .unwrap_or(ReplicationSelection::All),
        ),
        FollowSelection::All | FollowSelection::Scope(_) => {
            ReplicationCheckpoint::new(source_node, position)
        }
    }
}

impl IrohReplicator {
    /// Binds the retained application client to one authenticated Iroh peer.
    #[must_use]
    pub fn application_client(&self, peer: EndpointAddr) -> MykoClient {
        self.handler_connector(peer).client()
    }

    /// Creates a configurable Iroh adapter for retained durable handlers.
    #[must_use]
    pub fn handler_connector(&self, peer: EndpointAddr) -> IrohHandlerConnector {
        IrohHandlerConnector {
            replicator: self.clone(),
            peer,
            destination: None,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
            forwarding_provenance: Vec::new(),
        }
    }

    /// Binds the transport-neutral command client facade to one authenticated
    /// peer address.
    #[must_use]
    pub fn command_client(&self, peer: EndpointAddr) -> IrohCommandClient {
        IrohCommandClient {
            replicator: self.clone(),
            peer,
            authority: None,
            control_request_timeout: Duration::from_secs(10),
        }
    }

    /// Binds the transport-neutral current-state client to one authenticated
    /// peer address.
    #[must_use]
    pub fn item_client(&self, peer: EndpointAddr) -> IrohItemClient {
        IrohItemClient {
            replicator: self.clone(),
            peer,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
        }
    }

    /// Binds a new Iroh endpoint with a generated node key.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind(node: Node) -> Result<Self, IrohReplicationError> {
        Self::bind_with_policy(node, Arc::new(myko_federation::DenyAllAccessPolicy)).await
    }

    /// Binds a new Iroh endpoint with an application policy installed before
    /// the protocol router starts accepting connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_with_policy(
        node: Node,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::bind(presets::N0)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_endpoint(node, endpoint, initial_policy))
    }

    /// Binds a network endpoint that serves registered application handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_application(
        application: ApplicationHost,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_application_with_policy(
            application,
            Arc::new(myko_federation::DenyAllAccessPolicy),
        )
        .await
    }

    /// Binds a network endpoint that serves both node and registered
    /// application protocols.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_application_with_policy(
        application: ApplicationHost,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::bind(presets::N0)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_application_endpoint(
            application,
            endpoint,
            initial_policy,
        ))
    }

    /// Binds a loopback-only endpoint for local development and tests.
    ///
    /// This endpoint advertises only an ephemeral IPv4 loopback address and
    /// does not configure public relays or address discovery. It cannot accept
    /// connections from another machine.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback address is invalid or cannot be bound.
    pub async fn bind_loopback(node: Node) -> Result<Self, IrohReplicationError> {
        Self::bind_loopback_with_policy(node, Arc::new(myko_federation::DenyAllAccessPolicy)).await
    }

    /// Binds a loopback endpoint with policy installed before serving.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback address is invalid or cannot be bound.
    pub async fn bind_loopback_with_policy(
        node: Node,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr_with_opts(
                "127.0.0.1:0",
                BindOpts::default()
                    .set_prefix_len(8)
                    .set_is_default_route(false),
            )
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_endpoint(node, endpoint, initial_policy))
    }

    /// Binds a loopback endpoint that serves registered application handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback endpoint cannot bind.
    pub async fn bind_loopback_application(
        application: ApplicationHost,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_loopback_application_with_policy(
            application,
            Arc::new(myko_federation::DenyAllAccessPolicy),
        )
        .await
    }

    /// Binds a loopback endpoint that serves registered application handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback endpoint cannot bind.
    pub async fn bind_loopback_application_with_policy(
        application: ApplicationHost,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr_with_opts(
                "127.0.0.1:0",
                BindOpts::default()
                    .set_prefix_len(8)
                    .set_is_default_route(false),
            )
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_application_endpoint(
            application,
            endpoint,
            initial_policy,
        ))
    }

    /// Binds a loopback-only endpoint with a persistent transport identity.
    ///
    /// This is useful for local nodes that need stable peer addressing without
    /// enabling relays or public address discovery.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback address is invalid or cannot be bound.
    pub async fn bind_loopback_with_secret(
        node: Node,
        secret_key: SecretKey,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_loopback_with_secret_and_policy(
            node,
            secret_key,
            Arc::new(myko_federation::DenyAllAccessPolicy),
        )
        .await
    }

    /// Binds a persistent loopback identity with policy installed before
    /// serving.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback address is invalid or cannot be bound.
    pub async fn bind_loopback_with_secret_and_policy(
        node: Node,
        secret_key: SecretKey,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .clear_ip_transports()
            .bind_addr_with_opts(
                "127.0.0.1:0",
                BindOpts::default()
                    .set_prefix_len(8)
                    .set_is_default_route(false),
            )
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_endpoint(node, endpoint, initial_policy))
    }

    /// Binds a persistent loopback identity with registered application
    /// handlers and an initial access policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint cannot bind.
    pub async fn bind_loopback_application_with_secret_and_policy(
        application: ApplicationHost,
        secret_key: SecretKey,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .clear_ip_transports()
            .bind_addr_with_opts(
                "127.0.0.1:0",
                BindOpts::default()
                    .set_prefix_len(8)
                    .set_is_default_route(false),
            )
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_application_endpoint(
            application,
            endpoint,
            initial_policy,
        ))
    }

    /// Binds a new Iroh endpoint with a persistent identity key.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_with_secret(
        node: Node,
        secret_key: SecretKey,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_with_secret_and_policy(
            node,
            secret_key,
            Arc::new(myko_federation::DenyAllAccessPolicy),
        )
        .await
    }

    /// Binds a foreground edge endpoint with a stable identity.
    ///
    /// The endpoint may pair and initiate outbound Myko operations, but it
    /// never serves its local journal or application handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_edge_with_secret(
        node: Node,
        secret_key: SecretKey,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_with_secret_and_policy(node, secret_key, Arc::new(DenyAllAccessPolicy)).await
    }

    /// Binds a persistent identity with policy installed before serving.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_with_secret_and_policy(
        node: Node,
        secret_key: SecretKey,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_endpoint(node, endpoint, initial_policy))
    }

    /// Binds a persistent network identity with registered application
    /// handlers and an initial access policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint cannot bind.
    pub async fn bind_application_with_secret_and_policy(
        application: ApplicationHost,
        secret_key: SecretKey,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, IrohReplicationError> {
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(format!("{error:?}")))?;
        Ok(Self::from_application_endpoint(
            application,
            endpoint,
            initial_policy,
        ))
    }

    fn from_endpoint(
        node: Node,
        endpoint: Endpoint,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        Self::from_endpoint_inner(node, None, endpoint, initial_policy)
    }

    fn from_application_endpoint(
        application: ApplicationHost,
        endpoint: Endpoint,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let node = application.node().clone();
        Self::from_endpoint_inner(node, Some(application), endpoint, initial_policy)
    }

    fn from_endpoint_inner(
        node: Node,
        application: Option<ApplicationHost>,
        endpoint: Endpoint,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let sessions = match application {
            Some(application) => FederatedSession::for_application(application, initial_policy),
            None => FederatedSession::new(node.clone(), initial_policy),
        };
        let pairing = pairing::PairingRegistry::new();
        let descriptor = NativeNodeDescriptor::new(node.node_id(), endpoint.addr());
        let pairing_endpoint = endpoint.clone();
        let protocol = ReplicationProtocol {
            sessions: sessions.clone(),
        };
        let router = Router::builder(endpoint)
            .accept(MYKO_REPLICATION_ALPN, protocol)
            .accept(
                MYKO_PAIRING_ALPN,
                pairing::PairingProtocol::new(pairing.clone()),
            )
            .accept(
                MYKO_PAIRING_OFFER_ALPN,
                pairing::PairingOfferProtocol::new(pairing_endpoint, descriptor, pairing.clone()),
            )
            .spawn();
        Self {
            node,
            sessions,
            pairing,
            router,
        }
    }

    /// Returns the authenticated Iroh address advertised by this endpoint.
    #[must_use]
    pub fn address(&self) -> EndpointAddr {
        self.router.endpoint().addr()
    }

    /// Returns this endpoint paired with its stable Myko history identity.
    #[must_use]
    pub fn descriptor(&self) -> NativeNodeDescriptor {
        NativeNodeDescriptor::new(self.node.node_id(), self.address())
    }

    /// Returns the transport-neutral semantic endpoint served by Iroh.
    ///
    /// Local sockets and other transport adapters should share this value so
    /// every transport observes identical handlers, policy, and live events.
    #[must_use]
    pub const fn sessions(&self) -> &FederatedSession {
        &self.sessions
    }

    /// Issues an expiring one-use invitation for this identity-pinned node.
    ///
    /// The returned bearer secret is suitable for an outer QR/file encoding.
    /// This endpoint stores only its verifier, and successful redemption does
    /// not implicitly grant application authorization.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid TTL, random-source failure, poisoned
    /// state, or a full bounded invitation registry.
    pub fn issue_pairing_invitation(
        &self,
        ttl: Duration,
    ) -> Result<PairingInvitation, IrohReplicationError> {
        self.pairing.issue(self.descriptor(), ttl)
    }

    /// Redeems a one-use invitation as this authenticated native endpoint.
    ///
    /// The receipt binds both Iroh endpoint IDs and both Myko source IDs and
    /// carries a six-digit comparison code. Applications still decide whether
    /// to install infrastructure trust or application grants.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed/expired invitations, identity mismatch,
    /// invalid proof, replay, bounds, timeout, or transport failure.
    pub async fn redeem_pairing(
        &self,
        invitation: &PairingInvitation,
    ) -> Result<PairingReceipt, IrohReplicationError> {
        pairing::redeem_pairing(self.router.endpoint(), self.descriptor(), invitation).await
    }

    /// Offers a fresh one-use invitation to one identity-pinned node.
    ///
    /// The recipient redeems the invitation back to this endpoint. Both nodes
    /// receive the same pending receipt and must confirm its comparison code
    /// before either relationship is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, invitation limits, timeout, or
    /// a rejected/tampered offer.
    pub async fn offer_pairing(
        &self,
        peer: &NativeNodeDescriptor,
        ttl: Duration,
    ) -> Result<PairingReceipt, IrohReplicationError> {
        let invitation = self.issue_pairing_invitation(ttl)?;
        pairing::offer_pairing(self.router.endpoint(), &invitation, peer).await
    }

    /// Drains authenticated receipts awaiting local operator/application
    /// policy decisions.
    ///
    /// # Errors
    ///
    /// Returns an error if shared pairing state is poisoned.
    pub fn take_pairing_receipts(&self) -> Result<Vec<PairingReceipt>, IrohReplicationError> {
        self.pairing.take_receipts()
    }

    /// Starts a lossless wake-up stream over the bounded pending receipt queue.
    #[must_use]
    pub fn subscribe_pairing_receipts(&self) -> PairingReceiptSubscription {
        self.pairing.subscribe()
    }

    /// Reads the Myko source identity served by one authenticated endpoint.
    ///
    /// This bounded handshake exposes no application scopes or history and is
    /// available before application authorization so pairing clients can
    /// verify a descriptor before issuing commands.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint cannot be reached or does not answer
    /// with a valid Myko identity frame.
    pub async fn identify_remote(
        &self,
        peer: EndpointAddr,
    ) -> Result<NodeId, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(&mut send, &ReplicationRequest::Identify).await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote identity failed: {message}"
                )));
            }
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer returned application data during identity handshake".to_owned(),
                ));
            }
        };
        connection.close(0u32.into(), b"identity verified");
        Ok(source_node)
    }

    /// Verifies both identities carried by a native node descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported descriptor, transport failure, or a
    /// Myko source identity mismatch.
    pub async fn verify_descriptor(
        &self,
        descriptor: &NativeNodeDescriptor,
    ) -> Result<(), IrohReplicationError> {
        descriptor
            .validate()
            .map_err(IrohReplicationError::Stream)?;
        let actual = self.identify_remote(descriptor.endpoint.clone()).await?;
        if actual != descriptor.node_id {
            return Err(IrohReplicationError::Stream(format!(
                "peer {} advertised Myko source {actual}, expected {}",
                descriptor.endpoint.id, descriptor.node_id
            )));
        }
        Ok(())
    }

    /// Replaces the policy used for subsequently accepted native requests.
    ///
    /// Existing history and live streams immediately re-evaluate the new
    /// policy and close if their original request is no longer authorized. The
    /// default policy denies application and federation access until an
    /// explicit policy is installed.
    ///
    /// # Errors
    ///
    /// Returns an error if the shared policy lock is poisoned.
    pub fn set_access_policy(
        &self,
        policy: Arc<dyn AccessPolicy>,
    ) -> Result<(), IrohReplicationError> {
        self.sessions
            .set_access_policy(policy)
            .map_err(IrohReplicationError::Supervisor)
    }

    /// Forwards one canonical request envelope to an authenticated native peer.
    ///
    /// Frames are emitted unchanged until the destination completes the
    /// request. This is the transport primitive used by the generic node
    /// router; it does not interpret command, query, report, view, or
    /// subscription semantics.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint cannot be reached, framing fails, or
    /// the receiving frame stream has been dropped.
    pub async fn forward_request(
        &self,
        peer: EndpointAddr,
        request: NodeRequestEnvelope,
        frames: &flume::Sender<ReplicationFrame>,
    ) -> Result<(), IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request_envelope(&mut send, &request).await?;
        while let Some(frame) = read_frame_optional(&mut receive).await? {
            frames.send_async(frame).await.map_err(|_| {
                IrohReplicationError::Stream("request frame receiver was dropped".to_owned())
            })?;
        }
        connection.close(0u32.into(), b"request complete");
        Ok(())
    }

    /// Publishes one non-authoritative event without waiting for any peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the node-local live-event hub cannot publish.
    pub fn publish_live(
        &self,
        topic: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<LivePublishReport, IrohReplicationError> {
        self.sessions
            .live_events()
            .publish(topic, payload)
            .map_err(IrohReplicationError::Ingest)
    }

    /// Opens a best-effort live-event stream from an authenticated peer.
    ///
    /// Topics are exact matches. Passing no topics follows all live events.
    /// The stream begins after subscription and does not replay missed events.
    ///
    /// # Errors
    ///
    /// Returns an error if filters are invalid, the peer cannot be reached, or
    /// the stream handshake is malformed.
    pub async fn subscribe_live_remote(
        &self,
        peer: EndpointAddr,
        topics: Vec<String>,
    ) -> Result<IrohLiveEventSubscription, IrohReplicationError> {
        self.subscribe_live_remote_with_authority(peer, topics, None)
            .await
    }

    /// Opens a best-effort live-event stream with delegated authority,
    /// approvals, or a lease attached to the authenticated request.
    ///
    /// # Errors
    ///
    /// Returns an error if filters are invalid, the peer cannot be reached, or
    /// the stream handshake is malformed.
    pub async fn subscribe_live_remote_with_authority(
        &self,
        peer: EndpointAddr,
        topics: Vec<String>,
        authority: Option<AuthorityPresentation>,
    ) -> Result<IrohLiveEventSubscription, IrohReplicationError> {
        validate_live_topics(&topics).map_err(IrohReplicationError::Stream)?;
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request_with_authority(
            &mut send,
            &ReplicationRequest::FollowLive { topics },
            authority,
        )
        .await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote live subscription failed: {message}"
                )));
            }
            ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a live event before its source identity".to_owned(),
                ));
            }
        };
        Ok(IrohLiveEventSubscription {
            connection,
            receive,
            source_node,
        })
    }

    /// Lists one bounded page of scopes visible to this authenticated peer.
    ///
    /// Scope identifiers are ordered lexically. Passing `next_after` from the
    /// previous page resumes without duplicates. Event bodies are never part
    /// of the catalog response.
    ///
    /// # Errors
    ///
    /// Returns an error if the page limit is too large, the peer cannot be
    /// reached, or the response is malformed.
    pub async fn list_scopes_remote(
        &self,
        peer: EndpointAddr,
        after: Option<ScopeId>,
        limit: NonZeroUsize,
    ) -> Result<ScopeCatalogPage, IrohReplicationError> {
        if limit.get() > MAX_SCOPE_CATALOG_PAGE {
            return Err(IrohReplicationError::Cursor(format!(
                "scope catalog page exceeds {MAX_SCOPE_CATALOG_PAGE} entries"
            )));
        }
        let wire_limit = u32::try_from(limit.get()).map_err(|error| {
            IrohReplicationError::Cursor(format!("scope catalog limit is invalid: {error}"))
        })?;
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(
            &mut send,
            &ReplicationRequest::ListScopes {
                after: after.clone(),
                limit: wire_limit,
            },
        )
        .await?;
        let page = match read_frame(&mut receive).await? {
            ReplicationFrame::ScopeCatalog { page } => *page,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote scope catalog failed: {message}"
                )));
            }
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent an unexpected frame for a scope catalog".to_owned(),
                ));
            }
        };
        if page.scopes.len() > limit.get()
            || page.scopes.windows(2).any(|pair| match pair {
                [left, right] => left.as_str() >= right.as_str(),
                _ => false,
            })
            || page.scopes.iter().any(|scope| {
                after
                    .as_ref()
                    .is_some_and(|cursor| scope.as_str() <= cursor.as_str())
            })
            || page.next_after.is_some() && page.next_after != page.scopes.last().cloned()
        {
            return Err(IrohReplicationError::Stream(
                "peer returned an invalid scope catalog page".to_owned(),
            ));
        }
        connection.close(0u32.into(), b"scope catalog complete");
        Ok(page)
    }

    /// Pulls immutable events after a peer-local cursor and ingests them idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached, the stream is malformed,
    /// or replicated history conflicts with stable command identity.
    pub async fn pull(
        &self,
        peer: EndpointAddr,
        after: Option<LogPosition>,
    ) -> Result<ReplicationReport, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(&mut send, &ReplicationRequest::Pull { after }).await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote history pull failed: {message}"
                )));
            }
            ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a batch before its source identity".to_owned(),
                ));
            }
        };
        let batch = match read_frame(&mut receive).await? {
            ReplicationFrame::Batch { batch } => *batch,
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. }
            | ReplicationFrame::Error { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a second source identity frame".to_owned(),
                ));
            }
        };
        if batch.source_node != source_node {
            return Err(IrohReplicationError::Stream(
                "replication batch does not match the advertised source node".to_owned(),
            ));
        }
        let report = self.node.ingest_batch(batch)?;
        connection.close(0u32.into(), b"sync complete");
        Ok(report)
    }

    /// Pulls one selected history from a source- and selection-checked checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint belongs to another selection, the
    /// peer cannot be reached, or replicated history conflicts locally.
    pub async fn pull_selected(
        &self,
        peer: EndpointAddr,
        selection: ReplicationSelection,
        checkpoint: Option<SelectedReplicationCheckpoint>,
    ) -> Result<SelectedReplicationReport, IrohReplicationError> {
        if checkpoint.as_ref().is_some_and(|checkpoint| {
            !selected_response_matches_request(&checkpoint.selection, &selection)
        }) {
            return Err(IrohReplicationError::Cursor(
                "selected checkpoint belongs to another replication selection".to_owned(),
            ));
        }
        let after = checkpoint.as_ref().and_then(|value| value.position);
        let mut batch = self
            .fetch_selected_batch(peer.clone(), selection.clone(), after)
            .await?;
        if checkpoint.as_ref().is_some_and(|value| {
            value.source_node != batch.source_node || value.selection != batch.selection
        }) {
            batch = self.fetch_selected_batch(peer, selection, None).await?;
        }
        self.node
            .ingest_selected_batch(batch)
            .map_err(IrohReplicationError::Ingest)
    }

    async fn fetch_selected_batch(
        &self,
        peer: EndpointAddr,
        selection: ReplicationSelection,
        after: Option<LogPosition>,
    ) -> Result<SelectedReplicationBatch, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(
            &mut send,
            &ReplicationRequest::PullSelected {
                selection: selection.clone(),
                after,
            },
        )
        .await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote selected pull failed: {message}"
                )));
            }
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a selected batch before its source identity".to_owned(),
                ));
            }
        };
        let batch = match read_frame(&mut receive).await? {
            ReplicationFrame::SelectedBatch { batch } => *batch,
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer sent an unexpected frame for a selected pull".to_owned(),
                ));
            }
        };
        if batch.source_node != source_node
            || !selected_response_matches_request(&batch.selection, &selection)
        {
            return Err(IrohReplicationError::Stream(
                "selected batch does not match the advertised source or requested selection"
                    .to_owned(),
            ));
        }
        connection.close(0u32.into(), b"selected sync complete");
        Ok(batch)
    }

    /// Pulls one exact scope from a source- and scope-checked checkpoint.
    ///
    /// Unrelated events are not disclosed or ingested. If the authenticated
    /// transport peer now advertises a different Myko source history, the stale
    /// position is discarded and the scope is replayed from its beginning.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint belongs to another scope, the peer
    /// cannot be reached, or replicated history conflicts with command identity.
    pub async fn pull_scope(
        &self,
        peer: EndpointAddr,
        scope_id: ScopeId,
        checkpoint: Option<ScopedReplicationCheckpoint>,
    ) -> Result<ScopedReplicationReport, IrohReplicationError> {
        Self::pull_scope_on(
            &self.node,
            self.router.endpoint(),
            peer,
            scope_id,
            checkpoint,
        )
        .await
    }

    pub(super) async fn pull_scope_on(
        node: &Node,
        endpoint: &Endpoint,
        peer: EndpointAddr,
        scope_id: ScopeId,
        checkpoint: Option<ScopedReplicationCheckpoint>,
    ) -> Result<ScopedReplicationReport, IrohReplicationError> {
        if checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.scope_id != scope_id)
        {
            return Err(IrohReplicationError::Cursor(
                "scoped checkpoint belongs to another scope".to_owned(),
            ));
        }
        let after = checkpoint.as_ref().and_then(|value| value.position);
        let mut batch =
            Self::fetch_scoped_batch(endpoint, peer.clone(), scope_id.clone(), after).await?;
        if checkpoint
            .as_ref()
            .is_some_and(|value| value.source_node != batch.source_node)
        {
            batch = Self::fetch_scoped_batch(endpoint, peer, scope_id, None).await?;
        }
        node.ingest_scoped_batch(batch)
            .map_err(IrohReplicationError::Ingest)
    }

    async fn fetch_scoped_batch(
        endpoint: &Endpoint,
        peer: EndpointAddr,
        scope_id: ScopeId,
        after: Option<LogPosition>,
    ) -> Result<ScopedReplicationBatch, IrohReplicationError> {
        let connection = endpoint
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(
            &mut send,
            &ReplicationRequest::PullScope {
                scope_id: scope_id.clone(),
                after,
            },
        )
        .await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote scoped pull failed: {message}"
                )));
            }
            ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a scoped batch before its source identity".to_owned(),
                ));
            }
        };
        let batch = match read_frame(&mut receive).await? {
            ReplicationFrame::ScopedBatch { batch } => *batch,
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. }
            | ReplicationFrame::Error { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent an unexpected frame for a scoped pull".to_owned(),
                ));
            }
        };
        if batch.source_node != source_node || batch.scope_id != scope_id {
            return Err(IrohReplicationError::Stream(
                "scoped batch does not match the advertised source or requested scope".to_owned(),
            ));
        }
        connection.close(0u32.into(), b"scoped sync complete");
        Ok(batch)
    }

    async fn remote_command_request(
        &self,
        peer: EndpointAddr,
        request: ReplicationRequest,
    ) -> Result<CommandResponse, IrohReplicationError> {
        self.remote_command_request_with_authority(peer, request, None)
            .await
    }

    pub(super) async fn remote_single_frame(
        &self,
        peer: EndpointAddr,
        request: ReplicationRequest,
        authority: Option<AuthorityPresentation>,
    ) -> Result<ReplicationFrame, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request_envelope(
            &mut send,
            &NodeRequestEnvelope {
                destination: None,
                authority,
                forwarding_provenance: Vec::new(),
                request,
            },
        )
        .await?;
        let frame = read_frame(&mut receive).await?;
        connection.close(0u32.into(), b"request complete");
        Ok(frame)
    }

    pub(super) async fn remote_command_request_with_authority(
        &self,
        peer: EndpointAddr,
        request: ReplicationRequest,
        authority: Option<AuthorityPresentation>,
    ) -> Result<CommandResponse, IrohReplicationError> {
        let response = match self.remote_single_frame(peer, request, authority).await? {
            ReplicationFrame::Command { response } => *response,
            ReplicationFrame::Authorization { decision } => {
                return Err(authorization_error(decision));
            }
            ReplicationFrame::AuthorityUnavailable { reason } => {
                return Err(IrohReplicationError::AuthorityUnavailable(reason));
            }
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote command failed: {message}"
                )));
            }
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a replication frame for a command request".to_owned(),
                ));
            }
        };
        Ok(response)
    }

    /// Reads one bounded command-catalog page from an authenticated peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached, denies the scope, or
    /// returns a non-catalog response.
    pub async fn command_state_page_remote(
        &self,
        peer: EndpointAddr,
        request: CommandStateRequest,
    ) -> Result<CommandStatePage, IrohReplicationError> {
        self.command_state_page_remote_with_authority(peer, request, None)
            .await
    }

    pub(super) async fn command_state_page_remote_with_authority(
        &self,
        peer: EndpointAddr,
        request: CommandStateRequest,
        authority: Option<AuthorityPresentation>,
    ) -> Result<CommandStatePage, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request_with_authority(
            &mut send,
            &ReplicationRequest::CommandState { request },
            authority,
        )
        .await?;
        let page = match read_frame(&mut receive).await? {
            ReplicationFrame::CommandState { page } => *page,
            ReplicationFrame::Authorization { decision } => {
                return Err(authorization_error(decision));
            }
            ReplicationFrame::AuthorityUnavailable { reason } => {
                return Err(IrohReplicationError::AuthorityUnavailable(reason));
            }
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote command catalog failed: {message}"
                )));
            }
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a non-catalog frame for a command catalog request".to_owned(),
                ));
            }
        };
        connection.close(0u32.into(), b"command catalog complete");
        Ok(page)
    }

    /// Reads one bounded current-state projection from an authenticated peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached, denies the scope, or
    /// cannot materialize the requested item schema.
    pub async fn item_state_page_remote(
        &self,
        peer: EndpointAddr,
        request: ItemStateRequest,
    ) -> Result<ItemStatePage, IrohReplicationError> {
        self.item_state_page_remote_with_authority(peer, request, None)
            .await
    }

    pub(super) async fn item_state_page_remote_with_authority(
        &self,
        peer: EndpointAddr,
        request: ItemStateRequest,
        authority: Option<AuthorityPresentation>,
    ) -> Result<ItemStatePage, IrohReplicationError> {
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request_with_authority(
            &mut send,
            &ReplicationRequest::ItemState { request },
            authority,
        )
        .await?;
        let page = match read_frame(&mut receive).await? {
            ReplicationFrame::ItemState { page } => *page,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote item query failed: {message}"
                )));
            }
            ReplicationFrame::Authorization { decision } => {
                return Err(authorization_error(decision));
            }
            ReplicationFrame::AuthorityUnavailable { reason } => {
                return Err(IrohReplicationError::AuthorityUnavailable(reason));
            }
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a replication frame for an item query".to_owned(),
                ));
            }
        };
        connection.close(0u32.into(), b"item query complete");
        Ok(page)
    }

    /// Durably submits a command to an authenticated peer without claiming it.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached, rejects conflicting
    /// command reuse, or cannot durably store the submission.
    pub(super) async fn submit_remote(
        &self,
        peer: EndpointAddr,
        command: CommandSubmission,
    ) -> Result<CommandResponse, IrohReplicationError> {
        self.remote_command_request(peer, ReplicationRequest::Submit { command })
            .await
    }

    /// Reads the latest durable command state from an authenticated peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached or query its history.
    pub async fn command_remote(
        &self,
        peer: EndpointAddr,
        command_id: CommandId,
    ) -> Result<CommandResponse, IrohReplicationError> {
        self.remote_command_request(peer, ReplicationRequest::Command { command_id })
            .await
    }

    /// Durably cancels submitted or executing work on an authenticated peer.
    ///
    /// Cancellation remains idempotent and cannot overwrite a command that
    /// already reached a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer cannot be reached, the command is unknown,
    /// or the cancellation cannot be durably recorded.
    pub async fn cancel_remote(
        &self,
        peer: EndpointAddr,
        command_id: CommandId,
        reason: String,
    ) -> Result<CommandResponse, IrohReplicationError> {
        self.remote_command_request(peer, ReplicationRequest::Cancel { command_id, reason })
            .await
    }

    /// Continuously follows a peer's immutable log from a cursor.
    ///
    /// The peer first replays history after the cursor and then pushes new
    /// events over the same framed stream. Connection failures are retained in
    /// [`PeerSyncStatus`] and retried after the supplied interval.
    #[must_use]
    pub fn follow(
        &self,
        peer: EndpointAddr,
        after: Option<LogPosition>,
        retry_interval: Duration,
    ) -> PeerSync {
        self.spawn_follower(
            peer,
            FollowerConfig::new(
                None,
                None,
                after,
                retry_interval,
                None,
                FollowSelection::All,
            ),
        )
    }

    /// Continuously follows one exact scope from a checked checkpoint.
    ///
    /// The follower advances its remote cursor across unrelated source events,
    /// but only matching immutable events enter the local node projection. A
    /// changed source identity resets and replays the scope automatically.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint belongs to another scope.
    pub fn follow_scope(
        &self,
        peer: EndpointAddr,
        scope_id: ScopeId,
        checkpoint: Option<ScopedReplicationCheckpoint>,
        retry_interval: Duration,
    ) -> Result<PeerSync, IrohReplicationError> {
        if checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.scope_id != scope_id)
        {
            return Err(IrohReplicationError::Cursor(
                "scoped checkpoint belongs to another scope".to_owned(),
            ));
        }
        let source_node = checkpoint.as_ref().map(|value| value.source_node);
        let cursor = checkpoint.and_then(|value| value.position);
        Ok(self.spawn_follower(
            peer,
            FollowerConfig::new(
                None,
                source_node,
                cursor,
                retry_interval,
                None,
                FollowSelection::Scope(scope_id),
            ),
        ))
    }

    /// Continuously follows a peer from its last durable local checkpoint.
    ///
    /// The Iroh endpoint identity is the stable peer key, while an explicit
    /// stream handshake identifies the Myko history currently served by that
    /// peer. Each positioned checkpoint is saved only after its batch was
    /// ingested successfully. If a peer keeps its transport key but replaces
    /// its Myko journal, the follower durably resets and replays the new source
    /// from its beginning.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial durable checkpoint cannot be loaded.
    pub fn follow_persisted(
        &self,
        peer: EndpointAddr,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<PeerSync, IrohReplicationError> {
        self.follow_persisted_selected(peer, ReplicationSelection::All, store, retry_interval)
    }

    /// Continuously follows selected peer history from its durable checkpoint.
    ///
    /// Each selection has an independent, versioned cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial durable checkpoint cannot be loaded.
    pub fn follow_persisted_selected(
        &self,
        peer: EndpointAddr,
        selection: ReplicationSelection,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<PeerSync, IrohReplicationError> {
        let key = replication_cursor_key(peer.id, &selection);
        let mut checkpoint = store
            .load_checkpoint(&key)
            .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
        if selection != ReplicationSelection::All
            && checkpoint.as_ref().is_some_and(|checkpoint| {
                !selected_response_matches_request(&checkpoint.selection, &selection)
            })
        {
            checkpoint = checkpoint.map(|checkpoint| {
                ReplicationCheckpoint::selected(checkpoint.source_node, None, selection.clone())
            });
            if let Some(checkpoint) = checkpoint.as_ref() {
                store
                    .save_checkpoint(&key, checkpoint.clone())
                    .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
            }
        }
        // A stored cursor resumes transfer but cannot prove the remote is
        // still at that head. Completeness is restored only by a fresh
        // authenticated batch, including an empty batch at the current head.
        let source_node = checkpoint.as_ref().map(|checkpoint| checkpoint.source_node);
        let cursor = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.position);
        let effective_selection = checkpoint.map(|checkpoint| checkpoint.selection);
        if let (Some(source_node), Some(position), Some(effective)) =
            (source_node, cursor, effective_selection.as_ref())
        {
            self.node
                .prepare_replication_resume(source_node, effective.clone(), Some(position))
                .map_err(IrohReplicationError::Ingest)?;
        }
        Ok(self.spawn_follower(
            peer,
            FollowerConfig::new(
                None,
                source_node,
                cursor,
                retry_interval,
                Some(CursorPersistence { key, store }),
                FollowSelection::replication(selection),
            )
            .with_effective_selection(effective_selection),
        ))
    }

    /// Continuously follows one exact Myko history from a durable checkpoint.
    ///
    /// Unlike [`Self::follow_persisted`], this follower does not treat a new
    /// Myko history behind the same Iroh endpoint as a replacement to replay.
    /// The handshake must advertise `expected_source_node`; otherwise the
    /// follower records an error, ingests nothing, and retries. This is the
    /// pairing-safe path for a descriptor that binds both identities.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial durable checkpoint cannot be loaded or
    /// reset to the pinned source identity.
    pub fn follow_persisted_source(
        &self,
        peer: EndpointAddr,
        expected_source_node: NodeId,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<PeerSync, IrohReplicationError> {
        self.follow_persisted_source_selected(
            peer,
            expected_source_node,
            ReplicationSelection::All,
            store,
            retry_interval,
        )
    }

    /// Continuously follows selected history from one pinned Myko source.
    ///
    /// # Errors
    ///
    /// Returns an error when the selection-specific checkpoint cannot be
    /// loaded or reset to the pinned source identity.
    pub fn follow_persisted_source_selected(
        &self,
        peer: EndpointAddr,
        expected_source_node: NodeId,
        selection: ReplicationSelection,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<PeerSync, IrohReplicationError> {
        let key = replication_cursor_key(peer.id, &selection);
        let mut checkpoint = store
            .load_checkpoint(&key)
            .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
        if selection != ReplicationSelection::All
            && checkpoint.as_ref().is_some_and(|checkpoint| {
                !selected_response_matches_request(&checkpoint.selection, &selection)
            })
        {
            checkpoint = checkpoint.map(|checkpoint| {
                ReplicationCheckpoint::selected(checkpoint.source_node, None, selection.clone())
            });
            if let Some(checkpoint) = checkpoint.as_ref() {
                store
                    .save_checkpoint(&key, checkpoint.clone())
                    .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
            }
        }
        // Persisted coverage is intentionally stale until this follower
        // revalidates the remote head and effective selection.
        let (cursor, effective_selection) = match checkpoint {
            Some(checkpoint) if checkpoint.source_node == expected_source_node => {
                (checkpoint.position, Some(checkpoint.selection))
            }
            Some(_) => {
                store
                    .save_checkpoint(
                        &key,
                        ReplicationCheckpoint::selected(
                            expected_source_node,
                            None,
                            selection.clone(),
                        ),
                    )
                    .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
                (None, Some(selection.clone()))
            }
            None => (None, None),
        };
        if let (Some(position), Some(effective)) = (cursor, effective_selection.as_ref()) {
            self.node
                .prepare_replication_resume(expected_source_node, effective.clone(), Some(position))
                .map_err(IrohReplicationError::Ingest)?;
        }
        Ok(self.spawn_follower(
            peer,
            FollowerConfig::new(
                Some(expected_source_node),
                Some(expected_source_node),
                cursor,
                retry_interval,
                Some(CursorPersistence { key, store }),
                FollowSelection::replication(selection),
            )
            .with_effective_selection(effective_selection),
        ))
    }

    fn spawn_follower(&self, peer: EndpointAddr, config: FollowerConfig) -> PeerSync {
        let FollowerConfig {
            mut cursor,
            retry_interval,
            persistence,
            selection,
        } = config;
        let replicator = self.clone();
        let initial_status = PeerSyncStatus {
            peer: peer.clone(),
            expected_source_node: cursor.expected_source_node,
            source_node: cursor.source_node,
            cursor: cursor.cursor,
            connected: false,
            successful_connections: 0,
            successful_batches: 0,
            last_error: None,
        };
        let (task_status, status) = watch::channel(initial_status);
        let (shutdown, mut shutdown_requested) = watch::channel(false);
        if let Some(source_node) = cursor.expected_source_node.or(cursor.source_node) {
            let _ignored = self.node.mark_replication_source_unreachable(source_node);
        }
        let task = tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    result = replicator.consume_follow_stream(
                        peer.clone(),
                        &mut cursor,
                        persistence.as_ref(),
                        &task_status,
                        &selection,
                    ) => result,
                    changed = shutdown_requested.changed() => {
                        if changed.is_err() || *shutdown_requested.borrow() {
                            break;
                        }
                        continue;
                    }
                };
                task_status.send_modify(|current| {
                    current.connected = false;
                    current.source_node = cursor.source_node;
                    current.cursor = cursor.cursor;
                    current.last_error = result.as_ref().err().map(ToString::to_string);
                });
                if result.is_ok() {
                    continue;
                }
                if let Some(source_node) = cursor.source_node.or(cursor.expected_source_node) {
                    let _ignored = replicator
                        .node
                        .mark_replication_source_unreachable(source_node);
                }

                tokio::select! {
                    () = tokio::time::sleep(retry_interval) => {}
                    changed = shutdown_requested.changed() => {
                        if changed.is_err() || *shutdown_requested.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        PeerSync {
            shutdown,
            task,
            status,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn consume_follow_stream(
        &self,
        peer: EndpointAddr,
        cursor: &mut FollowCursorState,
        persistence: Option<&CursorPersistence>,
        status: &watch::Sender<PeerSyncStatus>,
        selection: &FollowSelection,
    ) -> Result<(), IrohReplicationError> {
        let peer_id = peer.id;
        let connection = self
            .router
            .endpoint()
            .connect(peer, MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        let request = match selection {
            FollowSelection::All => ReplicationRequest::Follow {
                after: cursor.cursor,
            },
            FollowSelection::Scope(scope_id) => ReplicationRequest::FollowScope {
                scope_id: scope_id.clone(),
                after: cursor.cursor,
            },
            FollowSelection::Selected(selection) => ReplicationRequest::FollowSelected {
                selection: selection.clone(),
                after: cursor.cursor,
            },
        };
        write_request(&mut send, &request).await?;
        let advertised_source = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote follow failed: {message}"
                )));
            }
            ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Authorization { .. }
            | ReplicationFrame::AuthorityUnavailable { .. }
            | ReplicationFrame::Approval { .. }
            | ReplicationFrame::ControlVote { .. }
            | ReplicationFrame::ControlProposal { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a batch before its source identity".to_owned(),
                ));
            }
        };
        if let Some(expected_source_node) = cursor.expected_source_node
            && expected_source_node != advertised_source
        {
            connection.close(0u32.into(), b"unexpected source history");
            return Err(IrohReplicationError::Stream(format!(
                "peer {peer_id} advertised Myko source {advertised_source}, expected {expected_source_node}"
            )));
        }
        self.node
            .mark_replication_source_reachable(advertised_source)
            .map_err(IrohReplicationError::Ingest)?;
        status.send_modify(|current| {
            current.connected = true;
            current.source_node = Some(advertised_source);
            current.successful_connections = current.successful_connections.saturating_add(1);
            current.last_error = None;
        });
        if cursor
            .source_node
            .is_some_and(|source| source != advertised_source)
        {
            cursor.source_node = Some(advertised_source);
            cursor.cursor = None;
            cursor.effective_selection = None;
            persist_cursor(
                persistence,
                follower_checkpoint(advertised_source, None, selection, None),
            )?;
            connection.close(0u32.into(), b"source history changed");
            return Ok(());
        }
        cursor.source_node = Some(advertised_source);
        persist_cursor(
            persistence,
            follower_checkpoint(
                advertised_source,
                cursor.cursor,
                selection,
                cursor.effective_selection.as_ref(),
            ),
        )?;
        loop {
            let frame = read_frame(&mut receive).await?;
            let through = self.ingest_follow_frame(selection, frame, advertised_source, cursor)?;
            persist_cursor(
                persistence,
                follower_checkpoint(
                    advertised_source,
                    through,
                    selection,
                    cursor.effective_selection.as_ref(),
                ),
            )?;
            cursor.cursor = through;
            status.send_modify(|current| {
                current.cursor = cursor.cursor;
                current.successful_batches = current.successful_batches.saturating_add(1);
                current.last_error = None;
            });
        }
    }

    fn ingest_follow_frame(
        &self,
        selection: &FollowSelection,
        frame: ReplicationFrame,
        source_node: NodeId,
        cursor: &mut FollowCursorState,
    ) -> Result<Option<LogPosition>, IrohReplicationError> {
        match (selection, frame) {
            (FollowSelection::All, ReplicationFrame::Batch { batch }) => {
                if batch.after != cursor.cursor || batch.source_node != source_node {
                    return Err(IrohReplicationError::Stream(
                        "full follower received a mismatched source or cursor".to_owned(),
                    ));
                }
                self.node
                    .ingest_batch(*batch)
                    .map_err(IrohReplicationError::Ingest)
                    .map(|report| report.through)
            }
            (FollowSelection::Scope(scope_id), ReplicationFrame::ScopedBatch { batch }) => {
                if batch.after != cursor.cursor
                    || batch.source_node != source_node
                    || &batch.scope_id != scope_id
                {
                    return Err(IrohReplicationError::Stream(
                        "scoped follower received a mismatched source, scope, or cursor".to_owned(),
                    ));
                }
                self.node
                    .ingest_scoped_batch(*batch)
                    .map_err(IrohReplicationError::Ingest)
                    .map(|report| report.through)
            }
            (FollowSelection::Selected(selection), ReplicationFrame::SelectedBatch { batch }) => {
                if batch.after != cursor.cursor
                    || batch.source_node != source_node
                    || !selected_response_matches_request(&batch.selection, selection)
                {
                    return Err(IrohReplicationError::Stream(
                        "selected follower received a mismatched source, selection, or cursor"
                            .to_owned(),
                    ));
                }
                if cursor
                    .effective_selection
                    .as_ref()
                    .is_some_and(|effective| {
                        effective != &batch.selection && cursor.cursor.is_some()
                    })
                {
                    cursor.cursor = None;
                    cursor.effective_selection = None;
                    return Err(IrohReplicationError::Stream(
                        "selected follower effective selection changed; replay is required"
                            .to_owned(),
                    ));
                }
                cursor.effective_selection = Some(batch.selection.clone());
                self.node
                    .ingest_selected_batch(*batch)
                    .map_err(IrohReplicationError::Ingest)
                    .map(|report| report.through)
            }
            (_, ReplicationFrame::Error { message }) => Err(IrohReplicationError::Stream(format!(
                "remote follow failed: {message}"
            ))),
            _ => Err(IrohReplicationError::Stream(
                "peer sent an unexpected frame on a follow stream".to_owned(),
            )),
        }
    }

    /// Gracefully shuts down the Iroh router and endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the router cannot shut down cleanly.
    pub async fn shutdown(self) -> Result<(), IrohReplicationError> {
        self.router
            .shutdown()
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))
    }
}

#[derive(Clone)]
struct ReplicationProtocol {
    sessions: FederatedSession,
}

impl std::fmt::Debug for ReplicationProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicationProtocol")
            .field("node_id", &self.sessions.node().node_id())
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for ReplicationProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.sessions.node().wait_until_ready().await;
        let (mut send, mut receive) = connection.accept_bi().await?;
        let request = read_request(&mut receive).await?;
        let principal = endpoint_principal_id(connection.remote_id());
        let mut frames = self.sessions.open(principal, request).await;
        while let Some(frame) = frames.recv().await {
            write_frame(&mut send, &frame)
                .await
                .map_err(AcceptError::from_err)?;
        }
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}
