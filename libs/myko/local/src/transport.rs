use super::*;
fn authorized_request_envelope(
    destination: Option<NodeId>,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: &[ProvenanceHop],
    request: PeerRequest,
) -> Envelope<NodeRequestEnvelope> {
    Envelope::new(NodeRequestEnvelope {
        destination,
        authority,
        forwarding_provenance: forwarding_provenance.to_vec(),
        request,
    })
}

/// Failure while serving or consuming the owner-local peer transport.
#[derive(Debug, Error)]
pub enum LocalPeerError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error("local peer I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("local peer frame encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("local peer protocol failed: {0}")]
    Protocol(String),
    #[error("local peer authorization decision: {}", .0.public_message())]
    Authorization(Box<AuthorizationDecision>),
}

/// Protected local peer endpoint for one Myko node.
pub struct LocalNodeServer {
    socket_path: PathBuf,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), LocalPeerError>>,
}

impl LocalNodeServer {
    /// Binds an owner-only socket and starts accepting Myko peer requests.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe or active path, bind failure, or
    /// permission failure.
    pub async fn spawn(
        socket_path: impl AsRef<Path>,
        node: Node,
        principal_id: PrincipalId,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, LocalPeerError> {
        Self::spawn_application(
            socket_path,
            ApplicationHost::new(node, MykoApplication::new()).map_err(LocalPeerError::Protocol)?,
            principal_id,
            access_policy,
        )
        .await
    }

    /// Binds an owner-only socket for a node and its registered application.
    ///
    /// Typed query, report, and view handlers are served as persistent Hyphae
    /// lifecycle streams alongside the node's item and command contracts.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe or active path, bind failure, or
    /// permission failure.
    pub async fn spawn_application(
        socket_path: impl AsRef<Path>,
        application: ApplicationHost,
        principal_id: PrincipalId,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, LocalPeerError> {
        Self::spawn_sessions(
            socket_path,
            FederatedSession::for_application(application, access_policy),
            principal_id,
        )
        .await
    }

    /// Binds an owner-only socket to an existing semantic node endpoint.
    ///
    /// Use this when Iroh and WebSocket adapters serve the same node so all
    /// transports share handlers, authorization, and live events.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe or active path, bind failure, or
    /// permission failure.
    pub async fn spawn_sessions(
        socket_path: impl AsRef<Path>,
        sessions: FederatedSession,
        principal_id: PrincipalId,
    ) -> Result<Self, LocalPeerError> {
        Self::spawn_sessions_authenticated(socket_path, sessions, Principal::node(principal_id))
            .await
    }

    /// Binds a local socket to an explicit complete authenticated principal.
    /// This is the local transport seam for person/agent/service identities;
    /// the wire cannot substitute another kind with the same string ID.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe or active path, bind failure, or
    /// permission failure.
    pub async fn spawn_sessions_authenticated(
        socket_path: impl AsRef<Path>,
        sessions: FederatedSession,
        principal: Principal,
    ) -> Result<Self, LocalPeerError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        tracing::info!(
            socket_path = %socket_path.display(),
            principal_id = %principal.id,
            principal_kind = ?principal.kind,
            "local Myko transport listening"
        );
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(serve(listener, sessions, principal, shutdown_rx));
        Ok(Self {
            socket_path,
            shutdown,
            task,
        })
    }

    /// Stops all local peer streams and removes the owned socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the server task or safe socket cleanup fails.
    pub async fn shutdown(self) -> Result<(), LocalPeerError> {
        self.shutdown.send_replace(true);
        let served = self.task.await.map_err(|error| {
            LocalPeerError::Protocol(format!("local peer server task failed: {error}"))
        })?;
        let cleanup = remove_owned_socket(&self.socket_path);
        served?;
        cleanup
    }
}

/// Transport-level client for one owner-local Myko node.
///
/// Application commands, items, and handlers retain their specialized typed
/// clients. This client exposes the remaining canonical node-session
/// operations without introducing an application-specific local protocol.
/// Connection attempts continue until the socket becomes available or the
/// pending operation is cancelled.
#[derive(Debug, Clone)]
pub struct LocalNodeClient {
    socket_path: PathBuf,
    destination: Option<NodeId>,
    reconnect_policy: ReconnectPolicy,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalNodeClient {
    /// Creates a client for one protected local Myko socket.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            destination: None,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
            forwarding_provenance: Vec::new(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent local requests and streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> Envelope<NodeRequestEnvelope> {
        authorized_request_envelope(
            self.destination,
            self.authority.clone(),
            &self.forwarding_provenance,
            request,
        )
    }

    /// Returns the stable identity of the serving Myko node.
    ///
    /// # Errors
    ///
    /// Waits through transient socket unavailability. Returns an error if an
    /// established connection violates the canonical identify exchange.
    pub async fn identify(&self) -> Result<NodeId, LocalPeerError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(&mut stream, &self.envelope(PeerRequest::Identify)).await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::Hello { source_node } => Ok(source_node),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer returned a non-identity frame".to_owned(),
            )),
        }
    }

    /// Opens a best-effort stream of canonical Myko live events.
    ///
    /// Topics are exact matches. An empty list follows every event. Durable
    /// application views remain the recovery path after a sequence gap.
    ///
    /// # Errors
    ///
    /// Waits through transient socket unavailability. Returns an error if the
    /// live-stream handshake does not identify its serving node.
    pub async fn follow_live(
        &self,
        topics: Vec<String>,
    ) -> Result<LocalLiveEventSubscription, LocalPeerError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(
            &mut stream,
            &self.envelope(PeerRequest::FollowLive { topics }),
        )
        .await?;
        let source_node = match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::Hello { source_node } => source_node,
            PeerFrame::Error { message } => return Err(LocalPeerError::Protocol(message)),
            _ => {
                return Err(LocalPeerError::Protocol(
                    "local peer sent a live event before its source identity".to_owned(),
                ));
            }
        };
        Ok(LocalLiveEventSubscription {
            stream,
            source_node,
        })
    }
}

/// Durable application-handler connector for the retained [`MykoClient`].
///
/// This type only adapts the owner-local Unix transport. Handler ownership,
/// typed decoding, revision checks, and reactive reconnects stay in
/// [`MykoClient`].
#[derive(Debug, Clone)]
pub struct LocalHandlerConnector {
    socket_path: PathBuf,
    destination: Option<NodeId>,
    reconnect_policy: ReconnectPolicy,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalHandlerConnector {
    /// Creates a connector for one protected local Myko socket.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            destination: None,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
            forwarding_provenance: Vec::new(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for durable handler streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Attaches the original principal and validated delegation presentation.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Reserves the next store-backed node-forward delegation in the route.
    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.forwarding_provenance.push(hop);
        self
    }

    /// Builds the retained application client over this transport adapter.
    #[must_use]
    pub fn client(self) -> MykoClient {
        MykoClient::with_handler_connector(Arc::new(self))
    }

    fn envelope(&self, request: PeerRequest) -> Envelope<NodeRequestEnvelope> {
        authorized_request_envelope(
            self.destination,
            self.authority.clone(),
            &self.forwarding_provenance,
            request,
        )
    }
}

struct LocalHandlerConnection {
    stream: UnixStream,
}

#[async_trait::async_trait]
impl HandlerConnection for LocalHandlerConnection {
    async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
        read_authorized_peer_frame(&mut self.stream)
            .await
            .map_err(local_handler_error)
            .and_then(local_handler_frame)
    }
}

#[async_trait::async_trait]
impl HandlerConnector for LocalHandlerConnector {
    async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
        if let Some(destination) = self.destination {
            return Ok(destination);
        }
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(&mut stream, &self.envelope(PeerRequest::Identify))
            .await
            .map_err(local_handler_error)?;
        match read_authorized_peer_frame(&mut stream)
            .await
            .map_err(local_handler_error)?
        {
            PeerFrame::Hello { source_node } => Ok(source_node),
            PeerFrame::Error { message } => Err(HandlerClientError::Protocol(message)),
            frame => Err(HandlerClientError::Protocol(format!(
                "local peer returned {} during handler target identification",
                frame.kind()
            ))),
        }
    }

    async fn connect(
        &self,
        request: HandlerRequest,
    ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(
            &mut stream,
            &self.envelope(PeerRequest::FollowHandler { request }),
        )
        .await
        .map_err(local_handler_error)?;
        let initial = read_authorized_peer_frame(&mut stream)
            .await
            .map_err(local_handler_error)
            .and_then(local_handler_frame)?;
        Ok((initial, Box::new(LocalHandlerConnection { stream })))
    }

    fn at(&self, destination: NodeId) -> Arc<dyn HandlerConnector> {
        Arc::new(self.clone().at(destination))
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        self.reconnect_policy
    }
}

fn local_handler_frame(frame: PeerFrame) -> Result<HandlerFrame, HandlerClientError> {
    match frame {
        PeerFrame::HandlerState { revision, state } => Ok(HandlerFrame::State {
            revision,
            state: *state,
        }),
        PeerFrame::HandlerViewDelta { revision, delta } => Ok(HandlerFrame::ViewDelta {
            revision,
            delta: *delta,
        }),
        PeerFrame::Error { message } => Err(HandlerClientError::Protocol(message)),
        frame => Err(HandlerClientError::Protocol(format!(
            "local handler stream returned {}",
            frame.kind()
        ))),
    }
}

fn local_handler_error(error: LocalPeerError) -> HandlerClientError {
    match error {
        LocalPeerError::Io(error) => HandlerClientError::Transport(error.to_string()),
        LocalPeerError::Json(error) => HandlerClientError::Decode(error),
        LocalPeerError::Protocol(message) => HandlerClientError::Protocol(message),
        LocalPeerError::Authorization(decision) => {
            HandlerClientError::Protocol(decision.public_message())
        }
        LocalPeerError::Node(error) => HandlerClientError::Transport(error.to_string()),
    }
}

/// One best-effort canonical live-event stream over the local Unix transport.
#[derive(Debug)]
pub struct LocalLiveEventSubscription {
    stream: UnixStream,
    source_node: NodeId,
}

impl LocalLiveEventSubscription {
    /// Returns the stable identity advertised by the serving node.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Receives the next canonical live event.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or changes frame type.
    pub async fn recv(&mut self) -> Result<LiveEvent, LocalPeerError> {
        match read_authorized_peer_frame(&mut self.stream).await? {
            PeerFrame::Live { event } => Ok(*event),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer returned a non-live frame".to_owned(),
            )),
        }
    }
}

/// Typed item client bound to one local Myko peer socket.
///
/// Connection attempts continue until the socket becomes available or the
/// pending operation is cancelled.
#[derive(Debug, Clone)]
pub struct LocalItemClient {
    socket_path: PathBuf,
    destination: Option<NodeId>,
    reconnect_policy: ReconnectPolicy,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalItemClient {
    /// Creates a local typed-state client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            destination: None,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
            forwarding_provenance: Vec::new(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent requests and streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> Envelope<NodeRequestEnvelope> {
        authorized_request_envelope(
            self.destination,
            self.authority.clone(),
            &self.forwarding_provenance,
            request,
        )
    }

    /// Reads and follows an explicit source already materialized by the peer.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, validation, or follow setup
    /// fails.
    pub async fn watch_items<Q>(
        &self,
        source_node: myko_federation::NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            LocalItemQuerySubscription<Q>,
        ),
        LocalPeerError,
    >
    where
        Q: ItemQuery,
    {
        self.watch_request(
            ItemStateRequest::for_item::<Q::Item>(source_node, scope_id),
            query,
        )
        .await
    }

    /// Reads and follows the serving node's authoritative typed state.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, validation, or follow setup
    /// fails.
    pub async fn watch_serving_items<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            LocalItemQuerySubscription<Q>,
        ),
        LocalPeerError,
    >
    where
        Q: ItemQuery,
    {
        self.watch_request(
            ItemStateRequest::for_serving_item::<Q::Item>(scope_id),
            query,
        )
        .await
    }

    /// Materializes a local typed stream into a first-class Hyphae cell.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, validation, or follow setup
    /// fails.
    pub async fn watch_serving_items_reactive<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<LocalReactiveItemSubscription<ItemQueryResult<Q>>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        ItemQueryResult<Q>: hyphae::CellValue,
    {
        self.watch_reactive_request(
            ItemStateRequest::for_serving_item::<Q::Item>(scope_id),
            query,
        )
        .await
    }

    /// Materializes an explicit replicated source into a reconnecting Hyphae
    /// cell through the owner-local socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial snapshot and follow stream cannot be
    /// established. Once returned, transient socket loss is represented as
    /// `Resynchronizing` on the same cell until a fresh gap-free watch starts.
    pub async fn watch_items_reactive<Q>(
        &self,
        source_node: myko_federation::NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<LocalReactiveItemSubscription<ItemQueryResult<Q>>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        ItemQueryResult<Q>: hyphae::CellValue,
    {
        self.watch_reactive_request(
            ItemStateRequest::for_item::<Q::Item>(source_node, scope_id),
            query,
        )
        .await
    }

    async fn watch_reactive_request<Q>(
        &self,
        request: ItemStateRequest,
        query: Q,
    ) -> Result<LocalReactiveItemSubscription<ItemQueryResult<Q>>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        ItemQueryResult<Q>: hyphae::CellValue,
    {
        let (initial, subscription) = self.watch_request(request.clone(), query.clone()).await?;
        Ok(drive_reactive(
            self.clone(),
            request,
            query,
            initial,
            subscription,
        ))
    }

    async fn watch_request<Q>(
        &self,
        request: ItemStateRequest,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            LocalItemQuerySubscription<Q>,
        ),
        LocalPeerError,
    >
    where
        Q: ItemQuery,
    {
        let snapshot = self.item_state(request).await?;
        let (initial, stream) = ItemQueryStream::from_snapshot(&snapshot, query)?;
        let subscription = LocalItemQuerySubscription::connect(
            &self.socket_path,
            self.destination,
            self.reconnect_policy,
            self.authority.clone(),
            self.forwarding_provenance.clone(),
            stream,
        )
        .await?;
        Ok((initial, subscription))
    }
}

impl ItemClient for LocalItemClient {
    type Error = LocalPeerError;

    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error> {
        Box::pin(async move {
            let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
            write_frame(
                &mut stream,
                &self.envelope(PeerRequest::ItemState { request }),
            )
            .await?;
            match read_authorized_peer_frame(&mut stream).await? {
                PeerFrame::ItemState { page } => Ok(*page),
                PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
                _ => Err(LocalPeerError::Protocol(
                    "local peer returned a non-item-state frame".to_owned(),
                )),
            }
        })
    }
}

/// Command client bound to one owner-local Myko peer socket.
///
/// Connection attempts continue until the socket becomes available or the
/// pending operation is cancelled.
#[derive(Debug, Clone)]
pub struct LocalCommandClient {
    socket_path: PathBuf,
    destination: Option<NodeId>,
    reconnect_policy: ReconnectPolicy,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalCommandClient {
    /// Creates a local command client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            destination: None,
            reconnect_policy: ReconnectPolicy::default(),
            authority: None,
            forwarding_provenance: Vec::new(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent local requests and streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Attaches the original principal and validated delegation presentation.
    ///
    /// The accepting session still binds the final executor to the local
    /// transport identity and the destination validates every stored fact.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Reserves the next store-backed node-forward delegation in the route.
    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> Envelope<NodeRequestEnvelope> {
        authorized_request_envelope(
            self.destination,
            self.authority.clone(),
            &self.forwarding_provenance,
            request,
        )
    }

    /// Reads one command and watches its lifecycle without polling.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or watch setup fails.
    pub async fn watch_command(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, LocalCommandSubscription), LocalPeerError> {
        self.watch_command_request(command_id, PeerRequest::WatchCommand { command_id })
            .await
    }

    async fn watch_command_request(
        &self,
        command_id: CommandId,
        request: PeerRequest,
    ) -> Result<(CommandResponse, LocalCommandSubscription), LocalPeerError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(&mut stream, &self.envelope(request)).await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::Command { response } if response.command.is_some() => Ok((
                (*response).clone(),
                LocalCommandSubscription {
                    stream,
                    source_node: response.source_node,
                    command_id,
                    current: response.command.clone().ok_or_else(|| {
                        LocalPeerError::Protocol(
                            "local command watch omitted initial state".to_owned(),
                        )
                    })?,
                },
            )),
            PeerFrame::Authorization { decision } => Err(LocalPeerError::Authorization(decision)),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer did not return command watch state".to_owned(),
            )),
        }
    }

    async fn request(&self, request: PeerRequest) -> Result<CommandResponse, LocalPeerError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(&mut stream, &self.envelope(request)).await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::Command { response } => Ok(*response),
            PeerFrame::Authorization { decision } => Err(LocalPeerError::Authorization(decision)),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer returned a non-command frame".to_owned(),
            )),
        }
    }

    /// Records one authenticated, immutable approval decision.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket is unavailable, the server rejects
    /// the authenticated decision, or the response is malformed.
    pub async fn approve_authority(
        &self,
        challenge_id: ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, LocalPeerError> {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(
            &mut stream,
            &self.envelope(PeerRequest::ApproveAuthority {
                challenge_id,
                approved,
            }),
        )
        .await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::Approval { decision } => Ok(*decision),
            PeerFrame::Authorization { decision } => Err(LocalPeerError::Authorization(decision)),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer returned a non-approval frame".to_owned(),
            )),
        }
    }
}

impl CommandClient for LocalCommandClient {
    type Error = LocalPeerError;

    fn submit_submission(
        &self,
        command: CommandSubmission,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::Submit { command }))
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::Command { command_id }))
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::Cancel { command_id, reason }))
    }
}

impl CommandSubscription for LocalCommandSubscription {
    type Error = LocalPeerError;

    fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error> {
        Box::pin(self.recv())
    }
}

impl CommandWatchingClient for LocalCommandClient {
    type Subscription = LocalCommandSubscription;

    fn watch_command(
        &self,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        Box::pin(async move {
            self.watch_command(command_id)
                .await
                .map(|(_initial, subscription)| subscription)
        })
    }

    fn watch_command_at(
        &self,
        source_node: NodeId,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        Box::pin(async move {
            self.clone()
                .at(source_node)
                .watch_command(command_id)
                .await
                .map(|(_initial, subscription)| subscription)
        })
    }
}

/// Current-then-live command lifecycle over an owner-local socket.
pub struct LocalCommandSubscription {
    stream: UnixStream,
    source_node: myko_federation::NodeId,
    command_id: CommandId,
    current: CommandSnapshot,
}

impl LocalCommandSubscription {
    /// Returns the serving node's stable identity.
    #[must_use]
    pub const fn source_node(&self) -> myko_federation::NodeId {
        self.source_node
    }

    /// Returns the latest received lifecycle state.
    #[must_use]
    pub const fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    /// Waits for the next durable lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or changes command identity.
    pub async fn recv(&mut self) -> Result<CommandSnapshot, LocalPeerError> {
        match read_authorized_peer_frame(&mut self.stream).await? {
            PeerFrame::Command { response }
                if response.source_node == self.source_node
                    && response
                        .command
                        .as_ref()
                        .is_some_and(|command| command.request.id == self.command_id) =>
            {
                self.current = response.command.ok_or_else(|| {
                    LocalPeerError::Protocol("local command update was empty".to_owned())
                })?;
                Ok(self.current.clone())
            }
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer changed command stream identity".to_owned(),
            )),
        }
    }
}

/// Lossless typed query stream over an owner-local socket.
pub struct LocalItemQuerySubscription<Q: ItemQuery> {
    stream: UnixStream,
    query: ItemQueryStream<Q>,
}

impl<Q: ItemQuery> LocalItemQuerySubscription<Q> {
    async fn connect(
        socket_path: &Path,
        destination: Option<NodeId>,
        reconnect_policy: ReconnectPolicy,
        authority: Option<AuthorityPresentation>,
        forwarding_provenance: Vec<ProvenanceHop>,
        query: ItemQueryStream<Q>,
    ) -> Result<Self, LocalPeerError> {
        let mut stream = connect_local_peer(socket_path, reconnect_policy).await;
        write_frame(
            &mut stream,
            &authorized_request_envelope(
                destination,
                authority,
                &forwarding_provenance,
                PeerRequest::FollowItems {
                    request: query.request().clone(),
                },
            ),
        )
        .await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::ItemFollowReady { request } if request.as_ref() == query.request() => {
                Ok(Self { stream, query })
            }
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer did not confirm the typed item stream".to_owned(),
            )),
        }
    }

    /// Returns the currently materialized query result.
    #[must_use]
    pub fn current(&self) -> ItemQueryResult<Q> {
        self.query.current()
    }

    /// Receives and atomically applies the next matching item update.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or violates its typed contract.
    pub async fn recv(&mut self) -> Result<ItemQueryUpdate<ItemQueryResult<Q>>, LocalPeerError> {
        match read_authorized_peer_frame(&mut self.stream).await? {
            PeerFrame::ItemUpdate { update } => Ok(self.query.apply(&update)?),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer sent a non-item frame on a typed item stream".to_owned(),
            )),
        }
    }
}

/// Runtime owner for a local Hyphae item subscription.
pub struct LocalReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    live: LiveSubscription<T>,
    writer: myko_federation::LiveSubscriptionWriter<T>,
    task: JoinHandle<()>,
}

impl<T> LocalReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    /// Returns the reactive value/cursor/liveness cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T> {
        &self.live
    }
}

impl<T> Drop for LocalReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

fn drive_reactive<Q>(
    client: LocalItemClient,
    request: ItemStateRequest,
    query: Q,
    initial: ItemQuerySnapshot<ItemQueryResult<Q>>,
    mut subscription: LocalItemQuerySubscription<Q>,
) -> LocalReactiveItemSubscription<ItemQueryResult<Q>>
where
    Q: ItemQuery + Send + 'static,
    ItemQueryResult<Q>: hyphae::CellValue,
{
    let (writer, live) = live_subscription(LiveSubscriptionState {
        value: Some(initial.value),
        through: initial.through,
        liveness: SubscriptionLiveness::Current,
    });
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(update) => {
                    task_writer.publish(update.value, Some(update.position));
                    continue;
                }
                Err(error) if local_subscription_error_is_recoverable(&error) => {
                    task_writer.resynchronizing(error.to_string());
                }
                Err(error) => {
                    task_writer.invalidate(error.to_string());
                    return;
                }
            }
            let mut delay = client.reconnect_policy.initial_delay();
            loop {
                tokio::time::sleep(delay).await;
                match client.watch_request(request.clone(), query.clone()).await {
                    Ok((snapshot, next)) => {
                        task_writer.publish(snapshot.value, snapshot.through);
                        subscription = next;
                        break;
                    }
                    Err(error) => {
                        if local_subscription_error_is_recoverable(&error) {
                            task_writer.resynchronizing(error.to_string());
                            delay = client.reconnect_policy.next_delay(delay);
                        } else {
                            task_writer.invalidate(error.to_string());
                            return;
                        }
                    }
                }
            }
        }
    });
    LocalReactiveItemSubscription { live, writer, task }
}

const fn local_subscription_error_is_recoverable(error: &LocalPeerError) -> bool {
    matches!(error, LocalPeerError::Io(_) | LocalPeerError::Protocol(_))
}

async fn connect_local_peer(socket_path: &Path, policy: ReconnectPolicy) -> UnixStream {
    let mut delay = policy.initial_delay();
    let mut attempts = 0_u64;
    loop {
        attempts = attempts.saturating_add(1);
        match UnixStream::connect(socket_path).await {
            Ok(stream) => {
                tracing::debug!(
                    socket_path = %socket_path.display(),
                    attempts,
                    "connected to local Myko transport"
                );
                return stream;
            }
            Err(error) => {
                tracing::debug!(
                    socket_path = %socket_path.display(),
                    attempts,
                    retry_after_ms = delay.as_millis(),
                    error = %error,
                    "local Myko transport unavailable; retrying"
                );
            }
        }
        tokio::time::sleep(delay).await;
        delay = policy.next_delay(delay);
    }
}

async fn serve(
    listener: UnixListener,
    sessions: FederatedSession,
    principal: Principal,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), LocalPeerError> {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let connection_id = NEXT_LOCAL_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::warn!(connection_id, "local connection limit reached; rejecting peer");
                    drop(stream);
                    continue;
                };
                tracing::debug!(
                    connection_id,
                    active_connections = connections.len(),
                    "accepted local Myko connection"
                );
                let span = tracing::debug_span!(
                    "myko.local.connection",
                    connection_id,
                    principal_id = %principal.id,
                );
                connections.spawn(
                    handle_connection(
                        stream,
                        sessions.clone(),
                        principal.clone(),
                        shutdown.clone(),
                        permit,
                    )
                    .instrument(span),
                );
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    return Err(LocalPeerError::Protocol(format!(
                        "local peer connection task failed: {error}"
                    )));
                }
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    sessions: FederatedSession,
    principal: Principal,
    shutdown: watch::Receiver<bool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let result = async {
        let request: Envelope<NodeRequestEnvelope> = read_frame(&mut stream).await?;
        let request = request
            .into_current()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        tracing::debug!(
            request = request.request.kind(),
            destination = ?request.destination,
            "received local Myko request"
        );
        serve_session_request(&mut stream, &sessions, principal, shutdown, request).await
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(error = %error, "local Myko connection failed");
        let _ignored = write_frame(
            &mut stream,
            &Envelope::new(PeerFrame::Error {
                message: error.to_string(),
            }),
        )
        .await;
    } else {
        tracing::debug!("local Myko connection closed");
    }
}

async fn serve_session_request(
    stream: &mut UnixStream,
    sessions: &FederatedSession,
    principal: Principal,
    mut shutdown: watch::Receiver<bool>,
    request: NodeRequestEnvelope,
) -> Result<(), LocalPeerError> {
    let mut frames = sessions.open_authenticated(principal, request).await;
    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(frame) = frame else {
                    tracing::debug!("session frame stream closed");
                    return Ok(());
                };
                tracing::trace!(frame = frame.kind(), "writing local Myko frame");
                write_frame(stream, &Envelope::new(frame)).await?;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::debug!("local Myko server shutdown closed connection");
                    return Ok(());
                }
            }
        }
    }
}

async fn read_peer_frame(stream: &mut UnixStream) -> Result<PeerFrame, LocalPeerError> {
    let envelope: Envelope<PeerFrame> = read_frame(stream).await?;
    envelope
        .into_current()
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))
}

async fn read_authorized_peer_frame(stream: &mut UnixStream) -> Result<PeerFrame, LocalPeerError> {
    loop {
        match read_peer_frame(stream).await? {
            PeerFrame::Authorization { decision }
                if matches!(decision.as_ref(), AuthorizationDecision::Permit(_)) =>
            {
                tracing::debug!("local Myko request authorized");
            }
            PeerFrame::Authorization { decision } => {
                tracing::warn!(decision = ?decision, "local Myko request authorization failed");
                return Err(LocalPeerError::Authorization(decision));
            }
            frame => {
                tracing::trace!(frame = frame.kind(), "read local Myko frame");
                return Ok(frame);
            }
        }
    }
}

async fn write_frame<T: Serialize + Sync>(
    stream: &mut UnixStream,
    value: &T,
) -> Result<(), LocalPeerError> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(LocalPeerError::Protocol(format!(
            "local peer frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    let length = u32::try_from(encoded.len()).map_err(|error| {
        LocalPeerError::Protocol(format!("local peer frame length is invalid: {error}"))
    })?;
    stream.write_u32(length).await?;
    stream.write_all(&encoded).await?;
    Ok(())
}

async fn read_frame<T: DeserializeOwned>(stream: &mut UnixStream) -> Result<T, LocalPeerError> {
    let length = stream.read_u32().await?;
    let length = usize::try_from(length).map_err(|error| {
        LocalPeerError::Protocol(format!("local peer frame length is invalid: {error}"))
    })?;
    if length > MAX_FRAME_BYTES {
        return Err(LocalPeerError::Protocol(format!(
            "local peer frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    let mut encoded = vec![0_u8; length];
    stream.read_exact(&mut encoded).await?;
    Ok(serde_json::from_slice(&encoded)?)
}

async fn prepare_socket_path(path: &Path) -> Result<(), LocalPeerError> {
    let parent = path.parent().ok_or_else(|| {
        LocalPeerError::Protocol("local peer socket path has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)?;
    if UnixStream::connect(path).await.is_ok() {
        return Err(LocalPeerError::Protocol(format!(
            "a Myko node is already serving {}",
            path.display()
        )));
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_socket() {
        return Err(LocalPeerError::Protocol(format!(
            "refusing to replace non-socket path {}",
            path.display()
        )));
    }
    fs::remove_file(path)?;
    Ok(())
}

fn remove_owned_socket(path: &Path) -> Result<(), LocalPeerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => Err(LocalPeerError::Protocol(format!(
            "refusing to remove replacement non-socket path {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
