use super::{
    ApprovalDecision, Arc, AuthorityPresentation, ChallengeId, CommandClient, CommandClientFuture,
    CommandId, CommandResponse, CommandSnapshot, CommandStateClient, CommandStatePageFuture,
    CommandStateRequest, CommandStateSnapshot, CommandStateStream, CommandSubmission,
    CommandSubscription, CommandSubscriptionFuture, CommandWatchFuture, CommandWatchingClient,
    Connection, EndpointAddr, FederatedSession, HandlerClientError, HandlerConnection,
    HandlerConnector, HandlerFrame, HandlerRequest, IrohReplicationError, ItemClient,
    ItemProjection, ItemQuery, ItemQueryResult, ItemQuerySnapshot, ItemQueryStream,
    ItemQueryUpdate, ItemStatePageFuture, ItemStateRequest, ItemStateSnapshot, JoinHandle,
    LiveSubscription, LiveSubscriptionState, MYKO_REPLICATION_ALPN, MykoClient, Node, NodeId,
    NodeRequestEnvelope, ProvenanceHop, ReconnectPolicy, RecvStream, ReplicationCursorKey,
    ReplicationCursorStore, ReplicationFrame, ReplicationRequest, ReplicationSelection, Router,
    ScopeId, SubscriptionLiveness, authorization_error, live_subscription, pairing,
    read_command_frame, read_frame, write_request_envelope, write_request_with_authority,
};
/// Running Iroh endpoint that serves and pulls Myko replication batches.
#[derive(Debug, Clone)]
pub struct IrohReplicator {
    pub(super) node: Node,
    pub(super) sessions: FederatedSession,
    pub(super) pairing: pairing::PairingRegistry,
    pub(super) router: Router,
}

/// Command-only client bound to one authenticated Iroh peer.
#[derive(Debug, Clone)]
pub struct IrohCommandClient {
    pub(super) replicator: IrohReplicator,
    pub(super) peer: EndpointAddr,
    pub(super) authority: Option<AuthorityPresentation>,
}

/// Authenticated current-then-live lifecycle stream for one native command.
pub struct IrohCommandSubscription {
    connection: Connection,
    receive: RecvStream,
    source_node: NodeId,
    command_id: CommandId,
    current: CommandSnapshot,
}

/// Authenticated snapshot-then-live stream for one filtered command catalog.
pub struct IrohCommandStateSubscription {
    connection: Connection,
    receive: RecvStream,
    stream: CommandStateStream,
}

/// Current-state client bound to one authenticated Iroh peer.
#[derive(Debug, Clone)]
pub struct IrohItemClient {
    pub(super) replicator: IrohReplicator,
    pub(super) peer: EndpointAddr,
    pub(super) reconnect_policy: ReconnectPolicy,
    pub(super) authority: Option<AuthorityPresentation>,
}

/// Iroh adapter for durable handlers owned by the retained [`MykoClient`].
#[derive(Debug, Clone)]
pub struct IrohHandlerConnector {
    pub(super) replicator: IrohReplicator,
    pub(super) peer: EndpointAddr,
    pub(super) destination: Option<NodeId>,
    pub(super) reconnect_policy: ReconnectPolicy,
    pub(super) authority: Option<AuthorityPresentation>,
    pub(super) forwarding_provenance: Vec<ProvenanceHop>,
}

impl IrohHandlerConnector {
    /// Overrides reconnect timing for durable handler streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Routes handler requests through the connected endpoint to another node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.destination = Some(destination);
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
}

struct IrohHandlerConnection {
    _connection: Connection,
    receive: RecvStream,
}

#[async_trait::async_trait]
impl HandlerConnection for IrohHandlerConnection {
    async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
        read_frame(&mut self.receive)
            .await
            .map_err(iroh_handler_error)
            .and_then(iroh_handler_frame)
    }
}

#[async_trait::async_trait]
impl HandlerConnector for IrohHandlerConnector {
    async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
        if let Some(destination) = self.destination {
            return Ok(destination);
        }
        self.replicator
            .identify_remote(self.peer.clone())
            .await
            .map_err(iroh_handler_error)
    }

    async fn connect(
        &self,
        request: HandlerRequest,
    ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
        let connection = self
            .replicator
            .router
            .endpoint()
            .connect(self.peer.clone(), MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| HandlerClientError::Transport(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| HandlerClientError::Transport(error.to_string()))?;
        write_request_envelope(
            &mut send,
            &NodeRequestEnvelope {
                destination: self.destination,
                authority: self.authority.clone(),
                forwarding_provenance: self.forwarding_provenance.clone(),
                request: ReplicationRequest::FollowHandler { request },
            },
        )
        .await
        .map_err(iroh_handler_error)?;
        let initial = read_frame(&mut receive)
            .await
            .map_err(iroh_handler_error)
            .and_then(iroh_handler_frame)?;
        Ok((
            initial,
            Box::new(IrohHandlerConnection {
                _connection: connection,
                receive,
            }),
        ))
    }

    fn at(&self, destination: NodeId) -> Arc<dyn HandlerConnector> {
        Arc::new(self.clone().at(destination))
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        self.reconnect_policy
    }
}

fn iroh_handler_frame(frame: ReplicationFrame) -> Result<HandlerFrame, HandlerClientError> {
    match frame {
        ReplicationFrame::HandlerState { revision, state } => Ok(HandlerFrame::State {
            revision,
            state: *state,
        }),
        ReplicationFrame::HandlerViewDelta { revision, delta } => Ok(HandlerFrame::ViewDelta {
            revision,
            delta: *delta,
        }),
        ReplicationFrame::Error { message } => Err(HandlerClientError::Protocol(message)),
        frame => Err(HandlerClientError::Protocol(format!(
            "Iroh handler stream returned {}",
            frame.kind()
        ))),
    }
}

fn iroh_handler_error(error: IrohReplicationError) -> HandlerClientError {
    match error {
        IrohReplicationError::Encoding(error) => HandlerClientError::Decode(error),
        IrohReplicationError::Authorization { message, .. } => {
            HandlerClientError::Protocol(message)
        }
        error => HandlerClientError::Transport(error.to_string()),
    }
}

/// Typed application-handler client bound to one authenticated Iroh peer.
pub struct IrohItemQuerySubscription<Q: ItemQuery> {
    connection: Connection,
    receive: RecvStream,
    stream: ItemQueryStream<Q>,
}

/// Runtime-owned Hyphae materialization of a native typed item stream.
pub struct IrohReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    live: LiveSubscription<T>,
    writer: myko_federation::LiveSubscriptionWriter<T>,
    task: JoinHandle<()>,
}

impl<T> IrohReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    /// Returns the transport-independent reactive lifecycle cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T> {
        &self.live
    }
}

impl<T> Drop for IrohReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

impl CommandClient for IrohCommandClient {
    type Error = IrohReplicationError;

    fn submit_submission(
        &self,
        command: CommandSubmission,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            if let Some(authority) = self.authority.clone() {
                self.replicator
                    .remote_command_request_with_authority(
                        self.peer.clone(),
                        ReplicationRequest::Submit { command },
                        Some(authority),
                    )
                    .await
            } else {
                self.replicator
                    .submit_remote(self.peer.clone(), command)
                    .await
            }
        })
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.replicator.remote_command_request_with_authority(
            self.peer.clone(),
            ReplicationRequest::Command { command_id },
            self.authority.clone(),
        ))
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.replicator.remote_command_request_with_authority(
            self.peer.clone(),
            ReplicationRequest::Cancel { command_id, reason },
            self.authority.clone(),
        ))
    }
}

impl CommandSubscription for IrohCommandSubscription {
    type Error = IrohReplicationError;

    fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error> {
        Box::pin(self.recv())
    }
}

impl CommandWatchingClient for IrohCommandClient {
    type Subscription = IrohCommandSubscription;

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
}

impl CommandStateClient for IrohCommandClient {
    type Error = IrohReplicationError;

    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error> {
        Box::pin(self.replicator.command_state_page_remote_with_authority(
            self.peer.clone(),
            request,
            self.authority.clone(),
        ))
    }
}

impl IrohCommandClient {
    /// Attaches a delegated authority, approvals, or lease to every request.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Records one authenticated approval decision on the remote authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the peer is unreachable, authorization fails, or
    /// the response is malformed.
    pub async fn approve_authority(
        &self,
        challenge_id: ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, IrohReplicationError> {
        let frame = self
            .replicator
            .remote_single_frame(
                self.peer.clone(),
                ReplicationRequest::ApproveAuthority {
                    challenge_id,
                    approved,
                },
                self.authority.clone(),
            )
            .await?;
        match frame {
            ReplicationFrame::Approval { decision } => Ok(*decision),
            ReplicationFrame::Authorization { decision } => Err(authorization_error(decision)),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(message)),
            _ => Err(IrohReplicationError::Stream(
                "peer returned a non-approval frame".to_owned(),
            )),
        }
    }

    /// Reads one current command state and watches its durable transitions
    /// without a query-to-subscribe cursor gap.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown, access is denied, or the
    /// peer returns a mismatched lifecycle stream.
    pub async fn watch_command(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, IrohCommandSubscription), IrohReplicationError> {
        IrohCommandSubscription::connect(
            &self.replicator,
            self.peer.clone(),
            self.authority.clone(),
            command_id,
        )
        .await
    }

    /// Reads one filtered command catalog and watches subsequent durable
    /// transitions without a snapshot-to-subscribe cursor gap.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, authorization, validation, or
    /// stream establishment fails.
    pub async fn watch_commands(
        &self,
        request: CommandStateRequest,
    ) -> Result<(CommandStateSnapshot, IrohCommandStateSubscription), IrohReplicationError> {
        let snapshot = self.command_states(request).await?;
        let subscription = self.watch_command_states(&snapshot).await?;
        Ok((snapshot, subscription))
    }

    /// Watches a command catalog from an already collected snapshot.
    ///
    /// This is useful when an application pins several typed projections to
    /// one shared serving-log ceiling before establishing their live streams.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot is invalid, access is denied, or the
    /// peer does not confirm the exact serving/source cursor.
    pub async fn watch_command_states(
        &self,
        snapshot: &CommandStateSnapshot,
    ) -> Result<IrohCommandStateSubscription, IrohReplicationError> {
        let stream =
            CommandStateStream::from_snapshot(snapshot).map_err(IrohReplicationError::Ingest)?;
        IrohCommandStateSubscription::connect(
            &self.replicator,
            self.peer.clone(),
            self.authority.clone(),
            stream,
        )
        .await
    }
}

impl IrohCommandSubscription {
    async fn connect(
        replicator: &IrohReplicator,
        peer: EndpointAddr,
        authority: Option<AuthorityPresentation>,
        command_id: CommandId,
    ) -> Result<(CommandResponse, Self), IrohReplicationError> {
        let connection = replicator
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
            &ReplicationRequest::WatchCommand { command_id },
            authority,
        )
        .await?;
        let response = read_command_frame(&mut receive, command_id).await?;
        let current = response.command.clone().ok_or_else(|| {
            IrohReplicationError::Stream("command watch returned no initial state".to_owned())
        })?;
        let subscription = Self {
            connection,
            receive,
            source_node: response.source_node,
            command_id,
            current,
        };
        Ok((response, subscription))
    }

    /// Returns the serving node's stable Myko identity.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Returns the latest lifecycle state materialized by this stream.
    #[must_use]
    pub const fn current(&self) -> &CommandSnapshot {
        &self.current
    }

    /// Receives the command's next durable lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes, is revoked, or returns another
    /// source or command identity.
    pub async fn recv(&mut self) -> Result<CommandSnapshot, IrohReplicationError> {
        let response = read_command_frame(&mut self.receive, self.command_id).await?;
        if response.source_node != self.source_node {
            return Err(IrohReplicationError::Stream(
                "command lifecycle stream changed its serving node".to_owned(),
            ));
        }
        let command = response.command.ok_or_else(|| {
            IrohReplicationError::Stream("command lifecycle update was empty".to_owned())
        })?;
        self.current = command;
        Ok(self.current.clone())
    }

    /// Closes the lifecycle stream without shutting down either node.
    pub fn close(self) {
        self.connection.close(0u32.into(), b"command watch closed");
    }
}

impl IrohCommandStateSubscription {
    async fn connect(
        replicator: &IrohReplicator,
        peer: EndpointAddr,
        authority: Option<AuthorityPresentation>,
        stream: CommandStateStream,
    ) -> Result<Self, IrohReplicationError> {
        let connection = replicator
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
            &ReplicationRequest::WatchCommands {
                request: stream.request().clone(),
            },
            authority,
        )
        .await?;
        match read_frame(&mut receive).await? {
            ReplicationFrame::CommandWatchReady { request }
                if request.as_ref() == stream.request() => {}
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote command-catalog subscription failed: {message}"
                )));
            }
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer did not confirm the requested command-catalog stream".to_owned(),
                ));
            }
        }
        Ok(Self {
            connection,
            receive,
            stream,
        })
    }

    /// Returns the currently materialized command catalog.
    #[must_use]
    pub fn current(&self) -> CommandStateSnapshot {
        self.stream.current()
    }

    /// Receives and applies the next matching durable command transition.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes, is revoked, or sends an invalid
    /// source, contract, or cursor.
    pub async fn recv(&mut self) -> Result<CommandStateSnapshot, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::CommandUpdate { update } => self
                .stream
                .apply(&update)
                .map_err(IrohReplicationError::Ingest),
            ReplicationFrame::Authorization { decision } => Err(authorization_error(decision)),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote command-catalog subscription failed: {message}"
            ))),
            _ => Err(IrohReplicationError::Stream(
                "peer sent a non-command frame on a command-catalog stream".to_owned(),
            )),
        }
    }

    /// Closes this command-catalog stream without shutting down either node.
    pub fn close(self) {
        self.connection
            .close(0u32.into(), b"command catalog subscription closed");
    }
}

impl ItemClient for IrohItemClient {
    type Error = IrohReplicationError;

    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error> {
        Box::pin(self.replicator.item_state_page_remote_with_authority(
            self.peer.clone(),
            request,
            self.authority.clone(),
        ))
    }
}

impl IrohItemClient {
    /// Overrides reconnect timing for subsequently created reactive item streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Attaches delegated authority, approvals, or a lease to item reads.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Materializes an explicit source's native stream into a Hyphae cell.
    ///
    /// # Errors
    ///
    /// Retries transport failures indefinitely. Returns an error if typed
    /// snapshot validation or materialization fails.
    pub async fn watch_items_reactive<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<IrohReactiveItemSubscription<ItemQueryResult<Q>>, IrohReplicationError>
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

    /// Materializes the serving peer's native typed stream into a Hyphae cell.
    ///
    /// # Errors
    ///
    /// Retries transport failures indefinitely. Returns an error if typed
    /// snapshot validation or materialization fails.
    pub async fn watch_serving_items_reactive<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<IrohReactiveItemSubscription<ItemQueryResult<Q>>, IrohReplicationError>
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

    async fn watch_reactive_request<Q>(
        &self,
        request: ItemStateRequest,
        query: Q,
    ) -> Result<IrohReactiveItemSubscription<ItemQueryResult<Q>>, IrohReplicationError>
    where
        Q: ItemQuery + Send + 'static,
        ItemQueryResult<Q>: hyphae::CellValue,
    {
        let mut delay = self.reconnect_policy.initial_delay();
        let (initial, subscription) = loop {
            match self.watch_request(request.clone(), query.clone()).await {
                Ok(connected) => break connected,
                Err(error) if reactive_item_error_is_recoverable(&error) => {
                    tokio::time::sleep(delay).await;
                    delay = self.reconnect_policy.next_delay(delay);
                }
                Err(error) => return Err(error),
            }
        };
        Ok(drive_reactive_item_subscription(
            self.clone(),
            request,
            query,
            initial,
            subscription,
        ))
    }

    /// Reads current state and starts a gap-free typed stream for an explicit
    /// source already represented by the serving peer.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, typed materialization,
    /// authorization, or stream establishment fails.
    pub async fn watch_items<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            IrohItemQuerySubscription<Q>,
        ),
        IrohReplicationError,
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

    /// Reads the serving peer's authoritative current state and starts a
    /// gap-free typed stream after the resulting snapshot cursor.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot collection, typed materialization,
    /// authorization, or stream establishment fails.
    pub async fn watch_serving_items<Q>(
        &self,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            IrohItemQuerySubscription<Q>,
        ),
        IrohReplicationError,
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

    async fn watch_request<Q>(
        &self,
        request: ItemStateRequest,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            IrohItemQuerySubscription<Q>,
        ),
        IrohReplicationError,
    >
    where
        Q: ItemQuery,
    {
        let snapshot = self.item_state(request).await?;
        self.follow_item_state(&snapshot, query).await
    }

    /// Follows a typed item projection from an already collected snapshot.
    ///
    /// This allows an application to collect multiple projections against one
    /// serving-log ceiling before any of their lossless live streams start.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot seed the typed projection,
    /// access is denied, or the peer rejects the exact follow cursor.
    pub async fn follow_item_state<Q>(
        &self,
        snapshot: &ItemStateSnapshot,
        query: Q,
    ) -> Result<
        (
            ItemQuerySnapshot<ItemQueryResult<Q>>,
            IrohItemQuerySubscription<Q>,
        ),
        IrohReplicationError,
    >
    where
        Q: ItemQuery,
    {
        let (initial, stream) = ItemQueryStream::from_snapshot(snapshot, query)
            .map_err(IrohReplicationError::Ingest)?;
        let subscription = IrohItemQuerySubscription::connect(
            &self.replicator,
            self.peer.clone(),
            self.authority.clone(),
            stream,
        )
        .await?;
        Ok((initial, subscription))
    }
}

fn drive_reactive_item_subscription<Q>(
    client: IrohItemClient,
    request: ItemStateRequest,
    query: Q,
    initial: ItemQuerySnapshot<ItemQueryResult<Q>>,
    mut subscription: IrohItemQuerySubscription<Q>,
) -> IrohReactiveItemSubscription<ItemQueryResult<Q>>
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
                Err(error) if reactive_item_error_is_recoverable(&error) => {
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
                        if reactive_item_error_is_recoverable(&error) {
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
    IrohReactiveItemSubscription { live, writer, task }
}

const fn reactive_item_error_is_recoverable(error: &IrohReplicationError) -> bool {
    matches!(
        error,
        IrohReplicationError::Endpoint(_) | IrohReplicationError::Stream(_)
    )
}

impl<Q: ItemQuery> IrohItemQuerySubscription<Q> {
    async fn connect(
        replicator: &IrohReplicator,
        peer: EndpointAddr,
        authority: Option<AuthorityPresentation>,
        stream: ItemQueryStream<Q>,
    ) -> Result<Self, IrohReplicationError> {
        let connection = replicator
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
            &ReplicationRequest::FollowItems {
                request: stream.request().clone(),
            },
            authority,
        )
        .await?;
        match read_frame(&mut receive).await? {
            ReplicationFrame::ItemFollowReady { request }
                if request.as_ref() == stream.request() => {}
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote item subscription failed: {message}"
                )));
            }
            _ => {
                return Err(IrohReplicationError::Stream(
                    "peer did not confirm the requested typed item stream".to_owned(),
                ));
            }
        }
        Ok(Self {
            connection,
            receive,
            stream,
        })
    }

    /// Returns the authoritative source bound to this stream.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.stream.request().source_node
    }

    /// Computes the currently materialized query value without receiving.
    #[must_use]
    pub fn current(&self) -> ItemQueryResult<Q> {
        self.stream.current()
    }

    /// Returns the current typed projection with Myko ordering metadata.
    #[must_use]
    pub const fn current_projection(&self) -> &ItemProjection<Q::Item> {
        self.stream.current_projection()
    }

    /// Receives and atomically applies the next matching durable item update.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes, is revoked, or sends an invalid
    /// identity, cursor, schema, or mutation.
    pub async fn recv(
        &mut self,
    ) -> Result<ItemQueryUpdate<ItemQueryResult<Q>>, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::ItemUpdate { update } => self
                .stream
                .apply(&update)
                .map_err(IrohReplicationError::Ingest),
            ReplicationFrame::Authorization { decision } => Err(authorization_error(decision)),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote item subscription failed: {message}"
            ))),
            _ => Err(IrohReplicationError::Stream(
                "peer sent a non-item frame on a typed item stream".to_owned(),
            )),
        }
    }

    /// Closes this typed stream without shutting down either node.
    pub fn close(self) {
        self.connection
            .close(0u32.into(), b"typed item subscription closed");
    }
}

pub struct CursorPersistence {
    pub(super) key: ReplicationCursorKey,
    pub(super) store: Arc<dyn ReplicationCursorStore>,
}

#[derive(Clone)]
pub enum FollowSelection {
    All,
    Scope(ScopeId),
    Selected(ReplicationSelection),
}
