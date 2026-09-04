use super::*;
use crate::session_mux::{
    LocalInitialBody, LocalMultiplexedSession, MuxRouteEvent, MuxSubscription, serve_session_mux,
};
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

#[derive(Clone, Debug, Default)]
pub struct LocalServerProbe {
    counters: Arc<LocalServerProbeCounters>,
}

#[derive(Debug, Default)]
struct LocalServerProbeCounters {
    accepted: AtomicU64,
    active: AtomicU64,
    peak_active: AtomicU64,
}

impl LocalServerProbe {
    fn connection_started(&self) -> LocalServerConnectionGuard {
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);
        let active = self
            .counters
            .active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.counters
            .peak_active
            .fetch_max(active, Ordering::Relaxed);
        LocalServerConnectionGuard {
            probe: self.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn accepted(&self) -> u64 {
        self.counters.accepted.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn peak_active(&self) -> u64 {
        self.counters.peak_active.load(Ordering::Relaxed)
    }
}

struct LocalServerConnectionGuard {
    probe: LocalServerProbe,
}

impl Drop for LocalServerConnectionGuard {
    fn drop(&mut self) {
        self.probe.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
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

    #[cfg(test)]
    pub(crate) async fn spawn_application_with_probe(
        socket_path: impl AsRef<Path>,
        application: ApplicationHost,
        principal_id: PrincipalId,
        access_policy: Arc<dyn AccessPolicy>,
        probe: LocalServerProbe,
    ) -> Result<Self, LocalPeerError> {
        Self::spawn_sessions_authenticated_inner(
            socket_path,
            FederatedSession::for_application(application, access_policy),
            Principal::node(principal_id),
            Some(probe),
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
        Self::spawn_sessions_authenticated_inner(socket_path, sessions, principal, None).await
    }

    async fn spawn_sessions_authenticated_inner(
        socket_path: impl AsRef<Path>,
        sessions: FederatedSession,
        principal: Principal,
        probe: Option<LocalServerProbe>,
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
        let task = tokio::spawn(serve(listener, sessions, principal, shutdown_rx, probe));
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

/// One shared multiplexed connection to an owner-local Myko node.
#[derive(Debug, Clone)]
pub struct LocalClientSession {
    session: Arc<LocalMultiplexedSession>,
}

#[derive(Debug, Clone, Default)]
struct LocalRoute {
    destination: Option<NodeId>,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalClientSession {
    /// Creates a shared client session for one protected local Myko socket.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            session: Arc::new(LocalMultiplexedSession::new(
                socket_path.as_ref().to_path_buf(),
                ReconnectPolicy::default(),
            )),
        }
    }

    /// Overrides reconnect timing for the shared socket.
    #[must_use]
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.session = Arc::new(LocalMultiplexedSession::new(
            self.session.socket_path().to_path_buf(),
            policy,
        ));
        self
    }

    /// Creates a node client on this session.
    #[must_use]
    pub fn node_client(&self) -> LocalNodeClient {
        LocalNodeClient::from_session(Arc::clone(&self.session))
    }

    /// Creates a command client on this session.
    #[must_use]
    pub fn command_client(&self) -> LocalCommandClient {
        LocalCommandClient::from_session(Arc::clone(&self.session))
    }

    /// Creates a typed item client on this session.
    #[must_use]
    pub fn item_client(&self) -> LocalItemClient {
        LocalItemClient::from_session(Arc::clone(&self.session))
    }

    /// Creates a durable handler connector on this session.
    #[must_use]
    pub fn handler_connector(&self) -> LocalHandlerConnector {
        LocalHandlerConnector::from_session(Arc::clone(&self.session))
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
    session: Arc<LocalMultiplexedSession>,
    route: LocalRoute,
}

impl LocalNodeClient {
    /// Creates a client for one protected local Myko socket.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        LocalClientSession::new(socket_path).node_client()
    }

    fn from_session(session: Arc<LocalMultiplexedSession>) -> Self {
        Self {
            session,
            route: LocalRoute::default(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.route.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent local requests and streams.
    #[must_use]
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.session = Arc::new(LocalMultiplexedSession::new(
            self.session.socket_path().to_path_buf(),
            policy,
        ));
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.route.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.route.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> NodeRequestEnvelope {
        authorized_request_envelope(
            self.route.destination,
            self.route.authority.clone(),
            &self.route.forwarding_provenance,
            request,
        )
        .body
    }

    /// Returns the stable identity of the serving Myko node.
    ///
    /// # Errors
    ///
    /// Waits through transient socket unavailability. Returns an error if an
    /// established connection violates the canonical identify exchange.
    pub async fn identify(&self) -> Result<NodeId, LocalPeerError> {
        let mut subscription =
            open_local(&self.session, self.envelope(PeerRequest::Identify)).await?;
        match read_authorized_mux_frame(&mut subscription).await? {
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
        let mut subscription = open_local(
            &self.session,
            self.envelope(PeerRequest::FollowLive { topics }),
        )
        .await?;
        let source_node = match read_authorized_mux_frame(&mut subscription).await? {
            PeerFrame::Hello { source_node } => source_node,
            PeerFrame::Error { message } => return Err(LocalPeerError::Protocol(message)),
            _ => {
                return Err(LocalPeerError::Protocol(
                    "local peer sent a live event before its source identity".to_owned(),
                ));
            }
        };
        Ok(LocalLiveEventSubscription {
            subscription,
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
    session: Arc<LocalMultiplexedSession>,
    route: LocalRoute,
}

impl LocalHandlerConnector {
    /// Creates a connector for one protected local Myko socket.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        LocalClientSession::new(socket_path).handler_connector()
    }

    fn from_session(session: Arc<LocalMultiplexedSession>) -> Self {
        Self {
            session,
            route: LocalRoute::default(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.route.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for durable handler streams.
    #[must_use]
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.session = Arc::new(LocalMultiplexedSession::new(
            self.session.socket_path().to_path_buf(),
            policy,
        ));
        self
    }

    /// Attaches the original principal and validated delegation presentation.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.route.authority = Some(authority);
        self
    }

    /// Reserves the next store-backed node-forward delegation in the route.
    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.route.forwarding_provenance.push(hop);
        self
    }

    /// Builds the retained application client over this transport adapter.
    #[must_use]
    pub fn client(self) -> MykoClient {
        MykoClient::with_handler_connector(Arc::new(self))
    }

    fn envelope(&self, request: PeerRequest) -> NodeRequestEnvelope {
        authorized_request_envelope(
            self.route.destination,
            self.route.authority.clone(),
            &self.route.forwarding_provenance,
            request,
        )
        .body
    }
}

struct LocalHandlerConnection {
    subscription: MuxSubscription,
}

#[async_trait::async_trait]
impl HandlerConnection for LocalHandlerConnection {
    async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
        match self.subscription.recv_authorized_event().await? {
            MuxRouteEvent::Frame(frame) => local_handler_frame(frame),
            MuxRouteEvent::Reconnecting { reason } => Ok(HandlerFrame::Resynchronizing {
                reason: reason.to_string(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl HandlerConnector for LocalHandlerConnector {
    async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
        if let Some(destination) = self.route.destination {
            return Ok(destination);
        }
        let mut subscription = self
            .session
            .mux()
            .await
            .open(self.envelope(PeerRequest::Identify))
            .await?;
        match subscription.recv_authorized().await? {
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
        let mut subscription = self
            .session
            .mux()
            .await
            .open(self.envelope(PeerRequest::FollowHandler { request }))
            .await?;
        let initial = subscription
            .recv_authorized()
            .await
            .and_then(local_handler_frame)?;
        Ok((initial, Box::new(LocalHandlerConnection { subscription })))
    }

    fn at(&self, destination: NodeId) -> Arc<dyn HandlerConnector> {
        let mut route = self.route.clone();
        route.destination = Some(destination);
        Arc::new(Self {
            session: Arc::clone(&self.session),
            route,
        })
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        self.session.reconnect_policy()
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
            "local session stream returned {}",
            frame.kind()
        ))),
    }
}

/// One best-effort canonical live-event stream over the local Unix transport.
#[derive(Debug)]
pub struct LocalLiveEventSubscription {
    subscription: MuxSubscription,
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
        match read_authorized_mux_frame(&mut self.subscription).await? {
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
    session: Arc<LocalMultiplexedSession>,
    route: LocalRoute,
}

impl LocalItemClient {
    /// Creates a local typed-state client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        LocalClientSession::new(socket_path).item_client()
    }

    fn from_session(session: Arc<LocalMultiplexedSession>) -> Self {
        Self {
            session,
            route: LocalRoute::default(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.route.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent requests and streams.
    #[must_use]
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.session = Arc::new(LocalMultiplexedSession::new(
            self.session.socket_path().to_path_buf(),
            policy,
        ));
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.route.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.route.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> NodeRequestEnvelope {
        authorized_request_envelope(
            self.route.destination,
            self.route.authority.clone(),
            &self.route.forwarding_provenance,
            request,
        )
        .body
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
        let (initial, subscription) = self.watch_request(request, query).await?;
        Ok(drive_reactive(initial, subscription))
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
            &self.session,
            self.envelope(PeerRequest::FollowItems {
                request: stream.request().clone(),
            }),
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
            let mut subscription = open_local(
                &self.session,
                self.envelope(PeerRequest::ItemState { request }),
            )
            .await?;
            match read_authorized_mux_frame(&mut subscription).await? {
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
    session: Arc<LocalMultiplexedSession>,
    route: LocalRoute,
}

impl LocalCommandClient {
    /// Creates a local command client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        LocalClientSession::new(socket_path).command_client()
    }

    fn from_session(session: Arc<LocalMultiplexedSession>) -> Self {
        Self {
            session,
            route: LocalRoute::default(),
        }
    }

    /// Addresses subsequent requests to one node through the connected node.
    #[must_use]
    pub const fn at(mut self, destination: NodeId) -> Self {
        self.route.destination = Some(destination);
        self
    }

    /// Overrides reconnect timing for subsequent local requests and streams.
    #[must_use]
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.session = Arc::new(LocalMultiplexedSession::new(
            self.session.socket_path().to_path_buf(),
            policy,
        ));
        self
    }

    /// Attaches the original principal and validated delegation presentation.
    ///
    /// The accepting session still binds the final executor to the local
    /// transport identity and the destination validates every stored fact.
    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityPresentation) -> Self {
        self.route.authority = Some(authority);
        self
    }

    /// Reserves the next store-backed node-forward delegation in the route.
    #[must_use]
    pub fn with_forwarding_hop(mut self, hop: ProvenanceHop) -> Self {
        self.route.forwarding_provenance.push(hop);
        self
    }

    fn envelope(&self, request: PeerRequest) -> NodeRequestEnvelope {
        authorized_request_envelope(
            self.route.destination,
            self.route.authority.clone(),
            &self.route.forwarding_provenance,
            request,
        )
        .body
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
        let mut subscription = open_local(&self.session, self.envelope(request)).await?;
        match read_authorized_mux_frame(&mut subscription).await? {
            PeerFrame::Command { response } if response.command.is_some() => Ok((
                (*response).clone(),
                LocalCommandSubscription {
                    subscription,
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
        let mut subscription = open_local(&self.session, self.envelope(request)).await?;
        match read_authorized_mux_frame(&mut subscription).await? {
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
        let mut subscription = open_local(
            &self.session,
            self.envelope(PeerRequest::ApproveAuthority {
                challenge_id,
                approved,
            }),
        )
        .await?;
        match read_authorized_mux_frame(&mut subscription).await? {
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
    subscription: MuxSubscription,
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
        match read_authorized_mux_frame(&mut self.subscription).await? {
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
    subscription: MuxSubscription,
    query: ItemQueryStream<Q>,
}

impl<Q: ItemQuery> LocalItemQuerySubscription<Q> {
    async fn connect(
        session: &LocalMultiplexedSession,
        request: NodeRequestEnvelope,
        query: ItemQueryStream<Q>,
    ) -> Result<Self, LocalPeerError> {
        let mut subscription = open_local(session, request).await?;
        match read_authorized_mux_frame(&mut subscription).await? {
            PeerFrame::ItemFollowReady { request } if request.as_ref() == query.request() => {
                Ok(Self {
                    subscription,
                    query,
                })
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
        loop {
            if let LocalItemQueryEvent::Update(update) = self.recv_event().await? {
                return Ok(update);
            }
        }
    }

    async fn recv_event(
        &mut self,
    ) -> Result<LocalItemQueryEvent<ItemQueryResult<Q>>, LocalPeerError> {
        match self
            .subscription
            .recv_authorized_event()
            .await
            .map_err(local_mux_error)?
        {
            MuxRouteEvent::Frame(PeerFrame::ItemUpdate { update }) => {
                Ok(LocalItemQueryEvent::Update(self.query.apply(&update)?))
            }
            MuxRouteEvent::Frame(PeerFrame::Error { message }) => {
                Err(LocalPeerError::Protocol(message))
            }
            MuxRouteEvent::Frame(_) => Err(LocalPeerError::Protocol(
                "local peer sent a non-item frame on a typed item stream".to_owned(),
            )),
            MuxRouteEvent::Reconnecting { reason } => {
                Ok(LocalItemQueryEvent::Resynchronizing { reason })
            }
        }
    }
}

enum LocalItemQueryEvent<T> {
    Update(ItemQueryUpdate<T>),
    Resynchronizing { reason: Arc<str> },
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
            match subscription.recv_event().await {
                Ok(LocalItemQueryEvent::Update(update)) => {
                    task_writer.publish(update.value, Some(update.position));
                }
                Ok(LocalItemQueryEvent::Resynchronizing { reason }) => {
                    task_writer.resynchronizing(reason.to_string());
                }
                Err(error) => {
                    task_writer.invalidate(error.to_string());
                    return;
                }
            }
        }
    });
    LocalReactiveItemSubscription { live, writer, task }
}

pub async fn connect_local_peer(socket_path: &Path, policy: ReconnectPolicy) -> UnixStream {
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
    probe: Option<LocalServerProbe>,
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
                let connection_guard = probe
                    .as_ref()
                    .map(LocalServerProbe::connection_started);
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
                        connection_guard,
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
    _connection_guard: Option<LocalServerConnectionGuard>,
) {
    let initial = async {
        let request: Envelope<LocalInitialBody> = read_frame(&mut stream).await?;
        request
            .into_current()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))
    }
    .await;
    let initial = match initial {
        Ok(initial) => initial,
        Err(error) => {
            tracing::warn!(error = %error, "local Myko connection failed");
            let _ignored = write_frame(
                &mut stream,
                &Envelope::new(PeerFrame::Error {
                    message: error.to_string(),
                }),
            )
            .await;
            return;
        }
    };
    let request = match initial {
        LocalInitialBody::Mux(hello) => {
            match serve_session_mux(stream, sessions, principal, shutdown, hello).await {
                Ok(()) => tracing::debug!("local Myko handler mux closed"),
                Err(error) => tracing::warn!(error = %error, "local Myko handler mux failed"),
            }
            return;
        }
        LocalInitialBody::Single(request) => *request,
    };
    tracing::debug!(
        request = request.request.kind(),
        destination = ?request.destination,
        "received local Myko request"
    );
    let result = serve_session_request(&mut stream, &sessions, principal, shutdown, request).await;
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
    let (mut reader, mut writer) = stream.split();
    let mut peer_data = [0_u8; 1];
    loop {
        tokio::select! {
            read = reader.read(&mut peer_data) => {
                match read? {
                    0 => {
                        tracing::debug!("local Myko client closed connection");
                        return Ok(());
                    }
                    _ => {
                        return Err(LocalPeerError::Protocol(
                            "client sent data after its session request".to_owned(),
                        ));
                    }
                }
            }
            frame = frames.recv() => {
                let Some(frame) = frame else {
                    tracing::debug!("session frame stream closed");
                    return Ok(());
                };
                tracing::trace!(frame = frame.kind(), "writing local Myko frame");
                write_frame(&mut writer, &Envelope::new(frame)).await?;
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

async fn open_local(
    session: &LocalMultiplexedSession,
    request: NodeRequestEnvelope,
) -> Result<MuxSubscription, LocalPeerError> {
    session
        .mux()
        .await
        .open(request)
        .await
        .map_err(local_mux_error)
}

async fn read_authorized_mux_frame(
    subscription: &mut MuxSubscription,
) -> Result<PeerFrame, LocalPeerError> {
    loop {
        match subscription.recv_frame().await.map_err(local_mux_error)? {
            PeerFrame::Authorization { decision }
                if matches!(decision.as_ref(), AuthorizationDecision::Permit(_)) =>
            {
                tracing::debug!("local multiplexed Myko request authorized");
            }
            PeerFrame::Authorization { decision } => {
                tracing::warn!(decision = ?decision, "local multiplexed Myko request authorization failed");
                return Err(LocalPeerError::Authorization(decision));
            }
            frame => {
                tracing::trace!(frame = frame.kind(), "read local multiplexed Myko frame");
                return Ok(frame);
            }
        }
    }
}

fn local_mux_error(error: HandlerClientError) -> LocalPeerError {
    match error {
        HandlerClientError::Decode(error) => LocalPeerError::Json(error),
        HandlerClientError::MissingConnector => {
            LocalPeerError::Protocol("local Myko session has no connector".to_owned())
        }
        HandlerClientError::Protocol(message) | HandlerClientError::Transport(message) => {
            LocalPeerError::Protocol(message)
        }
    }
}

pub async fn write_frame<T: Serialize + Sync, W: AsyncWrite + Unpin>(
    stream: &mut W,
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

pub async fn read_frame<T: DeserializeOwned, R: AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<T, LocalPeerError> {
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
