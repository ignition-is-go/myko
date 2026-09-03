//! Owner-local Myko peer transport.
//!
//! A protected Unix socket carries the same typed snapshot/follow contracts as
//! native Iroh peers. The transport does not define application requests or
//! projections: a local TUI, desktop application, or service manager remains a
//! lightweight Myko node-facing participant rather than a special server API.

#![forbid(unsafe_code)]

use std::{
    fs,
    os::unix::fs::{FileTypeExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    sync::Arc,
};

use myko_app::{
    ApplicationNode, ErasedHandlerState, ErasedViewDelta, HandlerKind, HandlerRequest,
    MykoApplication, QueryHandler, ReportHandler, ViewHandler,
};
use myko_federation::{
    AccessPolicy, ApprovalDecision, AuthorityPresentation, AuthorizationDecision, ChallengeId,
    CommandClient, CommandClientFuture, CommandId, CommandResponse, CommandSnapshot,
    CommandSubmission, CommandSubscription, CommandSubscriptionFuture, CommandWatchFuture,
    CommandWatchingClient, ItemClient, ItemQuery, ItemQuerySnapshot, ItemQueryStream,
    ItemQueryUpdate, ItemStatePageFuture, ItemStateRequest, LiveCollection, LiveCollectionState,
    LiveCollectionWriter, LiveEvent, LiveSubscription, LiveSubscriptionState, Node, NodeError,
    NodeId, Principal, PrincipalId, ProvenanceHop, ReconnectPolicy, ScopeId, SubscriptionLiveness,
    live_collection, live_subscription,
};
use myko_session::NodeSessionService;
use myko_wire::{
    NodeFrame as PeerFrame, NodeRequest as PeerRequest, NodeRequestEnvelope,
    WireEnvelope as Envelope,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
};

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
type LocalViewDelta<T> = fn(&mut T, &ErasedViewDelta) -> Result<(), LocalPeerError>;

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
            ApplicationNode::new(node, MykoApplication::new()),
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
        application: ApplicationNode,
        principal_id: PrincipalId,
        access_policy: Arc<dyn AccessPolicy>,
    ) -> Result<Self, LocalPeerError> {
        Self::spawn_sessions(
            socket_path,
            NodeSessionService::for_application(application, access_policy),
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
        sessions: NodeSessionService,
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
        sessions: NodeSessionService,
        principal: Principal,
    ) -> Result<Self, LocalPeerError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, LocalItemQuerySubscription<Q>), LocalPeerError>
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, LocalItemQuerySubscription<Q>), LocalPeerError>
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
    ) -> Result<LocalReactiveItemSubscription<Q::Output>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
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
    ) -> Result<LocalReactiveItemSubscription<Q::Output>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
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
    ) -> Result<LocalReactiveItemSubscription<Q::Output>, LocalPeerError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, LocalItemQuerySubscription<Q>), LocalPeerError>
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

/// Typed client for application-registered query, report, and view handlers.
///
/// Connection attempts continue until the socket becomes available or the
/// pending operation is cancelled.
#[derive(Debug, Clone)]
pub struct LocalApplicationClient {
    socket_path: PathBuf,
    destination: Option<NodeId>,
    reconnect_policy: ReconnectPolicy,
    authority: Option<AuthorityPresentation>,
    forwarding_provenance: Vec<ProvenanceHop>,
}

impl LocalApplicationClient {
    /// Creates an application handler client for one owner-local Myko node.
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

    /// Starts a registered typed query handler stream.
    ///
    /// # Errors
    ///
    /// Returns an error if parameters cannot be encoded, the handler is not
    /// registered, or its lifecycle stream cannot be decoded.
    pub async fn watch_query<Q>(
        &self,
        source_node: myko_federation::NodeId,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<LocalHandlerSubscription<Q::Output, myko_federation::LogPosition>, LocalPeerError>
    where
        Q: QueryHandler,
    {
        self.watch(HandlerRequest {
            kind: HandlerKind::Query,
            handler_id: Q::QUERY_ID.to_owned(),
            source_node: Some(source_node),
            scope_id: Some(scope_id),
            params: serde_json::to_value(query)?,
        })
        .await
    }

    /// Starts a registered query and drives its lifecycle into a Hyphae cell.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed handler stream cannot be established.
    pub async fn watch_query_reactive<Q>(
        &self,
        source_node: myko_federation::NodeId,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<
        LocalReactiveHandlerSubscription<Q::Output, myko_federation::LogPosition>,
        LocalPeerError,
    >
    where
        Q: QueryHandler,
    {
        let request = HandlerRequest {
            kind: HandlerKind::Query,
            handler_id: Q::QUERY_ID.to_owned(),
            source_node: Some(source_node),
            scope_id: Some(scope_id),
            params: serde_json::to_value(query)?,
        };
        let subscription = self.watch(request.clone()).await?;
        Ok(drive_handler_reactive(self.clone(), request, subscription))
    }

    /// Starts a registered reactive report stream.
    ///
    /// # Errors
    ///
    /// Returns an error if parameters cannot be encoded, the handler is not
    /// registered, or its lifecycle stream cannot be decoded.
    pub async fn watch_report<R>(
        &self,
        report: &R,
    ) -> Result<LocalHandlerSubscription<R::Output, R::Cursor>, LocalPeerError>
    where
        R: ReportHandler,
    {
        self.watch(HandlerRequest {
            kind: HandlerKind::Report,
            handler_id: R::REPORT_ID.to_owned(),
            source_node: None,
            scope_id: report.access_scope(),
            params: serde_json::to_value(report)?,
        })
        .await
    }

    /// Starts a registered report and drives its lifecycle into a Hyphae cell.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed handler stream cannot be established.
    pub async fn watch_report_reactive<R>(
        &self,
        report: &R,
    ) -> Result<LocalReactiveHandlerSubscription<R::Output, R::Cursor>, LocalPeerError>
    where
        R: ReportHandler,
    {
        let request = HandlerRequest {
            kind: HandlerKind::Report,
            handler_id: R::REPORT_ID.to_owned(),
            source_node: None,
            scope_id: report.access_scope(),
            params: serde_json::to_value(report)?,
        };
        let subscription = self.watch(request.clone()).await?;
        Ok(drive_handler_reactive(self.clone(), request, subscription))
    }

    /// Starts a registered reactive view stream.
    ///
    /// # Errors
    ///
    /// Returns an error if parameters cannot be encoded, the handler is not
    /// registered, or its lifecycle stream cannot be decoded.
    pub async fn watch_view<V>(
        &self,
        view: &V,
    ) -> Result<LocalHandlerSubscription<Vec<V::Item>, V::Cursor>, LocalPeerError>
    where
        V: ViewHandler,
    {
        self.watch_with_delta(
            HandlerRequest {
                kind: HandlerKind::View,
                handler_id: V::VIEW_ID.to_owned(),
                source_node: None,
                scope_id: view.access_scope(),
                params: serde_json::to_value(view)?,
            },
            Some(apply_view_delta::<V>),
        )
        .await
    }

    /// Starts a registered view and drives its lifecycle into a Hyphae cell.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed handler stream cannot be established.
    pub async fn watch_view_reactive<V>(
        &self,
        view: &V,
    ) -> Result<LocalReactiveViewSubscription<V::Item, V::Cursor>, LocalPeerError>
    where
        V: ViewHandler,
    {
        let request = HandlerRequest {
            kind: HandlerKind::View,
            handler_id: V::VIEW_ID.to_owned(),
            source_node: None,
            scope_id: view.access_scope(),
            params: serde_json::to_value(view)?,
        };
        let subscription = self
            .watch_with_delta(request.clone(), Some(apply_view_delta::<V>))
            .await?;
        Ok(drive_view_reactive::<V>(
            self.clone(),
            request,
            subscription,
        ))
    }

    async fn watch<T, C>(
        &self,
        request: HandlerRequest,
    ) -> Result<LocalHandlerSubscription<T, C>, LocalPeerError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        self.watch_with_delta(request, None).await
    }

    async fn watch_with_delta<T, C>(
        &self,
        request: HandlerRequest,
        view_delta: Option<LocalViewDelta<T>>,
    ) -> Result<LocalHandlerSubscription<T, C>, LocalPeerError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        let mut stream = connect_local_peer(&self.socket_path, self.reconnect_policy).await;
        write_frame(
            &mut stream,
            &self.envelope(PeerRequest::FollowHandler { request }),
        )
        .await?;
        match read_authorized_peer_frame(&mut stream).await? {
            PeerFrame::HandlerState { state } => Ok(LocalHandlerSubscription {
                stream,
                current: decode_handler_state(*state)?,
                view_delta,
            }),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer did not return initial handler state".to_owned(),
            )),
        }
    }
}

/// Current-then-live typed application handler over an owner-local socket.
pub struct LocalHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    stream: UnixStream,
    current: LiveSubscriptionState<T, C>,
    view_delta: Option<LocalViewDelta<T>>,
}

impl<T, C> LocalHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    /// Returns the newest coherent value, cursor, and liveness revision.
    #[must_use]
    pub const fn current(&self) -> &LiveSubscriptionState<T, C> {
        &self.current
    }

    /// Waits for the next handler lifecycle revision.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes, changes frame type, or contains
    /// a value that violates the registered typed contract.
    pub async fn recv(&mut self) -> Result<LiveSubscriptionState<T, C>, LocalPeerError> {
        match read_authorized_peer_frame(&mut self.stream).await? {
            PeerFrame::HandlerState { state } => {
                self.current = decode_handler_state(*state)?;
                Ok(self.current.clone())
            }
            PeerFrame::HandlerViewDelta { delta } => {
                let apply = self.view_delta.ok_or_else(|| {
                    LocalPeerError::Protocol(
                        "peer sent a keyed view delta to a snapshot handler".to_owned(),
                    )
                })?;
                let through = delta
                    .through
                    .clone()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|error| {
                        LocalPeerError::Protocol(format!(
                            "keyed view cursor decoding failed: {error}"
                        ))
                    })?;
                if let Some(value) = self.current.value.as_mut() {
                    apply(value, &delta)?;
                } else if delta.order.as_ref().is_some_and(|order| !order.is_empty()) {
                    return Err(LocalPeerError::Protocol(
                        "keyed view delta arrived before its initial snapshot".to_owned(),
                    ));
                }
                self.current.through = through;
                self.current.liveness = delta.liveness.clone();
                Ok(self.current.clone())
            }
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer changed application handler stream type".to_owned(),
            )),
        }
    }
}

fn apply_view_delta<V>(
    items: &mut Vec<V::Item>,
    delta: &ErasedViewDelta,
) -> Result<(), LocalPeerError>
where
    V: ViewHandler,
{
    let previous = std::mem::take(items);
    let mut by_key = previous
        .iter()
        .cloned()
        .map(|item| (V::item_key(&item), item))
        .collect::<std::collections::BTreeMap<_, _>>();
    for key in &delta.deletes {
        by_key.remove(key.as_str());
    }
    for encoded in &delta.upserts {
        let item = serde_json::from_value::<V::Item>(encoded.clone()).map_err(|error| {
            LocalPeerError::Protocol(format!("keyed view item decoding failed: {error}"))
        })?;
        by_key.insert(V::item_key(&item), item);
    }
    if let Some(order) = &delta.order {
        let mut ordered = Vec::with_capacity(order.len());
        for key in order {
            let item = by_key.remove(key.as_str()).ok_or_else(|| {
                LocalPeerError::Protocol(format!("keyed view delta omitted row {key:?}"))
            })?;
            ordered.push(item);
        }
        if !by_key.is_empty() {
            return Err(LocalPeerError::Protocol(
                "keyed view delta left rows outside its authoritative order".to_owned(),
            ));
        }
        *items = ordered;
    } else {
        let mut retained = Vec::with_capacity(previous.len());
        for item in previous {
            let key = V::item_key(&item);
            if let Some(current) = by_key.remove(&key) {
                retained.push(current);
            }
        }
        if !by_key.is_empty() {
            return Err(LocalPeerError::Protocol(
                "keyed view delta inserted rows without an ordering revision".to_owned(),
            ));
        }
        *items = retained;
    }
    Ok(())
}

fn decode_handler_state<T, C>(
    state: ErasedHandlerState,
) -> Result<LiveSubscriptionState<T, C>, LocalPeerError>
where
    T: DeserializeOwned,
    C: DeserializeOwned,
{
    Ok(LiveSubscriptionState {
        value: state
            .value
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                LocalPeerError::Protocol(format!("handler value decoding failed: {error}"))
            })?,
        through: state
            .through
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                LocalPeerError::Protocol(format!("handler cursor decoding failed: {error}"))
            })?,
        liveness: state.liveness,
    })
}

/// Runtime owner for a local application handler's Hyphae lifecycle cell.
pub struct LocalReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    writer: myko_federation::LiveSubscriptionWriter<T, C>,
    task: JoinHandle<()>,
}

/// Runtime owner for a local application's identity-preserving view.
pub struct LocalReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveCollection<T, C>,
    writer: LiveCollectionWriter<T, C>,
    task: JoinHandle<()>,
}

impl<T, C> LocalReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the authoritative keyed rows and coherent lifecycle cells.
    #[must_use]
    pub const fn live(&self) -> &LiveCollection<T, C> {
        &self.live
    }
}

impl<T, C> Drop for LocalReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

impl<T, C> LocalReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the reactive value/cursor/liveness cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T, C> {
        &self.live
    }
}

impl<T, C> Drop for LocalReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

fn drive_handler_reactive<T, C>(
    client: LocalApplicationClient,
    request: HandlerRequest,
    mut subscription: LocalHandlerSubscription<T, C>,
) -> LocalReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    let (writer, live) = live_subscription(subscription.current().clone());
    let view_delta = subscription.view_delta;
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(state) => {
                    task_writer.replace(state);
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
                match client.watch_with_delta(request.clone(), view_delta).await {
                    Ok(next) => {
                        task_writer.replace(next.current().clone());
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
    LocalReactiveHandlerSubscription { live, writer, task }
}

fn drive_view_reactive<V>(
    client: LocalApplicationClient,
    request: HandlerRequest,
    mut subscription: LocalHandlerSubscription<Vec<V::Item>, V::Cursor>,
) -> LocalReactiveViewSubscription<V::Item, V::Cursor>
where
    V: ViewHandler,
{
    let initial = subscription.current().clone();
    let rows = initial
        .value
        .as_deref()
        .unwrap_or_default()
        .iter()
        .cloned()
        .map(|item| (V::item_key(&item), Arc::new(item)))
        .collect();
    let (writer, live) = live_collection(
        rows,
        LiveCollectionState {
            through: initial.through,
            liveness: initial.liveness,
        },
    );
    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(state) => {
                    publish_local_view_state::<V>(&task_writer, state);
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
                match client
                    .watch_with_delta(request.clone(), Some(apply_view_delta::<V>))
                    .await
                {
                    Ok(next) => {
                        publish_local_view_state::<V>(&task_writer, next.current().clone());
                        subscription = next;
                        break;
                    }
                    Err(error) if local_subscription_error_is_recoverable(&error) => {
                        task_writer.resynchronizing(error.to_string());
                        delay = client.reconnect_policy.next_delay(delay);
                    }
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        return;
                    }
                }
            }
        }
    });
    LocalReactiveViewSubscription { live, writer, task }
}

fn publish_local_view_state<V>(
    writer: &LiveCollectionWriter<V::Item, V::Cursor>,
    state: LiveSubscriptionState<Vec<V::Item>, V::Cursor>,
) where
    V: ViewHandler,
{
    match state.liveness {
        SubscriptionLiveness::Current => {
            let rows = state
                .value
                .unwrap_or_default()
                .into_iter()
                .map(|item| (V::item_key(&item), Arc::new(item)))
                .collect();
            if let Err(error) = writer.reconcile(rows, state.through) {
                writer.invalidate(error.to_string());
            }
        }
        SubscriptionLiveness::Resynchronizing { reason } => writer.resynchronizing(reason),
        SubscriptionLiveness::Invalid { reason } => writer.invalidate(reason),
        SubscriptionLiveness::Connecting => {}
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
    pub fn current(&self) -> Q::Output {
        self.query.current()
    }

    /// Receives and atomically applies the next matching item update.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or violates its typed contract.
    pub async fn recv(&mut self) -> Result<ItemQueryUpdate<Q::Output>, LocalPeerError> {
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
    initial: ItemQuerySnapshot<Q::Output>,
    mut subscription: LocalItemQuerySubscription<Q>,
) -> LocalReactiveItemSubscription<Q::Output>
where
    Q: ItemQuery + Send + 'static,
    Q::Output: hyphae::CellValue,
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
    loop {
        if let Ok(stream) = UnixStream::connect(socket_path).await {
            return stream;
        }
        tokio::time::sleep(delay).await;
        delay = policy.next_delay(delay);
    }
}

async fn serve(
    listener: UnixListener,
    sessions: NodeSessionService,
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
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                connections.spawn(handle_connection(
                    stream,
                    sessions.clone(),
                    principal.clone(),
                    shutdown.clone(),
                    permit,
                ));
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
    sessions: NodeSessionService,
    principal: Principal,
    shutdown: watch::Receiver<bool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let result = async {
        let request: Envelope<NodeRequestEnvelope> = read_frame(&mut stream).await?;
        let request = request
            .into_current()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        serve_session_request(&mut stream, &sessions, principal, shutdown, request).await
    }
    .await;
    if let Err(error) = result {
        let _ignored = write_frame(
            &mut stream,
            &Envelope::new(PeerFrame::Error {
                message: error.to_string(),
            }),
        )
        .await;
    }
}

async fn serve_session_request(
    stream: &mut UnixStream,
    sessions: &NodeSessionService,
    principal: Principal,
    mut shutdown: watch::Receiver<bool>,
    request: NodeRequestEnvelope,
) -> Result<(), LocalPeerError> {
    let mut frames = sessions.open_authenticated(principal, request).await;
    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(frame) = frame else { return Ok(()); };
                write_frame(stream, &Envelope::new(frame)).await?;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
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
                if matches!(decision.as_ref(), AuthorizationDecision::Permit(_)) => {}
            PeerFrame::Authorization { decision } => {
                return Err(LocalPeerError::Authorization(decision));
            }
            frame => return Ok(frame),
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

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::panic_in_result_fn,
    clippy::unwrap_used
)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use hyphae::{Signal, Watchable as _};
    use myko_app::capability::{CollectionBuilding as _, EventPublishing as _, Querying as _};
    use myko_app::{
        CommandClient as _, CommandContext, CommandError, CommandHandler, QueryHandler, myko_query,
        myko_report, myko_view,
    };
    use myko_federation::{
        AccessOperation, AccessRequest, AllowAllAccessPolicy, ApprovalId, AuthorityPresentation,
        AuthorityRealmId, AuthorizationBinding, AuthorizationDecision, BatchId, ChangeBatch,
        CommandRequest, DelegationId, ObligationId, Principal, PrincipalId, PrincipalKind,
        ProvenanceOperation, ResourceClaim, ResourceClaimKind, ServiceId, SubscriptionLiveness,
    };
    use myko_items::{
        ItemMutation, ItemProjection, ItemQuery, myko_command, myko_item, myko_service,
    };

    #[derive(Debug)]
    struct ApprovalPolicy;

    impl AccessPolicy for ApprovalPolicy {
        fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
            Ok(())
        }

        fn approve(
            &self,
            authenticated_executor: &PrincipalId,
            presentation: &AuthorityPresentation,
            challenge_id: &ChallengeId,
            approved: bool,
        ) -> Result<ApprovalDecision, AuthorizationDecision> {
            let now = Utc::now();
            let binding_request = AccessRequest::scoped(
                authenticated_executor.clone(),
                presentation.clone(),
                AccessOperation::ApproveAuthority,
                ScopeId::new("authority:test"),
            );
            Ok(ApprovalDecision {
                id: ApprovalId::new("local-approval"),
                realm_id: AuthorityRealmId::new("test"),
                challenge_id: challenge_id.clone(),
                obligation_id: ObligationId::new("test-review"),
                approver: presentation.principal.clone(),
                binding: AuthorizationBinding::from_request(&binding_request),
                approved,
                decided_at: now,
                expires_at: now + ChronoDuration::minutes(1),
                max_uses: 1,
            })
        }
    }

    #[derive(Debug)]
    struct PresentationPolicy {
        expected: AuthorityPresentation,
        operations: Arc<Mutex<Vec<AccessOperation>>>,
    }

    impl AccessPolicy for PresentationPolicy {
        fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
            if request.presentation != self.expected {
                return Err("authority presentation was not preserved".to_owned());
            }
            self.operations
                .lock()
                .map_err(|_| "presentation-policy lock is poisoned".to_owned())?
                .push(request.operation);
            Ok(())
        }
    }

    #[myko_service(LocalRecord)]
    pub struct LocalService;

    #[myko_item(service = LocalService, scope_root)]
    pub struct LocalRecord {
        value: String,
    }

    #[myko_command(bool, item = LocalRecord)]
    struct SetLocalRecord {
        id: LocalRecordId,
        value: String,
    }

    impl CommandHandler for SetLocalRecord {
        fn scope(&self, _node_id: NodeId) -> LocalRecordId {
            LocalRecordId::from("local-scope")
        }

        fn execute(
            self,
            context: CommandContext<LocalService, LocalRecord>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&LocalRecord {
                id: self.id,
                value: self.value,
            })?;
            Ok(true)
        }
    }

    #[myko_query(LocalRecord)]
    struct AllLocalRecords;

    impl ItemQuery for AllLocalRecords {
        type Item = LocalRecord;
        type Output = Vec<LocalRecord>;
        fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
            projection.values().cloned().collect()
        }
    }

    impl QueryHandler for AllLocalRecords {}

    #[myko_report(u64, item = LocalRecord)]
    #[derive(Copy)]
    struct LocalRecordCount {
        source_node: myko_federation::NodeId,
    }

    impl ReportHandler for LocalRecordCount {
        type Output = u64;
        type Cursor = myko_federation::LogPosition;

        fn access_scope(&self) -> Option<ScopeId> {
            Some(ScopeId::new("local-scope"))
        }

        fn authority_claims(&self) -> Vec<myko_federation::ResourceClaim> {
            AllLocalRecords.authority_claims(self.source_node, &ScopeId::new("local-scope"))
        }

        fn build(
            &self,
            context: &myko_app::ReportContext,
        ) -> Result<LiveSubscription<Self::Output>, myko_app::AppError> {
            Ok(context
                .query(
                    self.source_node,
                    ScopeId::new("local-scope"),
                    AllLocalRecords,
                )?
                .map_value(|records| u64::try_from(records.len()).unwrap_or(u64::MAX)))
        }
    }

    #[myko_view(LocalRecord, item = LocalRecord)]
    #[derive(Copy)]
    struct LocalRecordsView {
        source_node: myko_federation::NodeId,
    }

    impl ViewHandler for LocalRecordsView {
        type Item = LocalRecord;
        type Cursor = myko_federation::LogPosition;

        fn access_scope(&self) -> Option<ScopeId> {
            Some(ScopeId::new("local-scope"))
        }

        fn authority_claims(&self) -> Vec<myko_federation::ResourceClaim> {
            AllLocalRecords.authority_claims(self.source_node, &ScopeId::new("local-scope"))
        }

        fn item_key(item: &Self::Item) -> Arc<str> {
            Arc::from(item.id.to_string())
        }

        fn build(
            &self,
            context: &myko_app::ViewContext,
        ) -> Result<LiveCollection<Self::Item>, myko_app::AppError> {
            let live = context.query(
                self.source_node,
                ScopeId::new("local-scope"),
                AllLocalRecords,
            )?;
            context.collection_from_subscription(&live, Self::item_key)
        }
    }

    fn local_record_application(node: Node) -> Result<ApplicationNode, LocalPeerError> {
        let application = MykoApplication::builder()
            .service::<LocalService>()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        Ok(ApplicationNode::new(node, application.build()))
    }

    fn commit_record(
        node: &Node,
        scope_id: ScopeId,
        id: &str,
    ) -> Result<LocalRecord, LocalPeerError> {
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(<LocalService as myko_federation::MykoService>::SERVICE_ID),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("local:test"),
            authority: AuthorityPresentation::direct_node(PrincipalId::new("local:test")),
            resource_claims: vec![ResourceClaim::scope(
                scope_id.clone(),
                ResourceClaimKind::Primary,
            )],
            application_capabilities: Vec::new(),
            arguments_digest: None,
            command_type: "local.insert".to_owned(),
            payload: Vec::new(),
        };
        let admission = node.admit(request.clone())?;
        let record = LocalRecord {
            id: LocalRecordId::from(id),
            value: id.to_owned(),
        };
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![
                    ItemMutation::set(&record)
                        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?,
                ],
            },
            Vec::new(),
        )?;
        Ok(record)
    }

    #[tokio::test]
    async fn local_peer_drives_reactive_query_without_polling() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let scope_id = ScopeId::new("local-scope");
        let initial = commit_record(&node, scope_id.clone(), "record-1")?;
        let server = LocalNodeServer::spawn(
            &socket,
            node.clone(),
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let reactive = LocalItemClient::new(&socket)
            .watch_serving_items_reactive(scope_id.clone(), AllLocalRecords)
            .await?;
        let (updates_tx, updates_rx) = flume::unbounded();
        let _guard = reactive.live().state().subscribe(move |signal| {
            if let Signal::Value(state) = signal {
                let _ignored = updates_tx.send(state.clone());
            }
        });
        let _initial_notification = updates_rx.try_recv();

        let second = commit_record(&node, scope_id.clone(), "record-2")?;
        let update = tokio::time::timeout(Duration::from_secs(2), updates_rx.recv_async())
            .await
            .map_err(|_| LocalPeerError::Protocol("local reactive update timed out".to_owned()))?
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        if update.value != Some(vec![initial.clone(), second.clone()])
            || update.liveness != SubscriptionLiveness::Current
        {
            return Err(LocalPeerError::Protocol(format!(
                "unexpected local reactive state: {update:?}"
            )));
        }

        server.shutdown().await?;
        let resynchronizing = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let update = updates_rx.recv_async().await.map_err(|error| {
                    LocalPeerError::Protocol(format!("reactive observation ended: {error}"))
                })?;
                if matches!(
                    update.liveness,
                    SubscriptionLiveness::Resynchronizing { .. }
                ) {
                    return Ok::<_, LocalPeerError>(update);
                }
            }
        })
        .await
        .map_err(|_| {
            LocalPeerError::Protocol("local reactive state did not begin resync".to_owned())
        })??;
        if resynchronizing.value != Some(vec![initial.clone(), second.clone()]) {
            return Err(LocalPeerError::Protocol(format!(
                "local reactive state did not retain stale data: {resynchronizing:?}"
            )));
        }

        let third = commit_record(&node, scope_id, "record-3")?;
        let server = LocalNodeServer::spawn(
            &socket,
            node,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let update = updates_rx.recv_async().await.map_err(|error| {
                    LocalPeerError::Protocol(format!("reactive observation ended: {error}"))
                })?;
                if update.liveness == SubscriptionLiveness::Current
                    && update.value == Some(vec![initial.clone(), second.clone(), third.clone()])
                {
                    return Ok::<_, LocalPeerError>(update);
                }
            }
        })
        .await
        .map_err(|_| {
            LocalPeerError::Protocol("local reactive state did not recover".to_owned())
        })??;
        if recovered.through.is_none() {
            return Err(LocalPeerError::Protocol(
                "recovered local reactive state omitted its cursor".to_owned(),
            ));
        }

        let retained = reactive.live().clone();
        drop(reactive);
        if !matches!(
            retained.current().liveness,
            SubscriptionLiveness::Invalid { ref reason } if reason == "subscription owner dropped"
        ) {
            return Err(LocalPeerError::Protocol(
                "dropping the owner did not invalidate retained state".to_owned(),
            ));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_peer_watches_command_lifecycle_without_polling() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let application = local_record_application(node.clone())?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            application,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let client = LocalCommandClient::new(&socket);
        let submitted = client
            .submit_command(SetLocalRecord {
                id: LocalRecordId::from("lifecycle-record"),
                value: "pending".to_owned(),
            })
            .await?;
        let Some(snapshot) = submitted.command else {
            return Err(LocalPeerError::Protocol(
                "local command submission returned no state".to_owned(),
            ));
        };
        let command_id = snapshot.request.id;
        let (_initial, mut subscription) = client.watch_command(command_id).await?;
        let admission = node.claim(command_id)?;
        node.commit(
            command_id,
            ChangeBatch {
                id: BatchId::new(),
                command_id,
                service_id: snapshot.request.service_id,
                scope_id: snapshot.request.scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: Vec::new(),
            },
            Vec::new(),
        )?;
        let committed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let command = subscription.recv().await?;
                if command.state.is_committed() {
                    return Ok::<_, LocalPeerError>(command);
                }
            }
        })
        .await
        .map_err(|_| LocalPeerError::Protocol("local command watch timed out".to_owned()))??;
        if !committed.state.is_committed() {
            return Err(LocalPeerError::Protocol(
                "local command watch returned a non-commit".to_owned(),
            ));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_client_submits_and_decodes_authenticated_approval() -> Result<(), LocalPeerError>
    {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let principal = Principal::node(PrincipalId::new("local:approver"));
        let sessions = NodeSessionService::new(node, Arc::new(ApprovalPolicy));
        let server =
            LocalNodeServer::spawn_sessions_authenticated(&socket, sessions, principal.clone())
                .await?;
        let decision = LocalCommandClient::new(&socket)
            .approve_authority(ChallengeId::new("local-challenge"), true)
            .await?;
        assert!(decision.approved);
        assert_eq!(decision.approver, principal);
        assert_eq!(decision.challenge_id.as_str(), "local-challenge");
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_item_application_and_live_clients_preserve_authority_presentations()
    -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let _record = commit_record(&node, ScopeId::new("local-scope"), "presented")?;
        let application = local_record_application(node.clone())?;
        let original = Principal::new(PrincipalId::new("person:owner"), PrincipalKind::Person);
        let executor = Principal::new(PrincipalId::new("agent:desktop"), PrincipalKind::Agent);
        let presentation = AuthorityPresentation::direct(original.clone()).forward(ProvenanceHop {
            delegation_id: DelegationId::new("local-delegation"),
            delegator: original,
            delegate: executor.clone(),
            operation: ProvenanceOperation::AgentInvocation {
                agent_id: "desktop".to_owned(),
            },
        });
        let operations = Arc::new(Mutex::new(Vec::new()));
        let policy: Arc<dyn AccessPolicy> = Arc::new(PresentationPolicy {
            expected: presentation.clone(),
            operations: Arc::clone(&operations),
        });
        let sessions = NodeSessionService::for_application(application, policy);
        let server =
            LocalNodeServer::spawn_sessions_authenticated(&socket, sessions, executor).await?;

        let (_initial, items) = LocalItemClient::new(&socket)
            .with_authority(presentation.clone())
            .watch_serving_items(ScopeId::new("local-scope"), AllLocalRecords)
            .await?;
        drop(items);
        let report = LocalApplicationClient::new(&socket)
            .with_authority(presentation.clone())
            .watch_report(&LocalRecordCount {
                source_node: node.node_id(),
            })
            .await?;
        drop(report);
        let live = LocalNodeClient::new(&socket)
            .with_authority(presentation)
            .follow_live(vec!["presented-topic".to_owned()])
            .await?;
        drop(live);

        {
            let observed = operations.lock().map_err(|_| {
                LocalPeerError::Protocol("presentation-policy lock is poisoned".to_owned())
            })?;
            for expected in [
                AccessOperation::ReadItems,
                AccessOperation::FollowItems,
                AccessOperation::FollowHandler,
                AccessOperation::SubscribeLive,
            ] {
                if !observed.contains(&expected) {
                    return Err(LocalPeerError::Protocol(format!(
                        "authority presentation was not exercised for {expected:?}"
                    )));
                }
            }
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_peer_executes_typed_command_to_its_result() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let application = local_record_application(node.clone())?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            application.clone(),
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let client = LocalCommandClient::new(&socket);
        let mut pending = application
            .watch_pending_commands()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        let command_application = application.clone();
        let (result, handled) = tokio::join!(
            client.exec_command(SetLocalRecord {
                id: LocalRecordId::from("record-command"),
                value: "typed result".to_owned(),
            }),
            async move {
                let command = pending.recv_async().await?;
                command_application
                    .dispatch_registered_command(command.request.id)
                    .map_err(|error| LocalPeerError::Protocol(error.to_string()))
            }
        );
        if !result? {
            return Err(LocalPeerError::Protocol(
                "typed command returned the wrong result".to_owned(),
            ));
        }
        handled.map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        let records = node.query_items_in(
            node.node_id(),
            &ScopeId::for_item::<LocalRecord>(&LocalRecordId::from("local-scope")),
            AllLocalRecords,
        )?;
        if !matches!(records.as_slice(), [record] if record.value == "typed result") {
            return Err(LocalPeerError::Protocol(
                "typed command did not commit its item".to_owned(),
            ));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_peer_executes_registered_report_as_live_stream() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let application = local_record_application(node.clone())?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            application,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let mut report = LocalApplicationClient::new(&socket)
            .watch_report(&LocalRecordCount {
                source_node: node.node_id(),
            })
            .await?;
        if report.current().value != Some(0) {
            return Err(LocalPeerError::Protocol(
                "registered report initial value was not empty".to_owned(),
            ));
        }

        let _record = commit_record(&node, ScopeId::new("local-scope"), "record-1")?;
        let update = tokio::time::timeout(Duration::from_secs(2), report.recv())
            .await
            .map_err(|_| LocalPeerError::Protocol("local report update timed out".to_owned()))??;
        if update.value != Some(1) || update.liveness != SubscriptionLiveness::Current {
            return Err(LocalPeerError::Protocol(format!(
                "unexpected registered report state: {update:?}"
            )));
        }
        drop(report);
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_view_sends_persisted_initial_rows() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let expected = commit_record(&node, ScopeId::new("local-scope"), "persisted")?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            local_record_application(node.clone())?,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;

        let view = LocalApplicationClient::new(&socket)
            .watch_view(&LocalRecordsView {
                source_node: node.node_id(),
            })
            .await?;
        if view.current().value.as_deref() != Some(std::slice::from_ref(&expected)) {
            return Err(LocalPeerError::Protocol(format!(
                "local view lost its initial rows: {:?}",
                view.current()
            )));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_client_waits_until_the_socket_starts_listening() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let expected_node = node.node_id();
        let reconnect_policy =
            ReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(20))
                .map_err(|error| LocalPeerError::Protocol(error.to_owned()))?;
        let client = LocalNodeClient::new(&socket).with_reconnect_policy(reconnect_policy);
        let pending = tokio::spawn(async move { client.identify().await });

        tokio::time::sleep(Duration::from_millis(80)).await;
        if pending.is_finished() {
            return Err(LocalPeerError::Protocol(
                "local client stopped retrying before the socket existed".to_owned(),
            ));
        }

        let server = LocalNodeServer::spawn(
            &socket,
            node,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let identified = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .map_err(|_| LocalPeerError::Protocol("local client did not reconnect".to_owned()))?
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))??;
        if identified != expected_node {
            return Err(LocalPeerError::Protocol(
                "reconnected local client identified the wrong node".to_owned(),
            ));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_requests_wait_for_the_node_startup_barrier() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let startup = node.hold_startup();
        let server = LocalNodeServer::spawn_application(
            &socket,
            local_record_application(node.clone())?,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let client = LocalApplicationClient::new(&socket);
        let source_node = node.node_id();
        let pending =
            tokio::spawn(
                async move { client.watch_report(&LocalRecordCount { source_node }).await },
            );
        tokio::time::sleep(Duration::from_millis(25)).await;
        if pending.is_finished() {
            return Err(LocalPeerError::Protocol(
                "local handler escaped the node startup barrier".to_owned(),
            ));
        }

        startup.ready();
        let report = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .map_err(|_| LocalPeerError::Protocol("startup-ready handler timed out".to_owned()))?
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))??;
        if report.current().value != Some(0) {
            return Err(LocalPeerError::Protocol(
                "startup-ready handler returned the wrong initial state".to_owned(),
            ));
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn local_reactive_handler_survives_server_restart() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let server = LocalNodeServer::spawn_application(
            &socket,
            local_record_application(node.clone())?,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let reactive = LocalApplicationClient::new(&socket)
            .watch_report_reactive(&LocalRecordCount {
                source_node: node.node_id(),
            })
            .await?;
        let (updates_tx, updates_rx) = flume::unbounded();
        let _guard = reactive.live().state().subscribe(move |signal| {
            if let Signal::Value(state) = signal {
                let _ignored = updates_tx.send(state.clone());
            }
        });
        let _initial_notification = updates_rx.try_recv();

        let _first = commit_record(&node, ScopeId::new("local-scope"), "record-1")?;
        let first = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let update = updates_rx.recv_async().await.map_err(|error| {
                    LocalPeerError::Protocol(format!("handler observation ended: {error}"))
                })?;
                if update.liveness == SubscriptionLiveness::Current && update.value == Some(1) {
                    return Ok::<_, LocalPeerError>(update);
                }
            }
        })
        .await
        .map_err(|_| LocalPeerError::Protocol("reactive handler did not update".to_owned()))??;
        if first.through.is_none() {
            return Err(LocalPeerError::Protocol(
                "reactive handler update omitted its cursor".to_owned(),
            ));
        }

        server.shutdown().await?;
        let resynchronizing = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let update = updates_rx.recv_async().await.map_err(|error| {
                    LocalPeerError::Protocol(format!("handler observation ended: {error}"))
                })?;
                if matches!(
                    update.liveness,
                    SubscriptionLiveness::Resynchronizing { .. }
                ) {
                    return Ok::<_, LocalPeerError>(update);
                }
            }
        })
        .await
        .map_err(|_| {
            LocalPeerError::Protocol("reactive handler did not begin resync".to_owned())
        })??;
        if resynchronizing.value != Some(1) || resynchronizing.through != first.through {
            return Err(LocalPeerError::Protocol(format!(
                "reactive handler did not retain its coherent state: {resynchronizing:?}"
            )));
        }

        let _second = commit_record(&node, ScopeId::new("local-scope"), "record-2")?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            local_record_application(node)?,
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let update = updates_rx.recv_async().await.map_err(|error| {
                    LocalPeerError::Protocol(format!("handler observation ended: {error}"))
                })?;
                if update.liveness == SubscriptionLiveness::Current && update.value == Some(2) {
                    return Ok::<_, LocalPeerError>(update);
                }
            }
        })
        .await
        .map_err(|_| LocalPeerError::Protocol("reactive handler did not recover".to_owned()))??;
        if recovered.through.is_none() || recovered.through == first.through {
            return Err(LocalPeerError::Protocol(
                "reactive handler recovery did not advance its cursor".to_owned(),
            ));
        }

        drop(reactive);
        server.shutdown().await
    }
}
