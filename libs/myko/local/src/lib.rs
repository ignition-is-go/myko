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

use hyphae::Watchable as _;
use myko_app::{
    ApplicationNode, ApplicationSchema, ErasedHandlerState, HandlerKind, HandlerRequest,
    QueryHandler, ReportHandler, ViewHandler,
};
use myko_federation::{
    AccessOperation, AccessPolicy, AccessRequest, CommandClient, CommandClientFuture, CommandId,
    CommandRequest, CommandResponse, CommandSnapshot, ItemClient, ItemFollowRequest, ItemQuery,
    ItemQuerySnapshot, ItemQueryStream, ItemQueryUpdate, ItemStatePage, ItemStatePageFuture,
    ItemStateRequest, ItemStateUpdate, LiveSubscription, LiveSubscriptionState, Node, NodeError,
    PrincipalId, ReconnectPolicy, ScopeId, SubscriptionLiveness, live_subscription,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
};

const PROTOCOL_VERSION: u32 = 2;
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Envelope<T> {
    version: u32,
    body: T,
}

impl<T> Envelope<T> {
    const fn new(body: T) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PeerRequest {
    ItemState {
        request: ItemStateRequest,
    },
    FollowItems {
        request: ItemFollowRequest,
    },
    SubmitCommand {
        command: CommandRequest,
    },
    CommandState {
        command_id: CommandId,
    },
    CancelCommand {
        command_id: CommandId,
        reason: String,
    },
    FollowCommand {
        command_id: CommandId,
    },
    FollowHandler {
        request: HandlerRequest,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PeerFrame {
    ItemState { page: Box<ItemStatePage> },
    ItemFollowReady { request: Box<ItemFollowRequest> },
    ItemUpdate { update: Box<ItemStateUpdate> },
    Command { response: Box<CommandResponse> },
    HandlerState { state: Box<ErasedHandlerState> },
    Error { message: String },
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
            ApplicationNode::new(node, ApplicationSchema::new()),
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
        let socket_path = socket_path.as_ref().to_path_buf();
        prepare_socket_path(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(serve(
            listener,
            application,
            principal_id,
            access_policy,
            shutdown_rx,
        ));
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

/// Typed item client bound to one local Myko peer socket.
#[derive(Debug, Clone)]
pub struct LocalItemClient {
    socket_path: PathBuf,
    reconnect_policy: ReconnectPolicy,
}

impl LocalItemClient {
    /// Creates a local typed-state client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    /// Overrides reconnect timing for subsequently created reactive streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
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
        let subscription = LocalItemQuerySubscription::connect(&self.socket_path, stream).await?;
        Ok((initial, subscription))
    }
}

impl ItemClient for LocalItemClient {
    type Error = LocalPeerError;

    fn item_state_page(&self, request: ItemStateRequest) -> ItemStatePageFuture<'_, Self::Error> {
        Box::pin(async move {
            let mut stream = UnixStream::connect(&self.socket_path).await?;
            write_frame(
                &mut stream,
                &Envelope::new(PeerRequest::ItemState { request }),
            )
            .await?;
            match read_peer_frame(&mut stream).await? {
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
#[derive(Debug, Clone)]
pub struct LocalCommandClient {
    socket_path: PathBuf,
}

impl LocalCommandClient {
    /// Creates a local command client.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    /// Reads one command and follows its lifecycle without polling.
    ///
    /// # Errors
    ///
    /// Returns an error if the command is unknown or follow setup fails.
    pub async fn watch_command(
        &self,
        command_id: CommandId,
    ) -> Result<(CommandResponse, LocalCommandSubscription), LocalPeerError> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        write_frame(
            &mut stream,
            &Envelope::new(PeerRequest::FollowCommand { command_id }),
        )
        .await?;
        match read_peer_frame(&mut stream).await? {
            PeerFrame::Command { response } if response.command.is_some() => Ok((
                (*response).clone(),
                LocalCommandSubscription {
                    stream,
                    source_node: response.source_node,
                    command_id,
                    current: response.command.clone().ok_or_else(|| {
                        LocalPeerError::Protocol(
                            "local command follow omitted initial state".to_owned(),
                        )
                    })?,
                },
            )),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer did not return command follow state".to_owned(),
            )),
        }
    }

    async fn request(&self, request: PeerRequest) -> Result<CommandResponse, LocalPeerError> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        write_frame(&mut stream, &Envelope::new(request)).await?;
        match read_peer_frame(&mut stream).await? {
            PeerFrame::Command { response } => Ok(*response),
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer returned a non-command frame".to_owned(),
            )),
        }
    }
}

impl CommandClient for LocalCommandClient {
    type Error = LocalPeerError;

    fn submit_command(&self, command: CommandRequest) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::SubmitCommand { command }))
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::CommandState { command_id }))
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(self.request(PeerRequest::CancelCommand { command_id, reason }))
    }
}

/// Typed client for application-registered query, report, and view handlers.
#[derive(Debug, Clone)]
pub struct LocalApplicationClient {
    socket_path: PathBuf,
    reconnect_policy: ReconnectPolicy,
}

impl LocalApplicationClient {
    /// Creates an application handler client for one owner-local Myko node.
    #[must_use]
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    /// Overrides reconnect timing for subsequently created reactive handlers.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
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
            scope_id: None,
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
            scope_id: None,
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
        self.watch(HandlerRequest {
            kind: HandlerKind::View,
            handler_id: V::VIEW_ID.to_owned(),
            source_node: None,
            scope_id: None,
            params: serde_json::to_value(view)?,
        })
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
    ) -> Result<LocalReactiveHandlerSubscription<Vec<V::Item>, V::Cursor>, LocalPeerError>
    where
        V: ViewHandler,
    {
        let request = HandlerRequest {
            kind: HandlerKind::View,
            handler_id: V::VIEW_ID.to_owned(),
            source_node: None,
            scope_id: None,
            params: serde_json::to_value(view)?,
        };
        let subscription = self.watch(request.clone()).await?;
        Ok(drive_handler_reactive(self.clone(), request, subscription))
    }

    async fn watch<T, C>(
        &self,
        request: HandlerRequest,
    ) -> Result<LocalHandlerSubscription<T, C>, LocalPeerError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        write_frame(
            &mut stream,
            &Envelope::new(PeerRequest::FollowHandler { request }),
        )
        .await?;
        match read_peer_frame(&mut stream).await? {
            PeerFrame::HandlerState { state } => Ok(LocalHandlerSubscription {
                stream,
                current: decode_handler_state(*state)?,
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
        match read_peer_frame(&mut self.stream).await? {
            PeerFrame::HandlerState { state } => {
                self.current = decode_handler_state(*state)?;
                Ok(self.current.clone())
            }
            PeerFrame::Error { message } => Err(LocalPeerError::Protocol(message)),
            _ => Err(LocalPeerError::Protocol(
                "local peer changed application handler stream type".to_owned(),
            )),
        }
    }
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
                match client.watch(request.clone()).await {
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
        match read_peer_frame(&mut self.stream).await? {
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
        query: ItemQueryStream<Q>,
    ) -> Result<Self, LocalPeerError> {
        let mut stream = UnixStream::connect(socket_path).await?;
        write_frame(
            &mut stream,
            &Envelope::new(PeerRequest::FollowItems {
                request: query.request().clone(),
            }),
        )
        .await?;
        match read_peer_frame(&mut stream).await? {
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
        match read_peer_frame(&mut self.stream).await? {
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

async fn serve(
    listener: UnixListener,
    application: ApplicationNode,
    principal_id: PrincipalId,
    access_policy: Arc<dyn AccessPolicy>,
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
                    application.clone(),
                    principal_id.clone(),
                    Arc::clone(&access_policy),
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
    application: ApplicationNode,
    principal_id: PrincipalId,
    access_policy: Arc<dyn AccessPolicy>,
    shutdown: watch::Receiver<bool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let result = async {
        let request: Envelope<PeerRequest> = read_frame(&mut stream).await?;
        require_version(request.version)?;
        serve_request(
            &mut stream,
            &application,
            &principal_id,
            access_policy.as_ref(),
            shutdown,
            request.body,
        )
        .await
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

async fn serve_request(
    stream: &mut UnixStream,
    application: &ApplicationNode,
    principal_id: &PrincipalId,
    access_policy: &dyn AccessPolicy,
    shutdown: watch::Receiver<bool>,
    request: PeerRequest,
) -> Result<(), LocalPeerError> {
    let node = application.node();
    match request {
        PeerRequest::ItemState { request } => {
            authorize_items(
                access_policy,
                principal_id,
                AccessOperation::ReadItems,
                &request,
            )?;
            let page = node.item_state_page(request)?;
            write_frame(
                stream,
                &Envelope::new(PeerFrame::ItemState {
                    page: Box::new(page),
                }),
            )
            .await
        }
        PeerRequest::FollowItems { request } => {
            serve_item_follow(stream, node, principal_id, access_policy, shutdown, request).await
        }
        PeerRequest::SubmitCommand { command } => {
            authorize_command(
                access_policy,
                principal_id,
                AccessOperation::SubmitCommand,
                &command,
            )?;
            let command = node.submit(command)?;
            write_command(stream, node.node_id(), Some(command)).await
        }
        PeerRequest::CommandState { command_id } => {
            let command = node.command(command_id)?;
            authorize_command_state(
                access_policy,
                principal_id,
                AccessOperation::ReadCommand,
                command_id,
                command.as_ref(),
            )?;
            write_command(stream, node.node_id(), command).await
        }
        PeerRequest::CancelCommand { command_id, reason } => {
            let current = node.command(command_id)?;
            authorize_command_state(
                access_policy,
                principal_id,
                AccessOperation::CancelCommand,
                command_id,
                current.as_ref(),
            )?;
            let command = node.cancel(command_id, reason)?;
            write_command(stream, node.node_id(), Some(command)).await
        }
        PeerRequest::FollowCommand { command_id } => {
            serve_command_follow(
                stream,
                node,
                principal_id,
                access_policy,
                shutdown,
                command_id,
            )
            .await
        }
        PeerRequest::FollowHandler { request } => {
            serve_handler_follow(
                stream,
                application,
                principal_id,
                access_policy,
                shutdown,
                request,
            )
            .await
        }
    }
}

async fn serve_handler_follow(
    stream: &mut UnixStream,
    application: &ApplicationNode,
    principal_id: &PrincipalId,
    access_policy: &dyn AccessPolicy,
    mut shutdown: watch::Receiver<bool>,
    request: HandlerRequest,
) -> Result<(), LocalPeerError> {
    authorize_handler(access_policy, principal_id, &request)?;
    let subscription = application
        .watch_handler(&request)
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    let (wake_tx, wake_rx) = flume::bounded(1);
    let _guard = subscription.live().state().subscribe(move |_| {
        let _ignored = wake_tx.try_send(());
    });
    let mut sent: Option<ErasedHandlerState> = None;
    loop {
        let current = subscription.live().current();
        if sent.as_ref() != Some(&current) {
            write_frame(
                stream,
                &Envelope::new(PeerFrame::HandlerState {
                    state: Box::new(current.clone()),
                }),
            )
            .await?;
            sent = Some(current);
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            wake = wake_rx.recv_async() => {
                if wake.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

async fn serve_item_follow(
    stream: &mut UnixStream,
    node: &Node,
    principal_id: &PrincipalId,
    access_policy: &dyn AccessPolicy,
    mut shutdown: watch::Receiver<bool>,
    request: ItemFollowRequest,
) -> Result<(), LocalPeerError> {
    if request.serving_node != node.node_id() {
        return Err(LocalPeerError::Protocol(
            "local item follow names another serving node".to_owned(),
        ));
    }
    authorize_follow(access_policy, principal_id, &request)?;
    let mut events = node.subscribe(request.after)?;
    write_frame(
        stream,
        &Envelope::new(PeerFrame::ItemFollowReady {
            request: Box::new(request.clone()),
        }),
    )
    .await?;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            event = events.recv_async() => {
                if let Some(update) = request.update_from_envelope(&event?)? {
                    write_frame(
                        stream,
                        &Envelope::new(PeerFrame::ItemUpdate {
                            update: Box::new(update),
                        }),
                    ).await?;
                }
            }
        }
    }
}

async fn serve_command_follow(
    stream: &mut UnixStream,
    node: &Node,
    principal_id: &PrincipalId,
    access_policy: &dyn AccessPolicy,
    mut shutdown: watch::Receiver<bool>,
    command_id: CommandId,
) -> Result<(), LocalPeerError> {
    let (response, mut commands) = node.watch_command_eventually(command_id).await?;
    authorize_command_state(
        access_policy,
        principal_id,
        AccessOperation::FollowCommand,
        command_id,
        response.command.as_ref(),
    )?;
    write_frame(
        stream,
        &Envelope::new(PeerFrame::Command {
            response: Box::new(response),
        }),
    )
    .await?;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            command = commands.recv_async() => {
                write_command(stream, node.node_id(), Some(command?)).await?;
            }
        }
    }
}

async fn write_command(
    stream: &mut UnixStream,
    source_node: myko_federation::NodeId,
    command: Option<CommandSnapshot>,
) -> Result<(), LocalPeerError> {
    write_frame(
        stream,
        &Envelope::new(PeerFrame::Command {
            response: Box::new(CommandResponse {
                source_node,
                command,
            }),
        }),
    )
    .await
}

fn authorize_command(
    policy: &dyn AccessPolicy,
    principal_id: &PrincipalId,
    operation: AccessOperation,
    command: &CommandRequest,
) -> Result<(), LocalPeerError> {
    policy
        .authorize(&AccessRequest {
            principal_id: principal_id.clone(),
            operation,
            service_id: Some(command.service_id.clone()),
            scope_id: Some(command.scope_id.clone()),
            command_id: Some(command.id),
            command_type: Some(command.command_type.clone()),
            command_principal_id: Some(command.principal_id.clone()),
            live_topics: Vec::new(),
        })
        .map_err(LocalPeerError::Protocol)
}

fn authorize_command_state(
    policy: &dyn AccessPolicy,
    principal_id: &PrincipalId,
    operation: AccessOperation,
    command_id: CommandId,
    command: Option<&CommandSnapshot>,
) -> Result<(), LocalPeerError> {
    policy
        .authorize(&AccessRequest {
            principal_id: principal_id.clone(),
            operation,
            service_id: command.map(|command| command.request.service_id.clone()),
            scope_id: command.map(|command| command.request.scope_id.clone()),
            command_id: Some(command_id),
            command_type: command.map(|command| command.request.command_type.clone()),
            command_principal_id: command.map(|command| command.request.principal_id.clone()),
            live_topics: Vec::new(),
        })
        .map_err(LocalPeerError::Protocol)
}

fn authorize_items(
    policy: &dyn AccessPolicy,
    principal_id: &PrincipalId,
    operation: AccessOperation,
    request: &ItemStateRequest,
) -> Result<(), LocalPeerError> {
    policy
        .authorize(&AccessRequest {
            principal_id: principal_id.clone(),
            operation,
            service_id: Some(request.service_id.clone()),
            scope_id: Some(request.scope_id.clone()),
            command_id: None,
            command_type: None,
            command_principal_id: None,
            live_topics: Vec::new(),
        })
        .map_err(LocalPeerError::Protocol)
}

fn authorize_follow(
    policy: &dyn AccessPolicy,
    principal_id: &PrincipalId,
    request: &ItemFollowRequest,
) -> Result<(), LocalPeerError> {
    policy
        .authorize(&AccessRequest {
            principal_id: principal_id.clone(),
            operation: AccessOperation::FollowItems,
            service_id: Some(request.service_id.clone()),
            scope_id: Some(request.scope_id.clone()),
            command_id: None,
            command_type: None,
            command_principal_id: None,
            live_topics: Vec::new(),
        })
        .map_err(LocalPeerError::Protocol)
}

fn authorize_handler(
    policy: &dyn AccessPolicy,
    principal_id: &PrincipalId,
    request: &HandlerRequest,
) -> Result<(), LocalPeerError> {
    policy
        .authorize(&AccessRequest {
            principal_id: principal_id.clone(),
            operation: AccessOperation::FollowHandler,
            service_id: None,
            scope_id: request.scope_id.clone(),
            command_id: None,
            command_type: None,
            command_principal_id: None,
            live_topics: vec![format!(
                "handler:{}:{}",
                request.kind.as_str(),
                request.handler_id
            )],
        })
        .map_err(LocalPeerError::Protocol)
}

fn require_version(version: u32) -> Result<(), LocalPeerError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(LocalPeerError::Protocol(format!(
            "unsupported local peer protocol version {version}"
        )))
    }
}

async fn read_peer_frame(stream: &mut UnixStream) -> Result<PeerFrame, LocalPeerError> {
    let envelope: Envelope<PeerFrame> = read_frame(stream).await?;
    require_version(envelope.version)?;
    Ok(envelope.body)
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
mod tests {
    use std::{sync::Arc, time::Duration};

    use hyphae::{Signal, Watchable as _};
    use myko_federation::{
        AllowAllAccessPolicy, BatchId, ChangeBatch, CommandId, CommandRequest, PrincipalId,
        ServiceId, SubscriptionLiveness,
    };
    use myko_items::{ItemMutation, ItemProjection, ItemQuery, myko_item};

    use super::*;

    #[myko_item(service = "myko.local.test", scope_root)]
    pub struct LocalRecord {
        value: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct AllLocalRecords;

    impl ItemQuery for AllLocalRecords {
        type Item = LocalRecord;
        type Output = Vec<LocalRecord>;
        const QUERY_ID: &'static str = "local.all_records";

        fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
            projection.values().cloned().collect()
        }
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    struct LocalRecordCount {
        source_node: myko_federation::NodeId,
    }

    impl ReportHandler for LocalRecordCount {
        type Output = u64;
        type Cursor = myko_federation::LogPosition;
        const REPORT_ID: &'static str = "local.record_count";

        fn build(
            &self,
            context: &myko_app::HandlerContext,
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

    fn local_record_application(node: Node) -> Result<ApplicationNode, LocalPeerError> {
        let mut schema = ApplicationSchema::new();
        schema
            .register_query::<AllLocalRecords>()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        schema
            .register_report::<LocalRecordCount>()
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        Ok(ApplicationNode::new(node, schema))
    }

    fn commit_record(
        node: &Node,
        scope_id: ScopeId,
        id: &str,
    ) -> Result<LocalRecord, LocalPeerError> {
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("myko.local.test"),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("local:test"),
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
    async fn local_peer_follows_command_lifecycle_without_polling() -> Result<(), LocalPeerError> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("myko.sock");
        let node = Node::in_memory();
        let server = LocalNodeServer::spawn(
            &socket,
            node.clone(),
            PrincipalId::new("local:owner"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let client = LocalCommandClient::new(&socket);
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("myko.local.command"),
            scope_id: ScopeId::new("local-command-scope"),
            principal_id: PrincipalId::new("local:owner"),
            command_type: "local.execute".to_owned(),
            payload: Vec::new(),
        };
        let submitted = client.submit_command(request.clone()).await?;
        if !matches!(
            submitted.command.as_ref().map(|command| &command.state),
            Some(myko_federation::CommandState::Submitted)
        ) {
            return Err(LocalPeerError::Protocol(
                "local command was not submitted".to_owned(),
            ));
        }
        let (_initial, mut subscription) = client.watch_command(request.id).await?;
        let admission = node.claim(request.id)?;
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
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
        .map_err(|_| LocalPeerError::Protocol("local command follow timed out".to_owned()))??;
        if !committed.state.is_committed() {
            return Err(LocalPeerError::Protocol(
                "local command follow returned a non-commit".to_owned(),
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
