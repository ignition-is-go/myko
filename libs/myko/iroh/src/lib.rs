//! Iroh transport adapter for Myko 7 immutable replication batches.
//!
//! This crate owns peer connectivity only. Command admission, history,
//! idempotency, and event ingestion remain in `myko-federation`. Explicit pulls
//! provide bounded catch-up, supervised follow streams replay then push, and
//! authenticated peers can submit, inspect, or cancel commands without
//! claiming them.
//! Exact-scope pull and follow streams let subscribers advance a source cursor
//! without receiving unrelated event bodies.
//! Every inbound operation is presented to Myko's transport-neutral
//! [`AccessPolicy`] using the authenticated Iroh endpoint as its principal.
//! Long-lived history and live streams re-evaluate that policy when it changes,
//! so revocation closes already-open streams rather than only blocking the next
//! connection.
//! The same endpoint carries filtered best-effort live events without turning
//! them into immutable history.

#![forbid(unsafe_code)]

mod pairing;

pub use pairing::{
    MYKO_PAIRING_ALPN, PairingInvitation, PairingReceipt, PairingReceiptSubscription,
};

use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    num::NonZeroUsize,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use iroh::{
    Endpoint,
    endpoint::{BindOpts, Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
pub use iroh::{EndpointAddr, EndpointId, SecretKey};
use myko_app::{
    ApplicationNode, ErasedHandlerState, ErasedViewDelta, HandlerKind, HandlerRequest,
    QueryHandler, ReportHandler, ViewHandler,
};
use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, CommandClient, CommandClientFuture, CommandId,
    CommandResponse, CommandSnapshot, CommandStateClient, CommandStatePage, CommandStatePageFuture,
    CommandStateRequest, CommandStateSnapshot, CommandStateStream, CommandSubmission,
    CommandSubscription, CommandSubscriptionFuture, CommandWatchFuture, CommandWatchingClient,
    DenyAllAccessPolicy, ItemClient, ItemProjection, ItemQuery, ItemQuerySnapshot, ItemQueryStream,
    ItemQueryUpdate, ItemStatePage, ItemStatePageFuture, ItemStateRequest, ItemStateSnapshot,
    LiveCollection, LiveCollectionState, LiveCollectionWriter, LiveEvent, LivePublishReport,
    LiveSubscription, LiveSubscriptionState, LogPosition, Node, NodeId, PrincipalId,
    ReconnectPolicy, ReplicationCheckpoint, ReplicationCursorKey, ReplicationCursorStore,
    ReplicationReport, ScopeCatalogPage, ScopeId, ScopedReplicationBatch,
    ScopedReplicationCheckpoint, ScopedReplicationReport, SubscriptionLiveness, live_collection,
    live_subscription,
};
use myko_session::NodeSessionService;
use myko_wire::{
    NodeFrame as ReplicationFrame, NodeRequest as ReplicationRequest, NodeRequestEnvelope,
    WireEnvelope,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

/// ALPN used for framed Myko history pull and live-follow streams over Iroh.
pub const MYKO_REPLICATION_ALPN: &[u8] = b"myko/federation/7";
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const NATIVE_NODE_DESCRIPTOR_VERSION: u32 = 1;
const MAX_LIVE_TOPICS: usize = 256;
const MAX_LIVE_TOPIC_BYTES: usize = 512;
const MAX_SCOPE_CATALOG_PAGE: usize = 1_024;
type IrohViewDelta<T> = fn(&mut T, &ErasedViewDelta) -> Result<(), IrohReplicationError>;

/// Maps an authenticated Iroh endpoint to its transport-neutral principal ID.
#[must_use]
pub fn endpoint_principal_id(endpoint_id: EndpointId) -> PrincipalId {
    PrincipalId::new(format!("iroh:{endpoint_id}"))
}

/// Loads or creates one persistent Iroh transport identity.
///
/// The key is JSON encoded for compatibility with Iroh's serde contract. New
/// files are created with owner-only permissions on Unix and synchronized
/// before the identity is returned. Applications can use this for durable
/// short-lived client identities without adopting Myko's Redb node runtime.
///
/// # Errors
///
/// Returns an error if the parent directory or key file cannot be accessed, or
/// if an existing key is malformed.
pub fn load_or_create_secret_key(
    path: impl AsRef<Path>,
) -> Result<SecretKey, IrohReplicationError> {
    let path = path.as_ref();
    match fs::read(path) {
        Ok(encoded) => serde_json::from_slice(&encoded).map_err(IrohReplicationError::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_secret_key(path),
        Err(error) => Err(IrohReplicationError::Identity(error.to_string())),
    }
}

fn create_secret_key(path: &Path) -> Result<SecretKey, IrohReplicationError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| IrohReplicationError::Identity(error.to_string()))?;
    }
    let secret = SecretKey::generate();
    let encoded = serde_json::to_vec(&secret)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return load_or_create_secret_key(path);
        }
        Err(error) => return Err(IrohReplicationError::Identity(error.to_string())),
    };
    let result = file
        .write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|error| IrohReplicationError::Identity(error.to_string()));
    if let Err(error) = result {
        drop(file);
        let _cleanup = fs::remove_file(path);
        return Err(error);
    }
    drop(file);
    Ok(secret)
}

/// Backwards-compatible name for Myko's transport-neutral command response.
pub type RemoteCommandResponse = CommandResponse;

/// Errors produced by the Iroh replication adapter.
#[derive(Debug, Error)]
pub enum IrohReplicationError {
    #[error("Iroh endpoint error: {0}")]
    Endpoint(String),
    #[error("Iroh stream error: {0}")]
    Stream(String),
    #[error("replication encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("replication ingest error: {0}")]
    Ingest(#[from] myko_federation::NodeError),
    #[error("replication cursor error: {0}")]
    Cursor(String),
    #[error("replication supervisor error: {0}")]
    Supervisor(String),
    #[error("Iroh identity error: {0}")]
    Identity(String),
}

/// Pairing descriptor binding an Iroh endpoint to one Myko source history.
///
/// Endpoint identity authenticates the native transport. `node_id` identifies
/// the immutable Myko log expected behind it. Pairing and discovery layers can
/// choose any outer ticket encoding while preserving this distinction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeNodeDescriptor {
    pub version: u32,
    pub node_id: NodeId,
    pub endpoint: EndpointAddr,
}

impl NativeNodeDescriptor {
    /// Creates the current descriptor representation.
    #[must_use]
    pub const fn new(node_id: NodeId, endpoint: EndpointAddr) -> Self {
        Self {
            version: NATIVE_NODE_DESCRIPTOR_VERSION,
            node_id,
            endpoint,
        }
    }

    /// Validates this descriptor's version.
    ///
    /// # Errors
    ///
    /// Returns an error when the representation is not supported.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != NATIVE_NODE_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported native node descriptor version {}",
                self.version
            ));
        }
        Ok(())
    }
}

/// Decoded native bootstrap input with explicit legacy compatibility.
///
/// New producers serialize [`NativeNodeDescriptor`]. Existing raw
/// [`EndpointAddr`] JSON remains accepted as an unpinned peer reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NativePeerReference {
    Descriptor(NativeNodeDescriptor),
    LegacyEndpoint(EndpointAddr),
}

impl NativePeerReference {
    /// Returns the authenticated Iroh endpoint to contact.
    #[must_use]
    pub const fn endpoint(&self) -> &EndpointAddr {
        match self {
            Self::Descriptor(descriptor) => &descriptor.endpoint,
            Self::LegacyEndpoint(endpoint) => endpoint,
        }
    }

    /// Returns the pinned descriptor, if this is not a legacy reference.
    #[must_use]
    pub const fn descriptor(&self) -> Option<&NativeNodeDescriptor> {
        match self {
            Self::Descriptor(descriptor) => Some(descriptor),
            Self::LegacyEndpoint(_) => None,
        }
    }
}

impl From<NativeNodeDescriptor> for NativePeerReference {
    fn from(descriptor: NativeNodeDescriptor) -> Self {
        Self::Descriptor(descriptor)
    }
}

impl From<EndpointAddr> for NativePeerReference {
    fn from(endpoint: EndpointAddr) -> Self {
        Self::LegacyEndpoint(endpoint)
    }
}

/// Observable state of one supervised peer follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSyncStatus {
    pub peer: EndpointAddr,
    /// Expected Myko history identity for a pinned peer, if configured.
    pub expected_source_node: Option<NodeId>,
    pub source_node: Option<NodeId>,
    pub cursor: Option<LogPosition>,
    pub connected: bool,
    pub successful_connections: u64,
    pub successful_batches: u64,
    pub last_error: Option<String>,
}

/// Handle to a background cursor-tracked peer follower.
#[derive(Debug)]
pub struct PeerSync {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
    status: watch::Receiver<PeerSyncStatus>,
}

/// One authenticated remote subscription to best-effort Myko live events.
///
/// The stream has no replay guarantee. Consumers detect sequence gaps and
/// recover authoritative state through durable Myko queries or change streams.
#[derive(Debug)]
pub struct IrohLiveEventSubscription {
    connection: Connection,
    receive: RecvStream,
    source_node: NodeId,
}

impl IrohLiveEventSubscription {
    /// Returns the stable Myko identity advertised by the serving peer.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Receives the next best-effort live event.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or sends an unexpected frame.
    pub async fn recv(&mut self) -> Result<LiveEvent, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::Live { event } => Ok(*event),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote live subscription failed: {message}"
            ))),
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. } => Err(IrohReplicationError::Stream(
                "peer sent a non-live frame on a live subscription".to_owned(),
            )),
        }
    }

    /// Closes the live stream without shutting down the shared endpoint.
    pub fn close(self) {
        self.connection
            .close(0u32.into(), b"live subscription closed");
    }
}

/// Node-level supervisor for concurrent replication followers.
///
/// Peers are keyed by authenticated Iroh endpoint identity. Updating an entry
/// installs its new address or cursor policy and then shuts down the replaced
/// follower. Removing one peer does not disturb any other stream.
#[derive(Debug)]
pub struct PeerSupervisor {
    replicator: IrohReplicator,
    peers: Mutex<HashMap<EndpointId, PeerSync>>,
    status_revision: watch::Sender<u64>,
}

impl PeerSync {
    /// Returns a snapshot of replication progress and the latest transient error.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor state lock is poisoned.
    pub fn status(&self) -> Result<PeerSyncStatus, IrohReplicationError> {
        Ok(self.status.borrow().clone())
    }

    /// Subscribes to status changes for this follower.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<PeerSyncStatus> {
        self.status.clone()
    }

    /// Stops the follower and waits for its current pull or retry delay to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the background task terminated abnormally.
    pub async fn shutdown(self) -> Result<(), IrohReplicationError> {
        let _ = self.shutdown.send(true);
        self.task
            .await
            .map_err(|error| IrohReplicationError::Supervisor(error.to_string()))
    }
}

impl PeerSupervisor {
    /// Creates an empty peer supervisor over a running replication endpoint.
    #[must_use]
    pub fn new(replicator: IrohReplicator) -> Self {
        let (status_revision, _status_updates) = watch::channel(0);
        Self {
            replicator,
            peers: Mutex::new(HashMap::new()),
            status_revision,
        }
    }

    /// Starts or replaces a transient-cursor follower for one peer.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a replaced follower
    /// cannot be shut down cleanly.
    pub async fn upsert(
        &self,
        peer: EndpointAddr,
        after: Option<LogPosition>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self.replicator.follow(peer, after, retry_interval);
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a durable, source-aware follower for one peer.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if its checkpoint cannot be loaded, supervisor state is
    /// poisoned, or a replaced follower cannot be shut down cleanly.
    pub async fn upsert_persisted(
        &self,
        peer: EndpointAddr,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self
            .replicator
            .follow_persisted(peer, store, retry_interval)?;
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a durable follower pinned to one Myko source.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if its checkpoint cannot be loaded, supervisor state is
    /// poisoned, or a replaced follower cannot be shut down cleanly.
    pub async fn upsert_persisted_source(
        &self,
        peer: EndpointAddr,
        expected_source_node: NodeId,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self.replicator.follow_persisted_source(
            peer,
            expected_source_node,
            store,
            retry_interval,
        )?;
        self.replace(peer_id, follower).await
    }

    async fn replace(
        &self,
        peer_id: EndpointId,
        follower: PeerSync,
    ) -> Result<bool, IrohReplicationError> {
        let mut follower_status = follower.subscribe_status();
        let status_revision = self.status_revision.clone();
        tokio::spawn(async move {
            while follower_status.changed().await.is_ok() {
                status_revision.send_modify(|revision| {
                    *revision = revision.saturating_add(1);
                });
            }
        });
        let replaced = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?
            .insert(peer_id, follower);
        self.status_revision.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        let was_replaced = replaced.is_some();
        if let Some(replaced) = replaced {
            replaced.shutdown().await?;
        }
        Ok(was_replaced)
    }

    /// Stops and removes one peer follower.
    ///
    /// Returns `false` if the peer was not being followed.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or the follower cannot
    /// be shut down cleanly.
    pub async fn remove(&self, peer_id: EndpointId) -> Result<bool, IrohReplicationError> {
        let removed = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?
            .remove(&peer_id);
        let was_removed = removed.is_some();
        if was_removed {
            self.status_revision.send_modify(|revision| {
                *revision = revision.saturating_add(1);
            });
        }
        if let Some(removed) = removed {
            removed.shutdown().await?;
        }
        Ok(was_removed)
    }

    /// Returns snapshots for every currently supervised peer.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor or follower state is poisoned.
    pub fn statuses(&self) -> Result<Vec<PeerSyncStatus>, IrohReplicationError> {
        let peers = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?;
        let mut statuses = peers
            .values()
            .map(PeerSync::status)
            .collect::<Result<Vec<_>, _>>()?;
        drop(peers);
        statuses.sort_by_key(|status| status.peer.id.to_string());
        Ok(statuses)
    }

    /// Subscribes to any follower status or membership change.
    #[must_use]
    pub fn subscribe_statuses(&self) -> watch::Receiver<u64> {
        self.status_revision.subscribe()
    }

    /// Stops every peer follower while retaining the empty supervisor.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a follower cannot be
    /// shut down cleanly.
    pub async fn shutdown_all(&self) -> Result<(), IrohReplicationError> {
        let peers = {
            let mut peers = self.peers.lock().map_err(|_| {
                IrohReplicationError::Supervisor("peer lock is poisoned".to_owned())
            })?;
            std::mem::take(&mut *peers)
        };
        self.status_revision.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        for follower in peers.into_values() {
            follower.shutdown().await?;
        }
        Ok(())
    }

    /// Stops every peer follower, leaving the shared Iroh endpoint running.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a follower cannot be
    /// shut down cleanly.
    pub async fn shutdown(self) -> Result<(), IrohReplicationError> {
        self.shutdown_all().await
    }
}

/// Running Iroh endpoint that serves and pulls Myko replication batches.
#[derive(Debug, Clone)]
pub struct IrohReplicator {
    node: Node,
    sessions: NodeSessionService,
    pairing: pairing::PairingRegistry,
    router: Router,
}

/// Command-only client bound to one authenticated Iroh peer.
#[derive(Debug, Clone)]
pub struct IrohCommandClient {
    replicator: IrohReplicator,
    peer: EndpointAddr,
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
    replicator: IrohReplicator,
    peer: EndpointAddr,
    reconnect_policy: ReconnectPolicy,
}

/// Typed application-handler client bound to one authenticated Iroh peer.
#[derive(Debug, Clone)]
pub struct IrohApplicationClient {
    replicator: IrohReplicator,
    peer: EndpointAddr,
    reconnect_policy: ReconnectPolicy,
}

/// Current-then-live registered application handler over Iroh.
pub struct IrohHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    connection: Connection,
    receive: RecvStream,
    current: LiveSubscriptionState<T, C>,
    view_delta: Option<IrohViewDelta<T>>,
}

/// Runtime owner for a reconnecting Iroh application-handler cell.
pub struct IrohReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    writer: myko_federation::LiveSubscriptionWriter<T, C>,
    task: JoinHandle<()>,
}

/// Runtime owner for a reconnecting identity-preserving Iroh view.
pub struct IrohReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveCollection<T, C>,
    writer: LiveCollectionWriter<T, C>,
    task: JoinHandle<()>,
}

impl<T, C> IrohReactiveViewSubscription<T, C>
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

impl<T, C> Drop for IrohReactiveViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

impl<T, C> IrohReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the transport-independent application lifecycle cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T, C> {
        &self.live
    }
}

impl<T, C> Drop for IrohReactiveHandlerSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

/// Authenticated lossless typed query stream from one native Myko peer.
///
/// The initial value comes from paginated current state. Subsequent values are
/// produced only after a complete matching atomic batch is applied; command
/// envelopes and unrelated item bodies never cross this client API.
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
        Box::pin(self.replicator.submit_remote(self.peer.clone(), command))
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(
            self.replicator
                .command_remote(self.peer.clone(), command_id),
        )
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(
            self.replicator
                .cancel_remote(self.peer.clone(), command_id, reason),
        )
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

impl IrohApplicationClient {
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
    /// Returns an error when the handler cannot be encoded, authorized,
    /// established, or decoded.
    pub async fn watch_query<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<IrohHandlerSubscription<Q::Output, LogPosition>, IrohReplicationError>
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

    /// Starts a registered query and drives it into a reconnecting Hyphae cell.
    ///
    /// # Errors
    ///
    /// Retries transport failures indefinitely. Returns an error when the
    /// initial typed request cannot be encoded or materialized.
    pub async fn watch_query_reactive<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: &Q,
    ) -> Result<IrohReactiveHandlerSubscription<Q::Output, LogPosition>, IrohReplicationError>
    where
        Q: QueryHandler,
    {
        self.watch_reactive(HandlerRequest {
            kind: HandlerKind::Query,
            handler_id: Q::QUERY_ID.to_owned(),
            source_node: Some(source_node),
            scope_id: Some(scope_id),
            params: serde_json::to_value(query)?,
        })
        .await
    }

    /// Starts a registered report stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler cannot be established or decoded.
    pub async fn watch_report<R>(
        &self,
        report: &R,
    ) -> Result<IrohHandlerSubscription<R::Output, R::Cursor>, IrohReplicationError>
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

    /// Starts a registered report and drives it into a reconnecting Hyphae cell.
    ///
    /// # Errors
    ///
    /// Retries transport failures indefinitely. Returns an error when the
    /// initial typed request cannot be encoded or materialized.
    pub async fn watch_report_reactive<R>(
        &self,
        report: &R,
    ) -> Result<IrohReactiveHandlerSubscription<R::Output, R::Cursor>, IrohReplicationError>
    where
        R: ReportHandler,
    {
        self.watch_reactive(HandlerRequest {
            kind: HandlerKind::Report,
            handler_id: R::REPORT_ID.to_owned(),
            source_node: None,
            scope_id: report.access_scope(),
            params: serde_json::to_value(report)?,
        })
        .await
    }

    /// Starts a registered view stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler cannot be established or decoded.
    pub async fn watch_view<V>(
        &self,
        view: &V,
    ) -> Result<IrohHandlerSubscription<Vec<V::Item>, V::Cursor>, IrohReplicationError>
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

    /// Starts a registered view and drives it into a reconnecting Hyphae cell.
    ///
    /// # Errors
    ///
    /// Retries transport failures indefinitely. Returns an error when the
    /// initial typed request cannot be encoded or materialized.
    pub async fn watch_view_reactive<V>(
        &self,
        view: &V,
    ) -> Result<IrohReactiveViewSubscription<V::Item, V::Cursor>, IrohReplicationError>
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
            .watch_with_delta_reconnecting(request.clone(), Some(apply_view_delta::<V>))
            .await?;
        Ok(drive_reactive_view_subscription::<V>(
            self.clone(),
            request,
            subscription,
        ))
    }

    async fn watch_reactive<T, C>(
        &self,
        request: HandlerRequest,
    ) -> Result<IrohReactiveHandlerSubscription<T, C>, IrohReplicationError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        let subscription = self
            .watch_with_delta_reconnecting(request.clone(), None)
            .await?;
        Ok(drive_reactive_handler_subscription(
            self.clone(),
            request,
            subscription,
        ))
    }

    async fn watch_with_delta_reconnecting<T, C>(
        &self,
        request: HandlerRequest,
        view_delta: Option<IrohViewDelta<T>>,
    ) -> Result<IrohHandlerSubscription<T, C>, IrohReplicationError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        let mut delay = self.reconnect_policy.initial_delay();
        loop {
            match self.watch_with_delta(request.clone(), view_delta).await {
                Ok(subscription) => return Ok(subscription),
                Err(error) if reactive_item_error_is_recoverable(&error) => {
                    tokio::time::sleep(delay).await;
                    delay = self.reconnect_policy.next_delay(delay);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn watch<T, C>(
        &self,
        request: HandlerRequest,
    ) -> Result<IrohHandlerSubscription<T, C>, IrohReplicationError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        self.watch_with_delta(request, None).await
    }

    async fn watch_with_delta<T, C>(
        &self,
        request: HandlerRequest,
        view_delta: Option<IrohViewDelta<T>>,
    ) -> Result<IrohHandlerSubscription<T, C>, IrohReplicationError>
    where
        T: hyphae::CellValue + DeserializeOwned,
        C: hyphae::CellValue + DeserializeOwned,
    {
        let connection = self
            .replicator
            .router
            .endpoint()
            .connect(self.peer.clone(), MYKO_REPLICATION_ALPN)
            .await
            .map_err(|error| IrohReplicationError::Endpoint(error.to_string()))?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| IrohReplicationError::Stream(error.to_string()))?;
        write_request(&mut send, &ReplicationRequest::FollowHandler { request }).await?;
        match read_frame(&mut receive).await? {
            ReplicationFrame::HandlerState { state } => Ok(IrohHandlerSubscription {
                connection,
                receive,
                current: decode_handler_state(*state)?,
                view_delta,
            }),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote application handler failed: {message}"
            ))),
            _ => Err(IrohReplicationError::Stream(
                "peer did not return initial application handler state".to_owned(),
            )),
        }
    }
}

impl<T, C> IrohHandlerSubscription<T, C>
where
    T: hyphae::CellValue + DeserializeOwned,
    C: hyphae::CellValue + DeserializeOwned,
{
    /// Returns the newest coherent handler lifecycle state.
    #[must_use]
    pub const fn current(&self) -> &LiveSubscriptionState<T, C> {
        &self.current
    }

    /// Receives the next handler lifecycle revision.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes, changes type, or cannot decode.
    pub async fn recv(&mut self) -> Result<LiveSubscriptionState<T, C>, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::HandlerState { state } => {
                self.current = decode_handler_state(*state)?;
                Ok(self.current.clone())
            }
            ReplicationFrame::HandlerViewDelta { delta } => {
                let apply = self.view_delta.ok_or_else(|| {
                    IrohReplicationError::Stream(
                        "peer sent a keyed view delta to a snapshot handler".to_owned(),
                    )
                })?;
                let through = delta
                    .through
                    .clone()
                    .map(serde_json::from_value)
                    .transpose()?;
                if let Some(value) = self.current.value.as_mut() {
                    apply(value, &delta)?;
                } else if delta.order.as_ref().is_some_and(|order| !order.is_empty()) {
                    return Err(IrohReplicationError::Stream(
                        "keyed view delta arrived before its initial snapshot".to_owned(),
                    ));
                }
                self.current.through = through;
                self.current.liveness = delta.liveness.clone();
                Ok(self.current.clone())
            }
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote application handler failed: {message}"
            ))),
            _ => Err(IrohReplicationError::Stream(
                "peer changed application handler stream type".to_owned(),
            )),
        }
    }

    /// Closes this handler stream without stopping either node.
    pub fn close(self) {
        self.connection.close(0u32.into(), b"handler follow closed");
    }
}

fn apply_view_delta<V>(
    items: &mut Vec<V::Item>,
    delta: &ErasedViewDelta,
) -> Result<(), IrohReplicationError>
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
        let item = serde_json::from_value::<V::Item>(encoded.clone())?;
        by_key.insert(V::item_key(&item), item);
    }
    if let Some(order) = &delta.order {
        let mut ordered = Vec::with_capacity(order.len());
        for key in order {
            let item = by_key.remove(key.as_str()).ok_or_else(|| {
                IrohReplicationError::Stream(format!("keyed view delta omitted row {key:?}"))
            })?;
            ordered.push(item);
        }
        if !by_key.is_empty() {
            return Err(IrohReplicationError::Stream(
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
            return Err(IrohReplicationError::Stream(
                "keyed view delta inserted rows without an ordering revision".to_owned(),
            ));
        }
        *items = retained;
    }
    Ok(())
}

fn decode_handler_state<T, C>(
    state: ErasedHandlerState,
) -> Result<LiveSubscriptionState<T, C>, IrohReplicationError>
where
    T: DeserializeOwned,
    C: DeserializeOwned,
{
    Ok(LiveSubscriptionState {
        value: state.value.map(serde_json::from_value).transpose()?,
        through: state.through.map(serde_json::from_value).transpose()?,
        liveness: state.liveness,
    })
}

fn drive_reactive_handler_subscription<T, C>(
    client: IrohApplicationClient,
    request: HandlerRequest,
    mut subscription: IrohHandlerSubscription<T, C>,
) -> IrohReactiveHandlerSubscription<T, C>
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
                match client.watch_with_delta(request.clone(), view_delta).await {
                    Ok(next) => {
                        task_writer.replace(next.current().clone());
                        subscription = next;
                        break;
                    }
                    Err(error) if reactive_item_error_is_recoverable(&error) => {
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
    IrohReactiveHandlerSubscription { live, writer, task }
}

fn drive_reactive_view_subscription<V>(
    client: IrohApplicationClient,
    request: HandlerRequest,
    mut subscription: IrohHandlerSubscription<Vec<V::Item>, V::Cursor>,
) -> IrohReactiveViewSubscription<V::Item, V::Cursor>
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
                    publish_iroh_view_state::<V>(&task_writer, state);
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
                match client
                    .watch_with_delta(request.clone(), Some(apply_view_delta::<V>))
                    .await
                {
                    Ok(next) => {
                        publish_iroh_view_state::<V>(&task_writer, next.current().clone());
                        subscription = next;
                        break;
                    }
                    Err(error) if reactive_item_error_is_recoverable(&error) => {
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
    IrohReactiveViewSubscription { live, writer, task }
}

fn publish_iroh_view_state<V>(
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

impl CommandStateClient for IrohCommandClient {
    type Error = IrohReplicationError;

    fn command_state_page(
        &self,
        request: CommandStateRequest,
    ) -> CommandStatePageFuture<'_, Self::Error> {
        Box::pin(
            self.replicator
                .command_state_page_remote(self.peer.clone(), request),
        )
    }
}

impl IrohCommandClient {
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
    ) -> Result<(RemoteCommandResponse, IrohCommandSubscription), IrohReplicationError> {
        IrohCommandSubscription::connect(&self.replicator, self.peer.clone(), command_id).await
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
        IrohCommandStateSubscription::connect(&self.replicator, self.peer.clone(), stream).await
    }
}

impl IrohCommandSubscription {
    async fn connect(
        replicator: &IrohReplicator,
        peer: EndpointAddr,
        command_id: CommandId,
    ) -> Result<(RemoteCommandResponse, Self), IrohReplicationError> {
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
        write_request(&mut send, &ReplicationRequest::WatchCommand { command_id }).await?;
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
        write_request(
            &mut send,
            &ReplicationRequest::WatchCommands {
                request: stream.request().clone(),
            },
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
        Box::pin(
            self.replicator
                .item_state_page_remote(self.peer.clone(), request),
        )
    }
}

impl IrohItemClient {
    /// Overrides reconnect timing for subsequently created reactive item streams.
    #[must_use]
    pub const fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
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
    ) -> Result<IrohReactiveItemSubscription<Q::Output>, IrohReplicationError>
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
    ) -> Result<IrohReactiveItemSubscription<Q::Output>, IrohReplicationError>
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

    async fn watch_reactive_request<Q>(
        &self,
        request: ItemStateRequest,
        query: Q,
    ) -> Result<IrohReactiveItemSubscription<Q::Output>, IrohReplicationError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, IrohItemQuerySubscription<Q>), IrohReplicationError>
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, IrohItemQuerySubscription<Q>), IrohReplicationError>
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, IrohItemQuerySubscription<Q>), IrohReplicationError>
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
    ) -> Result<(ItemQuerySnapshot<Q::Output>, IrohItemQuerySubscription<Q>), IrohReplicationError>
    where
        Q: ItemQuery,
    {
        let (initial, stream) = ItemQueryStream::from_snapshot(snapshot, query)
            .map_err(IrohReplicationError::Ingest)?;
        let subscription =
            IrohItemQuerySubscription::connect(&self.replicator, self.peer.clone(), stream).await?;
        Ok((initial, subscription))
    }
}

fn drive_reactive_item_subscription<Q>(
    client: IrohItemClient,
    request: ItemStateRequest,
    query: Q,
    initial: ItemQuerySnapshot<Q::Output>,
    mut subscription: IrohItemQuerySubscription<Q>,
) -> IrohReactiveItemSubscription<Q::Output>
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
        write_request(
            &mut send,
            &ReplicationRequest::FollowItems {
                request: stream.request().clone(),
            },
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
    pub fn current(&self) -> Q::Output {
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
    pub async fn recv(&mut self) -> Result<ItemQueryUpdate<Q::Output>, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::ItemUpdate { update } => self
                .stream
                .apply(&update)
                .map_err(IrohReplicationError::Ingest),
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

struct CursorPersistence {
    key: ReplicationCursorKey,
    store: Arc<dyn ReplicationCursorStore>,
}

#[derive(Clone)]
enum FollowSelection {
    All,
    Scope(ScopeId),
}

struct FollowCursorState {
    expected_source_node: Option<NodeId>,
    source_node: Option<NodeId>,
    cursor: Option<LogPosition>,
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
            },
            retry_interval,
            persistence,
            selection,
        }
    }
}

async fn write_request(
    send: &mut SendStream,
    request: &ReplicationRequest,
) -> Result<(), IrohReplicationError> {
    write_request_envelope(send, &NodeRequestEnvelope::connected(request.clone())).await
}

async fn write_request_envelope(
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

async fn read_frame(receive: &mut RecvStream) -> Result<ReplicationFrame, IrohReplicationError> {
    read_frame_optional(receive).await?.ok_or_else(|| {
        IrohReplicationError::Stream("peer closed before sending a response frame".to_owned())
    })
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

async fn read_command_frame(
    receive: &mut RecvStream,
    command_id: CommandId,
) -> Result<RemoteCommandResponse, IrohReplicationError> {
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

impl IrohReplicator {
    /// Binds the registered application-handler facade to one peer address.
    #[must_use]
    pub fn application_client(&self, peer: EndpointAddr) -> IrohApplicationClient {
        IrohApplicationClient {
            replicator: self.clone(),
            peer,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    /// Binds the transport-neutral command client facade to one authenticated
    /// peer address.
    #[must_use]
    pub fn command_client(&self, peer: EndpointAddr) -> IrohCommandClient {
        IrohCommandClient {
            replicator: self.clone(),
            peer,
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
        }
    }

    /// Binds a new Iroh endpoint with a generated node key.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind(node: Node) -> Result<Self, IrohReplicationError> {
        Self::bind_with_policy(node, Arc::new(AllowAllAccessPolicy)).await
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
        application: ApplicationNode,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_application_with_policy(application, Arc::new(AllowAllAccessPolicy)).await
    }

    /// Binds a network endpoint that serves both node and registered
    /// application protocols.
    ///
    /// # Errors
    ///
    /// Returns an error if the Iroh endpoint cannot bind.
    pub async fn bind_application_with_policy(
        application: ApplicationNode,
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
        Self::bind_loopback_with_policy(node, Arc::new(AllowAllAccessPolicy)).await
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
        application: ApplicationNode,
    ) -> Result<Self, IrohReplicationError> {
        Self::bind_loopback_application_with_policy(application, Arc::new(AllowAllAccessPolicy))
            .await
    }

    /// Binds a loopback endpoint that serves registered application handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if the loopback endpoint cannot bind.
    pub async fn bind_loopback_application_with_policy(
        application: ApplicationNode,
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
        Self::bind_loopback_with_secret_and_policy(node, secret_key, Arc::new(AllowAllAccessPolicy))
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
        application: ApplicationNode,
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
        Self::bind_with_secret_and_policy(node, secret_key, Arc::new(AllowAllAccessPolicy)).await
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
        application: ApplicationNode,
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
        application: ApplicationNode,
        endpoint: Endpoint,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let node = application.node().clone();
        Self::from_endpoint_inner(node, Some(application), endpoint, initial_policy)
    }

    fn from_endpoint_inner(
        node: Node,
        application: Option<ApplicationNode>,
        endpoint: Endpoint,
        initial_policy: Arc<dyn AccessPolicy>,
    ) -> Self {
        let sessions = match application {
            Some(application) => NodeSessionService::for_application(application, initial_policy),
            None => NodeSessionService::new(node.clone(), initial_policy),
        };
        let pairing = pairing::PairingRegistry::new();
        let protocol = ReplicationProtocol {
            sessions: sessions.clone(),
        };
        let router = Router::builder(endpoint)
            .accept(MYKO_REPLICATION_ALPN, protocol)
            .accept(
                MYKO_PAIRING_ALPN,
                pairing::PairingProtocol::new(pairing.clone()),
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
    /// Local sockets and compatibility gateways should share this value so
    /// every transport observes identical handlers, policy, and live events.
    #[must_use]
    pub const fn sessions(&self) -> &NodeSessionService {
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
    /// default policy allows every authenticated Iroh peer.
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
        write_request(&mut send, &ReplicationRequest::FollowLive { topics }).await?;
        let source_node = match read_frame(&mut receive).await? {
            ReplicationFrame::Hello { source_node } => source_node,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote live subscription failed: {message}"
                )));
            }
            ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
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
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
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
        if checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.scope_id != scope_id)
        {
            return Err(IrohReplicationError::Cursor(
                "scoped checkpoint belongs to another scope".to_owned(),
            ));
        }
        let after = checkpoint.as_ref().and_then(|value| value.position);
        let mut batch = self
            .fetch_scoped_batch(peer.clone(), scope_id.clone(), after)
            .await?;
        if checkpoint
            .as_ref()
            .is_some_and(|value| value.source_node != batch.source_node)
        {
            batch = self.fetch_scoped_batch(peer, scope_id, None).await?;
        }
        self.node
            .ingest_scoped_batch(batch)
            .map_err(IrohReplicationError::Ingest)
    }

    async fn fetch_scoped_batch(
        &self,
        peer: EndpointAddr,
        scope_id: ScopeId,
        after: Option<LogPosition>,
    ) -> Result<ScopedReplicationBatch, IrohReplicationError> {
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
    ) -> Result<RemoteCommandResponse, IrohReplicationError> {
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
        write_request(&mut send, &request).await?;
        let response = match read_frame(&mut receive).await? {
            ReplicationFrame::Command { response } => *response,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote command failed: {message}"
                )));
            }
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Live { .. } => {
                return Err(IrohReplicationError::Stream(
                    "peer sent a replication frame for a command request".to_owned(),
                ));
            }
        };
        connection.close(0u32.into(), b"command request complete");
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
        write_request(&mut send, &ReplicationRequest::CommandState { request }).await?;
        let page = match read_frame(&mut receive).await? {
            ReplicationFrame::CommandState { page } => *page,
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
        write_request(&mut send, &ReplicationRequest::ItemState { request }).await?;
        let page = match read_frame(&mut receive).await? {
            ReplicationFrame::ItemState { page } => *page,
            ReplicationFrame::Error { message } => {
                return Err(IrohReplicationError::Stream(format!(
                    "remote item query failed: {message}"
                )));
            }
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
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
    async fn submit_remote(
        &self,
        peer: EndpointAddr,
        command: CommandSubmission,
    ) -> Result<RemoteCommandResponse, IrohReplicationError> {
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
    ) -> Result<RemoteCommandResponse, IrohReplicationError> {
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
    ) -> Result<RemoteCommandResponse, IrohReplicationError> {
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
        let key = ReplicationCursorKey::new("iroh", peer.id.to_string());
        let checkpoint = store
            .load_checkpoint(&key)
            .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
        Ok(self.spawn_follower(
            peer,
            FollowerConfig::new(
                None,
                checkpoint.map(|checkpoint| checkpoint.source_node),
                checkpoint.and_then(|checkpoint| checkpoint.position),
                retry_interval,
                Some(CursorPersistence { key, store }),
                FollowSelection::All,
            ),
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
        let key = ReplicationCursorKey::new("iroh", peer.id.to_string());
        let checkpoint = store
            .load_checkpoint(&key)
            .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
        let cursor = match checkpoint {
            Some(checkpoint) if checkpoint.source_node == expected_source_node => {
                checkpoint.position
            }
            Some(_) => {
                store
                    .save_checkpoint(&key, ReplicationCheckpoint::new(expected_source_node, None))
                    .map_err(|error| IrohReplicationError::Cursor(error.to_string()))?;
                None
            }
            None => None,
        };
        Ok(self.spawn_follower(
            peer,
            FollowerConfig::new(
                Some(expected_source_node),
                Some(expected_source_node),
                cursor,
                retry_interval,
                Some(CursorPersistence { key, store }),
                FollowSelection::All,
            ),
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
            persist_cursor(
                persistence,
                ReplicationCheckpoint::new(advertised_source, None),
            )?;
            connection.close(0u32.into(), b"source history changed");
            return Ok(());
        }
        cursor.source_node = Some(advertised_source);
        persist_cursor(
            persistence,
            ReplicationCheckpoint::new(advertised_source, cursor.cursor),
        )?;
        loop {
            let frame = read_frame(&mut receive).await?;
            let through =
                self.ingest_follow_frame(selection, frame, advertised_source, cursor.cursor)?;
            persist_cursor(
                persistence,
                ReplicationCheckpoint::new(advertised_source, through),
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
        cursor: Option<LogPosition>,
    ) -> Result<Option<LogPosition>, IrohReplicationError> {
        match (selection, frame) {
            (FollowSelection::All, ReplicationFrame::Batch { batch }) => {
                if batch.after != cursor || batch.source_node != source_node {
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
                if batch.after != cursor
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
    sessions: NodeSessionService,
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

#[cfg(test)]
mod tests {
    use super::*;
    use myko_app::capability::{EventPublishing as _, Querying as _};
    use myko_app::{
        AppError, CommandClient as _, CommandContext, CommandError, CommandHandler,
        MykoApplication, ReportContext, ReportHandler, myko_report,
    };
    use myko_federation::{
        AccessOperation, AccessRequest, BatchId, ChangeBatch, CommandId, CommandRequest,
        CommandState, ItemMutation, MykoService as _, PrincipalId, ScopeId, ServiceId,
    };
    use myko_items::{myko_command, myko_item, myko_service};
    use myko_redb::RedbJournal;

    #[myko_service(RemoteRecord)]
    pub struct RemoteService;

    #[myko_item(service = RemoteService, scope_root)]
    pub struct RemoteRecord {
        pub value: String,
    }

    #[myko_command(bool, item = RemoteRecord)]
    struct SetRemoteRecord {
        id: RemoteRecordId,
        scope: String,
        value: String,
    }

    impl CommandHandler for SetRemoteRecord {
        fn scope(&self, _node_id: NodeId) -> RemoteRecordId {
            RemoteRecordId::from(self.scope.clone())
        }

        fn execute(
            self,
            context: CommandContext<RemoteService, RemoteRecord>,
        ) -> Result<bool, CommandError> {
            context.emit_set(&RemoteRecord {
                id: self.id,
                value: self.value,
            })?;
            Ok(true)
        }
    }

    #[myko_report(u64, item = RemoteRecord)]
    #[derive(Copy)]
    struct RemoteRecordCount {
        source_node: NodeId,
    }

    impl ReportHandler for RemoteRecordCount {
        type Output = u64;
        type Cursor = LogPosition;

        fn build(
            &self,
            context: &ReportContext,
        ) -> Result<LiveSubscription<Self::Output>, AppError> {
            Ok(context
                .query(
                    self.source_node,
                    ScopeId::new("application-handler"),
                    GetAllRemoteRecords,
                )?
                .map_value(|records| u64::try_from(records.len()).unwrap_or(u64::MAX)))
        }
    }

    #[tokio::test]
    async fn registered_report_streams_over_authenticated_iroh() -> Result<(), String> {
        let source = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<RemoteService>()
            .map_err(|error| error.to_string())?
            .build();
        let server = IrohReplicator::bind_loopback_application_with_policy(
            ApplicationNode::new(source.clone(), application),
            Arc::new(AllowAllAccessPolicy),
        )
        .await
        .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let mut report = client
            .application_client(server.address())
            .watch_report(&RemoteRecordCount {
                source_node: source.node_id(),
            })
            .await
            .map_err(|error| error.to_string())?;
        if report.current().value != Some(0) {
            return Err("remote report omitted its initial value".to_owned());
        }
        let _record = commit_remote_record(
            &source,
            ScopeId::new("application-handler"),
            "record",
            "live",
        )?;
        let updated = tokio::time::timeout(Duration::from_secs(5), report.recv())
            .await
            .map_err(|_| "remote report did not update".to_owned())?
            .map_err(|error| error.to_string())?;
        if updated.value != Some(1) || updated.liveness != SubscriptionLiveness::Current {
            return Err("remote report returned the wrong lifecycle state".to_owned());
        }
        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let revoked = tokio::time::timeout(Duration::from_secs(5), report.recv())
            .await
            .map_err(|_| "remote report did not observe policy revocation".to_owned())?;
        if revoked.is_ok() {
            return Err("revoked report stream remained authorized".to_owned());
        }
        report.close();
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[derive(Debug)]
    struct ReadOnlyScopePolicy {
        scope_id: ScopeId,
    }

    impl AccessPolicy for ReadOnlyScopePolicy {
        fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
            let is_read = matches!(
                request.operation,
                AccessOperation::ReadHistory
                    | AccessOperation::ReadItems
                    | AccessOperation::FollowItems
                    | AccessOperation::FollowHistory
                    | AccessOperation::ReadCommand
                    | AccessOperation::ReadCommands
                    | AccessOperation::WatchCommand
                    | AccessOperation::WatchCommands
            );
            if is_read && request.scope_id.as_ref() == Some(&self.scope_id) {
                Ok(())
            } else {
                Err("peer has read-only access to one scope".to_owned())
            }
        }
    }

    #[derive(Debug)]
    struct ReadScopeSetPolicy {
        scope_ids: Vec<ScopeId>,
    }

    impl AccessPolicy for ReadScopeSetPolicy {
        fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
            let permitted = request.operation == AccessOperation::ReadHistory
                && request
                    .scope_id
                    .as_ref()
                    .is_some_and(|scope_id| self.scope_ids.contains(scope_id));
            if permitted {
                Ok(())
            } else {
                Err("peer cannot read this scope".to_owned())
            }
        }
    }

    #[derive(Debug)]
    struct DenyAllPolicy;

    impl AccessPolicy for DenyAllPolicy {
        fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
            Err("test policy revoked access".to_owned())
        }
    }

    fn commit_test_command(node: &Node, command_type: &str) -> Result<CommandRequest, String> {
        commit_test_command_in_scope(node, command_type, ScopeId::new("durable-cursor"))
    }

    fn commit_test_command_in_scope(
        node: &Node,
        command_type: &str,
        scope_id: ScopeId,
    ) -> Result<CommandRequest, String> {
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("test"),
            scope_id,
            principal_id: PrincipalId::new("node:test"),
            command_type: command_type.to_owned(),
            payload: Vec::new(),
        };
        let admission = node
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                causal_parents: vec![admission.snapshot().updated_at],
                changes: Vec::new(),
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;
        Ok(request)
    }

    fn commit_remote_record(
        node: &Node,
        scope_id: ScopeId,
        id: &str,
        value: &str,
    ) -> Result<RemoteRecord, String> {
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(RemoteService::SERVICE_ID),
            scope_id,
            principal_id: PrincipalId::new("node:test"),
            command_type: "records.put".to_owned(),
            payload: Vec::new(),
        };
        let admission = node
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        let record = RemoteRecord {
            id: RemoteRecordId::from(id),
            value: value.to_owned(),
        };
        node.commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![ItemMutation::set(&record).map_err(|error| error.to_string())?],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;
        Ok(record)
    }

    async fn wait_for_committed(node: &Node, command_id: CommandId) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if node
                    .command(command_id)
                    .map_err(|error| error.to_string())?
                    .is_some_and(|command| command.state.is_committed())
                {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "peer follower did not deliver a committed command".to_owned())?
    }

    async fn wait_for_cursor(follower: &PeerSync) -> Result<LogPosition, String> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(cursor) = follower.status().map_err(|error| error.to_string())?.cursor {
                    return Ok::<LogPosition, String>(cursor);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "peer follower did not persist a cursor".to_owned())?
    }

    async fn wait_for_connection(follower: &PeerSync) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if follower
                    .status()
                    .map_err(|error| error.to_string())?
                    .connected
                {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "peer follower did not establish its live stream".to_owned())?
    }

    #[tokio::test]
    async fn remote_item_client_executes_the_same_typed_query_contract() -> Result<(), String> {
        let source = Node::in_memory();
        let service_id = ServiceId::new(RemoteService::SERVICE_ID);
        let scope_id = ScopeId::new("session:records");
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("node:test"),
            command_type: "records.put".to_owned(),
            payload: Vec::new(),
        };
        let admission = source
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        let record = RemoteRecord {
            id: RemoteRecordId::from("record-1"),
            value: "remote".to_owned(),
        };
        let second = RemoteRecord {
            id: RemoteRecordId::from("record-2"),
            value: "second page".to_owned(),
        };
        source
            .commit(
                request.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: request.id,
                    service_id: service_id.clone(),
                    scope_id: scope_id.clone(),
                    causal_parents: vec![admission.snapshot().updated_at],
                    changes: vec![
                        ItemMutation::set(&record).map_err(|error| error.to_string())?,
                        ItemMutation::set(&second).map_err(|error| error.to_string())?,
                    ],
                },
                Vec::new(),
            )
            .map_err(|error| error.to_string())?;

        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let state = client
            .item_client(server.address())
            .item_state(
                ItemStateRequest::for_serving_item::<RemoteRecord>(scope_id).with_page_size(1),
            )
            .await
            .map_err(|error| error.to_string())?;
        let snapshot = state
            .query(GetAllRemoteRecords)
            .map_err(|error| error.to_string())?;
        if snapshot.value != [record, second] || snapshot.through.is_none() {
            return Err("remote typed item query returned the wrong state".to_owned());
        }
        let typed = client
            .item_client(server.address())
            .query_serving_items(ScopeId::new("session:records"), GetAllRemoteRecords)
            .await
            .map_err(|error| error.to_string())?;
        if typed.value != snapshot.value || typed.through != snapshot.through {
            return Err("typed item client facade diverged from its collected state".to_owned());
        }
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn every_item_state_page_is_authorized_independently() -> Result<(), String> {
        let source = Node::in_memory();
        let service_id = ServiceId::new(RemoteService::SERVICE_ID);
        let scope_id = ScopeId::new("session:paged-policy");
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: service_id.clone(),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("node:test"),
            command_type: "records.put".to_owned(),
            payload: Vec::new(),
        };
        let admission = source
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        let first = RemoteRecord {
            id: RemoteRecordId::from("record-1"),
            value: "first".to_owned(),
        };
        let second = RemoteRecord {
            id: RemoteRecordId::from("record-2"),
            value: "second".to_owned(),
        };
        source
            .commit(
                request.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: request.id,
                    service_id: service_id.clone(),
                    scope_id: scope_id.clone(),
                    causal_parents: vec![admission.snapshot().updated_at],
                    changes: vec![
                        ItemMutation::set(&first).map_err(|error| error.to_string())?,
                        ItemMutation::set(&second).map_err(|error| error.to_string())?,
                    ],
                },
                Vec::new(),
            )
            .map_err(|error| error.to_string())?;

        let server = IrohReplicator::bind_loopback(source)
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: scope_id.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let item_client = client.item_client(server.address());
        let first_page = item_client
            .item_state_page(
                ItemStateRequest::for_serving_item::<RemoteRecord>(scope_id).with_page_size(1),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut continuation = first_page.request;
        continuation.after_item_id = first_page.next_after_item_id;
        if continuation.after_item_id.is_none() {
            return Err("first item-state page did not expose a continuation".to_owned());
        }

        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        if item_client.item_state_page(continuation).await.is_ok() {
            return Err("policy revocation did not deny the next item-state page".to_owned());
        }
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn native_typed_item_stream_filters_history_and_observes_revocation() -> Result<(), String>
    {
        let source = Node::in_memory();
        let scope_id = ScopeId::new("session:typed-stream");
        let initial = commit_remote_record(&source, scope_id.clone(), "record-1", "initial")?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: scope_id.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let (snapshot, mut subscription) = client
            .item_client(server.address())
            .watch_serving_items(scope_id.clone(), GetAllRemoteRecords)
            .await
            .map_err(|error| error.to_string())?;
        if snapshot.value != [initial.clone()]
            || subscription.current() != snapshot.value
            || subscription.source_node() != source.node_id()
        {
            return Err("typed item stream did not retain its initial snapshot".to_owned());
        }

        let _hidden = commit_remote_record(
            &source,
            ScopeId::new("session:hidden-stream"),
            "record-hidden",
            "must not cross the stream",
        )?;
        let second = commit_remote_record(&source, scope_id.clone(), "record-2", "live")?;
        let update = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "typed item stream did not deliver a matching commit".to_owned())?
            .map_err(|error| error.to_string())?;
        if update.value != [initial, second] || subscription.current() != update.value {
            return Err("typed item stream exposed unrelated or incomplete state".to_owned());
        }

        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let revoked = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "typed item stream did not observe policy revocation".to_owned())?;
        if !matches!(revoked, Err(ref error) if error.to_string().contains("access denied")) {
            return Err(format!(
                "typed item stream returned the wrong revocation result: {revoked:?}"
            ));
        }
        subscription.close();
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn native_typed_item_stream_drives_hyphae_lifecycle_state() -> Result<(), String> {
        use hyphae::{Signal, Watchable as _};

        let source = Node::in_memory();
        let scope_id = ScopeId::new("session:reactive-stream");
        let initial = commit_remote_record(&source, scope_id.clone(), "record-1", "initial")?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: scope_id.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let reactive = client
            .item_client(server.address())
            .watch_serving_items_reactive(scope_id.clone(), GetAllRemoteRecords)
            .await
            .map_err(|error| error.to_string())?;
        let (updates_tx, updates_rx) = flume::unbounded();
        let _guard = reactive.live().state().subscribe(move |signal| {
            if let Signal::Value(state) = signal {
                let _ignored = updates_tx.send(state.clone());
            }
        });
        let _initial_notification = updates_rx.try_recv();

        let second = commit_remote_record(&source, scope_id.clone(), "record-2", "live")?;
        let update = tokio::time::timeout(Duration::from_secs(10), updates_rx.recv_async())
            .await
            .map_err(|_| "reactive native item stream did not update".to_owned())?
            .map_err(|error| error.to_string())?;
        if update.value != Some(vec![initial.clone(), second.clone()])
            || update.liveness != SubscriptionLiveness::Current
        {
            return Err(format!("unexpected reactive native state: {update:?}"));
        }

        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let resynchronizing = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let update = updates_rx
                    .recv_async()
                    .await
                    .map_err(|error| error.to_string())?;
                if matches!(
                    update.liveness,
                    SubscriptionLiveness::Resynchronizing { .. }
                ) {
                    return Ok::<_, String>(update);
                }
            }
        })
        .await
        .map_err(|_| "reactive native item stream did not expose revocation".to_owned())??;
        if !matches!(
            resynchronizing.liveness,
            SubscriptionLiveness::Resynchronizing { ref reason }
                if reason.contains("access denied")
        ) {
            return Err(format!(
                "unexpected resynchronizing state: {resynchronizing:?}"
            ));
        }

        let third = commit_remote_record(&source, scope_id.clone(), "record-3", "while-offline")?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy { scope_id }))
            .map_err(|error| error.to_string())?;
        let recovered = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let update = updates_rx
                    .recv_async()
                    .await
                    .map_err(|error| error.to_string())?;
                if update.liveness == SubscriptionLiveness::Current
                    && update.value == Some(vec![initial.clone(), second.clone(), third.clone()])
                {
                    return Ok::<_, String>(update);
                }
            }
        })
        .await
        .map_err(|_| "reactive native item stream did not recover after regrant".to_owned())??;
        if recovered.through.is_none() {
            return Err("recovered reactive native state omitted its cursor".to_owned());
        }

        drop(reactive);
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    async fn assert_remote_cancellation(
        client: &IrohReplicator,
        server: &IrohReplicator,
        command_id: CommandId,
    ) -> Result<(), String> {
        let cancelled = client
            .cancel_remote(
                server.address(),
                command_id,
                "operator interrupted".to_owned(),
            )
            .await
            .map_err(|error| error.to_string())?;
        if !cancelled.command.as_ref().is_some_and(|command| {
            matches!(
                command.state,
                CommandState::Cancelled { ref reason } if reason == "operator interrupted"
            )
        }) {
            return Err(format!(
                "native cancellation did not become durable: {cancelled:?}"
            ));
        }
        let repeated = client
            .cancel_remote(
                server.address(),
                command_id,
                "different retry text".to_owned(),
            )
            .await
            .map_err(|error| error.to_string())?;
        if repeated.command != cancelled.command {
            return Err("native cancellation retry changed terminal state".to_owned());
        }
        Ok(())
    }

    #[tokio::test]
    async fn two_iroh_endpoints_exchange_immutable_history() -> Result<(), String> {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("test"),
            scope_id: ScopeId::new("test"),
            principal_id: PrincipalId::new("node:test"),
            command_type: "test".to_owned(),
            payload: Vec::new(),
        };
        let admission = source
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        source
            .commit(
                request.id,
                ChangeBatch {
                    id: BatchId::new(),
                    command_id: request.id,
                    service_id: request.service_id,
                    scope_id: request.scope_id,
                    causal_parents: vec![admission.snapshot().updated_at],
                    changes: Vec::<ItemMutation>::new(),
                },
                Vec::new(),
            )
            .map_err(|error| error.to_string())?;

        let source_transport = IrohReplicator::bind_loopback(source)
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let report = target_transport
            .pull(source_transport.address(), None)
            .await
            .map_err(|error| error.to_string())?;
        if report.applied != 2 {
            return Err(format!("unexpected Iroh replication report: {report:?}"));
        }
        let command = target
            .command(request.id)
            .map_err(|error| error.to_string())?;
        if !command.is_some_and(|command| command.state.is_committed()) {
            return Err("target did not ingest the committed command".to_owned());
        }
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn native_descriptor_verifies_transport_and_myko_identities() -> Result<(), String> {
        let source = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        source
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let descriptor = source.descriptor();
        let encoded = serde_json::to_vec(&descriptor).map_err(|error| error.to_string())?;
        let decoded: NativePeerReference =
            serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
        if decoded.descriptor() != Some(&descriptor) {
            return Err("native descriptor did not decode as a pinned peer".to_owned());
        }
        let legacy_encoded =
            serde_json::to_vec(&descriptor.endpoint).map_err(|error| error.to_string())?;
        let legacy: NativePeerReference =
            serde_json::from_slice(&legacy_encoded).map_err(|error| error.to_string())?;
        if legacy.descriptor().is_some() || legacy.endpoint() != &descriptor.endpoint {
            return Err("legacy endpoint did not decode as an unpinned peer".to_owned());
        }
        client
            .verify_descriptor(&descriptor)
            .await
            .map_err(|error| error.to_string())?;

        let wrong = NativeNodeDescriptor::new(NodeId::new(), descriptor.endpoint.clone());
        let error = match client.verify_descriptor(&wrong).await {
            Ok(()) => return Err("descriptor with another Myko identity was accepted".to_owned()),
            Err(error) => error,
        };
        if !error.to_string().contains("advertised Myko source") {
            return Err(format!(
                "descriptor mismatch returned the wrong error: {error}"
            ));
        }

        client.shutdown().await.map_err(|error| error.to_string())?;
        source.shutdown().await.map_err(|error| error.to_string())
    }

    fn assert_pairing_ttl_bounds(server: &IrohReplicator) -> Result<(), String> {
        let excessive = Duration::from_hours(24)
            .checked_add(Duration::from_millis(1))
            .ok_or_else(|| "test pairing TTL overflowed".to_owned())?;
        for invalid_ttl in [Duration::ZERO, Duration::from_nanos(1), excessive] {
            if server.issue_pairing_invitation(invalid_ttl).is_ok() {
                return Err(format!(
                    "invalid pairing invitation TTL was accepted: {invalid_ttl:?}"
                ));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn pairing_is_identity_bound_one_use_expiring_and_operator_visible() -> Result<(), String>
    {
        let server = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let mut receipts = server.subscribe_pairing_receipts();
        assert_pairing_ttl_bounds(&server)?;
        let invitation = server
            .issue_pairing_invitation(Duration::from_mins(1))
            .map_err(|error| error.to_string())?;
        let encoded = serde_json::to_vec(&invitation).map_err(|error| error.to_string())?;
        let mut encoded_value: serde_json::Value =
            serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
        let bearer = encoded_value
            .get("secret_hex")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "pairing invitation omitted its encoded bearer".to_owned())?
            .to_owned();
        let debug = format!("{invitation:?}");
        if debug.contains(&bearer) || !debug.contains("[redacted]") {
            return Err("pairing invitation debug output exposed its bearer".to_owned());
        }

        let secret = encoded_value
            .as_object_mut()
            .and_then(|object| object.get_mut("secret_hex"))
            .ok_or_else(|| "pairing invitation omitted its encoded bearer".to_owned())?;
        *secret = serde_json::Value::String("01".repeat(32));
        let tampered: PairingInvitation =
            serde_json::from_value(encoded_value).map_err(|error| error.to_string())?;
        let error = client
            .redeem_pairing(&tampered)
            .await
            .err()
            .ok_or_else(|| "tampered pairing bearer was accepted".to_owned())?;
        if !error.to_string().contains("proof did not verify") {
            return Err(format!("tampered pairing returned wrong error: {error}"));
        }

        let forged_client =
            NativeNodeDescriptor::new(client.node.node_id(), server.descriptor().endpoint);
        let mismatch =
            pairing::redeem_pairing(client.router.endpoint(), forged_client, &invitation)
                .await
                .err()
                .ok_or_else(|| "pairing accepted a descriptor for another endpoint".to_owned())?;
        if !mismatch
            .to_string()
            .contains("does not match authenticated endpoint")
        {
            return Err(format!(
                "identity-mismatched pairing returned wrong error: {mismatch}"
            ));
        }

        let receipt = client
            .redeem_pairing(&invitation)
            .await
            .map_err(|error| error.to_string())?;
        if receipt.server != server.descriptor()
            || receipt.client != client.descriptor()
            || receipt.comparison_code.len() != 6
        {
            return Err(format!(
                "pairing receipt lost identity binding: {receipt:?}"
            ));
        }
        let observed = tokio::time::timeout(Duration::from_secs(5), receipts.recv())
            .await
            .map_err(|_| "server did not observe redeemed pairing".to_owned())?
            .map_err(|error| error.to_string())?;
        if observed != [receipt.clone()] {
            return Err(format!(
                "server observed wrong pairing receipt: {observed:?}"
            ));
        }
        let replay = client
            .redeem_pairing(&invitation)
            .await
            .err()
            .ok_or_else(|| "one-use pairing invitation was replayed".to_owned())?;
        if !replay.to_string().contains("already used") {
            return Err(format!("pairing replay returned wrong error: {replay}"));
        }

        let expired = server
            .issue_pairing_invitation(Duration::from_millis(1))
            .map_err(|error| error.to_string())?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let expiry = client
            .redeem_pairing(&expired)
            .await
            .err()
            .ok_or_else(|| "expired pairing invitation was accepted".to_owned())?;
        if !expiry.to_string().contains("expired") {
            return Err(format!("pairing expiry returned wrong error: {expiry}"));
        }

        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[test]
    fn persistent_secret_key_restores_the_same_transport_identity() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("client").join("iroh-secret.json");
        let first = load_or_create_secret_key(&path).map_err(|error| error.to_string())?;
        let second = load_or_create_secret_key(&path).map_err(|error| error.to_string())?;

        if first.public() != second.public() {
            return Err("persistent key changed its Iroh identity".to_owned());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let mode = std::fs::metadata(path)
                .map_err(|error| error.to_string())?
                .permissions()
                .mode()
                & 0o777;
            if mode != 0o600 {
                return Err(format!("persistent key permissions were {mode:o}"));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_scope_catalog_is_paginated_and_policy_filtered() -> Result<(), String> {
        let source = Node::in_memory();
        let first = ScopeId::new("session:alpha");
        let second = ScopeId::new("session:bravo");
        let secret = ScopeId::new("session:secret");
        commit_test_command_in_scope(&source, "first", first.clone())?;
        commit_test_command_in_scope(&source, "secret", secret)?;
        commit_test_command_in_scope(&source, "second", second.clone())?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadScopeSetPolicy {
                scope_ids: vec![first.clone(), second.clone()],
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;

        let first_page = client
            .list_scopes_remote(server.address(), None, NonZeroUsize::MIN)
            .await
            .map_err(|error| error.to_string())?;
        if first_page.source_node != source.node_id()
            || first_page.scopes != vec![first.clone()]
            || first_page.next_after != Some(first.clone())
        {
            return Err(format!("unexpected first catalog page: {first_page:?}"));
        }
        let second_page = client
            .list_scopes_remote(server.address(), first_page.next_after, NonZeroUsize::MIN)
            .await
            .map_err(|error| error.to_string())?;
        if second_page.source_node != source.node_id()
            || second_page.scopes != vec![second]
            || second_page.next_after.is_some()
        {
            return Err(format!("unexpected second catalog page: {second_page:?}"));
        }

        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn native_command_watch_is_gap_free_and_observes_revocation() -> Result<(), String> {
        let source = Node::in_memory();
        let scope_id = ScopeId::new("session:command-watch");
        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("test"),
            scope_id: scope_id.clone(),
            principal_id: PrincipalId::new("node:test"),
            command_type: "test.watch".to_owned(),
            payload: Vec::new(),
        };
        source
            .submit(request.clone())
            .map_err(|error| error.to_string())?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy { scope_id }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let (initial, mut subscription) = client
            .command_client(server.address())
            .watch_command(request.id)
            .await
            .map_err(|error| error.to_string())?;
        if !initial.command.is_some_and(|command| {
            command.request == request && command.state == CommandState::Submitted
        }) || subscription.current().state != CommandState::Submitted
        {
            return Err("native command watch returned the wrong initial state".to_owned());
        }

        source
            .claim(request.id)
            .map_err(|error| error.to_string())?;
        let executing = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "native command watch did not receive execution".to_owned())?
            .map_err(|error| error.to_string())?;
        if executing.state != CommandState::Executing {
            return Err(format!(
                "native command watch returned the wrong transition: {executing:?}"
            ));
        }

        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        if tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "revoked command watch did not close".to_owned())?
            .is_ok()
        {
            return Err("policy revocation did not close command watch".to_owned());
        }
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn native_scoped_pull_omits_unrelated_history_and_advances_cursor() -> Result<(), String>
    {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let wanted_scope = ScopeId::new("session:wanted");
        let wanted = commit_test_command_in_scope(&source, "wanted", wanted_scope.clone())?;
        let hidden =
            commit_test_command_in_scope(&source, "hidden", ScopeId::new("session:hidden"))?;
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;

        let first = target_transport
            .pull_scope(source_transport.address(), wanted_scope.clone(), None)
            .await
            .map_err(|error| error.to_string())?;
        if first.applied != 2
            || first.through != Some(LogPosition::new(4))
            || !target
                .command(wanted.id)
                .map_err(|error| error.to_string())?
                .is_some_and(|command| command.state.is_committed())
            || target
                .command(hidden.id)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err(format!(
                "scoped native pull leaked or lost history: {first:?}"
            ));
        }

        let hidden_later =
            commit_test_command_in_scope(&source, "hidden-later", ScopeId::new("session:hidden"))?;
        let second = target_transport
            .pull_scope(
                source_transport.address(),
                wanted_scope,
                Some(first.checkpoint()),
            )
            .await
            .map_err(|error| error.to_string())?;
        if second.applied != 0
            || second.through != Some(LogPosition::new(6))
            || target
                .command(hidden_later.id)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err(format!(
                "scoped cursor did not skip hidden history: {second:?}"
            ));
        }

        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn scoped_pull_resets_on_replacement_history_and_rejects_cross_scope_cursor()
    -> Result<(), String> {
        let first_source = Node::in_memory();
        let replacement_source = Node::in_memory();
        let target = Node::in_memory();
        let scope = ScopeId::new("session:source-aware");
        commit_test_command_in_scope(&first_source, "first-a", scope.clone())?;
        commit_test_command_in_scope(&first_source, "first-b", scope.clone())?;
        let replacement =
            commit_test_command_in_scope(&replacement_source, "replacement", scope.clone())?;
        let first_transport = IrohReplicator::bind_loopback(first_source)
            .await
            .map_err(|error| error.to_string())?;
        let replacement_transport = IrohReplicator::bind_loopback(replacement_source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;

        let first = target_transport
            .pull_scope(first_transport.address(), scope.clone(), None)
            .await
            .map_err(|error| error.to_string())?;
        if first.through != Some(LogPosition::new(4)) {
            return Err(format!("unexpected initial scoped cursor: {first:?}"));
        }
        let replacement_report = target_transport
            .pull_scope(
                replacement_transport.address(),
                scope.clone(),
                Some(first.checkpoint()),
            )
            .await
            .map_err(|error| error.to_string())?;
        if replacement_report.source_node != replacement_source.node_id()
            || replacement_report.through != Some(LogPosition::new(2))
            || replacement_report.applied != 2
            || !target
                .command(replacement.id)
                .map_err(|error| error.to_string())?
                .is_some_and(|command| command.state.is_committed())
        {
            return Err(format!(
                "replacement scope was not replayed from its beginning: {replacement_report:?}"
            ));
        }
        let wrong_scope = target_transport
            .pull_scope(
                replacement_transport.address(),
                ScopeId::new("session:other"),
                Some(replacement_report.checkpoint()),
            )
            .await;
        if !matches!(wrong_scope, Err(IrohReplicationError::Cursor(_))) {
            return Err(format!("cross-scope cursor was accepted: {wrong_scope:?}"));
        }

        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        replacement_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        first_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn scoped_follower_replays_then_tracks_only_one_scope() -> Result<(), String> {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let wanted_scope = ScopeId::new("session:wanted-follow");
        let hidden = commit_test_command_in_scope(
            &source,
            "hidden-before-follow",
            ScopeId::new("session:hidden-follow"),
        )?;
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let follower = target_transport
            .follow_scope(
                source_transport.address(),
                wanted_scope.clone(),
                None,
                Duration::from_millis(10),
            )
            .map_err(|error| error.to_string())?;
        wait_for_connection(&follower).await?;

        let wanted = commit_test_command_in_scope(&source, "wanted-live", wanted_scope)?;
        wait_for_committed(&target, wanted.id).await?;
        if target
            .command(hidden.id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("scoped follower imported hidden replay history".to_owned());
        }
        let status = follower.status().map_err(|error| error.to_string())?;
        if status.cursor != Some(LogPosition::new(4)) || status.successful_batches < 4 {
            return Err(format!(
                "scoped follower did not advance globally: {status:?}"
            ));
        }

        follower
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn embedded_command_client_uses_the_same_unclaimed_contract() -> Result<(), String> {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<RemoteService>()
            .map_err(|error| error.to_string())?
            .build();
        let application = ApplicationNode::new(node.clone(), application);
        let submitted = application
            .submit_command(SetRemoteRecord {
                id: RemoteRecordId::from("embedded-command"),
                scope: "session:test".to_owned(),
                value: "hello in process".to_owned(),
            })
            .await
            .map_err(|error| error.to_string())?;
        let Some(snapshot) = submitted.command.as_ref() else {
            return Err("embedded command returned no state".to_owned());
        };
        if submitted.source_node != node.node_id() || snapshot.state != CommandState::Submitted {
            return Err(format!("unexpected embedded response: {submitted:?}"));
        }
        let queried =
            myko_federation::CommandClient::command_state(&application, snapshot.request.id)
                .await
                .map_err(|error| error.to_string())?;
        if queried != submitted {
            return Err("embedded command facade changed the command projection".to_owned());
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_command_catalog_collects_authorized_pages() -> Result<(), String> {
        let source = Node::in_memory();
        let scope_id = ScopeId::new("session:catalog");
        let first = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
        let second = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: scope_id.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;

        let catalog = client
            .command_client(server.address())
            .command_states(CommandStateRequest {
                source_node: None,
                service_id: ServiceId::new("test"),
                scope_id,
                command_type: "prompt".to_owned(),
                snapshot_through: None,
                after_command_id: None,
                page_size: 1,
            })
            .await
            .map_err(|error| error.to_string())?;
        if catalog.serving_node != source.node_id()
            || catalog.commands.len() != 2
            || !catalog
                .commands
                .iter()
                .all(|entry| entry.command.state.is_committed())
        {
            return Err(format!("unexpected remote command catalog: {catalog:?}"));
        }
        let mut command_ids = catalog
            .commands
            .iter()
            .map(|entry| entry.command.request.id)
            .collect::<Vec<_>>();
        command_ids.sort_unstable_by_key(|id| id.as_uuid());
        let mut expected = vec![first.id, second.id];
        expected.sort_unstable_by_key(|id| id.as_uuid());
        if command_ids != expected {
            return Err("remote command catalog returned the wrong commands".to_owned());
        }

        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn native_command_catalog_watches_new_and_changed_commands() -> Result<(), String> {
        let source = Node::in_memory();
        let scope_id = ScopeId::new("session:catalog-follow");
        let first = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
        let server = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        server
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: scope_id.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let client = IrohReplicator::bind_loopback(Node::in_memory())
            .await
            .map_err(|error| error.to_string())?;
        let remote = client.command_client(server.address());
        let (initial, mut subscription) = remote
            .watch_commands(CommandStateRequest {
                source_node: None,
                service_id: ServiceId::new("test"),
                scope_id: scope_id.clone(),
                command_type: "prompt".to_owned(),
                snapshot_through: None,
                after_command_id: None,
                page_size: 1,
            })
            .await
            .map_err(|error| error.to_string())?;
        if initial
            .commands
            .first()
            .is_none_or(|entry| entry.command.request.id != first.id)
            || initial.commands.len() != 1
        {
            return Err("command watch returned the wrong initial catalog".to_owned());
        }
        let mut wrong_server = initial.clone();
        wrong_server.serving_node = Node::in_memory().node_id();
        match remote.watch_command_states(&wrong_server).await {
            Err(IrohReplicationError::Stream(message))
                if message.contains("another serving node") => {}
            Err(error) => {
                return Err(format!(
                    "command watch returned the wrong serving-node error: {error}"
                ));
            }
            Ok(unexpected) => {
                unexpected.close();
                return Err("command watch accepted another server's cursor".to_owned());
            }
        }

        let second = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("test"),
            scope_id,
            principal_id: PrincipalId::new("node:test"),
            command_type: "prompt".to_owned(),
            payload: Vec::new(),
        };
        source
            .submit(second.clone())
            .map_err(|error| error.to_string())?;
        let submitted = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "command catalog did not receive submission".to_owned())?
            .map_err(|error| error.to_string())?;
        if !submitted.commands.iter().any(|entry| {
            entry.command.request.id == second.id && entry.command.state == CommandState::Submitted
        }) {
            return Err("command catalog did not materialize the new submission".to_owned());
        }
        source.claim(second.id).map_err(|error| error.to_string())?;
        let executing = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "command catalog did not receive execution".to_owned())?
            .map_err(|error| error.to_string())?;
        if !executing.commands.iter().any(|entry| {
            entry.command.request.id == second.id && entry.command.state == CommandState::Executing
        }) {
            return Err("command catalog did not advance the command lifecycle".to_owned());
        }

        server
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let revoked = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .map_err(|_| "command catalog access revocation was not enforced".to_owned())?;
        if !matches!(
            revoked,
            Err(IrohReplicationError::Stream(ref message)) if message.contains("access denied")
        ) {
            return Err(format!(
                "command catalog remained open after revocation: {revoked:?}"
            ));
        }
        subscription.close();
        client.shutdown().await.map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn native_remote_submission_is_idempotent_and_never_claims_execution()
    -> Result<(), String> {
        let server = Node::in_memory();
        let client = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<RemoteService>()
            .map_err(|error| error.to_string())?
            .build();
        let server_transport = IrohReplicator::bind_loopback_application_with_policy(
            ApplicationNode::new(server.clone(), application),
            Arc::new(AllowAllAccessPolicy),
        )
        .await
        .map_err(|error| error.to_string())?;
        let client_transport = IrohReplicator::bind_loopback(client)
            .await
            .map_err(|error| error.to_string())?;
        let server_address = server_transport.address();
        let command = CommandSubmission::for_command(&SetRemoteRecord {
            id: RemoteRecordId::from("remote-command"),
            scope: "session:test".to_owned(),
            value: "hello over native iroh".to_owned(),
        })
        .map_err(|error| error.to_string())?;

        let submitted = client_transport
            .submit_remote(server_address.clone(), command.clone())
            .await
            .map_err(|error| error.to_string())?;
        let Some(snapshot) = submitted.command else {
            return Err("remote submission returned no command".to_owned());
        };
        if submitted.source_node != server.node_id()
            || snapshot.request.id != command.id
            || snapshot.state != CommandState::Submitted
        {
            return Err(format!("unexpected remote submission: {snapshot:?}"));
        }

        let repeated = client_transport
            .submit_remote(server_address.clone(), command.clone())
            .await
            .map_err(|error| error.to_string())?;
        if repeated.command.as_ref() != Some(&snapshot) {
            return Err("stable remote submission was not idempotent".to_owned());
        }
        let mut conflicting = command.clone();
        conflicting.payload = serde_json::to_vec(&SetRemoteRecord {
            id: RemoteRecordId::from("remote-command"),
            scope: "session:test".to_owned(),
            value: "conflicting reuse".to_owned(),
        })
        .map_err(|error| error.to_string())?;
        let conflict = client_transport
            .submit_remote(server_address.clone(), conflicting)
            .await;
        if !matches!(
            conflict,
            Err(IrohReplicationError::Stream(ref message))
                if message.contains("remote command failed")
        ) {
            return Err(format!(
                "conflicting remote command was accepted: {conflict:?}"
            ));
        }
        let queried = client_transport
            .command_client(server_address)
            .command_state(command.id)
            .await
            .map_err(|error| error.to_string())?;
        if queried.command.as_ref() != Some(&snapshot) {
            return Err("native command query did not return submitted state".to_owned());
        }
        if !server
            .claim(command.id)
            .map_err(|error| error.to_string())?
            .should_execute()
        {
            return Err("remote client claimed execution before the local handler".to_owned());
        }
        assert_remote_cancellation(&client_transport, &server_transport, command.id).await?;

        client_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        server_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn native_policy_limits_history_and_commands_before_exposure_or_mutation()
    -> Result<(), String> {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let granted_scope = ScopeId::new("session:granted");
        let granted = commit_test_command_in_scope(&source, "granted", granted_scope.clone())?;
        let hidden =
            commit_test_command_in_scope(&source, "hidden", ScopeId::new("session:hidden-policy"))?;
        let application = MykoApplication::builder()
            .service::<RemoteService>()
            .map_err(|error| error.to_string())?
            .build();
        let source_transport = IrohReplicator::bind_loopback_application_with_policy(
            ApplicationNode::new(source.clone(), application),
            Arc::new(AllowAllAccessPolicy),
        )
        .await
        .map_err(|error| error.to_string())?;
        source_transport
            .set_access_policy(Arc::new(ReadOnlyScopePolicy {
                scope_id: granted_scope.clone(),
            }))
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let address = source_transport.address();

        let report = target_transport
            .pull_scope(address.clone(), granted_scope.clone(), None)
            .await
            .map_err(|error| error.to_string())?;
        if report.applied != 2
            || target
                .command(hidden.id)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err(format!("granted scoped read leaked history: {report:?}"));
        }
        if target_transport.pull(address.clone(), None).await.is_ok() {
            return Err("policy allowed unscoped history replication".to_owned());
        }
        let queried = target_transport
            .command_remote(address.clone(), granted.id)
            .await
            .map_err(|error| error.to_string())?;
        if !queried
            .command
            .is_some_and(|command| command.state.is_committed())
        {
            return Err("policy-blocked an allowed command read".to_owned());
        }
        if target_transport
            .cancel_remote(address.clone(), granted.id, "denied".to_owned())
            .await
            .is_ok()
        {
            return Err("read-only policy allowed command cancellation".to_owned());
        }
        let denied = CommandSubmission::for_command(&SetRemoteRecord {
            id: RemoteRecordId::from("denied-submit"),
            scope: granted_scope.to_string(),
            value: "denied".to_owned(),
        })
        .map_err(|error| error.to_string())?;
        if target_transport
            .submit_remote(address.clone(), denied.clone())
            .await
            .is_ok()
            || source
                .command(denied.id)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err("read-only policy allowed or persisted submission".to_owned());
        }
        if target_transport
            .subscribe_live_remote(address, vec!["session:granted".to_owned()])
            .await
            .is_ok()
        {
            return Err("read-only policy allowed an ungranted live topic".to_owned());
        }

        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn replacing_policy_revokes_open_history_and_live_streams() -> Result<(), String> {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let address = source_transport.address();
        let follower = target_transport.follow(address.clone(), None, Duration::from_millis(20));
        wait_for_connection(&follower).await?;
        let mut live = target_transport
            .subscribe_live_remote(address, vec!["session:revoked".to_owned()])
            .await
            .map_err(|error| error.to_string())?;

        source_transport
            .set_access_policy(Arc::new(DenyAllPolicy))
            .map_err(|error| error.to_string())?;
        let live_error = tokio::time::timeout(Duration::from_secs(10), live.recv())
            .await
            .map_err(|_| "open live stream did not observe policy revocation".to_owned())?;
        if !matches!(
            live_error,
            Err(IrohReplicationError::Stream(ref message))
                if message.contains("access denied")
        ) {
            return Err(format!(
                "open live stream returned the wrong revocation result: {live_error:?}"
            ));
        }
        live.close();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = follower.status().map_err(|error| error.to_string())?;
                if !status.connected
                    && status
                        .last_error
                        .as_ref()
                        .is_some_and(|message| message.contains("access denied"))
                {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "open history stream did not observe policy revocation".to_owned())??;

        let withheld = commit_test_command(&source, "withheld-while-revoked")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        if target
            .command(withheld.id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("revoked history stream continued importing events".to_owned());
        }

        source_transport
            .set_access_policy(Arc::new(AllowAllAccessPolicy))
            .map_err(|error| error.to_string())?;
        wait_for_committed(&target, withheld.id).await?;

        follower
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn native_live_stream_filters_best_effort_events_without_durable_history()
    -> Result<(), String> {
        let source = Node::in_memory();
        let client = Node::in_memory();
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let client_transport = IrohReplicator::bind_loopback(client)
            .await
            .map_err(|error| error.to_string())?;
        let mut live = client_transport
            .subscribe_live_remote(source_transport.address(), vec!["session:a".to_owned()])
            .await
            .map_err(|error| error.to_string())?;
        if live.source_node() != source.node_id() {
            return Err("live stream advertised the wrong Myko source".to_owned());
        }

        let unrelated = source_transport
            .publish_live("session:b", b"ignore".to_vec())
            .map_err(|error| error.to_string())?;
        if unrelated.delivered != 0 || unrelated.dropped != 0 {
            return Err(format!(
                "topic filter received an unrelated event: {unrelated:?}"
            ));
        }
        let published = source_transport
            .publish_live("session:a", b"delta".to_vec())
            .map_err(|error| error.to_string())?;
        if published.delivered != 1 || published.dropped != 0 {
            return Err(format!("live event was not delivered: {published:?}"));
        }
        let event = tokio::time::timeout(Duration::from_secs(10), live.recv())
            .await
            .map_err(|_| "timed out waiting for native live event".to_owned())?
            .map_err(|error| error.to_string())?;
        if event.source_node != source.node_id()
            || event.sequence != 1
            || event.topic != "session:a"
            || event.payload != b"delta"
        {
            return Err(format!("unexpected native live event: {event:?}"));
        }
        if !source
            .events_after(None)
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            return Err("best-effort live event entered durable history".to_owned());
        }

        live.close();
        client_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn follower_delivers_changes_committed_after_it_starts() -> Result<(), String> {
        let source = Node::in_memory();
        let target = Node::in_memory();
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let follower =
            target_transport.follow(source_transport.address(), None, Duration::from_mins(1));
        let mut target_events = target.subscribe(None).map_err(|error| error.to_string())?;
        wait_for_connection(&follower).await?;

        let request = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new("test"),
            scope_id: ScopeId::new("live"),
            principal_id: PrincipalId::new("node:test"),
            command_type: "after-follow".to_owned(),
            payload: Vec::new(),
        };
        let admission = source
            .admit(request.clone())
            .map_err(|error| error.to_string())?;
        source
            .commit(
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
            )
            .map_err(|error| error.to_string())?;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let envelope = target_events
                    .recv_async()
                    .await
                    .map_err(|error| error.to_string())?;
                if matches!(
                    envelope.event,
                    myko_federation::NodeEvent::CommandCommitted { ref command, .. }
                        if command.request.id == request.id
                ) {
                    return Ok::<(), String>(());
                }
            }
        })
        .await
        .map_err(|_| "peer follower did not deliver the live commit".to_owned())??;

        let status = follower.status().map_err(|error| error.to_string())?;
        if !status.connected
            || status.successful_connections != 1
            || status.successful_batches == 0
            || status.cursor.is_none()
            || status.last_error.is_some()
        {
            return Err(format!("unexpected peer follower status: {status:?}"));
        }
        follower
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn peer_supervisor_multiplexes_and_removes_independent_followers() -> Result<(), String> {
        let first_source = Node::in_memory();
        let second_source = Node::in_memory();
        let target = Node::in_memory();
        let first_initial = commit_test_command(&first_source, "first-initial")?;
        let second_initial = commit_test_command(&second_source, "second-initial")?;

        let first_transport = IrohReplicator::bind_loopback(first_source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let second_transport = IrohReplicator::bind_loopback(second_source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let first_address = first_transport.address();
        let second_address = second_transport.address();
        let supervisor = PeerSupervisor::new(target_transport.clone());

        if supervisor
            .upsert(first_address.clone(), None, Duration::from_mins(1))
            .await
            .map_err(|error| error.to_string())?
            || supervisor
                .upsert(second_address.clone(), None, Duration::from_mins(1))
                .await
                .map_err(|error| error.to_string())?
        {
            return Err("new peer unexpectedly replaced a follower".to_owned());
        }
        wait_for_committed(&target, first_initial.id).await?;
        wait_for_committed(&target, second_initial.id).await?;
        if supervisor
            .statuses()
            .map_err(|error| error.to_string())?
            .len()
            != 2
        {
            return Err("supervisor did not retain both peer followers".to_owned());
        }

        if !supervisor
            .remove(first_address.id)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("supervisor did not remove the first peer".to_owned());
        }
        let first_after_removal = commit_test_command(&first_source, "first-after-removal")?;
        let second_after_removal = commit_test_command(&second_source, "second-after-removal")?;
        wait_for_committed(&target, second_after_removal.id).await?;
        if target
            .command(first_after_removal.id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("removed peer continued delivering history".to_owned());
        }
        let statuses = supervisor.statuses().map_err(|error| error.to_string())?;
        if statuses.len() != 1
            || statuses
                .first()
                .is_none_or(|status| status.peer.id != second_address.id)
        {
            return Err(format!("unexpected remaining peer set: {statuses:?}"));
        }

        supervisor
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        second_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        first_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn follower_resumes_from_a_redb_cursor_after_restart() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("target.redb");
        let source = Node::in_memory();
        let first = commit_test_command(&source, "before-restart")?;
        let source_transport = IrohReplicator::bind_loopback(source.clone())
            .await
            .map_err(|error| error.to_string())?;
        let source_address = source_transport.address();

        let (target, journal) =
            RedbJournal::open_node_with_journal(&path).map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let follower = target_transport
            .follow_persisted(
                source_address.clone(),
                journal.clone(),
                Duration::from_millis(20),
            )
            .map_err(|error| error.to_string())?;
        wait_for_committed(&target, first.id).await?;
        let cursor_before_restart = wait_for_cursor(&follower).await?;
        follower
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        drop(target);
        drop(journal);

        let (reopened, reopened_journal) =
            RedbJournal::open_node_with_journal(&path).map_err(|error| error.to_string())?;
        let reopened_transport = IrohReplicator::bind_loopback(reopened.clone())
            .await
            .map_err(|error| error.to_string())?;
        let resumed = reopened_transport
            .follow_persisted(source_address, reopened_journal, Duration::from_millis(20))
            .map_err(|error| error.to_string())?;
        if resumed.status().map_err(|error| error.to_string())?.cursor
            != Some(cursor_before_restart)
        {
            return Err("restarted follower did not load its durable cursor".to_owned());
        }

        let second = commit_test_command(&source, "after-restart")?;
        wait_for_committed(&reopened, second.id).await?;
        resumed
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        reopened_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        source_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn persisted_follower_resets_when_transport_peer_has_a_new_myko_history()
    -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let target_path = directory.path().join("target.redb");
        let transport_secret = SecretKey::generate();

        let first_source = Node::in_memory();
        let first_source_id = first_source.node_id();
        let first_command = commit_test_command(&first_source, "first-history")?;
        let first_transport =
            IrohReplicator::bind_loopback_with_secret(first_source, transport_secret.clone())
                .await
                .map_err(|error| error.to_string())?;
        let first_address = first_transport.address();

        let (target, journal) =
            RedbJournal::open_node_with_journal(&target_path).map_err(|error| error.to_string())?;
        let target_transport = IrohReplicator::bind_loopback(target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let follower = target_transport
            .follow_persisted(
                first_address.clone(),
                journal.clone(),
                Duration::from_millis(20),
            )
            .map_err(|error| error.to_string())?;
        wait_for_committed(&target, first_command.id).await?;
        let first_cursor = wait_for_cursor(&follower).await?;
        follower
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        target_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        first_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        drop(target);
        drop(journal);

        let replacement_source = Node::in_memory();
        let replacement_source_id = replacement_source.node_id();
        if replacement_source_id == first_source_id {
            return Err("fresh source unexpectedly reused its Myko node identity".to_owned());
        }
        let replacement_command = commit_test_command(&replacement_source, "new-history")?;
        let replacement_transport =
            IrohReplicator::bind_loopback_with_secret(replacement_source, transport_secret)
                .await
                .map_err(|error| error.to_string())?;
        let replacement_address = replacement_transport.address();
        if replacement_address.id != first_address.id {
            return Err("test transport identity was not preserved".to_owned());
        }

        let (reopened_target, reopened_journal) =
            RedbJournal::open_node_with_journal(&target_path).map_err(|error| error.to_string())?;
        let reopened_transport = IrohReplicator::bind_loopback(reopened_target.clone())
            .await
            .map_err(|error| error.to_string())?;
        let resumed = reopened_transport
            .follow_persisted(
                replacement_address,
                reopened_journal.clone(),
                Duration::from_mins(1),
            )
            .map_err(|error| error.to_string())?;
        if resumed.status().map_err(|error| error.to_string())?.cursor != Some(first_cursor) {
            return Err("test did not begin from the first source's cursor".to_owned());
        }
        wait_for_committed(&reopened_target, replacement_command.id).await?;

        let status = resumed.status().map_err(|error| error.to_string())?;
        if status.source_node != Some(replacement_source_id) || status.successful_connections < 2 {
            return Err(format!(
                "follower did not identify and restart the replacement history: {status:?}"
            ));
        }
        let key = ReplicationCursorKey::new("iroh", first_address.id.to_string());
        let checkpoint = reopened_journal
            .load_checkpoint(&key)
            .map_err(|error| error.to_string())?;
        if !checkpoint.is_some_and(|checkpoint| {
            checkpoint.source_node == replacement_source_id && checkpoint.position.is_some()
        }) {
            return Err(format!(
                "replacement source checkpoint was not durable: {checkpoint:?}"
            ));
        }

        resumed
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        reopened_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        replacement_transport
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
