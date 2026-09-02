//! Restartable native node composition for Myko 7.
//!
//! This crate combines the transport-neutral federation node, a durable Redb
//! journal, and the native Iroh transport. It owns operational identity and
//! peer-replication state, but deliberately knows nothing about an application's
//! commands, projections, workspace paths, or authorization model.

#![forbid(unsafe_code)]

mod discovery;
mod live_state;
mod pairing;
mod peer;
mod status;

pub use discovery::{
    ConfigureLanDiscovery, DiscoverySettings, DiscoverySettingsReport, NearbyNodesView,
};
use discovery::{DiscoverySupervisor, DiscoveryViewState};
pub use live_state::RuntimeFeed;
use pairing::PairingSupervisor;
pub use pairing::{
    ConfirmPairing, InitiatePairing, IssuePairingInvitation, PairingInitiation,
    PairingInitiationId, PairingInitiationPhase, PairingInitiationReport, PairingReceiptsView,
    PairingRedemption, PairingRedemptionId, PairingRedemptionPhase, PairingRedemptionReport,
    PendingPairingReceipt, PendingPairingReceiptId, RedeemPairingInvitation,
};
pub use peer::{
    AddPeer, AdvertiseServices, AdvertisedService, AdvertisedServiceId, AdvertisedServicesView,
    FederationService, GetAdvertisedServices, GetPeers, Peer, PeerId, PeerReport, PeersView,
    RememberPeer, RemovePeer, ServiceCapabilityReport, SetPeerReplication,
    SetPeerReplicationSelection, peer_id,
};
use peer::{RestorePeer, peer_scope};
pub use status::{NodeStatus, NodeStatusView};
use status::{NodeStatusProjectionGuard, NodeStatusViewState, project_node_statuses};

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use myko_app::{
    ApplicationNode, CommandDispatchGuard, CommandHandler, HandlerSubscription, MykoApplication,
    ReportHandler, ViewHandler, ViewSubscription,
};
use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, ItemQuery, ItemQueryWatch, LiveSubscription,
    LiveSubscriptionState, Node as FederationNode, NodeError as FederationNodeError, NodeId,
    NodeStartupGuard, ReplicationSelection, ScopeId, ServiceId, SubscriptionLiveness,
    live_subscription,
};
pub use myko_iroh::{
    EndpointAddr, EndpointId, NativeNodeDescriptor, NativePeerReference, PairingInvitation,
    PairingReceipt, SecretKey, endpoint_principal_id,
};
use myko_iroh::{IrohReactiveHandlerSubscription, IrohReactiveViewSubscription};
use myko_iroh::{IrohReplicationError, IrohReplicator, PeerSupervisor, load_or_create_secret_key};
use myko_redb::RedbJournal;
use myko_session::{NodeRequestRouter, NodeRouteFuture, NodeSessionService};
use myko_wire::{NodeFrame, NodeRequestEnvelope};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;

const CONFIG_VERSION: u32 = 3;
const PREVIOUS_CONFIG_VERSION: u32 = 2;
const LEGACY_CONFIG_VERSION: u32 = 1;
const JOURNAL_FILE: &str = "node.redb";
const SECRET_FILE: &str = "iroh-secret.json";
const PEERS_FILE: &str = "peers.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPeerConfig {
    version: u32,
    peers: Vec<ConfiguredPeer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyStoredPeerConfig {
    version: u32,
    peers: Vec<EndpointAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StoredConfigHeader {
    version: u32,
}

/// One durable peer binding, optionally pinned to an expected Myko history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConfiguredPeer {
    pub endpoint: EndpointAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node: Option<NodeId>,
    /// Whether this node currently replicates the peer's history.
    ///
    /// A paired descriptor may be retained with this disabled. That records
    /// authenticated identity knowledge without copying any application data.
    #[serde(default = "default_peer_replication", alias = "following")]
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_selection: ReplicationSelection,
}

impl ConfiguredPeer {
    const fn unpinned(endpoint: EndpointAddr) -> Self {
        Self {
            endpoint,
            source_node: None,
            replication_enabled: true,
            replication_selection: ReplicationSelection::All,
        }
    }
}

impl From<&Peer> for ConfiguredPeer {
    fn from(peer: &Peer) -> Self {
        Self {
            endpoint: peer.endpoint.clone(),
            source_node: peer.source_node,
            replication_enabled: peer.replication_enabled,
            replication_selection: peer.replication_selection.clone(),
        }
    }
}

const fn default_peer_replication() -> bool {
    true
}

#[derive(Debug, Clone, Copy)]
enum BindMode {
    Network,
    Loopback,
}

/// Failure while opening or operating a native Myko node.
#[derive(Debug, Error)]
pub enum NodeError {
    /// The composed Myko application could not be activated or driven.
    #[error(transparent)]
    Application(#[from] myko_app::AppError),
    /// The event journal could not be opened or replayed.
    #[error(transparent)]
    Federation(#[from] FederationNodeError),
    /// The native Iroh endpoint or one of its peer-replication tasks failed.
    #[error(transparent)]
    Iroh(#[from] IrohReplicationError),
    /// Durable JSON state was malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Durable state could not be read or committed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Durable peer state violated a node invariant.
    #[error("invalid node configuration: {0}")]
    Configuration(String),
    /// Shared runtime state could not be accessed.
    #[error("node state unavailable: {0}")]
    State(String),
    /// No authenticated route exists to a requested source node.
    #[error("application route unavailable: {0}")]
    Route(String),
}

/// Typed application client bound to one source node.
///
/// The client resolves the source through the node's authoritative peer
/// projection. Callers use the same command, report, and view methods for the
/// local node and for an authenticated Iroh peer; transport selection never
/// enters application code.
#[derive(Debug, Clone)]
pub struct ApplicationClient {
    application: ApplicationNode,
    replicator: IrohReplicator,
    router: Arc<FederationRouter>,
    source_node: NodeId,
}

/// A live typed report whose transport-specific owner is retained privately.
pub struct ApplicationReportSubscription<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    owner: ApplicationReportOwner<T, C>,
}

enum ApplicationReportOwner<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    Local(HandlerSubscription<T, C>),
    Remote(IrohReactiveHandlerSubscription<T, C>),
}

impl<T, C> ApplicationReportSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn local(subscription: HandlerSubscription<T, C>) -> Self {
        Self {
            live: subscription.live().clone(),
            owner: ApplicationReportOwner::Local(subscription),
        }
    }

    fn remote(subscription: IrohReactiveHandlerSubscription<T, C>) -> Self {
        Self {
            live: subscription.live().clone(),
            owner: ApplicationReportOwner::Remote(subscription),
        }
    }

    /// Returns the transport-independent reactive report cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T, C> {
        &self.live
    }

    /// Stops the report and releases its local or remote driver.
    pub async fn shutdown(self) {
        match self.owner {
            ApplicationReportOwner::Local(subscription) => subscription.shutdown().await,
            ApplicationReportOwner::Remote(subscription) => drop(subscription),
        }
    }
}

/// A live typed view whose transport-specific owner is retained privately.
pub struct ApplicationViewSubscription<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: myko_federation::LiveCollection<T, C>,
    owner: ApplicationViewOwner<T, C>,
}

enum ApplicationViewOwner<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    Local(ViewSubscription<T, C>),
    Remote(IrohReactiveViewSubscription<T, C>),
}

impl<T, C> ApplicationViewSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    fn local(subscription: ViewSubscription<T, C>) -> Self {
        Self {
            live: subscription.live().clone(),
            owner: ApplicationViewOwner::Local(subscription),
        }
    }

    fn remote(subscription: IrohReactiveViewSubscription<T, C>) -> Self {
        Self {
            live: subscription.live().clone(),
            owner: ApplicationViewOwner::Remote(subscription),
        }
    }

    /// Returns the transport-independent identity-preserving view.
    #[must_use]
    pub const fn live(&self) -> &myko_federation::LiveCollection<T, C> {
        &self.live
    }

    /// Stops the view and releases its local or remote driver.
    pub async fn shutdown(self) {
        match self.owner {
            ApplicationViewOwner::Local(subscription) => subscription.shutdown().await,
            ApplicationViewOwner::Remote(subscription) => drop(subscription),
        }
    }
}

/// A restartable native Myko node.
///
/// The data directory is the node's operational identity boundary. Opening the
/// same directory restores its event identity, transport identity, and every
/// configured source-aware peer relationship.
#[derive(Debug)]
pub struct Node {
    data_dir: PathBuf,
    federation: FederationNode,
    application: ApplicationNode,
    journal: Arc<RedbJournal>,
    replicator: IrohReplicator,
    request_router: Arc<FederationRouter>,
    command_dispatch: Option<CommandDispatchGuard>,
    supervisor: Arc<PeerSupervisor>,
    peer_reconciler: Option<PeerReconcilerGuard>,
    pairing: Option<PairingSupervisor>,
    status_projection: Option<NodeStatusProjectionGuard>,
    discovery: Option<DiscoverySupervisor>,
}

#[derive(Debug)]
struct PeerReconcilerGuard {
    task: Option<JoinHandle<()>>,
}

struct PeerReconcilerContext {
    peers: Arc<Mutex<BTreeMap<EndpointId, ConfiguredPeer>>>,
    supervisor: Arc<PeerSupervisor>,
    journal: Arc<RedbJournal>,
    retry_interval: Duration,
    descriptor: NativeNodeDescriptor,
    status: NodeStatusViewState,
}

impl PeerReconcilerGuard {
    fn start(
        mut watch: ItemQueryWatch<GetPeers>,
        current: BTreeMap<EndpointId, ConfiguredPeer>,
        context: PeerReconcilerContext,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut current = current;
            loop {
                let update = match watch.recv_async().await {
                    Ok(update) => update,
                    Err(error) => {
                        context
                            .status
                            .invalidate(format!("peer configuration subscription failed: {error}"));
                        return;
                    }
                };
                let desired = configured_peer_map(&update.value);
                if let Err(error) = reconcile_peers(
                    &current,
                    &desired,
                    &context.peers,
                    &context.supervisor,
                    &context.journal,
                    context.retry_interval,
                )
                .await
                {
                    context
                        .status
                        .invalidate(format!("peer reconciliation failed: {error}"));
                    return;
                }
                current = desired;
                if let Ok(statuses) = context.supervisor.statuses() {
                    context.status.publish(project_node_statuses(
                        &context.descriptor,
                        &current,
                        &statuses,
                    ));
                }
            }
        });
        Self { task: Some(task) }
    }

    async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _stopped = task.await;
        }
    }
}

impl Drop for PeerReconcilerGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn configured_peer_map(peers: &[Peer]) -> BTreeMap<EndpointId, ConfiguredPeer> {
    peers
        .iter()
        .map(|peer| (peer.endpoint.id, ConfiguredPeer::from(peer)))
        .collect()
}

async fn start_configured_follower(
    supervisor: &PeerSupervisor,
    peer: &ConfiguredPeer,
    journal: Arc<RedbJournal>,
    retry_interval: Duration,
) -> Result<(), NodeError> {
    if let Some(source_node) = peer.source_node {
        supervisor
            .upsert_persisted_source_selected(
                peer.endpoint.clone(),
                source_node,
                peer.replication_selection.clone(),
                journal,
                retry_interval,
            )
            .await?;
    } else {
        supervisor
            .upsert_persisted_selected(
                peer.endpoint.clone(),
                peer.replication_selection.clone(),
                journal,
                retry_interval,
            )
            .await?;
    }
    Ok(())
}

async fn reconcile_peers(
    current: &BTreeMap<EndpointId, ConfiguredPeer>,
    desired: &BTreeMap<EndpointId, ConfiguredPeer>,
    peers: &Mutex<BTreeMap<EndpointId, ConfiguredPeer>>,
    supervisor: &PeerSupervisor,
    journal: &Arc<RedbJournal>,
    retry_interval: Duration,
) -> Result<(), NodeError> {
    {
        let mut routed = peers
            .lock()
            .map_err(|_| NodeError::State("peer configuration lock is poisoned".to_owned()))?;
        routed.clone_from(desired);
    }

    for (peer_id, previous) in current {
        if desired
            .get(peer_id)
            .is_none_or(|peer| !peer.replication_enabled || peer != previous)
        {
            supervisor.remove(*peer_id).await?;
        }
    }
    for (peer_id, peer) in desired {
        if peer.replication_enabled && current.get(peer_id) != Some(peer) {
            start_configured_follower(supervisor, peer, Arc::clone(journal), retry_interval)
                .await?;
        }
    }
    Ok(())
}

/// Routes canonical request envelopes through a durable node's pinned peers.
#[derive(Debug, Clone)]
struct FederationRouter {
    replicator: IrohReplicator,
    node: FederationNode,
}

impl FederationRouter {
    fn peer(&self, target_node: NodeId) -> Result<EndpointAddr, String> {
        self.node
            .query_items_in(
                self.node.node_id(),
                &peer_scope(self.node.node_id()),
                GetPeers,
            )
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|peer| peer.source_node == Some(target_node))
            .map(|peer| peer.endpoint)
            .ok_or_else(|| format!("node {target_node} is not a configured identity-pinned peer"))
    }

    fn peer_for_service(&self, service_id: &ServiceId) -> Result<Option<NodeId>, String> {
        let candidates = self
            .node
            .query_items_in(
                self.node.node_id(),
                &peer_scope(self.node.node_id()),
                GetPeers,
            )
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|peer| peer.replication_enabled)
            .filter_map(|peer| peer.source_node)
            .collect::<Vec<_>>();
        for source_node in candidates {
            let services = self
                .node
                .query_items_in(source_node, &peer_scope(source_node), GetAdvertisedServices)
                .map_err(|error| error.to_string())?;
            if services
                .iter()
                .any(|service| service.service_id == *service_id)
            {
                return Ok(Some(source_node));
            }
        }
        Ok(None)
    }
}

impl NodeRequestRouter for FederationRouter {
    fn service_destination(&self, service_id: &ServiceId) -> Result<Option<NodeId>, String> {
        self.peer_for_service(service_id)
    }

    fn route<'a>(
        &'a self,
        envelope: NodeRequestEnvelope,
        frames: &'a flume::Sender<NodeFrame>,
    ) -> NodeRouteFuture<'a> {
        Box::pin(async move {
            let destination = envelope
                .destination
                .ok_or_else(|| "routed request omitted its destination".to_owned())?;
            let peer = self.peer(destination)?;
            self.replicator
                .forward_request(peer, envelope, frames)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

impl ApplicationClient {
    fn remote_endpoint(&self) -> Result<Option<EndpointAddr>, NodeError> {
        if self.source_node == self.application.node_id() {
            return Ok(None);
        }
        self.router
            .peer(self.source_node)
            .map(Some)
            .map_err(NodeError::Route)
    }

    /// Returns the source node selected for every operation on this client.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Executes one bounded typed command on the selected source node.
    ///
    /// Myko owns admission, lifecycle observation, result decoding, peer
    /// resolution, and transport selection. The command is identical whether
    /// the selected source is local or remote.
    ///
    /// # Errors
    ///
    /// Returns an error if no authenticated route exists or the command cannot
    /// be admitted, executed, or decoded.
    pub async fn exec_command<C>(&self, command: C) -> Result<C::Output, NodeError>
    where
        C: CommandHandler,
    {
        let Some(endpoint) = self.remote_endpoint()? else {
            return myko_app::CommandClient::exec_command(&self.application, command)
                .await
                .map_err(NodeError::from);
        };
        myko_app::CommandClient::exec_command(&self.replicator.command_client(endpoint), command)
            .await
            .map_err(NodeError::from)
    }

    /// Opens one long-lived typed report on the selected source node.
    ///
    /// # Errors
    ///
    /// Returns an error if no authenticated route exists or the report cannot
    /// be built, established, authorized, or decoded.
    pub async fn watch_report<R>(
        &self,
        report: &R,
    ) -> Result<ApplicationReportSubscription<R::Output, R::Cursor>, NodeError>
    where
        R: ReportHandler,
    {
        let Some(endpoint) = self.remote_endpoint()? else {
            return self
                .application
                .watch_report(report)
                .map(ApplicationReportSubscription::local)
                .map_err(NodeError::from);
        };
        self.replicator
            .application_client(endpoint)
            .watch_report_reactive(report)
            .await
            .map(ApplicationReportSubscription::remote)
            .map_err(NodeError::from)
    }

    /// Opens one long-lived typed view on the selected source node.
    ///
    /// # Errors
    ///
    /// Returns an error if no authenticated route exists or the view cannot be
    /// built, established, authorized, or decoded.
    pub async fn watch_view<V>(
        &self,
        view: &V,
    ) -> Result<ApplicationViewSubscription<V::Item, V::Cursor>, NodeError>
    where
        V: ViewHandler,
    {
        let Some(endpoint) = self.remote_endpoint()? else {
            return self
                .application
                .watch_view(view)
                .map(ApplicationViewSubscription::local)
                .map_err(NodeError::from);
        };
        self.replicator
            .application_client(endpoint)
            .watch_view_reactive(view)
            .await
            .map(ApplicationViewSubscription::remote)
            .map_err(NodeError::from)
    }
}

struct FederationRuntimeParts {
    request_router: Arc<FederationRouter>,
    supervisor: Arc<PeerSupervisor>,
    peer_reconciler: PeerReconcilerGuard,
    peers: Arc<Mutex<BTreeMap<EndpointId, ConfiguredPeer>>>,
    node_status: NodeStatusViewState,
    discovery: DiscoveryViewState,
}

fn restore_legacy_peers(
    data_dir: &Path,
    node: &FederationNode,
    application: &ApplicationNode,
) -> Result<(), NodeError> {
    let legacy_peers = load_peers(&data_dir.join(PEERS_FILE))?;
    let existing_peers =
        node.query_items_in(node.node_id(), &peer_scope(node.node_id()), GetPeers)?;
    if !existing_peers.is_empty() {
        return Ok(());
    }
    for peer in legacy_peers.values() {
        let _restored = application.exec_command(RestorePeer {
            endpoint: peer.endpoint.clone(),
            source_node: peer.source_node,
            replication_enabled: peer.replication_enabled,
            replication_selection: peer.replication_selection.clone(),
        })?;
    }
    Ok(())
}

fn ensure_discovery_settings(
    node: &FederationNode,
    application: &ApplicationNode,
) -> Result<(), NodeError> {
    let discovery_id = discovery::DiscoverySettingsId::from(node.node_id().to_string());
    let settings = node.query_items_in(
        node.node_id(),
        &peer_scope(node.node_id()),
        discovery::GetDiscoverySettingsById { id: discovery_id },
    )?;
    if settings.is_none() {
        let _configured = application.exec_command(ConfigureLanDiscovery {
            display_name: default_node_name(node.node_id()),
            enabled: true,
        })?;
    }
    Ok(())
}

fn ensure_advertised_services(
    node: &FederationNode,
    application: &ApplicationNode,
    desired: Vec<ServiceId>,
) -> Result<(), NodeError> {
    let current = node.query_items_in(
        node.node_id(),
        &peer_scope(node.node_id()),
        GetAdvertisedServices,
    )?;
    let current = current
        .iter()
        .map(|service| service.service_id.as_str())
        .collect::<BTreeSet<_>>();
    let desired_ids = desired
        .iter()
        .map(ServiceId::as_str)
        .collect::<BTreeSet<_>>();
    if current != desired_ids {
        let _advertised = application.exec_command(AdvertiseServices { services: desired })?;
    }
    Ok(())
}

async fn initialize_federation_runtime(
    data_dir: &Path,
    retry_interval: Duration,
    node: &FederationNode,
    journal: &Arc<RedbJournal>,
    application: &ApplicationNode,
    replicator: &IrohReplicator,
) -> Result<FederationRuntimeParts, NodeError> {
    restore_legacy_peers(data_dir, node, application)?;
    ensure_discovery_settings(node, application)?;

    let (peer_snapshot, peer_watch) =
        node.watch_items_in(node.node_id(), peer_scope(node.node_id()), GetPeers)?;
    let configured = configured_peer_map(&peer_snapshot.value);
    if configured.contains_key(&replicator.address().id) {
        return Err(NodeError::Configuration(
            "peer configuration contains this node's own Iroh identity".to_owned(),
        ));
    }

    let peers = Arc::new(Mutex::new(configured.clone()));
    let request_router = Arc::new(FederationRouter {
        replicator: replicator.clone(),
        node: node.clone(),
    });
    let session_router: Arc<dyn NodeRequestRouter> = request_router.clone();
    replicator
        .sessions()
        .set_router(&session_router)
        .map_err(NodeError::State)?;

    let supervisor = Arc::new(PeerSupervisor::new(replicator.clone()));
    for peer in configured.values().filter(|peer| peer.replication_enabled) {
        start_configured_follower(
            supervisor.as_ref(),
            peer,
            Arc::clone(journal),
            retry_interval,
        )
        .await?;
    }

    let node_status = NodeStatusViewState::new(project_node_statuses(
        &replicator.descriptor(),
        &configured,
        &supervisor.statuses()?,
    ));
    let discovery = DiscoveryViewState::new();
    let _previous_replicator = application.resources().insert(replicator.clone())?;
    let _previous_status = application.resources().insert(node_status.clone())?;
    let _previous_discovery = application.resources().insert(discovery.clone())?;
    let peer_reconciler = PeerReconcilerGuard::start(
        peer_watch,
        configured,
        PeerReconcilerContext {
            peers: Arc::clone(&peers),
            supervisor: Arc::clone(&supervisor),
            journal: Arc::clone(journal),
            retry_interval,
            descriptor: replicator.descriptor(),
            status: node_status.clone(),
        },
    );

    Ok(FederationRuntimeParts {
        request_router,
        supervisor,
        peer_reconciler,
        peers,
        node_status,
        discovery,
    })
}

/// Runtime-owned driver for one Hyphae-backed typed item subscription.
///
/// Dropping this owner stops the receive task. Clones of [`Self::live`] remain
/// readable but no longer claim to be connected to an active transport.
pub struct ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    live: LiveSubscription<T>,
    writer: myko_federation::LiveSubscriptionWriter<T>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    /// Returns the composable Hyphae lifecycle cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T> {
        &self.live
    }
}

impl<T> Drop for ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

impl Node {
    /// Loads or creates the durable Myko history identity for a node directory.
    ///
    /// This does not bind a transport or start federation effects. It is
    /// intended for lifecycle-managed clients that need to display their node
    /// identity while the full node is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory or durable journal cannot be opened.
    pub fn load_or_create_node_id(data_dir: impl AsRef<Path>) -> Result<NodeId, NodeError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)?;
        let (node, _journal) = RedbJournal::open_node_with_journal(data_dir.join(JOURNAL_FILE))?;
        Ok(node.node_id())
    }

    /// Opens a durable node with normal Iroh relay and discovery configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state cannot be restored, the endpoint
    /// cannot bind, or configured peer replication cannot start.
    pub async fn open(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
    ) -> Result<Self, NodeError> {
        Self::open_with_policy(data_dir, retry_interval, |_| {
            Ok(Arc::new(AllowAllAccessPolicy))
        })
        .await
    }

    /// Opens a durable node and resolves its initial access policy from the
    /// restored Myko projection before the Iroh router begins serving.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state or policy cannot be restored, the
    /// endpoint cannot bind, or configured peer replication cannot start.
    pub async fn open_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            None,
            None,
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    /// Opens a durable loopback-only node for local development and tests.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state cannot be restored, the endpoint
    /// cannot bind, or configured peer replication cannot start.
    pub async fn open_loopback(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
    ) -> Result<Self, NodeError> {
        Self::open_loopback_with_policy(data_dir, retry_interval, |_| {
            Ok(Arc::new(AllowAllAccessPolicy))
        })
        .await
    }

    /// Opens a loopback node with policy restored before the router serves.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state or policy cannot be restored, the
    /// endpoint cannot bind, or configured peer replication cannot start.
    pub async fn open_loopback_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Loopback,
            None,
            None,
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    /// Opens a durable network node that serves one immutable Myko application
    /// over the same authenticated Iroh endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, application serving,
    /// endpoint binding, or peer restoration fails.
    pub async fn open_application_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            None,
            Some(application),
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    /// Opens an application node with its transport held below the startup
    /// barrier until the returned guard is completed.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, endpoint
    /// binding, or peer restoration fails.
    pub async fn open_application_starting_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<(Self, NodeStartupGuard), NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        let (node, startup) = Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            None,
            Some(application),
            true,
            resolve_policy,
        )
        .await?;
        let startup = startup.ok_or_else(|| {
            NodeError::State("starting node omitted its startup guard".to_owned())
        })?;
        Ok((node, startup))
    }

    /// Opens a durable loopback node that serves one immutable Myko application.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, application serving,
    /// endpoint binding, or peer restoration fails.
    pub async fn open_loopback_application_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Loopback,
            None,
            Some(application),
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    /// Opens a durable application node with a caller-owned native identity.
    ///
    /// Platforms with a secure key store can supply the endpoint identity
    /// directly. Myko uses it without copying it into the node data directory.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, endpoint
    /// binding, application serving, or peer restoration fails.
    pub async fn open_application_with_identity_and_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        identity: SecretKey,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            Some(identity),
            Some(application),
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    /// Opens a loopback application node with a caller-owned native identity.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, endpoint
    /// binding, application serving, or peer restoration fails.
    pub async fn open_loopback_application_with_identity_and_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        identity: SecretKey,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<Self, NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Loopback,
            Some(identity),
            Some(application),
            false,
            resolve_policy,
        )
        .await
        .map(|(node, _startup)| node)
    }

    async fn open_inner<F>(
        data_dir: &Path,
        retry_interval: Duration,
        bind_mode: BindMode,
        identity: Option<SecretKey>,
        application: Option<MykoApplication>,
        hold_startup: bool,
        resolve_policy: F,
    ) -> Result<(Self, Option<NodeStartupGuard>), NodeError>
    where
        F: FnOnce(&ApplicationNode) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        fs::create_dir_all(data_dir)?;
        let secret = match identity {
            Some(identity) => identity,
            None => load_or_create_secret_key(data_dir.join(SECRET_FILE))?,
        };
        let (node, journal) = RedbJournal::open_node_with_journal(data_dir.join(JOURNAL_FILE))?;
        let startup = hold_startup.then(|| node.hold_startup());
        let application = application
            .unwrap_or_default()
            .with_framework_service::<FederationService>()?;
        let advertised_services = application
            .services()
            .map(|service| ServiceId::new(service.service_id.as_str()))
            .collect::<Vec<_>>();
        let application = ApplicationNode::new(node.clone(), application);
        ensure_advertised_services(&node, &application, advertised_services)?;
        let initial_policy = resolve_policy(&application)?;
        let replicator = match bind_mode {
            BindMode::Network => {
                IrohReplicator::bind_application_with_secret_and_policy(
                    application.clone(),
                    secret,
                    initial_policy,
                )
                .await
            }
            BindMode::Loopback => {
                IrohReplicator::bind_loopback_application_with_secret_and_policy(
                    application.clone(),
                    secret,
                    initial_policy,
                )
                .await
            }
        }?;
        let FederationRuntimeParts {
            request_router,
            supervisor,
            peer_reconciler,
            peers,
            node_status,
            discovery,
        } = initialize_federation_runtime(
            data_dir,
            retry_interval,
            &node,
            &journal,
            &application,
            &replicator,
        )
        .await?;
        let command_dispatch = application.drive_commands()?;
        let pairing = PairingSupervisor::start(application.clone(), replicator.clone())
            .map_err(NodeError::State)?;
        let status_projection = NodeStatusProjectionGuard::start(
            replicator.descriptor(),
            Arc::clone(&peers),
            Arc::clone(&supervisor),
            node_status,
        );
        let discovery = DiscoverySupervisor::start(
            &application,
            replicator.descriptor(),
            discovery,
            matches!(bind_mode, BindMode::Network),
        )
        .map_err(NodeError::State)?;
        Ok((
            Self {
                data_dir: data_dir.to_path_buf(),
                federation: node,
                application,
                journal,
                replicator,
                request_router,
                command_dispatch: Some(command_dispatch),
                supervisor,
                peer_reconciler: Some(peer_reconciler),
                pairing: Some(pairing),
                status_projection: Some(status_projection),
                discovery: Some(discovery),
            },
            startup,
        ))
    }

    /// Returns the transport-neutral event-sourced node.
    #[must_use]
    pub const fn node(&self) -> &FederationNode {
        &self.federation
    }

    /// Returns the composed application runtime served by this node.
    ///
    /// Every durable node activates Myko's framework-owned federation service,
    /// even when no user application services were supplied.
    #[must_use]
    pub const fn application(&self) -> &ApplicationNode {
        &self.application
    }

    /// Returns the typed application surface for one local or paired source.
    ///
    /// The returned client resolves the current authenticated route for every
    /// operation, so applications never cache endpoint descriptors or branch
    /// on transport type.
    #[must_use]
    pub fn application_at(&self, source_node: NodeId) -> ApplicationClient {
        ApplicationClient {
            application: self.application.clone(),
            replicator: self.replicator.clone(),
            router: Arc::clone(&self.request_router),
            source_node,
        }
    }

    /// Materializes a gap-free typed query into a first-class Hyphae cell.
    ///
    /// The Redb-backed node remains authoritative storage. A runtime task
    /// advances the cell only after Myko applies each complete matching batch,
    /// so reports, views, and UI bridges never need to poll the journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial snapshot/live boundary cannot be
    /// established or no Tokio runtime is active to own the live driver.
    pub fn watch_items_reactive_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<ReactiveItemSubscription<Q::Output>, NodeError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
    {
        let (snapshot, mut watch) = self
            .federation
            .watch_items_in(source_node, scope_id, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(snapshot.value),
            through: snapshot.through,
            liveness: SubscriptionLiveness::Current,
        });
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            NodeError::Configuration(format!(
                "reactive subscription requires an active Tokio runtime: {error}"
            ))
        })?;
        let task_writer = writer.clone();
        let task = runtime.spawn(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        break;
                    }
                }
            }
        });
        Ok(ReactiveItemSubscription { live, writer, task })
    }

    /// Returns the durable journal and replication-checkpoint store.
    #[must_use]
    pub fn journal(&self) -> Arc<RedbJournal> {
        self.journal.clone()
    }

    /// Returns the running native replication endpoint.
    #[must_use]
    pub const fn replicator(&self) -> &IrohReplicator {
        &self.replicator
    }

    /// Returns the canonical transport-neutral request service for this node.
    ///
    /// Local sockets, Iroh connections, and WebSocket gateways all serve this
    /// same endpoint so applications never implement transport-specific
    /// routing or federation semantics.
    #[must_use]
    pub const fn sessions(&self) -> &NodeSessionService {
        self.replicator.sessions()
    }

    /// Returns this node's authenticated Iroh address.
    #[must_use]
    pub fn address(&self) -> EndpointAddr {
        self.replicator.address()
    }

    /// Returns this endpoint paired with its stable Myko history identity.
    #[must_use]
    pub fn descriptor(&self) -> NativeNodeDescriptor {
        self.replicator.descriptor()
    }

    /// Returns the operational identity directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Replaces authorization for subsequently accepted native requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's shared policy state is poisoned.
    pub fn set_access_policy(&self, policy: Arc<dyn AccessPolicy>) -> Result<(), NodeError> {
        self.replicator.set_access_policy(policy)?;
        Ok(())
    }

    /// Stops all peer replication and then the shared Iroh endpoint.
    ///
    /// Durable configuration remains intact for the next open.
    ///
    /// # Errors
    ///
    /// Returns an error if peer replication or the endpoint cannot shut down cleanly.
    pub async fn shutdown(mut self) -> Result<(), NodeError> {
        if let Some(discovery) = self.discovery.take() {
            discovery.shutdown().await.map_err(NodeError::State)?;
        }
        if let Some(pairing) = self.pairing.take() {
            pairing.shutdown().await.map_err(NodeError::State)?;
        }
        if let Some(status_projection) = self.status_projection.take() {
            status_projection.shutdown().await;
        }
        if let Some(reconciler) = self.peer_reconciler.take() {
            reconciler.shutdown().await;
        }
        if let Some(dispatch) = self.command_dispatch.take() {
            dispatch.shutdown().await;
        }
        self.replicator
            .sessions()
            .clear_application()
            .map_err(NodeError::State)?;
        self.supervisor.shutdown_all().await?;
        self.replicator.shutdown().await?;
        Ok(())
    }
}

fn default_node_name(node_id: NodeId) -> String {
    let short_id = node_id.to_string().chars().take(8).collect::<String>();
    format!("myko-{short_id}")
}

fn load_peers(path: &Path) -> Result<BTreeMap<EndpointId, ConfiguredPeer>, NodeError> {
    let encoded = match fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    };
    let header = serde_json::from_slice::<StoredConfigHeader>(&encoded)?;
    let decoded_peers = match header.version {
        CONFIG_VERSION | PREVIOUS_CONFIG_VERSION => {
            serde_json::from_slice::<StoredPeerConfig>(&encoded)?.peers
        }
        LEGACY_CONFIG_VERSION => serde_json::from_slice::<LegacyStoredPeerConfig>(&encoded)?
            .peers
            .into_iter()
            .map(ConfiguredPeer::unpinned)
            .collect(),
        version => {
            return Err(NodeError::Configuration(format!(
                "unsupported peer configuration version {version}"
            )));
        }
    };
    let mut peers = BTreeMap::new();
    for peer in decoded_peers {
        if peers.insert(peer.endpoint.id, peer).is_some() {
            return Err(NodeError::Configuration(
                "peer configuration contains a duplicate Iroh identity".to_owned(),
            ));
        }
    }
    Ok(peers)
}
