//! Restartable native node composition for Myko 7.
//!
//! This crate combines the transport-neutral federation node, a durable Redb
//! journal, and the native Iroh transport. It owns operational identity and
//! peer-replication state, but deliberately knows nothing about an application's
//! commands, projections, workspace paths, or authorization model.

#![forbid(unsafe_code)]

mod authority;
mod discovery;
mod pairing;
mod peer;
mod status;

pub use authority::{AuthorityControllerAddress, AuthorityRuntimeConfig};
pub use discovery::{
    ConfigureLanDiscovery, DiscoveredNodeRow, DiscoverySettings, DiscoverySettingsReport,
    NearbyNodesView,
};
use discovery::{DiscoverySupervisor, DiscoveryViewState};
use pairing::PairingSupervisor;
pub use pairing::{
    ConfirmPairing, InitiateDiscoveredPairing, InitiatePairing, IssuePairingInvitation,
    PairingInitiation, PairingInitiationId, PairingInitiationPhase, PairingInitiationReport,
    PairingReceiptRow, PairingReceiptsView, PairingRedemption, PairingRedemptionId,
    PairingRedemptionPhase, PairingRedemptionReport, PendingPairingReceipt,
    PendingPairingReceiptId, RedeemPairingInvitation,
};
pub use peer::{
    AddPeer, AdvertiseServices, AdvertisedService, AdvertisedServiceId, AdvertisedServicesView,
    FederationService, GetAdvertisedServices, GetPeers, Peer, PeerId, PeerReport, PeersView,
    RememberPeer, RemovePeer, ServiceCapabilityReport, SetPeerReplication,
    SetPeerReplicationSelection, peer_id, peer_scope,
};
pub use status::{NodeStatus, NodeStatusView};
use status::{NodeStatusProjectionGuard, NodeStatusViewState, project_node_statuses};

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use myko::{
    ApplicationHost, CommandDispatchGuard, MykoApplication,
    client::{HandlerClientError, HandlerConnection, HandlerConnector, HandlerFrame, MykoClient},
    server::{FederatedSession, NodeFrameStream, NodeRequestRouter, NodeRouteFuture},
};
use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, ApplicationCapability, AuthorityConstraints,
    AuthorityPresentation, CapabilityId, CommandClient, CommandClientFuture, CommandId,
    CommandSnapshot, CommandSubmission, CommandSubscription, CommandSubscriptionFuture,
    CommandWatchFuture, CommandWatchingClient, DenyAllAccessPolicy, ItemQuery, ItemQueryResult,
    ItemQueryWatch, LiveSubscription, LiveSubscriptionState, Node as FederationNode,
    NodeError as FederationNodeError, NodeId, NodeStartupGuard, Principal, PrincipalId,
    ProvenanceOperation, ReconnectPolicy, ReplicationSelection, ScopeId, ServiceId,
    SubscriptionLiveness, live_subscription,
};
pub use myko_iroh::{
    EndpointAddr, EndpointId, NativeNodeDescriptor, NativePeerReference, PairingInvitation,
    PairingReceipt, SecretKey, endpoint_principal_id,
};
use myko_iroh::{
    IrohCommandClient, IrohCommandSubscription, IrohReplicationError, IrohReplicator,
    PeerSupervisor, load_or_create_secret_key,
};
use myko_redb::RedbJournal;
use myko_wire::{HandlerRequest, NodeFrame, NodeRequest, NodeRequestEnvelope};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;

const JOURNAL_FILE: &str = "node.redb";
const SECRET_FILE: &str = "iroh-secret.json";
const IROH_REPLICATOR_CAPABILITY_ID: &str = "myko.node.iroh-replicator";
const NODE_STATUS_CAPABILITY_ID: &str = "myko.node.status-runtime";
const DISCOVERY_CAPABILITY_ID: &str = "myko.node.discovery-runtime";

fn runtime_resource_capability(
    id: &'static str,
    description: &'static str,
) -> ApplicationCapability {
    ApplicationCapability {
        id: CapabilityId::new(id),
        description: description.to_owned(),
        constraints: AuthorityConstraints::default(),
    }
}

pub(crate) fn iroh_replicator_capability_id() -> CapabilityId {
    CapabilityId::new(IROH_REPLICATOR_CAPABILITY_ID)
}

pub(crate) fn node_status_capability_id() -> CapabilityId {
    CapabilityId::new(NODE_STATUS_CAPABILITY_ID)
}

pub(crate) fn discovery_capability_id() -> CapabilityId {
    CapabilityId::new(DISCOVERY_CAPABILITY_ID)
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
    #[serde(default = "default_peer_replication")]
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_selection: ReplicationSelection,
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
    Application(#[from] myko::AppError),
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

/// A restartable native Myko node.
///
/// The data directory is the node's operational identity boundary. Opening the
/// same directory restores its event identity, transport identity, and every
/// configured source-aware peer relationship.
pub struct Node {
    data_dir: PathBuf,
    federation: FederationNode,
    application: ApplicationHost,
    journal: Arc<RedbJournal>,
    replicator: IrohReplicator,
    request_router: Arc<FederationRouter>,
    command_dispatch: Option<CommandDispatchGuard>,
    certified_authority: Option<myko_authority::certified::PreparedAuthorityGuard>,
    supervisor: Arc<PeerSupervisor>,
    peer_reconciler: Option<PeerReconcilerGuard>,
    pairing: Option<PairingSupervisor>,
    status_projection: Option<NodeStatusProjectionGuard>,
    discovery: Option<DiscoverySupervisor>,
}

/// Command facade that keeps node identity routing out of application code.
#[derive(Clone)]
pub struct NodeCommandClient {
    transport: NodeCommandTransport,
}

#[derive(Clone)]
enum NodeCommandTransport {
    Embedded {
        client: Box<ApplicationHost>,
        authority: Option<AuthorityPresentation>,
    },
    Iroh(Box<IrohCommandClient>),
}

/// Current-then-live lifecycle returned by [`NodeCommandClient`].
pub struct NodeCommandSubscription {
    transport: NodeCommandSubscriptionTransport,
}

enum NodeCommandSubscriptionTransport {
    Embedded(myko_federation::CommandWatch),
    Iroh(IrohCommandSubscription),
}

impl CommandClient for NodeCommandClient {
    type Error = NodeError;

    fn submit_submission(
        &self,
        submission: CommandSubmission,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            match &self.transport {
                NodeCommandTransport::Embedded { client, authority } => {
                    if let Some(authority) = authority {
                        client
                            .submit_authorized_submission(authority.clone(), submission)
                            .map_err(NodeError::Federation)
                    } else {
                        client
                            .submit_submission(submission)
                            .await
                            .map_err(NodeError::Federation)
                    }
                }
                NodeCommandTransport::Iroh(client) => client
                    .submit_submission(submission)
                    .await
                    .map_err(NodeError::Iroh),
            }
        })
    }

    fn command_state(&self, command_id: CommandId) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            match &self.transport {
                NodeCommandTransport::Embedded { client, .. } => client
                    .command_state(command_id)
                    .await
                    .map_err(NodeError::Federation),
                NodeCommandTransport::Iroh(client) => client
                    .command_state(command_id)
                    .await
                    .map_err(NodeError::Iroh),
            }
        })
    }

    fn cancel_command(
        &self,
        command_id: CommandId,
        reason: String,
    ) -> CommandClientFuture<'_, Self::Error> {
        Box::pin(async move {
            match &self.transport {
                NodeCommandTransport::Embedded { client, .. } => client
                    .cancel_command(command_id, reason)
                    .await
                    .map_err(NodeError::Federation),
                NodeCommandTransport::Iroh(client) => client
                    .cancel_command(command_id, reason)
                    .await
                    .map_err(NodeError::Iroh),
            }
        })
    }
}

impl CommandSubscription for NodeCommandSubscription {
    type Error = NodeError;

    fn current(&self) -> &CommandSnapshot {
        match &self.transport {
            NodeCommandSubscriptionTransport::Embedded(subscription) => subscription.current(),
            NodeCommandSubscriptionTransport::Iroh(subscription) => subscription.current(),
        }
    }

    fn recv(&mut self) -> CommandSubscriptionFuture<'_, Self::Error> {
        Box::pin(async move {
            match &mut self.transport {
                NodeCommandSubscriptionTransport::Embedded(subscription) => {
                    CommandSubscription::recv(subscription)
                        .await
                        .map_err(NodeError::Federation)
                }
                NodeCommandSubscriptionTransport::Iroh(subscription) => {
                    CommandSubscription::recv(subscription)
                        .await
                        .map_err(NodeError::Iroh)
                }
            }
        })
    }
}

impl CommandWatchingClient for NodeCommandClient {
    type Subscription = NodeCommandSubscription;

    fn watch_command(
        &self,
        command_id: CommandId,
    ) -> CommandWatchFuture<'_, Self::Subscription, Self::Error> {
        Box::pin(async move {
            match &self.transport {
                NodeCommandTransport::Embedded { client, .. } => {
                    CommandWatchingClient::watch_command(client.as_ref(), command_id)
                        .await
                        .map(|subscription| NodeCommandSubscription {
                            transport: NodeCommandSubscriptionTransport::Embedded(subscription),
                        })
                        .map_err(NodeError::Federation)
                }
                NodeCommandTransport::Iroh(client) => {
                    CommandWatchingClient::watch_command(client.as_ref(), command_id)
                        .await
                        .map(|subscription| NodeCommandSubscription {
                            transport: NodeCommandSubscriptionTransport::Iroh(subscription),
                        })
                        .map_err(NodeError::Iroh)
                }
            }
        })
    }
}

#[derive(Clone)]
struct EmbeddedHandlerConnector {
    sessions: FederatedSession,
    local_node: NodeId,
    destination: Option<NodeId>,
    authority: Option<AuthorityPresentation>,
}

struct EmbeddedHandlerConnection {
    frames: NodeFrameStream,
}

#[async_trait::async_trait]
impl HandlerConnection for EmbeddedHandlerConnection {
    async fn recv(&mut self) -> Result<HandlerFrame, HandlerClientError> {
        next_embedded_handler_frame(&mut self.frames).await
    }
}

#[async_trait::async_trait]
impl HandlerConnector for EmbeddedHandlerConnector {
    async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
        Ok(self.destination.unwrap_or(self.local_node))
    }

    async fn connect(
        &self,
        request: HandlerRequest,
    ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
        let mut frames = self
            .sessions
            .open_authenticated(
                self.authority.as_ref().map_or_else(
                    || Principal::node(PrincipalId::for_node(self.local_node)),
                    |authority| authority.executor.clone(),
                ),
                NodeRequestEnvelope {
                    destination: self.destination,
                    authority: self.authority.clone(),
                    forwarding_provenance: Vec::new(),
                    request: NodeRequest::FollowHandler { request },
                },
            )
            .await;
        let initial = next_embedded_handler_frame(&mut frames).await?;
        Ok((initial, Box::new(EmbeddedHandlerConnection { frames })))
    }

    fn at(&self, destination: NodeId) -> Arc<dyn HandlerConnector> {
        Arc::new(Self {
            sessions: self.sessions.clone(),
            local_node: self.local_node,
            destination: Some(destination),
            authority: self.authority.clone(),
        })
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        ReconnectPolicy::default()
    }
}

async fn next_embedded_handler_frame(
    frames: &mut NodeFrameStream,
) -> Result<HandlerFrame, HandlerClientError> {
    loop {
        match frames.recv().await {
            Some(NodeFrame::Authorization { decision })
                if matches!(
                    decision.as_ref(),
                    myko_federation::AuthorizationDecision::Permit(_)
                ) => {}
            Some(NodeFrame::Authorization { decision }) => {
                return Err(HandlerClientError::Protocol(decision.public_message()));
            }
            Some(NodeFrame::HandlerState { revision, state }) => {
                return Ok(HandlerFrame::State {
                    revision,
                    state: *state,
                });
            }
            Some(NodeFrame::HandlerViewDelta { revision, delta }) => {
                return Ok(HandlerFrame::ViewDelta {
                    revision,
                    delta: *delta,
                });
            }
            Some(NodeFrame::Error { message }) => {
                return Err(HandlerClientError::Protocol(message));
            }
            Some(frame) => {
                return Err(HandlerClientError::Protocol(format!(
                    "embedded handler stream returned {}",
                    frame.kind()
                )));
            }
            None => {
                return Err(HandlerClientError::Transport(
                    "embedded handler stream ended".to_owned(),
                ));
            }
        }
    }
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
            tracing::debug!(peer_count = current.len(), "peer reconciler started");
            loop {
                let update = match watch.recv_async().await {
                    Ok(update) => update,
                    Err(error) => {
                        tracing::error!(%error, "peer configuration subscription failed");
                        context
                            .status
                            .invalidate(format!("peer configuration subscription failed: {error}"));
                        return;
                    }
                };
                let desired = configured_peer_map(&update.value);
                tracing::debug!(
                    current_count = current.len(),
                    desired_count = desired.len(),
                    through = ?update.position,
                    "peer configuration changed"
                );
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
                    tracing::error!(%error, "peer reconciliation failed");
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
    tracing::debug!(
        endpoint_id = %peer.endpoint.id,
        source_node = ?peer.source_node,
        selection = ?peer.replication_selection,
        "starting configured peer follower"
    );
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
            tracing::debug!(endpoint_id = %peer_id, "stopping removed or changed peer follower");
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
        mut envelope: NodeRequestEnvelope,
        frames: &'a flume::Sender<NodeFrame>,
    ) -> NodeRouteFuture<'a> {
        Box::pin(async move {
            let destination = envelope
                .destination
                .ok_or_else(|| "routed request omitted its destination".to_owned())?;
            let peer = self.peer(destination)?;
            let mut presentation = envelope
                .authority
                .take()
                .ok_or_else(|| "routed request omitted its validated authority".to_owned())?;
            let forwarding_executor =
                Principal::node(endpoint_principal_id(self.replicator.address().id));
            if envelope.forwarding_provenance.is_empty() {
                return Err(
                    "routed request omitted an attenuating node-forward delegation".to_owned(),
                );
            }
            let hop = envelope.forwarding_provenance.remove(0);
            if hop.delegator != presentation.executor
                || hop.delegate != forwarding_executor
                || hop.operation
                    != (ProvenanceOperation::NodeForward {
                        node_id: self.node.node_id().to_string(),
                    })
            {
                return Err(
                    "node-forward delegation does not match the authenticated route".to_owned(),
                );
            }
            presentation = presentation.forward(hop);
            envelope.authority = Some(presentation);
            self.replicator
                .forward_request(peer, envelope, frames)
                .await
                .map_err(|error| error.to_string())
        })
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

fn ensure_discovery_settings(
    node: &FederationNode,
    application: &ApplicationHost,
) -> Result<(), NodeError> {
    let discovery_id = discovery::DiscoverySettingsId::from(node.node_id().to_string());
    let settings = node.query_items_in(
        node.node_id(),
        &peer_scope(node.node_id()),
        discovery::GetDiscoverySettingsById { id: discovery_id },
    )?;
    if settings.is_empty() {
        let _configured = application.exec_command(ConfigureLanDiscovery {
            display_name: default_node_name(node.node_id()),
            enabled: true,
        })?;
    }
    Ok(())
}

fn ensure_advertised_services(
    node: &FederationNode,
    application: &ApplicationHost,
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
    retry_interval: Duration,
    node: &FederationNode,
    journal: &Arc<RedbJournal>,
    application: &ApplicationHost,
    replicator: &IrohReplicator,
) -> Result<FederationRuntimeParts, NodeError> {
    tracing::debug!(node_id = %node.node_id(), "initializing federation runtime");
    ensure_discovery_settings(node, application)?;

    let (peer_snapshot, peer_watch) =
        node.watch_items_in(node.node_id(), peer_scope(node.node_id()), GetPeers)?;
    let configured = configured_peer_map(&peer_snapshot.value);
    tracing::debug!(
        node_id = %node.node_id(),
        configured_peers = configured.len(),
        "loaded durable peer configuration"
    );
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

    tracing::debug!(node_id = %node.node_id(), "federation runtime initialized");

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
            Ok(Arc::new(DenyAllAccessPolicy))
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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
            Ok(Arc::new(DenyAllAccessPolicy))
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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

    /// Opens an identity-backed application node with its transport held below
    /// the startup barrier until the returned guard is completed.
    ///
    /// Platforms with a secure key store can restore their endpoint identity
    /// while still preventing requests from reaching partially initialized
    /// application resources and supervisors.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, endpoint
    /// binding, application serving, or peer restoration fails.
    pub async fn open_application_starting_with_identity_and_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        identity: SecretKey,
        application: MykoApplication,
        resolve_policy: F,
    ) -> Result<(Self, NodeStartupGuard), NodeError>
    where
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        let (node, startup) = Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            Some(identity),
            Some(application),
            true,
            resolve_policy,
        )
        .await?;
        let startup = startup.ok_or_else(|| {
            NodeError::State("identity-backed starting node omitted its startup guard".to_owned())
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
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

    #[allow(clippy::too_many_lines)]
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
        F: FnOnce(&ApplicationHost) -> Result<Arc<dyn AccessPolicy>, NodeError>,
    {
        tracing::info!(
            data_dir = %data_dir.display(),
            bind_mode = ?bind_mode,
            hold_startup,
            "opening Myko node"
        );
        fs::create_dir_all(data_dir)?;
        let secret = match identity {
            Some(identity) => identity,
            None => load_or_create_secret_key(data_dir.join(SECRET_FILE))?,
        };
        let (node, journal) = RedbJournal::open_node_with_journal(data_dir.join(JOURNAL_FILE))?;
        tracing::debug!(node_id = %node.node_id(), "durable journal opened");
        let runtime_startup = node.hold_startup();
        let startup = hold_startup.then(|| node.hold_startup());
        let application = application
            .unwrap_or_default()
            .with_framework_service::<FederationService>()
            .with_framework_resource_capability::<IrohReplicator>(runtime_resource_capability(
                IROH_REPLICATOR_CAPABILITY_ID,
                "access the authenticated native peer transport",
            ))?
            .with_framework_resource_capability::<NodeStatusViewState>(
                runtime_resource_capability(
                    NODE_STATUS_CAPABILITY_ID,
                    "read the native node status runtime",
                ),
            )?
            .with_framework_resource_capability::<DiscoveryViewState>(
                runtime_resource_capability(
                    DISCOVERY_CAPABILITY_ID,
                    "read the native LAN discovery runtime",
                ),
            )?;
        let advertised_services = application
            .services()
            .map(|service| ServiceId::new(service.as_str()))
            .collect::<Vec<_>>();
        let application =
            ApplicationHost::new(node.clone(), application).map_err(NodeError::Configuration)?;
        tracing::debug!(
            node_id = %node.node_id(),
            service_count = advertised_services.len(),
            "application services attached"
        );
        // No transport or command driver exists during this bounded bootstrap
        // window. Framework-owned node configuration is written through the
        // ordinary command journal before the externally supplied policy is
        // installed and the endpoint begins serving.
        let startup_policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
        node.set_command_access_policy(startup_policy.clone())?;
        ensure_advertised_services(&node, &application, advertised_services)?;
        let initial_policy = resolve_policy(&application)?;
        let replicator = match bind_mode {
            BindMode::Network => {
                IrohReplicator::bind_application_with_secret_and_policy(
                    application.clone(),
                    secret,
                    initial_policy.clone(),
                )
                .await
            }
            BindMode::Loopback => {
                IrohReplicator::bind_loopback_application_with_secret_and_policy(
                    application.clone(),
                    secret,
                    initial_policy.clone(),
                )
                .await
            }
        }?;
        tracing::debug!(
            node_id = %node.node_id(),
            endpoint_id = %replicator.address().id,
            "native peer transport bound"
        );
        node.set_command_access_policy(startup_policy.clone())?;
        let FederationRuntimeParts {
            request_router,
            supervisor,
            peer_reconciler,
            peers,
            node_status,
            discovery,
        } = initialize_federation_runtime(
            retry_interval,
            &node,
            &journal,
            &application,
            &replicator,
        )
        .await?;
        replicator.set_access_policy(initial_policy)?;
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
        runtime_startup.ready();
        tracing::info!(
            node_id = %node.node_id(),
            endpoint_id = %replicator.address().id,
            externally_held = hold_startup,
            "Myko node runtime is ready"
        );
        Ok((
            Self {
                data_dir: data_dir.to_path_buf(),
                federation: node,
                application,
                journal,
                replicator,
                request_router,
                command_dispatch: Some(command_dispatch),
                certified_authority: None,
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
    pub const fn application(&self) -> &ApplicationHost {
        &self.application
    }

    /// Returns the retained application client routed to one node identity.
    ///
    /// The local identity uses the in-process session boundary. Configured
    /// peers use their identity-pinned Iroh endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the target is not this node or a configured peer.
    pub fn application_client(&self, source_node: NodeId) -> Result<MykoClient, NodeError> {
        self.application_client_routed(source_node, None)
    }

    /// Returns a retained application client with an explicit authority presentation.
    ///
    /// This is the in-process boundary for application-owned principals such
    /// as a local human owner. The presentation's executor becomes the
    /// authenticated embedded principal and must already match a remote Iroh
    /// transport identity when routing to a peer.
    ///
    /// # Errors
    ///
    /// Returns an error when the target is not this node or a configured peer.
    pub fn application_client_with_authority(
        &self,
        source_node: NodeId,
        authority: AuthorityPresentation,
    ) -> Result<MykoClient, NodeError> {
        self.application_client_routed(source_node, Some(authority))
    }

    fn application_client_routed(
        &self,
        source_node: NodeId,
        authority: Option<AuthorityPresentation>,
    ) -> Result<MykoClient, NodeError> {
        if source_node == self.federation.node_id() {
            return Ok(MykoClient::with_handler_connector(Arc::new(
                EmbeddedHandlerConnector {
                    sessions: self.sessions().clone(),
                    local_node: source_node,
                    destination: None,
                    authority,
                },
            )));
        }
        let peer = self
            .request_router
            .peer(source_node)
            .map_err(NodeError::Route)?;
        let connector = self.replicator.handler_connector(peer);
        Ok(match authority {
            Some(authority) => connector.with_authority(authority).client(),
            None => connector.client(),
        })
    }

    /// Returns a command client routed to one node identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the target is not this node or a configured peer.
    pub fn command_client(&self, source_node: NodeId) -> Result<NodeCommandClient, NodeError> {
        self.command_client_routed(source_node, None)
    }

    /// Returns a node-routed command client with an explicit authority presentation.
    ///
    /// # Errors
    ///
    /// Returns an error when the target is not this node or a configured peer.
    pub fn command_client_with_authority(
        &self,
        source_node: NodeId,
        authority: AuthorityPresentation,
    ) -> Result<NodeCommandClient, NodeError> {
        self.command_client_routed(source_node, Some(authority))
    }

    fn command_client_routed(
        &self,
        source_node: NodeId,
        authority: Option<AuthorityPresentation>,
    ) -> Result<NodeCommandClient, NodeError> {
        if source_node == self.federation.node_id() {
            return Ok(NodeCommandClient {
                transport: NodeCommandTransport::Embedded {
                    client: Box::new(self.application.clone()),
                    authority,
                },
            });
        }
        let peer = self
            .request_router
            .peer(source_node)
            .map_err(NodeError::Route)?;
        let client = self.replicator.command_client(peer);
        Ok(NodeCommandClient {
            transport: NodeCommandTransport::Iroh(Box::new(match authority {
                Some(authority) => client.with_authority(authority),
                None => client,
            })),
        })
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
    ) -> Result<ReactiveItemSubscription<ItemQueryResult<Q>>, NodeError>
    where
        Q: ItemQuery + Send + 'static,
        ItemQueryResult<Q>: hyphae::CellValue,
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
    pub const fn sessions(&self) -> &FederatedSession {
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
        let node_id = self.federation.node_id();
        tracing::info!(%node_id, "shutting down Myko node");
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
        if let Some(authority) = self.certified_authority.take() {
            authority.shutdown().await.map_err(NodeError::State)?;
        }
        self.replicator
            .sessions()
            .clear_access_policy()
            .map_err(NodeError::State)?;
        self.replicator
            .sessions()
            .clear_application()
            .map_err(NodeError::State)?;
        self.supervisor.shutdown_all().await?;
        self.replicator.shutdown().await?;
        tracing::info!(%node_id, "Myko node stopped");
        Ok(())
    }
}

fn default_node_name(node_id: NodeId) -> String {
    let short_id = node_id.to_string().chars().take(8).collect::<String>();
    format!("myko-{short_id}")
}
