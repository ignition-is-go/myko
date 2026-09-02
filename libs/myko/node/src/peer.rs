//! Framework-owned peer configuration, commands, and live projections.

use std::{collections::BTreeSet, sync::Arc};

use myko_app::capability::{
    CollectionBuilding as _, CommandQuerying as _, EventPublishing as _, NodeScoped as _,
    Querying as _,
};
use myko_app::{
    AppError, CommandContext, CommandError, CommandHandler, QueryHandler, ReportContext,
    ReportHandler, ViewContext, ViewHandler, myko_query, myko_report, myko_view,
};
use myko_federation::{
    ItemProjection, ItemQuery, LiveCollection, LiveSubscription, LogPosition, NodeId,
    ReplicationSelection, ScopeId,
};
use myko_iroh::{EndpointAddr, EndpointId, NativeNodeDescriptor, NativePeerReference};
use myko_items::MykoService;
use myko_items::{myko_command, myko_item, myko_service};

use crate::{DiscoverySettings, PairingInitiation, PairingRedemption, PendingPairingReceipt};

/// Myko's built-in node-federation service.
#[myko_service(
    PeerRoster,
    Peer,
    AdvertisedService,
    PairingInitiation,
    PairingRedemption,
    PendingPairingReceipt,
    DiscoverySettings
)]
pub struct FederationService;

/// Durable marker anchoring one node's local peer-configuration scope.
#[myko_item(service = FederationService, scope_root)]
pub struct PeerRoster {}

/// One desired, node-local native peer relationship.
///
/// This is configuration state, not replicated application authority. The
/// native runtime watches the local node's peer scope and reconciles Iroh
/// replication to these durable values.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct Peer {
    pub endpoint: EndpointAddr,
    pub source_node: Option<NodeId>,
    #[serde(alias = "following")]
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_selection: ReplicationSelection,
}

impl Eq for Peer {}

/// One typed application service executable by an authoritative Myko node.
///
/// Nodes publish this framework-owned catalog from their composed
/// [`myko_app::MykoApplication`]. Followers therefore learn routing
/// capabilities through the same durable, reactive federation log as every
/// other Myko item.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct AdvertisedService {
    pub service_id: myko_federation::ServiceId,
}

impl AdvertisedService {
    /// Returns whether this row advertises the given typed service.
    #[must_use]
    pub fn is<S: MykoService>(&self) -> bool {
        self.service_id.as_str() == S::SERVICE_ID.as_str()
    }
}

/// Adds or replaces a peer and enables directional history replication.
#[myko_command(Peer, item = Peer)]
pub struct AddPeer {
    pub reference: NativePeerReference,
}

/// Remembers a mutually authenticated descriptor without implicitly enabling
/// replication. An existing replication decision is preserved.
#[myko_command(Peer, item = Peer)]
pub struct RememberPeer {
    pub descriptor: NativeNodeDescriptor,
}

/// Changes this node's independent replication decision for one peer.
#[myko_command(Peer, item = Peer)]
pub struct SetPeerReplication {
    pub peer_id: PeerId,
    pub enabled: bool,
}

/// Changes which service history this node copies from one remembered peer.
#[myko_command(Peer, item = Peer)]
pub struct SetPeerReplicationSelection {
    pub peer_id: PeerId,
    pub selection: ReplicationSelection,
}

/// Removes one remembered peer relationship from this node.
#[myko_command(item = Peer)]
pub struct RemovePeer {
    pub peer_id: PeerId,
}

#[myko_command(Peer, item = Peer)]
pub struct RestorePeer {
    pub endpoint: EndpointAddr,
    pub source_node: Option<NodeId>,
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_selection: ReplicationSelection,
}

/// Reconciles the typed services executable by this node.
///
/// Native node composition issues this command whenever an application is
/// opened. Services removed by a new binary are deleted from the durable
/// catalog; newly compiled services are added atomically.
#[myko_command(Vec<AdvertisedService>, item = AdvertisedService)]
pub struct AdvertiseServices {
    pub services: Vec<myko_federation::ServiceId>,
}

impl CommandHandler for AddPeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        let peer = peer_from_reference(
            self.reference,
            true,
            PeerRosterId::from(context.node_id().to_string()),
        )
        .map_err(CommandError::reject)?;
        reject_self_history(&context, &peer)?;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for RememberPeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        self.descriptor.validate().map_err(CommandError::reject)?;
        let id = peer_id(self.descriptor.endpoint.id);
        let existing = context.exec_query(GetPeerById { id: id.clone() })?;
        let peer = Peer {
            id,
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            endpoint: self.descriptor.endpoint,
            source_node: Some(self.descriptor.node_id),
            replication_enabled: existing
                .as_ref()
                .is_some_and(|peer| peer.replication_enabled),
            replication_selection: existing
                .map(|peer| peer.replication_selection)
                .unwrap_or_default(),
        };
        reject_self_history(&context, &peer)?;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for SetPeerReplication {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        let Some(mut peer) = context.exec_query(GetPeerById {
            id: self.peer_id.clone(),
        })?
        else {
            return Err(CommandError::reject("peer is not remembered"));
        };
        peer.replication_enabled = self.enabled;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for SetPeerReplicationSelection {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        let Some(mut peer) = context.exec_query(GetPeerById {
            id: self.peer_id.clone(),
        })?
        else {
            return Err(CommandError::reject("peer is not remembered"));
        };
        peer.replication_selection = self.selection;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for RemovePeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<(), CommandError> {
        context.emit_delete::<Peer>(&self.peer_id)
    }
}

impl CommandHandler for RestorePeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        let peer = Peer {
            id: peer_id(self.endpoint.id),
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            endpoint: self.endpoint,
            source_node: self.source_node,
            replication_enabled: self.replication_enabled,
            replication_selection: self.replication_selection,
        };
        reject_self_history(&context, &peer)?;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for AdvertiseServices {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Vec<AdvertisedService>, CommandError> {
        let mut services = self
            .services
            .into_iter()
            .map(|service_id| service_id.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        if services.remove("") {
            return Err(CommandError::reject(
                "an advertised Myko service ID cannot be empty",
            ));
        }
        let existing = context.exec_query(GetAdvertisedServices)?;
        for service in existing {
            if !services.contains(service.service_id.as_str()) {
                context.emit_delete::<AdvertisedService>(&service.id)?;
            }
        }
        let peer_roster_id = PeerRosterId::from(context.node_id().to_string());
        context.emit_set(&PeerRoster {
            id: peer_roster_id.clone(),
        })?;
        let advertised = services
            .into_iter()
            .map(|service_id| AdvertisedService {
                id: AdvertisedServiceId::from(service_id.as_str()),
                peer_roster_id: peer_roster_id.clone(),
                service_id: myko_federation::ServiceId::new(service_id),
            })
            .collect::<Vec<_>>();
        for service in &advertised {
            context.emit_set(service)?;
        }
        Ok(advertised)
    }
}

fn reject_self_history(
    context: &CommandContext<FederationService, PeerRoster>,
    peer: &Peer,
) -> Result<(), CommandError> {
    if peer.source_node == Some(context.node_id()) {
        return Err(CommandError::reject(
            "a node cannot configure its own Myko history as a peer",
        ));
    }
    Ok(())
}

fn emit_peer(
    context: &CommandContext<FederationService, PeerRoster>,
    peer: &Peer,
) -> Result<(), CommandError> {
    context.emit_set(&PeerRoster {
        id: PeerRosterId::from(context.node_id().to_string()),
    })?;
    context.emit_set(peer)
}

fn peer_from_reference(
    reference: NativePeerReference,
    replication_enabled: bool,
    peer_roster_id: PeerRosterId,
) -> Result<Peer, String> {
    let (endpoint, source_node) = match reference {
        NativePeerReference::Descriptor(descriptor) => {
            descriptor.validate()?;
            (descriptor.endpoint, Some(descriptor.node_id))
        }
        NativePeerReference::LegacyEndpoint(endpoint) => (endpoint, None),
    };
    Ok(Peer {
        id: peer_id(endpoint.id),
        peer_roster_id,
        endpoint,
        source_node,
        replication_enabled,
        replication_selection: ReplicationSelection::All,
    })
}

/// Returns every configured peer in stable endpoint-identity order.
#[myko_query(Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct GetPeers;

impl ItemQuery for GetPeers {
    type Item = Peer;
    type Output = Vec<Peer>;

    fn execute(self, projection: &ItemProjection<Peer>) -> Self::Output {
        let mut peers = projection.values().cloned().collect::<Vec<_>>();
        peers.sort_by(|left, right| left.id.cmp(&right.id));
        peers
    }
}

impl QueryHandler for GetPeers {}

/// Returns every typed service advertised by one node in stable ID order.
#[myko_query(AdvertisedService)]
#[derive(Copy, PartialEq, Eq)]
pub struct GetAdvertisedServices;

impl ItemQuery for GetAdvertisedServices {
    type Item = AdvertisedService;
    type Output = Vec<AdvertisedService>;

    fn execute(self, projection: &ItemProjection<AdvertisedService>) -> Self::Output {
        let mut services = projection.values().cloned().collect::<Vec<_>>();
        services.sort_by(|left, right| left.service_id.as_str().cmp(right.service_id.as_str()));
        services
    }
}

impl QueryHandler for GetAdvertisedServices {}

/// Reactive configured-peer roster for one authoritative node.
#[myko_view(Peer, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct PeersView {
    pub source_node: NodeId,
}

impl ViewHandler for PeersView {
    type Item = Peer;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn item_key(item: &Self::Item) -> Arc<str> {
        Arc::from(item.id.to_string())
    }

    fn build(&self, context: &ViewContext) -> Result<LiveCollection<Self::Item>, AppError> {
        let live = context
            .query(self.source_node, peer_scope(self.source_node), GetPeers)?
            .map_value(Clone::clone);
        context.collection_from_subscription(&live, Self::item_key)
    }
}

/// Reactive catalog of typed services executable by one authoritative node.
#[myko_view(AdvertisedService, item = AdvertisedService)]
#[derive(Copy, PartialEq, Eq)]
pub struct AdvertisedServicesView {
    pub source_node: NodeId,
}

impl ViewHandler for AdvertisedServicesView {
    type Item = AdvertisedService;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn item_key(item: &Self::Item) -> Arc<str> {
        Arc::from(item.service_id.as_str())
    }

    fn build(&self, context: &ViewContext) -> Result<LiveCollection<Self::Item>, AppError> {
        let live = context
            .query(
                self.source_node,
                peer_scope(self.source_node),
                GetAdvertisedServices,
            )?
            .map_value(Clone::clone);
        context.collection_from_subscription(&live, Self::item_key)
    }
}

/// Reactive state of one configured peer.
#[myko_report(Option<Peer>, item = Peer)]
#[derive(PartialEq, Eq)]
pub struct PeerReport {
    pub source_node: NodeId,
    pub peer_id: PeerId,
}

impl ReportHandler for PeerReport {
    type Output = Option<Peer>;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build(&self, context: &ReportContext) -> Result<LiveSubscription<Self::Output>, AppError> {
        let peer_id = self.peer_id.clone();
        Ok(context
            .query(self.source_node, peer_scope(self.source_node), GetPeers)?
            .map_value(move |peers| peers.iter().find(|peer| peer.id == peer_id).cloned()))
    }
}

/// Reactive capability check for one service on one authoritative node.
#[myko_report(bool, item = AdvertisedService)]
#[derive(PartialEq, Eq)]
pub struct ServiceCapabilityReport {
    pub source_node: NodeId,
    pub service_id: myko_federation::ServiceId,
}

impl ServiceCapabilityReport {
    /// Builds a capability report without exposing a textual service ID to
    /// application code.
    #[must_use]
    pub fn for_service<S: MykoService>(source_node: NodeId) -> Self {
        Self {
            source_node,
            service_id: myko_federation::ServiceId::new(S::SERVICE_ID),
        }
    }
}

impl ReportHandler for ServiceCapabilityReport {
    type Output = bool;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build(&self, context: &ReportContext) -> Result<LiveSubscription<Self::Output>, AppError> {
        let service_id = self.service_id.clone();
        Ok(context
            .query(
                self.source_node,
                peer_scope(self.source_node),
                GetAdvertisedServices,
            )?
            .map_value(move |services| {
                services
                    .iter()
                    .any(|service| service.service_id == service_id)
            }))
    }
}

/// Returns the durable peer ID associated with an authenticated Iroh endpoint.
#[must_use]
pub fn peer_id(endpoint_id: EndpointId) -> PeerId {
    PeerId::from(endpoint_id.to_string())
}

/// Returns the node-local scope containing desired peer relationships.
#[must_use]
pub fn peer_scope(node_id: NodeId) -> ScopeId {
    ScopeId::for_item::<PeerRoster>(&PeerRosterId::from(node_id.to_string()))
}
