//! Framework-owned peer configuration, commands, and live projections.

#![allow(clippy::expect_used)] // Infallible handler builders rely on validated host wiring.

use std::{collections::BTreeSet, sync::Arc};

use hyphae::{Definite, MapExt as _, Materialize};
use myko::{
    CommandContext, CommandError, CommandHandler, myko_query, myko_report, myko_view,
    query::{QueryBuildArgs, QueryHandler},
    report::{ReportContext, ReportHandler},
    view::{ViewBuildArgs, ViewHandler},
};
use myko_federation::{
    AccessOperation, FederationPermission, NodeId, ReplicationSelection, ResourceClaim,
    ResourceClaimKind, ScopeId, ScopeSelection, ServiceId,
};
use myko_iroh::{EndpointAddr, EndpointId, NativeNodeDescriptor, NativePeerReference};
use myko_items::{MykoItem, MykoService, myko_command, myko_item, myko_service};

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
myko::register_federated_item!(PeerRoster);

/// One desired, node-local native peer relationship.
///
/// This is configuration state, not replicated application authority. The
/// native runtime watches the local node's peer scope and reconciles Iroh
/// replication to these durable values.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct Peer {
    pub endpoint: EndpointAddr,
    pub source_node: Option<NodeId>,
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_selection: ReplicationSelection,
}
myko::register_federated_item!(Peer);

impl Eq for Peer {}

/// One typed application service executable by an authoritative Myko node.
///
/// Nodes publish this framework-owned catalog from their composed
/// [`myko::MykoApplication`]. Followers therefore learn routing
/// capabilities through the same durable, reactive federation log as every
/// other Myko item.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct AdvertisedService {
    pub service_id: myko_federation::ServiceId,
}
myko::register_federated_item!(AdvertisedService);

impl AdvertisedService {
    /// Returns whether this row advertises the given typed service.
    #[must_use]
    pub fn is<S: MykoService>(&self) -> bool {
        self.service_id == S::SERVICE_ID
    }
}

/// Adds or replaces a peer and enables directional history replication.
#[myko_command(Peer, item = Peer)]
pub struct AddPeer {
    pub reference: NativePeerReference,
}

/// Remembers a mutually authenticated descriptor and enables grant-filtered
/// replication. This does not grant the peer access to local data.
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

/// Reconciles the typed services executable by this node.
///
/// Native node composition issues this command whenever an application is
/// opened. Services removed by a new binary are deleted from the durable
/// catalog; newly compiled services are added atomically.
#[myko_command(Vec<AdvertisedService>, item = AdvertisedService)]
pub struct AdvertiseServices {
    pub services: Vec<myko_federation::ServiceId>,
}

pub fn peer_roster_claims(node_id: NodeId, item_type: &'static str) -> Vec<ResourceClaim> {
    let selection = ScopeSelection::Exact(peer_scope(node_id));
    vec![
        ResourceClaim {
            selection: selection.clone(),
            kind: ResourceClaimKind::Primary,
            source_node: None,
            service_id: Some(ServiceId::new(FederationService::SERVICE_ID)),
            item_type: Some(item_type.to_owned()),
            item_id: None,
            required_permissions: vec![FederationPermission::ReadState],
            required_operations: vec![AccessOperation::ReadItems],
            required_capabilities: Vec::new(),
        },
        ResourceClaim {
            selection,
            kind: ResourceClaimKind::Affected,
            source_node: None,
            service_id: Some(ServiceId::new(FederationService::SERVICE_ID)),
            item_type: Some(PeerRoster::ITEM_TYPE.to_owned()),
            item_id: None,
            required_permissions: vec![FederationPermission::Write],
            required_operations: Vec::new(),
            required_capabilities: Vec::new(),
        },
    ]
}

impl CommandHandler for AddPeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, Peer::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<Peer, CommandError> {
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

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, Peer::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<Peer, CommandError> {
        self.descriptor.validate().map_err(CommandError::reject)?;
        let id = peer_id(self.descriptor.endpoint.id);
        let existing = context
            .exec_item_query(GetPeersByIds {
                ids: vec![id.clone()],
            })?
            .into_iter()
            .next();
        let peer = Peer {
            id,
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            endpoint: self.descriptor.endpoint,
            source_node: Some(self.descriptor.node_id),
            replication_enabled: true,
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

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, Peer::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<Peer, CommandError> {
        let Some(mut peer) = context
            .exec_item_query(GetPeersByIds {
                ids: vec![self.peer_id.clone()],
            })?
            .into_iter()
            .next()
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

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, Peer::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<Peer, CommandError> {
        let Some(mut peer) = context
            .exec_item_query(GetPeersByIds {
                ids: vec![self.peer_id.clone()],
            })?
            .into_iter()
            .next()
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

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, Peer::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
        context.emit_delete::<Peer>(&self.peer_id)
    }
}

impl CommandHandler for AdvertiseServices {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        peer_roster_claims(node_id, AdvertisedService::ITEM_TYPE)
    }

    fn execute(self, context: CommandContext) -> Result<Vec<AdvertisedService>, CommandError> {
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
        let existing = context.exec_item_query(GetAdvertisedServices)?;
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

fn reject_self_history(context: &CommandContext, peer: &Peer) -> Result<(), CommandError> {
    if peer.source_node == Some(context.node_id()) {
        return Err(CommandError::reject(
            "a node cannot configure its own Myko history as a peer",
        ));
    }
    Ok(())
}

fn emit_peer(context: &CommandContext, peer: &Peer) -> Result<(), CommandError> {
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
    let descriptor = reference.into_descriptor();
    descriptor.validate()?;
    let endpoint = descriptor.endpoint;
    let source_node = Some(descriptor.node_id);
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
#[myko_query(Peer, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct GetPeers;

impl QueryHandler for GetPeers {
    fn build_view(
        ctx: QueryBuildArgs<Self>,
    ) -> Option<impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<dyn myko::item::AnyItem>>> {
        Some(
            ctx.federated_items::<Peer>()
                .expect("validated peer federation source"),
        )
    }
}

/// Returns every typed service advertised by one node in stable ID order.
#[myko_query(AdvertisedService, item = AdvertisedService)]
#[derive(Copy, PartialEq, Eq)]
pub struct GetAdvertisedServices;

impl QueryHandler for GetAdvertisedServices {
    fn build_view(
        ctx: QueryBuildArgs<Self>,
    ) -> Option<impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<dyn myko::item::AnyItem>>> {
        Some(
            ctx.federated_items::<AdvertisedService>()
                .expect("validated advertised-service federation source"),
        )
    }
}

/// Reactive configured-peer roster for one authoritative node.
#[myko_view(Peer, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct PeersView {
    pub source_node: NodeId,
}

impl ViewHandler for PeersView {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        myko::item::typed_map_arc_from_any_item::<Peer>(
            ctx.federated_items::<Peer>()
                .expect("validated peer federation source"),
            "PeersView",
        )
    }
}

/// Reactive catalog of typed services executable by one authoritative node.
#[myko_view(AdvertisedService, item = AdvertisedService)]
#[derive(Copy, PartialEq, Eq)]
pub struct AdvertisedServicesView {
    pub source_node: NodeId,
}

impl ViewHandler for AdvertisedServicesView {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        myko::item::typed_map_arc_from_any_item::<AdvertisedService>(
            ctx.federated_items::<AdvertisedService>()
                .expect("validated advertised-service federation source"),
            "AdvertisedServicesView",
        )
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

    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn compute(&self, context: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let peer_id = self.peer_id.clone();
        myko::item::typed_map_arc_from_any_item::<Peer>(
            context
                .federated_items::<Peer>()
                .expect("validated peer federation source"),
            "PeerReport",
        )
        .entries()
        .map(move |peers| {
            Arc::new(
                peers
                    .iter()
                    .find(|(_, peer)| peer.id == peer_id)
                    .map(|(_, peer)| peer.as_ref().clone()),
            )
        })
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

    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn compute(&self, context: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let service_id = self.service_id.clone();
        myko::item::typed_map_arc_from_any_item::<AdvertisedService>(
            context
                .federated_items::<AdvertisedService>()
                .expect("validated advertised-service federation source"),
            "ServiceCapabilityReport",
        )
        .entries()
        .map(move |services| {
            Arc::new(
                services
                    .iter()
                    .any(|(_, service)| service.service_id == service_id),
            )
        })
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
