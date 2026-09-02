//! Framework-owned peer configuration, commands, and live projections.

use std::sync::Arc;

use myko_app::capability::{
    CollectionBuilding as _, CommandQuerying as _, EventPublishing as _, NodeScoped as _,
    Querying as _,
};
use myko_app::{
    AppError, CommandContext, CommandError, CommandHandler, QueryHandler, ReportContext,
    ReportHandler, ViewContext, ViewHandler, myko_query, myko_report, myko_view,
};
use myko_federation::{
    ItemProjection, ItemQuery, LiveCollection, LiveSubscription, LogPosition, NodeId, ScopeId,
};
use myko_iroh::{EndpointAddr, EndpointId, NativeNodeDescriptor, NativePeerReference};
use myko_items::{myko_command, myko_item, myko_service};

use crate::{DiscoverySettings, PairingRedemption, PendingPairingReceipt};

/// Myko's built-in node-federation service.
#[myko_service(
    PeerRoster,
    Peer,
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
/// followers to these durable values.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct Peer {
    pub endpoint: EndpointAddr,
    pub source_node: Option<NodeId>,
    pub following: bool,
}

impl Eq for Peer {}

/// Adds or replaces a peer and enables directional history replication.
#[myko_command(Peer, item = Peer)]
pub struct AddPeer {
    pub reference: NativePeerReference,
}

/// Remembers a mutually authenticated descriptor without implicitly enabling
/// replication. An existing following decision is preserved.
#[myko_command(Peer, item = Peer)]
pub struct RememberPeer {
    pub descriptor: NativeNodeDescriptor,
}

/// Changes this node's independent replication decision for one peer.
#[myko_command(Peer, item = Peer)]
pub struct SetPeerFollowing {
    pub peer_id: PeerId,
    pub following: bool,
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
    pub following: bool,
}

impl CommandHandler for AddPeer {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Peer, CommandError> {
        let peer = peer_from_reference(self.reference, true).map_err(CommandError::reject)?;
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
            endpoint: self.descriptor.endpoint,
            source_node: Some(self.descriptor.node_id),
            following: existing.is_some_and(|peer| peer.following),
        };
        reject_self_history(&context, &peer)?;
        emit_peer(&context, &peer)?;
        Ok(peer)
    }
}

impl CommandHandler for SetPeerFollowing {
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
        peer.following = self.following;
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
            endpoint: self.endpoint,
            source_node: self.source_node,
            following: self.following,
        };
        reject_self_history(&context, &peer)?;
        emit_peer(&context, &peer)?;
        Ok(peer)
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

fn peer_from_reference(reference: NativePeerReference, following: bool) -> Result<Peer, String> {
    let (endpoint, source_node) = match reference {
        NativePeerReference::Descriptor(descriptor) => {
            descriptor.validate()?;
            (descriptor.endpoint, Some(descriptor.node_id))
        }
        NativePeerReference::LegacyEndpoint(endpoint) => (endpoint, None),
    };
    Ok(Peer {
        id: peer_id(endpoint.id),
        endpoint,
        source_node,
        following,
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

/// Reactive configured-peer roster for one authoritative node.
#[myko_view(Peer, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct PeersView {
    pub source_node: NodeId,
}

impl ViewHandler for PeersView {
    type Item = Peer;
    type Cursor = LogPosition;

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

    fn build(&self, context: &ReportContext) -> Result<LiveSubscription<Self::Output>, AppError> {
        let peer_id = self.peer_id.clone();
        Ok(context
            .query(self.source_node, peer_scope(self.source_node), GetPeers)?
            .map_value(move |peers| peers.iter().find(|peer| peer.id == peer_id).cloned()))
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
