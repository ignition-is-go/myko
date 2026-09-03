//! Generated records and live-subscription projection for native federation.

use myko_discovery::{DiscoveredNode, ParticipantCapability, ParticipantKind};
use myko_federation::{
    LiveSubscriptionState, LogPosition, ReplicationSelection, ScopeSelection, SubscriptionLiveness,
};
use myko_node::{
    PairingInitiation, PairingInitiationPhase, PairingReceipt, Peer, endpoint_principal_id,
};

use crate::{BlockingCollectionSubscription, BlockingSubscription};

use super::{MykoFederationError, pairing_error, transport_error};

/// One successful invitation redemption awaiting code confirmation.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPairingReceipt {
    pub receipt_json: String,
    pub comparison_code: String,
    pub inviter_endpoint_id: String,
}

/// One authenticated inbound receipt awaiting local confirmation.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPendingPairingReceipt {
    pub invitation_id: String,
    pub receipt_json: String,
    pub comparison_code: String,
    pub peer_endpoint_id: String,
}

/// Current-or-live inviter-side pairing receipts.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPairingReceiptsUpdate {
    pub lifecycle: String,
    pub reason: Option<String>,
    pub receipts: Vec<MykoPendingPairingReceipt>,
}

/// One authenticated node remembered in the local peer roster.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPairedNode {
    pub peer_id: String,
    pub source_node_id: Option<String>,
    /// Authenticated authority principal derived by Myko from the transport.
    pub principal_id: String,
    /// Transport identity retained for diagnostics and invitation receipts.
    pub endpoint_id: String,
    pub replication_enabled: bool,
    pub replication_selection: String,
}

/// Current-or-live local peer configuration.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPairedNodesUpdate {
    pub lifecycle: String,
    pub reason: Option<String>,
    pub nodes: Vec<MykoPairedNode>,
}

/// One untrusted Myko node currently visible through LAN discovery.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoNearbyNode {
    pub node_id: String,
    pub endpoint_id: String,
    pub display_name: String,
    pub kind: String,
    pub capabilities: Vec<String>,
    pub addresses: Vec<String>,
    pub reachable: bool,
    pub last_error: Option<String>,
}

/// Current-or-live pre-pair LAN discovery state.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoNearbyNodesUpdate {
    pub lifecycle: String,
    pub reason: Option<String>,
    pub nodes: Vec<MykoNearbyNode>,
}

/// One durable discovered-node pairing initiation revision.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MykoPairingInitiationUpdate {
    pub lifecycle: String,
    pub phase: String,
    pub peer_node_id: Option<String>,
    pub peer_endpoint_id: Option<String>,
    pub receipt_json: Option<String>,
    pub comparison_code: Option<String>,
    pub error: Option<String>,
    pub is_terminal: bool,
}

/// Long-lived inviter-side receipt subscription.
#[derive(uniffi::Object)]
pub struct MykoPairingReceiptsSubscription {
    pub(super) subscription: BlockingCollectionSubscription<PairingReceipt, LogPosition>,
    pub(super) local_endpoint_id: String,
}

/// Long-lived local peer-roster subscription.
#[derive(uniffi::Object)]
pub struct MykoPairedNodesSubscription {
    pub(super) subscription: BlockingCollectionSubscription<Peer, LogPosition>,
}

/// Long-lived local LAN-discovery subscription.
#[derive(uniffi::Object)]
pub struct MykoNearbyNodesSubscription {
    pub(super) subscription: BlockingCollectionSubscription<DiscoveredNode, u64>,
}

/// Live report for one durable discovered-node pairing attempt.
#[derive(uniffi::Object)]
pub struct MykoPairingInitiationSubscription {
    pub(super) subscription: BlockingSubscription<Option<PairingInitiation>>,
}

crate::export_blocking_subscription! {
    MykoPairingReceiptsSubscription => MykoPairingReceiptsUpdate,
    field = subscription,
    error = MykoFederationError,
    transport_error = transport_error,
    map = |state, owner: &MykoPairingReceiptsSubscription| {
        pairing_receipts_update(state, &owner.local_endpoint_id)
    },
}

crate::export_blocking_subscription! {
    MykoPairedNodesSubscription => MykoPairedNodesUpdate,
    field = subscription,
    error = MykoFederationError,
    transport_error = transport_error,
    map = |state, _owner| Ok(paired_nodes_update(state)),
}

crate::export_blocking_subscription! {
    MykoNearbyNodesSubscription => MykoNearbyNodesUpdate,
    field = subscription,
    error = MykoFederationError,
    transport_error = transport_error,
    map = |state, _owner| Ok(nearby_nodes_update(state)),
}

crate::export_blocking_subscription! {
    MykoPairingInitiationSubscription => MykoPairingInitiationUpdate,
    field = subscription,
    error = MykoFederationError,
    transport_error = transport_error,
    map = |state, _owner| pairing_initiation_update(state),
}

fn paired_nodes_update(
    state: LiveSubscriptionState<Vec<Peer>, LogPosition>,
) -> MykoPairedNodesUpdate {
    let (lifecycle, reason) = lifecycle(&state.liveness);
    let mut nodes = state
        .value
        .unwrap_or_default()
        .into_iter()
        .map(|peer| {
            let endpoint_id = peer.endpoint.id;
            MykoPairedNode {
                peer_id: peer.id.to_string(),
                source_node_id: peer.source_node.map(|node_id| node_id.to_string()),
                principal_id: endpoint_principal_id(endpoint_id).to_string(),
                endpoint_id: endpoint_id.to_string(),
                replication_enabled: peer.replication_enabled,
                replication_selection: replication_selection(&peer.replication_selection),
            }
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.source_node_id
            .cmp(&right.source_node_id)
            .then_with(|| left.endpoint_id.cmp(&right.endpoint_id))
    });
    MykoPairedNodesUpdate {
        lifecycle,
        reason,
        nodes,
    }
}

fn replication_selection(selection: &ReplicationSelection) -> String {
    match selection {
        ReplicationSelection::All => "all".to_owned(),
        ReplicationSelection::Service(service_id) => format!("service:{service_id}"),
        ReplicationSelection::ServiceScope {
            service_id,
            scope_id,
        } => format!("service:{service_id}/scope:{scope_id}"),
        ReplicationSelection::Scopes(scopes) => scope_selection_list(scopes),
        ReplicationSelection::Intersection { requested, scopes } => format!(
            "intersection:{}&{}",
            replication_selection(requested),
            scope_selection_list(scopes)
        ),
    }
}

fn scope_selection_list(scopes: &[ScopeSelection]) -> String {
    scopes
        .iter()
        .map(|scope| match scope {
            ScopeSelection::Exact(scope_id) => format!("scope:{scope_id}"),
            ScopeSelection::Subtree(scope_id) => format!("subtree:{scope_id}"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn nearby_nodes_update(
    state: LiveSubscriptionState<Vec<DiscoveredNode>, u64>,
) -> MykoNearbyNodesUpdate {
    let (lifecycle, reason) = lifecycle(&state.liveness);
    let mut nodes = state
        .value
        .unwrap_or_default()
        .into_iter()
        .map(|node| {
            let kind = match node.kind {
                ParticipantKind::FullNode => "full node",
                ParticipantKind::ForegroundEdge => "foreground edge",
                ParticipantKind::WebSocketEdge => "websocket edge",
            }
            .to_owned();
            let capabilities = [
                (ParticipantCapability::DurableHistory, "durable history"),
                (
                    ParticipantCapability::BackgroundTransport,
                    "background transport",
                ),
                (ParticipantCapability::HostWorkloads, "host workloads"),
                (ParticipantCapability::LocalWorkspaces, "local workspaces"),
            ]
            .into_iter()
            .filter(|(capability, _)| node.capabilities.supports(*capability))
            .map(|(_, label)| label.to_owned())
            .collect();
            MykoNearbyNode {
                node_id: node.node_id().to_string(),
                endpoint_id: node.endpoint_id().to_string(),
                display_name: node.display_name,
                kind,
                capabilities,
                addresses: node
                    .descriptor
                    .endpoint
                    .addrs
                    .into_iter()
                    .map(|address| format!("{address:?}"))
                    .collect(),
                reachable: node.reachable,
                last_error: node.last_error,
            }
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.display_name
            .cmp(&right.display_name)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    MykoNearbyNodesUpdate {
        lifecycle,
        reason,
        nodes,
    }
}

fn pairing_receipts_update(
    state: LiveSubscriptionState<Vec<PairingReceipt>, LogPosition>,
    local_endpoint_id: &str,
) -> Result<MykoPairingReceiptsUpdate, MykoFederationError> {
    let (lifecycle, reason) = lifecycle(&state.liveness);
    let receipts = state
        .value
        .unwrap_or_default()
        .into_iter()
        .map(|receipt| {
            let server_endpoint_id = receipt.server.endpoint.id.to_string();
            let client_endpoint_id = receipt.client.endpoint.id.to_string();
            let peer_endpoint_id = if server_endpoint_id == local_endpoint_id {
                client_endpoint_id
            } else if client_endpoint_id == local_endpoint_id {
                server_endpoint_id
            } else {
                return Err(pairing_error(
                    "pending pairing receipt does not include this endpoint",
                ));
            };
            let receipt_json =
                serde_json::to_string(&receipt).map_err(|error| pairing_error(&error))?;
            Ok(MykoPendingPairingReceipt {
                invitation_id: receipt.invitation_id.to_string(),
                receipt_json,
                comparison_code: receipt.comparison_code,
                peer_endpoint_id,
            })
        })
        .collect::<Result<Vec<_>, MykoFederationError>>()?;
    Ok(MykoPairingReceiptsUpdate {
        lifecycle,
        reason,
        receipts,
    })
}

fn pairing_initiation_update(
    state: LiveSubscriptionState<Option<PairingInitiation>, LogPosition>,
) -> Result<MykoPairingInitiationUpdate, MykoFederationError> {
    let (lifecycle, reason) = lifecycle(&state.liveness);
    let Some(initiation) = state.value.flatten() else {
        return Ok(MykoPairingInitiationUpdate {
            lifecycle,
            phase: "queued".to_owned(),
            peer_node_id: None,
            peer_endpoint_id: None,
            receipt_json: None,
            comparison_code: None,
            error: reason,
            is_terminal: false,
        });
    };
    let peer_node_id = Some(initiation.peer.node_id.to_string());
    let peer_endpoint_id = Some(initiation.peer.endpoint.id.to_string());
    match initiation.phase {
        PairingInitiationPhase::Queued => Ok(MykoPairingInitiationUpdate {
            lifecycle,
            phase: "queued".to_owned(),
            peer_node_id,
            peer_endpoint_id,
            receipt_json: None,
            comparison_code: None,
            error: reason,
            is_terminal: false,
        }),
        PairingInitiationPhase::Running { .. } => Ok(MykoPairingInitiationUpdate {
            lifecycle,
            phase: "running".to_owned(),
            peer_node_id,
            peer_endpoint_id,
            receipt_json: None,
            comparison_code: None,
            error: reason,
            is_terminal: false,
        }),
        PairingInitiationPhase::Completed { receipt } => Ok(MykoPairingInitiationUpdate {
            lifecycle,
            phase: "completed".to_owned(),
            peer_node_id,
            peer_endpoint_id,
            receipt_json: Some(
                serde_json::to_string(&receipt).map_err(|error| pairing_error(&error))?,
            ),
            comparison_code: Some(receipt.comparison_code),
            error: reason,
            is_terminal: true,
        }),
        PairingInitiationPhase::Failed { reason: failure } => Ok(MykoPairingInitiationUpdate {
            lifecycle,
            phase: "failed".to_owned(),
            peer_node_id,
            peer_endpoint_id,
            receipt_json: None,
            comparison_code: None,
            error: Some(failure),
            is_terminal: true,
        }),
    }
}

pub(super) fn lifecycle(liveness: &SubscriptionLiveness) -> (String, Option<String>) {
    match liveness {
        SubscriptionLiveness::Current => ("current".to_owned(), None),
        SubscriptionLiveness::Connecting => ("connecting".to_owned(), None),
        SubscriptionLiveness::Resynchronizing { reason } => {
            ("resynchronizing".to_owned(), Some(reason.clone()))
        }
        SubscriptionLiveness::Invalid { reason } => ("invalid".to_owned(), Some(reason.clone())),
    }
}

#[cfg(test)]
mod tests {
    use myko_federation::{ReplicationSelection, ScopeId, ScopeSelection, ServiceId};

    use super::replication_selection;

    #[test]
    fn replication_selection_projection_is_stable_and_typed_at_source() {
        assert_eq!(replication_selection(&ReplicationSelection::All), "all");
        assert_eq!(
            replication_selection(&ReplicationSelection::Service(ServiceId::new("chat"))),
            "service:chat"
        );
        assert_eq!(
            replication_selection(&ReplicationSelection::Scopes(vec![
                ScopeSelection::Exact(ScopeId::new("a")),
                ScopeSelection::Subtree(ScopeId::new("b")),
            ])),
            "scope:a,subtree:b"
        );
        assert_eq!(
            replication_selection(&ReplicationSelection::Intersection {
                requested: Box::new(ReplicationSelection::Service(ServiceId::new("chat"))),
                scopes: vec![ScopeSelection::Exact(ScopeId::new("a"))],
            }),
            "intersection:service:chat&scope:a"
        );
    }
}
