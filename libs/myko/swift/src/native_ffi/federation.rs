//! Native federation operations routed through one composed application node.

use std::sync::Arc;

use myko_app::ApplicationNode;
use myko_federation::{NodeId, SubscriptionLiveness};
use myko_node::{
    ConfirmPairing, InitiateDiscoveredPairing, IssuePairingInvitation, NearbyNodesView,
    PairingInitiationReport, PairingInvitation, PairingReceiptsView, PairingRedemptionId,
    PairingRedemptionPhase, PairingRedemptionReport, PeerId, PeersView, RedeemPairingInvitation,
    RemovePeer, SetPeerReplication,
};
use uuid::Uuid;

use crate::BlockingSubscription;

use super::{
    MykoFederationError, MykoNearbyNodesSubscription, MykoPairedNodesSubscription,
    MykoPairingInitiationSubscription, MykoPairingReceipt, MykoPairingReceiptsSubscription,
    NativeApplicationAccess, pairing_error, transport_error,
};

/// Reusable typed federation surface for a composed native Myko node.
#[derive(uniffi::Object)]
pub struct MykoFederation {
    application: Arc<dyn NativeApplicationAccess>,
}

impl MykoFederation {
    /// Binds the reusable federation component to an application's active node.
    #[must_use]
    pub fn new(application: Arc<dyn NativeApplicationAccess>) -> Arc<Self> {
        Arc::new(Self { application })
    }

    fn active_application(&self) -> Result<ApplicationNode, MykoFederationError> {
        self.application.application()
    }
}

#[uniffi::export]
impl MykoFederation {
    /// Issues an expiring one-use pairing invitation suitable for sharing.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or invitation issuance fails.
    pub fn issue_pairing_invitation(
        &self,
        ttl_seconds: u64,
    ) -> Result<String, MykoFederationError> {
        let invitation = self
            .active_application()?
            .exec_command(IssuePairingInvitation { ttl_seconds })
            .map_err(|error| pairing_error(&error))?;
        serde_json::to_string(&invitation).map_err(|error| pairing_error(&error))
    }

    /// Watches authenticated receipts for invitations issued by this node.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the view cannot open.
    pub fn subscribe_pairing_receipts(
        &self,
    ) -> Result<Arc<MykoPairingReceiptsSubscription>, MykoFederationError> {
        let application = self.active_application()?;
        let subscription = application
            .watch_view(&PairingReceiptsView)
            .map_err(|error| transport_error(&error))?;
        let live = subscription.live().clone();
        Ok(Arc::new(MykoPairingReceiptsSubscription {
            subscription: crate::BlockingCollectionSubscription::new(subscription, &live),
            local_endpoint_id: self.application.endpoint_id().to_string(),
        }))
    }

    /// Watches every authenticated node remembered by this node.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the view cannot open.
    pub fn subscribe_paired_nodes(
        &self,
    ) -> Result<Arc<MykoPairedNodesSubscription>, MykoFederationError> {
        let application = self.active_application()?;
        let subscription = application
            .watch_view(&PeersView {
                source_node: application.node_id(),
            })
            .map_err(|error| transport_error(&error))?;
        let live = subscription.live().clone();
        Ok(Arc::new(MykoPairedNodesSubscription {
            subscription: crate::BlockingCollectionSubscription::new(subscription, &live),
        }))
    }

    /// Enables or pauses directional history replication from one peer.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the peer is unknown.
    pub fn set_paired_node_replication(
        &self,
        peer_id: String,
        enabled: bool,
    ) -> Result<(), MykoFederationError> {
        self.active_application()?
            .exec_command(SetPeerReplication {
                peer_id: PeerId::from(peer_id),
                enabled,
            })
            .map(|_| ())
            .map_err(|error| transport_error(&error))
    }

    /// Forgets one paired identity and stops its directional replication.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the peer is unknown.
    pub fn forget_paired_node(&self, peer_id: String) -> Result<(), MykoFederationError> {
        self.active_application()?
            .exec_command(RemovePeer {
                peer_id: PeerId::from(peer_id),
            })
            .map_err(|error| transport_error(&error))
    }

    /// Watches untrusted Myko advertisements visible on the LAN.
    ///
    /// # Errors
    ///
    /// Returns an error while the node is inactive or the view cannot open.
    pub fn subscribe_nearby_nodes(
        &self,
    ) -> Result<Arc<MykoNearbyNodesSubscription>, MykoFederationError> {
        let subscription = self
            .active_application()?
            .watch_view(&NearbyNodesView)
            .map_err(|error| transport_error(&error))?;
        let live = subscription.live().clone();
        Ok(Arc::new(MykoNearbyNodesSubscription {
            subscription: crate::BlockingCollectionSubscription::new(subscription, &live),
        }))
    }

    /// Starts the durable identity-pinned pairing flow for one discovered node.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid node ID, unavailable discovery row, or
    /// a pairing task/report which cannot start.
    #[allow(clippy::needless_pass_by_value)] // UniFFI exports owned Swift strings.
    pub fn start_nearby_pairing(
        &self,
        node_id: String,
        ttl_seconds: u64,
    ) -> Result<Arc<MykoPairingInitiationSubscription>, MykoFederationError> {
        let node_id = parse_node_id(&node_id)?;
        let application = self.active_application()?;
        let initiation = application
            .exec_command(InitiateDiscoveredPairing {
                peer_node_id: node_id,
                ttl_seconds,
            })
            .map_err(|error| pairing_error(&error))?;
        let subscription = application
            .watch_report(&PairingInitiationReport {
                source_node: application.node_id(),
                initiation_id: initiation.id,
            })
            .map_err(|error| transport_error(&error))?;
        let live = subscription.live().clone();
        Ok(Arc::new(MykoPairingInitiationSubscription {
            subscription: BlockingSubscription::new(subscription, &live),
        }))
    }

    /// Confirms an inviter-side receipt after its comparison code is checked.
    ///
    /// Confirmation remembers the peer but deliberately leaves the independent
    /// directional replication decision unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed input, a mismatched code, unavailable
    /// transport, or a receipt which does not bind peer history.
    #[allow(clippy::needless_pass_by_value)] // UniFFI exports owned Swift strings.
    pub fn confirm_pairing_receipt(
        &self,
        receipt_json: String,
        comparison_code: String,
    ) -> Result<String, MykoFederationError> {
        let receipt = serde_json::from_str::<myko_node::PairingReceipt>(&receipt_json)
            .map_err(|error| pairing_error(&format!("invalid pairing receipt: {error}")))?;
        let peer = self
            .active_application()?
            .exec_command(ConfirmPairing {
                receipt,
                comparison_code,
            })
            .map_err(|error| pairing_error(&error))?;
        peer.source_node
            .ok_or_else(|| pairing_error("confirmed pairing did not bind the peer history"))?;
        Ok(peer.endpoint.id.to_string())
    }

    /// Redeems a pairing invitation and waits for its typed durable task.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed input, an unavailable node, or a failed
    /// pairing redemption task.
    #[allow(clippy::needless_pass_by_value)] // UniFFI exports owned Swift strings.
    pub fn redeem_pairing(
        &self,
        invitation_json: String,
    ) -> Result<MykoPairingReceipt, MykoFederationError> {
        let invitation = serde_json::from_str::<PairingInvitation>(&invitation_json)
            .map_err(|error| pairing_error(&format!("invalid pairing invitation: {error}")))?;
        let application = self.active_application()?;
        let redemption = application
            .exec_command(RedeemPairingInvitation { invitation })
            .map_err(|error| pairing_error(&error))?;
        let receipt = wait_for_pairing_redemption(&application, redemption.id)?;
        let receipt_json =
            serde_json::to_string(&receipt).map_err(|error| pairing_error(&error))?;
        Ok(MykoPairingReceipt {
            receipt_json,
            comparison_code: receipt.comparison_code,
            inviter_endpoint_id: receipt.server.endpoint.id.to_string(),
        })
    }
}

fn parse_node_id(value: &str) -> Result<NodeId, MykoFederationError> {
    Uuid::parse_str(value)
        .map(NodeId::from_uuid)
        .map_err(|error| MykoFederationError::InvalidIdentifier {
            message: format!("invalid node ID: {error}"),
        })
}

fn wait_for_pairing_redemption(
    application: &ApplicationNode,
    redemption_id: PairingRedemptionId,
) -> Result<myko_node::PairingReceipt, MykoFederationError> {
    let report = application
        .watch_report(&PairingRedemptionReport {
            source_node: application.node_id(),
            redemption_id,
        })
        .map_err(|error| pairing_error(&error))?;
    let live = report.live().clone();
    let subscription = BlockingSubscription::new(report, &live);
    let mut state = subscription.current();
    loop {
        if let SubscriptionLiveness::Invalid { reason } = state.liveness {
            return Err(pairing_error(&reason));
        }
        if let Some(redemption) = state.value.flatten() {
            match redemption.phase {
                PairingRedemptionPhase::Completed { receipt } => return Ok(receipt),
                PairingRedemptionPhase::Failed { reason } => return Err(pairing_error(&reason)),
                PairingRedemptionPhase::Queued | PairingRedemptionPhase::Running { .. } => {}
            }
        }
        state = subscription.next().map_err(|error| pairing_error(&error))?;
    }
}
