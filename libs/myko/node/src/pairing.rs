//! Framework-owned pairing commands, redemption entity, report, and receipt view.

#![allow(clippy::expect_used)] // Infallible handler builders rely on validated host wiring.

use std::{collections::HashSet, hash::Hash, sync::Arc, time::Duration};

use hyphae::{Definite, MapEntriesExt as _, MapExt as _, Materialize};
use myko::{
    ApplicationHost, CommandContext, CommandError, CommandHandler, myko_report, myko_view,
    myko_view_item,
    report::{ReportContext, ReportHandler},
    view::{ViewBuildArgs, ViewHandler},
};
use myko_federation::{
    AccessOperation, FederationPermission, NodeId, PrincipalId, ResourceClaim, ResourceClaimKind,
    ScopeSelection, ServiceId,
};
use myko_iroh::{
    IrohReplicator, NativeNodeDescriptor, PairingInvitation, PairingReceipt,
    PairingReceiptSubscription,
};
use myko_items::{MykoItem, MykoService, myko_command, myko_item, myko_subtype};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use uuid::Uuid;

use crate::{
    FederationService, Peer, RememberPeer,
    discovery::DiscoveryViewState,
    iroh_replicator_capability_id,
    peer::{PeerRoster, PeerRosterId, peer_roster_claims, peer_scope},
};

/// Durable lifecycle of redeeming one remote pairing invitation.
#[myko_subtype(derive(Eq))]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum PairingRedemptionPhase {
    Queued,
    Running { node_id: NodeId },
    Completed { receipt: PairingReceipt },
    Failed { reason: String },
}

/// Durable lifecycle of offering this node's invitation to one peer.
#[myko_subtype(derive(Eq))]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum PairingInitiationPhase {
    Queued,
    Running { node_id: NodeId },
    Completed { receipt: PairingReceipt },
    Failed { reason: String },
}

impl PairingInitiationPhase {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }
}

impl PairingRedemptionPhase {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }
}

/// One durable, asynchronous pairing-invitation redemption.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct PairingRedemption {
    pub requested_by: String,
    pub invitation: PairingInvitation,
    pub phase: PairingRedemptionPhase,
}
myko::register_federated_item!(PairingRedemption);

impl Eq for PairingRedemption {}

/// One durable, asynchronous pairing offer to an identity-pinned peer.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct PairingInitiation {
    pub requested_by: String,
    pub peer: NativeNodeDescriptor,
    pub ttl_seconds: u64,
    pub phase: PairingInitiationPhase,
}
myko::register_federated_item!(PairingInitiation);

impl Eq for PairingInitiation {}

/// One authenticated inbound receipt awaiting local operator confirmation.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct PendingPairingReceipt {
    pub receipt: PairingReceipt,
}
myko::register_federated_item!(PendingPairingReceipt);

impl Eq for PendingPairingReceipt {}

impl PairingRedemptionId {
    #[must_use]
    pub fn random() -> Self {
        Self::from(Uuid::now_v7().to_string())
    }
}

impl PairingInitiationId {
    #[must_use]
    pub fn random() -> Self {
        Self::from(Uuid::now_v7().to_string())
    }
}

/// Starts an asynchronous pairing offer to one identity-pinned peer.
#[myko_command(PairingInitiation, item = PairingInitiation)]
pub struct InitiatePairing {
    pub peer: NativeNodeDescriptor,
    pub ttl_seconds: u64,
}

impl CommandHandler for InitiatePairing {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![iroh_replicator_capability_id()]
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        self.peer.validate().map_err(CommandError::reject)?;
        let local = context
            .resource::<IrohReplicator>()
            .map_err(|error| CommandError::reject(error.to_string()))?
            .descriptor();
        if same_descriptor_identity(&local, &self.peer) {
            return Err(CommandError::reject("cannot pair a node with itself"));
        }
        let initiation = PairingInitiation {
            id: PairingInitiationId::random(),
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            requested_by: context.principal_id().as_str().to_owned(),
            peer: self.peer,
            ttl_seconds: self.ttl_seconds,
            phase: PairingInitiationPhase::Queued,
        };
        context.emit_set(&initiation)?;
        Ok(initiation)
    }
}

/// Starts an asynchronous pairing offer to a node in the live discovery view.
///
/// Frontends identify the nearby node; the framework resolves its current,
/// transport-specific descriptor inside the command handler.
#[myko_command(PairingInitiation, item = PairingInitiation)]
pub struct InitiateDiscoveredPairing {
    pub peer_node_id: NodeId,
    pub ttl_seconds: u64,
}

impl CommandHandler for InitiateDiscoveredPairing {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![
            crate::discovery_capability_id(),
            iroh_replicator_capability_id(),
        ]
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let peer = context
            .resource::<DiscoveryViewState>()
            .map_err(|error| CommandError::reject(error.to_string()))?
            .descriptor_for(self.peer_node_id)
            .map_err(CommandError::reject)?;
        context.exec_command(InitiatePairing {
            peer,
            ttl_seconds: self.ttl_seconds,
        })
    }
}

/// Issues an expiring one-use invitation from this node's native endpoint.
#[myko_command(
    PairingInvitation,
    service = FederationService,
    scope = PeerRoster
)]
pub struct IssuePairingInvitation {
    pub ttl_seconds: u64,
}

impl CommandHandler for IssuePairingInvitation {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![iroh_replicator_capability_id()]
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let replicator = context
            .resource::<IrohReplicator>()
            .map_err(|error| CommandError::reject(error.to_string()))?;
        replicator
            .issue_pairing_invitation(Duration::from_secs(self.ttl_seconds))
            .map_err(|error| CommandError::reject(error.to_string()))
    }
}

/// Starts an asynchronous redemption of another node's invitation.
#[myko_command(PairingRedemption, item = PairingRedemption)]
pub struct RedeemPairingInvitation {
    pub invitation: PairingInvitation,
}

impl CommandHandler for RedeemPairingInvitation {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let redemption = PairingRedemption {
            id: PairingRedemptionId::random(),
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            requested_by: context.principal_id().as_str().to_owned(),
            invitation: self.invitation,
            phase: PairingRedemptionPhase::Queued,
        };
        context.emit_set(&redemption)?;
        Ok(redemption)
    }
}

/// Confirms a mutually authenticated receipt and remembers the opposite peer.
#[myko_command(Peer, item = Peer)]
pub struct ConfirmPairing {
    pub receipt: PairingReceipt,
    pub comparison_code: String,
}

impl CommandHandler for ConfirmPairing {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        let mut claims = peer_roster_claims(node_id, Peer::ITEM_TYPE);
        claims.push(ResourceClaim {
            selection: ScopeSelection::Exact(peer_scope(node_id)),
            kind: ResourceClaimKind::Affected,
            source_node: None,
            service_id: Some(ServiceId::new(FederationService::SERVICE_ID)),
            item_type: Some(PendingPairingReceipt::ITEM_TYPE.to_owned()),
            item_id: None,
            required_permissions: vec![FederationPermission::Write],
            required_operations: Vec::new(),
            required_capabilities: Vec::new(),
        });
        claims
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![iroh_replicator_capability_id()]
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let descriptor = confirmed_peer(
            context.node_id(),
            &context
                .resource::<IrohReplicator>()
                .map_err(|error| CommandError::reject(error.to_string()))?
                .descriptor(),
            &self.receipt,
            &self.comparison_code,
        )
        .map_err(CommandError::reject)?;
        let peer = context.exec_command(RememberPeer { descriptor })?;
        context.emit_delete::<PendingPairingReceipt>(&PendingPairingReceiptId::from(
            self.receipt.invitation_id.to_string(),
        ))?;
        Ok(peer)
    }
}

#[myko_command(PendingPairingReceipt, item = PendingPairingReceipt)]
struct RecordPairingReceipt {
    receipt: PairingReceipt,
}

impl CommandHandler for RecordPairingReceipt {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        if context.principal_id() != &PrincipalId::for_node(context.node_id()) {
            return Err(CommandError::reject(
                "pairing receipt recording requires the executing node principal",
            ));
        }
        self.receipt.validate().map_err(CommandError::reject)?;
        let receipt = PendingPairingReceipt {
            id: PendingPairingReceiptId::from(self.receipt.invitation_id.to_string()),
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            receipt: self.receipt,
        };
        context.emit_set(&receipt)?;
        Ok(receipt)
    }
}

#[myko_command(PairingRedemption, item = PairingRedemption)]
struct AdvancePairingRedemption {
    redemption_id: PairingRedemptionId,
    expected: PairingRedemptionPhase,
    next: PairingRedemptionPhase,
}

#[myko_command(PairingInitiation, item = PairingInitiation)]
struct AdvancePairingInitiation {
    initiation_id: PairingInitiationId,
    expected: PairingInitiationPhase,
    next: PairingInitiationPhase,
}

fn lifecycle_claims(node_id: NodeId) -> Vec<ResourceClaim> {
    let mut claim = ResourceClaim::scope(peer_scope(node_id), ResourceClaimKind::Primary);
    claim
        .required_permissions
        .extend([FederationPermission::ReadState, FederationPermission::Write]);
    claim
        .required_operations
        .extend([AccessOperation::ReadItems, AccessOperation::SubmitCommand]);
    vec![claim]
}

impl CommandHandler for AdvancePairingInitiation {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        lifecycle_claims(node_id)
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        if context.principal_id() != &PrincipalId::for_node(context.node_id()) {
            return Err(CommandError::reject(
                "pairing-initiation transition requires the executing node principal",
            ));
        }
        let mut initiation = context
            .exec_item_query(GetPairingInitiationById {
                id: self.initiation_id,
            })?
            .into_iter()
            .next()
            .ok_or_else(|| CommandError::reject("pairing initiation does not exist"))?;
        if initiation.phase == self.next {
            return Ok(initiation);
        }
        if initiation.phase != self.expected {
            return Err(CommandError::reject(
                "pairing initiation changed before this transition executed",
            ));
        }
        validate_initiation_transition(context.node_id(), &initiation.phase, &self.next)?;
        initiation.phase = self.next;
        context.emit_set(&initiation)?;
        Ok(initiation)
    }
}

impl CommandHandler for AdvancePairingRedemption {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn authority_claims(&self, node_id: NodeId) -> Vec<ResourceClaim> {
        lifecycle_claims(node_id)
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        if context.principal_id() != &PrincipalId::for_node(context.node_id()) {
            return Err(CommandError::reject(
                "pairing-redemption transition requires the executing node principal",
            ));
        }
        let mut redemption = context
            .exec_item_query(GetPairingRedemptionById {
                id: self.redemption_id,
            })?
            .into_iter()
            .next()
            .ok_or_else(|| CommandError::reject("pairing redemption does not exist"))?;
        if redemption.phase == self.next {
            return Ok(redemption);
        }
        if redemption.phase != self.expected {
            return Err(CommandError::reject(
                "pairing redemption changed before this transition executed",
            ));
        }
        validate_transition(context.node_id(), &redemption.phase, &self.next)?;
        redemption.phase = self.next;
        context.emit_set(&redemption)?;
        Ok(redemption)
    }
}

fn validate_transition(
    node_id: NodeId,
    current: &PairingRedemptionPhase,
    next: &PairingRedemptionPhase,
) -> Result<(), CommandError> {
    match (current, next) {
        (PairingRedemptionPhase::Queued, PairingRedemptionPhase::Running { node_id: owner })
            if *owner == node_id =>
        {
            Ok(())
        }
        (PairingRedemptionPhase::Queued, PairingRedemptionPhase::Failed { .. }) => Ok(()),
        (
            PairingRedemptionPhase::Running { node_id: owner },
            PairingRedemptionPhase::Completed { .. } | PairingRedemptionPhase::Failed { .. },
        ) if *owner == node_id => Ok(()),
        _ => Err(CommandError::reject(
            "invalid pairing-redemption lifecycle transition",
        )),
    }
}

fn validate_initiation_transition(
    node_id: NodeId,
    current: &PairingInitiationPhase,
    next: &PairingInitiationPhase,
) -> Result<(), CommandError> {
    match (current, next) {
        (PairingInitiationPhase::Queued, PairingInitiationPhase::Running { node_id: owner })
            if *owner == node_id =>
        {
            Ok(())
        }
        (PairingInitiationPhase::Queued, PairingInitiationPhase::Failed { .. }) => Ok(()),
        (
            PairingInitiationPhase::Running { node_id: owner },
            PairingInitiationPhase::Completed { .. } | PairingInitiationPhase::Failed { .. },
        ) if *owner == node_id => Ok(()),
        _ => Err(CommandError::reject(
            "invalid pairing-initiation lifecycle transition",
        )),
    }
}

/// Live state of one invitation redemption.
#[myko_report(Option<PairingRedemption>, item = PairingRedemption)]
#[derive(PartialEq, Eq)]
pub struct PairingRedemptionReport {
    pub source_node: NodeId,
    pub redemption_id: PairingRedemptionId,
}

impl ReportHandler for PairingRedemptionReport {
    type Output = Option<PairingRedemption>;

    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn compute(&self, context: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let redemption_id = self.redemption_id.clone();
        myko::item::typed_map_arc_from_any_item::<PairingRedemption>(
            context
                .federated_items::<PairingRedemption>()
                .expect("validated pairing-redemption federation source"),
            "PairingRedemptionReport",
        )
        .entries()
        .map(move |redemptions| {
            Arc::new(
                redemptions
                    .iter()
                    .find(|(_, redemption)| redemption.id == redemption_id)
                    .map(|(_, redemption)| redemption.as_ref().clone()),
            )
        })
    }
}

/// Live state of one outgoing pairing offer.
#[myko_report(Option<PairingInitiation>, item = PairingInitiation)]
#[derive(PartialEq, Eq)]
pub struct PairingInitiationReport {
    pub source_node: NodeId,
    pub initiation_id: PairingInitiationId,
}

impl ReportHandler for PairingInitiationReport {
    type Output = Option<PairingInitiation>;

    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn compute(&self, context: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let initiation_id = self.initiation_id.clone();
        myko::item::typed_map_arc_from_any_item::<PairingInitiation>(
            context
                .federated_items::<PairingInitiation>()
                .expect("validated pairing-initiation federation source"),
            "PairingInitiationReport",
        )
        .entries()
        .map(move |initiations| {
            Arc::new(
                initiations
                    .iter()
                    .find(|(_, initiation)| initiation.id == initiation_id)
                    .map(|(_, initiation)| initiation.as_ref().clone()),
            )
        })
    }
}

/// Live authenticated receipts for invitations issued by this node.
#[myko_view_item]
#[derive(Eq)]
pub struct PairingReceiptRow {
    pub id: Arc<str>,
    pub receipt: PairingReceipt,
}

#[myko_view(PairingReceiptRow, item = PendingPairingReceipt)]
#[derive(Copy, PartialEq, Eq)]
pub struct PairingReceiptsView {}

impl ViewHandler for PairingReceiptsView {
    fn scope_id(&self, local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(local_node))
    }

    fn build_cell(
        context: ViewBuildArgs<Self>,
    ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        myko::item::typed_map_arc_from_any_item::<PendingPairingReceipt>(
            context
                .federated_items::<PendingPairingReceipt>()
                .expect("validated pairing-receipt federation source"),
            "PairingReceiptsView",
        )
        .map_entries(|_, pending| {
            let id = Arc::from(pending.receipt.invitation_id.to_string());
            (
                Arc::clone(&id),
                Arc::new(PairingReceiptRow {
                    id,
                    receipt: pending.receipt.clone(),
                }),
            )
        })
    }
}

#[derive(Debug)]
pub struct PairingSupervisor {
    stopping: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl PairingSupervisor {
    pub fn start(application: ApplicationHost, replicator: IrohReplicator) -> Result<Self, String> {
        let initiations = application.watch_items::<PairingInitiation>(
            Some(application.node().node_id()),
            Some(peer_scope(application.node().node_id())),
        )?;
        let redemptions = application.watch_items::<PairingRedemption>(
            Some(application.node().node_id()),
            Some(peer_scope(application.node().node_id())),
        )?;
        let receipts = replicator.subscribe_pairing_receipts();
        let (stopping, stop) = watch::channel(false);
        let task = tokio::spawn(run(
            application,
            replicator,
            initiations,
            redemptions,
            receipts,
            stop,
        ));
        Ok(Self {
            stopping,
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) -> Result<(), String> {
        self.stopping.send_replace(true);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await
            .map_err(|error| format!("pairing supervisor panicked: {error}"))?
    }
}

impl Drop for PairingSupervisor {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run(
    application: ApplicationHost,
    replicator: IrohReplicator,
    initiations: myko::view::TypedViewCellMap<PairingInitiation>,
    redemptions: myko::view::TypedViewCellMap<PairingRedemption>,
    mut receipts: PairingReceiptSubscription,
    mut stopping: watch::Receiver<bool>,
) -> Result<(), String> {
    let (change_sender, mut change_receiver) = mpsc::channel(1);
    change_sender
        .try_send(())
        .map_err(|_| "pairing subscriptions could not publish initial state".to_owned())?;
    let updates = change_sender.clone();
    let initiation_changes_guard = initiations.subscribe_diffs(move |_| {
        let _sent = updates.try_send(());
    });
    let updates = change_sender.clone();
    let changes_guard = redemptions.subscribe_diffs(move |_| {
        let _sent = updates.try_send(());
    });
    let mut recover = true;
    let mut active_initiations = HashSet::new();
    let mut active_redemptions = HashSet::new();
    let mut effects = JoinSet::new();
    loop {
        tokio::select! {
            changed = stopping.changed() => {
                if changed.is_err() || *stopping.borrow() {
                    break;
                }
            }
            update = change_receiver.recv() => {
                update.ok_or_else(|| "pairing-redemption subscription closed".to_owned())?;
                refresh_pairing_tasks(
                    &application,
                    &replicator,
                    &initiations,
                    &redemptions,
                    &mut recover,
                    &mut active_initiations,
                    &mut active_redemptions,
                    &mut effects,
                )?;
            }
            incoming = receipts.recv() => {
                for receipt in incoming.map_err(|error| error.to_string())? {
                    let _recorded = application
                        .exec_command(RecordPairingReceipt { receipt })
                        .map_err(|error| error.to_string())?;
                }
            }
            completed = effects.join_next(), if !effects.is_empty() => {
                match completed {
                    Some(Ok(result)) => {
                        result?;
                    }
                    Some(Err(error)) => return Err(format!("pairing task panicked: {error}")),
                    None => {}
                }
            }
        }
    }
    effects.abort_all();
    while effects.join_next().await.is_some() {}
    drop(initiation_changes_guard);
    drop(changes_guard);
    drop(initiations);
    drop(redemptions);
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the pairing supervisor owns two parallel durable lifecycle queues"
)]
fn refresh_pairing_tasks(
    application: &ApplicationHost,
    replicator: &IrohReplicator,
    initiations: &myko::view::TypedViewCellMap<PairingInitiation>,
    redemptions: &myko::view::TypedViewCellMap<PairingRedemption>,
    recover: &mut bool,
    active_initiations: &mut HashSet<PairingInitiationId>,
    active_redemptions: &mut HashSet<PairingRedemptionId>,
    effects: &mut JoinSet<Result<(), String>>,
) -> Result<(), String> {
    let initiation_values: Vec<PairingInitiation> = initiations
        .snapshot()
        .into_iter()
        .map(|(_, initiation)| initiation.as_ref().clone())
        .collect();
    let redemption_values: Vec<PairingRedemption> = redemptions
        .snapshot()
        .into_iter()
        .map(|(_, redemption)| redemption.as_ref().clone())
        .collect();
    if *recover {
        recover_initiations(application, &initiation_values)?;
        recover_redemptions(application, &redemption_values)?;
        *recover = false;
    }
    start_initiations(
        application,
        replicator,
        initiation_values,
        active_initiations,
        effects,
    )?;
    start_redemptions(
        application,
        replicator,
        redemption_values,
        active_redemptions,
        effects,
    )
}

fn recover_initiations(
    application: &ApplicationHost,
    initiations: &[PairingInitiation],
) -> Result<(), String> {
    for initiation in initiations {
        if matches!(initiation.phase, PairingInitiationPhase::Running { .. }) {
            advance_initiation(
                application,
                initiation.id.clone(),
                initiation.phase.clone(),
                PairingInitiationPhase::Failed {
                    reason: "node restarted while pairing initiation was active".to_owned(),
                },
            )?;
        }
    }
    Ok(())
}

fn recover_redemptions(
    application: &ApplicationHost,
    redemptions: &[PairingRedemption],
) -> Result<(), String> {
    for redemption in redemptions {
        if matches!(redemption.phase, PairingRedemptionPhase::Running { .. }) {
            advance(
                application,
                redemption.id.clone(),
                redemption.phase.clone(),
                PairingRedemptionPhase::Failed {
                    reason: "node restarted while pairing redemption was active".to_owned(),
                },
            )?;
        }
    }
    Ok(())
}

fn start_redemptions(
    application: &ApplicationHost,
    replicator: &IrohReplicator,
    redemptions: Vec<PairingRedemption>,
    active: &mut HashSet<PairingRedemptionId>,
    effects: &mut JoinSet<Result<(), String>>,
) -> Result<(), String> {
    retain_observed_unsettled(
        active,
        redemptions
            .iter()
            .map(|redemption| (&redemption.id, redemption.phase.is_terminal())),
    );
    for redemption in redemptions {
        if redemption.phase != PairingRedemptionPhase::Queued
            || !active.insert(redemption.id.clone())
        {
            continue;
        }
        let running = PairingRedemptionPhase::Running {
            node_id: application.node().node_id(),
        };
        advance(
            application,
            redemption.id.clone(),
            PairingRedemptionPhase::Queued,
            running.clone(),
        )?;
        let effect_application = application.clone();
        let effect_replicator = replicator.clone();
        let id = redemption.id;
        effects.spawn(async move {
            match effect_replicator
                .redeem_pairing(&redemption.invitation)
                .await
            {
                Ok(receipt) => advance(
                    &effect_application,
                    id.clone(),
                    running,
                    PairingRedemptionPhase::Completed { receipt },
                )
                .map(|_| ()),
                Err(error) => {
                    let reason = error.to_string();
                    advance(
                        &effect_application,
                        id.clone(),
                        running,
                        PairingRedemptionPhase::Failed { reason },
                    )
                    .map(|_| ())
                }
            }
        });
    }
    Ok(())
}

fn start_initiations(
    application: &ApplicationHost,
    replicator: &IrohReplicator,
    initiations: Vec<PairingInitiation>,
    active: &mut HashSet<PairingInitiationId>,
    effects: &mut JoinSet<Result<(), String>>,
) -> Result<(), String> {
    retain_observed_unsettled(
        active,
        initiations
            .iter()
            .map(|initiation| (&initiation.id, initiation.phase.is_terminal())),
    );
    for initiation in initiations {
        if initiation.phase != PairingInitiationPhase::Queued
            || !active.insert(initiation.id.clone())
        {
            continue;
        }
        let running = PairingInitiationPhase::Running {
            node_id: application.node().node_id(),
        };
        advance_initiation(
            application,
            initiation.id.clone(),
            PairingInitiationPhase::Queued,
            running.clone(),
        )?;
        let effect_application = application.clone();
        let effect_replicator = replicator.clone();
        let id = initiation.id;
        effects.spawn(async move {
            match effect_replicator
                .offer_pairing(
                    &initiation.peer,
                    Duration::from_secs(initiation.ttl_seconds),
                )
                .await
            {
                Ok(receipt) => advance_initiation(
                    &effect_application,
                    id.clone(),
                    running,
                    PairingInitiationPhase::Completed { receipt },
                )
                .map(|_| ()),
                Err(error) => advance_initiation(
                    &effect_application,
                    id.clone(),
                    running,
                    PairingInitiationPhase::Failed {
                        reason: error.to_string(),
                    },
                )
                .map(|_| ()),
            }
        });
    }
    Ok(())
}

fn retain_observed_unsettled<'a, Id>(
    active: &mut HashSet<Id>,
    visible: impl Iterator<Item = (&'a Id, bool)>,
) where
    Id: Clone + Eq + Hash + 'a,
{
    let unsettled = visible
        .filter(|(_id, terminal)| !terminal)
        .map(|(id, _terminal)| id.clone())
        .collect::<HashSet<_>>();
    active.retain(|id| unsettled.contains(id));
}

fn advance(
    application: &ApplicationHost,
    redemption_id: PairingRedemptionId,
    expected: PairingRedemptionPhase,
    next: PairingRedemptionPhase,
) -> Result<PairingRedemption, String> {
    application
        .exec_command(AdvancePairingRedemption {
            redemption_id,
            expected,
            next,
        })
        .map_err(|error| error.to_string())
}

fn advance_initiation(
    application: &ApplicationHost,
    initiation_id: PairingInitiationId,
    expected: PairingInitiationPhase,
    next: PairingInitiationPhase,
) -> Result<PairingInitiation, String> {
    application
        .exec_command(AdvancePairingInitiation {
            initiation_id,
            expected,
            next,
        })
        .map_err(|error| error.to_string())
}

fn confirmed_peer(
    node_id: NodeId,
    local: &NativeNodeDescriptor,
    receipt: &PairingReceipt,
    comparison_code: &str,
) -> Result<NativeNodeDescriptor, String> {
    receipt.validate()?;
    if comparison_code != receipt.comparison_code {
        return Err("pairing comparison code does not match".to_owned());
    }
    if local.node_id != node_id {
        return Err("native descriptor does not match the executing Myko node".to_owned());
    }
    if same_descriptor_identity(local, &receipt.server) {
        Ok(receipt.client.clone())
    } else if same_descriptor_identity(local, &receipt.client) {
        Ok(receipt.server.clone())
    } else {
        Err("pairing receipt does not name this node".to_owned())
    }
}

fn same_descriptor_identity(left: &NativeNodeDescriptor, right: &NativeNodeDescriptor) -> bool {
    left.node_id == right.node_id && left.endpoint.id == right.endpoint.id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_effect_stays_claimed_until_live_state_observes_its_terminal_phase() {
        let id = PairingRedemptionId::random();
        let mut active = HashSet::from([id.clone()]);

        retain_observed_unsettled(&mut active, [(&id, false)].into_iter());
        assert!(active.contains(&id));

        retain_observed_unsettled(&mut active, [(&id, true)].into_iter());
        assert!(active.is_empty());
    }

    #[test]
    fn discovered_pairing_declares_discovery_runtime_access() {
        let command = InitiateDiscoveredPairing {
            peer_node_id: NodeId::new(),
            ttl_seconds: 600,
        };
        assert_eq!(
            command.required_capabilities(),
            vec![
                crate::discovery_capability_id(),
                iroh_replicator_capability_id()
            ]
        );
    }

    #[tokio::test]
    async fn discovered_pairing_passes_outer_capability_preflight() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let node = crate::Node::open_loopback_with_policy(
            directory.path(),
            Duration::from_millis(20),
            |_| Ok(Arc::new(myko_federation::AllowAllAccessPolicy)),
        )
        .await
        .map_err(|error| error.to_string())?;
        let peer = NativeNodeDescriptor::new(
            NodeId::new(),
            myko_iroh::EndpointAddr::new(myko_iroh::SecretKey::generate().public()),
        );
        node.application()
            .resources()
            .get::<DiscoveryViewState>()
            .map_err(|error| error.to_string())?
            .publish(vec![myko_discovery::DiscoveredNode {
                descriptor: peer.clone(),
                display_name: "capability-test-peer".to_owned(),
                hostname: "test-host".to_owned(),
                kind: myko_discovery::ParticipantKind::FullNode,
                capabilities: myko_discovery::ParticipantCapabilities::full_node(),
                reachable: true,
                last_error: None,
            }]);
        let result = myko_federation::CommandWatchingClient::exec_typed_command(
            node.application(),
            InitiateDiscoveredPairing {
                peer_node_id: peer.node_id,
                ttl_seconds: 60,
            },
        )
        .await
        .map_err(|error| error.to_string());
        node.shutdown().await.map_err(|error| error.to_string())?;
        let initiation = result?;
        if initiation.peer != peer {
            return Err("discovered pairing resolved the wrong peer".to_owned());
        }
        Ok(())
    }
}
