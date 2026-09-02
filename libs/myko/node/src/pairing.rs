//! Framework-owned pairing commands, redemption entity, report, and receipt view.

use std::{collections::HashSet, sync::Arc, time::Duration};

use hyphae::{Signal, Watchable as _};
use myko_app::capability::{
    CollectionBuilding as _, CommandExecuting as _, CommandQuerying as _, EventPublishing as _,
    NodeScoped as _, Querying as _, RequestScoped as _, ResourceScoped as _,
};
use myko_app::{
    AppError, ApplicationNode, CommandContext, CommandError, CommandHandler, HandlerSubscription,
    ReportContext, ReportHandler, ViewContext, ViewHandler, myko_query, myko_report, myko_view,
};
use myko_federation::{
    ItemProjection, ItemQuery, LiveCollection, LiveSubscription, LogPosition, NodeId, ScopeId,
};
use myko_iroh::{
    IrohReplicator, NativeNodeDescriptor, PairingInvitation, PairingReceipt,
    PairingReceiptSubscription,
};
use myko_items::{myko_command, myko_item, myko_subtype};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use uuid::Uuid;

use crate::{
    FederationService, Peer, RememberPeer,
    peer::{PeerRoster, PeerRosterId, peer_scope},
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

impl Eq for PairingRedemption {}

/// One authenticated inbound receipt awaiting local operator confirmation.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct PendingPairingReceipt {
    pub receipt: PairingReceipt,
}

impl Eq for PendingPairingReceipt {}

impl PairingRedemptionId {
    #[must_use]
    pub fn random() -> Self {
        Self::from(Uuid::now_v7().to_string())
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

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
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

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
        let redemption = PairingRedemption {
            id: PairingRedemptionId::random(),
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

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
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

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
        if context.principal_id().as_str() != format!("node:{}", context.node_id()) {
            return Err(CommandError::reject(
                "pairing receipt recording requires the executing node principal",
            ));
        }
        self.receipt.validate().map_err(CommandError::reject)?;
        let receipt = PendingPairingReceipt {
            id: PendingPairingReceiptId::from(self.receipt.invitation_id.to_string()),
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

impl CommandHandler for AdvancePairingRedemption {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
        if context.principal_id().as_str() != format!("node:{}", context.node_id()) {
            return Err(CommandError::reject(
                "pairing-redemption transition requires the executing node principal",
            ));
        }
        let mut redemption = context
            .exec_query(GetPairingRedemptionById {
                id: self.redemption_id,
            })?
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

/// Live state of one invitation redemption.
#[myko_report(Option<PairingRedemption>, item = PairingRedemption)]
#[derive(PartialEq, Eq)]
pub struct PairingRedemptionReport {
    pub source_node: NodeId,
    pub redemption_id: PairingRedemptionId,
}

impl ReportHandler for PairingRedemptionReport {
    type Output = Option<PairingRedemption>;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build(&self, context: &ReportContext) -> Result<LiveSubscription<Self::Output>, AppError> {
        Ok(context
            .query(
                self.source_node,
                peer_scope(self.source_node),
                GetPairingRedemptionById {
                    id: self.redemption_id.clone(),
                },
            )?
            .map_value(Clone::clone))
    }
}

#[myko_query(PendingPairingReceipt)]
#[derive(Copy, PartialEq, Eq)]
struct GetPendingPairingReceipts;

impl ItemQuery for GetPendingPairingReceipts {
    type Item = PendingPairingReceipt;
    type Output = Vec<PairingReceipt>;

    fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
        let mut receipts = projection
            .values()
            .map(|pending| pending.receipt.clone())
            .collect::<Vec<_>>();
        receipts.sort_by_key(|receipt| receipt.invitation_id);
        receipts
    }
}

impl myko_app::QueryHandler for GetPendingPairingReceipts {}

/// Live authenticated receipts for invitations issued by this node.
#[myko_view(PairingReceipt, item = PendingPairingReceipt)]
#[derive(Copy, PartialEq, Eq)]
pub struct PairingReceiptsView;

impl ViewHandler for PairingReceiptsView {
    type Item = PairingReceipt;
    type Cursor = LogPosition;

    fn item_key(item: &Self::Item) -> Arc<str> {
        Arc::from(item.invitation_id.to_string())
    }

    fn build(&self, context: &ViewContext) -> Result<LiveCollection<Self::Item>, AppError> {
        let source_node = context.node_id();
        let receipts = context.query(
            source_node,
            peer_scope(source_node),
            GetPendingPairingReceipts,
        )?;
        context.collection_from_subscription(&receipts, Self::item_key)
    }
}

#[derive(Debug)]
pub struct PairingSupervisor {
    stopping: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl PairingSupervisor {
    pub fn start(application: ApplicationNode, replicator: IrohReplicator) -> Result<Self, String> {
        let redemptions = application
            .watch_query(
                application.node().node_id(),
                peer_scope(application.node().node_id()),
                GetAllPairingRedemptions,
            )
            .map_err(|error| error.to_string())?;
        let receipts = replicator.subscribe_pairing_receipts();
        let (stopping, stop) = watch::channel(false);
        let task = tokio::spawn(run(application, replicator, redemptions, receipts, stop));
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
    application: ApplicationNode,
    replicator: IrohReplicator,
    redemptions: HandlerSubscription<Vec<PairingRedemption>>,
    mut receipts: PairingReceiptSubscription,
    mut stopping: watch::Receiver<bool>,
) -> Result<(), String> {
    let (change_sender, mut change_receiver) = mpsc::unbounded_channel();
    change_sender.send(()).map_err(|_| {
        "pairing-redemption subscription could not publish initial state".to_owned()
    })?;
    let updates = change_sender.clone();
    let changes_guard = redemptions.live().state().subscribe(move |signal| {
        if let Signal::Value(_) = signal {
            let _sent = updates.send(());
        }
    });
    let mut recover = true;
    let mut active = HashSet::new();
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
                let state = redemptions.live().current();
                match state.liveness {
                    myko_federation::SubscriptionLiveness::Current => {
                        let values = state.value.ok_or_else(|| {
                            "current pairing-redemption query omitted its value".to_owned()
                        })?;
                        if recover {
                            recover_redemptions(&application, &values)?;
                            recover = false;
                        }
                        start_redemptions(
                            &application,
                            &replicator,
                            values,
                            &mut active,
                            &mut effects,
                        )?;
                    }
                    myko_federation::SubscriptionLiveness::Connecting
                    | myko_federation::SubscriptionLiveness::Resynchronizing { .. } => {}
                    myko_federation::SubscriptionLiveness::Invalid { reason } => {
                        return Err(format!("pairing-redemption query became invalid: {reason}"));
                    }
                }
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
                    Some(Ok((id, result))) => {
                        active.remove(&id);
                        result?;
                    }
                    Some(Err(error)) => return Err(format!("pairing redemption panicked: {error}")),
                    None => {}
                }
            }
        }
    }
    effects.abort_all();
    while effects.join_next().await.is_some() {}
    drop(changes_guard);
    redemptions.shutdown().await;
    Ok(())
}

fn recover_redemptions(
    application: &ApplicationNode,
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
    application: &ApplicationNode,
    replicator: &IrohReplicator,
    redemptions: Vec<PairingRedemption>,
    active: &mut HashSet<PairingRedemptionId>,
    effects: &mut JoinSet<(PairingRedemptionId, Result<(), String>)>,
) -> Result<(), String> {
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
            let result = match effect_replicator
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
            };
            (id, result)
        });
    }
    Ok(())
}

fn advance(
    application: &ApplicationNode,
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
