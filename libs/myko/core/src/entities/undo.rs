//! Undo a transaction — reverse the changeset emitted by a single command.
//!
//! Sibling to restore points (see `docs/superpowers/specs/2026-06-17-restore-points-design.md`).
//! A `tx` groups the events of one command. Undoing it converges each touched entity
//! back to its state immediately before the transaction:
//!
//! - prior state exists (the tx modified or deleted it) -> SET to the prior value
//!   (revert / revive);
//! - no prior state (the tx created it) -> DEL it.
//!
//! Because myko events are full-entity snapshots, undoing a transaction that a *later*
//! transaction has since built on top of would clobber that newer change. Such entities
//! are flagged `conflict`; `UndoTransaction` refuses unless `force` is set.
//!
//! The undo is itself a forward transaction (no history rewrite) and is therefore
//! redoable by undoing the undo.

use std::sync::Arc;

use serde_json::Value;

use crate::prelude::*;

/// One entity's undo action for a transaction.
#[myko_report_output]
pub struct UndoTarget {
    pub item_type: Arc<str>,
    pub item_id: Arc<str>,
    /// State to revert/revive to. `None` => the transaction created this entity, so undo
    /// removes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub prior: Option<Value>,
    /// The value the transaction itself last wrote for this entity — used as the DEL
    /// payload when `prior` is `None`.
    pub in_tx_value: Value,
    /// A later transaction also modified this entity; reverting would clobber it.
    pub conflict: bool,
}

/// Report: the undo plan for a transaction (one `UndoTarget` per touched entity).
#[myko_report(Vec<UndoTarget>)]
pub struct TransactionUndoPlan {
    pub transaction_id: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReportHandler for TransactionUndoPlan {
    type Output = Vec<UndoTarget>;

    fn compute(&self, ctx: ReportContext) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let plan = match ctx.transaction_undo_plan(&self.transaction_id) {
            Ok(plan) => plan,
            Err(err) => {
                eprintln!("[TransactionUndoPlan] failed: {err}");
                Vec::new()
            }
        };
        hyphae::Cell::new(Arc::new(plan)).lock()
    }
}

/// Undo a transaction by re-emitting the inverse of its events.
///
/// Reverts/revives entities to their pre-transaction state and removes entities the
/// transaction created. Returns the number of events applied. Refuses if any touched
/// entity was modified by a later transaction unless `force` is set.
#[myko_command(usize)]
pub struct UndoTransaction {
    pub transaction_id: String,
    /// Override conflicts (a later transaction modified some entities). Default false.
    #[serde(default)]
    pub force: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl CommandHandler for UndoTransaction {
    fn execute(self, ctx: CommandContext) -> Result<usize, CommandError> {
        use crate::wire::{MEvent, MEventType};

        let plan = ctx.exec_report(TransactionUndoPlan {
            transaction_id: self.transaction_id.clone(),
        })?;

        if plan.is_empty() {
            return Ok(0);
        }

        if !self.force {
            let conflicted: Vec<String> = plan
                .iter()
                .filter(|t| t.conflict)
                .map(|t| format!("{}:{}", t.item_type, t.item_id))
                .collect();
            if !conflicted.is_empty() {
                return Err(CommandError {
                    tx: ctx.tx().to_string(),
                    command_id: "UndoTransaction".to_string(),
                    message: format!(
                        "Cannot undo transaction {}: {} entit{} modified by a later transaction \
                         (reverting would clobber newer changes). Pass force=true to override. \
                         Conflicts: {}",
                        self.transaction_id,
                        conflicted.len(),
                        if conflicted.len() == 1 { "y was" } else { "ies were" },
                        conflicted.join(", "),
                    ),
                });
            }
        }

        let tx = ctx.tx().to_string();
        let source_id = Some(ctx.host_id().to_string());
        let created_at = ctx.created_at().to_string();

        let mut events = Vec::with_capacity(plan.len());
        for target in &plan {
            match &target.prior {
                Some(prior) => events.push(MEvent {
                    item: prior.clone(),
                    change_type: MEventType::SET,
                    item_type: target.item_type.to_string(),
                    created_at: created_at.clone(),
                    tx: tx.clone(),
                    source_id: source_id.clone(),
                    options: None,
                }),
                None => events.push(MEvent {
                    item: target.in_tx_value.clone(),
                    change_type: MEventType::DEL,
                    item_type: target.item_type.to_string(),
                    created_at: created_at.clone(),
                    tx: tx.clone(),
                    source_id: source_id.clone(),
                    options: None,
                }),
            }
        }

        ctx.emit_event_batch(events)
    }
}
