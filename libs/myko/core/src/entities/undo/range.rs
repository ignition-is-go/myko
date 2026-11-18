//! Checked multi-transaction reversal and inspectable, user-scoped history.
use super::{
    Changes, TransactionUndo, UndoReadContext, contains_undoable_changes, dependencies, failure,
    fresh_history, prepare_changes_with, validate_target,
};
use crate::{
    command::{CommandContext, CommandError, CommandHandler},
    core::capability::{EventPublishing, RequestScoped, ServerScoped},
    item::AnyItem,
    prelude::*,
    server::{HistoryEntityKey, HistoryReplayProvider, TransactionHistory},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[myko_report_output]
pub struct UndoConflict {
    pub transaction_id: Arc<str>,
    pub user_id: Option<Arc<str>>,
    pub is_other_user: bool,
    pub entity_type: Arc<str>,
    pub entity_id: Arc<str>,
    pub created_at: Arc<str>,
}
#[myko_report_output]
pub struct UndoLogEntry {
    pub transaction_id: Arc<str>,
    pub first_id: i64,
    pub command: Arc<str>,
    pub context: Option<String>,
    pub created_at: Arc<str>,
    pub entity_types: Vec<Arc<str>>,
    pub entities_changed: usize,
    /// Whether undoing the entire prefix through this entry is currently blocked.
    pub blocked_reason: Option<String>,
    pub conflicts: Vec<UndoConflict>,
}
#[myko_report_output]
pub struct UndoLogPage {
    pub entries: Vec<UndoLogEntry>,
    pub next_before: Option<i64>,
}
#[myko_report_output]
pub struct UndoBatchResult {
    pub transactions_undone: usize,
    pub entities_changed: usize,
}
#[myko_command(UndoLogPage)]
pub struct GetUndoTransactions {
    pub limit: usize,
}
impl CommandHandler for GetUndoTransactions {
    fn execute(self, ctx: CommandContext) -> Result<UndoLogPage, CommandError> {
        undo_log_matching(&ctx, self.limit, |_, _| Ok(true))
    }
}
#[myko_command(UndoBatchResult)]
pub struct UndoTransactions {
    /// Newest first, covering a contiguous prefix of eligible user actions.
    pub transaction_ids: Vec<Arc<str>>,
}
impl CommandHandler for UndoTransactions {
    fn execute(self, ctx: CommandContext) -> Result<UndoBatchResult, CommandError> {
        undo_range_matching(&ctx, &self.transaction_ids, |_, _| Ok(true))
    }
}

type Overlay = HashMap<HistoryEntityKey, Option<Arc<dyn AnyItem>>>;
type Policy<'a> =
    &'a mut dyn FnMut(&dyn HistoryReplayProvider, &TransactionHistory) -> Result<bool, String>;

fn candidates(
    ctx: &impl UndoReadContext,
    limit: usize,
    matches: Policy<'_>,
) -> Result<Vec<(i64, TransactionHistory)>, CommandError> {
    if !(1..=100).contains(&limit) {
        return Err(failure(ctx, "undo history limit must be between 1 and 100"));
    }
    let user = ctx.undo_actor()?;
    let provider = fresh_history(ctx)?;
    let mut before = None;
    let mut selected = Vec::new();
    let mut ignored = Vec::new();
    loop {
        let page = provider
            .user_transaction_page(user, before, 32)
            .map_err(|e| failure(ctx, e))?;
        if page.is_empty() {
            break;
        }
        for candidate in page {
            if before.is_some_and(|cursor| candidate.first_id >= cursor) {
                return Err(failure(ctx, "transaction history cursor did not advance"));
            }
            before = Some(candidate.first_id);
            let history = provider
                .transaction_history_excluding(&candidate.transaction_id, &ignored)
                .map_err(|e| failure(ctx, e))?;
            if history.author().map_err(|e| failure(ctx, e))? != user {
                return Err(failure(ctx, "history returned another user's transaction"));
            }
            if contains_undoable_changes(&history)
                && matches(provider.as_ref(), &history).map_err(|e| failure(ctx, e))?
            {
                ignored.push(candidate.transaction_id);
                selected.push((candidate.first_id, history));
                if selected.len() == limit {
                    return Ok(selected);
                }
            }
        }
    }
    Ok(selected)
}

fn extend(
    ctx: &impl UndoReadContext,
    history: &TransactionHistory,
    overlay: &mut Overlay,
) -> Result<usize, CommandError> {
    validate_target(ctx, ctx.undo_actor()?, history)?;
    let (upserts, deletes) = prepare_changes_with(ctx, history, |key| {
        overlay.get(key).cloned().unwrap_or_else(|| {
            ctx.__server_ctx()
                .registry
                .get(&key.item_type)
                .and_then(|store| store.get_value(&key.item_id))
        })
    })?;
    let count = upserts.len().saturating_add(deletes.len());
    for item in upserts {
        overlay.insert(
            HistoryEntityKey::new(item.entity_type(), item.id()),
            Some(item),
        );
    }
    for item in deletes {
        overlay.insert(HistoryEntityKey::new(item.entity_type(), item.id()), None);
    }
    Ok(count)
}
fn changes(ctx: &impl UndoReadContext, overlay: &Overlay) -> Changes {
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    for (key, value) in overlay {
        match value {
            Some(value) => upserts.push(value.clone()),
            None => {
                if let Some(value) = ctx
                    .__server_ctx()
                    .registry
                    .get(&key.item_type)
                    .and_then(|store| store.get_value(&key.item_id))
                {
                    deletes.push(value);
                }
            }
        }
    }
    (upserts, deletes)
}
fn check(ctx: &impl UndoReadContext, plan: &Changes) -> Result<(), CommandError> {
    dependencies::validate(ctx, &plan.0, &plan.1)?;
    crate::server::ProposedState {
        server: Some(ctx.__server_ctx().as_ref()),
        request: Some(ctx.__request().clone()),
        registry: &ctx.__server_ctx().registry,
        host_id: ctx.host_id(),
        upserts: &plan.0,
        deletes: &plan.1,
    }
    .validate(true)
    .map_err(|e| failure(ctx, e))
}
fn log_entry(
    first_id: i64,
    history: &TransactionHistory,
    blocked_reason: Option<String>,
) -> UndoLogEntry {
    let first = history.records.first();
    let command = first
        .and_then(|record| record.provenance.as_ref())
        .map_or_else(|| Arc::from("Edit"), |p| p.command_id.clone());
    let mut kinds: Vec<Arc<str>> = history
        .changes
        .iter()
        .map(|change| change.key.item_type.clone())
        .collect();
    kinds.sort();
    kinds.dedup();
    let conflicts = history
        .changes
        .iter()
        .filter_map(|change| {
            let record = change.blocking_record.as_ref()?;
            Some(UndoConflict {
                transaction_id: record.event.tx.clone(),
                is_other_user: record
                    .provenance
                    .as_ref()
                    .and_then(|p| p.actor_user_id.as_ref())
                    .is_some_and(|actor| history.author().is_ok_and(|user| user != actor)),
                user_id: record
                    .provenance
                    .as_ref()
                    .and_then(|p| p.actor_user_id.as_ref())
                    .map(|user| Arc::from(user.as_ref())),
                entity_type: change.key.item_type.clone(),
                entity_id: change.key.item_id.clone(),
                created_at: record.event.created_at.clone(),
            })
        })
        .collect();
    UndoLogEntry {
        transaction_id: history.transaction_id.clone(),
        context: None,
        first_id,
        command,
        created_at: first
            .map(|record| record.event.created_at.clone())
            .unwrap_or_default(),
        entity_types: kinds,
        entities_changed: history.changes.len(),
        blocked_reason,
        conflicts,
    }
}

/// Inspect eligible user transactions and whether each prefix can be reversed.
/// # Errors
/// Returns authentication, persistence or history-selection errors.
pub fn undo_log_matching(
    ctx: &impl UndoReadContext,
    limit: usize,
    mut matches: impl FnMut(&dyn HistoryReplayProvider, &TransactionHistory) -> Result<bool, String>,
) -> Result<UndoLogPage, CommandError> {
    ctx.undo_actor()?;
    let registry = ctx.__server_ctx().registry.clone();
    registry.transactions.coordinate(true, || {
        ctx.__server_ctx()
            .flush_history_writes()
            .map_err(|e| failure(ctx, e))?;
        let histories = candidates(ctx, limit, &mut matches)?;
        let next_before = (histories.len() == limit)
            .then(|| histories.last().map(|(id, _)| *id))
            .flatten();
        let mut overlay = Overlay::new();
        let mut hard_error: Option<String> = None;
        let mut entries = Vec::new();
        for (first_id, history) in histories {
            if hard_error.is_none()
                && let Err(error) = extend(ctx, &history, &mut overlay)
            {
                hard_error = Some(error.message);
            }
            let reason = hard_error.clone().or_else(|| {
                check(ctx, &changes(ctx, &overlay))
                    .err()
                    .map(|error| error.message)
            });
            entries.push(log_entry(first_id, &history, reason));
        }
        Ok(UndoLogPage {
            entries,
            next_before,
        })
    })
}

/// Revalidate and undo a newest-first prefix in one coordinated request.
/// All conflicts and final-state dependencies are checked before any inverse write.
/// # Errors
/// Returns stale-selection, authorization, conflict, dependency or persistence errors.
pub fn undo_range_matching(
    ctx: &CommandContext,
    ids: &[Arc<str>],
    mut matches: impl FnMut(&dyn HistoryReplayProvider, &TransactionHistory) -> Result<bool, String>,
) -> Result<UndoBatchResult, CommandError> {
    let user = ctx.user_id()?.clone();
    let registry = ctx.__server_ctx().registry.clone();
    registry.transactions.coordinate(true, || {
        ctx.__server_ctx()
            .flush_history_writes()
            .map_err(|e| failure(ctx, e))?;
        if registry.transactions.busy_user(&user, ctx.tx()) {
            return Err(failure(
                ctx,
                "user still has active commands or pending saga work",
            ));
        }
        if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
            return Err(failure(
                ctx,
                "undo selection contains duplicate transactions",
            ));
        }
        let histories = candidates(ctx, ids.len(), &mut matches)?;
        if histories.len() != ids.len()
            || histories
                .iter()
                .zip(ids)
                .any(|((_, h), id)| &h.transaction_id != id)
        {
            return Err(failure(
                ctx,
                "Undo history changed. Refresh the log and select the range again.",
            ));
        }
        let mut overlay = Overlay::new();
        let mut counts = Vec::new();
        for (_, history) in &histories {
            counts.push(extend(ctx, history, &mut overlay)?);
        }
        let (upserts, deletes) = changes(ctx, &overlay);
        check(ctx, &(upserts.clone(), deletes.clone()))?;
        ctx.__server_ctx()
            .require_undo_backend(TransactionUndo::ENTITY_NAME_STATIC)
            .map_err(|e| failure(ctx, e))?;
        let result = UndoBatchResult {
            transactions_undone: ids.len(),
            entities_changed: upserts.len().saturating_add(deletes.len()),
        };
        let primary = ids
            .first()
            .ok_or_else(|| failure(ctx, "select at least one transaction"))?;
        let undo = ctx.clone().for_undo(primary.clone());
        undo.emit_replace_any_batch(upserts, deletes)?;
        for ((_, history), count) in histories.iter().zip(counts) {
            let receipt = TransactionUndo {
                id: history.transaction_id.clone().into(),
                transaction_id: history.transaction_id.clone(),
                undo_transaction_id: ctx.req.tx.clone(),
                user_id: Arc::from(user.as_ref()),
                events_applied: count,
            };
            ctx.clone()
                .for_undo(history.transaction_id.clone())
                .emit_set(&receipt)?;
        }
        ctx.__server_ctx()
            .flush_history_writes()
            .map_err(|e| failure(ctx, e))?;
        Ok(result)
    })
}
