//! Authenticated, conflict-checked append-only transaction reversal.
use std::sync::Arc;
mod dependencies;

use crate::{
    command::{CommandContext, CommandError, CommandHandler},
    core::capability::{EventPublishing, RequestScoped, ServerScoped},
    item::AnyItem,
    prelude::*,
    server::TransactionHistory,
    wire::MEventType,
};

mod boundary;
pub use boundary::*;
mod range;
pub use range::*;

/// An immutable log receipt. Removing its live row does not remove the receipt
/// from history or make the original transaction eligible a second time.
#[myko_item]
pub struct TransactionUndo {
    pub transaction_id: Arc<str>,
    pub undo_transaction_id: Arc<str>,
    pub user_id: Arc<str>,
    pub events_applied: usize,
}

#[myko_report_output]
pub struct UndoTransactionResult {
    pub transaction_id: Arc<str>,
    pub undo_transaction_id: Arc<str>,
    pub events_applied: usize,
}

/// Reverse one of the caller's transactions. Later edits to any affected
/// entity reject the entire plan; there is no implicit overwrite/force mode.
#[myko_command(UndoTransactionResult)]
pub struct UndoTransaction {
    pub transaction_id: String,
}

/// Undo the authenticated user's latest original transaction that has not
/// already been reversed. Returns None when there is no eligible history.
#[myko_command(Option<UndoTransactionResult>)]
pub struct UndoLastTransaction {}

fn failure(ctx: &CommandContext, error: impl std::fmt::Display) -> CommandError {
    CommandError::new(
        ctx.tx().to_owned(),
        ctx.command_id.to_string(),
        error.to_string(),
    )
}

impl CommandHandler for UndoTransaction {
    fn execute(self, ctx: CommandContext) -> Result<UndoTransactionResult, CommandError> {
        let user = ctx.user_id()?.clone();
        if ctx.tx().is_empty() || ctx.tx().contains('\0') {
            return Err(failure(&ctx, "undo transaction ID is invalid"));
        }
        let registry = ctx.__server_ctx().registry.clone();
        registry.transactions.coordinate(true, || {
            ctx.__server_ctx()
                .flush_history_writes()
                .map_err(|error| failure(&ctx, error))?;
            let provider = fresh_history(&ctx)?;
            let history = provider
                .transaction_history(&self.transaction_id)
                .map_err(|error| failure(&ctx, error))?;
            reverse(ctx, &user, history)
        })
    }
}

impl CommandHandler for UndoLastTransaction {
    fn execute(self, ctx: CommandContext) -> Result<Option<UndoTransactionResult>, CommandError> {
        undo_latest_matching(ctx, |_, _| Ok(true))
    }
}

/// Reverse the caller's latest transaction selected by a trusted domain policy.
///
/// The policy runs under the same local authority lock as conflict validation
/// and inverse writes. A matching conflict is returned, never skipped.
/// # Errors
/// Returns authentication, history, policy or reversal errors without forcing conflicts.
pub fn undo_latest_matching(
    ctx: CommandContext,
    mut matches: impl FnMut(
        &dyn crate::server::HistoryReplayProvider,
        &TransactionHistory,
    ) -> Result<bool, String>,
) -> Result<Option<UndoTransactionResult>, CommandError> {
    let user = ctx.user_id()?.clone();
    if ctx.tx().is_empty() || ctx.tx().contains('\0') {
        return Err(failure(&ctx, "undo transaction ID is invalid"));
    }
    let registry = ctx.__server_ctx().registry.clone();
    registry.transactions.coordinate(true, || {
        ctx.__server_ctx()
            .flush_history_writes()
            .map_err(|error| failure(&ctx, error))?;
        let provider = fresh_history(&ctx)?;
        if registry.transactions.busy_user(&user, ctx.tx()) {
            return Err(failure(
                &ctx,
                "user still has active commands or pending saga work",
            ));
        }
        let mut before = None;
        loop {
            let page = provider
                .user_transaction_page(&user, before, 32)
                .map_err(|error| failure(&ctx, error))?;
            if page.is_empty() {
                return Ok(None);
            }
            for candidate in page {
                if before.is_some_and(|cursor| candidate.first_id >= cursor) {
                    return Err(failure(&ctx, "transaction history cursor did not advance"));
                }
                before = Some(candidate.first_id);
                let history = provider
                    .transaction_history(&candidate.transaction_id)
                    .map_err(|error| failure(&ctx, error))?;
                // Validate ownership even for entries a domain policy would skip.
                if history.author().map_err(|error| failure(&ctx, error))? != &user {
                    return Err(failure(&ctx, "history returned another user's transaction"));
                }
                if contains_undoable_changes(&history)
                    && matches(provider.as_ref(), &history).map_err(|error| failure(&ctx, error))?
                {
                    return reverse(ctx, &user, history).map(Some);
                }
            }
        }
    })
}

fn fresh_history(
    ctx: &CommandContext,
) -> Result<Arc<dyn crate::server::HistoryReplayProvider>, CommandError> {
    if ctx.tx().is_empty() || ctx.tx().contains('\0') {
        return Err(failure(ctx, "undo transaction ID is invalid"));
    }
    let provider = ctx
        .__server_ctx()
        .history_replay()
        .cloned()
        .ok_or_else(|| failure(ctx, "durable transaction history is unavailable"))?;
    if provider
        .transaction_exists(ctx.tx())
        .map_err(|error| failure(ctx, error))?
    {
        return Err(failure(ctx, "undo requires a fresh transaction ID"));
    }
    Ok(provider)
}

fn reverse(
    ctx: CommandContext,
    user: &crate::identity::UserId,
    history: TransactionHistory,
) -> Result<UndoTransactionResult, CommandError> {
    validate_target(&ctx, user, &history)?;
    let (mut upserts, deletes) = prepare_changes(&ctx, &history)?;
    dependencies::validate(&ctx, &upserts, &deletes)?;
    crate::server::ProposedState {
        server: Some(ctx.__server_ctx().as_ref()),
        request: Some(ctx.req.clone()),
        host_id: ctx.req.host_id,
        registry: &ctx.__server_ctx().registry,
        upserts: &upserts,
        deletes: &deletes,
    }
    .validate(true)
    .map_err(|error| failure(&ctx, error))?;
    let count = upserts.len().saturating_add(deletes.len());
    let target = history.transaction_id;
    let result = UndoTransactionResult {
        transaction_id: target.clone(),
        undo_transaction_id: ctx.req.tx.clone(),
        events_applied: count,
    };
    let receipt = TransactionUndo {
        id: TransactionUndoId::from(target.clone()),
        transaction_id: target.clone(),
        undo_transaction_id: ctx.req.tx.clone(),
        user_id: Arc::from(user.as_ref()),
        events_applied: count,
    };
    ctx.__server_ctx()
        .require_undo_backend(TransactionUndo::ENTITY_NAME_STATIC)
        .map_err(|error| failure(&ctx, error))?;
    upserts.push(Arc::new(receipt));
    if ctx.user_id()? != user {
        return Err(failure(
            &ctx,
            "authenticated user changed while planning undo",
        ));
    }
    let undo = ctx.for_undo(target);
    undo.emit_replace_any_batch(upserts, deletes)?;
    undo.__server_ctx()
        .flush_history_writes()
        .map_err(|error| failure(&undo, error))?;
    Ok(result)
}

/// Whether a transaction contains any explicitly opted-in authored state.
#[must_use]
pub fn contains_undoable_changes(history: &TransactionHistory) -> bool {
    history
        .changes
        .iter()
        .any(|change| crate::item::is_undoable_entity(&change.key.item_type))
}

fn validate_target(
    ctx: &CommandContext,
    user: &crate::identity::UserId,
    history: &TransactionHistory,
) -> Result<(), CommandError> {
    if history.author().map_err(|error| failure(ctx, error))? != user {
        return Err(failure(ctx, "cannot undo another user's transaction"));
    }
    if history.transaction_id.as_ref() == ctx.tx() {
        return Err(failure(ctx, "undo requires a fresh transaction ID"));
    }
    let mut excluded: Vec<_> = history
        .changes
        .iter()
        .filter(|change| !crate::item::is_undoable_entity(&change.key.item_type))
        .map(|change| change.key.item_type.as_ref())
        .collect();
    excluded.sort_unstable();
    excluded.dedup();
    if !excluded.is_empty() {
        return Err(failure(
            ctx,
            format!(
                "transaction includes entities not opted into undo: {}",
                excluded.join(", ")
            ),
        ));
    }
    if history.predates_undo_boundary {
        return Err(failure(
            ctx,
            "transaction predates the start of undo history",
        ));
    }
    if !history.complete {
        return Err(failure(ctx, "transaction has no durable completion record"));
    }
    if history.already_undone
        || history.records.iter().any(|record| {
            record
                .provenance
                .as_ref()
                .is_some_and(|p| p.undo_of_tx.is_some())
        })
    {
        return Err(failure(
            ctx,
            "transaction is already reversed or is itself an undo",
        ));
    }
    if ctx
        .__server_ctx()
        .registry
        .transactions
        .busy(&history.transaction_id)
    {
        return Err(failure(
            ctx,
            "transaction still has active commands or pending saga work",
        ));
    }
    Ok(())
}

type Changes = (Vec<Arc<dyn AnyItem>>, Vec<Arc<dyn AnyItem>>);

fn prepare_changes(
    ctx: &CommandContext,
    history: &TransactionHistory,
) -> Result<Changes, CommandError> {
    prepare_changes_with(ctx, history, |key| {
        ctx.__server_ctx()
            .registry
            .get(&key.item_type)
            .and_then(|store| store.get_value(&key.item_id))
    })
}

fn prepare_changes_with(
    ctx: &CommandContext,
    history: &TransactionHistory,
    current: impl Fn(&crate::server::HistoryEntityKey) -> Option<Arc<dyn AnyItem>>,
) -> Result<Changes, CommandError> {
    let mut upserts = Vec::<Arc<dyn AnyItem>>::new();
    let mut deletes = Vec::<Arc<dyn AnyItem>>::new();
    let mut conflicts = Vec::new();
    for change in &history.changes {
        ctx.__server_ctx()
            .require_undo_backend(&change.key.item_type)
            .map_err(|error| failure(ctx, error))?;
        let current = current(&change.key);
        let expected = (change.last.change_type == MEventType::SET)
            .then(|| {
                ctx.__server_ctx()
                    .parse_item(&change.key.item_type, change.last.item.clone())
            })
            .flatten();
        if change.last.change_type == MEventType::SET && expected.is_none() {
            return Err(failure(
                ctx,
                format!(
                    "cannot decode historical {}:{}",
                    change.key.item_type, change.key.item_id
                ),
            ));
        }
        let matches = match (&current, &expected) {
            (None, None) => true,
            (Some(current), Some(expected)) => current.equals(expected.as_ref()),
            _ => false,
        };
        if change.has_conflict || !matches {
            conflicts.push(format!("{}:{}", change.key.item_type, change.key.item_id));
            continue;
        }
        if let Some(before) = change
            .before
            .as_ref()
            .filter(|event| event.change_type == MEventType::SET)
        {
            let item = ctx
                .__server_ctx()
                .parse_item(&change.key.item_type, before.item.clone())
                .ok_or_else(|| {
                    failure(
                        ctx,
                        format!(
                            "cannot decode prior {}:{}",
                            change.key.item_type, change.key.item_id
                        ),
                    )
                })?;
            if item.id() != change.key.item_id {
                return Err(failure(ctx, "historical entity identity mismatch"));
            }
            upserts.push(item);
        } else if let Some(current) = current {
            deletes.push(current);
        }
    }
    if !conflicts.is_empty() {
        return Err(failure(
            ctx,
            format!("undo conflicts with later edits: {}", conflicts.join(", ")),
        ));
    }
    Ok((upserts, deletes))
}

/// Restrict transaction-history reads to the authenticated originating user.
pub(crate) fn own_history(
    ctx: &ReportContext,
    tx: &str,
) -> Result<Vec<crate::wire::MEvent>, String> {
    let user = ctx.req.user_id().map_err(|error| error.to_string())?;
    let provider = ctx
        .__server_ctx()
        .history_replay()
        .ok_or("durable transaction history is unavailable")?;
    let history = provider.transaction_history(tx)?;
    if history.author()? != user {
        return Err("cannot inspect another user's transaction".into());
    }
    Ok(history
        .records
        .into_iter()
        .map(|record| record.event)
        .collect())
}
