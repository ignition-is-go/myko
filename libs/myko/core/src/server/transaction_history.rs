//! Backend-independent transaction history consumed by reversal planning.
use std::sync::Arc;

use super::{EventRecord, HistoryEntityKey};
use crate::wire::MEvent;

/// Stable keyset cursor for an original, completed user transaction.
#[derive(Clone, Debug)]
pub struct UserTransaction {
    pub transaction_id: Arc<str>,
    pub first_id: i64,
}

#[derive(Clone, Debug)]
pub struct TransactionChange {
    pub key: HistoryEntityKey,
    pub before: Option<MEvent>,
    pub last: MEvent,
    pub last_id: i64,
    pub latest_id: i64,
    pub has_conflict: bool,
    pub blocking_record: Option<EventRecord>,
}

#[derive(Clone, Debug)]
pub struct TransactionHistory {
    pub transaction_id: Arc<str>,
    pub records: Vec<EventRecord>,
    pub changes: Vec<TransactionChange>,
    pub already_undone: bool,
    pub complete: bool,
    pub predates_undo_boundary: bool,
}

impl TransactionHistory {
    /// Reject unknown or mixed authors rather than treating connection IDs as users.
    /// # Errors
    /// Returns an error if any event lacks the same verified originating user.
    pub fn author(&self) -> Result<&crate::identity::UserId, String> {
        let author = self
            .records
            .first()
            .and_then(|record| record.provenance.as_ref())
            .and_then(|provenance| provenance.actor_user_id.as_ref())
            .ok_or_else(|| "transaction has no authenticated author".to_owned())?;
        if self.records.iter().any(|record| {
            record.provenance.as_ref().is_none_or(|p| {
                p.actor_user_id.as_ref() != Some(author) || p.request_tx != self.transaction_id
            })
        }) {
            return Err("transaction has unknown or mixed authenticated authors".into());
        }
        Ok(author)
    }
}
