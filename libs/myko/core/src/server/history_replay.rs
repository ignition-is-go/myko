//! Provider for targeted reads of historical events.
//!
//! Note: there is intentionally no whole-store replay here. The only full reconstruction
//! of state from the log happens once, at server startup (the Postgres consumer's
//! bootstrap snapshot). Runtime features (restore points, undo, diffs) use the bounded,
//! targeted reads below instead.

use std::sync::Arc;

use crate::server::HandlerRegistry;

/// Provider for bounded, point-in-time reads of the durable event history, without
/// reconstructing the whole store. Backed by the server layer (e.g. Postgres).
pub trait HistoryReplayProvider: Send + Sync {
    /// Return every event of a transaction, ascending by event id.
    ///
    /// Default: unsupported. Backends that retain a transaction-keyed event log
    /// override this.
    fn events_for_transaction(
        &self,
        _transaction_id: &str,
        _handler_registry: &HandlerRegistry,
    ) -> Result<Vec<crate::wire::MEvent>, String> {
        Err("events_for_transaction not supported by this history provider".to_string())
    }

    /// Compute the undo plan for a transaction: for each entity the transaction touched,
    /// its state immediately before the transaction (`prior`), the value the transaction
    /// last wrote (`in_tx_value`), and whether a later transaction also touched it
    /// (`conflict`).
    ///
    /// Default: unsupported.
    fn transaction_undo_plan(
        &self,
        _transaction_id: &str,
        _handler_registry: &HandlerRegistry,
    ) -> Result<Vec<crate::entities::undo::UndoTarget>, String> {
        Err("transaction_undo_plan not supported by this history provider".to_string())
    }

    /// Every entity with an event strictly after `until`, paired with its value AT `until`:
    /// `Some(value)` if its latest event at or before `until` was a SET, `None` if it did not
    /// exist or was deleted then. A single time-bounded scan (events since `until`) — the
    /// basis for delta restore. Default: unsupported.
    fn restore_changes_since(
        &self,
        _until: &str,
        _handler_registry: &HandlerRegistry,
    ) -> Result<Vec<RestoreChange>, String> {
        Err("restore_changes_since not supported by this history provider".to_string())
    }
}

/// An entity that changed after a timestamp, with its value AT that timestamp:
/// `(item_type, item_id, value_at_timestamp)`. `None` = absent/deleted then.
pub type RestoreChange = (Arc<str>, Arc<str>, Option<serde_json::Value>);
