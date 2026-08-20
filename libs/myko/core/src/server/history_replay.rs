//! Trait for replaying persisted events into a temporary store.

use std::sync::Arc;

use crate::{TS, server::HandlerRegistry, store::StoreRegistry, wire::MEvent};

/// A durable event returned by an entity-history lookup.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct HistoryEvent {
    pub id: i64,
    pub created_at: String,
    pub event: MEvent,
}

/// Provider for replaying historical events into a temporary `StoreRegistry`.
///
/// Implemented by the server layer (e.g., `PostgresHistoryReplayProvider`)
/// to enable point-in-time snapshots without coupling myko to a
/// specific persistence backend.
pub trait HistoryReplayProvider: Send + Sync {
    /// Load durable events for one entity, newest first.
    fn entity_history(
        &self,
        item_type: &str,
        item_id: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>, String> {
        let _ = (item_type, item_id, limit);
        Err("Entity history is not supported by this provider".to_string())
    }

    /// Replay all events with `created_at <= until` into a fresh `StoreRegistry`.
    ///
    /// `until` is an ISO 8601 timestamp string.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn replay_to_store(
        &self,
        until: &str,
        handler_registry: &HandlerRegistry,
    ) -> Result<Arc<StoreRegistry>, String>;
}
