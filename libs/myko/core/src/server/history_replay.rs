//! Trait for replaying persisted events into a temporary store.

use std::sync::Arc;

use crate::{
    TS,
    hyphae::{Cell, CellImmutable},
    server::HandlerRegistry,
    store::StoreRegistry,
    wire::MEvent,
};

/// A durable event returned by an entity-history lookup.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct HistoryEvent {
    pub id: i64,
    pub created_at: String,
    pub event: MEvent,
}

/// Typed identity of an entity whose durable event history is queried.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HistoryEntityKey {
    pub item_type: Arc<str>,
    pub item_id: Arc<str>,
}

/// A committed PostgreSQL history row observed through the backend's notify stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedHistoryEvent {
    pub key: HistoryEntityKey,
    pub row_id: i64,
}

impl HistoryEntityKey {
    #[must_use]
    pub fn new(item_type: impl Into<Arc<str>>, item_id: impl Into<Arc<str>>) -> Self {
        Self {
            item_type: item_type.into(),
            item_id: item_id.into(),
        }
    }
}

/// Provider for replaying historical events into a temporary `StoreRegistry`.
///
/// Implemented by the server layer (e.g., `PostgresHistoryReplayProvider`)
/// to enable point-in-time snapshots without coupling myko to a
/// specific persistence backend.
pub trait HistoryReplayProvider: Send + Sync {
    /// Latest committed history row observed through the backend notification stream.
    fn committed_history_event(&self) -> Cell<Option<Arc<CommittedHistoryEvent>>, CellImmutable> {
        Cell::new(None).lock()
    }

    /// Load durable events for one entity, newest first.
    fn entity_history(
        &self,
        key: &HistoryEntityKey,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>, String> {
        let _ = (key, limit);
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
