//! Trait for replaying persisted events into a temporary store.

use std::sync::Arc;

use crate::{server::HandlerRegistry, store::StoreRegistry};

/// Provider for replaying historical events into a temporary StoreRegistry.
///
/// Implemented by the server layer (e.g., PostgresHistoryReplayProvider)
/// to enable point-in-time snapshots without coupling myko-rs to a
/// specific persistence backend.
pub trait HistoryReplayProvider: Send + Sync {
    /// Replay all events with `created_at <= until` into a fresh StoreRegistry.
    ///
    /// `until` is an ISO 8601 timestamp string.
    fn replay_to_store(
        &self,
        until: &str,
        handler_registry: &HandlerRegistry,
    ) -> Result<Arc<StoreRegistry>, String>;
}
