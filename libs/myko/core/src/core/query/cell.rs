//! Cell-based reactive query types
//!
//! Provides type aliases for cell-based query results.
//!
//! For querying, use `store.select(predicate)` directly:
//!
//! ```text
//! // 1) Build a reactive query map from MykoServerContext:
//! let map = ctx.query_map(GetTargetsByQuery { active: Some(true), ..Default::default() }, req);
//!
//! // 2) Derive values from the map:
//! let names = map
//!   .entries()
//!   .map(|entries| entries.iter().map(|(_, item)| item.id().to_string()).collect::<Vec<_>>());
//!
//! // 3) Subscribe once in UI/server code and react to updates.
//! // The map stays hot and updates when underlying entities change.
//! ```

use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap};

use super::super::item::AnyItem;
use crate::wire::QueryWindow;

/// Type alias for a filtered `CellMap` of entities.
///
/// This is an immutable `CellMap` that automatically stays synchronized
/// with its source store. The subscription is managed internally by
/// hyphae's `CellMap`.
pub type FilteredCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

/// One authoritative bounded query page plus the cardinality of its full
/// logical result set.
///
/// The entries are ordered exactly as they should appear on the client. A
/// pushed-down source retains only enough state to produce this page; it does
/// not need to materialize the full result map in the WebSocket session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowedQuerySnapshot {
    pub entries: Vec<(Arc<str>, Arc<dyn AnyItem>)>,
    pub total_count: usize,
    pub window: Option<QueryWindow>,
}

/// A live bounded query source with an in-place window controller.
#[derive(Clone)]
pub struct WindowedQuerySource {
    snapshots: Cell<Arc<WindowedQuerySnapshot>, CellImmutable>,
    set_window: Arc<dyn Fn(Option<QueryWindow>) + Send + Sync>,
}

impl WindowedQuerySource {
    #[must_use]
    pub fn new(
        snapshots: Cell<Arc<WindowedQuerySnapshot>, CellImmutable>,
        set_window: impl Fn(Option<QueryWindow>) + Send + Sync + 'static,
    ) -> Self {
        Self {
            snapshots,
            set_window: Arc::new(set_window),
        }
    }

    #[must_use]
    pub const fn snapshots(&self) -> &Cell<Arc<WindowedQuerySnapshot>, CellImmutable> {
        &self.snapshots
    }

    pub fn set_window(&self, window: Option<QueryWindow>) {
        (self.set_window)(window);
    }
}
