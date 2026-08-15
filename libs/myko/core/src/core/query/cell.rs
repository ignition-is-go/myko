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

use hyphae::{Cell, CellImmutable, CellMap, Gettable, MapDiff, Mutable};

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

    /// Push an ordered ID window into an existing reactive map.
    ///
    /// Insertions and removals can shift the page and therefore rebuild its
    /// bounded snapshot. Value updates rebuild only when their key is visible,
    /// suppressing off-page work while the full source map remains reactive.
    #[doc(hidden)]
    #[must_use]
    pub fn from_map(map: &FilteredCellMap, initial_window: QueryWindow) -> Self {
        fn snapshot(
            entries: Vec<(Arc<str>, Arc<dyn AnyItem>)>,
            window: Option<&QueryWindow>,
        ) -> WindowedQuerySnapshot {
            let total_count = entries.len();
            let mut entries = entries;
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            if let Some(window) = window {
                let start = window.offset.min(entries.len());
                let end = start.saturating_add(window.limit).min(entries.len());
                entries = entries.get(start..end).unwrap_or_default().to_vec();
            }
            WindowedQuerySnapshot {
                entries,
                total_count,
                window: window.cloned(),
            }
        }

        fn affects_page(
            diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
            visible: &[(Arc<str>, Arc<dyn AnyItem>)],
        ) -> bool {
            match diff {
                MapDiff::Initial { .. } | MapDiff::Insert { .. } | MapDiff::Remove { .. } => true,
                MapDiff::Update { key, .. } => visible.iter().any(|(id, _)| id == key),
                MapDiff::Batch { changes } => {
                    changes.iter().any(|change| affects_page(change, visible))
                }
            }
        }

        let window = Arc::new(std::sync::Mutex::new(Some(initial_window.clone())));
        let snapshots = Cell::new(Arc::new(WindowedQuerySnapshot {
            entries: Vec::new(),
            total_count: 0,
            window: Some(initial_window),
        }));
        let dispatch = Arc::new(parking_lot::ReentrantMutex::new(()));
        let snapshots_weak = snapshots.downgrade();
        let map_weak = map.downgrade();
        let window_for_diffs = window.clone();
        let dispatch_for_diffs = dispatch.clone();
        let guard = map.subscribe_diffs(move |diff| {
            let _dispatch_guard = dispatch_for_diffs.lock();
            let Some(snapshots) = snapshots_weak.upgrade() else {
                return;
            };
            let current = snapshots.get();
            if !affects_page(diff, &current.entries) {
                return;
            }
            let entries = match diff {
                MapDiff::Initial { entries } => entries.clone(),
                MapDiff::Insert { .. }
                | MapDiff::Update { .. }
                | MapDiff::Remove { .. }
                | MapDiff::Batch { .. } => {
                    let Some(map) = map_weak.upgrade() else {
                        return;
                    };
                    map.snapshot()
                }
            };
            let next = {
                let window = window_for_diffs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                snapshot(entries, window.as_ref())
            };
            snapshots.set(Arc::new(next));
        });
        snapshots.own(guard);

        let snapshots_weak = snapshots.downgrade();
        let map_weak = map.downgrade();
        let dispatch_for_window = dispatch;
        let set_window = move |next: Option<QueryWindow>| {
            let _dispatch_guard = dispatch_for_window.lock();
            let next_snapshot = {
                let mut current = window
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *current == next {
                    return;
                }
                *current = next;
                let Some(map) = map_weak.upgrade() else {
                    return;
                };
                snapshot(map.snapshot(), current.as_ref())
            };
            if let Some(snapshots) = snapshots_weak.upgrade() {
                snapshots.set(Arc::new(next_snapshot));
            }
        };

        Self::new(snapshots.lock(), set_window)
    }

    pub fn set_window(&self, window: Option<QueryWindow>) {
        (self.set_window)(window);
    }
}
