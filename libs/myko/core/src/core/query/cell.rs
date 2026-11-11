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
use crate::wire::{QueryCursorWindow, QueryWindow};

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
    set_cursor_window: Arc<dyn Fn(QueryCursorWindow) + Send + Sync>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WindowSelection {
    Offset(Option<QueryWindow>),
    Cursor(QueryCursorWindow),
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
            set_cursor_window: Arc::new(|_| {}),
        }
    }

    #[must_use]
    pub fn new_with_cursor(
        snapshots: Cell<Arc<WindowedQuerySnapshot>, CellImmutable>,
        set_window: impl Fn(Option<QueryWindow>) + Send + Sync + 'static,
        set_cursor_window: impl Fn(QueryCursorWindow) + Send + Sync + 'static,
    ) -> Self {
        Self {
            snapshots,
            set_window: Arc::new(set_window),
            set_cursor_window: Arc::new(set_cursor_window),
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
    #[allow(clippy::too_many_lines)]
    pub fn from_map(map: &FilteredCellMap, initial_window: QueryWindow) -> Self {
        fn snapshot(
            entries: Vec<(Arc<str>, Arc<dyn AnyItem>)>,
            selection: &WindowSelection,
        ) -> WindowedQuerySnapshot {
            let total_count = entries.len();
            let mut entries = entries;
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let window = match selection {
                WindowSelection::Offset(window) => {
                    if let Some(window) = window {
                        let start = window.offset.min(entries.len());
                        let end = start.saturating_add(window.limit).min(entries.len());
                        entries = entries.get(start..end).unwrap_or_default().to_vec();
                    }
                    window.clone()
                }
                WindowSelection::Cursor(cursor) => {
                    let (start, end) = cursor.after.as_ref().map_or_else(
                        || {
                            cursor.before.as_ref().map_or_else(
                                || (0, cursor.limit.min(entries.len())),
                                |before| {
                                    let end = entries.partition_point(|(id, _)| id < before);
                                    (end.saturating_sub(cursor.limit), end)
                                },
                            )
                        },
                        |after| {
                            let start = entries.partition_point(|(id, _)| id <= after);
                            (start, start.saturating_add(cursor.limit).min(entries.len()))
                        },
                    );
                    entries = entries.get(start..end).unwrap_or_default().to_vec();
                    None
                }
            };
            WindowedQuerySnapshot {
                entries,
                total_count,
                window,
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

        let selection = Arc::new(std::sync::Mutex::new(WindowSelection::Offset(Some(
            initial_window.clone(),
        ))));
        let snapshots = Cell::new(Arc::new(WindowedQuerySnapshot {
            entries: Vec::new(),
            total_count: 0,
            window: Some(initial_window),
        }));
        let dispatch = Arc::new(parking_lot::ReentrantMutex::new(()));
        let snapshots_weak = snapshots.downgrade();
        let map_weak = map.downgrade();
        let selection_for_diffs = selection.clone();
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
                let selection = selection_for_diffs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                snapshot(entries, &selection)
            };
            snapshots.set(Arc::new(next));
        });
        snapshots.own(guard);

        let snapshots_weak = snapshots.downgrade();
        let map_weak = map.downgrade();
        let dispatch_for_window = dispatch.clone();
        let selection_for_window = selection.clone();
        let snapshots_for_cursor = snapshots.downgrade();
        let map_for_cursor = map.downgrade();
        let dispatch_for_cursor = dispatch;
        let set_window = move |next: Option<QueryWindow>| {
            let _dispatch_guard = dispatch_for_window.lock();
            let next_selection = {
                let mut current = selection_for_window
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let next = WindowSelection::Offset(next);
                if *current == next {
                    return;
                }
                *current = next;
                current.clone()
            };
            let Some(map) = map_weak.upgrade() else {
                return;
            };
            let next_snapshot = snapshot(map.snapshot(), &next_selection);
            if let Some(snapshots) = snapshots_weak.upgrade() {
                snapshots.set(Arc::new(next_snapshot));
            }
        };

        let set_cursor_window = move |next: QueryCursorWindow| {
            if next.validate().is_err() {
                return;
            }
            let _dispatch_guard = dispatch_for_cursor.lock();
            let next_selection = {
                let mut current = selection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let next = WindowSelection::Cursor(next);
                if *current == next {
                    return;
                }
                *current = next;
                current.clone()
            };
            let Some(map) = map_for_cursor.upgrade() else {
                return;
            };
            let next_snapshot = snapshot(map.snapshot(), &next_selection);
            if let Some(snapshots) = snapshots_for_cursor.upgrade() {
                snapshots.set(Arc::new(next_snapshot));
            }
        };

        Self::new_with_cursor(snapshots.lock(), set_window, set_cursor_window)
    }

    pub fn set_window(&self, window: Option<QueryWindow>) {
        (self.set_window)(window);
    }

    pub fn set_cursor_window(&self, window: QueryCursorWindow) {
        (self.set_cursor_window)(window);
    }
}
