//! EntityStore - Type alias for reactive entity storage
//!
//! A CellMap storing entities by ID with automatic change propagation.

use std::sync::Arc;

use hypha::{Cell, CellImmutable, CellMap, MapDiff};

use crate::core::item::AnyItem;

/// Type alias for entity diff cell.
pub type EntityDiffCell = Cell<Option<MapDiff<Arc<str>, Arc<dyn AnyItem>>>, CellImmutable>;

/// Type alias for entity entries cell.
pub type EntityEntriesCell = Cell<Vec<(Arc<str>, Arc<dyn AnyItem>)>, CellImmutable>;

/// A reactive entity store backed by hypha's CellMap.
///
/// Thread-safe via DashMap internally. Changes propagate automatically
/// to all cell subscribers.
///
/// # Example
/// ```ignore
/// let store = EntityStore::new();
///
/// // Insert an entity
/// store.insert(id, item);
///
/// // Get snapshot
/// let items = store.entries().get();
///
/// // Subscribe to changes
/// store.subscribe_diffs(|diff| {
///     // Handle insert/update/remove
/// });
/// ```
pub type EntityStore = CellMap<Arc<str>, Arc<dyn AnyItem>>;
