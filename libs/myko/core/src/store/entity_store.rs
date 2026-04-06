//! EntityStore - Type alias for reactive entity storage
//!
//! A CellMap storing entities by ID with automatic change propagation.

use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap, MapDiff};

use crate::core::item::AnyItem;

/// Type alias for entity diff cell.
pub type EntityDiffCell = Cell<MapDiff<Arc<str>, Arc<dyn AnyItem>>, CellImmutable>;

/// Type alias for entity entries cell.
pub type EntityEntriesCell = Cell<Vec<(Arc<str>, Arc<dyn AnyItem>)>, CellImmutable>;

/// A reactive entity store backed by hyphae's CellMap.
///
/// Thread-safe via DashMap internally. Changes propagate automatically
/// to all cell subscribers.
///
/// # Example
/// ```text
/// // EntityStore is `CellMap<Arc<str>, Arc<dyn AnyItem>>`.
/// //
/// // Typical flow:
/// // 1) Get the store from StoreRegistry (by entity type).
/// // 2) Upsert/remove entities as events are applied.
/// // 3) Build reactive views from `entries()`, `select(...)`, and `len()`.
/// //
/// // let store = registry.get_or_create("Target");
/// // store.set(id, item);      // on SET events
/// // store.remove(id);         // on DEL events
/// // let reactive = store.select(|item| ...).entries();
/// ```
pub type EntityStore = CellMap<Arc<str>, Arc<dyn AnyItem>>;
