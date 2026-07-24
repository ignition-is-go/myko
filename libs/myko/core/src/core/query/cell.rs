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

use hyphae::{CellImmutable, CellMap};

use super::super::item::AnyItem;

/// Type alias for a filtered CellMap of entities.
///
/// This is an immutable CellMap that automatically stays synchronized
/// with its source store. The subscription is managed internally by
/// hyphae's CellMap.
pub type FilteredCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;
