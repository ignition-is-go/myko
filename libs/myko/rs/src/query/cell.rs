//! Cell-based reactive query types
//!
//! Provides type aliases for cell-based query results.
//!
//! For querying, use `store.select(predicate)` directly:
//!
//! ```ignore
//! let store = registry.get_or_create("Target");
//!
//! // Query all entities
//! let all = store.select(|_| true).entries().map(|entries| {
//!     entries.iter().map(|(_, item)| item.clone()).collect()
//! });
//!
//! // Query with predicate
//! let filtered = store.select(|item| {
//!     // your filter logic
//!     true
//! }).entries().map(|entries| {
//!     entries.iter().map(|(_, item)| item.clone()).collect()
//! });
//! ```

use std::sync::Arc;

use hypha::{CellImmutable, CellMap};

use crate::parsers::item::AnyItem;

/// Type alias for a filtered CellMap of entities.
///
/// This is an immutable CellMap that automatically stays synchronized
/// with its source store. The subscription is managed internally by
/// hypha's CellMap.
pub type FilteredCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;
