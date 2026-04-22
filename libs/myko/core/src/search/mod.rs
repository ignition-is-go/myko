//! Full-text search infrastructure for Myko entities.
//!
//! This module provides full-text search capabilities using tantivy with RAM directory
//! for in-memory indexing. Entities can mark fields as `#[searchable]` and the search
//! index is updated automatically when entities are stored or removed.
//!
//! ## Usage
//!
//! ```text
//! use myko::prelude::*;
//! use std::sync::Arc;
//!
//! #[myko_item]
//! pub struct Target {
//!     #[searchable]
//!     pub name: String,
//!     #[searchable]
//!     pub category: String,
//!     pub service_id: Arc<str>,  // not searchable
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//! CellServerCtx (set/del) --> SearchIndex --> tantivy RAM index
//!                                  |
//!                            SearchableRegistration (inventory)
//!                                  |
//!                         EntitySearch report --> returns matching IDs
//! ```

mod entity_search;
#[cfg(feature = "search")]
mod index;

pub use entity_search::{EntitySearch, EntitySearchResult};
#[cfg(feature = "search")]
pub use index::SearchIndex;

// When the `search` feature is off, expose a no-op `SearchIndex` with the
// same public surface. Keeps downstream code (server context, myko-server)
// compiling and behaving predictably (searches return empty, indexing is a
// no-op) without threading feature gates through every call site.
#[cfg(not(feature = "search"))]
mod stub_index {
    use std::sync::Arc;

    use crate::core::item::AnyItem;

    pub struct SearchIndex;

    impl SearchIndex {
        pub fn new() -> Self {
            Self
        }
        pub fn index_item(&self, _item: &Arc<dyn AnyItem>) {}
        pub fn remove_entity(&self, _entity_id: &str) {}
        pub fn commit(&self) {}
        pub fn search(&self, _entity_type: &str, _query: &str, _limit: usize) -> Vec<Arc<str>> {
            Vec::new()
        }
        pub fn is_searchable(&self, _entity_type: &str) -> bool {
            false
        }
        pub fn build_from_registry(&self, _registry: &crate::store::StoreRegistry) {}
    }

    impl Default for SearchIndex {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(not(feature = "search"))]
pub use stub_index::SearchIndex;

use serde_json::Value;

/// Registration for searchable entity types.
///
/// This is collected via `inventory` and used by SearchIndex to know which fields to index.
pub struct SearchableRegistration {
    /// Entity type name (e.g., "Target")
    pub entity_type: &'static str,
    /// JSON field names that are searchable (camelCase as they appear in serialized form)
    pub fields: &'static [&'static str],
}

inventory::collect!(SearchableRegistration);

/// Iterate over all registered searchable entity types.
pub fn iter_searchable() -> impl Iterator<Item = &'static SearchableRegistration> {
    inventory::iter::<SearchableRegistration>()
}

/// Extract searchable text from an item value given the searchable fields.
///
/// Concatenates all searchable field values into a single string for indexing.
pub fn extract_searchable_text(item: &Value, fields: &[&str]) -> String {
    let Some(obj) = item.as_object() else {
        return String::new();
    };

    fields
        .iter()
        .filter_map(|field| {
            obj.get(*field).and_then(|v| match v {
                Value::String(s) => Some(s.as_str()),
                Value::Number(n) => Some(n.to_string()).map(|s| s.leak() as &str),
                Value::Bool(b) => Some(if *b { "true" } else { "false" }),
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_extract_searchable_text() {
        let item = json!({
            "name": "Test Target",
            "category": "Audio",
            "serviceId": "svc-123"
        });

        let text = extract_searchable_text(&item, &["name", "category"]);
        assert_eq!(text, "Test Target Audio");
    }

    #[test]
    fn test_extract_searchable_text_missing_fields() {
        let item = json!({
            "name": "Test Target"
        });

        let text = extract_searchable_text(&item, &["name", "category"]);
        assert_eq!(text, "Test Target");
    }
}
