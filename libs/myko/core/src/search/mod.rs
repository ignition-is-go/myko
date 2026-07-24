//! Full-text search infrastructure for Myko entities.
//!
//! Entities mark fields with `#[searchable]` and the index updates
//! automatically as the server context applies SET/DEL events.
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
//! MykoServerCtx (set/del) --> SearchIndex (compat wrapper)
//!                                  |
//!                            typed::SearchRegistry (per-type)
//!                                  |
//!                          typed::SearchIndex<T>  (exact + nucleo + Levenshtein)
//!                                  |
//!                         EntitySearch report --> returns matching IDs
//! ```
//!
//! The legacy tantivy backend was replaced in favor of `typed::SearchIndex<T>`
//! — see `SPEC.md` for design and the typed module for implementation.

mod entity_search;
#[cfg(feature = "search")]
mod index;
#[cfg(feature = "search")]
pub mod search_stats;
/// Per-type, monomorphized search indexes. Always compiled (light deps) —
/// the `search` feature only gates the `SearchIndex` *wrapper* that exposes
/// these to downstream call sites.
pub mod typed;

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
        pub fn remove_entity(&self, _entity_type: &str, _entity_id: &str) {}
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

/// Registration for searchable entity types. Collected via `inventory` and
/// used by `build_typed_registry` to construct the per-type
/// `SearchIndex<T>` shims at startup.
pub struct SearchableRegistration {
    /// Entity type name (e.g., "Target").
    pub entity_type: &'static str,
    /// Searchable JSON field names (camelCase). Carried for back-compat
    /// with consumers that introspect the inventory; the typed search path
    /// no longer uses this — the macro-emitted `Searchable` impl handles
    /// extraction directly off the typed entity.
    pub fields: &'static [&'static str],
    /// Constructs the per-type `SearchIndex<T>` in the typed `SearchRegistry`.
    /// `Some` for entities whose macro emits the typed `Searchable` impl
    /// (i.e. anything with at least one `#[searchable]` field).
    pub register_typed: Option<fn(&mut typed::SearchRegistry)>,
}

inventory::collect!(SearchableRegistration);

/// Iterate over all registered searchable entity types.
pub fn iter_searchable() -> impl Iterator<Item = &'static SearchableRegistration> {
    inventory::iter::<SearchableRegistration>()
}

/// Build a `typed::SearchRegistry` populated with one `SearchIndex<T>` per
/// entity that registered a `register_typed` closure.
pub fn build_typed_registry() -> typed::SearchRegistry {
    let mut registry = typed::SearchRegistry::new();
    for reg in iter_searchable() {
        if let Some(register) = reg.register_typed {
            register(&mut registry);
        }
    }
    registry
}

// `extract_searchable_text` and its JSON-by-string-key path are gone. The
// typed registry consumes `&dyn AnyItem` directly via the macro-generated
// `Searchable` impl — no serde round-trip on the write path.

/// Default `limit` for macro-generated typed `Search<T>` reports. Referenced
/// by `#[serde(default = "...")]` so unspecified limits use this constant.
pub fn default_search_limit() -> usize {
    100
}
