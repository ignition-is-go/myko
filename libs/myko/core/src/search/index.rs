//! Process-wide search index. Thin wrapper over `typed::SearchRegistry` —
//! all the matching logic lives in `typed/`. This file exists to preserve
//! the call-site shape used in `server/context.rs`, `server/src/lib.rs`,
//! and `core/report/handler.rs` so the migration off tantivy didn't ripple
//! through ~18 call sites.
//!
//! Per-entity dispatch (insert/remove/search by `entity_type` string) is
//! handled by `SearchRegistry`. The `entity_type` parameter on
//! `remove_entity` is new — tantivy could delete by id alone, but the typed
//! registry needs the type to find the right `SearchIndex<T>` shim.

use std::sync::Arc;
use std::time::Instant;

use super::{build_typed_registry, search_stats, typed};
use crate::core::item::AnyItem;

/// Process-wide search dispatcher. Constructs one per-type `SearchIndex<T>`
/// at startup by walking the `SearchableRegistration` inventory.
pub struct SearchIndex {
    registry: typed::SearchRegistry,
}

impl SearchIndex {
    pub fn new() -> Self {
        let registry = build_typed_registry();
        log::info!(
            "SearchIndex: initialized with {} typed entity indexes",
            registry.len()
        );
        Self { registry }
    }

    /// Index a dynamic entity item.
    ///
    /// No-op when the entity type has no registered searchable index.
    pub fn index_item(&self, item: &Arc<dyn AnyItem>) {
        // NOTE(ts): Skip indexing when MYKO_SEARCH_INDEX_DISABLED is set
        // (originally added for memory profiling against the tantivy backend;
        // kept here so the same toggle still works for ablation studies).
        static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        if *DISABLED.get_or_init(|| std::env::var("MYKO_SEARCH_INDEX_DISABLED").is_ok()) {
            return;
        }
        self.registry.insert(item.as_ref());
    }

    /// Remove an entity from the search index. The `entity_type` parameter
    /// is required to dispatch into the right per-type index.
    pub fn remove_entity(&self, entity_type: &str, entity_id: &str) {
        self.registry.remove(entity_type, entity_id);
    }

    /// No-op preserved for call-site compatibility. The typed registry's
    /// writes are visible to readers immediately — there is no commit phase
    /// like the tantivy backend had.
    pub fn commit(&self) {}

    /// Search for entities of `entity_type` matching `query`. Returns ids
    /// (still as `Arc<str>` to match the `EntitySearchResult` wire format).
    /// Honors the full three-tier ranking; legacy callers pass `limit` only.
    pub fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        let opts = typed::SearchOptions {
            limit,
            ..typed::SearchOptions::default()
        };
        let started = Instant::now();
        let hits: Vec<Arc<str>> = self
            .registry
            .search(entity_type, query, opts)
            .into_iter()
            .map(|h| h.id)
            .collect();
        search_stats::record_search(entity_type, hits.len(), started.elapsed());
        hits
    }

    /// True if a `SearchIndex<T>` is registered for this entity type.
    pub fn is_searchable(&self, entity_type: &str) -> bool {
        self.registry.entity_types().any(|t| t == entity_type)
    }

    /// Build the initial index from all entities currently in the store
    /// registry. Call after durable backend catch-up.
    pub fn build_from_registry(&self, registry: &crate::store::StoreRegistry) {
        use hyphae::Gettable;

        let mut count = 0;
        // Snapshot the registered entity types so we don't hold any registry
        // borrows while iterating each store.
        let entity_types: Vec<&'static str> = self.registry.entity_types().collect();

        for entity_type in entity_types {
            let Some(store) = registry.get(entity_type) else {
                continue;
            };
            let entries = store.entries().get();
            for (_, item) in entries.iter() {
                self.registry.insert(item.as_ref());
                count += 1;
            }
        }
        log::info!("SearchIndex: built initial index with {} entities", count);
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_search_index_is_empty_for_unknown_types() {
        let index = SearchIndex::new();
        let hits = index.search("UnknownType", "anything", 10);
        assert!(hits.is_empty());
    }
}
