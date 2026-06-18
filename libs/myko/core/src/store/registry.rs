//! StoreRegistry - Central registry for all entity stores
//!
//! Manages one EntityStore per entity type, providing a unified
//! interface for event processing.

use std::sync::Arc;

use dashmap::DashMap;

use super::{EntityStore, LwwStamp};
use crate::core::item::AnyItem;

/// Per-type LWW stamp index: `id -> stamp` (a `deleted` stamp is a tombstone).
type StampIndex = DashMap<Arc<str>, LwwStamp>;

/// Central registry holding all entity stores.
///
/// Thread-safe via DashMap. Automatically creates stores on first access.
///
/// # Example
/// ```text
/// // One registry per server/runtime:
/// let registry = StoreRegistry::new();
///
/// // Resolve stores by entity type:
/// let targets = registry.get_or_create("Target");
/// let scenes = registry.get_or_create("Scene");
///
/// // Introspection helpers:
/// assert!(!registry.is_empty());
/// let all_types = registry.entity_types();
/// ```
pub struct StoreRegistry {
    // AHash on Arc<str> keys: ~1.6× faster than DashMap's default SipHash on
    // the lookup-by-entity-type hot path. Bench: dashmap_default 183 µs vs
    // dashmap_ahash 115 µs / 10k lookups.
    stores: DashMap<Arc<str>, Arc<EntityStore>, ahash::RandomState>,
    /// Per-type LWW stamps + tombstones, parallel to `stores`. The convergence
    /// gate for every write (see `lww_*`).
    stamps: DashMap<Arc<str>, Arc<StampIndex>, ahash::RandomState>,
}

impl StoreRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            stores: DashMap::with_hasher(ahash::RandomState::new()),
            stamps: DashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    fn stamps_for(&self, entity_type: &str) -> Arc<StampIndex> {
        self.stamps
            .entry(entity_type.into())
            .or_insert_with(|| Arc::new(StampIndex::new()))
            .clone()
    }

    /// Apply a single SET under last-writer-wins.
    ///
    /// Mutates the reactive store and records the stamp only if `stamp`
    /// **strictly wins** over the stored stamp/tombstone for `id`. Returns
    /// whether the write was applied (so callers can skip cascade/produce for a
    /// suppressed, stale write).
    pub fn lww_set(&self, entity_type: &str, id: Arc<str>, item: Arc<dyn AnyItem>, stamp: LwwStamp) -> bool {
        let stamps = self.stamps_for(entity_type);
        let win = stamps.get(&id).is_none_or(|cur| stamp.wins_over(cur.value()));
        if win {
            stamps.insert(id.clone(), stamp);
            self.get_or_create(entity_type).insert(id, item);
        }
        win
    }

    /// Apply a single DEL under last-writer-wins, leaving a tombstone.
    pub fn lww_del(&self, entity_type: &str, id: Arc<str>, stamp: LwwStamp) -> bool {
        let stamps = self.stamps_for(entity_type);
        let win = stamps.get(&id).is_none_or(|cur| stamp.wins_over(cur.value()));
        if win {
            stamps.insert(id.clone(), stamp);
            self.get_or_create(entity_type).remove(&id);
        }
        win
    }

    /// Batch SET under LWW. Applies the winning entries to the store with a
    /// single diff and returns the winning items (those actually applied, in
    /// input order). `entries` must all be the same `entity_type`.
    pub fn lww_set_many(
        &self,
        entity_type: &str,
        entries: Vec<(Arc<dyn AnyItem>, LwwStamp)>,
    ) -> Vec<Arc<dyn AnyItem>> {
        let stamps = self.stamps_for(entity_type);
        let mut winners: Vec<Arc<dyn AnyItem>> = Vec::with_capacity(entries.len());
        let mut store_entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> = Vec::with_capacity(entries.len());
        for (item, stamp) in entries {
            let id = item.id();
            let win = stamps.get(&id).is_none_or(|cur| stamp.wins_over(cur.value()));
            if win {
                stamps.insert(id.clone(), stamp);
                store_entries.push((id, item.clone()));
                winners.push(item);
            }
        }
        if !store_entries.is_empty() {
            self.get_or_create(entity_type).insert_many(store_entries);
        }
        winners
    }

    /// Batch DEL under LWW, leaving tombstones. Returns the winning items
    /// removed (so callers can cascade on exactly the applied deletes).
    pub fn lww_del_many(
        &self,
        entity_type: &str,
        entries: Vec<(Arc<dyn AnyItem>, LwwStamp)>,
    ) -> Vec<Arc<dyn AnyItem>> {
        let stamps = self.stamps_for(entity_type);
        let mut winners: Vec<Arc<dyn AnyItem>> = Vec::with_capacity(entries.len());
        let mut ids: Vec<Arc<str>> = Vec::with_capacity(entries.len());
        for (item, stamp) in entries {
            let id = item.id();
            let win = stamps.get(&id).is_none_or(|cur| stamp.wins_over(cur.value()));
            if win {
                stamps.insert(id.clone(), stamp);
                ids.push(id);
                winners.push(item);
            }
        }
        if !ids.is_empty() {
            self.get_or_create(entity_type).remove_many(ids);
        }
        winners
    }

    /// Get or create an entity store for the given type.
    ///
    /// Creates a new store if one doesn't exist for this type.
    pub fn get_or_create(&self, entity_type: &str) -> Arc<EntityStore> {
        let key: Arc<str> = entity_type.into();

        self.stores
            .entry(key.clone())
            .or_insert_with(|| Arc::new(EntityStore::new().with_name(format!("store:{}", key))))
            .clone()
    }

    /// Get an entity store if it exists.
    pub fn get(&self, entity_type: &str) -> Option<Arc<EntityStore>> {
        self.stores.get(entity_type).map(|r| r.clone())
    }

    /// List all registered entity types.
    pub fn entity_types(&self) -> Vec<Arc<str>> {
        self.stores.iter().map(|r| r.key().clone()).collect()
    }

    /// Get the number of registered entity types.
    pub fn len(&self) -> usize {
        self.stores.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_get_or_create() {
        let registry = StoreRegistry::new();

        // First access creates store
        let store1 = registry.get_or_create("Target");
        assert_eq!(registry.len(), 1);

        // Second access returns same store
        let store2 = registry.get_or_create("Target");
        assert!(Arc::ptr_eq(&store1, &store2));

        // Different type creates new store
        let store3 = registry.get_or_create("Scene");
        assert_eq!(registry.len(), 2);
        assert!(!Arc::ptr_eq(&store1, &store3));
    }

    #[test]
    fn test_registry_get() {
        let registry = StoreRegistry::new();

        // Non-existent returns None
        assert!(registry.get("Target").is_none());

        // After creation, returns Some
        registry.get_or_create("Target");
        assert!(registry.get("Target").is_some());
    }

    #[test]
    fn test_registry_entity_types() {
        let registry = StoreRegistry::new();

        registry.get_or_create("Target");
        registry.get_or_create("Scene");
        registry.get_or_create("Binding");

        let types = registry.entity_types();
        assert_eq!(types.len(), 3);
    }
}
