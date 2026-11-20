//! `StoreRegistry` - Central registry for all entity stores
//!
//! Manages one `EntityStore` per entity type, providing a unified
//! interface for event processing.

use std::sync::Arc;

use dashmap::DashMap;

use super::EntityStore;

/// Central registry holding all entity stores.
///
/// Thread-safe via `DashMap`. Automatically creates stores on first access.
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
    pub(crate) transactions: super::transactions::TransactionCoordinator,
    // AHash on Arc<str> keys: ~1.6× faster than DashMap's default SipHash on
    // the lookup-by-entity-type hot path. Bench: dashmap_default 183 µs vs
    // dashmap_ahash 115 µs / 10k lookups.
    stores: DashMap<Arc<str>, Arc<EntityStore>, ahash::RandomState>,
    /// Indexes derived from these stores. They are costly to rebuild, so
    /// they live as long as the stores do.
    retained: parking_lot::Mutex<Vec<Arc<dyn std::any::Any + Send + Sync>>>,
}

impl StoreRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            transactions: super::transactions::TransactionCoordinator::default(),
            stores: DashMap::with_hasher(ahash::RandomState::new()),
            retained: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Keep a derived index alive for the lifetime of this registry.
    pub(crate) fn retain(&self, index: Arc<dyn std::any::Any + Send + Sync>) {
        self.retained.lock().push(index);
    }

    /// Coordinate a persistence consumer's store update with checked reversals.
    #[doc(hidden)]
    pub fn with_mutation<T>(&self, action: impl FnOnce() -> T) -> T {
        self.transactions.coordinate(false, action)
    }

    /// Get or create an entity store for the given type.
    ///
    /// Creates a new store if one doesn't exist for this type.
    #[must_use]
    pub fn get_or_create(&self, entity_type: &str) -> Arc<EntityStore> {
        if let Some(store) = self.stores.get(entity_type) {
            return store.clone();
        }
        let key: Arc<str> = entity_type.into();

        self.stores
            .entry(key.clone())
            .or_insert_with(|| Arc::new(EntityStore::new().with_name(format!("store:{key}"))))
            .clone()
    }

    /// Get an entity store if it exists.
    #[must_use]
    pub fn get(&self, entity_type: &str) -> Option<Arc<EntityStore>> {
        self.stores.get(entity_type).map(|r| r.clone())
    }

    /// Fork stores touched by a proposal while sharing untouched, read-only stores.
    #[must_use]
    pub fn overlay(
        &self,
        upserts: &[Arc<dyn crate::item::AnyItem>],
        deletes: &[Arc<dyn crate::item::AnyItem>],
    ) -> Arc<Self> {
        let view = Self::new();
        for store in &self.stores {
            view.stores
                .insert(store.key().clone(), store.value().clone());
        }
        let types: std::collections::HashSet<_> = upserts
            .iter()
            .chain(deletes)
            .map(|item| item.entity_type())
            .collect();
        for kind in types {
            let copy = Arc::new(crate::store::EntityStore::new());
            if let Some(original) = self.get(kind) {
                copy.insert_many(original.snapshot());
            }
            for item in deletes.iter().filter(|item| item.entity_type() == kind) {
                copy.remove(&item.id());
            }
            for item in upserts.iter().filter(|item| item.entity_type() == kind) {
                copy.insert(item.id(), item.clone());
            }
            view.stores.insert(Arc::from(kind), copy);
        }
        Arc::new(view)
    }

    /// List all registered entity types.
    #[must_use]
    pub fn entity_types(&self) -> Vec<Arc<str>> {
        self.stores.iter().map(|r| r.key().clone()).collect()
    }

    /// Get the number of registered entity types.
    #[must_use]
    pub fn len(&self) -> usize {
        self.stores.len()
    }

    /// Check if the registry is empty.
    #[must_use]
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
        drop(registry.get_or_create("Target"));
        assert!(registry.get("Target").is_some());
    }

    #[test]
    fn test_registry_entity_types() {
        let registry = StoreRegistry::new();

        drop(registry.get_or_create("Target"));
        drop(registry.get_or_create("Scene"));
        drop(registry.get_or_create("Binding"));

        let types = registry.entity_types();
        assert_eq!(types.len(), 3);
    }
}
