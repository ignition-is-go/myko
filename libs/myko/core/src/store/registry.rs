//! `StoreRegistry` - Central registry for all entity stores
//!
//! Manages one `EntityStore` per entity type, providing a unified
//! interface for event processing.

use std::sync::Arc;

use dashmap::DashMap;
use hyphae::{Cell, CellMutable, MapDiff, Signal};

use super::{EntityStore, settled_writes::SettledWrites};

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
    // AHash on Arc<str> keys: ~1.6× faster than DashMap's default SipHash on
    // the lookup-by-entity-type hot path. Bench: dashmap_default 183 µs vs
    // dashmap_ahash 115 µs / 10k lookups.
    stores: DashMap<Arc<str>, Arc<EntityStore>, ahash::RandomState>,
    /// The write queue of each store whose writes are applied by a settle.
    settled_writes: DashMap<Arc<str>, Arc<SettledWrites>, ahash::RandomState>,
    /// Fires on every change to every store, including stores created
    /// later. Query, view and report families anchor to it.
    root: Cell<(), CellMutable>,
}

impl StoreRegistry {
    /// A registry holding a store for every registered entity type.
    ///
    /// Stores are graph nodes, and a node may be built only between tickets,
    /// so every store a ticket can reach is built here, up front.
    #[must_use]
    pub fn new() -> Self {
        let registry = Self {
            stores: DashMap::with_hasher(ahash::RandomState::new()),
            settled_writes: DashMap::with_hasher(ahash::RandomState::new()),
            root: Cell::new(()),
        };
        for item in inventory::iter::<crate::core::item::ItemRegistration> {
            let _ = registry.get_or_create(item.entity_type);
        }
        registry
    }

    /// Get or create an entity store for the given type.
    ///
    /// Every registered type's store already exists. Creating one for an
    /// unregistered type builds nodes, so do it between tickets.
    #[must_use]
    pub fn get_or_create(&self, entity_type: &str) -> Arc<EntityStore> {
        let key: Arc<str> = entity_type.into();

        self.stores
            .entry(key.clone())
            .or_insert_with(|| {
                let store = Arc::new(EntityStore::new().with_name(format!("store:{key}")));
                // The root owns the subscription, which puts it below the
                // store: it fires in the settle of the store's change.
                let root = self.root.downgrade();
                self.root.own(store.subscribe_diffs(move |diff| {
                    if !matches!(diff, MapDiff::Initial { .. })
                        && let Some(root) = root.upgrade()
                    {
                        root.notify(Signal::value(()));
                    }
                }));
                store
            })
            .clone()
    }

    /// The node every store's changes feed.
    #[must_use]
    pub fn root(&self) -> &Cell<(), CellMutable> {
        &self.root
    }

    /// The queue that applies a write to this type's store when the settle
    /// carrying the write fires.
    pub(crate) fn settled_writes(&self, entity_type: &str) -> Arc<SettledWrites> {
        if let Some(writes) = self.settled_writes.get(entity_type) {
            return writes.clone();
        }
        let store = self.get_or_create(entity_type);
        self.settled_writes
            .entry(entity_type.into())
            .or_insert_with(|| Arc::new(SettledWrites::new(&store)))
            .clone()
    }

    /// Get an entity store if it exists.
    #[must_use]
    pub fn get(&self, entity_type: &str) -> Option<Arc<EntityStore>> {
        self.stores.get(entity_type).map(|r| r.clone())
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
        let registered = registry.len();

        let store1 = registry.get_or_create("UnregisteredA");
        assert_eq!(registry.len(), registered + 1);

        let store2 = registry.get_or_create("UnregisteredA");
        assert!(Arc::ptr_eq(&store1, &store2));

        let store3 = registry.get_or_create("UnregisteredB");
        assert_eq!(registry.len(), registered + 2);
        assert!(!Arc::ptr_eq(&store1, &store3));
    }

    #[test]
    fn test_registry_get() {
        let registry = StoreRegistry::new();

        assert!(registry.get("Unregistered").is_none());

        drop(registry.get_or_create("Unregistered"));
        assert!(registry.get("Unregistered").is_some());
    }

    #[test]
    fn every_registered_type_has_a_store_before_any_ticket() {
        let registry = StoreRegistry::new();

        for item in inventory::iter::<crate::core::item::ItemRegistration> {
            assert!(registry.get(item.entity_type).is_some(), "{}", item.entity_type);
        }
    }
}
