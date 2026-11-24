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
    /// Set on the registry of one partition. `None` on a root registry.
    partition: Option<PartitionStores>,
    /// Set on a root registry once its server declares partitions: whether an
    /// entity type is partitioned, and so has no store here.
    partitioned: std::sync::OnceLock<IsPartitioned>,
    // AHash on Arc<str> keys: ~1.6× faster than DashMap's default SipHash on
    // the lookup-by-entity-type hot path. Bench: dashmap_default 183 µs vs
    // dashmap_ahash 115 µs / 10k lookups.
    stores: DashMap<Arc<str>, Arc<EntityStore>, ahash::RandomState>,
    /// Indexes derived from these stores. They are costly to rebuild, so
    /// they live as long as the stores do.
    retained: parking_lot::Mutex<Vec<Arc<dyn std::any::Any + Send + Sync>>>,
}

/// What makes a registry the registry of one partition: the stores of
/// partitioned entity types live here, in the partition's scheduling domain,
/// and every other type resolves to the root registry's store.
struct PartitionStores {
    root: Arc<StoreRegistry>,
    domain: hyphae::scheduler::Domain,
    is_partitioned: IsPartitioned,
}

pub type IsPartitioned = Arc<dyn Fn(&str) -> bool + Send + Sync>;

thread_local! {
    /// The first partitioned entity type a root registry was asked for on
    /// this thread since the innermost `watch_root_reads` began.
    static ROOT_READ: std::cell::RefCell<Option<Arc<str>>> = const { std::cell::RefCell::new(None) };
}

pub fn record_root_read(entity_type: &str) {
    let first = ROOT_READ.with(|read| {
        let mut read = read.borrow_mut();
        let first = read.is_none();
        if first {
            *read = Some(Arc::from(entity_type));
        }
        first
    });
    if first {
        tracing::error!(
            entity_type,
            "a partitioned entity type was read from the root context; it has rows only in its \
             partitions' contexts"
        );
    }
}

/// Run `construct` and return the first partitioned entity type it asked a root
/// registry for, on this thread. Nested watches report to their caller too.
pub fn watch_root_reads<R>(construct: impl FnOnce() -> R) -> (R, Option<Arc<str>>) {
    let outer = ROOT_READ.with(|read| read.borrow_mut().take());
    let built = construct();
    let inner = ROOT_READ.with(|read| {
        let mut read = read.borrow_mut();
        let inner = read.take();
        *read = outer.or_else(|| inner.clone());
        inner
    });
    (built, inner)
}

impl StoreRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            transactions: super::transactions::TransactionCoordinator::default(),
            partition: None,
            partitioned: std::sync::OnceLock::new(),
            stores: DashMap::with_hasher(ahash::RandomState::new()),
            retained: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// The registry of one partition of `root`.
    ///
    /// A store of an entity type for which `is_partitioned` holds is created
    /// here, inside `domain`. Every other type resolves to `root`'s store, so
    /// a graph built over this registry reads shared data from the root
    /// domain and partition data from its own.
    pub(crate) fn for_partition(
        root: Arc<Self>,
        domain: hyphae::scheduler::Domain,
        is_partitioned: IsPartitioned,
    ) -> Self {
        Self {
            partition: Some(PartitionStores {
                root,
                domain,
                is_partitioned,
            }),
            ..Self::new()
        }
    }

    /// Tell this root registry which entity types are partitioned.
    pub(crate) fn declare_partitioned(&self, is_partitioned: IsPartitioned) {
        drop(self.partitioned.set(is_partitioned));
    }

    /// The root registry when `entity_type` is shared data read from a
    /// partition's registry.
    fn shared(&self, entity_type: &str) -> Option<&Arc<Self>> {
        self.partition
            .as_ref()
            .filter(|partition| !(partition.is_partitioned)(entity_type))
            .map(|partition| &partition.root)
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
        if let Some(root) = self.shared(entity_type) {
            return root.get_or_create(entity_type);
        }
        if self
            .partitioned
            .get()
            .is_some_and(|is_partitioned| is_partitioned(entity_type))
        {
            // NOTE(ts): the signature cannot refuse. The read is recorded,
            // and a build watched by `watch_root_reads` is refused; the
            // store returned is one no partition writes.
            record_root_read(entity_type);
        }
        if let Some(store) = self.stores.get(entity_type) {
            return store.clone();
        }
        let key: Arc<str> = entity_type.into();
        let create = || Arc::new(EntityStore::new().with_name(format!("store:{key}")));

        self.stores
            .entry(key.clone())
            .or_insert_with(|| {
                // A store lives in the domain it is born in, whichever thread
                // asks for it first.
                self.partition
                    .as_ref()
                    .map_or_else(create, |partition| partition.domain.enter(create))
            })
            .clone()
    }

    /// Get an entity store if it exists.
    #[must_use]
    pub fn get(&self, entity_type: &str) -> Option<Arc<EntityStore>> {
        if let Some(root) = self.shared(entity_type) {
            return root.get(entity_type);
        }
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
