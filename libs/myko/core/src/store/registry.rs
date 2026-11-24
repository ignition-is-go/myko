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
    /// Indexes derived from these stores, by what they index. They are
    /// costly to rebuild, so they live as long as the stores do, and they
    /// index this registry's stores only.
    indexes: DashMap<String, Arc<dyn std::any::Any + Send + Sync>, ahash::RandomState>,
    /// The relationship index over the stores this registry owns.
    relations: std::sync::OnceLock<Arc<dyn std::any::Any + Send + Sync>>,
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
    /// this thread since the innermost `watch_root_reads` began, and how many
    /// watches are open on this thread.
    static ROOT_READ: std::cell::RefCell<(usize, Option<Arc<str>>)> =
        const { std::cell::RefCell::new((0, None)) };
}

/// Record that a root registry was asked for the partitioned `entity_type`.
///
/// # Panics
/// Outside every `watch_root_reads`: the read would see no rows, as the
/// type's rows live only in its partitions, and no caller is left to refuse
/// it to. A watched read is refused to the watch's caller instead.
pub fn record_root_read(entity_type: &str) {
    let watched = ROOT_READ.with(|read| {
        let mut read = read.borrow_mut();
        if read.0 > 0 && read.1.is_none() {
            read.1 = Some(Arc::from(entity_type));
        }
        read.0 > 0
    });
    #[allow(
        clippy::panic,
        reason = "a root read of partition data is a program error"
    )]
    {
        assert!(
            watched,
            "partitioned entity type {entity_type} was read from the root context, where it has \
             no rows: read it on its partition's context, or build the read inside \
             MykoServerContext::checked to be refused with an error"
        );
    }
}

/// Closes one `watch_root_reads` on this thread, also when its construction
/// unwinds, handing the outer watch its first read.
struct WatchScope(Option<Arc<str>>);

impl Drop for WatchScope {
    fn drop(&mut self) {
        ROOT_READ.with(|read| {
            let mut read = read.borrow_mut();
            read.0 = read.0.saturating_sub(1);
            read.1 = self.0.take().or_else(|| read.1.take());
        });
    }
}

/// Run `construct` and return the first partitioned entity type it asked a root
/// registry for, on this thread. Nested watches report to their caller too.
pub fn watch_root_reads<R>(construct: impl FnOnce() -> R) -> (R, Option<Arc<str>>) {
    let outer = ROOT_READ.with(|read| {
        let mut read = read.borrow_mut();
        read.0 = read.0.saturating_add(1);
        read.1.take()
    });
    let mut close = WatchScope(outer);
    let built = construct();
    let inner = ROOT_READ.with(|read| read.borrow().1.clone());
    // The outer watch keeps its own first read, else learns this one.
    close.0 = close.0.take().or_else(|| inner.clone());
    ROOT_READ.with(|read| read.borrow_mut().1 = None);
    drop(close);
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
            indexes: DashMap::with_hasher(ahash::RandomState::new()),
            relations: std::sync::OnceLock::new(),
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

    /// Tell this root registry which entity types are partitioned, and
    /// create the store of every shared entity type now.
    ///
    /// A partition reads shared stores from inside its own propagation, and
    /// a store is a root graph node, so it must already exist when a
    /// partition first asks for it.
    pub(crate) fn declare_partitioned(&self, is_partitioned: IsPartitioned) {
        for entity_type in crate::item::registered_entity_types() {
            if !is_partitioned(entity_type) {
                drop(self.get_or_create(entity_type));
            }
        }
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

    /// The registry that owns the store of `entity_type`: root's for shared
    /// data read from a partition's registry, this one otherwise.
    pub(crate) fn owner(&self, entity_type: &str) -> &Self {
        self.shared(entity_type).map_or(self, |root| root.as_ref())
    }

    /// The relationship index over this registry's own stores, created on
    /// first use. It lives and dies with the stores it indexes.
    pub(crate) fn relation_index<T: Default + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.relations
            .get_or_init(|| self.born_here(|| Arc::new(T::default())))
            .clone()
            .downcast::<T>()
            .ok()
    }

    /// The index named `key` over the stores of the registry that owns
    /// `entity_type`, built by `build` on first use.
    ///
    /// Every registry keeps its own indexes, born in its own domain: a
    /// partition's index over a shared store is the partition's copy, fed
    /// from root's store. An index is a graph node, and a partition builds
    /// only its own graph, whenever it first asks, including from inside its
    /// own propagation.
    pub(crate) fn index<T: Send + Sync + 'static>(
        &self,
        entity_type: &str,
        key: String,
        build: impl FnOnce(&Self) -> Arc<T>,
    ) -> Arc<T> {
        if let Some(index) = self.indexes.get(&key)
            && let Ok(index) = Arc::clone(index.value()).downcast::<T>()
        {
            return index;
        }
        let owner = self.owner(entity_type);
        let index = build(owner);
        let erased: Arc<dyn std::any::Any + Send + Sync> = index.clone();
        // A racing build loses to the first one stored.
        Arc::clone(self.indexes.entry(key).or_insert(erased).value())
            .downcast::<T>()
            .unwrap_or(index)
    }

    /// Run `build` with this registry's domain ambient: a partition's
    /// domain for a partition's registry, root otherwise. Whatever this
    /// registry keeps for later readers is built here, so it lives in the
    /// domain of the stores it derives from, whichever context asks first.
    pub(crate) fn born_here<R>(&self, build: impl FnOnce() -> R) -> R {
        match &self.partition {
            Some(partition) => partition.domain.enter(build),
            None => hyphae::scheduler::Domain::root().enter(build),
        }
    }

    /// Every index this registry holds of type `T`.
    pub(crate) fn indexes_of<T: Send + Sync + 'static>(&self) -> Vec<Arc<T>> {
        self.indexes
            .iter()
            .filter_map(|entry| Arc::clone(entry.value()).downcast::<T>().ok())
            .collect()
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
                self.born_here(create)
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
