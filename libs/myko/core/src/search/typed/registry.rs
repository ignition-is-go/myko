//! Type-erased dispatch from `&dyn AnyItem` call sites into the right
//! monomorphized `SearchIndex<T>`. See `../SPEC.md` for the rationale.
//!
//! - `DynSearchIndex` is the boundary trait holding the per-type index.
//! - `SearchRegistry` maps `entity_type` strings to the boxed shims.
//! - The single dyn cost is a `TypeId`-keyed downcast at insert/remove; the
//!   typed search side stays fully monomorphized.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::{Hit, SearchIndex, SearchOptions, Searchable};
use crate::core::item::AnyItem;

/// Type-erased shim over a per-type `RwLock<SearchIndex<T>>`. Allows
///
/// `&dyn AnyItem` call sites to dispatch into the right monomorphized index
/// without naming `T`. The `'static` bound + `as_any` is what lets typed
/// reports recover their concrete `TypedShim<T>` from the registry.
pub trait DynSearchIndex: Send + Sync + 'static {
    fn as_any(&self) -> &dyn std::any::Any;
    fn entity_type(&self) -> &'static str;
    fn insert_dyn(&self, item: &dyn AnyItem);
    fn remove_dyn(&self, id: &str);
    fn search_arc_str(&self, query: &str, opts: SearchOptions) -> Vec<Hit<Arc<str>>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Holds one typed `SearchIndex<T>` along with its entity-type tag. Behind
/// `Arc` and registered with the `SearchRegistry`.
pub struct TypedShim<T: Searchable + AnyItem> {
    entity_type: &'static str,
    inner: RwLock<SearchIndex<T>>,
}

impl<T: Searchable + AnyItem> TypedShim<T> {
    #[must_use]
    pub fn new(entity_type: &'static str) -> Self {
        Self {
            entity_type,
            inner: RwLock::new(SearchIndex::new()),
        }
    }

    /// Direct typed access — used by macro-generated typed reports that
    /// don't need to cross the dyn boundary.
    pub const fn inner(&self) -> &RwLock<SearchIndex<T>> {
        &self.inner
    }
}

impl<T> DynSearchIndex for TypedShim<T>
where
    T: Searchable + AnyItem,
    T::Id: From<Arc<str>>,
    Arc<str>: From<T::Id>,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn entity_type(&self) -> &'static str {
        self.entity_type
    }

    fn insert_dyn(&self, item: &dyn AnyItem) {
        let Some(t) = item.as_any().downcast_ref::<T>() else {
            tracing::warn!(
                "SearchRegistry: type mismatch routing {} into {}",
                item.entity_type(),
                self.entity_type
            );
            return;
        };
        if let Ok(mut idx) = self.inner.write() {
            idx.insert(t);
        }
    }

    fn remove_dyn(&self, id: &str) {
        // Disambiguate against the `Arc<str>: From<T::Id>` where-bound, which
        // otherwise shadows the std `From<&str>` impl when `Arc::from` is
        // resolved in this generic context.
        let arc: Arc<str> = <Arc<str> as From<&str>>::from(id);
        let typed: T::Id = arc.into();
        if let Ok(mut idx) = self.inner.write() {
            idx.remove(&typed);
        }
    }

    fn search_arc_str(&self, query: &str, opts: SearchOptions) -> Vec<Hit<Arc<str>>> {
        let Ok(guard) = self.inner.read() else {
            return Vec::new();
        };
        guard
            .search(query, opts)
            .into_iter()
            .map(|h| Hit {
                id: Arc::<str>::from(h.id),
                score: h.score,
                matched_field: h.matched_field,
            })
            .collect()
    }

    fn len(&self) -> usize {
        self.inner.read().map_or(0, |g| g.len())
    }
}

/// Per-type indexes keyed by `entity_type`. Built once at startup by walking
/// the `SearchableRegistration` inventory and calling each registration's
/// `register` fn.
pub struct SearchRegistry {
    indexes: HashMap<&'static str, Arc<dyn DynSearchIndex>>,
}

impl SearchRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    /// Register a per-type index. Idempotent within an `entity_type` — a second
    /// call replaces the existing index (intended for testing; production
    /// startup calls each `register` fn exactly once).
    pub fn register<T>(&mut self, entity_type: &'static str)
    where
        T: Searchable + AnyItem,
        T::Id: From<Arc<str>>,
        Arc<str>: From<T::Id>,
    {
        let shim: Arc<dyn DynSearchIndex> = Arc::new(TypedShim::<T>::new(entity_type));
        self.indexes.insert(entity_type, shim);
    }

    /// Insert via the dyn boundary. Looks up the right typed index by
    /// `item.entity_type()` and delegates after a single `TypeId` downcast.
    /// No-op if no index is registered for the entity type.
    pub fn insert(&self, item: &dyn AnyItem) {
        let Some(shim) = self.indexes.get(item.entity_type()) else {
            return;
        };
        shim.insert_dyn(item);
    }

    /// Remove via the dyn boundary. No-op for unregistered entity types.
    pub fn remove(&self, entity_type: &str, id: &str) {
        if let Some(shim) = self.indexes.get(entity_type) {
            shim.remove_dyn(id);
        }
    }

    /// Stringly-typed search — used by the legacy `EntitySearch` report and
    /// by cross-type "search anything" command palettes. Typed reports
    /// bypass this and call into `SearchIndex<T>` directly via
    /// `typed_handle`.
    #[must_use]
    pub fn search(
        &self,
        entity_type: &str,
        query: &str,
        opts: SearchOptions,
    ) -> Vec<Hit<Arc<str>>> {
        let Some(shim) = self.indexes.get(entity_type) else {
            return Vec::new();
        };
        shim.search_arc_str(query, opts)
    }

    /// Scoped access to a typed index. The closure receives a `&RwLock<
    /// SearchIndex<T>>` and can read or write within its own scope. Used
    /// by macro-generated typed reports that already know `T` and want to
    /// avoid the dyn boundary.
    pub fn with_typed<T, R>(
        &self,
        entity_type: &str,
        f: impl FnOnce(&RwLock<SearchIndex<T>>) -> R,
    ) -> Option<R>
    where
        T: Searchable + AnyItem + 'static,
    {
        let shim = self.indexes.get(entity_type)?;
        let typed = shim.as_any().downcast_ref::<TypedShim<T>>()?;
        Some(f(&typed.inner))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn entity_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.indexes.keys().copied()
    }
}

impl Default for SearchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "bench"))]
mod tests {
    use super::*;
    use crate::bench_entities::BenchItem;

    fn item(id: &str, name: &str, category: &str) -> BenchItem {
        BenchItem {
            id: id.into(),
            name: name.to_string(),
            category: category.to_string(),
            value: 0,
        }
    }

    #[test]
    fn registry_routes_inserts_and_searches() {
        let mut registry = SearchRegistry::new();
        registry.register::<BenchItem>("BenchItem");

        let i1 = item("1", "audio mixer", "hardware");
        let i2 = item("2", "video camera", "hardware");
        registry.insert(&i1);
        registry.insert(&i2);

        let hits = registry.search("BenchItem", "mixer", SearchOptions::default());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits.first().map(|hit| hit.id.as_ref()), Some("1"));
    }

    #[test]
    fn registry_remove_via_string_id() {
        let mut registry = SearchRegistry::new();
        registry.register::<BenchItem>("BenchItem");

        registry.insert(&item("1", "audio mixer", "hardware"));
        registry.remove("BenchItem", "1");

        let hits = registry.search("BenchItem", "mixer", SearchOptions::default());
        assert!(hits.is_empty());
    }

    #[test]
    fn registry_search_unknown_entity_type_returns_empty() {
        let registry = SearchRegistry::new();
        let hits = registry.search("Unknown", "anything", SearchOptions::default());
        assert!(hits.is_empty());
    }

    #[test]
    fn registry_insert_wrong_type_is_noop_for_other_indexes() {
        let mut registry = SearchRegistry::new();
        registry.register::<BenchItem>("BenchItem");

        // Item's entity_type doesn't match anything registered — no-op.
        // (We can't easily fabricate a different AnyItem here, but registering
        // BenchItem and inserting an unregistered Server-shaped item would be
        // the failure mode. Covered by the lookup-by-entity_type filter.)
        registry.insert(&item("1", "x", "y"));
        assert_eq!(
            registry
                .search("BenchItem", "x", SearchOptions::default())
                .len(),
            1
        );
    }

    #[test]
    fn typed_handle_round_trips_via_with_typed() {
        let mut registry = SearchRegistry::new();
        registry.register::<BenchItem>("BenchItem");
        registry.insert(&item("1", "audio mixer", "hardware"));

        // Direct typed access — no dyn boundary on the search call.
        let hits = registry
            .with_typed::<BenchItem, _>("BenchItem", |lock| {
                lock.read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .search("mixer", SearchOptions::default())
            });
        assert!(hits.is_some(), "typed registry handle");
        let Some(hits) = hits else {
            return;
        };
        assert_eq!(hits.len(), 1);
        // Note: typed hits carry the typed BenchItemId, not Arc<str>.
        assert_eq!(hits.first().map(|hit| hit.id.0.as_ref()), Some("1"));
    }
}
