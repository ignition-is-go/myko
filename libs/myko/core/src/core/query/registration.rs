//! Query registration via inventory.

use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use hyphae::{MapDiff, SelectExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use super::{
    super::item::Eventable,
    cell::FilteredCellMap,
    context::{QueryCellContext, QueryContext},
    request::QueryRequest,
    traits::{AnyQuery, QueryBuildCellCtx, QueryHandler, QueryParams, QueryTestCtx},
};
use crate::{
    common::with_id::WithId,
    core::{item::downcast_any_item_arc, relationship::FkExtractor},
    request::RequestContext,
    server::CellServerCtx,
    store::StoreRegistry,
};

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Type alias for query parse function.
pub type QueryParseFn = fn(Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and host_id, returns a FilteredCellMap.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Option<Arc<CellServerCtx>>,
) -> Result<FilteredCellMap, String>;

type AnyItemArc = Arc<dyn crate::core::item::AnyItem>;
type AnyItemMap = hyphae::CellMap<Arc<str>, AnyItemArc>;
type WeakAnyItemMap = hyphae::WeakCellMap<Arc<str>, AnyItemArc>;
type BucketEntries = Vec<(Arc<str>, AnyItemArc)>;
type BucketDiff = MapDiff<Arc<str>, AnyItemArc>;
type BucketDiffs = Vec<BucketDiff>;

// ─────────────────────────────────────────────────────────────────────────────
// QueryRegistration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(QueryRegistration);

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryRuntimeMetrics {
    pub cell_factories_created: u64,
    pub per_item_guards_created: u64,
    pub per_item_guards_removed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct QueryRuntimePerIdMetrics {
    pub query_id: Arc<str>,
    pub cell_factories_created: u64,
    pub per_item_guards_created: u64,
    pub per_item_guards_removed: u64,
}

static QUERY_CELL_FACTORIES_CREATED: AtomicU64 = AtomicU64::new(0);
static QUERY_PER_ITEM_GUARDS_CREATED: AtomicU64 = AtomicU64::new(0);
static QUERY_PER_ITEM_GUARDS_REMOVED: AtomicU64 = AtomicU64::new(0);
static QUERY_FACTORIES_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static QUERY_GUARDS_CREATED_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static QUERY_GUARDS_REMOVED_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static BELONGS_TO_SOURCE_INDEXES: OnceLock<DashMap<String, Arc<BelongsToSourceIndex>>> =
    OnceLock::new();

fn query_factories_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_FACTORIES_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_created_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_CREATED_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_removed_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_REMOVED_BY_ID.get_or_init(DashMap::new)
}

fn belongs_to_source_indexes() -> &'static DashMap<String, Arc<BelongsToSourceIndex>> {
    BELONGS_TO_SOURCE_INDEXES.get_or_init(DashMap::new)
}

fn increment_counter(map: &DashMap<Arc<str>, u64>, key: Arc<str>) {
    if let Some(mut value) = map.get_mut(&key) {
        *value = value.saturating_add(1);
    } else {
        map.insert(key, 1);
    }
}

pub fn query_runtime_metrics() -> QueryRuntimeMetrics {
    QueryRuntimeMetrics {
        cell_factories_created: QUERY_CELL_FACTORIES_CREATED.load(Ordering::Relaxed),
        per_item_guards_created: QUERY_PER_ITEM_GUARDS_CREATED.load(Ordering::Relaxed),
        per_item_guards_removed: QUERY_PER_ITEM_GUARDS_REMOVED.load(Ordering::Relaxed),
    }
}

pub fn query_runtime_metrics_by_id(limit: usize) -> Vec<QueryRuntimePerIdMetrics> {
    let mut rows: Vec<QueryRuntimePerIdMetrics> = query_factories_by_id()
        .iter()
        .map(|entry| {
            let query_id = entry.key().clone();
            let cell_factories_created = *entry.value();
            let per_item_guards_created = query_guards_created_by_id()
                .get(&query_id)
                .map(|v| *v.value())
                .unwrap_or(0);
            let per_item_guards_removed = query_guards_removed_by_id()
                .get(&query_id)
                .map(|v| *v.value())
                .unwrap_or(0);
            QueryRuntimePerIdMetrics {
                query_id,
                cell_factories_created,
                per_item_guards_created,
                per_item_guards_removed,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        let a_live = a
            .per_item_guards_created
            .saturating_sub(a.per_item_guards_removed);
        let b_live = b
            .per_item_guards_created
            .saturating_sub(b.per_item_guards_removed);
        b_live
            .cmp(&a_live)
            .then_with(|| b.cell_factories_created.cmp(&a.cell_factories_created))
    });
    if rows.len() > limit {
        rows.truncate(limit);
    }
    rows
}

/// Per-foreign-id bucket index backing `#[belongs_to]` reactive queries.
///
/// `buckets` holds only *weak* handles: a bucket's `AnyItemMap` stays alive
/// exactly as long as some subscriber (via [`build_belongs_to_source_map`])
/// holds a strong reference to it. Once the last subscriber drops it, the
/// weak entry naturally fails to upgrade and gets lazily reaped — the
/// alternative (a strong `Arc<AnyItemMap>` retained forever) is a real
/// memory leak: one permanent bucket per distinct foreign id ever seen,
/// which never shrinks even after every relation is unassigned.
struct BelongsToSourceIndex {
    store: Arc<crate::store::EntityStore>,
    buckets: DashMap<Arc<str>, WeakAnyItemMap>,
    _driver: Arc<AnyItemMap>,
}

impl BelongsToSourceIndex {
    fn new(store: Arc<crate::store::EntityStore>, extract_fk: FkExtractor) -> Arc<Self> {
        let driver = Arc::new(AnyItemMap::new());
        let index = Arc::new(Self {
            store: store.clone(),
            buckets: DashMap::new(),
            _driver: driver.clone(),
        });

        let index_for_diffs = index.clone();
        let guard = store.subscribe_diffs(move |diff| {
            index_for_diffs.apply_diff(diff, extract_fk);
        });
        driver.own_guard(guard);
        index
    }

    /// Look up a bucket for *internal diff routing only* — never creates
    /// one. If nobody's subscribed to `foreign_id` there's no bucket state
    /// worth maintaining; a dead weak entry found here is reaped
    /// immediately, so `self.buckets` never accumulates more than what's
    /// currently live. See [`sweep_dead_buckets`](Self::sweep_dead_buckets)
    /// for the backstop covering ids that go dead but are never looked up
    /// again.
    fn route_to_live_bucket(&self, foreign_id: &Arc<str>) -> Option<AnyItemMap> {
        let entry = self.buckets.get(foreign_id)?;
        if let Some(map) = entry.upgrade() {
            return Some(map);
        }
        drop(entry);
        self.buckets.remove(foreign_id);
        None
    }

    /// Subscriber entry point (via [`build_belongs_to_source_map`]): returns
    /// the live bucket if one exists, or creates a fresh one backfilled from
    /// the current store state.
    ///
    /// The backfill matters: `apply_diff` (via `route_to_live_bucket`) never
    /// creates or updates a bucket nobody's watching, so a newly-live bucket
    /// may have missed every diff since this relation's one-time index-wide
    /// bootstrap in [`new`](Self::new). Without backfilling here, a client
    /// subscribing (or re-subscribing after a prior subscriber dropped off)
    /// would silently see an empty result instead of the parent's actual
    /// current children.
    fn bucket_for(&self, foreign_id: Arc<str>, extract_fk: FkExtractor) -> AnyItemMap {
        if let Some(map) = self.route_to_live_bucket(&foreign_id) {
            return map;
        }

        let map = AnyItemMap::new();
        let backfill: BucketEntries = self
            .store
            .snapshot()
            .into_iter()
            .filter(|(_, item)| extract_fk(item.as_any()).as_ref() == Some(&foreign_id))
            .collect();
        if !backfill.is_empty() {
            map.apply_batch(vec![MapDiff::Initial { entries: backfill }]);
        }
        self.buckets.insert(foreign_id, map.downgrade());
        map
    }

    /// Drop bucket entries nobody's subscribed to anymore. `route_to_live_bucket`
    /// already reaps dead entries lazily on next access, but a foreign id
    /// that goes dead and is never looked up again would otherwise sit in
    /// `self.buckets` forever (just a `Weak` + an `Arc<str>` key, far
    /// smaller than the leak this replaces, but still unbounded over time).
    /// Called from `CellServerCtx::sweep_dead_cache_entries` via
    /// [`sweep_all_belongs_to_source_indexes`].
    fn sweep_dead_buckets(&self) {
        self.buckets.retain(|_, weak| weak.upgrade().is_some());
    }

    fn apply_diff(&self, diff: &BucketDiff, extract_fk: FkExtractor) {
        match diff {
            MapDiff::Initial { entries } => {
                let mut grouped: HashMap<Arc<str>, BucketEntries> = HashMap::new();
                for (id, item) in entries {
                    if let Some(fk) = extract_fk(item.as_any()) {
                        grouped
                            .entry(fk)
                            .or_default()
                            .push((id.clone(), item.clone()));
                    }
                }

                // Every currently-*live* bucket needs to receive the new
                // state — if its FK is absent from `grouped`, that bucket is
                // now empty and must emit `Initial { empty }` so downstream
                // subscribers observe the drop. (Dead/unsubscribed buckets
                // don't need this — nobody's watching them, and
                // `route_to_live_bucket` reaps them lazily regardless.)
                // This explicit empty-notify (rather than just letting a
                // dead bucket's Drop silently orphan subscribers) is why
                // this path exists at all: an earlier version just cleared
                // `self.buckets` and relied on ownership drop to clean up,
                // which silently dropped removal events on the floor — a
                // full-clear source diff (e.g. the last matching row being
                // deleted via `remove_many`) never reached bucket
                // subscribers.
                let live_fks: Vec<Arc<str>> = self
                    .buckets
                    .iter()
                    .filter(|entry| entry.value().upgrade().is_some())
                    .map(|entry| entry.key().clone())
                    .collect();
                for fk in &live_fks {
                    if !grouped.contains_key(fk)
                        && let Some(bucket) = self.route_to_live_bucket(fk)
                    {
                        bucket.apply_batch(vec![MapDiff::Initial {
                            entries: Vec::new(),
                        }]);
                    }
                }

                // Route new state only to buckets someone's actually
                // watching — an FK with no live subscriber gets built
                // lazily (with backfill) next time `bucket_for` is called
                // for it via `build_belongs_to_source_map`.
                for (fk, bucket_entries) in grouped {
                    if let Some(bucket) = self.route_to_live_bucket(&fk) {
                        bucket.apply_batch(vec![MapDiff::Initial {
                            entries: bucket_entries,
                        }]);
                    }
                }
            }
            MapDiff::Insert { key, value } => {
                if let Some(fk) = extract_fk(value.as_any())
                    && let Some(bucket) = self.route_to_live_bucket(&fk)
                {
                    bucket.apply_batch(vec![MapDiff::Insert {
                        key: key.clone(),
                        value: value.clone(),
                    }]);
                }
            }
            MapDiff::Remove { key, old_value } => {
                if let Some(fk) = extract_fk(old_value.as_any())
                    && let Some(bucket) = self.route_to_live_bucket(&fk)
                {
                    bucket.apply_batch(vec![MapDiff::Remove {
                        key: key.clone(),
                        old_value: old_value.clone(),
                    }]);
                }
            }
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => {
                let old_fk = extract_fk(old_value.as_any());
                let new_fk = extract_fk(new_value.as_any());
                match (old_fk, new_fk) {
                    (Some(old_fk), Some(new_fk)) if old_fk == new_fk => {
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Update {
                                key: key.clone(),
                                old_value: old_value.clone(),
                                new_value: new_value.clone(),
                            }]);
                        }
                    }
                    (Some(old_fk), Some(new_fk)) => {
                        if let Some(bucket) = self.route_to_live_bucket(&old_fk) {
                            bucket.apply_batch(vec![MapDiff::Remove {
                                key: key.clone(),
                                old_value: old_value.clone(),
                            }]);
                        }
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Insert {
                                key: key.clone(),
                                value: new_value.clone(),
                            }]);
                        }
                    }
                    (Some(old_fk), None) => {
                        if let Some(bucket) = self.route_to_live_bucket(&old_fk) {
                            bucket.apply_batch(vec![MapDiff::Remove {
                                key: key.clone(),
                                old_value: old_value.clone(),
                            }]);
                        }
                    }
                    (None, Some(new_fk)) => {
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Insert {
                                key: key.clone(),
                                value: new_value.clone(),
                            }]);
                        }
                    }
                    (None, None) => {}
                }
            }
            MapDiff::Batch { changes } => {
                let mut by_fk: HashMap<Arc<str>, BucketDiffs> = HashMap::new();

                for change in changes {
                    match change {
                        MapDiff::Insert { key, value } => {
                            if let Some(fk) = extract_fk(value.as_any()) {
                                by_fk.entry(fk).or_default().push(MapDiff::Insert {
                                    key: key.clone(),
                                    value: value.clone(),
                                });
                            }
                        }
                        MapDiff::Remove { key, old_value } => {
                            if let Some(fk) = extract_fk(old_value.as_any()) {
                                by_fk.entry(fk).or_default().push(MapDiff::Remove {
                                    key: key.clone(),
                                    old_value: old_value.clone(),
                                });
                            }
                        }
                        MapDiff::Update {
                            key,
                            old_value,
                            new_value,
                        } => {
                            let old_fk = extract_fk(old_value.as_any());
                            let new_fk = extract_fk(new_value.as_any());
                            match (old_fk, new_fk) {
                                (Some(old_fk), Some(new_fk)) if old_fk == new_fk => {
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Update {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                        new_value: new_value.clone(),
                                    });
                                }
                                (Some(old_fk), Some(new_fk)) => {
                                    by_fk.entry(old_fk).or_default().push(MapDiff::Remove {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                    });
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Insert {
                                        key: key.clone(),
                                        value: new_value.clone(),
                                    });
                                }
                                (Some(old_fk), None) => {
                                    by_fk.entry(old_fk).or_default().push(MapDiff::Remove {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                    });
                                }
                                (None, Some(new_fk)) => {
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Insert {
                                        key: key.clone(),
                                        value: new_value.clone(),
                                    });
                                }
                                (None, None) => {}
                            }
                        }
                        MapDiff::Initial { .. } | MapDiff::Batch { .. } => {
                            self.apply_diff(change, extract_fk);
                        }
                    }
                }

                for (fk, bucket_changes) in by_fk {
                    if let Some(bucket) = self.route_to_live_bucket(&fk) {
                        bucket.apply_batch(bucket_changes);
                    }
                }
            }
        }
    }
}

pub fn build_belongs_to_source_map(
    registry: Arc<StoreRegistry>,
    host_id: Uuid,
    local_type: &'static str,
    field_name: &'static str,
    extract_fk: FkExtractor,
    foreign_id: Arc<str>,
) -> FilteredCellMap {
    let key = format!("{host_id}:{local_type}:{field_name}");
    let index = belongs_to_source_indexes()
        .entry(key)
        .or_insert_with(|| {
            let store = registry.get_or_create(local_type);
            BelongsToSourceIndex::new(store, extract_fk)
        })
        .clone();
    index.bucket_for(foreign_id, extract_fk).lock()
}

/// Sweep dead (no-longer-subscribed) buckets across every belongs-to
/// relation's source index. `route_to_live_bucket` reaps dead entries lazily
/// on next access, but a foreign id that goes dead and is never looked up
/// again would otherwise linger; called from
/// `CellServerCtx::sweep_dead_cache_entries`.
pub fn sweep_all_belongs_to_source_indexes() {
    for entry in belongs_to_source_indexes().iter() {
        entry.value().sweep_dead_buckets();
    }
}

/// Build a `FilteredCellMap` containing only the entries at the given ids,
/// using direct per-key store lookups instead of an O(N) `test_entity` scan.
///
/// Used by `Get<Entity>sByIds::build_view` so the initial query result is
/// constructed in O(M) where M = ids.len(). Per-key cells from the store
/// keep the result reactive to inserts / updates / deletes for those
/// specific ids; `test_entity` semantics still hold because the returned
/// map only ever contains keys from `ids`.
pub fn build_ids_source_map(
    store: &Arc<crate::store::EntityStore>,
    ids: &[Arc<str>],
) -> FilteredCellMap {
    use hyphae::{Signal, Watchable};

    let result: hyphae::CellMap<Arc<str>, AnyItemArc> = hyphae::CellMap::new();
    for id in ids {
        let key_cell = store.get(id);
        // Weak, not a strong clone: `key_cell` belongs to the store, which
        // lives for the whole process — a strong capture here would make the
        // store's per-key subscriber list hold `result` (and everything
        // built on top of it downstream) alive forever, regardless of
        // whether any external caller still references it. This is exactly
        // the reference cycle `query_cache`'s weak-ref design assumes never
        // happens (see `MapCacheEntry` in server/context.rs).
        let result_weak = result.downgrade();
        let key_for_cb = id.clone();
        let guard = key_cell.subscribe(move |signal| {
            let Some(result_for_cb) = result_weak.upgrade() else {
                return;
            };
            if let Signal::Value(arc_opt) = signal {
                match arc_opt.as_ref() {
                    Some(item) => {
                        result_for_cb.insert(key_for_cb.clone(), item.clone());
                    }
                    None => {
                        result_for_cb.remove(&key_for_cb);
                    }
                }
            }
        });
        result.own(guard);
    }
    result.lock()
}

pub fn filter_query_over_source<Q>(
    source: FilteredCellMap,
    query: Arc<Q>,
    query_context: Arc<QueryContext>,
) -> FilteredCellMap
where
    Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
    Q::Item:
        DeserializeOwned + Eventable + WithId + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    hyphae::MapQuery::materialize(source.select(move |item_any: &AnyItemArc| {
        let item = downcast_any_item_arc::<Q::Item>(item_any, "filter_query_over_source");
        Q::test_entity(QueryTestCtx {
            item,
            query: query.clone(),
            query_context: query_context.clone(),
        })
    }))
}

/// Registration entry for a query type.
/// Collected via inventory for automatic discovery.
pub struct QueryRegistration {
    /// Query identifier (e.g., "GetAllTargets")
    pub query_id: &'static str,
    /// Entity type this query returns (e.g., "Target")
    pub query_item_type: &'static str,
    /// Crate where this query is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing query from JSON
    pub parse: QueryParseFn,
    /// Factory for creating reactive cell from query
    pub cell_factory: QueryCellFactory,
    /// Query struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// Query struct's doc comment, if any.
    pub description: Option<&'static str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryFactory - Static methods for query types
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating query registration data.
///
/// This trait has a blanket implementation for all types implementing `QueryParams`,
/// so user-defined queries automatically get `parse` and `cell_factory` methods.
pub trait QueryFactory: QueryParams {
    /// Parse JSON into this query type.
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

    /// Create a reactive cell for this query.
    fn cell_factory(
        query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Result<FilteredCellMap, String>;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let query = serde_json::from_value::<QueryRequest<Q>>(value)?;
        Ok(Arc::new(query))
    }

    fn cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Result<FilteredCellMap, String> {
        QUERY_CELL_FACTORIES_CREATED.fetch_add(1, Ordering::Relaxed);
        let query_id = Q::query_id_static();
        // Bounded cardinality (one span per query *registration*, not per
        // `test_entity` item test — that runs reactively per store mutation
        // and would be far too hot to span), matching `myko.command`.
        let _span = tracing::trace_span!("myko.query", query = query_id.as_ref()).entered();
        crate::server::dispatch_metrics::record_query(query_id.as_ref(), request_ctx.origin());
        increment_counter(query_factories_by_id(), query_id);
        let any_ref: &dyn Any = any_query.as_ref();
        let request: QueryRequest<Q> = any_ref
            .downcast_ref::<QueryRequest<Q>>()
            .cloned()
            .ok_or_else(|| "Failed to downcast query payload".to_string())?;
        let query: Arc<Q> = Arc::new(request.query);

        let query_ctx = Arc::new(QueryContext {
            req: request_ctx.clone(),
        });
        let query_cell_ctx =
            QueryCellContext::new(request_ctx, query_ctx.clone(), registry.clone(), server_ctx);

        if let Some(built) = Q::build_view(QueryBuildCellCtx {
            query: query.clone(),
            query_context: query_cell_ctx.clone(),
        }) {
            return Ok(hyphae::MapQuery::materialize(built));
        }

        let store: crate::store::EntityStore =
            (*registry.get_or_create(&Q::query_item_type_static())).clone();
        Ok(hyphae::MapQuery::materialize(store.select(
            move |item_any: &AnyItemArc| {
                let item = downcast_any_item_arc::<Q::Item>(item_any, "QueryFactory::cell_factory");
                Q::test_entity(QueryTestCtx {
                    item,
                    query: query.clone(),
                    query_context: query_ctx.clone(),
                })
            },
        )))
    }
}

#[cfg(test)]
mod belongs_to_source_index_tests {
    use std::any::Any;

    use serde::Serialize;

    use super::*;
    use crate::common::with_id::WithId;

    #[derive(Debug, Clone, PartialEq, Serialize)]
    struct TestChild {
        id: Arc<str>,
        parent_id: Arc<str>,
    }

    impl WithId for TestChild {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl crate::core::item::AnyItem for TestChild {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestChild"
        }

        fn equals(&self, other: &dyn crate::core::item::AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|t| t == self)
                .unwrap_or(false)
        }
    }

    fn extract_parent_fk(item: &dyn Any) -> Option<Arc<str>> {
        item.downcast_ref::<TestChild>()
            .map(|c| c.parent_id.clone())
    }

    fn child(id: &str, parent: &str) -> (Arc<str>, AnyItemArc) {
        (
            Arc::from(id),
            Arc::new(TestChild {
                id: Arc::from(id),
                parent_id: Arc::from(parent),
            }) as AnyItemArc,
        )
    }

    fn new_store() -> Arc<crate::store::EntityStore> {
        Arc::new(hyphae::CellMap::new())
    }

    #[test]
    fn dropped_subscriptions_do_not_leak_across_many_distinct_parents() {
        // Reproduces the leak: every distinct foreign id ever subscribed to
        // used to leave a permanent bucket behind. With weak-ref buckets,
        // dropping every subscriber and sweeping must bring the count to 0
        // regardless of how many distinct parents were ever seen.
        let store = new_store();
        let index = BelongsToSourceIndex::new(store, extract_parent_fk);

        for i in 0..50 {
            let parent: Arc<str> = Arc::from(format!("parent-{i}"));
            let bucket = index.bucket_for(parent, extract_parent_fk);
            drop(bucket); // simulates every subscriber unsubscribing
        }

        index.sweep_dead_buckets();
        assert_eq!(
            index.buckets.len(),
            0,
            "sweep must reap all buckets once every subscriber has dropped"
        );
    }

    #[test]
    fn live_subscription_survives_going_empty_then_repopulating() {
        // The naive fix (remove a bucket the moment it goes empty) breaks
        // this: a still-live subscriber would get orphaned from a bucket
        // that later gets silently replaced. Weak-ref buckets avoid this —
        // as long as the subscriber holds their strong handle, `bucket_for`
        // keeps returning the *same* object.
        let store = new_store();
        let (id, item) = child("c1", "parent-x");
        store.insert(id.clone(), item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);
        let bucket = index.bucket_for(Arc::from("parent-x"), extract_parent_fk);
        assert_eq!(bucket.snapshot().len(), 1);

        // Remove the only child — bucket goes empty, but `bucket` is still
        // held here, simulating a live subscriber.
        store.remove(&id);
        assert_eq!(bucket.snapshot().len(), 0);

        // A new child arrives under the same parent — the still-held handle
        // must see it, not a disconnected/orphaned bucket.
        let (id2, item2) = child("c2", "parent-x");
        store.insert(id2, item2);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "a live subscriber must see re-population after its bucket went empty"
        );
    }

    #[test]
    fn resubscribing_after_reap_backfills_current_children() {
        // The bug a naive weak-ref swap alone would introduce: once a
        // bucket is reaped, apply_diff never creates buckets nobody's
        // watching, so a fresh subscription must explicitly backfill from
        // the current store state rather than starting empty.
        let store = new_store();
        let (id, item) = child("c1", "parent-y");
        store.insert(id, item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);

        {
            let bucket = index.bucket_for(Arc::from("parent-y"), extract_parent_fk);
            assert_eq!(bucket.snapshot().len(), 1);
        }
        index.sweep_dead_buckets();
        assert!(index.buckets.is_empty());

        let bucket = index.bucket_for(Arc::from("parent-y"), extract_parent_fk);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "resubscribing after the bucket was reaped must backfill current children, not start empty"
        );
    }
}
