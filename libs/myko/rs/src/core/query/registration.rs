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
use hypha::{Gettable, MapDiff};
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
    common::with_id::WithId, core::relationship::FkExtractor, request::RequestContext,
    server::CellServerCtx, store::StoreRegistry,
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

struct BelongsToSourceIndex {
    buckets: DashMap<Arc<str>, Arc<hypha::CellMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>>>,
    _driver: Arc<hypha::CellMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>>,
}

impl BelongsToSourceIndex {
    fn new(store: Arc<crate::store::EntityStore>, extract_fk: FkExtractor) -> Arc<Self> {
        let driver = Arc::new(hypha::CellMap::<
            Arc<str>,
            Arc<dyn crate::core::item::AnyItem>,
        >::new());
        let index = Arc::new(Self {
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

    fn bucket_for(
        &self,
        foreign_id: Arc<str>,
    ) -> Arc<hypha::CellMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>> {
        self.buckets
            .entry(foreign_id)
            .or_insert_with(|| {
                Arc::new(hypha::CellMap::<
                    Arc<str>,
                    Arc<dyn crate::core::item::AnyItem>,
                >::new())
            })
            .clone()
    }

    fn apply_diff(
        &self,
        diff: &MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
        extract_fk: FkExtractor,
    ) {
        match diff {
            MapDiff::Initial { entries } => {
                self.buckets.clear();
                for (id, item) in entries {
                    if let Some(fk) = extract_fk(item.as_any()) {
                        self.bucket_for(fk).insert(id.clone(), item.clone());
                    }
                }
            }
            MapDiff::Insert { key, value } => {
                if let Some(fk) = extract_fk(value.as_any()) {
                    self.bucket_for(fk).insert(key.clone(), value.clone());
                }
            }
            MapDiff::Remove { key, old_value } => {
                if let Some(fk) = extract_fk(old_value.as_any()) {
                    self.bucket_for(fk).remove(key);
                }
            }
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => {
                let old_fk = extract_fk(old_value.as_any());
                let new_fk = extract_fk(new_value.as_any());
                if old_fk != new_fk
                    && let Some(old_fk) = old_fk
                {
                    self.bucket_for(old_fk).remove(key);
                }
                if let Some(new_fk) = new_fk {
                    self.bucket_for(new_fk)
                        .insert(key.clone(), new_value.clone());
                }
            }
            MapDiff::Batch { changes } => {
                let mut by_fk: HashMap<
                    Arc<str>,
                    Vec<MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>>,
                > = HashMap::new();

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
                    self.bucket_for(fk).apply_batch(bucket_changes);
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
    index.bucket_for(foreign_id).as_ref().clone().lock()
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
    let output = hypha::CellMap::<Arc<str>, Arc<dyn crate::core::item::AnyItem>>::new();
    let output_for_diffs = output.clone();
    let query_for_diffs = query.clone();
    let query_ctx_for_diffs = query_context.clone();

    let guard = source.subscribe_diffs(move |diff| {
        fn apply_diff<Q>(
            diff: &MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
            output: &hypha::CellMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
            query: &Arc<Q>,
            query_ctx: &Arc<QueryContext>,
        ) where
            Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
            Q::Item: DeserializeOwned
                + Eventable
                + WithId
                + Clone
                + std::fmt::Debug
                + Send
                + Sync
                + 'static,
        {
            match diff {
                MapDiff::Initial { entries } => {
                    for (_id, item_any) in entries {
                        let Some(item) = item_any.as_any().downcast_ref::<Q::Item>() else {
                            continue;
                        };
                        let item = Arc::new(item.clone());
                        let include = Q::test_entity(QueryTestCtx {
                            item: item.clone(),
                            query: query.clone(),
                            query_context: query_ctx.clone(),
                        });
                        if include {
                            output.insert(item.id(), item as Arc<dyn crate::core::item::AnyItem>);
                        }
                    }
                }
                MapDiff::Insert { value, .. }
                | MapDiff::Update {
                    new_value: value, ..
                } => {
                    let Some(item) = value.as_any().downcast_ref::<Q::Item>() else {
                        return;
                    };
                    let item = Arc::new(item.clone());
                    let id = item.id();
                    let include = Q::test_entity(QueryTestCtx {
                        item: item.clone(),
                        query: query.clone(),
                        query_context: query_ctx.clone(),
                    });
                    if include {
                        output.insert(id, item as Arc<dyn crate::core::item::AnyItem>);
                    } else {
                        output.remove(&id);
                    }
                }
                MapDiff::Remove { key, .. } => {
                    output.remove(key);
                }
                MapDiff::Batch { changes } => {
                    let mut current: HashMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>> =
                        output.entries().get().into_iter().collect();
                    let mut downstream_changes: Vec<
                        MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                    > = Vec::new();

                    fn apply_batch_change<Q>(
                        change: &MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                        current: &mut HashMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                        downstream_changes: &mut Vec<
                            MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                        >,
                        query: &Arc<Q>,
                        query_ctx: &Arc<QueryContext>,
                    ) where
                        Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
                        Q::Item: DeserializeOwned
                            + Eventable
                            + WithId
                            + Clone
                            + std::fmt::Debug
                            + Send
                            + Sync
                            + 'static,
                    {
                        match change {
                            MapDiff::Initial { entries } => {
                                let mut next: HashMap<
                                    Arc<str>,
                                    Arc<dyn crate::core::item::AnyItem>,
                                > = HashMap::new();
                                for (_id, item_any) in entries {
                                    let Some(item) = item_any.as_any().downcast_ref::<Q::Item>()
                                    else {
                                        continue;
                                    };
                                    let item = Arc::new(item.clone());
                                    let include = Q::test_entity(QueryTestCtx {
                                        item: item.clone(),
                                        query: query.clone(),
                                        query_context: query_ctx.clone(),
                                    });
                                    if include {
                                        next.insert(
                                            item.id(),
                                            item as Arc<dyn crate::core::item::AnyItem>,
                                        );
                                    }
                                }
                                let entries = next
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect::<Vec<_>>();
                                *current = next;
                                downstream_changes.push(MapDiff::Initial { entries });
                            }
                            MapDiff::Insert { value, .. }
                            | MapDiff::Update {
                                new_value: value, ..
                            } => {
                                let Some(item) = value.as_any().downcast_ref::<Q::Item>() else {
                                    return;
                                };
                                let item = Arc::new(item.clone());
                                let id = item.id();
                                let include = Q::test_entity(QueryTestCtx {
                                    item: item.clone(),
                                    query: query.clone(),
                                    query_context: query_ctx.clone(),
                                });
                                if include {
                                    let new_value = item as Arc<dyn crate::core::item::AnyItem>;
                                    if let Some(old_value) =
                                        current.insert(id.clone(), new_value.clone())
                                    {
                                        downstream_changes.push(MapDiff::Update {
                                            key: id,
                                            old_value,
                                            new_value,
                                        });
                                    } else {
                                        downstream_changes.push(MapDiff::Insert {
                                            key: id,
                                            value: new_value,
                                        });
                                    }
                                } else if let Some(old_value) = current.remove(&id) {
                                    downstream_changes.push(MapDiff::Remove { key: id, old_value });
                                }
                            }
                            MapDiff::Remove { key, .. } => {
                                if let Some(old_value) = current.remove(key) {
                                    downstream_changes.push(MapDiff::Remove {
                                        key: key.clone(),
                                        old_value,
                                    });
                                }
                            }
                            MapDiff::Batch { changes } => {
                                for nested in changes {
                                    apply_batch_change::<Q>(
                                        nested,
                                        current,
                                        downstream_changes,
                                        query,
                                        query_ctx,
                                    );
                                }
                            }
                        }
                    }

                    for change in changes {
                        apply_batch_change::<Q>(
                            change,
                            &mut current,
                            &mut downstream_changes,
                            query,
                            query_ctx,
                        );
                    }
                    if !downstream_changes.is_empty() {
                        output.apply_batch(downstream_changes);
                    }
                }
            }
        }

        apply_diff::<Q>(
            diff,
            &output_for_diffs,
            &query_for_diffs,
            &query_ctx_for_diffs,
        );
    });
    output.own_guard(guard);
    output.lock()
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
            return Ok(built);
        }

        let store = registry.get_or_create(&Q::query_item_type_static());
        let output = hypha::CellMap::<Arc<str>, Arc<dyn crate::core::item::AnyItem>>::new();

        let output_for_diffs = output.clone();
        let query_for_diffs = query.clone();
        let query_ctx_for_diffs = query_ctx.clone();
        let guard = store.subscribe_diffs(move |diff| {
            match diff {
                hypha::MapDiff::Batch { changes } => {
                    log::trace!(
                        target: "myko_rs::core::query::registration",
                        "query source diff: query_id={} item_type={} kind=batch size={}",
                        Q::query_id_static(),
                        Q::query_item_type_static(),
                        changes.len()
                    );
                }
                hypha::MapDiff::Initial { entries } => {
                    log::trace!(
                        target: "myko_rs::core::query::registration",
                        "query source diff: query_id={} item_type={} kind=initial size={}",
                        Q::query_id_static(),
                        Q::query_item_type_static(),
                        entries.len()
                    );
                }
                hypha::MapDiff::Insert { .. } => {
                    log::trace!(
                        target: "myko_rs::core::query::registration",
                        "query source diff: query_id={} item_type={} kind=insert",
                        Q::query_id_static(),
                        Q::query_item_type_static()
                    );
                }
                hypha::MapDiff::Update { .. } => {
                    log::trace!(
                        target: "myko_rs::core::query::registration",
                        "query source diff: query_id={} item_type={} kind=update",
                        Q::query_id_static(),
                        Q::query_item_type_static()
                    );
                }
                hypha::MapDiff::Remove { .. } => {
                    log::trace!(
                        target: "myko_rs::core::query::registration",
                        "query source diff: query_id={} item_type={} kind=remove",
                        Q::query_id_static(),
                        Q::query_item_type_static()
                    );
                }
            }
            fn apply_diff<Q>(
                diff: &hypha::MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                output: &hypha::CellMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                query: &Arc<Q>,
                query_ctx: &Arc<QueryContext>,
            ) where
                Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
                Q::Item: DeserializeOwned
                    + Eventable
                    + WithId
                    + Clone
                    + std::fmt::Debug
                    + Send
                    + Sync
                    + 'static,
            {
                match diff {
                    hypha::MapDiff::Initial { entries } => {
                        for (_id, item_any) in entries {
                            let Some(item) = item_any.as_any().downcast_ref::<Q::Item>() else {
                                continue;
                            };
                            let item = Arc::new(item.clone());
                            let include = Q::test_entity(QueryTestCtx {
                                item: item.clone(),
                                query: query.clone(),
                                query_context: query_ctx.clone(),
                            });
                            if include {
                                output
                                    .insert(item.id(), item as Arc<dyn crate::core::item::AnyItem>);
                            }
                        }
                    }
                    hypha::MapDiff::Insert { value, .. }
                    | hypha::MapDiff::Update {
                        new_value: value, ..
                    } => {
                        let Some(item) = value.as_any().downcast_ref::<Q::Item>() else {
                            return;
                        };
                        let item = Arc::new(item.clone());
                        let id = item.id();
                        let include = Q::test_entity(QueryTestCtx {
                            item: item.clone(),
                            query: query.clone(),
                            query_context: query_ctx.clone(),
                        });
                        if include {
                            output.insert(id, item as Arc<dyn crate::core::item::AnyItem>);
                        } else {
                            output.remove(&id);
                        }
                    }
                    hypha::MapDiff::Remove { key, .. } => {
                        output.remove(key);
                    }
                    hypha::MapDiff::Batch { changes } => {
                        let mut current: HashMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>> =
                            output.entries().get().into_iter().collect();
                        let mut downstream_changes: Vec<
                            hypha::MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                        > = Vec::new();

                        fn apply_batch_change<Q>(
                            change: &hypha::MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                            current: &mut HashMap<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                            downstream_changes: &mut Vec<
                                hypha::MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
                            >,
                            query: &Arc<Q>,
                            query_ctx: &Arc<QueryContext>,
                        ) where
                            Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
                            Q::Item: DeserializeOwned
                                + Eventable
                                + WithId
                                + Clone
                                + std::fmt::Debug
                                + Send
                                + Sync
                                + 'static,
                        {
                            match change {
                                hypha::MapDiff::Initial { entries } => {
                                    let mut next: HashMap<
                                        Arc<str>,
                                        Arc<dyn crate::core::item::AnyItem>,
                                    > = HashMap::new();
                                    for (_id, item_any) in entries {
                                        let Some(item) =
                                            item_any.as_any().downcast_ref::<Q::Item>()
                                        else {
                                            continue;
                                        };
                                        let item = Arc::new(item.clone());
                                        let include = Q::test_entity(QueryTestCtx {
                                            item: item.clone(),
                                            query: query.clone(),
                                            query_context: query_ctx.clone(),
                                        });
                                        if include {
                                            next.insert(
                                                item.id(),
                                                item as Arc<dyn crate::core::item::AnyItem>,
                                            );
                                        }
                                    }
                                    let entries = next
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect::<Vec<_>>();
                                    *current = next;
                                    downstream_changes.push(hypha::MapDiff::Initial { entries });
                                }
                                hypha::MapDiff::Insert { value, .. }
                                | hypha::MapDiff::Update {
                                    new_value: value, ..
                                } => {
                                    let Some(item) = value.as_any().downcast_ref::<Q::Item>()
                                    else {
                                        return;
                                    };
                                    let item = Arc::new(item.clone());
                                    let id = item.id();
                                    let include = Q::test_entity(QueryTestCtx {
                                        item: item.clone(),
                                        query: query.clone(),
                                        query_context: query_ctx.clone(),
                                    });
                                    if include {
                                        let new_value = item as Arc<dyn crate::core::item::AnyItem>;
                                        if let Some(old_value) =
                                            current.insert(id.clone(), new_value.clone())
                                        {
                                            downstream_changes.push(hypha::MapDiff::Update {
                                                key: id,
                                                old_value,
                                                new_value,
                                            });
                                        } else {
                                            downstream_changes.push(hypha::MapDiff::Insert {
                                                key: id,
                                                value: new_value,
                                            });
                                        }
                                    } else if let Some(old_value) = current.remove(&id) {
                                        downstream_changes
                                            .push(hypha::MapDiff::Remove { key: id, old_value });
                                    }
                                }
                                hypha::MapDiff::Remove { key, .. } => {
                                    if let Some(old_value) = current.remove(key) {
                                        downstream_changes.push(hypha::MapDiff::Remove {
                                            key: key.clone(),
                                            old_value,
                                        });
                                    }
                                }
                                hypha::MapDiff::Batch { changes } => {
                                    for nested in changes {
                                        apply_batch_change::<Q>(
                                            nested,
                                            current,
                                            downstream_changes,
                                            query,
                                            query_ctx,
                                        );
                                    }
                                }
                            }
                        }

                        for change in changes {
                            apply_batch_change::<Q>(
                                change,
                                &mut current,
                                &mut downstream_changes,
                                query,
                                query_ctx,
                            );
                        }
                        if !downstream_changes.is_empty() {
                            output.apply_batch(downstream_changes);
                        }
                    }
                }
            }

            apply_diff::<Q>(
                diff,
                &output_for_diffs,
                &query_for_diffs,
                &query_ctx_for_diffs,
            );
        });
        output.own_guard(guard);

        Ok(output.lock())
    }
}
