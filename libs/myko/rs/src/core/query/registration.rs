//! Query registration via inventory.

use std::{
    any::Any,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use hypha::{JoinExt, MapExt, Signal, SubscriptionGuard, Watchable};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    super::item::Eventable,
    cell::FilteredCellMap,
    context::{QueryCellContext, QueryContext},
    request::QueryRequest,
    traits::{AnyQuery, QueryParams, QueryTestCellCtx},
};
use crate::{
    common::with_id::WithId, request::RequestContext, server::CellServerCtx, store::StoreRegistry,
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

fn query_factories_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_FACTORIES_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_created_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_CREATED_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_removed_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_REMOVED_BY_ID.get_or_init(DashMap::new)
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
        increment_counter(query_factories_by_id(), query_id.clone());

        // Downcast to the QueryRequest wrapper
        let any_ref: &dyn Any = any_query.as_ref();
        let request: QueryRequest<Q> = any_ref
            .downcast_ref::<QueryRequest<Q>>()
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Failed to downcast query to QueryRequest<{}>",
                    Q::query_id_static()
                )
            })?;

        // Extract the inner query params
        let query: Arc<Q> = Arc::new(request.query);

        // Get the store for this entity type
        let store = registry.get_or_create(&Q::query_item_type_static());

        log::trace!(
            "Creating query cell for {} with host_id={}",
            Q::query_id_static(),
            request_ctx.host_id
        );

        // Create server context with actual host_id for test_entity
        let query_ctx = Arc::new(QueryContext {
            req: request_ctx.clone(),
        });

        let output = hypha::CellMap::<Arc<str>, Arc<dyn crate::core::item::AnyItem>>::new();
        let output_for_diffs = output.clone();
        let output_for_ensure = output.clone();
        let store_for_diffs = store.clone();
        let store_for_ensure = store.clone();
        let query_cell_ctx = QueryCellContext::new(
            request_ctx.clone(),
            query_ctx.clone(),
            registry.clone(),
            server_ctx,
        );

        let per_item_guards = Arc::new(DashMap::<Arc<str>, SubscriptionGuard>::new());
        let per_item_guards_for_diffs = per_item_guards.clone();
        let query_id_for_ensure = query_id.clone();
        let query_id_for_diffs = query_id.clone();

        let ensure_watch = move |id: Arc<str>| {
            if per_item_guards.contains_key(&id) {
                return;
            }

            let item_cell = store_for_ensure.get(&id).map(|item_opt| {
                item_opt.as_ref().and_then(|item| {
                    item.as_any()
                        .downcast_ref::<Q::Item>()
                        .map(|typed| Arc::new(typed.clone()))
                })
            });

            let include_cell = Q::test_entity(QueryTestCellCtx {
                item: item_cell.clone(),
                query: query.clone(),
                query_context: query_cell_ctx.clone(),
            });

            let visible_item_cell = include_cell
                .join(&item_cell)
                .map(|(include, item_opt)| if *include { item_opt.clone() } else { None });

            let output_for_item = output_for_ensure.downgrade();
            let id_for_item = id.clone();
            let guard = visible_item_cell.subscribe(move |signal| {
                let Some(output) = output_for_item.upgrade() else {
                    return;
                };
                if let Signal::Value(item_opt) = signal {
                    if let Some(item) = item_opt.as_ref() {
                        output.insert(id_for_item.clone(), item.clone());
                    } else {
                        output.remove(&id_for_item);
                    }
                }
            });

            per_item_guards.insert(id, guard);
            QUERY_PER_ITEM_GUARDS_CREATED.fetch_add(1, Ordering::Relaxed);
            increment_counter(query_guards_created_by_id(), query_id_for_ensure.clone());
        };

        let guard = store_for_diffs.subscribe_diffs(move |diff| match diff {
            hypha::MapDiff::Initial { entries } => {
                for (id, _) in entries {
                    ensure_watch(id.clone());
                }
            }
            hypha::MapDiff::Insert { key, .. } | hypha::MapDiff::Update { key, .. } => {
                ensure_watch(key.clone());
            }
            hypha::MapDiff::Remove { key, .. } => {
                if per_item_guards_for_diffs.remove(key).is_some() {
                    QUERY_PER_ITEM_GUARDS_REMOVED.fetch_add(1, Ordering::Relaxed);
                    increment_counter(query_guards_removed_by_id(), query_id_for_diffs.clone());
                }
                output_for_diffs.remove(key);
            }
        });
        output.own_guard(guard);

        Ok(output.lock())
    }
}
