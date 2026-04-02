use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap, CellValue};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use crate::client::{ConnectionStatus, MykoClient};
#[cfg(not(target_arch = "wasm32"))]
use crate::core::view::{TypedViewCellMap, ViewFactory};
#[cfg(not(target_arch = "wasm32"))]
use crate::item::typed_map_from_any_item_with_typed_id;
#[cfg(not(target_arch = "wasm32"))]
use crate::query::FilteredCellMap;
#[cfg(not(target_arch = "wasm32"))]
use crate::server::CellServerCtx;
#[cfg(not(target_arch = "wasm32"))]
use crate::server::PersistHealth;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::StoreRegistry;
use crate::{
    cache::CacheKey,
    common::{
        to_value::ToValue,
        with_id::{WithId, WithTypedId},
    },
    core::item::Eventable,
    query::QueryParams,
    report::ReportId,
    request::RequestContext,
};

/// Context provided to report handlers for accessing dependencies.
///
/// ReportContext allows handlers to:
/// - Subscribe to queries and get reactive streams
/// - Access the report arguments via `report_args`
/// - Access request context (tx, client_id, lineage, host_id)
#[derive(Clone)]
pub struct ReportContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: Arc<RequestContext>,
    #[cfg(not(target_arch = "wasm32"))]
    server_ctx: Arc<CellServerCtx>,
}

impl ReportContext {
    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors for request context
    // ─────────────────────────────────────────────────────────────────────────

    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(req: Arc<RequestContext>, server_ctx: Arc<CellServerCtx>) -> Self {
        Self { req, server_ctx }
    }

    /// Get the transaction ID.
    pub fn tx(&self) -> &str {
        &self.req.tx
    }

    /// Get the client ID if present.
    pub fn client_id(&self) -> Option<&str> {
        self.req.client_id.as_deref()
    }

    /// Get the host ID.
    pub fn host_id(&self) -> Uuid {
        self.req.host_id
    }

    /// Get the lineage (call chain).
    pub fn lineage(&self) -> &[Arc<str>] {
        &self.req.lineage
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Report-specific methods
    // ─────────────────────────────────────────────────────────────────────────

    #[cfg(not(target_arch = "wasm32"))]
    /// Subscribe to a query dependency and get a typed reactive CellMap keyed
    /// by the item's typed id.
    ///
    /// Use this when you need incremental `MapDiff` semantics with row-level
    /// granularity, while preserving the query's concrete item type.
    pub fn query_map<Q>(
        &self,
        query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + WithTypedId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        typed_map_from_any_item_with_typed_id(
            self.server_ctx.query_map_untyped(query, self.req.clone()),
            "ReportContext::query_map",
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn query_map<Q>(
        &self,
        query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + WithTypedId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        let _ = query;
        unreachable!("ReportContext::query_map is not available on wasm32");
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Subscribe to a query dependency and get a typed reactive CellMap keyed
    /// by canonical `Arc<str>` ids.
    ///
    /// Prefer `query_map()` unless you specifically need string ids.
    pub fn query_map_by_str<Q>(&self, query: Q) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        self.server_ctx.query_map(query, self.req.clone())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn query_map_by_str<Q>(&self, query: Q) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + CellValue
            + 'static,
    {
        let _ = query;
        unreachable!("ReportContext::query_map_by_str is not available on wasm32");
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Subscribe to a query dependency and get an untyped reactive CellMap.
    ///
    /// Prefer `query_map()` unless you explicitly need erased `AnyItem`.
    pub fn query_map_untyped<Q>(&self, query: Q) -> FilteredCellMap
    where
        Q: QueryParams + 'static,
        Q::Item:
            Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.query_map_untyped(query, self.req.clone())
    }

    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Uses tantivy full-text search on fields marked with `#[searchable]`.
    pub fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.server_ctx
                .search_index()
                .search(entity_type, query, limit)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (entity_type, query, limit);
            unreachable!();
        }
    }

    /// Subscribe to a sub-report dependency.
    ///
    /// Returns a cell that updates whenever the sub-report output changes.
    /// This allows reports to compose other reports.
    ///
    /// Forwards to `CellServerCtx::report()` which wraps the compute result
    /// in a named relay for inspector visibility.
    pub fn report<R>(&self, report: R) -> Cell<Arc<R::Output>, CellImmutable>
    where
        R: ReportHandler + ReportId + CacheKey + Clone + serde::Serialize + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.server_ctx.report(report, self.req.clone())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = report;
            unreachable!();
        }
    }

    /// Subscribe to a view dependency and get a typed reactive CellMap.
    ///
    /// This lets reports reuse incremental, map-native view logic instead of
    /// rebuilding equivalent query/join pipelines locally.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn view<V>(&self, view: V) -> TypedViewCellMap<V::Item>
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.view(view, self.req.clone())
    }

    /// Get the live peer client for a peer server id, if present.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn peer_client(&self, peer_id: &str) -> Option<Arc<MykoClient>> {
        self.server_ctx.peer_client(peer_id)
    }

    /// Get the current connection status for a peer server id, if a peer client exists.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn peer_connection_status(&self, peer_id: &str) -> Option<ConnectionStatus> {
        self.server_ctx.peer_connection_status(peer_id)
    }

    /// Reactive tick that updates when peer client membership changes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn peer_clients_tick(&self) -> Cell<u64, CellImmutable> {
        self.server_ctx.peer_clients_tick()
    }

    /// Get the live persist health counters (queued, errors, throughput).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist_health(&self) -> Arc<PersistHealth> {
        self.server_ctx.persist_health()
    }

    /// Access the store registry for type-erased entity lookups.
    ///
    /// This enables reports that need to traverse entities by runtime-determined
    /// type names (e.g., entity tree export walking relationship graphs).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.server_ctx.registry.clone()
    }

    /// Replay historical events into a temporary StoreRegistry.
    ///
    /// Returns an error if no HistoryReplayProvider is configured.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn replay_store(&self, until: &str) -> Result<Arc<StoreRegistry>, String> {
        let provider = self
            .server_ctx
            .history_replay()
            .ok_or_else(|| "No history replay provider configured".to_string())?;
        provider.replay_to_store(until, &self.server_ctx.handler_registry)
    }
}

/// Trait for report handlers - defines how a report computes its output.
///
/// Unlike queries which filter existing items, reports can:
/// - Aggregate data from multiple queries
/// - Transform and combine data
/// - Depend on other reports
///
/// # Reactivity
///
/// The `compute` method returns a Cell, not a single value. This cell
/// automatically updates whenever any dependency changes.
///
/// # Argument Parsing
///
/// Report arguments are parsed by the framework before `compute` is called,
/// and passed as `&self`. Fields are directly accessible (e.g., `self.target_id`).
///
/// # Example
///
/// ```text
/// // Reports are for derived/read-model data.
/// // Pattern:
/// // 1) Define params (or an empty struct for no params)
/// // 2) Implement ReportHandler::compute
/// // 3) Use ctx.query(...) / ctx.report(...) to compose dependencies
///
/// #[myko_report_output]
/// pub struct ActiveTargetCount {
///   pub count: usize,
/// }
///
/// #[myko_report(ActiveTargetCount)]
/// pub struct GetActiveTargetCount;
///
/// impl ReportHandler for GetActiveTargetCount {
///   type Output = ActiveTargetCount;
///
///   fn compute(&self, ctx: ReportContext) -> Cell<Arc<Self::Output>, CellImmutable> {
///     ctx.query(GetTargetsByQuery { active: Some(true), ..Default::default() })
///       .map(|items| Arc::new(ActiveTargetCount { count: items.len() }))
///   }
/// }
/// ```
pub trait ReportHandler: Sized + Send + Sync + 'static {
    type Output: Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + ToValue
        + 'static;

    /// Compute the report output as a shared reactive cell.
    ///
    /// This method is called once when the report is first subscribed to.
    /// The returned cell automatically updates whenever dependencies change.
    ///
    /// Report arguments are parsed by the framework and passed as `&self`,
    /// so fields are directly accessible (e.g., `self.target_id`).
    fn compute(&self, ctx: ReportContext) -> Cell<Arc<Self::Output>, CellImmutable>;
}
