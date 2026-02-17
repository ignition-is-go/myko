//! Minimal server context for query handlers.

use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use hypha::{Cell, CellImmutable, MapExt};

#[cfg(not(target_arch = "wasm32"))]
use super::{
    cell::FilteredCellMap, registration::QueryFactory, request::QueryRequest, traits::AnyQuery,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::core::report::{AnyReport, ReportFactory, ReportOutputType, ReportRequest};
use crate::request::RequestContext;
#[cfg(not(target_arch = "wasm32"))]
use crate::server::CellServerCtx;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::StoreRegistry;

/// Minimal server context provided to query handlers.
///
/// This is a lightweight context that provides queries access to:
/// - Server identity (`host_id`)
/// - Entity stores (`registry`)
///
/// For more capabilities (publishing, relationships), use `CellServerCtx`.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub req: Arc<RequestContext>,
}

/// Server-only context for advanced query composition.
///
/// Use this from `QueryHandler::build_cell` to compose query cells from other
/// queries while preserving request context (tx, host_id, lineage).
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct QueryCellContext {
    pub request_ctx: Arc<RequestContext>,
    pub query_context: Arc<QueryContext>,
    registry: Arc<StoreRegistry>,
    server_ctx: Option<Arc<CellServerCtx>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl QueryCellContext {
    pub fn new(
        request_ctx: Arc<RequestContext>,
        query_context: Arc<QueryContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Self {
        Self {
            request_ctx,
            query_context,
            registry,
            server_ctx,
        }
    }

    /// Build a reactive CellMap for another query using the same request context.
    pub fn query<Q>(&self, query: Q) -> Result<FilteredCellMap, String>
    where
        Q: QueryFactory + Clone,
        Q::Item: crate::core::item::Eventable
            + crate::common::with_id::WithId
            + serde::de::DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        let wrapped = QueryRequest::with_tx(query, self.request_ctx.tx.clone());
        let any_query: Arc<dyn AnyQuery> = Arc::new(wrapped);
        Q::cell_factory(
            any_query,
            self.registry.clone(),
            self.request_ctx.clone(),
            self.server_ctx.clone(),
        )
    }

    /// Build a reactive cell for a report using the same request context.
    pub fn report<R>(
        &self,
        report: R,
    ) -> Result<Cell<<R as ReportOutputType>::Output, CellImmutable>, String>
    where
        R: ReportFactory + Clone,
        <R as ReportOutputType>::Output:
            crate::common::to_value::ToValue + std::fmt::Debug + Send + Sync + 'static,
    {
        let Some(server_ctx) = self.server_ctx.clone() else {
            return Err("QueryCellContext.report requires server context".to_string());
        };

        let wrapped = ReportRequest::with_tx(report, self.request_ctx.tx.clone());
        let any_report: Arc<dyn AnyReport> = Arc::new(wrapped);
        let erased = R::cell_factory(any_report, self.request_ctx.clone(), server_ctx)
            .map_err(|e| e.to_string())?;
        Ok(erased.map(|output| {
            output
                .as_ref()
                .as_any()
                .downcast_ref::<<R as ReportOutputType>::Output>()
                .expect("Report output downcast should match ReportFactory type")
                .clone()
        }))
    }

    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }
}
