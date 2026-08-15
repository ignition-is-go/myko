//! Minimal server context for query handlers.

use std::sync::Arc;

use hyphae::{Cell, CellImmutable};

use super::{
    cell::FilteredCellMap, registration::QueryFactory, request::QueryRequest, traits::AnyQuery,
};
use crate::{
    core::report::{ReportFactory, ReportHandler, ReportOutputType},
    request::RequestContext,
    server::MykoServerContext,
    store::StoreRegistry,
};

/// Minimal server context provided to query handlers.
///
/// This is a lightweight context that provides queries access to:
/// - Server identity (`host_id`)
/// - Entity stores (`registry`)
///
/// For more capabilities (publishing, relationships), use `MykoServerContext`.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub req: Arc<RequestContext>,
}

impl crate::core::capability::sealed::Sealed for QueryContext {}
impl crate::core::capability::RequestScoped for QueryContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.req
    }
}

/// Server-only context for advanced query composition.
///
/// Use this from `QueryHandler::build_view` to compose query cells from other
/// queries while preserving request context (tx, `host_id`, lineage).
#[derive(Clone)]
pub struct QueryBuildContext {
    pub query_context: Arc<QueryContext>,
    registry: Arc<StoreRegistry>,
    server_ctx: Option<Arc<MykoServerContext>>,
}

impl QueryBuildContext {
    #[must_use]
    pub const fn new(
        query_context: Arc<QueryContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Option<Arc<MykoServerContext>>,
    ) -> Self {
        Self {
            query_context,
            registry,
            server_ctx,
        }
    }

    /// Build a reactive `CellMap` for another query using the same request context.
    ///
    /// Delegates to `MykoServerContext::query_map_untyped`, which is the canonical
    /// cached path. A previous local `subquery_cache` was removed so that
    /// dedupe lives in exactly one place (the server context).
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
        if let Some(server_ctx) = self.server_ctx.clone() {
            return Ok(server_ctx.query_map_untyped(query, self.query_context.req.clone()));
        }

        // Fallback for test/wasm contexts that don't carry a MykoServerContext.
        // Builds the cell directly via the type's cell factory.
        let wrapped = QueryRequest::with_tx(query, self.query_context.req.tx.clone());
        let any_query: Arc<dyn AnyQuery> = Arc::new(wrapped);
        Q::cell_factory(
            any_query,
            self.registry.clone(),
            self.query_context.req.clone(),
            self.server_ctx.clone(),
        )
    }

    /// Build a reactive cell for a report using the same request context.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn report<R>(
        &self,
        report: R,
    ) -> Result<Cell<Arc<<R as ReportOutputType>::Output>, CellImmutable>, String>
    where
        R: ReportFactory + ReportHandler<Output = <R as ReportOutputType>::Output> + Clone,
        <R as ReportOutputType>::Output:
            crate::common::to_value::ToValue + std::fmt::Debug + Send + Sync + 'static,
    {
        let Some(server_ctx) = self.server_ctx.clone() else {
            return Err("QueryBuildContext.report requires server context".to_string());
        };

        Ok(server_ctx.report(report, self.query_context.req.clone()))
    }

    #[must_use]
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    /// Build an index-seeded graph watch for generated edge queries.
    #[doc(hidden)]
    pub fn graph_watch_at<E>(
        &self,
        position: crate::graph::EndPosition,
        endpoint: &crate::graph::EndpointValue,
    ) -> Result<FilteredCellMap, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let server = self
            .server_ctx
            .as_ref()
            .ok_or_else(|| "graph query requires server context".to_string())?;
        let graph = server
            .graph_index()
            .ok_or_else(|| "application has no graph registrations".to_string())?;
        graph
            .watch_at(E::ENTITY_NAME_STATIC, position, endpoint)
            .map_err(|error| error.to_string())
    }

    /// Build a routed live view of one concrete entity type reached through
    /// matching graph edges.
    #[doc(hidden)]
    pub fn graph_related_at<E, T>(
        &self,
        edge_position: crate::graph::EndPosition,
        endpoint: &crate::graph::EndpointValue,
        related_position: crate::graph::EndPosition,
    ) -> Result<FilteredCellMap, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
        T: crate::graph::EntityEndpointSpec,
    {
        let edges = self.graph_watch_at::<E>(edge_position, endpoint)?;
        let edges = crate::item::typed_map_arc_from_any_item::<E>(
            edges,
            "QueryBuildContext::graph_related_at",
        );
        Ok(crate::graph::graph_related_entity_watch::<E, T>(
            &edges,
            self.registry.as_ref(),
            related_position,
        ))
    }

    /// Build a routed live view of the distinct entities adjacent through an
    /// undirected edge type.
    #[doc(hidden)]
    pub fn graph_neighbors_at<E, T>(
        &self,
        endpoint: &crate::graph::EndpointValue,
    ) -> Result<FilteredCellMap, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
        T: crate::graph::EntityEndpointSpec,
    {
        let server = self
            .server_ctx
            .as_ref()
            .ok_or_else(|| "graph query requires server context".to_string())?;
        let graph = server
            .graph_index()
            .ok_or_else(|| "application has no graph registrations".to_string())?;
        let edges = graph
            .watch_incident(E::ENTITY_NAME_STATIC, endpoint)
            .map_err(|error| error.to_string())?;
        let edges = crate::item::typed_map_arc_from_any_item::<E>(
            edges,
            "QueryBuildContext::graph_neighbors_at",
        );
        Ok(crate::graph::graph_neighbor_entity_watch::<E, T>(
            &edges,
            self.registry.as_ref(),
            endpoint,
        ))
    }

    /// Build an index-seeded exact-pair graph watch for generated edge queries.
    #[doc(hidden)]
    pub fn graph_watch_between<E>(
        &self,
        a: &crate::graph::EndpointValue,
        b: &crate::graph::EndpointValue,
    ) -> Result<FilteredCellMap, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let server = self
            .server_ctx
            .as_ref()
            .ok_or_else(|| "graph query requires server context".to_string())?;
        let graph = server
            .graph_index()
            .ok_or_else(|| "application has no graph registrations".to_string())?;
        graph
            .watch_between(E::ENTITY_NAME_STATIC, a, b)
            .map_err(|error| error.to_string())
    }

    /// Build a bounded, index-backed graph watch without materializing the
    /// complete matching edge set in the WebSocket session.
    #[doc(hidden)]
    pub fn graph_window_at<E>(
        &self,
        position: crate::graph::EndPosition,
        endpoint: &crate::graph::EndpointValue,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<super::WindowedQuerySource>, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let server = self
            .server_ctx
            .as_ref()
            .ok_or_else(|| "graph query requires server context".to_string())?;
        let graph = server
            .graph_index()
            .ok_or_else(|| "application has no graph registrations".to_string())?;
        graph
            .watch_window_at(E::ENTITY_NAME_STATIC, position, endpoint, window)
            .map_err(|error| error.to_string())
    }

    /// Build a bounded, index-backed exact-pair graph watch.
    #[doc(hidden)]
    pub fn graph_window_between<E>(
        &self,
        a: &crate::graph::EndpointValue,
        b: &crate::graph::EndpointValue,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<super::WindowedQuerySource>, String>
    where
        E: crate::graph::GraphEdge,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        let server = self
            .server_ctx
            .as_ref()
            .ok_or_else(|| "graph query requires server context".to_string())?;
        let graph = server
            .graph_index()
            .ok_or_else(|| "application has no graph registrations".to_string())?;
        graph
            .watch_window_between(E::ENTITY_NAME_STATIC, a, b, window)
            .map_err(|error| error.to_string())
    }
}

// Query build keeps its own `query`/`report` (fallible over an optional
// server context) and `registry` inherent — they don't fit the infallible
// capability traits — but adopts RequestScoped for tx/host_id/lineage.
impl crate::core::capability::sealed::Sealed for QueryBuildContext {}
impl crate::core::capability::RequestScoped for QueryBuildContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.query_context.req
    }
}
