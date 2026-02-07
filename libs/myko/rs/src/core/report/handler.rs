use std::sync::Arc;

use hypha::{Cell, CellImmutable};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    common::to_value::ToValue, query::QueryParams, request::RequestContext, server::CellServerCtx,
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
    server_ctx: Arc<CellServerCtx>,
}

impl ReportContext {
    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors for request context
    // ─────────────────────────────────────────────────────────────────────────

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

    /// Subscribe to a query dependency.
    ///
    /// Returns a cell that updates whenever the query results change.
    /// Uses cell-based reactive queries.
    ///
    /// Accepts bare query params (e.g., `GetServersByIds { ids: vec![...] }`)
    pub fn query<Q>(&self, query: Q) -> Cell<Vec<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        self.server_ctx.query(query, self.req.clone())
    }

    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Uses tantivy full-text search on fields marked with `#[searchable]`.
    pub fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        self.server_ctx.search_index().search(entity_type, query, limit)
    }

    /// Subscribe to a sub-report dependency.
    ///
    /// Returns a cell that updates whenever the sub-report output changes.
    /// This allows reports to compose other reports.
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + Clone + 'static,
    {
        // Compute the sub-report
        report.compute(self.clone())
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
/// ```ignore
/// impl ReportHandler for CountActiveTargets {
///     type Output = usize;
///
///     fn compute(&self, ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
///         ctx.query(GetTargetsByQuery(PartialTarget { active: Some(true), ..Default::default() }))
///             .map(|targets| targets.len())
///     }
/// }
/// ```
pub trait ReportHandler: Sized + Send + Sync + 'static {
    type Output: Serialize + DeserializeOwned + Clone + Send + Sync + ToValue + 'static;

    /// Compute the report output as a reactive cell.
    ///
    /// This method is called once when the report is first subscribed to.
    /// The returned cell automatically updates whenever dependencies change.
    ///
    /// Report arguments are parsed by the framework and passed as `&self`,
    /// so fields are directly accessible (e.g., `self.target_id`).
    fn compute(&self, ctx: ReportContext) -> Cell<Self::Output, CellImmutable>;
}
