use std::sync::Arc;

use hypha::{Cell, CellImmutable, MapExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::common::to_value::ToValue;
use crate::context::RequestContext;
use crate::query::QueryParams;
use crate::store::StoreRegistry;

/// Context provided to report handlers for accessing dependencies.
///
/// ReportContext allows handlers to:
/// - Subscribe to queries and get reactive streams
/// - Access the report arguments via `report_args`
/// - Access request context (tx, client_id, lineage, host_id)
#[derive(Clone)]
pub struct ReportContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: RequestContext,
    /// Store registry for queries
    pub registry: Arc<StoreRegistry>,
    /// The report arguments as a JSON Value - handlers should parse this to their Args type
    pub report_args: Value,
}

impl ReportContext {
    /// Create a new ReportContext.
    pub fn new(req: RequestContext, registry: Arc<StoreRegistry>, report_args: Value) -> Self {
        Self {
            req,
            registry,
            report_args,
        }
    }

    /// Create a minimal ReportContext for cell-based server.
    /// Uses a default RequestContext with generated tx.
    pub fn minimal(registry: Arc<StoreRegistry>) -> Self {
        Self {
            req: RequestContext {
                tx: Uuid::new_v4().to_string().into(),
                client_id: None,
                lineage: vec![],
                host_id: Uuid::nil(),
                created_at: chrono::Utc::now().to_rfc3339(),
                windback: None,
            },
            registry,
            report_args: Value::Null,
        }
    }

    /// Create a ReportContext with a specific host_id.
    pub fn with_host_id(registry: Arc<StoreRegistry>, host_id: Uuid) -> Self {
        Self {
            req: RequestContext {
                tx: Uuid::new_v4().to_string().into(),
                client_id: None,
                lineage: vec![],
                host_id,
                created_at: chrono::Utc::now().to_rfc3339(),
                windback: None,
            },
            registry,
            report_args: Value::Null,
        }
    }

    /// Create a ReportContext with just registry and args.
    pub fn with_args(registry: Arc<StoreRegistry>, report_args: Value) -> Self {
        Self {
            req: RequestContext {
                tx: Uuid::new_v4().to_string().into(),
                client_id: None,
                lineage: vec![],
                host_id: Uuid::nil(),
                created_at: chrono::Utc::now().to_rfc3339(),
                windback: None,
            },
            registry,
            report_args,
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience accessors for request context
    // ─────────────────────────────────────────────────────────────────────────

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

    /// Parse the report arguments to the expected Args type.
    ///
    /// This is a convenience method to deserialize the report_args Value
    /// to the specific Args struct for the report.
    pub fn args<A: serde::de::DeserializeOwned>(&self) -> Result<A, serde_json::Error> {
        serde_json::from_value(self.report_args.clone())
    }

    /// Subscribe to a query dependency.
    ///
    /// Returns a cell that updates whenever the query results change.
    /// Uses cell-based reactive queries.
    ///
    /// Accepts bare query params (e.g., `GetServersByIds { ids: vec![...] }`)
    pub fn query<Q>(&self, _query: Q) -> Cell<Vec<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let query_item_type = Q::query_item_type_static();
        let store = self.registry.get_or_create(&query_item_type.to_string());

        // Get the entries cell and map to typed items
        store.entries().map(move |items| {
            items
                .iter()
                .filter_map(|(_, item)| item.as_any().downcast_ref::<Q::Item>().cloned())
                .collect()
        })
    }

    /// Get the store registry for direct access.
    pub fn store_registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }

    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Note: Full-text search requires tantivy integration (not yet implemented in cell-based server).
    pub fn search(&self, _entity_type: &str, _query: &str, _limit: usize) -> Vec<Arc<str>> {
        // Search not yet implemented in cell-based server
        vec![]
    }

    /// Subscribe to a sub-report dependency.
    ///
    /// Returns a cell that updates whenever the sub-report output changes.
    /// This allows reports to compose other reports.
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + Clone + 'static,
    {
        // Create a nested context - sub-report args are accessed via &self in compute
        let nested_ctx = ReportContext::minimal(self.registry.clone());

        // Compute the sub-report
        report.compute(nested_ctx)
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
