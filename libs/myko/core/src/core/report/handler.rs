use std::sync::Arc;

use hyphae::MaterializeDefinite;
#[cfg(target_arch = "wasm32")]
use hyphae::Cell;
use serde::{Serialize, de::DeserializeOwned};

use crate::core::capability::{
    Querying, RegistryScoped, Reporting, RequestScoped, Searching, ServerScoped,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::core::capability::{PeerAccess, Replaying, Viewing};
#[cfg(not(target_arch = "wasm32"))]
use crate::server::MykoServerCtx;
use crate::{common::to_value::ToValue, request::RequestContext, store::StoreRegistry};

/// Context provided to report handlers for accessing dependencies.
///
/// ReportContext allows handlers to:
/// - Subscribe to queries and get reactive streams
/// - Access the report arguments directly via `&self` fields (e.g. `self.target_id`)
/// - Access request context (tx, client_id, lineage, host_id)
#[derive(Clone)]
pub struct ReportContext {
    /// Request context with tracing information (tx, client_id, lineage, host_id).
    pub req: Arc<RequestContext>,
    /// Store registry for reactive entity lookups (available on all targets).
    /// Named `store_registry` so it doesn't shadow the `registry()` method
    /// from [`RegistryScoped`].
    pub(crate) store_registry: Arc<StoreRegistry>,
    #[cfg(not(target_arch = "wasm32"))]
    server_ctx: Arc<MykoServerCtx>,
}

impl ReportContext {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(req: Arc<RequestContext>, server_ctx: Arc<MykoServerCtx>) -> Self {
        let store_registry = server_ctx.registry.clone();
        Self {
            req,
            store_registry,
            server_ctx,
        }
    }
}

// The report handler's scope: reactive queries, sub-reports, views, search,
// federation, and history replay — but NOT command emission. A report cannot
// mutate state, and that is enforced structurally by not implementing
// `EventPublishing` (see `core::capability`): `ctx.emit_set(...)` in a report is a
// compile error, not a convention.
impl crate::core::capability::sealed::Sealed for ReportContext {}
impl RequestScoped for ReportContext {
    fn __request(&self) -> &Arc<RequestContext> {
        &self.req
    }
}
impl RegistryScoped for ReportContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.store_registry
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl ServerScoped for ReportContext {
    fn __server_ctx(&self) -> &Arc<MykoServerCtx> {
        &self.server_ctx
    }
}
#[cfg(target_arch = "wasm32")]
impl ServerScoped for ReportContext {}

// Cross-platform: authored once, compiled for wasm too (where the bodies are
// `unreachable!` — reports only run server-side).
impl Querying for ReportContext {}
impl Searching for ReportContext {}
impl Reporting for ReportContext {}

// Native-only (server-only return types).
#[cfg(not(target_arch = "wasm32"))]
impl Viewing for ReportContext {}
#[cfg(not(target_arch = "wasm32"))]
impl PeerAccess for ReportContext {}
#[cfg(not(target_arch = "wasm32"))]
impl Replaying for ReportContext {}

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
///   fn compute(
///     &self,
///     ctx: ReportContext,
///   ) -> impl MaterializeDefinite<Arc<Self::Output>> {
///     ctx.query(GetTargetsByQuery { active: Some(true), ..Default::default() })
///       .map(|items| Arc::new(ActiveTargetCount { count: items.len() }))
///   }
/// }
/// ```
pub trait ReportHandler: Sized {
    type Output: Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + ToValue
        + 'static;

    /// Compute the report output as a reactive pipeline.
    ///
    /// This method is called once when the report is first subscribed to.
    /// The returned pipeline automatically updates whenever dependencies change.
    ///
    /// Report arguments are parsed by the framework and passed as `&self`,
    /// so fields are directly accessible (e.g., `self.target_id`).
    ///
    /// # Returning a `MaterializeDefinite` pipeline (not a `Cell`)
    ///
    /// `compute` returns `impl MaterializeDefinite<Arc<Output>>` rather than a
    /// concrete `Cell`, so reports can chain `.map(...)`, `.tap(...)`, etc. on
    /// hyphae's lazy operators without materializing an intermediate cell.
    /// `MaterializeDefinite` is the bound for pipelines that have a known
    /// initial value (definite seedness) and can be compiled into a `Cell`
    /// via `.materialize()`. The framework type-erases the output and
    /// materializes once at the registration boundary, so each report
    /// incurs at most one cell allocation regardless of the chain depth of
    /// `ctx.report(...)` calls.
    ///
    /// Concrete `Cell<U>` values produced by `ctx.query_map()`, `switch_map`,
    /// `deduped`, etc. already implement `MaterializeDefinite<U>`, so
    /// returning them directly is fine.
    #[cfg(not(target_arch = "wasm32"))]
    fn compute(&self, ctx: ReportContext) -> impl MaterializeDefinite<Arc<Self::Output>>;

    /// Compute the report output (wasm no-op).
    ///
    /// Report computation only runs server-side; on wasm32 the hand-written
    /// native body is gated out and this no-op default applies. It is never
    /// invoked on wasm (clients receive report values over the wire), so it
    /// only has to type-check. A `Cell<Arc<Self::Output>>` implements
    /// `MaterializeDefinite<Arc<Self::Output>>`; the seed value is produced by
    /// `unreachable!()`, which diverges and coerces to any `Output` (no
    /// `Default` bound required).
    #[cfg(target_arch = "wasm32")]
    fn compute(&self, _ctx: ReportContext) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let _seed: Arc<Self::Output> =
            unreachable!("ReportHandler::compute is not available on wasm32");
        #[allow(unreachable_code)]
        Cell::new(_seed)
    }
}
