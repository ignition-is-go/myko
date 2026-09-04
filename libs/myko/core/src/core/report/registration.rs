//! Report registration via inventory.

use std::{any::Any, sync::Arc};

use hyphae::{Cell, CellImmutable, MapExt, Materialize};
use serde_json::Value;

use super::{
    request::ReportRequest,
    traits::{AnyReport, ReportParams},
};
use crate::{common::to_value::ToValue, request::RequestContext, server::MykoServerContext};

// ─────────────────────────────────────────────────────────────────────────────
// AnyOutput - Type-erased output for the WebSocket layer
// ─────────────────────────────────────────────────────────────────────────────

/// Type-erased report output trait.
/// Report outputs implement this to enable serialization at the WebSocket layer.
pub trait AnyOutput: ToValue + std::fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn equals(&self, other: &dyn AnyOutput) -> bool;
}

/// Blanket implementation for any type that satisfies the bounds.
impl<T: ToValue + std::fmt::Debug + PartialEq + Send + Sync + 'static> AnyOutput for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn AnyOutput) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|typed| self == typed)
    }
}

impl PartialEq for dyn AnyOutput {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Type alias for report parse function.
pub type ReportParseFn = fn(Value) -> Result<Arc<dyn AnyReport>, anyhow::Error>;

#[cfg(not(target_arch = "wasm32"))]
pub type ReportAuthorityFactory =
    fn(Value, myko_federation::NodeId) -> Result<crate::server::HandlerAuthority, String>;

/// Type-erased cell factory for reports.
/// Takes a typed report, registry, and `host_id`, returns a cell of type-erased output.
pub type ReportCellFactory = fn(
    Arc<dyn AnyReport>,
    Arc<RequestContext>,
    Arc<MykoServerContext>,
    Option<crate::server::federated_source::FederatedRequest>,
) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String>;

// ─────────────────────────────────────────────────────────────────────────────
// ReportRegistration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(ReportRegistration);

/// Registration entry for a report type.
/// Collected via inventory for automatic discovery.
pub struct ReportRegistration {
    /// Report identifier (e.g., "`ServerStats`")
    pub report_id: &'static str,
    /// Typed service owner used by application activation.
    pub service_id: Option<crate::ServiceTypeId>,
    /// Crate where this report is defined (for `type_gen` filtering)
    pub crate_name: &'static str,
    /// Output type name (e.g., "`ServerStatsOutput`")
    pub output_type: &'static str,
    /// Crate where the output type is defined
    pub output_type_crate: &'static str,
    /// Parse function for deserializing report from JSON
    pub parse: ReportParseFn,
    /// Factory for creating reactive cell from report
    pub cell_factory: ReportCellFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: ReportAuthorityFactory,
    /// Report struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// Report struct's doc comment, if any.
    pub description: Option<&'static str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// ReportFactory - Static methods for report types
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating report registration data.
///
/// This trait has a blanket implementation for all types implementing `ReportParams`,
/// so user-defined reports automatically get `parse` and `cell_factory` methods.
pub trait ReportFactory: ReportParams {
    /// Parse JSON into this report type.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn parse(value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error>;

    /// Create a reactive cell for this report.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn cell_factory(
        report: Arc<dyn AnyReport>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
    ) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String>;

    #[cfg(not(target_arch = "wasm32"))]
    /// Resolve typed source, scope, claims, and capabilities before opening.
    ///
    /// # Errors
    ///
    /// Returns an error when the serialized report parameters are invalid.
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String>;
}

impl<R: ReportParams> ReportFactory for R {
    #[cfg(not(target_arch = "wasm32"))]
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String> {
        let report: R = serde_json::from_value(value).map_err(|error| error.to_string())?;
        Ok(crate::server::HandlerAuthority {
            source_node: report.source_node(local_node),
            scope_id: report.scope_id(local_node),
            resource_claims: report.authority_claims(local_node),
            application_capabilities: report.required_capabilities(),
        })
    }

    fn parse(value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error> {
        let report = serde_json::from_value::<ReportRequest<R>>(value)?;
        Ok(Arc::new(report))
    }

    fn cell_factory(
        any_report: Arc<dyn AnyReport>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
    ) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String> {
        // Downcast to the ReportRequest wrapper
        let any_ref: &dyn Any = any_report.as_ref();
        let request: ReportRequest<R> = crate::common::downcast::downcast_request(
            any_ref,
            &format!("report to ReportRequest<{}>", R::report_id_static()),
        )?;

        let report_id = R::report_id_static();

        // Route through the canonical cached path so WS / QueryContext callers
        // share the same cached cell as internal sub-report subscribers (those
        // that go through `ReportContext::report`). Previously this called
        // `<R as ReportHandler>::compute()` directly, bypassing the cache and
        // producing a fresh cell graph for every WS subscribe.
        let cell = server_ctx.report_routed(
            request.report,
            request_ctx,
            #[cfg(not(target_arch = "wasm32"))]
            federated,
        );

        // Map to type-erased output for the WS/report subscription layer.
        let report_name = format!("report:{report_id}");
        Ok(cell
            .map(|output| -> Arc<dyn AnyOutput> { output.clone() })
            .materialize()
            .with_name(report_name.as_str()))
    }
}
