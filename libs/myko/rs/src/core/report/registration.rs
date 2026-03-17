//! Report registration via inventory.

use std::{any::Any, sync::Arc};

use hypha::{Cell, CellImmutable, MapExt};
use serde_json::Value;

use super::{
    handler::{ReportContext, ReportHandler},
    request::ReportRequest,
    traits::{AnyReport, ReportParams},
};
use crate::{common::to_value::ToValue, request::RequestContext, server::CellServerCtx};

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
            .map(|typed| self == typed)
            .unwrap_or(false)
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

/// Type-erased cell factory for reports.
/// Takes a typed report, registry, and host_id, returns a cell of type-erased output.
pub type ReportCellFactory = fn(
    Arc<dyn AnyReport>,
    Arc<RequestContext>,
    Arc<CellServerCtx>,
) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String>;

// ─────────────────────────────────────────────────────────────────────────────
// ReportRegistration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(ReportRegistration);

/// Registration entry for a report type.
/// Collected via inventory for automatic discovery.
pub struct ReportRegistration {
    /// Report identifier (e.g., "ServerStats")
    pub report_id: &'static str,
    /// Crate where this report is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Output type name (e.g., "ServerStatsOutput")
    pub output_type: &'static str,
    /// Crate where the output type is defined
    pub output_type_crate: &'static str,
    /// Parse function for deserializing report from JSON
    pub parse: ReportParseFn,
    /// Factory for creating reactive cell from report
    pub cell_factory: ReportCellFactory,
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
    fn parse(value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error>;

    /// Create a reactive cell for this report.
    fn cell_factory(
        report: Arc<dyn AnyReport>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<CellServerCtx>,
    ) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String>;
}

impl<R: ReportParams> ReportFactory for R {
    fn parse(value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error> {
        let report = serde_json::from_value::<ReportRequest<R>>(value)?;
        Ok(Arc::new(report))
    }

    fn cell_factory(
        any_report: Arc<dyn AnyReport>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<CellServerCtx>,
    ) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String> {
        // Downcast to the ReportRequest wrapper
        let any_ref: &dyn Any = any_report.as_ref();
        let request: ReportRequest<R> = any_ref
            .downcast_ref::<ReportRequest<R>>()
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Failed to downcast report to ReportRequest<{}>",
                    R::report_id_static()
                )
            })?;

        // Create a ReportContext with host_id - report args are accessed via &self in compute
        let ctx = ReportContext::new(request_ctx, server_ctx);

        // Call the inner report's compute method
        let report_id = R::report_id_static();
        let cell = <R as ReportHandler>::compute(&request.report, ctx);

        // Map to type-erased output for the WS/report subscription layer.
        let report_name = format!("report:{}", report_id);
        Ok(cell
            .map(|output| output.clone() as Arc<dyn AnyOutput>)
            .with_name(report_name.as_str()))
    }
}
