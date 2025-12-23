//! Report registration via inventory.

use std::any::Any;
use std::sync::Arc;

use hypha::{Cell, CellImmutable, MapExt};
use uuid::Uuid;

use crate::{
    common::to_value::ToValue,
    parsers::report::{CapturedReportParser, MykoReportParser},
    store::StoreRegistry,
};

use super::{
    handler::{ReportContext, ReportHandler},
    request::ReportRequest,
    traits::{AnyReport, ReportIdStatic, ReportParams},
};

// ─────────────────────────────────────────────────────────────────────────────
// AnyOutput - Type-erased output for the WebSocket layer
// ─────────────────────────────────────────────────────────────────────────────

/// Type-erased report output trait.
/// Report outputs implement this to enable serialization at the WebSocket layer.
pub trait AnyOutput: ToValue + Send + Sync + 'static {}

/// Blanket implementation for any type that satisfies the bounds.
impl<T: ToValue + Send + Sync + 'static> AnyOutput for T {}

// ─────────────────────────────────────────────────────────────────────────────
// Registration types
// ─────────────────────────────────────────────────────────────────────────────

/// Type-erased cell factory for reports.
/// Takes a typed report, registry, and host_id, returns a cell of type-erased output.
pub type ReportCellFactory = fn(
    Arc<dyn AnyReport>,
    Arc<StoreRegistry>,
    Uuid, // host_id for context
) -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String>;

/// Data needed to register a report.
pub struct RegisterReportData {
    pub report_id: Arc<str>,
    pub parser: Arc<dyn MykoReportParser>,
    pub cell_factory: ReportCellFactory,
}

/// Type alias for the report factory function pointer.
pub type ReportFactoryFn = fn() -> RegisterReportData;

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
    /// Factory function that creates the registration data
    pub factory: ReportFactoryFn,
}

inventory::collect!(ReportRegistration);

// ─────────────────────────────────────────────────────────────────────────────
// ReportFactory - Creates registration data for cell-based reports
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating report registrations.
///
/// This trait has a blanket implementation for all types implementing `ReportParams`,
/// so user-defined reports automatically get a `create_registration()` method.
pub trait ReportFactory: ReportParams {
    /// Create the registration data (cell factory) for this report type.
    fn create_registration() -> RegisterReportData;
}

impl<R: ReportParams> ReportFactory for R {
    fn create_registration() -> RegisterReportData {
        // Use ReportRequest<R> for parsing since that's what implements AnyReport
        let parser: Arc<dyn MykoReportParser> = Arc::new(CapturedReportParser::<ReportRequest<R>>::new());

        RegisterReportData {
            report_id: <R as ReportIdStatic>::report_id_static().into(),
            parser,
            cell_factory: |any_report: Arc<dyn AnyReport>, registry: Arc<StoreRegistry>, host_id: Uuid| -> Result<Cell<Arc<dyn AnyOutput>, CellImmutable>, String> {
                // Downcast to the ReportRequest wrapper
                let any_ref: &dyn Any = any_report.as_ref();
                let request: ReportRequest<R> = any_ref
                    .downcast_ref::<ReportRequest<R>>()
                    .cloned()
                    .ok_or_else(|| format!("Failed to downcast report to ReportRequest<{}>", R::report_id_static()))?;

                // Create a ReportContext with host_id - report args are accessed via &self in compute
                let ctx = ReportContext::with_host_id(registry, host_id);

                // Call the inner report's compute method
                let cell = <R as ReportHandler>::compute(&request.report, ctx);

                // Map to type-erased output (clone since map receives a reference)
                Ok(cell.map(|output| Arc::new(output.clone()) as Arc<dyn AnyOutput>))
            },
        }
    }
}
