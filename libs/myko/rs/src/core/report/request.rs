//! ReportRequest wrapper type.

use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

use crate::common::with_transaction::WithTransaction;

use super::traits::{AnyReport, ReportId, ReportIdStatic, ReportOutputType, ReportParams};

/// Wraps report parameters with transaction metadata.
///
/// This type adds `tx` (transaction ID) to any report parameters struct.
/// Uses `#[serde(flatten)]` to serialize as a flat structure.
///
/// # Example
///
/// ```ignore
/// // Report params (what user defines):
/// #[myko_report(ServerStatsOutput)]
/// pub struct ServerStats {}
///
/// // Create a request:
/// let request = ReportRequest::new(ServerStats {});
///
/// // Serializes to: { "tx": "...", ...params }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ReportRequest<R> {
    pub tx: Arc<str>,
    #[serde(flatten)]
    pub report: R,
}

impl<R> ReportRequest<R> {
    /// Create a new report request with auto-generated tx.
    pub fn new(report: R) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            report,
        }
    }
}

impl<R: Default> Default for ReportRequest<R> {
    fn default() -> Self {
        Self::new(R::default())
    }
}

/// Convert report params directly into a ReportRequest.
/// This only works for types that implement ReportParams (actual report param structs),
/// not for ReportRequest itself, which avoids ambiguity with From<&ReportRequest<R>>.
impl<R: ReportParams> From<R> for ReportRequest<R> {
    fn from(report: R) -> Self {
        Self::new(report)
    }
}

/// Convert a reference to a ReportRequest into an owned ReportRequest by cloning.
impl<R: Clone> From<&ReportRequest<R>> for ReportRequest<R> {
    fn from(request: &ReportRequest<R>) -> Self {
        request.clone()
    }
}

impl<R: Send + Sync + 'static> WithTransaction for ReportRequest<R> {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone()
    }
}

impl<R: ReportId> ReportId for ReportRequest<R> {
    fn report_id(&self) -> Arc<str> {
        self.report.report_id()
    }
}

impl<R: ReportIdStatic> ReportIdStatic for ReportRequest<R> {
    fn report_id_static() -> &'static str {
        R::report_id_static()
    }
}

impl<R: ReportOutputType> ReportOutputType for ReportRequest<R> {
    type Output = R::Output;
}

impl<R: ReportId + Serialize + Debug + Send + Sync + 'static> AnyReport for ReportRequest<R> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("ReportRequest should serialize to JSON")
    }
}
