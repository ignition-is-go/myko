//! Wire protocol types for reports.

use serde::{Deserialize, Serialize, ser::Error};
use serde_json::Value;

use crate::{TS, core::report::ReportId};

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ReportResponse {
    pub response: Value,
    pub tx: String,
}

impl ReportResponse {
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WrappedReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<std::sync::Arc<str>>,
    pub report: Value,
    pub report_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ReportError {
    pub tx: String,
    pub report_id: String,
    pub message: String,
}

impl ReportError {
    pub fn new(
        tx: impl Into<String>,
        report_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            tx: tx.into(),
            report_id: report_id.into(),
            message: message.into(),
        }
    }
}

///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn wrap_report<Q: ReportId + crate::report::ReportIdStatic + Serialize + Clone>(
    tx: String,
    report: &Q,
) -> Result<WrappedReport, serde_json::Error> {
    let mut json = serde_json::to_value(report.clone())?;

    let Some(obj) = json.as_object_mut() else {
        return Err(serde_json::Error::custom("Could not convert to object"));
    };

    obj.insert("tx".to_string(), tx.into());

    Ok(WrappedReport {
        service_id: Q::SERVICE_ID.map(Into::into),
        report: json,
        report_id: report.report_id().to_string(),
    })
}
