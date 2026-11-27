//! Wire protocol types for reports.

use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize, ser::Error};
use serde_json::{Value, value::RawValue};

use super::{encoded::EncodedJsonCache, message::WS_EVENT_REPORT_RESPONSE};
use crate::{
    TS,
    core::report::{AnyOutput, ReportId},
};

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

#[derive(Serialize)]
struct ReportResponseRef<'a> {
    response: &'a RawValue,
    tx: &'a str,
}

#[derive(Serialize)]
struct ReportResponseMessageRef<'a> {
    event: &'static str,
    data: ReportResponseRef<'a>,
}

fn output_json_cache() -> &'static EncodedJsonCache<dyn AnyOutput> {
    static CACHE: OnceLock<EncodedJsonCache<dyn AnyOutput>> = OnceLock::new();
    CACHE.get_or_init(EncodedJsonCache::default)
}

/// The JSON text frame of `MykoMessage::ReportResponse` for `output`.
///
/// Byte-identical to serializing the message built from `output.to_value()`,
/// but the output is encoded once per allocation and shared by every
/// subscriber and frame that carries the same `Arc`.
///
/// # Errors
///
/// Returns an error when the output or the frame cannot be serialized.
pub fn encode_report_response_json(
    tx: &str,
    output: &Arc<dyn AnyOutput>,
) -> Result<String, serde_json::Error> {
    let response = output_json_cache()
        .get_or_encode(output, |output| {
            serde_json::value::to_raw_value(&output.to_value()).ok()
        })
        .ok_or_else(|| serde_json::Error::custom("report output is not JSON-serializable"))?;
    serde_json::to_string(&ReportResponseMessageRef {
        event: WS_EVENT_REPORT_RESPONSE,
        data: ReportResponseRef {
            response: &response,
            tx,
        },
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WrappedReport {
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
pub fn wrap_report<Q: ReportId + Serialize + Clone>(
    tx: String,
    report: &Q,
) -> Result<WrappedReport, serde_json::Error> {
    let mut json = serde_json::to_value(report.clone())?;

    let Some(obj) = json.as_object_mut() else {
        return Err(serde_json::Error::custom("Could not convert to object"));
    };

    obj.insert("tx".to_string(), tx.into());

    Ok(WrappedReport {
        report: json,
        report_id: report.report_id().to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::wire::MykoMessage;

    #[test]
    fn encoded_report_response_matches_the_serialized_message() {
        let output: Arc<dyn AnyOutput> = Arc::new(json!({
            "zeta": [1, 2.5, "x\"y"],
            "alpha": {"nested": null, "flag": true},
        }));
        let expected = serde_json::to_string(&MykoMessage::ReportResponse(ReportResponse {
            response: output.to_value(),
            tx: "tx-\u{1}".to_owned(),
        }))
        .unwrap();
        for _ in 0..2 {
            assert_eq!(
                encode_report_response_json("tx-\u{1}", &output).unwrap(),
                expected
            );
        }
    }
}
