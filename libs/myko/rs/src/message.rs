use myko_wasm::event::MEvent;
use serde::{Deserialize, Serialize};

use crate::{
    query::{QueryResponse, WrappedQuery},
    report::{ReportResponse, WrappedReport},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", content = "data")]
pub enum MykoMessage<Commands> {
    #[serde(rename = "ws:m:query")]
    Query(WrappedQuery),
    #[serde(rename = "ws:m:query-response")]
    QueryResponse(QueryResponse),
    #[serde(rename = "ws:m:report")]
    Report(WrappedReport),
    #[serde(rename = "ws:m:report-response")]
    ReportResponse(ReportResponse),
    // #[serde(rename = "ws:m:report-error")]
    // ReportError(Value),
    // #[serde(rename = "ws:m:query-error")]
    // QueryError(Value),
    #[serde(rename = "ws:m:event")]
    Event(MEvent),
    #[serde(rename = "ws:m:command")]
    Command(Commands),
}
