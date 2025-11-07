use serde::{Deserialize, Serialize};

use crate::{
    api::query::{QueryError, QueryResponse, WrappedQuery},
    command::{CommandError, CommandResponse},
    event::MEvent,
    report::{ReportError, ReportResponse, WrappedReport},
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
    #[serde(rename = "ws:m:report-error")]
    ReportError(ReportError),
    #[serde(rename = "ws:m:query-error")]
    QueryError(QueryError),
    #[serde(rename = "ws:m:event")]
    Event(MEvent),
    #[serde(rename = "ws:m:command")]
    Command(Commands),
    #[serde(rename = "ws:m:command-response")]
    CommandResponse(CommandResponse),
    #[serde(rename = "ws:m:command-error")]
    CommandError(CommandError),
}
