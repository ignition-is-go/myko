use serde::{Deserialize, Serialize};

use crate::{
    api::query::{QueryError, QueryResponse, WrappedQuery},
    command::{CommandError, CommandResponse},
    event::MEvent,
    report::{ReportError, ReportResponse, WrappedReport},
};

/// Cancel subscription payload - just the transaction ID
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct CancelSubscription {
    pub tx: String,
}

/// Registration for message event types - used for TypeScript codegen
#[derive(Debug)]
pub struct MessageEventRegistration {
    pub variant_name: &'static str,
    pub event_value: &'static str,
}

inventory::collect!(MessageEventRegistration);

#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS, myko_macros::MessageEvents)]
#[ts(export)]
#[serde(tag = "event", content = "data")]
pub enum MykoMessage<Commands> {
    #[serde(rename = "ws:m:query")]
    Query(WrappedQuery),
    #[serde(rename = "ws:m:query-response")]
    QueryResponse(QueryResponse),
    #[serde(rename = "ws:m:query-cancel")]
    QueryCancel(CancelSubscription),
    #[serde(rename = "ws:m:report")]
    Report(WrappedReport),
    #[serde(rename = "ws:m:report-response")]
    ReportResponse(ReportResponse),
    #[serde(rename = "ws:m:report-cancel")]
    ReportCancel(CancelSubscription),
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
