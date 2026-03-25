use serde::{Deserialize, Serialize};

use super::{
    command::{CommandError, CommandResponse, WrappedCommand},
    event::MEvent,
    query::{QueryError, QueryResponse, QueryWindowUpdate, WrappedQuery},
    report::{ReportError, ReportResponse, WrappedReport},
    view::{ViewError, ViewResponse, ViewWindowUpdate, WrappedView},
};

pub const WS_EVENT_COMMAND: &str = "ws:m:command";

/// Cancel subscription payload - just the transaction ID
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct CancelSubscription {
    pub tx: String,
}

/// Ping payload for latency measurement
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct PingData {
    /// Unique identifier to correlate ping/pong
    pub id: String,
    /// Client-side timestamp when ping was sent (milliseconds since epoch)
    pub timestamp: i64,
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
pub enum MykoMessage {
    #[serde(rename = "ws:m:query")]
    Query(WrappedQuery),
    #[serde(rename = "ws:m:query-response")]
    QueryResponse(QueryResponse),
    #[serde(rename = "ws:m:query-cancel")]
    QueryCancel(CancelSubscription),
    #[serde(rename = "ws:m:query-window")]
    QueryWindow(QueryWindowUpdate),
    #[serde(rename = "ws:m:view")]
    View(WrappedView),
    #[serde(rename = "ws:m:view-response")]
    ViewResponse(ViewResponse),
    #[serde(rename = "ws:m:view-cancel")]
    ViewCancel(CancelSubscription),
    #[serde(rename = "ws:m:view-window")]
    ViewWindow(ViewWindowUpdate),
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
    #[serde(rename = "ws:m:view-error")]
    ViewError(ViewError),
    #[serde(rename = "ws:m:event")]
    Event(MEvent),
    #[serde(rename = "ws:m:event-batch")]
    EventBatch(Vec<MEvent>),
    // Keep this literal aligned with `WS_EVENT_COMMAND`.
    #[serde(rename = "ws:m:command")]
    Command(WrappedCommand),
    #[serde(rename = "ws:m:command-response")]
    CommandResponse(CommandResponse),
    #[serde(rename = "ws:m:command-error")]
    CommandError(CommandError),
    #[serde(rename = "ws:m:ping")]
    Ping(PingData),
    /// Benchmark message — server counts messages/bytes per second, no processing
    #[serde(rename = "ws:m:benchmark")]
    Benchmark(serde_json::Value),
    /// Protocol switch confirmation - sent by server when client requests binary mode
    #[serde(rename = "ws:m:protocol-switch")]
    ProtocolSwitch { protocol: String },
}
