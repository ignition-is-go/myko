//! OTLP dispatch-rate counters for commands/queries/reports/views.
//!
//! Mirrors the `entity_set_stats.rs`/`report_cache_stats.rs` idiom but
//! reports through `OTel` `Counter`s instead of a periodic log line, so a
//! dashboard can compute per-second rates and slice by tag (command/query/
//! report/view id, and dispatch origin) instead of grepping logs.
//!
//! Cheap/no-op without a registered `MeterProvider` (see myko-server's
//! `telemetry::init_from_env`) — `opentelemetry::global::meter` falls back
//! to a no-op meter when telemetry isn't configured, so these calls are
//! safe to leave in unconditionally.

use std::sync::OnceLock;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};

fn meter() -> &'static Meter {
    static METER: OnceLock<Meter> = OnceLock::new();
    METER.get_or_init(|| opentelemetry::global::meter("myko"))
}

macro_rules! dispatch_counter {
    ($fn_name:ident, $otel_name:literal, $tag_key:literal) => {
        pub fn $fn_name(id: &str, origin: &str) {
            static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
            let counter = COUNTER.get_or_init(|| meter().u64_counter($otel_name).build());
            counter.add(
                1,
                &[
                    KeyValue::new($tag_key, id.to_string()),
                    KeyValue::new("origin", origin.to_string()),
                ],
            );
        }
    };
}

macro_rules! response_counter {
    ($fn_name:ident, $otel_name:literal, $tag_key:literal) => {
        pub fn $fn_name(id: &str) {
            static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
            let counter = COUNTER.get_or_init(|| meter().u64_counter($otel_name).build());
            counter.add(1, &[KeyValue::new($tag_key, id.to_string())]);
        }
    };
}

/// One increment per command execution, tagged by `command` id and `source`
///
/// — `"external"` for commands arriving pre-serialized via
/// `CommandExecutorAdapter::execute_from_value` (the funnel for both
/// native-WS command dispatch and MCP HTTP/WS in-process tool calls), vs
/// `"internal"` for commands composed in-process by another command handler
/// or a saga via `CommandContext::execute_command`. This is a *dispatch
/// path* distinction, not a transport one — it answers "did this arrive
/// from outside the process or get called by server-side code composing
/// commands," which is what actually matters for load attribution.
pub fn record_command(command_id: &str, source: &str) {
    static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
    let counter = COUNTER.get_or_init(|| meter().u64_counter("myko.command.count").build());
    counter.add(
        1,
        &[
            KeyValue::new("command", command_id.to_string()),
            KeyValue::new("source", source.to_string()),
        ],
    );
}

// Dispatch-rate: one increment per query/view registration or report
// compute, tagged by id and origin ("client" for native-WS/MCP-stdio-over-WS,
// "mcp" for MCP HTTP/WS in-process, "saga" for saga-initiated — see
// `RequestContext::origin`).
dispatch_counter!(record_query, "myko.query.count", "query");
dispatch_counter!(record_report, "myko.report.count", "report");
dispatch_counter!(record_view, "myko.view.count", "view");

// Response-rate: one increment per diff/value actually pushed to a
// subscriber. Native-WS-only today — MCP takes a one-shot snapshot and never
// reaches the persistent subscription path (see `ClientSession::subscribe_*`
// in `client_session.rs`), so there is currently nothing to count there.
response_counter!(record_query_response, "myko.query.response.count", "query");
response_counter!(
    record_report_response,
    "myko.report.response.count",
    "report"
);
response_counter!(record_view_response, "myko.view.response.count", "view");
