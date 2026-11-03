//! Lightweight WS message-throughput instrumentation.
//!
//! Counts inbound (client → server) and outbound (server → client) WS messages
//! per kind into atomic counters, and a single dedicated thread emits a
//! summary log line every `WINDOW_MS`, without per-message log I/O. Separate
//! connection-owned counters export client totals and full operation detail.
//!
//! Used to diagnose "server CPU is idle but loads are slow" — comparing the
//! inbound and outbound rates against the client-side equivalents tells us
//! whether time is in server-reply latency, client-send pacing, or round-trip.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use dashmap::DashMap;
use myko::wire::MykoMessage;

const WINDOW_MS: u64 = 250;

fn in_counts() -> &'static DashMap<&'static str, AtomicU64> {
    static C: OnceLock<DashMap<&'static str, AtomicU64>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

fn out_counts() -> &'static DashMap<&'static str, AtomicU64> {
    static C: OnceLock<DashMap<&'static str, AtomicU64>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

/// Record an inbound WS message (already parsed). `kind` should be the
/// `'static` discriminant string from `message_kind`.
pub fn record_inbound(kind: &'static str) {
    in_counts()
        .entry(kind)
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

/// Record an outbound WS message about to be serialized to the wire.
pub fn record_outbound(kind: &'static str) {
    out_counts()
        .entry(kind)
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a parsed inbound message and retain only transaction-to-operation IDs.
pub fn record_inbound_message(message: &MykoMessage, client_id: &str) {
    let request = match message {
        MykoMessage::Command(w) => Some(("command", w.command_id.as_str(), &w.command)),
        MykoMessage::Query(w) => Some(("query", w.query_id.as_ref(), &w.query)),
        MykoMessage::View(w) => Some(("view", w.view_id.as_ref(), &w.view)),
        MykoMessage::Report(w) => Some(("report", w.report_id.as_str(), &w.report)),
        _ => None,
    };
    if let Some((family, id, payload)) = request
        && let Some(tx) = payload.get("tx").and_then(serde_json::Value::as_str)
    {
        myko_telemetry::websocket_metrics::bind_operation(client_id, family, tx, id);
    }
    let kind = message_kind(message);
    record_inbound(kind);
    let tag = match message {
        MykoMessage::CommandResponse(response) => {
            myko_telemetry::websocket_metrics::take_operation(
                client_id,
                "server-command",
                &response.tx,
            )
        }
        MykoMessage::CommandError(error) => {
            let _ = myko_telemetry::websocket_metrics::take_operation(
                client_id,
                "server-command",
                &error.tx,
            );
            Some(error.command_id.as_str().into())
        }
        _ => resolved_message_tag(message, client_id),
    };
    myko_telemetry::websocket_metrics::record(client_id, "in", kind, tag.as_deref(), None);
}

/// Record a successfully written frame, with its encoded payload size.
pub fn record_outbound_for_client(
    kind: &'static str,
    client_id: &str,
    tag: Option<&str>,
    bytes: usize,
) {
    record_outbound(kind);
    myko_telemetry::websocket_metrics::record(
        client_id,
        "out",
        kind,
        tag,
        Some(u64::try_from(bytes).unwrap_or(u64::MAX)),
    );
}

/// Responses carry a transaction ID; recover the original operation ID instead
/// of losing command/query/report/view attribution on the most useful measurements.
#[must_use]
pub fn resolved_message_tag(message: &MykoMessage, client_id: &str) -> Option<std::sync::Arc<str>> {
    match message {
        MykoMessage::CommandResponse(response) => {
            return myko_telemetry::websocket_metrics::take_operation(
                client_id,
                "command",
                &response.tx,
            );
        }
        MykoMessage::CommandError(error) => {
            let _ =
                myko_telemetry::websocket_metrics::take_operation(client_id, "command", &error.tx);
        }
        _ => {}
    }
    if let Some(tag) = message_tag(message) {
        return Some(tag.into());
    }
    let (family, tx) = match message {
        MykoMessage::QueryResponse(w) => ("query", w.tx.as_ref()),
        MykoMessage::ViewResponse(w) => ("view", w.tx.as_ref()),
        MykoMessage::ReportResponse(w) => ("report", w.tx.as_str()),
        MykoMessage::QueryCancel(w) => ("query", w.tx.as_str()),
        MykoMessage::QueryWindow(w) => ("query", w.tx.as_str()),
        MykoMessage::QueryCursorWindow(w) => ("query", w.tx.as_str()),
        MykoMessage::ViewCancel(w) => ("view", w.tx.as_str()),
        MykoMessage::ViewWindow(w) => ("view", w.tx.as_str()),
        MykoMessage::ViewSampleRate(w) => ("view", w.tx.as_str()),
        MykoMessage::ReportCancel(w) => ("report", w.tx.as_str()),
        _ => return None,
    };
    myko_telemetry::websocket_metrics::operation(client_id, family, tx)
}

/// Return the operation id carried by a message.
///
/// This is the specific command/query/report/view id when the
/// wire type carries one directly. `*Request` and `*Error` variants carry
/// their id inline; `*Response`/`*Cancel`/`*Window` variants only carry
/// `tx` (the id lives in a tx→id side table the caller already tracks per
/// subscription) — those return `None` here rather than duplicating that
/// lookup.
#[must_use]
pub fn message_tag(msg: &MykoMessage) -> Option<&str> {
    match msg {
        MykoMessage::Query(w) => Some(&w.query_id),
        MykoMessage::QueryError(e) => Some(&e.query_id),
        MykoMessage::View(w) => Some(&w.view_id),
        MykoMessage::ViewError(e) => Some(&e.view_id),
        MykoMessage::Report(w) => Some(&w.report_id),
        MykoMessage::ReportError(e) => Some(&e.report_id),
        MykoMessage::Command(w) => Some(&w.command_id),
        MykoMessage::CommandError(e) => Some(&e.command_id),
        MykoMessage::Event(event) => Some(event.item_type.as_ref()),
        MykoMessage::EventBatch(events) => Some(match events.first() {
            Some(first)
                if events
                    .iter()
                    .all(|event| event.item_type == first.item_type) =>
            {
                first.item_type.as_ref()
            }
            Some(_) => "mixed-event-batch",
            None => "empty-event-batch",
        }),
        MykoMessage::Ping(_) => Some("ping"),
        MykoMessage::Benchmark(_) => Some("benchmark"),
        MykoMessage::QueryResponse(_)
        | MykoMessage::ViewResponse(_)
        | MykoMessage::ReportResponse(_)
        | MykoMessage::CommandResponse(_)
        | MykoMessage::QueryCancel(_)
        | MykoMessage::QueryWindow(_)
        | MykoMessage::QueryCursorWindow(_)
        | MykoMessage::ViewCancel(_)
        | MykoMessage::ViewWindow(_)
        | MykoMessage::ViewSampleRate(_)
        | MykoMessage::ReportCancel(_) => None,
    }
}

/// Stable `'static` kind tag for a message. Must match what the TS client
/// emits for symmetric cross-side correlation.
#[must_use]
pub const fn message_kind(msg: &MykoMessage) -> &'static str {
    match msg {
        MykoMessage::Query(_) => "Query",
        MykoMessage::QueryResponse(_) => "QueryResponse",
        MykoMessage::QueryCancel(_) => "QueryCancel",
        MykoMessage::QueryWindow(_) => "QueryWindow",
        MykoMessage::QueryCursorWindow(_) => "QueryCursorWindow",
        MykoMessage::QueryError(_) => "QueryError",
        MykoMessage::View(_) => "View",
        MykoMessage::ViewResponse(_) => "ViewResponse",
        MykoMessage::ViewCancel(_) => "ViewCancel",
        MykoMessage::ViewWindow(_) => "ViewWindow",
        MykoMessage::ViewSampleRate(_) => "ViewSampleRate",
        MykoMessage::ViewError(_) => "ViewError",
        MykoMessage::Report(_) => "Report",
        MykoMessage::ReportResponse(_) => "ReportResponse",
        MykoMessage::ReportCancel(_) => "ReportCancel",
        MykoMessage::ReportError(_) => "ReportError",
        MykoMessage::Event(_) => "Event",
        MykoMessage::EventBatch(_) => "EventBatch",
        MykoMessage::Command(_) => "Command",
        MykoMessage::CommandResponse(_) => "CommandResponse",
        MykoMessage::CommandError(_) => "CommandError",
        MykoMessage::Ping(_) => "Ping",
        MykoMessage::Benchmark(_) => "Benchmark",
    }
}

/// Spawn the dedicated summary thread. Idempotent — safe to call from any
/// number of `MykoServerContext::new` invocations.
pub fn start_periodic_logger() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }

    let _ = thread::Builder::new()
        .name("myko-ws-timing".to_string())
        .spawn(run_logger_loop)
        .map_err(|e| {
            tracing::warn!(
                target: "myko_server::ws_timing",
                "Failed to spawn ws_timing thread: {}", e
            );
        });
}

fn run_logger_loop() {
    loop {
        thread::sleep(Duration::from_millis(WINDOW_MS));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit_window));
    }
}

fn emit_window() {
    let in_snap = drain_counts(in_counts());
    let out_snap = drain_counts(out_counts());
    if in_snap.is_empty() && out_snap.is_empty() {
        return;
    }
    let in_total: u64 = in_snap.iter().map(|(_, n)| *n).sum();
    let out_total: u64 = out_snap.iter().map(|(_, n)| *n).sum();
    tracing::info!(
        target: "myko_server::ws_timing",
        "[ws_timing window={}ms] in={} [{}] out={} [{}]",
        WINDOW_MS,
        in_total,
        format_kinds(&in_snap),
        out_total,
        format_kinds(&out_snap),
    );
}

fn drain_counts(counts: &DashMap<&'static str, AtomicU64>) -> Vec<(&'static str, u64)> {
    let mut out: Vec<(&'static str, u64)> = counts
        .iter()
        .filter_map(|e| {
            let n = e.value().swap(0, Ordering::Relaxed);
            if n == 0 { None } else { Some((*e.key(), n)) }
        })
        .collect();
    out.sort_by_key(|b| std::cmp::Reverse(b.1));
    out
}

fn format_kinds(snap: &[(&'static str, u64)]) -> String {
    snap.iter()
        .map(|(k, n)| format!("{k}={n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn messages_with_inline_identity_and_protocol_traffic_have_tags() {
        for (event, data, expected) in [
            (
                "ws:m:command",
                json!({"commandId": "Execute", "command": {"tx": "one"}}),
                "Execute",
            ),
            (
                "ws:m:command-error",
                json!({"commandId": "Execute", "tx": "one", "message": "failed"}),
                "Execute",
            ),
            (
                "ws:m:query-error",
                json!({"queryId": "List", "tx": "one", "message": "failed"}),
                "List",
            ),
            (
                "ws:m:view-error",
                json!({"viewId": "Live", "tx": "one", "message": "failed"}),
                "Live",
            ),
            (
                "ws:m:report-error",
                json!({"reportId": "Status", "tx": "one", "message": "failed"}),
                "Status",
            ),
            ("ws:m:ping", json!({"id": "one", "timestamp": 0}), "ping"),
            ("ws:m:benchmark", json!({}), "benchmark"),
            ("ws:m:event-batch", json!([]), "empty-event-batch"),
        ] {
            let message: MykoMessage =
                serde_json::from_value(json!({"event": event, "data": data}))
                    .expect("valid fixture");
            assert_eq!(message_tag(&message), Some(expected), "{event}");
        }
    }
}
