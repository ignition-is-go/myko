//! Lightweight WS message-throughput instrumentation.
//!
//! Counts inbound (client → server) and outbound (server → client) WS messages
//! per kind into atomic counters, and a single dedicated thread emits a
//! summary log line every `WINDOW_MS`. No per-message log I/O, no allocations
//! on the hot path, no work when no messages flowed.
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
use opentelemetry::{KeyValue, metrics::Counter};

const WINDOW_MS: u64 = 250;

fn per_client_counter() -> &'static Counter<u64> {
    static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
    COUNTER.get_or_init(|| {
        opentelemetry::global::meter("myko-server")
            .u64_counter("myko.ws.message.count")
            .with_description("WS messages by direction, kind, and client")
            .build()
    })
}

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

/// Same as [`record_inbound`], plus an OTLP counter tagged by `client_id` and
/// `tag` (the specific command/query/report/view id, from [`message_tag`],
/// when the message carries one) — the aggregate `DashMap` counters above
/// are cheap enough for an unbounded number of message kinds, but tagging
/// *those* by client_id/tag too would put an OTel series per (kind × client
/// × tag) into the periodic in-process log line, which is the wrong place
/// for this breakdown. This is that breakdown, exported as a proper metric
/// instead (cardinality bounded by concurrent connections × distinct
/// command/query/report/view ids, which is the norm for these tags).
pub fn record_inbound_for_client(kind: &'static str, client_id: &str, tag: Option<&str>) {
    record_inbound(kind);
    let mut attrs = vec![
        KeyValue::new("direction", "in"),
        KeyValue::new("kind", kind),
        KeyValue::new("client_id", client_id.to_string()),
    ];
    if let Some(tag) = tag {
        attrs.push(KeyValue::new("tag", tag.to_string()));
    }
    per_client_counter().add(1, &attrs);
}

/// Same as [`record_outbound`], plus a per-client/per-tag OTLP counter — see
/// [`record_inbound_for_client`].
pub fn record_outbound_for_client(kind: &'static str, client_id: &str, tag: Option<&str>) {
    record_outbound(kind);
    let mut attrs = vec![
        KeyValue::new("direction", "out"),
        KeyValue::new("kind", kind),
        KeyValue::new("client_id", client_id.to_string()),
    ];
    if let Some(tag) = tag {
        attrs.push(KeyValue::new("tag", tag.to_string()));
    }
    per_client_counter().add(1, &attrs);
}

/// The specific command/query/report/view id carried by a message, when the
/// wire type carries one directly. `*Request` and `*Error` variants carry
/// their id inline; `*Response`/`*Cancel`/`*Window` variants only carry
/// `tx` (the id lives in a tx→id side table the caller already tracks per
/// subscription) — those return `None` here rather than duplicating that
/// lookup.
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
        _ => None,
    }
}

/// Stable `'static` kind tag for a message. Must match what the TS client
/// emits for symmetric cross-side correlation.
pub fn message_kind(msg: &MykoMessage) -> &'static str {
    match msg {
        MykoMessage::Query(_) => "Query",
        MykoMessage::QueryResponse(_) => "QueryResponse",
        MykoMessage::QueryCancel(_) => "QueryCancel",
        MykoMessage::QueryWindow(_) => "QueryWindow",
        MykoMessage::QueryError(_) => "QueryError",
        MykoMessage::View(_) => "View",
        MykoMessage::ViewResponse(_) => "ViewResponse",
        MykoMessage::ViewCancel(_) => "ViewCancel",
        MykoMessage::ViewWindow(_) => "ViewWindow",
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
/// number of `CellServerCtx::new` invocations.
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
            )
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
        .map(|(k, n)| format!("{}={}", k, n))
        .collect::<Vec<_>>()
        .join(", ")
}
