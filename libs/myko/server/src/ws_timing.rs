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
        MykoMessage::ProtocolSwitch { .. } => "ProtocolSwitch",
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
        .spawn(move || run_logger_loop())
        .map_err(|e| {
            log::warn!(
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
    log::info!(
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
    out.sort_by(|a, b| b.1.cmp(&a.1));
    out
}

fn format_kinds(snap: &[(&'static str, u64)]) -> String {
    snap.iter()
        .map(|(k, n)| format!("{}={}", k, n))
        .collect::<Vec<_>>()
        .join(", ")
}
