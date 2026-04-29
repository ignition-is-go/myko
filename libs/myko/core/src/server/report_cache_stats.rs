//! Periodic-summary report-cache hit/miss instrumentation.
//!
//! Replaces per-call `log::debug!` lines in `CellServerCtx::report` (which
//! were firing thousands of times per scene load and dominated I/O).
//!
//! The hot path increments per-report-id atomic counters via DashMap entry
//! lookup; a single dedicated thread emits one summary log line every
//! `WINDOW_MS`. Quiet windows emit nothing.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use dashmap::DashMap;

const WINDOW_MS: u64 = 1000;

#[derive(Default)]
struct ReportCounts {
    hits: AtomicU64,
    hits_after_gate: AtomicU64,
    misses: AtomicU64,
}

fn counts() -> &'static DashMap<String, ReportCounts> {
    static C: OnceLock<DashMap<String, ReportCounts>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

#[inline]
pub fn record_hit(report_id: &str) {
    counts()
        .entry(report_id.to_string())
        .or_default()
        .hits
        .fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_hit_after_gate(report_id: &str) {
    counts()
        .entry(report_id.to_string())
        .or_default()
        .hits_after_gate
        .fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_miss(report_id: &str) {
    counts()
        .entry(report_id.to_string())
        .or_default()
        .misses
        .fetch_add(1, Ordering::Relaxed);
}

/// Spawn the summary thread. Idempotent.
pub fn start_periodic_logger() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    let _ = thread::Builder::new()
        .name("myko-report-cache-stats".to_string())
        .spawn(run_loop)
        .map_err(|e| {
            log::warn!(
                target: "myko::server::report_cache_stats",
                "Failed to spawn report_cache_stats thread: {}", e
            )
        });
}

fn run_loop() {
    loop {
        thread::sleep(Duration::from_millis(WINDOW_MS));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit_window));
    }
}

fn emit_window() {
    let mut snap: Vec<(String, u64, u64, u64)> = counts()
        .iter()
        .filter_map(|e| {
            let h = e.value().hits.swap(0, Ordering::Relaxed);
            let g = e.value().hits_after_gate.swap(0, Ordering::Relaxed);
            let m = e.value().misses.swap(0, Ordering::Relaxed);
            if h == 0 && g == 0 && m == 0 {
                None
            } else {
                Some((e.key().clone(), h, g, m))
            }
        })
        .collect();
    if snap.is_empty() {
        return;
    }
    snap.sort_by_key(|b| std::cmp::Reverse(b.1 + b.2 + b.3));
    let total_hits: u64 = snap.iter().map(|s| s.1 + s.2).sum();
    let total_misses: u64 = snap.iter().map(|s| s.3).sum();
    let detail = snap
        .iter()
        .map(|(rid, h, g, m)| {
            if *g > 0 {
                format!("{}=H{}/G{}/M{}", rid, h, g, m)
            } else {
                format!("{}=H{}/M{}", rid, h, m)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    log::info!(
        target: "myko::server::report_cache_stats",
        "[report_cache window={}ms] hits={} misses={} miss_rate={:.0}% [{}]",
        WINDOW_MS,
        total_hits,
        total_misses,
        if total_hits + total_misses > 0 {
            100.0 * total_misses as f64 / (total_hits + total_misses) as f64
        } else {
            0.0
        },
        detail,
    );
}
