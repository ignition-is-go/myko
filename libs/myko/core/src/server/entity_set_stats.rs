//! Periodic-summary entity-`SET`/`DEL` instrumentation.
//!
//! Replaces the per-`set`/`del` `tracing::debug!("[entity] SET|DEL {} id={}", ...)`
//! lines in [`ServerContext`], which fire tens of thousands of times per second
//! under pulse-heavy workloads (e.g. comp-engine field/cap presence pulses) and
//! dominate log I/O.
//!
//! The hot path increments a per-entity-type atomic counter (SET and DEL kept
//! separate) via a `DashMap` entry lookup; a single dedicated thread emits one
//! summary log line every `WINDOW_MS`. Quiet windows emit nothing.
//!
//! Mirrors the [`super::report_cache_stats`] pattern. To silence even the
//! summary, set `RUST_LOG=myko::server::entity_set_stats=off`.

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

/// Per-entity-type counters for one window, SET and DEL tracked separately.
#[derive(Default)]
struct Counts {
    set: AtomicU64,
    del: AtomicU64,
}

fn counts() -> &'static DashMap<String, Counts> {
    static C: OnceLock<DashMap<String, Counts>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

/// Record a single entity `SET` for `entity_type`. Cheap: one `DashMap` lookup
/// plus a relaxed atomic increment.
#[inline]
pub fn record_set(entity_type: &str) {
    counts()
        .entry(entity_type.to_string())
        .or_default()
        .set
        .fetch_add(1, Ordering::Relaxed);
}

/// Record a single entity `DEL` for `entity_type`. Counted separately from
/// `SET` so the summary distinguishes churn from removal.
#[inline]
pub fn record_del(entity_type: &str) {
    counts()
        .entry(entity_type.to_string())
        .or_default()
        .del
        .fetch_add(1, Ordering::Relaxed);
}

/// Spawn the summary thread. Idempotent.
pub fn start_periodic_logger() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    let _ = thread::Builder::new()
        .name("myko-entity-set-stats".to_string())
        .spawn(run_loop)
        .map_err(|e| {
            tracing::warn!(
                target: "myko::server::entity_set_stats",
                "Failed to spawn entity_set_stats thread: {}", e
            );
        });
}

fn run_loop() {
    loop {
        thread::sleep(Duration::from_millis(WINDOW_MS));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit_window));
    }
}

fn emit_window() {
    // (entity_type, sets, dels) for every type with activity this window.
    let mut snap: Vec<(String, u64, u64)> = counts()
        .iter()
        .filter_map(|e| {
            let sets = e.value().set.swap(0, Ordering::Relaxed);
            let dels = e.value().del.swap(0, Ordering::Relaxed);
            if sets == 0 && dels == 0 {
                None
            } else {
                Some((e.key().clone(), sets, dels))
            }
        })
        .collect();
    if snap.is_empty() {
        return;
    }
    // Highest combined volume first.
    snap.sort_by_key(|bucket| std::cmp::Reverse(bucket.1.saturating_add(bucket.2)));
    let set_total: u64 = snap.iter().map(|s| s.1).sum();
    let del_total: u64 = snap.iter().map(|s| s.2).sum();
    let detail = snap
        .iter()
        .map(|(et, s, d)| format!("{et}=set:{s} del:{d}"))
        .collect::<Vec<_>>()
        .join(", ");
    tracing::debug!(
        target: "myko::server::entity_set_stats",
        "[entity] window={}ms set_total={} del_total={} [{}]",
        WINDOW_MS,
        set_total,
        del_total,
        detail,
    );
}
