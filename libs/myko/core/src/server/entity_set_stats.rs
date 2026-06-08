//! Periodic-summary entity-`SET` instrumentation.
//!
//! Replaces the per-`set` `log::debug!("[entity] SET {} id={}", ...)` lines in
//! [`ServerContext`], which fire tens of thousands of times per second under
//! pulse-heavy workloads (e.g. comp-engine field/cap presence pulses) and
//! dominate log I/O.
//!
//! The hot path increments a per-entity-type atomic counter via a DashMap entry
//! lookup; a single dedicated thread emits one summary log line every
//! `WINDOW_MS`. Quiet windows emit nothing.
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

fn counts() -> &'static DashMap<String, AtomicU64> {
    static C: OnceLock<DashMap<String, AtomicU64>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

/// Record a single entity `SET` for `entity_type`. Cheap: one DashMap lookup
/// plus a relaxed atomic increment.
#[inline]
pub fn record_set(entity_type: &str) {
    counts()
        .entry(entity_type.to_string())
        .or_default()
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
            log::warn!(
                target: "myko::server::entity_set_stats",
                "Failed to spawn entity_set_stats thread: {}", e
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
    let mut snap: Vec<(String, u64)> = counts()
        .iter()
        .filter_map(|e| {
            let n = e.value().swap(0, Ordering::Relaxed);
            if n == 0 { None } else { Some((e.key().clone(), n)) }
        })
        .collect();
    if snap.is_empty() {
        return;
    }
    // Highest-volume entity types first.
    snap.sort_by_key(|b| std::cmp::Reverse(b.1));
    let total: u64 = snap.iter().map(|s| s.1).sum();
    let detail = snap
        .iter()
        .map(|(et, n)| format!("{}={}", et, n))
        .collect::<Vec<_>>()
        .join(", ");
    log::debug!(
        target: "myko::server::entity_set_stats",
        "[entity] SET window={}ms total={} [{}]",
        WINDOW_MS,
        total,
        detail,
    );
}
