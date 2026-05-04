//! Periodic-summary search instrumentation.
//!
//! Records `(entity_type, hit_count, elapsed)` for every `SearchIndex::search`
//! call and emits one log line per `WINDOW_MS` listing the searches that
//! completed in the window. Quiet windows emit nothing.
//!
//! Same shape as `server::report_cache_stats` — single dedicated thread, no
//! per-call allocator pressure beyond the small record push.

use std::{
    sync::{Mutex, OnceLock},
    thread,
    time::Duration,
};

const WINDOW_MS: u64 = 1000;

struct SearchRecord {
    entity_type: String,
    hits: u32,
    elapsed_us: u32,
}

fn records() -> &'static Mutex<Vec<SearchRecord>> {
    static R: OnceLock<Mutex<Vec<SearchRecord>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(Vec::new()))
}

/// Record a completed search. Cost: one small `String` allocation; negligible
/// next to the search work itself.
#[inline]
pub fn record_search(entity_type: &str, hits: usize, elapsed: Duration) {
    let elapsed_us = elapsed.as_micros().min(u32::MAX as u128) as u32;
    let hits = hits.min(u32::MAX as usize) as u32;
    if let Ok(mut guard) = records().lock() {
        guard.push(SearchRecord {
            entity_type: entity_type.to_string(),
            hits,
            elapsed_us,
        });
    }
}

/// Spawn the summary thread. Idempotent.
pub fn start_periodic_logger() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    let _ = thread::Builder::new()
        .name("myko-search-stats".to_string())
        .spawn(run_loop)
        .map_err(|e| {
            log::warn!(
                target: "myko::search::search_stats",
                "Failed to spawn search_stats thread: {}", e
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
    let drained: Vec<SearchRecord> = match records().lock() {
        Ok(mut g) => std::mem::take(&mut *g),
        Err(_) => return,
    };
    if drained.is_empty() {
        return;
    }
    let total_searches = drained.len();
    let total_hits: u64 = drained.iter().map(|r| r.hits as u64).sum();
    let detail = drained
        .iter()
        .map(|r| {
            format!(
                "{}=N{}/{:.2}ms",
                r.entity_type,
                r.hits,
                r.elapsed_us as f64 / 1000.0,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    log::info!(
        target: "myko::search::search_stats",
        "[search window={}ms] searches={} total_hits={} [{}]",
        WINDOW_MS,
        total_searches,
        total_hits,
        detail,
    );
}
