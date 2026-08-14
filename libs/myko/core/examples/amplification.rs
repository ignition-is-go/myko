//! M1 harness — resident-memory amplification per live predicate.
//!
//! **This binds no socket.** It is a pure in-process measurement: no server, no
//! WebSocket listener, no port. It cannot collide with a running myko gateway
//! (dev default `5155`) or anything else.
//!
//! # The question
//!
//! A `rack` deployment holding ~82k records across 156 types — 40–170 MB at rest
//! — was observed at process RSS in the tens of GB. That is 100–1000×
//! amplification, and until it is understood, the memory cost of a myko node
//! cannot be predicted from what it holds.
//!
//! # The model under test
//!
//! Per *distinct live predicate* installed over a type with `N` source entities
//! and `M` matches, hyphae's map runtime (`traits/collections/internal/map_runtime.rs`)
//! keeps three maps:
//!
//! | Structure            | Scales with            |
//! |----------------------|------------------------|
//! | `source_rows`        | **N — every entity, matching or not** |
//! | `output_cache`       | M                      |
//! | `source_output_keys` | M — a `HashSet` *plus its own heap allocation*, per match |
//!
//! `source_rows` is the term that scales with **source size rather than
//! matches**, which is the shape reported as `lv-4a87`.
//!
//! # Measured results (2026-07-26; reproduced on myko main @ 0ed566f9)
//!
//! See `docs/architecture/mesh/M1-findings.md` for the full write-up **including
//! the 2026-07-27 refutation in its §8** — read that before quoting anything here.
//!
//! - **~208 B per entity per live predicate** at N=25k, exactly linear in `N × P`.
//!   Layout arithmetic predicted ~38 B; the 5.5× gap is mostly hashbrown's
//!   capacity doubling. There is also a fixed ~100 KB per predicate.
//! - **Teardown returns 100%** of installed memory. Hyphae does not retain.
//! - **RSS overstated live heap by 889×** under the **default glibc allocator** —
//!   5,167 MB resident against 5.81 MB live, collapsing to 13 MB on
//!   `malloc_trim(0)`.
//!
//! # Two scope limits that were learned the hard way
//!
//! **1. The 889× RSS result is glibc-only, and does NOT explain the rack
//! deployment.** `rship_server` runs `tikv-jemallocator`, where RSS/live is
//! 1.33× — the memory there is genuinely live. `malloc_trim` measures nothing
//! under jemalloc. Do not re-propose it for rship/rack.
//!
//! **2. This harness measures `select`, i.e. hyphae's `MapState` path — which
//! myko's GENERATED queries mostly do not use.** `Get*ById` is a per-key
//! observation cell; `Get*sByIds` and `belongs_to`-routed `Get*sByQuery` get
//! pre-narrowed sources; unpinned `Get*sByQuery` runs scan mode with a
//! matches-only result and no source mirror. `MapState` appears in **views,
//! reports, `capability.rs`'s `query_diff`, and hand-written operator chains**.
//! Quote 208 B/(N·P) for those, never for generated queries. (See `lv-4a87`.)
//!
//! Also not measured: `ClientSession`'s `all_items`/`visible_items` (~76 B/match
//! per WS subscription) or myko's query/view/report caches — both need a running
//! server. And the teardown result only proves hyphae releases on drop, since
//! this harness drops its predicates explicitly; a myko-layer retention bug
//! would be invisible here.
//!
//! Run with:
//! ```text
//! cargo run --release --target-dir target/claude -p myko --features bench --example amplification
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use hyphae::{CellMap, MapQuery, SelectExt};
use myko::{
    bench_entities::BenchItem,
    core::item::{AnyItem, downcast_any_item_arc},
};

// ──────────────────────────────────────────────────────────────────────────
// Counting allocator
//
// Tracks live bytes, which is what separates a real retention problem from
// allocator behaviour: RSS can stay high because the allocator has not returned
// pages, while live bytes tell you whether anything is still referenced. Both
// are reported so the two can be told apart rather than conflated.
// ──────────────────────────────────────────────────────────────────────────

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

impl Counting {
    #[inline]
    fn add(size: usize) {
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        // Monotonic max. Racy under threads, but this harness is single-threaded
        // on the measured path and an occasional lost update would only
        // *understate* the peak — never invent one.
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            Self::add(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            Self::add(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            // Net the delta rather than add-then-subtract, so a shrink is
            // recorded correctly and the peak is not inflated by a phantom spike.
            if new_size >= layout.size() {
                Self::add(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Resident set size, from `/proc/self/statm` field 2 (resident pages).
/// Returns 0 where unavailable, which is reported as `n/a` rather than as zero.
fn rss() -> usize {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let Some(pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    pages.parse::<usize>().unwrap_or(0) * 4096
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Ask glibc to return free arena pages to the OS.
///
/// This is the discriminator between "we are still holding the memory" and
/// "we freed it and the allocator kept the pages." If live heap is small but
/// RSS is large, and this call collapses RSS, the memory was never leaked —
/// the allocator simply had not given it back. That distinction decides whether
/// an RSS-based observation is evidence of a retention bug at all.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_allocator() -> bool {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    unsafe { malloc_trim(0) == 1 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_allocator() -> bool {
    false
}

// ──────────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────────

/// The dyn store is what myko actually uses — `EntityStore = CellMap<Arc<str>,
/// Arc<dyn AnyItem>>` — so the measurement is of the real shape, not a typed
/// approximation of it.
fn build_store(n: usize) -> CellMap<Arc<str>, Arc<dyn AnyItem>> {
    let entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> = (0..n)
        .map(|i| {
            let id: Arc<str> = format!("item-{i}").into();
            let item = BenchItem {
                id: id.clone().into(),
                name: format!("name-{i}"),
                category: if i % 4 == 0 {
                    "hot".into()
                } else {
                    "cold".into()
                },
                value: (i as i64) % 100,
            };
            let arc: Arc<dyn AnyItem> = Arc::new(item);
            (id, arc)
        })
        .collect();

    let store = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
    store.insert_many(entries);
    store
}

/// A materialized `select`, held alive. Each predicate is *distinct* (a
/// different threshold), because identical predicates would be the case myko's
/// query cache exists to collapse — and collapsing is precisely not what is
/// being measured here.
type Materialized = CellMap<Arc<str>, Arc<dyn AnyItem>, hyphae::CellImmutable>;

/// `CellMap::lock` consumes the receiver, so the store is locked once and the
/// resulting immutable view is cloned per predicate — which is also what myko
/// does, one locked view feeding many subscriptions.
fn install_predicates(locked: &Materialized, count: usize) -> Vec<Materialized> {
    (0..count)
        .map(|p| {
            // Thresholds cycle 0..100 over `value`, so selectivity is stable at
            // ~50% on average while every predicate remains a distinct closure.
            let threshold = (p % 100) as i64;
            MapQuery::materialize(locked.clone().select(move |item_any: &Arc<dyn AnyItem>| {
                let item = downcast_any_item_arc::<BenchItem>(item_any, "amplification");
                item.value > threshold
            }))
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────────────────
// Experiment
// ──────────────────────────────────────────────────────────────────────────

const ENTITY_COUNTS: &[usize] = &[1_000, 10_000, 25_000];
const PREDICATE_COUNTS: &[usize] = &[0, 1, 10, 100, 1_000];

/// `Action` in the measured `rack` deployment, the largest single type.
const TEARDOWN_N: usize = 25_457;
const TEARDOWN_P: usize = 1_000;

fn main() {
    println!("M1 amplification harness — no socket is opened by this process.\n");
    println!("Model under test: heap ≈ base + entities + Σ_predicates (38·N + 166·M)\n");

    sweep();
    teardown_test();
}

fn sweep() {
    println!("=== Sweep: live heap vs entity count × live predicate count ===\n");
    println!(
        "{:>8} {:>7} {:>12} {:>12} {:>12} {:>12}",
        "N", "P", "store MB", "preds MB", "B/(N·P)", "RSS MB"
    );
    println!("{}", "-".repeat(68));

    for &n in ENTITY_COUNTS {
        let before_store = live();
        let store = build_store(n).lock();
        let store_bytes = live().saturating_sub(before_store);

        for &p in PREDICATE_COUNTS {
            let before_preds = live();
            let held = install_predicates(&store, p);
            let pred_bytes = live().saturating_sub(before_preds);

            let per_entity_per_pred = if n > 0 && p > 0 {
                format!("{:.1}", pred_bytes as f64 / (n as f64 * p as f64))
            } else {
                "—".to_string()
            };

            println!(
                "{:>8} {:>7} {:>12.2} {:>12.2} {:>12} {:>12.1}",
                n,
                p,
                mb(store_bytes),
                mb(pred_bytes),
                per_entity_per_pred,
                mb(rss())
            );

            // Drop before the next P, so each row measures P predicates rather
            // than the cumulative total.
            drop(held);
        }
        drop(store);
        println!();
    }
}

/// The decisive measurement: does dropping every predicate return the live heap
/// to baseline?
///
/// Teardown is driven by `SubscriptionGuard` drop, not by myko's cache sweep —
/// so if bytes do not come back here, something is retaining guards, and the
/// effective predicate count grows with uptime rather than with client count.
fn teardown_test() {
    println!("=== Teardown: do dropped predicates return their memory? ===\n");

    let store = build_store(TEARDOWN_N).lock();
    let baseline = live();
    let rss_baseline = rss();
    println!(
        "baseline (N={TEARDOWN_N}, P=0)      live {:>9.2} MB   rss {:>9.2} MB",
        mb(baseline),
        mb(rss_baseline)
    );

    let held = install_predicates(&store, TEARDOWN_P);
    let loaded = live();
    println!(
        "loaded   (N={TEARDOWN_N}, P={TEARDOWN_P})   live {:>9.2} MB   rss {:>9.2} MB   (+{:.2} MB)",
        mb(loaded),
        mb(rss()),
        mb(loaded.saturating_sub(baseline))
    );

    drop(held);
    let after = live();
    let retained = after.saturating_sub(baseline);
    let installed = loaded.saturating_sub(baseline);

    println!(
        "dropped  (N={TEARDOWN_N}, P=0)      live {:>9.2} MB   rss {:>9.2} MB   (+{:.2} MB retained)",
        mb(after),
        mb(rss()),
        mb(retained)
    );

    // RSS almost certainly has NOT come back at this point, even though live
    // bytes have. That gap is the whole point of the next measurement.
    let rss_before_trim = rss();
    let trimmed = trim_allocator();
    println!(
        "trimmed  (malloc_trim)           live {:>9.2} MB   rss {:>9.2} MB   {}",
        mb(live()),
        mb(rss()),
        if trimmed {
            "(pages returned)"
        } else {
            "(no-op / unsupported)"
        }
    );

    let retained_pct = if installed > 0 {
        (retained as f64 / installed as f64) * 100.0
    } else {
        0.0
    };

    println!("\nretained after teardown: {retained_pct:.1}% of what was installed");

    let rss_gap = rss_before_trim as f64 / after.max(1) as f64;
    println!(
        "RSS/live-heap ratio before trim:  {rss_gap:.0}×   ({:.2} MB RSS vs {:.2} MB live)",
        mb(rss_before_trim),
        mb(after)
    );
    if rss_gap > 10.0 {
        println!(
            "\n  >>> RSS IS NOT LIVE HEAP. An RSS-based observation of this process would\n\
             \x20    report ~{:.0} MB while it actually references {:.0} MB. Any amplification\n\
             \x20    figure derived from RSS must be re-derived from live heap before it is\n\
             \x20    treated as a retention problem.",
            mb(rss_before_trim),
            mb(after)
        );
    }

    // 5% tolerance: hashbrown does not shrink on drop, and a handful of
    // long-lived registry entries are expected. Anything beyond that is
    // retention, not noise.
    if retained_pct > 5.0 {
        println!(
            "\n  >>> PREDICATE LEAK: dropping every predicate did not return the memory.\n\
             \x20    Effective predicate count grows with uptime, not client count.\n\
             \x20    This is the shape that turns ~1 MB/predicate into tens of GB."
        );
    } else {
        println!(
            "\n  >>> Teardown is clean. Cost is bounded by *live* predicates, so the\n\
             \x20    deployment must genuinely hold that many. Get the live-predicate\n\
             \x20    count from query_cache_len()/view_cache_len() before concluding."
        );
    }

    println!(
        "\npeak live heap over the whole run: {:.2} MB",
        mb(PEAK.load(Ordering::Relaxed))
    );
}
