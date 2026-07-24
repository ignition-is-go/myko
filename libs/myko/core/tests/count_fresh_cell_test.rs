//! Regression tests for CountAll* reports returning 0 (or freezing at a
//! stale count) — reported against myko v4.24.1 (rship canary.71).
//!
//! Root cause (confirmed by cosmic-marten against hyphae directly):
//! `CellMap::size()` returns a bare clone of the map's internal `len_cell`
//! without capturing the parent `CellMapInner`'s keepalive, unlike
//! `entries()`/`items()`/`subscribe_diffs`. myko's generated `CountAll`/
//! `Count` report `compute()` built its source query map as a temporary and
//! never held onto it, so once `compute()` returned, the source map's store
//! subscription could drop out from under `len_cell`, freezing the count at
//! whatever value existed at that moment. Fixed in
//! `libs/myko/macros/src/item.rs` by capturing the source map in the
//! returned count cell's own closure. Pre-existing back to hyphae 1.3.1,
//! not something a version bump introduced.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchItem, CountAllBenchItems},
    server::{MykoServerContext, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

/// hyphae's scheduler tick queue is process-wide — see the identical
/// serialization comment in `query_cache_leak_test.rs`. Guard every test
/// against cross-test interference the same way.
fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        Arc::new(dashmap::DashMap::new()),
        None,
        None,
    )
}

fn insert_bench_item(ctx: &MykoServerContext, id: &str, category: &str, value: i64) {
    let item = BenchItem {
        id: id.into(),
        name: format!("item-{id}"),
        category: category.to_string(),
        value,
    };
    let event = MEvent::from_item(&item, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

#[test]
fn count_all_report_sees_correct_count_on_first_compute() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    for i in 0..5 {
        insert_bench_item(&ctx, &format!("item-{i}"), "cat", i as i64);
    }

    let request = Arc::new(myko::request::RequestContext::from_client(
        Arc::from("tx-1"),
        Arc::from("client-1"),
        ctx.host_id,
    ));

    // Freshly-computed CountAll cell must see the 5 items already in the
    // store — not 0, which is what myko 4.24.1 returned.
    let cell = ctx.report(CountAllBenchItems {}, request);
    let count = myko::hyphae::Gettable::get(&cell);
    assert_eq!(
        count.count, 5,
        "a freshly-computed CountAll report must reflect items already present in the store"
    );
}

#[test]
fn count_all_report_correct_under_concurrent_fresh_reads() {
    let _serial = scheduler_test_serial();
    // rship reported this as per-request FLAPPING (53/0/53) behind concurrent
    // MCP probes — try to trigger it by hammering fresh report construction
    // from multiple threads concurrently with the data already present, many
    // rounds, on a brand new ctx each round.
    for round in 0..200 {
        let ctx = Arc::new(make_ctx());
        for i in 0..5 {
            insert_bench_item(&ctx, &format!("item-{round}-{i}"), "cat", i as i64);
        }

        let barrier = Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let ctx = ctx.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let request = Arc::new(myko::request::RequestContext::from_client(
                        Arc::from(format!("tx-{round}-{t}")),
                        Arc::from(format!("client-{round}-{t}")),
                        ctx.host_id,
                    ));
                    barrier.wait();
                    let cell = ctx.report(CountAllBenchItems {}, request);
                    myko::hyphae::Gettable::get(&cell).count
                })
            })
            .collect();

        for (t, h) in handles.into_iter().enumerate() {
            let count = h.join().unwrap();
            assert_eq!(
                count, 5,
                "round {round} thread {t}: fresh concurrent CountAll read must see 5, not a stale/racing value"
            );
        }
    }
}

#[test]
fn count_all_report_tracks_writes_after_the_computing_call_returns() {
    let _serial = scheduler_test_serial();
    // Deterministic red case for the retention bug: the generated CountAll
    // compute() builds its source query map as a local variable entirely
    // internal to the function — this test has no way to hold a reference
    // to it externally, so it proves the fix keeps the chain alive from
    // *inside* compute() itself, not via some accidental external retention.
    let ctx = make_ctx();
    insert_bench_item(&ctx, "item-0", "cat", 0);

    let request = Arc::new(myko::request::RequestContext::from_client(
        Arc::from("tx-1"),
        Arc::from("client-1"),
        ctx.host_id,
    ));
    let cell = ctx.report(CountAllBenchItems {}, request);
    assert_eq!(myko::hyphae::Gettable::get(&cell).count, 1);

    // Writes AFTER compute() has already returned must still be tracked —
    // a frozen chain would leave `cell` stuck at 1 forever from here on.
    for i in 1..5 {
        insert_bench_item(&ctx, &format!("item-{i}"), "cat", i as i64);
    }

    assert_eq!(
        myko::hyphae::Gettable::get(&cell).count,
        5,
        "the count cell must keep tracking store writes made after compute() \
         returned — a bare `.size()` clone without retaining the source map \
         freezes at whatever value existed when the local chain inside \
         compute() was dropped"
    );
}
