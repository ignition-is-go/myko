//! Tests for `query_map` row cleanup when the query is a reactive key.
//!
//! Validates that a `query_map_at` lookup into the shared query family
//! releases each row it leaves and tears every row down once dropped. These tests were written
//! while investigating a 47 GB memory leak in a downstream server triggered
//! by `CuePlayback` state changes. The tests prove the cache layer is clean —
//! the actual leak is in the O(n^2) reactive fan-out of the `CueEngine`
//! blanket watcher, not in cache accumulation.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchItem, BenchItemQuery, GetBenchItemsByQuery, SwitchMapReport},
    hyphae::{Cell, Gettable, MapExt, Materialize, Mutable},
    query::StringFilter,
    search::SearchIndex,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

/// hyphae's scheduler tick queue is process-wide (deliberately, per hyphae's
/// scheduler module docs — a per-thread queue can't coordinate two threads'
/// batches converging on a shared cell). These tests each do
/// batch()+query+drop+sweep and assert on cache counts at a specific instant,
/// so running them concurrently lets one test's batch/drain perturb another's
/// count mid-flight (no entries are ever stranded — they still reclaim once
/// the shared queue drains — the assertion just samples too early). Serialize
/// them against each other; take the guard as the first line of every #[test].
fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        myko::server::MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
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
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

/// Verify that rows a `query_map_at` lookup leaves are reclaimable after it
/// moves to a new key.
///
/// This reproduces the `CuePaused` pattern:
/// ```rust,ignore
/// ctx.query_map_at(playbacks.map(|p| GetCuesByIds { ids: cue_ids(p) }))
/// ```
#[test]
fn query_map_at_changing_key_rows_become_reclaimable() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    // Seed items in different categories
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("a-{i}"), "alpha", i);
        insert_bench_item(&ctx, &format!("b-{i}"), "beta", i);
        insert_bench_item(&ctx, &format!("g-{i}"), "gamma", i);
    }

    let categories = ["alpha", "beta", "gamma"];

    // Outer signal that changes the query key (simulates playback list changing)
    let selector = Cell::new(0usize);
    let key = selector
        .clone()
        .map(move |idx| {
            let category = idx
                .checked_rem(categories.len())
                .and_then(|index| categories.get(index))
                .copied()
                .unwrap_or_default()
                .to_string();
            GetBenchItemsByQuery(BenchItemQuery {
                category: Some(StringFilter::Eq(category.into())),
                ..Default::default()
            })
        })
        .materialize();
    let switched = ctx
        .query_map_at(key, ctx.new_server_transaction())
        .items()
        .materialize();

    // Initial: should see alpha items
    assert_eq!(switched.get().len(), 10);

    let cache_before = ctx.query_rows();

    // Switch through many different query params — each creates a new cache entry
    // because the category field changes the payload_hash
    for i in 1..=30 {
        selector.set(i);
    }

    // The switched cell should have the latest result
    assert_eq!(switched.get().len(), 10);

    // Every row the lookup left is torn down: only the current key's row lives
    ctx.sweep_dead_families();
    let cache_after = ctx.query_rows();
    assert_eq!(
        cache_after, cache_before,
        "left rows should be torn down (before={cache_before}, after={cache_after})"
    );

    // Now drop the switched cell — all inner query_maps should become reclaimable
    drop(switched);
    drop(selector);

    // Sweep should reclaim dead entries
    ctx.sweep_dead_families();
    let cache_final = ctx.query_rows();

    // After dropping all references, every row is torn down
    assert!(
        cache_final == 0,
        "sweep should have removed dead query cache entries \
         (cache before={cache_before}, after switch={cache_after}, final={cache_final})",
    );
}

/// Verify that a `query_map_at` key that keeps re-emitting the SAME params
/// keeps one row rather than building new ones.
#[test]
fn query_map_at_same_params_reuses_row() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    for i in 0..5 {
        insert_bench_item(&ctx, &format!("item-{i}"), "tools", i);
    }

    let trigger = Cell::new(0u64);
    // Every trigger re-emits the SAME params — the lookup must keep its row
    let key = trigger
        .clone()
        .map(|_| {
            GetBenchItemsByQuery(BenchItemQuery {
                category: Some(StringFilter::Eq("tools".into())),
                ..Default::default()
            })
        })
        .materialize();
    let switched = ctx
        .query_map_at(key, ctx.new_server_transaction())
        .items()
        .materialize();

    assert_eq!(switched.get().len(), 5);
    let cache_after_init = ctx.query_rows();

    // Switch 50 times with same query params
    for i in 1..=50 {
        trigger.set(i);
    }

    assert_eq!(switched.get().len(), 5);

    // Cache should NOT have grown significantly — same params = same cache key
    // (as long as the previous CellMap is still alive when we look it up)
    let cache_after_switches = ctx.query_rows();

    // Allow some growth due to weak refs dying between switches,
    // but it should NOT be 50 new entries
    assert!(
        cache_after_switches <= cache_after_init.saturating_add(5),
        "cache grew from {cache_after_init} to {cache_after_switches} after 50 same-param switches — \
         expected cache reuse, got accumulation",
    );
}

/// Reproduce the production leak shape: a `query_map_at` lookup changes key
/// while the underlying store is actively receiving updates. Store mutations
/// fire the rows' internal subscriptions, which must not keep a left row
/// alive after the lookup moves on.
#[test]
fn query_map_at_changing_key_with_active_store_mutations() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    // Seed initial items
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("a-{i}"), "alpha", i);
        insert_bench_item(&ctx, &format!("b-{i}"), "beta", i);
    }

    let categories = ["alpha", "beta"];
    let selector = Cell::new(0usize);
    let key = selector
        .clone()
        .map(move |idx| {
            let category = idx
                .checked_rem(categories.len())
                .and_then(|index| categories.get(index))
                .copied()
                .unwrap_or_default()
                .to_string();
            GetBenchItemsByQuery(BenchItemQuery {
                category: Some(StringFilter::Eq(category.into())),
                ..Default::default()
            })
        })
        .materialize();
    let switched = ctx
        .query_map_at(key, ctx.new_server_transaction())
        .items()
        .materialize();

    assert_eq!(switched.get().len(), 10);

    // Interleave switches with store mutations (simulating playback state changes
    // that trigger both the outer signal AND entity updates)
    for round in 0usize..20 {
        // Switch to new category
        selector.set(round.saturating_add(1));

        // Mutate items in the store (simulates CuePlayback state changes)
        let cat = round
            .checked_rem(categories.len())
            .and_then(|index| categories.get(index))
            .copied()
            .unwrap_or_default();
        for i in 0usize..5 {
            insert_bench_item(
                &ctx,
                &format!("{}-{i}", cat.chars().next().unwrap_or('?')),
                cat,
                i64::try_from(round.saturating_mul(10).saturating_add(i)).unwrap_or(i64::MAX),
            );
        }
    }

    let live_before_drop = ctx.query_rows();
    let total_before_drop = ctx.query_rows();

    // Drop the lookup chain
    drop(switched);
    drop(selector);

    // Sweep
    ctx.sweep_dead_families();
    let live_after = ctx.query_rows();

    // All query cache entries should be reclaimable after dropping the lookup
    assert_eq!(
        live_after, 0,
        "expected 0 live cache entries after dropping the lookup, found {live_after} \
         (total before drop={total_before_drop}, live before drop={live_before_drop})",
    );
}

/// Verify the `query_cache_live_count` accurately reflects reachable entries.
#[test]
fn query_cache_live_count_tracks_reachable_entries() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    for i in 0..3 {
        insert_bench_item(&ctx, &format!("item-{i}"), "test", i);
    }

    assert_eq!(ctx.query_rows(), 0);

    // Create a query_map and hold onto it
    let request = ctx.new_server_transaction();
    let map = ctx.query_map(
        GetBenchItemsByQuery(BenchItemQuery {
            category: Some(StringFilter::Eq("test".into())),
            ..Default::default()
        }),
        request,
    );

    assert_eq!(ctx.query_rows(), 1);

    // Create another with different params
    let request2 = ctx.new_server_transaction();
    let map2 = ctx.query_map(
        GetBenchItemsByQuery(BenchItemQuery {
            category: Some(StringFilter::Eq("other".into())),
            ..Default::default()
        }),
        request2,
    );

    assert_eq!(ctx.query_rows(), 2);

    // Drop one — live count should decrease after sweep
    drop(map);
    ctx.sweep_dead_families();
    assert_eq!(ctx.query_rows(), 1);

    // Drop the other
    drop(map2);
    ctx.sweep_dead_families();
    assert_eq!(ctx.query_rows(), 0);
}

// =============================================================================
// Report-level tests: ctx.report() with a query_map at a changing key
// =============================================================================

/// Core reproduction of the `CuePaused` leak: a report whose `compute()` keys
/// an inner `query_map` on an outer query. Store mutations change the key,
/// the lookup moves to a new row, and the rows it left must be reclaimable.
#[test]
fn report_with_query_map_at_cleans_up_rows() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    // Seed items
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("item-{i}"), "alpha", i);
    }

    let cache_before = ctx.query_rows();
    let report_cache_before = ctx.report_rows();

    // Create the report — this calls SwitchMapReport::compute() which sets up
    // the outer query + keyed inner query_map
    let request = ctx.new_server_transaction();
    let report_cell = ctx.report(
        SwitchMapReport {
            category: "alpha".to_string(),
        },
        request,
    );

    // Should see all 10 item names
    assert_eq!(report_cell.get().len(), 10);

    let cache_after_report = ctx.query_rows();
    let report_cache_after_report = ctx.report_rows();

    // Report should have created cache entries
    assert!(cache_after_report > cache_before);
    assert!(report_cache_after_report > report_cache_before);

    // Now mutate items to trigger the outer query → new key → new inner row
    // Each mutation changes the item list, moving the lookup to a new row
    for round in 0..20 {
        insert_bench_item(
            &ctx,
            &format!("new-{round}"),
            "alpha",
            100 + i64::from(round),
        );
    }

    // Report should reflect the new items
    assert_eq!(report_cell.get().len(), 30); // 10 original + 20 new

    let cache_during = ctx.query_rows();

    // Drop the report cell — everything should become reclaimable
    drop(report_cell);

    ctx.sweep_dead_families();
    let cache_final = ctx.query_rows();
    let report_cache_final = ctx.report_rows();

    assert_eq!(
        cache_final, 0,
        "expected 0 live query cache entries after dropping report, found {cache_final} \
         (cache grew to {cache_during} during mutations)",
    );
    assert_eq!(
        report_cache_final, 0,
        "expected 0 live report cache entries after dropping report, found {report_cache_final}",
    );
}

/// Stress test: many rounds of store mutations while report is alive.
/// Verifies cache doesn't grow unboundedly during active use.
#[test]
fn report_query_map_at_rows_bounded_during_active_mutations() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();

    for i in 0..5 {
        insert_bench_item(&ctx, &format!("item-{i}"), "beta", i);
    }

    let request = ctx.new_server_transaction();
    let report_cell = ctx.report(
        SwitchMapReport {
            category: "beta".to_string(),
        },
        request,
    );

    assert_eq!(report_cell.get().len(), 5);

    // Run 100 rounds of mutations — each moves the inner lookup to a row
    // with new IDs
    for round in 0..100 {
        insert_bench_item(
            &ctx,
            &format!("stress-{round}"),
            "beta",
            1000_i64.saturating_add(i64::from(round)),
        );
    }

    assert_eq!(report_cell.get().len(), 105); // 5 + 100

    // Sweep mid-flight to reclaim dead entries
    ctx.sweep_dead_families();

    let live_count = ctx.query_rows();
    let total_count = ctx.query_rows();

    // The live count should be bounded — only the currently-active query_maps
    // should be alive, not all 100+ historical ones.
    // With the outer query + inner query, we expect a small number of live entries.
    assert!(
        live_count <= 10,
        "live query cache entries ({live_count}) should be bounded during active use, \
         but found {total_count} total cache entries — old entries are being kept alive",
    );

    drop(report_cell);
    ctx.sweep_dead_families();
    assert_eq!(ctx.query_rows(), 0, "all entries should be dead after drop");
}
