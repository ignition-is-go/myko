//! Tests for query_map cache cleanup when used inside switch_map.
//!
//! Validates that the myko query cache and hyphae switch_map correctly
//! clean up cell factories and subscriptions. These tests were written
//! while investigating a 47 GB memory leak in rship_server triggered by
//! CuePlayback state changes. The tests prove the cache layer is clean —
//! the actual leak is in the O(n^2) reactive fan-out of the CueEngine
//! blanket watcher, not in cache accumulation.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchItem, GetBenchItemsByQuery, PartialBenchItem, SwitchMapReport},
    hyphae::{Cell, Gettable, Mutable, SwitchMapExt},
    search::SearchIndex,
    server::{CellServerCtx, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn make_ctx() -> CellServerCtx {
    CellServerCtx::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        Arc::new(dashmap::DashMap::new()),
        None,
        None,
    )
}

fn insert_bench_item(ctx: &CellServerCtx, id: &str, category: &str, value: i64) {
    let item = BenchItem {
        id: id.into(),
        name: format!("item-{}", id),
        category: category.to_string(),
        value,
    };
    let event = MEvent::from_item(&item, MEventType::SET, &format!("tx-{}", id));
    ctx.apply_event_batch(vec![event]).unwrap();
}

/// Verify that query_map results created inside switch_map are reclaimable
/// after the switch_map moves to a new inner chain.
///
/// This reproduces the CuePaused pattern:
/// ```rust,ignore
/// playbacks.switch_map(|playbacks| {
///     ctx.query_map(GetCuesByIds { ids: cue_ids }, req).items().map(...)
/// })
/// ```
#[test]
fn query_map_inside_switch_map_cache_entries_become_reclaimable() {
    let ctx = make_ctx();

    // Seed items in different categories
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("a-{}", i), "alpha", i);
        insert_bench_item(&ctx, &format!("b-{}", i), "beta", i);
        insert_bench_item(&ctx, &format!("g-{}", i), "gamma", i);
    }

    let categories = ["alpha", "beta", "gamma"];

    // Outer signal that triggers switch_map re-creation (simulates playback list changing)
    let selector = Cell::new(0usize);
    let ctx_clone = ctx.clone();

    let switched = selector.switch_map(move |idx| {
        let category = categories[*idx % categories.len()].to_string();
        let request = ctx_clone.new_server_transaction();
        // Each different category produces a different cache key (different payload_hash)
        let query_result = ctx_clone.query_map(
            GetBenchItemsByQuery(PartialBenchItem {
                category: Some(category),
                ..Default::default()
            }),
            request,
        );
        query_result.items()
    });

    // Initial: should see alpha items
    assert_eq!(switched.get().len(), 10);

    let cache_before = ctx.query_cache_len();

    // Switch through many different query params — each creates a new cache entry
    // because the category field changes the payload_hash
    for i in 1..=30 {
        selector.set(i);
    }

    // The switched cell should have the latest result
    assert_eq!(switched.get().len(), 10);

    // Cache has accumulated entries (some may be dead weak refs now)
    let cache_after = ctx.query_cache_len();
    assert!(
        cache_after >= cache_before,
        "cache should have grown or stayed the same"
    );

    // Now drop the switched cell — all inner query_maps should become reclaimable
    drop(switched);
    drop(selector);

    // Sweep should reclaim dead entries
    let (q_removed, _, _) = ctx.sweep_dead_cache_entries();
    let cache_final = ctx.query_cache_len();

    // After dropping all references, sweep should clean up most/all cache entries
    assert!(
        q_removed > 0,
        "sweep should have removed dead query cache entries, but removed 0 \
         (cache before={}, after switch={}, final={})",
        cache_before,
        cache_after,
        cache_final,
    );
}

/// Verify that repeated query_map calls with the SAME params inside switch_map
/// reuse the cache entry (cache hit) rather than creating new factories.
#[test]
fn query_map_same_params_inside_switch_map_reuses_cache() {
    let ctx = make_ctx();

    for i in 0..5 {
        insert_bench_item(&ctx, &format!("item-{}", i), "tools", i);
    }

    let trigger = Cell::new(0u64);
    let ctx_clone = ctx.clone();

    // Every switch creates query_map with SAME params — should cache-hit
    let switched = trigger.switch_map(move |_| {
        let request = ctx_clone.new_server_transaction();
        ctx_clone
            .query_map(
                GetBenchItemsByQuery(PartialBenchItem {
                    category: Some("tools".to_string()),
                    ..Default::default()
                }),
                request,
            )
            .items()
    });

    assert_eq!(switched.get().len(), 5);
    let cache_after_init = ctx.query_cache_len();

    // Switch 50 times with same query params
    for i in 1..=50 {
        trigger.set(i);
    }

    assert_eq!(switched.get().len(), 5);

    // Cache should NOT have grown significantly — same params = same cache key
    // (as long as the previous CellMap is still alive when we look it up)
    let cache_after_switches = ctx.query_cache_len();

    // Allow some growth due to weak refs dying between switches,
    // but it should NOT be 50 new entries
    assert!(
        cache_after_switches <= cache_after_init + 5,
        "cache grew from {} to {} after 50 same-param switches — \
         expected cache reuse, got accumulation",
        cache_after_init,
        cache_after_switches,
    );
}

/// Reproduce the production leak: switch_map creates query_map().items()
/// while the underlying store is actively receiving updates. Store mutations
/// cause the CellMap's internal subscriptions to fire, which may keep the
/// CellMap alive (via map_keepalive in items()) even after switch_map replaces
/// the inner chain.
#[test]
fn query_map_inside_switch_map_with_active_store_mutations() {
    let ctx = make_ctx();

    // Seed initial items
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("a-{}", i), "alpha", i);
        insert_bench_item(&ctx, &format!("b-{}", i), "beta", i);
    }

    let categories = ["alpha", "beta"];
    let selector = Cell::new(0usize);
    let ctx_clone = ctx.clone();

    let switched = selector.switch_map(move |idx| {
        let category = categories[*idx % categories.len()].to_string();
        let request = ctx_clone.new_server_transaction();
        ctx_clone
            .query_map(
                GetBenchItemsByQuery(PartialBenchItem {
                    category: Some(category),
                    ..Default::default()
                }),
                request,
            )
            .items()
    });

    assert_eq!(switched.get().len(), 10);

    // Interleave switches with store mutations (simulating playback state changes
    // that trigger both the outer signal AND entity updates)
    for round in 0..20 {
        // Switch to new category
        selector.set(round + 1);

        // Mutate items in the store (simulates CuePlayback state changes)
        let cat = categories[round % categories.len()];
        for i in 0..5 {
            insert_bench_item(
                &ctx,
                &format!("{}-{}", cat.chars().next().unwrap(), i),
                cat,
                (round * 10 + i) as i64,
            );
        }
    }

    let live_before_drop = ctx.query_cache_live_count();
    let total_before_drop = ctx.query_cache_len();

    // Drop the switch_map chain
    drop(switched);
    drop(selector);

    // Sweep
    let (q_removed, _, _) = ctx.sweep_dead_cache_entries();
    let live_after = ctx.query_cache_live_count();

    // All query cache entries should be reclaimable after dropping the switch_map
    assert_eq!(
        live_after, 0,
        "expected 0 live cache entries after dropping switch_map, found {} \
         (total before drop={}, live before drop={}, swept={})",
        live_after, total_before_drop, live_before_drop, q_removed,
    );
}

/// Verify the query_cache_live_count accurately reflects reachable entries.
#[test]
fn query_cache_live_count_tracks_reachable_entries() {
    let ctx = make_ctx();

    for i in 0..3 {
        insert_bench_item(&ctx, &format!("item-{}", i), "test", i);
    }

    assert_eq!(ctx.query_cache_live_count(), 0);

    // Create a query_map and hold onto it
    let request = ctx.new_server_transaction();
    let map = ctx.query_map(
        GetBenchItemsByQuery(PartialBenchItem {
            category: Some("test".to_string()),
            ..Default::default()
        }),
        request,
    );

    assert_eq!(ctx.query_cache_live_count(), 1);

    // Create another with different params
    let request2 = ctx.new_server_transaction();
    let map2 = ctx.query_map(
        GetBenchItemsByQuery(PartialBenchItem {
            category: Some("other".to_string()),
            ..Default::default()
        }),
        request2,
    );

    assert_eq!(ctx.query_cache_live_count(), 2);

    // Drop one — live count should decrease after sweep
    drop(map);
    ctx.sweep_dead_cache_entries();
    assert_eq!(ctx.query_cache_live_count(), 1);

    // Drop the other
    drop(map2);
    ctx.sweep_dead_cache_entries();
    assert_eq!(ctx.query_cache_live_count(), 0);
}

// =============================================================================
// Report-level tests: ctx.report() with switch_map + nested query_map
// =============================================================================

/// Core reproduction of the CuePaused leak: a report whose compute() uses
/// switch_map with a nested query_map. Store mutations trigger the outer
/// query, switch_map recreates the inner query_map, and old cache entries
/// must be reclaimable.
#[test]
fn report_with_switch_map_query_map_cleans_up_cache() {
    let ctx = make_ctx();

    // Seed items
    for i in 0..10 {
        insert_bench_item(&ctx, &format!("item-{}", i), "alpha", i);
    }

    let cache_before = ctx.query_cache_len();
    let report_cache_before = ctx.report_cache_len();

    // Create the report — this calls SwitchMapReport::compute() which sets up
    // the switch_map + nested query_map chain
    let request = ctx.new_server_transaction();
    let report_cell = ctx.report(
        SwitchMapReport {
            category: "alpha".to_string(),
        },
        request,
    );

    // Should see all 10 item names
    assert_eq!(report_cell.get().len(), 10);

    let cache_after_report = ctx.query_cache_len();
    let report_cache_after_report = ctx.report_cache_len();

    // Report should have created cache entries
    assert!(cache_after_report > cache_before);
    assert!(report_cache_after_report > report_cache_before);

    // Now mutate items to trigger the outer query → switch_map → new inner query_map
    // Each mutation changes the item list, causing switch_map to recreate
    for round in 0..20 {
        insert_bench_item(&ctx, &format!("new-{}", round), "alpha", 100 + round as i64);
    }

    // Report should reflect the new items
    assert_eq!(report_cell.get().len(), 30); // 10 original + 20 new

    let cache_during = ctx.query_cache_len();

    // Drop the report cell — everything should become reclaimable
    drop(report_cell);

    let (q_swept, _, r_swept) = ctx.sweep_dead_cache_entries();
    let cache_final = ctx.query_cache_live_count();
    let report_cache_final = ctx.report_cache_live_count();

    assert_eq!(
        cache_final, 0,
        "expected 0 live query cache entries after dropping report, found {} \
         (cache grew to {} during mutations, swept {})",
        cache_final, cache_during, q_swept,
    );
    assert_eq!(
        report_cache_final, 0,
        "expected 0 live report cache entries after dropping report, found {} (swept {})",
        report_cache_final, r_swept,
    );
}

/// Stress test: many rounds of store mutations while report is alive.
/// Verifies cache doesn't grow unboundedly during active use.
#[test]
fn report_switch_map_cache_bounded_during_active_mutations() {
    let ctx = make_ctx();

    for i in 0..5 {
        insert_bench_item(&ctx, &format!("item-{}", i), "beta", i);
    }

    let request = ctx.new_server_transaction();
    let report_cell = ctx.report(
        SwitchMapReport {
            category: "beta".to_string(),
        },
        request,
    );

    assert_eq!(report_cell.get().len(), 5);

    // Run 100 rounds of mutations — each triggers switch_map to recreate
    // the inner query_map with new IDs
    for round in 0..100 {
        insert_bench_item(
            &ctx,
            &format!("stress-{}", round),
            "beta",
            1000 + round as i64,
        );
    }

    assert_eq!(report_cell.get().len(), 105); // 5 + 100

    // Sweep mid-flight to reclaim dead entries
    ctx.sweep_dead_cache_entries();

    let live_count = ctx.query_cache_live_count();
    let total_count = ctx.query_cache_len();

    // The live count should be bounded — only the currently-active query_maps
    // should be alive, not all 100+ historical ones.
    // With the outer query + inner query, we expect a small number of live entries.
    assert!(
        live_count <= 10,
        "live query cache entries ({}) should be bounded during active use, \
         but found {} total cache entries — old entries are being kept alive",
        live_count,
        total_count,
    );

    drop(report_cell);
    ctx.sweep_dead_cache_entries();
    assert_eq!(
        ctx.query_cache_live_count(),
        0,
        "all entries should be dead after drop"
    );
}
