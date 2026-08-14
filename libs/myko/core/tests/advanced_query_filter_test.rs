//! Smoke tests for the generated XQuery/GetXsByQuery per-field-filter query
//! types (docs/superpowers/specs/2026-07-14-myko-5-query-api.md). Full
//! acceptance-criteria coverage (routing metrics, reactive tick, TS
//! round-trip, wasm) lands alongside the routing work.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchItem, BenchItemQuery, CountBenchItems, GetBenchItemsByQuery},
    hyphae::Gettable,
    query::{NumericFilter, StringFilter},
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
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

fn request(ctx: &MykoServerContext, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

/// hyphae's scheduler tick queue is process-wide, so cache-count assertions
/// across tests in this binary can perturb each other mid-flight — see the
/// matching helper (and full rationale) in `query_cache_leak_test.rs`.
fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn default_filter_matches_everything() {
    let ctx = make_ctx();
    insert_bench_item(&ctx, "a", "cat", 1);
    insert_bench_item(&ctx, "b", "cat", 2);

    let cell = ctx.query_map(
        GetBenchItemsByQuery(BenchItemQuery::default()),
        request(&ctx, "tx-1"),
    );
    assert_eq!(cell.snapshot().len(), 2);
}

#[test]
fn numeric_in_filter_matches_only_listed_values() {
    let ctx = make_ctx();
    insert_bench_item(&ctx, "a", "cat", 1);
    insert_bench_item(&ctx, "b", "cat", 2);
    insert_bench_item(&ctx, "c", "cat", 3);

    let filter = BenchItemQuery {
        value: Some(NumericFilter::In(vec![1, 3])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetBenchItemsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 2);
}

#[test]
fn numeric_range_filter_matches_inclusive_bounds() {
    let ctx = make_ctx();
    insert_bench_item(&ctx, "a", "cat", 1);
    insert_bench_item(&ctx, "b", "cat", 5);
    insert_bench_item(&ctx, "c", "cat", 10);

    let filter = BenchItemQuery {
        value: Some(NumericFilter::Range {
            min: Some(1),
            max: Some(5),
        }),
        ..Default::default()
    };
    let cell = ctx.query_map(GetBenchItemsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 2);
}

#[test]
fn permuted_in_filters_canonicalize_identically() {
    let a = BenchItemQuery {
        value: Some(NumericFilter::In(vec![3, 1, 2])),
        ..Default::default()
    }
    .canonicalize();
    let b = BenchItemQuery {
        value: Some(NumericFilter::In(vec![1, 2, 3, 2])),
        ..Default::default()
    }
    .canonicalize();
    assert_eq!(a, b);
}

#[test]
fn single_element_in_canonicalizes_to_eq() {
    let filter = BenchItemQuery {
        value: Some(NumericFilter::In(vec![7])),
        ..Default::default()
    }
    .canonicalize();
    assert_eq!(filter.value, Some(NumericFilter::Eq(7)));
}

// CountBenchItems migrated from CountBenchItems(PartialBenchItem) to
// CountBenchItems(BenchItemQuery) as part of the myko 5.0 rename (docs/
// superpowers/specs/2026-07-14-myko-5-query-api.md §3.2) — the one place
// that rename was a real behavior change (Eq-only matching -> full
// In/Range/Contains, plus a canonicalized CacheKey it never had before),
// not just an identifier swap. Nothing exercised it before this file.

#[test]
fn count_report_matches_only_filtered_items() {
    let ctx = make_ctx();
    insert_bench_item(&ctx, "a", "alpha", 1);
    insert_bench_item(&ctx, "b", "alpha", 2);
    insert_bench_item(&ctx, "c", "beta", 3);

    let filter = BenchItemQuery {
        category: Some(StringFilter::Eq("alpha".into())),
        ..Default::default()
    };
    let cell = ctx.report(CountBenchItems(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.get().count, 2);
}

#[test]
fn count_report_permuted_in_filters_share_one_report_cell() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_bench_item(&ctx, "a", "alpha", 1);
    insert_bench_item(&ctx, "b", "beta", 2);
    insert_bench_item(&ctx, "c", "gamma", 3);

    let before = ctx.report_cache_len();

    let filter_a = BenchItemQuery {
        category: Some(StringFilter::In(vec![
            Arc::from("alpha"),
            Arc::from("beta"),
        ])),
        ..Default::default()
    };
    let filter_b = BenchItemQuery {
        category: Some(StringFilter::In(vec![
            Arc::from("beta"),
            Arc::from("alpha"),
            Arc::from("beta"),
        ])),
        ..Default::default()
    };

    let cell_a = ctx.report(CountBenchItems(filter_a), request(&ctx, "tx-a"));
    assert_eq!(cell_a.get().count, 2);
    let after_a = ctx.report_cache_len();
    assert_eq!(after_a, before + 1, "first filter creates one report cell");

    let cell_b = ctx.report(CountBenchItems(filter_b), request(&ctx, "tx-b"));
    assert_eq!(cell_b.get().count, 2);
    let after_b = ctx.report_cache_len();
    assert_eq!(
        after_b, after_a,
        "permuted+duplicated In canonicalizes identically, so the second \
         call reuses the first filter's report cell instead of creating a new one"
    );
}
