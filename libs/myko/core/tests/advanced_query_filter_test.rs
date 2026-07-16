//! Smoke tests for the generated XQuery/GetXsByQuery per-field-filter query
//! types (docs/superpowers/specs/2026-07-14-myko-5-query-api.md). Full
//! acceptance-criteria coverage (routing metrics, reactive tick, TS
//! round-trip, wasm) lands alongside the routing work.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchItem, BenchItemQuery, GetBenchItemsByQuery},
    query::NumericFilter,
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
        Arc::new(myko::search::SearchIndex::new()),
        Arc::new(dashmap::DashMap::new()),
        None,
        None,
    )
}

fn insert_bench_item(ctx: &CellServerCtx, id: &str, category: &str, value: i64) {
    let item = BenchItem {
        id: id.into(),
        name: format!("item-{id}"),
        category: category.to_string(),
        value,
    };
    let event = MEvent::from_item(&item, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn request(ctx: &CellServerCtx, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
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
