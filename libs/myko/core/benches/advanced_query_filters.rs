//! Benchmarks for the two concrete improvements
//! docs/superpowers/specs/2026-07-13-advanced-query-design.md targets:
//!
//! - Phase 1: N separate per-value query cells (the rship-audit workaround
//!   shape) collapsing into one `In`-filter cell.
//! - Phase 2: a filter change costing a full query reconstruction (what
//!   wrapping a query in `switch_map` forces on every tick) collapsing
//!   into `query_live`'s incremental bucket-diff — only the delta buckets
//!   are touched, not the whole existing set.

use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use hyphae::{Cell, CellMutable, Mutable};
use myko::{
    entities::{
        client::{Client, ClientQuery, GetClientsByQuery},
        server::ServerId,
    },
    query::IdFilter,
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
        Arc::new(dashmap::DashMap::new()),
        None,
        None,
    )
}

fn insert_client(ctx: &MykoServerContext, id: &str, server_id: &str) {
    let client = Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: None,
        windback: None,
    };
    let event = MEvent::from_item(&client, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn request(ctx: &MykoServerContext, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 1: N `Eq` cells (the audit's "one query cell per value" shape) vs
// one `In` cell covering the same N values.
// ─────────────────────────────────────────────────────────────────────────

fn bench_n_cells_vs_in_filter(c: &mut Criterion) {
    const N: usize = 50;
    let ctx = make_ctx();
    for i in 0..N {
        insert_client(&ctx, &format!("c{i}"), &format!("server-{i}"));
    }
    let server_ids: Vec<String> = (0..N).map(|i| format!("server-{i}")).collect();

    let mut g = c.benchmark_group("n_values_query_construction");
    g.throughput(criterion::Throughput::Elements(N as u64));

    g.bench_function("n_separate_eq_cells", |b| {
        b.iter(|| {
            let cells: Vec<_> = server_ids
                .iter()
                .enumerate()
                .map(|(i, sid)| {
                    let filter = ClientQuery {
                        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from(
                            sid.as_str(),
                        )))),
                        ..Default::default()
                    };
                    ctx.query_map(GetClientsByQuery(filter), request(&ctx, &format!("tx-{i}")))
                })
                .collect();
            std::hint::black_box(cells)
        })
    });

    g.bench_function("one_in_filter_cell", |b| {
        b.iter(|| {
            let filter = ClientQuery {
                server_id: Some(IdFilter::In(
                    server_ids
                        .iter()
                        .map(|s| ServerId::from(Arc::<str>::from(s.as_str())))
                        .collect(),
                )),
                ..Default::default()
            };
            let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-in"));
            std::hint::black_box(cell)
        })
    });

    g.finish();
}

// ─────────────────────────────────────────────────────────────────────────
// Phase 2: cost of a filter CHANGE that adds one more value to an already-
// large `In` set — full reconstruction (what switch_map forces on every
// tick) vs query_live's incremental bucket-diff (only the new key's bucket
// gets subscribed; the pre-existing buckets are left completely alone).
// ─────────────────────────────────────────────────────────────────────────

fn bench_filter_change_cost(c: &mut Criterion) {
    const MAX_EXISTING: usize = 200;
    let ctx = make_ctx();
    for i in 0..MAX_EXISTING {
        insert_client(&ctx, &format!("c{i}"), &format!("server-{i}"));
    }
    insert_client(&ctx, "c-new", "server-new");

    let mut g = c.benchmark_group("filter_change_cost");

    for &existing in &[10usize, 50, 100, 200] {
        g.throughput(criterion::Throughput::Elements(existing as u64));

        g.bench_with_input(
            BenchmarkId::new("full_rebuild_like_switch_map", existing),
            &existing,
            |b, &existing| {
                let base_ids: Vec<String> = (0..existing).map(|i| format!("server-{i}")).collect();
                let mut counter = 0u64;
                b.iter(|| {
                    // A unique marker id each iteration guarantees this is
                    // never a query-cache hit — genuinely simulating a
                    // fresh reconstruction on every "tick", exactly what
                    // wrapping query_map in switch_map forces since a
                    // changed filter value is a new cache key every time.
                    counter += 1;
                    let mut ids: Vec<ServerId> = base_ids
                        .iter()
                        .map(|s| ServerId::from(Arc::<str>::from(s.as_str())))
                        .collect();
                    ids.push(ServerId::from(Arc::<str>::from(format!(
                        "unique-{counter}"
                    ))));
                    let filter = ClientQuery {
                        server_id: Some(IdFilter::In(ids)),
                        ..Default::default()
                    };
                    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx"));
                    std::hint::black_box(cell)
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("query_live_incremental_add_one", existing),
            &existing,
            |b, &existing| {
                b.iter_batched(
                    || {
                        let ids: Vec<ServerId> = (0..existing)
                            .map(|i| ServerId::from(Arc::<str>::from(format!("server-{i}"))))
                            .collect();
                        let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(ClientQuery {
                            server_id: Some(IdFilter::In(ids.clone())),
                            ..Default::default()
                        });
                        let result = ctx.query_live(filter_cell.clone());
                        (filter_cell, ids, result)
                    },
                    |(filter_cell, mut ids, result)| {
                        // The operation actually being measured: add ONE
                        // more key to the existing In set. The `existing`
                        // already-subscribed buckets must be left alone —
                        // this is the whole point of the incremental-diff
                        // design (see registration.rs's query_live).
                        ids.push(ServerId::from(Arc::<str>::from("server-new")));
                        filter_cell.set(ClientQuery {
                            server_id: Some(IdFilter::In(ids)),
                            ..Default::default()
                        });
                        std::hint::black_box(result)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    g.finish();
}

criterion_group!(
    benches,
    bench_n_cells_vs_in_filter,
    bench_filter_change_cost
);
criterion_main!(benches);
