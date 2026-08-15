//! Graph runtime benchmark matrix: opt-out overhead, write projection cost,
//! cold/hot adjacency lookup, and mutation-authority contention.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::significant_drop_tightening
)]

use std::{hint::black_box, sync::Arc};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hyphae::{MapQuery, SelectExt};
use myko::{
    bench_entities::{
        BenchForwardGraphEdge, BenchForwardGraphEdgeId, BenchGraphEdge, BenchGraphEdgeId,
        BenchGraphNode, BenchGraphNodeId, BenchItem, BenchItemId,
    },
    core::item::downcast_any_item_arc,
    prelude::*,
    query::QueryRequest,
    request::RequestContext,
    search::SearchIndex,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, PersisterRouter, RelationshipManager,
    },
    store::StoreRegistry,
};

const N: usize = 1_000;
type BenchGraphFromQuery = <BenchGraphEdge as GraphClientQueries>::FromQuery;

fn context(graph: bool) -> MykoServerContext {
    let context = MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    );
    if graph {
        context
    } else {
        context.without_graph_for_benchmark()
    }
}

fn nodes(n: usize) -> Vec<BenchGraphNode> {
    (0..n)
        .map(|ordinal| BenchGraphNode {
            id: BenchGraphNodeId::from(format!("node-{ordinal}")),
            ordinal: i64::try_from(ordinal).unwrap_or(i64::MAX),
        })
        .collect()
}

fn edges(n: usize) -> Vec<BenchGraphEdge> {
    (0..n)
        .map(|ordinal| BenchGraphEdge {
            id: BenchGraphEdgeId::from(format!("edge-{ordinal}")),
            from_id: BenchGraphNodeId::from("node-0"),
            to_id: BenchGraphNodeId::from(format!("node-{}", ordinal.saturating_add(1))),
        })
        .collect()
}

fn distributed_edges(n: usize, sources: usize) -> Vec<BenchGraphEdge> {
    (0..n)
        .map(|ordinal| BenchGraphEdge {
            id: BenchGraphEdgeId::from(format!("distributed-edge-{ordinal}")),
            from_id: BenchGraphNodeId::from(format!("node-{}", ordinal % sources)),
            to_id: BenchGraphNodeId::from(format!("node-{}", sources + ordinal)),
        })
        .collect()
}

fn forward_edges(n: usize) -> Vec<BenchForwardGraphEdge> {
    (0..n)
        .map(|ordinal| BenchForwardGraphEdge {
            id: BenchForwardGraphEdgeId::from(format!("forward-edge-{ordinal}")),
            from_id: BenchGraphNodeId::from("node-0"),
            to_id: BenchGraphNodeId::from(format!("node-{}", ordinal.saturating_add(1))),
        })
        .collect()
}

fn distributed_forward_edges(n: usize, sources: usize) -> Vec<BenchForwardGraphEdge> {
    (0..n)
        .map(|ordinal| BenchForwardGraphEdge {
            id: BenchForwardGraphEdgeId::from(format!("distributed-forward-edge-{ordinal}")),
            from_id: BenchGraphNodeId::from(format!("node-{}", ordinal % sources)),
            to_id: BenchGraphNodeId::from(format!("node-{}", sources + ordinal)),
        })
        .collect()
}

fn seeded(n: usize) -> MykoServerContext {
    let context = context(true);
    let all_nodes = nodes(n.saturating_add(1));
    context.batch_set(&all_nodes).expect("seed graph nodes");
    context.batch_set(&edges(n)).expect("seed graph edges");
    context
}

fn bench_zero_registration_overhead(c: &mut Criterion) {
    let with_catalog = context(true);
    let without_catalog = context(false);
    let mut ordinal = 0_u64;
    let mut group = c.benchmark_group("graph/zero_registration_fast_path");
    for (name, context) in [
        ("catalog_present", with_catalog),
        ("catalog_absent", without_catalog),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                ordinal = ordinal.saturating_add(1);
                context
                    .set(&BenchItem {
                        id: BenchItemId::from(format!("plain-{name}-{ordinal}")),
                        name: "plain".to_string(),
                        category: "control".to_string(),
                        value: i64::try_from(ordinal).unwrap_or(i64::MAX),
                    })
                    .expect("ordinary item set");
            });
        });
    }
    group.finish();
}

fn bench_projection_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph/batch_write");
    group.throughput(Throughput::Elements(u64::try_from(N).unwrap_or(u64::MAX)));
    for graph in [false, true] {
        group.bench_with_input(BenchmarkId::from_parameter(graph), &graph, |b, graph| {
            b.iter_batched(
                || {
                    let context = context(*graph);
                    let all_nodes = nodes(N.saturating_add(1));
                    context.batch_set(&all_nodes).expect("seed graph nodes");
                    (context, edges(N))
                },
                |(context, edges)| context.batch_set(black_box(&edges)).expect("edge batch"),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();

    let mut shape = c.benchmark_group("graph/batch_write_projection_shape");
    shape.throughput(Throughput::Elements(u64::try_from(N).unwrap_or(u64::MAX)));
    shape.bench_function("both_endpoints", |b| {
        b.iter_batched(
            || {
                let context = context(true);
                context
                    .batch_set(&nodes(N.saturating_add(1)))
                    .expect("seed graph nodes");
                (context, edges(N))
            },
            |(context, edges)| context.batch_set(black_box(&edges)).expect("edge batch"),
            BatchSize::LargeInput,
        );
    });
    shape.bench_function("a_endpoint_only", |b| {
        b.iter_batched(
            || {
                let context = context(true);
                context
                    .batch_set(&nodes(N.saturating_add(1)))
                    .expect("seed graph nodes");
                (context, forward_edges(N))
            },
            |(context, edges)| context.batch_set(black_box(&edges)).expect("edge batch"),
            BatchSize::LargeInput,
        );
    });
    shape.finish();
}

fn bench_adjacency_lookup(c: &mut Criterion) {
    let high_degree_context = seeded(N);
    let from = BenchGraphNodeId::from("node-0");
    let store = high_degree_context.registry.get_or_create("BenchGraphEdge");
    let mut group = c.benchmark_group("graph/one_hop_lookup");
    group.throughput(Throughput::Elements(u64::try_from(N).unwrap_or(u64::MAX)));
    group.bench_function("canonical_full_scan", |b| {
        b.iter(|| {
            let matches = store
                .snapshot()
                .into_iter()
                .filter_map(|(_, item)| {
                    downcast_any_item_arc::<BenchGraphEdge>(&item, "graph benchmark")
                })
                .filter(|edge| edge.from_id == from)
                .count();
            black_box(matches)
        });
    });
    group.bench_function("eager_adjacency", |b| {
        b.iter(|| {
            black_box(
                high_degree_context
                    .edges::<BenchGraphEdge>()
                    .from(&from)
                    .expect("adjacency lookup")
                    .len(),
            )
        });
    });
    group.finish();

    let sparse_n = 10_000_usize;
    let sparse_sources = 1_000_usize;
    let sparse_context = context(true);
    sparse_context
        .batch_set(&nodes(sparse_n.saturating_add(sparse_sources)))
        .expect("seed sparse graph nodes");
    sparse_context
        .batch_set(&distributed_edges(sparse_n, sparse_sources))
        .expect("seed sparse graph edges");
    let sparse_store = sparse_context.registry.get_or_create("BenchGraphEdge");
    let sparse_from = BenchGraphNodeId::from("node-0");
    let mut sparse = c.benchmark_group("graph/one_hop_sparse_lookup");
    sparse.bench_function("canonical_full_scan", |b| {
        b.iter(|| {
            black_box(
                sparse_store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        downcast_any_item_arc::<BenchGraphEdge>(&item, "graph benchmark")
                    })
                    .filter(|edge| edge.from_id == sparse_from)
                    .count(),
            )
        });
    });
    sparse.bench_function("eager_adjacency", |b| {
        b.iter(|| {
            black_box(
                sparse_context
                    .edges::<BenchGraphEdge>()
                    .from(&sparse_from)
                    .expect("sparse adjacency lookup")
                    .len(),
            )
        });
    });
    sparse.finish();

    let forward_context = context(true);
    forward_context
        .batch_set(&nodes(sparse_n.saturating_add(sparse_sources)))
        .expect("seed one-sided graph nodes");
    forward_context
        .batch_set(&distributed_forward_edges(sparse_n, sparse_sources))
        .expect("seed one-sided graph edges");
    let mut shape = c.benchmark_group("graph/one_hop_sparse_projection_shape");
    shape.bench_function("both_endpoints", |b| {
        b.iter(|| {
            black_box(
                sparse_context
                    .edges::<BenchGraphEdge>()
                    .from_ids(&sparse_from)
                    .expect("both-end A lookup")
                    .len(),
            )
        });
    });
    shape.bench_function("a_endpoint_only", |b| {
        b.iter(|| {
            black_box(
                forward_context
                    .edges::<BenchForwardGraphEdge>()
                    .from_ids(&sparse_from)
                    .expect("one-sided A lookup")
                    .len(),
            )
        });
    });
    shape.finish();
}

fn bench_watch_initialization(c: &mut Criterion) {
    let edge_count = 10_000_usize;
    let source_count = 1_000_usize;
    let context = context(true);
    context
        .batch_set(&nodes(edge_count.saturating_add(source_count)))
        .expect("seed watch graph nodes");
    context
        .batch_set(&distributed_edges(edge_count, source_count))
        .expect("seed watch graph edges");
    let store = context.registry.get_or_create("BenchGraphEdge");
    let from = BenchGraphNodeId::from("node-0");
    let request = Arc::new(RequestContext::from_client(
        "graph-benchmark".into(),
        "graph-benchmark-client".into(),
        context.host_id,
    ));
    let query: Arc<dyn AnyQuery> = Arc::new(QueryRequest::with_tx(
        BenchGraphEdge::from_query(&from),
        request.tx.clone(),
    ));
    let server = Arc::new(context.clone());

    let mut group = c.benchmark_group("graph/sparse_watch_initialization");
    group.bench_function("canonical_select", |b| {
        b.iter(|| {
            let from = from.clone();
            let selected = MapQuery::materialize((*store).clone().select(move |item| {
                downcast_any_item_arc::<BenchGraphEdge>(item, "graph watch benchmark")
                    .is_some_and(|edge| edge.from_id == from)
            }));
            black_box(selected.snapshot().len())
        });
    });
    group.bench_function("index_seeded", |b| {
        b.iter(|| {
            let watched = context
                .edges::<BenchGraphEdge>()
                .watch_from(&from)
                .expect("index-seeded watch");
            black_box(watched.snapshot().len())
        });
    });
    group.bench_function("ordinary_query_factory", |b| {
        b.iter(|| {
            let watched = <BenchGraphFromQuery as QueryFactory>::cell_factory(
                query.clone(),
                context.registry.clone(),
                request.clone(),
                Some(server.clone()),
            )
            .expect("generated graph query");
            black_box(watched.snapshot().len())
        });
    });
    group.finish();
}

fn bench_exact_pair_lookup(c: &mut Criterion) {
    let pair_n = 10_000_usize;
    let context = seeded(pair_n);
    let from = BenchGraphNodeId::from("node-0");
    let to = BenchGraphNodeId::from(format!("node-{pair_n}"));
    let store = context.registry.get_or_create("BenchGraphEdge");
    let mut group = c.benchmark_group("graph/exact_pair_lookup");

    group.bench_function("canonical_full_scan_exists", |b| {
        b.iter(|| {
            black_box(store.snapshot().into_iter().any(|(_, item)| {
                downcast_any_item_arc::<BenchGraphEdge>(&item, "graph benchmark")
                    .is_some_and(|edge| edge.from_id == from && edge.to_id == to)
            }))
        });
    });
    group.bench_function("pair_materialized", |b| {
        b.iter(|| {
            black_box(
                context
                    .edges::<BenchGraphEdge>()
                    .one_between(&from, &to)
                    .expect("materialized pair lookup"),
            )
        });
    });
    group.bench_function("pair_id", |b| {
        b.iter(|| {
            black_box(
                context
                    .edges::<BenchGraphEdge>()
                    .between_id(&from, &to)
                    .expect("pair ID lookup"),
            )
        });
    });
    group.bench_function("pair_exists", |b| {
        b.iter(|| {
            black_box(
                context
                    .edges::<BenchGraphEdge>()
                    .exists_between(&from, &to)
                    .expect("pair existence lookup"),
            )
        });
    });
    group.finish();
}

fn bench_endpoint_delete_plan(c: &mut Criterion) {
    let edge_count = 10_000_usize;
    let source_count = 1_000_usize;
    let context = context(true);
    context
        .batch_set(&nodes(edge_count.saturating_add(source_count)))
        .expect("seed endpoint-delete nodes");
    context
        .batch_set(&distributed_edges(edge_count, source_count))
        .expect("seed endpoint-delete edges");
    let store = context.registry.get_or_create("BenchGraphEdge");
    let endpoint_id = BenchGraphNodeId::from("node-0");
    let endpoint = EntityRef::new("BenchGraphNode", endpoint_id.as_ref());
    let graph = context.graph_index().expect("graph index");
    let mut group = c.benchmark_group("graph/endpoint_delete_sparse");

    // This is deliberately a conservative baseline: it scans only the one
    // populated store and performs a typed predicate, while the pre-index
    // runtime dynamically extracted every edge from every registered store.
    group.bench_function("canonical_full_scan", |b| {
        b.iter(|| {
            black_box(
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        downcast_any_item_arc::<BenchGraphEdge>(&item, "graph benchmark")
                    })
                    .filter(|edge| edge.from_id == endpoint_id || edge.to_id == endpoint_id)
                    .count(),
            )
        });
    });
    group.bench_function("eager_incidence_plan", |b| {
        b.iter(|| {
            black_box(
                graph
                    .endpoint_delete_plan(&endpoint)
                    .expect("endpoint delete plan")
                    .cascade_edges
                    .len(),
            )
        });
    });
    group.finish();
}

fn bench_authority_contention(c: &mut Criterion) {
    c.bench_function("graph/two_writer_authority_contention", |b| {
        b.iter_batched(
            || {
                let context = Arc::new(context(true));
                context.batch_set(&nodes(5)).expect("seed graph nodes");
                context
            },
            |context| {
                let mut joins = Vec::new();
                for writer in 0..2 {
                    let context = context.clone();
                    joins.push(std::thread::spawn(move || {
                        for ordinal in 0..100 {
                            context
                                .set(&BenchGraphEdge {
                                    id: BenchGraphEdgeId::from(format!("{writer}-{ordinal}")),
                                    from_id: BenchGraphNodeId::from(format!("node-{writer}")),
                                    to_id: BenchGraphNodeId::from(format!("node-{}", writer + 2)),
                                })
                                .ok();
                        }
                    }));
                }
                for join in joins {
                    join.join().expect("writer thread");
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_zero_registration_overhead,
    bench_projection_write,
    bench_adjacency_lookup,
    bench_watch_initialization,
    bench_exact_pair_lookup,
    bench_endpoint_delete_plan,
    bench_authority_contention,
);
criterion_main!(benches);
