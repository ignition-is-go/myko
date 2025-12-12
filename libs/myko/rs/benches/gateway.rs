//! Gateway benchmarks for myko-rs
//!
//! Run with: cargo bench --package myko-rs --features bench
//!
//! Benchmarks:
//! - Query performance over large entity lists
//! - Command-to-query-update latency
//! - Event throughput under load

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::stream;
use futures::StreamExt;
use futures_signals::signal_map::MapDiff;
use myko_rs::{
    actors::{
        event::{
            common::{PersistEvent, ProcessEventData},
            event_manager::EventManagerMsg,
        },
        query::query_manager::QueryManagerMsg,
    },
    api::query::WrappedQuery,
    bench_entities::{
        BenchItem, GetAllBenchItems, GetAllBenchItemsArgs, GetBenchItemsByCategory,
        GetBenchItemsByCategoryArgs,
    },
    event::{MEvent, MEventType},
    item::Eventable,
    prelude::AnyItem,
    query::Query,
    saga::SagaStreamExt,
    server::{ManagerRefs, MykoServer, MykoServerArgs},
    utils::signal_stream::SignalMapStream,
};
use std::{sync::Arc, time::Duration};
use tokio::runtime::Runtime;
use uuid::Uuid;

// =============================================================================
// Benchmark Harness (simplified using MykoServer)
// =============================================================================

struct BenchHarness {
    #[allow(dead_code)]
    server: Arc<MykoServer>,
    managers: ManagerRefs,
}

impl BenchHarness {
    async fn new() -> Self {
        // Create server with no Kafka (in-memory mode)
        let server = MykoServer::init(MykoServerArgs {
            bind_addr: "127.0.0.1".to_string(),
            bind_path: "/".to_string(),
            bind_port: 0, // Don't actually bind
            kafka_config: None, // In-memory mode
            public_host_address: "127.0.0.1".to_string(),
        })
        .await
        .expect("Failed to create MykoServer");

        // Register benchmark entities
        BenchItem::register(&server).expect("Failed to register BenchItem");
        GetBenchItemsByCategory::register(&server).expect("Failed to register GetBenchItemsByCategory");

        // Get manager refs for direct access (now synchronous)
        let managers = server.get_managers();

        // Initialize (in-memory mode signals caught up immediately)
        server.init_modules().expect("Failed to init");

        // Small delay to let actors initialize
        tokio::time::sleep(Duration::from_millis(50)).await;

        Self { server, managers }
    }

    /// Populate with N items across categories
    async fn populate(&self, count: usize, categories: &[&str]) {
        for i in 0..count {
            let category = categories[i % categories.len()];
            let item = BenchItem {
                id: format!("bench-{}", i).into(),
                hash: Uuid::new_v4().to_string().into(),
                name: format!("Item {}", i),
                category: category.to_string(),
                value: i as i64,
            };

            self.emit_set(&item).await;
        }

        // Let events propagate
        tokio::time::sleep(Duration::from_millis(50 + (count as u64 / 100))).await;
    }

    /// Emit a single SET event
    async fn emit_set(&self, item: &BenchItem) {
        let event = MEvent::from_item(item, MEventType::SET, Uuid::new_v4().to_string());

        // Use the optimized path with pre-parsed item
        let parsed_item: Arc<dyn AnyItem> = Arc::new(item.clone());

        self.managers
            .event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::NoPersist,
                parsed_item: Some(parsed_item),
            }))
            .expect("Failed to send event");
    }

    /// Start a query and get initial result count
    async fn start_query(&self, query: WrappedQuery) -> usize {
        let signal = ractor::call!(self.managers.query_manager, QueryManagerMsg::StartQuery, query)
            .expect("Failed to start query");

        let mut stream = SignalMapStream::new(signal);
        if let Some(diff) = stream.next().await {
            if let MapDiff::Replace { entries } = diff {
                return entries.len();
            }
        }
        0
    }
}

// =============================================================================
// Benchmarks
// =============================================================================

fn query_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("query");
    group.sample_size(50);

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Pre-create harness outside of async context
        let harness = rt.block_on(async {
            let h = BenchHarness::new().await;
            h.populate(*size, &["cat-a", "cat-b", "cat-c"]).await;
            h
        });
        let harness = Arc::new(harness);

        let h = harness.clone();
        group.bench_with_input(BenchmarkId::new("get_all", size), size, move |b, _size| {
            let harness = h.clone();
            b.to_async(Runtime::new().unwrap()).iter(|| {
                let harness = harness.clone();
                async move {
                    let query = GetAllBenchItems::new(GetAllBenchItemsArgs {});
                    let wrapped = WrappedQuery {
                        query: serde_json::to_value(&query).unwrap(),
                        query_id: "GetAllBenchItems".into(),
                        query_item_type: "BenchItem".into(),
                    };
                    black_box(harness.start_query(wrapped).await)
                }
            });
        });

        // Create new harness for category query
        let harness = rt.block_on(async {
            let h = BenchHarness::new().await;
            h.populate(*size, &["cat-a", "cat-b", "cat-c"]).await;
            h
        });
        let harness = Arc::new(harness);

        let h = harness.clone();
        group.bench_with_input(
            BenchmarkId::new("get_by_category", size),
            size,
            move |b, _size| {
                let harness = h.clone();
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let harness = harness.clone();
                    async move {
                        let query = GetBenchItemsByCategory::new(GetBenchItemsByCategoryArgs {
                            category: "cat-a".to_string(),
                        });
                        let wrapped = WrappedQuery {
                            query: serde_json::to_value(&query).unwrap(),
                            query_id: "GetBenchItemsByCategory".into(),
                            query_item_type: "BenchItem".into(),
                        };
                        black_box(harness.start_query(wrapped).await)
                    }
                });
            },
        );
    }

    group.finish();
}

fn event_throughput_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("event_throughput");
    group.sample_size(50);

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        // Pre-create harness and items
        let harness = rt.block_on(BenchHarness::new());
        let harness = Arc::new(harness);
        let items: Vec<BenchItem> = (0..*batch_size)
            .map(|i| BenchItem {
                id: format!("batch-{}", i).into(),
                hash: Uuid::new_v4().to_string().into(),
                name: format!("Item {}", i),
                category: "batch".to_string(),
                value: i as i64,
            })
            .collect();

        let h = harness.clone();
        group.bench_with_input(
            BenchmarkId::new("emit_batch", batch_size),
            batch_size,
            move |b, _batch_size| {
                let harness = h.clone();
                let items = items.clone();
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let harness = harness.clone();
                    let items = items.clone();
                    async move {
                        for item in items {
                            harness.emit_set(&item).await;
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

fn latency_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("latency");
    group.sample_size(100);

    // Pre-setup for latency benchmark
    let harness = rt.block_on(async {
        let harness = BenchHarness::new().await;
        harness.populate(100, &["cat-a", "cat-b", "cat-c"]).await;
        harness
    });
    let harness = Arc::new(harness);

    // Measure event emission latency
    let h = harness.clone();
    group.bench_function("event_emit", move |b| {
        let harness = h.clone();
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let harness = harness.clone();
            async move {
                let item = BenchItem {
                    id: format!("latency-{}", Uuid::new_v4()).into(),
                    hash: Uuid::new_v4().to_string().into(),
                    name: "Latency Test".to_string(),
                    category: "latency".to_string(),
                    value: 999,
                };
                harness.emit_set(&item).await;
            }
        });
    });

    // Measure query start latency
    let harness = rt.block_on(async {
        let harness = BenchHarness::new().await;
        harness.populate(100, &["cat-a", "cat-b", "cat-c"]).await;
        harness
    });
    let harness = Arc::new(harness);

    let h = harness.clone();
    group.bench_function("query_start", move |b| {
        let harness = h.clone();
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let harness = harness.clone();
            async move {
                let query = GetAllBenchItems::new(GetAllBenchItemsArgs {});
                let wrapped = WrappedQuery {
                    query: serde_json::to_value(&query).unwrap(),
                    query_id: "GetAllBenchItems".into(),
                    query_item_type: "BenchItem".into(),
                };
                let _signal = ractor::call!(
                    harness.managers.query_manager,
                    QueryManagerMsg::StartQuery,
                    wrapped
                )
                .expect("Failed to start query");
            }
        });
    });

    group.finish();
}

// =============================================================================
// Saga Stream Operator Benchmarks
// =============================================================================

fn saga_stream_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("saga_stream");
    group.sample_size(100);

    // Helper to create test events
    fn make_events(count: usize) -> Vec<MEvent> {
        (0..count)
            .map(|i| {
                let item = BenchItem {
                    id: format!("item-{}", i).into(),
                    hash: format!("hash-{}", i).into(),
                    name: format!("Item {}", i),
                    category: if i % 2 == 0 { "cat-a" } else { "cat-b" }.to_string(),
                    value: i as i64,
                };
                MEvent::from_item(
                    &item,
                    if i % 3 == 0 {
                        MEventType::DEL
                    } else {
                        MEventType::SET
                    },
                    format!("tx-{}", i),
                )
            })
            .collect()
    }

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Benchmark of_item_type filter
        let events = make_events(*size);
        group.bench_with_input(
            BenchmarkId::new("of_item_type", size),
            size,
            |b, _size| {
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let events = events.clone();
                    async move {
                        let filtered: Vec<_> = stream::iter(events)
                            .of_item_type("BenchItem")
                            .collect()
                            .await;
                        black_box(filtered.len())
                    }
                });
            },
        );

        // Benchmark of_change_type filter
        let events = make_events(*size);
        group.bench_with_input(
            BenchmarkId::new("of_change_type", size),
            size,
            |b, _size| {
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let events = events.clone();
                    async move {
                        let filtered: Vec<_> = stream::iter(events)
                            .of_change_type(MEventType::SET)
                            .collect()
                            .await;
                        black_box(filtered.len())
                    }
                });
            },
        );

        // Benchmark chained filters (common saga pattern)
        let events = make_events(*size);
        group.bench_with_input(
            BenchmarkId::new("chained_filters", size),
            size,
            |b, _size| {
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let events = events.clone();
                    async move {
                        let filtered: Vec<_> = stream::iter(events)
                            .of_item_type("BenchItem")
                            .of_change_type(MEventType::SET)
                            .collect()
                            .await;
                        black_box(filtered.len())
                    }
                });
            },
        );

        // Benchmark pairwise operator
        let events = make_events(*size);
        group.bench_with_input(BenchmarkId::new("pairwise", size), size, |b, _size| {
            b.to_async(Runtime::new().unwrap()).iter(|| {
                let events = events.clone();
                async move {
                    let pairs: Vec<_> = stream::iter(events).pairwise().collect().await;
                    black_box(pairs.len())
                }
            });
        });

        // Benchmark accumulate (scan) operator
        let events = make_events(*size);
        group.bench_with_input(BenchmarkId::new("accumulate", size), size, |b, _size| {
            b.to_async(Runtime::new().unwrap()).iter(|| {
                let events = events.clone();
                async move {
                    let counts: Vec<_> = stream::iter(events)
                        .accumulate(0i64, |acc, event| {
                            // Simulate state accumulation
                            *acc + event.item_json().get("value").and_then(|v| v.as_i64()).unwrap_or(0)
                        })
                        .collect()
                        .await;
                    black_box(counts.last().copied())
                }
            });
        });

        // Benchmark full saga pipeline (typical use case)
        let events = make_events(*size);
        group.bench_with_input(
            BenchmarkId::new("full_pipeline", size),
            size,
            |b, _size| {
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let events = events.clone();
                    async move {
                        let result: Vec<_> = stream::iter(events)
                            .of_item_type("BenchItem")
                            .of_change_type(MEventType::SET)
                            .pairwise()
                            .collect()
                            .await;
                        black_box(result.len())
                    }
                });
            },
        );
    }

    group.finish();

    // Separate benchmark for saga event broadcast overhead
    let mut group = c.benchmark_group("saga_broadcast");
    group.sample_size(50);

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        let harness = rt.block_on(BenchHarness::new());
        let harness = Arc::new(harness);

        // Measure overhead of event broadcast through SagaManager
        let h = harness.clone();
        group.bench_with_input(
            BenchmarkId::new("emit_with_saga", batch_size),
            batch_size,
            move |b, batch_size| {
                let harness = h.clone();
                let items: Vec<BenchItem> = (0..*batch_size)
                    .map(|i| BenchItem {
                        id: format!("saga-{}", i).into(),
                        hash: Uuid::new_v4().to_string().into(),
                        name: format!("Item {}", i),
                        category: "saga-test".to_string(),
                        value: i as i64,
                    })
                    .collect();

                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let harness = harness.clone();
                    let items = items.clone();
                    async move {
                        for item in items {
                            harness.emit_set(&item).await;
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    query_benchmarks,
    event_throughput_benchmarks,
    latency_benchmarks,
    saga_stream_benchmarks
);
criterion_main!(benches);
