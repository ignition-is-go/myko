//! Gateway benchmarks for myko-rs
//!
//! Run with: cargo bench --package myko-rs --features bench
//!
//! Benchmarks:
//! - Query performance over large entity lists
//! - Command-to-query-update latency
//! - Event throughput under load

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::Stream;
use futures::StreamExt;
use futures_signals::signal_map::{MapDiff, SignalMap};
use myko_rs::{
    actors::{
        command::command_manager::{CommandManager, CommandManagerArgs, CommandManagerMsg},
        event::{
            common::{PersistEvent, ProcessEventData},
            event_manager::{EventManager, EventManagerArgs, EventManagerMsg},
        },
        kafka::common::KafkaSharedConfig,
        query::query_manager::{QueryManager, QueryManagerArgs, QueryManagerMsg},
        report::report_manager::{ReportManager, ReportManagerArgs},
        server::{Server, ServerArgs, ServerMsg},
    },
    api::query::WrappedQuery,
    bench_entities::{
        BenchItem, GetAllBenchItems, GetAllBenchItemsArgs, GetBenchItemsByCategory,
        GetBenchItemsByCategoryArgs,
    },
    event::{MEvent, MEventType},
    item::Eventable,
    server::MykoServerCtx,
};
use ractor::{Actor, ActorRef};
use std::{pin::Pin, sync::Arc, task::Poll, time::Duration};
use tokio::runtime::Runtime;
use uuid::Uuid;

// =============================================================================
// SignalMap to Stream adapter (copied from internal code)
// =============================================================================

struct SignalMapStream<S> {
    signal: S,
}

impl<S: SignalMap + Unpin> Stream for SignalMapStream<S> {
    type Item = MapDiff<S::Key, S::Value>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.signal).poll_map_change(cx)
    }
}

// =============================================================================
// Benchmark Harness
// =============================================================================

struct BenchHarness {
    #[allow(dead_code)]
    server: ActorRef<ServerMsg>,
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    #[allow(dead_code)]
    command_manager: ActorRef<CommandManagerMsg>,
    #[allow(dead_code)]
    ctx: Arc<MykoServerCtx>,
}

impl BenchHarness {
    async fn new() -> Self {
        let ctx = Arc::new(MykoServerCtx {
            host_id: Uuid::new_v4(),
        });

        // Spawn a minimal server for message routing
        let (server, _) = Actor::spawn(
            None,
            Server,
            ServerArgs {
                bind_addr: "127.0.0.1".to_string(),
                bind_path: "/".to_string(),
                bind_port: 0,
                kafka_config: KafkaSharedConfig {
                    bootstrap_servers: vec![],
                },
                public_host_address: "127.0.0.1".to_string(),
            },
        )
        .await
        .expect("Failed to spawn server");

        // Spawn event manager
        let (event_manager, _) = Actor::spawn(
            None,
            EventManager,
            EventManagerArgs {
                server: server.clone(),
                ctx: ctx.clone(),
            },
        )
        .await
        .expect("Failed to spawn event manager");

        // Spawn query manager
        let (query_manager, _) = Actor::spawn(
            None,
            QueryManager,
            QueryManagerArgs {
                ctx: ctx.clone(),
                server: server.clone(),
                event_manager: event_manager.clone(),
            },
        )
        .await
        .expect("Failed to spawn query manager");

        // Spawn report manager
        let (report_manager, _) = Actor::spawn(
            None,
            ReportManager,
            ReportManagerArgs {
                ctx: ctx.clone(),
                query_manager: query_manager.clone(),
            },
        )
        .await
        .expect("Failed to spawn report manager");

        // Spawn command manager
        let (command_manager, _) = Actor::spawn(
            None,
            CommandManager,
            CommandManagerArgs {
                ctx: ctx.clone(),
                event_manager: event_manager.clone(),
                query_manager: query_manager.clone(),
                report_manager: report_manager.clone(),
            },
        )
        .await
        .expect("Failed to spawn command manager");

        // Register BenchItem entity and its queries using the macro-generated register methods
        BenchItem::register_with_managers(&event_manager, &query_manager)
            .await
            .expect("Failed to register BenchItem");

        // Small delay to let actors initialize
        tokio::time::sleep(Duration::from_millis(10)).await;

        Self {
            server,
            event_manager,
            query_manager,
            command_manager,
            ctx,
        }
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

    /// Emit a single SET event using the macro-generated MEvent::from_item
    async fn emit_set(&self, item: &BenchItem) {
        let event = MEvent::from_item(item, MEventType::SET, Uuid::new_v4().to_string());

        // Use the optimized path with pre-parsed item
        let parsed_item: Arc<dyn myko_rs::parsers::item::AnyItem> = Arc::new(item.clone());

        self.event_manager
            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                event,
                persist: PersistEvent::NoPersist,
                parsed_item: Some(parsed_item),
            }))
            .expect("Failed to send event");
    }

    /// Start a query and get initial result count
    async fn start_query(&self, query: WrappedQuery) -> usize {
        let signal = ractor::call!(self.query_manager, QueryManagerMsg::StartQuery, query)
            .expect("Failed to start query");

        let mut stream = SignalMapStream { signal };
        if let Some(diff) = stream.next().await {
            if let MapDiff::Replace { entries } = diff {
                return entries.len();
            }
        }
        0
    }
}

/// Helper trait for registering entities without a full MykoServer
trait RegisterWithManagers: Eventable {
    async fn register_with_managers(
        event_manager: &ActorRef<EventManagerMsg>,
        query_manager: &ActorRef<QueryManagerMsg>,
    ) -> Result<(), anyhow::Error>;
}

impl RegisterWithManagers for BenchItem {
    async fn register_with_managers(
        event_manager: &ActorRef<EventManagerMsg>,
        query_manager: &ActorRef<QueryManagerMsg>,
    ) -> Result<(), anyhow::Error> {
        use myko_rs::actors::query::query_manager::RegisterQueryData;
        use myko_rs::parsers::item::{CapturedItemParser, MykoItemParser};
        use myko_rs::parsers::query::{CapturedQueryParser, MykoQueryParser};
        use myko_rs::query::QueryHandlerCtxAny;

        // Register entity parser
        let parser: Arc<dyn MykoItemParser> = Arc::new(CapturedItemParser::<BenchItem>::new());
        event_manager.send_message(EventManagerMsg::RegisterRepo("BenchItem".into(), parser))?;

        // Register GetAllBenchItems query
        let closure: Arc<dyn Fn(QueryHandlerCtxAny) -> bool + Send + Sync> =
            Arc::new(|_ctx| true);
        let parser: Arc<dyn MykoQueryParser> =
            Arc::new(CapturedQueryParser::<GetAllBenchItems>::new());
        query_manager.send_message(QueryManagerMsg::RegisterQuery(RegisterQueryData {
            query_id: "GetAllBenchItems".into(),
            query_item_type: "BenchItem".into(),
            closure,
            parser,
        }))?;

        // Register GetBenchItemsByCategory query
        use std::any::Any;
        let closure: Arc<dyn Fn(QueryHandlerCtxAny) -> bool + Send + Sync> = Arc::new(|ctx| {
            let item_any: Arc<dyn Any + Send + Sync> = ctx.item;
            let query_any: Arc<dyn Any + Send + Sync> = ctx.query;

            let item = item_any.downcast::<BenchItem>();
            let query = query_any.downcast::<GetBenchItemsByCategory>();

            match (item, query) {
                (Ok(item), Ok(query)) => item.category == query.category,
                _ => false,
            }
        });
        let parser: Arc<dyn MykoQueryParser> =
            Arc::new(CapturedQueryParser::<GetBenchItemsByCategory>::new());
        query_manager.send_message(QueryManagerMsg::RegisterQuery(RegisterQueryData {
            query_id: "GetBenchItemsByCategory".into(),
            query_item_type: "BenchItem".into(),
            closure,
            parser,
        }))?;

        Ok(())
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

    // Pre-setup for latency benchmark - measure event emission time
    let harness = rt.block_on(async {
        let harness = BenchHarness::new().await;
        harness
            .populate(100, &["cat-a", "cat-b", "cat-c"])
            .await;
        harness
    });
    let harness = Arc::new(harness);

    // Measure event emission latency (send_message is sync, but we measure the full path)
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

    // Measure query start latency (creating subscription)
    let harness = rt.block_on(async {
        let harness = BenchHarness::new().await;
        harness
            .populate(100, &["cat-a", "cat-b", "cat-c"])
            .await;
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
                let _signal =
                    ractor::call!(harness.query_manager, QueryManagerMsg::StartQuery, wrapped)
                        .expect("Failed to start query");
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    query_benchmarks,
    event_throughput_benchmarks,
    latency_benchmarks
);
criterion_main!(benches);
