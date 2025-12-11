//! Gateway benchmarks for myko-rs
//!
//! Run with: cargo bench --package myko-rs
//!
//! Benchmarks:
//! - Query performance over large entity lists
//! - Command-to-query-update latency
//! - Event throughput under load

use chrono::Utc;
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
        query::query_manager::{
            QueryClosureType, QueryManager, QueryManagerArgs, QueryManagerMsg, RegisterQueryData,
        },
        report::report_manager::{ReportManager, ReportManagerArgs},
        server::{Server, ServerArgs, ServerMsg},
    },
    api::query::WrappedQuery,
    event::{MEvent, MEventType},
    parsers::{
        item::{AnyItem, CapturedItemParser, MykoItemParser},
        query::{AnyQuery, MykoQueryParser},
    },
    prelude::{ToValue, WithId, WithTransaction},
    query::{QueryHandlerCtxAny, QueryId},
    server::MykoServerCtx,
};
use partially::Partial;
use ractor::{Actor, ActorRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{any::Any, pin::Pin, sync::Arc, task::Poll, time::Duration};
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
// Test Entity - Simple benchmarkable item
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Partial, PartialEq)]
#[partially(derive(Debug, Clone, Serialize, Deserialize, Default))]
pub struct BenchItem {
    pub id: Arc<str>,
    pub hash: Arc<str>,
    pub name: String,
    pub category: String,
    pub value: i64,
}

impl BenchItem {
    fn entity_name() -> &'static str {
        "BenchItem"
    }
}

impl WithId for BenchItem {
    fn id(&self) -> Arc<str> {
        self.id.clone()
    }
}

impl ToValue for BenchItem {
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

impl AnyItem for BenchItem {}

/// Create an MEvent for BenchItem without requiring Eventable trait
fn create_bench_event(item: &BenchItem, change_type: MEventType) -> MEvent {
    MEvent {
        item: serde_json::to_value(item).unwrap(),
        change_type,
        item_type: BenchItem::entity_name().to_string(),
        created_at: Utc::now().to_rfc3339(),
        tx: Uuid::new_v4().to_string(),
        source_id: None,
    }
}

// =============================================================================
// Test Queries
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAllBenchItems {
    pub tx: String,
}

impl WithTransaction for GetAllBenchItems {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone().into()
    }
}

impl QueryId for GetAllBenchItems {
    fn query_id(&self) -> Arc<str> {
        "GetAllBenchItems".into()
    }
}

impl AnyQuery for GetAllBenchItems {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBenchItemsByCategory {
    pub tx: String,
    pub category: String,
}

impl WithTransaction for GetBenchItemsByCategory {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone().into()
    }
}

impl QueryId for GetBenchItemsByCategory {
    fn query_id(&self) -> Arc<str> {
        "GetBenchItemsByCategory".into()
    }
}

impl AnyQuery for GetBenchItemsByCategory {}

// =============================================================================
// Simple Query Parser (bypasses full Query trait requirements)
// =============================================================================

struct SimpleQueryParser<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> SimpleQueryParser<T> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyQuery> MykoQueryParser for SimpleQueryParser<T> {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let item = serde_json::from_value::<T>(value)?;
        Ok(Arc::new(item))
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

        // Register BenchItem entity
        let parser: Arc<dyn MykoItemParser> = Arc::new(CapturedItemParser::<BenchItem>::new());
        event_manager
            .send_message(EventManagerMsg::RegisterRepo("BenchItem".into(), parser))
            .expect("Failed to register");

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

        // Register GetAllBenchItems query
        let get_all_closure: QueryClosureType = Arc::new(|_ctx: QueryHandlerCtxAny| true);
        let get_all_parser: Arc<dyn MykoQueryParser> =
            Arc::new(SimpleQueryParser::<GetAllBenchItems>::new());
        query_manager
            .send_message(QueryManagerMsg::RegisterQuery(RegisterQueryData {
                query_id: "GetAllBenchItems".into(),
                query_item_type: "BenchItem".into(),
                closure: get_all_closure,
                parser: get_all_parser,
            }))
            .expect("Failed to register query");

        // Register GetBenchItemsByCategory query
        let get_by_category_closure: QueryClosureType = Arc::new(|ctx: QueryHandlerCtxAny| {
            // Cast to Any for downcast
            let item_any: Arc<dyn Any + Send + Sync> = ctx.item;
            let query_any: Arc<dyn Any + Send + Sync> = ctx.query;

            let item = item_any.downcast::<BenchItem>();
            let query = query_any.downcast::<GetBenchItemsByCategory>();

            match (item, query) {
                (Ok(item), Ok(query)) => item.category == query.category,
                _ => false,
            }
        });
        let get_by_category_parser: Arc<dyn MykoQueryParser> =
            Arc::new(SimpleQueryParser::<GetBenchItemsByCategory>::new());
        query_manager
            .send_message(QueryManagerMsg::RegisterQuery(RegisterQueryData {
                query_id: "GetBenchItemsByCategory".into(),
                query_item_type: "BenchItem".into(),
                closure: get_by_category_closure,
                parser: get_by_category_parser,
            }))
            .expect("Failed to register query");

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

            let event = create_bench_event(&item, MEventType::SET);
            let parsed_item: Arc<dyn AnyItem> = Arc::new(item);

            self.event_manager
                .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                    event,
                    persist: PersistEvent::NoPersist,
                    parsed_item: Some(parsed_item),
                }))
                .expect("Failed to send event");
        }

        // Let events propagate
        tokio::time::sleep(Duration::from_millis(50 + (count as u64 / 100))).await;
    }

    /// Emit a single SET event
    async fn emit_set(&self, item: &BenchItem) {
        let event = create_bench_event(item, MEventType::SET);
        let parsed_item: Arc<dyn AnyItem> = Arc::new(item.clone());

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
                    let query = GetAllBenchItems {
                        tx: Uuid::new_v4().to_string(),
                    };
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
                        let query = GetBenchItemsByCategory {
                            tx: Uuid::new_v4().to_string(),
                            category: "cat-a".to_string(),
                        };
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
                let query = GetAllBenchItems {
                    tx: Uuid::new_v4().to_string(),
                };
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
