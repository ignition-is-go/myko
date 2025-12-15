//! Runtime benchmarks comparing ractor (tokio) vs crossbeam actor implementations
//!
//! Run with: cargo bench --package myko-rs --bench runtime
//!
//! Benchmarks:
//! - Message send latency (single actor)
//! - Throughput (messages/sec)
//! - Sharded throughput with per-key ordering
//! - Request/response (call) latency

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use myko_rs::runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;

// =============================================================================
// Message Types
// =============================================================================

#[derive(Clone, Debug)]
struct TestMessage {
    id: usize,
    #[allow(dead_code)]
    payload: [u8; 64], // Realistic payload size
}

impl TestMessage {
    fn new(id: usize) -> Self {
        Self {
            id,
            payload: [0u8; 64],
        }
    }
}

// =============================================================================
// Ractor Actor Implementation
// =============================================================================

struct RactorCounter;

impl ractor::Actor for RactorCounter {
    type Msg = TestMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        black_box(message.id);
        Ok(())
    }
}

// =============================================================================
// Benchmarks
// =============================================================================

fn message_send_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_send_latency");
    group.sample_size(1000);

    // Crossbeam actor
    let handle = runtime::spawn::spawn_fn(|msg: TestMessage| {
        black_box(msg.id);
    });
    let cb_actor = handle.actor_ref();

    group.bench_function("crossbeam_actor", |b| {
        let actor = cb_actor.clone();
        b.iter(|| {
            actor.send(TestMessage::new(1)).expect("send failed");
        });
    });

    // Ractor actor (needs tokio runtime)
    let rt = Runtime::new().unwrap();
    let ractor_actor = rt.block_on(async {
        let (actor, _handle) = ractor::Actor::spawn(None, RactorCounter, ())
            .await
            .expect("Failed to spawn ractor actor");
        actor
    });

    group.bench_function("ractor_actor", |b| {
        let actor = ractor_actor.clone();
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let actor = actor.clone();
            async move {
                actor
                    .send_message(TestMessage::new(1))
                    .expect("send failed");
            }
        });
    });

    group.finish();

    // Cleanup
    drop(cb_actor);
    handle.shutdown().expect("shutdown failed");
}

fn throughput_single_actor(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_single_actor");
    group.sample_size(50);

    for batch_size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        // Crossbeam actor
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::spawn_fn(move |msg: TestMessage| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(msg.id);
        });
        let cb_actor = handle.actor_ref();

        group.bench_with_input(
            BenchmarkId::new("crossbeam", batch_size),
            batch_size,
            |b, &size| {
                let actor = cb_actor.clone();
                b.iter(|| {
                    for i in 0..size {
                        actor.send(TestMessage::new(i)).expect("send failed");
                    }
                });
            },
        );

        drop(cb_actor);
        handle.shutdown().expect("shutdown failed");

        // Ractor actor
        let rt = Runtime::new().unwrap();
        let ractor_actor = rt.block_on(async {
            let (actor, _handle) = ractor::Actor::spawn(None, RactorCounter, ())
                .await
                .expect("Failed to spawn ractor actor");
            actor
        });

        group.bench_with_input(
            BenchmarkId::new("ractor", batch_size),
            batch_size,
            |b, &size| {
                let actor = ractor_actor.clone();
                b.to_async(Runtime::new().unwrap()).iter(|| {
                    let actor = actor.clone();
                    async move {
                        for i in 0..size {
                            actor
                                .send_message(TestMessage::new(i))
                                .expect("send failed");
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

fn throughput_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_pool");
    group.sample_size(50);

    let num_workers = num_cpus::get();

    for batch_size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        // Crossbeam pool
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::pool(num_workers, move |msg: TestMessage| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(msg.id);
        });
        let pool = handle.pool();

        group.bench_with_input(
            BenchmarkId::new("crossbeam_pool", batch_size),
            batch_size,
            |b, &size| {
                let pool = pool.clone();
                b.iter(|| {
                    for i in 0..size {
                        pool.send(TestMessage::new(i)).expect("send failed");
                    }
                });
            },
        );

        drop(pool);
        handle.shutdown().expect("shutdown failed");
    }

    group.finish();
}

fn throughput_sharded(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_sharded");
    group.sample_size(50);

    let num_shards = num_cpus::get();

    for batch_size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        // Crossbeam sharded (with per-key ordering)
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |msg: TestMessage| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(msg.id);
        });
        let sharded = handle.sharded();

        group.bench_with_input(
            BenchmarkId::new("crossbeam_sharded", batch_size),
            batch_size,
            |b, &size| {
                let sharded = sharded.clone();
                b.iter(|| {
                    for i in 0..size {
                        // Use message id as the routing key
                        sharded
                            .send_keyed(&i, TestMessage::new(i))
                            .expect("send failed");
                    }
                });
            },
        );

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    group.finish();
}

fn sharded_ordering_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharded_ordering");
    group.sample_size(100);

    let num_shards = num_cpus::get();
    let batch_size = 10_000;

    group.throughput(Throughput::Elements(batch_size as u64));

    // Many unique keys (no ordering benefit, max parallelism)
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let handle = runtime::spawn::sharded(num_shards, move |msg: TestMessage| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
        black_box(msg.id);
    });
    let sharded = handle.sharded();

    group.bench_function("many_unique_keys", |b| {
        let sharded = sharded.clone();
        b.iter(|| {
            for i in 0..batch_size {
                sharded
                    .send_keyed(&i, TestMessage::new(i))
                    .expect("send failed");
            }
        });
    });

    drop(sharded);
    handle.shutdown().expect("shutdown failed");

    // Few keys (high contention per shard)
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let handle = runtime::spawn::sharded(num_shards, move |msg: TestMessage| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
        black_box(msg.id);
    });
    let sharded = handle.sharded();

    group.bench_function("few_keys_high_contention", |b| {
        let sharded = sharded.clone();
        b.iter(|| {
            for i in 0..batch_size {
                // Only 4 unique keys - high per-key ordering overhead
                let key = i % 4;
                sharded
                    .send_keyed(&key, TestMessage::new(i))
                    .expect("send failed");
            }
        });
    });

    drop(sharded);
    handle.shutdown().expect("shutdown failed");

    group.finish();
}

fn call_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("call_latency");
    group.sample_size(500);

    // Crossbeam actor with call (request/response)
    use crossbeam::channel::Sender;

    struct CallableActor;
    impl runtime::Actor for CallableActor {
        type Msg = (usize, Sender<usize>);

        fn handle(&mut self, (value, reply): Self::Msg) {
            let _ = reply.send(value * 2);
        }
    }

    let handle = runtime::spawn::spawn(CallableActor);
    let actor = handle.actor_ref();

    group.bench_function("crossbeam_call", |b| {
        let actor = actor.clone();
        b.iter(|| {
            let result = actor.call(|reply| (21, reply)).expect("call failed");
            black_box(result);
        });
    });

    drop(actor);
    handle.shutdown().expect("shutdown failed");

    // Ractor - just benchmark send for comparison
    // (ractor::call! requires more complex setup with reply channels)
    let rt = Runtime::new().unwrap();
    let ractor_actor = rt.block_on(async {
        let (actor, _handle) = ractor::Actor::spawn(None, RactorCounter, ())
            .await
            .expect("Failed to spawn ractor actor");
        actor
    });

    group.bench_function("ractor_send", |b| {
        let actor = ractor_actor.clone();
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let actor = actor.clone();
            async move {
                actor
                    .send_message(TestMessage::new(21))
                    .expect("send failed");
            }
        });
    });

    group.finish();
}

// =============================================================================
// Mixed Workload Benchmarks
// =============================================================================

/// Simulates event with entity type and ID for realistic routing
#[derive(Clone, Debug)]
struct Event {
    entity_type: &'static str,
    entity_id: usize,
    #[allow(dead_code)]
    payload: [u8; 64],
}

impl Event {
    fn new(entity_type: &'static str, entity_id: usize) -> Self {
        Self {
            entity_type,
            entity_id,
            payload: [0u8; 64],
        }
    }

    fn routing_key(&self) -> (&'static str, usize) {
        (self.entity_type, self.entity_id)
    }
}

fn mixed_workload_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(50);

    let num_shards = num_cpus::get();

    // Pattern 1: Uniform distribution across entity types
    // Simulates normal operation with many different entities
    {
        let entity_types = ["Target", "Emitter", "Action", "Binding", "Scene"];
        let entities_per_type = 100;
        let total_events = 10_000;

        group.throughput(Throughput::Elements(total_events as u64));

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |event: Event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(event.entity_id);
        });
        let sharded = handle.sharded();

        group.bench_function("uniform_entity_distribution", |b| {
            let sharded = sharded.clone();
            b.iter(|| {
                for i in 0..total_events {
                    let entity_type = entity_types[i % entity_types.len()];
                    let entity_id = i % entities_per_type;
                    let event = Event::new(entity_type, entity_id);
                    sharded
                        .send_keyed(&event.routing_key(), event)
                        .expect("send failed");
                }
            });
        });

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    // Pattern 2: Hot entity (one entity gets most traffic)
    // Simulates a popular target getting many pulses
    {
        let total_events = 10_000;
        group.throughput(Throughput::Elements(total_events as u64));

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |event: Event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(event.entity_id);
        });
        let sharded = handle.sharded();

        group.bench_function("hot_entity_80_20", |b| {
            let sharded = sharded.clone();
            b.iter(|| {
                for i in 0..total_events {
                    // 80% of traffic goes to entity 0, 20% spread across others
                    let entity_id = if i % 5 == 0 { i % 100 } else { 0 };
                    let event = Event::new("Target", entity_id);
                    sharded
                        .send_keyed(&event.routing_key(), event)
                        .expect("send failed");
                }
            });
        });

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    // Pattern 3: Burst traffic (sudden spike then quiet)
    // Simulates scene activation causing many bindings to fire
    {
        let burst_size = 1_000;
        let quiet_size = 100;
        let iterations = 10;
        let total_events = (burst_size + quiet_size) * iterations;

        group.throughput(Throughput::Elements(total_events as u64));

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |event: Event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(event.entity_id);
        });
        let sharded = handle.sharded();

        group.bench_function("burst_traffic", |b| {
            let sharded = sharded.clone();
            b.iter(|| {
                for iter in 0..iterations {
                    // Burst: many events quickly
                    for i in 0..burst_size {
                        let event = Event::new("Binding", i);
                        sharded
                            .send_keyed(&event.routing_key(), event)
                            .expect("send failed");
                    }
                    // Quiet: few events
                    for i in 0..quiet_size {
                        let event = Event::new("Scene", iter * quiet_size + i);
                        sharded
                            .send_keyed(&event.routing_key(), event)
                            .expect("send failed");
                    }
                }
            });
        });

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    group.finish();
}

fn multi_producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_producer");
    group.sample_size(30);

    let num_shards = num_cpus::get();
    let events_per_producer = 1_000;

    for num_producers in [1, 2, 4, 8].iter() {
        let total_events = events_per_producer * num_producers;
        group.throughput(Throughput::Elements(total_events as u64));

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |event: Event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            black_box(event.entity_id);
        });
        let sharded = handle.sharded();

        group.bench_with_input(
            BenchmarkId::new("producers", num_producers),
            num_producers,
            |b, &num_producers| {
                let sharded = sharded.clone();
                b.iter(|| {
                    std::thread::scope(|s| {
                        for producer_id in 0..num_producers {
                            let sharded = sharded.clone();
                            s.spawn(move || {
                                for i in 0..events_per_producer {
                                    let event = Event::new("Target", producer_id * 1000 + i);
                                    sharded
                                        .send_keyed(&event.routing_key(), event)
                                        .expect("send failed");
                                }
                            });
                        }
                    });
                });
            },
        );

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    group.finish();
}

fn latency_under_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_under_load");
    group.sample_size(100);

    // Measure latency of a single message while background load is running
    let num_shards = num_cpus::get();

    for background_rate in [0, 1_000, 10_000].iter() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let handle = runtime::spawn::sharded(num_shards, move |event: Event| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            // Simulate some work
            std::hint::black_box((0..10).sum::<i32>());
            black_box(event.entity_id);
        });
        let sharded = handle.sharded();

        // Start background load generator if rate > 0
        let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let background_handle = if *background_rate > 0 {
            let sharded = sharded.clone();
            let stop = stop_flag.clone();
            let rate = *background_rate;
            Some(std::thread::spawn(move || {
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..rate / 100 {
                        let event = Event::new("Background", i);
                        let _ = sharded.send_keyed(&event.routing_key(), event);
                        i = i.wrapping_add(1);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }))
        } else {
            None
        };

        // Give background thread time to start
        std::thread::sleep(std::time::Duration::from_millis(50));

        group.bench_with_input(
            BenchmarkId::new("send_latency_bg", background_rate),
            background_rate,
            |b, _| {
                let sharded = sharded.clone();
                let mut i = 0usize;
                b.iter(|| {
                    let event = Event::new("Foreground", i);
                    sharded
                        .send_keyed(&event.routing_key(), event)
                        .expect("send failed");
                    i = i.wrapping_add(1);
                });
            },
        );

        // Stop background thread
        stop_flag.store(true, Ordering::Relaxed);
        if let Some(h) = background_handle {
            h.join().expect("background thread panicked");
        }

        drop(sharded);
        handle.shutdown().expect("shutdown failed");
    }

    group.finish();
}

fn broadcast_fanout(c: &mut Criterion) {
    use runtime::{Broadcast, BroadcastBuilder};

    let mut group = c.benchmark_group("broadcast_fanout");
    group.sample_size(50);

    let batch_size = 1_000;
    group.throughput(Throughput::Elements(batch_size as u64));

    for num_subscribers in [1, 2, 4, 8].iter() {
        let mut handles = Vec::new();
        let mut builder = BroadcastBuilder::<Event>::new();

        for _ in 0..*num_subscribers {
            let counter = Arc::new(AtomicUsize::new(0));
            let counter_clone = counter.clone();
            let handle = runtime::spawn::pool(2, move |event: Event| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                black_box(event.entity_id);
            });
            builder = builder.subscribe(handle.pool());
            handles.push(handle);
        }

        let broadcast = builder.build();

        group.bench_with_input(
            BenchmarkId::new("subscribers", num_subscribers),
            num_subscribers,
            |b, _| {
                let broadcast = broadcast.clone();
                b.iter(|| {
                    for i in 0..batch_size {
                        let event = Event::new("Broadcast", i);
                        broadcast.send(event).expect("send failed");
                    }
                });
            },
        );

        drop(broadcast);
        for handle in handles {
            handle.shutdown().expect("shutdown failed");
        }
    }

    group.finish();
}

fn broadcast_clone_vs_arc(c: &mut Criterion) {
    use runtime::{BroadcastBuilder};

    let mut group = c.benchmark_group("broadcast_clone_vs_arc");
    group.sample_size(50);

    let batch_size = 1_000;
    let num_subscribers = 4;
    group.throughput(Throughput::Elements(batch_size as u64));

    // Direct clone: Event is cloned for each subscriber
    {
        let mut handles = Vec::new();
        let mut builder = BroadcastBuilder::<Event>::new();

        for _ in 0..num_subscribers {
            let handle = runtime::spawn::pool(2, move |event: Event| {
                black_box(event.entity_id);
            });
            builder = builder.subscribe(handle.pool());
            handles.push(handle);
        }

        let broadcast = builder.build();

        group.bench_function("direct_clone", |b| {
            let broadcast = broadcast.clone();
            b.iter(|| {
                for i in 0..batch_size {
                    let event = Event::new("Broadcast", i);
                    broadcast.send(event).expect("send failed");
                }
            });
        });

        drop(broadcast);
        for handle in handles {
            handle.shutdown().expect("shutdown failed");
        }
    }

    // Arc-wrapped: only Arc pointer is cloned (cheap)
    {
        let mut handles = Vec::new();
        let mut builder = BroadcastBuilder::<Arc<Event>>::new();

        for _ in 0..num_subscribers {
            let handle = runtime::spawn::pool(2, move |event: Arc<Event>| {
                black_box(event.entity_id);
            });
            builder = builder.subscribe(handle.pool());
            handles.push(handle);
        }

        let broadcast = builder.build();

        group.bench_function("arc_wrapped", |b| {
            let broadcast = broadcast.clone();
            b.iter(|| {
                for i in 0..batch_size {
                    let event = Arc::new(Event::new("Broadcast", i));
                    broadcast.send(event).expect("send failed");
                }
            });
        });

        drop(broadcast);
        for handle in handles {
            handle.shutdown().expect("shutdown failed");
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    message_send_latency,
    throughput_single_actor,
    throughput_pool,
    throughput_sharded,
    sharded_ordering_overhead,
    call_latency,
    mixed_workload_patterns,
    multi_producer,
    latency_under_load,
    broadcast_fanout,
    broadcast_clone_vs_arc,
);
criterion_main!(benches);
