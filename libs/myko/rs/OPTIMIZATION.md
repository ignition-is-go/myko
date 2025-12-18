# @myko/rs Performance Optimization Guide

**Target**: Hundreds of clients, thousands of messages/second per client. Show-critical, zero-tolerance for dropped messages or latency spikes.

## Already Implemented

| Optimization | Status | Details |
|-------------|--------|---------|
| Direct actor references | ✅ | EventHandler → QueryManager directly (no Server routing) |
| Server lifecycle-only | ✅ | ServerMsg only has Start/Init/AllInitComplete/GetManagers |
| EventBus fan-out | ✅ | Lock-free broadcast::channel (16384 capacity) for sagas/relationships |
| RelationshipManager | ✅ | Cascade operations via EventBus subscription |
| Saga stream operators | ✅ | `of_item_type`, `of_change_type`, `pairwise`, `accumulate` |

## Not Yet Implemented

| Optimization | Priority | Notes |
|-------------|----------|-------|
| MessagePack serialization | High | Currently using JSON text for WebSocket messages |
| DashMap for QueryRunner | High | Currently using BTreeMap with locks |
| ahash for HashMaps | Medium | Currently using default SipHash |
| Query update batching | Medium | Currently per-item updates |
| Saga item-type pre-filtering | Medium | Currently all events go to all sagas |
| Relationship cascade batching | Medium | Currently per-item cascades |
| Async parse pool | Low | Currently sync JSON parsing in actor |
| jemalloc | Low | Using system allocator |

## Performance Benchmarks (Targets)

| Metric | Target | Priority | Notes |
|--------|--------|----------|-------|
| WebSocket latency | <5ms p99 | **CRITICAL** | End-to-end message round trip, show-critical |
| Event throughput | 100,000/sec | High | Per server, all entity types combined |
| Query update latency | <1ms p99 | High | From event to subscriber notification |
| Snapshot create | <100ms | Medium | For 10,000 entities |
| Search query | <10ms | Medium | For 100,000 indexed items |
| Memory per client | Unconstrained | Low | Optimize for latency, not memory |

## Crate Selection

| Purpose | Crate | Rationale |
|---------|-------|-----------|
| Allocator | tikv-jemallocator | Best multi-threaded performance |
| HashMap | dashmap + ahash | Lock-free concurrent + fast hashing |
| Channels | crossbeam-channel | Faster than tokio::sync for hot paths |
| Serialization | rmp-serde | MessagePack, 3-5x faster than JSON |
| Compression | lz4_flex | Fastest compression, good ratio |
| Bloom filter | growable-bloom-filter | Space-efficient deduplication |
| LRU cache | quick-cache | Concurrent, low-latency |
| Search | tantivy | Rust-native full-text search |

---

## Optimization Strategies

### 1. Serialization Strategy

**Problem**: JSON serialization is 5-10x slower than binary formats.

**Decision**: MessagePack everywhere, with pre-serialized caching.

```rust
pub struct CachedMessage {
    bytes: Arc<[u8]>,
}

impl CachedMessage {
    pub fn new<T: Serialize>(msg: &T) -> Self {
        let bytes = rmp_serde::to_vec_named(msg).unwrap();
        Self { bytes: bytes.into() }
    }
}

impl WebSocketConnection {
    fn send(&mut self, msg: &CachedMessage) {
        self.tx.send(Message::Binary(msg.bytes.clone())); // Arc clone only
    }
}
```

**Rationale**: When broadcasting to 100 clients, serialize once, send 100 times.

### 2. Event Fan-Out Architecture

**Problem**: Current design routes all messages through Server actor, creating a bottleneck.

**Decision**: Direct actor references with sharded broadcast.

```rust
pub struct ShardedBroadcast<T> {
    shards: Vec<broadcast::Sender<T>>,
}

impl<T: Clone> ShardedBroadcast<T> {
    pub fn new(shard_count: usize, capacity: usize) -> Self {
        Self {
            shards: (0..shard_count)
                .map(|_| broadcast::channel(capacity).0)
                .collect(),
        }
    }
}
```

**Rationale**: Ractor actors are single-threaded. Sharding distributes load across cores.

### 3. Query Runner Optimization

**Problem**: `MutableBTreeMap::lock_mut()` on every update creates contention.

**Decision**: Lock-free concurrent map with batch updates using `DashMap`.

### 4. Zero-Copy Item References

**Problem**: `Arc<dyn AnyItem>` requires vtable lookup and prevents inlining.

**Decision**: Type-erased but cache-friendly item storage with inline IDs/hashes.

### 5. WebSocket Batching

**Problem**: Per-message WebSocket frames have ~14 byte overhead + syscall per send.

**Decision**: Accumulate and flush with configurable interval (8ms for 120fps).

### 6. Channel Sizing and Backpressure

Tiered channel strategy:
- Hot path (events, queries): 16384
- Warm path (commands, reports): 4096
- Cold path (admin, config): 256

### 7. Memory Allocation Strategy

- Use jemalloc for better multi-threaded performance
- Object pools for frequently allocated types

### 8. Hash Function Selection

- Keep MD5 for content hashing (cross-language compatibility)
- Use ahash for internal lookups (5-10x faster than SipHash)

### 9. Query Matching Optimization

Compile queries to optimized matchers at registration time to avoid per-item closure evaluation.

### 10. Saga Event Filtering

Pre-filter by item type at registration to eliminate 90%+ of saga invocations.

### 11. Relationship Cascade Batching

Batch cascade operations with single transaction to prevent partial cascade states.

### 12. Snapshot Storage Efficiency

Delta compression from previous snapshot (consecutive snapshots often differ by <5%).

### 13. Search Index Updates

Micro-batched tantivy commits with debouncing (10ms batches, 50ms commit interval).

### 14. Authentication Token Caching

LRU cache for validated JWT tokens (5 minute TTL).

### 15. Peer Event Deduplication

Bloom filter for recent event IDs with exact set for false-positive verification.

---

## Actor Structure Refactoring

### ✅ Solved: Server Routing Bottleneck

**Before** (all traffic through Server):
```
┌─────────────────────────────────────────────────────────────────┐
│                        Server Actor                             │
│  (BOTTLENECK: all messages route through single-threaded actor) │
└───────────────────────────┬─────────────────────────────────────┘
```

**After** (direct actor references):
```
EventHandler ──► QueryManager (direct)
EventManager ──► EventBus ──► SagaRunners (broadcast)
                          └──► RelationshipManager (broadcast)
Server: lifecycle only (Start/Init/AllInitComplete)
```

### Remaining Bottlenecks

| Problem | Location | Impact | Priority |
|---------|----------|--------|----------|
| JSON serialization | `websocket_connection.rs:175` | CPU overhead on every WS send | High |
| BTreeMap locks | `QueryRunner` state | Contention under load | High |
| Clone per handler | `QueryManager` loop | N allocations per event | Medium |
| Sync JSON parsing | `MessageHandler::ProcessText` | Blocks actor | Low |
| No update batching | `QueryRunnerMsg::ProcessUpdate` | Per-item messages | Medium |

### Target Architecture (Partial Progress)

```
                    ┌─────────────────────┐
                    │     EventBus        │ ✅ DONE (lock-free broadcast)
                    └──────────┬──────────┘
                               │
       ┌───────────────────────┼───────────────────────┐
       ▼                       ▼                       ▼
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│ SagaRunner  │         │ SagaRunner  │         │Relationship │
│             │         │             │         │  Manager    │
└─────────────┘         └─────────────┘         └─────────────┘

WebSocket ──► MessageHandler ──► EventManager ──► EventHandler ──► QueryManager
                   │                                    │
                   └─ TODO: ParsePool                   └─ TODO: DashMap
```

### Migration Plan

| Phase | Changes | Risk | Status |
|-------|---------|------|--------|
| 1 | Direct actor refs (remove Server routing) | Low | ✅ DONE |
| 2 | EventBus for saga/relationship broadcast | Low | ✅ DONE |
| 3 | MessagePack serialization | Low | Not started |
| 4 | Batch updates to handlers | Low | Not started |
| 5 | Replace QueryRunner with shared DashMap | Medium | Not started |
| 6 | Async parse pool | Low | Not started |

### Achieved Performance Improvements

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Event routing hops | 3 (EH→Server→QM) | 1 (EH→QM direct) | ✅ 3x fewer messages |
| Saga event delivery | Server routing | EventBus broadcast | ✅ Lock-free fan-out |
| Relationship cascades | N/A | EventBus subscription | ✅ Decoupled from event path |

### Remaining Optimizations

| Metric | Current | Target | Priority |
|--------|---------|--------|----------|
| WebSocket serialization | JSON text | MessagePack binary | High |
| Query state storage | BTreeMap + locks | DashMap (lock-free) | High |
| Hash function | SipHash | ahash | Medium |
| JSON parsing | Sync in actor | Async thread pool | Low |

---

## 1M Events/Sec Scale Plan

**Target**: 1M events/sec with 300 reports, 300 queries, 50 clients, <10ms p99 latency.

### Current Capacity Analysis

| Metric | Current | Required | Gap |
|--------|---------|----------|-----|
| Events/sec (single server) | ~500 | 1,000,000 | 2,000x |
| p99 latency | 75-150ms | <10ms | 10-15x |
| Serializations per event | 1,500 (per client) | 2 (per format) | 750x |

### Workload Assumptions

- Few hot entity types (90%+ events on Target, Emitter)
- Low match rate (<10% of queries match each event)
- Horizontal scaling available

### Phase 1: Serialization (P0) - 100x Improvement

#### 1.1 MessagePack as Opt-In via WebSocket Subprotocol

Not all clients support MessagePack. Opt-in via WebSocket subprotocol negotiation:

```
// Client request header
Sec-WebSocket-Protocol: msgpack

// Server response (if supported)
Sec-WebSocket-Protocol: msgpack
```

**Files:**
- `src/actors/ws/websocket_server.rs` - subprotocol negotiation
- `src/actors/ws/websocket_connection.rs` - per-client format tracking

```rust
pub enum SerializationFormat {
    Json,       // Default, always supported
    MessagePack, // Opt-in via subprotocol
}

pub struct WebSocketConnection {
    format: SerializationFormat,
    // ...
}
```

#### 1.2 Serialize-Once Per Format, Broadcast-Many

Serialize once per format, not per client:

```rust
struct SerializedUpdate {
    msgpack: Option<Arc<[u8]>>,  // Lazy, computed on first msgpack client
    json: Option<Arc<str>>,      // Lazy, computed on first json client
}

impl SerializedUpdate {
    fn get_for_format(&mut self, format: SerializationFormat, update: &QueryStreamUpdate) -> Message {
        match format {
            SerializationFormat::MessagePack => {
                let bytes = self.msgpack.get_or_insert_with(|| {
                    Arc::from(rmp_serde::to_vec_named(update).unwrap())
                });
                Message::Binary(bytes.to_vec())
            }
            SerializationFormat::Json => {
                let text = self.json.get_or_insert_with(|| {
                    Arc::from(serde_json::to_string(update).unwrap())
                });
                Message::Text(text.to_string())
            }
        }
    }
}
```

**Impact**: 50 JSON + 50 MessagePack clients = 2 serializations (not 100)

#### 1.3 Entity-Type Pre-Filtering

Only evaluate queries matching the event's entity type:

```rust
// Before: Iterate all 300 queries
for handler in self.handlers.values() {
    handler.send_message(ProcessUpdate(data));
}

// After: Only queries for matching entity type
if let Some(handlers) = self.handlers.get(&data.entity_type) {
    for handler in handlers.values() {
        handler.send_message(ProcessUpdate(data));
    }
}
```

**Impact**: With 10 entity types, 10x fewer handler invocations

### Phase 2: Fanout & Bridging (P1) - 10x Improvement

#### 2.1 Batched Notifications

Batch updates within 1-2ms window:

```rust
pending_updates.push(update);

if pending_updates.len() >= 100 || elapsed >= 2ms {
    query_manager.send_message(ProcessBatch(pending_updates.drain(..).collect()));
}
```

**Impact**: 10-100x fewer channel sends under load

#### 2.2 Flume for Report Channels

Replace crossbeam with flume at sync→async bridge points to eliminate spawn_blocking:

**Files:**
- `src/report/handler.rs`
- `src/actors/report/report_runner.rs`
- `src/actors/report/report_manager.rs`

```rust
// Before: spawn_blocking overhead per poll
let result = tokio::task::spawn_blocking(move || rx.recv()).await;

// After: native async, no thread pool
let result = rx.recv_async().await;
```

**Impact**: Eliminates blocking thread pool contention

### Phase 3: State & Sharding (P2) - Linear Scaling

#### 3.1 DashMap for QueryHandler State

```rust
// Before
state: Arc<Mutex<BTreeMap<Arc<str>, Arc<dyn AnyItem>>>>

// After
state: DashMap<Arc<str>, Arc<dyn AnyItem>>
```

#### 3.2 Horizontal Sharding by Entity Type

- Server 1: Target, Emitter (high-volume)
- Server 2: Scene, Binding, Calendar
- Server 3: Reports, aggregations

### Expected Results

| Phase | Single Server Capacity | Cumulative |
|-------|------------------------|------------|
| Current | ~500/sec | - |
| P0 (Serialization) | 50,000-100,000/sec | 100-200x |
| P1 (Batching + Flume) | 500,000/sec | 1,000x |
| P2 (Sharding) | 500,000+/server | Linear |

### Files to Modify

| File | Change |
|------|--------|
| `Cargo.toml` | Add `rmp-serde`, `flume`, `dashmap` |
| `src/actors/ws/websocket_server.rs` | Subprotocol negotiation |
| `src/actors/ws/websocket_connection.rs` | Per-client format, MessagePack |
| `src/actors/query/query_handler.rs` | Serialize-once, DashMap |
| `src/actors/query/query_manager.rs` | Entity-type pre-filtering |
| `src/actors/event/event_handler.rs` | Batched notifications |
| `src/report/handler.rs` | Flume channels |
| `src/actors/report/report_runner.rs` | Flume async recv |
| `src/actors/report/report_manager.rs` | Flume async recv |
