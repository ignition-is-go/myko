//! Server context, client sessions and the delivery sink shared by every
//! scenario.
//!
//! A subscription is built the way `WsHandler` builds one: the wire JSON is
//! parsed by the registered handler, the registered cell factory builds the
//! cell, and a `ClientSession` subscribes it. The session's `WsWriter` does
//! what the server's `ChannelWriter` does: it stamps the callback and queues
//! the response. Drain threads then serialize it, as the server's writer
//! tasks do.

use std::{
    any::Any,
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use myko::{
    client::MykoProtocol,
    core::item::AnyItem,
    prelude::*,
    report::AnyOutput,
    request::RequestContext,
    search::SearchIndex,
    serde_json::{self, Value},
    server::{
        ClientSession, HandlerRegistry, MykoServerContext, MykoServerRuntime, PendingQueryResponse,
        PersisterRouter, RelationshipManager, WsWriter,
    },
    store::StoreRegistry,
    wire::{EncodedCommandMessage, MykoMessage, ReportResponse},
};

use super::{entities::*, reports::*, views::*};
use crate::compat::pulse::BPulse;

pub const NONE: u64 = u64::MAX;

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Nanoseconds since the process epoch.
#[inline]
pub fn now_ns() -> u64 {
    u64::try_from(EPOCH.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(NONE)
}

/// Sleeps until `deadline_ns`, spinning for the last stretch so a paced
/// writer does not inherit the scheduler's wake-up jitter.
pub fn sleep_until(deadline_ns: u64) {
    loop {
        let now = now_ns();
        if now >= deadline_ns {
            return;
        }
        let left = deadline_ns - now;
        if left > 300_000 {
            std::thread::sleep(Duration::from_nanos(left - 200_000));
        } else {
            std::hint::spin_loop();
        }
    }
}

/// What a delivered value says about the write that produced it.
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    pub tick: u64,
    pub seq: u64,
    pub writer: u32,
}

pub fn stamp_of_item(item: &Arc<dyn AnyItem>) -> Option<Stamp> {
    let any: &dyn Any = &**item;
    if let Some(node) = any.downcast_ref::<BNode>() {
        return Some(Stamp {
            tick: node.tick,
            seq: node.seq,
            writer: node.writer,
        });
    }
    if let Some(probe) = any.downcast_ref::<BProbe>() {
        return Some(Stamp {
            tick: NONE,
            seq: probe.seq,
            writer: probe.writer,
        });
    }
    if let Some(row) = any.downcast_ref::<NodeWithScene>() {
        return Some(Stamp {
            tick: row.tick,
            seq: row.seq,
            writer: row.writer,
        });
    }
    if let Some(pulse) = any.downcast_ref::<BPulse>() {
        return Some(Stamp {
            tick: NONE,
            seq: pulse.seq,
            writer: pulse.writer,
        });
    }
    if let Some(emitter) = any.downcast_ref::<BEmitter>() {
        return Some(Stamp {
            tick: NONE,
            seq: emitter.revision,
            writer: 0,
        });
    }
    None
}

pub fn stamp_of_output(output: &Arc<dyn AnyOutput>) -> Option<Stamp> {
    let any = output.as_any();
    if let Some(out) = any.downcast_ref::<NodeValueOut>() {
        return Some(Stamp {
            tick: out.tick,
            seq: out.seq,
            writer: out.writer,
        });
    }
    if let Some(Some(node)) = any.downcast_ref::<Option<Arc<BNode>>>() {
        return Some(Stamp {
            tick: node.tick,
            seq: node.seq,
            writer: node.writer,
        });
    }
    if let Some(out) = any.downcast_ref::<ChainOut>() {
        return Some(Stamp {
            tick: NONE,
            seq: out.seq,
            writer: 0,
        });
    }
    if let Some(out) = any.downcast_ref::<SceneStatsOut>() {
        return Some(Stamp {
            tick: NONE,
            seq: out.max_seq,
            writer: 0,
        });
    }
    None
}

#[derive(Default)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
}

type OrderMap = HashMap<u64, u64, BuildHasherDefault<IdHasher>>;

enum Out {
    Query {
        slot: usize,
        response: PendingQueryResponse,
        is_view: bool,
    },
    Report {
        slot: usize,
        tx: Arc<str>,
        output: Arc<dyn AnyOutput>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    Json,
    Cbor,
    /// Skip serialization, to separate its cost from the graph's.
    None,
}

/// Per-subscription state. One slot per subscription, indexed by the number
/// encoded in the subscription's `tx`.
pub struct Slot {
    pub last_seq: AtomicU64,
    pub msgs: AtomicU64,
    pub rows: AtomicU64,
    pub deletes: AtomicU64,
    pub last_cb_ns: AtomicU64,
    /// `seq` of the first value in the latest message.
    pub last_val: AtomicU64,
}

pub struct Sink {
    pub wire: Wire,
    pub slots: Vec<Slot>,
    next_slot: AtomicU64,
    lanes: Vec<flume::Sender<Out>>,
    pub pending: AtomicI64,

    pub tick_cb_done: Vec<AtomicU64>,
    pub tick_ser_done: Vec<AtomicU64>,
    pub tick_rows: Vec<AtomicU64>,

    pub seq_gaps: AtomicU64,
    pub seq_resets: AtomicU64,
    pub order_violations: AtomicU64,
    pub errors: AtomicU64,
    pub other_msgs: AtomicU64,
    pub rows_ser: AtomicU64,
    pub deletes_ser: AtomicU64,
    pub msgs_ser: AtomicU64,
    pub bytes_ser: AtomicU64,
    pub ser_ns: AtomicU64,
    /// When set, every message's upserts are recorded for a scenario that
    /// inspects message contents (the atomic-batch check).
    pub capture: AtomicBool,
    pub captured: parking_lot::Mutex<Vec<Vec<Arc<dyn AnyItem>>>>,
}

pub const MAX_TICKS: usize = 4096;

impl Sink {
    pub fn new(wire: Wire, max_slots: usize, drains: usize) -> Arc<Self> {
        let mut lanes = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..drains.max(1) {
            let (tx, rx) = flume::unbounded::<Out>();
            lanes.push(tx);
            receivers.push(rx);
        }
        let atoms = |n: usize, init: u64| (0..n).map(|_| AtomicU64::new(init)).collect::<Vec<_>>();
        let sink = Arc::new(Self {
            wire,
            slots: (0..max_slots)
                .map(|_| Slot {
                    last_seq: AtomicU64::new(NONE),
                    msgs: AtomicU64::new(0),
                    rows: AtomicU64::new(0),
                    deletes: AtomicU64::new(0),
                    last_cb_ns: AtomicU64::new(0),
                    last_val: AtomicU64::new(NONE),
                })
                .collect(),
            next_slot: AtomicU64::new(0),
            lanes,
            pending: AtomicI64::new(0),
            tick_cb_done: atoms(MAX_TICKS, 0),
            tick_ser_done: atoms(MAX_TICKS, 0),
            tick_rows: atoms(MAX_TICKS, 0),
            seq_gaps: AtomicU64::new(0),
            seq_resets: AtomicU64::new(0),
            order_violations: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            other_msgs: AtomicU64::new(0),
            rows_ser: AtomicU64::new(0),
            deletes_ser: AtomicU64::new(0),
            msgs_ser: AtomicU64::new(0),
            bytes_ser: AtomicU64::new(0),
            ser_ns: AtomicU64::new(0),
            capture: AtomicBool::new(false),
            captured: parking_lot::Mutex::new(Vec::new()),
        });
        for rx in receivers {
            let sink = sink.clone();
            std::thread::Builder::new()
                .name("drain".into())
                .spawn(move || sink.drain(&rx))
                .unwrap_or_else(|error| panic!("spawn drain: {error}"));
        }
        sink
    }

    pub fn alloc_slot(&self) -> usize {
        let slot = usize::try_from(self.next_slot.fetch_add(1, Relaxed)).unwrap_or(usize::MAX);
        assert!(slot < self.slots.len(), "sink slots exhausted");
        slot
    }

    pub fn slots_used(&self) -> usize {
        usize::try_from(self.next_slot.load(Relaxed)).unwrap_or(0)
    }

    fn slot_of(tx: &str) -> usize {
        usize::from_str_radix(tx, 16).unwrap_or(0)
    }

    fn on_query(&self, response: PendingQueryResponse, is_view: bool) {
        let cb = now_ns();
        let slot_index = Self::slot_of(&response.tx);
        let slot = &self.slots[slot_index];
        let previous = slot.last_seq.swap(response.sequence, Relaxed);
        if response.sequence == 0 {
            if previous != NONE {
                self.seq_resets.fetch_add(1, Relaxed);
            }
        } else if previous == NONE || response.sequence != previous.wrapping_add(1) {
            self.seq_gaps.fetch_add(1, Relaxed);
        }
        slot.rows
            .fetch_add(response.upsert_items.len() as u64, Relaxed);
        slot.deletes
            .fetch_add(response.deletes.len() as u64, Relaxed);
        if let Some(stamp) = response.upsert_items.first().and_then(stamp_of_item) {
            slot.last_val.store(stamp.seq, Relaxed);
            if let Some(done) = self.tick_slot(&self.tick_cb_done, stamp.tick) {
                done.fetch_max(cb, Relaxed);
            }
        }
        slot.last_cb_ns.store(cb, Relaxed);
        // Last, so a waiter that sees the count also sees the stamp and value.
        slot.msgs.fetch_add(1, Relaxed);
        self.enqueue(
            slot_index,
            Out::Query {
                slot: slot_index,
                response,
                is_view,
            },
        );
    }

    fn on_report(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        let cb = now_ns();
        let slot_index = Self::slot_of(&tx);
        let slot = &self.slots[slot_index];
        slot.rows.fetch_add(1, Relaxed);
        if let Some(stamp) = stamp_of_output(&output) {
            slot.last_val.store(stamp.seq, Relaxed);
            if let Some(done) = self.tick_slot(&self.tick_cb_done, stamp.tick) {
                done.fetch_max(cb, Relaxed);
            }
        }
        slot.last_cb_ns.store(cb, Relaxed);
        slot.msgs.fetch_add(1, Relaxed);
        self.enqueue(
            slot_index,
            Out::Report {
                slot: slot_index,
                tx,
                output,
            },
        );
    }

    fn enqueue(&self, slot: usize, out: Out) {
        self.pending.fetch_add(1, Relaxed);
        // One lane per slot keeps a subscription's messages in order.
        let lane = &self.lanes[slot % self.lanes.len()];
        if lane.send(out).is_err() {
            self.errors.fetch_add(1, Relaxed);
        }
    }

    fn tick_slot<'a>(&self, table: &'a [AtomicU64], tick: u64) -> Option<&'a AtomicU64> {
        usize::try_from(tick).ok().and_then(|tick| table.get(tick))
    }

    fn encode(&self, message: &MykoMessage) -> usize {
        match self.wire {
            Wire::Json => serde_json::to_string(message).map_or_else(
                |_| {
                    self.errors.fetch_add(1, Relaxed);
                    0
                },
                |text| text.len(),
            ),
            Wire::Cbor => {
                let mut bytes = Vec::new();
                if ciborium::ser::into_writer(message, &mut bytes).is_err() {
                    self.errors.fetch_add(1, Relaxed);
                }
                bytes.len()
            }
            Wire::None => 0,
        }
    }

    fn drain(&self, rx: &flume::Receiver<Out>) {
        let mut order = OrderMap::default();
        while let Ok(out) = rx.recv() {
            match out {
                Out::Query {
                    slot,
                    response,
                    is_view,
                } => {
                    let deletes = response.deletes.len() as u64;
                    let wire = response.into_wire();
                    let message = if is_view {
                        MykoMessage::ViewResponse(wire)
                    } else {
                        MykoMessage::QueryResponse(wire)
                    };
                    let started = now_ns();
                    let bytes = self.encode(&message);
                    let done = now_ns();
                    self.ser_ns.fetch_add(done - started, Relaxed);
                    let (MykoMessage::ViewResponse(wire) | MykoMessage::QueryResponse(wire)) =
                        message
                    else {
                        continue;
                    };
                    let mut tick = NONE;
                    let mut tick_rows = 0u64;
                    for wrapped in &wire.upserts {
                        let Some(stamp) = stamp_of_item(&wrapped.item) else {
                            continue;
                        };
                        self.check_order(&mut order, slot, stamp, wire.sequence == 0);
                        if stamp.tick == NONE {
                            continue;
                        }
                        if stamp.tick != tick {
                            self.flush_tick(tick, tick_rows, done);
                            tick = stamp.tick;
                            tick_rows = 0;
                        }
                        tick_rows += 1;
                    }
                    self.flush_tick(tick, tick_rows, done);
                    self.rows_ser.fetch_add(wire.upserts.len() as u64, Relaxed);
                    self.deletes_ser.fetch_add(deletes, Relaxed);
                    self.bytes_ser.fetch_add(bytes as u64, Relaxed);
                    self.msgs_ser.fetch_add(1, Relaxed);
                    if self.capture.load(Relaxed) {
                        self.captured
                            .lock()
                            .push(wire.upserts.iter().map(|w| w.item.clone()).collect());
                    }
                }
                Out::Report { slot, tx, output } => {
                    let started = now_ns();
                    let bytes = if self.wire == Wire::None {
                        0
                    } else {
                        self.encode(&MykoMessage::ReportResponse(ReportResponse {
                            response: output.to_value(),
                            tx: tx.to_string(),
                        }))
                    };
                    let done = now_ns();
                    self.ser_ns.fetch_add(done - started, Relaxed);
                    if let Some(stamp) = stamp_of_output(&output) {
                        self.check_order(&mut order, slot, stamp, false);
                        if stamp.tick != NONE {
                            self.flush_tick(stamp.tick, 1, done);
                        }
                    }
                    self.rows_ser.fetch_add(1, Relaxed);
                    self.bytes_ser.fetch_add(bytes as u64, Relaxed);
                    self.msgs_ser.fetch_add(1, Relaxed);
                }
            }
            self.pending.fetch_sub(1, Relaxed);
        }
    }

    fn flush_tick(&self, tick: u64, rows: u64, done: u64) {
        if rows == 0 {
            return;
        }
        if let Some(count) = self.tick_slot(&self.tick_rows, tick) {
            count.fetch_add(rows, Relaxed);
        }
        if let Some(stamp) = self.tick_slot(&self.tick_ser_done, tick) {
            stamp.fetch_max(done, Relaxed);
        }
    }

    /// Per-writer order: within one subscription, a writer's values arrive in
    /// the order the writer produced them. A full replacement (`initial`) may
    /// repeat a value but may not go back either.
    fn check_order(&self, order: &mut OrderMap, slot: usize, stamp: Stamp, _initial: bool) {
        let key = ((slot as u64) << 20) | u64::from(stamp.writer & 0xf_ffff);
        let last = order.entry(key).or_insert(0);
        if stamp.seq < *last {
            self.order_violations.fetch_add(1, Relaxed);
        } else {
            *last = stamp.seq;
        }
    }

    /// Waits until the drain queues are empty and no callback has fired for
    /// `quiet`. Returns false on timeout.
    pub fn quiesce(&self, quiet: Duration, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut last = (self.total_msgs(), Instant::now());
        loop {
            std::thread::sleep(Duration::from_millis(2));
            let msgs = self.total_msgs();
            if msgs != last.0 {
                last = (msgs, Instant::now());
            } else if self.pending.load(Relaxed) == 0 && last.1.elapsed() >= quiet {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
        }
    }

    pub fn total_msgs(&self) -> u64 {
        self.slots[..self.slots_used()]
            .iter()
            .map(|slot| slot.msgs.load(Relaxed))
            .sum()
    }

    pub fn total_rows_cb(&self) -> u64 {
        self.slots[..self.slots_used()]
            .iter()
            .map(|slot| slot.rows.load(Relaxed))
            .sum()
    }

    /// Waits for `slot` to have received more than `msgs_before` messages.
    pub fn wait_slot(&self, slot: usize, msgs_before: u64, timeout: Duration) -> Option<u64> {
        let state = &self.slots[slot];
        let deadline = Instant::now() + timeout;
        let mut spins = 0u32;
        loop {
            if state.msgs.load(Relaxed) > msgs_before {
                return Some(state.last_cb_ns.load(Relaxed));
            }
            spins += 1;
            if spins > 20_000 {
                if Instant::now() > deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_micros(50));
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

#[derive(Clone)]
pub struct Writer {
    sink: Arc<Sink>,
}

impl WsWriter for Writer {
    fn send(&self, msg: MykoMessage) {
        if matches!(msg, MykoMessage::ReportError(_)) {
            self.sink.errors.fetch_add(1, Relaxed);
        } else {
            self.sink.other_msgs.fetch_add(1, Relaxed);
        }
    }

    fn protocol(&self) -> MykoProtocol {
        match self.sink.wire {
            Wire::Cbor => MykoProtocol::CBOR,
            Wire::Json | Wire::None => MykoProtocol::JSON,
        }
    }

    fn send_serialized_command(&self, _: Arc<str>, _: String, _: EncodedCommandMessage) {}

    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        self.sink.on_report(tx, output);
    }

    fn send_query_response(&self, response: PendingQueryResponse, is_view: bool) {
        self.sink.on_query(response, is_view);
    }
}

#[derive(Clone)]
pub struct Bench {
    pub ctx: Arc<MykoServerContext>,
    pub sink: Arc<Sink>,
}

impl Bench {
    pub fn new(wire: Wire, max_slots: usize, drains: usize) -> Self {
        let _ = now_ns();
        let relationships = Arc::new(RelationshipManager::new());
        let ctx = MykoServerContext::new(
            Uuid::new_v4(),
            Arc::new(StoreRegistry::new()),
            Arc::new(HandlerRegistry::new()),
            relationships.clone(),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink: None,
                history_replay: None,
            },
        );
        if let Err(error) = relationships.establish_relations(&ctx) {
            panic!("establish relations: {error:?}");
        }
        Self {
            ctx: Arc::new(ctx),
            sink: Sink::new(wire, max_slots, drains),
        }
    }

    pub fn client(&self, name: &str) -> Client {
        Client {
            bench: self.clone(),
            id: Arc::from(name),
            session: ClientSession::new(
                Arc::from(name),
                Writer {
                    sink: self.sink.clone(),
                },
            ),
        }
    }
}

/// One connected client. Dropping it is a disconnect.
pub struct Client {
    bench: Bench,
    id: Arc<str>,
    pub session: ClientSession<Writer>,
}

#[derive(Clone)]
pub struct Sub {
    pub slot: usize,
    pub tx: Arc<str>,
}

fn wire_request<T: serde::Serialize>(params: &T, tx: &Arc<str>) -> Value {
    let mut value = serde_json::to_value(params).unwrap_or_else(|error| panic!("{error}"));
    if let Value::Object(map) = &mut value {
        map.insert("tx".into(), Value::String(tx.to_string()));
    } else {
        panic!("request params must serialize to an object");
    }
    value
}

impl Client {
    fn begin(&self) -> (Sub, Arc<RequestContext>) {
        let slot = self.bench.sink.alloc_slot();
        let tx: Arc<str> = Arc::from(format!("{slot:x}"));
        let request = Arc::new(RequestContext::from_client(
            tx.clone(),
            self.id.clone(),
            self.bench.ctx.host_id,
        ));
        (Sub { slot, tx }, request)
    }

    pub fn sub_query<Q>(&mut self, query: &Q) -> Sub
    where
        Q: serde::Serialize + myko::query::QueryIdStatic,
    {
        let (sub, request) = self.begin();
        let ctx = &self.bench.ctx;
        let id = Q::query_id_static();
        let data = ctx
            .handler_registry
            .query(&id)
            .unwrap_or_else(|| panic!("query {id} is not registered"));
        let parsed = (data.parse)(wire_request(query, &sub.tx))
            .unwrap_or_else(|error| panic!("parse {id}: {error}"));
        let cell = (data.cell_factory)(parsed, ctx.registry.clone(), request, Some(ctx.clone()))
            .unwrap_or_else(|error| panic!("build {id}: {error}"));
        self.session.subscribe_query(sub.tx.clone(), id, cell, None);
        sub
    }

    pub fn sub_report<R>(&mut self, report: &R) -> Sub
    where
        R: serde::Serialize + myko::report::ReportIdStatic,
    {
        let (sub, request) = self.begin();
        let ctx = &self.bench.ctx;
        let id = R::report_id_static();
        let data = ctx
            .handler_registry
            .report(id)
            .unwrap_or_else(|| panic!("report {id} is not registered"));
        let parsed = (data.parse)(wire_request(report, &sub.tx))
            .unwrap_or_else(|error| panic!("parse {id}: {error}"));
        let cell = (data.cell_factory)(parsed, request, ctx.clone())
            .unwrap_or_else(|error| panic!("build {id}: {error}"));
        self.session
            .subscribe_report(sub.tx.clone(), Arc::from(id), cell);
        sub
    }

    pub fn sub_view<V>(&mut self, view: &V) -> Sub
    where
        V: serde::Serialize + myko::view::ViewIdStatic,
    {
        let (sub, request) = self.begin();
        let ctx = &self.bench.ctx;
        let id = V::view_id_static();
        let data = ctx
            .handler_registry
            .view(&id)
            .unwrap_or_else(|| panic!("view {id} is not registered"));
        let parsed = (data.parse)(wire_request(view, &sub.tx))
            .unwrap_or_else(|error| panic!("parse {id}: {error}"));
        let cell = (data.cell_factory)(parsed, ctx.registry.clone(), request, ctx.clone())
            .unwrap_or_else(|error| panic!("build {id}: {error}"));
        self.session
            .subscribe_view_with_id(sub.tx.clone(), id, cell, None);
        sub
    }

    pub fn cancel(&mut self, sub: &Sub) {
        self.session.cancel(&sub.tx);
    }
}

/// Bytes the allocator has handed out and not got back.
pub fn heap_bytes() -> u64 {
    // SAFETY: mallinfo2 only reads allocator statistics.
    let info = unsafe { libc::mallinfo2() };
    (info.uordblks + info.hblkhd) as u64
}

pub fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|text| text.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

/// User plus system CPU seconds consumed by the process.
pub fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is given and RUSAGE_SELF is valid.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// Latency samples in nanoseconds.
#[derive(Default, Clone)]
pub struct Samples(pub Vec<u64>);

impl Samples {
    pub fn push(&mut self, value: u64) {
        self.0.push(value);
    }

    pub fn quantile(&self, q: f64) -> f64 {
        if self.0.is_empty() {
            return f64::NAN;
        }
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        let rank = (q * (sorted.len() - 1) as f64).round() as usize;
        sorted[rank.min(sorted.len() - 1)] as f64
    }

    pub fn mean(&self) -> f64 {
        if self.0.is_empty() {
            return f64::NAN;
        }
        self.0.iter().sum::<u64>() as f64 / self.0.len() as f64
    }

    /// p50, p99, max and mean in microseconds, keyed with `prefix`.
    pub fn report_us(&self, prefix: &str, into: &mut serde_json::Map<String, Value>) {
        let us = |ns: f64| (ns / 10.0).round() / 100.0;
        into.insert(format!("{prefix}_p50_us"), us(self.quantile(0.5)).into());
        into.insert(format!("{prefix}_p99_us"), us(self.quantile(0.99)).into());
        into.insert(format!("{prefix}_max_us"), us(self.quantile(1.0)).into());
        into.insert(format!("{prefix}_mean_us"), us(self.mean()).into());
        into.insert(format!("{prefix}_n"), self.0.len().into());
    }
}

/// One data point: metrics plus the correctness checks that ran inside it.
pub struct Record {
    pub scenario: String,
    pub params: serde_json::Map<String, Value>,
    pub metrics: serde_json::Map<String, Value>,
    pub checks: serde_json::Map<String, Value>,
}

impl Record {
    pub fn new(scenario: &str) -> Self {
        Self {
            scenario: scenario.into(),
            params: serde_json::Map::new(),
            metrics: serde_json::Map::new(),
            checks: serde_json::Map::new(),
        }
    }

    pub fn param(&mut self, key: &str, value: impl Into<Value>) -> &mut Self {
        self.params.insert(key.into(), value.into());
        self
    }

    pub fn metric(&mut self, key: &str, value: impl Into<Value>) -> &mut Self {
        self.metrics.insert(key.into(), value.into());
        self
    }

    /// Records a correctness check. A failed check fails the data point.
    pub fn check(&mut self, key: &str, ok: bool) -> &mut Self {
        self.checks.insert(key.into(), ok.into());
        self
    }

    /// Standard sink checks: nothing errored, no sequence gap, per-writer
    /// order held.
    pub fn sink_checks(&mut self, sink: &Sink) -> &mut Self {
        let gaps = sink.seq_gaps.load(Relaxed);
        let order = sink.order_violations.load(Relaxed);
        let errors = sink.errors.load(Relaxed);
        self.metric("seq_gaps", gaps)
            .metric("order_violations", order)
            .metric("sink_errors", errors)
            .metric("seq_resets", sink.seq_resets.load(Relaxed))
            .check("no_seq_gaps", gaps == 0)
            .check("per_writer_order", order == 0)
            .check("no_errors", errors == 0)
    }

    /// The record as one JSON object: metric values, scale, commit and checks.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "version": crate::compat::VERSION,
            "commit": commit(),
            "scale": scale(),
            "scenario": self.scenario,
            "params": self.params,
            "metrics": self.metrics,
            "checks": self.checks,
        })
    }

    pub fn failed_checks(&self) -> Vec<&str> {
        self.checks
            .iter()
            .filter(|(_, ok)| ok.as_bool() != Some(true))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Prints the record as a `RESULT` line and keeps it for [`take_records`].
    pub fn emit(self) {
        println!("RESULT {}", self.to_json());
        RECORDS.lock().push(self);
    }
}

static RECORDS: parking_lot::Mutex<Vec<Record>> = parking_lot::Mutex::new(Vec::new());
static SCALE: AtomicU64 = AtomicU64::new(0);

/// Records emitted since the last call.
pub fn take_records() -> Vec<Record> {
    std::mem::take(&mut *RECORDS.lock())
}

pub fn set_scale(scale: f64) {
    SCALE.store(scale.to_bits(), Relaxed);
}

/// The `scale=` multiplier of the running scenario. 1.0 is full scale.
pub fn scale() -> f64 {
    match SCALE.load(Relaxed) {
        0 => 1.0,
        bits => f64::from_bits(bits),
    }
}

/// The commit under test: `MYKO_BENCH_COMMIT`, or `git rev-parse` in the
/// crate directory.
pub fn commit() -> String {
    static COMMIT: OnceLock<String> = OnceLock::new();
    COMMIT
        .get_or_init(|| {
            std::env::var("MYKO_BENCH_COMMIT").ok().unwrap_or_else(|| {
                std::process::Command::new("git")
                    .args(["rev-parse", "--short=10", "HEAD"])
                    .current_dir(env!("CARGO_MANIFEST_DIR"))
                    .output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map_or_else(
                        || "unknown".to_owned(),
                        |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
                    )
            })
        })
        .clone()
}
