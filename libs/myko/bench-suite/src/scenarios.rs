//! The scenarios. Each emits one `RESULT` line per data point, with the
//! correctness checks that ran inside it. `GATES.md` states what each must
//! meet.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use myko::{
    core::item::AnyItem,
    event::{MEvent, MEventType},
    hyphae::{MapDiff, Signal},
    prelude::*,
};

use super::{
    entities::{blob, *},
    harness::{
        Bench, Client, NONE, Record, Samples, Sub, Wire, heap_bytes, now_ns, rss_bytes, sleep_until,
    },
    payload::{self, NodeMaker},
    reports::*,
    tick::{self, PROBE_WRITER, TickPlan, spawn_churn, spawn_probe},
    views::*,
};
use crate::compat::{self as adapter, pulse::*};

pub struct Args(HashMap<String, String>);

impl Args {
    pub fn parse(args: impl Iterator<Item = String>) -> Self {
        let args = Self(
            args.filter_map(|arg| {
                let (key, value) = arg.split_once('=')?;
                Some((key.to_owned(), value.to_owned()))
            })
            .collect(),
        );
        super::harness::set_scale(args.f64("scale", 1.0));
        args
    }

    /// Arguments from one `key=value ...` string.
    pub fn of(text: &str) -> Self {
        Self::parse(text.split_whitespace().map(str::to_owned))
    }

    pub fn str(&self, key: &str, default: &str) -> String {
        self.0
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_owned())
    }

    pub fn f64(&self, key: &str, default: f64) -> f64 {
        self.0
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// A count, multiplied by `scale` so a smoke run can shrink everything.
    pub fn count(&self, key: &str, default: usize) -> usize {
        let raw = self
            .0
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default);
        ((raw as f64 * self.f64("scale", 1.0)).round() as usize).max(1)
    }

    pub fn usize(&self, key: &str, default: usize) -> usize {
        self.0
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    pub fn wire(&self) -> Wire {
        match self.str("wire", "json").as_str() {
            "cbor" => Wire::Cbor,
            "none" => Wire::None,
            _ => Wire::Json,
        }
    }
}

const QUIET: Duration = Duration::from_millis(60);
const LONG: Duration = Duration::from_secs(120);

fn settle(bench: &Bench) -> bool {
    bench.sink.quiesce(QUIET, LONG)
}

fn load_scenes(bench: &Bench, scenes: usize, projects: usize) {
    for chunk in (0..scenes).collect::<Vec<_>>().chunks(5_000) {
        let items: Vec<BScene> = chunk.iter().map(|i| payload::scene(*i, projects)).collect();
        bench
            .ctx
            .batch_set(items.iter().cloned())
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
}

/// Loads `rows` nodes in batches. Returns seconds.
fn load_nodes(bench: &Bench, maker: &NodeMaker, rows: usize, batch: usize) -> f64 {
    let started = now_ns();
    let mut next = 0;
    while next < rows {
        let end = (next + batch).min(rows);
        let items: Vec<Arc<dyn AnyItem>> = (next..end).map(|i| maker.item(i, NONE, 0, 0)).collect();
        bench
            .ctx
            .set_batch_any(items)
            .unwrap_or_else(|error| panic!("{error:?}"));
        next = end;
    }
    (now_ns() - started) as f64 / 1e9
}

fn store_len(bench: &Bench, entity_type: &str) -> usize {
    bench
        .ctx
        .registry
        .get_or_create(entity_type)
        .snapshot()
        .len()
}

fn in_query(ids: impl Iterator<Item = usize>) -> GetBNodesByQuery {
    GetBNodesByQuery(BNodeQuery {
        id: Some(IdFilter::In(ids.map(payload::node_id).collect())),
        ..Default::default()
    })
}

fn us(ns: f64) -> f64 {
    (ns / 10.0).round() / 100.0
}

// ───────────────────────── tick scenarios: M1, M2, M3, M5, M7, S6, S8 ─────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Surface {
    Query,
    View,
    Report,
}

/// M1 (store and query), M2 (view with a join) and M3 (report family) under
/// the 30 Hz tick, with the M4 probe and the M5 churn beside it.
pub fn tick_scenario(name: &str, args: &Args) {
    let surface = match args.str("surface", "query").as_str() {
        "view" => Surface::View,
        "report" => Surface::Report,
        _ => Surface::Query,
    };
    let clients = args.usize("clients", 12);
    let ids_each = args.count("ids_each", 5_400);
    let tick_rows = args.count("tick_rows", 25_000);
    let rows = args
        .count("rows", 200_000)
        .max(clients * ids_each + tick_rows + 2_048);
    let payload_bytes = args.usize("payload", 512);
    let shared = args.usize("shared", 0) == 1;
    let hz = args.f64("hz", 30.0);
    let ticks = args.usize("ticks", 150);
    let warmup = args.usize("warmup", 30);
    let counting = args.usize("count", 0) == 1;
    let churn_rate = args.f64("churn", 0.0);
    let probe_hz = args.f64("probe_hz", 10.0);
    let scenes = 240;
    super::harness::gate_writes(args.usize("gate", 0) == 1);

    let mut record = Record::new(name);
    record
        .param("surface", args.str("surface", "query"))
        .param("rows", rows)
        .param("payload", payload_bytes)
        .param("clients", clients)
        .param("tick_rows", tick_rows)
        .param("hz", hz)
        .param("wire", args.str("wire", "json"))
        .param("shared", shared)
        .param("churn_per_s", churn_rate)
        .param("counting", counting)
        .param("gate", args.usize("gate", 0))
        .param("probe_hz", probe_hz)
        .param("pulse_hz", args.f64("pulse_hz", 0.0));

    let bench = Bench::new(args.wire(), 400_000, 8);
    let maker = Arc::new(NodeMaker::new(payload_bytes, scenes, 64));
    record.metric("payload_json_bytes", maker.payload_len());
    load_scenes(&bench, scenes, clients);
    let load_s = load_nodes(&bench, &maker, rows, 1_000);
    record.metric("load_s", load_s);

    // Rows the tick rewrites, and how many deliveries one tick must cause.
    let mut tick_set: Vec<usize> = Vec::with_capacity(tick_rows);
    let expect_per_tick;
    let initial_rows;
    let subscribe_started = now_ns();
    let mut sessions: Vec<Client> = (0..clients)
        .map(|c| bench.client(&format!("client-{c}")))
        .collect();
    // (subscription, the hot row it must end on) for the convergence check.
    let mut watched: Vec<(Sub, usize)> = Vec::new();
    match surface {
        Surface::Query => {
            let hot_each = if shared {
                ids_each.min(tick_rows)
            } else {
                ids_each.min(tick_rows / clients)
            };
            for (c, session) in sessions.iter_mut().enumerate() {
                let base = if shared { 0 } else { c * ids_each };
                let sub = session.sub_query(&in_query(base..base + ids_each));
                watched.extend((base..base + hot_each).map(|row| (sub.clone(), row)));
                if !shared || c == 0 {
                    tick_set.extend(base..base + hot_each);
                }
            }
            expect_per_tick = (clients * hot_each) as u64;
            initial_rows = (clients * ids_each) as u64;
            let mut pad = clients * ids_each;
            while tick_set.len() < tick_rows {
                tick_set.push(pad);
                pad += 1;
            }
        }
        Surface::View => {
            for (c, session) in sessions.iter_mut().enumerate() {
                session.sub_view(&GetNodesWithScene {
                    project_id: Arc::from(format!("project-{c}")),
                });
            }
            // Every node belongs to one project's view, so every changed row
            // is delivered once. The last row is left for the probe.
            let step = ((rows - 1) / tick_rows).max(1);
            tick_set.extend((0..rows - 1).step_by(step).take(tick_rows));
            expect_per_tick = tick_set.len() as u64;
            initial_rows = rows as u64;
        }
        Surface::Report => {
            for (c, session) in sessions.iter_mut().enumerate() {
                for row in c * ids_each..(c + 1) * ids_each {
                    let sub = session.sub_report(&NodeValue {
                        node_id: payload::node_id(row),
                    });
                    if row < tick_rows {
                        watched.push((sub, row));
                    }
                }
            }
            let hot = tick_rows.min(clients * ids_each);
            tick_set.extend(0..hot);
            let mut pad = clients * ids_each;
            while tick_set.len() < tick_rows {
                tick_set.push(pad);
                pad += 1;
            }
            expect_per_tick = hot as u64;
            initial_rows = (clients * ids_each) as u64;
        }
    }
    let settled = settle(&bench);
    record
        .metric("subscribe_s", (now_ns() - subscribe_started) as f64 / 1e9)
        .metric("initial_rows", bench.sink.total_rows_cb())
        .check("initial_settled", settled)
        .check(
            "initial_rows_exact",
            bench.sink.total_rows_cb() == initial_rows,
        );
    let probe_row = rows - 1;
    assert!(!tick_set.contains(&probe_row));

    // Taps below the client session: one on the store, one on the cell a
    // subscription reads. Comparing their counts with the session's shows
    // which layer merges writes.
    let taps = args.usize("taps", 0) == 1;
    let store_tap = Arc::new(AtomicU64::new(0));
    let cell_tap = Arc::new(AtomicU64::new(0));
    let mut tap_guards = Vec::new();
    let mut cell_tap_rows = 0u64;
    if taps {
        fn tick_rows_in(diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>) -> u64 {
            match diff {
                MapDiff::Insert { value, .. }
                | MapDiff::Update {
                    new_value: value, ..
                } => u64::from(
                    super::harness::stamp_of_item(value).is_some_and(|stamp| stamp.tick != NONE),
                ),
                MapDiff::Batch { changes } => changes.iter().map(tick_rows_in).sum(),
                MapDiff::Initial { .. } | MapDiff::Remove { .. } => 0,
            }
        }
        let store = bench
            .ctx
            .registry
            .get_or_create(maker.node(0, NONE, 0, 0).entity_type());
        let counter = store_tap.clone();
        tap_guards.push(store.subscribe_diffs(move |diff| {
            counter.fetch_add(tick_rows_in(diff), Relaxed);
        }));
        let request = bench.ctx.new_server_transaction();
        match surface {
            Surface::Query => {
                let hot_each = if shared {
                    ids_each.min(tick_rows)
                } else {
                    ids_each.min(tick_rows / clients)
                };
                let cell = bench.ctx.query_map_untyped(in_query(0..ids_each), request);
                let counter = cell_tap.clone();
                tap_guards.push(cell.subscribe_diffs(move |diff| {
                    counter.fetch_add(tick_rows_in(diff), Relaxed);
                }));
                cell_tap_rows = hot_each as u64;
            }
            Surface::Report => {
                cell_tap_rows = 200.min(tick_rows) as u64;
                for row in 0..cell_tap_rows as usize {
                    let cell = bench.ctx.report(
                        NodeValue {
                            node_id: payload::node_id(row),
                        },
                        request.clone(),
                    );
                    let counter = cell_tap.clone();
                    tap_guards.push(cell.subscribe(move |signal| {
                        if let Signal::Value(out) = signal {
                            counter.fetch_add(u64::from(out.tick != NONE), Relaxed);
                        }
                    }));
                }
            }
            Surface::View => {}
        }
        settle(&bench);
        store_tap.store(0, Relaxed);
        cell_tap.store(0, Relaxed);
    }
    bench
        .sink
        .track_rows
        .store(args.usize("track", 0) == 1, Relaxed);

    let stop = Arc::new(AtomicBool::new(false));
    // An event stream beside the tick: single pulses for one emitter, each of
    // which must reach its subscriber on its own.
    let pulse_hz = args.f64("pulse_hz", 0.0);
    let pulses = (pulse_hz > 0.0).then(|| {
        bench
            .ctx
            .set(payload::target(0, 0))
            .unwrap_or_else(|error| panic!("{error:?}"));
        bench
            .ctx
            .set(payload::emitter(0, 0, 0))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut client = bench.client("pulse-watcher");
        let sub = client.sub_query(&GetBPulsesByQuery(BPulseQuery {
            emitter_id: Some(IdFilter::Eq(payload::emitter_id(0))),
            ..Default::default()
        }));
        settle(&bench);
        let (bench, stop) = (bench.clone(), stop.clone());
        let before = bench.sink.slots[sub.slot].msgs.load(Relaxed);
        let handle = std::thread::spawn(move || {
            let data = payload::blob(128, 9);
            let mut pace = Paced::new(pulse_hz);
            let mut sent = 0u64;
            while !stop.load(Relaxed) {
                pace.wait();
                sent += 1;
                let event = MEvent::from_item(
                    &payload::pulse(0, sent, 200, &data),
                    MEventType::SET,
                    "bench",
                );
                super::harness::write(|| adapter::ingest(&bench.ctx, vec![event]));
            }
            sent
        });
        (client, sub, before, handle)
    });
    let probe = (probe_hz > 0.0)
        .then(|| spawn_probe(&bench, maker.clone(), probe_row, probe_hz, stop.clone()));
    // Churned subscriptions read rows the tick never writes.
    let cold = match surface {
        Surface::View => 0..0,
        Surface::Query | Surface::Report => {
            tick_set.iter().max().map_or(0, |max| max + 1)..rows - 1
        }
    };
    assert!(
        churn_rate == 0.0 || cold.len() > 1_000,
        "no cold rows for churn"
    );
    let churn = (churn_rate > 0.0).then(|| spawn_churn(&bench, cold, churn_rate, 50, stop.clone()));
    let rss_early = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (stop, rss_early) = (stop.clone(), rss_early.clone());
        let after = Duration::from_secs_f64((warmup + ticks) as f64 / hz * 0.1);
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            while started.elapsed() < after && !stop.load(Relaxed) {
                std::thread::sleep(Duration::from_millis(20));
            }
            rss_early.store(rss_bytes(), Relaxed);
        })
    };
    std::thread::sleep(Duration::from_millis(250));

    let (clones0, eqs0, sers0) = (
        blob::CLONES.load(Relaxed),
        blob::EQS.load(Relaxed),
        blob::SERIALIZES.load(Relaxed),
    );
    blob::COUNTING.store(counting, Relaxed);
    let outcome = tick::run_ticks(
        &bench,
        &TickPlan {
            rows: Arc::new(tick_set),
            maker,
            hz,
            warmup,
            ticks,
            expect_per_tick,
            paced: !counting,
            budget: Duration::from_secs_f64(((warmup + ticks) as f64 / hz * 2.0).max(30.0)),
        },
    );
    blob::COUNTING.store(false, Relaxed);
    let rss_late = rss_bytes();
    stop.store(true, Relaxed);
    let _ = sampler.join();

    outcome
        .completion_ser
        .report_us("tick_ser", &mut record.metrics);
    outcome
        .completion_cb
        .report_us("tick_cb", &mut record.metrics);
    outcome
        .service_ser
        .report_us("tick_service", &mut record.metrics);
    outcome
        .write_call
        .report_us("tick_write_call", &mut record.metrics);
    outcome
        .lateness
        .report_us("tick_lateness", &mut record.metrics);
    outcome
        .builder_late
        .report_us("tick_builder_late", &mut record.metrics);
    let changed = (outcome.issued.saturating_sub(warmup) * tick_rows).max(1) as f64;
    record
        .metric("ticks_issued", outcome.issued)
        .metric("ticks_incomplete", outcome.incomplete)
        .metric("rows_delivered", outcome.delivered)
        .metric("rows_expected", outcome.expected)
        .metric("cores", (outcome.cores * 100.0).round() / 100.0)
        .metric(
            "per_row_service_us",
            us(outcome.service_ser.quantile(0.5) / tick_rows as f64 * 1000.0) / 1000.0,
        )
        .metric(
            "per_row_cpu_us",
            us(outcome.cores * outcome.wall_s * 1e9 / changed * 1000.0) / 1000.0,
        )
        .metric(
            "bytes_per_row",
            bench.sink.bytes_ser.load(Relaxed) / bench.sink.rows_ser.load(Relaxed).max(1),
        )
        .metric(
            "ser_ns_per_row",
            bench.sink.ser_ns.load(Relaxed) / bench.sink.rows_ser.load(Relaxed).max(1),
        )
        .metric("rss_mb", rss_late / 1_048_576)
        .metric("rss_early_mb", rss_early.load(Relaxed) / 1_048_576)
        .metric(
            "rss_drift",
            (rss_late as f64 / rss_early.load(Relaxed).max(1) as f64 * 1000.0).round() / 1000.0,
        )
        .check("tick_not_aborted", !outcome.aborted)
        .check(
            "rows_delivered_exact",
            outcome.delivered == outcome.expected && outcome.incomplete == 0,
        );
    if counting {
        let all = ((warmup + outcome.issued.saturating_sub(warmup)) * tick_rows).max(1) as f64;
        let delivered = (outcome.issued as u64 * expect_per_tick).max(1) as f64;
        let per = |now: u64, before: u64| ((now - before) as f64 / all * 1000.0).round() / 1000.0;
        record
            .metric(
                "clones_per_changed_row",
                per(blob::CLONES.load(Relaxed), clones0),
            )
            .metric("eqs_per_changed_row", per(blob::EQS.load(Relaxed), eqs0))
            .metric(
                "serializes_per_changed_row",
                per(blob::SERIALIZES.load(Relaxed), sers0),
            )
            .metric(
                "serializes_per_delivered_row",
                (((blob::SERIALIZES.load(Relaxed) - sers0) as f64 / delivered) * 1000.0).round()
                    / 1000.0,
            )
            // Per distinct subscribed row that changed. With 12 identical
            // subscriptions this is 12 unless serialization is shared.
            .metric(
                "serializes_per_subscribed_row",
                (((blob::SERIALIZES.load(Relaxed) - sers0) as f64 / delivered
                    * if shared { clients as f64 } else { 1.0 })
                    * 1000.0)
                    .round()
                    / 1000.0,
            );
    }
    if let Some(probe) = probe {
        let probe = probe.join().unwrap_or_default();
        probe.other.report_us("probe_other", &mut record.metrics);
        probe
            .other_call
            .report_us("probe_other_call", &mut record.metrics);
        probe.same.report_us("probe_same", &mut record.metrics);
        probe
            .same_call
            .report_us("probe_same_call", &mut record.metrics);
        record
            .metric("probe_early_returns", probe.early_returns)
            .metric("probe_lost", probe.lost)
            .check("probe_nothing_lost", probe.lost == 0)
            .check("probe_values_correct", probe.wrong_value == 0);
    }
    if let Some(churn) = churn {
        let churn = churn.join().unwrap_or_default();
        churn
            .subscribe_call
            .report_us("churn_subscribe_call", &mut record.metrics);
        churn
            .first_delivery
            .report_us("churn_first_delivery", &mut record.metrics);
        churn
            .cancel_call
            .report_us("churn_cancel_call", &mut record.metrics);
        record
            .metric("churn_subscribes", churn.subscribes)
            .metric("churn_cancels", churn.cancels)
            .check("churn_nothing_lost", churn.lost == 0);
    }
    settle(&bench);
    if let Some((client, sub, before, handle)) = pulses {
        let sent = handle.join().unwrap_or(0);
        settle(&bench);
        let delivered = bench.sink.slots[sub.slot].msgs.load(Relaxed) - before;
        record
            .metric("pulses_sent", sent)
            .metric("pulses_delivered", delivered)
            .check("every_pulse_delivered", delivered == sent)
            .check(
                "last_pulse_delivered",
                bench.sink.slots[sub.slot].last_val.load(Relaxed) == sent,
            );
        drop(client);
    }
    // Per tick: did every row arrive, some, or none.
    let (mut complete, mut partial, mut empty) = (0u64, 0u64, 0u64);
    for rows in bench
        .sink
        .tick_rows
        .iter()
        .take(outcome.issued)
        .skip(warmup)
    {
        match rows.load(Relaxed) {
            0 => empty += 1,
            rows if rows == expect_per_tick => complete += 1,
            _ => partial += 1,
        }
    }
    record
        .metric("ticks_complete", complete)
        .metric("ticks_partial", partial)
        .metric("ticks_empty", empty);
    if taps {
        let issued = outcome.issued as u64;
        record
            .metric("store_tap_rows", store_tap.load(Relaxed))
            .metric("store_tap_expected", issued * tick_rows as u64)
            .metric("cell_tap_rows", cell_tap.load(Relaxed))
            .metric("cell_tap_expected", issued * cell_tap_rows);
    }
    if bench.sink.track_rows.load(Relaxed) || surface == Surface::Report {
        // Does each subscription end on the last value written?
        let last = outcome.issued as u64;
        let rows = bench.sink.row_last.lock();
        let stale = watched
            .iter()
            .filter(|(sub, row)| match surface {
                Surface::Report => bench.sink.slots[sub.slot].last_val.load(Relaxed) != last,
                _ => rows.get(&(sub.slot, payload::node_id(*row).0)).copied() != Some(last),
            })
            .count();
        record
            .metric("stale_rows_after_settle", stale)
            .metric("watched_rows", watched.len())
            .check("converged_to_last_write", stale == 0);
    }
    drop(tap_guards);
    record.sink_checks(&bench.sink);
    record.emit();
    drop(sessions);
}

/// M4 idle baseline: the probe alone, on the M1 graph, with no tick.
pub fn idle(args: &Args) {
    let clients = 12;
    let ids_each = args.count("ids_each", 5_400);
    let rows = args.count("rows", 200_000).max(clients * ids_each + 8);
    let seconds = args.f64("seconds", 20.0);
    let bench = Bench::new(args.wire(), 100_000, 8);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    load_scenes(&bench, 240, clients);
    load_nodes(&bench, &maker, rows, 1_000);
    let mut sessions: Vec<Client> = (0..clients)
        .map(|c| bench.client(&format!("client-{c}")))
        .collect();
    for (c, session) in sessions.iter_mut().enumerate() {
        session.sub_query(&in_query(c * ids_each..(c + 1) * ids_each));
    }
    settle(&bench);
    let stop = Arc::new(AtomicBool::new(false));
    let probe = spawn_probe(
        &bench,
        maker,
        rows - 1,
        args.f64("probe_hz", 10.0),
        stop.clone(),
    );
    std::thread::sleep(Duration::from_secs_f64(seconds));
    stop.store(true, Relaxed);
    let probe = probe.join().unwrap_or_default();
    let mut record = Record::new("idle");
    record.param("rows", rows);
    probe.other.report_us("probe_other", &mut record.metrics);
    probe
        .other_call
        .report_us("probe_other_call", &mut record.metrics);
    probe.same.report_us("probe_same", &mut record.metrics);
    probe
        .same_call
        .report_us("probe_same_call", &mut record.metrics);
    record
        .metric("probe_early_returns", probe.early_returns)
        .check("probe_nothing_lost", probe.lost == 0)
        .check("probe_values_correct", probe.wrong_value == 0);
    settle(&bench);
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── S1 + S2: latency and cost against query count ──

struct Paced {
    period_ns: u64,
    next: u64,
}

impl Paced {
    fn new(hz: f64) -> Self {
        Self {
            period_ns: (1e9 / hz) as u64,
            next: now_ns(),
        }
    }

    fn wait(&mut self) {
        self.next = (self.next + self.period_ns).max(now_ns());
        sleep_until(self.next);
    }
}

/// One write to `row`, timed to the callback on `sub`.
struct WriteProbe {
    latency: Samples,
    call: Samples,
    lost: u64,
    wrong: u64,
    seq: u64,
}

impl WriteProbe {
    fn new() -> Self {
        Self {
            latency: Samples::default(),
            call: Samples::default(),
            lost: 0,
            wrong: 0,
            seq: 0,
        }
    }

    fn write(&mut self, bench: &Bench, maker: &NodeMaker, row: usize, sub: &Sub, seq: u64) {
        let sink = &bench.sink;
        let before = sink.slots[sub.slot].msgs.load(Relaxed);
        let node = maker.node(row, NONE, seq, PROBE_WRITER);
        let t0 = now_ns();
        let _ = bench.ctx.set(node);
        let t1 = now_ns();
        match sink.wait_slot(sub.slot, before, Duration::from_secs(10)) {
            Some(cb) => {
                self.latency.push(cb.saturating_sub(t0));
                self.call.push(t1 - t0);
                self.wrong += u64::from(sink.slots[sub.slot].last_val.load(Relaxed) != seq);
            }
            None => self.lost += 1,
        }
        self.seq = seq;
    }
}

/// S1 and S2 in one sweep. At each query count it times writes to one hot
/// row that a single query matches (S2: cost against non-matching queries)
/// and writes spread over rows that each match one of the queries (S1).
pub fn sweep(args: &Args) {
    let kind = args.str("kind", "by_ids");
    let default_steps = match kind.as_str() {
        "scan" => "0,250,1000,4000",
        "view" => "0,12,48,192",
        _ => "0,1000,10000,50000",
    };
    let steps: Vec<usize> = args
        .str("steps", default_steps)
        .split(',')
        .filter_map(|s| s.parse::<usize>().ok())
        .map(|s| (s as f64 * args.f64("scale", 1.0)).round() as usize)
        .collect();
    let max_q = steps.iter().copied().max().unwrap_or(0);
    let rows = args.count("rows", 50_000).max(max_q) + 1;
    let writes = args.usize("writes", 300);
    let hot_row = max_q;
    // Query j selects by row j, scene j or category j. The hot row has its
    // own scene and category, which no other query names.
    let groups = max_q + 1;

    let bench = Bench::new(args.wire(), max_q + 64, 4);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), groups, groups));
    if matches!(kind.as_str(), "fk" | "live" | "scene_stats" | "view") {
        // One scene per query, and one project per scene for the view kind.
        load_scenes(&bench, groups, groups);
    } else {
        load_scenes(&bench, 16, 12);
    }
    load_nodes(&bench, &maker, rows, 1_000);

    let subscribe = |client: &mut Client, index: usize| -> Sub {
        match kind.as_str() {
            "by_ids" => client.sub_query(&GetBNodesByIds {
                ids: vec![payload::node_id(index)],
            }),
            "by_id_report" => client.sub_report(&GetBNodeById {
                id: payload::node_id(index),
            }),
            "fk" => client.sub_query(&GetBNodesByQuery(BNodeQuery {
                scene_id: Some(IdFilter::Eq(payload::scene_id(index))),
                ..Default::default()
            })),
            "scan" => client.sub_query(&GetBNodesByQuery(BNodeQuery {
                category: Some(StringFilter::Eq(format!("cat-{index}").into())),
                ..Default::default()
            })),
            "report" => client.sub_report(&NodeValue {
                node_id: payload::node_id(index),
            }),
            "scene_stats" => client.sub_report(&SceneStats {
                scene_id: payload::scene_id(index),
            }),
            "live" => client.sub_report(&LiveSceneStats {
                scene_id: payload::scene_id(index),
            }),
            "view" => client.sub_view(&GetNodesWithScene {
                project_id: Arc::from(format!("project-{index}")),
            }),
            other => panic!("unknown kind {other}"),
        }
    };

    let mut hot_client = bench.client("hot");
    let hot_sub = subscribe(&mut hot_client, hot_row);
    let mut clients: Vec<Client> = Vec::new();
    let mut subs: Vec<Sub> = Vec::new();
    let mut seq = 0u64;
    let mut rng = 0x243f_6a88_85a3_08d3u64;
    settle(&bench);

    for queries in steps {
        let started = now_ns();
        while subs.len() < queries {
            if subs.len() % 10 == 0 {
                clients.push(bench.client(&format!("client-{}", clients.len())));
            }
            let index = subs.len();
            let client = clients.last_mut().unwrap_or_else(|| unreachable!());
            subs.push(subscribe(client, index));
        }
        let subscribe_s = (now_ns() - started) as f64 / 1e9;
        let settled = settle(&bench);
        let msgs_before = bench.sink.total_msgs();
        let heap = heap_bytes();
        let (clones0, eqs0) = (blob::CLONES.load(Relaxed), blob::EQS.load(Relaxed));
        blob::COUNTING.store(true, Relaxed);

        let mut hot = WriteProbe::new();
        let mut pace = Paced::new(args.f64("write_hz", 500.0));
        for _ in 0..writes {
            pace.wait();
            seq += 1;
            hot.write(&bench, &maker, hot_row, &hot_sub, seq);
        }
        let mut spread = WriteProbe::new();
        if queries > 0 {
            for _ in 0..writes {
                pace.wait();
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let index = (rng >> 33) as usize % queries;
                seq += 1;
                spread.write(&bench, &maker, index, &subs[index], seq);
            }
        }
        settle(&bench);
        blob::COUNTING.store(false, Relaxed);
        let written = (hot.latency.0.len() + spread.latency.0.len()) as u64;
        let msgs = bench.sink.total_msgs() - msgs_before;

        let mut record = Record::new("sweep");
        record
            .param("kind", kind.clone())
            .param("queries", queries)
            .param("rows", rows)
            .metric("subscribe_s", subscribe_s)
            .metric("heap_mb", heap / 1_048_576)
            .metric("heap_bytes", heap)
            .metric(
                "payload_clones_per_write",
                (blob::CLONES.load(Relaxed) - clones0) as f64 / written.max(1) as f64,
            )
            .metric(
                "payload_eqs_per_write",
                (blob::EQS.load(Relaxed) - eqs0) as f64 / written.max(1) as f64,
            )
            .metric("msgs_for_writes", msgs)
            .metric("writes", written);
        hot.latency.report_us("hot_latency", &mut record.metrics);
        hot.call.report_us("hot_call", &mut record.metrics);
        if queries > 0 {
            spread
                .latency
                .report_us("spread_latency", &mut record.metrics);
            spread.call.report_us("spread_call", &mut record.metrics);
        }
        record
            .check("settled", settled)
            .check("nothing_lost", hot.lost + spread.lost == 0)
            .check("values_correct", hot.wrong + spread.wrong == 0)
            // A write reaches the one query that matches it and no other.
            .check("no_spurious_delivery", msgs == written);
        record.sink_checks(&bench.sink);
        record.emit();
    }
}

// ───────────────────────── S3: cost of a client connecting and leaving ────

pub fn churn_cost(args: &Args) {
    let steps: Vec<usize> = args
        .str("steps", "1000,10000,50000")
        .split(',')
        .filter_map(|s| s.parse::<usize>().ok())
        .map(|s| (s as f64 * args.f64("scale", 1.0)).round() as usize)
        .collect();
    let max_q = steps.iter().copied().max().unwrap_or(0);
    let rows = args.count("rows", 50_000).max(max_q) + 1;
    let cycles = args.usize("cycles", 300);
    let scenes = 1_000;
    let bench = Bench::new(args.wire(), max_q + cycles * 4 * steps.len() + 64, 4);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), scenes, 64));
    load_scenes(&bench, scenes, 12);
    load_nodes(&bench, &maker, rows, 1_000);

    let mut clients: Vec<Client> = Vec::new();
    let mut population = 0usize;
    let mut rng = 0x1319_8a2e_0370_7344u64;
    let mut next = move |bound: usize| {
        rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (rng >> 33) as usize % bound
    };
    for queries in steps {
        while population < queries {
            if population % 10 == 0 {
                clients.push(bench.client(&format!("client-{}", clients.len())));
            }
            let client = clients.last_mut().unwrap_or_else(|| unreachable!());
            client.sub_query(&GetBNodesByIds {
                ids: vec![payload::node_id(population)],
            });
            population += 1;
        }
        let settled = settle(&bench);
        adapter::sweep(&bench.ctx);
        let (live0, heap0) = (adapter::cache_live(&bench.ctx), heap_bytes());

        let names = ["by_ids", "fk", "report", "by_id_report"];
        let mut call: Vec<Samples> = names.iter().map(|_| Samples::default()).collect();
        let mut first: Vec<Samples> = names.iter().map(|_| Samples::default()).collect();
        let mut disconnect = Samples::default();
        let mut cycle = Samples::default();
        let mut lost = 0u64;
        for index in 0..cycles {
            let started = now_ns();
            let mut client = bench.client(&format!("visitor-{index}"));
            let row = next(rows - 2);
            for (kind, name) in names.iter().enumerate() {
                let t0 = now_ns();
                let sub = match *name {
                    "by_ids" => client.sub_query(&GetBNodesByIds {
                        ids: vec![payload::node_id(row), payload::node_id(row + 1)],
                    }),
                    "fk" => client.sub_query(&GetBNodesByQuery(BNodeQuery {
                        scene_id: Some(IdFilter::Eq(payload::scene_id(next(scenes)))),
                        ..Default::default()
                    })),
                    "report" => client.sub_report(&NodeValue {
                        node_id: payload::node_id(row),
                    }),
                    _ => client.sub_report(&GetBNodeById {
                        id: payload::node_id(row),
                    }),
                };
                call[kind].push(now_ns() - t0);
                match bench.sink.wait_slot(sub.slot, 0, Duration::from_secs(10)) {
                    Some(cb) => first[kind].push(cb.saturating_sub(t0)),
                    None => lost += 1,
                }
            }
            let t0 = now_ns();
            drop(client);
            disconnect.push(now_ns() - t0);
            cycle.push(now_ns() - started);
        }
        settle(&bench);
        adapter::sweep(&bench.ctx);
        let mut record = Record::new("churn_cost");
        record.param("population", queries).param("rows", rows);
        for (kind, name) in names.iter().enumerate() {
            call[kind].report_us(&format!("{name}_subscribe_call"), &mut record.metrics);
            first[kind].report_us(&format!("{name}_first_delivery"), &mut record.metrics);
        }
        disconnect.report_us("disconnect", &mut record.metrics);
        cycle.report_us("connect_cycle", &mut record.metrics);
        record
            .metric(
                "heap_growth_kb",
                (heap_bytes() as i64 - heap0 as i64) / 1024,
            )
            .check("settled", settled)
            .check("nothing_lost", lost == 0);
        if let (Some(before), Some(after)) = (live0, adapter::cache_live(&bench.ctx)) {
            let query_growth = after.0 as i64 - before.0 as i64;
            let report_growth = after.1 as i64 - before.1 as i64;
            record
                .metric("query_cache_live_growth", query_growth)
                .metric("report_cache_live_growth", report_growth)
                .check(
                    "no_live_cache_growth",
                    query_growth == 0 && report_growth == 0,
                );
        }
        record.sink_checks(&bench.sink);
        record.emit();
    }
}

// ───────────────────────── S4: a report that reads reports ────────────────

pub fn chain(args: &Args) {
    let depths: Vec<u32> = args
        .str("depths", "1,5,20")
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    let chains = args.count("chains", 1_000);
    let rows = args.count("rows", 50_000).max(chains * 32);
    let writes = args.usize("writes", 300);
    let bench = Bench::new(args.wire(), chains * depths.len() + 64, 4);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    load_scenes(&bench, 240, 12);
    load_nodes(&bench, &maker, rows, 1_000);
    let mut seq = 0u64;
    for depth in depths {
        // Each depth gets its own nodes, so no level is shared across depths.
        let base = depth as usize * chains;
        let started = now_ns();
        let mut client = bench.client(&format!("chain-{depth}"));
        let subs: Vec<Sub> = (0..chains)
            .map(|index| {
                client.sub_report(&Chain {
                    node_id: payload::node_id(base + index),
                    depth,
                })
            })
            .collect();
        let subscribe_s = (now_ns() - started) as f64 / 1e9;
        let settled = settle(&bench);

        let mut single = WriteProbe::new();
        let mut pace = Paced::new(500.0);
        for write in 0..writes {
            pace.wait();
            seq += 1;
            let index = write * 7 % chains;
            single.write(&bench, &maker, base + index, &subs[index], seq);
        }
        settle(&bench);

        // Bulk: rewrite every base node in one batch and time the last output.
        let mut bulk = Samples::default();
        let mut bulk_lost = 0u64;
        for _ in 0..20 {
            seq += 1;
            let before: Vec<u64> = subs
                .iter()
                .map(|sub| bench.sink.slots[sub.slot].msgs.load(Relaxed))
                .collect();
            let items: Vec<Arc<dyn AnyItem>> = (0..chains)
                .map(|index| maker.item(base + index, NONE, seq, PROBE_WRITER))
                .collect();
            let t0 = now_ns();
            let _ = bench.ctx.set_batch_any(items);
            let mut last = 0u64;
            for (sub, before) in subs.iter().zip(&before) {
                match bench
                    .sink
                    .wait_slot(sub.slot, *before, Duration::from_secs(10))
                {
                    Some(cb) => last = last.max(cb),
                    None => bulk_lost += 1,
                }
            }
            bulk.push(last.saturating_sub(t0));
            settle(&bench);
        }
        let wrong = subs
            .iter()
            .filter(|sub| bench.sink.slots[sub.slot].last_val.load(Relaxed) != seq)
            .count();

        let mut record = Record::new("chain");
        record
            .param("depth", depth)
            .param("chains", chains)
            .metric("subscribe_s", subscribe_s)
            .metric(
                "subscribe_us_per_chain",
                us(subscribe_s * 1e9 / chains as f64),
            );
        single.latency.report_us("latency", &mut record.metrics);
        single.call.report_us("call", &mut record.metrics);
        bulk.report_us("bulk", &mut record.metrics);
        record
            .metric("bulk_us_per_chain", us(bulk.quantile(0.5) / chains as f64))
            .check("settled", settled)
            .check("nothing_lost", single.lost + bulk_lost == 0)
            .check("values_correct", single.wrong == 0 && wrong == 0);
        record.sink_checks(&bench.sink);
        record.emit();
        drop(client);
        settle(&bench);
    }
}

// ───────────────────────── S5: belongs_to cascade and index churn ─────────

pub fn belongs_to(args: &Args) {
    let emitters = args.count("emitters", 40_000);
    let targets = args.count("targets", 5_000);
    let watched = args.count("watched", 1_000).min(targets);
    let batch = args.count("batch", 1_000).min(emitters);
    let batches = args.usize("batches", 30);
    let bench = Bench::new(args.wire(), watched + 64, 4);
    let ctx = &bench.ctx;

    let target_rows: Vec<BTarget> = (0..targets).map(|t| payload::target(t, 0)).collect();
    ctx.batch_set(target_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let mut parent: Vec<usize> = (0..emitters).map(|e| e % targets).collect();
    let started = now_ns();
    for chunk in (0..emitters).collect::<Vec<_>>().chunks(1_000) {
        let rows: Vec<BEmitter> = chunk
            .iter()
            .map(|e| payload::emitter(*e, parent[*e], 0))
            .collect();
        ctx.batch_set(rows.iter().cloned())
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    let load_s = (now_ns() - started) as f64 / 1e9;

    let mut clients: Vec<Client> = Vec::new();
    let subs: Vec<Sub> = (0..watched)
        .map(|t| {
            let mut client = bench.client(&format!("watcher-{t}"));
            let sub = client.sub_query(&GetBEmittersByQuery(BEmitterQuery {
                target_id: Some(IdFilter::Eq(payload::target_id(t))),
                ..Default::default()
            }));
            clients.push(client);
            sub
        })
        .collect();
    let settled = settle(&bench);
    let totals = |bench: &Bench| {
        let (mut rows, mut deletes) = (0u64, 0u64);
        for sub in &subs {
            rows += bench.sink.slots[sub.slot].rows.load(Relaxed);
            deletes += bench.sink.slots[sub.slot].deletes.load(Relaxed);
        }
        (rows, deletes)
    };
    let wait_totals = |bench: &Bench, rows: u64, deletes: u64| {
        let deadline = now_ns() + 20_000_000_000;
        loop {
            let now = totals(bench);
            if now.0 >= rows && now.1 >= deletes {
                return now_ns();
            }
            if now_ns() > deadline {
                return 0;
            }
            std::hint::spin_loop();
        }
    };
    let (initial_rows, _) = totals(&bench);
    let expected_initial = (0..emitters).filter(|e| parent[*e] < watched).count() as u64;

    // Re-parent: every emitter in the batch moves to the next target, so the
    // index drops it from one bucket and adds it to another.
    let mut reparent = Samples::default();
    let mut reparent_call = Samples::default();
    let (mut expect_rows, mut expect_deletes) = totals(&bench);
    let mut reparent_lost = 0u64;
    let mut revision = 0u64;
    for index in 0..batches {
        revision += 1;
        let moved: Vec<usize> = (0..batch).map(|k| (index * batch + k) % emitters).collect();
        let rows: Vec<BEmitter> = moved
            .iter()
            .map(|e| {
                let (old, new) = (parent[*e], (parent[*e] + 1) % targets);
                parent[*e] = new;
                expect_deletes += u64::from(old < watched);
                expect_rows += u64::from(new < watched);
                payload::emitter(*e, new, revision)
            })
            .collect();
        let t0 = now_ns();
        ctx.batch_set(rows.iter().cloned())
            .unwrap_or_else(|error| panic!("{error:?}"));
        reparent_call.push(now_ns() - t0);
        match wait_totals(&bench, expect_rows, expect_deletes) {
            0 => reparent_lost += 1,
            done => reparent.push(done - t0),
        }
    }
    settle(&bench);
    let after_reparent = totals(&bench);

    // Cascade: deleting a target deletes its emitters.
    let doomed = (watched / 5).max(1);
    let cascaded = (0..emitters).filter(|e| parent[*e] < doomed).count() as u64;
    let doomed_rows: Vec<BTarget> = (0..doomed).map(|t| payload::target(t, 0)).collect();
    let cascade_t0 = now_ns();
    ctx.batch_del(doomed_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let cascade_call = now_ns() - cascade_t0;
    let cascade_done = wait_totals(&bench, after_reparent.0, after_reparent.1 + cascaded);
    settle(&bench);
    let after_cascade = totals(&bench);
    let emitters_left = store_len(&bench, payload::emitter(0, 0, 0).entity_type());

    // owns_many: deleting a bundle deletes the parts it owns.
    let (bundles, parts_each) = (args.count("bundles", 2_000), 5);
    load_scenes(&bench, 16, 4);
    let part_rows: Vec<BPart> = (0..bundles * parts_each).map(payload::part).collect();
    ctx.batch_set(part_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let bundle_rows: Vec<BBundle> = (0..bundles)
        .map(|b| payload::bundle(b, parts_each, 16))
        .collect();
    ctx.batch_set(bundle_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    settle(&bench);
    let part_type = payload::part(0).entity_type();
    let parts_before = store_len(&bench, part_type);
    let gone = bundles / 4;
    let t0 = now_ns();
    ctx.batch_del(bundle_rows[..gone].iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let owns_call = now_ns() - t0;
    settle(&bench);
    let parts_after = store_len(&bench, part_type);

    let mut record = Record::new("belongs_to");
    record
        .param("emitters", emitters)
        .param("targets", targets)
        .param("watched", watched)
        .metric("load_s", load_s);
    reparent.report_us("reparent_batch", &mut record.metrics);
    reparent_call.report_us("reparent_call", &mut record.metrics);
    record
        .metric(
            "reparent_us_per_row",
            us(reparent.quantile(0.5) / batch as f64),
        )
        .metric("cascade_rows", cascaded)
        .metric("cascade_call_us", us(cascade_call as f64))
        .metric(
            "cascade_us_per_row",
            us(cascade_done.saturating_sub(cascade_t0) as f64 / cascaded.max(1) as f64),
        )
        .metric(
            "cascade_call_us_per_row",
            us(cascade_call as f64 / cascaded.max(1) as f64),
        )
        .metric(
            "owns_many_us_per_part",
            us(owns_call as f64 / (gone * parts_each).max(1) as f64),
        )
        .metric("emitters_left", emitters_left)
        .metric("parts_removed", parts_before.saturating_sub(parts_after))
        .check("settled", settled)
        .check("initial_rows_exact", initial_rows == expected_initial)
        .check("reparent_nothing_lost", reparent_lost == 0)
        .check(
            "reparent_deliveries_exact",
            after_reparent == (expect_rows, expect_deletes),
        )
        .check(
            "cascade_deletes_delivered",
            cascade_done != 0 && after_cascade.1 == after_reparent.1 + cascaded,
        )
        .check(
            "cascade_store_exact",
            emitters_left as u64 == emitters as u64 - cascaded,
        )
        .check(
            "owns_many_cascade_exact",
            parts_before - parts_after == gone * parts_each,
        );
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── S7: sequential-key event bursts ────────────────

pub fn bursts(args: &Args) {
    let emitters = args.count("emitters", 1_000);
    let targets = (emitters / 4).max(1);
    let writers = args.usize("writers", 4);
    let bursts = args.usize("bursts", 100);
    let sizes: Vec<usize> = args
        .str("sizes", "1,16,64")
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    let split = args.usize("split", 0) == 1;
    super::harness::gate_writes(args.usize("gate", 0) == 1);
    let bench = Bench::new(args.wire(), emitters + 64, 4);
    let ctx = &bench.ctx;
    let target_rows: Vec<BTarget> = (0..targets).map(|t| payload::target(t, 0)).collect();
    ctx.batch_set(target_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let emitter_rows: Vec<BEmitter> = (0..emitters)
        .map(|e| payload::emitter(e, e % targets, 0))
        .collect();
    ctx.batch_set(emitter_rows.iter().cloned())
        .unwrap_or_else(|error| panic!("{error:?}"));
    let mut clients: Vec<Client> = Vec::new();
    let subs: Vec<Sub> = (0..emitters)
        .map(|e| {
            let mut client = bench.client(&format!("watcher-{e}"));
            let sub = client.sub_query(&GetBPulsesByQuery(BPulseQuery {
                emitter_id: Some(IdFilter::Eq(payload::emitter_id(e))),
                ..Default::default()
            }));
            clients.push(client);
            sub
        })
        .collect();
    let settled = settle(&bench);
    let data = Arc::new(payload::blob(args.usize("payload", 512), 1));
    let seqs: Arc<Vec<AtomicU64>> = Arc::new((0..emitters).map(|_| AtomicU64::new(0)).collect());

    for size in sizes {
        let msgs_before = bench.sink.total_msgs();
        let started = now_ns();
        let handles: Vec<_> = (0..writers)
            .map(|writer| {
                let (bench, data, seqs) = (bench.clone(), data.clone(), seqs.clone());
                std::thread::spawn(move || {
                    let mut call = Samples::default();
                    let mut sent = 0u64;
                    // Each writer owns the emitters congruent to it, and one
                    // burst interleaves up to four of them.
                    let owned: Vec<usize> = (writer..emitters).step_by(writers).collect();
                    for burst in 0..bursts {
                        let lanes = size.min(4);
                        let events: Vec<MEvent> = (0..size)
                            .map(|k| {
                                let emitter = owned[(burst * 4 + k % lanes) % owned.len()];
                                let seq = seqs[emitter].fetch_add(1, Relaxed) + 1;
                                MEvent::from_item(
                                    &payload::pulse(emitter, seq, 100 + writer as u32, &data),
                                    MEventType::SET,
                                    "bench",
                                )
                            })
                            .collect();
                        sent += events.len() as u64;
                        let t0 = now_ns();
                        if split {
                            // One event per ingest call: no two events of a
                            // key ever share a batch.
                            for event in events {
                                super::harness::write(|| adapter::ingest(&bench.ctx, vec![event]));
                            }
                        } else {
                            super::harness::write(|| adapter::ingest(&bench.ctx, events));
                        }
                        call.push(now_ns() - t0);
                    }
                    (call, sent)
                })
            })
            .collect();
        let mut call = Samples::default();
        let mut sent = 0u64;
        for handle in handles {
            if let Ok((samples, count)) = handle.join() {
                call.0.extend(samples.0);
                sent += count;
            }
        }
        // Every event is its own delivery, so wait for that many messages.
        let deadline = now_ns() + 5_000_000_000;
        while bench.sink.total_msgs() - msgs_before < sent && now_ns() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let done = now_ns();
        settle(&bench);
        let delivered = bench.sink.total_msgs() - msgs_before;
        let latest_ok = subs.iter().enumerate().all(|(e, sub)| {
            let last = seqs[e].load(Relaxed);
            last == 0 || bench.sink.slots[sub.slot].last_val.load(Relaxed) == last
        });
        let mut record = Record::new("bursts");
        record
            .param("burst", size)
            .param("emitters", emitters)
            .param("writers", writers)
            .param("has_sequential", adapter::HAS_SEQUENTIAL)
            .param("split", split)
            .param("gate", args.usize("gate", 0))
            .metric("events_sent", sent)
            .metric("events_delivered", delivered)
            .metric("events_coalesced", sent.saturating_sub(delivered))
            .metric(
                "drain_us_per_event",
                us((done - started) as f64 / sent.max(1) as f64),
            );
        call.report_us("ingest_call", &mut record.metrics);
        record
            .metric("ingest_us_per_event", us(call.mean() / size as f64))
            .check("settled", settled)
            .check("every_event_delivered", delivered == sent)
            .check("latest_value_delivered", latest_ok);
        record.sink_checks(&bench.sink);
        record.emit();
    }
}

// ───────────────────────── M6: atomic batch across three types ────────────

pub fn atomic(args: &Args) {
    let trios = args.count("trios", 2_000);
    let group = args.usize("group", 1);
    let mutations = args.usize("mutations", 300);
    let writers = 2usize;
    let via_events = args.str("path", "typed") == "events";
    let bench = Bench::new(args.wire(), 64, 4);
    let ctx = &bench.ctx;
    ctx.batch_set((0..trios).map(|i| payload::target(i, 0)))
        .unwrap_or_else(|error| panic!("{error:?}"));
    ctx.batch_set((0..trios).map(|i| payload::emitter(i, i, 0)))
        .unwrap_or_else(|error| panic!("{error:?}"));
    ctx.batch_set((0..trios).map(|i| payload::gauge(i, 0)))
        .unwrap_or_else(|error| panic!("{error:?}"));
    let mut clients: Vec<Client> = (0..4)
        .map(|c| bench.client(&format!("client-{c}")))
        .collect();
    let subs: Vec<Sub> = clients
        .iter_mut()
        .map(|client| client.sub_view(&GetTrios {}))
        .collect();
    let settled = settle(&bench);
    let initial_ok = subs
        .iter()
        .all(|sub| bench.sink.slots[sub.slot].rows.load(Relaxed) == trios as u64);
    bench.sink.capture.store(true, Relaxed);

    let started = now_ns();
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let bench = bench.clone();
            std::thread::spawn(move || {
                let owned: Vec<usize> = (writer..trios).step_by(writers).collect();
                let mut revisions = vec![0u64; trios];
                let mut call = Samples::default();
                for mutation in 0..mutations {
                    let picked: Vec<usize> = (0..group)
                        .map(|k| owned[(mutation * group + k) % owned.len()])
                        .collect();
                    let mut typed: Vec<Arc<dyn AnyItem>> = Vec::new();
                    let mut events: Vec<MEvent> = Vec::new();
                    for trio in picked {
                        revisions[trio] += 1;
                        let revision = revisions[trio];
                        let (target, emitter, gauge) = (
                            payload::target(trio, revision),
                            payload::emitter(trio, trio, revision),
                            payload::gauge(trio, revision),
                        );
                        if via_events {
                            events.push(MEvent::from_item(&target, MEventType::SET, "bench"));
                            events.push(MEvent::from_item(&emitter, MEventType::SET, "bench"));
                            events.push(MEvent::from_item(&gauge, MEventType::SET, "bench"));
                        } else {
                            typed.push(Arc::new(target));
                            typed.push(Arc::new(emitter));
                            typed.push(Arc::new(gauge));
                        }
                    }
                    let t0 = now_ns();
                    if via_events {
                        adapter::ingest(&bench.ctx, events);
                    } else {
                        let _ = bench.ctx.set_batch_any(typed);
                    }
                    call.push(now_ns() - t0);
                }
                (call, revisions)
            })
        })
        .collect();
    let mut call = Samples::default();
    let mut last = vec![0u64; trios];
    for handle in handles {
        if let Ok((samples, revisions)) = handle.join() {
            call.0.extend(samples.0);
            for (trio, revision) in revisions.iter().enumerate() {
                last[trio] = last[trio].max(*revision);
            }
        }
    }
    settle(&bench);
    let elapsed = now_ns() - started;

    let captured = bench.sink.captured.lock();
    let (mut partial_rows, mut partial_msgs, mut rows) = (0u64, 0u64, 0u64);
    let mut seen: HashMap<Arc<str>, u64> = HashMap::new();
    for message in captured.iter() {
        let mut partial = false;
        for item in message {
            let any: &dyn std::any::Any = &**item;
            let Some(trio) = any.downcast_ref::<Trio>() else {
                continue;
            };
            rows += 1;
            if trio.target_revision != trio.emitter_revision
                || trio.emitter_revision != trio.gauge_revision
            {
                partial_rows += 1;
                partial = true;
            }
            seen.insert(
                trio.id.clone(),
                trio.gauge_revision
                    .min(trio.target_revision)
                    .min(trio.emitter_revision),
            );
        }
        partial_msgs += u64::from(partial);
    }
    let converged = (0..trios).all(|trio| {
        last[trio] == 0 || seen.get(&*format!("g-{trio:06}")).copied() == Some(last[trio])
    });
    let mut record = Record::new("atomic");
    record
        .param("group", group)
        .param("path", args.str("path", "typed"))
        .param("trios", trios)
        .metric("messages", captured.len())
        .metric("rows", rows)
        .metric("partial_rows", partial_rows)
        .metric("partial_messages", partial_msgs)
        .metric(
            "mutation_us",
            us(elapsed as f64 / (writers * mutations) as f64),
        );
    call.report_us("mutation_call", &mut record.metrics);
    record
        .check("settled", settled)
        .check("initial_rows_exact", initial_ok)
        .check("no_partial_mutation_visible", partial_msgs == 0)
        .check("converged_to_last_write", converged);
    drop(captured);
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── M8: identical subscriptions share state ────────

pub fn shared_params(args: &Args) {
    let rows = args.count("rows", 100_000);
    let ids = args.count("ids", 50_000).min(rows);
    let bench = Bench::new(args.wire(), 64, 4);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    load_scenes(&bench, 240, 12);
    load_nodes(&bench, &maker, rows, 1_000);
    settle(&bench);
    let (heap0, cache0) = (
        heap_bytes(),
        adapter::query_cache_len(&bench.ctx).unwrap_or(0),
    );
    let mut clients: Vec<Client> = vec![bench.client("client-0")];
    clients[0].sub_query(&in_query(0..ids));
    let settled = settle(&bench);
    let (heap1, cache1) = (
        heap_bytes(),
        adapter::query_cache_len(&bench.ctx).unwrap_or(0),
    );
    for c in 1..12 {
        let mut client = bench.client(&format!("client-{c}"));
        client.sub_query(&in_query(0..ids));
        clients.push(client);
    }
    settle(&bench);
    let (heap12, cache12) = (
        heap_bytes(),
        adapter::query_cache_len(&bench.ctx).unwrap_or(0),
    );
    let one = heap1.saturating_sub(heap0).max(1) as f64;
    let twelve = heap12.saturating_sub(heap0) as f64;
    let mut record = Record::new("shared_params");
    record
        .param("rows", rows)
        .param("ids", ids)
        .metric("state_one_kb", (one / 1024.0).round())
        .metric("state_twelve_kb", (twelve / 1024.0).round())
        .metric("state_ratio", (twelve / one * 100.0).round() / 100.0)
        .metric("query_cache_entries_one", cache1 - cache0)
        .metric("query_cache_entries_twelve", cache12 - cache0)
        .check("settled", settled)
        .check(
            "initial_rows_exact",
            bench.sink.total_rows_cb() == 12 * ids as u64,
        );
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── S9: setup burst from an empty store ────────────

pub fn setup(args: &Args) {
    let rows = args.count("rows", 200_000);
    let batch = args.usize("batch", 1_000);
    let with_subs = args.usize("subs", 12);
    let ids_each = args.count("ids_each", 5_400).min(rows / with_subs.max(1));
    let bench = Bench::new(args.wire(), 64, 8);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    load_scenes(&bench, 240, 12);
    // Subscriptions exist before the rows do, as in a client-driven setup.
    let mut clients: Vec<Client> = (0..with_subs)
        .map(|c| bench.client(&format!("client-{c}")))
        .collect();
    for (c, client) in clients.iter_mut().enumerate() {
        client.sub_query(&in_query(c * ids_each..(c + 1) * ids_each));
    }
    settle(&bench);
    let rows_before = bench.sink.total_rows_cb();
    let heap0 = heap_bytes();
    let started = now_ns();
    let call_s = if batch == 1 {
        for index in 0..rows {
            let _ = bench.ctx.set(maker.node(index, NONE, 0, 0));
        }
        (now_ns() - started) as f64 / 1e9
    } else {
        load_nodes(&bench, &maker, rows, batch)
    };
    let expected = (with_subs * ids_each) as u64;
    let deadline = now_ns() + 120_000_000_000;
    while bench.sink.total_rows_cb() - rows_before < expected && now_ns() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    while bench.sink.pending.load(Relaxed) > 0 && now_ns() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let total_s = (now_ns() - started) as f64 / 1e9;
    settle(&bench);
    let stored = store_len(&bench, maker.node(0, NONE, 0, 0).entity_type());
    let mut record = Record::new("setup");
    record
        .param("rows", rows)
        .param("batch", batch)
        .param("subs", with_subs)
        .metric("load_call_s", (call_s * 1000.0).round() / 1000.0)
        .metric("load_total_s", (total_s * 1000.0).round() / 1000.0)
        .metric("us_per_row", us(total_s * 1e9 / rows as f64))
        .metric("heap_mb", (heap_bytes() - heap0) / 1_048_576)
        .metric("rss_mb", rss_bytes() / 1_048_576)
        .check("store_rows_exact", stored == rows)
        .check(
            "rows_delivered_exact",
            bench.sink.total_rows_cb() - rows_before == expected,
        );
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── diag: where a write's cost goes ───────────────

/// Cost of one changed row, built up one component at a time: a store with
/// no relationship, a store with `belongs_to`, one subscription, twelve, and
/// serialization. Not a gate. It attributes the per-row cost that M1 fails on.
pub fn write_cost(args: &Args) {
    let rows = args.count("rows", 50_000);
    let batch = args.count("batch", 5_000).min(rows);
    let rounds = args.usize("rounds", 30);
    let subs = args.usize("subs", 0);
    let entity = args.str("entity", "node");
    let single = args.usize("single", 0) == 1;
    let bench = Bench::new(args.wire(), 64, 8);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    let data = payload::blob(args.usize("payload", 512), 0);
    load_scenes(&bench, 240, 12);
    let build = |index: usize, seq: u64| -> Arc<dyn AnyItem> {
        if entity == "part" {
            Arc::new(BPart {
                id: payload::part_id(index),
                name: Arc::from("part"),
                revision: seq,
                data: Blob(data.0.clone()),
            })
        } else {
            maker.item(index, NONE, seq, PROBE_WRITER)
        }
    };
    for chunk in (0..rows).collect::<Vec<_>>().chunks(1_000) {
        let items: Vec<Arc<dyn AnyItem>> = chunk.iter().map(|index| build(*index, 0)).collect();
        bench
            .ctx
            .set_batch_any(items)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    let mut clients: Vec<Client> = Vec::new();
    for c in 0..subs {
        let mut client = bench.client(&format!("client-{c}"));
        if entity == "part" {
            client.sub_query(&GetBPartsByIds {
                ids: (0..batch).map(payload::part_id).collect(),
            });
        } else {
            client.sub_query(&in_query(0..batch));
        }
        clients.push(client);
    }
    settle(&bench);
    let mut call = Samples::default();
    let mut done = Samples::default();
    for round in 0..rounds {
        let seq = round as u64 + 1;
        let items: Vec<Arc<dyn AnyItem>> = (0..batch).map(|index| build(index, seq)).collect();
        let rows_before = bench.sink.rows_ser.load(Relaxed);
        let t0 = now_ns();
        if single {
            for item in items {
                let _ = bench.ctx.set_batch_any(vec![item]);
            }
        } else {
            let _ = bench.ctx.set_batch_any(items);
        }
        call.push(now_ns() - t0);
        let expected = rows_before + (subs * batch) as u64;
        let deadline = now_ns() + 20_000_000_000;
        while bench.sink.rows_ser.load(Relaxed) < expected && now_ns() < deadline {
            std::hint::spin_loop();
        }
        done.push(now_ns() - t0);
        settle(&bench);
    }
    let per_row = |samples: &Samples| us(samples.quantile(0.5) / batch as f64 * 1000.0) / 1000.0;
    let mut record = Record::new("write_cost");
    record
        .param("entity", entity)
        .param("subs", subs)
        .param("single", single)
        .param("wire", args.str("wire", "json"))
        .param("batch", batch)
        .param("payload", args.usize("payload", 512))
        .metric("call_us_per_row", per_row(&call))
        .metric("done_us_per_row", per_row(&done))
        .check(
            "rows_delivered_exact",
            bench.sink.rows_ser.load(Relaxed) >= (subs * batch * rounds) as u64,
        );
    record.sink_checks(&bench.sink);
    record.emit();
}

// ───────────────────────── dedup: work caused by values that did not change

/// Counts the downstream work of a write that changes nothing a reader
/// uses. Phase `identical` rewrites every row with the value it already
/// has. Phase `irrelevant` changes fields the label reports never read.
pub fn dedup(args: &Args) {
    let rows = args.count("rows", 20_000);
    let watched = args.count("watched", 2_000).min(rows);
    let rounds = args.usize("rounds", 20);
    let bench = Bench::new(args.wire(), watched * 6 + 64, 8);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), 240, 64));
    load_scenes(&bench, 240, 12);
    load_nodes(&bench, &maker, rows, 1_000);
    let kinds = [
        "by_ids",
        "node_value",
        "label",
        "label_deduped",
        "label_upper",
        "view",
    ];
    let mut clients: Vec<Client> = Vec::new();
    let mut slots: Vec<Vec<usize>> = Vec::new();
    for kind in kinds {
        let mut client = bench.client(kind);
        let mut mine = Vec::new();
        if kind == "view" {
            // Every project, so the view holds every watched row.
            for project in 0..12 {
                mine.push(
                    client
                        .sub_view(&GetNodesWithScene {
                            project_id: Arc::from(format!("project-{project}")),
                        })
                        .slot,
                );
            }
        } else {
            for row in 0..watched {
                let node_id = payload::node_id(row);
                let sub = match kind {
                    "by_ids" => client.sub_query(&GetBNodesByIds { ids: vec![node_id] }),
                    "node_value" => client.sub_report(&NodeValue { node_id }),
                    "label" => client.sub_report(&NodeLabel { node_id }),
                    "label_deduped" => client.sub_report(&NodeLabelDeduped { node_id }),
                    _ => client.sub_report(&LabelUpper { node_id }),
                };
                mine.push(sub.slot);
            }
        }
        clients.push(client);
        slots.push(mine);
    }
    let settled = settle(&bench);
    let delivered = |bench: &Bench, kind: usize| -> u64 {
        slots[kind]
            .iter()
            .map(|slot| bench.sink.slots[*slot].rows.load(Relaxed))
            .sum()
    };
    let computes = || {
        (
            LABEL_COMPUTES.load(Relaxed),
            LABEL_DEDUPED_COMPUTES.load(Relaxed),
            LABEL_UPPER_COMPUTES.load(Relaxed),
        )
    };

    for phase in ["identical", "irrelevant"] {
        let before: Vec<u64> = (0..kinds.len())
            .map(|kind| delivered(&bench, kind))
            .collect();
        let (computes0, bytes0) = (computes(), bench.sink.bytes_ser.load(Relaxed));
        let (eqs0, clones0) = (blob::EQS.load(Relaxed), blob::CLONES.load(Relaxed));
        blob::COUNTING.store(true, Relaxed);
        let mut call = Samples::default();
        let mut done = Samples::default();
        for round in 0..rounds {
            // `identical` writes the loaded value again. `irrelevant` moves
            // seq, x and y, which no label report reads.
            let seq = if phase == "identical" {
                0
            } else {
                round as u64 + 1
            };
            let items: Vec<Arc<dyn AnyItem>> = (0..watched)
                .map(|row| maker.item(row, NONE, seq, 0))
                .collect();
            let t0 = now_ns();
            let _ = bench.ctx.set_batch_any(items);
            call.push(now_ns() - t0);
            settle(&bench);
            done.push(now_ns().saturating_sub(t0 + QUIET.as_nanos() as u64));
        }
        blob::COUNTING.store(false, Relaxed);
        let written = (rounds * watched) as f64;
        let per = |count: u64| ((count as f64 / written) * 1000.0).round() / 1000.0;
        let computes1 = computes();
        let mut record = Record::new("dedup");
        record
            .param("phase", phase)
            .param("rows_per_write", watched)
            .metric(
                "call_us_per_row",
                us(call.quantile(0.5) / watched as f64 * 1000.0) / 1000.0,
            )
            .metric(
                "settled_us_per_row",
                us(done.quantile(0.5) / watched as f64 * 1000.0) / 1000.0,
            )
            .metric(
                "bytes_per_written_row",
                per(bench.sink.bytes_ser.load(Relaxed) - bytes0),
            )
            .metric("payload_eqs_per_row", per(blob::EQS.load(Relaxed) - eqs0))
            .metric(
                "payload_clones_per_row",
                per(blob::CLONES.load(Relaxed) - clones0),
            )
            .metric("label_computes_per_row", per(computes1.0 - computes0.0))
            .metric(
                "label_deduped_computes_per_row",
                per(computes1.1 - computes0.1),
            )
            .metric(
                "label_upper_computes_per_row",
                per(computes1.2 - computes0.2),
            );
        for (kind, name) in kinds.iter().enumerate() {
            record.metric(
                &format!("deliveries_per_row_{name}"),
                per(delivered(&bench, kind) - before[kind]),
            );
        }
        record.check("settled", settled);
        record.sink_checks(&bench.sink);
        record.emit();
    }
}

// ───────────────────────── one-shot reads ─────────────────────────────────

/// `query_snapshot`, the read a command handler makes: by one id, by 100
/// ids, by a `belongs_to` key and by a field with no index. Each read
/// follows a write, and must see it.
pub fn snapshot(args: &Args) {
    let rows = args.count("rows", 50_000);
    let scenes = 240;
    let calls = args.usize("calls", 300);
    let kind = args.str("kind", "id");
    let bench = Bench::new(args.wire(), 64, 2);
    let maker = Arc::new(NodeMaker::new(args.usize("payload", 512), scenes, 64));
    load_scenes(&bench, scenes, 12);
    load_nodes(&bench, &maker, rows, 1_000);
    let request = bench.ctx.new_server_transaction();
    let mut latency = Samples::default();
    let mut wrong = 0u64;
    let mut stale = 0u64;
    for call in 0..calls {
        let row = (call * 7919) % rows;
        let seq = call as u64 + 1;
        bench
            .ctx
            .set_dyn(maker.item(row, NONE, seq, PROBE_WRITER))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let t0 = now_ns();
        let found = match kind.as_str() {
            "ids" => bench.ctx.query_snapshot(
                GetBNodesByIds {
                    ids: (0..100)
                        .map(|k| payload::node_id((row + k) % rows))
                        .collect(),
                },
                request.clone(),
            ),
            "fk" => bench.ctx.query_snapshot(
                GetBNodesByQuery(BNodeQuery {
                    scene_id: Some(IdFilter::Eq(payload::scene_id(row % scenes))),
                    ..Default::default()
                }),
                request.clone(),
            ),
            "scan" => bench.ctx.query_snapshot(
                GetBNodesByQuery(BNodeQuery {
                    seq: Some(seq.into()),
                    ..Default::default()
                }),
                request.clone(),
            ),
            _ => bench
                .ctx
                .query_snapshot(in_query(row..=row), request.clone()),
        };
        latency.push(now_ns() - t0);
        let expected = match kind.as_str() {
            "ids" => 100,
            "fk" => (rows - row % scenes).div_ceil(scenes),
            _ => 1,
        };
        wrong += u64::from(found.len() != expected);
        stale += u64::from(
            !found
                .iter()
                .any(|node| node.id == payload::node_id(row) && node.seq == seq),
        );
    }
    let mut record = Record::new("snapshot");
    record.param("kind", kind).param("rows", rows);
    latency.report_us("latency", &mut record.metrics);
    record
        .metric("wrong_results", wrong)
        .metric("stale_reads", stale)
        .check("results_exact", wrong == 0)
        .check("read_sees_the_write_before_it", stale == 0);
    record.emit();
}
