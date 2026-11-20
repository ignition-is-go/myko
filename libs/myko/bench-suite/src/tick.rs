//! The 30 Hz tick writer, the independent probe and the subscription churn
//! that run beside it.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    thread::JoinHandle,
    time::Duration,
};

use myko::core::item::AnyItem;

use super::{
    entities::*,
    harness::{Bench, NONE, Samples, cpu_seconds, now_ns, sleep_until},
    payload::{self, NodeMaker},
};

pub const TICK_WRITER: u32 = 1;
pub const PROBE_WRITER: u32 = 7;

pub struct TickPlan {
    /// Ordinals of the rows every tick rewrites.
    pub rows: Arc<Vec<usize>>,
    pub maker: Arc<NodeMaker>,
    pub hz: f64,
    pub warmup: usize,
    pub ticks: usize,
    /// Rows a tick must deliver, summed over all subscriptions.
    pub expect_per_tick: u64,
    /// Open loop at `hz` when true. Closed loop (each tick waits for the last
    /// to be delivered) when false, used by the accounting runs.
    pub paced: bool,
    /// Stop issuing ticks once the run has taken this long.
    pub budget: Duration,
}

#[derive(Default)]
pub struct TickOutcome {
    /// Scheduled start to last callback.
    pub completion_cb: Samples,
    /// Scheduled start to last serialized byte. This is the L2-1 quantity.
    pub completion_ser: Samples,
    /// Actual start to last serialized byte: service time without queueing.
    pub service_ser: Samples,
    pub write_call: Samples,
    pub lateness: Samples,
    /// Lateness caused by the harness's own row builders, not by the server.
    /// A run where this is not near zero under-measures nothing but must be
    /// read with care: the writer was starved of input.
    pub builder_late: Samples,
    pub issued: usize,
    pub incomplete: u64,
    pub delivered: u64,
    pub expected: u64,
    pub cores: f64,
    pub wall_s: f64,
    pub aborted: bool,
}

pub fn run_ticks(bench: &Bench, plan: &TickPlan) -> TickOutcome {
    const BUILDERS: usize = 6;
    let total = plan.warmup + plan.ticks;
    let stop = Arc::new(AtomicBool::new(false));
    let mut lanes = Vec::new();
    for builder in 0..BUILDERS {
        let (tx, rx) = flume::bounded::<Vec<Arc<dyn AnyItem>>>(2);
        let (rows, maker, stop) = (plan.rows.clone(), plan.maker.clone(), stop.clone());
        std::thread::spawn(move || {
            for tick in (builder..total).step_by(BUILDERS) {
                if stop.load(Relaxed) {
                    return;
                }
                let batch = rows
                    .iter()
                    .map(|row| maker.item(*row, tick as u64, tick as u64 + 1, TICK_WRITER))
                    .collect();
                if tx.send(batch).is_err() {
                    return;
                }
            }
        });
        lanes.push(rx);
    }

    let period = (1e9 / plan.hz) as u64;
    let sink = &bench.sink;
    let mut scheduled = vec![0u64; total];
    let mut actual = vec![0u64; total];
    let mut returned = vec![0u64; total];
    let mut starved = vec![0u64; total];
    let wait_tick = |tick: usize, timeout: Duration| {
        let deadline = now_ns() + timeout.as_nanos() as u64;
        while sink.tick_rows[tick].load(Relaxed) < plan.expect_per_tick {
            if now_ns() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        true
    };

    let start = now_ns() + 50_000_000;
    let mut outcome = TickOutcome::default();
    let (mut cpu0, mut wall0) = (cpu_seconds(), now_ns());
    for tick in 0..total {
        let Ok(batch) = lanes[tick % BUILDERS].recv() else {
            break;
        };
        if tick == plan.warmup {
            (cpu0, wall0) = (cpu_seconds(), now_ns());
        }
        scheduled[tick] = start + tick as u64 * period;
        let ready = now_ns();
        let free = if tick == 0 { 0 } else { returned[tick - 1] };
        if plan.paced {
            starved[tick] = ready.saturating_sub(scheduled[tick].max(free));
        }
        if plan.paced {
            sleep_until(scheduled[tick]);
        }
        actual[tick] = now_ns();
        if !plan.paced {
            scheduled[tick] = actual[tick];
        }
        if let Err(error) = bench.ctx.set_batch_any(batch) {
            panic!("tick write failed: {error:?}");
        }
        returned[tick] = now_ns();
        outcome.issued = tick + 1;
        if !plan.paced {
            wait_tick(tick, Duration::from_secs(10));
        }
        if now_ns().saturating_sub(start) > plan.budget.as_nanos() as u64 {
            outcome.aborted = tick + 1 < total;
            break;
        }
    }
    stop.store(true, Relaxed);
    drop(lanes);
    if outcome.issued > 0 {
        wait_tick(outcome.issued - 1, Duration::from_secs(20));
    }
    outcome.wall_s = (now_ns() - wall0) as f64 / 1e9;
    outcome.cores = (cpu_seconds() - cpu0) / outcome.wall_s;

    for tick in plan.warmup..outcome.issued {
        let rows = sink.tick_rows[tick].load(Relaxed);
        outcome.delivered += rows;
        outcome.expected += plan.expect_per_tick;
        outcome
            .write_call
            .push(returned[tick].saturating_sub(actual[tick]));
        outcome
            .lateness
            .push(actual[tick].saturating_sub(scheduled[tick]));
        outcome.builder_late.push(starved[tick]);
        if rows != plan.expect_per_tick {
            outcome.incomplete += 1;
            continue;
        }
        let cb = sink.tick_cb_done[tick].load(Relaxed);
        let ser = sink.tick_ser_done[tick].load(Relaxed);
        outcome
            .completion_cb
            .push(cb.saturating_sub(scheduled[tick]));
        outcome
            .completion_ser
            .push(ser.saturating_sub(scheduled[tick]));
        outcome.service_ser.push(ser.saturating_sub(actual[tick]));
    }
    outcome
}

#[derive(Default)]
pub struct ProbeOutcome {
    /// Write to own-subscription callback, for a row in its own store.
    pub other: Samples,
    pub other_call: Samples,
    /// The same, for a row in the store the tick writes.
    pub same: Samples,
    pub same_call: Samples,
    /// Writes whose call returned before the subscription update arrived. A
    /// command response sent on return would overtake the update.
    pub early_returns: u64,
    pub lost: u64,
    pub wrong_value: u64,
}

/// Starts the independent probe: at `hz` it writes one row and waits for the
/// update on its own subscription. `same_row` must not be a tick row.
pub fn spawn_probe(
    bench: &Bench,
    maker: Arc<NodeMaker>,
    same_row: usize,
    hz: f64,
    stop: Arc<AtomicBool>,
) -> JoinHandle<ProbeOutcome> {
    let bench = bench.clone();
    std::thread::spawn(move || {
        let mut client = bench.client("probe");
        bench
            .ctx
            .set(&payload::probe(0))
            .unwrap_or_else(|error| panic!("{error:?}"));
        let other = client.sub_query(&GetBProbesByIds {
            ids: vec![BProbeId(Arc::from("probe-0"))],
        });
        let same = client.sub_query(&GetBNodesByIds {
            ids: vec![payload::node_id(same_row)],
        });
        std::thread::sleep(Duration::from_millis(100));
        let mut outcome = ProbeOutcome::default();
        let period = (1e9 / hz) as u64;
        let start = now_ns();
        let mut index = 0u64;
        let timeout = Duration::from_secs(10);
        while !stop.load(Relaxed) {
            index += 1;
            sleep_until(start + index * period);
            let sink = &bench.sink;

            let before = sink.slots[other.slot].msgs.load(Relaxed);
            let t0 = now_ns();
            let _ = bench.ctx.set(&payload::probe(index));
            let t1 = now_ns();
            match sink.wait_slot(other.slot, before, timeout) {
                Some(cb) => {
                    outcome.other.push(cb.saturating_sub(t0));
                    outcome.other_call.push(t1 - t0);
                    outcome.early_returns += u64::from(cb > t1);
                    outcome.wrong_value +=
                        u64::from(sink.slots[other.slot].last_val.load(Relaxed) != index);
                }
                None => outcome.lost += 1,
            }

            let before = sink.slots[same.slot].msgs.load(Relaxed);
            let t0 = now_ns();
            let _ = bench
                .ctx
                .set(&maker.node(same_row, NONE, index, PROBE_WRITER));
            let t1 = now_ns();
            match sink.wait_slot(same.slot, before, timeout) {
                Some(cb) => {
                    outcome.same.push(cb.saturating_sub(t0));
                    outcome.same_call.push(t1 - t0);
                    outcome.early_returns += u64::from(cb > t1);
                    outcome.wrong_value +=
                        u64::from(sink.slots[same.slot].last_val.load(Relaxed) != index);
                }
                None => outcome.lost += 1,
            }
        }
        drop(client);
        outcome
    })
}

#[derive(Default)]
pub struct ChurnOutcome {
    pub subscribes: u64,
    pub cancels: u64,
    pub subscribe_call: Samples,
    pub first_delivery: Samples,
    pub cancel_call: Samples,
    pub lost: u64,
}

/// Subscribes and cancels fresh two-id queries at `per_second` each, holding
/// `live` of them at a time. `rows` must hold no row the tick writes, so the
/// churned subscriptions never add to a tick's deliveries.
pub fn spawn_churn(
    bench: &Bench,
    rows: std::ops::Range<usize>,
    per_second: f64,
    live: usize,
    stop: Arc<AtomicBool>,
) -> JoinHandle<ChurnOutcome> {
    let bench = bench.clone();
    std::thread::spawn(move || {
        let mut client = bench.client("churn");
        let mut held = VecDeque::new();
        let mut outcome = ChurnOutcome::default();
        let period = (1e9 / per_second) as u64;
        let start = now_ns();
        let mut index = 0u64;
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        while !stop.load(Relaxed) {
            index += 1;
            sleep_until(start + index * period);
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let span = rows.len();
            let first = rows.start + (state >> 33) as usize % span;
            let second = rows.start + (first - rows.start + 1 + index as usize % 97) % span;
            let t0 = now_ns();
            let sub = client.sub_query(&GetBNodesByIds {
                ids: vec![payload::node_id(first), payload::node_id(second)],
            });
            let t1 = now_ns();
            outcome.subscribe_call.push(t1 - t0);
            match bench.sink.wait_slot(sub.slot, 0, Duration::from_secs(10)) {
                Some(cb) => outcome.first_delivery.push(cb.saturating_sub(t0)),
                None => outcome.lost += 1,
            }
            outcome.subscribes += 1;
            held.push_back(sub);
            if held.len() > live {
                if let Some(sub) = held.pop_front() {
                    let t0 = now_ns();
                    client.cancel(&sub);
                    outcome.cancel_call.push(now_ns() - t0);
                    outcome.cancels += 1;
                }
            }
        }
        drop(client);
        outcome
    })
}
