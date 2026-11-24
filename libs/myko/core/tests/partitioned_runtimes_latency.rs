//! Latency of one write, from the call to the report output reaching a
//! subscribed client, for a server without partitions and inside one
//! partition while another partition is idle or parked inside a wave.
//!
//! Not a pass/fail test: it prints a table. Run it in release, alone:
//!
//! ```text
//! cargo test -p myko --release --test partitioned_runtimes_latency -- --ignored --nocapture
//! ```

#![allow(clippy::expect_used, clippy::panic, clippy::print_stdout)]

use std::{
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;
use myko::{
    hyphae::{Definite, JoinExt, MapExt, Materialize, scheduler::Domain},
    prelude::Querying,
    report::{AnyOutput, ReportContext, ReportHandler},
    server::{
        ClientSession, HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey,
        PartitionSchema, RelationshipManager, WsWriter, persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{EncodedCommandMessage, MykoMessage},
};
use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

const WRITES: usize = 20_000;
const ROUNDS: usize = 7;
const DEADLINE: Duration = Duration::from_secs(20);

pub mod setting {
    use myko::prelude::*;

    /// Shared in every configuration.
    #[myko_item]
    pub struct LpSetting {
        pub gain: i64,
    }
}

pub mod reading {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by `runtime` when the server declares partitions.
    #[myko_item]
    pub struct LpReading {
        pub runtime: Arc<str>,
        pub level: i64,
    }
}

use reading::{GetAllLpReadings, LpReading};
use setting::{GetAllLpSettings, LpSetting};

/// Set while the busy runtime must park inside its report's wave.
static PARK: Mutex<Option<(Domain, Arc<Parked>)>> = Mutex::new(None);

#[derive(Default)]
struct Parked {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

fn park() {
    let here = Domain::current();
    let parked = PARK
        .lock()
        .as_ref()
        .filter(|(domain, _)| *domain == here)
        .map(|(_, parked)| Arc::clone(parked));
    if let Some(parked) = parked {
        let mut state = parked.state.lock();
        state.0 = true;
        parked.changed.notify_all();
        while !state.1 {
            if parked.changed.wait_for(&mut state, DEADLINE).timed_out() {
                break;
            }
        }
        drop(state);
    }
}

/// The readings a context sees, scaled by the shared setting.
#[myko_report(i64)]
pub struct LpTotal {}

impl ReportHandler for LpTotal {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        let gain = ctx
            .query_map(GetAllLpSettings {})
            .entries()
            .map(|rows| rows.iter().map(|(_, row)| row.gain).sum::<i64>())
            .materialize();
        let readings = ctx
            .query_map(GetAllLpReadings {})
            .entries()
            .map(|rows| rows.iter().map(|(_, row)| row.level).sum::<i64>())
            .materialize();
        gain.join(readings)
            .map(|(gain, readings)| {
                park();
                Arc::new(gain.saturating_mul(*readings))
            })
            .materialize()
    }
}

#[derive(Clone)]
struct Socket(Arc<Mutex<mpsc::Sender<i64>>>);

impl WsWriter for Socket {
    fn send(&self, _message: MykoMessage) {}

    fn send_serialized_command(
        &self,
        _tx: Arc<str>,
        _command_id: String,
        _payload: EncodedCommandMessage,
    ) {
    }

    fn send_report_response(&self, _tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        if let Some(total) = output.as_any().downcast_ref::<i64>() {
            // The receiver is gone once the probe has finished.
            let _closed = self.0.lock().send(*total).is_err();
        }
    }
}

/// A client subscribed to `LpTotal` of `ctx`.
fn connect(ctx: &MykoServerContext) -> (ClientSession<Socket>, mpsc::Receiver<i64>) {
    let (sender, received) = mpsc::channel();
    let mut session = ClientSession::new(
        Arc::from(Uuid::new_v4().to_string()),
        Socket(Arc::new(Mutex::new(sender))),
    );
    let request = ctx.new_server_transaction();
    let cell = ctx.in_domain(|| {
        ctx.report(LpTotal {}, request)
            .map(|output| -> Arc<dyn AnyOutput> { output.clone() })
            .materialize()
    });
    session.subscribe_report("total".into(), "LpTotal".into(), cell);
    (session, received)
}

fn root_context() -> MykoServerContext {
    let root = MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    );
    root.set(&LpSetting {
        id: "gain".into(),
        gain: 1,
    })
    .expect("setting");
    root
}

/// Write `WRITES` readings of `runtime` through `root`, each waiting for its
/// output on `received`, and return the sorted per-write latencies.
fn measure(root: &MykoServerContext, runtime: &str, received: &mpsc::Receiver<i64>) -> Vec<u64> {
    for _stale in received.try_iter() {}
    let mut latencies = Vec::with_capacity(WRITES);
    for write in 1..=WRITES {
        let level = i64::try_from(write).expect("level");
        let reading = LpReading {
            id: "probe".into(),
            runtime: runtime.into(),
            level,
        };
        let begun = Instant::now();
        root.set(&reading).expect("write");
        loop {
            let total = received
                .recv_timeout(DEADLINE)
                .expect("the write's output never reached the client");
            if total == level {
                break;
            }
        }
        latencies.push(u64::try_from(begun.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    latencies.sort_unstable();
    latencies
}

fn percentile(sorted: &[u64], per_mille: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_sub(1)
        .saturating_mul(per_mille)
        .checked_div(1000)
        .unwrap_or(0);
    sorted.get(index).copied().unwrap_or(0)
}

fn report(label: &str, rounds: &[Vec<u64>]) {
    let mut medians: Vec<u64> = rounds.iter().map(|round| percentile(round, 500)).collect();
    let mut tails: Vec<u64> = rounds.iter().map(|round| percentile(round, 990)).collect();
    medians.sort_unstable();
    tails.sort_unstable();
    println!(
        "{label:<34} p50 {:>6} ns (rounds {:>6}..{:<6})  p99 {:>7} ns (rounds {:>7}..{:<7})",
        percentile(&medians, 500),
        medians.first().copied().unwrap_or(0),
        medians.last().copied().unwrap_or(0),
        percentile(&tails, 500),
        tails.first().copied().unwrap_or(0),
        tails.last().copied().unwrap_or(0),
    );
}

#[test]
#[ignore = "prints a latency table; run in release with --ignored --nocapture"]
fn write_to_delivery_latency() {
    // No partitions: every type is shared and the server runs in root.
    let plain = root_context();
    let (_plain_session, plain_received) = connect(&plain);

    // Two partitions. Runtime b is measured; runtime a is idle or parked.
    let root = root_context().with_partitions(
        PartitionSchema::new()
            .partition_by::<LpReading>(|reading| PartitionKey::new(reading.runtime.clone())),
    );
    let a = root.open_partition(PartitionKey::new("a")).expect("a");
    let b = root.open_partition(PartitionKey::new("b")).expect("b");
    let (_a_session, _a_received) = connect(&a);
    let (_b_session, b_received) = connect(&b);
    let busy = |level: i64| LpReading {
        id: "busy".into(),
        runtime: "a".into(),
        level,
    };
    root.set(&busy(1)).expect("first write to a");

    let mut unpartitioned = Vec::new();
    let mut idle = Vec::new();
    let mut parked = Vec::new();
    for round in 0..ROUNDS {
        unpartitioned.push(measure(&plain, "plain", &plain_received));
        idle.push(measure(&root, "b", &b_received));

        let gate = Arc::new(Parked::default());
        *PARK.lock() = Some((a.domain(), Arc::clone(&gate)));
        let writer = {
            let root = root.clone();
            let level = i64::try_from(round).expect("round").saturating_add(2);
            thread::spawn(move || root.set(&busy(level)))
        };
        {
            let mut state = gate.state.lock();
            while !state.0 {
                assert!(
                    !gate.changed.wait_for(&mut state, DEADLINE).timed_out(),
                    "runtime a never parked"
                );
            }
        }
        parked.push(measure(&root, "b", &b_received));
        assert!(!gate.state.lock().1, "runtime a was not parked throughout");
        *PARK.lock() = None;
        gate.state.lock().1 = true;
        gate.changed.notify_all();
        assert!(writer.join().expect("writer").is_ok());
        a.domain().flush();
    }

    println!("{WRITES} writes per round, {ROUNDS} rounds, interleaved");
    report("no partitions", &unpartitioned);
    report("runtime b, runtime a idle", &idle);
    report("runtime b, runtime a parked", &parked);
}
