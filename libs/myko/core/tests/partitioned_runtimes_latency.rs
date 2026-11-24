//! Latency of one write, from the call to the report output reaching a
//! subscribed client, for a server without partitions and inside one
//! partition while another partition is idle or parked inside a wave.
//!
//! Three paths: a keyed write, a routed write (its partition resolved from
//! shared data), and a keyed write recorded by an in-memory persister.
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
    prelude::{Eventable, Querying},
    report::{AnyOutput, ReportContext, ReportHandler},
    server::{
        ClientSession, HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey,
        PartitionSchema, PersistError, RelationshipManager, WsWriter,
        persister::{Persister, PersisterRouter},
    },
    store::StoreRegistry,
    wire::{EncodedCommandMessage, MEvent, MykoMessage},
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

pub mod assignment {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared: the runtime a sample's emitter feeds. Its id is the emitter's.
    #[myko_item]
    pub struct LpAssignment {
        pub runtime: Arc<str>,
    }
}

pub mod sample {
    use myko::prelude::*;

    /// Routed by its emitter's assignment when the server declares
    /// partitions.
    #[myko_item]
    pub struct LpSample {
        pub level: i64,
    }
}

use assignment::LpAssignment;
use reading::{GetAllLpReadings, LpReading};
use sample::{GetAllLpSamples, LpSample};
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

/// The samples a context sees.
#[myko_report(i64)]
pub struct LpSampleTotal {}

impl ReportHandler for LpSampleTotal {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        ctx.query_map(GetAllLpSamples {})
            .entries()
            .map(|rows| Arc::new(rows.iter().map(|(_, row)| row.level).sum::<i64>()))
            .materialize()
    }
}

/// Keeps every event in memory, as a test persister does.
#[derive(Clone, Default)]
struct Memory(Arc<Mutex<Vec<MEvent>>>);

impl Persister for Memory {
    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        self.0.lock().push(event);
        Ok(())
    }
}

#[derive(Clone)]
struct Socket(Arc<Mutex<mpsc::Sender<i64>>>);

/// A client subscribed to the report `$report` of `$ctx`.
macro_rules! connect_to {
    ($ctx:expr, $report:expr) => {{
        let ctx: &MykoServerContext = $ctx;
        let (sender, received) = mpsc::channel();
        let mut session = ClientSession::new(
            Arc::from(Uuid::new_v4().to_string()),
            Socket(Arc::new(Mutex::new(sender))),
        );
        let request = ctx.new_server_transaction();
        let cell = ctx.in_domain(|| {
            ctx.report($report, request)
                .map(|output| -> Arc<dyn AnyOutput> { output.clone() })
                .materialize()
        });
        session.subscribe_report("total".into(), "report".into(), cell);
        (session, received)
    }};
}

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
    connect_to!(ctx, LpTotal {})
}

fn root_context(persisters: PersisterRouter) -> MykoServerContext {
    let root = MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(persisters),
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
    let runtime: Arc<str> = runtime.into();
    measure_with(received, |level| {
        root.set(&LpReading {
            id: "probe".into(),
            runtime: runtime.clone(),
            level,
        })
        .expect("keyed write");
    })
}

/// Write the probe sample through `root`, routed by its emitter's
/// assignment where the server declares partitions.
fn measure_routed(root: &MykoServerContext, received: &mpsc::Receiver<i64>) -> Vec<u64> {
    measure_with(received, |level| {
        root.set(&LpSample {
            id: "probe".into(),
            level,
        })
        .expect("routed write");
    })
}

/// Make `WRITES` writes, each of `level` 1, 2, ..., and wait for each one's
/// output on `received`. Returns the sorted per-write latencies.
fn measure_with(received: &mpsc::Receiver<i64>, write: impl Fn(i64)) -> Vec<u64> {
    for _stale in received.try_iter() {}
    let mut latencies = Vec::with_capacity(WRITES);
    for write_index in 1..=WRITES {
        let level = i64::try_from(write_index).expect("level");
        let begun = Instant::now();
        write(level);
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

/// One server without partitions and one with runtimes `a` and `b`, each
/// with a client of both reports.
struct Setup {
    plain: MykoServerContext,
    plain_readings: mpsc::Receiver<i64>,
    plain_samples: mpsc::Receiver<i64>,
    root: MykoServerContext,
    a: MykoServerContext,
    b_readings: mpsc::Receiver<i64>,
    b_samples: mpsc::Receiver<i64>,
    _sessions: Vec<ClientSession<Socket>>,
    _received: Vec<mpsc::Receiver<i64>>,
}

fn router(persister: Option<&Memory>) -> PersisterRouter {
    let mut persisters = PersisterRouter::default();
    if let Some(persister) = persister {
        persisters.set_default(Some(Arc::new(persister.clone())));
    }
    persisters
}

fn assign(root: &MykoServerContext, emitter: &str, runtime: &str) {
    root.set(&LpAssignment {
        id: emitter.into(),
        runtime: runtime.into(),
    })
    .expect("assignment");
}

fn setup(persister: Option<&Memory>) -> Setup {
    let plain = root_context(router(persister));
    let (plain_reading_session, plain_readings) = connect(&plain);
    let (plain_sample_session, plain_samples) = connect_to!(&plain, LpSampleTotal {});

    let root = root_context(router(persister)).with_partitions(
        PartitionSchema::new()
            .partition_by::<LpReading>(|reading| PartitionKey::new(reading.runtime.clone()))
            .route::<LpSample>(&[LpAssignment::ENTITY_NAME_STATIC], |root, sample| {
                let assignment = root
                    .registry
                    .get(LpAssignment::ENTITY_NAME_STATIC)?
                    .get_value(&sample.id.0)?;
                let assignment = myko::prelude::AnyItem::as_any(assignment.as_ref())
                    .downcast_ref::<LpAssignment>()?;
                Some(PartitionKey::new(assignment.runtime.clone()))
            }),
    );
    let a = root.open_partition(PartitionKey::new("a")).expect("a");
    let b = root.open_partition(PartitionKey::new("b")).expect("b");
    assign(&root, "probe", "b");
    let (a_session, a_received) = connect(&a);
    let (b_reading_session, b_readings) = connect(&b);
    let (b_sample_session, b_samples) = connect_to!(&b, LpSampleTotal {});
    root.set(&busy(1)).expect("first write to a");
    Setup {
        plain,
        plain_readings,
        plain_samples,
        root,
        a,
        b_readings,
        b_samples,
        _sessions: vec![
            plain_reading_session,
            plain_sample_session,
            a_session,
            b_reading_session,
            b_sample_session,
        ],
        _received: vec![a_received],
    }
}

fn busy(level: i64) -> LpReading {
    LpReading {
        id: "busy".into(),
        runtime: "a".into(),
        level,
    }
}

/// Run `probe` while runtime `a` is parked inside its report's wave.
fn while_a_parked(setup: &Setup, level: i64, probe: impl FnOnce() -> Vec<u64>) -> Vec<u64> {
    let gate = Arc::new(Parked::default());
    *PARK.lock() = Some((setup.a.domain(), Arc::clone(&gate)));
    let writer = {
        let root = setup.root.clone();
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
    let latencies = probe();
    assert!(!gate.state.lock().1, "runtime a was not parked throughout");
    *PARK.lock() = None;
    gate.state.lock().1 = true;
    gate.changed.notify_all();
    assert!(writer.join().expect("writer").is_ok());
    setup.a.domain().flush();
    latencies
}

#[derive(Default)]
struct Series {
    plain: Vec<Vec<u64>>,
    idle: Vec<Vec<u64>>,
    parked: Vec<Vec<u64>>,
}

impl Series {
    fn report(&self, path: &str) {
        report(&format!("{path}, no partitions"), &self.plain);
        report(&format!("{path}, b with a idle"), &self.idle);
        report(&format!("{path}, b with a parked"), &self.parked);
    }
}

#[test]
#[ignore = "prints a latency table; run in release with --ignored --nocapture"]
fn write_to_delivery_latency() {
    let memory = Memory::default();
    let bare = setup(None);
    let logged = setup(Some(&memory));

    let mut keyed = Series::default();
    let mut routed = Series::default();
    let mut persisted = Series::default();
    for round in 0..ROUNDS {
        // Each parked series writes a new level to a, so a's report runs.
        let level = i64::try_from(round)
            .expect("round")
            .saturating_mul(3)
            .saturating_add(2);

        keyed
            .plain
            .push(measure(&bare.plain, "plain", &bare.plain_readings));
        keyed.idle.push(measure(&bare.root, "b", &bare.b_readings));
        keyed.parked.push(while_a_parked(&bare, level, || {
            measure(&bare.root, "b", &bare.b_readings)
        }));

        routed
            .plain
            .push(measure_routed(&bare.plain, &bare.plain_samples));
        routed
            .idle
            .push(measure_routed(&bare.root, &bare.b_samples));
        routed
            .parked
            .push(while_a_parked(&bare, level.saturating_add(1), || {
                measure_routed(&bare.root, &bare.b_samples)
            }));

        persisted
            .plain
            .push(measure(&logged.plain, "plain", &logged.plain_readings));
        persisted
            .idle
            .push(measure(&logged.root, "b", &logged.b_readings));
        persisted.parked.push(while_a_parked(&logged, level, || {
            measure(&logged.root, "b", &logged.b_readings)
        }));
        let recorded = std::mem::take(&mut *memory.0.lock()).len();
        assert!(
            recorded >= WRITES.saturating_mul(3),
            "every persisted probe was recorded: {recorded}"
        );
    }

    println!("{WRITES} writes per series, {ROUNDS} rounds, interleaved");
    keyed.report("keyed");
    routed.report("routed");
    persisted.report("keyed + persister");
}
