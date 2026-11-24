//! Partitioned runtimes: shared data in the root domain, one child domain per
//! partition, and nothing one partition does delays another.
//!
//! The tests are structural. A gate parks a closure inside a wave, a graph
//! build or a graph drop of one partition, and the assertions run while it is
//! parked, so "does not delay" means "completed while the other partition was
//! provably still busy". The only clock is a failure deadline on each wait.

#![allow(clippy::expect_used, clippy::panic)]

use std::{
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;
use myko::{
    hyphae::{
        Cell, Definite, Gettable, JoinExt, MapExt, Materialize, SwitchMapExt, scheduler::Domain,
    },
    prelude::{AnyItem, Eventable, Querying, Reporting, WithId},
    report::{AnyOutput, ReportContext, ReportHandler},
    request::RequestContext,
    server::{
        ClientSession, HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionError,
        PartitionKey, PartitionSchema, RelationshipManager, WsWriter, persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{EncodedCommandMessage, MykoMessage},
};
use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(10);

/// Root's tick is shared by the process, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

// One module per entity: the item macro brings its own names into scope.

pub mod runtime {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Setting a runtime opens the partition named by its id.
    #[myko_item]
    pub struct PrRuntime {
        pub label: Arc<str>,
    }
}

pub mod setting {
    use myko::prelude::*;

    /// Shared. Every runtime reads it.
    #[myko_item]
    pub struct PrSetting {
        pub gain: i64,
    }
}

pub mod assignment {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Names the runtime an emitter feeds; its id is the emitter's.
    #[myko_item]
    pub struct PrAssignment {
        pub runtime: Arc<str>,
    }
}

pub mod reading {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by the runtime each row names.
    #[myko_item]
    pub struct PrReading {
        #[myko_partition_key]
        pub runtime: Arc<str>,
        pub level: i64,
    }
}

pub mod sample {
    use myko::prelude::*;

    /// Partitioned, and keyed by its emitter: only the emitter's assignment
    /// says which runtime a sample belongs to.
    #[myko_item]
    pub struct PrSample {
        pub level: i64,
    }
}

use assignment::PrAssignment;
use reading::{GetAllPrReadings, PrReading};
use runtime::PrRuntime;
use sample::{GetAllPrSamples, PrSample};
use setting::{GetAllPrSettings, PrSetting};

// gates ----------------------------------------------------------------------

#[derive(Default)]
struct GateState {
    entered: bool,
    open: bool,
    gave_up: bool,
}

/// Parks whoever passes it until it is opened.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl Gate {
    fn pass(&self) {
        let mut state = self.state.lock();
        state.entered = true;
        self.changed.notify_all();
        while !state.open {
            if self.changed.wait_for(&mut state, DEADLINE).timed_out() {
                state.gave_up = true;
                break;
            }
        }
        drop(state);
    }

    fn wait_entered(&self) -> bool {
        let mut state = self.state.lock();
        while !state.entered {
            if self.changed.wait_for(&mut state, DEADLINE).timed_out() {
                return false;
            }
        }
        true
    }

    fn open(&self) {
        self.state.lock().open = true;
        self.changed.notify_all();
    }
}

/// Where a report's graph can be made to park.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Inside a propagation wave.
    Wave,
    /// While a wave builds a new inner graph.
    Build,
    /// While a wave drops the inner graph it replaced.
    Drop,
    /// While a report is computed for the first time.
    Compute,
}

static GATES: Mutex<Vec<(Domain, Stage, Arc<Gate>)>> = Mutex::new(Vec::new());

/// Park the calling thread if `stage` is armed for the domain it is in.
fn park(stage: Stage) {
    let here = Domain::current();
    let gate = GATES
        .lock()
        .iter()
        .find(|(domain, armed, _)| *domain == here && *armed == stage)
        .map(|(_, _, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.pass();
    }
}

/// A stage armed in one domain. Dropping it opens the gate and disarms the
/// stage, so a failed test cannot park the next one.
struct Armed(Arc<Gate>);

impl Armed {
    fn wait_entered(&self) -> bool {
        self.0.wait_entered()
    }

    /// Whether a thread is parked at the gate right now. Asserted after the
    /// work that must not wait for it, so that work provably overlapped it.
    fn is_parked(&self) -> bool {
        let state = self.0.state.lock();
        state.entered && !state.open && !state.gave_up
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        self.0.open();
        GATES
            .lock()
            .retain(|(_, _, armed)| !Arc::ptr_eq(armed, &self.0));
    }
}

fn arm(domain: &Domain, stage: Stage) -> Armed {
    let gate = Arc::new(Gate::default());
    GATES
        .lock()
        .push((domain.clone(), stage, Arc::clone(&gate)));
    Armed(gate)
}

// reports --------------------------------------------------------------------

/// A partition's readings and samples joined with the shared setting.
#[myko_report_output]
#[derive(Eq)]
pub struct PrTotals {
    pub gain: i64,
    pub readings: i64,
    pub samples: i64,
}

/// The shared setting alone: a report over shared data only.
#[myko_report(i64)]
pub struct PrGain {}

impl ReportHandler for PrGain {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        ctx.query_map(GetAllPrSettings {})
            .entries()
            .map(|rows| {
                park(Stage::Wave);
                Arc::new(rows.iter().map(|(_, row)| row.gain).sum::<i64>())
            })
            .materialize()
    }
}

#[myko_report(PrTotals)]
pub struct PrTotal {}

impl ReportHandler for PrTotal {
    type Output = PrTotals;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<PrTotals>, Definite> {
        let gain = ctx.report(PrGain {}).map(|gain| **gain).materialize();
        let readings = ctx
            .query_map(GetAllPrReadings {})
            .entries()
            .map(|rows| rows.iter().map(|(_, row)| row.level).sum::<i64>())
            .materialize();
        let samples = ctx
            .query_map(GetAllPrSamples {})
            .entries()
            .map(|rows| rows.iter().map(|(_, row)| row.level).sum::<i64>())
            .materialize();
        gain.join(readings)
            .materialize()
            .join(samples)
            .map(|((gain, readings), samples)| {
                park(Stage::Wave);
                Arc::new(PrTotals {
                    gain: *gain,
                    readings: *readings,
                    samples: *samples,
                })
            })
            .materialize()
    }
}

/// Parks its dropper when [`Stage::Drop`] is armed.
#[derive(Debug)]
struct SlowDrop;

impl Drop for SlowDrop {
    fn drop(&mut self) {
        park(Stage::Drop);
    }
}

/// The sum of a partition's readings, through an inner graph that every
/// change of the sum replaces.
#[myko_report(i64)]
pub struct PrRebuilt {}

impl ReportHandler for PrRebuilt {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        ctx.query_map(GetAllPrReadings {})
            .entries()
            .map(|rows| rows.iter().map(|(_, row)| row.level).sum::<i64>())
            .materialize()
            .switch_map(|sum| {
                park(Stage::Build);
                let held = Arc::new(SlowDrop);
                Cell::new(*sum)
                    .map(move |sum| {
                        let _held = &held;
                        Arc::new(*sum)
                    })
                    .materialize()
            })
            .materialize()
    }
}

/// A report whose first computation can be made to park.
#[myko_report(i64)]
pub struct PrScaled {
    pub factor: i64,
}

impl ReportHandler for PrScaled {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        park(Stage::Compute);
        let factor = self.factor;
        ctx.query_map(GetAllPrReadings {})
            .entries()
            .map(move |rows| {
                Arc::new(
                    rows.iter()
                        .map(|(_, row)| row.level.saturating_mul(factor))
                        .sum::<i64>(),
                )
            })
            .materialize()
    }
}

// a websocket client ---------------------------------------------------------

/// A report output sent to a client, with its subscription's transaction.
type Sent = (Arc<str>, Arc<dyn AnyOutput>);

/// Receives what a client's socket would be sent.
#[derive(Clone)]
struct Socket(Arc<Mutex<mpsc::Sender<Sent>>>);

impl WsWriter for Socket {
    fn send(&self, _message: MykoMessage) {}

    fn send_serialized_command(
        &self,
        _tx: Arc<str>,
        _command_id: String,
        _payload: EncodedCommandMessage,
    ) {
    }

    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        drop(self.0.lock().send((tx, output)));
    }
}

/// One client subscribed to reports of one context.
struct Client {
    _session: ClientSession<Socket>,
    received: mpsc::Receiver<Sent>,
    /// Outputs received for a subscription nobody has asked about yet.
    unread: Mutex<Vec<Sent>>,
}

impl Client {
    /// Subscribe to `PrTotal` (tx `total`) and `PrRebuilt` (tx `rebuilt`) the
    /// way the websocket layer does: the subscription's partition picks the
    /// context, and the report's cell is built in that context's domain.
    fn connect(ctx: &MykoServerContext) -> Self {
        let (sender, received) = mpsc::channel();
        let mut session = ClientSession::new(
            Arc::from(Uuid::new_v4().to_string()),
            Socket(Arc::new(Mutex::new(sender))),
        );
        let total = erased(ctx, PrTotal {});
        session.subscribe_report("total".into(), "PrTotal".into(), total);
        let rebuilt = erased(ctx, PrRebuilt {});
        session.subscribe_report("rebuilt".into(), "PrRebuilt".into(), rebuilt);
        Self {
            _session: session,
            received,
            unread: Mutex::default(),
        }
    }

    /// The next output of `tx`. Waits for it until `deadline` when one is
    /// given, and otherwise returns only what has already arrived.
    fn next(&self, tx: &str, deadline: Option<Instant>) -> Option<Arc<dyn AnyOutput>> {
        loop {
            let mut unread = self.unread.lock();
            let index = unread.iter().position(|(sent, _)| sent.as_ref() == tx);
            let found = index.map(|index| unread.remove(index).1);
            drop(unread);
            if found.is_some() {
                return found;
            }
            let received = match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    self.received.recv_timeout(left).ok()?
                }
                None => self.received.try_recv().ok()?,
            };
            self.unread.lock().push(received);
        }
    }

    /// Every output received so far for `tx`, in order, without waiting.
    fn drain(&self, tx: &str) -> Vec<Arc<dyn AnyOutput>> {
        std::iter::from_fn(|| self.next(tx, None)).collect()
    }

    /// Wait until an output for `tx` satisfies `matches`, and return every
    /// output for `tx` received on the way, in order.
    fn wait<T: 'static>(&self, tx: &str, matches: impl Fn(&T) -> bool) -> Vec<Arc<dyn AnyOutput>> {
        let deadline = Instant::now().checked_add(DEADLINE);
        let mut seen = Vec::new();
        loop {
            let Some(output) = self.next(tx, deadline) else {
                panic!("no matching `{tx}` output arrived: saw {seen:?}");
            };
            let done = output.as_any().downcast_ref::<T>().is_some_and(&matches);
            seen.push(output);
            if done {
                return seen;
            }
        }
    }
}

fn erased<R>(ctx: &MykoServerContext, report: R) -> Cell<Arc<dyn AnyOutput>, hyphae::CellImmutable>
where
    R: myko::report::ReportParams,
{
    let request = ctx.new_server_transaction();
    ctx.in_domain(|| {
        ctx.report(report, request)
            .map(|output| -> Arc<dyn AnyOutput> { output.clone() })
            .materialize()
    })
}

fn totals(outputs: &[Arc<dyn AnyOutput>]) -> Vec<(i64, i64, i64)> {
    outputs
        .iter()
        .filter_map(|output| output.as_any().downcast_ref::<PrTotals>())
        .map(|totals| (totals.gain, totals.readings, totals.samples))
        .collect()
}

/// Visit the single-row changes of `diff`, in order.
fn each_change<K, V>(diff: &hyphae::MapDiff<K, V>, visit: &mut impl FnMut(&hyphae::MapDiff<K, V>)) {
    match diff {
        hyphae::MapDiff::Batch { changes } => {
            for change in changes {
                each_change(change, visit);
            }
        }
        change => visit(change),
    }
}

/// Every change of the shared setting `gain` as one context receives it.
fn gain_log(ctx: &MykoServerContext) -> (Arc<Mutex<Vec<i64>>>, hyphae::SubscriptionGuard) {
    let log: Arc<Mutex<Vec<i64>>> = Arc::default();
    let settings = ctx.query_map(GetAllPrSettings {}, ctx.new_server_transaction());
    let guard = {
        let log = Arc::clone(&log);
        ctx.in_domain(|| {
            settings.subscribe_diffs(move |diff| {
                each_change(diff, &mut |change| {
                    if let hyphae::MapDiff::Update { new_value, .. } = change {
                        log.lock().push(new_value.gain);
                    }
                });
            })
        })
    };
    (log, guard)
}

// fixtures -------------------------------------------------------------------

fn root_context() -> MykoServerContext {
    MykoServerContext::new(
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
    )
}

fn schema() -> PartitionSchema {
    PartitionSchema::new()
        .lifecycle::<PrRuntime>()
        .route::<PrSample>(&[PrAssignment::ENTITY_NAME_STATIC], |root, sample| {
            let assignment = root
                .registry
                .get(PrAssignment::ENTITY_NAME_STATIC)?
                .get_value(&sample.id())?;
            // `AnyOutput` is in scope and is implemented for the `Arc` itself.
            let assignment = AnyItem::as_any(assignment.as_ref()).downcast_ref::<PrAssignment>()?;
            Some(PartitionKey::new(assignment.runtime.clone()))
        })
}

struct Fixture {
    root: MykoServerContext,
    a: MykoServerContext,
    b: MykoServerContext,
}

/// One shared setting, and the runtimes `a` and `b`.
fn fixture() -> Fixture {
    let root = root_context().with_partitions(schema());
    root.set(&PrSetting {
        id: "gain".into(),
        gain: 1,
    })
    .expect("shared setting");
    for runtime in ["a", "b"] {
        root.set(&PrRuntime {
            id: runtime.into(),
            label: runtime.into(),
        })
        .expect("runtime");
    }
    let a = root.partition(&PartitionKey::new("a")).expect("runtime a");
    let b = root.partition(&PartitionKey::new("b")).expect("runtime b");
    Fixture { root, a, b }
}

fn reading(runtime: &str, id: &str, level: i64) -> PrReading {
    PrReading {
        id: id.into(),
        runtime: runtime.into(),
        level,
    }
}

fn sample(emitter: &str, level: i64) -> PrSample {
    PrSample {
        id: emitter.into(),
        level,
    }
}

fn assign(root: &MykoServerContext, emitter: &str, runtime: &str) {
    root.set(&PrAssignment {
        id: emitter.into(),
        runtime: runtime.into(),
    })
    .expect("assignment");
}

fn request(ctx: &MykoServerContext) -> Arc<RequestContext> {
    ctx.new_server_transaction()
}

/// Wait until `domain` has applied everything handed to it so far.
fn settle(ctx: &MykoServerContext) {
    ctx.domain().flush();
}

// (a) isolation --------------------------------------------------------------

/// While `busy` is parked, runtime `b` takes a write, updates both of its
/// reports and delivers them to its client, and computes a report for the
/// first time under the cache key the busy runtime may be holding.
fn assert_b_progresses(fixture: &Fixture, client_b: &Client, level: i64) {
    fixture
        .root
        .set(&reading("b", "progress", level))
        .expect("write to runtime b");
    client_b.wait::<PrTotals>("total", |totals| totals.readings == level);
    client_b.wait::<i64>("rebuilt", |sum| *sum == level);
    let scaled = fixture
        .b
        .report(PrScaled { factor: 7 }, request(&fixture.b));
    assert_eq!(*scaled.get(), level.saturating_mul(7));
}

fn busy_runtime_does_not_delay_another(stage: Stage, level: i64) {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let client_a = Client::connect(&fixture.a);
    let client_b = Client::connect(&fixture.b);
    fixture
        .root
        .set(&reading("a", "first", 1))
        .expect("first reading");
    settle(&fixture.a);

    let gate = arm(&fixture.a.domain(), stage);
    let writer = {
        let root = fixture.root.clone();
        thread::spawn(move || root.set(&reading("a", "first", 2)))
    };
    assert!(gate.wait_entered(), "runtime a never reached {stage:?}");

    assert_b_progresses(&fixture, &client_b, level);
    assert!(gate.is_parked(), "runtime a was not busy throughout");

    drop(gate);
    assert!(writer.join().expect("writer").is_ok());
    client_a.wait::<PrTotals>("total", |totals| totals.readings == 2);
    client_a.wait::<i64>("rebuilt", |sum| *sum == 2);
}

#[test]
fn a_long_wave_in_one_runtime_does_not_delay_another() {
    busy_runtime_does_not_delay_another(Stage::Wave, 10);
}

#[test]
fn a_graph_build_in_one_runtime_does_not_delay_another() {
    busy_runtime_does_not_delay_another(Stage::Build, 20);
}

#[test]
fn a_graph_drop_in_one_runtime_does_not_delay_another() {
    busy_runtime_does_not_delay_another(Stage::Drop, 30);
}

#[test]
fn a_first_computation_in_one_runtime_does_not_gate_the_same_report_in_another() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let client_b = Client::connect(&fixture.b);

    let gate = arm(&fixture.a.domain(), Stage::Compute);
    let builder = {
        let a = fixture.a.clone();
        thread::spawn(move || {
            *a.report(PrScaled { factor: 7 }, a.new_server_transaction())
                .get()
        })
    };
    assert!(gate.wait_entered(), "runtime a never began computing");

    // Runtime a holds the compute gate of `PrScaled { factor: 7 }`. Runtime b
    // computes the report with the same parameters.
    assert_b_progresses(&fixture, &client_b, 40);
    assert!(gate.is_parked(), "runtime a was not computing throughout");

    drop(gate);
    assert_eq!(builder.join().expect("builder"), 0);
}

// (b) shared writes ----------------------------------------------------------

#[test]
fn a_shared_write_reaches_every_runtime_once_in_order_while_one_is_busy() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let client_a = Client::connect(&fixture.a);
    let client_b = Client::connect(&fixture.b);
    for runtime in ["a", "b"] {
        fixture
            .root
            .set(&reading(runtime, "first", 5))
            .expect("reading");
    }
    let (gains_a, _guard_a) = gain_log(&fixture.a);
    let (gains_b, _guard_b) = gain_log(&fixture.b);
    settle(&fixture.a);
    settle(&fixture.b);
    drop(client_a.drain("total"));
    drop(client_b.drain("total"));

    let gate = arm(&fixture.a.domain(), Stage::Wave);
    let writer = {
        let root = fixture.root.clone();
        thread::spawn(move || root.set(&reading("a", "first", 6)))
    };
    assert!(gate.wait_entered(), "runtime a never parked");

    let gains = [2, 3, 4, 5];
    for gain in gains {
        fixture
            .root
            .set(&PrSetting {
                id: "gain".into(),
                gain,
            })
            .expect("shared write");
    }

    // Runtime b has every shared write while runtime a is still parked.
    let seen_b = client_b.wait::<PrTotals>("total", |totals| totals.gain == 5);
    assert_eq!(
        totals(&seen_b),
        gains.map(|gain| (gain, 5, 0)).to_vec(),
        "runtime b saw each shared write once, in order"
    );
    assert!(gate.is_parked(), "runtime a was not busy throughout");

    drop(gate);
    assert!(writer.join().expect("writer").is_ok());
    let seen_a = totals(&client_a.wait::<PrTotals>("total", |totals| totals.gain == 5));
    settle(&fixture.a);

    // Runtime a received every shared write once, in order.
    assert_eq!(*gains_a.lock(), gains);
    assert_eq!(*gains_b.lock(), gains);
    // Its report is latest-value state. The wave that was parked finished
    // with shared writes already waiting, so it may step over the outputs in
    // between, and never goes back or repeats one.
    assert!(
        seen_a.iter().all(|(_, readings, _)| *readings == 6)
            && seen_a
                .windows(2)
                .all(|pair| matches!(pair, [(before, ..), (after, ..)] if before < after)),
        "runtime a's report went back or repeated an output: {seen_a:?}"
    );
}

#[test]
fn the_partition_part_of_a_write_follows_its_shared_part() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let client_a = Client::connect(&fixture.a);
    let (readings, _guard) = reading_log(&fixture.a);
    settle(&fixture.a);
    drop(client_a.drain("total"));

    // Root is inside a wave on another thread, so the shared part below is
    // propagated by that thread after this call has returned.
    let observed = fixture.root.report(PrGain {}, request(&fixture.root));
    let gate = arm(&Domain::root(), Stage::Wave);
    let root_writer = {
        let root = fixture.root.clone();
        thread::spawn(move || {
            root.set(&PrSetting {
                id: "other".into(),
                gain: 10,
            })
        })
    };
    assert!(gate.wait_entered(), "root never parked");

    let shared: Arc<dyn AnyItem> = Arc::new(PrSetting {
        id: "gain".into(),
        gain: 100,
    });
    let partitioned: Arc<dyn AnyItem> = Arc::new(reading("a", "first", 7));
    fixture
        .root
        .set_batch_any([partitioned, shared])
        .expect("mixed write");
    // A later write to the same row keeps its place behind the mixed write,
    // so it waits for root, which this thread is about to release.
    let later = {
        let root = fixture.root.clone();
        thread::spawn(move || root.set(&reading("a", "first", 8)))
    };

    drop(gate);
    assert!(root_writer.join().expect("root writer").is_ok());
    assert!(later.join().expect("later writer").is_ok());
    let seen = client_a.wait::<PrTotals>("total", |totals| totals.readings == 8);
    let seen = totals(&seen);
    assert!(
        seen.iter()
            .all(|(gain, readings, _)| *readings == 0 || *gain == 110),
        "runtime a applied the reading before the shared rows written with it: {seen:?}"
    );
    settle(&fixture.a);
    assert_eq!(
        *readings.lock(),
        [(true, 7), (true, 8)],
        "the two writes of one row arrived in the order they were made"
    );
    drop(observed);
}

// (c) routed writes ----------------------------------------------------------

#[test]
fn a_routed_write_lands_in_its_runtime_while_root_is_busy() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-1", "b");
    let client_a = Client::connect(&fixture.a);
    let client_b = Client::connect(&fixture.b);
    settle(&fixture.a);
    settle(&fixture.b);

    // Park root inside a wave: nothing below may need root's tick.
    let observed = fixture.root.report(PrGain {}, request(&fixture.root));
    let gate = arm(&Domain::root(), Stage::Wave);
    let root_writer = {
        let root = fixture.root.clone();
        thread::spawn(move || {
            root.set(&PrSetting {
                id: "other".into(),
                gain: 10,
            })
        })
    };
    assert!(gate.wait_entered(), "root never parked");

    fixture
        .root
        .set(&sample("emitter-1", 9))
        .expect("routed write");
    client_b.wait::<PrTotals>("total", |totals| totals.samples == 9);

    let key = Arc::<str>::from("emitter-1");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        Some(PartitionKey::new("b"))
    );
    assert!(
        fixture
            .b
            .registry
            .get_or_create(PrSample::ENTITY_NAME_STATIC)
            .get_value(&key)
            .is_some(),
        "the sample is in runtime b"
    );
    assert!(
        fixture
            .a
            .registry
            .get_or_create(PrSample::ENTITY_NAME_STATIC)
            .get_value(&key)
            .is_none(),
        "the sample is not in runtime a"
    );
    assert!(
        fixture
            .root
            .registry
            .get(PrSample::ENTITY_NAME_STATIC)
            .is_none(),
        "root holds no store for a partitioned type"
    );
    assert!(gate.is_parked(), "root was not busy throughout");

    drop(gate);
    assert!(root_writer.join().expect("root writer").is_ok());
    settle(&fixture.a);
    assert!(
        totals(&client_a.drain("total"))
            .iter()
            .all(|(_, _, samples)| *samples == 0),
        "runtime a never saw the sample"
    );
    drop(observed);
}

#[test]
fn a_write_for_a_runtime_that_is_not_open_is_refused_or_held() {
    let _serial = SERIAL.lock();
    let fixture = fixture();

    let refused = fixture.root.set(&reading("missing", "first", 1));
    assert!(
        refused.is_err(),
        "a keyed write names a runtime that is not open"
    );

    // A routed row with no runtime to go to is held, and enters the runtime
    // its assignment names once there is one.
    fixture
        .root
        .set(&sample("emitter-2", 3))
        .expect("held write");
    let key = Arc::<str>::from("emitter-2");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        None
    );
    assign(&fixture.root, "emitter-2", "c");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        None
    );
    fixture
        .root
        .set(&PrRuntime {
            id: "c".into(),
            label: "c".into(),
        })
        .expect("runtime c");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        Some(PartitionKey::new("c"))
    );

    // Closing the runtime refuses its keyed writes and holds its routed rows.
    fixture
        .root
        .del(&PrRuntime {
            id: "c".into(),
            label: "c".into(),
        })
        .expect("close runtime c");
    assert!(fixture.root.partition(&PartitionKey::new("c")).is_none());
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        None
    );
    assert!(fixture.root.set(&reading("c", "first", 1)).is_err());
}

// (d) re-routing -------------------------------------------------------------

type LevelLog = Arc<Mutex<Vec<(bool, i64)>>>;

/// Every insert (`true`) and removal (`false`) of `PrSample` rows in one
/// runtime, in order, with the row's level.
fn sample_log(ctx: &MykoServerContext) -> (LevelLog, hyphae::SubscriptionGuard) {
    level_log(ctx, PrSample::ENTITY_NAME_STATIC, |row| {
        AnyItem::as_any(row)
            .downcast_ref::<PrSample>()
            .map(|sample| sample.level)
    })
}

/// [`sample_log`] for `PrReading` rows.
fn reading_log(ctx: &MykoServerContext) -> (LevelLog, hyphae::SubscriptionGuard) {
    level_log(ctx, PrReading::ENTITY_NAME_STATIC, |row| {
        AnyItem::as_any(row)
            .downcast_ref::<PrReading>()
            .map(|reading| reading.level)
    })
}

fn level_log(
    ctx: &MykoServerContext,
    entity_type: &str,
    level: fn(&dyn AnyItem) -> Option<i64>,
) -> (LevelLog, hyphae::SubscriptionGuard) {
    let log = LevelLog::default();
    let store = ctx.registry.get_or_create(entity_type);
    let level = move |row: &Arc<dyn AnyItem>| level(row.as_ref()).unwrap_or(i64::MIN);
    let guard = {
        let log = Arc::clone(&log);
        ctx.in_domain(|| {
            store.subscribe_diffs(move |diff| {
                each_change(diff, &mut |change| match change {
                    hyphae::MapDiff::Insert { value, .. } => log.lock().push((true, level(value))),
                    hyphae::MapDiff::Update { new_value, .. } => {
                        log.lock().push((true, level(new_value)));
                    }
                    hyphae::MapDiff::Remove { old_value, .. } => {
                        log.lock().push((false, level(old_value)));
                    }
                    hyphae::MapDiff::Initial { .. } | hyphae::MapDiff::Batch { .. } => {}
                });
            })
        })
    };
    (log, guard)
}

#[test]
fn reassigning_an_emitter_moves_its_row_exactly_once() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-1", "a");
    let (log_a, _guard_a) = sample_log(&fixture.a);
    let (log_b, _guard_b) = sample_log(&fixture.b);
    let client_b = Client::connect(&fixture.b);

    fixture.root.set(&sample("emitter-1", 1)).expect("sample");
    fixture.root.set(&sample("emitter-1", 2)).expect("sample");
    assign(&fixture.root, "emitter-1", "b");
    fixture.root.set(&sample("emitter-1", 3)).expect("sample");
    client_b.wait::<PrTotals>("total", |totals| totals.samples == 3);
    settle(&fixture.a);
    settle(&fixture.b);

    assert_eq!(*log_a.lock(), [(true, 1), (true, 2), (false, 2)]);
    assert_eq!(*log_b.lock(), [(true, 2), (true, 3)]);
    let key = Arc::<str>::from("emitter-1");
    assert!(
        fixture
            .a
            .registry
            .get_or_create(PrSample::ENTITY_NAME_STATIC)
            .get_value(&key)
            .is_none()
    );
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        Some(PartitionKey::new("b"))
    );
}

#[test]
fn a_row_written_while_it_is_reassigned_is_never_lost_or_in_two_runtimes() {
    const WRITES: i64 = 2_000;
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-1", "a");
    let (log_a, _guard_a) = sample_log(&fixture.a);
    let (log_b, _guard_b) = sample_log(&fixture.b);

    let writer = {
        let root = fixture.root.clone();
        thread::spawn(move || {
            for level in 1..=WRITES {
                root.set(&sample("emitter-1", level)).expect("sample");
            }
        })
    };
    for round in 0..40 {
        assign(
            &fixture.root,
            "emitter-1",
            if round % 2 == 0 { "b" } else { "a" },
        );
    }
    assert!(writer.join().is_ok());
    assign(&fixture.root, "emitter-1", "b");
    settle(&fixture.a);
    settle(&fixture.b);

    let key = Arc::<str>::from("emitter-1");
    let in_a = fixture
        .a
        .registry
        .get_or_create(PrSample::ENTITY_NAME_STATIC)
        .get_value(&key);
    let in_b = fixture
        .b
        .registry
        .get_or_create(PrSample::ENTITY_NAME_STATIC)
        .get_value(&key);
    assert!(in_a.is_none(), "the row left runtime a");
    let last = in_b
        .as_ref()
        .and_then(|row| AnyItem::as_any(row.as_ref()).downcast_ref::<PrSample>())
        .map(|sample| sample.level);
    assert_eq!(last, Some(WRITES), "runtime b holds the last write");

    // Each runtime alternates between holding the row and not holding it, and
    // every level it was handed is at least the one it last gave up: a move
    // carries the latest write and nothing is replayed.
    let mut inserted = std::collections::BTreeSet::new();
    for log in [&log_a, &log_b] {
        let mut held: Option<i64> = None;
        let mut released = 0;
        for (insert, level) in log.lock().iter().copied() {
            if insert {
                assert!(level >= released && held.is_none_or(|held| level > held));
                held = Some(level);
                inserted.insert(level);
            } else {
                assert_eq!(held, Some(level), "a runtime gave up a row it did not hold");
                released = level;
                held = None;
            }
        }
    }
    assert_eq!(
        inserted.len(),
        usize::try_from(WRITES).expect("count"),
        "every written level was observed in a runtime"
    );
}

// (e) declared reads and flow direction -------------------------------------

fn refused_root_read<R>(result: Result<R, PartitionError>) -> Option<Arc<str>> {
    match result {
        Err(PartitionError::RootRead { entity_type }) => Some(entity_type),
        _ => None,
    }
}

#[test]
fn a_reactive_read_of_a_partitioned_type_from_root_is_refused() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let root = &fixture.root;
    root.set(&reading("a", "first", 4)).expect("reading");
    let partitioned = Some(Arc::<str>::from(PrReading::ENTITY_NAME_STATIC));

    for attempt in ["first", "again, not served from a cache"] {
        assert_eq!(
            refused_root_read(root.checked(|| root.query_map(GetAllPrReadings {}, request(root)))),
            partitioned,
            "a root query over a partitioned type is refused ({attempt})"
        );
        assert_eq!(
            refused_root_read(root.checked(|| root.report(PrTotal {}, request(root)))),
            partitioned,
            "a root report that reads a partitioned type is refused ({attempt})"
        );
    }
    assert!(
        root.checked(|| root.query_map(GetAllPrSettings {}, request(root)))
            .is_ok(),
        "a root query over a shared type is built"
    );
    let rows = fixture
        .a
        .checked(|| {
            fixture
                .a
                .query_map(GetAllPrReadings {}, request(&fixture.a))
        })
        .expect("the partition builds its own read");
    assert_eq!(
        fixture
            .a
            .in_domain(|| rows.entries().materialize())
            .get()
            .len(),
        1,
        "the partition's read sees its row"
    );
}

#[test]
fn a_subscription_opens_in_the_partition_its_request_names() {
    let _serial = SERIAL.lock();
    let root = root_context().with_partitions(schema().subscribe_in(|_, payload| {
        payload
            .get("partition")
            .and_then(|key| key.as_str())
            .map(PartitionKey::new)
    }));
    root.set(&PrRuntime {
        id: "b".into(),
        label: "b".into(),
    })
    .expect("runtime");
    let request = request(&root);
    let in_b = myko::serde_json::json!({ "partition": "b" });
    let in_none = myko::serde_json::json!({});
    let in_missing = myko::serde_json::json!({ "partition": "missing" });
    let reading = Some(PrReading::ENTITY_NAME_STATIC);
    let setting = Some(PrSetting::ENTITY_NAME_STATIC);

    let opened = root
        .subscription_context(&request, &in_b, reading)
        .expect("partition b is open");
    assert_eq!(opened.partition_key(), Some(&PartitionKey::new("b")));
    let report = root
        .subscription_context(&request, &in_b, None)
        .expect("a report opens where the request names");
    assert_eq!(report.partition_key(), Some(&PartitionKey::new("b")));
    let shared = root
        .subscription_context(&request, &in_b, setting)
        .expect("a shared read");
    assert_eq!(shared.partition_key(), None, "a shared read stays in root");
    assert!(refused_root_read(root.subscription_context(&request, &in_none, reading)).is_some());
    assert_eq!(
        root.subscription_context(&request, &in_missing, reading)
            .err(),
        Some(PartitionError::NotOpen(PartitionKey::new("missing")))
    );
}

/// Subscribe to `entity_type` rows in `ctx`'s domain and, each time the row
/// `trigger` arrives, run `write` and keep its result.
fn on_row(
    ctx: &MykoServerContext,
    entity_type: &str,
    trigger: &'static str,
    write: impl Fn() -> Result<(), myko::server::PersistError> + Send + Sync + 'static,
) -> (Arc<Mutex<Vec<bool>>>, hyphae::SubscriptionGuard) {
    let results: Arc<Mutex<Vec<bool>>> = Arc::default();
    let store = ctx.registry.get_or_create(entity_type);
    let guard = {
        let results = Arc::clone(&results);
        ctx.in_domain(|| {
            store.subscribe_diffs(move |diff| {
                let mut hit = false;
                each_change(diff, &mut |change| {
                    if let hyphae::MapDiff::Insert { key, .. }
                    | hyphae::MapDiff::Update { key, .. } = change
                    {
                        hit |= key.as_ref() == trigger;
                    }
                });
                if hit {
                    results.lock().push(write().is_ok());
                }
            })
        })
    };
    (results, guard)
}

#[test]
fn a_partition_wave_cannot_write_shared_data_or_another_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let b = fixture.b.clone();
    let (to_shared, _shared) = on_row(
        &fixture.b,
        PrReading::ENTITY_NAME_STATIC,
        "to-shared",
        move || {
            b.set(&PrSetting {
                id: "gain".into(),
                gain: 99,
            })
        },
    );
    let b = fixture.b.clone();
    let (to_a, _across) = on_row(
        &fixture.b,
        PrReading::ENTITY_NAME_STATIC,
        "to-a",
        move || b.set(&reading("a", "from-b", 99)),
    );
    let b = fixture.b.clone();
    let (to_b, _own) = on_row(
        &fixture.b,
        PrReading::ENTITY_NAME_STATIC,
        "to-b",
        move || b.set(&reading("b", "from-b", 7)),
    );

    for trigger in ["to-shared", "to-a", "to-b"] {
        fixture
            .root
            .set(&reading("b", trigger, 1))
            .expect("trigger");
    }
    settle(&fixture.b);
    settle(&fixture.a);

    assert_eq!(
        *to_shared.lock(),
        [false],
        "the write to shared data was refused"
    );
    assert_eq!(
        *to_a.lock(),
        [false],
        "the write to another partition was refused"
    );
    assert_eq!(
        *to_b.lock(),
        [true],
        "a write to its own partition is taken"
    );
    let gain = fixture
        .root
        .registry
        .get(PrSetting::ENTITY_NAME_STATIC)
        .and_then(|store| store.get_value(&Arc::from("gain")));
    let gain = gain
        .as_ref()
        .and_then(|row| AnyItem::as_any(row.as_ref()).downcast_ref::<PrSetting>())
        .map(|setting| setting.gain);
    assert_eq!(gain, Some(1), "shared data is unchanged");
    let in_a = fixture
        .a
        .registry
        .get_or_create(PrReading::ENTITY_NAME_STATIC)
        .get_value(&Arc::from("from-b"));
    assert!(in_a.is_none(), "partition a is unchanged");
}

#[test]
fn a_shared_wave_writes_into_a_partition_behind_it() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let root = fixture.root.clone();
    let (results, _guard) = on_row(
        &fixture.root,
        PrSetting::ENTITY_NAME_STATIC,
        "gain",
        move || root.set(&reading("b", "from-shared", 3)),
    );
    drop(results.lock().drain(..));
    fixture
        .root
        .set(&PrSetting {
            id: "gain".into(),
            gain: 2,
        })
        .expect("shared write");
    settle(&fixture.b);
    assert_eq!(
        *results.lock(),
        [true],
        "the write from the shared wave was taken"
    );
    let level = fixture
        .b
        .registry
        .get_or_create(PrReading::ENTITY_NAME_STATIC)
        .get_value(&Arc::from("from-shared"))
        .and_then(|row| {
            AnyItem::as_any(row.as_ref())
                .downcast_ref::<PrReading>()
                .map(|row| row.level)
        });
    assert_eq!(level, Some(3), "partition b took the row");
}
