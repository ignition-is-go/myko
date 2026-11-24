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
        Cell, Definite, Gettable, JoinExt, MapExt, Materialize, Mutable, SwitchMapExt, Watchable,
        scheduler::Domain,
    },
    prelude::{AnyItem, Eventable, Querying, Reporting, WithId},
    report::{AnyOutput, ReportContext, ReportHandler},
    request::RequestContext,
    server::{
        ClientSession, HandlerRegistry, MutationValidatorRegistration, MykoServerContext,
        MykoServerRuntime, PartitionError, PartitionKey, PartitionSchema, PersistError,
        ProposedState, RelationshipManager, WsWriter,
        persister::{Persister, PersisterRouter},
    },
    store::StoreRegistry,
    wire::{EncodedCommandMessage, MEvent, MEventType, MykoMessage},
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
    root_context_logging(PersisterRouter::default())
}

fn root_context_logging(persisters: PersisterRouter) -> MykoServerContext {
    MykoServerContext::new(
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
    fixture_on(root_context())
}

fn fixture_on(root: MykoServerContext) -> Fixture {
    let root = root.with_partitions(schema());
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
    let root = root_context().with_partitions(schema().subscribe_in(&[], |_, _, payload| {
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
    assert_eq!(
        shared.partition_key(),
        Some(&PartitionKey::new("b")),
        "the hook places a subscription whatever it reads"
    );
    let unnamed = root
        .subscription_context(&request, &in_none, setting)
        .expect("a shared read the hook places nowhere");
    assert_eq!(unnamed.partition_key(), None, "it opens in root");
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
    assert_eq!(
        levels_in(&fixture.b, "from-shared"),
        Some(3),
        "partition b took the row"
    );

    // A root graph that is not itself a write reacts and writes into b.
    let signal = fixture.root.in_domain(|| Cell::new(0_i64));
    let written: Arc<Mutex<Vec<bool>>> = Arc::default();
    let _reaction = {
        let root = fixture.root.clone();
        let written = Arc::clone(&written);
        fixture.root.in_domain(|| {
            signal.subscribe(move |level| {
                if let hyphae::Signal::Value(level) = level
                    && **level > 0
                {
                    written
                        .lock()
                        .push(root.set(&reading("b", "from-graph", **level)).is_ok());
                }
            })
        })
    };
    signal.set(4);
    settle(&fixture.b);
    assert_eq!(
        *written.lock(),
        [true],
        "the write from a root graph was taken"
    );
    assert_eq!(levels_in(&fixture.b, "from-graph"), Some(4));
}

fn levels_in(ctx: &MykoServerContext, id: &str) -> Option<i64> {
    ctx.registry
        .get_or_create(PrReading::ENTITY_NAME_STATIC)
        .get_value(&Arc::from(id))
        .and_then(|row| {
            AnyItem::as_any(row.as_ref())
                .downcast_ref::<PrReading>()
                .map(|row| row.level)
        })
}

#[test]
fn a_partitions_write_of_shared_data_from_outside_a_wave_is_roots_write() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-3", "b");
    fixture
        .root
        .set(&sample("emitter-3", 5))
        .expect("routed write");
    let key = Arc::<str>::from("emitter-3");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        Some(PartitionKey::new("b"))
    );

    // Root re-routes on its own writes of what the resolver reads; a
    // partition's replacement of that data is root's write too.
    let moved: Arc<dyn AnyItem> = Arc::new(PrAssignment {
        id: "emitter-3".into(),
        runtime: "a".into(),
    });
    fixture
        .b
        .replace_batch_any([moved], [])
        .expect("shared replacement from a partition");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &key),
        Some(PartitionKey::new("a")),
        "the row followed the assignment a partition wrote"
    );

    let opened: Arc<dyn AnyItem> = Arc::new(PrRuntime {
        id: "c".into(),
        label: "c".into(),
    });
    fixture
        .b
        .replace_batch_any([opened], [])
        .expect("lifecycle write from a partition");
    assert!(
        fixture.root.partition(&PartitionKey::new("c")).is_some(),
        "a runtime a partition wrote opened"
    );
    fixture
        .b
        .del_by_id(PrRuntime::ENTITY_NAME_STATIC, "c")
        .expect("lifecycle delete from a partition");
    assert!(
        fixture.root.partition(&PartitionKey::new("c")).is_none(),
        "a runtime a partition deleted by id closed"
    );

    let mixed: [Arc<dyn AnyItem>; 2] = [
        Arc::new(PrSetting {
            id: "gain".into(),
            gain: 3,
        }),
        Arc::new(reading("b", "mixed", 1)),
    ];
    assert!(
        fixture.b.replace_batch_any(mixed, []).is_err(),
        "a replacement that mixes shared and partition data is refused"
    );
    assert_eq!(levels_in(&fixture.b, "mixed"), None);
}

type ParkedRoot = (
    Armed,
    thread::JoinHandle<Result<(), myko::server::PersistError>>,
    Cell<Arc<i64>, hyphae::CellImmutable>,
);

/// Park root inside a wave on another thread. Root's deliveries made while
/// it is parked run once the returned gate drops and the writer is joined.
fn park_root(root: &MykoServerContext) -> ParkedRoot {
    let observed = root.report(PrGain {}, request(root));
    let gate = arm(&Domain::root(), Stage::Wave);
    let writer = {
        let root = root.clone();
        thread::spawn(move || {
            root.set(&PrSetting {
                id: "other".into(),
                gain: 10,
            })
        })
    };
    assert!(gate.wait_entered(), "root never parked");
    (gate, writer, observed)
}

#[test]
fn a_write_behind_a_shared_write_is_applied_when_its_runtime_closes_first() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let runtime = PrRuntime {
        id: "c".into(),
        label: "c".into(),
    };
    fixture.root.set(&runtime).expect("runtime c");
    let c = fixture
        .root
        .partition(&PartitionKey::new("c"))
        .expect("runtime c is open");
    let (applied, received) = mpsc::channel();
    let store = c.registry.get_or_create(PrReading::ENTITY_NAME_STATIC);
    let _applied = {
        let applied = Mutex::new(applied);
        c.in_domain(|| {
            store.subscribe_diffs(move |diff| {
                each_change(diff, &mut |change| {
                    if let hyphae::MapDiff::Insert { key, .. } = change {
                        drop(applied.lock().send(key.clone()));
                    }
                });
            })
        })
    };

    let (gate, writer, observed) = park_root(&fixture.root);
    let shared: Arc<dyn AnyItem> = Arc::new(PrSetting {
        id: "gain".into(),
        gain: 5,
    });
    let partitioned: Arc<dyn AnyItem> = Arc::new(reading("c", "last", 1));
    fixture
        .root
        .set_batch_any([shared, partitioned])
        .expect("the write is admitted while runtime c is open");
    // Runtime c closes while the write is still behind root's wave.
    fixture.root.del(&runtime).expect("close runtime c");
    assert!(
        fixture.root.set(&reading("c", "late", 1)).is_err(),
        "a write made after the close is refused"
    );
    drop(gate);
    assert!(writer.join().expect("root writer").is_ok());

    assert_eq!(
        received.recv_timeout(DEADLINE).ok().as_deref(),
        Some("last"),
        "the admitted write was applied in its runtime before it closed"
    );
    drop(observed);
}

fn samples_in(ctx: &MykoServerContext, id: &str) -> Option<i64> {
    ctx.registry
        .get_or_create(PrSample::ENTITY_NAME_STATIC)
        .get_value(&Arc::from(id))
        .and_then(|row| {
            AnyItem::as_any(row.as_ref())
                .downcast_ref::<PrSample>()
                .map(|row| row.level)
        })
}

#[test]
fn a_routed_write_from_a_shared_wave_lands_where_its_row_is_placed_on_arrival() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-4", "b");
    assign(&fixture.root, "emitter-5", "b");
    let root = fixture.root.clone();
    let (results, _guard) = on_row(
        &fixture.root,
        PrSetting::ENTITY_NAME_STATIC,
        "gain",
        move || {
            root.set(&sample("emitter-4", 4))?;
            // Written, then reassigned, in one wave: the row is in transit
            // to b when its assignment moves it to a.
            root.set(&sample("emitter-5", 5))?;
            root.set(&PrAssignment {
                id: "emitter-5".into(),
                runtime: "a".into(),
            })
        },
    );
    drop(results.lock().drain(..));
    // Runtime a is parked below before the shared write that sends both
    // copies, so the copy sent to b arrives before the re-sent one reaches a.
    let client_a = Client::connect(&fixture.a);
    let (in_b, _in_b) = sample_log(&fixture.b);
    settle(&fixture.a);
    let gate = arm(&fixture.a.domain(), Stage::Wave);
    let writer = {
        let root = fixture.root.clone();
        thread::spawn(move || root.set(&reading("a", "busy", 1)))
    };
    assert!(gate.wait_entered(), "runtime a never parked");
    fixture
        .root
        .set(&PrSetting {
            id: "gain".into(),
            gain: 2,
        })
        .expect("shared write");
    settle(&fixture.b);
    assert_eq!(
        *in_b.lock(),
        [(true, 4)],
        "the write that reached b after its row moved was not applied there"
    );
    assert!(gate.is_parked(), "runtime a was busy throughout");
    drop(gate);
    assert!(writer.join().expect("writer").is_ok());
    settle(&fixture.a);
    drop(client_a);

    assert_eq!(
        *results.lock(),
        [true],
        "the routed writes from the shared wave were taken"
    );
    assert_eq!(samples_in(&fixture.b, "emitter-4"), Some(4));
    assert_eq!(
        samples_in(&fixture.a, "emitter-5"),
        Some(5),
        "the moved row is in a"
    );
    assert_eq!(samples_in(&fixture.b, "emitter-5"), None, "and not in b");
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &Arc::from("emitter-5")),
        Some(PartitionKey::new("a"))
    );
}

// (f) the event log ----------------------------------------------------------

/// A persister that keeps every event it is given, in order.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<MEvent>>>);

impl Persister for Log {
    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        self.0.lock().push(event);
        Ok(())
    }
}

impl Log {
    fn fixture(&self) -> Fixture {
        let mut persisters = PersisterRouter::default();
        persisters.set_default(Some(Arc::new(self.clone())));
        fixture_on(root_context_logging(persisters))
    }

    /// `(change, id, level)` of every logged event of `entity_type`.
    fn of(&self, entity_type: &str) -> Vec<(MEventType, String, i64)> {
        self.0
            .lock()
            .iter()
            .filter(|event| event.item_type.as_ref() == entity_type)
            .map(|event| {
                (
                    event.change_type,
                    event
                        .item
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    event
                        .item
                        .get("level")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or_default(),
                )
            })
            .collect()
    }
}

fn set(id: &str, level: i64) -> (MEventType, String, i64) {
    (MEventType::SET, id.to_owned(), level)
}

fn del(id: &str, level: i64) -> (MEventType, String, i64) {
    (MEventType::DEL, id.to_owned(), level)
}

/// Refuses a partitioned row with a negative level.
fn no_negative_levels(state: &ProposedState<'_>, _reversal: bool) -> Result<(), String> {
    let negative = state.upserts.iter().any(|item| {
        let any = AnyItem::as_any(item.as_ref());
        any.downcast_ref::<PrReading>()
            .map(|row| row.level)
            .or_else(|| any.downcast_ref::<PrSample>().map(|row| row.level))
            .is_some_and(|level| level < 0)
    });
    if negative {
        Err("negative level".into())
    } else {
        Ok(())
    }
}

myko::inventory::submit! {
    MutationValidatorRegistration {
        validate_reversal_deletes: false,
        entity_types: &[PrReading::ENTITY_NAME_STATIC, PrSample::ENTITY_NAME_STATIC],
        validate: no_negative_levels,
    }
}

#[test]
fn a_routed_row_is_logged_once_per_write_whatever_its_placement() {
    let _serial = SERIAL.lock();
    let log = Log::default();
    let fixture = log.fixture();
    let samples = || log.of(PrSample::ENTITY_NAME_STATIC);

    // Held, then placed when its partition is named.
    fixture.root.set(&sample("held", 1)).expect("held write");
    assert_eq!(samples(), [set("held", 1)], "a held row's write is logged");
    assign(&fixture.root, "held", "b");
    assert_eq!(samples_in(&fixture.b, "held"), Some(1));
    // Moved.
    assign(&fixture.root, "held", "a");
    assert_eq!(samples_in(&fixture.a, "held"), Some(1));
    assert_eq!(
        samples(),
        [set("held", 1)],
        "placing and moving log nothing"
    );
    fixture.root.set(&sample("held", 2)).expect("write in a");

    // In transit behind a shared wave, then held by the same wave.
    let root = fixture.root.clone();
    let (results, _guard) = on_row(
        &fixture.root,
        PrSetting::ENTITY_NAME_STATIC,
        "gain",
        move || {
            root.set(&sample("held", 3))?;
            root.set(&PrAssignment {
                id: "held".into(),
                runtime: "nowhere".into(),
            })
        },
    );
    drop(results.lock().drain(..));
    fixture
        .root
        .set(&PrSetting {
            id: "gain".into(),
            gain: 3,
        })
        .expect("shared write");
    assert_eq!(*results.lock(), [true]);
    assert_eq!(
        fixture
            .root
            .partition_of(PrSample::ENTITY_NAME_STATIC, &Arc::from("held")),
        None,
        "the row in transit is held"
    );
    fixture
        .root
        .del(&sample("held", 3))
        .expect("delete of the held row");
    settle(&fixture.a);
    settle(&fixture.b);
    assert_eq!(samples_in(&fixture.a, "held"), None);
    assert_eq!(samples_in(&fixture.b, "held"), None);

    // A closed partition's row is held and its writes are logged.
    assign(&fixture.root, "closing", "a");
    fixture.root.set(&sample("closing", 4)).expect("write in a");
    fixture
        .root
        .del(&PrRuntime {
            id: "a".into(),
            label: "a".into(),
        })
        .expect("close a");
    fixture.root.set(&sample("closing", 5)).expect("held write");
    settle(&fixture.b);

    assert_eq!(
        samples(),
        [
            set("held", 1),
            set("held", 2),
            set("held", 3),
            del("held", 3),
            set("closing", 4),
            set("closing", 5),
        ],
        "every admitted write is logged exactly once, in admission order"
    );
}

#[test]
fn a_keyed_write_behind_a_shared_write_is_logged_when_admitted() {
    let _serial = SERIAL.lock();
    let log = Log::default();
    let fixture = log.fixture();
    let (gate, writer, observed) = park_root(&fixture.root);
    let shared: Arc<dyn AnyItem> = Arc::new(PrSetting {
        id: "gain".into(),
        gain: 3,
    });
    let keyed: Arc<dyn AnyItem> = Arc::new(reading("b", "behind", 7));
    fixture
        .root
        .set_batch_any([shared, keyed])
        .expect("admitted");
    assert_eq!(
        log.of(PrReading::ENTITY_NAME_STATIC),
        [set("behind", 7)],
        "logged on the writer's path, before it is applied"
    );
    drop(gate);
    assert!(writer.join().expect("root writer").is_ok());
    drop(observed);
    settle(&fixture.b);
    assert_eq!(levels_in(&fixture.b, "behind"), Some(7));
    assert_eq!(
        log.of(PrReading::ENTITY_NAME_STATIC),
        [set("behind", 7)],
        "and not logged again when applied"
    );
}

#[test]
fn a_write_that_would_fail_when_applied_is_refused_when_made() {
    let _serial = SERIAL.lock();
    let log = Log::default();
    let fixture = log.fixture();
    assign(&fixture.root, "emitter", "b");
    let (gate, writer, observed) = park_root(&fixture.root);
    let shared = || -> Arc<dyn AnyItem> {
        Arc::new(PrSetting {
            id: "gain".into(),
            gain: 3,
        })
    };
    let keyed: Arc<dyn AnyItem> = Arc::new(reading("b", "bad", -1));
    let routed: Arc<dyn AnyItem> = Arc::new(sample("emitter", -1));
    assert!(
        fixture.root.set_batch_any([shared(), keyed]).is_err(),
        "a keyed write behind a shared write is refused to its writer"
    );
    assert!(
        fixture.root.set_batch_any([shared(), routed]).is_err(),
        "a routed write behind a shared write is refused to its writer"
    );
    assert!(
        fixture.root.set(&sample("unplaced", -1)).is_err(),
        "a write that would be held is refused to its writer"
    );
    drop(gate);
    assert!(writer.join().expect("root writer").is_ok());
    drop(observed);
    settle(&fixture.b);
    assert_eq!(levels_in(&fixture.b, "bad"), None);
    assert_eq!(samples_in(&fixture.b, "emitter"), None);
    assert!(log.of(PrReading::ENTITY_NAME_STATIC).is_empty());
    assert!(log.of(PrSample::ENTITY_NAME_STATIC).is_empty());
}

/// Run `write` on its own thread and fail rather than hang when it does
/// not return.
fn returns(write: impl FnOnce() + Send + 'static) -> bool {
    let (done, finished) = mpsc::channel();
    thread::spawn(move || {
        write();
        let _ = done.send(());
    });
    finished.recv_timeout(DEADLINE).is_ok()
}

#[test]
fn a_wave_that_rewrites_the_routed_row_it_applies_does_not_deadlock() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "echo", "b");
    let b = fixture.b.clone();
    let (results, _guard) = on_row(
        &fixture.b,
        PrSample::ENTITY_NAME_STATIC,
        "echo",
        move || match samples_in(&b, "echo") {
            Some(level) if level % 2 == 1 => b.set(&sample("echo", level + 1)),
            _ => Ok(()),
        },
    );

    let root = fixture.root.clone();
    assert!(
        returns(move || drop(root.set(&sample("echo", 1)))),
        "a write applied on its writer's path"
    );
    settle(&fixture.b);
    assert_eq!(samples_in(&fixture.b, "echo"), Some(2));

    let root = fixture.root.clone();
    assert!(
        returns(move || {
            let shared: Arc<dyn AnyItem> = Arc::new(PrSetting {
                id: "gain".into(),
                gain: 4,
            });
            let routed: Arc<dyn AnyItem> = Arc::new(sample("echo", 3));
            drop(root.set_batch_any([shared, routed]));
            root.partition(&PartitionKey::new("b"))
                .expect("b")
                .domain()
                .flush();
        }),
        "a write landing behind a shared write"
    );
    assert_eq!(samples_in(&fixture.b, "echo"), Some(4));
    assert!(results.lock().iter().all(|ok| *ok));
}

/// Park `ctx`'s runtime worker: work handed to the runtime waits behind it
/// until the returned gate is dropped.
fn park_worker(ctx: &MykoServerContext) -> Armed {
    let gate = Armed(Arc::new(Gate::default()));
    let parked = Arc::clone(&gate.0);
    ctx.domain().hand_off(move || parked.pass());
    assert!(gate.wait_entered(), "the runtime's worker never parked");
    gate
}

#[test]
fn a_routed_write_that_reaches_its_old_runtime_after_a_move_is_skipped_there() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assign(&fixture.root, "emitter-6", "b");
    let (in_b, _in_b) = sample_log(&fixture.b);
    let root = fixture.root.clone();
    let (results, _guard) = on_row(
        &fixture.root,
        PrSetting::ENTITY_NAME_STATIC,
        "gain",
        move || {
            // Sent to b, then moved to a, in one wave.
            root.set(&sample("emitter-6", 6))?;
            assign(&root, "emitter-6", "a");
            Ok(())
        },
    );
    drop(results.lock().drain(..));
    // Runtime a's worker is parked, so the copy sent to b lands first.
    let gate = park_worker(&fixture.a);
    fixture
        .root
        .set(&PrSetting {
            id: "gain".into(),
            gain: 2,
        })
        .expect("shared write");
    assert_eq!(*results.lock(), [true]);
    settle(&fixture.b);
    assert!(gate.is_parked(), "runtime a had not taken its copy yet");
    assert!(
        in_b.lock().iter().all(|(_, level)| *level != 6),
        "the copy that reached b after its row moved was not applied there"
    );
    drop(gate);
    settle(&fixture.a);
    assert_eq!(samples_in(&fixture.a, "emitter-6"), Some(6));
    assert_eq!(samples_in(&fixture.b, "emitter-6"), None);
}

fn runtime_row(id: &str) -> PrRuntime {
    PrRuntime {
        id: id.into(),
        label: id.into(),
    }
}

#[test]
fn a_write_racing_a_close_is_either_refused_or_logged_and_applied() {
    let _serial = SERIAL.lock();
    for seed in 0..40_u32 {
        let log = Log::default();
        let fixture = log.fixture();
        fixture.root.set(&runtime_row("c")).expect("open c");
        let c = fixture.root.partition(&PartitionKey::new("c")).expect("c");
        let (applied, _applied) = reading_log(&c);
        let (wrote, written) = mpsc::channel();
        let writer = {
            let root = fixture.root.clone();
            thread::spawn(move || {
                (0..20_i64)
                    .filter(|level| {
                        let _ = wrote.send(());
                        let shared: Arc<dyn AnyItem> = Arc::new(PrSetting {
                            id: "gain".into(),
                            gain: *level,
                        });
                        let keyed: Arc<dyn AnyItem> =
                            Arc::new(reading("c", &format!("r{level}"), *level));
                        root.set_batch_any([shared, keyed]).is_ok()
                    })
                    .collect::<Vec<_>>()
            })
        };
        // Close while the writer is between, or inside, its writes.
        for _ in 0..seed % 20 {
            written.recv_timeout(DEADLINE).expect("writer is writing");
        }
        for _ in 0..seed / 2 {
            thread::yield_now();
        }
        fixture.root.del(&runtime_row("c")).expect("close c");
        let admitted = writer.join().expect("writer");
        c.domain().flush();
        let logged: Vec<i64> = log
            .of(PrReading::ENTITY_NAME_STATIC)
            .into_iter()
            .map(|(_, _, level)| level)
            .collect();
        assert_eq!(
            logged, admitted,
            "seed {seed}: logged exactly the admitted writes"
        );
        let landed: Vec<i64> = applied
            .lock()
            .iter()
            .filter(|(inserted, _)| *inserted)
            .map(|(_, level)| *level)
            .collect();
        assert_eq!(
            landed, admitted,
            "seed {seed}: applied exactly the admitted writes"
        );
    }
}

// (g) replay -----------------------------------------------------------------

/// Every row of `entity_type` in `ctx`'s own store, as `(id, value)`.
fn rows(ctx: &MykoServerContext, entity_type: &str) -> Vec<(String, serde_json::Value)> {
    let mut rows: Vec<_> = ctx
        .registry
        .get_or_create(entity_type)
        .snapshot()
        .into_iter()
        .map(|(id, row)| (id.to_string(), myko::prelude::ToValue::to_value(&row)))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

type State = (
    Vec<(String, Vec<(String, serde_json::Value)>)>,
    Vec<(
        String,
        Vec<(String, serde_json::Value)>,
        Vec<(String, serde_json::Value)>,
    )>,
    Vec<(String, Option<PartitionKey>)>,
);

/// Shared rows, each open partition's rows, and where each routed row is.
fn state(root: &MykoServerContext, routed: &[&str]) -> State {
    let shared = [
        PrSetting::ENTITY_NAME_STATIC,
        PrRuntime::ENTITY_NAME_STATIC,
        PrAssignment::ENTITY_NAME_STATIC,
    ]
    .into_iter()
    .map(|entity_type| (entity_type.to_owned(), rows(root, entity_type)))
    .collect();
    let mut keys = root.partition_keys();
    keys.sort();
    let partitions = keys
        .into_iter()
        .map(|key| {
            let ctx = root.partition(&key).expect("open");
            settle(&ctx);
            (
                key.to_string(),
                rows(&ctx, PrReading::ENTITY_NAME_STATIC),
                rows(&ctx, PrSample::ENTITY_NAME_STATIC),
            )
        })
        .collect();
    let placements = routed
        .iter()
        .map(|id| {
            (
                (*id).to_owned(),
                root.partition_of(PrSample::ENTITY_NAME_STATIC, &Arc::from(*id)),
            )
        })
        .collect();
    (shared, partitions, placements)
}

#[test]
fn replaying_the_log_rebuilds_what_live_execution_built() {
    let _serial = SERIAL.lock();
    let log = Log::default();
    let live = log.fixture();
    let root = &live.root;
    assign(root, "moved", "a");
    root.set(&sample("moved", 1)).expect("routed write");
    root.set(&reading("a", "kept", 1)).expect("keyed write");
    root.set(&sample("held", 2)).expect("held write");
    root.set(&PrSetting {
        id: "gain".into(),
        gain: 2,
    })
    .expect("shared write");
    assign(root, "moved", "b");
    root.set(&sample("moved", 3)).expect("routed write in b");
    root.set(&reading("b", "gone", 4)).expect("keyed write");
    root.del(&reading("b", "gone", 4)).expect("keyed delete");
    // A partition that first appears late in the log.
    root.set(&runtime_row("late")).expect("open late");
    assign(root, "late-row", "late");
    root.set(&sample("late-row", 5))
        .expect("routed write in late");
    root.set(&reading("late", "late-reading", 6))
        .expect("keyed write in late");
    // A partition that closes and opens again: its keyed rows went with it,
    // its routed rows were held and came back.
    root.set(&runtime_row("again")).expect("open again");
    assign(root, "returns", "again");
    root.set(&sample("returns", 7))
        .expect("routed write in again");
    root.set(&reading("again", "lost", 8))
        .expect("keyed write in again");
    root.del(&runtime_row("again")).expect("close again");
    root.set(&runtime_row("again")).expect("reopen again");
    root.set(&reading("a", "kept", 9)).expect("keyed write");
    let routed = ["moved", "held", "late-row", "returns"];
    let expected = state(root, &routed);
    assert_eq!(
        expected.2,
        [
            ("moved".to_owned(), Some(PartitionKey::new("b"))),
            ("held".to_owned(), None),
            ("late-row".to_owned(), Some(PartitionKey::new("late"))),
            ("returns".to_owned(), Some(PartitionKey::new("again"))),
        ]
    );

    let events = log.0.lock().clone();
    let replayed_log = Log::default();
    let mut persisters = PersisterRouter::default();
    persisters.set_default(Some(Arc::new(replayed_log.clone())));
    let replayed = root_context_logging(persisters).with_partitions(schema());
    let count = replayed.replay(events.clone()).expect("replay");
    assert_eq!(count, events.len(), "every event was read");
    assert_eq!(
        state(&replayed, &routed),
        expected,
        "replay ends where live execution ended"
    );
    assert!(
        replayed_log.0.lock().is_empty(),
        "replay records nothing again"
    );
}
