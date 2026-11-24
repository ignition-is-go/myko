//! Client subscriptions on a partitioned server: each opens in the partition
//! its request names and is delivered from there, so a partition that is
//! busy delays no client of another.

#![allow(clippy::panic)]

use std::sync::{Condvar, Mutex as StdMutex, mpsc as std_mpsc};

use myko::{
    hyphae::{Definite, MapExt, MapQuery, MapValuesExt, Materialize, scheduler::Domain},
    prelude::{AnyItem, Querying},
    report::{ReportContext, ReportHandler},
    server::{PartitionKey, PartitionSchema},
    view::{ViewBuildArgs, ViewHandler},
    wire::{wrap_query, wrap_report, wrap_view},
};

use super::*;

mod runtime {
    use myko::prelude::*;

    /// Shared. Setting one opens the partition named by its id.
    #[myko::myko_item]
    pub struct PtRuntime {}
}

mod reading {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by the partition each row names.
    #[myko::myko_item]
    pub struct PtReading {
        #[myko_partition_key]
        pub partition: Arc<str>,
        pub level: i64,
    }
}

mod summary {
    use myko::prelude::*;

    /// Rows a view derives from readings. Never stored, and not named by
    /// the partition schema.
    #[myko::myko_item]
    pub struct PtSummary {
        pub level: i64,
    }
}

mod seat {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Assigns a seat to the partition a client of it reads.
    #[myko::myko_item]
    pub struct PtSeat {
        pub partition: Arc<str>,
    }
}

use reading::{GetAllPtReadings, PtReading};
use runtime::{GetAllPtRuntimes, PtRuntime};
use seat::PtSeat;
use summary::PtSummary;

const DEADLINE: Duration = Duration::from_secs(10);

fn deadline() -> Instant {
    Instant::now()
        .checked_add(DEADLINE)
        .unwrap_or_else(Instant::now)
}

/// Tests in this module share the parking gate.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct Gate {
    /// `(entered, open)`
    state: StdMutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    fn lock(&self) -> std::sync::MutexGuard<'_, (bool, bool)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn pass(&self) {
        let mut state = self.lock();
        state.0 = true;
        self.changed.notify_all();
        let deadline = deadline();
        while !state.1 && Instant::now() < deadline {
            state = self
                .changed
                .wait_timeout(state, DEADLINE)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        drop(state);
    }

    fn wait_entered(&self) -> bool {
        let mut state = self.lock();
        let deadline = deadline();
        while !state.0 && Instant::now() < deadline {
            state = self
                .changed
                .wait_timeout(state, DEADLINE)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        state.0
    }

    fn is_parked(&self) -> bool {
        let state = self.lock();
        state.0 && !state.1
    }

    fn open(&self) {
        self.lock().1 = true;
        self.changed.notify_all();
    }
}

static ARMED: StdMutex<Option<(Domain, Arc<Gate>)>> = StdMutex::new(None);

/// Park the calling thread while it propagates the armed partition.
fn park() {
    let gate = ARMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .filter(|(domain, _)| Domain::propagating().as_ref() == Some(domain))
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.pass();
    }
}

struct Armed(Arc<Gate>);

impl Drop for Armed {
    fn drop(&mut self) {
        self.0.open();
        *ARMED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

fn arm(domain: Domain) -> Armed {
    let gate = Arc::new(Gate::default());
    *ARMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((domain, Arc::clone(&gate)));
    Armed(gate)
}

/// The sum of one partition's levels. Its `partition` parameter is what the
/// server's subscription hook reads.
#[myko::myko_report(i64)]
pub struct PtLevels {
    pub partition: Arc<str>,
}

impl ReportHandler for PtLevels {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<i64>, Definite> {
        ctx.query_map(GetAllPtReadings {})
            .entries()
            .map(|rows| {
                park();
                Arc::new(rows.iter().map(|(_, row)| row.level).sum::<i64>())
            })
            .materialize()
    }
}

/// Every reading of one partition. Its `partition` parameter is what the
/// server's subscription hook reads.
#[myko::myko_view(PtReading)]
pub struct PtReadingsView {
    pub partition: Arc<str>,
}

impl ViewHandler for PtReadingsView {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<PtReading>> {
        ctx.view_context.query_map_by_str(GetAllPtReadings {})
    }
}

/// Every reading of one partition as a summary row. Its `partition`
/// parameter is what the server's subscription hook reads.
#[myko::myko_view(PtSummary)]
pub struct PtSummaryView {
    pub partition: Arc<str>,
}

impl ViewHandler for PtSummaryView {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<PtSummary>> {
        ctx.view_context
            .query_map_by_str(GetAllPtReadings {})
            .map_values(|id, reading| {
                Arc::new(PtSummary {
                    id: summary::PtSummaryId(Arc::clone(id)),
                    level: reading.level,
                })
            })
    }
}

/// Every reading of the partition a seat is assigned to.
#[myko::myko_view(PtReading)]
pub struct PtSeatView {
    pub seat: Arc<str>,
}

impl ViewHandler for PtSeatView {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<PtReading>> {
        ctx.view_context.query_map_by_str(GetAllPtReadings {})
    }
}

/// The sum of the levels its context reads.
#[myko::myko_command(i64)]
pub struct PtLevelHere {
    pub partition: Arc<str>,
}

impl myko::command::CommandHandler for PtLevelHere {
    fn execute(
        self,
        ctx: myko::command::CommandContext,
    ) -> Result<i64, myko::command::CommandError> {
        Ok(ctx
            .exec_query(GetAllPtReadings {})?
            .iter()
            .map(|row| row.level)
            .sum())
    }
}

/// The sum of one partition's levels, read through an explicit handle.
#[myko::myko_command(i64)]
pub struct PtLevelIn {
    pub key: Arc<str>,
}

impl myko::command::CommandHandler for PtLevelIn {
    fn execute(
        self,
        ctx: myko::command::CommandContext,
    ) -> Result<i64, myko::command::CommandError> {
        Ok(ctx
            .in_partition(&PartitionKey::new(self.key))?
            .exec_query(GetAllPtReadings {})?
            .iter()
            .map(|row| row.level)
            .sum())
    }
}

/// Run `command` as the connection's command worker does, and return the
/// response or error message it sends.
fn run_command(
    ctx: &Arc<MykoServerContext>,
    command: &impl serde::Serialize,
    id: &str,
) -> MykoMessage {
    let (deferred_tx, mut deferred_rx) = mpsc::channel(4);
    let mut payload = serde_json::to_value(command).unwrap_or_else(|error| panic!("{error}"));
    if let Some(object) = payload.as_object_mut() {
        object.insert("tx".into(), "command".into());
    }
    WsHandler::execute_command_job(
        ctx,
        &deferred_tx,
        &mpsc::channel(4).0,
        &DropLogger::new("client".into(), watch::channel(false).0),
        "client".into(),
        CommandJob {
            auth: None,
            tx_id: "command".into(),
            command_id: id.into(),
            command: payload,
            received_at: Instant::now(),
        },
    );
    match deferred_rx.try_recv() {
        Ok(DeferredOutbound::CommandResult(message)) => message,
        _ => panic!("the command sent no result"),
    }
}

#[derive(Clone)]
struct Socket(Arc<StdMutex<std_mpsc::Sender<MykoMessage>>>);

impl WsWriter for Socket {
    fn send(&self, message: MykoMessage) {
        drop(
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .send(message),
        );
    }

    fn send_serialized_command(
        &self,
        _tx: Arc<str>,
        _command_id: String,
        _payload: EncodedCommandMessage,
    ) {
    }
}

/// One websocket client of the server, driven through the handler's own
/// request, build and registration steps.
struct Client {
    session: ClientSession<Socket>,
    received: std_mpsc::Receiver<MykoMessage>,
    builds: SubscriptionBuilds,
    built_tx: mpsc::UnboundedSender<SubscriptionBuilt>,
    built_rx: mpsc::UnboundedReceiver<SubscriptionBuilt>,
    priority_tx: mpsc::Sender<MykoMessage>,
    priority_rx: mpsc::Receiver<MykoMessage>,
    logger: Arc<DropLogger>,
    placements: SubscriptionPlacements,
    moved_rx: MovedSubscriptions,
}

impl Client {
    fn new(name: &str) -> Self {
        let (sender, received) = std_mpsc::channel();
        let (built_tx, built_rx) = mpsc::unbounded_channel();
        let (priority_tx, priority_rx) = mpsc::channel(16);
        let (placements, moved_rx) = SubscriptionPlacements::new();
        Self {
            session: ClientSession::new(name.into(), Socket(Arc::new(StdMutex::new(sender)))),
            received,
            builds: SubscriptionBuilds::default(),
            built_tx,
            built_rx,
            priority_tx,
            priority_rx,
            logger: Arc::new(DropLogger::new(name.into(), watch::channel(false).0)),
            placements,
            moved_rx,
        }
    }

    fn send(&mut self, ctx: &Arc<MykoServerContext>, message: MykoMessage) {
        self.handle(ctx, |session, ctx, context| {
            WsHandler::handle_message(session, ctx, context, message);
        });
    }

    /// Wait for a subscription of this client to move, and open it again
    /// where it now belongs, as the read loop does.
    async fn follow_move(&mut self, ctx: &Arc<MykoServerContext>) {
        let moved = tokio::time::timeout(DEADLINE, self.moved_rx.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("no subscription moved"));
        self.handle(ctx, |session, ctx, context| {
            WsHandler::replace_subscription(session, ctx, context, moved);
        });
    }

    fn handle(
        &mut self,
        ctx: &Arc<MykoServerContext>,
        action: impl FnOnce(&mut ClientSession<Socket>, Arc<MykoServerContext>, &MessageContext<'_>),
    ) {
        let context = MessageContext {
            auth: &None,
            priority_tx: &self.priority_tx,
            drop_logger: &self.logger,
            query_ids_by_tx: &Arc::default(),
            view_ids_by_tx: &Arc::default(),
            subscribe_started_by_tx: &Arc::default(),
            command_started_by_tx: &Arc::default(),
            outbound_commands_by_tx: &Arc::default(),
            command_tx: &mpsc::unbounded_channel().0,
            query_window_tx: &mpsc::unbounded_channel().0,
            subscribe_tx: &self.built_tx,
            subscription_builds: &self.builds,
            view_delivery_tx: &mpsc::unbounded_channel().0,
            placements: &self.placements,
        };
        action(&mut self.session, ctx.clone(), &context);
    }

    /// Wait for the build the last request started and register it, as the
    /// read loop does.
    async fn register(&mut self) {
        let built = tokio::time::timeout(DEADLINE, self.built_rx.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("the subscription build never finished"));
        let (_, ready) =
            WsHandler::complete_subscription(&self.builds, built, &self.priority_tx, &self.logger)
                .unwrap_or_else(|| panic!("the build was not current"));
        if let Some(ready) = ready {
            tests_support::install(&mut self.session, ready);
        }
    }

    fn subscribe_levels(&mut self, ctx: &Arc<MykoServerContext>, partition: &str) {
        let report = wrap_report(
            "levels".into(),
            &PtLevels {
                partition: partition.into(),
            },
        )
        .unwrap_or_else(|error| panic!("report: {error}"));
        self.send(ctx, MykoMessage::Report(report));
    }

    /// Wait until the subscription delivers `level`.
    fn wait_level(&self, level: i64) {
        let deadline = deadline();
        let mut seen = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.received.recv_timeout(left) {
                Ok(MykoMessage::ReportResponse(response)) => {
                    let value = response.response.as_i64();
                    if value == Some(level) {
                        return;
                    }
                    seen.push(value);
                }
                Ok(_) => {}
                Err(error) => panic!("level {level} never arrived ({error}): saw {seen:?}"),
            }
        }
    }

    fn subscribe_readings(&mut self, ctx: &Arc<MykoServerContext>, partition: &str) {
        let view = wrap_view(
            format!("readings-{partition}").into(),
            &PtReadingsView {
                partition: partition.into(),
            },
        )
        .unwrap_or_else(|error| panic!("view: {error}"));
        self.send(ctx, MykoMessage::View(view));
    }

    /// Wait until the view delivers a row with `level`, and return every
    /// `(partition, level)` upserted on the way.
    fn wait_reading(&self, level: i64) -> Vec<(Arc<str>, i64)> {
        let deadline = deadline();
        let mut seen = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.received.recv_timeout(left) {
                Ok(MykoMessage::ViewResponse(response)) => {
                    let rows = response.upserts.iter().filter_map(|upsert| {
                        AnyItem::as_any(upsert.item.as_ref())
                            .downcast_ref::<PtReading>()
                            .map(|row| (row.partition.clone(), row.level))
                    });
                    seen.extend(rows);
                    if seen.iter().any(|(_, seen)| *seen == level) {
                        return seen;
                    }
                }
                Ok(_) => {}
                Err(error) => panic!("reading {level} never arrived ({error}): saw {seen:?}"),
            }
        }
    }

    /// The next view response, its sequence and `(partition, level)` rows.
    fn next_view(&self) -> (u64, Vec<(Arc<str>, i64)>) {
        loop {
            match self.received.recv_timeout(DEADLINE) {
                Ok(MykoMessage::ViewResponse(response)) => {
                    let rows = response
                        .upserts
                        .iter()
                        .filter_map(|upsert| {
                            AnyItem::as_any(upsert.item.as_ref())
                                .downcast_ref::<PtReading>()
                                .map(|row| (row.partition.clone(), row.level))
                        })
                        .collect();
                    return (response.sequence, rows);
                }
                Ok(_) => {}
                Err(error) => panic!("no view response ({error})"),
            }
        }
    }

    /// The next error the server sent this client.
    async fn error(&mut self) -> MykoMessage {
        tokio::time::timeout(DEADLINE, self.priority_rx.recv())
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("no error was sent"))
    }
}

mod tests_support {
    use super::*;

    pub(super) fn install<W: WsWriter>(session: &mut ClientSession<W>, ready: SubscriptionReady) {
        match ready {
            SubscriptionReady::Report {
                tx_id,
                report_id,
                cell,
            } => session.subscribe_report(tx_id, report_id, cell),
            SubscriptionReady::WindowedQuery {
                tx_id,
                query_id,
                source,
            } => session.subscribe_windowed_query(tx_id, query_id, source),
            SubscriptionReady::Query {
                tx_id,
                query_id,
                cellmap,
                window,
            } => session.subscribe_query(tx_id, query_id, cellmap, window),
            SubscriptionReady::View {
                tx_id,
                view_id,
                cellmap,
                window,
            } => session.subscribe_erased_view(tx_id, view_id, cellmap, window),
        }
    }
}

fn server() -> Arc<MykoServerContext> {
    let schema = PartitionSchema::new()
        .lifecycle::<PtRuntime>()
        .command_in(|_, _, payload| {
            payload
                .get("partition")
                .and_then(serde_json::Value::as_str)
                .filter(|key| !key.is_empty())
                .map(PartitionKey::new)
        })
        .subscribe_in(&[], |_, _, payload| {
            payload
                .get("partition")
                .and_then(serde_json::Value::as_str)
                .filter(|key| !key.is_empty())
                .map(PartitionKey::new)
        });
    let server = crate::MykoServer::builder().with_partitions(schema).build();
    let ctx = Arc::new(server.ctx());
    for partition in ["a", "b"] {
        ctx.set(&PtRuntime {
            id: partition.into(),
        })
        .unwrap_or_else(|error| panic!("open {partition}: {error}"));
    }
    ctx
}

fn reading(partition: &str, level: i64) -> PtReading {
    PtReading {
        id: format!("{partition}-row").into(),
        partition: partition.into(),
        level,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_of_one_partition_is_delivered_while_another_partition_is_parked() {
    let _serial = SERIAL.lock().await;
    let ctx = server();
    for partition in ["a", "b"] {
        ctx.set(&reading(partition, 1))
            .unwrap_or_else(|error| panic!("reading: {error}"));
    }
    let mut client_a = Client::new("client-a");
    let mut client_b = Client::new("client-b");
    client_a.subscribe_levels(&ctx, "a");
    client_a.register().await;
    client_b.subscribe_levels(&ctx, "b");
    client_b.register().await;
    client_a.wait_level(1);
    client_b.wait_level(1);

    let a = ctx
        .partition(&PartitionKey::new("a"))
        .unwrap_or_else(|| panic!("partition a"));
    let gate = arm(a.domain());
    let writer = {
        let ctx = Arc::clone(&ctx);
        thread::spawn(move || ctx.set(&reading("a", 2)))
    };
    assert!(gate.0.wait_entered(), "partition a never parked");

    ctx.set(&reading("b", 5))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    client_b.wait_level(5);
    assert!(gate.0.is_parked(), "partition a was not busy throughout");

    drop(gate);
    assert!(writer.join().is_ok_and(|written| written.is_ok()));
    client_a.wait_level(2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_names_no_open_partition_is_refused_to_the_client() {
    let _serial = SERIAL.lock().await;
    let ctx = server();
    let mut client = Client::new("client");

    let query = wrap_query("readings".into(), &GetAllPtReadings {})
        .unwrap_or_else(|error| panic!("query: {error}"));
    client.send(&ctx, MykoMessage::Query(query));
    let refused = client.error().await;
    assert!(
        matches!(&refused, MykoMessage::QueryError(error) if error.message.contains("partitioned")),
        "a query over a partitioned type from root is refused: {refused:?}"
    );

    client.subscribe_levels(&ctx, "");
    client.register().await;
    let refused = client.error().await;
    assert!(
        matches!(&refused, MykoMessage::ReportError(error) if error.message.contains("partitioned")),
        "a report in root that reads a partitioned type is refused: {refused:?}"
    );

    client.subscribe_levels(&ctx, "missing");
    let refused = client.error().await;
    assert!(
        matches!(&refused, MykoMessage::ReportError(error) if error.message.contains("not open")),
        "a report in a partition that is not open is refused: {refused:?}"
    );

    let shared = wrap_query("runtimes".into(), &GetAllPtRuntimes {})
        .unwrap_or_else(|error| panic!("query: {error}"));
    client.send(&ctx, MykoMessage::Query(shared));
    client.register().await;
    assert!(
        client.priority_rx.try_recv().is_err(),
        "a query over a shared type opens in root"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_view_over_a_partitioned_type_delivers_only_its_partitions_rows() {
    let _serial = SERIAL.lock().await;
    let ctx = server();
    ctx.set(&reading("a", 1))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("b", 5))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    let mut client = Client::new("client");

    client.subscribe_readings(&ctx, "a");
    client.register().await;
    let first = client.wait_reading(1);
    ctx.set(&reading("b", 6))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("a", 2))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    let later = client.wait_reading(2);
    let partitions: Vec<&str> = first
        .iter()
        .chain(&later)
        .map(|(key, _)| key.as_ref())
        .collect();
    assert!(
        partitions.iter().all(|key| *key == "a"),
        "the view delivered rows of another partition: {first:?} {later:?}"
    );

    client.subscribe_readings(&ctx, "");
    let refused = client.error().await;
    assert!(
        matches!(&refused, MykoMessage::ViewError(error) if error.message.contains("partitioned")),
        "a view in root over a partitioned type is refused: {refused:?}"
    );
}

// Startup and the log ---------------------------------------------------------

fn partitioned_server() -> crate::MykoServer {
    crate::MykoServer::builder()
        .with_partitions(PartitionSchema::new().lifecycle::<PtRuntime>())
        .build()
}

fn logged_by(item: &impl myko::prelude::Eventable, host: &str) -> myko::wire::MEvent {
    let mut event = myko::wire::MEvent::from_item(item, myko::wire::MEventType::SET, "logged");
    event.source_id = Some(host.into());
    event
}

fn logged(item: &impl myko::prelude::Eventable) -> myko::wire::MEvent {
    logged_by(item, "another-host")
}

fn reading_in(ctx: &MykoServerContext, partition: &str) -> Option<i64> {
    let ctx = ctx.partition(&PartitionKey::new(partition))?;
    let row = ctx
        .registry
        .get_or_create(<PtReading as myko::prelude::Eventable>::ENTITY_NAME_STATIC)
        .get_value(&format!("{partition}-row").into())?;
    AnyItem::as_any(row.as_ref())
        .downcast_ref::<PtReading>()
        .map(|reading| reading.level)
}

#[test]
fn a_hydrated_snapshot_places_each_partitioned_row_in_its_partition() {
    let _serial = SERIAL.blocking_lock();
    let server = partitioned_server();
    // A partitioned row before the shared row that opens its partition.
    let loaded = server.hydrate_snapshot([
        logged(&reading("a", 1)),
        logged(&PtRuntime { id: "a".into() }),
        logged(&PtRuntime { id: "b".into() }),
        logged(&reading("b", 2)),
    ]);
    assert_eq!(loaded, Ok(4));
    let ctx = server.ctx();
    assert_eq!(reading_in(&ctx, "a"), Some(1));
    assert_eq!(reading_in(&ctx, "b"), Some(2));
    assert!(
        server
            .registry
            .get(<PtReading as myko::prelude::Eventable>::ENTITY_NAME_STATIC)
            .is_none_or(|store| store.snapshot().is_empty()),
        "root holds no partitioned row"
    );
}

#[test]
fn a_tailed_event_lands_in_its_partition() {
    let _serial = SERIAL.blocking_lock();
    let server = partitioned_server();
    let ctx = server.ctx();
    ctx.set(&PtRuntime { id: "a".into() })
        .unwrap_or_else(|error| panic!("open a: {error}"));
    let landing = crate::postgres::Landing::Context(ctx.clone());
    let handlers = server.handler_registry();
    let host = server.host_id.to_string();
    landing.apply_remote(&logged(&reading("a", 5)), &host, &handlers);
    assert_eq!(reading_in(&ctx, "a"), Some(5));
    landing.apply_remote(&logged_by(&reading("a", 6), &host), &host, &handlers);
    assert_eq!(
        reading_in(&ctx, "a"),
        Some(5),
        "this host's own event is skipped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_view_of_rows_never_stored_opens_where_the_hook_places_it() {
    let _serial = SERIAL.lock().await;
    let ctx = server();
    ctx.set(&reading("a", 1))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("b", 5))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    let mut client = Client::new("client");
    let view = wrap_view(
        "summaries".into(),
        &PtSummaryView {
            partition: "b".into(),
        },
    )
    .unwrap_or_else(|error| panic!("view: {error}"));
    client.send(&ctx, MykoMessage::View(view));
    client.register().await;
    assert!(
        client.priority_rx.try_recv().is_err(),
        "the view opened in its partition, not refused in root"
    );
    let levels: Vec<i64> = loop {
        match client.received.recv_timeout(DEADLINE) {
            Ok(MykoMessage::ViewResponse(response)) => {
                break response
                    .upserts
                    .iter()
                    .filter_map(|upsert| {
                        AnyItem::as_any(upsert.item.as_ref())
                            .downcast_ref::<PtSummary>()
                            .map(|row| row.level)
                    })
                    .collect();
            }
            Ok(_) => {}
            Err(error) => panic!("no view response ({error})"),
        }
    };
    assert_eq!(levels, [5], "b's summaries, and only b's");
}

fn seated_server() -> Arc<MykoServerContext> {
    let schema = PartitionSchema::new()
        .lifecycle::<PtRuntime>()
        .subscribe_in(
            &[<PtSeat as myko::prelude::Eventable>::ENTITY_NAME_STATIC],
            |root, _, payload| {
                let seat = payload.get("seat")?.as_str()?;
                let row = root
                    .registry
                    .get_or_create(<PtSeat as myko::prelude::Eventable>::ENTITY_NAME_STATIC)
                    .get_value(&Arc::from(seat))?;
                AnyItem::as_any(row.as_ref())
                    .downcast_ref::<PtSeat>()
                    .map(|seat| PartitionKey::new(seat.partition.clone()))
            },
        );
    let server = crate::MykoServer::builder().with_partitions(schema).build();
    let ctx = Arc::new(server.ctx());
    for partition in ["a", "b"] {
        ctx.set(&PtRuntime {
            id: partition.into(),
        })
        .unwrap_or_else(|error| panic!("open {partition}: {error}"));
    }
    ctx
}

fn seat(partition: &str) -> PtSeat {
    PtSeat {
        id: "s1".into(),
        partition: partition.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_moves_when_the_shared_data_placing_it_changes() {
    let _serial = SERIAL.lock().await;
    let ctx = seated_server();
    ctx.set(&seat("a"))
        .unwrap_or_else(|error| panic!("seat: {error}"));
    ctx.set(&reading("a", 1))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("b", 5))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    let mut client = Client::new("client");
    let view = wrap_view("seated".into(), &PtSeatView { seat: "s1".into() })
        .unwrap_or_else(|error| panic!("view: {error}"));
    client.send(&ctx, MykoMessage::View(view));
    client.register().await;
    assert_eq!(client.next_view(), (0, vec![("a".into(), 1)]));

    ctx.set(&seat("b"))
        .unwrap_or_else(|error| panic!("seat: {error}"));
    client.follow_move(&ctx).await;
    client.register().await;
    assert_eq!(
        client.next_view(),
        (0, vec![("b".into(), 5)]),
        "an initial response of b replaces what the client held"
    );
    ctx.set(&reading("a", 2))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("b", 6))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    assert_eq!(
        client.next_view(),
        (1, vec![("b".into(), 6)]),
        "nothing of a follows the move"
    );

    ctx.set(&seat("b"))
        .unwrap_or_else(|error| panic!("seat: {error}"));
    assert!(
        client.moved_rx.try_recv().is_err(),
        "an unchanged answer moves nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_runs_in_the_partition_the_hook_names() {
    let _serial = SERIAL.lock().await;
    let ctx = server();
    ctx.set(&reading("a", 1))
        .unwrap_or_else(|error| panic!("reading: {error}"));
    ctx.set(&reading("b", 5))
        .unwrap_or_else(|error| panic!("reading: {error}"));

    let level = |partition: &str| {
        run_command(
            &ctx,
            &PtLevelHere {
                partition: partition.into(),
            },
            "PtLevelHere",
        )
    };
    assert!(
        matches!(level("b"), MykoMessage::CommandResponse(response) if response.response == 5),
        "the command read b"
    );
    let refused = level("");
    assert!(
        matches!(&refused, MykoMessage::CommandError(error) if error.message.contains("partitioned")),
        "a root read of a partitioned type is an error, not empty: {refused:?}"
    );
    let missing = level("missing");
    assert!(
        matches!(&missing, MykoMessage::CommandError(error) if error.message.contains("not open")),
        "{missing:?}"
    );
    let handle = run_command(&ctx, &PtLevelIn { key: "a".into() }, "PtLevelIn");
    assert!(
        matches!(&handle, MykoMessage::CommandResponse(response) if response.response == 1),
        "a root command reads a partition through a handle: {handle:?}"
    );
}
