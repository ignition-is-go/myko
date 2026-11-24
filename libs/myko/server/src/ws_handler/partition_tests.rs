//! Client subscriptions on a partitioned server: each opens in the partition
//! its request names and is delivered from there, so a partition that is
//! busy delays no client of another.

#![allow(clippy::panic)]

use std::sync::{Condvar, Mutex as StdMutex, mpsc as std_mpsc};

use myko::{
    hyphae::{Definite, MapExt, Materialize, scheduler::Domain},
    prelude::Querying,
    report::{ReportContext, ReportHandler},
    server::{PartitionKey, PartitionSchema},
    wire::{wrap_query, wrap_report},
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

use reading::{GetAllPtReadings, PtReading};
use runtime::{GetAllPtRuntimes, PtRuntime};

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
}

impl Client {
    fn new(name: &str) -> Self {
        let (sender, received) = std_mpsc::channel();
        let (built_tx, built_rx) = mpsc::unbounded_channel();
        let (priority_tx, priority_rx) = mpsc::channel(16);
        Self {
            session: ClientSession::new(name.into(), Socket(Arc::new(StdMutex::new(sender)))),
            received,
            builds: SubscriptionBuilds::default(),
            built_tx,
            built_rx,
            priority_tx,
            priority_rx,
            logger: Arc::new(DropLogger::new(name.into(), watch::channel(false).0)),
        }
    }

    fn send(&mut self, ctx: &Arc<MykoServerContext>, message: MykoMessage) {
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
        };
        WsHandler::handle_message(&mut self.session, ctx.clone(), &context, message);
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
        .subscribe_in(|_, payload| {
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
