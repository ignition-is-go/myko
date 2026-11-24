//! WebSocket handler for the cell-based server.
//!
//! Handles WebSocket connections using `ClientSession` for subscription management.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::outbound_queue::{self, Pushed};
use futures_util::{SinkExt, StreamExt};
use hyphae::SelectExt;
use myko::{
    WS_MAX_FRAME_SIZE_BYTES, WS_MAX_MESSAGE_SIZE_BYTES,
    client::MykoProtocol,
    command::{CommandContext, CommandHandlerRegistration},
    entities::client::{Client, ClientId},
    query::WindowedQuerySource,
    relationship::{
        iter_client_id_registrations, iter_fallback_to_id_registrations,
        iter_server_owned_registrations,
    },
    report::AnyOutput,
    request::RequestContext,
    server::{
        ClientSession, CommandValueKey, MykoServerContext, PendingQueryResponse, WsWriter,
        client_registry::try_client_registry,
    },
    wire::{
        CancelSubscription, CommandError, CommandResponse, EncodedCommandMessage, MEvent,
        MEventType, MykoMessage, QueryCursorWindowUpdate, QueryWindow, QueryWindowUpdate,
        ViewError, ViewWindowUpdate, WrappedQuery, WrappedView,
    },
};
use myko_auth::{AuthSession, AuthorizationRequest, EventChange, OperationKind};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::interval,
};
use tokio_tungstenite::{
    accept_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message, error::ProtocolError, protocol::WebSocketConfig,
    },
};
use uuid::Uuid;

use crate::{
    GatewayAccess,
    blocking_work::{WS_CLEANUP_BLOCKING, WS_COMMAND_BLOCKING, WS_SUBSCRIPTION_BLOCKING},
    delivery_diagnostics::DeliveryDiagnostics,
    subscription_authorization::{Delivery, SubscriptionAuthorization},
    view_sampling::{ViewDelivery, ViewDeliveryControl},
};

mod subscription_builds;
use subscription_builds::{SubscriptionBuilds, SubscriptionBuilt};

struct WsBenchmarkStats {
    message_count: AtomicU64,
    total_bytes: AtomicU64,
}

static WS_BENCHMARK_STATS: OnceLock<Arc<WsBenchmarkStats>> = OnceLock::new();
static WS_BENCHMARK_LOGGER_STARTED: AtomicBool = AtomicBool::new(false);

fn is_routine_websocket_disconnect(error: &WebSocketError) -> bool {
    match error {
        WebSocketError::ConnectionClosed
        | WebSocketError::AlreadyClosed
        | WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WebSocketError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

fn ws_benchmark_stats() -> Arc<WsBenchmarkStats> {
    WS_BENCHMARK_STATS
        .get_or_init(|| {
            Arc::new(WsBenchmarkStats {
                message_count: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
            })
        })
        .clone()
}

fn ensure_ws_benchmark_logger() {
    if WS_BENCHMARK_LOGGER_STARTED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let stats = ws_benchmark_stats();
    if let Err(error) = thread::Builder::new()
        .name("ws-benchmark-logger".into())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));

                let count = stats.message_count.swap(0, Ordering::Relaxed);
                let bytes = stats.total_bytes.swap(0, Ordering::Relaxed);

                if count == 0 {
                    continue;
                }

                tracing::info!(
                    "WebSocket benchmark last_1s messages={} bytes={} avg_bytes={}",
                    count,
                    bytes,
                    bytes.checked_div(count).unwrap_or_default()
                );
            }
        })
    {
        WS_BENCHMARK_LOGGER_STARTED.store(false, Ordering::Relaxed);
        tracing::error!(%error, "failed to spawn websocket benchmark logger thread");
    }
}

fn normalize_incoming_event(event: &mut MEvent, client_id: &str, host_id: uuid::Uuid) {
    if event.change_type != MEventType::SET {
        return;
    }

    // Auto-populate #[myko_client_id] fields with the connection's client_id
    for reg in iter_client_id_registrations() {
        if reg.entity_type == &*event.item_type {
            if let Some(obj) = event.item.as_object_mut() {
                obj.insert(
                    reg.field_name_json.to_string(),
                    serde_json::Value::String(client_id.to_string()),
                );
            }
            break;
        }
    }

    // Auto-populate #[server_owned] fields with this server's ID
    for reg in iter_server_owned_registrations() {
        if reg.entity_type == &*event.item_type {
            if let Some(obj) = event.item.as_object_mut() {
                let field = reg.field_name_json;
                let current = obj.get(field).and_then(|v| v.as_str()).unwrap_or("");
                if current.is_empty() {
                    obj.insert(
                        field.to_string(),
                        serde_json::Value::String(host_id.to_string()),
                    );
                }
            }
            break;
        }
    }

    // Auto-populate #[fallback_to_id] fields with the entity's own id
    // if the field is null or missing.
    if let Some(obj) = event.item.as_object_mut()
        && let Some(id) = obj.get("id").and_then(|v| v.as_str()).map(String::from)
    {
        for reg in iter_fallback_to_id_registrations() {
            if reg.entity_type == &*event.item_type {
                let field = reg.field_name_json;
                if matches!(obj.get(field), None | Some(serde_json::Value::Null)) {
                    obj.insert(field.to_string(), serde_json::Value::String(id.clone()));
                }
            }
        }
    }
}

/// Sequenced `Message` frames a connection may have queued before it is torn
/// down for a clean resnapshot.
const MESSAGE_QUEUE_CAPACITY: usize = 10_000;

static OUTBOUND_DROPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static OVERLOAD_DISCONNECTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SUPERSEDED_VALUE_COMMANDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Value commands replaced in a client's queue by a newer value for the same
/// key before they were written, since the process started.
#[must_use]
pub fn ws_superseded_value_commands() -> u64 {
    SUPERSEDED_VALUE_COMMANDS.load(std::sync::atomic::Ordering::Relaxed)
}

static TEARDOWN_DISCARDED_EVENT_COMMANDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static TEARDOWN_DISCARDED_VALUE_COMMANDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Event commands still queued for a client when its connection closed, and
/// so never written, since the process started.
#[must_use]
pub fn ws_teardown_discarded_event_commands() -> u64 {
    TEARDOWN_DISCARDED_EVENT_COMMANDS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Value commands still queued for a client when its connection closed, and
/// so never written, since the process started.
#[must_use]
pub fn ws_teardown_discarded_value_commands() -> u64 {
    TEARDOWN_DISCARDED_VALUE_COMMANDS.load(std::sync::atomic::Ordering::Relaxed)
}

/// What the server gave up delivering, since the process started.
///
/// Read it before and after a load test: a client that was disconnected for
/// overload reconnects and resubscribes, so the test itself may not notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WsDeliveryCounters {
    /// Outbound frames that could not be queued for a client.
    pub outbound_drops: u64,
    /// Connections closed because a frame for them was dropped or could not
    /// be written.
    pub overload_disconnects: u64,
}

/// Process-wide WebSocket delivery failure counts.
#[must_use]
pub fn ws_delivery_counters() -> WsDeliveryCounters {
    use std::sync::atomic::Ordering;
    WsDeliveryCounters {
        outbound_drops: OUTBOUND_DROPS.load(Ordering::Relaxed),
        overload_disconnects: OVERLOAD_DISCONNECTS.load(Ordering::Relaxed),
    }
}

/// Per-connection delivery-failure tracking without log storms.
///
/// A bounded channel can reject a frame when full or during teardown. Either
/// way, the session must stop before it can emit frames after the missing one.
struct DropLogger {
    client_id: Arc<str>,
    dropped: std::sync::atomic::AtomicU64,
    last_log_ms: std::sync::atomic::AtomicU64,
    overload_tx: watch::Sender<bool>,
    diagnostics: Option<Arc<DeliveryDiagnostics>>,
}

impl DropLogger {
    const fn new(client_id: Arc<str>, overload_tx: watch::Sender<bool>) -> Self {
        Self {
            client_id,
            dropped: std::sync::atomic::AtomicU64::new(0),
            last_log_ms: std::sync::atomic::AtomicU64::new(0),
            overload_tx,
            diagnostics: None,
        }
    }

    fn subscription_ready(&self, tx: &str) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.ready(tx);
        }
    }

    /// Ask the read loop to close the connection, counting it once.
    fn disconnect_for_overload(&self) {
        if !self.overload_tx.send_replace(true) {
            OVERLOAD_DISCONNECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn on_drop(&self, kind: &'static str, err: &dyn std::fmt::Display) {
        use std::sync::atomic::Ordering;

        self.dropped.fetch_add(1, Ordering::Relaxed);
        OUTBOUND_DROPS.fetch_add(1, Ordering::Relaxed);

        // A dropped sequenced diff cannot be repaired while keeping this
        // session alive: every later sequence would be based on state the
        // client never received. Wake the read loop so it tears down the
        // connection; reconnecting clients then resubscribe and receive a
        // fresh sequence-0 snapshot. `send_replace` is synchronous, so this
        // remains safe to call from reactive callbacks.
        self.disconnect_for_overload();

        // Log at most once per second per connection.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let last_ms = self.last_log_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_ms) < 1000 {
            return;
        }

        if self
            .last_log_ms
            .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let n = self.dropped.swap(0, Ordering::Relaxed);
        tracing::warn!(
            "WebSocket outbound delivery failed {} time(s) for client {} (latest: {}): {}",
            n,
            self.client_id,
            kind,
            err
        );
    }
}

impl Drop for DropLogger {
    fn drop(&mut self) {
        myko_telemetry::websocket_metrics::client_disconnected(&self.client_id);
    }
}

struct CommandJob {
    auth: Option<AuthSession>,
    tx_id: Arc<str>,
    command_id: String,
    command: serde_json::Value,
    received_at: Instant,
}

struct QueryWindowJob {
    tx_id: Arc<str>,
    source: WindowedQuerySource,
    window: Option<QueryWindow>,
}

/// Result of an async subscription build (`cell_factory`).
enum SubscriptionReady {
    Report {
        tx_id: Arc<str>,
        report_id: Arc<str>,
        cell: hyphae::Cell<Arc<dyn AnyOutput>, hyphae::CellImmutable>,
    },
    WindowedQuery {
        tx_id: Arc<str>,
        query_id: Arc<str>,
        source: myko::query::WindowedQuerySource,
    },
    Query {
        tx_id: Arc<str>,
        query_id: Arc<str>,
        cellmap: hyphae::CellMap<Arc<str>, Arc<dyn myko::item::AnyItem>, hyphae::CellImmutable>,
        window: Option<myko::wire::QueryWindow>,
    },
    View {
        tx_id: Arc<str>,
        view_id: Arc<str>,
        cellmap: myko::view::ErasedViewMap,
        window: Option<myko::wire::QueryWindow>,
    },
}

type SharedTxNames = Arc<Mutex<HashMap<Arc<str>, Arc<str>>>>;
type SharedTxTimes = Arc<Mutex<HashMap<Arc<str>, Instant>>>;
type SharedOutboundCommands = Arc<Mutex<HashMap<String, (String, Instant)>>>;

/// The live subscriptions whose partition follows shared data, by tx. One
/// whose partition changed is handed to the read loop through `moved`, with
/// the request that opened it, and opened again where it now belongs.
#[derive(Clone)]
struct SubscriptionPlacements {
    watches: Arc<Mutex<HashMap<Arc<str>, myko::server::SubscriptionWatch>>>,
    moved: mpsc::UnboundedSender<(Arc<str>, MykoMessage)>,
}

type MovedSubscriptions = mpsc::UnboundedReceiver<(Arc<str>, MykoMessage)>;

impl SubscriptionPlacements {
    fn new() -> (Self, MovedSubscriptions) {
        let (moved, moved_rx) = mpsc::unbounded_channel();
        (
            Self {
                watches: Arc::default(),
                moved,
            },
            moved_rx,
        )
    }

    /// Follow the subscription `tx`, opened in `opened` from `root` by
    /// `request`, which `resend` makes again.
    fn follow(
        &self,
        root: &MykoServerContext,
        opened: &MykoServerContext,
        (tx, request, payload): (&Arc<str>, &Arc<RequestContext>, &serde_json::Value),
        resend: MykoMessage,
    ) {
        let moved = self.moved.clone();
        let moved_tx = Arc::clone(tx);
        let watch = root.watch_subscription(
            Arc::clone(request),
            payload.clone(),
            opened.partition_key().cloned(),
            move || drop(moved.send((Arc::clone(&moved_tx), resend.clone()))),
        );
        if let (Some(watch), Ok(mut watches)) = (watch, self.watches.lock()) {
            watches.insert(Arc::clone(tx), watch);
        }
    }

    fn forget(&self, tx: &str) {
        if let Ok(mut watches) = self.watches.lock() {
            watches.remove(tx);
        }
    }
}

struct MessageContext<'a> {
    auth: &'a Option<AuthSession>,
    priority_tx: &'a mpsc::Sender<MykoMessage>,
    drop_logger: &'a Arc<DropLogger>,
    query_ids_by_tx: &'a SharedTxNames,
    view_ids_by_tx: &'a SharedTxNames,
    subscribe_started_by_tx: &'a SharedTxTimes,
    command_started_by_tx: &'a SharedTxTimes,
    outbound_commands_by_tx: &'a SharedOutboundCommands,
    command_tx: &'a mpsc::UnboundedSender<CommandJob>,
    query_window_tx: &'a mpsc::UnboundedSender<QueryWindowJob>,
    subscribe_tx: &'a mpsc::UnboundedSender<SubscriptionBuilt>,
    subscription_builds: &'a SubscriptionBuilds,
    view_delivery_tx: &'a mpsc::UnboundedSender<ViewDeliveryControl>,
    placements: &'a SubscriptionPlacements,
}

struct ReadLoopState {
    subscription_auth: Arc<SubscriptionAuthorization>,
    auth: Option<AuthSession>,
    access: GatewayAccess,
    ctx: Arc<MykoServerContext>,
    client_id: Arc<str>,
    outgoing_format: Arc<AtomicU8>,
    priority_tx: mpsc::Sender<MykoMessage>,
    drop_logger: Arc<DropLogger>,
    query_ids_by_tx: SharedTxNames,
    view_ids_by_tx: SharedTxNames,
    subscribe_started_by_tx: SharedTxTimes,
    command_started_by_tx: SharedTxTimes,
    outbound_commands_by_tx: SharedOutboundCommands,
    command_tx: mpsc::UnboundedSender<CommandJob>,
    query_window_tx: mpsc::UnboundedSender<QueryWindowJob>,
    subscribe_tx: mpsc::UnboundedSender<SubscriptionBuilt>,
    subscription_builds: SubscriptionBuilds,
    view_delivery_tx: mpsc::UnboundedSender<ViewDeliveryControl>,
    placements: SubscriptionPlacements,
    overload_rx: watch::Receiver<bool>,
}

struct WriterState {
    subscription_auth: Arc<SubscriptionAuthorization>,
    auth: Option<AuthSession>,
    write: futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    rx: OutboundReceiver,
    deferred_rx: mpsc::Receiver<DeferredOutbound>,
    view_delivery_rx: mpsc::UnboundedReceiver<ViewDeliveryControl>,
    priority_rx: mpsc::Receiver<MykoMessage>,
    ctx: Arc<MykoServerContext>,
    client_id: Arc<str>,
    addr: SocketAddr,
    outgoing_format: Arc<AtomicU8>,
    outbound_commands: SharedOutboundCommands,
    drop_logger: Arc<DropLogger>,
    diagnostics: Arc<DeliveryDiagnostics>,
}

enum OutboundMessage {
    Message(MykoMessage),
    SerializedCommand {
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    },
}

/// Writer half of the ordered outbound queue; releases a `Message` frame's
/// slot in the shared bound as the writer takes it.
struct OutboundReceiver {
    rx: outbound_queue::Receiver<OutboundMessage>,
    queued_messages: Arc<AtomicUsize>,
    diagnostics: Option<Arc<DeliveryDiagnostics>>,
}

/// Runs however the writer ends, including when its task is aborted.
impl Drop for OutboundReceiver {
    fn drop(&mut self) {
        let discarded = self.rx.close();
        if discarded == outbound_queue::Discarded::default() {
            return;
        }
        TEARDOWN_DISCARDED_EVENT_COMMANDS.fetch_add(discarded.events, Ordering::Relaxed);
        TEARDOWN_DISCARDED_VALUE_COMMANDS.fetch_add(discarded.values, Ordering::Relaxed);
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.teardown_discarded(discarded.events, discarded.values);
        }
    }
}

impl OutboundReceiver {
    async fn recv(&self) -> Option<OutboundMessage> {
        let message = self.rx.recv().await?;
        if matches!(message, OutboundMessage::Message(_)) {
            self.queued_messages.fetch_sub(1, Ordering::AcqRel);
        }
        Some(message)
    }

    fn len(&self) -> usize {
        self.rx.len()
    }
}

enum DeferredOutbound {
    Report(Arc<str>, Arc<dyn AnyOutput>, Instant),
    Query {
        response: PendingQueryResponse,
        is_view: bool,
        queued_at: Instant,
    },
    /// Successful command completion is deliberately serialized behind the
    /// query/report updates emitted by that command. Reactive publication is
    /// synchronous, so sharing this FIFO gives clients read-your-writes when
    /// the command promise resolves without changing the wire protocol.
    CommandResult(MykoMessage),
}

struct ConnectionGuard {
    ctx: Arc<MykoServerContext>,
    client_entity: Client,
    client_id: Arc<str>,
    addr: SocketAddr,
    tasks: [tokio::task::JoinHandle<()>; 3],
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        if let Some(registry) = try_client_registry() {
            registry.unregister(&self.client_id);
        }
        if let Err(error) = self.ctx.del(&self.client_entity) {
            tracing::error!("Failed to delete client entity: {error}");
        }
        tracing::info!("Client disconnected: {} from {}", self.client_id, self.addr);
    }
}

/// WebSocket handler for a single client connection.
pub struct WsHandler;

impl WsHandler {
    fn publish_client(ctx: &MykoServerContext, client_id: Arc<str>, addr: SocketAddr) -> Client {
        let client = Client {
            id: ClientId(client_id.clone()),
            server_id: ctx.host_id.to_string().into(),
            address: Some(Arc::from(addr.to_string())),
            windback: None,
        };
        if let Err(error) = ctx.set(&client) {
            tracing::error!("Failed to persist client entity: {error}");
        }
        tracing::info!("Client connected: {} from {}", client_id, addr);
        drop(client_id);
        client
    }

    fn authorize_incoming(
        auth: &AuthSession,
        message: &mut MykoMessage,
        client: &str,
        host: uuid::Uuid,
    ) -> Result<(), myko_auth::AuthError> {
        auth.check()?;
        let request = match message {
            MykoMessage::Query(w) => AuthorizationRequest::Execute {
                kind: OperationKind::Query,
                operation: &w.query_id,
                arguments: &w.query,
            },
            MykoMessage::View(w) => AuthorizationRequest::Execute {
                kind: OperationKind::View,
                operation: &w.view_id,
                arguments: &w.view,
            },
            MykoMessage::Report(w) => AuthorizationRequest::Execute {
                kind: OperationKind::Report,
                operation: &w.report_id,
                arguments: &w.report,
            },
            MykoMessage::Command(w) => AuthorizationRequest::Execute {
                kind: OperationKind::Command,
                operation: &w.command_id,
                arguments: &w.command,
            },
            MykoMessage::Event(event) => return Self::authorize_event(auth, event, client, host),
            MykoMessage::EventBatch(events) => {
                for event in events {
                    Self::authorize_event(auth, event, client, host)?;
                }
                return auth.check();
            }
            MykoMessage::CommandResponse(w) => {
                AuthorizationRequest::CommandResult { transaction: &w.tx }
            }
            MykoMessage::CommandError(w) => {
                AuthorizationRequest::CommandResult { transaction: &w.tx }
            }
            MykoMessage::Ping(_)
            | MykoMessage::QueryCancel(_)
            | MykoMessage::QueryWindow(_)
            | MykoMessage::QueryCursorWindow(_)
            | MykoMessage::ViewCancel(_)
            | MykoMessage::ViewWindow(_)
            | MykoMessage::ViewSampleRate(_)
            | MykoMessage::ReportCancel(_) => return Ok(()),
            _ => return Err(myko_auth::AuthError::Forbidden),
        };
        auth.authorize(request)
    }

    fn authorize_event(
        auth: &AuthSession,
        event: &mut MEvent,
        client: &str,
        host: uuid::Uuid,
    ) -> Result<(), myko_auth::AuthError> {
        event.sanitize_null_bytes();
        normalize_incoming_event(event, client, host);
        auth.authorize(AuthorizationRequest::ClientEvent {
            entity_type: &event.item_type,
            change: if event.change_type == MEventType::SET {
                EventChange::Set
            } else {
                EventChange::Delete
            },
            proposed: &event.item,
        })
    }

    fn dispatch_incoming<W: WsWriter>(
        session: &mut ClientSession<W>,
        state: &ReadLoopState,
        mut message: MykoMessage,
    ) -> bool {
        if !state.access.allows(&message) {
            Self::reject_incoming(state, &message, "gateway is read-only".into());
            return true;
        }
        if let Some(auth) = &state.auth {
            // An invalid session terminates the transport; a denied operation does not.
            if auth.check().is_err() {
                return false;
            }
            if let Err(error) =
                Self::authorize_incoming(auth, &mut message, &session.client_id, state.ctx.host_id)
            {
                if auth.check().is_err() {
                    return false;
                }
                Self::reject_incoming(state, &message, error.to_string());
                return true;
            }
        }
        // Entity declarations apply even to trusted gateways without an auth provider.
        let mutability = match &mut message {
            MykoMessage::Event(event) => {
                event.sanitize_null_bytes();
                myko_auth::client_events::authorize(
                    &event.item_type,
                    if event.change_type == MEventType::SET {
                        EventChange::Set
                    } else {
                        EventChange::Delete
                    },
                )
            }
            MykoMessage::EventBatch(events) => events.iter_mut().try_for_each(|event| {
                event.sanitize_null_bytes();
                myko_auth::client_events::authorize(
                    &event.item_type,
                    if event.change_type == MEventType::SET {
                        EventChange::Set
                    } else {
                        EventChange::Delete
                    },
                )
            }),
            _ => Ok(()),
        };
        if let Err(error) = mutability {
            Self::reject_incoming(state, &message, error.to_string());
            return true;
        }
        if state
            .auth
            .as_ref()
            .is_some_and(AuthSession::uses_live_permissions)
        {
            match &message {
                MykoMessage::QueryCancel(w)
                | MykoMessage::ViewCancel(w)
                | MykoMessage::ReportCancel(w) => state.subscription_auth.cancel(&w.tx),
                _ => {}
            }
            if !state.subscription_auth.register(&message) {
                Self::reject_incoming(
                    state,
                    &message,
                    "subscription transaction is no longer active; use a fresh transaction".into(),
                );
                return true;
            }
        }
        Self::handle_message(
            session,
            state.ctx.clone(),
            &Self::message_context(state),
            message,
        );
        true
    }

    const fn message_context(state: &ReadLoopState) -> MessageContext<'_> {
        MessageContext {
            auth: &state.auth,
            priority_tx: &state.priority_tx,
            drop_logger: &state.drop_logger,
            query_ids_by_tx: &state.query_ids_by_tx,
            view_ids_by_tx: &state.view_ids_by_tx,
            subscribe_started_by_tx: &state.subscribe_started_by_tx,
            command_started_by_tx: &state.command_started_by_tx,
            outbound_commands_by_tx: &state.outbound_commands_by_tx,
            command_tx: &state.command_tx,
            query_window_tx: &state.query_window_tx,
            subscribe_tx: &state.subscribe_tx,
            subscription_builds: &state.subscription_builds,
            view_delivery_tx: &state.view_delivery_tx,
            placements: &state.placements,
        }
    }

    fn drop_denied<W: WsWriter>(
        session: &mut ClientSession<W>,
        state: &ReadLoopState,
        tx: &Arc<str>,
    ) {
        state.placements.forget(tx);
        state.subscription_builds.cancel(tx);
        session.cancel(tx);
        let _ = state
            .view_delivery_tx
            .send(ViewDeliveryControl::Cancel(tx.clone()));
        if let Ok(mut map) = state.query_ids_by_tx.lock() {
            map.remove(tx);
        }
        if let Ok(mut map) = state.view_ids_by_tx.lock() {
            map.remove(tx);
        }
        if let Ok(mut map) = state.subscribe_started_by_tx.lock() {
            map.remove(tx);
        }
    }

    /// Open the subscription `tx` again where its partition now is. The
    /// client keeps what it holds until the new subscription's initial
    /// response replaces it.
    fn replace_subscription<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<MykoServerContext>,
        message_context: &MessageContext<'_>,
        (tx, message): (Arc<str>, MykoMessage),
    ) {
        message_context.placements.forget(&tx);
        message_context.subscription_builds.cancel(&tx);
        session.cancel(&tx);
        Self::handle_message(session, ctx, message_context, message);
    }

    fn reject_incoming(state: &ReadLoopState, message: &MykoMessage, reason: String) {
        let transaction = |value: &serde_json::Value| {
            value
                .get("tx")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned()
        };
        let response = match message {
            MykoMessage::Query(w) => MykoMessage::QueryError(myko::wire::QueryError::new(
                transaction(&w.query),
                w.query_id.to_string(),
                reason,
            )),
            MykoMessage::View(w) => MykoMessage::ViewError(ViewError::new(
                transaction(&w.view),
                w.view_id.to_string(),
                reason,
            )),
            MykoMessage::Report(w) => MykoMessage::ReportError(myko::wire::ReportError::new(
                transaction(&w.report),
                w.report_id.clone(),
                reason,
            )),
            MykoMessage::Command(w) => MykoMessage::CommandError(CommandError::new(
                transaction(&w.command),
                w.command_id.clone(),
                reason,
            )),
            // Entity events have no request/reply transaction. Reject the complete
            // batch before applying anything, while preserving other subscriptions.
            _ => {
                tracing::warn!(client_id = %state.client_id, %reason, "Rejected WebSocket input");
                return;
            }
        };
        if let Err(error) = state.priority_tx.try_send(response) {
            state.drop_logger.on_drop("AuthorizationError", &error);
        }
    }

    async fn handle_ws_frame<W: WsWriter>(
        session: &mut ClientSession<W>,
        state: &ReadLoopState,
        message: Message,
    ) -> bool {
        match message {
            Message::Binary(data) => {
                if state.outgoing_format.load(Ordering::SeqCst) != u8::from(MykoProtocol::CBOR) {
                    tracing::debug!(
                        "Client {} promoted outgoing format to CBOR via demonstration",
                        state.client_id
                    );
                    state
                        .outgoing_format
                        .store(MykoProtocol::CBOR.into(), Ordering::SeqCst);
                }
                match ciborium::de::from_reader::<MykoMessage, _>(data.as_ref()) {
                    Ok(message) => {
                        if !Self::dispatch_incoming(session, state, message) {
                            return false;
                        }
                        tokio::task::yield_now().await;
                    }
                    Err(error) => tracing::warn!(
                        "Failed to parse message from {}: {}",
                        state.client_id,
                        error
                    ),
                }
            }
            Message::Text(text) => match serde_json::from_str::<MykoMessage>(&text) {
                Ok(message) => {
                    if !Self::dispatch_incoming(session, state, message) {
                        return false;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => tracing::warn!(
                    "Failed to parse JSON message from {}: {} | raw: {}",
                    state.client_id,
                    error,
                    text.get(..1000).unwrap_or(&text)
                ),
            },
            Message::Ping(_) => tracing::trace!("Ping from {}", state.client_id),
            Message::Pong(_) => tracing::trace!("Pong from {}", state.client_id),
            Message::Close(frame) => {
                tracing::warn!("Client {} sent close frame: {:?}", state.client_id, frame);
                return false;
            }
            Message::Frame(_) => {}
        }
        true
    }

    fn register_ready_subscription<W: WsWriter>(
        session: &mut ClientSession<W>,
        ready: SubscriptionReady,
        state: &ReadLoopState,
    ) -> bool {
        if state
            .auth
            .as_ref()
            .is_some_and(|auth| auth.check().is_err())
        {
            return false;
        }
        let tx_id = match &ready {
            SubscriptionReady::Report { tx_id, .. }
            | SubscriptionReady::WindowedQuery { tx_id, .. }
            | SubscriptionReady::Query { tx_id, .. }
            | SubscriptionReady::View { tx_id, .. } => tx_id.clone(),
        };
        if state
            .auth
            .as_ref()
            .is_some_and(AuthSession::uses_live_permissions)
            && !state.subscription_auth.active(&tx_id)
        {
            return true;
        }
        if let Ok(mut map) = state.subscribe_started_by_tx.lock() {
            map.remove(&tx_id);
        }
        if let Some(diagnostics) = &state.drop_logger.diagnostics {
            diagnostics.ready(&tx_id);
        }
        match ready {
            SubscriptionReady::Report {
                tx_id,
                report_id,
                cell,
            } => {
                session.subscribe_report(tx_id, report_id, cell);
            }
            SubscriptionReady::WindowedQuery {
                tx_id,
                query_id,
                source,
            } => {
                session.subscribe_windowed_query(tx_id, query_id, source);
            }
            SubscriptionReady::Query {
                tx_id,
                query_id,
                cellmap,
                window,
            } => {
                session.subscribe_query(tx_id, query_id, cellmap, window);
            }
            SubscriptionReady::View {
                tx_id,
                view_id,
                cellmap,
                window,
            } => {
                session.subscribe_erased_view(tx_id, view_id, cellmap, window);
            }
        }
        true
    }

    async fn run_read_loop<W: WsWriter>(
        mut read: futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<TcpStream>>,
        session: &mut ClientSession<W>,
        mut subscribe_rx: mpsc::UnboundedReceiver<SubscriptionBuilt>,
        mut denied_rx: mpsc::UnboundedReceiver<Arc<str>>,
        mut moved_rx: MovedSubscriptions,
        state: ReadLoopState,
    ) {
        let mut diagnostics_interval = interval(Duration::from_secs(5));
        let mut outbound_ttl_interval = interval(Duration::from_secs(10));
        outbound_ttl_interval.tick().await; // NOTE(ts): consume the immediate first tick
        let mut overload_rx = state.overload_rx.clone();
        let mut auth_interval = interval(Duration::from_millis(100));
        let mut policy_revision = None;
        loop {
            if state
                .auth
                .as_ref()
                .is_some_and(|auth| auth.check().is_err())
            {
                break;
            }
            if let Some(auth) = &state.auth
                && auth.uses_live_permissions()
            {
                let revision = auth.policy_revision();
                if policy_revision != Some(revision) {
                    for error in state.subscription_auth.recheck(auth) {
                        if let Err(error) = state.priority_tx.try_send(error) {
                            state
                                .drop_logger
                                .on_drop("subscription authorization error", &error);
                        }
                    }
                    policy_revision = Some(revision);
                }
            }
            tokio::select! {
                _ = auth_interval.tick(), if state.auth.is_some() => {}
                Some(moved) = moved_rx.recv() => {
                    Self::replace_subscription(
                        session,
                        state.ctx.clone(),
                        &Self::message_context(&state),
                        moved,
                    );
                }
                Some(tx) = denied_rx.recv() => Self::drop_denied(session, &state, &tx),
                _ = diagnostics_interval.tick() => {
                    if let Some(diagnostics) = &state.drop_logger.diagnostics { diagnostics.flush(false); }
                }
                changed = overload_rx.changed() => {
                    if changed.is_err() || *overload_rx.borrow() {
                        tracing::warn!(
                            "Disconnecting WebSocket client {} after outbound delivery failure to force a clean resnapshot",
                            state.client_id
                        );
                        break;
                    }
                }
                // Completed subscription builds — register with session
                Some(built) = subscribe_rx.recv() => {
                    if !Self::register_built_subscription(session, built, &state) { break; }
                }
                // NOTE(ts): Sweep outbound command entries older than 10s.
                // Responses normally arrive quickly; stale entries are from
                // dropped connections or commands that will never get a response.
                _ = outbound_ttl_interval.tick() => {
                    if let Ok(mut map) = state.outbound_commands_by_tx.lock() {
                        let before = map.len();
                        map.retain(|_, (_, started)| started.elapsed() < Duration::from_secs(10));
                        let removed = before.saturating_sub(map.len());
                        if removed > 0 {
                            tracing::debug!(
                                "Outbound command TTL sweep client={}: removed {} stale entries, {} remaining",
                                session.client_id,
                                removed,
                                map.len()
                            );
                        }
                    }
                }
                // Incoming WebSocket messages
                msg = read.next() => {
                    let Some(msg) = msg else { break };
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            if is_routine_websocket_disconnect(&e) {
                                tracing::debug!(
                                    "WebSocket peer {} disconnected: {}",
                                    state.client_id,
                                    e
                                );
                            } else {
                                tracing::error!(
                                    "WebSocket read error from {}: {}",
                                    state.client_id,
                                    e
                                );
                            }
                            break;
                        }
                    };
                    if !Self::handle_ws_frame(session, &state, msg).await {
                        break;
                    }
                }
            }
        }
    }

    async fn run_command_worker(
        mut command_rx: mpsc::UnboundedReceiver<CommandJob>,
        command_ctx: Arc<MykoServerContext>,
        command_deferred_tx: mpsc::Sender<DeferredOutbound>,
        command_priority_tx: mpsc::Sender<MykoMessage>,
        command_drop_logger: Arc<DropLogger>,
        command_client_id: Arc<str>,
        command_started_cleanup: SharedTxTimes,
    ) {
        while let Some(job) = command_rx.recv().await {
            let command_ctx = command_ctx.clone();
            let command_deferred_tx = command_deferred_tx.clone();
            let command_priority_tx = command_priority_tx.clone();
            let command_drop_logger = command_drop_logger.clone();
            let command_client_id = command_client_id.clone();
            let tx_id = job.tx_id.clone();
            let started_map = command_started_cleanup.clone();
            let result = WS_COMMAND_BLOCKING
                .run("command", move || {
                    Self::execute_command_job(
                        &command_ctx,
                        &command_deferred_tx,
                        &command_priority_tx,
                        command_drop_logger.as_ref(),
                        command_client_id,
                        job,
                    );
                })
                .await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("Command worker panicked: {}", e);
                }
            }
            // NOTE(ts): Clean up timing entry after command completes (success or panic).
            if let Ok(mut map) = started_map.lock() {
                map.remove(&tx_id);
            }
        }
    }

    async fn run_query_window_worker(mut query_window_rx: mpsc::UnboundedReceiver<QueryWindowJob>) {
        while let Some(job) = query_window_rx.recv().await {
            let tx_id = job.tx_id.clone();
            if let Err(error) = WS_SUBSCRIPTION_BLOCKING
                .run("query-window", move || {
                    job.source.set_window(job.window);
                })
                .await
            {
                tracing::error!(tx = %tx_id, %error, "query window worker panicked");
            }
        }
    }

    fn spawn_query_window_worker() -> (
        mpsc::UnboundedSender<QueryWindowJob>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(Self::run_query_window_worker(rx));
        (tx, task)
    }

    fn outbound_metadata(
        message: &OutboundMessage,
    ) -> (&'static str, Option<Arc<str>>, Option<u64>) {
        match message {
            OutboundMessage::SerializedCommand { tx, .. } => ("command", Some(tx.clone()), None),
            OutboundMessage::Message(message) => match message {
                MykoMessage::ViewResponse(response) => (
                    "view_response",
                    Some(response.tx.clone()),
                    Some(response.sequence),
                ),
                MykoMessage::QueryResponse(response) => (
                    "query_response",
                    Some(response.tx.clone()),
                    Some(response.sequence),
                ),
                MykoMessage::CommandResponse(response) => (
                    "command_response",
                    Some(Arc::from(response.tx.clone())),
                    None,
                ),
                MykoMessage::CommandError(error) => {
                    ("command_error", Some(Arc::from(error.tx.clone())), None)
                }
                _ => ("other", None, None),
            },
        }
    }

    fn track_outbound_command(
        message: &OutboundMessage,
        commands: &SharedOutboundCommands,
        client_id: &str,
    ) {
        let command = match message {
            OutboundMessage::SerializedCommand { tx, command_id, .. } if !tx.trim().is_empty() => {
                Some((tx.as_ref(), command_id.as_str()))
            }
            OutboundMessage::Message(MykoMessage::Command(wrapped)) => wrapped
                .command
                .get("tx")
                .and_then(|value| value.as_str())
                .filter(|tx| !tx.trim().is_empty())
                .map(|tx| (tx, wrapped.command_id.as_str())),
            _ => None,
        };
        if let Some((tx, command_id)) = command
            && let Ok(mut commands) = commands.lock()
        {
            myko_telemetry::websocket_metrics::bind_operation(
                client_id,
                "server-command",
                tx,
                command_id,
            );
            commands.insert(tx.to_string(), (command_id.to_string(), Instant::now()));
        }
    }

    fn serialize_outbound(
        message: &OutboundMessage,
        outgoing_format: &AtomicU8,
    ) -> Option<Message> {
        match message {
            OutboundMessage::SerializedCommand {
                payload: EncodedCommandMessage::Json(json),
                ..
            } => Some(Message::Text(json.clone().into())),
            OutboundMessage::SerializedCommand {
                payload: EncodedCommandMessage::Cbor(bytes),
                ..
            } => Some(Message::Binary(bytes.clone().into())),
            OutboundMessage::Message(message)
                if outgoing_format.load(Ordering::SeqCst) == u8::from(MykoProtocol::CBOR) =>
            {
                let mut bytes = Vec::new();
                ciborium::ser::into_writer(message, &mut bytes)
                    .map(|()| Message::Binary(bytes.into()))
                    .map_err(|error| {
                        tracing::error!("Failed to serialize message to CBOR: {}", error);
                    })
                    .ok()
            }
            OutboundMessage::Message(message) => serde_json::to_string(message)
                .map(|json| Message::Text(json.into()))
                .map_err(|error| {
                    tracing::error!("Failed to serialize message to JSON: {}", error);
                })
                .ok(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_outbound(
        write: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<TcpStream>,
            Message,
        >,
        mut message: OutboundMessage,
        client_id: &str,
        addr: SocketAddr,
        outgoing_format: &AtomicU8,
        commands: &SharedOutboundCommands,
        diagnostics: &DeliveryDiagnostics,
        auth: Option<&AuthSession>,
        subscription_auth: &SubscriptionAuthorization,
    ) -> bool {
        let (kind, tx, sequence) = Self::outbound_metadata(&message);
        Self::track_outbound_command(&message, commands, client_id);
        let serialize_started = Instant::now();
        let Some(mut frame) = Self::serialize_outbound(&message, outgoing_format) else {
            // A serialization failure is just as lossy as a full queue. Stop
            // this session rather than allowing later sequenced diffs through.
            return false;
        };
        let mut serialize_elapsed = serialize_started.elapsed();
        if auth.is_some_and(|session| session.check().is_err()) {
            return false;
        }
        if let (Some(auth), OutboundMessage::Message(output)) = (auth, &message)
            && auth.uses_live_permissions()
        {
            match subscription_auth.delivery(output, auth) {
                Delivery::Allow => {}
                Delivery::Drop => return true,
                Delivery::Error(error) => {
                    message = OutboundMessage::Message(error);
                    let serialize_started = Instant::now();
                    let Some(error_frame) = Self::serialize_outbound(&message, outgoing_format)
                    else {
                        return false;
                    };
                    serialize_elapsed =
                        serialize_elapsed.saturating_add(serialize_started.elapsed());
                    frame = error_frame;
                }
            }
        }
        let payload_bytes = match &frame {
            Message::Binary(bytes) => bytes.len(),
            Message::Text(text) => text.len(),
            _ => 0,
        };
        if auth.is_some_and(|session| session.check().is_err()) {
            return false;
        }
        let send_started = Instant::now();
        let result = write.send(frame).await;
        let send_elapsed = send_started.elapsed();
        let (diagnostic_message, command_id) = match &message {
            OutboundMessage::Message(message) => (Some(message), None),
            OutboundMessage::SerializedCommand { command_id, .. } => {
                (None, Some(command_id.as_str()))
            }
        };
        diagnostics.timing(
            diagnostic_message,
            command_id,
            serialize_elapsed,
            send_elapsed,
            result.is_ok(),
        );
        if let Err(error) = result {
            tracing::error!(
                "WebSocket write failed for client {} from {} kind={} tx={:?} seq={:?} payload_bytes={} binary={}: {}",
                client_id,
                addr,
                kind,
                tx,
                sequence,
                payload_bytes,
                outgoing_format.load(Ordering::SeqCst) == u8::from(MykoProtocol::CBOR),
                error
            );
            return false;
        }
        match &message {
            OutboundMessage::Message(message) => {
                let tag = crate::ws_timing::resolved_message_tag(message, client_id);
                crate::ws_timing::record_outbound_for_client(
                    crate::ws_timing::message_kind(message),
                    client_id,
                    tag.as_deref(),
                    payload_bytes,
                );
                diagnostics.sent(message, payload_bytes);
            }
            OutboundMessage::SerializedCommand { command_id, .. } => {
                crate::ws_timing::record_outbound_for_client(
                    "Command",
                    client_id,
                    Some(command_id),
                    payload_bytes,
                );
                diagnostics.serialized_command(command_id, payload_bytes);
            }
        }
        true
    }

    fn materialize_report(
        tx: &str,
        output: &dyn AnyOutput,
        queued_at: Instant,
        diagnostics: &DeliveryDiagnostics,
    ) -> OutboundMessage {
        diagnostics.queue(1, Some((tx, queued_at.elapsed())));
        let started = Instant::now();
        let message =
            OutboundMessage::Message(MykoMessage::ReportResponse(myko::wire::ReportResponse {
                response: output.to_value(),
                tx: tx.to_string(),
            }));
        diagnostics.materialized(tx, started.elapsed());
        message
    }

    fn materialize_query(
        response: PendingQueryResponse,
        is_view: bool,
        diagnostics: &DeliveryDiagnostics,
    ) -> OutboundMessage {
        let tx = response.tx.clone();
        let started = Instant::now();
        let response = response.into_wire();
        let message = OutboundMessage::Message(if is_view {
            MykoMessage::ViewResponse(response)
        } else {
            MykoMessage::QueryResponse(response)
        });
        diagnostics.materialized(&tx, started.elapsed());
        message
    }

    async fn run_writer(state: WriterState) {
        let WriterState {
            subscription_auth,
            auth,
            mut write,
            rx,
            mut deferred_rx,
            mut view_delivery_rx,
            mut priority_rx,
            ctx: _ctx,
            client_id: write_client_id,
            addr: write_addr,
            outgoing_format: outgoing_format_writer,
            outbound_commands: outbound_commands_by_tx_writer,
            drop_logger,
            diagnostics,
        } = state;
        let mut normal_open = true;
        let mut priority_open = true;
        let mut deferred_open = true;
        let mut delivery = ViewDelivery::default();
        while normal_open || priority_open || deferred_open {
            diagnostics.queue(
                rx.len()
                    .saturating_add(deferred_rx.len())
                    .saturating_add(priority_rx.len()),
                None,
            );
            let deadline = delivery.next_deadline();
            let msg = tokio::select! {
                biased;
                Some(control) = view_delivery_rx.recv() => {
                    delivery.control(control, Instant::now());
                    continue;
                }
                () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now).into()), if deadline.is_some() => {
                    let Some(response) = delivery.take_due(Instant::now()) else { continue; };
                    Self::materialize_query(response, true, &diagnostics)
                }
                maybe = priority_rx.recv(), if priority_open => {
                    if let Some(msg) = maybe { OutboundMessage::Message(msg) } else {
                        priority_open = false;
                        continue;
                    }
                }
                maybe = deferred_rx.recv(), if deferred_open => {
                    match maybe {
                        Some(DeferredOutbound::Report(tx, output, queued_at)) => {
                            Self::materialize_report(&tx, output.as_ref(), queued_at, &diagnostics)
                        }
                        Some(DeferredOutbound::Query { response, is_view, queued_at }) => {
                            diagnostics.queue(deferred_rx.len().saturating_add(1), Some((&response.tx, queued_at.elapsed())));
                            if is_view {
                                let Some(response) = delivery.push(response, Instant::now()) else { continue; };
                                Self::materialize_query(response, true, &diagnostics)
                            } else {
                                Self::materialize_query(response, false, &diagnostics)
                            }
                        }
                        Some(DeferredOutbound::CommandResult(response)) => {
                            OutboundMessage::Message(response)
                        }
                        None => {
                            deferred_open = false;
                            continue;
                        }
                    }
                }
                maybe = rx.recv(), if normal_open => {
                    if let Some(msg) = maybe { msg } else {
                        normal_open = false;
                        continue;
                    }
                }
            };
            if !Self::send_outbound(
                &mut write,
                msg,
                &write_client_id,
                write_addr,
                &outgoing_format_writer,
                &outbound_commands_by_tx_writer,
                &diagnostics,
                auth.as_ref(),
                &subscription_auth,
            )
            .await
            {
                // Wake the read half so connection cleanup drops both halves.
                // Reconnecting clients will resubscribe from a fresh snapshot.
                drop_logger.disconnect_for_overload();
                break;
            }
        }
        Self::writer_exited(
            &write_client_id,
            write_addr,
            normal_open,
            priority_open,
            deferred_open,
        );
    }

    fn writer_exited(
        write_client_id: &str,
        write_addr: SocketAddr,
        normal_open: bool,
        priority_open: bool,
        deferred_open: bool,
    ) {
        // NOTE(ts): Unregister from client registry immediately so the node
        // executor stops serializing commands into a dead channel.
        if let Some(registry) = try_client_registry() {
            registry.unregister(write_client_id);
            tracing::info!(
                "WebSocket writer unregistered client {} from {} (write task exiting)",
                write_client_id,
                write_addr,
            );
        }
        tracing::warn!(
            "WebSocket writer task exiting for client {} from {} normal_open={} priority_open={} deferred_open={}",
            write_client_id,
            write_addr,
            normal_open,
            priority_open,
            deferred_open
        );
    }

    /// Handle a new WebSocket connection (performs the handshake).
    /// # Errors
    ///
    /// Returns an error when the WebSocket connection cannot be upgraded or handled.
    pub async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        ctx: Arc<MykoServerContext>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(WS_MAX_MESSAGE_SIZE_BYTES);
        ws_config.max_frame_size = Some(WS_MAX_FRAME_SIZE_BYTES);
        let ws_stream = accept_async_with_config(stream, Some(ws_config)).await?;
        Self::handle_upgraded(ws_stream, addr, ctx).await
    }

    /// Handle a WebSocket connection whose HTTP/1.1 handshake has already
    /// completed and produced a [`tokio_tungstenite::WebSocketStream`].
    ///
    /// Used by the front-door router when it pre-parses the HTTP request
    /// (to dispatch between `/myko` WS and `/myko/mcp` HTTP/WS) and then
    /// completes the WS handshake itself.
    /// # Errors
    ///
    /// Returns an error when the upgraded WebSocket session fails.
    pub async fn handle_upgraded(
        ws_stream: tokio_tungstenite::WebSocketStream<TcpStream>,
        addr: SocketAddr,
        ctx: Arc<MykoServerContext>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Self::handle_upgraded_with_access(ws_stream, addr, ctx, GatewayAccess::Full).await
    }

    /// Handle an upgraded gateway with immutable inbound capabilities.
    /// # Errors
    /// Returns an error when the WebSocket session fails.
    pub async fn handle_upgraded_with_access(
        ws_stream: tokio_tungstenite::WebSocketStream<TcpStream>,
        addr: SocketAddr,
        ctx: Arc<MykoServerContext>,
        access: GatewayAccess,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Self::handle_upgraded_with_access_and_auth(ws_stream, addr, ctx, access, None).await
    }

    /// Run a gateway with verified authority and an independent access ceiling.
    /// # Errors
    /// Returns an error when the session is invalid or the transport fails.
    #[allow(clippy::too_many_lines)]
    pub async fn handle_upgraded_with_access_and_auth(
        ws_stream: tokio_tungstenite::WebSocketStream<TcpStream>,
        addr: SocketAddr,
        ctx: Arc<MykoServerContext>,
        access: GatewayAccess,
        auth: Option<AuthSession>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(session) = &auth {
            session.check()?;
        }
        let (write, read) = ws_stream.split();

        // Outbound frames share one ordered queue. Only sequenced messages
        // count against MESSAGE_QUEUE_CAPACITY (overflow resnapshots); commands
        // are events that a resnapshot cannot replay, so they are never refused
        // while the connection lives.
        let (tx, rx) = outbound_queue::channel::<OutboundMessage>();
        let queued_messages = Arc::new(AtomicUsize::new(0));
        let (deferred_tx, deferred_rx) = mpsc::channel::<DeferredOutbound>(10_000);
        let (priority_tx, priority_rx) = mpsc::channel::<MykoMessage>(1_000);
        let (command_tx, command_rx) = mpsc::unbounded_channel::<CommandJob>();
        let (query_window_tx, query_window_task) = Self::spawn_query_window_worker();
        let (subscribe_tx, subscribe_rx) = mpsc::unbounded_channel::<SubscriptionBuilt>();
        let (view_delivery_tx, view_delivery_rx) = mpsc::unbounded_channel();
        let (overload_tx, overload_rx) = watch::channel(false);
        // Outgoing format for this session: defaults to JSON, sticky-promotes
        // to CBOR on the first received binary frame. Never demotes.
        let outgoing_format = Arc::new(AtomicU8::new(MykoProtocol::JSON.into()));

        // Create client session with channel-based writer
        let client_id: Arc<str> = Uuid::new_v4().to_string().into();
        myko_telemetry::websocket_metrics::client_connected(&client_id);
        let diagnostics = Arc::new(DeliveryDiagnostics::new(&client_id, addr));
        let mut logger = DropLogger::new(client_id.clone(), overload_tx);
        logger.diagnostics = Some(diagnostics.clone());
        let drop_logger = Arc::new(logger);
        let writer = ChannelWriter {
            tx: tx.clone(),
            queued_messages: queued_messages.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            outgoing_format: outgoing_format.clone(),
        };

        // Register writer in the global client registry (if initialized)
        let writer_arc: Arc<dyn WsWriter> = Arc::new(writer.clone());
        if let Some(registry) = try_client_registry() {
            registry.register(client_id.clone(), writer_arc);
        }
        let mut session = ClientSession::new(client_id.clone(), writer);

        let outgoing_format_writer = outgoing_format.clone();
        let query_ids_by_tx: SharedTxNames = Arc::new(Mutex::new(HashMap::new()));
        let view_ids_by_tx: SharedTxNames = Arc::new(Mutex::new(HashMap::new()));
        let subscribe_started_by_tx: SharedTxTimes = Arc::new(Mutex::new(HashMap::new()));
        let command_started_by_tx: SharedTxTimes = Arc::new(Mutex::new(HashMap::new()));
        let outbound_commands_by_tx: SharedOutboundCommands = Arc::new(Mutex::new(HashMap::new()));
        let outbound_commands_by_tx_writer = outbound_commands_by_tx.clone();
        let client_entity = Self::publish_client(&ctx, client_id.clone(), addr);

        let write_ctx = ctx.clone();
        let write_client_id = client_id.clone();
        let command_ctx = ctx.clone();
        let command_deferred_tx = deferred_tx.clone();
        let command_priority_tx = priority_tx.clone();
        let command_drop_logger = drop_logger.clone();
        let command_client_id = client_id.clone();

        let (denied_tx, denied_rx) = mpsc::unbounded_channel();
        let (placements, moved_rx) = SubscriptionPlacements::new();
        let subscription_auth = Arc::new(SubscriptionAuthorization::new(denied_tx));
        let write_task = tokio::spawn(Self::run_writer(WriterState {
            subscription_auth: subscription_auth.clone(),
            auth: auth.clone(),
            write,
            rx: OutboundReceiver {
                rx,
                queued_messages: queued_messages.clone(),
                diagnostics: Some(diagnostics.clone()),
            },
            deferred_rx,
            view_delivery_rx,
            priority_rx,
            ctx: write_ctx,
            client_id: write_client_id,
            addr,
            outgoing_format: outgoing_format_writer,
            outbound_commands: outbound_commands_by_tx_writer,
            drop_logger: drop_logger.clone(),
            diagnostics,
        }));

        let command_task = tokio::spawn(Self::run_command_worker(
            command_rx,
            command_ctx,
            command_deferred_tx,
            command_priority_tx,
            command_drop_logger,
            command_client_id,
            command_started_by_tx.clone(),
        ));
        let _connection = ConnectionGuard {
            ctx: ctx.clone(),
            client_entity,
            client_id: client_id.clone(),
            addr,
            tasks: [write_task, command_task, query_window_task],
        };
        Self::run_read_loop(
            read,
            &mut session,
            subscribe_rx,
            denied_rx,
            moved_rx,
            ReadLoopState {
                subscription_auth,
                auth,
                access,
                ctx: ctx.clone(),
                client_id: client_id.clone(),
                outgoing_format: outgoing_format.clone(),
                priority_tx: priority_tx.clone(),
                drop_logger: drop_logger.clone(),
                query_ids_by_tx: query_ids_by_tx.clone(),
                view_ids_by_tx: view_ids_by_tx.clone(),
                subscribe_started_by_tx: subscribe_started_by_tx.clone(),
                command_started_by_tx: command_started_by_tx.clone(),
                outbound_commands_by_tx: outbound_commands_by_tx.clone(),
                command_tx: command_tx.clone(),
                query_window_tx: query_window_tx.clone(),
                subscribe_tx: subscribe_tx.clone(),
                subscription_builds: SubscriptionBuilds::default(),
                view_delivery_tx,
                placements,
                overload_rx,
            },
        )
        .await;

        WS_CLEANUP_BLOCKING.spawn("session-teardown", move || drop(session));

        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "a query subscription's routing, build and fallback read in one place"
    )]
    fn handle_query_request<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: &MykoServerContext,
        message_context: &MessageContext<'_>,
        wrapped: WrappedQuery,
    ) {
        let tx_id: Arc<str> = wrapped
            .query
            .get("tx")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .into();
        let query_id = wrapped.query_id.clone();
        if session.has_subscription(&tx_id) {
            tracing::debug!(
                "Ignoring duplicate query subscribe client={} tx={} query_id={}",
                session.client_id,
                tx_id,
                query_id
            );
            return;
        }
        if let Ok(mut map) = message_context.query_ids_by_tx.lock() {
            map.insert(tx_id.clone(), query_id.clone());
        }
        if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
            map.entry(tx_id.clone()).or_insert_with(Instant::now);
        }
        let request = Arc::new(
            RequestContext::from_client(tx_id.clone(), session.client_id.clone(), ctx.host_id)
                .with_auth(message_context.auth.clone()),
        );
        let Some(ctx) = Self::open_subscription(
            ctx,
            message_context,
            (&tx_id, &request, &wrapped.query),
            (
                Some(&wrapped.query_item_type),
                MykoMessage::Query(wrapped.clone()),
            ),
            |error| {
                MykoMessage::QueryError(myko::wire::QueryError::new(&*tx_id, &*query_id, error))
            },
        ) else {
            return;
        };
        if let Some(query_data) = ctx.handler_registry.query(&query_id) {
            match (query_data.parse)(wrapped.query.clone()) {
                Ok(query) => {
                    let registry = ctx.registry.clone();
                    let factory = query_data.cell_factory;
                    let window_factory = query_data.window_cell_factory;
                    let window = wrapped.window;
                    let sender = message_context.subscribe_tx.clone();
                    message_context.subscription_builds.spawn(
                        "query-subscription",
                        tx_id.clone(),
                        sender,
                        move || {
                            if let (Some(window_factory), Some(initial_window)) =
                                (window_factory, window.clone())
                            {
                                match ctx.in_domain(|| {
                                    window_factory(
                                        query.clone(),
                                        registry.clone(),
                                        request.clone(),
                                        ctx.clone(),
                                        initial_window,
                                    )
                                }) {
                                    Ok(Some(source)) => {
                                        return Some(Ok(SubscriptionReady::WindowedQuery {
                                            tx_id,
                                            query_id,
                                            source,
                                        }));
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to create pushed query window for {}: {}",
                                            query_id,
                                            error
                                        );
                                        return None;
                                    }
                                }
                            }
                            let built = ctx
                                .checked(|| factory(query, registry, request, Some(ctx.clone())))
                                .map_err(|error| error.to_string())
                                .and_then(|built| built);
                            match built {
                                Ok(cellmap) => Some(Ok(SubscriptionReady::Query {
                                    tx_id,
                                    query_id,
                                    cellmap,
                                    window,
                                })),
                                Err(error) => {
                                    tracing::error!(
                                        "Failed to create query cell for {}: {}",
                                        query_id,
                                        error
                                    );
                                    Some(Err(MykoMessage::QueryError(myko::wire::QueryError::new(
                                        tx_id.to_string(),
                                        query_id.to_string(),
                                        error,
                                    ))))
                                }
                            }
                        },
                    );
                }
                Err(error) => tracing::error!(
                    "Failed to parse query {}: {} | payload: {}",
                    query_id,
                    error,
                    serde_json::to_string(&wrapped.query).unwrap_or_default()
                ),
            }
        } else {
            let store = (*ctx.registry.get_or_create(&wrapped.query_item_type)).clone();
            let cellmap = ctx.in_domain(|| hyphae::MapQuery::materialize(store.select(|_| true)));
            message_context.drop_logger.subscription_ready(&tx_id);
            message_context.subscription_builds.cancel(&tx_id);
            session.subscribe_query(tx_id, query_id, cellmap, wrapped.window);
        }
    }

    fn handle_view_request<W: WsWriter>(
        session: &ClientSession<W>,
        ctx: Arc<MykoServerContext>,
        message_context: &MessageContext<'_>,
        wrapped: WrappedView,
    ) {
        let tx_id: Arc<str> = wrapped
            .view
            .get("tx")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .into();
        let view_id = wrapped.view_id.clone();
        if session.has_subscription(&tx_id) {
            tracing::debug!(
                "Ignoring duplicate view subscribe client={} tx={} view_id={}",
                session.client_id,
                tx_id,
                view_id
            );
            return;
        }
        if let Ok(mut map) = message_context.view_ids_by_tx.lock() {
            map.insert(tx_id.clone(), view_id.clone());
        }
        let _ = message_context
            .view_delivery_tx
            .send(ViewDeliveryControl::Subscribe {
                tx: tx_id.clone(),
                rate: wrapped.sample_rate,
            });
        if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
            map.entry(tx_id.clone()).or_insert_with(Instant::now);
        }
        let request = Arc::new(
            RequestContext::from_client(tx_id.clone(), session.client_id.clone(), ctx.host_id)
                .with_auth(message_context.auth.clone()),
        );
        let Some(view_data) = ctx.handler_registry.view(&view_id) else {
            let message = format!("No registered handler for view: {view_id}");
            Self::send_view_error(message_context, &tx_id, &view_id, message);
            drop(ctx);
            return;
        };
        if let Some(span) = ctx.span_contexts(&view_id, &request, &wrapped.view) {
            Self::open_merged_view(&ctx, message_context, (&tx_id, &request), &wrapped, span);
            return;
        }
        let Some(ctx) = Self::open_subscription(
            &ctx,
            message_context,
            (&tx_id, &request, &wrapped.view),
            (
                Some(&view_data.view_item_type),
                MykoMessage::View(wrapped.clone()),
            ),
            |error| MykoMessage::ViewError(ViewError::new(&*tx_id, &*view_id, error)),
        ) else {
            return;
        };
        match (view_data.parse)(wrapped.view.clone()) {
            Ok(view) => {
                let cell_factory = view_data.cell_factory;
                let subscribe_factory = ctx.handler_registry.view_subscribe_factory(&view_id);
                let factory = move |view, registry, request, ctx| match subscribe_factory {
                    Some(subscribe) => subscribe(view, registry, request, ctx),
                    None => cell_factory(view, registry, request, ctx)
                        .map(myko::view::ErasedViewMap::from),
                };
                let registry = ctx.registry.clone();
                let window = wrapped.window;
                let sender = message_context.subscribe_tx.clone();
                message_context.subscription_builds.spawn(
                    "view-subscription",
                    tx_id.clone(),
                    sender,
                    move || match ctx
                        .checked(|| factory(view, registry, request, ctx.clone()))
                        .map_err(|error| error.to_string())
                        .and_then(|built| built)
                    {
                        Ok(cellmap) => Some(Ok(SubscriptionReady::View {
                            tx_id,
                            view_id,
                            cellmap,
                            window,
                        })),
                        Err(error) => Some(Err(MykoMessage::ViewError(ViewError::new(
                            tx_id.to_string(),
                            view_id.to_string(),
                            error,
                        )))),
                    },
                );
            }
            Err(error) => Self::send_view_error(
                message_context,
                &tx_id,
                &view_id,
                format!("Failed to parse view {view_id}: {error}"),
            ),
        }
    }

    /// The context a subscription opens in, or `None` after sending the
    /// client `refuse` of why it cannot open.
    ///
    /// A subscription the hook places from shared data is followed, and
    /// `resend` opens it again when it belongs elsewhere.
    fn open_subscription(
        ctx: &MykoServerContext,
        message_context: &MessageContext<'_>,
        opening: (&Arc<str>, &Arc<RequestContext>, &serde_json::Value),
        (reads, resend): (Option<&str>, MykoMessage),
        refuse: impl FnOnce(String) -> MykoMessage,
    ) -> Option<Arc<MykoServerContext>> {
        let (_, request, payload) = opening;
        match ctx.subscription_context(request, payload, reads) {
            Ok(opened) => {
                message_context
                    .placements
                    .follow(ctx, &opened, opening, resend);
                Some(Arc::new(opened))
            }
            Err(error) => {
                if let Err(error) = message_context
                    .priority_tx
                    .try_send(refuse(error.to_string()))
                {
                    message_context
                        .drop_logger
                        .on_drop("SubscriptionError", &error);
                }
                None
            }
        }
    }

    /// Open a subscription of a view merged across partitions: build its part
    /// in each partition of `span` and deliver the union. It is opened again
    /// when the span changes or one of its partitions opens or closes.
    fn open_merged_view(
        root: &MykoServerContext,
        message_context: &MessageContext<'_>,
        (tx_id, request): (&Arc<str>, &Arc<RequestContext>),
        wrapped: &WrappedView,
        (keys, contexts): (
            std::collections::BTreeSet<myko::server::PartitionKey>,
            Vec<MykoServerContext>,
        ),
    ) {
        let view_id = wrapped.view_id.clone();
        let moved = message_context.placements.moved.clone();
        let moved_tx = Arc::clone(tx_id);
        let resend = MykoMessage::View(wrapped.clone());
        let watch = root.watch_span(
            Arc::clone(&view_id),
            (Arc::clone(request), wrapped.view.clone()),
            (keys, &contexts),
            move || drop(moved.send((Arc::clone(&moved_tx), resend.clone()))),
        );
        if let (Some(watch), Ok(mut watches)) = (watch, message_context.placements.watches.lock()) {
            watches.insert(Arc::clone(tx_id), watch);
        }
        let Some(view_data) = root.handler_registry.view(&view_id) else {
            return;
        };
        let subscribe_factory = root.handler_registry.view_subscribe_factory(&view_id);
        let cell_factory = view_data.cell_factory;
        let parse = view_data.parse;
        let payload = wrapped.view.clone();
        let request = Arc::clone(request);
        let tx = Arc::clone(tx_id);
        let window = wrapped.window.clone();
        message_context.subscription_builds.spawn(
            "merged-view-subscription",
            Arc::clone(tx_id),
            message_context.subscribe_tx.clone(),
            move || {
                let mut parts = Vec::with_capacity(contexts.len());
                for part in contexts {
                    let built = parse(payload.clone())
                        .map_err(|error| error.to_string())
                        .and_then(|view| {
                            let registry = part.registry.clone();
                            let request = Arc::clone(&request);
                            let ctx = Arc::new(part.clone());
                            part.checked(|| match subscribe_factory {
                                Some(subscribe) => subscribe(view, registry, request, ctx),
                                None => cell_factory(view, registry, request, ctx)
                                    .map(myko::view::ErasedViewMap::from),
                            })
                            .map_err(|error| error.to_string())
                            .and_then(|built| built)
                        });
                    match built {
                        Ok(map) => parts.push(map),
                        Err(error) => {
                            return Some(Err(MykoMessage::ViewError(ViewError::new(
                                tx.to_string(),
                                view_id.to_string(),
                                error,
                            ))));
                        }
                    }
                }
                Some(Ok(SubscriptionReady::View {
                    tx_id: tx,
                    view_id,
                    cellmap: myko::view::ErasedViewMap::merged(parts),
                    window,
                }))
            },
        );
    }

    fn send_view_error(
        message_context: &MessageContext<'_>,
        tx_id: &str,
        view_id: &str,
        message: String,
    ) {
        let error = MykoMessage::ViewError(ViewError::new(
            tx_id.to_string(),
            view_id.to_string(),
            message,
        ));
        if let Err(error) = message_context.priority_tx.try_send(error) {
            message_context.drop_logger.on_drop("ViewError", &error);
        }
    }

    /// Handle a parsed `MykoMessage`.
    fn handle_message<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<MykoServerContext>,
        message_context: &MessageContext<'_>,
        msg: MykoMessage,
    ) {
        if let Some(diagnostics) = &message_context.drop_logger.diagnostics {
            diagnostics.inbound(&msg);
        }
        crate::ws_timing::record_inbound_message(&msg, &session.client_id);
        match msg {
            MykoMessage::Query(wrapped) => {
                Self::handle_query_request(session, &ctx, message_context, wrapped);
            }
            MykoMessage::View(wrapped) => {
                Self::handle_view_request(session, ctx, message_context, wrapped);
            }
            message @ (MykoMessage::QueryCancel(_)
            | MykoMessage::QueryWindow(_)
            | MykoMessage::QueryCursorWindow(_)
            | MykoMessage::ViewCancel(_)
            | MykoMessage::ViewWindow(_)
            | MykoMessage::ViewSampleRate(_)
            | MykoMessage::ReportCancel(_)) => {
                Self::handle_subscription_control(session, message_context, message);
                drop(ctx);
            }
            message @ (MykoMessage::Report(_)
            | MykoMessage::Event(_)
            | MykoMessage::EventBatch(_)) => {
                Self::handle_report_or_event(session, ctx, message_context, message);
            }
            message @ (MykoMessage::Command(_) | MykoMessage::Ping(_)) => {
                Self::handle_command_or_ping(session, message_context, message);
                drop(ctx);
            }
            message @ (MykoMessage::CommandResponse(_) | MykoMessage::CommandError(_)) => {
                Self::handle_command_result(session, message_context, message);
                drop(ctx);
            }
            MykoMessage::Benchmark(payload) => {
                let stats = ws_benchmark_stats();
                ensure_ws_benchmark_logger();
                stats.message_count.fetch_add(1, Ordering::Relaxed);
                let size = u64::try_from(payload.to_string().len()).unwrap_or(u64::MAX);
                stats.total_bytes.fetch_add(size, Ordering::Relaxed);
                drop(ctx);
            }
            unexpected => {
                tracing::warn!(
                    "Unexpected client message kind={} client={} active_subscriptions={}",
                    crate::ws_timing::message_kind(&unexpected),
                    session.client_id,
                    session.subscription_count()
                );
                drop(ctx);
            }
        }
    }

    fn register_built_subscription<W: WsWriter>(
        session: &mut ClientSession<W>,
        built: SubscriptionBuilt,
        state: &ReadLoopState,
    ) -> bool {
        let Some((tx, ready)) = Self::complete_subscription(
            &state.subscription_builds,
            built,
            &state.priority_tx,
            &state.drop_logger,
        ) else {
            return true;
        };
        if let Some(ready) = ready {
            return Self::register_ready_subscription(session, ready, state);
        }
        if let Ok(mut map) = state.subscribe_started_by_tx.lock() {
            map.remove(&tx);
        }
        true
    }

    fn complete_subscription(
        builds: &SubscriptionBuilds,
        built: SubscriptionBuilt,
        priority: &mpsc::Sender<MykoMessage>,
        logger: &DropLogger,
    ) -> Option<(Arc<str>, Option<SubscriptionReady>)> {
        if !builds.finish(&built) {
            return None;
        }
        let ready = match built.result {
            Some(Ok(ready)) => Some(ready),
            Some(Err(error)) => {
                if let Err(error) = priority.try_send(error) {
                    logger.on_drop("SubscriptionError", &error);
                }
                None
            }
            None => None,
        };
        Some((built.tx, ready))
    }

    fn handle_subscription_control<W: WsWriter>(
        session: &mut ClientSession<W>,
        message_context: &MessageContext<'_>,
        message: MykoMessage,
    ) {
        match message {
            MykoMessage::QueryCancel(CancelSubscription { tx }) => {
                message_context.placements.forget(&tx);
                message_context.subscription_builds.cancel(&tx);
                let tx: Arc<str> = tx.into();
                if let Ok(mut map) = message_context.query_ids_by_tx.lock() {
                    map.remove(&tx);
                }
                if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
                    map.remove(&tx);
                }
                session.cancel(&tx);
            }
            MykoMessage::QueryWindow(QueryWindowUpdate { tx, window }) => {
                let tx_id: Arc<str> = tx.into();
                if let Some((source, window)) = session.prepare_query_window_update(&tx_id, window)
                    && let Err(error) = message_context.query_window_tx.send(QueryWindowJob {
                        tx_id: tx_id.clone(),
                        source,
                        window,
                    })
                {
                    tracing::error!(tx = %tx_id, %error, "query window worker is unavailable");
                }
            }
            MykoMessage::QueryCursorWindow(QueryCursorWindowUpdate { tx, window }) => {
                session.update_query_cursor_window(&Arc::from(tx), window);
            }
            MykoMessage::ViewCancel(CancelSubscription { tx }) => {
                message_context.placements.forget(&tx);
                message_context.subscription_builds.cancel(&tx);
                let tx: Arc<str> = tx.into();
                let _ = message_context
                    .view_delivery_tx
                    .send(ViewDeliveryControl::Cancel(tx.clone()));
                if let Ok(mut map) = message_context.view_ids_by_tx.lock() {
                    map.remove(&tx);
                }
                if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
                    map.remove(&tx);
                }
                session.cancel(&tx);
            }
            MykoMessage::ViewSampleRate(update) => {
                let _ = message_context
                    .view_delivery_tx
                    .send(ViewDeliveryControl::SetRate {
                        tx: update.tx.into(),
                        rate: update.sample_rate,
                    });
            }
            MykoMessage::ViewWindow(ViewWindowUpdate { tx, window }) => {
                session.update_view_window(&Arc::from(tx), window);
            }
            MykoMessage::ReportCancel(CancelSubscription { tx }) => {
                message_context.placements.forget(&tx);
                message_context.subscription_builds.cancel(&tx);
                session.cancel(&Arc::from(tx));
            }
            _ => {}
        }
    }

    fn handle_report_or_event<W: WsWriter>(
        session: &ClientSession<W>,
        ctx: Arc<MykoServerContext>,
        message_context: &MessageContext<'_>,
        message: MykoMessage,
    ) {
        match message {
            MykoMessage::Report(wrapped) => {
                let tx: Arc<str> = wrapped
                    .report
                    .get("tx")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown")
                    .into();
                let report_id = wrapped.report_id.clone();
                let Some(data) = ctx.handler_registry.report(&report_id) else {
                    tracing::warn!("No registered handler for report: {}", report_id);
                    return;
                };
                match (data.parse)(wrapped.report.clone()) {
                    Ok(report) => {
                        let request = Arc::new(
                            RequestContext::from_client(
                                tx.clone(),
                                session.client_id.clone(),
                                ctx.host_id,
                            )
                            .with_auth(message_context.auth.clone()),
                        );
                        let Some(ctx) = Self::open_subscription(
                            &ctx,
                            message_context,
                            (&tx, &request, &wrapped.report),
                            (None, MykoMessage::Report(wrapped.clone())),
                            |error| {
                                MykoMessage::ReportError(myko::wire::ReportError::new(
                                    &*tx, &report_id, error,
                                ))
                            },
                        ) else {
                            return;
                        };
                        let factory = data.cell_factory;
                        let sender = message_context.subscribe_tx.clone();
                        message_context.subscription_builds.spawn(
                            "report-subscription",
                            tx.clone(),
                            sender,
                            move || match ctx
                                .checked(|| factory(report, request, ctx.clone()))
                                .map_err(|error| error.to_string())
                                .and_then(|built| built)
                            {
                                Ok(cell) => Some(Ok(SubscriptionReady::Report {
                                    tx_id: tx,
                                    report_id: report_id.into(),
                                    cell,
                                })),
                                Err(error) => {
                                    tracing::error!(
                                        "Failed to create report cell for {}: {}",
                                        report_id,
                                        error
                                    );
                                    Some(Err(MykoMessage::ReportError(
                                        myko::wire::ReportError::new(&*tx, &report_id, error),
                                    )))
                                }
                            },
                        );
                    }
                    Err(error) => tracing::error!(
                        "Failed to parse report {}: {} | payload: {}",
                        report_id,
                        error,
                        serde_json::to_string(&wrapped.report).unwrap_or_default()
                    ),
                }
            }
            MykoMessage::Event(mut event) => {
                event.sanitize_null_bytes();
                normalize_incoming_event(&mut event, &session.client_id, ctx.host_id);
                if let Err(error) = ctx.apply_event(event) {
                    tracing::error!(
                        "Failed to apply event from client {}: {error}",
                        session.client_id
                    );
                }
                drop(ctx);
            }
            MykoMessage::EventBatch(mut events) => {
                for event in &mut events {
                    event.sanitize_null_bytes();
                    normalize_incoming_event(event, &session.client_id, ctx.host_id);
                }
                if let Err(error) = ctx.apply_event_batch(events) {
                    tracing::error!(
                        "Failed to apply event batch from client {}: {}",
                        session.client_id,
                        error
                    );
                }
                drop(ctx);
            }
            _ => drop(ctx),
        }
    }

    fn handle_command_or_ping<W: WsWriter>(
        session: &ClientSession<W>,
        message_context: &MessageContext<'_>,
        message: MykoMessage,
    ) {
        match message {
            MykoMessage::Command(wrapped) => {
                let tx_id: Arc<str> = wrapped
                    .command
                    .get("tx")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown")
                    .into();
                let received_at = Instant::now();
                if let Ok(mut map) = message_context.command_started_by_tx.lock() {
                    map.insert(tx_id.clone(), received_at);
                }
                let command_id = wrapped.command_id;
                if let Err(error) = message_context.command_tx.send(CommandJob {
                    auth: message_context.auth.clone(),
                    tx_id: tx_id.clone(),
                    command_id: command_id.clone(),
                    command: wrapped.command,
                    received_at,
                }) {
                    tracing::error!(
                        "Failed to enqueue command {} for client {} tx={}: {}",
                        command_id,
                        session.client_id,
                        tx_id,
                        error
                    );
                    let response = MykoMessage::CommandError(CommandError::new(
                        tx_id.to_string(),
                        command_id,
                        "Command queue unavailable",
                    ));
                    if let Err(error) = message_context.priority_tx.try_send(response) {
                        message_context.drop_logger.on_drop("CommandError", &error);
                    }
                }
            }
            MykoMessage::Ping(data) => {
                if let Err(error) = message_context
                    .priority_tx
                    .try_send(MykoMessage::Ping(data))
                {
                    message_context.drop_logger.on_drop("Ping", &error);
                }
            }
            _ => {}
        }
    }

    fn handle_command_result<W: WsWriter>(
        session: &ClientSession<W>,
        message_context: &MessageContext<'_>,
        message: MykoMessage,
    ) {
        let (tx, reported_id, error_message) = match message {
            MykoMessage::CommandResponse(response) => (response.tx, None, None),
            MykoMessage::CommandError(error) => {
                (error.tx, Some(error.command_id), Some(error.message))
            }
            _ => return,
        };
        if tx.trim().is_empty() {
            tracing::warn!(
                "Malformed client command result client={} tx=<empty> command_id={:?} message={:?}",
                session.client_id,
                reported_id,
                error_message
            );
            return;
        }
        let correlated = message_context
            .outbound_commands_by_tx
            .lock()
            .ok()
            .and_then(|mut map| map.remove(&tx));
        correlated.map_or_else(
            || tracing::warn!(
                "Client command result without outbound match client={} tx={} command_id={:?} message={:?}",
                session.client_id,
                tx,
                reported_id,
                error_message
            ),
            |(command_id, started)| tracing::trace!(
                "Client command result matched client={} tx={} command_id={} reported_id={:?} message={:?} roundtrip_ms={}",
                session.client_id,
                tx,
                command_id,
                reported_id,
                error_message,
                started.elapsed().as_millis()
            ),
        );
    }

    fn send_command_result(
        deferred_tx: &mpsc::Sender<DeferredOutbound>,
        drop_logger: &DropLogger,
        result: MykoMessage,
    ) {
        if let Err(error) = deferred_tx.try_send(DeferredOutbound::CommandResult(result)) {
            drop_logger.on_drop("CommandResult", &error);
        }
    }

    fn execute_command_job(
        ctx: &MykoServerContext,
        deferred_tx: &mpsc::Sender<DeferredOutbound>,
        priority_tx: &mpsc::Sender<MykoMessage>,
        drop_logger: &DropLogger,
        client_id: Arc<str>,
        job: CommandJob,
    ) {
        let host_id = ctx.host_id;
        let started = Instant::now();
        let queue_wait_ms = started.duration_since(job.received_at).as_millis();
        let command_id = job.command_id.clone();

        let mut handler_found = false;
        for registration in inventory::iter::<CommandHandlerRegistration> {
            if registration.command_id == command_id {
                handler_found = true;
                let executor = (registration.factory)();

                let req = Arc::new(
                    RequestContext::from_client(job.tx_id.clone(), client_id.clone(), host_id)
                        .with_auth(job.auth.clone()),
                );
                let cmd_id: Arc<str> = Arc::from(command_id.clone());
                let ctx = match ctx.command_context(&req, &job.command) {
                    Ok(placed) => Arc::new(placed),
                    Err(refused) => {
                        Self::send_command_result(
                            deferred_tx,
                            drop_logger,
                            MykoMessage::CommandError(CommandError::new(
                                job.tx_id.to_string(),
                                command_id,
                                refused.to_string(),
                            )),
                        );
                        return;
                    }
                };
                let cmd_ctx = CommandContext::new(cmd_id, req, ctx);
                let execute_started = Instant::now();

                if let Some(auth) = &job.auth
                    && auth
                        .authorize(AuthorizationRequest::Execute {
                            kind: OperationKind::Command,
                            operation: &job.command_id,
                            arguments: &job.command,
                        })
                        .is_err()
                {
                    return;
                }
                let result = match executor.execute_from_value(job.command.clone(), cmd_ctx) {
                    Ok(response) => MykoMessage::CommandResponse(CommandResponse {
                        response,
                        tx: job.tx_id.to_string(),
                    }),
                    Err(error) => MykoMessage::CommandError(CommandError::new(
                        job.tx_id.to_string(),
                        command_id.clone(),
                        error.message,
                    )),
                };
                Self::send_command_result(deferred_tx, drop_logger, result);
                let execute_ms = execute_started.elapsed().as_millis();
                let total_ms = job.received_at.elapsed().as_millis();
                tracing::trace!(
                    target: "myko_server::ws_perf",
                    "command_exec client={} tx={} command_id={} queue_wait_ms={} execute_ms={} total_ms={}",
                    client_id,
                    job.tx_id,
                    command_id,
                    queue_wait_ms,
                    execute_ms,
                    total_ms
                );
                break;
            }
        }

        if !handler_found {
            tracing::warn!("No registered handler for command: {}", command_id);
            let error = MykoMessage::CommandError(CommandError::new(
                job.tx_id.to_string(),
                command_id.clone(),
                format!("Command handler not found: {command_id}"),
            ));
            if let Err(e) = priority_tx.try_send(error) {
                drop_logger.on_drop("CommandError", &e);
            }
        }

        if !handler_found {
            tracing::debug!(
                target: "myko_server::ws_perf",
                "command_exec client={} tx={} command_id={} queue_wait_ms={} execute_ms=0 total_ms={} handler_found=false",
                client_id,
                job.tx_id,
                command_id,
                queue_wait_ms,
                job.received_at.elapsed().as_millis()
            );
        }
        drop(client_id);
        drop(job);
    }
}

/// Channel-based WebSocket writer.
///
/// Sends messages through an mpsc channel which are then
/// forwarded to the actual WebSocket.
#[derive(Clone)]
struct ChannelWriter {
    tx: outbound_queue::Sender<OutboundMessage>,
    /// `Message` frames enqueued on `tx` and not yet taken by the writer.
    queued_messages: Arc<AtomicUsize>,
    deferred_tx: mpsc::Sender<DeferredOutbound>,
    drop_logger: Arc<DropLogger>,
    outgoing_format: Arc<AtomicU8>,
}

impl ChannelWriter {
    /// Cheap "is the writer task gone?" check used to short-circuit
    /// subscriber callbacks for a disconnected client. `Sender::is_closed`
    /// returns true once the matching receiver is dropped, which happens as
    /// soon as the write task exits. Avoiding the work here prevents the
    /// "buffer full / channel closed" log storm we used to see for 10+
    /// seconds after every disconnect while the session was still tearing
    /// down its subscription guards.
    #[inline]
    fn tx_dead(&self) -> bool {
        self.tx.is_closed()
    }

    #[inline]
    fn deferred_dead(&self) -> bool {
        self.deferred_tx.is_closed()
    }
}

impl WsWriter for ChannelWriter {
    fn send(&self, msg: MykoMessage) {
        // Fast path: writer is gone. Don't try to send and don't log; the
        // dead-channel state is expected after the client disconnects, and
        // a dropped subscription will follow shortly when the session
        // teardown finishes. Avoiding the log+counter prevents the
        // "buffer full / channel closed" warning storm we used to see for
        // 10+ seconds after every disconnect.
        if self.tx_dead() {
            return;
        }
        if self.queued_messages.fetch_add(1, Ordering::AcqRel) >= MESSAGE_QUEUE_CAPACITY {
            self.queued_messages.fetch_sub(1, Ordering::AcqRel);
            self.drop_logger
                .on_drop("message", &"outbound message queue full");
            return;
        }
        // A send error means the writer task exited after the is_dead check;
        // the session is tearing down, so there is nothing to report.
        if self.tx.push(OutboundMessage::Message(msg)) == Pushed::Closed {
            self.queued_messages.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn protocol(&self) -> MykoProtocol {
        MykoProtocol::from(self.outgoing_format.load(Ordering::SeqCst))
    }

    fn send_serialized_command(
        &self,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    ) {
        let _ = self.try_send_serialized_command(tx, command_id, payload);
    }

    fn try_send_serialized_command(
        &self,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    ) -> bool {
        if self.tx_dead() {
            return false;
        }
        // Unbounded: a command is an event, and dropping it would lose it for
        // good. It is also a barrier no queued value may be replaced across.
        // A closed queue means the writer already exited (disconnect).
        self.tx.push_barrier(OutboundMessage::SerializedCommand {
            tx,
            command_id,
            payload,
        }) != Pushed::Closed
    }

    fn send_serialized_value_command(
        &self,
        key: CommandValueKey,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    ) {
        let _ = self.try_send_serialized_value_command(key, tx, command_id, payload);
    }

    fn try_send_serialized_value_command(
        &self,
        key: CommandValueKey,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    ) -> bool {
        if self.tx_dead() {
            return false;
        }
        // NOTE(ts): The newest value for a key makes a queued older one
        // redundant, so it overwrites it in place unless an event was queued
        // in between; a slow client then gets fresh values sooner and queued
        // values are bounded by keys plus queued events.
        let pushed = self.tx.push_value(
            key,
            OutboundMessage::SerializedCommand {
                tx,
                command_id,
                payload,
            },
        );
        if pushed == Pushed::Superseded {
            SUPERSEDED_VALUE_COMMANDS.fetch_add(1, Ordering::Relaxed);
            if let Some(diagnostics) = &self.drop_logger.diagnostics {
                diagnostics.superseded();
            }
        }
        pushed != Pushed::Closed
    }

    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        if self.deferred_dead() {
            return;
        }
        let result =
            self.deferred_tx
                .try_send(DeferredOutbound::Report(tx.clone(), output, Instant::now()));
        if let Some(diagnostics) = &self.drop_logger.diagnostics {
            diagnostics.produced(
                &tx,
                self.deferred_tx
                    .max_capacity()
                    .saturating_sub(self.deferred_tx.capacity()),
                result.is_ok(),
            );
        }
        if let Err(e) = result
            && !matches!(e, mpsc::error::TrySendError::Closed(_))
        {
            self.drop_logger.on_drop("ReportResponseDeferred", &e);
        }
    }

    fn send_query_response(&self, response: PendingQueryResponse, is_view: bool) {
        if self.deferred_dead() {
            return;
        }
        let tx = response.tx.clone();
        let result = self.deferred_tx.try_send(DeferredOutbound::Query {
            response,
            is_view,
            queued_at: Instant::now(),
        });
        if let Some(diagnostics) = &self.drop_logger.diagnostics {
            diagnostics.produced(
                &tx,
                self.deferred_tx
                    .max_capacity()
                    .saturating_sub(self.deferred_tx.capacity()),
                result.is_ok(),
            );
        }
        if let Err(e) = result
            && !matches!(e, mpsc::error::TrySendError::Closed(_))
        {
            self.drop_logger.on_drop("QueryResponseDeferred", &e);
        }
    }
}

#[cfg(test)]
mod delivery_timing_tests;
#[cfg(test)]
mod partition_tests;

#[cfg(test)]
mod tests {
    use std::io;

    use myko::{hyphae::Cell, query::WindowedQuerySnapshot};
    use tungstenite::{Error as WebSocketError, error::ProtocolError};

    use super::*;

    #[test]
    fn dropped_frames_and_overload_disconnects_are_counted() {
        let before = ws_delivery_counters();
        let (overload_tx, overload_rx) = watch::channel(false);
        let logger = DropLogger::new("counted".into(), overload_tx);
        logger.on_drop("test", &"queue full");
        logger.on_drop("test", &"queue full");
        logger.disconnect_for_overload();
        let after = ws_delivery_counters();

        assert!(*overload_rx.borrow());
        // Other tests in this binary may drop frames too, so these are floors.
        assert!(after.outbound_drops.saturating_sub(before.outbound_drops) >= 2);
        assert!(
            after
                .overload_disconnects
                .saturating_sub(before.overload_disconnects)
                >= 1
        );
    }

    struct SubscriptionWriter;
    impl WsWriter for SubscriptionWriter {
        fn send(&self, _message: MykoMessage) {}
        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: EncodedCommandMessage,
        ) {
        }
    }

    fn with_subscription_context<R>(
        builds: &SubscriptionBuilds,
        action: impl FnOnce(&MessageContext<'_>) -> R,
    ) -> R {
        let context = MessageContext {
            auth: &None,
            priority_tx: &mpsc::channel(1).0,
            drop_logger: &Arc::new(DropLogger::new("test".into(), watch::channel(false).0)),
            query_ids_by_tx: &Arc::default(),
            view_ids_by_tx: &Arc::default(),
            subscribe_started_by_tx: &Arc::default(),
            command_started_by_tx: &Arc::default(),
            outbound_commands_by_tx: &Arc::default(),
            command_tx: &mpsc::unbounded_channel().0,
            query_window_tx: &mpsc::unbounded_channel().0,
            subscribe_tx: &mpsc::unbounded_channel().0,
            subscription_builds: builds,
            view_delivery_tx: &mpsc::unbounded_channel().0,
            placements: &SubscriptionPlacements::new().0,
        };
        action(&context)
    }

    fn cancel_subscription(
        session: &mut ClientSession<SubscriptionWriter>,
        builds: &SubscriptionBuilds,
        message: MykoMessage,
    ) {
        with_subscription_context(builds, |context| {
            WsHandler::handle_subscription_control(session, context, message);
        });
    }

    fn install_test_subscription<W: WsWriter>(
        session: &mut ClientSession<W>,
        ready: SubscriptionReady,
    ) {
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

    fn complete_test_subscription<W: WsWriter>(
        session: &mut ClientSession<W>,
        builds: &SubscriptionBuilds,
        built: SubscriptionBuilt,
        priority: &mpsc::Sender<MykoMessage>,
        logger: &DropLogger,
    ) -> Option<Arc<str>> {
        let (tx, ready) = WsHandler::complete_subscription(builds, built, priority, logger)?;
        if let Some(ready) = ready {
            install_test_subscription(session, ready);
        }
        Some(tx)
    }

    #[derive(Clone, Default)]
    struct RecordingSubscriptionWriter(Arc<Mutex<Vec<MykoMessage>>>);

    impl WsWriter for RecordingSubscriptionWriter {
        fn send(&self, message: MykoMessage) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(message);
        }
        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: EncodedCommandMessage,
        ) {
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn synchronous_query_fallback_retires_pending_factory() {
        use myko::{
            entities::server::{GetAllServers, Server},
            wire::wrap_query,
        };

        let tx: Arc<str> = "fallback-replacement".into();
        let builds = SubscriptionBuilds::default();
        let (sender, mut completed) = mpsc::unbounded_channel();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let captured = Arc::new(());
        let released = Arc::downgrade(&captured);
        let old_tx = tx.clone();
        builds.spawn("test-fallback", tx.clone(), sender, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            drop(captured);
            Some(Ok(SubscriptionReady::Query {
                tx_id: old_tx,
                query_id: "old-query".into(),
                cellmap: hyphae::CellMap::new().lock(),
                window: None,
            }))
        });
        assert!(started_rx.await.is_ok());
        let server = crate::MykoServer::builder().build();
        let context = Arc::new(server.ctx());
        let writer = RecordingSubscriptionWriter::default();
        let mut session = ClientSession::new("test".into(), writer.clone());
        let query = wrap_query(tx.clone(), &GetAllServers {});
        assert!(query.is_ok());
        if let Ok(mut query) = query {
            query.query_id = "unregistered-fallback-query".into();
            assert!(context.handler_registry.query(&query.query_id).is_none());
            with_subscription_context(&builds, |messages| {
                WsHandler::handle_query_request(&mut session, &context, messages, query);
            });
        }
        assert!(session.has_subscription(&tx));
        // NOTE(ts): at least one, not exactly one. The server writes its own
        // `Server` row on its own schedule, which this query also reports.
        assert!(
            !writer
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "the synchronous fallback sent no initial response"
        );
        assert!(release_tx.send(()).is_ok());
        let result = tokio::time::timeout(Duration::from_secs(5), completed.recv()).await;
        assert!(matches!(result, Ok(None)));
        assert!(released.upgrade().is_none());
        assert!(
            context
                .set(&Server {
                    id: "fallback-server".into(),
                    version: "test".into(),
                    address: "127.0.0.1".into(),
                    port: 1,
                    started_at: String::new(),
                })
                .is_ok()
        );
        // NOTE(ts): wait for the update itself, not for a message count. The
        // tick queue is process-wide, so when another test is mid-drain this
        // `set` settles on that test's thread after it returns here, and the
        // server writes its own `Server` row on its own schedule.
        let carries_update = |message: &MykoMessage| {
            serde_json::to_string(message).is_ok_and(|json| json.contains("fallback-server"))
        };
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            while !writer
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(carries_update)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            delivered.is_ok(),
            "the live update never reached the client"
        );
        let messages = writer
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            messages
                .iter()
                .all(|message| matches!(message, MykoMessage::QueryResponse(_)))
        );
        drop(messages);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_factory_cannot_install_late_subscription() {
        let tx: Arc<str> = "cancelled-build".into();
        let output: Arc<dyn AnyOutput> = Arc::new(1_u64);
        let cases = [
            SubscriptionReady::Query {
                tx_id: tx.clone(),
                query_id: "test-query".into(),
                cellmap: hyphae::CellMap::new().lock(),
                window: None,
            },
            SubscriptionReady::View {
                tx_id: tx.clone(),
                view_id: "test-view".into(),
                cellmap: hyphae::CellMap::new().lock().into(),
                window: None,
            },
            SubscriptionReady::Report {
                tx_id: tx.clone(),
                report_id: "test-report".into(),
                cell: Cell::new(output).lock(),
            },
        ];
        for ready in cases {
            let cancel = match &ready {
                SubscriptionReady::Query { .. } => {
                    MykoMessage::QueryCancel(CancelSubscription { tx: tx.to_string() })
                }
                SubscriptionReady::View { .. } => {
                    MykoMessage::ViewCancel(CancelSubscription { tx: tx.to_string() })
                }
                _ => MykoMessage::ReportCancel(CancelSubscription { tx: tx.to_string() }),
            };
            let builds = SubscriptionBuilds::default();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (sender, mut completed) = mpsc::unbounded_channel();
            builds.spawn("test-cancel", tx.clone(), sender, move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
                Some(Ok(ready))
            });
            assert!(started_rx.await.is_ok());
            let mut session = ClientSession::new("test".into(), SubscriptionWriter);
            cancel_subscription(&mut session, &builds, cancel);
            assert!(release_tx.send(()).is_ok());
            let result = tokio::time::timeout(Duration::from_secs(5), completed.recv()).await;
            assert!(matches!(result, Ok(None)));
            assert!(!session.has_subscription(&tx));
        }
    }

    #[test]
    fn stale_failed_build_cannot_remove_or_error_new_owner() {
        let tx: Arc<str> = "replaced-build".into();
        let builds = SubscriptionBuilds::default();
        let old = builds.begin(tx.clone());
        let owner = builds.begin(tx.clone());
        let mut session = ClientSession::new("test".into(), SubscriptionWriter);
        let (priority, mut errors) = mpsc::channel(1);
        let logger = DropLogger::new("test".into(), watch::channel(false).0);
        let stale = SubscriptionBuilt {
            tx: tx.clone(),
            owner: old,
            result: Some(Err(MykoMessage::ViewError(ViewError::new(
                tx.to_string(),
                "test-view",
                "old failure",
            )))),
        };
        assert!(
            complete_test_subscription(&mut session, &builds, stale, &priority, &logger).is_none()
        );
        assert!(errors.try_recv().is_err());
        let current = SubscriptionBuilt {
            tx: tx.clone(),
            owner,
            result: Some(Ok(SubscriptionReady::Query {
                tx_id: tx.clone(),
                query_id: "test-query".into(),
                cellmap: hyphae::CellMap::new().lock(),
                window: None,
            })),
        };
        assert!(
            complete_test_subscription(&mut session, &builds, current, &priority, &logger)
                .is_some()
        );
        assert!(session.has_subscription(&tx));
    }

    #[tokio::test]
    async fn cancellation_fences_completion_already_queued_for_installation() {
        let tx: Arc<str> = "queued-completion".into();
        let builds = SubscriptionBuilds::default();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let completed_tx = tx.clone();
        builds.spawn("test-queued", tx.clone(), sender, move || {
            Some(Ok(SubscriptionReady::Query {
                tx_id: completed_tx,
                query_id: "test-query".into(),
                cellmap: hyphae::CellMap::new().lock(),
                window: None,
            }))
        });
        let completed = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await;
        assert!(matches!(completed, Ok(Some(_))));
        if let Ok(Some(built)) = completed {
            let mut session = ClientSession::new("test".into(), SubscriptionWriter);
            cancel_subscription(
                &mut session,
                &builds,
                MykoMessage::QueryCancel(CancelSubscription { tx: tx.to_string() }),
            );
            let logger = DropLogger::new("test".into(), watch::channel(false).0);
            assert!(
                complete_test_subscription(
                    &mut session,
                    &builds,
                    built,
                    &mpsc::channel(1).0,
                    &logger
                )
                .is_none()
            );
            assert!(!session.has_subscription(&tx));
        }
    }

    #[tokio::test]
    async fn panicking_factory_releases_pending_owner() {
        let tx: Arc<str> = "panicking-build".into();
        let builds = SubscriptionBuilds::default();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        builds.spawn("test-panic", tx.clone(), sender, || {
            std::panic::resume_unwind(Box::new("factory panic"));
        });
        let completed = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await;
        assert!(matches!(completed, Ok(Some(_))));
        if let Ok(Some(built)) = completed {
            let owner = Arc::downgrade(&built.owner);
            let mut session = ClientSession::new("test".into(), SubscriptionWriter);
            let logger = DropLogger::new("test".into(), watch::channel(false).0);
            assert!(
                complete_test_subscription(
                    &mut session,
                    &builds,
                    built,
                    &mpsc::channel(1).0,
                    &logger
                )
                .is_some()
            );
            assert!(owner.upgrade().is_none());
            assert!(!session.has_subscription(&tx));
        }
    }

    #[test]
    fn failed_build_releases_pending_owner_and_delivers_current_view_error() {
        let tx: Arc<str> = "failed-build".into();
        let builds = SubscriptionBuilds::default();
        let owner = builds.begin(tx.clone());
        let retained = Arc::downgrade(&owner);
        let mut session = ClientSession::new("test".into(), SubscriptionWriter);
        let (priority, mut errors) = mpsc::channel(1);
        let logger = DropLogger::new("test".into(), watch::channel(false).0);
        let failed = SubscriptionBuilt {
            tx: tx.clone(),
            owner,
            result: Some(Err(MykoMessage::ViewError(ViewError::new(
                tx.to_string(),
                "test-view",
                "current failure",
            )))),
        };
        assert!(
            complete_test_subscription(&mut session, &builds, failed, &priority, &logger).is_some()
        );
        assert!(matches!(errors.try_recv(), Ok(MykoMessage::ViewError(_))));
        assert!(retained.upgrade().is_none());
        assert!(!session.has_subscription(&tx));
    }

    #[test]
    fn classifies_routine_websocket_disconnects() {
        assert!(is_routine_websocket_disconnect(
            &WebSocketError::ConnectionClosed
        ));
        assert!(is_routine_websocket_disconnect(
            &WebSocketError::AlreadyClosed
        ));
        assert!(is_routine_websocket_disconnect(&WebSocketError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake
        )));
        assert!(is_routine_websocket_disconnect(&WebSocketError::Io(
            io::Error::from(io::ErrorKind::ConnectionReset)
        )));

        assert!(!is_routine_websocket_disconnect(&WebSocketError::Protocol(
            ProtocolError::InvalidOpcode(3)
        )));
        assert!(!is_routine_websocket_disconnect(&WebSocketError::Io(
            io::Error::from(io::ErrorKind::PermissionDenied)
        )));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_window_worker_runs_callbacks_outside_tokio_in_fifo_order() {
        let snapshots = Cell::new(Arc::new(WindowedQuerySnapshot {
            entries: Vec::new(),
            total_count: 0,
            window: Some(QueryWindow {
                offset: 0,
                limit: 24,
            }),
        }));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_source = observed.clone();
        let source = WindowedQuerySource::new(snapshots.lock(), move |window| {
            let runtime = tokio::runtime::Builder::new_current_thread().build();
            assert!(runtime.is_ok());
            let Ok(runtime) = runtime else {
                return;
            };
            runtime.block_on(async {});
            if let Some(window) = window {
                observed_for_source
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(window.offset);
            }
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(WsHandler::run_query_window_worker(rx));

        for offset in [24, 48] {
            assert!(
                tx.send(QueryWindowJob {
                    tx_id: "history-query".into(),
                    source: source.clone(),
                    window: Some(QueryWindow { offset, limit: 24 }),
                })
                .is_ok()
            );
        }
        drop(tx);
        assert!(worker.await.is_ok());

        assert_eq!(
            *observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![24, 48]
        );
    }

    #[test]
    fn test_channel_writer() {
        let (tx, rx) = outbound_queue::channel();
        let (deferred_tx, _deferred_rx) = mpsc::channel(10);
        let (overload_tx, _overload_rx) = watch::channel(false);
        let drop_logger = Arc::new(DropLogger::new("test-client".into(), overload_tx));
        let writer = ChannelWriter {
            tx,
            queued_messages: Arc::default(),
            deferred_tx,
            drop_logger,
            outgoing_format: Arc::new(AtomicU8::new(MykoProtocol::JSON.into())),
        };

        let msg = MykoMessage::Ping(myko::wire::PingData {
            id: "test".to_string(),
            timestamp: 0,
        });
        writer.send(msg);

        let received = rx.try_recv();
        assert!(received.is_some(), "writer must enqueue ping");
        let Some(received) = received else {
            return;
        };
        assert!(matches!(
            received,
            OutboundMessage::Message(MykoMessage::Ping(_))
        ));
    }

    fn bounded_test_writer() -> (
        ChannelWriter,
        outbound_queue::Receiver<OutboundMessage>,
        watch::Receiver<bool>,
    ) {
        let (tx, rx) = outbound_queue::channel();
        let (deferred_tx, _deferred_rx) = mpsc::channel(1);
        let (overload_tx, overload_rx) = watch::channel(false);
        let writer = ChannelWriter {
            tx,
            queued_messages: Arc::default(),
            deferred_tx,
            drop_logger: Arc::new(DropLogger::new("test-client".into(), overload_tx)),
            outgoing_format: Arc::new(AtomicU8::new(MykoProtocol::JSON.into())),
        };
        (writer, rx, overload_rx)
    }

    fn ping() -> MykoMessage {
        MykoMessage::Ping(myko::wire::PingData {
            id: "test".to_string(),
            timestamp: 0,
        })
    }

    #[test]
    fn commands_past_the_message_bound_are_all_queued_in_order() {
        let (writer, rx, overload_rx) = bounded_test_writer();
        let commands = MESSAGE_QUEUE_CAPACITY * 2;
        for sequence in 0..commands {
            writer.send_serialized_command(
                "tx".into(),
                sequence.to_string(),
                EncodedCommandMessage::Json(String::new()),
            );
        }
        assert!(
            !*overload_rx.borrow(),
            "a command burst must not tear down the session"
        );
        // Commands do not consume the message bound.
        writer.send(ping());
        assert!(!*overload_rx.borrow());
        for sequence in 0..commands {
            let received = rx.try_recv();
            assert!(
                matches!(
                    &received,
                    Some(OutboundMessage::SerializedCommand { command_id, .. })
                        if *command_id == sequence.to_string()
                ),
                "command {sequence} missing or out of order"
            );
        }
        assert!(matches!(rx.try_recv(), Some(OutboundMessage::Message(_))));
    }

    #[test]
    fn slow_reader_gets_every_event_once_and_the_latest_value_per_key() {
        let (writer, rx, overload_rx) = bounded_test_writer();
        let keys: Vec<_> = (0..10)
            .map(|k| CommandValueKey::new(format!("k{k}")))
            .collect();
        let frame_id = |frame: &OutboundMessage| match frame {
            OutboundMessage::SerializedCommand {
                command_id,
                payload: EncodedCommandMessage::Json(round),
                ..
            } => format!("{command_id}@{round}"),
            _ => String::from("unexpected frame"),
        };
        let before = ws_superseded_value_commands();
        let mut sent = Vec::new();
        let mut received = Vec::new();
        let mut peak = 0;
        for round in 0..10_000 {
            for (k, key) in keys.iter().enumerate() {
                let payload = EncodedCommandMessage::Json(round.to_string());
                sent.push(format!("v{k}@{round}"));
                writer.send_serialized_value_command(
                    key.clone(),
                    "tx".into(),
                    format!("v{k}"),
                    payload,
                );
            }
            if round % 50 == 0 {
                sent.push(format!("e{round}@-"));
                let payload = EncodedCommandMessage::Json("-".into());
                writer.send_serialized_command("tx".into(), format!("e{round}"), payload);
            }
            peak = peak.max(rx.len());
            // The socket writer is blocked; it takes one frame every few rounds.
            if round % 7 == 0
                && let Some(frame) = rx.try_recv()
            {
                received.push(frame_id(&frame));
            }
        }
        received.extend(std::iter::from_fn(|| rx.try_recv()).map(|frame| frame_id(&frame)));
        assert!(!*overload_rx.borrow());

        // Values are bounded by keys per event interval, not by sends.
        assert!(peak <= (10_000 / 50 + 1) * (keys.len() + 1), "peak {peak}");
        // Delivery is the sent order with superseded values removed.
        let mut sent_iter = sent.iter();
        for frame in &received {
            assert!(sent_iter.any(|s| s == frame), "{frame} out of order");
        }
        let events: Vec<_> = received
            .iter()
            .filter(|f| f.starts_with('e'))
            .cloned()
            .collect();
        let sent_events: Vec<_> = sent
            .iter()
            .filter(|f| f.starts_with('e'))
            .cloned()
            .collect();
        assert_eq!(events, sent_events);
        for k in 0..keys.len() {
            let last = received
                .iter()
                .rev()
                .find(|f| f.starts_with(&format!("v{k}@")));
            assert_eq!(
                last.map(String::as_str),
                Some(format!("v{k}@9999").as_str())
            );
        }
        assert!(ws_superseded_value_commands() > before);
    }

    #[derive(serde::Serialize)]
    struct Probe;

    impl myko::command::CommandId for Probe {
        fn command_id(&self) -> Arc<str> {
            "Probe".into()
        }
    }

    #[test]
    fn registry_sends_report_false_once_the_client_queue_is_closed() {
        let (writer, rx, _overload_rx) = bounded_test_writer();
        let registry = myko::server::client_registry();
        let client_id = Uuid::new_v4().to_string();
        registry.register(client_id.as_str().into(), Arc::new(writer));
        let request = myko::command::CommandRequest::new(Probe);
        let key = CommandValueKey::new("probe");

        assert!(registry.send_command_request_to(&client_id, &request));
        assert!(registry.send_value_command_request_to(&client_id, key.clone(), &request));
        assert_eq!(rx.len(), 2);

        // The writer exited but the registry entry is still there.
        drop(rx);
        assert!(!registry.send_command_request_to(&client_id, &request));
        assert!(!registry.send_value_command_request_to(&client_id, key, &request));
        registry.unregister(&client_id);
    }

    #[test]
    fn teardown_counts_queued_events_and_values_it_discards() {
        let (writer, rx, _overload_rx) = bounded_test_writer();
        let receiver = OutboundReceiver {
            rx,
            queued_messages: Arc::default(),
            diagnostics: None,
        };
        let command = |id: &str| {
            (
                Arc::<str>::from("tx"),
                id.to_string(),
                EncodedCommandMessage::Json("{}".into()),
            )
        };
        writer.send(ping());
        for id in ["e1", "e2", "e3"] {
            let (tx, id, payload) = command(id);
            writer.send_serialized_command(tx, id, payload);
        }
        for k in ["a", "a", "b"] {
            let (tx, id, payload) = command(k);
            writer.send_serialized_value_command(CommandValueKey::new(k), tx, id, payload);
        }
        // The first event was written before the connection went away.
        let _ = receiver.rx.try_recv();
        let _ = receiver.rx.try_recv();

        let events = ws_teardown_discarded_event_commands();
        let values = ws_teardown_discarded_value_commands();
        drop(receiver);
        // Other tests in this binary may tear down queues too: floors.
        assert!(ws_teardown_discarded_event_commands().saturating_sub(events) >= 2);
        assert!(ws_teardown_discarded_value_commands().saturating_sub(values) >= 2);
        let (tx, id, payload) = command("late");
        assert!(!writer.try_send_serialized_command(tx, id, payload));
    }

    #[test]
    fn teardown_with_nothing_queued_counts_nothing() {
        let (_writer, rx, _overload_rx) = bounded_test_writer();
        let receiver = OutboundReceiver {
            rx,
            queued_messages: Arc::default(),
            diagnostics: None,
        };
        let events = ws_teardown_discarded_event_commands();
        let values = ws_teardown_discarded_value_commands();
        drop(receiver);
        assert_eq!(ws_teardown_discarded_event_commands(), events);
        assert_eq!(ws_teardown_discarded_value_commands(), values);
    }

    #[test]
    fn messages_past_the_bound_mark_connection_for_resnapshot() {
        let (writer, _rx, overload_rx) = bounded_test_writer();
        for _ in 0..MESSAGE_QUEUE_CAPACITY {
            writer.send(ping());
        }
        assert!(!*overload_rx.borrow());
        writer.send(ping());
        assert!(
            *overload_rx.borrow(),
            "message overflow must force a resnapshot"
        );
    }

    #[test]
    fn full_deferred_queue_marks_connection_for_resnapshot() {
        let (tx, _rx) = outbound_queue::channel();
        let (deferred_tx, mut deferred_rx) = mpsc::channel(1);
        let (overload_tx, overload_rx) = watch::channel(false);
        let drop_logger = Arc::new(DropLogger::new("test-client".into(), overload_tx));
        let writer = ChannelWriter {
            tx,
            queued_messages: Arc::default(),
            deferred_tx,
            drop_logger,
            outgoing_format: Arc::new(AtomicU8::new(MykoProtocol::JSON.into())),
        };
        let response = |sequence| PendingQueryResponse {
            tx: "query-tx".into(),
            sequence,
            upsert_items: Vec::new(),
            deletes: Vec::new(),
            total_count: 0,
            window: None,
            window_order_ids: None,
        };

        writer.send_query_response(response(0), false);
        assert!(!*overload_rx.borrow());
        writer.send_query_response(response(1), false);

        assert!(
            *overload_rx.borrow(),
            "dropping a sequenced response must force reconnect/resnapshot"
        );
        assert!(matches!(
            deferred_rx.try_recv(),
            Ok(DeferredOutbound::Query {
                response: PendingQueryResponse { sequence: 0, .. },
                is_view: false,
                ..
            })
        ));
    }

    #[test]
    fn command_completion_follows_causally_prior_query_response() {
        let (deferred_tx, mut deferred_rx) = mpsc::channel(2);
        let query = PendingQueryResponse {
            tx: "query-tx".into(),
            sequence: 1,
            upsert_items: Vec::new(),
            deletes: Vec::new(),
            total_count: 0,
            window: None,
            window_order_ids: None,
        };
        let command = MykoMessage::CommandResponse(CommandResponse {
            response: serde_json::Value::Null,
            tx: "command-tx".to_string(),
        });

        assert!(
            deferred_tx
                .try_send(DeferredOutbound::Query {
                    response: query,
                    is_view: false,
                    queued_at: Instant::now(),
                })
                .is_ok(),
            "query response must enqueue"
        );
        assert!(
            deferred_tx
                .try_send(DeferredOutbound::CommandResult(command))
                .is_ok(),
            "command completion must enqueue"
        );

        assert!(matches!(
            deferred_rx.try_recv(),
            Ok(DeferredOutbound::Query {
                response: PendingQueryResponse { sequence: 1, .. },
                ..
            })
        ));
        assert!(matches!(
            deferred_rx.try_recv(),
            Ok(DeferredOutbound::CommandResult(
                MykoMessage::CommandResponse(CommandResponse { tx, .. })
            )) if tx == "command-tx"
        ));
    }

    #[test]
    fn outgoing_format_starts_as_json_and_promotes_to_cbor() {
        use std::sync::atomic::{AtomicU8, Ordering};

        let outgoing_format = AtomicU8::new(MykoProtocol::JSON.into());

        // Initially JSON.
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::JSON,
        );

        // Simulate receiving a binary frame: promote.
        outgoing_format.store(MykoProtocol::CBOR.into(), Ordering::SeqCst);
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::CBOR,
        );

        // Simulate receiving more text frames after promotion: no change.
        // (The handler in the read loop only writes on Binary, never on Text,
        // so this is a no-op assertion that the field's last-write-wins
        // semantics give us stickiness for free.)
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::CBOR,
        );
    }
}
