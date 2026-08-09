//! WebSocket handler for the cell-based server.
//!
//! Handles WebSocket connections using `ClientSession` for subscription management.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use hyphae::SelectExt;
use myko::{
    WS_MAX_FRAME_SIZE_BYTES, WS_MAX_MESSAGE_SIZE_BYTES,
    client::MykoProtocol,
    command::{CommandContext, CommandHandlerRegistration},
    entities::client::{Client, ClientId},
    relationship::{
        iter_client_id_registrations, iter_fallback_to_id_registrations,
        iter_server_owned_registrations,
    },
    report::AnyOutput,
    request::RequestContext,
    server::{
        ClientSession, MykoServerContext, PendingQueryResponse, WsWriter,
        client_registry::try_client_registry,
    },
    wire::{
        CancelSubscription, CommandError, CommandResponse, EncodedCommandMessage, MEvent,
        MEventType, MykoMessage, QueryWindowUpdate, ViewError, ViewWindowUpdate, WrappedQuery,
        WrappedView,
    },
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::interval,
};
use tokio_tungstenite::{
    accept_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use uuid::Uuid;

struct WsBenchmarkStats {
    message_count: AtomicU64,
    total_bytes: AtomicU64,
}

static WS_BENCHMARK_STATS: OnceLock<Arc<WsBenchmarkStats>> = OnceLock::new();
static WS_BENCHMARK_LOGGER_STARTED: AtomicBool = AtomicBool::new(false);

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

/// Per-connection drop tracking to avoid log storms when clients fall behind.
///
/// When the outbound channel is full, we will drop messages (same as today),
/// but we must not `warn!` for every drop or we can effectively `DoS` ourselves.
struct DropLogger {
    client_id: Arc<str>,
    dropped: std::sync::atomic::AtomicU64,
    last_log_ms: std::sync::atomic::AtomicU64,
    overload_tx: watch::Sender<bool>,
}

impl DropLogger {
    const fn new(client_id: Arc<str>, overload_tx: watch::Sender<bool>) -> Self {
        Self {
            client_id,
            dropped: std::sync::atomic::AtomicU64::new(0),
            last_log_ms: std::sync::atomic::AtomicU64::new(0),
            overload_tx,
        }
    }

    fn on_drop(&self, kind: &'static str, err: &dyn std::fmt::Display) {
        use std::sync::atomic::Ordering;

        self.dropped.fetch_add(1, Ordering::Relaxed);

        // A dropped sequenced diff cannot be repaired while keeping this
        // session alive: every later sequence would be based on state the
        // client never received. Wake the read loop so it tears down the
        // connection; reconnecting clients then resubscribe and receive a
        // fresh sequence-0 snapshot. `send_replace` is synchronous, so this
        // remains safe to call from reactive callbacks.
        self.overload_tx.send_replace(true);

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
            "WebSocket send buffer full; dropped {} message(s) for client {} (latest: {}): {}",
            n,
            self.client_id,
            kind,
            err
        );
    }
}

struct CommandJob {
    tx_id: Arc<str>,
    command_id: String,
    command: serde_json::Value,
    received_at: Instant,
}

/// Result of an async subscription build (query or view `cell_factory`).
enum SubscriptionReady {
    Query {
        tx_id: Arc<str>,
        query_id: Arc<str>,
        cellmap: hyphae::CellMap<Arc<str>, Arc<dyn myko::item::AnyItem>, hyphae::CellImmutable>,
        window: Option<myko::wire::QueryWindow>,
    },
    View {
        tx_id: Arc<str>,
        view_id: Arc<str>,
        cellmap: hyphae::CellMap<Arc<str>, Arc<dyn myko::item::AnyItem>, hyphae::CellImmutable>,
        window: Option<myko::wire::QueryWindow>,
    },
}

type SharedTxNames = Arc<Mutex<HashMap<Arc<str>, Arc<str>>>>;
type SharedTxTimes = Arc<Mutex<HashMap<Arc<str>, Instant>>>;
type SharedOutboundCommands = Arc<Mutex<HashMap<String, (String, Instant)>>>;

struct MessageContext<'a> {
    priority_tx: &'a mpsc::Sender<MykoMessage>,
    drop_logger: &'a Arc<DropLogger>,
    query_ids_by_tx: &'a SharedTxNames,
    view_ids_by_tx: &'a SharedTxNames,
    subscribe_started_by_tx: &'a SharedTxTimes,
    command_started_by_tx: &'a SharedTxTimes,
    outbound_commands_by_tx: &'a SharedOutboundCommands,
    command_tx: &'a mpsc::UnboundedSender<CommandJob>,
    subscribe_tx: &'a mpsc::UnboundedSender<SubscriptionReady>,
}

struct ReadLoopState {
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
    subscribe_tx: mpsc::UnboundedSender<SubscriptionReady>,
    overload_rx: watch::Receiver<bool>,
}

struct WriterState {
    write: futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, Message>,
    rx: mpsc::Receiver<OutboundMessage>,
    deferred_rx: mpsc::Receiver<DeferredOutbound>,
    priority_rx: mpsc::Receiver<MykoMessage>,
    ctx: Arc<MykoServerContext>,
    client_id: Arc<str>,
    addr: SocketAddr,
    outgoing_format: Arc<AtomicU8>,
    outbound_commands: SharedOutboundCommands,
    outbound_failure_tx: watch::Sender<bool>,
}

enum OutboundMessage {
    Message(MykoMessage),
    SerializedCommand {
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    },
}

enum DeferredOutbound {
    Report(Arc<str>, Arc<dyn AnyOutput>),
    Query {
        response: PendingQueryResponse,
        is_view: bool,
    },
}

/// WebSocket handler for a single client connection.
pub struct WsHandler;

impl WsHandler {
    fn cleanup_connection<W: WsWriter>(
        session: ClientSession<W>,
        ctx: &MykoServerContext,
        client_entity: &Client,
        client_id: Arc<str>,
        addr: SocketAddr,
        tasks: (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>),
    ) {
        let (write_task, command_task) = tasks;
        write_task.abort();
        command_task.abort();
        if let Some(registry) = try_client_registry() {
            registry.unregister(&client_id);
        }
        let drop_client_id = client_id.clone();
        std::mem::drop(tokio::task::spawn_blocking(move || {
            drop(session);
            tracing::trace!(
                "Client session subscriptions torn down for {}",
                drop_client_id
            );
        }));
        if let Err(error) = ctx.del(client_entity) {
            tracing::error!("Failed to delete client entity: {error}");
        }
        tracing::info!("Client disconnected: {} from {}", client_id, addr);
        drop(client_id);
    }

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

    fn dispatch_incoming<W: WsWriter>(
        session: &mut ClientSession<W>,
        state: &ReadLoopState,
        message: MykoMessage,
    ) {
        Self::handle_message(
            session,
            state.ctx.clone(),
            &MessageContext {
                priority_tx: &state.priority_tx,
                drop_logger: &state.drop_logger,
                query_ids_by_tx: &state.query_ids_by_tx,
                view_ids_by_tx: &state.view_ids_by_tx,
                subscribe_started_by_tx: &state.subscribe_started_by_tx,
                command_started_by_tx: &state.command_started_by_tx,
                outbound_commands_by_tx: &state.outbound_commands_by_tx,
                command_tx: &state.command_tx,
                subscribe_tx: &state.subscribe_tx,
            },
            message,
        );
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
                        Self::dispatch_incoming(session, state, message);
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
                    Self::dispatch_incoming(session, state, message);
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

    async fn run_read_loop<W: WsWriter>(
        mut read: futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<TcpStream>>,
        session: &mut ClientSession<W>,
        mut subscribe_rx: mpsc::UnboundedReceiver<SubscriptionReady>,
        state: ReadLoopState,
    ) {
        let mut outbound_ttl_interval = interval(Duration::from_secs(10));
        outbound_ttl_interval.tick().await; // NOTE(ts): consume the immediate first tick
        let mut overload_rx = state.overload_rx.clone();
        loop {
            tokio::select! {
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
                Some(ready) = subscribe_rx.recv() => {
                    let tx_id = match &ready {
                        SubscriptionReady::Query { tx_id, .. }
                        | SubscriptionReady::View { tx_id, .. } => tx_id.clone(),
                    };
                    if let Ok(mut map) = state.subscribe_started_by_tx.lock() {
                        map.remove(&tx_id);
                    }
                    match ready {
                        SubscriptionReady::Query { tx_id, query_id, cellmap, window } => {
                            session.subscribe_query(tx_id, query_id, cellmap, window);
                        }
                        SubscriptionReady::View { tx_id, view_id, cellmap, window } => {
                            session.subscribe_view_with_id(tx_id, view_id, cellmap, window);
                        }
                    }
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
                            tracing::error!("WebSocket read error from {}: {}", state.client_id, e);
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
        command_priority_tx: mpsc::Sender<MykoMessage>,
        command_drop_logger: Arc<DropLogger>,
        command_client_id: Arc<str>,
        command_started_cleanup: SharedTxTimes,
    ) {
        while let Some(job) = command_rx.recv().await {
            let command_ctx = command_ctx.clone();
            let command_priority_tx = command_priority_tx.clone();
            let command_drop_logger = command_drop_logger.clone();
            let command_client_id = command_client_id.clone();
            let tx_id = job.tx_id.clone();
            let started_map = command_started_cleanup.clone();
            match tokio::task::spawn_blocking(move || {
                Self::execute_command_job(
                    command_ctx,
                    &command_priority_tx,
                    command_drop_logger.as_ref(),
                    command_client_id,
                    job,
                );
            })
            .await
            {
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

    fn outbound_metadata(
        message: &OutboundMessage,
        client_id: &str,
    ) -> (&'static str, Option<Arc<str>>, Option<u64>) {
        match message {
            OutboundMessage::SerializedCommand { tx, command_id, .. } => {
                crate::ws_timing::record_outbound_for_client(
                    "Command",
                    client_id,
                    Some(command_id),
                );
                ("command", Some(tx.clone()), None)
            }
            OutboundMessage::Message(message) => {
                crate::ws_timing::record_outbound_for_client(
                    crate::ws_timing::message_kind(message),
                    client_id,
                    crate::ws_timing::message_tag(message),
                );
                match message {
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
                }
            }
        }
    }

    fn track_outbound_command(message: &OutboundMessage, commands: &SharedOutboundCommands) {
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

    async fn send_outbound(
        write: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<TcpStream>,
            Message,
        >,
        message: OutboundMessage,
        client_id: &str,
        addr: SocketAddr,
        outgoing_format: &AtomicU8,
        commands: &SharedOutboundCommands,
    ) -> bool {
        let (kind, tx, sequence) = Self::outbound_metadata(&message, client_id);
        Self::track_outbound_command(&message, commands);
        let Some(message) = Self::serialize_outbound(&message, outgoing_format) else {
            // A serialization failure is just as lossy as a full queue. Stop
            // this session rather than allowing later sequenced diffs through.
            return false;
        };
        let payload_bytes = match &message {
            Message::Binary(bytes) => bytes.len(),
            Message::Text(text) => text.len(),
            _ => 0,
        };
        if let Err(error) = write.send(message).await {
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
        true
    }

    async fn run_writer(state: WriterState) {
        let WriterState {
            mut write,
            mut rx,
            mut deferred_rx,
            mut priority_rx,
            ctx: _ctx,
            client_id: write_client_id,
            addr: write_addr,
            outgoing_format: outgoing_format_writer,
            outbound_commands: outbound_commands_by_tx_writer,
            outbound_failure_tx,
        } = state;
        let mut normal_open = true;
        let mut priority_open = true;
        let mut deferred_open = true;
        while normal_open || priority_open || deferred_open {
            let msg = tokio::select! {
                biased;
                maybe = priority_rx.recv(), if priority_open => {
                    if let Some(msg) = maybe { OutboundMessage::Message(msg) } else {
                        priority_open = false;
                        continue;
                    }
                }
                maybe = deferred_rx.recv(), if deferred_open => {
                    match maybe {
                        Some(DeferredOutbound::Report(tx, output)) => {
                            OutboundMessage::Message(MykoMessage::ReportResponse(myko::wire::ReportResponse {
                                response: output.to_value(),
                                tx: tx.to_string(),
                            }))
                        }
                        Some(DeferredOutbound::Query { response, is_view }) => {
                            if is_view {
                                OutboundMessage::Message(MykoMessage::ViewResponse(response.into_wire()))
                            } else {
                                OutboundMessage::Message(MykoMessage::QueryResponse(response.into_wire()))
                            }
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
            )
            .await
            {
                // Wake the read half so connection cleanup drops both halves.
                // Reconnecting clients will resubscribe from a fresh snapshot.
                outbound_failure_tx.send_replace(true);
                break;
            }
        }
        // NOTE(ts): Unregister from client registry immediately so the node
        // executor stops serializing commands into a dead channel.
        if let Some(registry) = try_client_registry() {
            registry.unregister(&write_client_id);
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
        let (write, read) = ws_stream.split();

        // Create a bounded channel for sending messages to the client
        // High limit (10k) since we have good memory availability
        let (tx, rx) = mpsc::channel::<OutboundMessage>(10_000);
        let (deferred_tx, deferred_rx) = mpsc::channel::<DeferredOutbound>(10_000);
        let (priority_tx, priority_rx) = mpsc::channel::<MykoMessage>(1_000);
        let (command_tx, command_rx) = mpsc::unbounded_channel::<CommandJob>();
        let (subscribe_tx, subscribe_rx) = mpsc::unbounded_channel::<SubscriptionReady>();
        let (overload_tx, overload_rx) = watch::channel(false);

        // Outgoing format for this session: defaults to JSON, sticky-promotes
        // to CBOR on the first received binary frame. Never demotes.
        let outgoing_format = Arc::new(AtomicU8::new(MykoProtocol::JSON.into()));

        // Create client session with channel-based writer
        let client_id: Arc<str> = Uuid::new_v4().to_string().into();
        let drop_logger = Arc::new(DropLogger::new(client_id.clone(), overload_tx));
        let writer = ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            outgoing_format: outgoing_format.clone(),
        };

        // Register writer in the global client registry (if initialized)
        let writer_arc: Arc<dyn WsWriter> = Arc::new(ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            outgoing_format: outgoing_format.clone(),
        });
        if let Some(registry) = try_client_registry() {
            registry.register(client_id.clone(), writer_arc);
        }

        let mut session = ClientSession::new(client_id.clone(), writer);

        let outgoing_format_writer = outgoing_format.clone();
        let query_ids_by_tx: Arc<Mutex<HashMap<Arc<str>, Arc<str>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let view_ids_by_tx: Arc<Mutex<HashMap<Arc<str>, Arc<str>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let subscribe_started_by_tx: Arc<Mutex<HashMap<Arc<str>, Instant>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let command_started_by_tx: Arc<Mutex<HashMap<Arc<str>, Instant>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let outbound_commands_by_tx: Arc<Mutex<HashMap<String, (String, Instant)>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let outbound_commands_by_tx_writer = outbound_commands_by_tx.clone();

        let client_entity = Self::publish_client(&ctx, client_id.clone(), addr);

        let write_ctx = ctx.clone();
        let write_client_id = client_id.clone();
        let write_addr = addr;
        let command_ctx = ctx.clone();
        let command_priority_tx = priority_tx.clone();
        let command_drop_logger = drop_logger.clone();
        let command_client_id = client_id.clone();

        let write_task = tokio::spawn(Self::run_writer(WriterState {
            write,
            rx,
            deferred_rx,
            priority_rx,
            ctx: write_ctx,
            client_id: write_client_id,
            addr: write_addr,
            outgoing_format: outgoing_format_writer,
            outbound_commands: outbound_commands_by_tx_writer,
            outbound_failure_tx: drop_logger.overload_tx.clone(),
        }));

        let command_task = tokio::spawn(Self::run_command_worker(
            command_rx,
            command_ctx,
            command_priority_tx,
            command_drop_logger,
            command_client_id,
            command_started_by_tx.clone(),
        ));

        Self::run_read_loop(
            read,
            &mut session,
            subscribe_rx,
            ReadLoopState {
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
                subscribe_tx: subscribe_tx.clone(),
                overload_rx,
            },
        )
        .await;

        Self::cleanup_connection(
            session,
            &ctx,
            &client_entity,
            client_id,
            addr,
            (write_task, command_task),
        );

        Ok(())
    }

    fn handle_query_request<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<MykoServerContext>,
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
        let request = Arc::new(RequestContext::from_client(
            tx_id.clone(),
            session.client_id.clone(),
            ctx.host_id,
        ));
        if let Some(query_data) = ctx.handler_registry.query(&query_id) {
            match (query_data.parse)(wrapped.query.clone()) {
                Ok(query) => {
                    let registry = ctx.registry.clone();
                    let factory = query_data.cell_factory;
                    let window = wrapped.window;
                    let sender = message_context.subscribe_tx.clone();
                    tokio::task::spawn_blocking(move || {
                        match factory(query, registry, request, Some(ctx)) {
                            Ok(cellmap) => {
                                let _ = sender.send(SubscriptionReady::Query {
                                    tx_id,
                                    query_id,
                                    cellmap,
                                    window,
                                });
                            }
                            Err(error) => tracing::error!(
                                "Failed to create query cell for {}: {}",
                                query_id,
                                error
                            ),
                        }
                    });
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
            let cellmap = hyphae::MapQuery::materialize(store.select(|_| true));
            session.subscribe_query(tx_id, query_id, cellmap, wrapped.window);
            drop(ctx);
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
        if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
            map.entry(tx_id.clone()).or_insert_with(Instant::now);
        }
        let request = Arc::new(RequestContext::from_client(
            tx_id.clone(),
            session.client_id.clone(),
            ctx.host_id,
        ));
        let Some(view_data) = ctx.handler_registry.view(&view_id) else {
            let message = format!("No registered handler for view: {view_id}");
            Self::send_view_error(message_context, &tx_id, &view_id, message);
            drop(ctx);
            return;
        };
        match (view_data.parse)(wrapped.view.clone()) {
            Ok(view) => {
                let factory = view_data.cell_factory;
                let registry = ctx.registry.clone();
                let window = wrapped.window;
                let sender = message_context.subscribe_tx.clone();
                let priority = message_context.priority_tx.clone();
                let logger = message_context.drop_logger.clone();
                tokio::task::spawn_blocking(move || match factory(view, registry, request, ctx) {
                    Ok(cellmap) => {
                        let _ = sender.send(SubscriptionReady::View {
                            tx_id,
                            view_id,
                            cellmap,
                            window,
                        });
                    }
                    Err(error) => {
                        let message = MykoMessage::ViewError(ViewError::new(
                            tx_id.to_string(),
                            view_id.to_string(),
                            error,
                        ));
                        if let Err(error) = priority.try_send(message) {
                            logger.on_drop("ViewError", &error);
                        }
                    }
                });
            }
            Err(error) => Self::send_view_error(
                message_context,
                &tx_id,
                &view_id,
                format!("Failed to parse view {view_id}: {error}"),
            ),
        }
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
        crate::ws_timing::record_inbound_for_client(
            crate::ws_timing::message_kind(&msg),
            &session.client_id,
            crate::ws_timing::message_tag(&msg),
        );
        match msg {
            MykoMessage::Query(wrapped) => {
                Self::handle_query_request(session, ctx, message_context, wrapped);
            }
            MykoMessage::View(wrapped) => {
                Self::handle_view_request(session, ctx, message_context, wrapped);
            }
            message @ (MykoMessage::QueryCancel(_)
            | MykoMessage::QueryWindow(_)
            | MykoMessage::ViewCancel(_)
            | MykoMessage::ViewWindow(_)
            | MykoMessage::ReportCancel(_)) => {
                Self::handle_subscription_control(session, message_context, message);
                drop(ctx);
            }
            message @ (MykoMessage::Report(_)
            | MykoMessage::Event(_)
            | MykoMessage::EventBatch(_)) => {
                Self::handle_report_or_event(session, ctx, message);
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

    fn handle_subscription_control<W: WsWriter>(
        session: &mut ClientSession<W>,
        message_context: &MessageContext<'_>,
        message: MykoMessage,
    ) {
        match message {
            MykoMessage::QueryCancel(CancelSubscription { tx }) => {
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
                session.update_query_window(&Arc::from(tx), window);
            }
            MykoMessage::ViewCancel(CancelSubscription { tx }) => {
                let tx: Arc<str> = tx.into();
                if let Ok(mut map) = message_context.view_ids_by_tx.lock() {
                    map.remove(&tx);
                }
                if let Ok(mut map) = message_context.subscribe_started_by_tx.lock() {
                    map.remove(&tx);
                }
                session.cancel(&tx);
            }
            MykoMessage::ViewWindow(ViewWindowUpdate { tx, window }) => {
                session.update_view_window(&Arc::from(tx), window);
            }
            MykoMessage::ReportCancel(CancelSubscription { tx }) => {
                session.cancel(&Arc::from(tx));
            }
            _ => {}
        }
    }

    fn handle_report_or_event<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<MykoServerContext>,
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
                let report_id = wrapped.report_id;
                let Some(data) = ctx.handler_registry.report(&report_id) else {
                    tracing::warn!("No registered handler for report: {}", report_id);
                    return;
                };
                match (data.parse)(wrapped.report.clone()) {
                    Ok(report) => {
                        let request = Arc::new(RequestContext::from_client(
                            tx.clone(),
                            session.client_id.clone(),
                            ctx.host_id,
                        ));
                        match (data.cell_factory)(report, request, ctx) {
                            Ok(cell) => session.subscribe_report(tx, report_id.into(), cell),
                            Err(error) => tracing::error!(
                                "Failed to create report cell for {}: {}",
                                report_id,
                                error
                            ),
                        }
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

    fn execute_command_job(
        ctx: Arc<MykoServerContext>,
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

                let req = Arc::new(RequestContext::from_client(
                    job.tx_id.clone(),
                    client_id.clone(),
                    host_id,
                ));
                let cmd_id: Arc<str> = Arc::from(command_id.clone());
                let cmd_ctx = CommandContext::new(cmd_id, req, ctx);
                let execute_started = Instant::now();

                match executor.execute_from_value(job.command.clone(), cmd_ctx) {
                    Ok(result) => {
                        let response = MykoMessage::CommandResponse(CommandResponse {
                            response: result,
                            tx: job.tx_id.to_string(),
                        });
                        if let Err(e) = priority_tx.try_send(response) {
                            drop_logger.on_drop("CommandResponse", &e);
                        }
                    }
                    Err(e) => {
                        let error = MykoMessage::CommandError(CommandError::new(
                            job.tx_id.to_string(),
                            command_id.clone(),
                            e.message,
                        ));
                        if let Err(err) = priority_tx.try_send(error) {
                            drop_logger.on_drop("CommandError", &err);
                        }
                    }
                }
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
struct ChannelWriter {
    tx: mpsc::Sender<OutboundMessage>,
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
        if let Err(e) = self.tx.try_send(OutboundMessage::Message(msg)) {
            // Closed errors here race with the writer task exiting between
            // the is_dead check and the try_send; suppress them too.
            if !matches!(e, mpsc::error::TrySendError::Closed(_)) {
                self.drop_logger.on_drop("message", &e);
            }
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
        if self.tx_dead() {
            return;
        }
        if let Err(e) = self.tx.try_send(OutboundMessage::SerializedCommand {
            tx,
            command_id,
            payload,
        }) && !matches!(e, mpsc::error::TrySendError::Closed(_))
        {
            self.drop_logger.on_drop("serialized_command", &e);
        }
    }

    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        if self.deferred_dead() {
            return;
        }
        if let Err(e) = self
            .deferred_tx
            .try_send(DeferredOutbound::Report(tx, output))
            && !matches!(e, mpsc::error::TrySendError::Closed(_))
        {
            self.drop_logger.on_drop("ReportResponseDeferred", &e);
        }
    }

    fn send_query_response(&self, response: PendingQueryResponse, is_view: bool) {
        if self.deferred_dead() {
            return;
        }
        if let Err(e) = self
            .deferred_tx
            .try_send(DeferredOutbound::Query { response, is_view })
            && !matches!(e, mpsc::error::TrySendError::Closed(_))
        {
            self.drop_logger.on_drop("QueryResponseDeferred", &e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_writer() {
        let (tx, mut rx) = mpsc::channel(10);
        let (deferred_tx, _deferred_rx) = mpsc::channel(10);
        let (overload_tx, _overload_rx) = watch::channel(false);
        let drop_logger = Arc::new(DropLogger::new("test-client".into(), overload_tx));
        let writer = ChannelWriter {
            tx,
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
        assert!(received.is_ok(), "writer must enqueue ping");
        let Ok(received) = received else {
            return;
        };
        assert!(matches!(
            received,
            OutboundMessage::Message(MykoMessage::Ping(_))
        ));
    }

    #[test]
    fn full_deferred_queue_marks_connection_for_resnapshot() {
        let (tx, _rx) = mpsc::channel(1);
        let (deferred_tx, mut deferred_rx) = mpsc::channel(1);
        let (overload_tx, overload_rx) = watch::channel(false);
        let drop_logger = Arc::new(DropLogger::new("test-client".into(), overload_tx));
        let writer = ChannelWriter {
            tx,
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
        let queued = deferred_rx.try_recv().expect("initial response queued");
        assert!(matches!(
            queued,
            DeferredOutbound::Query {
                response: PendingQueryResponse { sequence: 0, .. },
                is_view: false,
            }
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
