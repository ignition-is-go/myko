//! WebSocket handler for the cell-based server.
//!
//! Handles WebSocket connections using ClientSession for subscription management.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use hyphae::SelectExt;
use myko::{
    WS_MAX_FRAME_SIZE_BYTES, WS_MAX_MESSAGE_SIZE_BYTES,
    command::{CommandContext, CommandHandlerRegistration},
    entities::client::{Client, ClientId},
    relationship::{iter_client_id_registrations, iter_fallback_to_id_registrations},
    report::AnyOutput,
    request::RequestContext,
    server::{
        CellServerCtx, ClientSession, PendingQueryResponse, WsWriter,
        client_registry::try_client_registry,
    },
    wire::{
        CancelSubscription, CommandError, CommandResponse, EncodedCommandMessage, MEvent,
        MEventType, MykoMessage, PingData, QueryWindowUpdate, ViewError, ViewWindowUpdate,
    },
};
use tokio::{net::TcpStream, sync::mpsc, time::interval};
use tokio_tungstenite::{
    accept_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use uuid::Uuid;

/// Protocol switch message sent by client to enable binary (msgpack) encoding.
/// Must match ProtocolMessages.SwitchToMSGPACK in TypeScript client.
const SWITCH_TO_MSGPACK: &str = "myko:switch-to-msgpack";

struct WsIngestStats {
    counts_by_type: DashMap<Arc<str>, Arc<AtomicU64>>,
}

struct WsBenchmarkStats {
    message_count: AtomicU64,
    total_bytes: AtomicU64,
}

static WS_INGEST_STATS: OnceLock<Arc<WsIngestStats>> = OnceLock::new();
static WS_INGEST_LOGGER_STARTED: AtomicBool = AtomicBool::new(false);
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
    thread::Builder::new()
        .name("ws-benchmark-logger".into())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));

                let count = stats.message_count.swap(0, Ordering::Relaxed);
                let bytes = stats.total_bytes.swap(0, Ordering::Relaxed);

                if count == 0 {
                    continue;
                }

                log::info!(
                    "WebSocket benchmark last_1s messages={} bytes={} avg_bytes={}",
                    count,
                    bytes,
                    if count > 0 { bytes / count } else { 0 },
                );
            }
        })
        .expect("failed to spawn websocket benchmark logger thread");
}

fn ws_ingest_stats() -> Arc<WsIngestStats> {
    WS_INGEST_STATS
        .get_or_init(|| {
            Arc::new(WsIngestStats {
                counts_by_type: DashMap::new(),
            })
        })
        .clone()
}

fn ensure_ws_ingest_logger() {
    if WS_INGEST_LOGGER_STARTED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let stats = ws_ingest_stats();
    thread::Builder::new()
        .name("ws-ingest-logger".into())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));

                let mut per_type = Vec::new();
                let mut total = 0u64;
                for entry in &stats.counts_by_type {
                    let count = entry.value().swap(0, Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    total += count;
                    per_type.push((entry.key().clone(), count));
                }

                if total == 0 {
                    continue;
                }

                per_type.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                let details = per_type
                    .into_iter()
                    .map(|(entity_type, count)| format!("{entity_type}={count}"))
                    .collect::<Vec<_>>()
                    .join(" ");

                log::info!("WebSocket ingest last_1s total={} {}", total, details);
            }
        })
        .expect("failed to spawn websocket ingest logger thread");
}

fn record_ws_ingest(events: &[MEvent]) {
    if events.is_empty() {
        return;
    }

    let stats = ws_ingest_stats();
    for event in events {
        let counter = stats
            .counts_by_type
            .entry(Arc::<str>::from(event.item_type.as_str()))
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn normalize_incoming_event(event: &mut MEvent, client_id: &str) {
    if event.change_type != MEventType::SET {
        return;
    }

    // Auto-populate #[myko_client_id] fields with the connection's client_id
    for reg in iter_client_id_registrations() {
        if reg.entity_type == event.item_type {
            if let Some(obj) = event.item.as_object_mut() {
                obj.insert(
                    reg.field_name_json.to_string(),
                    serde_json::Value::String(client_id.to_string()),
                );
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
            if reg.entity_type == event.item_type {
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
/// but we must not `warn!` for every drop or we can effectively DoS ourselves.
struct DropLogger {
    client_id: Arc<str>,
    dropped: std::sync::atomic::AtomicU64,
    last_log_ms: std::sync::atomic::AtomicU64,
}

impl DropLogger {
    fn new(client_id: Arc<str>) -> Self {
        Self {
            client_id,
            dropped: std::sync::atomic::AtomicU64::new(0),
            last_log_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn on_drop(&self, kind: &'static str, err: &dyn std::fmt::Display) {
        use std::sync::atomic::Ordering;

        self.dropped.fetch_add(1, Ordering::Relaxed);

        // Log at most once per second per connection.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
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
        log::warn!(
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

/// Result of an async subscription build (query or view cell_factory).
enum SubscriptionReady {
    Query {
        tx_id: Arc<str>,
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
    /// Handle a new WebSocket connection.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        ctx: Arc<CellServerCtx>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ensure_ws_ingest_logger();

        let host_id = ctx.host_id;

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(WS_MAX_MESSAGE_SIZE_BYTES);
        ws_config.max_frame_size = Some(WS_MAX_FRAME_SIZE_BYTES);
        let ws_stream = accept_async_with_config(stream, Some(ws_config)).await?;
        let (mut write, mut read) = ws_stream.split();

        // Create a bounded channel for sending messages to the client
        // High limit (10k) since we have good memory availability
        let (tx, mut rx) = mpsc::channel::<OutboundMessage>(10_000);
        let (deferred_tx, mut deferred_rx) = mpsc::channel::<DeferredOutbound>(10_000);
        let (priority_tx, mut priority_rx) = mpsc::channel::<MykoMessage>(1_000);
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<CommandJob>();
        let (subscribe_tx, mut subscribe_rx) = mpsc::unbounded_channel::<SubscriptionReady>();

        // Protocol: default to JSON, switch to binary only if client opts in
        let use_binary = Arc::new(AtomicBool::new(false));

        // Create client session with channel-based writer
        let client_id: Arc<str> = Uuid::new_v4().to_string().into();
        let drop_logger = Arc::new(DropLogger::new(client_id.clone()));
        let writer = ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            use_binary_writer: use_binary.clone(),
        };

        // Register writer in the global client registry (if initialized)
        let writer_arc: Arc<dyn WsWriter> = Arc::new(ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            use_binary_writer: use_binary.clone(),
        });
        if let Some(registry) = try_client_registry() {
            registry.register(client_id.clone(), writer_arc);
        }

        let mut session = ClientSession::new(client_id.clone(), writer);

        let use_binary_writer = use_binary.clone();
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

        // Publish Client entity
        let client_entity = Client {
            id: ClientId(client_id.clone()),
            server_id: host_id.to_string().into(),
            address: Some(Arc::from(addr.to_string())),
            windback: None,
        };
        if let Err(e) = ctx.set(&client_entity) {
            log::error!("Failed to persist client entity: {e}");
        }

        log::info!("Client connected: {} from {}", client_id, addr);

        let write_ctx = ctx.clone();
        let write_client_id = client_id.clone();
        let write_addr = addr;
        let command_ctx = ctx.clone();
        let command_priority_tx = priority_tx.clone();
        let command_drop_logger = drop_logger.clone();
        let command_client_id = client_id.clone();

        // Spawn task to forward messages from channel to WebSocket
        let write_task = tokio::spawn(async move {
            let _ctx = write_ctx;
            let mut normal_open = true;
            let mut priority_open = true;
            let mut deferred_open = true;
            while normal_open || priority_open || deferred_open {
                let msg = tokio::select! {
                    biased;
                    maybe = priority_rx.recv(), if priority_open => {
                        match maybe {
                            Some(msg) => OutboundMessage::Message(msg),
                            None => {
                                priority_open = false;
                                continue;
                            }
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
                        match maybe {
                            Some(msg) => msg,
                            None => {
                                normal_open = false;
                                continue;
                            }
                        }
                    }
                };
                let (kind, tx_id, seq, _upserts, _deletes, _total_count) = match &msg {
                    OutboundMessage::SerializedCommand { tx, .. } => {
                        ("command", Some(tx.clone()), None, None, None, None)
                    }
                    OutboundMessage::Message(msg) => match msg {
                        MykoMessage::ViewResponse(r) => (
                            "view_response",
                            Some(r.tx.clone()),
                            Some(r.sequence),
                            Some(r.upserts.len()),
                            Some(r.deletes.len()),
                            r.total_count,
                        ),
                        MykoMessage::QueryResponse(r) => (
                            "query_response",
                            Some(r.tx.clone()),
                            Some(r.sequence),
                            Some(r.upserts.len()),
                            Some(r.deletes.len()),
                            r.total_count,
                        ),
                        MykoMessage::CommandResponse(r) => (
                            "command_response",
                            Some(Arc::<str>::from(r.tx.clone())),
                            None,
                            None,
                            None,
                            None,
                        ),
                        MykoMessage::CommandError(r) => (
                            "command_error",
                            Some(Arc::<str>::from(r.tx.clone())),
                            None,
                            None,
                            None,
                            None,
                        ),
                        _ => ("other", None, None, None, None, None),
                    },
                };

                match &msg {
                    OutboundMessage::SerializedCommand { tx, command_id, .. } => {
                        if !tx.trim().is_empty()
                            && let Ok(mut map) = outbound_commands_by_tx_writer.lock()
                        {
                            map.insert(tx.to_string(), (command_id.clone(), Instant::now()));
                        }
                    }
                    OutboundMessage::Message(MykoMessage::Command(wrapped)) => {
                        if let Some(tx_id) = wrapped.command.get("tx").and_then(|v| v.as_str())
                            && !tx_id.trim().is_empty()
                            && let Ok(mut map) = outbound_commands_by_tx_writer.lock()
                        {
                            map.insert(
                                tx_id.to_string(),
                                (wrapped.command_id.clone(), Instant::now()),
                            );
                        }
                    }
                    _ => {}
                }

                let ws_msg = match &msg {
                    OutboundMessage::SerializedCommand {
                        payload: EncodedCommandMessage::Json(json),
                        ..
                    } => Message::Text(json.clone().into()),
                    OutboundMessage::SerializedCommand {
                        payload: EncodedCommandMessage::Msgpack(bytes),
                        ..
                    } => Message::Binary(bytes.clone().into()),
                    OutboundMessage::Message(msg) if use_binary_writer.load(Ordering::SeqCst) => {
                        match rmp_serde::to_vec(msg) {
                            Ok(bytes) => Message::Binary(bytes.into()),
                            Err(e) => {
                                log::error!("Failed to serialize message to msgpack: {}", e);
                                continue;
                            }
                        }
                    }
                    OutboundMessage::Message(msg) => match serde_json::to_string(msg) {
                        Ok(json) => Message::Text(json.into()),
                        Err(e) => {
                            log::error!("Failed to serialize message to JSON: {}", e);
                            continue;
                        }
                    },
                };
                let payload_bytes = match &ws_msg {
                    Message::Binary(b) => b.len(),
                    Message::Text(t) => t.len(),
                    _ => 0,
                };

                if let Err(err) = write.send(ws_msg).await {
                    log::error!(
                        "WebSocket write failed for client {} from {} kind={} tx={:?} seq={:?} payload_bytes={} binary={}: {}",
                        write_client_id,
                        write_addr,
                        kind,
                        tx_id,
                        seq,
                        payload_bytes,
                        use_binary_writer.load(Ordering::SeqCst),
                        err
                    );
                    break;
                }
            }
            // NOTE(ts): Unregister from client registry immediately so the node
            // executor stops serializing commands into a dead channel.
            if let Some(registry) = try_client_registry() {
                registry.unregister(&write_client_id);
                log::info!(
                    "WebSocket writer unregistered client {} from {} (write task exiting)",
                    write_client_id,
                    write_addr,
                );
            }
            log::warn!(
                "WebSocket writer task exiting for client {} from {} normal_open={} priority_open={} deferred_open={}",
                write_client_id,
                write_addr,
                normal_open,
                priority_open,
                deferred_open
            );
        });

        // Execute commands on a dedicated worker so ping/cancel traffic is never
        // blocked by long-running command handlers.
        let command_started_cleanup = command_started_by_tx.clone();
        let command_task = tokio::spawn(async move {
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
                        log::error!("Command worker panicked: {}", e);
                    }
                }
                // NOTE(ts): Clean up timing entry after command completes (success or panic).
                if let Ok(mut map) = started_map.lock() {
                    map.remove(&tx_id);
                }
            }
        });

        // Process incoming messages and completed subscription builds concurrently.
        // NOTE(ts): View/query cell_factory calls are spawned on the blocking thread pool
        // so they don't block command processing or other messages.
        let mut outbound_ttl_interval = interval(Duration::from_secs(10));
        outbound_ttl_interval.tick().await; // NOTE(ts): consume the immediate first tick
        loop {
            tokio::select! {
                // Completed subscription builds — register with session
                Some(ready) = subscribe_rx.recv() => {
                    let tx_id = match &ready {
                        SubscriptionReady::Query { tx_id, .. } => tx_id.clone(),
                        SubscriptionReady::View { tx_id, .. } => tx_id.clone(),
                    };
                    if let Ok(mut map) = subscribe_started_by_tx.lock() {
                        map.remove(&tx_id);
                    }
                    match ready {
                        SubscriptionReady::Query { tx_id, cellmap, window } => {
                            session.subscribe_query(tx_id, cellmap, window);
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
                    if let Ok(mut map) = outbound_commands_by_tx.lock() {
                        let before = map.len();
                        map.retain(|_, (_, started)| started.elapsed() < Duration::from_secs(10));
                        let removed = before - map.len();
                        if removed > 0 {
                            log::debug!(
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
                    let ctx = ctx.clone();
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            log::error!("WebSocket read error from {}: {}", client_id, e);
                            break;
                        }
                    };

                    match msg {
                        Message::Binary(data) => {
                            if !use_binary.load(Ordering::SeqCst) {
                                log::debug!(
                                    "Client {} switched to binary (msgpack) protocol via auto-detect",
                                    client_id
                                );
                                use_binary.store(true, Ordering::SeqCst);
                            }

                            match rmp_serde::from_slice::<MykoMessage>(&data) {
                                Ok(myko_msg) => {
                                    if let Err(e) = Self::handle_message(
                                        &mut session,
                                        ctx,
                                        &priority_tx,
                                        &drop_logger,
                                        &query_ids_by_tx,
                                        &view_ids_by_tx,
                                        &subscribe_started_by_tx,
                                        &command_started_by_tx,
                                        &outbound_commands_by_tx,
                                        &command_tx,
                                        &subscribe_tx,
                                        myko_msg,
                                    ) {
                                        log::error!("Error handling message: {}", e);
                                    }
                                    tokio::task::yield_now().await;
                                }
                                Err(e) => {
                                    log::warn!("Failed to parse message from {}: {}", client_id, e);
                                }
                            }
                        }
                        Message::Text(text) => {
                            if text == SWITCH_TO_MSGPACK {
                                log::debug!(
                                    "Client {} switched to binary (msgpack) protocol via explicit request",
                                    client_id
                                );
                                if let Err(e) = priority_tx.try_send(MykoMessage::ProtocolSwitch {
                                    protocol: "msgpack".into(),
                                }) {
                                    drop_logger.on_drop("ProtocolSwitch", &e);
                                }
                                use_binary.store(true, Ordering::SeqCst);
                                continue;
                            }

                            match serde_json::from_str::<MykoMessage>(&text) {
                                Ok(myko_msg) => {
                                    if let Err(e) = Self::handle_message(
                                        &mut session,
                                        ctx,
                                        &priority_tx,
                                        &drop_logger,
                                        &query_ids_by_tx,
                                        &view_ids_by_tx,
                                        &subscribe_started_by_tx,
                                        &command_started_by_tx,
                                        &outbound_commands_by_tx,
                                        &command_tx,
                                        &subscribe_tx,
                                        myko_msg,
                                    ) {
                                        log::error!("Error handling message: {}", e);
                                    }
                                    tokio::task::yield_now().await;
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Failed to parse JSON message from {}: {} | raw: {}",
                                        client_id,
                                        e,
                                        if text.len() > 1000 {
                                            &text[..1000]
                                        } else {
                                            &text
                                        }
                                    );
                                }
                            }
                        }
                        Message::Ping(data) => {
                            log::trace!("Ping from {}", client_id);
                            let _ = data;
                        }
                        Message::Pong(_) => {
                            log::trace!("Pong from {}", client_id);
                        }
                        Message::Close(frame) => {
                            log::warn!("Client {} sent close frame: {:?}", client_id, frame);
                            break;
                        }
                        Message::Frame(_) => {}
                    }
                }
            }
        }

        // Cleanup
        drop(session); // Drops all subscription guards
        write_task.abort();
        command_task.abort();

        // Unregister from client registry
        if let Some(registry) = try_client_registry() {
            registry.unregister(&client_id);
        }

        // Delete Client entity
        if let Err(e) = ctx.del(&client_entity) {
            log::error!("Failed to delete client entity: {e}");
        }

        log::info!("Client disconnected: {} from {}", client_id, addr);

        Ok(())
    }

    /// Handle a parsed MykoMessage.
    #[allow(clippy::too_many_arguments)]
    fn handle_message<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<CellServerCtx>,
        priority_tx: &mpsc::Sender<MykoMessage>,
        drop_logger: &Arc<DropLogger>,
        query_ids_by_tx: &Arc<Mutex<HashMap<Arc<str>, Arc<str>>>>,
        view_ids_by_tx: &Arc<Mutex<HashMap<Arc<str>, Arc<str>>>>,
        subscribe_started_by_tx: &Arc<Mutex<HashMap<Arc<str>, Instant>>>,
        command_started_by_tx: &Arc<Mutex<HashMap<Arc<str>, Instant>>>,
        outbound_commands_by_tx: &Arc<Mutex<HashMap<String, (String, Instant)>>>,
        command_tx: &mpsc::UnboundedSender<CommandJob>,
        subscribe_tx: &mpsc::UnboundedSender<SubscriptionReady>,
        msg: MykoMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handler_registry = ctx.handler_registry.clone();

        let registry = ctx.registry.clone();

        let host_id = ctx.host_id;

        match msg {
            MykoMessage::Query(wrapped) => {
                // Extract tx from the query JSON
                let tx_id: Arc<str> = wrapped
                    .query
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let query_id = &wrapped.query_id;
                let entity_type = &wrapped.query_item_type;

                // Defensive dedupe: some clients can replay the same subscribe request.
                // A duplicate for the same tx would reset server-side sequence/window state.
                if session.has_subscription(&tx_id) {
                    log::debug!(
                        "Ignoring duplicate query subscribe client={} tx={} query_id={} item_type={}",
                        session.client_id,
                        tx_id,
                        query_id,
                        entity_type
                    );
                    return Ok(());
                }
                if let Ok(mut map) = query_ids_by_tx.lock() {
                    map.insert(tx_id.clone(), query_id.clone());
                }
                if let Ok(mut map) = subscribe_started_by_tx.lock() {
                    map.entry(tx_id.clone()).or_insert_with(Instant::now);
                }

                log::trace!("Query {} for {} (tx: {})", query_id, entity_type, tx_id);
                log::trace!(
                    "Query subscribe request client={} tx={} query_id={} item_type={} window={} active_subscriptions_before={}",
                    session.client_id,
                    tx_id,
                    query_id,
                    entity_type,
                    wrapped.window.is_some(),
                    session.subscription_count()
                );

                let request_context = Arc::new(RequestContext::from_client(
                    tx_id.clone(),
                    session.client_id.clone(),
                    host_id,
                ));

                if let Some(query_data) = handler_registry.get_query(query_id) {
                    let parsed = (query_data.parse)(wrapped.query.clone());
                    match parsed {
                        Ok(any_query) => {
                            // NOTE(ts): Spawn cell_factory on blocking pool so it doesn't
                            // block command processing or other messages.
                            let cell_factory = query_data.cell_factory;
                            let registry = registry.clone();
                            let request_context = request_context.clone();
                            let ctx = ctx.clone();
                            let window = wrapped.window.clone();
                            let query_id = query_id.clone();
                            let sub_tx = subscribe_tx.clone();
                            tokio::task::spawn_blocking(move || {
                                match cell_factory(any_query, registry, request_context, Some(ctx))
                                {
                                    Ok(filtered_cellmap) => {
                                        let _ = sub_tx.send(SubscriptionReady::Query {
                                            tx_id,
                                            cellmap: filtered_cellmap,
                                            window,
                                        });
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "Failed to create query cell for {}: {}",
                                            query_id,
                                            e
                                        );
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse query {}: {} | payload: {}",
                                query_id,
                                e,
                                serde_json::to_string(&wrapped.query).unwrap_or_default()
                            );
                        }
                    }
                } else {
                    // Fall back to select all for unknown queries
                    log::warn!(
                        "No registered query handler for {}, falling back to select all",
                        query_id
                    );
                    let cellmap = registry.get_or_create(entity_type).select(|_| true);
                    session.subscribe_query(tx_id, cellmap, wrapped.window.clone());
                }
            }

            MykoMessage::View(wrapped) => {
                let tx_id: Arc<str> = wrapped
                    .view
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let view_id = &wrapped.view_id;
                let item_type = &wrapped.view_item_type;

                // Defensive dedupe: ignore repeated subscribe for an already-active tx.
                if session.has_subscription(&tx_id) {
                    log::debug!(
                        "Ignoring duplicate view subscribe client={} tx={} view_id={} item_type={}",
                        session.client_id,
                        tx_id,
                        view_id,
                        item_type
                    );
                    return Ok(());
                }
                if let Ok(mut map) = view_ids_by_tx.lock() {
                    map.insert(tx_id.clone(), view_id.clone());
                }
                if let Ok(mut map) = subscribe_started_by_tx.lock() {
                    map.entry(tx_id.clone()).or_insert_with(Instant::now);
                }

                log::trace!("View {} for {} (tx: {})", view_id, item_type, tx_id);
                log::trace!(
                    "View subscribe request client={} tx={} view_id={} item_type={} window={:?}",
                    session.client_id,
                    tx_id,
                    view_id,
                    item_type,
                    wrapped.window
                );

                let request_context = Arc::new(RequestContext::from_client(
                    tx_id.clone(),
                    session.client_id.clone(),
                    host_id,
                ));

                if let Some(view_data) = handler_registry.get_view(view_id) {
                    let parsed = (view_data.parse)(wrapped.view.clone());
                    match parsed {
                        Ok(any_view) => {
                            log::trace!(
                                "View parsed successfully client={} tx={} view_id={}",
                                session.client_id,
                                tx_id,
                                view_id
                            );
                            // NOTE(ts): Spawn cell_factory on blocking pool so it doesn't
                            // block command processing or other messages.
                            let cell_factory = view_data.cell_factory;
                            let registry = registry.clone();
                            let ctx = ctx.clone();
                            let window = wrapped.window.clone();
                            let view_id_clone = view_id.clone();
                            let sub_tx = subscribe_tx.clone();
                            let priority_tx = priority_tx.clone();
                            let drop_logger = drop_logger.clone();
                            tokio::task::spawn_blocking(move || {
                                match cell_factory(any_view, registry, request_context, ctx) {
                                    Ok(filtered_cellmap) => {
                                        log::trace!(
                                            "View cell factory succeeded tx={} view_id={}",
                                            tx_id,
                                            view_id_clone
                                        );
                                        let _ = sub_tx.send(SubscriptionReady::View {
                                            tx_id,
                                            view_id: view_id_clone,
                                            cellmap: filtered_cellmap,
                                            window,
                                        });
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "Failed to create view cell for {}: {}",
                                            view_id_clone,
                                            e
                                        );
                                        if let Err(err) = priority_tx.try_send(
                                            MykoMessage::ViewError(ViewError {
                                                tx: tx_id.to_string(),
                                                view_id: view_id_clone.to_string(),
                                                message: e,
                                            }),
                                        ) {
                                            drop_logger.on_drop("ViewError", &err);
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            let message = format!("Failed to parse view {}: {}", view_id, e);
                            log::error!(
                                "{} | payload: {}",
                                message,
                                serde_json::to_string(&wrapped.view).unwrap_or_default()
                            );
                            if let Err(err) =
                                priority_tx.try_send(MykoMessage::ViewError(ViewError {
                                    tx: tx_id.to_string(),
                                    view_id: view_id.to_string(),
                                    message,
                                }))
                            {
                                drop_logger.on_drop("ViewError", &err);
                            }
                        }
                    }
                } else {
                    let message = format!("No registered handler for view: {}", view_id);
                    log::warn!("{}", message);
                    if let Err(err) = priority_tx.try_send(MykoMessage::ViewError(ViewError {
                        tx: tx_id.to_string(),
                        view_id: view_id.to_string(),
                        message,
                    })) {
                        drop_logger.on_drop("ViewError", &err);
                    }
                }
            }

            MykoMessage::QueryCancel(CancelSubscription { tx: tx_id }) => {
                log::trace!(
                    "QueryCancel received: client={} tx={}",
                    session.client_id,
                    tx_id
                );
                let tx_id: Arc<str> = tx_id.into();
                if let Ok(mut map) = query_ids_by_tx.lock() {
                    map.remove(&tx_id);
                }
                if let Ok(mut map) = subscribe_started_by_tx.lock() {
                    map.remove(&tx_id);
                }
                session.cancel(&tx_id);
            }

            MykoMessage::QueryWindow(QueryWindowUpdate { tx, window }) => {
                let tx_id: Arc<str> = tx.into();
                log::trace!(
                    "Query window request client={} tx={} has_window={} active_subscriptions={}",
                    session.client_id,
                    tx_id,
                    window.is_some(),
                    session.subscription_count()
                );
                session.update_query_window(&tx_id, window);
            }
            MykoMessage::ViewCancel(CancelSubscription { tx: tx_id }) => {
                log::trace!("View cancel: {}", tx_id);
                let tx_id: Arc<str> = tx_id.into();
                if let Ok(mut map) = view_ids_by_tx.lock() {
                    map.remove(&tx_id);
                }
                if let Ok(mut map) = subscribe_started_by_tx.lock() {
                    map.remove(&tx_id);
                }
                session.cancel(&tx_id);
            }
            MykoMessage::ViewWindow(ViewWindowUpdate { tx, window }) => {
                let tx_id: Arc<str> = tx.into();
                log::trace!("View window update: {}", tx_id);
                session.update_view_window(&tx_id, window);
            }

            MykoMessage::Report(wrapped) => {
                // Extract tx from the report JSON
                let tx_id: Arc<str> = wrapped
                    .report
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let report_id = &wrapped.report_id;

                log::trace!(
                    "Report subscribe request client={} tx={} report_id={} active_subscriptions_before={}",
                    session.client_id,
                    tx_id,
                    report_id,
                    session.subscription_count()
                );

                // Look up the report registration
                if let Some(report_data) = handler_registry.get_report(report_id) {
                    // Parse the report JSON to the concrete type
                    let parsed = (report_data.parse)(wrapped.report.clone());
                    match parsed {
                        Ok(any_report) => {
                            let request_context = Arc::new(RequestContext::from_client(
                                tx_id.clone(),
                                session.client_id.clone(),
                                host_id,
                            ));

                            // Create the cell using the factory (with host_id for context)
                            match (report_data.cell_factory)(any_report, request_context, ctx) {
                                Ok(cell) => {
                                    session.subscribe_report(
                                        tx_id,
                                        report_id.as_str().into(),
                                        cell,
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create report cell for {}: {}",
                                        report_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse report {}: {} | payload: {}",
                                report_id,
                                e,
                                serde_json::to_string(&wrapped.report).unwrap_or_default()
                            );
                        }
                    }
                } else {
                    log::warn!("No registered handler for report: {}", report_id);
                }
            }

            MykoMessage::ReportCancel(CancelSubscription { tx: tx_id }) => {
                log::trace!(
                    "ReportCancel received: client={} tx={} active_subscriptions_before={}",
                    session.client_id,
                    tx_id,
                    session.subscription_count()
                );
                session.cancel(&tx_id.into());
            }

            MykoMessage::Event(mut event) => {
                record_ws_ingest(std::slice::from_ref(&event));
                event.sanitize_null_bytes();
                normalize_incoming_event(&mut event, &session.client_id);
                if let Err(e) = ctx.apply_event(event) {
                    log::error!(
                        "Failed to apply event from client {}: {e}",
                        session.client_id
                    );
                }
            }

            MykoMessage::EventBatch(mut events) => {
                record_ws_ingest(&events);
                let incoming = events.len();
                if incoming >= 64 {
                    log::trace!(
                        "Received event batch from client {} size={}",
                        session.client_id,
                        incoming
                    );
                }
                for event in &mut events {
                    event.sanitize_null_bytes();
                    normalize_incoming_event(event, &session.client_id);
                }
                match ctx.apply_event_batch(events) {
                    Ok(applied) => {
                        log::trace!(
                            "Applied event batch from client {} size={}",
                            session.client_id,
                            applied
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to apply event batch from client {}: {}",
                            session.client_id,
                            e
                        );
                    }
                }
            }

            MykoMessage::Command(wrapped) => {
                // Extract tx from the command JSON
                let tx_id: Arc<str> = wrapped
                    .command
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();

                let command_id = &wrapped.command_id;

                log::trace!("Command {} (tx: {})", command_id, tx_id,);
                let received_at = Instant::now();
                if let Ok(mut map) = command_started_by_tx.lock() {
                    map.insert(tx_id.clone(), received_at);
                }
                if let Err(e) = command_tx.send(CommandJob {
                    tx_id: tx_id.clone(),
                    command_id: wrapped.command_id.clone(),
                    command: wrapped.command.clone(),
                    received_at,
                }) {
                    log::error!(
                        "Failed to enqueue command {} for client {} tx={}: {}",
                        command_id,
                        session.client_id,
                        tx_id,
                        e
                    );
                    let error = MykoMessage::CommandError(CommandError {
                        tx: tx_id.to_string(),
                        command_id: command_id.to_string(),
                        message: "Command queue unavailable".to_string(),
                    });
                    if let Err(err) = priority_tx.try_send(error) {
                        drop_logger.on_drop("CommandError", &err);
                    }
                }
            }

            MykoMessage::Ping(PingData { id, timestamp }) => {
                // Echo back the ping data
                let pong = MykoMessage::Ping(PingData { id, timestamp });
                if let Err(e) = priority_tx.try_send(pong) {
                    drop_logger.on_drop("Ping", &e);
                }
            }

            // Response messages - these shouldn't come from clients.
            MykoMessage::QueryResponse(resp) => {
                log::warn!(
                    "Unexpected client message kind=query_response client={} tx={} seq={} upserts={} deletes={} active_subscriptions={}",
                    session.client_id,
                    resp.tx,
                    resp.sequence,
                    resp.upserts.len(),
                    resp.deletes.len(),
                    session.subscription_count()
                );
            }
            MykoMessage::QueryError(err) => {
                log::warn!(
                    "Unexpected client message kind=query_error client={} tx={} query_id={} message={} active_subscriptions={}",
                    session.client_id,
                    err.tx,
                    err.query_id,
                    err.message,
                    session.subscription_count()
                );
            }
            MykoMessage::ViewResponse(resp) => {
                log::warn!(
                    "Unexpected client message kind=view_response client={} tx={} seq={} upserts={} deletes={} active_subscriptions={}",
                    session.client_id,
                    resp.tx,
                    resp.sequence,
                    resp.upserts.len(),
                    resp.deletes.len(),
                    session.subscription_count()
                );
            }
            MykoMessage::ViewError(err) => {
                log::warn!(
                    "Unexpected client message kind=view_error client={} tx={} view_id={} message={} active_subscriptions={}",
                    session.client_id,
                    err.tx,
                    err.view_id,
                    err.message,
                    session.subscription_count()
                );
            }
            MykoMessage::ReportResponse(resp) => {
                log::warn!(
                    "Unexpected client message kind=report_response client={} tx={} active_subscriptions={}",
                    session.client_id,
                    resp.tx,
                    session.subscription_count()
                );
            }
            MykoMessage::ReportError(err) => {
                log::warn!(
                    "Unexpected client message kind=report_error client={} tx={} report_id={} message={} active_subscriptions={}",
                    session.client_id,
                    err.tx,
                    err.report_id,
                    err.message,
                    session.subscription_count()
                );
            }
            MykoMessage::CommandResponse(resp) => {
                if resp.tx.trim().is_empty() {
                    log::warn!(
                        "Malformed client message kind=command_response client={} tx=<empty> active_subscriptions={}",
                        session.client_id,
                        session.subscription_count()
                    );
                } else {
                    let correlated = outbound_commands_by_tx
                        .lock()
                        .ok()
                        .and_then(|mut map| map.remove(&resp.tx));
                    if let Some((command_id, started)) = correlated {
                        log::debug!(
                            "Client command response matched outbound command client={} tx={} command_id={} roundtrip_ms={} active_subscriptions={}",
                            session.client_id,
                            resp.tx,
                            command_id,
                            started.elapsed().as_millis(),
                            session.subscription_count()
                        );
                    } else {
                        log::warn!(
                            "Client command response without outbound match client={} tx={} active_subscriptions={}",
                            session.client_id,
                            resp.tx,
                            session.subscription_count()
                        );
                    }
                }
            }
            MykoMessage::CommandError(err) => {
                if err.tx.trim().is_empty() {
                    log::warn!(
                        "Malformed client message kind=command_error client={} tx=<empty> command_id={} message={} active_subscriptions={}",
                        session.client_id,
                        err.command_id,
                        err.message,
                        session.subscription_count()
                    );
                } else {
                    let correlated = outbound_commands_by_tx
                        .lock()
                        .ok()
                        .and_then(|mut map| map.remove(&err.tx));
                    if let Some((command_id, started)) = correlated {
                        log::warn!(
                            "Client command error matched outbound command client={} tx={} command_id={} transport_command_id={} message={} roundtrip_ms={} active_subscriptions={}",
                            session.client_id,
                            err.tx,
                            err.command_id,
                            command_id,
                            err.message,
                            started.elapsed().as_millis(),
                            session.subscription_count()
                        );
                    } else {
                        log::warn!(
                            "Client command error without outbound match client={} tx={} command_id={} message={} active_subscriptions={}",
                            session.client_id,
                            err.tx,
                            err.command_id,
                            err.message,
                            session.subscription_count()
                        );
                    }
                }
            }
            MykoMessage::Benchmark(payload) => {
                let stats = ws_benchmark_stats();
                ensure_ws_benchmark_logger();
                stats.message_count.fetch_add(1, Ordering::Relaxed);
                // Estimate payload size from the JSON value
                let size = payload.to_string().len() as u64;
                stats.total_bytes.fetch_add(size, Ordering::Relaxed);
            }
            MykoMessage::ProtocolSwitch { protocol } => {
                log::warn!(
                    "Unexpected client message kind=protocol_switch_ack client={} protocol={} active_subscriptions={}",
                    session.client_id,
                    protocol,
                    session.subscription_count()
                );
            }
        }

        Ok(())
    }

    fn execute_command_job(
        ctx: Arc<CellServerCtx>,
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
                let cmd_ctx = CommandContext::new(cmd_id, req, ctx.clone());
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
                        let error = MykoMessage::CommandError(CommandError {
                            tx: job.tx_id.to_string(),
                            command_id: command_id.clone(),
                            message: e.message,
                        });
                        if let Err(err) = priority_tx.try_send(error) {
                            drop_logger.on_drop("CommandError", &err);
                        }
                    }
                }
                let execute_ms = execute_started.elapsed().as_millis();
                let total_ms = job.received_at.elapsed().as_millis();
                log::trace!(
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
            log::warn!("No registered handler for command: {}", command_id);
            let error = MykoMessage::CommandError(CommandError {
                tx: job.tx_id.to_string(),
                command_id: command_id.clone(),
                message: format!("Command handler not found: {}", command_id),
            });
            if let Err(e) = priority_tx.try_send(error) {
                drop_logger.on_drop("CommandError", &e);
            }
        }

        if !handler_found {
            log::debug!(
                target: "myko_server::ws_perf",
                "command_exec client={} tx={} command_id={} queue_wait_ms={} execute_ms=0 total_ms={} handler_found=false",
                client_id,
                job.tx_id,
                command_id,
                queue_wait_ms,
                job.received_at.elapsed().as_millis()
            );
        }
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
    use_binary_writer: Arc<AtomicBool>,
}

impl WsWriter for ChannelWriter {
    fn send(&self, msg: MykoMessage) {
        // Use try_send since we're in sync context
        if let Err(e) = self.tx.try_send(OutboundMessage::Message(msg)) {
            self.drop_logger.on_drop("message", &e);
        }
    }

    fn protocol(&self) -> myko::client::MykoProtocol {
        if self.use_binary_writer.load(Ordering::SeqCst) {
            myko::client::MykoProtocol::MSGPACK
        } else {
            myko::client::MykoProtocol::JSON
        }
    }

    fn send_serialized_command(
        &self,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    ) {
        if let Err(e) = self.tx.try_send(OutboundMessage::SerializedCommand {
            tx,
            command_id,
            payload,
        }) {
            self.drop_logger.on_drop("serialized_command", &e);
        }
    }

    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        if let Err(e) = self
            .deferred_tx
            .try_send(DeferredOutbound::Report(tx, output))
        {
            self.drop_logger.on_drop("ReportResponseDeferred", &e);
        }
    }

    fn send_query_response(&self, response: PendingQueryResponse, is_view: bool) {
        if let Err(e) = self
            .deferred_tx
            .try_send(DeferredOutbound::Query { response, is_view })
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
        let drop_logger = Arc::new(DropLogger::new("test-client".into()));
        let writer = ChannelWriter {
            tx,
            deferred_tx,
            drop_logger,
            use_binary_writer: Arc::new(AtomicBool::new(false)),
        };

        let msg = MykoMessage::Ping(PingData {
            id: "test".to_string(),
            timestamp: 0,
        });
        writer.send(msg);

        let received = rx.try_recv().unwrap();
        assert!(matches!(
            received,
            OutboundMessage::Message(MykoMessage::Ping(_))
        ));
    }
}
