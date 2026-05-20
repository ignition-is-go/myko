use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

#[cfg(not(target_arch = "wasm32"))]
pub mod entity_sync;
mod query_map;

pub use autosocket::SocketConnectionStatus as ConnectionStatus;
use autosocket::{CallbackGuard, SocketTransport, WsFrame};
use dashmap::DashMap;
use hyphae::{
    Cell, CellImmutable, CellMutable, Gettable, MapExt, MaterializeDefinite, Mutable,
    SubscriptionGuard, Watchable,
};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use url::Url;

use crate::{
    command::{CommandId, CommandRequest, WrappedCommand},
    common::with_id::WithId,
    core::item::Eventable,
    entities::server::{GetPeerServers, Server},
    query::{QueryParams, QueryRequest},
    report::{ReportIdStatic, ReportParams, ReportRequest},
    view::{ViewParams, ViewRequest},
    wire::{
        MEvent, MykoMessage, PingData, WrappedQuery, WrappedReport, WrappedView,
        wrap_command_request, wrap_view,
    },
};

/// Wire protocol for encoding messages.
/// Defaults to JSON; clients opt into CBOR by calling `set_protocol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::TS)]
#[ts(export)]
pub enum MykoProtocol {
    JSON = 0,
    CBOR = 1,
}

impl From<u8> for MykoProtocol {
    fn from(v: u8) -> Self {
        match v {
            0 => MykoProtocol::JSON,
            _ => MykoProtocol::CBOR,
        }
    }
}

/// Response handler for incoming command responses (one-shot).
type CommandResponseHandler = Box<dyn FnOnce(Result<Value, String>) + Send>;

/// Handler for incoming query responses.
type QueryHandler = Box<dyn Fn(Value) + Send + Sync>;

/// Handler for incoming report responses.
type ReportHandler = Box<dyn Fn(Value) + Send + Sync>;

/// Handler for incoming command requests (from server).
type CommandRequestHandler = Box<dyn Fn(Value, CommandResponder) + Send + Sync>;

// ─────────────────────────────────────────────────────────────────────────────
// CommandResponder — allows sync command handlers to send responses
// ─────────────────────────────────────────────────────────────────────────────

/// Allows a command handler to send a response back to the server.
pub struct CommandResponder {
    socket: Arc<dyn SocketTransport>,
    protocol: Arc<AtomicU8>,
    tx: String,
    command_id: Arc<str>,
}

impl CommandResponder {
    /// Send a successful response.
    pub fn respond_ok(&self, response: Value) {
        let resp = crate::command::CommandResponse {
            tx: self.tx.clone(),
            response,
        };
        let msg = MykoMessage::CommandResponse(resp);
        if let Some(frame) = encode_protocol(&self.protocol, &msg) {
            let _ = self.socket.send(frame);
        }
    }

    /// Send an error response.
    pub fn respond_err(&self, message: String) {
        let err = crate::command::CommandError {
            tx: self.tx.clone(),
            command_id: self.command_id.to_string(),
            message,
        };
        let msg = MykoMessage::CommandError(err);
        if let Some(frame) = encode_protocol(&self.protocol, &msg) {
            let _ = self.socket.send(frame);
        }
    }
}

fn encode_protocol(protocol: &AtomicU8, msg: &MykoMessage) -> Option<WsFrame> {
    match MykoProtocol::from(protocol.load(Ordering::SeqCst)) {
        MykoProtocol::JSON => serde_json::to_string(msg).ok().map(WsFrame::Text),
        MykoProtocol::CBOR => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(msg, &mut bytes).ok()?;
            Some(WsFrame::Binary(bytes))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscription cancel helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Create a guard that sends a cancel message and removes the handler on drop.
fn query_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("query_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("query_cancel_guard: cancelling tx={}", tx);
        inner.query_handlers.remove(&tx);
        let msg = MykoMessage::QueryCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(_) => debug!("query_cancel_guard: sent QueryCancel tx={}", tx),
                Err(e) => warn!(
                    "query_cancel_guard: failed to send QueryCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

/// Cancel guard for views. View responses share the `query_handlers`
/// dispatch table on the client, but the server requires a `ViewCancel`
/// to release the view subscription (not a `QueryCancel`).
fn view_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("view_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("view_cancel_guard: cancelling tx={}", tx);
        inner.query_handlers.remove(&tx);
        let msg = MykoMessage::ViewCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(_) => debug!("view_cancel_guard: sent ViewCancel tx={}", tx),
                Err(e) => warn!(
                    "view_cancel_guard: failed to send ViewCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

fn report_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("report_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("report_cancel_guard: cancelling tx={}", tx);
        inner.report_handlers.remove(&tx);
        let msg = MykoMessage::ReportCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(_) => debug!("report_cancel_guard: sent ReportCancel tx={}", tx),
                Err(e) => warn!(
                    "report_cancel_guard: failed to send ReportCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Report cache — deduplicate identical report subscriptions over the wire
// ─────────────────────────────────────────────────────────────────────────────

trait ClientReportCacheEntryDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

struct ClientReportCacheEntry<T> {
    weak: hyphae::cell::WeakCell<T, CellImmutable>,
}

impl<T: Clone + Send + Sync + 'static> ClientReportCacheEntry<T> {
    fn new(cell: &Cell<T, CellImmutable>) -> Self {
        Self {
            weak: cell.downgrade(),
        }
    }

    fn get(&self) -> Option<Cell<T, CellImmutable>> {
        self.weak.upgrade()
    }
}

impl<T: Clone + Send + Sync + 'static> ClientReportCacheEntryDyn for ClientReportCacheEntry<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MykoClient
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MykoClient {
    inner: Arc<MykoClientInner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MykoClientOptions {
    pub auto_reconnect: bool,
    pub peer_failover: bool,
    pub app_ping: bool,
}

impl Default for MykoClientOptions {
    fn default() -> Self {
        Self {
            auto_reconnect: true,
            peer_failover: false,
            app_ping: true,
        }
    }
}

struct MykoClientInner {
    socket: Arc<dyn SocketTransport>,
    protocol: Arc<AtomicU8>,
    last_message: Cell<Option<Value>, CellMutable>,
    ping_ms: Cell<Option<u64>, CellMutable>,
    peer_failover_enabled: bool,
    known_servers: Mutex<Vec<String>>,
    current_address: Mutex<Option<String>>,
    _peer_failover_status_guard: Mutex<Option<SubscriptionGuard>>,
    _peer_discovery_guard: Mutex<Option<SubscriptionGuard>>,

    // Dispatch maps keyed by tx
    query_handlers: DashMap<Arc<str>, QueryHandler>,
    report_handlers: DashMap<Arc<str>, ReportHandler>,
    command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>>,
    command_request_handlers: DashMap<Arc<str>, CommandRequestHandler>,

    // Report subscription cache — keyed by report_id:params_hash
    report_cache: DashMap<String, Box<dyn ClientReportCacheEntryDyn>>,

    // Frames queued while disconnected
    pending_sends: Mutex<Vec<WsFrame>>,

    // Report-response dispatch: the WS read thread hands incoming
    // `report-response` payloads off to a dedicated worker so the hot
    // reactive fan-out (Cell::set → notify chain → arc_swap CAS →
    // subscriber map closures) runs off the WS read path. Without this,
    // a slow subscriber tree back-pressures frame decoding and incoming
    // report frames pile up in the TCP buffer.
    //
    // Order is preserved: a single worker drains this channel FIFO, so
    // report responses execute in the exact order they arrived from the
    // server — including responses for the same tx. Other frame types
    // (query/command/ping) stay synchronous in `handle_frame`; they're
    // low-rate and often depend on synchronous completion semantics.
    report_dispatch_tx: flume::Sender<(Arc<str>, serde_json::Value)>,

    // Guards that keep subscriptions alive
    _read_guard: CallbackGuard,
    _report_dispatch_guard: CallbackGuard,
    _status_guard: SubscriptionGuard,
}

impl Default for MykoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoClient {
    /// Create a new MykoClient with the platform-default transport.
    ///
    /// On native: uses `AutoReconnectSocket` (tokio-tungstenite).
    /// On WASM: uses `WasmSocket` (web-sys WebSocket).
    pub fn new() -> MykoClient {
        Self::with_options(MykoClientOptions::default())
    }

    /// Create a client with peer failover enabled.
    pub fn with_failover() -> MykoClient {
        Self::with_options(MykoClientOptions {
            auto_reconnect: true,
            peer_failover: true,
            app_ping: true,
        })
    }

    /// Create a new MykoClient with configurable transport auto-reconnect behavior.
    pub fn new_with_auto_reconnect(auto_reconnect: bool) -> MykoClient {
        Self::with_options(MykoClientOptions {
            auto_reconnect,
            peer_failover: false,
            app_ping: true,
        })
    }

    /// Create a new MykoClient with explicit options.
    pub fn with_options(options: MykoClientOptions) -> MykoClient {
        #[cfg(not(target_arch = "wasm32"))]
        let socket: Arc<dyn SocketTransport> = Arc::new(
            autosocket::AutoReconnectSocket::with_auto_reconnect_and_limits(
                options.auto_reconnect,
                crate::WS_MAX_MESSAGE_SIZE_BYTES,
                crate::WS_MAX_FRAME_SIZE_BYTES,
            ),
        );

        #[cfg(target_arch = "wasm32")]
        let socket: Arc<dyn SocketTransport> = Arc::new(
            autosocket::WasmSocket::with_auto_reconnect(options.auto_reconnect),
        );

        Self::with_transport_and_options(socket, options)
    }

    /// Create a MykoClient with a custom transport implementation.
    pub fn with_transport(transport: Arc<dyn SocketTransport>) -> MykoClient {
        Self::with_transport_and_options(transport, MykoClientOptions::default())
    }

    fn with_transport_and_options(
        transport: Arc<dyn SocketTransport>,
        options: MykoClientOptions,
    ) -> MykoClient {
        let protocol = Arc::new(AtomicU8::new(MykoProtocol::JSON as u8));
        let last_message = Cell::new(None).with_name("last_message");
        let ping_ms = Cell::new(None).with_name("ping_ms");

        let query_handlers: DashMap<Arc<str>, QueryHandler> = DashMap::new();
        let report_handlers: DashMap<Arc<str>, ReportHandler> = DashMap::new();
        let command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>> =
            Mutex::new(HashMap::new());
        let command_request_handlers: DashMap<Arc<str>, CommandRequestHandler> = DashMap::new();
        let pending_sends: Mutex<Vec<WsFrame>> = Mutex::new(Vec::new());

        // Report-response dispatch channel. Unbounded, single-consumer.
        // See the field comment on `report_dispatch_tx` for rationale —
        // the WS read thread must not run the reactive fan-out inline.
        let (report_dispatch_tx, report_dispatch_rx) =
            flume::unbounded::<(Arc<str>, serde_json::Value)>();

        // We need to set up the callbacks, but they reference the inner struct.
        // Use a two-step initialization: create with noop guards, then replace.
        let inner = Arc::new_cyclic(|weak| {
            let read_guard = {
                let weak_for_msg = weak.clone();
                let rx = transport.read_rx();
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let cancelled = Arc::new(AtomicBool::new(false));
                    let cancelled_for_thread = Arc::clone(&cancelled);
                    let handle = std::thread::spawn(move || {
                        loop {
                            if cancelled_for_thread.load(Ordering::SeqCst) {
                                break;
                            }
                            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                                Ok(frame) => {
                                    let Some(inner) = weak_for_msg.upgrade() else {
                                        break;
                                    };
                                    Self::handle_frame(&inner, &frame);
                                }
                                Err(flume::RecvTimeoutError::Timeout) => {}
                                Err(flume::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                    });
                    CallbackGuard::new(move || {
                        cancelled.store(true, Ordering::SeqCst);
                        let _ = handle.join();
                    })
                }
                #[cfg(target_arch = "wasm32")]
                {
                    wasm_bindgen_futures::spawn_local(async move {
                        while let Ok(frame) = rx.recv_async().await {
                            let Some(inner) = weak_for_msg.upgrade() else {
                                break;
                            };
                            Self::handle_frame(&inner, &frame);
                        }
                    });
                    CallbackGuard::noop()
                }
            };

            // Dedicated worker for report-response payloads. Drains the
            // channel FIFO and calls each registered handler synchronously
            // — so the order of handler invocations is identical to the
            // order of incoming frames. The WS read thread enqueues and
            // returns immediately.
            let report_dispatch_guard = {
                let weak_for_dispatch = weak.clone();
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let cancelled = Arc::new(AtomicBool::new(false));
                    let cancelled_for_thread = Arc::clone(&cancelled);
                    let rx = report_dispatch_rx.clone();
                    let handle = std::thread::spawn(move || {
                        loop {
                            if cancelled_for_thread.load(Ordering::SeqCst) {
                                break;
                            }
                            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                                Ok((tx, response)) => {
                                    let Some(inner) = weak_for_dispatch.upgrade() else {
                                        break;
                                    };
                                    if let Some(handler) = inner.report_handlers.get(&tx) {
                                        handler(response);
                                    }
                                }
                                Err(flume::RecvTimeoutError::Timeout) => {}
                                Err(flume::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                    });
                    CallbackGuard::new(move || {
                        cancelled.store(true, Ordering::SeqCst);
                        let _ = handle.join();
                    })
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let rx = report_dispatch_rx.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        while let Ok((tx, response)) = rx.recv_async().await {
                            let Some(inner) = weak_for_dispatch.upgrade() else {
                                break;
                            };
                            if let Some(handler) = inner.report_handlers.get(&tx) {
                                handler(response);
                            }
                        }
                    });
                    CallbackGuard::noop()
                }
            };

            let weak_for_status = weak.clone();
            let status_guard = transport
                .actual_connection_state()
                .subscribe(move |signal| {
                    let Some(inner) = weak_for_status.upgrade() else {
                        return;
                    };
                    let hyphae::Signal::Value(status) = signal else {
                        return;
                    };
                    let conn_status = (**status).clone();

                    if !matches!(conn_status, ConnectionStatus::Connected(_)) {
                        inner.ping_ms.set(None);
                    }

                    // Flush pending sends on connect
                    if let ConnectionStatus::Connected(_) = conn_status {
                        let mut pending = inner.pending_sends.lock().unwrap();
                        for frame in pending.drain(..) {
                            let _ = inner.socket.send(frame);
                        }
                    }
                });

            MykoClientInner {
                socket: transport.clone(),
                protocol: protocol.clone(),
                last_message,
                ping_ms,
                peer_failover_enabled: options.peer_failover,
                known_servers: Mutex::new(Vec::new()),
                current_address: Mutex::new(None),
                _peer_failover_status_guard: Mutex::new(None),
                _peer_discovery_guard: Mutex::new(None),
                query_handlers,
                report_handlers,
                command_response_handlers,
                command_request_handlers,
                report_cache: DashMap::new(),
                pending_sends,
                report_dispatch_tx,
                _read_guard: read_guard,
                _report_dispatch_guard: report_dispatch_guard,
                _status_guard: status_guard,
            }
        });

        let client = MykoClient { inner };

        #[cfg(not(target_arch = "wasm32"))]
        if options.app_ping {
            Self::spawn_ping_loop(Arc::downgrade(&client.inner));
        }

        if client.inner.peer_failover_enabled {
            client.setup_peer_failover();
        }

        client
    }

    fn setup_peer_failover(&self) {
        let this = self.clone();
        let status_guard = self
            .inner
            .socket
            .actual_connection_state()
            .subscribe(move |signal| {
                if let hyphae::Signal::Value(status) = signal
                    && matches!(&**status, ConnectionStatus::Disconnected)
                {
                    this.try_failover();
                }
            });
        *self.inner._peer_failover_status_guard.lock().unwrap() = Some(status_guard);

        let discovery = self.watch_query(GetPeerServers {});
        let this = self.clone();
        let discovery_guard = discovery.subscribe(move |signal| {
            if let hyphae::Signal::Value(servers) = signal {
                this.update_known_servers_from_peers(servers.as_ref());
            }
        });
        *self.inner._peer_discovery_guard.lock().unwrap() = Some(discovery_guard);
    }

    fn update_known_servers_from_peers(&self, peers: &[Arc<Server>]) {
        let current = self.inner.current_address.lock().unwrap().clone();
        let use_wss = current
            .as_ref()
            .map(|a| a.starts_with("wss://"))
            .unwrap_or(false);

        let mut next = Vec::new();
        if let Some(current) = current {
            next.push(current);
        }

        for server in peers {
            let addr = if use_wss {
                format!("wss://{}:{}/myko", server.address, server.port)
            } else {
                format!("ws://{}:{}/myko", server.address, server.port)
            };
            if !next.iter().any(|x| x == &addr) {
                next.push(addr);
            }
        }

        *self.inner.known_servers.lock().unwrap() = next;
    }

    fn try_failover(&self) {
        if !self.inner.peer_failover_enabled {
            return;
        }

        let current = self.inner.current_address.lock().unwrap().clone();
        let servers = self.inner.known_servers.lock().unwrap().clone();
        if servers.is_empty() {
            return;
        }

        let start_idx = current
            .as_ref()
            .and_then(|c| servers.iter().position(|s| s == c))
            .unwrap_or(0);

        for offset in 1..=servers.len() {
            let idx = (start_idx + offset) % servers.len();
            let candidate = servers[idx].clone();
            if current.as_ref() == Some(&candidate) {
                continue;
            }
            info!("MykoClient failover attempting {}", candidate);
            self.set_address(Some(candidate));
            return;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn_ping_loop(inner: std::sync::Weak<MykoClientInner>) {
        std::thread::spawn(move || {
            loop {
                let Some(inner) = inner.upgrade() else {
                    break;
                };

                if matches!(
                    inner.socket.actual_connection_state().get(),
                    ConnectionStatus::Connected(_)
                ) {
                    let msg = MykoMessage::Ping(PingData {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now().timestamp_millis(),
                    });
                    if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
                        let _ = inner.socket.send(frame);
                    }
                }

                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
    }

    /// Handle an incoming WebSocket frame by dispatching to registered handlers.
    fn handle_frame(inner: &MykoClientInner, frame: &WsFrame) {
        let Some(value) = Self::decode_message(frame) else {
            return;
        };
        inner.last_message.set(Some(value.clone()));

        // Fast-path: extract event tag and data from the raw Value to avoid
        // deserializing QueryResponse/ViewResponse through MykoMessage (which
        // would require round-tripping Arc<dyn AnyItem> through serde).
        let event_tag = value.get("event").and_then(|v| v.as_str()).unwrap_or("");

        let data = || {
            value
                .get("data")
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        };

        match event_tag {
            "ws:m:query-response" | "ws:m:view-response" => {
                let data_val = data();
                let tx_str = data_val.get("tx").and_then(|v| v.as_str()).unwrap_or("");
                let tx: Arc<str> = Arc::from(tx_str);
                if let Some(handler) = inner.query_handlers.get(&tx) {
                    handler(data_val);
                }
            }
            "ws:m:report-response" => {
                if let Ok(response) = serde_json::from_value::<crate::wire::ReportResponse>(data())
                {
                    // Dispatch off the WS read thread — the handler runs
                    // the full reactive fan-out (Cell::set → notify →
                    // subscriber map closures), which can be many ms.
                    // Keeping that off this loop lets the decoder keep
                    // pulling frames and avoids TCP backpressure.
                    // Order is preserved: the worker drains the channel
                    // FIFO, so same-tx responses execute in send order.
                    let tx: Arc<str> = response.tx.clone().into();
                    let _ = inner.report_dispatch_tx.send((tx, response.response));
                }
            }
            "ws:m:command-response" => {
                if let Ok(response) = serde_json::from_value::<crate::wire::CommandResponse>(data())
                {
                    let mut handlers = inner.command_response_handlers.lock().unwrap();
                    if let Some(handler) = handlers.remove(&response.tx) {
                        handler(Ok(response.response));
                    }
                }
            }
            "ws:m:command-error" => {
                if let Ok(err) = serde_json::from_value::<crate::wire::CommandError>(data()) {
                    let mut handlers = inner.command_response_handlers.lock().unwrap();
                    if let Some(handler) = handlers.remove(&err.tx) {
                        handler(Err(err.message));
                    }
                }
            }
            "ws:m:command" => {
                if let Ok(wrapped) = serde_json::from_value::<crate::wire::WrappedCommand>(data()) {
                    let command_id: Arc<str> = wrapped.command_id.clone().into();
                    if let Some(handler) = inner.command_request_handlers.get(&command_id) {
                        let tx = wrapped
                            .command
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        if tx.is_empty() {
                            return;
                        }
                        let responder = CommandResponder {
                            socket: inner.socket.clone(),
                            protocol: inner.protocol.clone(),
                            tx,
                            command_id: command_id.clone(),
                        };
                        handler(wrapped.command, responder);
                    }
                }
            }
            "ws:m:ping" => {
                if let Ok(ping) = serde_json::from_value::<PingData>(data()) {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let ping_ms = now_ms.saturating_sub(ping.timestamp) as u64;
                    inner.ping_ms.set(Some(ping_ms));
                }
            }
            _ => {}
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Protocol and encoding
    // ─────────────────────────────────────────────────────────────────────────

    /// Set the wire protocol (JSON or CBOR). Default is JSON.
    pub fn set_protocol(&self, protocol: MykoProtocol) {
        self.inner.protocol.store(protocol as u8, Ordering::SeqCst);
    }

    /// Get the current wire protocol.
    pub fn get_protocol(&self) -> MykoProtocol {
        MykoProtocol::from(self.inner.protocol.load(Ordering::SeqCst))
    }

    /// Encode a message according to the current protocol.
    fn encode_message<T: Serialize>(&self, msg: &T) -> Result<WsFrame, String> {
        match self.get_protocol() {
            MykoProtocol::JSON => {
                let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
                Ok(WsFrame::Text(json))
            }
            MykoProtocol::CBOR => {
                let mut bytes = Vec::new();
                ciborium::ser::into_writer(msg, &mut bytes).map_err(|e| e.to_string())?;
                Ok(WsFrame::Binary(bytes))
            }
        }
    }

    /// Decode a WebSocket frame according to its type.
    fn decode_message(frame: &WsFrame) -> Option<Value> {
        match frame {
            WsFrame::Text(content) => serde_json::from_str::<Value>(content).ok(),
            WsFrame::Binary(bytes) => match ciborium::de::from_reader::<Value, _>(bytes.as_slice())
            {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("CBOR decode failed ({} bytes): {}", bytes.len(), e);
                    None
                }
            },
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Connection
    // ─────────────────────────────────────────────────────────────────────────

    /// Get a reactive cell of the connection status.
    pub fn connection_status(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.inner.socket.actual_connection_state()
    }

    /// Get the current connection status synchronously.
    pub fn get_connection_status_sync(&self) -> ConnectionStatus {
        self.inner.socket.actual_connection_state().get()
    }

    /// Get a reactive cell of live ping in milliseconds (None when unavailable).
    pub fn ping_ms(&self) -> &Cell<Option<u64>, CellMutable> {
        &self.inner.ping_ms
    }

    /// Get the latest raw incoming message.
    pub fn messages(&self) -> Cell<Option<Value>, CellImmutable> {
        self.inner.last_message.clone().lock()
    }

    /// Get the current ping synchronously.
    pub fn get_ping_ms_sync(&self) -> Option<u64> {
        self.inner.ping_ms.get()
    }

    pub fn set_address(&self, addr: Option<String>) {
        if addr.is_none() {
            let current = self.inner.current_address.lock().unwrap().clone();
            if current.is_none() {
                debug!("set_address(None) ignored; already disconnected");
                return;
            }
            debug!("Setting address to None, disconnecting socket");
            self.inner.socket.set_addr(None);
            *self.inner.current_address.lock().unwrap() = None;
            return;
        }

        let addr = addr.unwrap();

        let mut parsed = match Url::parse(addr.as_str()) {
            Ok(url) if url.scheme() == "ws" || url.scheme() == "wss" => url,
            _ => {
                let add_ws = format!("ws://{addr}");
                match Url::parse(add_ws.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Could not parse url: {e:?}");
                        self.inner.socket.set_addr(None);
                        return;
                    }
                }
            }
        };

        if parsed.path() != "/myko" {
            parsed.set_path("/myko");
        }

        if parsed.port().is_none() {
            let _ = parsed.set_port(Some(5155));
        }

        let parsed_addr = parsed.to_string();
        let current = self.inner.current_address.lock().unwrap().clone();
        if current.as_ref() == Some(&parsed_addr) {
            debug!("set_address({parsed_addr}) ignored; address unchanged");
            return;
        }

        info!("MykoClient connecting to {}", parsed_addr);
        self.inner.socket.set_addr(Some(parsed_addr.clone()));
        *self.inner.current_address.lock().unwrap() = Some(parsed_addr.clone());
        if self.inner.peer_failover_enabled {
            let mut servers = self.inner.known_servers.lock().unwrap();
            if let Some(pos) = servers.iter().position(|s| s == &parsed_addr) {
                servers.remove(pos);
            }
            servers.insert(0, parsed_addr);
        }
    }

    /// Disconnect the client and stop any reconnection attempts.
    pub fn disconnect(&self) {
        debug!("Disconnecting MykoClient");
        self.inner.socket.close();
    }

    /// Close the client and stop any reconnection attempts.
    pub fn close(&self) {
        debug!("Closing MykoClient");
        self.inner.socket.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Send a frame, or queue it if disconnected.
    fn send_or_queue(&self, frame: WsFrame) {
        if let ConnectionStatus::Connected(_) = self.inner.socket.actual_connection_state().get() {
            let _ = self.inner.socket.send(frame);
        } else {
            let mut pending = self.inner.pending_sends.lock().unwrap();
            pending.push(frame);
            let len = pending.len();
            if len == 1 || len.is_multiple_of(10_000) {
                warn!(
                    "MykoClient queued frame while disconnected; pending_sends={}",
                    len
                );
            }
        }
    }

    pub fn send_event(&self, event: MEvent) -> Result<(), String> {
        let myko_msg = MykoMessage::Event(event);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame);
        Ok(())
    }

    pub fn send_event_batch(&self, events: Vec<MEvent>) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }
        let myko_msg = MykoMessage::EventBatch(events);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame);
        Ok(())
    }

    pub fn send_query(&self, query: WrappedQuery) -> Result<(), String> {
        let myko_msg = MykoMessage::Query(query);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame);
        Ok(())
    }

    /// Send a raw wrapped command (for federation forwarding)
    pub fn send_command_raw(&self, command: crate::command::WrappedCommand) -> Result<(), String> {
        let myko_msg = MykoMessage::Command(command);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame);
        Ok(())
    }

    /// Send a raw wrapped report (for federation forwarding)
    pub fn send_report_raw(&self, report: crate::report::WrappedReport) -> Result<(), String> {
        let myko_msg = MykoMessage::Report(report);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame);
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch Query — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a query and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the current list of matching items.
    /// The Cell updates whenever the server pushes query diffs.
    /// On reconnect, the query is automatically re-subscribed.
    pub fn watch_query<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> Cell<Vec<Arc<Q::Item>>, CellImmutable>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let query: QueryRequest<Q> = query.into();
        let tx: Arc<str> = query.tx.clone();
        let query_id = query.query.query_id();
        let query_item_type = Q::query_item_type_static();

        let query_value = serde_json::to_value(&query).expect("Query should serialize");

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: query_id.clone(),
            query_item_type,
            window: None,
        };

        let cell = Cell::new(vec![]).with_name(query_id.as_ref());
        let cell_weak = cell.downgrade();

        // State for accumulating query diffs
        #[allow(clippy::type_complexity)]
        let state: Arc<Mutex<HashMap<Arc<str>, Arc<Q::Item>>>> = Arc::default();

        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();

        // Register handler for query responses matching this tx
        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    warn!(
                        "watch_query: weak cell dead for query={} tx={}",
                        query_id_for_handler, tx_for_handler
                    );
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_for_handler {
                    return;
                }

                let mut state = state.lock().unwrap();

                if response.sequence == 0 {
                    trace!("Sequence reset: Clearing {} state", query_id_for_handler);
                    state.clear();
                }

                let upserts: Vec<Arc<Q::Item>> = response
                    .upserts
                    .iter()
                    .filter_map(
                        |x| match serde_json::from_value::<Q::Item>(x.item.clone()) {
                            Ok(item) => Some(Arc::new(item)),
                            Err(e) => {
                                error!(
                                    "Failed to parse query '{}' upsert as {}: {}",
                                    query_id_for_handler,
                                    std::any::type_name::<Q::Item>(),
                                    e
                                );
                                None
                            }
                        },
                    )
                    .collect();

                for up in upserts.iter() {
                    state.insert(up.id().clone(), up.clone());
                }

                for del in response.deletes.iter() {
                    state.remove(del);
                }

                cell_writer.set(state.values().cloned().collect());
            }),
        );

        // Build the frame to send (and re-send on reconnect)
        let msg = MykoMessage::Query(wrapped);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let send_query_id = query_id.clone();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(_) => match socket.send(frame_clone.clone()) {
                        Ok(_) => debug!("Watching query {send_query_id}"),
                        Err(e) => error!("Could not send query: {e:?}"),
                    },
                    _ => {
                        debug!("Query {send_query_id} disconnected");
                    }
                }
            }
        });

        // Send immediately if connected
        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);
        cell.own(query_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch Report — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a report and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the latest report value (None until first response).
    /// On reconnect, the report is automatically re-subscribed.
    pub fn watch_report<R, O>(
        &self,
        report: impl Into<ReportRequest<R>>,
    ) -> Cell<Option<O>, CellImmutable>
    where
        R: ReportParams + ReportIdStatic + Clone,
        O: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let report: ReportRequest<R> = report.into();
        let report_id: Arc<str> = R::report_id_static().into();

        // NOTE(ts): Cache key is report_id + params hash (excludes tx).
        // Identical report params share a single WS subscription.
        let cache_key = format!("{}:{:016x}", report_id, report.report.cache_key_hash());

        // Cache hit: if the cell is still alive (has subscribers), reuse it.
        if let Some(existing) = self.inner.report_cache.get(&cache_key) {
            if let Some(entry) = existing
                .value()
                .as_any()
                .downcast_ref::<ClientReportCacheEntry<Option<O>>>()
                && let Some(shared) = entry.get()
            {
                debug!("watch_report: cache hit for {cache_key}");
                return shared;
            }
            drop(existing);
            self.inner.report_cache.remove(&cache_key);
        }

        let tx: Arc<str> = report.tx.clone();

        let report_value = serde_json::to_value(&report).expect("Report should serialize");
        let wrapped = WrappedReport {
            report: report_value,
            report_id: report_id.to_string(),
        };

        let cell = Cell::new(None).with_name(report_id.as_ref());
        let cell_weak = cell.downgrade();

        // Register handler for report responses matching this tx
        let report_id_for_handler = report_id.clone();
        let tx_for_handler = tx.clone();
        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(move |response: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    warn!(
                        "watch_report: weak cell dead for report={} tx={}",
                        report_id_for_handler, tx_for_handler
                    );
                    return;
                };
                match serde_json::from_value::<O>(response) {
                    Ok(data) => cell_writer.set(Some(data)),
                    Err(e) => error!("Could not parse report value: {e:?}"),
                }
            }),
        );

        // Build the frame to send
        let msg = MykoMessage::Report(wrapped);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let send_report_id = report_id.clone();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                match socket.send(frame_clone.clone()) {
                    Ok(_) => debug!("Watching report {send_report_id}"),
                    Err(e) => error!("Could not send report: {e:?}"),
                }
            }
        });

        // Send immediately if connected
        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);
        cell.own(report_cancel_guard(tx, self.inner.clone()));

        let locked = cell.lock();

        // Store weak ref in cache — dies when all subscribers drop.
        self.inner
            .report_cache
            .insert(cache_key, Box::new(ClientReportCacheEntry::new(&locked)));

        locked
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch View — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a view and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the current list of items in the view.
    /// The Cell updates whenever the server pushes view diffs.
    /// On reconnect, the view is automatically re-subscribed.
    pub fn watch_view<V>(
        &self,
        view: impl Into<ViewRequest<V>>,
    ) -> Cell<Vec<V::Item>, CellImmutable>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let view: ViewRequest<V> = view.into();
        let tx: Arc<str> = view.tx.clone();
        let view_id = view.view.view_id();

        let wrapped = wrap_view(tx.clone(), &view.view).expect("View should serialize");

        let cell = Cell::new(vec![]).with_name(view_id.as_ref());
        let cell_weak = cell.downgrade();

        let state: Arc<Mutex<HashMap<Arc<str>, V::Item>>> = Arc::default();
        let tx_for_handler = tx.clone();
        let view_id_for_handler = view_id.clone();

        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_for_handler {
                    return;
                }

                let mut state = state.lock().unwrap();

                if response.sequence == 0 {
                    trace!("Sequence reset: Clearing {} state", view_id_for_handler);
                    state.clear();
                }

                let upserts: Vec<V::Item> = response
                    .upserts
                    .iter()
                    .filter_map(
                        |x| match serde_json::from_value::<V::Item>(x.item.clone()) {
                            Ok(item) => Some(item),
                            Err(e) => {
                                error!(
                                    "Failed to parse view '{}' upsert as {}: {}",
                                    view_id_for_handler,
                                    std::any::type_name::<V::Item>(),
                                    e
                                );
                                None
                            }
                        },
                    )
                    .collect();

                for up in upserts.iter() {
                    state.insert(up.id().clone(), up.clone());
                }

                for del in response.deletes.iter() {
                    state.remove(del);
                }

                cell_writer.set(state.values().cloned().collect());
            }),
        );

        let msg = MykoMessage::View(wrapped);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let send_view_id = view_id.clone();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(_) => match socket.send(frame_clone.clone()) {
                        Ok(_) => debug!("Watching view {send_view_id}"),
                        Err(e) => error!("Could not send view: {e:?}"),
                    },
                    _ => {
                        debug!("View {send_view_id} disconnected");
                    }
                }
            }
        });

        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);

        cell.lock()
    }

    /// Watch a report and receive updates as a reactive Cell with an initial value.
    ///
    /// Like `watch_report`, but starts with a concrete initial value instead of None.
    pub fn watch_report_cell<R, O>(
        &self,
        report: impl Into<ReportRequest<R>>,
        initial: O,
    ) -> Cell<O, CellImmutable>
    where
        R: ReportParams + ReportIdStatic + Clone,
        O: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let cell = self.watch_report::<R, O>(report);
        // Map Option<O> -> O using the initial value as default
        cell.map(move |opt| match opt {
            Some(val) => val.clone(),
            None => initial.clone(),
        })
        .materialize()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send Command — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Send a command and get the result as a Cell.
    ///
    /// Returns a Cell<Option<Result<R, String>>> that starts as None
    /// and becomes Some when the server responds.
    pub fn send_command<C, R>(&self, command: &C) -> Cell<Option<Result<R, String>>, CellImmutable>
    where
        C: Serialize + Clone + CommandId,
        R: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let request = CommandRequest::new(command.clone());
        let tx = request.tx.to_string();

        let wrapped = match wrap_command_request(&request) {
            Ok(w) => w,
            Err(e) => {
                let cell = Cell::new(Some(Err(e.to_string())));
                return cell.lock();
            }
        };

        let msg = MykoMessage::Command(wrapped);
        let frame = match self.encode_message(&msg) {
            Ok(f) => f,
            Err(e) => {
                let cell = Cell::new(Some(Err(e)));
                return cell.lock();
            }
        };

        let cell = Cell::new(None).with_name(format!("cmd:{}", command.command_id()).as_str());
        let cell_writer = cell.clone();

        // Register one-shot handler
        {
            let mut handlers = self.inner.command_response_handlers.lock().unwrap();
            handlers.insert(
                tx.clone(),
                Box::new(move |result: Result<Value, String>| {
                    let mapped = result.and_then(|value| {
                        serde_json::from_value::<R>(value).map_err(|e| e.to_string())
                    });
                    cell_writer.set(Some(mapped));
                }),
            );
        }

        self.send_or_queue(frame);

        cell.lock()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Handle Command — callback-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Register a handler for incoming commands of a specific type.
    ///
    /// The handler receives the deserialized command and a `CommandResponder`
    /// that can be used to send back a response.
    ///
    /// Returns a `CallbackGuard` — drop it to unregister the handler.
    pub fn on_command<C, F>(&self, handler: F) -> CallbackGuard
    where
        C: DeserializeOwned
            + Clone
            + Send
            + crate::command::CommandId
            + crate::command::CommandIdStatic
            + 'static,
        F: Fn(C, CommandResponder) + Send + Sync + 'static,
    {
        let command_id: Arc<str> = C::COMMAND_ID.into();

        self.inner.command_request_handlers.insert(
            command_id.clone(),
            Box::new(move |value: Value, responder: CommandResponder| {
                if let Ok(cmd) = serde_json::from_value::<C>(value) {
                    handler(cmd, responder);
                }
            }),
        );

        let inner = self.inner.clone();
        let id = command_id;
        CallbackGuard::new(move || {
            inner.command_request_handlers.remove(&id);
        })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic Cell APIs
    // =========================================================================

    /// Dynamic/raw query watch for runtimes that only know wrapped query data.
    pub fn watch_query_raw(&self, query: WrappedQuery) -> Cell<Vec<Value>, CellImmutable> {
        let tx: Arc<str> = query
            .query
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let state: Arc<Mutex<HashMap<Arc<str>, Value>>> = Arc::default();
        let cell = Cell::new(Vec::<Value>::new()).with_name(query.query_id.as_ref());
        let cell_weak = cell.downgrade();
        let state_clone = state.clone();
        let tx_clone = tx.clone();

        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_clone {
                    return;
                }

                let mut state = state_clone.lock().unwrap();

                if response.sequence == 0 {
                    state.clear();
                }

                for wrapped_item in response.upserts {
                    if let Some(id) = wrapped_item.item.get("id").and_then(|v| v.as_str()) {
                        state.insert(id.into(), wrapped_item.item);
                    }
                }

                for id in response.deletes {
                    state.remove(&id);
                }

                let items: Vec<Value> = state.values().cloned().collect();
                cell_writer.set(items);
            }),
        );

        // Build frame and set up reconnection
        let msg = MykoMessage::Query(query);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_clone.clone());
            }
        });

        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);
        cell.own(query_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Dynamic/raw view watch for runtimes that only know wrapped view data.
    ///
    /// Mirrors [`watch_query_raw`](Self::watch_query_raw) but sends a
    /// `View` message and cancels with a `ViewCancel` on drop. View
    /// responses share the same `ws:m:view-response` event tag handling
    /// path as queries on the wire, so the same `query_handlers` slot
    /// stores the dispatch closure.
    pub fn watch_view_raw(&self, view: WrappedView) -> Cell<Vec<Value>, CellImmutable> {
        let tx: Arc<str> = view
            .view
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let state: Arc<Mutex<HashMap<Arc<str>, Value>>> = Arc::default();
        let cell = Cell::new(Vec::<Value>::new()).with_name(view.view_id.as_ref());
        let cell_weak = cell.downgrade();
        let state_clone = state.clone();
        let tx_clone = tx.clone();

        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_clone {
                    return;
                }

                let mut state = state_clone.lock().unwrap();

                if response.sequence == 0 {
                    state.clear();
                }

                for wrapped_item in response.upserts {
                    if let Some(id) = wrapped_item.item.get("id").and_then(|v| v.as_str()) {
                        state.insert(id.into(), wrapped_item.item);
                    }
                }

                for id in response.deletes {
                    state.remove(&id);
                }

                let items: Vec<Value> = state.values().cloned().collect();
                cell_writer.set(items);
            }),
        );

        let msg = MykoMessage::View(view);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_clone.clone());
            }
        });

        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);
        cell.own(view_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Dynamic/raw report watch for runtimes that only know wrapped report data.
    pub fn watch_report_raw(
        &self,
        report: crate::report::WrappedReport,
    ) -> Cell<Option<Value>, CellImmutable> {
        let tx: Arc<str> = report
            .report
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let cell = Cell::new(None).with_name(report.report_id.as_str());
        let cell_weak = cell.downgrade();

        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(move |response: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };
                cell_writer.set(Some(response));
            }),
        );

        let msg = MykoMessage::Report(report);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize report");

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_clone.clone());
            }
        });

        if let ConnectionStatus::Connected(_) = status_cell.get() {
            let _ = self.inner.socket.send(frame);
        }

        cell.own(status_guard);
        cell.own(report_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Send a raw wrapped command and receive result as a reactive cell.
    pub fn send_command_raw_result(
        &self,
        command: WrappedCommand,
    ) -> Cell<Option<Result<Value, String>>, CellImmutable> {
        let tx = command
            .command
            .get("tx")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let cell = Cell::new(None).with_name(format!("cmd:{}", command.command_id).as_str());
        let cell_writer = cell.clone();

        {
            let mut handlers = self.inner.command_response_handlers.lock().unwrap();
            handlers.insert(
                tx.clone(),
                Box::new(move |result: Result<Value, String>| {
                    cell_writer.set(Some(result));
                }),
            );
        }

        if let Ok(frame) = self.encode_message(&MykoMessage::Command(command)) {
            self.send_or_queue(frame);
        } else {
            cell.set(Some(Err("Could not serialize command".to_string())));
        }

        cell.lock()
    }
}
