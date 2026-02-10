use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};

use autosocket::{CallbackGuard, SocketConnectionStatus, SocketTransport, WsFrame};
use dashmap::DashMap;
use hypha::{Cell, CellImmutable, CellMutable, Gettable, MapExt, Mutable, Watchable};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use url::Url;

use crate::{
    command::{CommandId, CommandRequest},
    common::with_id::WithId,
    core::item::Eventable,
    query::{QueryParams, QueryRequest},
    report::{ReportIdStatic, ReportParams, ReportRequest},
    wire::{MEvent, MykoMessage, WrappedQuery, WrappedReport, wrap_command_request},
};

/// Wire protocol for encoding messages.
/// Defaults to MSGPACK for better performance - server auto-detects binary frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub enum MykoProtocol {
    JSON = 0,
    MSGPACK = 1,
}

impl From<u8> for MykoProtocol {
    fn from(v: u8) -> Self {
        match v {
            0 => MykoProtocol::JSON,
            _ => MykoProtocol::MSGPACK,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
#[serde(rename_all = "camelCase", tag = "type", content = "data")]
pub enum ConnectionStatus {
    Connected(String),
    Disconnected,
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
        MykoProtocol::MSGPACK => rmp_serde::to_vec(msg).ok().map(WsFrame::Binary),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MykoClient
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MykoClient {
    inner: Arc<MykoClientInner>,
}

struct MykoClientInner {
    socket: Arc<dyn SocketTransport>,
    protocol: Arc<AtomicU8>,
    connection_status: Cell<ConnectionStatus, CellMutable>,

    // Dispatch maps keyed by tx
    query_handlers: DashMap<Arc<str>, QueryHandler>,
    report_handlers: DashMap<Arc<str>, ReportHandler>,
    command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>>,
    command_request_handlers: DashMap<Arc<str>, CommandRequestHandler>,

    // Frames queued while disconnected
    pending_sends: Mutex<Vec<WsFrame>>,

    // Guards that keep subscriptions alive
    _message_guard: CallbackGuard,
    _status_guard: CallbackGuard,
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
        #[cfg(not(target_arch = "wasm32"))]
        let socket: Arc<dyn SocketTransport> = Arc::new(autosocket::AutoReconnectSocket::new());

        #[cfg(target_arch = "wasm32")]
        let socket: Arc<dyn SocketTransport> = Arc::new(autosocket::WasmSocket::new());

        Self::with_transport(socket)
    }

    /// Create a MykoClient with a custom transport implementation.
    pub fn with_transport(transport: Arc<dyn SocketTransport>) -> MykoClient {
        let protocol = Arc::new(AtomicU8::new(MykoProtocol::MSGPACK as u8));
        let connection_status = Cell::new(ConnectionStatus::Disconnected);

        let query_handlers: DashMap<Arc<str>, QueryHandler> = DashMap::new();
        let report_handlers: DashMap<Arc<str>, ReportHandler> = DashMap::new();
        let command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>> =
            Mutex::new(HashMap::new());
        let command_request_handlers: DashMap<Arc<str>, CommandRequestHandler> = DashMap::new();
        let pending_sends: Mutex<Vec<WsFrame>> = Mutex::new(Vec::new());

        // We need to set up the callbacks, but they reference the inner struct.
        // Use a two-step initialization: create with noop guards, then replace.
        let inner = Arc::new_cyclic(|weak| {
            let weak_for_msg = weak.clone();
            let message_guard = transport.on_message(Box::new(move |frame| {
                let Some(inner) = weak_for_msg.upgrade() else {
                    return;
                };
                Self::handle_frame(&inner, &frame);
            }));

            let weak_for_status = weak.clone();
            let status_guard = transport.on_status_change(Box::new(move |status| {
                let Some(inner) = weak_for_status.upgrade() else {
                    return;
                };
                let conn_status = match &status {
                    SocketConnectionStatus::Connecting(_) => ConnectionStatus::Disconnected,
                    SocketConnectionStatus::Connected(addr) => {
                        ConnectionStatus::Connected(addr.clone())
                    }
                    SocketConnectionStatus::Disconnected => ConnectionStatus::Disconnected,
                };

                inner.connection_status.set(conn_status.clone());

                // Flush pending sends on connect
                if let ConnectionStatus::Connected(_) = conn_status {
                    let mut pending = inner.pending_sends.lock().unwrap();
                    for frame in pending.drain(..) {
                        let _ = inner.socket.send(frame);
                    }
                }
            }));

            MykoClientInner {
                socket: transport.clone(),
                protocol: protocol.clone(),
                connection_status,
                query_handlers,
                report_handlers,
                command_response_handlers,
                command_request_handlers,
                pending_sends,
                _message_guard: message_guard,
                _status_guard: status_guard,
            }
        });

        MykoClient { inner }
    }

    /// Handle an incoming WebSocket frame by dispatching to registered handlers.
    fn handle_frame(inner: &MykoClientInner, frame: &WsFrame) {
        let Some(value) = Self::decode_message(frame) else {
            return;
        };

        let parsed = match serde_json::from_value::<MykoMessage>(value.clone()) {
            Ok(msg) => msg,
            Err(_) => return,
        };

        match parsed {
            MykoMessage::QueryResponse(response) => {
                let tx: Arc<str> = response.tx.clone();
                if let Some(handler) = inner.query_handlers.get(&tx) {
                    if let Ok(response_value) = serde_json::to_value(&response) {
                        handler(response_value);
                    }
                }
            }
            MykoMessage::ReportResponse(response) => {
                let tx: Arc<str> = response.tx.clone().into();
                if let Some(handler) = inner.report_handlers.get(&tx) {
                    handler(response.response);
                }
            }
            MykoMessage::CommandResponse(response) => {
                let mut handlers = inner.command_response_handlers.lock().unwrap();
                if let Some(handler) = handlers.remove(&response.tx) {
                    handler(Ok(response.response));
                }
            }
            MykoMessage::CommandError(err) => {
                let mut handlers = inner.command_response_handlers.lock().unwrap();
                if let Some(handler) = handlers.remove(&err.tx) {
                    handler(Err(err.message));
                }
            }
            MykoMessage::Command(wrapped) => {
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
            _ => {}
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Protocol and encoding
    // ─────────────────────────────────────────────────────────────────────────

    /// Set the wire protocol (JSON or MSGPACK). Default is MSGPACK.
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
            MykoProtocol::MSGPACK => {
                let bytes = rmp_serde::to_vec(msg).map_err(|e| e.to_string())?;
                Ok(WsFrame::Binary(bytes))
            }
        }
    }

    /// Decode a WebSocket frame according to its type.
    fn decode_message(frame: &WsFrame) -> Option<Value> {
        match frame {
            WsFrame::Text(content) => serde_json::from_str::<Value>(content).ok(),
            WsFrame::Binary(bytes) => rmp_serde::from_slice::<Value>(bytes).ok(),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Connection
    // ─────────────────────────────────────────────────────────────────────────

    /// Get a reactive cell of the connection status.
    pub fn connection_status(&self) -> &Cell<ConnectionStatus, CellMutable> {
        &self.inner.connection_status
    }

    /// Get the current connection status synchronously.
    pub fn get_connection_status_sync(&self) -> ConnectionStatus {
        self.inner.connection_status.get()
    }

    pub fn set_address(&self, addr: Option<String>) {
        if addr.is_none() {
            debug!("Setting address to None, disconnecting socket");
            self.inner.socket.set_addr(None);
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

        info!("MykoClient connecting to {}", parsed);
        self.inner.socket.set_addr(Some(parsed.to_string()));
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
        if let ConnectionStatus::Connected(_) = self.inner.connection_status.get() {
            let _ = self.inner.socket.send(frame);
        } else {
            self.inner.pending_sends.lock().unwrap().push(frame);
        }
    }

    pub fn send_event(&self, event: MEvent) -> Result<(), String> {
        let myko_msg = MykoMessage::Event(event);
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
    ) -> Cell<Vec<Q::Item>, CellImmutable>
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
        };

        let cell = Cell::new(vec![]);
        let cell_writer = cell.clone();

        // State for accumulating query diffs
        let state: Arc<Mutex<HashMap<Arc<str>, Q::Item>>> = Arc::default();

        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();

        // Register handler for query responses matching this tx
        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Ok(response) =
                    serde_json::from_value::<crate::wire::QueryResponse>(response_value)
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

                let upserts: Vec<Q::Item> = response
                    .upserts
                    .iter()
                    .filter_map(|x| serde_json::from_value::<Q::Item>(x.item.clone()).ok())
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
        let send_query_id = query_id.clone();
        let frame_clone = frame.clone();
        let status_guard = self.inner.connection_status.subscribe(move |signal| {
            if let hypha::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(_) => match socket.send(frame_clone.clone()) {
                        Ok(_) => debug!("Watching query {send_query_id}"),
                        Err(e) => error!("Could not send query: {e:?}"),
                    },
                    ConnectionStatus::Disconnected => {
                        warn!("Query {send_query_id} Disconnected");
                    }
                }
            }
        });

        // Send immediately if connected
        if let ConnectionStatus::Connected(_) = self.inner.connection_status.get() {
            let _ = self.inner.socket.send(frame);
        }

        // Tie the reconnection guard's lifetime to the cell
        cell.own(status_guard);

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
        O: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let report: ReportRequest<R> = report.into();
        let report_id: Arc<str> = R::report_id_static().into();
        let tx: Arc<str> = report.tx.clone();

        let report_value = serde_json::to_value(&report).expect("Report should serialize");
        let wrapped = WrappedReport {
            report: report_value,
            report_id: report_id.to_string(),
        };

        let cell = Cell::new(None);
        let cell_writer = cell.clone();

        // Register handler for report responses matching this tx
        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(
                move |response: Value| match serde_json::from_value::<O>(response) {
                    Ok(data) => cell_writer.set(Some(data)),
                    Err(e) => error!("Could not parse report value: {e:?}"),
                },
            ),
        );

        // Build the frame to send
        let msg = MykoMessage::Report(wrapped);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let send_report_id = report_id.clone();
        let frame_clone = frame.clone();
        let status_guard = self.inner.connection_status.subscribe(move |signal| {
            if let hypha::Signal::Value(status) = signal {
                if let ConnectionStatus::Connected(_) = &**status {
                    match socket.send(frame_clone.clone()) {
                        Ok(_) => debug!("Watching report {send_report_id}"),
                        Err(e) => error!("Could not send report: {e:?}"),
                    }
                }
            }
        });

        // Send immediately if connected
        if let ConnectionStatus::Connected(_) = self.inner.connection_status.get() {
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
        O: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let cell = self.watch_report::<R, O>(report);
        // Map Option<O> -> O using the initial value as default
        cell.map(move |opt| match opt {
            Some(val) => val.clone(),
            None => initial.clone(),
        })
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
        R: DeserializeOwned + Clone + Send + Sync + 'static,
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

        let cell = Cell::new(None);
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
                match serde_json::from_value::<C>(value) {
                    Ok(cmd) => handler(cmd, responder),
                    Err(_) => {} // Not for this handler
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
    // Message callback (raw)
    // ─────────────────────────────────────────────────────────────────────────

    /// Register a callback for all incoming messages (decoded as Value).
    /// Drop the returned guard to unsubscribe.
    pub fn on_message(&self, cb: impl Fn(Value) + Send + Sync + 'static) -> CallbackGuard {
        self.inner.socket.on_message(Box::new(move |frame| {
            if let Some(value) = Self::decode_message(&frame) {
                cb(value);
            }
        }))
    }

    // =========================================================================
    // FFI-friendly APIs for language bindings (callback-based, JSON in/out)
    // =========================================================================

    /// Watch connection status changes with a callback.
    /// Callback receives JSON: `{"type":"Connected","data":"ws://..."}` or `{"type":"Disconnected"}`
    pub fn watch_connection_status_callback<F>(&self, callback: F) -> CallbackGuard
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let guard = self.inner.connection_status.subscribe(move |signal| {
            if let hypha::Signal::Value(status) = signal {
                if let Ok(json) = serde_json::to_string(&*status) {
                    callback(json);
                }
            }
        });
        // Convert SubscriptionGuard to CallbackGuard
        CallbackGuard::new(move || {
            drop(guard);
        })
    }

    /// Watch a query with a callback that receives the current state as Vec<Value>.
    ///
    /// Returns a cancel function that stops the query when called.
    pub fn watch_query_callback<F>(
        &self,
        query: WrappedQuery,
        callback: F,
    ) -> impl Fn() + Send + Sync
    where
        F: Fn(Vec<Value>) + Send + Sync + 'static,
    {
        let tx: Arc<str> = query
            .query
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let state: Arc<Mutex<HashMap<Arc<str>, Value>>> = Arc::default();
        let callback = Arc::new(callback);

        let state_clone = state.clone();
        let callback_clone = callback.clone();
        let tx_clone = tx.clone();

        // Register handler
        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Ok(response) =
                    serde_json::from_value::<crate::wire::QueryResponse>(response_value)
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
                callback_clone(items);
            }),
        );

        // Build frame and set up reconnection
        let msg = MykoMessage::Query(query);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        let socket = self.inner.socket.clone();
        let frame_clone = frame.clone();
        let status_guard = self.inner.connection_status.subscribe(move |signal| {
            if let hypha::Signal::Value(status) = signal {
                if let ConnectionStatus::Connected(_) = &**status {
                    let _ = socket.send(frame_clone.clone());
                }
            }
        });

        if let ConnectionStatus::Connected(_) = self.inner.connection_status.get() {
            let _ = self.inner.socket.send(frame);
        }

        // Return cancel function — captures status_guard so it lives until cancelled
        let inner = self.inner.clone();
        let cancel_tx = tx;
        let guard = std::sync::Mutex::new(Some(status_guard));
        move || {
            let _ = guard.lock().unwrap().take();
            inner.query_handlers.remove(&cancel_tx);
        }
    }

    /// Send an event using JSON input.
    /// Returns error message if failed, empty string on success.
    pub fn send_event_json(&self, event_json: String) -> String {
        match serde_json::from_str::<MEvent>(&event_json) {
            Ok(event) => match self.send_event(event) {
                Ok(()) => String::new(),
                Err(e) => e,
            },
            Err(e) => e.to_string(),
        }
    }

    /// Watch a report with a callback that receives the report result as Value.
    ///
    /// Returns a cancel function that stops the report when called.
    pub fn watch_report_callback<F>(
        &self,
        report: crate::report::WrappedReport,
        callback: F,
    ) -> impl Fn() + Send + Sync
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        // Extract tx from the report
        let tx: Arc<str> = report
            .report
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let _report_id = report.report_id.clone();
        let callback = Arc::new(callback);
        let callback_clone = callback.clone();

        // Register handler
        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(move |response: Value| {
                callback_clone(response);
            }),
        );

        // Build frame and set up reconnection
        let msg = MykoMessage::Report(report);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize report");

        let socket = self.inner.socket.clone();
        let frame_clone = frame.clone();
        let status_guard = self.inner.connection_status.subscribe(move |signal| {
            if let hypha::Signal::Value(status) = signal {
                if let ConnectionStatus::Connected(_) = &**status {
                    let _ = socket.send(frame_clone.clone());
                }
            }
        });

        if let ConnectionStatus::Connected(_) = self.inner.connection_status.get() {
            let _ = self.inner.socket.send(frame);
        }

        // Return cancel function — captures status_guard so it lives until cancelled
        let inner = self.inner.clone();
        let cancel_tx = tx;
        let guard = std::sync::Mutex::new(Some(status_guard));
        move || {
            let _ = guard.lock().unwrap().take();
            inner.report_handlers.remove(&cancel_tx);
        }
    }
}
