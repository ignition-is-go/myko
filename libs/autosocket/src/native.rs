use std::{
    net::TcpStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use hyphae::{Cell, CellImmutable, CellMutable, Gettable, Mutable};
use log::{debug, error, info, warn};
use tungstenite::{Message, WebSocket, connect, stream::MaybeTlsStream};
use url::Url;

use crate::{SocketConnectionStatus, SocketTransport, WsFrame};

const OUTGOING_QUEUE_CAPACITY: usize = 1024;

type SharedSocket = Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>;

struct ConnectionSession {
    cancel: Arc<AtomicBool>,
    read_handle: JoinHandle<()>,
    write_handle: JoinHandle<()>,
}

impl ConnectionSession {
    fn start(
        socket: SharedSocket,
        incoming_tx: flume::Sender<WsFrame>,
        outgoing_rx: flume::Receiver<WsFrame>,
    ) -> Self {
        let cancel = Arc::new(AtomicBool::new(false));

        let read_cancel = Arc::clone(&cancel);
        let read_socket = Arc::clone(&socket);
        let read_handle = thread::spawn(move || {
            AutoReconnectSocket::run_read_loop(read_socket, incoming_tx, read_cancel);
        });

        let write_cancel = Arc::clone(&cancel);
        let write_socket = Arc::clone(&socket);
        let write_handle = thread::spawn(move || {
            AutoReconnectSocket::run_write_loop(write_socket, outgoing_rx, write_cancel);
        });

        Self {
            cancel,
            read_handle,
            write_handle,
        }
    }

    fn wait_for_disconnect_reason(&self, worker_cancel: &Arc<AtomicBool>) -> Option<&'static str> {
        while !worker_cancel.load(Ordering::SeqCst) {
            if self.read_handle.is_finished() {
                return Some("read loop exited");
            }
            if self.write_handle.is_finished() {
                return Some("write loop exited");
            }
            thread::sleep(Duration::from_millis(50));
        }
        None
    }

    fn shutdown(self) {
        self.cancel.store(true, Ordering::SeqCst);
        let _ = self.read_handle.join();
        let _ = self.write_handle.join();
    }
}

pub struct AutoReconnectSocket {
    /// What the caller wants this socket to be doing.
    intended_status: Cell<SocketConnectionStatus, CellMutable>,
    /// What the socket is actually doing right now.
    actual_status: Cell<SocketConnectionStatus, CellMutable>,
    /// Shared outgoing channel for sync+async producers.
    outgoing_tx: flume::Sender<WsFrame>,
    outgoing_rx: flume::Receiver<WsFrame>,
    /// Shared incoming channel for sync+async consumers.
    incoming_tx: flume::Sender<WsFrame>,
    incoming_rx: flume::Receiver<WsFrame>,
    /// Cancellation token for the active worker loop.
    worker_cancel: Mutex<Arc<AtomicBool>>,
    /// Whether to automatically reconnect after failures/disconnects
    auto_reconnect: bool,
    /// Max websocket message size in bytes.
    max_message_size_bytes: usize,
    /// Max websocket frame size in bytes.
    max_frame_size_bytes: usize,
}

impl Default for AutoReconnectSocket {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversions between WsFrame and tungstenite::Message
// ─────────────────────────────────────────────────────────────────────────────

fn frame_to_message(frame: WsFrame) -> Message {
    match frame {
        WsFrame::Text(s) => Message::Text(s.into()),
        WsFrame::Binary(b) => Message::Binary(b.into()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SocketTransport implementation
// ─────────────────────────────────────────────────────────────────────────────

impl SocketTransport for AutoReconnectSocket {
    fn set_addr(&self, addr: Option<String>) {
        self.set_addr(addr);
    }

    fn close(&self) {
        self.close();
    }

    fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.intended_status.clone().lock()
    }

    fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.actual_status.clone().lock()
    }

    fn send(&self, frame: WsFrame) -> Result<(), String> {
        self.outgoing_tx
            .send(frame)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming_rx.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core implementation
// ─────────────────────────────────────────────────────────────────────────────

impl AutoReconnectSocket {
    pub fn new() -> Self {
        Self::with_auto_reconnect_and_limits(true, 64 * 1024 * 1024, 64 * 1024 * 1024)
    }

    pub fn with_auto_reconnect(auto_reconnect: bool) -> Self {
        Self::with_auto_reconnect_and_limits(auto_reconnect, 64 * 1024 * 1024, 64 * 1024 * 1024)
    }

    pub fn with_auto_reconnect_and_limits(
        auto_reconnect: bool,
        max_message_size_bytes: usize,
        max_frame_size_bytes: usize,
    ) -> Self {
        // Bounded queue enforces backpressure: producers block when the writer can't keep up.
        let (outgoing_tx, outgoing_rx) = flume::bounded(OUTGOING_QUEUE_CAPACITY);
        let (incoming_tx, incoming_rx) = flume::unbounded();
        Self {
            intended_status: Cell::new(SocketConnectionStatus::Idle)
                .with_name("autosocket.intended_status"),
            actual_status: Cell::new(SocketConnectionStatus::Idle)
                .with_name("autosocket.actual_status"),
            outgoing_tx,
            outgoing_rx,
            incoming_tx,
            incoming_rx,
            worker_cancel: Mutex::new(Arc::new(AtomicBool::new(false))),
            auto_reconnect,
            max_message_size_bytes,
            max_frame_size_bytes,
        }
    }

    /// Get the current connection status
    pub fn get_status(&self) -> SocketConnectionStatus {
        self.actual_status.get()
    }

    /// Update status and notify all status callbacks
    fn set_status(
        status: &Cell<SocketConnectionStatus, CellMutable>,
        new_status: SocketConnectionStatus,
    ) {
        status.set(new_status.clone());
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let current_status = self.actual_status.get();
        let intended_status = match addr.clone() {
            Some(a) => SocketConnectionStatus::Connected(a),
            None => SocketConnectionStatus::Idle,
        };
        self.intended_status.set(intended_status);

        // Cancel existing connection/reconnection loop
        if let SocketConnectionStatus::Connected(ref current_addr)
        | SocketConnectionStatus::Connecting(ref current_addr)
        | SocketConnectionStatus::Reconnecting(ref current_addr) = current_status
            && Some(current_addr.clone()) == addr
        {
            info!("Already connected to {current_addr}");
            return;
        }

        self.stop_worker();
        Self::set_status(&self.actual_status, SocketConnectionStatus::Disconnected);

        // Start new connection if address provided
        if let Some(addr) = addr {
            info!("Setting up connection to {addr}");
            self.build(addr);
        } else {
            Self::set_status(&self.actual_status, SocketConnectionStatus::Idle);
        }
    }

    /// Close the socket and stop any reconnection attempts.
    pub fn close(&self) {
        info!("Closing socket and stopping reconnection");
        self.intended_status.set(SocketConnectionStatus::Idle);
        self.stop_worker();
        Self::set_status(&self.actual_status, SocketConnectionStatus::Idle);
    }

    fn build(&self, addr: String) {
        info!("Building Connection to {addr}");

        let outgoing_rx = self.outgoing_rx.clone();
        let incoming_tx = self.incoming_tx.clone();
        let actual_status = self.actual_status.clone();
        let worker_cancel = self.worker_cancel.lock().unwrap().clone();
        let auto_reconnect = self.auto_reconnect;
        let max_message_size_bytes = self.max_message_size_bytes;
        let max_frame_size_bytes = self.max_frame_size_bytes;

        thread::spawn(move || {
            let mut attempt: u64 = 0;
            while !worker_cancel.load(Ordering::SeqCst) {
                attempt = attempt.saturating_add(1);
                Self::set_status(
                    &actual_status,
                    if attempt == 1 {
                        SocketConnectionStatus::Connecting(addr.clone())
                    } else {
                        SocketConnectionStatus::Reconnecting(addr.clone())
                    },
                );

                let url = match Self::parse_websocket_url(&addr) {
                    Ok(url) => url,
                    Err(_) => {
                        if !auto_reconnect {
                            Self::set_status(&actual_status, SocketConnectionStatus::Disconnected);
                            break;
                        }
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };

                let (mut ws, _) = match connect(url.as_str()) {
                    Ok(pair) => pair,
                    Err(e) => {
                        if !auto_reconnect {
                            error!(
                                "Failed to connect to {} (attempt {}): {}. Auto-reconnect disabled; giving up.",
                                url, attempt, e
                            );
                            Self::set_status(&actual_status, SocketConnectionStatus::Disconnected);
                            break;
                        }
                        error!(
                            "Failed to connect to {} (attempt {}): {}. Retrying in 1s...",
                            url, attempt, e
                        );
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };

                // Keep native socket limits aligned with myko transport settings.
                ws.set_config(|cfg| {
                    cfg.max_message_size = Some(max_message_size_bytes);
                    cfg.max_frame_size = Some(max_frame_size_bytes);
                });

                // Use non-blocking read loop so cancellation can take effect quickly.
                if let MaybeTlsStream::Plain(stream) = ws.get_mut() {
                    let _ = stream.set_nonblocking(true);
                }

                attempt = 0;
                info!("Autoreconnect socket Connected to {url}");

                let socket = Arc::new(Mutex::new(ws));
                Self::set_status(
                    &actual_status,
                    SocketConnectionStatus::Connected(addr.clone()),
                );

                let session = ConnectionSession::start(
                    Arc::clone(&socket),
                    incoming_tx.clone(),
                    outgoing_rx.clone(),
                );
                let disconnect_reason = session.wait_for_disconnect_reason(&worker_cancel);
                session.shutdown();

                if let Some(reason) = disconnect_reason {
                    warn!(
                        "WebSocket disconnected from {}: {}. auto_reconnect={}",
                        addr, reason, auto_reconnect
                    );
                } else if worker_cancel.load(Ordering::SeqCst) {
                    info!("WebSocket worker for {} stopped by teardown", addr);
                }

                Self::set_status(&actual_status, SocketConnectionStatus::Disconnected);
                if !auto_reconnect {
                    break;
                }
                if worker_cancel.load(Ordering::SeqCst) {
                    break;
                }
                warn!("Retrying WebSocket connection to {} in 1s", addr);
                thread::sleep(Duration::from_secs(1));
            }
        });
    }

    fn parse_websocket_url(addr: &str) -> Result<String, ()> {
        let url = match Url::parse(addr).or_else(|_| Url::parse(&format!("ws://{addr}"))) {
            Ok(url) => url,
            Err(e) => {
                error!("Could not parse URL: {e} for {addr}");
                return Err(());
            }
        };

        let mut url = url;
        if url.scheme() != "ws" && url.scheme() != "wss" {
            let _ = url.set_scheme("ws");
        }

        Ok(url.to_string())
    }

    fn run_write_loop(
        socket: Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>,
        receiver: flume::Receiver<WsFrame>,
        teardown: Arc<AtomicBool>,
    ) {
        debug!("Starting Write Loop");
        while !teardown.load(Ordering::SeqCst) {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(frame) => {
                    let msg = frame_to_message(frame);
                    let mut attempts: u64 = 0;
                    loop {
                        if teardown.load(Ordering::SeqCst) {
                            break;
                        }
                        attempts = attempts.saturating_add(1);
                        let send_result = {
                            let mut ws = socket.lock().unwrap();
                            ws.send(msg.clone())
                        };
                        match send_result {
                            Ok(()) => break,
                            Err(tungstenite::Error::Io(e))
                                if e.kind() == std::io::ErrorKind::WouldBlock
                                    || e.kind() == std::io::ErrorKind::TimedOut
                                    || e.kind() == std::io::ErrorKind::Interrupted =>
                            {
                                if attempts == 1 || attempts.is_multiple_of(100) {
                                    warn!(
                                        "Websocket write backpressure (kind={:?}) queue_len={} attempts={}",
                                        e.kind(),
                                        receiver.len(),
                                        attempts
                                    );
                                }
                                thread::sleep(Duration::from_millis(10));
                            }
                            Err(e) => {
                                error!(
                                    "Websocket write failed: {e:?}; queue_len={} attempts={}",
                                    receiver.len(),
                                    attempts
                                );
                                break;
                            }
                        }
                    }
                }
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(flume::RecvTimeoutError::Disconnected) => {
                    error!("Outgoing channel disconnected");
                    break;
                }
            }
        }

        if let Ok(mut ws) = socket.lock() {
            let _ = ws.close(None);
        }
        debug!("Websocket Write Loop Exited");
    }

    fn run_read_loop(
        socket: Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>,
        incoming_tx: flume::Sender<WsFrame>,
        teardown: Arc<AtomicBool>,
    ) {
        debug!("Starting Read Loop");
        while !teardown.load(Ordering::SeqCst) {
            let msg_result = {
                let mut ws = socket.lock().unwrap();
                ws.read()
            };
            match msg_result {
                Ok(msg) => match msg {
                    Message::Text(text) => {
                        let frame = WsFrame::Text(text.to_string());
                        let _ = incoming_tx.send(frame);
                    }
                    Message::Binary(bin) => {
                        let frame = WsFrame::Binary(bin.to_vec());
                        let _ = incoming_tx.send(frame);
                    }
                    Message::Ping(payload) => {
                        debug!("Websocket received Ping ({} bytes)", payload.len());
                    }
                    Message::Pong(payload) => {
                        debug!("Websocket received Pong ({} bytes)", payload.len());
                    }
                    Message::Close(frame) => {
                        warn!("Websocket received Close: {:?}", frame);
                        break;
                    }
                    _ => {}
                },
                Err(tungstenite::Error::Io(e))
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(tungstenite::Error::ConnectionClosed)
                | Err(tungstenite::Error::AlreadyClosed) => {
                    warn!("Websocket read stream ended");
                    break;
                }
                Err(e) => {
                    error!("Websocket read failed: {e:?}");
                    break;
                }
            }
        }
        debug!("Websocket read loop exited");
    }

    fn stop_worker(&self) {
        let mut guard = self.worker_cancel.lock().unwrap();
        guard.store(true, Ordering::SeqCst);
        *guard = Arc::new(AtomicBool::new(false));
    }

    pub fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.intended_status.clone().lock()
    }

    pub fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.actual_status.clone().lock()
    }

    pub fn write_tx(&self) -> flume::Sender<WsFrame> {
        self.outgoing_tx.clone()
    }

    pub fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming_rx.clone()
    }
}
