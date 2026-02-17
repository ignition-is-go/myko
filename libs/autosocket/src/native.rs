use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::{select, sync::broadcast};
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;
use url::Url;

use crate::{CallbackGuard, SocketConnectionStatus, SocketTransport, WsFrame, next_callback_id};

type MessageCallback = Box<dyn Fn(WsFrame) + Send + Sync>;
type StatusCallback = Box<dyn Fn(SocketConnectionStatus) + Send + Sync>;

pub struct AutoReconnectSocket {
    /// Current status, updated atomically
    status: Arc<RwLock<SocketConnectionStatus>>,
    /// Callbacks notified on incoming messages
    message_callbacks: Arc<DashMap<u64, MessageCallback>>,
    /// Callbacks notified on status changes
    status_callbacks: Arc<DashMap<u64, StatusCallback>>,
    /// Internal broadcast for write task dispatch (tokio-internal, not exposed)
    outgoing: broadcast::Sender<WsFrame>,
    /// Token to cancel the current connection/reconnection loop
    teardown: Arc<std::sync::Mutex<Option<CancellationToken>>>,
    /// Whether to automatically reconnect after failures/disconnects
    auto_reconnect: bool,
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
        WsFrame::Text(s) => Message::Text(s),
        WsFrame::Binary(b) => Message::Binary(b),
    }
}

fn message_to_frame(msg: Message) -> Option<WsFrame> {
    match msg {
        Message::Text(s) => Some(WsFrame::Text(s)),
        Message::Binary(b) => Some(WsFrame::Binary(b)),
        _ => None,
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

    fn get_status(&self) -> SocketConnectionStatus {
        self.get_status()
    }

    fn send(&self, frame: WsFrame) -> Result<(), String> {
        self.outgoing
            .send(frame)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn on_message(&self, cb: Box<dyn Fn(WsFrame) + Send + Sync>) -> CallbackGuard {
        let id = next_callback_id();
        self.message_callbacks.insert(id, cb);
        let callbacks = self.message_callbacks.clone();
        CallbackGuard::new(move || {
            callbacks.remove(&id);
        })
    }

    fn on_status_change(
        &self,
        cb: Box<dyn Fn(SocketConnectionStatus) + Send + Sync>,
    ) -> CallbackGuard {
        // Call immediately with current status
        let current = self.status.read().unwrap().clone();
        cb(current);

        let id = next_callback_id();
        self.status_callbacks.insert(id, cb);
        let callbacks = self.status_callbacks.clone();
        CallbackGuard::new(move || {
            callbacks.remove(&id);
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core implementation
// ─────────────────────────────────────────────────────────────────────────────

impl AutoReconnectSocket {
    pub fn new() -> Self {
        Self::with_auto_reconnect(true)
    }

    pub fn with_auto_reconnect(auto_reconnect: bool) -> Self {
        Self {
            status: Arc::new(RwLock::new(SocketConnectionStatus::Idle)),
            message_callbacks: Arc::new(DashMap::new()),
            status_callbacks: Arc::new(DashMap::new()),
            outgoing: broadcast::channel(1000).0,
            teardown: Arc::new(std::sync::Mutex::new(None)),
            auto_reconnect,
        }
    }

    /// Get the current connection status
    pub fn get_status(&self) -> SocketConnectionStatus {
        self.status.read().unwrap().clone()
    }

    /// Update status and notify all status callbacks
    fn set_status(
        status: &RwLock<SocketConnectionStatus>,
        callbacks: &DashMap<u64, StatusCallback>,
        new_status: SocketConnectionStatus,
    ) {
        {
            let mut s = status.write().unwrap();
            *s = new_status.clone();
        }
        for entry in callbacks.iter() {
            (entry.value())(new_status.clone());
        }
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let current_status = self.status.read().unwrap().clone();

        // Cancel existing connection/reconnection loop
        if let SocketConnectionStatus::Connected(ref current_addr)
        | SocketConnectionStatus::Connecting(ref current_addr)
        | SocketConnectionStatus::Reconnecting(ref current_addr) = current_status
            && Some(current_addr.clone()) == addr
        {
            info!("Already connected to {current_addr}");
            return;
        }

        // Cancel the reconnection loop via stored token
        if let Ok(guard) = self.teardown.lock()
            && let Some(ref teardown) = *guard
        {
            teardown.cancel();
        }
        {
            let mut guard = self.teardown.lock().unwrap();
            *guard = None;
        }
        Self::set_status(
            &self.status,
            &self.status_callbacks,
            if addr.is_some() {
                SocketConnectionStatus::Disconnected
            } else {
                SocketConnectionStatus::Idle
            },
        );

        // Start new connection if address provided
        if let Some(addr) = addr {
            info!("Setting up connection to {addr}");
            self.build(addr);
        }
    }

    /// Close the socket and stop any reconnection attempts.
    pub fn close(&self) {
        info!("Closing socket and stopping reconnection");
        if let Ok(guard) = self.teardown.lock()
            && let Some(ref teardown) = *guard
        {
            teardown.cancel();
        }
        {
            let mut guard = self.teardown.lock().unwrap();
            *guard = None;
        }
        Self::set_status(
            &self.status,
            &self.status_callbacks,
            SocketConnectionStatus::Idle,
        );
    }

    fn build(&self, addr: String) {
        info!("Building Connection to {addr}");

        let teardown = CancellationToken::new();
        // Store the teardown token so it can be cancelled externally
        {
            let mut guard = self.teardown.lock().unwrap();
            *guard = Some(teardown.clone());
        }

        let send = self.outgoing.clone();
        let message_callbacks = self.message_callbacks.clone();
        let status = self.status.clone();
        let status_callbacks = self.status_callbacks.clone();
        let teardown_clone = teardown.clone();
        let auto_reconnect = self.auto_reconnect;

        tokio::spawn(async move {
            tokio::select! {
                _ = teardown_clone.cancelled() => {
                    debug!("Connection teardown requested");
                }
                _ = async {
                    let mut attempt: u64 = 0;
                    loop {
                        attempt = attempt.saturating_add(1);
                        Self::set_status(
                            &status,
                            &status_callbacks,
                            if attempt == 1 {
                                SocketConnectionStatus::Connecting(addr.clone())
                            } else {
                                SocketConnectionStatus::Reconnecting(addr.clone())
                            },
                        );

                        // Parse URL with fallback to ws:// scheme
                        let url = match Self::parse_websocket_url(&addr) {
                            Ok(url) => url,
                            Err(_) => {
                                if !auto_reconnect {
                                    Self::set_status(
                                        &status,
                                        &status_callbacks,
                                        SocketConnectionStatus::Disconnected,
                                    );
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };

                        let ws_stream = match connect_async(&url).await {
                            Ok((ws_stream, _)) => ws_stream,
                            Err(e) => {
                                if !auto_reconnect {
                                    error!(
                                        "Failed to connect to {} (attempt {}): {}. Auto-reconnect disabled; giving up.",
                                        url, attempt, e
                                    );
                                    Self::set_status(
                                        &status,
                                        &status_callbacks,
                                        SocketConnectionStatus::Disconnected,
                                    );
                                    break;
                                }
                                // Many environments default to showing only errors. If a service is down,
                                // make retry behavior unmistakable.
                                error!(
                                    "Failed to connect to {} (attempt {}): {}. Retrying in 1s...",
                                    url, attempt, e
                                );
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };

                        attempt = 0;
                        info!("Autoreconnect socket Connected to {url}");

                        let (write, read) = ws_stream.split();

                        // Spawn write task
                        let write_task = Self::spawn_write_task(send.subscribe(), write, teardown.clone());

                        // Spawn read task
                        let read_task = Self::spawn_read_task(read, message_callbacks.clone(), teardown.clone());

                        Self::set_status(
                            &status,
                            &status_callbacks,
                            SocketConnectionStatus::Connected(addr.clone()),
                        );

                        select! {
                            _ = write_task => {
                                warn!("Websocket Write Task Exited");
                            }
                            _ = read_task => {
                                warn!("Websocket Read Task Exited");
                            }
                        }

                        Self::set_status(
                            &status,
                            &status_callbacks,
                            SocketConnectionStatus::Disconnected,
                        );
                        if !auto_reconnect {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                } => {}
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

    async fn spawn_write_task(
        mut receiver: broadcast::Receiver<WsFrame>,
        mut write: futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
        teardown: CancellationToken,
    ) {
        debug!("Starting Write Loop");
        loop {
            tokio::select! {
                _ = teardown.cancelled() => {
                    debug!("Write task cancelled");
                    break;
                }
                msg_result = receiver.recv() => {
                    let frame = match msg_result {
                        Ok(frame) => frame,
                        Err(e) => {
                            error!("Error receiving message to send: {e:?}");
                            continue;
                        }
                    };

                    let msg = frame_to_message(frame);
                    if let Err(e) = write.send(msg).await {
                        error!("Websocket write failed: {e:?}");
                        break;
                    }
                }
            }
        }
        debug!("Websocket Write Loop Exited");
    }

    async fn spawn_read_task(
        mut read: futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        message_callbacks: Arc<DashMap<u64, MessageCallback>>,
        teardown: CancellationToken,
    ) {
        debug!("Starting Read Loop");
        loop {
            tokio::select! {
                _ = teardown.cancelled() => {
                    debug!("Read task cancelled");
                    break;
                }
                msg_result = read.next() => {
                    match msg_result {
                        Some(Ok(msg)) => {
                            if let Some(frame) = message_to_frame(msg) {
                                for entry in message_callbacks.iter() {
                                    (entry.value())(frame.clone());
                                }
                            }
                        }
                        Some(Err(_)) | None => break,
                    }
                }
            }
        }
        debug!("Websocket Read Exited");
    }
}
