use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;
use url::Url;

#[derive(Clone, Debug, PartialEq)]
pub enum SocketConnectionStatus {
    Disconnected,
    Connecting(String),
    Connected(String),
}

pub struct AutoReconnectSocket {
    status_tx: watch::Sender<SocketConnectionStatus>,
    pub status_rx: watch::Receiver<SocketConnectionStatus>,
    pub incoming: tokio::sync::broadcast::Sender<Message>,
    pub outgoing: tokio::sync::broadcast::Sender<Message>,
    /// Token to cancel the current connection/reconnection loop
    teardown: Arc<std::sync::Mutex<Option<CancellationToken>>>,
}

impl Default for AutoReconnectSocket {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoReconnectSocket {
    pub fn new() -> Self {
        let (status_tx, status_rx) = watch::channel(SocketConnectionStatus::Disconnected);
        Self {
            status_tx,
            status_rx,
            incoming: tokio::sync::broadcast::channel(1000).0,
            outgoing: tokio::sync::broadcast::channel(1000).0,
            teardown: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Get the current connection status
    pub fn get_status(&self) -> SocketConnectionStatus {
        self.status_rx.borrow().clone()
    }

    /// Subscribe to status changes. Returns a receiver that can be used as a stream.
    pub fn subscribe_status(&self) -> watch::Receiver<SocketConnectionStatus> {
        self.status_rx.clone()
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let current_status = self.status_rx.borrow().clone();

        // Cancel existing connection/reconnection loop
        if let SocketConnectionStatus::Connected(ref current_addr)
        | SocketConnectionStatus::Connecting(ref current_addr) = current_status
        {
            if Some(current_addr.clone()) == addr {
                info!("Already connected to {current_addr}");
                return;
            }
        }

        // Cancel the reconnection loop via stored token
        if let Ok(guard) = self.teardown.lock() {
            if let Some(ref teardown) = *guard {
                teardown.cancel();
            }
        }
        {
            let mut guard = self.teardown.lock().unwrap();
            *guard = None;
        }
        let _ = self.status_tx.send(SocketConnectionStatus::Disconnected);

        // Start new connection if address provided
        if let Some(addr) = addr {
            info!("Setting up connection to {addr}");
            self.build(addr);
        }
    }

    /// Close the socket and stop any reconnection attempts.
    /// This cancels the reconnection loop even if currently disconnected and retrying.
    pub fn close(&self) {
        info!("Closing socket and stopping reconnection");
        if let Ok(guard) = self.teardown.lock() {
            if let Some(ref teardown) = *guard {
                teardown.cancel();
            }
        }
        {
            let mut guard = self.teardown.lock().unwrap();
            *guard = None;
        }
        let _ = self.status_tx.send(SocketConnectionStatus::Disconnected);
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
        let recv = self.incoming.clone();
        let status_tx = self.status_tx.clone();
        let teardown_clone = teardown.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = teardown_clone.cancelled() => {
                    debug!("Connection teardown requested");
                }
                _ = async {
                    loop {
                        info!("Connecting to {addr}");

                        // Parse URL with fallback to ws:// scheme
                        let url = match Self::parse_websocket_url(&addr) {
                            Ok(url) => url,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };

                        let ws_stream = match connect_async(&url).await {
                            Ok((ws_stream, _)) => ws_stream,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };

                        info!("Autoreconnect socket Connected to {url}");

                        let (write, read) = ws_stream.split();

                        // Spawn write task
                        let write_task = Self::spawn_write_task(send.subscribe(), write, teardown.clone());

                        // Spawn read task
                        let read_task = Self::spawn_read_task(read, recv.clone(), teardown.clone());

                        let _ = status_tx.send(SocketConnectionStatus::Connected(addr.clone()));

                        select! {
                            _ = write_task => {
                                warn!("Websocket Write Task Exited");
                            }
                            _ = read_task => {
                                warn!("Websocket Read Task Exited");
                            }
                        }


                        let _ = status_tx.send(SocketConnectionStatus::Disconnected);
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
        mut receiver: tokio::sync::broadcast::Receiver<Message>,
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
                    let msg = match msg_result {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("Error receiving message to send: {e:?}");
                            continue;
                        }
                    };

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
        sender: tokio::sync::broadcast::Sender<Message>,
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
                            if sender.send(msg).is_err() {
                                debug!("No Downstream Listeners");
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
