use futures_signals::signal::Mutable;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::select;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;
use url::Url;

#[derive(Clone, Debug)]
pub enum SocketConnectionStatus {
    Disconnected,
    Connecting(String, CancellationToken),
    Connected(String, CancellationToken),
}

pub struct AutoReconnectSocket {
    pub status: Mutable<SocketConnectionStatus>,
    pub incoming: tokio::sync::broadcast::Sender<Message>,
    pub outgoing: tokio::sync::broadcast::Sender<Message>,
    /// Token to cancel the current connection/reconnection loop
    teardown: Mutable<Option<CancellationToken>>,
}

impl Default for AutoReconnectSocket {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoReconnectSocket {
    pub fn new() -> Self {
        Self {
            status: Mutable::new(SocketConnectionStatus::Disconnected),
            incoming: tokio::sync::broadcast::channel(1000).0,
            outgoing: tokio::sync::broadcast::channel(1000).0,
            teardown: Mutable::new(None),
        }
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let current_status = self.status.lock_ref().clone();

        // Cancel existing connection/reconnection loop
        if let SocketConnectionStatus::Connected(current_addr, _)
        | SocketConnectionStatus::Connecting(current_addr, _) = &current_status
        {
            if Some(current_addr.clone()) == addr {
                info!("Already connected to {current_addr}");
                return;
            }
        }

        // Cancel the reconnection loop via stored token
        if let Some(teardown) = self.teardown.lock_ref().as_ref() {
            teardown.cancel();
        }
        self.teardown.set(None);
        self.status.set(SocketConnectionStatus::Disconnected);

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
        if let Some(teardown) = self.teardown.lock_ref().as_ref() {
            teardown.cancel();
        }
        self.teardown.set(None);
        self.status.set(SocketConnectionStatus::Disconnected);
    }

    fn build(&self, addr: String) {
        info!("Building Connection to {addr}");

        let teardown = CancellationToken::new();
        // Store the teardown token so it can be cancelled externally
        self.teardown.set(Some(teardown.clone()));

        let send = self.outgoing.clone();
        let recv = self.incoming.clone();
        let status = self.status.clone();
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

                        status.set(SocketConnectionStatus::Connected(
                            addr.clone(),
                            teardown.clone(),
                        ));

                        select! {
                            _ = write_task => {
                                warn!("Websocket Write Task Exited");
                            }
                            _ = read_task => {
                                warn!("Websocket Read Task Exited");
                            }
                        }


                        status.set(SocketConnectionStatus::Disconnected);
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
