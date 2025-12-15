use std::sync::Arc;

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use log::{debug, error, info, trace};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};
use tungstenite::{
    Message,
    handshake::server::{Request, Response},
};
use uuid::Uuid;

use crate::{
    actors::{
        message_handler::{MessageHandlerMsg, ProcessTextData},
        ws::websocket_server::WebSocketServerMsg,
    },
    message::MykoMessage,
    runtime::{Actor, ActorHandle, ActorRef},
};

pub struct WebSocketConnection {
    client_id: Arc<str>,
    /// Channel to send messages to the async WebSocket writer task
    ws_tx: mpsc::Sender<Message>,
}

#[derive(Debug)]
pub enum WebSocketConnectionMsg {
    Transmit(MykoMessage),
}

pub struct WebSocketConnectionArgs {
    pub stream: TcpStream,
    pub message_handler: ActorRef<MessageHandlerMsg>,
    pub websocket_server: ActorRef<WebSocketServerMsg>,
    pub server_id: Arc<str>,
}

impl WebSocketConnection {
    pub fn spawn(args: WebSocketConnectionArgs) -> Option<ActorHandle<WebSocketConnectionMsg>> {
        let WebSocketConnectionArgs {
            stream,
            message_handler,
            websocket_server,
            server_id,
        } = args;

        // Get peer address before consuming the stream
        let peer_addr = stream.peer_addr().ok();
        let peer_addr_str = peer_addr
            .map(|addr| format!("{}:{}", addr.ip(), addr.port()))
            .unwrap_or_else(|| "unknown".to_string());

        let client_id: Arc<str> = Uuid::new_v4().to_string().into();

        // Create channel for sending messages to the async writer task
        let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(256);

        let actor = Self {
            client_id: client_id.clone(),
            ws_tx,
        };

        let handle = crate::runtime::spawn::spawn(actor);
        let actor_ref = handle.actor_ref();

        // Register this connection with the WebSocket server
        if let Err(e) = websocket_server.send_message(WebSocketServerMsg::RegisterClient {
            client_id: client_id.clone(),
            client: actor_ref.clone(),
        }) {
            error!("Failed to register client with server: {}", e);
        }

        // Notify MessageHandler of new client connection
        if let Err(e) = message_handler.send_message(MessageHandlerMsg::ClientConnected {
            client_id: client_id.clone(),
            server_id: server_id.clone(),
        }) {
            error!("Failed to notify MessageHandler of client connection: {}", e);
        }

        // Spawn a thread with tokio runtime to handle async WebSocket I/O
        let task_client_id = client_id.clone();
        let task_message_handler = message_handler.clone();
        let task_websocket_server = websocket_server.clone();
        let task_server_id = server_id.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for WebSocket connection");

            rt.block_on(async move {
                // Perform WebSocket handshake
                let ws_stream = match accept_hdr_async(stream, |req: &Request, response: Response| {
                    let path = req.uri().path();
                    trace!("WebSocket handshake request path: {}", path);
                    if !(path == "/myko" || path == "/myko/") {
                        let res = Response::builder()
                            .status(404u16)
                            .body(Some("Not Found".to_string()))
                            .unwrap();
                        return Err(res);
                    }
                    Ok(response)
                })
                .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        error!("Failed to accept WebSocket connection: {}", err);
                        return;
                    }
                };

                let (ws_sink, mut ws_stream) = ws_stream.split();

                info!(
                    "WebSocket client connected: {} from {}",
                    task_client_id, peer_addr_str
                );

                // Spawn writer task
                let writer_client_id = task_client_id.clone();
                let mut ws_sink: SplitSink<WebSocketStream<TcpStream>, Message> = ws_sink;
                let writer_handle = tokio::spawn(async move {
                    while let Some(message) = ws_rx.recv().await {
                        if let Err(e) = ws_sink.send(message).await {
                            error!("Failed to send WebSocket message to {}: {}", writer_client_id, e);
                            break;
                        }
                    }
                    trace!("WebSocket writer task ended for {}", writer_client_id);
                });

                // Reader loop - process incoming messages
                while let Some(message) = ws_stream.next().await {
                    let message = match message {
                        Err(e) => {
                            error!("WebSocket read error for {}: {}", task_client_id, e);
                            break;
                        }
                        Ok(message) => message,
                    };

                    // Handle close messages
                    if message.is_close() {
                        debug!("Received close message from {}", task_client_id);
                        break;
                    }

                    let text = match message.into_text() {
                        Ok(text) => text,
                        Err(error) => {
                            error!("Failed to parse WebSocket message: {}", error);
                            continue;
                        }
                    };

                    if let Err(err) = task_message_handler.send_message(MessageHandlerMsg::ProcessText(
                        ProcessTextData {
                            text,
                            client_id: task_client_id.clone(),
                        },
                    )) {
                        error!("Failed to send message to message handler: {}", err);
                    }
                }

                // Connection closed - cleanup
                debug!("WebSocket reader loop ended for client {}", task_client_id);

                // Abort the writer task
                writer_handle.abort();

                // Unregister from WebSocket server
                if let Err(e) = task_websocket_server.send_message(WebSocketServerMsg::UnregisterClient {
                    client_id: task_client_id.clone(),
                }) {
                    error!("Failed to unregister client from server: {}", e);
                }

                // Notify MessageHandler of client disconnection
                if let Err(e) = task_message_handler.send_message(MessageHandlerMsg::ClientDisconnected {
                    client_id: task_client_id.clone(),
                    server_id: task_server_id,
                }) {
                    error!("Failed to notify MessageHandler of client disconnection: {}", e);
                }

                info!("WebSocket connection closed, client_id: {}", task_client_id);
            });
        });

        Some(handle)
    }
}

impl Actor for WebSocketConnection {
    type Msg = WebSocketConnectionMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            WebSocketConnectionMsg::Transmit(myko_msg) => {
                let json = match serde_json::to_string(&myko_msg) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("Failed to serialize message: {}", e);
                        return;
                    }
                };

                trace!(
                    "Sending message to client {}: {}",
                    self.client_id,
                    &json[..json.len().min(100)]
                );

                // Send to the async writer task via channel
                // Use try_send to avoid blocking the sync actor
                if let Err(e) = self.ws_tx.try_send(Message::Text(json)) {
                    trace!(
                        "Failed to send to WebSocket writer for {} (likely disconnected): {}",
                        self.client_id,
                        e
                    );
                }
            }
        }
    }
}
