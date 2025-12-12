use std::sync::Arc;

use futures_util::{StreamExt, stream::SplitSink};
use log::{debug, error, info, trace};
use ractor::{Actor, ActorRef};
use tokio::net::TcpStream;
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
};

pub struct WebSocketConnection;

pub enum WebSocketConnectionMsg {
    Transmit(MykoMessage),
}

pub struct WebSocketConnectionState {
    pub tx: SplitSink<WebSocketStream<TcpStream>, Message>,
    pub client_id: Arc<str>,
    pub server_id: Arc<str>,
    pub message_handler: ActorRef<MessageHandlerMsg>,
    pub websocket_server: ActorRef<WebSocketServerMsg>,
}

pub struct WebSocketConnectionArgs {
    pub stream: TcpStream,
    pub message_handler: ActorRef<MessageHandlerMsg>,
    pub websocket_server: ActorRef<WebSocketServerMsg>,
    pub server_id: Arc<str>,
}

impl Actor for WebSocketConnection {
    type Arguments = WebSocketConnectionArgs;

    type State = WebSocketConnectionState;

    type Msg = WebSocketConnectionMsg;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        trace!("WebSocketConnection started");

        let WebSocketConnectionArgs {
            stream,
            message_handler,
            websocket_server,
            server_id,
        } = args;

        // Get peer address before consuming the stream
        let peer_addr = stream.peer_addr().ok();

        let (tx, mut rx) = match accept_hdr_async(stream, |req: &Request, response: Response| {
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
            Ok(stream) => stream.split(),
            Err(err) => {
                log::error!("Failed to accept WebSocket connection: {}", err);
                return Err(ractor::ActorProcessingErr::from(String::from(
                    "Failed to accept WebSocket connection",
                )));
            }
        };

        let client_id: Arc<str> = Uuid::new_v4().to_string().into();

        let peer_addr_str = peer_addr
            .map(|addr| format!("{}:{}", addr.ip(), addr.port()))
            .unwrap_or_else(|| "unknown".to_string());
        info!(
            "WebSocket client connected: {} from {}",
            client_id, peer_addr_str
        );

        // Clone myself for the spawned task before moving to RegisterClient
        let task_myself = myself.clone();

        // Register this connection with the WebSocket server
        if let Err(e) = websocket_server.send_message(WebSocketServerMsg::RegisterClient {
            client_id: client_id.clone(),
            client: myself,
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

        let task_client_id = client_id.clone();
        let task_message_handler = message_handler.clone();

        tokio::spawn(async move {
            while let Some(message) = rx.next().await {
                let message = match message {
                    Err(e) => {
                        log::error!("Failed to accept WebSocket connection: {}", e);
                        continue;
                    }
                    Ok(message) => message,
                };

                let text = match message.into_text() {
                    Ok(text) => text,
                    Err(error) => {
                        log::error!("Failed to parse WebSocket message: {}", error);
                        continue;
                    }
                };

                match task_message_handler.send_message(MessageHandlerMsg::ProcessText(
                    ProcessTextData {
                        text,
                        client_id: task_client_id.clone(),
                    },
                )) {
                    Ok(_) => (),
                    Err(err) => {
                        log::error!("Failed to send message to message handler: {}", err);
                    }
                }
            }
            debug!("Websocket reader loop ended for client, stopping actor");
            task_myself.stop(Some("WebSocket connection closed".to_string()));
        });

        Ok(WebSocketConnectionState {
            tx,
            client_id,
            server_id,
            message_handler,
            websocket_server,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        use futures_util::SinkExt;

        match message {
            WebSocketConnectionMsg::Transmit(msg) => {
                let json = serde_json::to_string(&msg).map_err(|e| {
                    ractor::ActorProcessingErr::from(format!("Failed to serialize message: {}", e))
                })?;

                trace!("Sending message to client {}: {}", state.client_id, &json[..json.len().min(100)]);

                state
                    .tx
                    .send(Message::Text(json))
                    .await
                    .map_err(|e| {
                        ractor::ActorProcessingErr::from(format!("Failed to send message: {}", e))
                    })?;
            }
        }

        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        info!("WebSocket connection closed, client_id: {}", state.client_id);

        // Unregister from WebSocket server
        if let Err(e) = state.websocket_server.send_message(WebSocketServerMsg::UnregisterClient {
            client_id: state.client_id.clone(),
        }) {
            error!("Failed to unregister client from server: {}", e);
        }

        // Notify MessageHandler of client disconnection (cancels subscriptions, deletes Client entity)
        if let Err(e) = state.message_handler.send_message(MessageHandlerMsg::ClientDisconnected {
            client_id: state.client_id.clone(),
            server_id: state.server_id.clone(),
        }) {
            error!("Failed to notify MessageHandler of client disconnection: {}", e);
        }

        Ok(())
    }
}
