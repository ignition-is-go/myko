use std::{collections::HashMap, sync::Arc};

use log::{debug, info, warn};
use ractor::{Actor, ActorRef, cast};
use tokio::net::TcpListener;

use crate::{
    actors::{
        message_handler::MessageHandlerMsg,
        ws::websocket_connection::{
            WebSocketConnection, WebSocketConnectionArgs, WebSocketConnectionMsg,
        },
    },
    message::MykoMessage,
};

pub struct WebSocketServer;

pub struct SendToClientData {
    pub client_id: Arc<str>,
    pub message: MykoMessage,
}

pub enum WebSocketServerMsg {
    SendToClient(SendToClientData),
    RegisterClient {
        client_id: Arc<str>,
        client: ActorRef<WebSocketConnectionMsg>,
    },
    UnregisterClient {
        client_id: Arc<str>,
    },
    Start,
}

pub struct WebSocketServerState {
    _connections: HashMap<Arc<str>, ActorRef<WebSocketConnectionMsg>>,
    config: WebSocketServerArgs,
}

#[derive(Clone)]
pub struct WebSocketServerArgs {
    pub port: u16,
    pub message_handler: ActorRef<MessageHandlerMsg>,
}

impl Actor for WebSocketServer {
    type Arguments = WebSocketServerArgs;

    type State = WebSocketServerState;

    type Msg = WebSocketServerMsg;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(WebSocketServerState {
            config: args,
            _connections: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            WebSocketServerMsg::SendToClient(SendToClientData { client_id, message }) => {
                if let Some(client) = state._connections.get(&client_id) {
                    if let Err(e) = cast!(client, WebSocketConnectionMsg::Transmit(message)) {
                        warn!("Failed to send to client {}: {}", client_id, e);
                    }
                } else {
                    warn!("Client {} not found in connections", client_id);
                }
                Ok(())
            }
            WebSocketServerMsg::RegisterClient { client_id, client } => {
                debug!("Registering client {}", client_id);
                state._connections.insert(client_id, client);
                Ok(())
            }
            WebSocketServerMsg::UnregisterClient { client_id } => {
                debug!("Unregistering client {}", client_id);
                state._connections.remove(&client_id);
                Ok(())
            }
            WebSocketServerMsg::Start => {
                let WebSocketServerArgs {
                    port,
                    message_handler,
                } = state.config.clone();

                let address = format!("0.0.0.0:{port}");
                debug!("Trying to bind to {address}");

                let msg_handler_clone = message_handler.clone();
                let ws_server = _myself.clone();

                // Spawn the accept loop in a separate task so the actor can process other messages
                tokio::spawn(async move {
                    match TcpListener::bind(&address).await {
                        Ok(listener) => {
                            info!("WebSocket server listening on {address}/myko");
                            while let Ok((stream, _)) = listener.accept().await {
                                debug!("Accepted connection");
                                let _ = Actor::spawn(
                                    None,
                                    WebSocketConnection,
                                    WebSocketConnectionArgs {
                                        stream,
                                        message_handler: msg_handler_clone.clone(),
                                        websocket_server: ws_server.clone(),
                                    },
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            warn!("Failed to bind to port {port}: {e}");
                        }
                    }
                });
                Ok(())
            }
        }
    }
}
