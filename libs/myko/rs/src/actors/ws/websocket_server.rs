use std::{collections::HashMap, sync::Arc};

use log::{debug, error, info, trace, warn};
use ractor::{Actor, ActorRef, RpcReplyPort, cast};
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
    /// Start accepting connections. MessageHandler passed here to break circular dependency.
    /// Replies with Ok(()) when the listener is bound and ready, or Err(String) on failure.
    Start {
        message_handler: ActorRef<MessageHandlerMsg>,
        reply: RpcReplyPort<Result<(), String>>,
    },
}

pub struct WebSocketServerState {
    _connections: HashMap<Arc<str>, ActorRef<WebSocketConnectionMsg>>,
    port: u16,
    server_id: Arc<str>,
}

#[derive(Clone)]
pub struct WebSocketServerArgs {
    pub port: u16,
    pub server_id: Arc<str>,
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
            port: args.port,
            server_id: args.server_id,
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
                        trace!("Failed to send to client {} (likely disconnected): {}", client_id, e);
                    }
                } else {
                    // Client not found - this is normal when clients disconnect while
                    // query/report tasks are still sending, or during orphan cleanup
                    trace!("Client {} not found in connections (already disconnected)", client_id);
                }
                Ok(())
            }
            WebSocketServerMsg::RegisterClient { client_id, client } => {
                trace!("Registering client {}", client_id);
                state._connections.insert(client_id, client);
                Ok(())
            }
            WebSocketServerMsg::UnregisterClient { client_id } => {
                debug!("Unregistering client {}", client_id);
                state._connections.remove(&client_id);
                Ok(())
            }
            WebSocketServerMsg::Start {
                message_handler,
                reply,
            } => {
                let port = state.port;
                let server_id = state.server_id.clone();
                let address = format!("0.0.0.0:{port}");
                debug!("Trying to bind to {address}");

                // Bind the listener first, then signal ready
                match TcpListener::bind(&address).await {
                    Ok(listener) => {
                        info!("WebSocket server listening on {address}/myko");

                        // Signal that we're ready to accept connections
                        if let Err(e) = reply.send(Ok(())) {
                            error!("Failed to send WebSocket ready signal: {:?}", e);
                        }

                        let msg_handler_clone = message_handler.clone();
                        let ws_server = _myself.clone();

                        // Spawn the accept loop in a separate task
                        tokio::spawn(async move {
                            while let Ok((stream, _)) = listener.accept().await {
                                debug!("Accepted connection");
                                let _ = Actor::spawn(
                                    None,
                                    WebSocketConnection,
                                    WebSocketConnectionArgs {
                                        stream,
                                        message_handler: msg_handler_clone.clone(),
                                        websocket_server: ws_server.clone(),
                                        server_id: server_id.clone(),
                                    },
                                )
                                .await;
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to bind to port {port}: {e}");
                        let _ = reply.send(Err(format!("Failed to bind: {e}")));
                    }
                }
                Ok(())
            }
        }
    }
}
