use std::{collections::HashMap, sync::Arc};

use log::{debug, error, info, trace, warn};
use tokio::net::TcpListener;

use crate::{
    actors::{
        message_handler::MessageHandlerMsg,
        ws::websocket_connection::{
            WebSocketConnection, WebSocketConnectionArgs, WebSocketConnectionMsg,
        },
    },
    message::MykoMessage,
    runtime::{Actor, ActorHandle, ActorRef, RpcReplyPort},
};

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
    /// Set self-reference (called immediately after spawn)
    SetMyself(ActorRef<WebSocketServerMsg>),
}

impl std::fmt::Debug for WebSocketServerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketServerMsg::SendToClient(data) => {
                write!(f, "SendToClient({})", data.client_id)
            }
            WebSocketServerMsg::RegisterClient { client_id, .. } => {
                write!(f, "RegisterClient({})", client_id)
            }
            WebSocketServerMsg::UnregisterClient { client_id } => {
                write!(f, "UnregisterClient({})", client_id)
            }
            WebSocketServerMsg::Start { .. } => write!(f, "Start"),
            WebSocketServerMsg::SetMyself(_) => write!(f, "SetMyself"),
        }
    }
}

pub struct WebSocketServer {
    connections: HashMap<Arc<str>, ActorRef<WebSocketConnectionMsg>>,
    port: u16,
    server_id: Arc<str>,
    myself: Option<ActorRef<WebSocketServerMsg>>,
}

#[derive(Clone)]
pub struct WebSocketServerArgs {
    pub port: u16,
    pub server_id: Arc<str>,
}

impl WebSocketServer {
    pub fn new(args: WebSocketServerArgs) -> Self {
        Self {
            port: args.port,
            server_id: args.server_id,
            connections: HashMap::new(),
            myself: None,
        }
    }

    pub fn spawn(args: WebSocketServerArgs) -> ActorHandle<WebSocketServerMsg> {
        let actor = Self::new(args);
        let handle = crate::runtime::spawn::spawn(actor);

        // Set self-reference
        let actor_ref = handle.actor_ref();
        let _ = actor_ref.send_message(WebSocketServerMsg::SetMyself(actor_ref.clone()));

        handle
    }

    fn handle_send_to_client(&self, data: SendToClientData) {
        let SendToClientData { client_id, message } = data;
        if let Some(client) = self.connections.get(&client_id) {
            if let Err(e) = client.send_message(WebSocketConnectionMsg::Transmit(message)) {
                trace!(
                    "Failed to send to client {} (likely disconnected): {}",
                    client_id,
                    e
                );
            }
        } else {
            // Client not found - this is normal when clients disconnect while
            // query/report tasks are still sending, or during orphan cleanup
            trace!(
                "Client {} not found in connections (already disconnected)",
                client_id
            );
        }
    }

    fn handle_register_client(&mut self, client_id: Arc<str>, client: ActorRef<WebSocketConnectionMsg>) {
        trace!("Registering client {}", client_id);
        self.connections.insert(client_id, client);
    }

    fn handle_unregister_client(&mut self, client_id: Arc<str>) {
        debug!("Unregistering client {}", client_id);
        self.connections.remove(&client_id);
    }

    fn handle_start(&self, message_handler: ActorRef<MessageHandlerMsg>, reply: RpcReplyPort<Result<(), String>>) {
        let port = self.port;
        let server_id = self.server_id.clone();
        let address = format!("0.0.0.0:{port}");
        debug!("Trying to bind to {address}");

        let myself = match &self.myself {
            Some(m) => m.clone(),
            None => {
                error!("WebSocketServer myself reference not set");
                let _ = reply.send(Err("myself reference not set".to_string()));
                return;
            }
        };

        // Spawn a thread with tokio runtime to handle async accept loop
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for WebSocket server");

            rt.block_on(async move {
                // Bind the listener first, then signal ready
                match TcpListener::bind(&address).await {
                    Ok(listener) => {
                        info!("WebSocket server listening on {address}/myko");

                        // Signal that we're ready to accept connections
                        if let Err(e) = reply.send(Ok(())) {
                            error!("Failed to send WebSocket ready signal: {:?}", e);
                        }

                        // Accept loop
                        while let Ok((stream, _)) = listener.accept().await {
                            debug!("Accepted connection");
                            let _ = WebSocketConnection::spawn(WebSocketConnectionArgs {
                                stream,
                                message_handler: message_handler.clone(),
                                websocket_server: myself.clone(),
                                server_id: server_id.clone(),
                            });
                        }
                    }
                    Err(e) => {
                        warn!("Failed to bind to port {port}: {e}");
                        let _ = reply.send(Err(format!("Failed to bind: {e}")));
                    }
                }
            });
        });
    }
}

impl Actor for WebSocketServer {
    type Msg = WebSocketServerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            WebSocketServerMsg::SendToClient(data) => {
                self.handle_send_to_client(data);
            }
            WebSocketServerMsg::RegisterClient { client_id, client } => {
                self.handle_register_client(client_id, client);
            }
            WebSocketServerMsg::UnregisterClient { client_id } => {
                self.handle_unregister_client(client_id);
            }
            WebSocketServerMsg::Start {
                message_handler,
                reply,
            } => {
                self.handle_start(message_handler, reply);
            }
            WebSocketServerMsg::SetMyself(myself) => {
                self.myself = Some(myself);
            }
        }
    }
}
