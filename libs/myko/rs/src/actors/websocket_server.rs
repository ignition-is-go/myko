use std::{collections::HashMap, sync::Arc};

use log::{debug, error, info, warn};
use ractor::{Actor, ActorRef};
use tokio::net::TcpListener;

use crate::{actors::websocket_connection::WebSocketConnectionMsg, message::MykoMessage};

pub struct WebSocketServer;

pub enum WebSocketServerMsg {
    SendToClient(MykoMessage<()>),
}

#[derive(Default)]
pub struct WebSocketServerState {
    _connections: HashMap<Arc<str>, ActorRef<WebSocketConnectionMsg>>,
}

pub struct WebSocketServerArgs {
    pub min_port: u16,
    pub max_port: u16,
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
        let WebSocketServerArgs { min_port, max_port } = args;

        let mut port = min_port;

        loop {
            let address = format!("0.0.0.0:{port}");
            debug!("Trying to bind to {address}");

            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    info!("WebSocket server listening on {address}");
                    while let Ok((_stream, _)) = listener.accept().await {}
                    break; // Exit loop if successfully bound
                }
                Err(e) => {
                    warn!("Failed to bind to port {port}: {e}");
                    port += 1;
                    if port > max_port {
                        error!("Exceeded maximum port limit");
                        return Err(ractor::ActorProcessingErr::from(String::from(
                            "Max port limit exceeded",
                        )));
                    }
                }
            }
        }
        Ok(WebSocketServerState::default())
    }
}
