use std::{collections::HashMap, sync::Arc};

use log::{debug, info, warn};
use ractor::{Actor, ActorRef};
use tokio::net::TcpListener;

use crate::{
    actors::{
        message_handler::MessageHandlerMsg,
        websocket_connection::{
            WebSocketConnection, WebSocketConnectionArgs, WebSocketConnectionMsg,
        },
    },
    message::MykoMessage,
};

pub struct WebSocketServer;

pub enum WebSocketServerMsg {
    SendToClient(MykoMessage<()>),
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
            WebSocketServerMsg::SendToClient(_msg) => Ok(()),
            WebSocketServerMsg::Start => {
                let WebSocketServerArgs {
                    port,
                    message_handler,
                } = state.config.clone();

                let address = format!("0.0.0.0:{port}");
                debug!("Trying to bind to {address}");

                let msg_handler_clone = message_handler.clone();

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
                                },
                            )
                            .await;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to bind to port {port}: {e}");
                    }
                }
                Ok(())
            }
        }
    }
}
