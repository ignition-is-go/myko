use std::sync::Arc;

use futures_util::{StreamExt, stream::SplitSink};
use log::{debug, error, trace};
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
    Transmit(MykoMessage<()>),
}

pub struct WebSocketConnectionState {
    pub tx: SplitSink<WebSocketStream<TcpStream>, Message>,
    pub client_id: Arc<str>,
}

pub struct WebSocketConnectionArgs {
    pub stream: TcpStream,
    pub message_handler: ActorRef<MessageHandlerMsg>,
    pub websocket_server: ActorRef<WebSocketServerMsg>,
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
        debug!("WebSocketConnection started");

        let WebSocketConnectionArgs {
            stream,
            message_handler,
            websocket_server,
        } = args;

        let (tx, mut rx) = match accept_hdr_async(stream, |req: &Request, response: Response| {
            let path = req.uri().path();
            debug!("WebSocket handshake request path: {}", path);
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

        // Register this connection with the WebSocket server
        if let Err(e) = websocket_server.send_message(WebSocketServerMsg::RegisterClient {
            client_id: client_id.clone(),
            client: myself,
        }) {
            error!("Failed to register client with server: {}", e);
        }

        let task_client_id = client_id.clone();

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

                match message_handler.send_message(MessageHandlerMsg::ProcessText(
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
            error!("Websocket Disconnected")
        });

        Ok(WebSocketConnectionState { tx, client_id })
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
}
