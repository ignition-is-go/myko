use futures_util::{StreamExt, stream::SplitSink};
use log::{debug, error};
use ractor::{Actor, ActorRef};
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};
use tungstenite::{
    Message,
    handshake::server::{Request, Response},
};

use crate::{actors::message_handler::MessageHandlerMsg, message::MykoMessage};

pub struct WebSocketConnection;

pub enum WebSocketConnectionMsg {
    Transmit(MykoMessage<()>),
}

pub struct WebSocketConnectionState {
    pub tx: SplitSink<WebSocketStream<TcpStream>, Message>,
}

pub struct WebSocketConnectionArgs {
    pub stream: TcpStream,
    pub message_handler: ActorRef<MessageHandlerMsg>,
}

impl Actor for WebSocketConnection {
    type Arguments = WebSocketConnectionArgs;

    type State = WebSocketConnectionState;

    type Msg = WebSocketConnectionMsg;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("WebSocketConnection started");

        let WebSocketConnectionArgs {
            stream,
            message_handler,
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

                match message_handler.send_message(MessageHandlerMsg::ProcessText(text)) {
                    Ok(_) => (),
                    Err(err) => {
                        log::error!("Failed to send message to message handler: {}", err);
                    }
                }
            }
            error!("Websocket Disconnected")
        });

        Ok(WebSocketConnectionState { tx })
    }
}
