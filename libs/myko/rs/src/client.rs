use myko_wasm::event::MEvent;
use rdkafka::client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;

use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::websocket::{AutoReconnectSocket, SocketConnectionStatus};

#[derive(Clone)]
pub struct ConnectionInfo {
    pub address: String,
    pub client_id: String,
}

#[derive(Clone)]
pub enum ConnectionStatus {
    Client(ConnectionInfo),
    Connected(String),
    Disconnected,
}

#[derive(Clone)]
pub struct MykoClient {
    connection_status: Arc<Mutex<ConnectionStatus>>,
    socket: Arc<AutoReconnectSocket>,
    client_pub: tokio::sync::broadcast::Sender<ConnectionStatus>,
}

impl Default for MykoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoClient {
    pub fn new() -> MykoClient {
        let connection = Arc::new(Mutex::new(ConnectionStatus::Disconnected));

        let socket = Arc::new(AutoReconnectSocket::new());

        let mut incoming = socket.incoming.subscribe();
        let mut status = socket.status_tx.subscribe();

        let connection_ref = connection.clone();
        let client_pub = tokio::sync::broadcast::channel(1).0;
        let client_pub_ref = client_pub.clone();

        tokio::spawn(async move {
            while let Ok(msg) = incoming.recv().await {
                process_message(connection_ref.clone(), client_pub_ref.clone(), msg).await;
            }
        });

        let connection_ref = connection.clone();

        let client_pub_ref = client_pub.clone();

        tokio::spawn(async move {
            while let Ok(conn) = status.recv().await {
                match conn {
                    SocketConnectionStatus::Connecting(addr, _) => {
                        let mut connection = connection_ref.lock().await;
                        *connection = ConnectionStatus::Connected(addr.clone());
                        if client_pub_ref
                            .send(ConnectionStatus::Connected(addr))
                            .is_err()
                        {
                            println!("Nothing listening to connection status");
                        }
                    }
                    SocketConnectionStatus::Connected(addr, _) => {
                        let mut connection = connection_ref.lock().await;
                        *connection = ConnectionStatus::Connected(addr.clone());
                        if client_pub_ref
                            .send(ConnectionStatus::Connected(addr))
                            .is_err()
                        {
                            println!("Nothing listening to connection status");
                        }
                    }

                    SocketConnectionStatus::Disconnected => {
                        let mut connection = connection_ref.lock().await;
                        *connection = ConnectionStatus::Disconnected;
                        if client_pub_ref.send(ConnectionStatus::Disconnected).is_err() {
                            println!("Nothing listening to connection status");
                        }
                    }
                }
            }
        });

        MykoClient {
            connection_status: connection,
            socket,
            client_pub,
        }
    }

    pub async fn send_event(&self, event: MEvent) {
        let myko_msg = MykoClientMessage::Event(event);

        let val = json!(myko_msg);

        let str = serde_json::to_string(&val).expect("Could not serialize message");

        let msg = Message::Text(str);

        if self.socket.outgoing.send(msg).is_err() {
            println!("Could not send message to ws");
        }
    }

    pub fn get_messages(&self) -> impl tokio_stream::Stream<Item = Value> {
        let stream = BroadcastStream::new(self.socket.incoming.clone().subscribe());

        stream.filter_map(|x| match x {
            Ok(Message::Text(content)) => {
                let d = serde_json::from_str::<Value>(content.as_str());

                let data = d.expect("did not parse data");

                Some(data)
            }
            _ => None,
        })
    }

    pub async fn set_address(&self, addr: String) {
        self.socket.set_addr(addr).await;
    }

    pub async fn get_connection_status(&self) -> ConnectionStatus {
        let status = self.connection_status.lock().await;
        status.clone()
    }

    pub fn watch_connection_status(&self) -> impl tokio_stream::Stream<Item = ConnectionStatus> {
        BroadcastStream::new(self.client_pub.clone().subscribe()).filter_map(|x| x.ok())
    }

    pub async fn get_client_id(&self) -> Option<String> {
        let status = self.connection_status.lock().await;
        match &*status {
            ConnectionStatus::Client(info) => Some(info.client_id.clone()),
            _ => None,
        }
    }
}

async fn process_message(
    connection: Arc<Mutex<ConnectionStatus>>,
    client_pub: tokio::sync::broadcast::Sender<ConnectionStatus>,
    message: Message,
) {
    if let Message::Text(content) = message {
        let d = serde_json::from_str::<TextMessage>(content.as_str());

        let data = d.expect("did not parse data").data;

        let command = serde_json::from_value::<Command>(data.to_owned());

        process_command(command, connection, client_pub).await;
    }
}

async fn process_command(
    command: Result<Command, serde_json::Error>,
    connection: Arc<Mutex<ConnectionStatus>>,
    client_pub: tokio::sync::broadcast::Sender<ConnectionStatus>,
) {
    if let Ok(command) = command {
        match command {
            Command::SetId(set_id) => {
                let con_state = connection.lock().await.clone();

                let mut connection = connection.lock().await;

                match con_state {
                    ConnectionStatus::Disconnected => {
                        unreachable!("Received SetId Command, but not connected");
                    }
                    ConnectionStatus::Connected(addr) => {
                        println!("Received Client Id: {:?}", set_id.client_id);
                        *connection = ConnectionStatus::Client(ConnectionInfo {
                            address: addr,
                            client_id: set_id.client_id,
                        });
                    }
                    ConnectionStatus::Client(info) => {
                        println!("Received New Client Id: {:?}", set_id.client_id);
                        *connection = ConnectionStatus::Client(ConnectionInfo {
                            address: info.address,
                            client_id: set_id.client_id,
                        });
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TextMessage {
    data: Value,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetId {
    client_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "commandId", content = "command")]
enum Command {
    #[serde(rename = "client:setId")]
    SetId(SetId),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "event", content = "data")]
enum MykoClientMessage {
    #[serde(rename = "ws:m:event")]
    Event(MEvent),

    #[serde(rename = "ws:m:command")]
    Command(Command),
}
