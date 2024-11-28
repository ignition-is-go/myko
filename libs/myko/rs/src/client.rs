use myko_wasm::{event::MEvent, item::Eventable};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;

use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::{
    message::MykoMessage,
    query::WrappedQuery,
    websocket::{AutoReconnectSocket, SocketConnectionStatus},
};

use url::Url;

#[derive(Clone, Debug)]
pub enum ConnectionStatus {
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

        let mut status = socket.status_tx.subscribe();

        let client_pub = tokio::sync::broadcast::channel(1).0;

        let connection_ref = connection.clone();

        let client_pub_ref = client_pub.clone();

        tokio::spawn(async move {
            loop {
                match status.recv().await {
                    Ok(conn) => match conn {
                        SocketConnectionStatus::Connecting(addr, _) => {
                            let mut connection = connection_ref.lock().await;
                            *connection = ConnectionStatus::Connected(addr.clone());
                            drop(connection);
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
                            drop(connection);

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
                            drop(connection);

                            if client_pub_ref.send(ConnectionStatus::Disconnected).is_err() {
                                println!("Nothing listening to connection status");
                            }
                        }
                    },
                    Err(e) => {
                        println!("Error in connection status stream: {:?}", e);
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

    pub async fn send_event(&self, event: MEvent) -> Result<(), ()> {
        let myko_msg = MykoClientMessage::Event(event);

        let val = json!(myko_msg);

        let str = serde_json::to_string(&val).expect("Could not serialize message");

        let msg = Message::Text(str);

        if self.socket.outgoing.send(msg).is_err() {
            println!("Could not send message to ws");
            return Err(());
        }
        Ok(())
    }

    pub fn get_messages(&self) -> impl tokio_stream::Stream<Item = Value> {
        let stream = BroadcastStream::new(self.socket.incoming.clone().subscribe());

        stream.filter_map(|x| match x {
            Ok(Message::Text(content)) => {
                let d = serde_json::from_str::<Value>(content.as_str());

                let data = d.expect("did not parse data @ get_messages");

                Some(data)
            }
            _ => None,
        })
    }

    pub async fn set_address(&self, addr: String) {
        let parsed = Url::parse(addr.as_str());

        let mut parsed = match parsed {
            Ok(c) => c,
            Err(e) => {
                println!("Could not parse url: {:?}", e);

                let add_ws = format!("ws://{}", addr);

                match Url::parse(add_ws.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        println!("Could not parse url: {:?}", e);
                        self.socket.set_addr(None).await;

                        *self.connection_status.lock().await = ConnectionStatus::Disconnected;
                        let _ = self.client_pub.send(ConnectionStatus::Disconnected);
                        return;
                    }
                }
            }
        };

        if parsed.scheme() != "ws" {
            let _ = parsed.set_scheme("ws");
        }

        if parsed.path() != "/myko" {
            parsed.set_path("/myko");
        }

        if parsed.port().is_none() {
            let _ = parsed.set_port(Some(5155));
        }

        self.socket.set_addr(Some(parsed.to_string())).await;
    }

    pub async fn get_connection_status(&self) -> ConnectionStatus {
        let status = self.connection_status.lock().await;
        status.clone()
    }

    pub fn watch_connection_status(&self) -> impl tokio_stream::Stream<Item = ConnectionStatus> {
        BroadcastStream::new(self.client_pub.clone().subscribe()).filter_map(|x| x.ok())
    }

    pub fn watch_query<
        T: Clone + DeserializeOwned + Eventable<T, PT> + PartialEq + DeserializeOwned + 'static,
        PT: Clone,
    >(
        &self,
        query: WrappedQuery,
    ) -> impl tokio_stream::Stream<Item = Vec<T>> {
        let stream = self.get_messages();

        let msg = MykoMessage::Query(query);

        let msg = Message::Text(serde_json::to_string(&msg).expect("Could not serialize message"));

        match self.socket.outgoing.send(msg) {
            Ok(_) => {}
            Err(e) => {
                println!("Could not send message to ws: {:?}", e);
            }
        }

        let state: Arc<std::sync::Mutex<HashMap<String, T>>> = Arc::default();

        stream.filter_map(move |x| {
            let d = serde_json::from_value::<MykoMessage>(x.clone());

            let data = d.expect("did not parse data @ watch_query");

            match data {
                MykoMessage::QueryResponse(response) => {
                    let mut state = state.lock().expect("Cannot lock state");
                    let upserts = response.upserts;
                    let deletes = response.deletes;

                    let upserts: Vec<T> = upserts
                        .iter()
                        .map(|x| {
                            let item = serde_json::from_value::<T>(x.item.clone());

                            item.expect("Could not parse item")
                        })
                        .collect();

                    for up in upserts.iter() {
                        state.insert(up.id().clone(), up.clone());
                    }

                    for del in deletes.iter() {
                        state.remove(del);
                    }

                    Some(state.values().cloned().collect())
                }
                _ => None,
            }
        })
    }
}

#[derive(Serialize, Deserialize)]
struct TextMessage {
    data: Value,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetClientId {
    client_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "commandId", content = "command")]
enum Command {
    // SetClientId(SetClientId),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "event", content = "data")]
enum MykoClientMessage {
    #[serde(rename = "ws:m:event")]
    Event(MEvent),

    #[serde(rename = "ws:m:command")]
    Command(Command),
}
