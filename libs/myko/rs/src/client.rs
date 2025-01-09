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
    report::{MykoReport, WrappedReport},
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

    pub async fn send_event(&self, event: MEvent) -> Result<(), String> {
        let myko_msg = MykoMessage::<()>::Event(event);

        let val = json!(myko_msg);

        let str = serde_json::to_string(&val).expect("Could not serialize message");

        let msg = Message::Text(str);

        self.socket
            .outgoing
            .send(msg)
            .map(|_| ())
            .map_err(|err| err.to_string())
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

    pub fn watch_report<T: MykoReport<U>, U: DeserializeOwned>(
        &self,
        report: WrappedReport,
    ) -> impl tokio_stream::Stream<Item = U> {
        let stream = self.get_messages();

        let report_id = report.report_id.clone();
        let msg = MykoMessage::<()>::Report(report);

        let msg = Message::Text(serde_json::to_string(&msg).expect("Could not serialize message"));

        let report_send_socket = self.socket.clone();
        let report_send_self = self.clone();
        let report_send_report_id = report_id.clone();

        match report_send_socket.outgoing.send(msg.clone()) {
            Ok(_) => {
                println!("Watching report {}", report_send_report_id);
            }
            Err(e) => {
                println!("Could not send message to ws: {:?}", e);
            }
        }

        tokio::spawn(async move {
            while let Some(status) = report_send_self.watch_connection_status().next().await {
                match status {
                    ConnectionStatus::Connected(_) => {
                        match report_send_socket.outgoing.send(msg) {
                            Ok(_) => {
                                println!("Watching report {}", report_send_report_id);
                            }
                            Err(e) => {
                                println!("Could not send message to ws: {:?}", e);
                            }
                        }
                        break;
                    }
                    ConnectionStatus::Disconnected => {
                        println!("Not connected, waiting for connection");
                    }
                }
            }
        });

        stream.filter_map(move |x| {
            let d = serde_json::from_value::<MykoMessage<()>>(x);

            let data = match d {
                Ok(d) => d,
                Err(e) => {
                    println!("Could not parse data @ watch_report: {:?}", e);
                    return None;
                }
            };

            match data {
                MykoMessage::ReportResponse(response) => {
                    let data = serde_json::from_value::<U>(response.response.clone())
                        .expect("could not parse report value @ watch_report ");

                    println!("Report {} had response: {}", report_id, response.response);

                    Some(data)
                }
                _ => None,
            }
        })
    }

    pub fn watch_query<
        T: Clone + DeserializeOwned + Eventable<T, PT> + PartialEq + DeserializeOwned + 'static,
        PT: Clone,
    >(
        &self,
        query: WrappedQuery,
    ) -> impl tokio_stream::Stream<Item = Vec<T>> {
        let stream = self.get_messages();

        let query_id = query.query_id.clone();
        let msg = MykoMessage::<()>::Query(query);

        let msg = Message::Text(serde_json::to_string(&msg).expect("Could not serialize message"));

        let query_send_socket = self.socket.clone();
        let query_send_self = self.clone();
        let query_send_query_id = query_id.clone();

        match query_send_socket.outgoing.send(msg.clone()) {
            Ok(_) => {
                println!("Watching query {}", query_send_query_id);
            }
            Err(e) => {
                println!("Could not send message to ws: {:?}", e);
            }
        }

        tokio::spawn(async move {
            while let Some(status) = query_send_self.watch_connection_status().next().await {
                match status {
                    ConnectionStatus::Connected(_) => {
                        match query_send_socket.outgoing.send(msg) {
                            Ok(_) => {
                                println!("Watching query {}", query_send_query_id);
                            }
                            Err(e) => {
                                println!("Could not send message to ws: {:?}", e);
                            }
                        }
                        break;
                    }
                    ConnectionStatus::Disconnected => {
                        println!("Not connected, waiting for connection");
                    }
                }
            }
        });

        let state: Arc<std::sync::Mutex<HashMap<String, T>>> = Arc::default();
        stream.filter_map(move |x| {
            let d = serde_json::from_value::<MykoMessage<()>>(x);

            let data = match d {
                Ok(d) => d,
                Err(e) => {
                    println!("Could not parse data @ watch_query: {:?}", e);
                    return None;
                }
            };

            match data {
                MykoMessage::QueryResponse(response) => {
                    let mut state = state.lock().expect("Cannot lock state");
                    let upserts = response.upserts;
                    let deletes = response.deletes;
                    let seq = response.sequence;

                    if seq == 0 {
                        println!("Clearing {} state", query_id);
                        state.clear();
                    }

                    let upserts: Vec<T> = upserts
                        .iter()
                        .filter_map(|x| serde_json::from_value::<T>(x.item.clone()).ok())
                        .collect();

                    for up in upserts.iter() {
                        state.insert(up.id().clone(), up.clone());
                    }

                    for del in deletes.iter() {
                        state.remove(del);
                    }

                    println!(
                        "Query {} had {} inserts and {} deletes",
                        query_id,
                        upserts.len(),
                        deletes.len()
                    );

                    Some(state.values().cloned().collect())
                }
                _ => None,
            }
        })
    }
}
