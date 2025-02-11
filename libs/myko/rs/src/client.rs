use crate::{
    event::MEvent,
    item::Eventable,
    message::MykoMessage,
    query::{wrap_query, QueryId, QueryItemType},
    report::{wrap_report, MykoReport, ReportId},
    websocket::{AutoReconnectSocket, SocketConnectionStatus},
};
use futures_signals::signal::{Signal, SignalExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;

use url::Url;

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionStatus {
    Connected(String),
    Disconnected,
}

#[derive(Clone)]
pub struct MykoClient {
    socket: Arc<AutoReconnectSocket>,
}

impl Default for MykoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoClient {
    pub fn new() -> MykoClient {
        let socket = Arc::new(AutoReconnectSocket::new());

        MykoClient { socket }
    }

    pub fn get_status(&self) -> impl Signal<Item = ConnectionStatus> {
        self.socket
            .status
            .signal_cloned()
            .map(|x| match x {
                SocketConnectionStatus::Connecting(_addr, _) => ConnectionStatus::Disconnected,
                SocketConnectionStatus::Connected(addr, _) => ConnectionStatus::Connected(addr),
                SocketConnectionStatus::Disconnected => ConnectionStatus::Disconnected,
            })
            .dedupe_cloned()
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

    pub fn set_address(&self, addr: Option<String>) {
        if addr.is_none() {
            self.socket.set_addr(None);
            return;
        }

        let addr = addr.unwrap();

        let parsed = Url::parse(addr.as_str());

        let mut parsed = match parsed {
            Ok(c) => c,
            Err(e) => {
                println!("Could not parse url: {:?}", e);

                let add_ws = format!("ws://{}", addr);

                match Url::parse(add_ws.as_str()) {
                    Ok(c) => c,
                    Err(_e) => {
                        println!("Setting Url to None");
                        self.socket.set_addr(None);
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

        self.socket.set_addr(Some(parsed.to_string()));
    }

    pub async fn get_connection_status(&self) -> ConnectionStatus {
        self.get_status()
            .to_stream()
            .take(1)
            .next()
            .await
            .unwrap_or(ConnectionStatus::Disconnected)
    }

    pub fn watch_connection_status(&self) -> impl tokio_stream::Stream<Item = ConnectionStatus> {
        self.get_status().to_stream()
    }

    pub fn watch_report<T: MykoReport<U> + Clone + Serialize + ReportId, U: DeserializeOwned>(
        &self,
        report: &T,
    ) -> impl tokio_stream::Stream<Item = U> {
        let stream = self.get_messages();

        let report_id = report.report_id().clone();

        let tx = uuid::Uuid::new_v4().to_string();

        let wrapped = wrap_report(tx.clone(), report);

        let stream = stream.filter_map(move |x| {
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
                    if response.tx != tx {
                        return None;
                    }

                    let data = serde_json::from_value::<U>(response.response.clone())
                        .expect("could not parse report value @ watch_report ");

                    // println!("Report {} had response: {}", report_id, response.response);

                    Some(data)
                }
                _ => None,
            }
        });

        if wrapped.is_err() {
            eprint!("Could not wrap report: {:?}", wrapped.err());
            return stream;
        }

        let wrapped = wrapped.unwrap();
        let msg = MykoMessage::<()>::Report(wrapped);

        let msg = Message::Text(serde_json::to_string(&msg).expect("Could not serialize message"));

        let report_send_socket = self.socket.clone();
        let report_send_self = self.clone();
        let report_send_report_id = report_id.clone();

        tokio::spawn(async move {
            let mut stream = report_send_self.watch_connection_status();
            while let Some(status) = stream.next().await {
                match status {
                    ConnectionStatus::Connected(_) => {
                        match report_send_socket.outgoing.send(msg.clone()) {
                            Ok(_) => {
                                println!("Watching report {}", report_send_report_id);
                            }
                            Err(e) => {
                                println!("Could not send message to ws: {:?}", e);
                            }
                        }
                    }
                    ConnectionStatus::Disconnected => {
                        println!("Report {} Disconnected", report_send_report_id);
                    }
                }
            }
        });

        stream
    }

    pub fn watch_query<
        T: Clone
            + DeserializeOwned
            + Eventable<T, PT>
            + PartialEq
            + DeserializeOwned
            + std::fmt::Debug,
        PT: Clone,
        Q: QueryId + QueryItemType + Serialize + Clone,
    >(
        &self,
        query: Q,
    ) -> impl tokio_stream::Stream<Item = Vec<T>> {
        let stream = self.get_messages();

        let tx = uuid::Uuid::new_v4().to_string();

        let query_id = query.query_id();
        let wrapped = wrap_query(tx.clone(), query);

        let send_query_id = query_id.clone();
        let state: Arc<std::sync::Mutex<HashMap<String, T>>> = Arc::default();

        let stream = stream.filter_map(move |x| {
            let d = serde_json::from_value::<MykoMessage<()>>(x);

            let data = match d {
                Ok(d) => d,
                Err(e) => {
                    println!("Could not parse data @ watch_query: {:?}", e);
                    return None;
                }
            };

            let vec = match data {
                MykoMessage::QueryResponse(response) => {
                    if response.tx != tx {
                        return None;
                    }

                    let mut state = state.lock().expect("Cannot lock state");
                    let upserts = response.upserts;
                    let deletes = response.deletes;
                    let seq = response.sequence;

                    if seq == 0 {
                        println!("Clearing {} state", query_id.clone());
                        state.clear();
                    }

                    let upserts: Vec<T> = upserts
                        .iter()
                        .filter_map(|x| serde_json::from_value::<T>(x.item.clone()).ok())
                        .collect();

                    for up in upserts.iter() {
                        let _len = state.len();
                        state.insert(up.id().clone(), up.clone());
                    }

                    for del in deletes.iter() {
                        let _len = state.len();
                        state.remove(del);
                    }

                    Some(state.values().cloned().collect::<Vec<T>>())
                }
                _ => None,
            };

            vec
        });

        if wrapped.is_err() {
            eprint!("Could not wrap query: {:?}", wrapped.err());
            return stream;
        }

        let wrapped = wrapped.unwrap();

        let msg = MykoMessage::<()>::Query(wrapped);

        let msg = Message::Text(serde_json::to_string(&msg).expect("Could not serialize message"));

        let query_send_socket = self.socket.clone();
        let query_send_self = self.clone();

        tokio::spawn(async move {
            let mut stream = query_send_self.watch_connection_status();
            while let Some(status) = stream.next().await {
                match status {
                    ConnectionStatus::Connected(_) => {
                        match query_send_socket.outgoing.send(msg.clone()) {
                            Ok(_) => {
                                println!("Watching query {}", send_query_id);
                            }
                            Err(e) => {
                                println!("Could not send message to ws: {:?}", e);
                            }
                        }
                    }
                    ConnectionStatus::Disconnected => {
                        println!("Query {} Disconnected", send_query_id);
                    }
                }
            }
        });

        stream
    }
}
