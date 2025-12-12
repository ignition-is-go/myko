use crate::{
    api::query::wrap_query,
    command::{CommandId, wrap_command},
    event::MEvent,
    item::Eventable,
    message::MykoMessage,
    query::{QueryId, QueryItemType},
    report::{MykoReport, ReportId, wrap_report},
};
use autosocket::{AutoReconnectSocket, SocketConnectionStatus};
use futures_signals::signal::{Signal, SignalExt};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
#[serde(rename_all = "camelCase", tag = "type", content = "data")]
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
        let myko_msg = MykoMessage::Event(event);

        let val = json!(myko_msg);

        let str = serde_json::to_string(&val).expect("Could not serialize message");

        let msg = Message::Text(str);

        self.socket
            .outgoing
            .send(msg)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    pub fn send_query(&self, query: crate::api::query::WrappedQuery) -> Result<(), String> {
        let myko_msg = MykoMessage::Query(query);

        let val = json!(myko_msg);

        let str = serde_json::to_string(&val).expect("Could not serialize message");

        let msg = Message::Text(str);

        self.socket
            .outgoing
            .send(msg)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    /// Send a raw wrapped command (for federation forwarding)
    pub fn send_command_raw(
        &self,
        command: crate::command::WrappedCommand,
    ) -> Result<(), String> {
        let myko_msg = MykoMessage::Command(command);

        let str = serde_json::to_string(&myko_msg).map_err(|e| e.to_string())?;

        let msg = Message::Text(str);

        self.socket
            .outgoing
            .send(msg)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    /// Send a raw wrapped report (for federation forwarding)
    pub fn send_report_raw(&self, report: crate::report::WrappedReport) -> Result<(), String> {
        let myko_msg = MykoMessage::Report(report);

        let str = serde_json::to_string(&myko_msg).map_err(|e| e.to_string())?;

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
            debug!("Setting address to None, disconnecting socket");
            self.socket.set_addr(None);
            return;
        }

        let addr = addr.unwrap();

        let parsed = Url::parse(addr.as_str());

        let mut parsed = match parsed {
            Ok(c) => c,
            Err(e) => {
                warn!("Could not parse url: {e:?} - attempting to add ws://");

                let add_ws = format!("ws://{addr}");

                match Url::parse(add_ws.as_str()) {
                    Ok(c) => c,
                    Err(_e) => {
                        info!("Setting Url to None");
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

    /// Disconnect the client and stop any reconnection attempts.
    /// This will cancel all reconnection attempts and fire the disconnected event.
    pub fn disconnect(&self) {
        debug!("Disconnecting MykoClient");
        self.socket.close();
    }

    /// Close the client and stop any reconnection attempts.
    /// Alias for disconnect() - useful for peer connections that should not reconnect.
    pub fn close(&self) {
        debug!("Closing MykoClient");
        self.socket.close();
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

    pub fn handle_command<C, F, Fut>(&self, handler: F)
    where
        C: DeserializeOwned + Clone + Send + crate::command::CommandId + 'static,
        F: Fn(C) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'static,
    {
        let outgoing = self.socket.clone();
        let mut msgs = BroadcastStream::new(self.socket.incoming.clone().subscribe()).filter_map(
            |x| match x {
                Ok(Message::Text(s)) => serde_json::from_str::<serde_json::Value>(&s).ok(),
                _ => None,
            },
        );

        tokio::spawn(async move {
            while let Some(val) = msgs.next().await {
                let parsed = serde_json::from_value::<MykoMessage>(
                    val.clone(),
                );
                let Ok(MykoMessage::Command(wrapped)) = parsed else {
                    continue;
                };

                // extract tx from wrapped.command
                let tx: String = wrapped
                    .command
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if tx.is_empty() {
                    // malformed command without tx; ignore
                    continue;
                }

                // try to deserialize to requested command type; if it fails, it's not for this handler
                match serde_json::from_value::<C>(wrapped.command.clone()) {
                    Ok(cmd) => {
                        // Ensure the commandId matches the type this handler expects
                        if *wrapped.command_id != *cmd.command_id() {
                            continue;
                        }
                        let result = handler(cmd).await;
                        match result {
                            Ok(response) => {
                                let resp = crate::command::CommandResponse {
                                    tx: tx.clone(),
                                    response,
                                };
                                let msg = MykoMessage::CommandResponse(resp);
                                if let Ok(s) = serde_json::to_string(&msg) {
                                    let _ = outgoing.outgoing.send(Message::Text(s));
                                }
                            }
                            Err(message) => {
                                let err = crate::command::CommandError {
                                    tx: tx.clone(),
                                    message,
                                };
                                let msg = MykoMessage::CommandError(err);
                                if let Ok(s) = serde_json::to_string(&msg) {
                                    let _ = outgoing.outgoing.send(Message::Text(s));
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    pub async fn send_command<
        C: Serialize + Clone + CommandId,
        R: DeserializeOwned + Clone + 'static,
    >(
        &self,
        command: &C,
    ) -> Result<R, String> {
        let tx = uuid::Uuid::new_v4().to_string();

        let wrapped = wrap_command(tx.clone(), command).map_err(|e| e.to_string())?;

        let msg = MykoMessage::Command(wrapped);

        let json = serde_json::to_string(&msg).map_err(|e| e.to_string())?;

        let ws_msg = Message::Text(json);

        // listen for matching response/error
        let mut stream =
            BroadcastStream::new(self.socket.incoming.clone().subscribe()).filter_map(move |x| {
                match x {
                    Ok(Message::Text(content)) => {
                        let d = serde_json::from_str::<serde_json::Value>(content.as_str());
                        let data = match d {
                            Ok(v) => v,
                            Err(_) => return None,
                        };
                        let parsed =
                            serde_json::from_value::<MykoMessage>(data.clone()).ok()?;
                        match parsed {
                            MykoMessage::CommandResponse(resp) => {
                                if resp.tx != tx {
                                    return None;
                                }
                                Some(Ok(resp.response))
                            }
                            MykoMessage::CommandError(err) => {
                                if err.tx != tx {
                                    return None;
                                }
                                Some(Err(err.message))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                }
            });

        // send message (on next Connected)
        let send_socket = self.socket.clone();
        let me = self.clone();

        // ensure connection ready or wait for it
        if let ConnectionStatus::Disconnected = self.get_connection_status().await {
            // wait once until connected
            let mut st = me.watch_connection_status();
            while let Some(status) = st.next().await {
                if let ConnectionStatus::Connected(_) = status {
                    break;
                }
            }
        }

        send_socket
            .outgoing
            .send(ws_msg)
            .map_err(|err| err.to_string())?;

        // await first response
        let next = stream
            .next()
            .await
            .ok_or_else(|| "No response".to_string())?;
        let value = next?;
        let res: R = serde_json::from_value(value).map_err(|e| e.to_string())?;
        Ok(res)
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
            let d = serde_json::from_value::<MykoMessage>(x);
            let data = match d {
                Ok(d) => d,
                Err(e) => {
                    error!("Could not parse data @ watch_report: {e:?}");
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
        let msg = MykoMessage::Report(wrapped);

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
                                debug!("Watching report {report_send_report_id}");
                            }
                            Err(e) => {
                                error!("Could not send message to ws: {e:?}");
                            }
                        }
                    }
                    ConnectionStatus::Disconnected => {
                        debug!("Report {report_send_report_id} Disconnected");
                    }
                }
            }
        });

        stream
    }

    pub fn watch_query<
        T: Eventable + std::fmt::Debug,
        Q: QueryId + QueryItemType + Serialize + Clone,
    >(
        &self,
        query: &Q,
    ) -> impl tokio_stream::Stream<Item = Vec<T>> {
        let stream = self.get_messages();

        let tx: Arc<str> = uuid::Uuid::new_v4().to_string().into();

        let query_id = query.query_id();
        let wrapped = wrap_query(tx.clone(), query);

        let send_query_id = query_id.clone();
        let state: Arc<std::sync::Mutex<HashMap<Arc<str>, T>>> = Arc::default();

        let stream = stream.filter_map(move |x| {
            let d = serde_json::from_value::<MykoMessage>(x);
            let data = match d {
                Ok(d) => d,
                Err(e) => {
                    error!("Could not parse data @ watch_query: {e:?}");
                    return None;
                }
            };

            match data {
                MykoMessage::QueryResponse(response) => {
                    if response.tx != tx {
                        return None;
                    }

                    let mut state = state.lock().expect("Cannot lock state");
                    let upserts = response.upserts;
                    let deletes = response.deletes;
                    let seq = response.sequence;

                    if seq == 0 {
                        trace!("Sequence reset: Clearing {} state", query_id.clone());
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
            }
        });

        if wrapped.is_err() {
            eprint!("Could not wrap query: {:?}", wrapped.err());
            return stream;
        }

        let wrapped = wrapped.unwrap();

        let msg = MykoMessage::Query(wrapped);

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
                                debug!("Watching query {send_query_id}");
                            }
                            Err(e) => {
                                error!("Could not send message to ws: {e:?}");
                            }
                        }
                    }
                    ConnectionStatus::Disconnected => {
                        warn!("Query {send_query_id} Disconnected");
                    }
                }
            }
        });

        stream
    }

    // =========================================================================
    // FFI-friendly APIs for language bindings (callback-based, JSON in/out)
    // =========================================================================

    /// Watch connection status changes with a callback.
    /// Callback receives JSON: `{"type":"Connected","data":"ws://..."}` or `{"type":"Disconnected"}`
    pub fn watch_connection_status_callback<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let client = self.clone();
        tokio::spawn(async move {
            let mut stream = client.watch_connection_status();
            while let Some(status) = stream.next().await {
                if let Ok(json) = serde_json::to_string(&status) {
                    callback(json);
                }
            }
        });
    }

    /// Watch a query with a callback that receives the current state as Vec<Value>.
    ///
    /// - `query`: WrappedQuery with tx and createdAt already set.
    /// - `callback`: Called with current items whenever state changes.
    ///
    /// Returns a cancel function that stops the query when called.
    pub fn watch_query_callback<F>(
        &self,
        query: crate::api::query::WrappedQuery,
        callback: F,
    ) -> impl Fn() + Send + Sync
    where
        F: Fn(Vec<Value>) + Send + Sync + 'static,
    {
        let client = self.clone();
        let callback = Arc::new(callback);
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();

        // Extract tx from the query
        let tx: Arc<str> = query
            .query
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        tokio::spawn(async move {
            // State for accumulating query results
            let state: Arc<std::sync::Mutex<HashMap<Arc<str>, Value>>> = Arc::default();

            // Get message stream
            let mut stream = client.get_messages();

            // Wait for connection and send query
            loop {
                if cancelled_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                let status = client.get_connection_status().await;
                if let ConnectionStatus::Connected(_) = status {
                    if let Err(e) = client.send_query(query.clone()) {
                        error!("Failed to send query: {}", e);
                    }
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Process responses
            while let Some(msg) = stream.next().await {
                if cancelled_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                let Ok(myko_msg) = serde_json::from_value::<MykoMessage>(msg) else {
                    continue;
                };

                if let MykoMessage::QueryResponse(response) = myko_msg {
                    if response.tx != tx {
                        continue;
                    }

                    let mut state = state.lock().expect("Cannot lock state");

                    // Reset on sequence 0
                    if response.sequence == 0 {
                        state.clear();
                    }

                    // Apply upserts
                    for wrapped_item in response.upserts {
                        if let Some(id) = wrapped_item.item.get("id").and_then(|v| v.as_str()) {
                            state.insert(id.into(), wrapped_item.item);
                        }
                    }

                    // Apply deletes
                    for id in response.deletes {
                        state.remove(&id);
                    }

                    // Send current state
                    let items: Vec<Value> = state.values().cloned().collect();
                    callback(items);
                }
            }
        });

        // Return cancel function
        move || {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Send an event using JSON input.
    /// Returns error message if failed, empty string on success.
    pub async fn send_event_json(&self, event_json: String) -> String {
        match serde_json::from_str::<MEvent>(&event_json) {
            Ok(event) => match self.send_event(event).await {
                Ok(()) => String::new(),
                Err(e) => e,
            },
            Err(e) => e.to_string(),
        }
    }

    /// Watch a report with a callback that receives the report result as Value.
    ///
    /// - `report`: WrappedReport with tx already set.
    /// - `callback`: Called with report result whenever it updates.
    ///
    /// Returns a cancel function that stops the report when called.
    pub fn watch_report_callback<F>(
        &self,
        report: crate::report::WrappedReport,
        callback: F,
    ) -> impl Fn() + Send + Sync
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        let client = self.clone();
        let callback = Arc::new(callback);
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();

        // Extract tx from the report
        let tx: Arc<str> = report
            .report
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let report_id = report.report_id.clone();

        tokio::spawn(async move {
            // Get message stream
            let mut stream = client.get_messages();

            // Wait for connection and send report
            loop {
                if cancelled_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                let status = client.get_connection_status().await;
                if let ConnectionStatus::Connected(_) = status {
                    let msg = MykoMessage::Report(report.clone());
                    let msg_str = serde_json::to_string(&msg).expect("Could not serialize report");
                    let msg = Message::Text(msg_str);
                    if let Err(e) = client.socket.outgoing.send(msg) {
                        error!("Failed to send report: {}", e);
                    } else {
                        debug!("Watching report {}", report_id);
                    }
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Process responses
            while let Some(msg) = stream.next().await {
                if cancelled_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                let Ok(myko_msg) = serde_json::from_value::<MykoMessage>(msg) else {
                    continue;
                };

                if let MykoMessage::ReportResponse(response) = myko_msg {
                    if response.tx != *tx {
                        continue;
                    }

                    callback(response.response);
                }
            }
        });

        // Return cancel function
        move || {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
