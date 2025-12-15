use super::event::common::PersistEvent;
use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::{common::ProcessEventData, event_manager::EventManagerMsg},
        query::{common::WebSocketSink, query_manager::QueryManagerMsg},
        report::report_manager::ReportManagerMsg,
        ws::websocket_server::{SendToClientData, WebSocketServerMsg},
    },
    command::{CommandError, CommandResponse},
    context::RequestContext,
    entities::client::Client,
    event::{MEvent, MEventType},
    message::MykoMessage,
    report::ReportResponse,
    runtime::{Actor, ActorHandle, ActorRef},
};
use log::{debug, error, trace};
use std::{collections::{HashMap, HashSet}, sync::Arc};
use uuid::Uuid;

pub struct MessageHandler {
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    ws_server: ActorRef<WebSocketServerMsg>,
    /// Host ID for creating RequestContext
    host_id: Uuid,
    /// Shared tokio runtime handle for async dispatch
    tokio_handle: tokio::runtime::Handle,
    /// Track active subscriptions per client for cleanup on disconnect
    /// Maps client_id -> set of (subscription_type, tx)
    client_subscriptions: HashMap<Arc<str>, HashSet<(SubscriptionType, Arc<str>)>>,
}

pub struct MessageHandlerArgs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
    pub command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    pub ws_server: ActorRef<WebSocketServerMsg>,
    /// Host ID for request context
    pub host_id: Uuid,
    /// Shared tokio runtime handle
    pub tokio_handle: tokio::runtime::Handle,
}

/// Type of subscription for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionType {
    Query,
    Report,
}

pub struct ProcessTextData {
    pub client_id: Arc<str>,
    pub text: String,
}

pub enum MessageHandlerMsg {
    ProcessText(ProcessTextData),
    /// Client connected - publish Client entity
    ClientConnected {
        client_id: Arc<str>,
        server_id: Arc<str>,
    },
    /// Client disconnected - cancel all their subscriptions and delete Client entity
    ClientDisconnected {
        client_id: Arc<str>,
        server_id: Arc<str>,
    },
}

impl std::fmt::Debug for MessageHandlerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageHandlerMsg::ProcessText(data) => {
                write!(f, "ProcessText(client_id={})", data.client_id)
            }
            MessageHandlerMsg::ClientConnected { client_id, server_id } => {
                write!(f, "ClientConnected(client={}, server={})", client_id, server_id)
            }
            MessageHandlerMsg::ClientDisconnected { client_id, server_id } => {
                write!(f, "ClientDisconnected(client={}, server={})", client_id, server_id)
            }
        }
    }
}

impl MessageHandler {
    pub fn new(args: MessageHandlerArgs) -> Self {
        Self {
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            command_manager: args.command_manager,
            ws_server: args.ws_server,
            host_id: args.host_id,
            tokio_handle: args.tokio_handle,
            client_subscriptions: HashMap::new(),
        }
    }

    pub fn spawn(args: MessageHandlerArgs) -> ActorHandle<MessageHandlerMsg> {
        let actor = Self::new(args);
        crate::runtime::spawn::spawn(actor)
    }
}

impl Actor for MessageHandler {
    type Msg = MessageHandlerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            MessageHandlerMsg::ProcessText(ProcessTextData { client_id, text }) => {
                trace!("Processing text message from {}: {}", client_id, &text[..text.len().min(200)]);
                let myko_message = match serde_json::from_str::<MykoMessage>(&text) {
                    Ok(msg) => {
                        trace!("Parsed message type: {:?}", std::mem::discriminant(&msg));
                        msg
                    }
                    Err(e) => {
                        debug!("Failed to parse message: {} (text: {:?})", e, text);
                        return;
                    }
                };
                match myko_message {
                    MykoMessage::Event(event) => {
                        let _ = self
                            .event_manager
                            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                                event,
                                persist: PersistEvent::Persist,
                                parsed_item: None, // External events need parsing
                                client_id: Some(client_id.clone()),
                            }));
                    }
                    MykoMessage::Query(query) => {
                        trace!("Received query: {:?}", query);
                        let item_type = query.query_item_type.clone();
                        let tx: Arc<str> = query
                            .query
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .into();

                        // Track subscription for cleanup on disconnect
                        self.client_subscriptions
                            .entry(client_id.clone())
                            .or_default()
                            .insert((SubscriptionType::Query, tx.clone()));

                        // Create a WebSocket sink that sends directly to the client
                        // No intermediate channel or async task needed
                        let sink = Box::new(WebSocketSink::new(
                            client_id.clone(),
                            self.ws_server.clone(),
                            item_type,
                            tx,
                        ));

                        // Start the query with direct WebSocket forwarding
                        if let Err(e) = self.query_manager.send_message(
                            QueryManagerMsg::WatchWrappedQueryWithSink(query, sink)
                        ) {
                            error!("Failed to start query: {}", e);
                        }
                    }
                    MykoMessage::Report(report) => {
                        trace!("Received report: {:?}", report);
                        let tx: Arc<str> = report
                            .report
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .into();

                        // Track subscription for cleanup on disconnect
                        self.client_subscriptions
                            .entry(client_id.clone())
                            .or_default()
                            .insert((SubscriptionType::Report, tx.clone()));

                        // Create RequestContext for this report
                        let req = RequestContext::from_client(
                            tx.clone(),
                            client_id.clone(),
                            self.host_id,
                        );

                        // Create channel for report outputs
                        let (output_tx, mut output_rx) =
                            tokio::sync::mpsc::channel::<serde_json::Value>(16);

                        // Start the report
                        if let Err(e) = self.report_manager.send_message(
                            ReportManagerMsg::StartReport(report, req, output_tx),
                        ) {
                            error!("Failed to start report: {}", e);
                            return;
                        }

                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = self.ws_server.clone();
                        let client_id = client_id.clone();

                        // Use shared tokio runtime for report output forwarding
                        // This avoids creating a new runtime per report subscription
                        self.tokio_handle.spawn(async move {
                            while let Some(value) = output_rx.recv().await {
                                let value_str = value.to_string();
                                let preview = if value_str.len() > 100 {
                                    format!("{}...", &value_str[..100])
                                } else {
                                    value_str
                                };
                                trace!("Sending report response [tx={}]: {}", tx, preview);

                                let response = ReportResponse {
                                    response: value,
                                    tx: tx.to_string(),
                                };

                                // Direct send to WebSocketServer (bypasses Server actor)
                                if let Err(e) = ws_server_ref.send_message(
                                    WebSocketServerMsg::SendToClient(
                                        SendToClientData {
                                            client_id: client_id.clone(),
                                            message: MykoMessage::ReportResponse(response),
                                        }
                                    )
                                ) {
                                    error!("Failed to send report response: {}", e);
                                    break;
                                }
                            }
                        });
                    }
                    MykoMessage::QueryCancel(cancel) => {
                        trace!("Query cancel requested: {}", cancel.tx);
                        if let Err(e) = self.query_manager.send_message(
                            QueryManagerMsg::CancelQuery(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to QueryManager: {}", e);
                        }
                    }
                    MykoMessage::ReportCancel(cancel) => {
                        trace!("Report cancel requested: {}", cancel.tx);
                        if let Err(e) = self.report_manager.send_message(
                            ReportManagerMsg::StopReport(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to ReportManager: {}", e);
                        }
                    }
                    MykoMessage::Command(wrapped_command) => {
                        trace!("Received Command message: {}", wrapped_command.command_id);

                        let tx: Arc<str> = wrapped_command
                            .command
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .map(Arc::from)
                            .unwrap_or_else(|| Arc::from(Uuid::new_v4().to_string()));

                        trace!("Received command: {} with tx {}", wrapped_command.command_id, tx);

                        // Create RequestContext for this command
                        let req = RequestContext::from_client(
                            tx.clone(),
                            client_id.clone(),
                            self.host_id,
                        );

                        let command_manager = self.command_manager.clone();
                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = self.ws_server.clone();
                        let client_id_clone = client_id.clone();

                        // Execute command using the tokio blocking thread pool
                        // This bounds the number of concurrent command executions
                        self.tokio_handle.spawn(async move {
                            let result = tokio::task::spawn_blocking(move || {
                                command_manager.call(|r| CommandManagerMsg::Execute(
                                    wrapped_command,
                                    req,
                                    r
                                ))
                            }).await;

                            let response_message = match result {
                                Ok(Ok(Ok(response))) => {
                                    MykoMessage::CommandResponse(CommandResponse {
                                        response,
                                        tx: tx.to_string(),
                                    })
                                }
                                Ok(Ok(Err(cmd_error))) => {
                                    MykoMessage::CommandError(cmd_error)
                                }
                                Ok(Err(e)) => {
                                    error!("Failed to execute command: {}", e);
                                    MykoMessage::CommandError(CommandError {
                                        tx: tx.to_string(),
                                        message: format!("Internal error: {}", e),
                                    })
                                }
                                Err(e) => {
                                    error!("Command task panicked: {}", e);
                                    MykoMessage::CommandError(CommandError {
                                        tx: tx.to_string(),
                                        message: format!("Internal error: {}", e),
                                    })
                                }
                            };

                            // Direct send to WebSocketServer (bypasses Server actor)
                            if let Err(e) = ws_server_ref.send_message(
                                WebSocketServerMsg::SendToClient(
                                    SendToClientData {
                                        client_id: client_id_clone,
                                        message: response_message,
                                    }
                                )
                            ) {
                                error!("Failed to send command response: {}", e);
                            }
                        });
                    }
                    MykoMessage::Ping(ping_data) => {
                        // Echo ping back immediately for latency measurement
                        trace!("Ping received, echoing back: id={}", ping_data.id);
                        if let Err(e) = self.ws_server.send_message(
                            WebSocketServerMsg::SendToClient(
                                SendToClientData {
                                    client_id: client_id.clone(),
                                    message: MykoMessage::Ping(ping_data),
                                }
                            )
                        ) {
                            error!("Failed to send ping response: {}", e);
                        }
                    }
                    _ => {
                        trace!("Unhandled message type: {:?}", myko_message);
                    }
                }
            }
            MessageHandlerMsg::ClientConnected { client_id, server_id } => {
                trace!("Client connected: {}", client_id);

                // Publish Client entity
                let client = Client {
                    id: client_id.clone(),
                    hash: uuid::Uuid::new_v4().to_string().into(),
                    server_id,
                };

                let event = MEvent::from_item(&client, MEventType::SET, uuid::Uuid::new_v4().to_string());

                if let Err(e) = self.event_manager.send_message(
                    EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: Some(Arc::new(client)),
                        client_id: Some(client_id.clone()),
                    })
                ) {
                    error!("Failed to publish Client entity: {}", e);
                }
            }
            MessageHandlerMsg::ClientDisconnected { client_id, server_id } => {
                debug!("Client disconnected: {}, cancelling subscriptions", client_id);

                // Cancel all subscriptions for this client
                if let Some(subscriptions) = self.client_subscriptions.remove(&client_id) {
                    for (sub_type, tx) in subscriptions {
                        match sub_type {
                            SubscriptionType::Query => {
                                if let Err(e) = self.query_manager.send_message(
                                    QueryManagerMsg::CancelQuery(tx.clone())
                                ) {
                                    error!("Failed to cancel query {}: {}", tx, e);
                                }
                            }
                            SubscriptionType::Report => {
                                if let Err(e) = self.report_manager.send_message(
                                    ReportManagerMsg::StopReport(tx.clone())
                                ) {
                                    error!("Failed to cancel report {}: {}", tx, e);
                                }
                            }
                        }
                    }
                }

                // Delete Client entity
                let client = Client {
                    id: client_id.clone(),
                    hash: "".into(), // Hash doesn't matter for DEL
                    server_id,
                };

                let event = MEvent::from_item(&client, MEventType::DEL, uuid::Uuid::new_v4().to_string());

                if let Err(e) = self.event_manager.send_message(
                    EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: Some(Arc::new(client)),
                        client_id: Some(client_id.clone()),
                    })
                ) {
                    error!("Failed to delete Client entity: {}", e);
                }
            }
        }
    }
}
