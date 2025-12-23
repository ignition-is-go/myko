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
    report::{ReportError, ReportOutput, ReportResponse},
    runtime::{Actor, ActorRef},
};
use log::{debug, error, trace};
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
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
    /// CancellationToken per client - cancelled on disconnect to clean up all subscriptions
    client_tokens: HashMap<Arc<str>, CancellationToken>,
    /// Cache of client windback times for efficient lookup
    /// Maps client_id -> windback ISO timestamp (None = live mode)
    client_windback: HashMap<Arc<str>, Option<Arc<str>>>,
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
    /// Update windback cache for a client (called when SetClientWindbackTime/ClearClientWindbackTime succeeds)
    UpdateWindback {
        client_id: Arc<str>,
        windback: Option<Arc<str>>,
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
            MessageHandlerMsg::UpdateWindback { client_id, windback } => {
                write!(f, "UpdateWindback(client={}, windback={:?})", client_id, windback)
            }
        }
    }
}

impl MessageHandler {
    fn create(args: MessageHandlerArgs) -> Self {
        Self {
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            command_manager: args.command_manager,
            ws_server: args.ws_server,
            host_id: args.host_id,
            tokio_handle: args.tokio_handle,
            client_tokens: HashMap::new(),
            client_windback: HashMap::new(),
        }
    }

    /// Get or create a CancellationToken for a client.
    /// All subscriptions for this client will use child tokens of this parent.
    fn get_client_token(&mut self, client_id: &Arc<str>) -> CancellationToken {
        self.client_tokens
            .entry(client_id.clone())
            .or_default()
            .clone()
    }

    /// Get the windback timestamp for a client (if in windback mode).
    fn get_windback(&self, client_id: &Arc<str>) -> Option<Arc<str>> {
        self.client_windback.get(client_id).cloned().flatten()
    }

    /// Update the windback cache for a client.
    pub fn set_windback(&mut self, client_id: Arc<str>, windback: Option<Arc<str>>) {
        self.client_windback.insert(client_id, windback);
    }
}

impl Actor for MessageHandler {
    type Msg = MessageHandlerMsg;
    type Args = MessageHandlerArgs;

    fn new(args: Self::Args, _myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args)
    }

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
                        let query_id = query.query_id.clone();
                        let tx: Arc<str> = query
                            .query
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .into();

                        // Get or create client token - cancelling this cancels all client subscriptions
                        let client_token = self.get_client_token(&client_id);

                        // Create a WebSocket sink that sends directly to the client
                        // No intermediate channel or async task needed
                        let sink = Box::new(WebSocketSink::new(
                            client_id.clone(),
                            self.ws_server.clone(),
                            item_type,
                            query_id,
                            tx,
                        ));

                        // Start the query with direct WebSocket forwarding
                        // Pass client token so query is cancelled when client disconnects
                        // None lineage indicates external WebSocket client query
                        if let Err(e) = self.query_manager.send_message(
                            QueryManagerMsg::WatchWrappedQueryWithSink(query, sink, client_token, None)
                        ) {
                            error!("Failed to start query: {}", e);
                        }
                    }
                    MykoMessage::Report(report) => {
                        trace!("Received report: {:?}", report);
                        let report_id = report.report_id.clone();
                        let tx: Arc<str> = report
                            .report
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .into();

                        // Get or create client token - cancelling this cancels all client subscriptions
                        let client_token = self.get_client_token(&client_id);

                        // Create RequestContext for this report with windback state
                        let windback = self.get_windback(&client_id);
                        let req = RequestContext::from_client_with_windback(
                            tx.clone(),
                            client_id.clone(),
                            self.host_id,
                            windback,
                        );

                        // Create channel for report outputs
                        let (output_tx, mut output_rx) =
                            tokio::sync::mpsc::channel::<ReportOutput>(16);

                        // Start the report with client token as parent
                        // When client disconnects, token is cancelled and report stops
                        if let Err(e) = self.report_manager.send_message(
                            ReportManagerMsg::StartReport(report, req, output_tx, Some(client_token)),
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
                            while let Some(output) = output_rx.recv().await {
                                let message = match output {
                                    ReportOutput::Value(value) => {
                                        let value_str = value.to_string();
                                        let preview = if value_str.len() > 100 {
                                            format!("{}...", &value_str[..100])
                                        } else {
                                            value_str
                                        };
                                        trace!("Sending report response [tx={}]: {}", tx, preview);

                                        MykoMessage::ReportResponse(ReportResponse {
                                            response: value,
                                            tx: tx.to_string(),
                                        })
                                    }
                                    ReportOutput::Error(err_msg) => {
                                        error!("Report error [tx={}]: {}", tx, err_msg);
                                        MykoMessage::ReportError(ReportError {
                                            tx: tx.to_string(),
                                            report_id: report_id.clone(),
                                            message: err_msg,
                                        })
                                    }
                                };

                                // Direct send to WebSocketServer (bypasses Server actor)
                                if let Err(e) = ws_server_ref.send_message(
                                    WebSocketServerMsg::SendToClient(
                                        SendToClientData {
                                            client_id: client_id.clone(),
                                            message,
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

                        let command_id = wrapped_command.command_id.clone();
                        let tx: Arc<str> = wrapped_command
                            .command
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .map(Arc::from)
                            .unwrap_or_else(|| Arc::from(Uuid::new_v4().to_string()));

                        trace!("Received command: {} with tx {}", command_id, tx);

                        // Create RequestContext for this command with windback state
                        let windback = self.get_windback(&client_id);
                        let req = RequestContext::from_client_with_windback(
                            tx.clone(),
                            client_id.clone(),
                            self.host_id,
                            windback,
                        );

                        let command_manager = self.command_manager.clone();
                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = self.ws_server.clone();
                        let client_id_clone = client_id.clone();
                        let command_id_for_log = command_id.clone();
                        let tx_for_log = tx.clone();

                        // Execute command using the tokio blocking thread pool
                        // This bounds the number of concurrent command executions
                        debug!("[CMD:{}] Spawning command task for tx={}", command_id, tx);
                        self.tokio_handle.spawn(async move {
                            debug!("[CMD:{}] Inside async task, calling command_manager for tx={}", command_id_for_log, tx_for_log);
                            // Use block_in_place instead of spawn_blocking to avoid blocking thread pool exhaustion
                            // when many reports are running (each report holds a blocking thread for channel recv)
                            let result = tokio::task::block_in_place(|| {
                                debug!("[CMD:{}] Inside block_in_place, calling command_manager for tx={}", command_id_for_log, tx_for_log);
                                let res = command_manager.call(|r| CommandManagerMsg::Execute(
                                    wrapped_command,
                                    req,
                                    r
                                ));
                                debug!("[CMD:{}] command_manager.call returned for tx={}", command_id_for_log, tx_for_log);
                                res
                            });
                            debug!("[CMD:{}] block_in_place completed for tx={}", command_id_for_log, tx_for_log);

                            // block_in_place returns Result<Result<Value, CommandError>, CallError>
                            let response_message = match result {
                                Ok(Ok(response)) => {
                                    MykoMessage::CommandResponse(CommandResponse {
                                        response,
                                        tx: tx_for_log.to_string(),
                                    })
                                }
                                Ok(Err(cmd_error)) => {
                                    MykoMessage::CommandError(cmd_error)
                                }
                                Err(e) => {
                                    error!("Failed to execute command: {}", e);
                                    MykoMessage::CommandError(CommandError {
                                        tx: tx_for_log.to_string(),
                                        command_id: command_id_for_log.clone(),
                                        message: format!("Internal error: {:?}", e),
                                    })
                                }
                            };

                            // Direct send to WebSocketServer (bypasses Server actor)
                            debug!("[CMD:{}] Sending response to client {} for tx={}", command_id_for_log, client_id_clone, tx_for_log);
                            if let Err(e) = ws_server_ref.send_message(
                                WebSocketServerMsg::SendToClient(
                                    SendToClientData {
                                        client_id: client_id_clone.clone(),
                                        message: response_message,
                                    }
                                )
                            ) {
                                error!("Failed to send command response: {}", e);
                            } else {
                                debug!("[CMD:{}] Response sent successfully for tx={}", command_id_for_log, tx_for_log);
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

                // Create CancellationToken for this client - cancelling this on disconnect
                // will automatically cancel all queries and reports for this client
                let _ = self.get_client_token(&client_id);

                // Initialize windback cache for this client (starts in live mode)
                self.client_windback.insert(client_id.clone(), None);

                // Publish Client entity
                let client = Client {
                    id: client_id.clone(),
                    hash: uuid::Uuid::new_v4().to_string().into(),
                    server_id,
                    windback: None,
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

                // Remove from windback cache
                self.client_windback.remove(&client_id);

                // Cancel client token - this automatically cancels all queries and reports
                // that were started with this token as parent
                if let Some(token) = self.client_tokens.remove(&client_id) {
                    token.cancel();
                }

                // Delete Client entity
                let client = Client {
                    id: client_id.clone(),
                    hash: "".into(), // Hash doesn't matter for DEL
                    server_id,
                    windback: None, // Doesn't matter for DEL
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
            MessageHandlerMsg::UpdateWindback { client_id, windback } => {
                debug!("Updating windback for client {}: {:?}", client_id, windback);
                self.client_windback.insert(client_id, windback);
            }
        }
    }
}
