use super::event::common::PersistEvent;
use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::{common::ProcessEventData, event_manager::EventManagerMsg},
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
        ws::websocket_server::{SendToClientData, WebSocketServerMsg},
    },
    api::query::QueryResponse,
    command::{CommandError, CommandResponse},
    context::RequestContext,
    entities::client::Client,
    event::{MEvent, MEventType},
    item::WrappedItem,
    message::MykoMessage,
    report::ReportResponse,
};
use futures_signals::signal_map::SignalMapExt;
use log::{debug, error, trace};
use ractor::{Actor, ActorRef, cast};
use std::{collections::{HashMap, HashSet}, sync::Arc};
use uuid::Uuid;

pub struct MessageHandler;

pub struct MessageHandlerArgs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
    pub command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    pub ws_server: ActorRef<WebSocketServerMsg>,
    /// Host ID for request context
    pub host_id: Uuid,
}

pub struct MessageHandlerState {
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    ws_server: ActorRef<WebSocketServerMsg>,
    /// Host ID for creating RequestContext
    host_id: Uuid,
    /// Track active subscriptions per client for cleanup on disconnect
    /// Maps client_id -> set of (subscription_type, tx)
    client_subscriptions: HashMap<Arc<str>, HashSet<(SubscriptionType, Arc<str>)>>,
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

impl Actor for MessageHandler {
    type Msg = MessageHandlerMsg;

    type State = MessageHandlerState;

    type Arguments = MessageHandlerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(MessageHandlerState {
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            report_manager: args.report_manager,
            command_manager: args.command_manager,
            ws_server: args.ws_server,
            host_id: args.host_id,
            client_subscriptions: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            MessageHandlerMsg::ProcessText(ProcessTextData { client_id, text }) => {
                trace!("Processing text message from {}: {}", client_id, &text[..text.len().min(200)]);
                let myko_message = match serde_json::from_str::<MykoMessage>(&text) {
                    Ok(msg) => {
                        trace!("Parsed message type: {:?}", std::mem::discriminant(&msg));
                        msg
                    }
                    Err(e) => {
                        debug!("Failed to parse message: {} (text: {:?})", e, text);
                        return Ok(());
                    }
                };
                match myko_message {
                    MykoMessage::Event(event) => {
                        state
                            .event_manager
                            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                                event,
                                persist: PersistEvent::Persist,
                                parsed_item: None, // External events need parsing
                                client_id: Some(client_id.clone()),
                            }))?;
                        Ok(())
                    }
                    MykoMessage::Query(query) => {
                        trace!("Received query: {:?}", query);
                        let item_type_clone = query.query_item_type.clone();
                        let tx: Arc<str> = query
                            .query
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .into();

                        // Track subscription for cleanup on disconnect
                        state.client_subscriptions
                            .entry(client_id.clone())
                            .or_default()
                            .insert((SubscriptionType::Query, tx.clone()));

                        let sig =
                            ractor::call!(state.query_manager, QueryManagerMsg::StartQuery, query)?;

                        let mut sequence = 0_u64;

                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = state.ws_server.clone();

                        tokio::spawn(sig.for_each(move |v| {
                            trace!("Map Diff in MessageHandler: {:?}", v);

                            let update: QueryResponse = match v {
                                futures_signals::signal_map::MapDiff::Replace { entries } => {
                                    sequence = 0;
                                    let items =
                                        entries.iter().map(|f| f.1.to_value()).collect::<Vec<_>>();

                                    let upserts = items
                                        .iter()
                                        .cloned()
                                        .map(|item| {

                                            WrappedItem {
                                                item,
                                                item_type: item_type_clone.clone(),
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    QueryResponse {
                                        sequence,
                                        deletes: vec![],
                                        upserts,
                                        tx: tx.clone(),
                                    }
                                }
                                futures_signals::signal_map::MapDiff::Insert { key: _, value }
                                | futures_signals::signal_map::MapDiff::Update { key: _, value } => {
                                    let upserts = vec![WrappedItem {
                                        item: value.to_value(),
                                        item_type: item_type_clone.clone(),
                                    }];

                                    sequence += 1;

                                    QueryResponse {
                                        deletes: vec![],
                                        upserts,
                                        tx: tx.clone(),
                                        sequence,
                                    }
                                }

                                futures_signals::signal_map::MapDiff::Remove { key } => {
                                    sequence += 1;

                                    let deletes = vec![key];

                                    QueryResponse {
                                        deletes,
                                        upserts: vec![],
                                        tx: tx.clone(),
                                        sequence,
                                    }
                                }
                                futures_signals::signal_map::MapDiff::Clear {} => {
                                    sequence = 0;

                                    QueryResponse {
                                        deletes: vec![],
                                        upserts: vec![],
                                        tx: tx.clone(),
                                        sequence,
                                    }
                                }
                            };

                            // Direct send to WebSocketServer (bypasses Server actor)
                            if let Err(e) = cast!(
                                ws_server_ref,
                                WebSocketServerMsg::SendToClient(
                                    SendToClientData {
                                        client_id: client_id.clone(),
                                        message: MykoMessage::QueryResponse(update),
                                    }
                                )
                            ) {
                                error!("Failed to send query response: {}", e);
                            };

                            async {}
                        }));

                        Ok(())
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
                        state.client_subscriptions
                            .entry(client_id.clone())
                            .or_default()
                            .insert((SubscriptionType::Report, tx.clone()));

                        // Create RequestContext for this report
                        let req = RequestContext::from_client(
                            tx.clone(),
                            client_id.clone(),
                            state.host_id,
                        );

                        // Create channel for report outputs
                        let (output_tx, mut output_rx) =
                            tokio::sync::mpsc::channel::<serde_json::Value>(16);

                        // Start the report
                        if let Err(e) = state.report_manager.send_message(
                            ReportManagerMsg::StartReport(report, req, output_tx),
                        ) {
                            error!("Failed to start report: {}", e);
                            return Ok(());
                        }

                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = state.ws_server.clone();
                        let client_id = client_id.clone();

                        // Spawn task to forward report outputs to client
                        tokio::spawn(async move {
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
                                if let Err(e) = cast!(
                                    ws_server_ref,
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

                        Ok(())
                    }
                    MykoMessage::QueryCancel(cancel) => {
                        trace!("Query cancel requested: {}", cancel.tx);
                        if let Err(e) = state.query_manager.send_message(
                            QueryManagerMsg::CancelQuery(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to QueryManager: {}", e);
                        }
                        Ok(())
                    }
                    MykoMessage::ReportCancel(cancel) => {
                        trace!("Report cancel requested: {}", cancel.tx);
                        if let Err(e) = state.report_manager.send_message(
                            ReportManagerMsg::StopReport(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to ReportManager: {}", e);
                        }
                        Ok(())
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
                            state.host_id,
                        );

                        let command_manager = state.command_manager.clone();
                        // Direct reference to WS server (bypasses Server routing)
                        let ws_server_ref = state.ws_server.clone();
                        let client_id_clone = client_id.clone();

                        // Execute command asynchronously and send response
                        tokio::spawn(async move {
                            let result = ractor::call!(
                                command_manager,
                                CommandManagerMsg::Execute,
                                wrapped_command,
                                req
                            );

                            let response_message = match result {
                                Ok(Ok(response)) => {
                                    MykoMessage::CommandResponse(CommandResponse {
                                        response,
                                        tx: tx.to_string(),
                                    })
                                }
                                Ok(Err(cmd_error)) => {
                                    MykoMessage::CommandError(cmd_error)
                                }
                                Err(e) => {
                                    error!("Failed to execute command: {}", e);
                                    MykoMessage::CommandError(CommandError {
                                        tx: tx.to_string(),
                                        message: format!("Internal error: {}", e),
                                    })
                                }
                            };

                            // Direct send to WebSocketServer (bypasses Server actor)
                            if let Err(e) = ractor::cast!(
                                ws_server_ref,
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

                        Ok(())
                    }
                    MykoMessage::Ping(ping_data) => {
                        // Echo ping back immediately for latency measurement
                        trace!("Ping received, echoing back: id={}", ping_data.id);
                        if let Err(e) = cast!(
                            state.ws_server,
                            WebSocketServerMsg::SendToClient(
                                SendToClientData {
                                    client_id: client_id.clone(),
                                    message: MykoMessage::Ping(ping_data),
                                }
                            )
                        ) {
                            error!("Failed to send ping response: {}", e);
                        }
                        Ok(())
                    }
                    _ => {
                        trace!("Unhandled message type: {:?}", myko_message);
                        Ok(())
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

                if let Err(e) = state.event_manager.send_message(
                    EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: Some(Arc::new(client)),
                        client_id: Some(client_id.clone()),
                    })
                ) {
                    error!("Failed to publish Client entity: {}", e);
                }

                Ok(())
            }
            MessageHandlerMsg::ClientDisconnected { client_id, server_id } => {
                debug!("Client disconnected: {}, cancelling subscriptions", client_id);

                // Cancel all subscriptions for this client
                if let Some(subscriptions) = state.client_subscriptions.remove(&client_id) {
                    for (sub_type, tx) in subscriptions {
                        match sub_type {
                            SubscriptionType::Query => {
                                if let Err(e) = state.query_manager.send_message(
                                    QueryManagerMsg::CancelQuery(tx.clone())
                                ) {
                                    error!("Failed to cancel query {}: {}", tx, e);
                                }
                            }
                            SubscriptionType::Report => {
                                if let Err(e) = state.report_manager.send_message(
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

                if let Err(e) = state.event_manager.send_message(
                    EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: Some(Arc::new(client)),
                        client_id: Some(client_id.clone()),
                    })
                ) {
                    error!("Failed to delete Client entity: {}", e);
                }

                Ok(())
            }
        }
    }
}
