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
    item::WrappedItem,
    message::MykoMessage,
    report::ReportResponse,
};
use futures_signals::signal_map::SignalMapExt;
use log::{debug, error, trace};
use ractor::{Actor, ActorRef, cast};
use std::sync::Arc;

pub struct MessageHandler;

pub struct MessageHandlerArgs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
    pub command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    pub ws_server: ActorRef<WebSocketServerMsg>,
}

pub struct MessageHandlerState {
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    command_manager: ActorRef<CommandManagerMsg>,
    /// Direct reference to WebSocketServer (bypasses Server routing)
    ws_server: ActorRef<WebSocketServerMsg>,
}

pub struct ProcessTextData {
    pub client_id: Arc<str>,
    pub text: String,
}

pub enum MessageHandlerMsg {
    ProcessText(ProcessTextData),
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
                debug!("Processing text message from {}: {}", client_id, &text[..text.len().min(200)]);
                let myko_message = match serde_json::from_str::<MykoMessage>(&text) {
                    Ok(msg) => {
                        debug!("Parsed message type: {:?}", std::mem::discriminant(&msg));
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

                        // Create channel for report outputs
                        let (output_tx, mut output_rx) =
                            tokio::sync::mpsc::channel::<serde_json::Value>(16);

                        // Start the report
                        if let Err(e) = state.report_manager.send_message(
                            ReportManagerMsg::StartReport(report, output_tx),
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
                        debug!("Query cancel requested: {}", cancel.tx);
                        if let Err(e) = state.query_manager.send_message(
                            QueryManagerMsg::CancelQuery(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to QueryManager: {}", e);
                        }
                        Ok(())
                    }
                    MykoMessage::ReportCancel(cancel) => {
                        debug!("Report cancel requested: {}", cancel.tx);
                        if let Err(e) = state.report_manager.send_message(
                            ReportManagerMsg::StopReport(cancel.tx.into())
                        ) {
                            error!("Failed to send cancel to ReportManager: {}", e);
                        }
                        Ok(())
                    }
                    MykoMessage::Command(wrapped_command) => {
                        debug!("Received Command message: {}", wrapped_command.command_id);

                        let tx: String = wrapped_command
                            .command
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

                        trace!("Received command: {} with tx {}", wrapped_command.command_id, tx);

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
                                client_id_clone.clone()
                            );

                            let response_message = match result {
                                Ok(Ok(response)) => {
                                    MykoMessage::CommandResponse(CommandResponse {
                                        response,
                                        tx: tx.clone(),
                                    })
                                }
                                Ok(Err(cmd_error)) => {
                                    MykoMessage::CommandError(cmd_error)
                                }
                                Err(e) => {
                                    error!("Failed to execute command: {}", e);
                                    MykoMessage::CommandError(CommandError {
                                        tx: tx.clone(),
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
                    _ => {
                        trace!("Unhandled message type: {:?}", myko_message);
                        Ok(())
                    }
                }
            }
        }
    }
}
