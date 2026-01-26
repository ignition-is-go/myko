//! WebSocket handler for the cell-based server.
//!
//! Handles WebSocket connections using ClientSession for subscription management.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use hypha::SelectExt;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use uuid::Uuid;

use super::{
    CellServerCtx,
    client_session::{ClientSession, WsWriter},
};
use crate::{
    command::{CommandContext, CommandHandlerRegistration},
    entities::client::Client,
    request::RequestContext,
    wire::{CancelSubscription, CommandError, CommandResponse, MykoMessage, PingData},
};

/// Protocol switch message sent by client to enable binary (msgpack) encoding.
/// Must match ProtocolMessages.SwitchToMSGPACK in TypeScript client.
const SWITCH_TO_MSGPACK: &str = "myko:switch-to-msgpack";

/// WebSocket handler for a single client connection.
pub struct WsHandler;

impl WsHandler {
    /// Handle a new WebSocket connection.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        ctx: Arc<CellServerCtx>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let host_id = ctx.host_id;

        let ws_stream = accept_async(stream).await?;
        let (mut write, mut read) = ws_stream.split();

        // Create a bounded channel for sending messages to the client
        // High limit (10k) since we have good memory availability
        let (tx, mut rx) = mpsc::channel::<MykoMessage>(10_000);

        // Create client session with channel-based writer
        let client_id: Arc<str> = Uuid::new_v4().to_string().into();
        let writer = ChannelWriter { tx: tx.clone() };
        let mut session = ClientSession::new(client_id.clone(), writer);

        // Protocol: default to JSON, switch to binary only if client opts in
        let use_binary = Arc::new(AtomicBool::new(false));
        let use_binary_writer = use_binary.clone();

        // Publish Client entity
        let client_entity = Client {
            id: client_id.clone(),
            hash: client_id.clone(),
            server_id: host_id.to_string().into(),
            windback: None,
        };
        ctx.set(&client_entity);

        log::info!("Client connected: {} from {}", client_id, addr);

        let write_ctx = ctx.clone();

        // Spawn task to forward messages from channel to WebSocket
        let write_task = tokio::spawn(async move {
            let _ctx = write_ctx;
            while let Some(msg) = rx.recv().await {
                let ws_msg = if use_binary_writer.load(Ordering::SeqCst) {
                    // Binary mode: use msgpack
                    match rmp_serde::to_vec(&msg) {
                        Ok(bytes) => Message::Binary(bytes),
                        Err(e) => {
                            log::error!("Failed to serialize message to msgpack: {}", e);
                            continue;
                        }
                    }
                } else {
                    // JSON mode (default)
                    match serde_json::to_string(&msg) {
                        Ok(json) => Message::Text(json),
                        Err(e) => {
                            log::error!("Failed to serialize message to JSON: {}", e);
                            continue;
                        }
                    }
                };
                if write.send(ws_msg).await.is_err() {
                    break;
                }
            }
        });

        // Process incoming messages
        while let Some(msg) = read.next().await {
            let ctx = ctx.clone();
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    log::error!("WebSocket error from {}: {}", client_id, e);
                    break;
                }
            };

            match msg {
                Message::Binary(data) => {
                    // Auto-detect: receiving binary means client wants binary responses
                    if !use_binary.load(Ordering::SeqCst) {
                        log::info!(
                            "Client {} auto-switching to binary (msgpack) protocol",
                            client_id
                        );
                        use_binary.store(true, Ordering::SeqCst);
                    }

                    match rmp_serde::from_slice::<MykoMessage>(&data) {
                        Ok(myko_msg) => {
                            if let Err(e) = Self::handle_message(&mut session, ctx, &tx, myko_msg) {
                                log::error!("Error handling message: {}", e);
                            }
                        }
                        Err(e) => {
                            log::warn!("Failed to parse message from {}: {}", client_id, e);
                        }
                    }
                }
                Message::Text(text) => {
                    // Check for protocol switch request
                    if text == SWITCH_TO_MSGPACK {
                        log::info!(
                            "Client {} switching to binary (msgpack) protocol",
                            client_id
                        );
                        // Send confirmation FIRST (still in JSON mode)
                        if let Err(e) = tx.try_send(MykoMessage::ProtocolSwitch {
                            protocol: "msgpack".into(),
                        }) {
                            log::warn!(
                                "WebSocket send buffer full, dropping ProtocolSwitch: {}",
                                e
                            );
                        }
                        // Then switch to binary for subsequent messages
                        use_binary.store(true, Ordering::SeqCst);
                        continue;
                    }

                    match serde_json::from_str::<MykoMessage>(&text) {
                        Ok(myko_msg) => {
                            if let Err(e) = Self::handle_message(&mut session, ctx, &tx, myko_msg) {
                                log::error!("Error handling message: {}", e);
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to parse JSON message from {}: {} | raw: {}",
                                client_id,
                                e,
                                if text.len() > 1000 { &text[..1000] } else { &text }
                            );
                        }
                    }
                }
                Message::Ping(data) => {
                    // Pong is sent automatically by tungstenite
                    log::trace!("Ping from {}", client_id);
                    let _ = data; // silence unused warning
                }
                Message::Pong(_) => {
                    log::trace!("Pong from {}", client_id);
                }
                Message::Close(_) => {
                    log::info!("Client {} disconnecting", client_id);
                    break;
                }
                Message::Frame(_) => {
                    // Raw frames - ignore
                }
            }
        }

        // Cleanup
        drop(session); // Drops all subscription guards
        write_task.abort();

        // Delete Client entity
        ctx.del(&client_entity);

        log::info!("Client disconnected: {}", client_id);

        Ok(())
    }

    /// Handle a parsed MykoMessage.
    fn handle_message<W: WsWriter>(
        session: &mut ClientSession<W>,
        ctx: Arc<CellServerCtx>,
        tx: &mpsc::Sender<MykoMessage>,
        msg: MykoMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handler_registry = ctx.handler_registry.clone();

        let registry = ctx.registry.clone();

        let host_id = ctx.host_id;

        match msg {
            MykoMessage::Query(wrapped) => {
                // Extract tx from the query JSON
                let tx_id: Arc<str> = wrapped
                    .query
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let query_id = &wrapped.query_id;
                let entity_type = &wrapped.query_item_type;

                log::debug!("Query {} for {} (tx: {})", query_id, entity_type, tx_id);

                let request_context = Arc::new(RequestContext::from_client(
                    tx_id.clone(),
                    session.client_id.clone(),
                    host_id,
                ));

                // Look up the query registration
                if let Some(query_data) = handler_registry.get_query(query_id) {
                    // Parse the query JSON to the concrete type
                    let parsed = (query_data.parse)(wrapped.query.clone());
                    match parsed {
                        Ok(any_query) => {
                            // Create the cell using the factory (with host_id for server context)
                            match (query_data.cell_factory)(
                                any_query,
                                registry.clone(),
                                request_context.clone(),
                            ) {
                                Ok(filtered_cellmap) => {
                                    session.subscribe_query(tx_id, filtered_cellmap);
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create query cell for {}: {}",
                                        query_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse query {}: {} | payload: {}",
                                query_id,
                                e,
                                serde_json::to_string(&wrapped.query).unwrap_or_default()
                            );
                        }
                    }
                } else {
                    // Fall back to select all for unknown queries
                    log::warn!(
                        "No registered handler for query {}, falling back to select all",
                        query_id
                    );
                    let cellmap = registry.get_or_create(entity_type).select(|_| true);
                    session.subscribe_query(tx_id, cellmap);
                }
            }

            MykoMessage::QueryCancel(CancelSubscription { tx: tx_id }) => {
                log::debug!("Query cancel: {}", tx_id);
                session.cancel(&tx_id.into());
            }

            MykoMessage::Report(wrapped) => {
                // Extract tx from the report JSON
                let tx_id: Arc<str> = wrapped
                    .report
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let report_id = &wrapped.report_id;

                log::debug!("Report {} (tx: {})", report_id, tx_id);

                // Look up the report registration
                if let Some(report_data) = handler_registry.get_report(report_id) {
                    // Parse the report JSON to the concrete type
                    let parsed = (report_data.parse)(wrapped.report.clone());
                    match parsed {
                        Ok(any_report) => {
                            let request_context = Arc::new(RequestContext::from_client(
                                tx_id.clone(),
                                session.client_id.clone(),
                                host_id,
                            ));

                            // Create the cell using the factory (with host_id for context)
                            match (report_data.cell_factory)(any_report, request_context, ctx) {
                                Ok(cell) => {
                                    session.subscribe_report(
                                        tx_id,
                                        report_id.as_str().into(),
                                        cell,
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create report cell for {}: {}",
                                        report_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse report {}: {} | payload: {}",
                                report_id,
                                e,
                                serde_json::to_string(&wrapped.report).unwrap_or_default()
                            );
                        }
                    }
                } else {
                    log::warn!("No registered handler for report: {}", report_id);
                }
            }

            MykoMessage::ReportCancel(CancelSubscription { tx: tx_id }) => {
                log::debug!("Report cancel: {}", tx_id);
                session.cancel(&tx_id.into());
            }

            MykoMessage::Event(event) => {
                use crate::event::MEventType;

                // log::debug!("Event: {:?} {}", event.change_type(), event.item_type());

                match event.change_type {
                    MEventType::SET => {
                        // Parse JSON to typed entity
                        if let Some(item) = ctx.parse_item(&event.item_type, &event.item) {
                            // Publish with default options (Reduce + Relationships + Persist)
                            ctx.set_dyn(item);
                        } else {
                            log::warn!("Unknown entity type or parse error: {}", event.item_type);
                        }
                    }
                    MEventType::DEL => {
                        // For DEL, parse to get the entity (needed for relationships)
                        if let Some(item) = ctx.parse_item(&event.item_type, &event.item) {
                            ctx.del_dyn(item);
                        } else {
                            log::warn!(
                                "Unknown entity type or parse error for DEL: {}",
                                event.item_type
                            );
                        }
                    }
                }
            }

            MykoMessage::Command(wrapped) => {
                // Extract tx from the command JSON
                let tx_id: Arc<str> = wrapped
                    .command
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();

                let command_id = &wrapped.command_id;

                log::debug!("Command {} (tx: {})", command_id, tx_id);

                // Look up the command handler via inventory
                let mut handler_found = false;
                for registration in inventory::iter::<CommandHandlerRegistration> {
                    if registration.command_id == command_id {
                        handler_found = true;
                        let executor = (registration.factory)();

                        // Create request context
                        let req = Arc::new(RequestContext::from_client(
                            tx_id.clone(),
                            session.client_id.clone(),
                            host_id,
                        ));

                        let cmd_id: Arc<str> = Arc::from(wrapped.command_id.clone());

                        // Create command context
                        let cmd_ctx = CommandContext::new(cmd_id, req, ctx.clone());

                        // Execute the command
                        match executor.execute_from_value(wrapped.command.clone(), cmd_ctx) {
                            Ok(result) => {
                                let response = MykoMessage::CommandResponse(CommandResponse {
                                    response: result,
                                    tx: tx_id.to_string(),
                                });
                                if let Err(e) = tx.try_send(response) {
                                    log::warn!(
                                        "WebSocket send buffer full, dropping CommandResponse: {}",
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                let error = MykoMessage::CommandError(CommandError {
                                    tx: tx_id.to_string(),
                                    command_id: command_id.to_string(),
                                    message: e.message,
                                });
                                if let Err(e) = tx.try_send(error) {
                                    log::warn!(
                                        "WebSocket send buffer full, dropping CommandError: {}",
                                        e
                                    );
                                }
                            }
                        }
                        break;
                    }
                }

                if !handler_found {
                    log::warn!("No registered handler for command: {}", command_id);
                    let error = MykoMessage::CommandError(CommandError {
                        tx: tx_id.to_string(),
                        command_id: command_id.to_string(),
                        message: format!("Command handler not found: {}", command_id),
                    });
                    if let Err(e) = tx.try_send(error) {
                        log::warn!("WebSocket send buffer full, dropping CommandError: {}", e);
                    }
                }
            }

            MykoMessage::Ping(PingData { id, timestamp }) => {
                // Echo back the ping data
                let pong = MykoMessage::Ping(PingData { id, timestamp });
                if let Err(e) = tx.try_send(pong) {
                    log::warn!("WebSocket send buffer full, dropping Ping response: {}", e);
                }
            }

            // Response messages - these shouldn't come from clients
            MykoMessage::QueryResponse(_)
            | MykoMessage::QueryError(_)
            | MykoMessage::ReportResponse(_)
            | MykoMessage::ReportError(_)
            | MykoMessage::CommandResponse(_)
            | MykoMessage::CommandError(_)
            | MykoMessage::ProtocolSwitch { .. } => {
                log::warn!("Received unexpected response message from client");
            }
        }

        Ok(())
    }
}

/// Channel-based WebSocket writer.
///
/// Sends messages through an mpsc channel which are then
/// forwarded to the actual WebSocket.
struct ChannelWriter {
    tx: mpsc::Sender<MykoMessage>,
}

impl WsWriter for ChannelWriter {
    fn send(&self, msg: MykoMessage) {
        // Use try_send since we're in sync context
        if let Err(e) = self.tx.try_send(msg) {
            log::warn!("WebSocket send buffer full, dropping message: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_writer() {
        let (tx, mut rx) = mpsc::channel(10);
        let writer = ChannelWriter { tx };

        let msg = MykoMessage::Ping(PingData {
            id: "test".to_string(),
            timestamp: 0,
        });
        writer.send(msg);

        let received = rx.try_recv().unwrap();
        assert!(matches!(received, MykoMessage::Ping(_)));
    }
}
