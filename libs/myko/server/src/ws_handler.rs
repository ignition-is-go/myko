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
use myko_rs::{
    command::{CommandContext, CommandHandlerRegistration},
    entities::client::Client,
    relationship::{iter_client_id_registrations, iter_fallback_to_id_registrations},
    request::RequestContext,
    server::{CellServerCtx, ClientSession, WsWriter, client_registry::try_client_registry},
    wire::{
        CancelSubscription, CommandError, CommandResponse, MEvent, MEventType, MykoMessage,
        PingData, QueryWindowUpdate, ViewError, ViewWindowUpdate,
    },
};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use uuid::Uuid;

/// Protocol switch message sent by client to enable binary (msgpack) encoding.
/// Must match ProtocolMessages.SwitchToMSGPACK in TypeScript client.
const SWITCH_TO_MSGPACK: &str = "myko:switch-to-msgpack";

fn normalize_incoming_event(event: &mut MEvent, client_id: &str) {
    if event.change_type != MEventType::SET {
        return;
    }

    // Auto-populate #[myko_client_id] fields with the connection's client_id
    for reg in iter_client_id_registrations() {
        if reg.entity_type == event.item_type {
            if let Some(obj) = event.item.as_object_mut() {
                obj.insert(
                    reg.field_name_json.to_string(),
                    serde_json::Value::String(client_id.to_string()),
                );
            }
            break;
        }
    }

    // Auto-populate #[fallback_to_id] fields with the entity's own id
    // if the field is null or missing.
    if let Some(obj) = event.item.as_object_mut()
        && let Some(id) = obj.get("id").and_then(|v| v.as_str()).map(String::from)
    {
        for reg in iter_fallback_to_id_registrations() {
            if reg.entity_type == event.item_type {
                let field = reg.field_name_json;
                if matches!(obj.get(field), None | Some(serde_json::Value::Null)) {
                    obj.insert(field.to_string(), serde_json::Value::String(id.clone()));
                }
            }
        }
    }
}

/// Per-connection drop tracking to avoid log storms when clients fall behind.
///
/// When the outbound channel is full, we will drop messages (same as today),
/// but we must not `warn!` for every drop or we can effectively DoS ourselves.
struct DropLogger {
    client_id: Arc<str>,
    dropped: std::sync::atomic::AtomicU64,
    last_log_ms: std::sync::atomic::AtomicU64,
}

impl DropLogger {
    fn new(client_id: Arc<str>) -> Self {
        Self {
            client_id,
            dropped: std::sync::atomic::AtomicU64::new(0),
            last_log_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn on_drop(&self, kind: &'static str, err: &dyn std::fmt::Display) {
        use std::sync::atomic::Ordering;

        self.dropped.fetch_add(1, Ordering::Relaxed);

        // Log at most once per second per connection.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last_ms = self.last_log_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_ms) < 1000 {
            return;
        }

        if self
            .last_log_ms
            .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let n = self.dropped.swap(0, Ordering::Relaxed);
        log::warn!(
            "WebSocket send buffer full; dropped {} message(s) for client {} (latest: {}): {}",
            n,
            self.client_id,
            kind,
            err
        );
    }
}

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
        let drop_logger = Arc::new(DropLogger::new(client_id.clone()));
        let writer = ChannelWriter {
            tx: tx.clone(),
            drop_logger: drop_logger.clone(),
        };

        // Register writer in the global client registry (if initialized)
        let writer_arc: Arc<dyn WsWriter> = Arc::new(ChannelWriter {
            tx: tx.clone(),
            drop_logger: drop_logger.clone(),
        });
        if let Some(registry) = try_client_registry() {
            registry.register(client_id.clone(), writer_arc);
        }

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
                            if let Err(e) = Self::handle_message(
                                &mut session,
                                ctx,
                                &tx,
                                drop_logger.as_ref(),
                                myko_msg,
                            ) {
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
                            drop_logger.on_drop("ProtocolSwitch", &e);
                        }
                        // Then switch to binary for subsequent messages
                        use_binary.store(true, Ordering::SeqCst);
                        continue;
                    }

                    match serde_json::from_str::<MykoMessage>(&text) {
                        Ok(myko_msg) => {
                            if let Err(e) = Self::handle_message(
                                &mut session,
                                ctx,
                                &tx,
                                drop_logger.as_ref(),
                                myko_msg,
                            ) {
                                log::error!("Error handling message: {}", e);
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to parse JSON message from {}: {} | raw: {}",
                                client_id,
                                e,
                                if text.len() > 1000 {
                                    &text[..1000]
                                } else {
                                    &text
                                }
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

        // Unregister from client registry
        if let Some(registry) = try_client_registry() {
            registry.unregister(&client_id);
        }

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
        drop_logger: &DropLogger,
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

                log::trace!("Query {} for {} (tx: {})", query_id, entity_type, tx_id);
                log::debug!(
                    "Query subscribe request client={} tx={} query_id={} item_type={} window={} active_subscriptions_before={}",
                    session.client_id,
                    tx_id,
                    query_id,
                    entity_type,
                    wrapped.window.is_some(),
                    session.subscription_count()
                );

                let request_context = Arc::new(RequestContext::from_client(
                    tx_id.clone(),
                    session.client_id.clone(),
                    host_id,
                ));

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
                                Some(ctx.clone()),
                            ) {
                                Ok(filtered_cellmap) => {
                                    session.subscribe_query(
                                        tx_id,
                                        filtered_cellmap,
                                        wrapped.window.clone(),
                                    );
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
                        "No registered query handler for {}, falling back to select all",
                        query_id
                    );
                    let cellmap = registry.get_or_create(entity_type).select(|_| true);
                    session.subscribe_query(tx_id, cellmap, wrapped.window.clone());
                }
            }

            MykoMessage::View(wrapped) => {
                let tx_id: Arc<str> = wrapped
                    .view
                    .get("tx")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .into();
                let view_id = &wrapped.view_id;
                let item_type = &wrapped.view_item_type;

                log::trace!("View {} for {} (tx: {})", view_id, item_type, tx_id);
                log::debug!(
                    "View subscribe request client={} tx={} view_id={} item_type={} window={:?}",
                    session.client_id,
                    tx_id,
                    view_id,
                    item_type,
                    wrapped.window
                );

                let request_context = Arc::new(RequestContext::from_client(
                    tx_id.clone(),
                    session.client_id.clone(),
                    host_id,
                ));

                if let Some(view_data) = handler_registry.get_view(view_id) {
                    let parsed = (view_data.parse)(wrapped.view.clone());
                    match parsed {
                        Ok(any_view) => {
                            log::debug!(
                                "View parsed successfully client={} tx={} view_id={}",
                                session.client_id,
                                tx_id,
                                view_id
                            );
                            match (view_data.cell_factory)(
                                any_view,
                                registry.clone(),
                                request_context,
                                Some(ctx.clone()),
                            ) {
                                Ok(filtered_cellmap) => {
                                    log::debug!(
                                        "View cell factory succeeded client={} tx={} view_id={}",
                                        session.client_id,
                                        tx_id,
                                        view_id
                                    );
                                    session.subscribe_view(
                                        tx_id,
                                        filtered_cellmap,
                                        wrapped.window.clone(),
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create view cell for {}: {}",
                                        view_id,
                                        e
                                    );
                                    if let Err(err) =
                                        tx.try_send(MykoMessage::ViewError(ViewError {
                                            tx: tx_id.to_string(),
                                            view_id: view_id.to_string(),
                                            message: e,
                                        }))
                                    {
                                        drop_logger.on_drop("ViewError", &err);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let message = format!("Failed to parse view {}: {}", view_id, e);
                            log::error!(
                                "{} | payload: {}",
                                message,
                                serde_json::to_string(&wrapped.view).unwrap_or_default()
                            );
                            if let Err(err) = tx.try_send(MykoMessage::ViewError(ViewError {
                                tx: tx_id.to_string(),
                                view_id: view_id.to_string(),
                                message,
                            })) {
                                drop_logger.on_drop("ViewError", &err);
                            }
                        }
                    }
                } else {
                    let message = format!("No registered handler for view: {}", view_id);
                    log::warn!("{}", message);
                    if let Err(err) = tx.try_send(MykoMessage::ViewError(ViewError {
                        tx: tx_id.to_string(),
                        view_id: view_id.to_string(),
                        message,
                    })) {
                        drop_logger.on_drop("ViewError", &err);
                    }
                }
            }

            MykoMessage::QueryCancel(CancelSubscription { tx: tx_id }) => {
                log::trace!("Query cancel: {}", tx_id);
                session.cancel(&tx_id.into());
            }

            MykoMessage::QueryWindow(QueryWindowUpdate { tx, window }) => {
                let tx_id: Arc<str> = tx.into();
                log::trace!("Query window update: {}", tx_id);
                log::debug!(
                    "Query window request client={} tx={} has_window={} active_subscriptions={}",
                    session.client_id,
                    tx_id,
                    window.is_some(),
                    session.subscription_count()
                );
                session.update_query_window(&tx_id, window);
            }
            MykoMessage::ViewCancel(CancelSubscription { tx: tx_id }) => {
                log::trace!("View cancel: {}", tx_id);
                session.cancel(&tx_id.into());
            }
            MykoMessage::ViewWindow(ViewWindowUpdate { tx, window }) => {
                let tx_id: Arc<str> = tx.into();
                log::trace!("View window update: {}", tx_id);
                session.update_view_window(&tx_id, window);
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

                log::trace!("Report {} (tx: {})", report_id, tx_id);
                log::debug!(
                    "Report subscribe request client={} tx={} report_id={} active_subscriptions_before={}",
                    session.client_id,
                    tx_id,
                    report_id,
                    session.subscription_count()
                );

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
                log::trace!("Report cancel: {}", tx_id);
                session.cancel(&tx_id.into());
            }

            MykoMessage::Event(mut event) => {
                normalize_incoming_event(&mut event, &session.client_id);
                let _ = ctx.apply_event(event);
            }

            MykoMessage::EventBatch(mut events) => {
                let incoming = events.len();
                if incoming >= 64 {
                    log::info!(
                        "Received event batch from client {} size={}",
                        session.client_id,
                        incoming
                    );
                }
                for event in &mut events {
                    normalize_incoming_event(event, &session.client_id);
                }
                let applied = ctx.apply_event_batch(events);
                if applied >= 64 {
                    log::info!(
                        "Applied event batch from client {} size={}",
                        session.client_id,
                        applied
                    );
                } else {
                    log::debug!(
                        "Applied event batch from client {} size={}",
                        session.client_id,
                        applied
                    );
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
                                    drop_logger.on_drop("CommandResponse", &e);
                                }
                            }
                            Err(e) => {
                                let error = MykoMessage::CommandError(CommandError {
                                    tx: tx_id.to_string(),
                                    command_id: command_id.to_string(),
                                    message: e.message,
                                });
                                if let Err(e) = tx.try_send(error) {
                                    drop_logger.on_drop("CommandError", &e);
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
                        drop_logger.on_drop("CommandError", &e);
                    }
                }
            }

            MykoMessage::Ping(PingData { id, timestamp }) => {
                // Echo back the ping data
                let pong = MykoMessage::Ping(PingData { id, timestamp });
                if let Err(e) = tx.try_send(pong) {
                    drop_logger.on_drop("Ping", &e);
                }
            }

            // Response messages - these shouldn't come from clients
            MykoMessage::QueryResponse(_)
            | MykoMessage::QueryError(_)
            | MykoMessage::ViewResponse(_)
            | MykoMessage::ViewError(_)
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
    drop_logger: Arc<DropLogger>,
}

impl WsWriter for ChannelWriter {
    fn send(&self, msg: MykoMessage) {
        // Use try_send since we're in sync context
        if let Err(e) = self.tx.try_send(msg) {
            self.drop_logger.on_drop("message", &e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_writer() {
        let (tx, mut rx) = mpsc::channel(10);
        let drop_logger = Arc::new(DropLogger::new("test-client".into()));
        let writer = ChannelWriter { tx, drop_logger };

        let msg = MykoMessage::Ping(PingData {
            id: "test".to_string(),
            timestamp: 0,
        });
        writer.send(msg);

        let received = rx.try_recv().unwrap();
        assert!(matches!(received, MykoMessage::Ping(_)));
    }
}
