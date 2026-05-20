//! Tool executor abstraction for MCP transports.
//!
//! Two execution paths share the same dispatch core:
//!
//! - [`Executor::Client`] — wraps a [`MykoClient`]; talks to a remote Myko server
//!   over WebSocket. Used by the stdio MCP binary.
//! - [`Executor::InProcess`] — talks directly to a [`CellServerCtx`]; used by the
//!   HTTP/WS MCP endpoints hosted inside the server.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use hyphae::{Gettable, Watchable};
use myko::{
    client::{ConnectionStatus, MykoClient},
    command::{CommandContext, CommandHandlerRegistration},
    query::QueryRegistration,
    report::ReportRegistration,
    request::RequestContext,
    server::CellServerCtx,
    view::ViewRegistration,
    wire::{WrappedCommand, WrappedQuery, WrappedReport, WrappedView},
};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const REPORT_TIMEOUT: Duration = Duration::from_secs(5);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How MCP dispatch reaches the underlying Myko queries / reports / commands.
pub enum Executor {
    /// Talk to a remote Myko server over WebSocket via a `MykoClient`.
    Client(Arc<MykoClient>),
    /// Talk to a server hosted in the same process via its `CellServerCtx`.
    InProcess(Arc<CellServerCtx>),
}

impl Executor {
    /// Execute a query and return its current items as JSON.
    pub async fn execute_query(&self, query_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Executor::Client(client) => client_execute_query(client.clone(), query_id, args).await,
            Executor::InProcess(ctx) => in_process_execute_query(ctx.clone(), query_id, args),
        }
    }

    /// Execute a report and return its current output as JSON.
    pub async fn execute_report(&self, report_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Executor::Client(client) => {
                client_execute_report(client.clone(), report_id, args).await
            }
            Executor::InProcess(ctx) => in_process_execute_report(ctx.clone(), report_id, args).await,
        }
    }

    /// Execute a view (list-typed report) and return its current items as JSON.
    pub async fn execute_view(&self, view_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Executor::Client(client) => client_execute_view(client.clone(), view_id, args).await,
            Executor::InProcess(ctx) => in_process_execute_view(ctx.clone(), view_id, args),
        }
    }

    /// Execute a command and return its result as JSON.
    pub async fn execute_command(&self, command_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Executor::Client(client) => {
                client_execute_command(client.clone(), command_id, args).await
            }
            Executor::InProcess(ctx) => in_process_execute_command(ctx.clone(), command_id, args),
        }
    }

    /// Status string for the built-in `connection_status` tool.
    pub fn connection_status(&self) -> Value {
        match self {
            Executor::Client(client) => {
                let status = client.connection_status().get();
                let text = match &status {
                    ConnectionStatus::Connected(addr) => format!("Connected to {}", addr),
                    ConnectionStatus::Connecting(addr) => format!("Connecting to {}", addr),
                    ConnectionStatus::Reconnecting(addr) => format!("Reconnecting to {}", addr),
                    ConnectionStatus::Idle => "Idle".to_string(),
                    ConnectionStatus::Disconnected => "Disconnected".to_string(),
                };
                json!({ "status": text })
            }
            Executor::InProcess(_) => json!({ "status": "In-process (always connected)" }),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Client-mode execution (stdio MCP path)
// ─────────────────────────────────────────────────────────────────────────────

async fn client_execute_query(
    client: Arc<MykoClient>,
    query_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    for reg in inventory::iter::<QueryRegistration> {
        if reg.query_id == query_id {
            let tx = Uuid::new_v4().to_string();
            let mut query_json = arguments_object(arguments);
            if let Some(obj) = query_json.as_object_mut() {
                obj.insert("tx".to_string(), json!(tx));
                obj.insert(
                    "createdAt".to_string(),
                    json!(chrono::Utc::now().to_rfc3339()),
                );
            }

            let wrapped = WrappedQuery {
                query: query_json,
                query_id: reg.query_id.into(),
                query_item_type: reg.query_item_type.into(),
                window: None,
            };

            let cell = client.watch_query_raw(wrapped);
            let (result_tx, result_rx) = oneshot::channel::<Vec<Value>>();
            let result_tx = Arc::new(Mutex::new(Some(result_tx)));
            let seen_initial = Arc::new(Mutex::new(false));
            let result_tx_sub = result_tx.clone();
            let seen_initial_sub = seen_initial.clone();
            let _guard = cell.subscribe(move |signal| {
                if let hyphae::Signal::Value(items) = signal {
                    let mut seen = seen_initial_sub.lock().unwrap();
                    if !*seen {
                        *seen = true;
                        return;
                    }
                    if let Some(tx) = result_tx_sub.lock().unwrap().take() {
                        let _ = tx.send((**items).clone());
                    }
                }
            });

            return match tokio::time::timeout(QUERY_TIMEOUT, result_rx).await {
                Ok(Ok(items)) => Ok(json!({
                    "query_id": query_id,
                    "item_type": reg.query_item_type,
                    "count": items.len(),
                    "items": items,
                })),
                Ok(Err(_)) => Err("Query channel closed".to_string()),
                Err(_) => Err("Timeout waiting for query response".to_string()),
            };
        }
    }
    Err(format!("Query not found: {}", query_id))
}

async fn client_execute_view(
    client: Arc<MykoClient>,
    view_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    for reg in inventory::iter::<ViewRegistration> {
        if reg.view_id == view_id {
            let tx = Uuid::new_v4().to_string();
            let mut view_json = arguments_object(arguments);
            if let Some(obj) = view_json.as_object_mut() {
                obj.insert("tx".to_string(), json!(tx));
                obj.insert(
                    "createdAt".to_string(),
                    json!(chrono::Utc::now().to_rfc3339()),
                );
            }

            let wrapped = WrappedView {
                view: view_json,
                view_id: reg.view_id.into(),
                view_item_type: reg.view_item_type.into(),
                window: None,
            };

            let cell = client.watch_view_raw(wrapped);
            let (result_tx, result_rx) = oneshot::channel::<Vec<Value>>();
            let result_tx = Arc::new(Mutex::new(Some(result_tx)));
            let seen_initial = Arc::new(Mutex::new(false));
            let result_tx_sub = result_tx.clone();
            let seen_initial_sub = seen_initial.clone();
            let _guard = cell.subscribe(move |signal| {
                if let hyphae::Signal::Value(items) = signal {
                    let mut seen = seen_initial_sub.lock().unwrap();
                    if !*seen {
                        *seen = true;
                        return;
                    }
                    if let Some(tx) = result_tx_sub.lock().unwrap().take() {
                        let _ = tx.send((**items).clone());
                    }
                }
            });

            return match tokio::time::timeout(QUERY_TIMEOUT, result_rx).await {
                Ok(Ok(items)) => Ok(json!({
                    "view_id": view_id,
                    "item_type": reg.view_item_type,
                    "count": items.len(),
                    "items": items,
                })),
                Ok(Err(_)) => Err("View channel closed".to_string()),
                Err(_) => Err("Timeout waiting for view response".to_string()),
            };
        }
    }
    Err(format!("View not found: {}", view_id))
}

async fn client_execute_report(
    client: Arc<MykoClient>,
    report_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    for reg in inventory::iter::<ReportRegistration> {
        if reg.report_id == report_id {
            let tx = Uuid::new_v4().to_string();
            let mut report_json = arguments_object(arguments);
            if let Some(obj) = report_json.as_object_mut() {
                obj.insert("tx".to_string(), json!(tx));
            }

            let wrapped = WrappedReport {
                report: report_json,
                report_id: reg.report_id.to_string(),
            };

            let cell = client.watch_report_raw(wrapped);
            let (result_tx, result_rx) = oneshot::channel::<Value>();
            let result_tx = Arc::new(Mutex::new(Some(result_tx)));
            let _guard = cell.subscribe(move |signal| {
                if let hyphae::Signal::Value(value_opt) = signal
                    && let Some(value) = &**value_opt
                    && let Some(tx) = result_tx.lock().unwrap().take()
                {
                    let _ = tx.send(value.clone());
                }
            });

            return match tokio::time::timeout(REPORT_TIMEOUT, result_rx).await {
                Ok(Ok(value)) => Ok(json!({
                    "report_id": report_id,
                    "output_type": reg.output_type,
                    "result": value,
                })),
                Ok(Err(_)) => Err("Report channel closed".to_string()),
                Err(_) => Err("Timeout waiting for report response".to_string()),
            };
        }
    }
    Err(format!("Report not found: {}", report_id))
}

async fn client_execute_command(
    client: Arc<MykoClient>,
    command_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let status = client.connection_status().get();
    if !matches!(status, ConnectionStatus::Connected(_)) {
        let (tx_connected, rx_connected) = oneshot::channel::<bool>();
        let tx_connected = Mutex::new(Some(tx_connected));
        let guard = client.connection_status().subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
                && let Some(sender) = tx_connected.lock().unwrap().take()
            {
                let _ = sender.send(true);
            }
        });

        let connected = tokio::time::timeout(CONNECT_TIMEOUT, rx_connected)
            .await
            .unwrap_or(Ok(false))
            .unwrap_or(false);
        drop(guard);

        if !connected {
            return Err("Not connected to Myko server".to_string());
        }
    }

    let tx = Uuid::new_v4().to_string();
    let mut command_json = arguments_object(arguments);
    if let Some(obj) = command_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx));
    }

    let wrapped = WrappedCommand {
        command: command_json,
        command_id: command_id.to_string(),
    };

    let result_cell = client.send_command_raw_result(wrapped);
    let (resp_tx, resp_rx) = oneshot::channel::<Result<Value, String>>();
    let resp_tx = Arc::new(Mutex::new(Some(resp_tx)));
    let _guard = result_cell.subscribe(move |signal| {
        if let hyphae::Signal::Value(result_opt) = signal
            && let Some(result) = &**result_opt
            && let Some(sender) = resp_tx.lock().unwrap().take()
        {
            let _ = sender.send(result.clone());
        }
    });

    match tokio::time::timeout(COMMAND_TIMEOUT, resp_rx).await {
        Ok(Ok(Ok(response))) => Ok(json!({
            "command_id": command_id,
            "success": true,
            "result": response,
        })),
        Ok(Ok(Err(e))) => Err(e),
        _ => Err("Timeout waiting for response".to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// In-process execution (HTTP/WS MCP path)
// ─────────────────────────────────────────────────────────────────────────────

fn in_process_execute_query(
    ctx: Arc<CellServerCtx>,
    query_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let registration = inventory::iter::<QueryRegistration>
        .into_iter()
        .find(|r| r.query_id == query_id)
        .ok_or_else(|| format!("Query not found: {}", query_id))?;

    let query_data = ctx
        .handler_registry
        .get_query(query_id)
        .ok_or_else(|| format!("Query handler not registered: {}", query_id))?;

    let mut query_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = query_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
        obj.insert(
            "createdAt".to_string(),
            json!(chrono::Utc::now().to_rfc3339()),
        );
    }

    let parsed = (query_data.parse)(query_json)
        .map_err(|e| format!("Failed to parse query {}: {}", query_id, e))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let cellmap = (query_data.cell_factory)(
        parsed,
        ctx.registry.clone(),
        request_context,
        Some(ctx.clone()),
    )
    .map_err(|e| format!("Failed to build query cell: {}", e))?;

    let items: Vec<Value> = cellmap
        .snapshot()
        .into_iter()
        .map(|(_, item)| serde_json::to_value(&*item).unwrap_or(Value::Null))
        .collect();

    Ok(json!({
        "query_id": query_id,
        "item_type": registration.query_item_type,
        "count": items.len(),
        "items": items,
    }))
}

fn in_process_execute_view(
    ctx: Arc<CellServerCtx>,
    view_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let registration = inventory::iter::<ViewRegistration>
        .into_iter()
        .find(|r| r.view_id == view_id)
        .ok_or_else(|| format!("View not found: {}", view_id))?;

    let view_data = ctx
        .handler_registry
        .get_view(view_id)
        .ok_or_else(|| format!("View handler not registered: {}", view_id))?;

    let mut view_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = view_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
        obj.insert(
            "createdAt".to_string(),
            json!(chrono::Utc::now().to_rfc3339()),
        );
    }

    let parsed = (view_data.parse)(view_json)
        .map_err(|e| format!("Failed to parse view {}: {}", view_id, e))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let cellmap = (view_data.cell_factory)(
        parsed,
        ctx.registry.clone(),
        request_context,
        ctx.clone(),
    )
    .map_err(|e| format!("Failed to build view cell: {}", e))?;

    let items: Vec<Value> = cellmap
        .snapshot()
        .into_iter()
        .map(|(_, item)| serde_json::to_value(&*item).unwrap_or(Value::Null))
        .collect();

    Ok(json!({
        "view_id": view_id,
        "item_type": registration.view_item_type,
        "count": items.len(),
        "items": items,
    }))
}

async fn in_process_execute_report(
    ctx: Arc<CellServerCtx>,
    report_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let registration = inventory::iter::<ReportRegistration>
        .into_iter()
        .find(|r| r.report_id == report_id)
        .ok_or_else(|| format!("Report not found: {}", report_id))?;

    let report_data = ctx
        .handler_registry
        .get_report(report_id)
        .ok_or_else(|| format!("Report handler not registered: {}", report_id))?;

    let mut report_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = report_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
    }

    let parsed = (report_data.parse)(report_json)
        .map_err(|e| format!("Failed to parse report {}: {}", report_id, e))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let cell = (report_data.cell_factory)(parsed, request_context, ctx)
        .map_err(|e| format!("Failed to build report cell: {}", e))?;

    // Subscribe to drive reactive evaluation; capture the first emission.
    let (tx_resp, rx_resp) = oneshot::channel::<Value>();
    let tx_resp = Arc::new(Mutex::new(Some(tx_resp)));
    let tx_resp_sub = tx_resp.clone();
    let _guard = cell.subscribe(move |signal| {
        if let hyphae::Signal::Value(output) = signal
            && let Some(sender) = tx_resp_sub.lock().unwrap().take()
        {
            let _ = sender.send(output.to_value());
        }
    });

    match tokio::time::timeout(REPORT_TIMEOUT, rx_resp).await {
        Ok(Ok(value)) => Ok(json!({
            "report_id": report_id,
            "output_type": registration.output_type,
            "result": value,
        })),
        Ok(Err(_)) => Err("Report cell dropped before emitting".to_string()),
        Err(_) => Err("Timeout waiting for report value".to_string()),
    }
}

fn in_process_execute_command(
    ctx: Arc<CellServerCtx>,
    command_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let mut command_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = command_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
    }

    for registration in inventory::iter::<CommandHandlerRegistration> {
        if registration.command_id == command_id {
            let executor = (registration.factory)();
            let req = Arc::new(RequestContext::internal(tx.clone(), ctx.host_id, "mcp"));
            let cmd_id: Arc<str> = Arc::from(command_id);
            let cmd_ctx = CommandContext::new(cmd_id, req, ctx.clone());

            return match executor.execute_from_value(command_json, cmd_ctx) {
                Ok(result) => Ok(json!({
                    "command_id": command_id,
                    "success": true,
                    "result": result,
                })),
                Err(err) => Err(err.message),
            };
        }
    }

    Err(format!("Command handler not found: {}", command_id))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn arguments_object(arguments: Value) -> Value {
    if arguments.is_object() {
        arguments
    } else {
        json!({})
    }
}
