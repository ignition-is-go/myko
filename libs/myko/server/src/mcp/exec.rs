//! Tool executor abstraction for MCP transports.
//!
//! Two execution paths share the same dispatch core:
//!
//! - [`Executor::Client`] — wraps a [`MykoClient`]; talks to a remote Myko server
//!   over WebSocket. Used by the stdio MCP binary.
//! - [`Executor::InProcess`] — talks directly to a [`MykoServerContext`]; used by the
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
    server::MykoServerContext,
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

fn take_mutex<T>(value: &Mutex<Option<T>>) -> Option<T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

/// How MCP dispatch reaches the underlying Myko queries / reports / commands.
#[derive(Clone)]
pub enum Executor {
    /// Talk to a remote Myko server over WebSocket via a `MykoClient`.
    Client(Arc<MykoClient>),
    /// Talk to a server hosted in the same process via its `MykoServerContext`.
    InProcess(Arc<MykoServerContext>),
}

impl Executor {
    /// Execute a query and return its current items as JSON.
    /// # Errors
    ///
    /// Returns an error when the query cannot be executed.
    pub async fn execute_query(&self, query_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Self::Client(client) => client_execute_query(client.clone(), query_id, args).await,
            Self::InProcess(ctx) => in_process_execute_query(ctx.clone(), query_id, args),
        }
    }

    /// Execute a report and return its current output as JSON.
    /// # Errors
    ///
    /// Returns an error when the report cannot be executed.
    pub async fn execute_report(&self, report_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Self::Client(client) => client_execute_report(client.clone(), report_id, args).await,
            Self::InProcess(ctx) => in_process_execute_report(ctx.clone(), report_id, args),
        }
    }

    /// Execute a view (list-typed report) and return its current items as JSON.
    /// # Errors
    ///
    /// Returns an error when the view cannot be executed.
    pub async fn execute_view(&self, view_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Self::Client(client) => client_execute_view(client.clone(), view_id, args).await,
            Self::InProcess(ctx) => in_process_execute_view(ctx.clone(), view_id, args),
        }
    }

    /// Execute a command and return its result as JSON.
    /// # Errors
    ///
    /// Returns an error when the command cannot be executed.
    pub async fn execute_command(&self, command_id: &str, args: Value) -> Result<Value, String> {
        match self {
            Self::Client(client) => client_execute_command(client.clone(), command_id, args).await,
            Self::InProcess(ctx) => in_process_execute_command(ctx.clone(), command_id, args),
        }
    }

    /// Status string for the built-in `connection_status` tool. Includes
    /// server name/version (and, in-process, the host id) so a caller can
    /// confirm which instance they hit without a separate report call.
    #[must_use]
    pub fn connection_status(&self, info: &super::dispatch::ServerInfo) -> Value {
        match self {
            Self::Client(client) => {
                let status = client.connection_status().get();
                let text = match &status {
                    ConnectionStatus::Connected(addr) => format!("Connected to {addr}"),
                    ConnectionStatus::Connecting(addr) => format!("Connecting to {addr}"),
                    ConnectionStatus::Reconnecting(addr) => format!("Reconnecting to {addr}"),
                    ConnectionStatus::Idle => "Idle".to_string(),
                    ConnectionStatus::Disconnected => "Disconnected".to_string(),
                };
                json!({ "status": text, "name": info.name, "version": info.version })
            }
            Self::InProcess(ctx) => json!({
                "status": "In-process (always connected)",
                "name": info.name,
                "version": info.version,
                "hostId": ctx.host_id,
            }),
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
    let reg = unique_registration(
        inventory::iter::<QueryRegistration>
            .into_iter()
            .filter(|reg| reg.query_id == query_id),
        "Query",
        query_id,
    )?;
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
        service_id: reg.service_id.map(Into::into),
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
            let is_followup = {
                let mut seen = seen_initial_sub
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let was_seen = *seen;
                *seen = true;
                was_seen
            };
            if !is_followup {
                return;
            }
            if let Some(tx) = take_mutex(&result_tx_sub) {
                let _ = tx.send((**items).clone());
            }
        }
    });

    match tokio::time::timeout(QUERY_TIMEOUT, result_rx).await {
        Ok(Ok(items)) => Ok(json!({
            "query_id": query_id,
            "item_type": reg.query_item_type,
            "count": items.len(),
            "items": items,
        })),
        Ok(Err(_)) => Err("Query channel closed".to_string()),
        Err(_) => Err("Timeout waiting for query response".to_string()),
    }
}

async fn client_execute_view(
    client: Arc<MykoClient>,
    view_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let reg = unique_registration(
        inventory::iter::<ViewRegistration>
            .into_iter()
            .filter(|reg| reg.view_id == view_id),
        "View",
        view_id,
    )?;
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
        service_id: reg.service_id.map(Into::into),
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
            let is_followup = {
                let mut seen = seen_initial_sub
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let was_seen = *seen;
                *seen = true;
                was_seen
            };
            if !is_followup {
                return;
            }
            if let Some(tx) = take_mutex(&result_tx_sub) {
                let _ = tx.send((**items).clone());
            }
        }
    });

    match tokio::time::timeout(QUERY_TIMEOUT, result_rx).await {
        Ok(Ok(items)) => Ok(json!({
            "view_id": view_id,
            "item_type": reg.view_item_type,
            "count": items.len(),
            "items": items,
        })),
        Ok(Err(_)) => Err("View channel closed".to_string()),
        Err(_) => Err("Timeout waiting for view response".to_string()),
    }
}

async fn client_execute_report(
    client: Arc<MykoClient>,
    report_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let reg = unique_registration(
        inventory::iter::<ReportRegistration>
            .into_iter()
            .filter(|reg| reg.report_id == report_id),
        "Report",
        report_id,
    )?;
    let tx = Uuid::new_v4().to_string();
    let mut report_json = arguments_object(arguments);
    if let Some(obj) = report_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx));
    }

    let wrapped = WrappedReport {
        service_id: reg.service_id.map(Into::into),
        report: report_json,
        report_id: reg.report_id.to_string(),
    };

    let cell = client.watch_report_raw(wrapped);
    let (result_tx, result_rx) = oneshot::channel::<Value>();
    let result_tx = Arc::new(Mutex::new(Some(result_tx)));
    let _guard = cell.subscribe(move |signal| {
        if let hyphae::Signal::Value(value_opt) = signal
            && let Some(value) = &**value_opt
            && let Some(tx) = take_mutex(&result_tx)
        {
            let _ = tx.send(value.clone());
        }
    });

    match tokio::time::timeout(REPORT_TIMEOUT, result_rx).await {
        Ok(Ok(value)) => Ok(json!({
            "report_id": report_id,
            "output_type": reg.output_type,
            "result": value,
        })),
        Ok(Err(_)) => Err("Report channel closed".to_string()),
        Err(_) => Err("Timeout waiting for report response".to_string()),
    }
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
                && let Some(sender) = take_mutex(&tx_connected)
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
            && let Some(sender) = take_mutex(&resp_tx)
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
    ctx: Arc<MykoServerContext>,
    query_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let query_data = unique_registration(
        ctx.handler_registry
            .queries()
            .filter(|data| data.query_id.as_ref() == query_id),
        "Query",
        query_id,
    )?;
    let item_type = query_data.query_item_type.clone();

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
        .map_err(|e| format!("Failed to parse query {query_id}: {e}"))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let output = (query_data.cell_factory)(
        parsed,
        ctx.registry.clone(),
        request_context,
        Some(ctx.clone()),
        None,
    )
    .map_err(|e| format!("Failed to build query cell: {e}"))?;

    let items: Vec<Value> = output
        .read_current()
        .map_err(|e| format!("Failed to read current query {query_id}: {e}"))?
        .into_values()
        .map(|item| serde_json::to_value(&*item).unwrap_or(Value::Null))
        .collect();
    drop(ctx);

    Ok(json!({
        "query_id": query_id,
        "item_type": item_type,
        "count": items.len(),
        "items": items,
    }))
}

fn in_process_execute_view(
    ctx: Arc<MykoServerContext>,
    view_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let view_data = unique_registration(
        ctx.handler_registry
            .views()
            .filter(|data| data.view_id.as_ref() == view_id),
        "View",
        view_id,
    )?;
    let item_type = view_data.view_item_type.clone();

    let mut view_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = view_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
        obj.insert(
            "createdAt".to_string(),
            json!(chrono::Utc::now().to_rfc3339()),
        );
    }

    let parsed =
        (view_data.parse)(view_json).map_err(|e| format!("Failed to parse view {view_id}: {e}"))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let output = (view_data.cell_factory)(
        parsed,
        ctx.registry.clone(),
        request_context,
        ctx.clone(),
        None,
    )
    .map_err(|e| format!("Failed to build view cell: {e}"))?;

    let (items, through, liveness) = view_output_snapshot(output);
    drop(ctx);

    Ok(json!({
        "view_id": view_id,
        "item_type": item_type,
        "count": items.len(),
        "items": items,
        "through": through,
        "liveness": liveness,
    }))
}

fn view_output_snapshot(output: myko::view::RegisteredViewOutput) -> (Vec<Value>, Value, Value) {
    match output {
        myko::view::RegisteredViewOutput::LocalMap(cellmap) => {
            let items = cellmap
                .snapshot()
                .into_iter()
                .map(|(_, item)| serde_json::to_value(&*item).unwrap_or(Value::Null))
                .collect();
            (
                items,
                Value::Null,
                serde_json::to_value(myko_federation::SubscriptionLiveness::Current)
                    .unwrap_or(Value::Null),
            )
        }
        myko::view::RegisteredViewOutput::RetainedPublication(publication) => {
            let snapshot = publication.current();
            let items = snapshot
                .value
                .unwrap_or_default()
                .into_values()
                .map(|item| serde_json::to_value(&*item).unwrap_or(Value::Null))
                .collect();
            (
                items,
                serde_json::to_value(snapshot.through).unwrap_or(Value::Null),
                serde_json::to_value(&snapshot.liveness).unwrap_or(Value::Null),
            )
        }
    }
}

fn in_process_execute_report(
    ctx: Arc<MykoServerContext>,
    report_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let report_data = unique_registration(
        ctx.handler_registry
            .reports()
            .filter(|data| data.report_id.as_ref() == report_id),
        "Report",
        report_id,
    )?;
    let output_type = report_data.output_type;

    let mut report_json = arguments_object(arguments);
    let tx: Arc<str> = Uuid::new_v4().to_string().into();
    if let Some(obj) = report_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.as_ref()));
    }

    let parsed = (report_data.parse)(report_json)
        .map_err(|e| format!("Failed to parse report {report_id}: {e}"))?;

    let request_context = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));

    let cell = (report_data.cell_factory)(parsed, request_context, ctx, None)
        .map_err(|e| format!("Failed to build report cell: {e}"))?;

    let value = cell.read_current()?.to_value();
    Ok(json!({
        "report_id": report_id,
        "output_type": output_type,
        "result": value,
    }))
}

fn in_process_execute_command(
    ctx: Arc<MykoServerContext>,
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
            let req = Arc::new(RequestContext::internal(tx, ctx.host_id, "mcp"));
            let cmd_id: Arc<str> = Arc::from(command_id);
            let cmd_ctx = CommandContext::new(cmd_id, req, ctx);

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

    Err(format!("Command handler not found: {command_id}"))
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

fn unique_registration<T>(
    candidates: impl IntoIterator<Item = T>,
    kind: &str,
    id: &str,
) -> Result<T, String> {
    let mut candidates = candidates.into_iter();
    let registration = candidates
        .next()
        .ok_or_else(|| format!("{kind} not found: {id}"))?;
    if candidates.next().is_some() {
        return Err(format!("{kind} name is ambiguous across services: {id}"));
    }
    Ok(registration)
}

#[cfg(test)]
mod tests {
    use std::{any::Any, collections::BTreeMap, sync::Arc};

    use hyphae::CellMap;
    use myko::{common::with_id::WithId, item::AnyItem};

    use super::*;

    #[derive(Clone, Debug, PartialEq, serde::Serialize)]
    struct TestItem {
        id: Arc<str>,
        value: u32,
    }

    impl WithId for TestItem {
        fn id(&self) -> Arc<str> {
            Arc::clone(&self.id)
        }
    }

    impl AnyItem for TestItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "McpViewOutputTestItem"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other.as_any().downcast_ref::<Self>() == Some(self)
        }
    }

    fn item(id: &str, value: u32) -> Arc<dyn AnyItem> {
        Arc::new(TestItem {
            id: id.into(),
            value,
        })
    }

    #[test]
    fn mcp_local_view_output_uses_typed_current_liveness_schema() {
        let rows = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
        rows.insert("local".into(), item("local", 1));
        let output = myko::view::RegisteredViewOutput::LocalMap(rows.lock());

        let (items, through, liveness) = view_output_snapshot(output);

        assert_eq!(items, vec![json!({"id": "local", "value": 1})]);
        assert_eq!(through, Value::Null);
        assert_eq!(
            liveness,
            serde_json::to_value(myko_federation::SubscriptionLiveness::Current)
                .unwrap_or(Value::Null)
        );
    }

    #[test]
    fn mcp_retained_view_output_preserves_rows_cursor_and_liveness() {
        let retained_rows = BTreeMap::from([(Arc::from("retained"), item("retained", 7))]);
        let (_writer, publication) =
            myko_federation::live_subscription(myko_federation::LiveSubscriptionState {
                value: Some(retained_rows),
                through: Some(myko_federation::LogPosition::new(9)),
                liveness: myko_federation::SubscriptionLiveness::Resynchronizing {
                    reason: "waiting for parent".to_owned(),
                },
            });
        let output = myko::view::RegisteredViewOutput::RetainedPublication(publication);

        let (items, through, liveness) = view_output_snapshot(output);

        assert_eq!(items, vec![json!({"id": "retained", "value": 7})]);
        assert_eq!(through, json!(9));
        assert_eq!(
            liveness,
            json!({"resynchronizing": {"reason": "waiting for parent"}})
        );
    }

    #[test]
    fn unqualified_names_require_exactly_one_registration() {
        assert_eq!(unique_registration(["left"], "Query", "Rows"), Ok("left"));
        assert_eq!(
            unique_registration(["left", "right"], "Query", "Rows"),
            Err("Query name is ambiguous across services: Rows".to_owned()),
        );
        assert_eq!(
            unique_registration(Vec::<()>::new(), "Report", "Label"),
            Err("Report not found: Label".to_owned()),
        );
    }
}
