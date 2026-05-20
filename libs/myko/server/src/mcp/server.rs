//! MCP server implementation.

use std::{
    collections::HashSet,
    io::{self, BufRead, Write},
    sync::Arc,
};

use hyphae::{Gettable, Watchable};
use myko::{
    client::{ConnectionStatus, MykoClient},
    command::CommandRegistration,
    query::QueryRegistration,
    report::ReportRegistration,
    wire::{WrappedCommand, WrappedQuery, WrappedReport},
};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::types::*;

/// Predicate on tool names. Names use prefixes `query:`, `report:`,
/// `command:`, plus the built-in `connection_status`.
pub type ToolFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Argument-aware hook for `tools/call`. Runs after the name [`ToolFilter`]
/// passes; sees the tool name and the JSON `arguments`. `Err(msg)` surfaces
/// to the caller as `isError: true` content with `msg` as the text — the
/// MCP-spec shape for "invalid input data" (distinct from the protocol
/// error `-32602` used for unknown tools).
pub type ToolCallFilter = Arc<dyn Fn(&str, &Value) -> Result<(), String> + Send + Sync>;

#[inline]
fn filter_allows(filter: Option<&ToolFilter>, name: &str) -> bool {
    filter.map(|f| f(name)).unwrap_or(true)
}

/// MCP Server for Myko.
///
/// Auto-exposes registered queries, reports, and commands over MCP.
/// Install a [`ToolFilter`] via [`McpServer::with_tool_filter`] to
/// restrict which tool names are exposed and callable. Install a
/// [`ToolCallFilter`] via [`McpServer::with_tool_call_filter`] to gate
/// `tools/call` on the JSON arguments (e.g. allowlist of
/// `playbook_id` values), with the rejection message flowing through
/// as `isError: true` content per the MCP spec.
pub struct McpServer {
    server_name: String,
    server_version: String,
    tool_filter: Option<ToolFilter>,
    tool_call_filter: Option<ToolCallFilter>,
}

impl Default for McpServer {
    fn default() -> Self {
        Self::new()
    }
}

impl McpServer {
    /// Create a new MCP server with default settings.
    pub fn new() -> Self {
        Self {
            server_name: "myko-mcp".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            tool_filter: None,
            tool_call_filter: None,
        }
    }

    /// Create a new MCP server with custom name and version.
    pub fn with_info(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            server_name: name.into(),
            server_version: version.into(),
            tool_filter: None,
            tool_call_filter: None,
        }
    }

    /// Install a filter that decides which tool names are exposed and
    /// callable. Replaces any previous filter.
    pub fn with_tool_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.tool_filter = Some(Arc::new(filter));
        self
    }

    /// Install an argument-aware hook on `tools/call` dispatch. Runs after
    /// the name [`with_tool_filter`](Self::with_tool_filter) passes; sees
    /// the tool name and the JSON `arguments`. `Err(msg)` surfaces as
    /// `isError: true` content. Replaces any previous call filter.
    pub fn with_tool_call_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str, &Value) -> Result<(), String> + Send + Sync + 'static,
    {
        self.tool_call_filter = Some(Arc::new(filter));
        self
    }

    /// Install an explicit allowlist of tool names. Equivalent to
    /// `with_tool_filter` over a `HashSet` lookup. An empty iterator
    /// denies everything.
    pub fn with_allowed_tool_names<I, S>(self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = names.into_iter().map(Into::into).collect();
        self.with_tool_filter(move |name| set.contains(name))
    }

    /// True if `name` passes the filter (or no filter is set).
    pub fn is_tool_allowed(&self, name: &str) -> bool {
        filter_allows(self.tool_filter.as_ref(), name)
    }

    /// Names that would appear in `tools/list` after the filter runs.
    pub fn exposed_tool_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if self.is_tool_allowed("connection_status") {
            names.push("connection_status".to_string());
        }
        for reg in inventory::iter::<QueryRegistration> {
            let n = format!("query:{}", reg.query_id);
            if self.is_tool_allowed(&n) {
                names.push(n);
            }
        }
        for reg in inventory::iter::<ReportRegistration> {
            let n = format!("report:{}", reg.report_id);
            if self.is_tool_allowed(&n) {
                names.push(n);
            }
        }
        for reg in inventory::iter::<CommandRegistration> {
            let n = format!("command:{}", reg.command_id);
            if self.is_tool_allowed(&n) {
                names.push(n);
            }
        }
        names
    }

    /// Run the MCP server over stdio (blocking).
    ///
    /// This reads JSON-RPC requests from stdin and writes responses to stdout.
    /// Logs and errors are written to stderr.
    ///
    /// Connects to a Myko WebSocket server (via MYKO_ADDRESS env var) to execute
    /// queries, reports, and commands.
    pub fn run_stdio(&self) -> io::Result<()> {
        // Build a tokio runtime for async operations
        let rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async { self.run_stdio_async().await })
    }

    async fn run_stdio_async(&self) -> io::Result<()> {
        let myko_address =
            std::env::var("MYKO_ADDRESS").unwrap_or_else(|_| "ws://localhost:5155".to_string());

        eprintln!("[myko-mcp] Connecting to Myko at {}", myko_address);

        if self.tool_filter.is_some() {
            let exposed = self.exposed_tool_names();
            eprintln!(
                "[myko-mcp] tool filter installed ({} exposed)",
                exposed.len()
            );
        }

        // Create client and connect
        let client = Arc::new(MykoClient::new());
        client.set_address(Some(myko_address));

        // Watch connection status
        let status_guard = client.connection_status().subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(addr) => {
                        eprintln!("[myko-mcp] Connected to {}", addr);
                    }
                    ConnectionStatus::Connecting(addr) => {
                        eprintln!("[myko-mcp] Connecting to {}", addr);
                    }
                    ConnectionStatus::Reconnecting(addr) => {
                        eprintln!("[myko-mcp] Reconnecting to {}", addr);
                    }
                    ConnectionStatus::Idle => {
                        eprintln!("[myko-mcp] Idle");
                    }
                    ConnectionStatus::Disconnected => {
                        eprintln!("[myko-mcp] Disconnected");
                    }
                }
            }
        });
        client.connection_status().own(status_guard);

        // Channels for async tool execution
        let (tool_tx, tool_rx) = mpsc::channel::<ToolRequest>(32);
        let (response_tx, mut response_rx) = mpsc::channel::<McpResponse>(32);

        // Start tool executor.
        let executor_client = client.clone();
        let executor_filter = self.tool_filter.clone();
        let executor_call_filter = self.tool_call_filter.clone();
        tokio::spawn(async move {
            tool_executor(
                executor_client,
                executor_filter,
                executor_call_filter,
                tool_rx,
            )
            .await;
        });

        // Create request handler
        let handler = RequestHandler {
            server_name: self.server_name.clone(),
            server_version: self.server_version.clone(),
            tool_tx,
            tool_filter: self.tool_filter.clone(),
        };

        // Spawn stdin reader
        let response_tx_clone = response_tx.clone();
        std::thread::spawn(move || {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("[myko-mcp] stdin error: {}", e);
                        continue;
                    }
                };

                if line.is_empty() {
                    continue;
                }

                let request: McpRequest = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("[myko-mcp] Parse error: {}", e);
                        let response =
                            McpResponse::error(Value::Null, McpError::parse_error(e.to_string()));
                        let _ = response_tx_clone.blocking_send(response);
                        continue;
                    }
                };

                handler.handle_request(request, response_tx_clone.clone());
            }
        });

        // Write responses to stdout
        let mut stdout = io::stdout().lock();
        while let Some(response) = response_rx.recv().await {
            let json = serde_json::to_string(&response)?;
            writeln!(stdout, "{}", json)?;
            stdout.flush()?;
        }

        Ok(())
    }

    /// Get a summary of all registered items.
    pub fn summary(&self) -> McpSummary {
        let mut queries = Vec::new();
        let mut reports = Vec::new();
        let mut commands = Vec::new();

        for reg in inventory::iter::<QueryRegistration> {
            queries.push(QueryInfo {
                query_id: reg.query_id.to_string(),
                query_item_type: reg.query_item_type.to_string(),
            });
        }

        for reg in inventory::iter::<ReportRegistration> {
            reports.push(ReportInfo {
                report_id: reg.report_id.to_string(),
                output_type: reg.output_type.to_string(),
            });
        }

        for reg in inventory::iter::<CommandRegistration> {
            commands.push(CommandInfo {
                command_id: reg.command_id.to_string(),
                result_type: reg.result_type.to_string(),
            });
        }

        McpSummary {
            queries,
            reports,
            commands,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Request Handler
// ─────────────────────────────────────────────────────────────────────────────

struct ToolRequest {
    id: Value,
    tool_name: String,
    arguments: Value,
    response_tx: mpsc::Sender<McpResponse>,
}

struct RequestHandler {
    server_name: String,
    server_version: String,
    tool_tx: mpsc::Sender<ToolRequest>,
    tool_filter: Option<ToolFilter>,
}

impl RequestHandler {
    fn tool_allowed(&self, name: &str) -> bool {
        filter_allows(self.tool_filter.as_ref(), name)
    }

    fn handle_request(&self, request: McpRequest, response_tx: mpsc::Sender<McpResponse>) {
        match request.method.as_str() {
            "initialize" => {
                let _ = response_tx.blocking_send(self.handle_initialize(request.id));
            }
            "notifications/initialized" | "notifications/cancelled" => {
                let _ = response_tx.blocking_send(McpResponse::success(request.id, Value::Null));
            }
            "tools/list" => {
                let _ = response_tx.blocking_send(self.handle_tools_list(request.id));
            }
            "tools/call" => {
                self.handle_tools_call(request.id, request.params, response_tx);
            }
            "resources/list" => {
                let _ = response_tx.blocking_send(self.handle_resources_list(request.id));
            }
            "resources/read" => {
                let _ = response_tx
                    .blocking_send(self.handle_resources_read(request.id, request.params));
            }
            _ => {
                let _ = response_tx.blocking_send(McpResponse::error(
                    request.id,
                    McpError::method_not_found(&request.method),
                ));
            }
        }
    }

    fn handle_initialize(&self, id: Value) -> McpResponse {
        McpResponse::success(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "resources": {}
                },
                "serverInfo": {
                    "name": self.server_name,
                    "version": self.server_version
                }
            }),
        )
    }

    fn handle_tools_list(&self, id: Value) -> McpResponse {
        let mut tools = Vec::new();

        // Built-in connection_status tool
        if self.tool_allowed("connection_status") {
            tools.push(McpTool {
                name: "connection_status".to_string(),
                description: "Check the connection status to the Myko server".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {},
                    "required": []
                }),
            });
        }

        // Queries as tools
        for reg in inventory::iter::<QueryRegistration> {
            let name = format!("query:{}", reg.query_id);
            if !self.tool_allowed(&name) {
                continue;
            }
            tools.push(McpTool {
                name,
                description: format!("Query returning {} entities", reg.query_item_type),
                input_schema: json!({
                    "type": "object",
                    "additionalProperties": true
                }),
            });
        }

        // Reports as tools
        for reg in inventory::iter::<ReportRegistration> {
            let name = format!("report:{}", reg.report_id);
            if !self.tool_allowed(&name) {
                continue;
            }
            tools.push(McpTool {
                name,
                description: format!("Report returning {}", reg.output_type),
                input_schema: json!({
                    "type": "object",
                    "additionalProperties": true
                }),
            });
        }

        // Commands as tools
        for reg in inventory::iter::<CommandRegistration> {
            let name = format!("command:{}", reg.command_id);
            if !self.tool_allowed(&name) {
                continue;
            }
            tools.push(McpTool {
                name,
                description: format!("Command returning {}", reg.result_type),
                input_schema: json!({
                    "type": "object",
                    "additionalProperties": true
                }),
            });
        }

        McpResponse::success(id, json!({ "tools": tools }))
    }

    fn handle_tools_call(
        &self,
        id: Value,
        params: Option<Value>,
        response_tx: mpsc::Sender<McpResponse>,
    ) {
        let params = match params {
            Some(p) => p,
            None => {
                let _ = response_tx.blocking_send(McpResponse::error(
                    id,
                    McpError::invalid_params("Missing params"),
                ));
                return;
            }
        };

        let tool_name = match params.get("name").and_then(|v| v.as_str()) {
            Some(n) => n.to_string(),
            None => {
                let _ = response_tx.blocking_send(McpResponse::error(
                    id,
                    McpError::invalid_params("Missing tool name"),
                ));
                return;
            }
        };

        // Filter check lives in execute_tool so the response matches the
        // unknown-tool path.
        let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

        let _ = self.tool_tx.blocking_send(ToolRequest {
            id,
            tool_name,
            arguments,
            response_tx,
        });
    }

    fn handle_resources_list(&self, id: Value) -> McpResponse {
        let mut resources = Vec::new();

        for reg in inventory::iter::<QueryRegistration> {
            let tool_name = format!("query:{}", reg.query_id);
            if !self.tool_allowed(&tool_name) {
                continue;
            }
            resources.push(McpResource {
                uri: format!("myko://schema/query/{}", reg.query_id),
                name: reg.query_id.to_string(),
                description: Some(format!("Query returning {} entities", reg.query_item_type)),
                mime_type: Some("application/json".to_string()),
            });
        }

        for reg in inventory::iter::<ReportRegistration> {
            let tool_name = format!("report:{}", reg.report_id);
            if !self.tool_allowed(&tool_name) {
                continue;
            }
            resources.push(McpResource {
                uri: format!("myko://schema/report/{}", reg.report_id),
                name: reg.report_id.to_string(),
                description: Some(format!("Report returning {}", reg.output_type)),
                mime_type: Some("application/json".to_string()),
            });
        }

        for reg in inventory::iter::<CommandRegistration> {
            let tool_name = format!("command:{}", reg.command_id);
            if !self.tool_allowed(&tool_name) {
                continue;
            }
            resources.push(McpResource {
                uri: format!("myko://schema/command/{}", reg.command_id),
                name: format!("{} (command)", reg.command_id),
                description: Some(format!("Command returning {}", reg.result_type)),
                mime_type: Some("application/json".to_string()),
            });
        }

        McpResponse::success(id, json!({ "resources": resources }))
    }

    fn handle_resources_read(&self, id: Value, params: Option<Value>) -> McpResponse {
        let params = match params {
            Some(p) => p,
            None => {
                return McpResponse::error(id, McpError::invalid_params("Missing params"));
            }
        };

        let uri = match params.get("uri").and_then(|v| v.as_str()) {
            Some(u) => u,
            None => {
                return McpResponse::error(id, McpError::invalid_params("Missing uri"));
            }
        };

        if let Some(path) = uri.strip_prefix("myko://schema/") {
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            if parts.len() == 2 {
                let (schema_type, schema_id) = (parts[0], parts[1]);
                let tool_name = format!("{}:{}", schema_type, schema_id);

                // Fall through to "Resource not found" if filtered, so
                // filtered URIs look unknown.
                if self.tool_allowed(&tool_name) {
                    let content = match schema_type {
                        "query" => get_query_schema(schema_id),
                        "report" => get_report_schema(schema_id),
                        "command" => get_command_schema(schema_id),
                        _ => None,
                    };

                    if let Some(content) = content {
                        return McpResponse::success(
                            id,
                            json!({
                                "contents": [{
                                    "uri": uri,
                                    "mimeType": "application/json",
                                    "text": content
                                }]
                            }),
                        );
                    }
                }
            }
        }

        McpResponse::error(
            id,
            McpError {
                code: McpError::INVALID_PARAMS,
                message: format!("Resource not found: {}", uri),
                data: None,
            },
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tool Execution
// ─────────────────────────────────────────────────────────────────────────────

async fn tool_executor(
    client: Arc<MykoClient>,
    filter: Option<ToolFilter>,
    call_filter: Option<ToolCallFilter>,
    mut rx: mpsc::Receiver<ToolRequest>,
) {
    while let Some(request) = rx.recv().await {
        let client = client.clone();
        let result = execute_tool(
            client,
            &request.tool_name,
            request.arguments,
            filter.as_ref(),
            call_filter.as_ref(),
        )
        .await;

        let response = match result {
            Ok(data) => McpResponse::success(
                request.id,
                json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&data).unwrap_or_default()
                    }]
                }),
            ),
            Err(e) => McpResponse::success(
                request.id,
                json!({
                    "content": [{
                        "type": "text",
                        "text": format!("Error: {}", e)
                    }],
                    "isError": true
                }),
            ),
        };

        let _ = request.response_tx.send(response).await;
    }
}

async fn execute_tool(
    client: Arc<MykoClient>,
    tool_name: &str,
    arguments: Value,
    filter: Option<&ToolFilter>,
    call_filter: Option<&ToolCallFilter>,
) -> Result<Value, String> {
    // Match the unknown-tool Err below so tool_executor wraps both
    // cases the same way.
    if !filter_allows(filter, tool_name) {
        return Err(format!("Unknown tool: {}", tool_name));
    }

    // Argument-aware gate. Per the MCP spec, invalid input data should
    // be surfaced via `isError: true` content (distinct from the
    // protocol-level "Unknown tool" error). Hook returns Err(msg);
    // we propagate it as our Err, tool_executor wraps as isError.
    if let Some(hook) = call_filter {
        hook(tool_name, &arguments)?;
    }

    if tool_name == "connection_status" {
        let status = client.connection_status().get();
        return Ok(json!({
            "status": match &status {
                ConnectionStatus::Connected(addr) => format!("Connected to {}", addr),
                ConnectionStatus::Connecting(addr) => format!("Connecting to {}", addr),
                ConnectionStatus::Reconnecting(addr) => format!("Reconnecting to {}", addr),
                ConnectionStatus::Idle => "Idle".to_string(),
                ConnectionStatus::Disconnected => "Disconnected".to_string(),
            }
        }));
    }

    if let Some(query_id) = tool_name.strip_prefix("query:") {
        return execute_query(client, query_id, arguments).await;
    }

    if let Some(report_id) = tool_name.strip_prefix("report:") {
        return execute_report(client, report_id, arguments).await;
    }

    if let Some(command_id) = tool_name.strip_prefix("command:") {
        return execute_command(client, command_id, arguments).await;
    }

    Err(format!("Unknown tool: {}", tool_name))
}

async fn execute_query(
    client: Arc<MykoClient>,
    query_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    for reg in inventory::iter::<QueryRegistration> {
        if reg.query_id == query_id {
            let tx = Uuid::new_v4().to_string();
            let mut query_json = if arguments.is_object() {
                arguments.clone()
            } else {
                json!({})
            };

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
            let result_tx = Arc::new(std::sync::Mutex::new(Some(result_tx)));
            let seen_initial = Arc::new(std::sync::Mutex::new(false));
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

            return match tokio::time::timeout(std::time::Duration::from_secs(5), result_rx).await {
                Ok(Ok(items)) => Ok(json!({
                    "query_id": query_id,
                    "item_type": reg.query_item_type,
                    "count": items.len(),
                    "items": items
                })),
                Ok(Err(_)) => Err("Query channel closed".to_string()),
                Err(_) => Err("Timeout waiting for query response".to_string()),
            };
        }
    }

    Err(format!("Query not found: {}", query_id))
}

async fn execute_report(
    client: Arc<MykoClient>,
    report_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    for reg in inventory::iter::<ReportRegistration> {
        if reg.report_id == report_id {
            let tx = Uuid::new_v4().to_string();
            let mut report_json = if arguments.is_object() {
                arguments.clone()
            } else {
                json!({})
            };

            if let Some(obj) = report_json.as_object_mut() {
                obj.insert("tx".to_string(), json!(tx));
            }

            let wrapped = WrappedReport {
                report: report_json,
                report_id: reg.report_id.to_string(),
            };

            let cell = client.watch_report_raw(wrapped);
            let (result_tx, result_rx) = oneshot::channel::<Value>();
            let result_tx = Arc::new(std::sync::Mutex::new(Some(result_tx)));
            let _guard = cell.subscribe(move |signal| {
                if let hyphae::Signal::Value(value_opt) = signal
                    && let Some(value) = &**value_opt
                    && let Some(tx) = result_tx.lock().unwrap().take()
                {
                    let _ = tx.send(value.clone());
                }
            });

            return match tokio::time::timeout(std::time::Duration::from_secs(5), result_rx).await {
                Ok(Ok(value)) => Ok(json!({
                    "report_id": report_id,
                    "output_type": reg.output_type,
                    "result": value
                })),
                Ok(Err(_)) => Err("Report channel closed".to_string()),
                Err(_) => Err("Timeout waiting for report response".to_string()),
            };
        }
    }

    Err(format!("Report not found: {}", report_id))
}

async fn execute_command(
    client: Arc<MykoClient>,
    command_id: &str,
    arguments: Value,
) -> Result<Value, String> {
    let status = client.connection_status().get();
    if !matches!(status, ConnectionStatus::Connected(_)) {
        // Wait for connection with timeout
        let (tx_connected, rx_connected) = tokio::sync::oneshot::channel::<bool>();
        let tx_connected = std::sync::Mutex::new(Some(tx_connected));
        let guard = client.connection_status().subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
                && let Some(sender) = tx_connected.lock().unwrap().take()
            {
                let _ = sender.send(true);
            }
        });

        let connected = tokio::time::timeout(std::time::Duration::from_secs(5), rx_connected)
            .await
            .unwrap_or(Ok(false))
            .unwrap_or(false);

        drop(guard);

        if !connected {
            return Err("Not connected to Myko server".to_string());
        }
    }

    let tx = Uuid::new_v4().to_string();
    let mut command_json = if arguments.is_object() {
        arguments.clone()
    } else {
        json!({})
    };

    if let Some(obj) = command_json.as_object_mut() {
        obj.insert("tx".to_string(), json!(tx.clone()));
    }

    let wrapped = WrappedCommand {
        command: command_json,
        command_id: command_id.to_string(),
    };

    let result_cell = client.send_command_raw_result(wrapped);
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<Result<Value, String>>();
    let resp_tx = Arc::new(std::sync::Mutex::new(Some(resp_tx)));
    let _guard = result_cell.subscribe(move |signal| {
        if let hyphae::Signal::Value(result_opt) = signal
            && let Some(result) = &**result_opt
            && let Some(sender) = resp_tx.lock().unwrap().take()
        {
            let _ = sender.send(result.clone());
        }
    });

    match tokio::time::timeout(std::time::Duration::from_secs(10), resp_rx).await {
        Ok(Ok(Ok(response))) => Ok(json!({
            "command_id": command_id,
            "success": true,
            "result": response
        })),
        Ok(Ok(Err(e))) => Err(e),
        _ => Err("Timeout waiting for response".to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn get_query_schema(query_id: &str) -> Option<String> {
    for reg in inventory::iter::<QueryRegistration> {
        if reg.query_id == query_id {
            let schema = json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": reg.query_id,
                "description": format!("Query returning {} entities", reg.query_item_type),
                "type": "object",
                "additionalProperties": true
            });
            return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
        }
    }
    None
}

fn get_report_schema(report_id: &str) -> Option<String> {
    for reg in inventory::iter::<ReportRegistration> {
        if reg.report_id == report_id {
            let schema = json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": reg.report_id,
                "description": format!("Report returning {}", reg.output_type),
                "type": "object",
                "additionalProperties": true
            });
            return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
        }
    }
    None
}

fn get_command_schema(command_id: &str) -> Option<String> {
    for reg in inventory::iter::<CommandRegistration> {
        if reg.command_id == command_id {
            let schema = json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": reg.command_id,
                "description": format!("Command returning {}", reg.result_type),
                "type": "object",
                "additionalProperties": true
            });
            return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Summary Types
// ─────────────────────────────────────────────────────────────────────────────

/// Summary of registered Myko items.
#[derive(Debug, Clone)]
pub struct McpSummary {
    pub queries: Vec<QueryInfo>,
    pub reports: Vec<ReportInfo>,
    pub commands: Vec<CommandInfo>,
}

/// Query registration info.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub query_id: String,
    pub query_item_type: String,
}

/// Report registration info.
#[derive(Debug, Clone)]
pub struct ReportInfo {
    pub report_id: String,
    pub output_type: String,
}

/// Command registration info.
#[derive(Debug, Clone)]
pub struct CommandInfo {
    pub command_id: String,
    pub result_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filter_allows_everything() {
        let server = McpServer::with_info("test", "0.0.0");
        assert!(server.is_tool_allowed("command:RunPlaybook"));
        assert!(server.is_tool_allowed("command:DeleteEverything"));
        assert!(server.is_tool_allowed("query:GetAllFoos"));
        assert!(server.is_tool_allowed("connection_status"));
    }

    #[test]
    fn closure_filter_gates_by_predicate() {
        let server = McpServer::with_info("test", "0.0.0")
            .with_tool_filter(|name| !name.starts_with("command:Delete"));

        assert!(server.is_tool_allowed("command:RunPlaybook"));
        assert!(server.is_tool_allowed("query:GetAllFoos"));
        assert!(!server.is_tool_allowed("command:DeleteFoo"));
        assert!(!server.is_tool_allowed("command:DeleteFoos"));
    }

    #[test]
    fn allowlist_via_hashset_closure() {
        let allowed: HashSet<String> = ["query:GetAllRuns", "command:RunPlaybook"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let server = McpServer::with_info("test", "0.0.0")
            .with_tool_filter(move |name| allowed.contains(name));

        assert!(server.is_tool_allowed("query:GetAllRuns"));
        assert!(server.is_tool_allowed("command:RunPlaybook"));
        assert!(!server.is_tool_allowed("command:CancelRun"));
        assert!(!server.is_tool_allowed("connection_status"));
    }

    #[test]
    fn with_allowed_tool_names_builds_allowlist() {
        let server = McpServer::with_info("test", "0.0.0")
            .with_allowed_tool_names(["connection_status", "query:GetAllFoos"]);

        assert!(server.is_tool_allowed("connection_status"));
        assert!(server.is_tool_allowed("query:GetAllFoos"));
        assert!(!server.is_tool_allowed("query:GetAllBars"));
        assert!(!server.is_tool_allowed("command:DeleteFoo"));
    }

    #[test]
    fn empty_allowlist_denies_everything() {
        let names: [&str; 0] = [];
        let server = McpServer::with_info("test", "0.0.0").with_allowed_tool_names(names);
        assert!(!server.is_tool_allowed("connection_status"));
        assert!(!server.is_tool_allowed("query:Anything"));
        assert!(!server.is_tool_allowed(""));
    }

    #[test]
    fn duplicate_names_dedupe_in_allowlist() {
        let server =
            McpServer::with_info("test", "0.0.0").with_allowed_tool_names(["foo", "foo", "bar"]);
        assert!(server.is_tool_allowed("foo"));
        assert!(server.is_tool_allowed("bar"));
        assert!(!server.is_tool_allowed("baz"));
    }

    #[test]
    fn with_tool_filter_replaces_previous() {
        let server = McpServer::with_info("test", "0.0.0")
            .with_tool_filter(|_| true)
            .with_tool_filter(|_| false);
        assert!(!server.is_tool_allowed("anything"));
    }

    #[test]
    fn exposed_tool_names_respects_filter() {
        let allowing = McpServer::with_info("test", "0.0.0");
        assert!(
            allowing
                .exposed_tool_names()
                .contains(&"connection_status".to_string())
        );

        let denying = McpServer::with_info("test", "0.0.0").with_tool_filter(|_| false);
        assert!(denying.exposed_tool_names().is_empty());

        let only_status =
            McpServer::with_info("test", "0.0.0").with_allowed_tool_names(["connection_status"]);
        assert_eq!(only_status.exposed_tool_names(), vec!["connection_status"]);
    }

    // ToolCallFilter tests exercise the hook contract directly. Going
    // through execute_tool would require a live MykoClient; the hook
    // semantics are simple enough that direct invocation is sufficient.

    #[test]
    fn tool_call_filter_ok_allows() {
        let hook: ToolCallFilter = Arc::new(|_, _| Ok(()));
        assert!(hook("command:RunPlaybook", &json!({"playbook_id": "x"})).is_ok());
    }

    #[test]
    fn tool_call_filter_err_carries_message() {
        let hook: ToolCallFilter = Arc::new(|name, args| {
            if name == "command:RunPlaybook" {
                let id = args
                    .get("playbook_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if id != "safe" {
                    return Err(format!("Playbook '{}' not in agent allowlist", id));
                }
            }
            Ok(())
        });
        assert_eq!(
            hook("command:RunPlaybook", &json!({"playbook_id": "site"})).unwrap_err(),
            "Playbook 'site' not in agent allowlist"
        );
        assert!(hook("command:RunPlaybook", &json!({"playbook_id": "safe"})).is_ok());
    }

    #[test]
    fn with_tool_call_filter_installs_hook() {
        let server = McpServer::with_info("test", "0.0.0").with_tool_call_filter(|_, _| Ok(()));
        assert!(server.tool_call_filter.is_some());
    }
}
