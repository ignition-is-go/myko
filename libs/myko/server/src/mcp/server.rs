//! MCP server implementation.

use std::{
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

/// Filter callback used to gate which tools and schema resources are exposed.
///
/// Invoked with the full tool name — e.g. `"query:Scene"`, `"report:RuntimeStats"`,
/// `"command:CreateBinding"`. Returning `false` hides the item from `tools/list` and
/// `resources/list`, and causes `tools/call` / `resources/read` to behave as if the
/// item does not exist.
///
/// Built-in tools (currently just `connection_status`) are not subject to the filter.
///
/// Available behind the `mcp-tool-filter` Cargo feature.
#[cfg(feature = "mcp-tool-filter")]
pub type ToolFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// MCP Server for Myko.
///
/// Automatically exposes all registered queries, reports, and commands
/// through the MCP protocol.
pub struct McpServer {
    server_name: String,
    server_version: String,
    #[cfg(feature = "mcp-tool-filter")]
    filter: Option<ToolFilter>,
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
            #[cfg(feature = "mcp-tool-filter")]
            filter: None,
        }
    }

    /// Create a new MCP server with custom name and version.
    pub fn with_info(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            server_name: name.into(),
            server_version: version.into(),
            #[cfg(feature = "mcp-tool-filter")]
            filter: None,
        }
    }

    /// Restrict which inventory-backed tools and schema resources are exposed.
    ///
    /// The provided closure is called with the full tool name (e.g. `"query:Scene"`,
    /// `"command:CreateBinding"`). Returning `false` hides the item from listings and
    /// makes call/read return a not-found error.
    ///
    /// Without a filter, all registered queries, reports, and commands are exposed.
    ///
    /// Available behind the `mcp-tool-filter` Cargo feature.
    #[cfg(feature = "mcp-tool-filter")]
    pub fn with_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.filter = Some(Arc::new(filter));
        self
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

        // Start tool executor
        let executor_client = client.clone();
        tokio::spawn(async move {
            tool_executor(executor_client, tool_rx).await;
        });

        // Create request handler
        let handler = RequestHandler {
            server_name: self.server_name.clone(),
            server_version: self.server_version.clone(),
            tool_tx,
            #[cfg(feature = "mcp-tool-filter")]
            filter: self.filter.clone(),
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

    /// Get a summary of registered items, after applying the configured filter.
    pub fn summary(&self) -> McpSummary {
        let mut queries = Vec::new();
        let mut reports = Vec::new();
        let mut commands = Vec::new();

        let allows = |_name: &str| -> bool {
            #[cfg(feature = "mcp-tool-filter")]
            if let Some(f) = &self.filter {
                return f(_name);
            }
            true
        };

        for reg in inventory::iter::<QueryRegistration> {
            if !allows(&format!("query:{}", reg.query_id)) {
                continue;
            }
            queries.push(QueryInfo {
                query_id: reg.query_id.to_string(),
                query_item_type: reg.query_item_type.to_string(),
            });
        }

        for reg in inventory::iter::<ReportRegistration> {
            if !allows(&format!("report:{}", reg.report_id)) {
                continue;
            }
            reports.push(ReportInfo {
                report_id: reg.report_id.to_string(),
                output_type: reg.output_type.to_string(),
            });
        }

        for reg in inventory::iter::<CommandRegistration> {
            if !allows(&format!("command:{}", reg.command_id)) {
                continue;
            }
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
    #[cfg(feature = "mcp-tool-filter")]
    filter: Option<ToolFilter>,
}

impl RequestHandler {
    /// Returns true if a tool name passes the configured filter.
    ///
    /// When the `mcp-tool-filter` feature is disabled, this is unconditionally `true`
    /// and inlines to a no-op so the dispatch hot path is unaffected.
    #[inline]
    fn allows(&self, _tool_name: &str) -> bool {
        #[cfg(feature = "mcp-tool-filter")]
        if let Some(f) = &self.filter {
            return f(_tool_name);
        }
        true
    }
}

impl RequestHandler {
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
        tools.push(McpTool {
            name: "connection_status".to_string(),
            description: "Check the connection status to the Myko server".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        });

        // Queries as tools
        for reg in inventory::iter::<QueryRegistration> {
            let name = format!("query:{}", reg.query_id);
            if !self.allows(&name) {
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
            if !self.allows(&name) {
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
            if !self.allows(&name) {
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

        let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

        // Built-in tools bypass the filter; inventory-backed tools are gated.
        if tool_name != "connection_status" && !self.allows(&tool_name) {
            let _ = response_tx.blocking_send(McpResponse::error(
                id,
                McpError {
                    code: McpError::METHOD_NOT_FOUND,
                    message: format!("Unknown tool: {}", tool_name),
                    data: None,
                },
            ));
            return;
        }

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
            let name = format!("query:{}", reg.query_id);
            if !self.allows(&name) {
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
            let name = format!("report:{}", reg.report_id);
            if !self.allows(&name) {
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
            let name = format!("command:{}", reg.command_id);
            if !self.allows(&name) {
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
                if !self.allows(&tool_name) {
                    return McpResponse::error(
                        id,
                        McpError {
                            code: McpError::INVALID_PARAMS,
                            message: format!("Resource not found: {}", uri),
                            data: None,
                        },
                    );
                }

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

async fn tool_executor(client: Arc<MykoClient>, mut rx: mpsc::Receiver<ToolRequest>) {
    while let Some(request) = rx.recv().await {
        let client = client.clone();
        let result = execute_tool(client, &request.tool_name, request.arguments).await;

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
) -> Result<Value, String> {
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

#[cfg(all(test, feature = "mcp-tool-filter"))]
mod tests {
    use super::*;

    #[test]
    fn filter_rejects_all_yields_empty_summary() {
        let summary = McpServer::new().with_filter(|_| false).summary();
        assert!(summary.queries.is_empty());
        assert!(summary.reports.is_empty());
        assert!(summary.commands.is_empty());
    }

    #[test]
    fn filter_passes_all_matches_default_summary() {
        let default_summary = McpServer::new().summary();
        let pass_through = McpServer::new().with_filter(|_| true).summary();
        assert_eq!(default_summary.queries.len(), pass_through.queries.len());
        assert_eq!(default_summary.reports.len(), pass_through.reports.len());
        assert_eq!(default_summary.commands.len(), pass_through.commands.len());
    }

    #[test]
    fn filter_receives_prefixed_tool_names() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let recorder = seen.clone();
        let _ = McpServer::new()
            .with_filter(move |name| {
                recorder.lock().unwrap().push(name.to_string());
                true
            })
            .summary();
        let names = seen.lock().unwrap();
        assert!(
            names.iter().all(|n| n.starts_with("query:")
                || n.starts_with("report:")
                || n.starts_with("command:")),
            "expected only prefixed names, got: {:?}",
            names
        );
    }
}
