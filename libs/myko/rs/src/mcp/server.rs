//! MCP server implementation.

use std::{
    io::{self, BufRead, Write},
    thread,
};

use serde_json::{json, Value};

use crate::{
    command::{CommandHandlerRegistration, CommandRegistration},
    query::QueryRegistration,
    report::ReportRegistration,
};

use super::types::*;

/// MCP Server for Myko.
///
/// Automatically exposes all registered queries, reports, and commands
/// through the MCP protocol.
pub struct McpServer {
    server_name: String,
    server_version: String,
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
        }
    }

    /// Create a new MCP server with custom name and version.
    pub fn with_info(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            server_name: name.into(),
            server_version: version.into(),
        }
    }

    /// Spawn the MCP server in a background thread.
    ///
    /// This runs the stdio-based MCP server in a separate thread.
    /// Returns a handle to the thread.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let server = McpServer::new();
    /// let handle = server.spawn_stdio();
    /// // Server is now running in the background
    /// // handle.join() to wait for it
    /// ```
    pub fn spawn_stdio(self) -> thread::JoinHandle<io::Result<()>> {
        thread::spawn(move || self.run_stdio())
    }

    /// Run the MCP server over stdio (blocking).
    ///
    /// This reads JSON-RPC requests from stdin and writes responses to stdout.
    /// Logs and errors are written to stderr.
    pub fn run_stdio(&self) -> io::Result<()> {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut stdout = stdout.lock();

        for line in stdin.lock().lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[myko-mcp] Error reading stdin: {}", e);
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
                    let response = McpResponse::error(Value::Null, McpError::parse_error(e.to_string()));
                    let json = serde_json::to_string(&response)?;
                    writeln!(stdout, "{}", json)?;
                    stdout.flush()?;
                    continue;
                }
            };

            let response = self.handle_request(request);
            let json = serde_json::to_string(&response)?;
            writeln!(stdout, "{}", json)?;
            stdout.flush()?;
        }

        Ok(())
    }

    /// Handle a single MCP request.
    pub fn handle_request(&self, request: McpRequest) -> McpResponse {
        match request.method.as_str() {
            "initialize" => self.handle_initialize(request.id),
            "notifications/initialized" | "notifications/cancelled" => {
                McpResponse::success(request.id, Value::Null)
            }
            "tools/list" => self.handle_tools_list(request.id),
            "tools/call" => self.handle_tools_call(request.id, request.params),
            "resources/list" => self.handle_resources_list(request.id),
            "resources/read" => self.handle_resources_read(request.id, request.params),
            _ => McpResponse::error(request.id, McpError::method_not_found(&request.method)),
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
        let tools = self.collect_tools();
        McpResponse::success(id, json!({ "tools": tools }))
    }

    fn handle_tools_call(&self, id: Value, params: Option<Value>) -> McpResponse {
        let params = match params {
            Some(p) => p,
            None => {
                return McpResponse::error(id, McpError::invalid_params("Missing params"));
            }
        };

        let tool_name = match params.get("name").and_then(|v| v.as_str()) {
            Some(n) => n,
            None => {
                return McpResponse::error(id, McpError::invalid_params("Missing tool name"));
            }
        };

        let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

        // Find the command handler
        for registration in inventory::iter::<CommandHandlerRegistration> {
            if registration.command_id == tool_name {
                // NOTE: We can't actually execute commands without a CellServerCtx.
                // This MCP server is primarily for introspection/discovery.
                // For actual execution, the MCP client should connect to the Myko WebSocket.
                return McpResponse::success(
                    id,
                    json!({
                        "content": [{
                            "type": "text",
                            "text": format!(
                                "Command '{}' found. To execute commands, connect to the Myko WebSocket server.\n\nArguments received: {}",
                                tool_name,
                                serde_json::to_string_pretty(&arguments).unwrap_or_default()
                            )
                        }],
                        "isError": false
                    }),
                );
            }
        }

        McpResponse::error(
            id,
            McpError {
                code: McpError::METHOD_NOT_FOUND,
                message: format!("Unknown tool: {}", tool_name),
                data: None,
            },
        )
    }

    fn handle_resources_list(&self, id: Value) -> McpResponse {
        let resources = self.collect_resources();
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

        // Parse the URI: myko://schema/{type}/{id}
        if let Some(path) = uri.strip_prefix("myko://schema/") {
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            if parts.len() == 2 {
                let (schema_type, schema_id) = (parts[0], parts[1]);

                let content = match schema_type {
                    "query" => self.get_query_schema(schema_id),
                    "report" => self.get_report_schema(schema_id),
                    "command" => self.get_command_schema(schema_id),
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

    /// Collect all registered commands as MCP tools.
    fn collect_tools(&self) -> Vec<McpTool> {
        let mut tools = Vec::new();

        // Collect from CommandRegistration (has type info)
        for registration in inventory::iter::<CommandRegistration> {
            tools.push(McpTool {
                name: registration.command_id.to_string(),
                description: format!(
                    "Myko command: {} (returns {})",
                    registration.command_id, registration.result_type
                ),
                input_schema: json!({
                    "type": "object",
                    "description": format!("Arguments for {} command", registration.command_id),
                    "additionalProperties": true
                }),
            });
        }

        tools
    }

    /// Collect all registered queries and reports as MCP resources.
    fn collect_resources(&self) -> Vec<McpResource> {
        let mut resources = Vec::new();

        // Queries
        for registration in inventory::iter::<QueryRegistration> {
            resources.push(McpResource {
                uri: format!("myko://schema/query/{}", registration.query_id),
                name: registration.query_id.to_string(),
                description: Some(format!(
                    "Query returning {} entities",
                    registration.query_item_type
                )),
                mime_type: Some("application/json".to_string()),
            });
        }

        // Reports
        for registration in inventory::iter::<ReportRegistration> {
            resources.push(McpResource {
                uri: format!("myko://schema/report/{}", registration.report_id),
                name: registration.report_id.to_string(),
                description: Some(format!(
                    "Report returning {}",
                    registration.output_type
                )),
                mime_type: Some("application/json".to_string()),
            });
        }

        // Commands (as resources for schema inspection)
        for registration in inventory::iter::<CommandRegistration> {
            resources.push(McpResource {
                uri: format!("myko://schema/command/{}", registration.command_id),
                name: format!("{} (command)", registration.command_id),
                description: Some(format!(
                    "Command schema for {} (returns {})",
                    registration.command_id, registration.result_type
                )),
                mime_type: Some("application/json".to_string()),
            });
        }

        resources
    }

    fn get_query_schema(&self, query_id: &str) -> Option<String> {
        for registration in inventory::iter::<QueryRegistration> {
            if registration.query_id == query_id {
                let schema = json!({
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "title": registration.query_id,
                    "type": "object",
                    "description": format!(
                        "Query '{}' returns entities of type '{}'",
                        registration.query_id,
                        registration.query_item_type
                    ),
                    "properties": {
                        "query_id": {
                            "const": registration.query_id,
                            "description": "Query identifier"
                        },
                        "query_item_type": {
                            "const": registration.query_item_type,
                            "description": "Entity type returned by this query"
                        }
                    },
                    "additionalProperties": true
                });
                return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
            }
        }
        None
    }

    fn get_report_schema(&self, report_id: &str) -> Option<String> {
        for registration in inventory::iter::<ReportRegistration> {
            if registration.report_id == report_id {
                let schema = json!({
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "title": registration.report_id,
                    "type": "object",
                    "description": format!(
                        "Report '{}' returns output of type '{}'",
                        registration.report_id,
                        registration.output_type
                    ),
                    "properties": {
                        "report_id": {
                            "const": registration.report_id,
                            "description": "Report identifier"
                        },
                        "output_type": {
                            "const": registration.output_type,
                            "description": "Output type of this report"
                        }
                    },
                    "additionalProperties": true
                });
                return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
            }
        }
        None
    }

    fn get_command_schema(&self, command_id: &str) -> Option<String> {
        for registration in inventory::iter::<CommandRegistration> {
            if registration.command_id == command_id {
                let schema = json!({
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "title": registration.command_id,
                    "type": "object",
                    "description": format!(
                        "Command '{}' returns result of type '{}'",
                        registration.command_id,
                        registration.result_type
                    ),
                    "properties": {
                        "command_id": {
                            "const": registration.command_id,
                            "description": "Command identifier"
                        },
                        "result_type": {
                            "const": registration.result_type,
                            "description": "Result type of this command"
                        }
                    },
                    "additionalProperties": true
                });
                return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
            }
        }
        None
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
    fn test_initialize() {
        let server = McpServer::new();
        let request = McpRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            method: "initialize".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none());
        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert_eq!(result["protocolVersion"], "2024-11-05");
        assert!(result["capabilities"]["tools"].is_object());
        assert!(result["capabilities"]["resources"].is_object());
    }

    #[test]
    fn test_tools_list() {
        let server = McpServer::new();
        let request = McpRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(2),
            method: "tools/list".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none());
        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert!(result["tools"].is_array());
    }

    #[test]
    fn test_resources_list() {
        let server = McpServer::new();
        let request = McpRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(3),
            method: "resources/list".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none());
        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert!(result["resources"].is_array());
    }

    #[test]
    fn test_unknown_method() {
        let server = McpServer::new();
        let request = McpRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(4),
            method: "unknown/method".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_some());
        assert_eq!(response.error.unwrap().code, McpError::METHOD_NOT_FOUND);
    }
}
