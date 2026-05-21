//! Transport-agnostic MCP JSON-RPC dispatch.
//!
//! Handles `initialize`, `tools/list`, `tools/call`, `resources/list`,
//! `resources/read`, and the relevant notifications.
//!
//! ## Tool / resource split
//!
//! - **Tools** — every registered query / view / report / command is
//!   exposed as a tool the LLM can invoke on demand. Tools take structured
//!   `arguments` matching the registration's input shape.
//! - **Resources** — every tool also surfaces a *schema* resource at
//!   `myko://schema/<kind>/<id>` whose content is the JSON Schema for the
//!   tool's input. The schema goes through the resource shape rather than
//!   the data because:
//!   - Resources are URI-keyed and can't carry structured arguments, but
//!     every query / view / report registration takes args.
//!   - Even argument-less reads are backed by reactive cells (the data is
//!     live), so pre-loading a snapshot into context at startup would
//!     just go stale. On-demand `tools/call` is the right shape for live
//!     reads.
//!
//! Reactive query subscriptions via `resources/subscribe` are future work.
//!
//! Error responses follow the [MCP 2025-06-18 error-handling shape][spec]:
//!
//! - **Protocol Error** — JSON-RPC error response with `code: -32602` and
//!   message `"Unknown tool: …"`. Used when a tool is hidden by visibility
//!   filtering (indistinguishable on the wire from a tool that does not
//!   exist) or when required `tools/call` params are missing.
//! - **Tool Execution Error** — successful JSON-RPC response with
//!   `isError: true` content carrying a descriptive message. Used when
//!   `tools/call` arguments fail client-supplied argument constraints
//!   (the spec's "Invalid input data" category) or when tool execution
//!   raises an error downstream.
//!
//! [spec]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools#error-handling

use myko::{
    command::CommandRegistration, query::QueryRegistration, report::ReportRegistration,
    view::ViewRegistration,
};
use serde_json::{Value, json};

use super::{
    exec::Executor,
    filter::ClientFilters,
    types::{McpError, McpRequest, McpResource, McpResponse, McpTool},
};

const CONNECTION_STATUS_TOOL: &str = "connection_status";

/// Server identity for the `initialize` response.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
    /// Optional `instructions` text returned in the `initialize` response.
    /// MCP clients surface this to the model on connect; use it to teach
    /// agents how to use this server.
    pub instructions: Option<String>,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: "myko-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            instructions: None,
        }
    }
}

/// Dispatch one JSON-RPC request. Returns `None` for notifications that do
/// not produce a response.
pub async fn handle_request(
    request: McpRequest,
    filter: &ClientFilters,
    executor: &Executor,
    info: &ServerInfo,
) -> Option<McpResponse> {
    match request.method.as_str() {
        "initialize" => Some(handle_initialize(request.id, info)),
        "notifications/initialized" | "notifications/cancelled" => None,
        "tools/list" => Some(handle_tools_list(request.id, filter)),
        "tools/call" => Some(handle_tools_call(request.id, request.params, filter, executor).await),
        "resources/list" => Some(handle_resources_list(request.id, filter)),
        "resources/read" => Some(handle_resources_read(request.id, request.params, filter)),
        _ => Some(McpResponse::error(
            request.id,
            McpError::method_not_found(&request.method),
        )),
    }
}

fn handle_initialize(id: Value, info: &ServerInfo) -> McpResponse {
    let mut payload = json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {},
            "resources": {}
        },
        "serverInfo": {
            "name": info.name,
            "version": info.version,
        }
    });
    if let Some(text) = &info.instructions {
        payload
            .as_object_mut()
            .expect("payload is an object literal above")
            .insert("instructions".to_string(), Value::String(text.clone()));
    }
    McpResponse::success(id, payload)
}

fn handle_tools_list(id: Value, filter: &ClientFilters) -> McpResponse {
    let mut tools: Vec<McpTool> = Vec::new();

    if filter.tool_visible(CONNECTION_STATUS_TOOL) {
        tools.push(McpTool {
            name: CONNECTION_STATUS_TOOL.to_string(),
            description: "Check the connection status to the Myko server".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        });
    }

    for reg in inventory::iter::<QueryRegistration> {
        let name = format!("query:{}", reg.query_id);
        if !filter.tool_visible(&name) {
            continue;
        }
        tools.push(McpTool {
            name,
            description: format!("Query returning {} entities", reg.query_item_type),
            input_schema: open_object_schema(),
        });
    }

    for reg in inventory::iter::<ViewRegistration> {
        let name = format!("view:{}", reg.view_id);
        if !filter.tool_visible(&name) {
            continue;
        }
        tools.push(McpTool {
            name,
            description: format!("View returning a list of {}", reg.view_item_type),
            input_schema: open_object_schema(),
        });
    }

    for reg in inventory::iter::<ReportRegistration> {
        let name = format!("report:{}", reg.report_id);
        if !filter.tool_visible(&name) {
            continue;
        }
        tools.push(McpTool {
            name,
            description: format!("Report returning {}", reg.output_type),
            input_schema: open_object_schema(),
        });
    }

    for reg in inventory::iter::<CommandRegistration> {
        let name = format!("command:{}", reg.command_id);
        if !filter.tool_visible(&name) {
            continue;
        }
        tools.push(McpTool {
            name,
            description: format!("Command returning {}", reg.result_type),
            input_schema: open_object_schema(),
        });
    }

    McpResponse::success(id, json!({ "tools": tools }))
}

async fn handle_tools_call(
    id: Value,
    params: Option<Value>,
    filter: &ClientFilters,
    executor: &Executor,
) -> McpResponse {
    let Some(params) = params else {
        return McpResponse::error(id, McpError::invalid_params("Missing params"));
    };
    let Some(tool_name) = params
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return McpResponse::error(id, McpError::invalid_params("Missing tool name"));
    };

    // MCP Protocol Error: a hidden tool is indistinguishable on the wire from
    // a tool that doesn't exist. Code -32602 + "Unknown tool: …" matches the
    // example in the MCP 2025-06-18 spec (Tools / Error Handling).
    if !filter.tool_visible(&tool_name) {
        return McpResponse::error(
            id,
            McpError {
                code: McpError::INVALID_PARAMS,
                message: format!("Unknown tool: {}", tool_name),
                data: None,
            },
        );
    }

    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    // MCP Tool Execution Error ("Invalid input data" category): result is a
    // successful JSON-RPC response carrying `isError: true` content with the
    // descriptive constraint message verbatim — distinct from the protocol
    // error path above.
    if let Err(message) = filter.tool_callable(&tool_name, &arguments) {
        return McpResponse::success(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": message,
                }],
                "isError": true,
            }),
        );
    }

    let result = execute_tool(executor, &tool_name, arguments).await;

    match result {
        Ok(data) => McpResponse::success(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string_pretty(&data).unwrap_or_default()
                }]
            }),
        ),
        Err(message) => McpResponse::success(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": format!("Error: {}", message)
                }],
                "isError": true,
            }),
        ),
    }
}

async fn execute_tool(executor: &Executor, tool_name: &str, args: Value) -> Result<Value, String> {
    if tool_name == CONNECTION_STATUS_TOOL {
        return Ok(executor.connection_status());
    }
    if let Some(id) = tool_name.strip_prefix("query:") {
        return executor.execute_query(id, args).await;
    }
    if let Some(id) = tool_name.strip_prefix("view:") {
        return executor.execute_view(id, args).await;
    }
    if let Some(id) = tool_name.strip_prefix("report:") {
        return executor.execute_report(id, args).await;
    }
    if let Some(id) = tool_name.strip_prefix("command:") {
        return executor.execute_command(id, args).await;
    }
    Err(format!("Unknown tool: {}", tool_name))
}

fn handle_resources_list(id: Value, filter: &ClientFilters) -> McpResponse {
    let mut resources: Vec<McpResource> = Vec::new();

    for reg in inventory::iter::<QueryRegistration> {
        let tool_name = format!("query:{}", reg.query_id);
        if !filter.tool_visible(&tool_name) {
            continue;
        }
        resources.push(McpResource {
            uri: format!("myko://schema/query/{}", reg.query_id),
            name: reg.query_id.to_string(),
            description: Some(format!("Query returning {} entities", reg.query_item_type)),
            mime_type: Some("application/json".to_string()),
        });
    }

    for reg in inventory::iter::<ViewRegistration> {
        let tool_name = format!("view:{}", reg.view_id);
        if !filter.tool_visible(&tool_name) {
            continue;
        }
        resources.push(McpResource {
            uri: format!("myko://schema/view/{}", reg.view_id),
            name: reg.view_id.to_string(),
            description: Some(format!("View returning a list of {}", reg.view_item_type)),
            mime_type: Some("application/json".to_string()),
        });
    }

    for reg in inventory::iter::<ReportRegistration> {
        let tool_name = format!("report:{}", reg.report_id);
        if !filter.tool_visible(&tool_name) {
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
        if !filter.tool_visible(&tool_name) {
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

fn handle_resources_read(id: Value, params: Option<Value>, filter: &ClientFilters) -> McpResponse {
    let Some(params) = params else {
        return McpResponse::error(id, McpError::invalid_params("Missing params"));
    };
    let Some(uri) = params.get("uri").and_then(|v| v.as_str()) else {
        return McpResponse::error(id, McpError::invalid_params("Missing uri"));
    };

    if let Some(path) = uri.strip_prefix("myko://schema/") {
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() == 2 {
            let (schema_type, schema_id) = (parts[0], parts[1]);
            let tool_name = format!("{}:{}", schema_type, schema_id);
            if !filter.tool_visible(&tool_name) {
                return McpResponse::error(
                    id,
                    McpError {
                        code: McpError::INVALID_PARAMS,
                        message: format!("Resource not accessible: {}", uri),
                        data: None,
                    },
                );
            }
            let content = match schema_type {
                "query" => get_query_schema(schema_id),
                "view" => get_view_schema(schema_id),
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
                            "text": content,
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

fn open_object_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true
    })
}

fn get_query_schema(query_id: &str) -> Option<String> {
    for reg in inventory::iter::<QueryRegistration> {
        if reg.query_id == query_id {
            let schema = json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": reg.query_id,
                "description": format!("Query returning {} entities", reg.query_item_type),
                "type": "object",
                "additionalProperties": true,
            });
            return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
        }
    }
    None
}

fn get_view_schema(view_id: &str) -> Option<String> {
    for reg in inventory::iter::<ViewRegistration> {
        if reg.view_id == view_id {
            let schema = json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": reg.view_id,
                "description": format!("View returning a list of {}", reg.view_item_type),
                "type": "object",
                "additionalProperties": true,
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
                "additionalProperties": true,
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
                "additionalProperties": true,
            });
            return Some(serde_json::to_string_pretty(&schema).unwrap_or_default());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn make_request(method: &str) -> McpRequest {
        McpRequest {
            jsonrpc: "2.0".to_string(),
            id: Value::Number(1.into()),
            method: method.to_string(),
            params: None,
        }
    }

    #[test]
    fn server_info_default_omits_instructions() {
        let info = ServerInfo::default();
        assert_eq!(info.instructions, None);
    }

    #[test]
    fn server_info_can_carry_instructions() {
        let info = ServerInfo {
            name: "test".into(),
            version: "0.0.0".into(),
            instructions: Some("test instructions text".into()),
        };
        assert_eq!(info.instructions.as_deref(), Some("test instructions text"));
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "test".into(),
            version: "0.0.0".into(),
            instructions: None,
        };
        // Executor is irrelevant for initialize but we need *some* executor;
        // use an in-process one wrapped around a minimal ctx is heavy here,
        // so just use a dummy MykoClient.
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let response = handle_request(make_request("initialize"), &filter, &executor, &info)
            .await
            .expect("initialize must produce a response");
        let result = response.result.expect("initialize must have a result");
        assert_eq!(result["serverInfo"]["name"], "test");
        assert_eq!(result["serverInfo"]["version"], "0.0.0");
    }

    #[tokio::test]
    async fn initialize_includes_instructions_when_set() {
        use serde_json::json;

        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "pulse-mcp".into(),
            version: "0.2.0".into(),
            instructions: Some("teach me".into()),
        };
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);

        let resp = handle_request(make_request("initialize"), &filter, &executor, &info)
            .await
            .expect("initialize must return a response");
        let result = resp.result.expect("initialize must succeed");

        assert_eq!(result["serverInfo"]["name"], json!("pulse-mcp"));
        assert_eq!(result["serverInfo"]["version"], json!("0.2.0"));
        assert_eq!(result["instructions"], json!("teach me"));
    }

    #[tokio::test]
    async fn initialize_omits_instructions_when_unset() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);

        let resp = handle_request(make_request("initialize"), &filter, &executor, &info)
            .await
            .expect("response");
        let result = resp.result.expect("ok");
        assert!(
            result.get("instructions").is_none(),
            "instructions must be omitted when ServerInfo.instructions is None"
        );
    }

    #[tokio::test]
    async fn notifications_produce_no_response() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        assert!(
            handle_request(
                make_request("notifications/initialized"),
                &filter,
                &executor,
                &info,
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let response = handle_request(make_request("unknown/method"), &filter, &executor, &info)
            .await
            .expect("must produce a response");
        assert!(response.error.is_some());
    }
}
