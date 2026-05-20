//! Transport-agnostic MCP JSON-RPC dispatch.
//!
//! Handles `initialize`, `tools/list`, `tools/call`, `resources/list`,
//! `resources/read`, and the relevant notifications. Tool filtering is
//! applied to both the listing and call paths so a denied tool is invisible
//! to the client and an attempt to call it returns an error.

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
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: "myko-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
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
    McpResponse::success(
        id,
        json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {},
                "resources": {}
            },
            "serverInfo": {
                "name": info.name,
                "version": info.version,
            }
        }),
    )
}

fn handle_tools_list(id: Value, filter: &ClientFilters) -> McpResponse {
    let mut tools: Vec<McpTool> = Vec::new();

    if filter.allows_name(CONNECTION_STATUS_TOOL) {
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
        if !filter.allows_name(&name) {
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
        if !filter.allows_name(&name) {
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
        if !filter.allows_name(&name) {
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
        if !filter.allows_name(&name) {
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
    let Some(tool_name) = params.get("name").and_then(|v| v.as_str()).map(str::to_string) else {
        return McpResponse::error(id, McpError::invalid_params("Missing tool name"));
    };

    if !filter.allows_name(&tool_name) {
        return McpResponse::error(
            id,
            McpError {
                code: McpError::METHOD_NOT_FOUND,
                message: "Tool denied by filter".to_string(),
                data: None,
            },
        );
    }

    let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

    // Argument-aware client filter. Rejection surfaces as MCP `isError: true`
    // content per the spec's "invalid input data" shape — distinct from the
    // protocol-level method-not-found used for unknown / name-denied tools.
    if let Err(message) = filter.allows_call(&tool_name, &arguments) {
        return McpResponse::success(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": format!("Call denied by filter: {}", message)
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
        if !filter.allows_name(&tool_name) {
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
        if !filter.allows_name(&tool_name) {
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
        if !filter.allows_name(&tool_name) {
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
        if !filter.allows_name(&tool_name) {
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
            if !filter.allows_name(&tool_name) {
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

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "test".into(),
            version: "0.0.0".into(),
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
