//! Transport-agnostic MCP JSON-RPC dispatch.
//!
//! Handles `initialize`, `tools/list`, `tools/call`, `resources/list`,
//! `resources/read`, and the relevant notifications.
//!
//! ## Code Mode: `search` + `execute`
//!
//! Rather than one MCP tool per registered query/view/report/command (which
//! scales as `N_entities × ~8 auto-ops`, blowing up the tools/list token
//! footprint the same way a large hand-rolled REST-per-endpoint MCP server
//! does — see Cloudflare's [`Code Mode`][code-mode] writeup), `tools/list`
//! advertises exactly two operational tools:
//!
//! - **`search`** — looks up operations in [`ServerInfo::operation_index`]
//!   by substring/kind, returning compact `{id, kind, args, outputType}`
//!   entries instead of a full per-operation tool + JSON Schema.
//! - **`execute`** — runs a JS function body (see [`sandbox`]) against a
//!   generated `myko.*` API bound to the *same* [`Executor`] methods the
//!   old per-operation tools called, so a script can chain several
//!   query/command calls in one round trip instead of one MCP call each.
//!
//! [`ClientFilters`] visibility/callability checks move accordingly: they
//! used to gate `tools/list`/`tools/call` per operation name; now `search`
//! filters its index by the same names, and `execute`'s sandbox re-checks
//! them per `myko.*` call the script makes (see [`sandbox::call_operation`]
//! — not public, but that's where the check lives).
//!
//! This is a breaking change from the prior one-tool-per-operation wire
//! shape — existing `ClientFilters` glob configs (`query_*`, etc.) still
//! work exactly as before since they match against the same `{kind}_{id}`
//! strings, just from a different call site.
//!
//! [code-mode]: https://blog.cloudflare.com/code-mode-mcp/
//!
//! ## Resources
//!
//! Every tool also surfaces a *schema* resource at `myko://schema/<kind>/<id>`
//! whose content is the JSON Schema for the tool's input. This predates (and
//! is orthogonal to) `search`/`execute` — it's not part of the tool-count
//! problem `search`/`execute` fixes, since resources aren't tool
//! definitions loaded into the model's context by default. Left unchanged:
//! - Resources are URI-keyed and can't carry structured arguments, but
//!   every query / view / report registration takes args.
//! - Even argument-less reads are backed by reactive cells (the data is
//!   live), so pre-loading a snapshot into context at startup would
//!   just go stale. On-demand `tools/call` is the right shape for live
//!   reads.
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

use std::sync::Arc;

use myko::{
    command::CommandRegistration, operation_index::OperationSchema, query::QueryRegistration,
    report::ReportRegistration, view::ViewRegistration,
};
use serde_json::{Value, json};

use super::{
    exec::Executor,
    filter::ClientFilters,
    sandbox,
    types::{McpError, McpRequest, McpResource, McpResponse, McpTool},
};

const CONNECTION_STATUS_TOOL: &str = "connection_status";
const SEARCH_TOOL: &str = "search";
const EXECUTE_TOOL: &str = "execute";

/// Server identity for the `initialize` response.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
    /// Optional `instructions` text returned in the `initialize` response.
    /// MCP clients surface this to the model on connect; use it to teach
    /// agents how to use this server.
    pub instructions: Option<String>,
    /// Backs the `search` tool and the `execute` sandbox's `myko.*` API
    /// surface. Built automatically from `inventory`-registered operations
    /// (see [`myko::operation_index::build_operation_index`]) — no I/O, no
    /// configuration, works for every crate's operations regardless of
    /// which crate hosts the MCP server.
    pub operation_index: Arc<Vec<OperationSchema>>,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: "myko-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            instructions: None,
            operation_index: Arc::new(myko::operation_index::build_operation_index()),
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
        "tools/call" => {
            Some(handle_tools_call(request.id, request.params, filter, executor, info).await)
        }
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
    if let Some(text) = &info.instructions
        && let Value::Object(object) = &mut payload {
            object.insert("instructions".to_string(), Value::String(text.clone()));
        }
    McpResponse::success(id, payload)
}

fn handle_tools_list(id: Value, filter: &ClientFilters) -> McpResponse {
    let mut tools: Vec<McpTool> = Vec::new();

    if filter.meta_tool_visible(CONNECTION_STATUS_TOOL) {
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

    if filter.meta_tool_visible(SEARCH_TOOL) {
        tools.push(McpTool {
            name: SEARCH_TOOL.to_string(),
            description: "Search the index of available Myko queries/views/reports/commands. \
                Returns compact {id, kind, args, outputType} entries — call this before \
                `execute` to find operation ids and argument shapes."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Case-insensitive substring match against operation id/description."
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["query", "view", "report", "command"],
                        "description": "Restrict results to one operation kind."
                    }
                },
                "required": []
            }),
        });
    }

    if filter.meta_tool_visible(EXECUTE_TOOL) {
        tools.push(McpTool {
            name: EXECUTE_TOOL.to_string(),
            description: "Run JavaScript against the Myko API. The code runs as an async \
                function body — use `await myko.query(id, args)`, `myko.view(id, args)`, \
                `myko.report(id, args)`, or `myko.command(id, args)` (ids/args from `search`), \
                and optionally `return` a JSON-serializable value. Chain multiple calls in one \
                script instead of one `execute` call per operation. Each call resolves to a \
                wrapper object, not the raw payload directly: query/view resolve to \
                {query_id|view_id, item_type, count, items} (items is the payload); \
                report resolves to {report_id, output_type, result} (result is the payload); \
                command resolves to {command_id, success, result} (result is the payload). \
                E.g. `(await myko.report('ServerStats', {})).result`, not `.serverStats`."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "JavaScript function body to run."
                    }
                },
                "required": ["code"]
            }),
        });
    }

    McpResponse::success(id, json!({ "tools": tools }))
}

async fn handle_tools_call(
    id: Value,
    params: Option<Value>,
    filter: &ClientFilters,
    executor: &Executor,
    info: &ServerInfo,
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
    // example in the MCP 2025-06-18 spec (Tools / Error Handling). Uses
    // `meta_tool_visible` (deny-only) since "search"/"execute"/
    // "connection_status" are the only top-level tools now — see its doc
    // comment for why a positive allow list shouldn't gate them.
    if !filter.meta_tool_visible(&tool_name) {
        return McpResponse::error(
            id,
            McpError {
                code: McpError::INVALID_PARAMS,
                message: format!("Unknown tool: {tool_name}"),
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

    let result = execute_tool(executor, info, filter, &tool_name, arguments).await;

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

async fn execute_tool(
    executor: &Executor,
    info: &ServerInfo,
    filter: &ClientFilters,
    tool_name: &str,
    args: Value,
) -> Result<Value, String> {
    match tool_name {
        CONNECTION_STATUS_TOOL => Ok(executor.connection_status(info)),
        SEARCH_TOOL => Ok(handle_search(&args, filter, &info.operation_index)),
        EXECUTE_TOOL => handle_execute(&args, executor, filter).await,
        // Most commonly hit by a client with a cached pre-Code-Mode tool
        // name (e.g. `report_ServerStats`) from before this server
        // collapsed to search/execute — point it at the fix directly
        // rather than leaving it to guess from a bare "unknown tool".
        _ => Err(format!(
            "Unknown tool: {tool_name}. This server uses search + execute — \
             call `search` to discover operations, then `execute` to run them."
        )),
    }
}

fn handle_search(args: &Value, filter: &ClientFilters, index: &[OperationSchema]) -> Value {
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .map(str::to_lowercase);
    let kind = args.get("kind").and_then(|v| v.as_str());

    let operations: Vec<&OperationSchema> = index
        .iter()
        .filter(|op| filter.tool_visible(&format!("{}_{}", op.kind, op.id)))
        .filter(|op| kind.is_none_or(|k| op.kind == k))
        .filter(|op| {
            query.as_deref().is_none_or(|q| {
                op.id.to_lowercase().contains(q)
                    || op
                        .description
                        .as_deref()
                        .is_some_and(|d| d.to_lowercase().contains(q))
            })
        })
        .collect();

    json!({ "operations": operations })
}

async fn handle_execute(
    args: &Value,
    executor: &Executor,
    filter: &ClientFilters,
) -> Result<Value, String> {
    let Some(code) = args.get("code").and_then(|v| v.as_str()) else {
        return Err("Missing required `code` argument".to_string());
    };
    sandbox::execute(code, Arc::new(executor.clone()), filter.clone()).await
}

fn handle_resources_list(id: Value, filter: &ClientFilters) -> McpResponse {
    let mut resources: Vec<McpResource> = Vec::new();

    for reg in inventory::iter::<QueryRegistration> {
        let tool_name = format!("query_{}", reg.query_id);
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
        let tool_name = format!("view_{}", reg.view_id);
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
        let tool_name = format!("report_{}", reg.report_id);
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
        let tool_name = format!("command_{}", reg.command_id);
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

    if let Some(path) = uri.strip_prefix("myko://schema/")
        && let Some((schema_type, schema_id)) = path.split_once('/') {
            let tool_name = format!("{schema_type}:{schema_id}");
            if !filter.tool_visible(&tool_name) {
                return McpResponse::error(
                    id,
                    McpError {
                        code: McpError::INVALID_PARAMS,
                        message: format!("Resource not accessible: {uri}"),
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

    McpResponse::error(
        id,
        McpError {
            code: McpError::INVALID_PARAMS,
            message: format!("Resource not found: {uri}"),
            data: None,
        },
    )
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
    use serde_json::Value;

    use super::*;

    macro_rules! require_some {
        ($value:expr, $message:literal) => {
            match $value {
                Some(value) => value,
                None => {
                    assert!(false, $message);
                    return;
                }
            }
        };
    }

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
            ..Default::default()
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
            ..Default::default()
        };
        // Executor is irrelevant for initialize but we need *some* executor;
        // use an in-process one wrapped around a minimal ctx is heavy here,
        // so just use a dummy MykoClient.
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let response = require_some!(
            handle_request(make_request("initialize"), &filter, &executor, &info).await,
            "initialize must produce a response"
        );
        let result = require_some!(response.result, "initialize must have a result");
        assert_eq!(result.pointer("/serverInfo/name"), Some(&json!("test")));
        assert_eq!(result.pointer("/serverInfo/version"), Some(&json!("0.0.0")));
    }

    #[tokio::test]
    async fn initialize_includes_instructions_when_set() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "pulse-mcp".into(),
            version: "0.2.0".into(),
            instructions: Some("teach me".into()),
            ..Default::default()
        };
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);

        let resp = require_some!(
            handle_request(make_request("initialize"), &filter, &executor, &info).await,
            "initialize must return a response"
        );
        let result = require_some!(resp.result, "initialize must succeed");
        assert_eq!(result.pointer("/serverInfo/name"), Some(&json!("pulse-mcp")));
        assert_eq!(result.pointer("/serverInfo/version"), Some(&json!("0.2.0")));
        assert_eq!(result.get("instructions"), Some(&json!("teach me")));
    }

    #[tokio::test]
    async fn initialize_omits_instructions_when_unset() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);

        let resp = require_some!(
            handle_request(make_request("initialize"), &filter, &executor, &info).await,
            "response"
        );
        let result = require_some!(resp.result, "ok");
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
        let response = require_some!(
            handle_request(make_request("unknown/method"), &filter, &executor, &info).await,
            "must produce a response"
        );
        assert!(response.error.is_some());
    }

    // ─── Code Mode: search / execute ──────────────────────────────────────

    fn make_tool_call(name: &str, arguments: Value) -> McpRequest {
        let request = McpRequest {
            jsonrpc: "2.0".to_string(),
            id: Value::Number(1.into()),
            method: "tools/call".to_string(),
            params: Some(json!({ "name": name, "arguments": arguments })),
        };
        drop(arguments);
        request
    }

    fn dummy_executor() -> Executor {
        Executor::Client(std::sync::Arc::new(myko::client::MykoClient::new()))
    }

    fn info_with_index() -> ServerInfo {
        ServerInfo {
            operation_index: Arc::new(vec![
                OperationSchema {
                    id: "GetAllServers".to_string(),
                    kind: "query".to_string(),
                    description: Some("All servers".to_string()),
                    args: vec![],
                    output_type: "Server[]".to_string(),
                },
                OperationSchema {
                    id: "DeleteServer".to_string(),
                    kind: "command".to_string(),
                    description: Some("Delete a server".to_string()),
                    args: vec![],
                    output_type: "DeleteServerResult".to_string(),
                },
            ]),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn tools_list_only_exposes_search_execute_and_connection_status() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let executor = dummy_executor();
        let resp = require_some!(
            handle_request(make_request("tools/list"), &filter, &executor, &info).await,
            "response"
        );
        let result = require_some!(resp.result, "ok");
        let tools = require_some!(result.get("tools").and_then(Value::as_array), "array")
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            tools,
            std::collections::HashSet::from([
                CONNECTION_STATUS_TOOL.to_string(),
                SEARCH_TOOL.to_string(),
                EXECUTE_TOOL.to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn search_filters_by_kind_and_query_text() {
        let filter = ClientFilters::allow_all();
        let info = info_with_index();
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("search", json!({ "kind": "command" })),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        let result = require_some!(resp.result, "ok");
        let text = require_some!(result.pointer("/content/0/text").and_then(Value::as_str), "text");
        let parsed = require_some!(serde_json::from_str::<Value>(text).ok(), "valid JSON content");
        let ops = require_some!(parsed.get("operations").and_then(Value::as_array), "array");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops.first().and_then(|op| op.get("id")), Some(&json!("DeleteServer")));
    }

    #[tokio::test]
    async fn search_respects_visibility_filter() {
        // Same glob patterns used to hide per-operation tools before Code
        // Mode still apply — just checked inside `search` now.
        let filter = ClientFilters::from_strings(None, Some("command_*"), None, None);
        let info = info_with_index();
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("search", json!({})),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        let result = require_some!(resp.result, "ok");
        let text = require_some!(result.pointer("/content/0/text").and_then(Value::as_str), "text");
        let parsed = require_some!(serde_json::from_str::<Value>(text).ok(), "valid JSON");
        let operations = require_some!(parsed.get("operations").and_then(Value::as_array), "operations");
        let ids: Vec<&str> = operations
            .iter()
            .filter_map(|operation| operation.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(ids, vec!["GetAllServers"]);
    }

    #[tokio::test]
    async fn execute_runs_a_script_and_returns_its_value() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("execute", json!({ "code": "return 21 * 2;" })),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        let result = require_some!(resp.result, "ok");
        assert_ne!(result.get("isError"), Some(&json!(true)));
        let text = require_some!(result.pointer("/content/0/text").and_then(Value::as_str), "text");
        assert_eq!(text.trim(), "42");
    }

    #[tokio::test]
    async fn connection_status_identifies_the_server_instance() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "pulse-ctx".into(),
            version: "1.2.3".into(),
            ..Default::default()
        };
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("connection_status", json!({})),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        let result = require_some!(resp.result, "ok");
        let text = require_some!(result.pointer("/content/0/text").and_then(Value::as_str), "text");
        let parsed = require_some!(serde_json::from_str::<Value>(text).ok(), "valid JSON");
        assert_eq!(parsed.get("name"), Some(&json!("pulse-ctx")));
        assert_eq!(parsed.get("version"), Some(&json!("1.2.3")));
    }

    #[tokio::test]
    async fn execute_without_code_argument_is_a_tool_execution_error() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("execute", json!({})),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        let result = require_some!(resp.result, "ok");
        assert_eq!(result.get("isError"), Some(&json!(true)));
    }

    #[tokio::test]
    async fn hidden_execute_tool_is_a_protocol_error() {
        let filter = ClientFilters::from_strings(None, Some("execute"), None, None);
        let info = ServerInfo::default();
        let executor = dummy_executor();

        let resp = handle_request(
            make_tool_call("execute", json!({ "code": "return 1;" })),
            &filter,
            &executor,
            &info,
        )
        .await;
        let resp = require_some!(resp, "response");
        assert!(
            resp.error.is_some(),
            "denied tool must be a protocol error, not a tool result"
        );
    }

    #[tokio::test]
    async fn op_level_allow_list_scopes_operations_without_hiding_search_and_execute() {
        // The exact pulse-ctx upgrade scenario: an allow list written for
        // the old one-tool-per-operation wire shape (op-level patterns,
        // no explicit "search"/"execute" entry) must still expose
        // search/execute — and search/execute must still only surface and
        // allow the operations the list actually names.
        // Allows only the query, not the command — so the assertion below
        // actually exercises scoping (not just "search still works").
        let filter = ClientFilters::from_strings(Some("query_GetAllServers"), None, None, None);
        let info = info_with_index();
        let executor = dummy_executor();

        let list_resp = require_some!(
            handle_request(make_request("tools/list"), &filter, &executor, &info).await,
            "response"
        );
        let list_result = require_some!(list_resp.result, "ok");
        let tools: Vec<String> = require_some!(list_result.get("tools").and_then(Value::as_array), "array")
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect();
        assert!(
            tools.contains(&SEARCH_TOOL.to_string()) && tools.contains(&EXECUTE_TOOL.to_string()),
            "op-level allow list must not hide search/execute, got {tools:?}"
        );

        let search_resp = handle_request(
            make_tool_call("search", json!({})),
            &filter,
            &executor,
            &info,
        )
        .await;
        let search_resp = require_some!(search_resp, "response");
        let search_result = require_some!(search_resp.result, "ok");
        let text = require_some!(search_result.pointer("/content/0/text").and_then(Value::as_str), "text");
        let parsed = require_some!(serde_json::from_str::<Value>(text).ok(), "valid JSON");
        let operations = require_some!(parsed.get("operations").and_then(Value::as_array), "operations");
        let ids: Vec<&str> = operations
            .iter()
            .filter_map(|operation| operation.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(
            ids,
            vec!["GetAllServers"],
            "search must only surface the allow-listed query, not the un-listed DeleteServer command"
        );

        // execute itself is reachable (op-level allow list doesn't hide
        // it)...
        let execute_resp = handle_request(
            make_tool_call(
                "execute",
                json!({ "code": "try { await myko.command('DeleteServer', {id: 'x'}); return 'no-throw'; } catch (e) { return e.message; }" }),
            ),
            &filter,
            &executor,
            &info,
        )
        .await;
        let execute_resp = require_some!(execute_resp, "response");
        let execute_result = require_some!(execute_resp.result, "ok");
        assert_ne!(execute_result.get("isError"), Some(&json!(true)));
        let message = require_some!(execute_result.pointer("/content/0/text").and_then(Value::as_str), "text");
        // ...but the un-listed command it tries to call is still rejected
        // per-call inside the sandbox.
        assert_eq!(
            message.trim(),
            "\"Unknown operation: command_DeleteServer\""
        );
    }
}
