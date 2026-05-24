//! Transport-agnostic MCP JSON-RPC dispatch.
//!
//! Handles `server/discover`, `initialize` (transition shim — see below),
//! `tools/list`, `tools/call`, `resources/list`, `resources/read`, and the
//! relevant notifications.
//!
//! ## Protocol
//!
//! Implements MCP **2026-07-28** (SEP-2575 et al.). The
//! `initialize`/`notifications/initialized` handshake and `Mcp-Session-Id`
//! header are removed; protocol version, client info, and client
//! capabilities now travel in `_meta` on every request. The new
//! `server/discover` RPC advertises server identity, capabilities, and
//! instructions on demand.
//!
//! **Transition shim.** Until clients catch up, `initialize` is still
//! accepted and returns the same payload as `server/discover`. This is not
//! required by the spec; it's a kindness during the 10-week 2026-07-28 RC
//! window. The shim will be removed when all known consumers have migrated.
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
//! List and resource-read results carry `ttlMs` + `cacheScope` per
//! SEP-2549. Tool registrations are static (compile-time inventory) so
//! `tools/list` is safely cacheable for a long window; resource schema
//! reads are equally static.
//!
//! Reactive query subscriptions via the new `subscriptions/listen`
//! endpoint (SEP-2575) are future work.
//!
//! ## Error responses
//!
//! - **Protocol Error** — JSON-RPC error response with `code: -32602` and
//!   message `"Unknown tool: …"`. Used when a tool is hidden by visibility
//!   filtering (indistinguishable on the wire from a tool that does not
//!   exist) or when required `tools/call` params are missing. Missing
//!   resources also map to `-32602` per SEP-2164.
//! - **Tool Execution Error** — successful JSON-RPC response with
//!   `isError: true` content carrying a descriptive message. Used when
//!   `tools/call` arguments fail client-supplied argument constraints or
//!   when tool execution raises an error downstream.

use myko::{
    command::CommandRegistration, query::QueryRegistration, report::ReportRegistration,
    view::ViewRegistration,
};
use serde_json::{Value, json};

use super::{
    exec::Executor,
    filter::ClientFilters,
    types::{McpError, McpRequest, McpResource, McpResponse, McpTool, PROTOCOL_VERSION},
};

const CONNECTION_STATUS_TOOL: &str = "connection_status";

/// `ttlMs` advertised on `tools/list` (SEP-2549). Tool registrations are
/// compile-time-static (`inventory::iter`), so the list is stable across
/// the server's lifetime — a long TTL is appropriate. Clients re-fetch
/// when this expires or when (future) `toolsListChanged` notifications
/// fire.
const TOOLS_LIST_TTL_MS: u64 = 3_600_000;

/// `ttlMs` for `resources/list` and `resources/read`. Resource schemas
/// are derived from the same compile-time registrations, equally stable.
const RESOURCES_TTL_MS: u64 = 3_600_000;

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
    // SEP-2575: protocol version travels in `_meta` on every request.
    // Reject mismatches with `UnsupportedProtocolVersionError`. Absence
    // is tolerated during the transition window (a 2024-11-05 client
    // negotiates via `initialize` and never sends the meta key).
    let bad_version = request
        .client_protocol_version()
        .filter(|v| *v != PROTOCOL_VERSION)
        .map(str::to_string);
    if let Some(asserted) = bad_version {
        return Some(McpResponse::error(
            request.id,
            McpError::unsupported_protocol_version(&asserted),
        ));
    }

    match request.method.as_str() {
        // SEP-2575: the canonical discovery RPC.
        "server/discover" => Some(handle_discover(request.id, info)),
        // SEP-2575 transition shim: `initialize` no longer exists in the
        // protocol but we honor it so 2024-11-05 clients keep working.
        // Same payload as `server/discover`, plus the legacy
        // `protocolVersion` field at the top level.
        "initialize" => Some(handle_initialize_legacy(request.id, info)),
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

/// SEP-2575 `server/discover`. Returns server identity, capabilities, and
/// instructions. Replaces the `initialize` handshake.
fn handle_discover(id: Value, info: &ServerInfo) -> McpResponse {
    McpResponse::success(id, discover_payload(info))
}

/// Transition shim for 2024-11-05 clients. Same content as
/// [`handle_discover`], plus the legacy `protocolVersion` field at the
/// top level (which `server/discover` carries inside `serverInfo` instead).
fn handle_initialize_legacy(id: Value, info: &ServerInfo) -> McpResponse {
    let mut payload = discover_payload(info);
    payload
        .as_object_mut()
        .expect("discover_payload returns an object")
        .insert(
            "protocolVersion".to_string(),
            Value::String(PROTOCOL_VERSION.to_string()),
        );
    McpResponse::success(id, payload)
}

fn discover_payload(info: &ServerInfo) -> Value {
    let mut payload = json!({
        "serverInfo": {
            "name": info.name,
            "version": info.version,
            "protocolVersion": PROTOCOL_VERSION,
        },
        "capabilities": {
            "tools": { "listChanged": false },
            "resources": { "listChanged": false, "subscribe": false },
            // SEP-2133: extensions framework. Empty map = no extensions
            // advertised yet. Per-extension support lands in follow-ups.
            "extensions": {},
        }
    });
    if let Some(text) = &info.instructions {
        payload
            .as_object_mut()
            .expect("payload is an object literal above")
            .insert("instructions".to_string(), Value::String(text.clone()));
    }
    payload
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

    McpResponse::success(
        id,
        json!({
            "tools": tools,
            // SEP-2549 CacheableResult: tool registrations are
            // compile-time-static — clients can cache the list for
            // TOOLS_LIST_TTL_MS. `private` because the visibility filter
            // is per-client (header-driven).
            "ttlMs": TOOLS_LIST_TTL_MS,
            "cacheScope": "private",
        }),
    )
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

    McpResponse::success(
        id,
        json!({
            "resources": resources,
            // SEP-2549 CacheableResult — same reasoning as tools/list.
            "ttlMs": RESOURCES_TTL_MS,
            "cacheScope": "private",
        }),
    )
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
                        }],
                        // SEP-2549 CacheableResult: schema contents derive
                        // from compile-time registrations and are stable
                        // across the server's lifetime.
                        "ttlMs": RESOURCES_TTL_MS,
                        "cacheScope": "private",
                    }),
                );
            }
        }
    }

    // SEP-2164: missing-resource errors use JSON-RPC standard
    // `-32602 Invalid Params` (not the legacy MCP `-32002`).
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

/// SEP-2106: tool input/output schemas use JSON Schema Draft 2020-12.
const JSON_SCHEMA_DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";

fn get_query_schema(query_id: &str) -> Option<String> {
    for reg in inventory::iter::<QueryRegistration> {
        if reg.query_id == query_id {
            let schema = json!({
                "$schema": JSON_SCHEMA_DRAFT,
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
                "$schema": JSON_SCHEMA_DRAFT,
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
                "$schema": JSON_SCHEMA_DRAFT,
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
                "$schema": JSON_SCHEMA_DRAFT,
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

    /// Build a request with `_meta.io.modelcontextprotocol/protocolVersion`
    /// set, exercising the SEP-2575 per-request protocol-version carriage.
    fn make_request_with_protocol_version(method: &str, version: &str) -> McpRequest {
        McpRequest {
            jsonrpc: "2.0".to_string(),
            id: Value::Number(1.into()),
            method: method.to_string(),
            params: Some(json!({
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": version,
                }
            })),
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

    // ─── MCP 2026-07-28 ───────────────────────────────────────────────

    #[tokio::test]
    async fn discover_returns_server_info_with_protocol_version() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo {
            name: "pulse-ctx".into(),
            version: "0.3.0".into(),
            instructions: Some("teach me".into()),
        };
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let resp = handle_request(make_request("server/discover"), &filter, &executor, &info)
            .await
            .expect("server/discover must return a response");
        let result = resp.result.expect("server/discover must succeed");
        assert_eq!(result["serverInfo"]["name"], json!("pulse-ctx"));
        assert_eq!(result["serverInfo"]["version"], json!("0.3.0"));
        assert_eq!(
            result["serverInfo"]["protocolVersion"],
            json!(PROTOCOL_VERSION),
            "server/discover must advertise the 2026-07-28 protocol version"
        );
        assert_eq!(result["instructions"], json!("teach me"));
        // SEP-2133: extensions map must be present even when empty.
        assert!(
            result["capabilities"]["extensions"].is_object(),
            "capabilities must advertise an extensions map per SEP-2133"
        );
    }

    #[tokio::test]
    async fn initialize_legacy_shim_carries_protocol_version_top_level() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let resp = handle_request(make_request("initialize"), &filter, &executor, &info)
            .await
            .expect("initialize shim must return a response");
        let result = resp.result.expect("initialize shim must succeed");
        // 2024-11-05 clients expect `protocolVersion` at the top level;
        // the shim must satisfy that.
        assert_eq!(
            result["protocolVersion"],
            json!(PROTOCOL_VERSION),
            "initialize legacy shim must keep protocolVersion at top level"
        );
        // And the new shape under serverInfo as well, so a tooling that
        // already moved to server/discover sees a consistent payload.
        assert_eq!(
            result["serverInfo"]["protocolVersion"],
            json!(PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn version_mismatch_in_meta_returns_unsupported_protocol_version_error() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let req = make_request_with_protocol_version("tools/list", "1999-01-01");
        let resp = handle_request(req, &filter, &executor, &info)
            .await
            .expect("must return a response");
        let err = resp.error.expect("must be an error");
        assert!(
            err.message.contains("UnsupportedProtocolVersionError"),
            "expected UnsupportedProtocolVersionError, got: {}",
            err.message
        );
        assert!(
            err.message.contains("1999-01-01"),
            "error must echo the asserted version"
        );
    }

    #[tokio::test]
    async fn matching_version_in_meta_dispatches_normally() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let req = make_request_with_protocol_version("tools/list", PROTOCOL_VERSION);
        let resp = handle_request(req, &filter, &executor, &info)
            .await
            .expect("must return a response");
        assert!(
            resp.error.is_none(),
            "matching version must not produce an UnsupportedProtocolVersionError"
        );
        let result = resp.result.expect("tools/list must succeed");
        assert!(result["tools"].is_array());
    }

    #[tokio::test]
    async fn tools_list_carries_cache_metadata() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let resp = handle_request(make_request("tools/list"), &filter, &executor, &info)
            .await
            .expect("must return a response");
        let result = resp.result.expect("tools/list must succeed");
        // SEP-2549 CacheableResult fields.
        assert!(
            result["ttlMs"].as_u64().is_some(),
            "tools/list must carry ttlMs per SEP-2549, got: {result}"
        );
        assert_eq!(
            result["cacheScope"],
            json!("private"),
            "visibility filter is per-client, so cacheScope must be private"
        );
    }

    #[tokio::test]
    async fn resources_list_carries_cache_metadata() {
        let filter = ClientFilters::allow_all();
        let info = ServerInfo::default();
        let client = std::sync::Arc::new(myko::client::MykoClient::new());
        let executor = Executor::Client(client);
        let resp = handle_request(make_request("resources/list"), &filter, &executor, &info)
            .await
            .expect("must return a response");
        let result = resp.result.expect("resources/list must succeed");
        assert!(result["ttlMs"].as_u64().is_some());
        assert_eq!(result["cacheScope"], json!("private"));
    }

    #[test]
    fn schema_emits_2020_12_draft_uri() {
        // Spot-check that the four schema getters use 2020-12. Per SEP-2106
        // they must be 2020-12 in 2026-07-28. We don't have a registered
        // entity here to look up, so just verify the constant.
        assert_eq!(
            JSON_SCHEMA_DRAFT, "https://json-schema.org/draft/2020-12/schema",
            "tool schemas must declare Draft 2020-12 per SEP-2106"
        );
    }
}
