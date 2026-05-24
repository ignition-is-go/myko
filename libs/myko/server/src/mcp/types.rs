//! MCP protocol types for JSON-RPC communication.
//!
//! Supports the MCP **2026-07-28** revision. The handshake (`initialize` +
//! `notifications/initialized`) and `Mcp-Session-Id` header are removed
//! per SEP-2575 and SEP-2567; protocol version, client info, and client
//! capabilities now travel in `_meta` on every request via reverse-DNS
//! keys (`io.modelcontextprotocol/protocolVersion`,
//! `io.modelcontextprotocol/clientInfo`,
//! `io.modelcontextprotocol/clientCapabilities`). The new
//! [`server/discover`][dispatch] RPC method advertises server identity +
//! capabilities + instructions on demand.
//!
//! For the transition window the dispatcher still accepts `initialize` as
//! a backwards-compat shim returning the same payload `server/discover`
//! does, so 2024-11-05 clients keep working until they migrate.
//!
//! [dispatch]: super::dispatch

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// MCP protocol version advertised by this server and validated against
/// `_meta.io.modelcontextprotocol/protocolVersion` on inbound requests.
pub const PROTOCOL_VERSION: &str = "2026-07-28";

/// Reverse-DNS key under which clients send their advertised protocol
/// version in each request's `_meta` map (SEP-2575).
pub const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";

/// Reverse-DNS key under which clients send `{name, version}` per request.
pub const META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";

/// Reverse-DNS key under which clients send their advertised capabilities.
pub const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";

/// MCP JSON-RPC request.
#[derive(Debug, Deserialize)]
pub struct McpRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

impl McpRequest {
    /// Extract a `_meta` value by key from this request's `params._meta`,
    /// if present. Returns `None` when params is missing, params has no
    /// `_meta` object, or the key is absent.
    pub fn meta(&self, key: &str) -> Option<&Value> {
        self.params.as_ref()?.get("_meta")?.as_object()?.get(key)
    }

    /// Protocol version asserted by the client in `_meta` per SEP-2575.
    /// `None` when the key is absent (either a 2024-11-05 client that
    /// expects the handshake-driven version negotiation, or a request
    /// that genuinely omits it).
    pub fn client_protocol_version(&self) -> Option<&str> {
        self.meta(META_PROTOCOL_VERSION).and_then(Value::as_str)
    }
}

/// MCP JSON-RPC response.
#[derive(Debug, Serialize)]
pub struct McpResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpError>,
}

impl McpResponse {
    /// Create a success response.
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Create an error response.
    pub fn error(id: Value, error: McpError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// MCP JSON-RPC error.
#[derive(Debug, Serialize)]
pub struct McpError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl McpError {
    /// Standard JSON-RPC error codes.
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;

    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self {
            code: Self::PARSE_ERROR,
            message: msg.into(),
            data: None,
        }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: Self::METHOD_NOT_FOUND,
            message: format!("Method not found: {}", method),
            data: None,
        }
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self {
            code: Self::INVALID_PARAMS,
            message: msg.into(),
            data: None,
        }
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self {
            code: Self::INTERNAL_ERROR,
            message: msg.into(),
            data: None,
        }
    }

    /// MCP 2026-07-28 SEP-2575 `UnsupportedProtocolVersionError`.
    /// Returned when the version asserted in `_meta` does not match
    /// `PROTOCOL_VERSION`.
    pub fn unsupported_protocol_version(asserted: &str) -> Self {
        Self {
            code: Self::INVALID_PARAMS,
            message: format!(
                "UnsupportedProtocolVersionError: client asserted '{}', server supports '{}'",
                asserted, PROTOCOL_VERSION
            ),
            data: Some(serde_json::json!({
                "asserted": asserted,
                "supported": [PROTOCOL_VERSION],
            })),
        }
    }
}

/// MCP tool definition.
#[derive(Debug, Clone, Serialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// MCP resource definition.
#[derive(Debug, Clone, Serialize)]
pub struct McpResource {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

/// MCP resource contents.
#[derive(Debug, Clone, Serialize)]
pub struct McpResourceContents {
    pub uri: String,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

/// MCP tool call result content.
#[derive(Debug, Clone, Serialize)]
pub struct McpToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

impl McpToolContent {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content_type: "text".to_string(),
            text: text.into(),
        }
    }
}

/// MCP server capabilities.
#[derive(Debug, Clone, Serialize, Default)]
pub struct McpCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<Value>,
}

/// MCP server info.
#[derive(Debug, Clone, Serialize)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
}
