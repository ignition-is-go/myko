//! Custom MCP tool and resource registrations.
//!
//! `#[myko_query]` / `#[myko_command]` / `#[myko_report]` declarations
//! auto-derive an MCP tool surface that mirrors the underlying entity
//! shape (e.g. `query_GetAllSessions`, `command_SendMessage`). That
//! shape is great for generic exploration but uncomfortable as a
//! human-facing curated surface — tools like `roster` / `send_message`
//! / `whoami` are friendlier names with friendlier argument shapes
//! than what the macro can synthesise.
//!
//! This module lets downstream servers (marshal-daemon, pulse-ctx, …)
//! register their own MCP tools and resources alongside the
//! auto-derived ones. Custom registrations are checked first in
//! `tools/list` and `tools/call`; the auto-derived surface remains
//! visible underneath for power users.

use std::sync::{Arc, Mutex};

use myko::{command::CommandContext, server::CellServerCtx};
use serde_json::Value;

/// Handler closure for a custom MCP tool. Called from `tools/call`
/// with the deserialized `arguments` object and a fresh `CommandContext`
/// (with `mcp_session_id` set when the caller arrived via HTTP MCP).
pub type CustomToolHandler =
    Arc<dyn Fn(Value, CommandContext) -> Result<Value, String> + Send + Sync>;

/// Per-call context handed to a `CustomResourceHandler`. Carries the
/// `Mcp-Session-Id` of the HTTP-MCP caller (if any) so curated
/// resources like `marshal://whoami` can identify the reader without a
/// separate roundtrip.
#[derive(Clone)]
pub struct CustomResourceContext {
    pub ctx: Arc<CellServerCtx>,
    /// `Mcp-Session-Id` of the calling HTTP-MCP session, when known.
    /// `None` for WS-MCP / sagas / internal callers.
    pub caller_session_id: Option<Arc<str>>,
}

/// Handler closure for a custom MCP resource. Called from
/// `resources/read` with the requested URI; returns the resource body
/// as a UTF-8 string (the dispatch wraps it with the registered
/// `mime_type` per the MCP `resources/read` response shape).
pub type CustomResourceHandler =
    Arc<dyn Fn(&str, CustomResourceContext) -> Result<String, String> + Send + Sync>;

#[derive(Clone)]
pub struct CustomTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub handler: CustomToolHandler,
}

#[derive(Clone)]
pub struct CustomResource {
    pub uri: String,
    pub name: String,
    pub description: String,
    pub mime_type: String,
    pub handler: CustomResourceHandler,
}

/// Shared registry of custom tools and resources. Cloned cheaply (it
/// wraps an `Arc<Mutex<…>>`). Held by `CellServer` and threaded into
/// the `Executor::InProcess` for HTTP/POST + WS-MCP dispatch.
#[derive(Clone, Default)]
pub struct CustomMcpRegistry {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    tools: Vec<CustomTool>,
    resources: Vec<CustomResource>,
}

impl CustomMcpRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_tool(&self, tool: CustomTool) {
        let mut g = self.inner.lock().expect("CustomMcpRegistry mutex poisoned");
        g.tools.retain(|t| t.name != tool.name);
        g.tools.push(tool);
    }

    pub fn register_resource(&self, resource: CustomResource) {
        let mut g = self.inner.lock().expect("CustomMcpRegistry mutex poisoned");
        g.resources.retain(|r| r.uri != resource.uri);
        g.resources.push(resource);
    }

    /// Snapshot the tool list — used by `tools/list`.
    pub fn tools(&self) -> Vec<CustomTool> {
        self.inner
            .lock()
            .expect("CustomMcpRegistry mutex poisoned")
            .tools
            .clone()
    }

    /// Snapshot the resource list — used by `resources/list`.
    pub fn resources(&self) -> Vec<CustomResource> {
        self.inner
            .lock()
            .expect("CustomMcpRegistry mutex poisoned")
            .resources
            .clone()
    }

    /// Look up a tool by name — used by `tools/call`.
    pub fn tool(&self, name: &str) -> Option<CustomTool> {
        self.inner
            .lock()
            .expect("CustomMcpRegistry mutex poisoned")
            .tools
            .iter()
            .find(|t| t.name == name)
            .cloned()
    }

    /// Look up a resource by URI — used by `resources/read`. Performs
    /// an exact-prefix match against the registered URI ignoring query
    /// string, so `marshal://messages?inbox=true` resolves to a
    /// `marshal://messages` registration. The handler receives the
    /// original URI verbatim so it can parse query params itself.
    pub fn resource(&self, uri: &str) -> Option<CustomResource> {
        let path = uri.split('?').next().unwrap_or(uri);
        self.inner
            .lock()
            .expect("CustomMcpRegistry mutex poisoned")
            .resources
            .iter()
            .find(|r| r.uri == path)
            .cloned()
    }
}
