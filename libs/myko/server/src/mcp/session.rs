//! MCP session lifecycle observation.
//!
//! HTTP MCP clients are correlated server-side by the `Mcp-Session-Id`
//! header assigned on the `initialize` response (Streamable HTTP
//! transport). Downstream code that wants to materialise per-session
//! state — e.g. a marshal-style coordination daemon that needs to model
//! every connected agent as an entity — installs an [`McpSessionObserver`]
//! via the server builder and receives lifecycle events as connections
//! come and go.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;

/// Minimum identifying metadata pulled out of the MCP `initialize`
/// request's `clientInfo` block. Other fields are deliberately not
/// surfaced here; observers that need them can pull from the raw
/// headers/body via future API additions.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
    pub title: Option<String>,
}

/// Lifecycle events fired for each MCP HTTP session.
#[derive(Clone)]
pub enum McpSessionEvent {
    /// A new HTTP MCP session has just completed `initialize`. The
    /// `session_id` is the value the server returned in the response's
    /// `Mcp-Session-Id` header — same string the client will echo on
    /// every subsequent request and on its SSE GET.
    Started {
        session_id: String,
        client_info: Option<ClientInfo>,
        user_agent: Option<String>,
    },
    /// The session's SSE channel has just opened. `channel` is the
    /// push sink — call `send_notification` on it to push a JSON-RPC
    /// notification frame into the agent's transcript.
    SseConnected {
        session_id: String,
        channel: Arc<McpSessionChannel>,
    },
    /// The HTTP MCP session has closed (either explicitly via DELETE, or
    /// because the SSE channel went away). Best-effort signal — clients
    /// that disappear without notice get cleaned up by an external
    /// reaper, not by this event.
    Ended { session_id: String },
    /// A non-initialize JSON-RPC request arrived on this session.
    /// Downstream reapers use this to keep HTTP-MCP sessions alive when
    /// the client doesn't (or hasn't yet) opened an SSE channel —
    /// Claude Code's HTTP-MCP transport only opens SSE on demand, so
    /// SSE-open is not a reliable liveness signal on its own. Fired
    /// before the request is dispatched; the observer should do only
    /// fast in-memory work (e.g. bump a `last_seen_at` field).
    Activity { session_id: String },
}

/// Push sink for the server side of an SSE channel. Owned by the
/// `handle_sse` loop; cloned references handed out via `SseConnected`
/// let observers push JSON-RPC notification frames toward the client.
///
/// Cheap to clone (it wraps an `mpsc::UnboundedSender`) and safe to keep
/// past the lifetime of the request — sends after the SSE loop has
/// exited return `false` rather than panicking.
pub struct McpSessionChannel {
    tx: mpsc::UnboundedSender<String>,
}

impl McpSessionChannel {
    pub(crate) fn new(tx: mpsc::UnboundedSender<String>) -> Self {
        Self { tx }
    }

    /// Frame a JSON-RPC notification and push it onto the SSE stream.
    /// Returns `false` when the underlying SSE connection has already
    /// gone away (the receiver was dropped). Callers should treat
    /// `false` as a signal to stop trying.
    pub fn send_notification(&self, method: &str, params: Value) -> bool {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let body = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let frame = format!("event: message\ndata: {body}\n\n");
        self.tx.send(frame).is_ok()
    }
}

/// Synchronous observer trait — kept simple so registering one doesn't
/// require pulling in tokio types or trait-async-fn. Observers should do
/// only fast in-memory work here and defer I/O to spawned tasks.
pub trait McpSessionObserver: Send + Sync {
    fn on_session_event(&self, event: McpSessionEvent);
}

/// Convenience: any `Fn(McpSessionEvent) + Send + Sync` becomes an
/// observer. Lets callers register a closure without defining a struct.
impl<F> McpSessionObserver for F
where
    F: Fn(McpSessionEvent) + Send + Sync,
{
    fn on_session_event(&self, event: McpSessionEvent) {
        self(event)
    }
}

/// Type alias used in the server builder for clarity.
pub type SharedObserver = Arc<dyn McpSessionObserver>;
