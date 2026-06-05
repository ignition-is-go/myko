//! Custom plain-HTTP route registry.
//!
//! The MCP endpoint (`/myko/mcp`) is the structured, tool-oriented
//! surface. Some downstream integrations need a *dumb* HTTP endpoint
//! instead — one a trivial `curl` one-liner can POST a raw body to and
//! read a `text/plain` response from, with all logic server-side.
//!
//! marshal uses this for its Claude Code hook endpoints (`/hook/*`):
//! the hook command is just `curl --data-binary @- $URL/hook/...`, and
//! the daemon does the register / fetch / ack / format work, returning
//! the text to print into the agent's context. No client-side scripts,
//! no per-platform port — the logic lives in one place.
//!
//! Handlers are registered by exact path. They run synchronously (the
//! `CellServerCtx` store operations they use are sync) and return a
//! status + content-type + body.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use myko::server::CellServerCtx;

/// One inbound custom-HTTP request handed to a handler.
pub struct CustomHttpRequest {
    pub method: String,
    /// Path with the query string stripped (the routing key).
    pub path: String,
    /// Raw query string (everything after `?`), empty if none. Handlers
    /// parse it themselves — marshal's hooks read `host` / `operator`
    /// the client expanded locally into the curl URL.
    pub query: String,
    /// Request body bytes (e.g. the raw Claude Code hook JSON).
    pub body: Vec<u8>,
    /// Server context for store access.
    pub ctx: Arc<CellServerCtx>,
}

/// What a handler returns.
pub struct CustomHttpResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

impl CustomHttpResponse {
    /// `200 OK` `text/plain` with the given body (may be empty).
    pub fn text(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; charset=utf-8".to_string(),
            body: body.into(),
        }
    }

    /// `200 OK` with an empty body — the common "did the work, nothing
    /// to print into context" case (SessionEnd, an empty inbox).
    pub fn empty() -> Self {
        Self::text(String::new())
    }

    /// A non-200 status with a short plain-text message.
    pub fn status(code: u16, message: impl Into<String>) -> Self {
        Self {
            status: code,
            content_type: "text/plain; charset=utf-8".to_string(),
            body: message.into(),
        }
    }
}

/// Handler closure. `CellServerCtx` arrives on the request; any other
/// state (e.g. marshal's `LastSeen` map) is captured by the closure.
pub type CustomHttpHandler = Arc<dyn Fn(CustomHttpRequest) -> CustomHttpResponse + Send + Sync>;

/// Registry of custom plain-HTTP routes, keyed by exact path. Cloned
/// cheaply (wraps `Arc<Mutex<…>>`); held by `CellServer` and consulted
/// by the router for POSTs that don't match a built-in route.
#[derive(Clone, Default)]
pub struct CustomHttpRegistry {
    inner: Arc<Mutex<HashMap<String, CustomHttpHandler>>>,
}

impl CustomHttpRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the handler for an exact path.
    pub fn register(&self, path: impl Into<String>, handler: CustomHttpHandler) {
        self.inner
            .lock()
            .expect("CustomHttpRegistry mutex poisoned")
            .insert(path.into(), handler);
    }

    /// Look up the handler for a path. The router calls this for POSTs
    /// that fall through the built-in routes.
    pub fn get(&self, path: &str) -> Option<CustomHttpHandler> {
        self.inner
            .lock()
            .expect("CustomHttpRegistry mutex poisoned")
            .get(path)
            .cloned()
    }
}
