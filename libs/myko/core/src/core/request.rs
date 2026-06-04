//! Request context for tracing operations across the Myko system.
//!
//! [`RequestContext`] carries identity and tracing information through command,
//! query, and report execution. It enables:
//!
//! - **Transaction correlation**: All operations in a request share the same `tx`
//! - **Call tracing**: `lineage` tracks the chain of operations (e.g., `["client", "CreateScene", "CreateBinding"]`)
//! - **Client identification**: `client_id` identifies the WebSocket connection that initiated the request
//! - **Server identification**: `host_id` identifies which server is processing the request
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use uuid::Uuid;
//! use myko::request::RequestContext;
//!
//! // Create initial context from WebSocket request
//! let tx: Arc<str> = "tx-1".into();
//! let client_id: Arc<str> = "client-1".into();
//! let host_id = Uuid::new_v4();
//! let ctx = RequestContext::from_client(tx, client_id, host_id);
//!
//! // Extend lineage for nested operations
//! let child_ctx = ctx.child("CreateBinding");
//! // child_ctx.lineage = ["client", "CreateBinding"]
//! ```

use std::sync::Arc;

use uuid::Uuid;

use crate::entities::client::ClientId;

/// Context that propagates through request processing.
///
/// Created when a request arrives via WebSocket and flows through
/// command execution, query subscriptions, and report generation.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Transaction ID - same across all operations in one request.
    /// Used to correlate logs, events, and responses.
    pub tx: Arc<str>,

    /// Client ID of the WebSocket connection that initiated this request.
    /// `None` for internal operations (sagas, startup tasks).
    pub client_id: Option<ClientId>,

    /// MCP Streamable-HTTP `Mcp-Session-Id` of the request, when the
    /// caller arrived via `/myko/mcp` POST. `None` for WebSocket
    /// callers (they use `client_id`) and for internal operations.
    /// Commands that need to identify the caller without a live
    /// `Client` entity (e.g. an HTTP-MCP agent calling `send_message`)
    /// branch on this.
    pub mcp_session_id: Option<Arc<str>>,

    /// Call chain tracking the path of execution.
    /// Starts with `["client"]` for WebSocket requests or `["saga", "SagaName"]` for sagas.
    /// Extended via `child()` for nested operations.
    pub lineage: Vec<Arc<str>>,

    /// Server that received the original request.
    pub host_id: Uuid,

    /// ISO timestamp when the request started.
    pub created_at: String,

    /// Windback timestamp for historical state viewing.
    /// When set, queries should return state as of this ISO timestamp.
    /// `None` means the client is viewing live state.
    pub windback: Option<Arc<str>>,
}

impl RequestContext {
    /// Create a new RequestContext with explicit values.
    pub fn new(
        tx: Arc<str>,
        client_id: Option<Arc<str>>,
        lineage: Vec<Arc<str>>,
        host_id: Uuid,
        created_at: String,
    ) -> Self {
        Self {
            tx,
            client_id: client_id.map(Into::into),
            lineage,
            host_id,
            created_at,
            mcp_session_id: None,
            windback: None,
        }
    }

    /// Create a context for a client-initiated request.
    ///
    /// Sets lineage to `["client"]` and created_at to current time.
    /// Use `from_client_with_windback` to include windback state.
    pub fn from_client(tx: Arc<str>, client_id: Arc<str>, host_id: Uuid) -> Self {
        Self {
            tx,
            client_id: Some(client_id.into()),
            lineage: vec![Arc::from("client")],
            host_id,
            created_at: chrono::Utc::now().to_rfc3339(),
            mcp_session_id: None,
            windback: None,
        }
    }

    /// Create a context for a client-initiated request with windback state.
    ///
    /// Sets lineage to `["client"]` and created_at to current time.
    pub fn from_client_with_windback(
        tx: Arc<str>,
        client_id: Arc<str>,
        host_id: Uuid,
        windback: Option<Arc<str>>,
    ) -> Self {
        Self {
            tx,
            client_id: Some(client_id.into()),
            mcp_session_id: None,
            lineage: vec![Arc::from("client")],
            host_id,
            created_at: chrono::Utc::now().to_rfc3339(),
            windback,
        }
    }

    /// Create a context for an internal operation (no client).
    ///
    /// Used for saga-initiated operations, startup tasks, etc.
    pub fn internal(tx: Arc<str>, host_id: Uuid, origin: &str) -> Self {
        Self {
            tx,
            client_id: None,
            lineage: vec![Arc::from(origin)],
            host_id,
            created_at: chrono::Utc::now().to_rfc3339(),
            mcp_session_id: None,
            windback: None,
        }
    }

    /// Create a child context with extended lineage.
    ///
    /// Used when making sub-operations (nested commands, queries from reports, etc.)
    /// to track the call chain. Preserves the parent's windback state.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use uuid::Uuid;
    /// use myko::request::RequestContext;
    ///
    /// let tx: Arc<str> = "tx-1".into();
    /// let client_id: Arc<str> = "client-1".into();
    /// let host_id = Uuid::new_v4();
    /// let parent = RequestContext::from_client(tx, client_id, host_id);
    /// let child = parent.child("CreateBinding");
    /// assert_eq!(child.lineage.len(), 2);
    /// assert_eq!(child.lineage[0].as_ref(), "client");
    /// assert_eq!(child.lineage[1].as_ref(), "CreateBinding");
    /// ```
    pub fn child(&self, operation: &str) -> Self {
        let mut lineage = self.lineage.clone();
        lineage.push(Arc::from(operation));
        Self {
            tx: self.tx.clone(),
            client_id: self.client_id.clone(),
            mcp_session_id: self.mcp_session_id.clone(),
            lineage,
            host_id: self.host_id,
            created_at: self.created_at.clone(),
            windback: self.windback.clone(),
        }
    }

    /// Builder-style: attach an `Mcp-Session-Id` (Streamable HTTP
    /// transport correlator) to this context. Used by the HTTP MCP
    /// executor when dispatching a command on behalf of an
    /// HTTP-MCP-connected caller, so command handlers can identify
    /// the caller without a `client_id`.
    pub fn with_mcp_session_id(mut self, mcp_session_id: Arc<str>) -> Self {
        self.mcp_session_id = Some(mcp_session_id);
        self
    }

    /// Check if this context is in windback mode.
    pub fn is_windback(&self) -> bool {
        self.windback.is_some()
    }

    /// Get the windback timestamp if set.
    pub fn windback(&self) -> Option<&str> {
        self.windback.as_deref()
    }

    /// Get the transaction ID as a string slice.
    pub fn tx(&self) -> &str {
        &self.tx
    }

    /// Get the client ID if present.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Get the lineage as a string for logging.
    pub fn lineage_string(&self) -> String {
        self.lineage
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<_>>()
            .join(" → ")
    }
}
