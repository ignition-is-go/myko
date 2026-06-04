//! MCP (Model Context Protocol) for Myko.
//!
//! The server hosts MCP at `/myko/mcp` over three content-negotiated
//! transports (HTTP POST, WebSocket, SSE) — see [`http`] and [`ws`].
//! [`dispatch`] is the transport-agnostic JSON-RPC core, [`exec`] is the
//! tool executor abstraction (in-process or remote client), and [`filter`]
//! parses per-client filters from request headers (or env vars on stdio).
//!
//! Every type decorated with `#[myko_query]`, `#[myko_view]`,
//! `#[myko_report]`, or `#[myko_command]` is auto-discovered via
//! `inventory` and exposed as an MCP tool with prefix `query:`, `view:`,
//! `report:`, or `command:`. The tool's input schema also surfaces as a
//! resource at `myko://schema/<kind>/<id>`.
//!
//! ## Client filters
//!
//! Two layers (see [`filter::ClientFilters`]):
//!
//! - **Visibility** — name allow/deny via
//!   `X-Myko-Tool-Visibility-Allow` / `X-Myko-Tool-Visibility-Deny`
//!   headers (or `MYKO_MCP_TOOL_VISIBILITY_{ALLOW,DENY}` env vars on
//!   stdio). Patterns are globs: `*`, `prefix*`, `*suffix`, exact.
//!   Denial → MCP **Protocol Error** (`-32602`, "Unknown tool: …").
//!
//! - **Callability** — per-tool / per-arg JSON value lists via
//!   `X-Myko-Tool-Callable-Allow` / `X-Myko-Tool-Callable-Deny`
//!   (or the matching `MYKO_MCP_TOOL_CALLABLE_*` env vars). Denial →
//!   MCP **Tool Execution Error** (`isError: true` content carrying a
//!   short reason).
//!
//! Both layers compose AND; deny wins inside each layer.
//!
//! ## Protocol
//!
//! Implements MCP 2024-11-05 JSON-RPC (compatible with the 2025-06-18
//! error-handling conventions).
//!
//! ## Legacy stdio
//!
//! [`McpServer::run_stdio`] is a transitional stdio transport that wraps a
//! `MykoClient` and connects out over WebSocket. It will be removed once
//! all consumers have migrated to the in-server `/myko/mcp` endpoint.

pub mod custom;
pub mod dispatch;
pub mod exec;
pub mod filter;
pub mod http;
mod server;
pub mod session;
mod types;
pub mod ws;

pub use custom::{
    CustomMcpRegistry, CustomResource, CustomResourceContext, CustomResourceHandler, CustomTool,
    CustomToolHandler,
};
pub use server::McpServer;
pub use session::{
    ClientInfo, McpSessionChannel, McpSessionEvent, McpSessionObserver, SharedObserver,
};
pub use types::*;
