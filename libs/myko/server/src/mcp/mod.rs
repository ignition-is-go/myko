//! MCP (Model Context Protocol) for Myko.
//!
//! The server hosts MCP at `/myko/mcp` over three content-negotiated
//! transports (HTTP POST, WebSocket, SSE) — see [`http`] and [`ws`].
//! [`dispatch`] is the transport-agnostic JSON-RPC core, [`exec`] is the
//! tool executor abstraction (in-process or remote client), and [`filter`]
//! parses per-client tool filters from request headers.
//!
//! Every type decorated with `#[myko_query]`, `#[myko_report]`, or
//! `#[myko_command]` is auto-discovered via `inventory` and exposed as an
//! MCP tool / resource.
//!
//! ## Tool filtering
//!
//! Send `X-Myko-Tools-Allow` / `X-Myko-Tools-Deny` headers on the request
//! (or at WS handshake) to restrict which tools an MCP client can see and
//! call. Patterns are globs: `*`, `prefix*`, `*suffix`, exact. Deny wins.
//!
//! ## Protocol
//!
//! Implements MCP 2024-11-05 JSON-RPC. Resources:
//!
//! - `myko://schema/query/{query_id}`
//! - `myko://schema/report/{report_id}`
//! - `myko://schema/command/{command_id}`
//!
//! ## Legacy stdio
//!
//! [`McpServer::run_stdio`] is a transitional stdio transport that wraps a
//! `MykoClient` and connects out over WebSocket. It will be removed once
//! all consumers have migrated to the in-server `/myko/mcp` endpoint.

pub mod dispatch;
pub mod exec;
pub mod filter;
pub mod http;
mod server;
mod types;
pub mod ws;

pub use server::McpServer;
pub use types::*;
