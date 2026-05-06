//! MCP (Model Context Protocol) server module for Myko.
//!
//! This module provides an MCP server that automatically exposes:
//! - **Resources**: Queries and reports as readable resources
//! - **Tools**: Commands as executable tools
//!
//! All registrations are discovered automatically via the inventory system,
//! so any type decorated with `#[myko_query]`, `#[myko_report]`, or `#[myko_command]`
//! will be available through the MCP interface.
//!
//! ## Usage
//!
//! For Rship, use the `rship-mcp` binary which includes all entity registrations:
//!
//! ```bash
//! cargo run -p rship-mcp
//! ```
//!
//! Or use programmatically in your own crate:
//!
//! ```rust,ignore
//! // Link your entities crate to register them
//! my_entities::link();
//!
//! // Start the MCP server
//! let server = myko::mcp::McpServer::new();
//! server.run_stdio()?;
//! ```
//!
//! ## Gating exposed tools
//!
//! Three Cargo features control which categories the server advertises:
//!
//! - `mcp-queries` — exposes every registered query
//! - `mcp-reports` — exposes every registered report
//! - `mcp-commands` — exposes every registered command
//!
//! All three are enabled by default. Embedders whose MCP client enforces a
//! tool-count limit (or who want to restrict the server to read-only
//! operations) can disable categories individually:
//!
//! ```toml
//! # Cargo.toml — expose only queries
//! myko-server = { version = "...", default-features = false, features = ["mcp-queries"] }
//! ```
//!
//! Disabled categories are dropped from `tools/list` and `resources/list`,
//! and calls/reads against them return method-not-found at compile time —
//! no runtime cost.
//!
//! ## MCP Client Configuration
//!
//! Add to Claude Desktop or other MCP clients:
//!
//! ```json
//! {
//!   "mcpServers": {
//!     "rship": {
//!       "command": "cargo",
//!       "args": ["run", "-p", "rship-mcp"],
//!       "cwd": "/path/to/rship"
//!     }
//!   }
//! }
//! ```
//!
//! ## Protocol
//!
//! The server implements the MCP protocol (2024-11-05) over stdio using JSON-RPC.
//!
//! ### Resources
//!
//! - `myko://schema/query/{query_id}` - JSON schema for a query
//! - `myko://schema/report/{report_id}` - JSON schema for a report
//! - `myko://schema/command/{command_id}` - JSON schema for a command
//!
//! ### Tools
//!
//! Each registered command becomes an MCP tool with the command's JSON schema
//! as the input schema.

mod server;
mod types;

pub use server::McpServer;
pub use types::*;
