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
//! ## Filtering exposed tools (`mcp-tool-filter` feature)
//!
//! By default every registered query, report, and command is exposed. Enable
//! the `mcp-tool-filter` Cargo feature to opt in to [`McpServer::with_filter`],
//! which restricts the surface — handy when an MCP client has a tool-count
//! limit, or when only a subset of operations should be addressable from a
//! given embedding context.
//!
//! The filter receives the full tool name (`query:<id>`, `report:<id>`,
//! `command:<id>`) and is consulted for both `tools/list` and `tools/call`, plus
//! the corresponding schema resources. Built-in tools are not filtered.
//!
//! ```rust,ignore
//! // Cargo.toml: myko-server = { ..., features = ["mcp-tool-filter"] }
//! let server = myko::mcp::McpServer::new()
//!     .with_filter(|name| name.starts_with("query:") || name == "command:CreateScene");
//! server.run_stdio()?;
//! ```
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
#[cfg(feature = "mcp-tool-filter")]
pub use server::ToolFilter;
pub use types::*;
