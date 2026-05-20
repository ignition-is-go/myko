//! MCP stdio server.
//!
//! Reads JSON-RPC requests from stdin, writes responses to stdout, and
//! ferries tool execution through a remote Myko server via `MykoClient`.
//!
//! The in-process HTTP/WS MCP endpoints use the same dispatch core but a
//! different [`Executor`]; see `mcp::http`, `mcp::ws`, and `mcp::dispatch`.

use std::{
    io::{self, BufRead, Write},
    sync::Arc,
};

use hyphae::Watchable;
use myko::{
    client::{ConnectionStatus, MykoClient},
    command::CommandRegistration,
    query::QueryRegistration,
    report::ReportRegistration,
};
use serde_json::Value;
use tokio::sync::mpsc;

use super::{
    dispatch::{self, ServerInfo},
    exec::Executor,
    filter::{VISIBILITY_ALLOW_ENV, ARGUMENTS_ENV, ClientFilters, VISIBILITY_DENY_ENV},
    types::{McpError, McpRequest, McpResponse},
};

/// MCP Server for Myko stdio transport.
///
/// Automatically exposes all registered queries, reports, and commands
/// through the MCP protocol.
pub struct McpServer {
    info: ServerInfo,
}

impl Default for McpServer {
    fn default() -> Self {
        Self::new()
    }
}

impl McpServer {
    /// Create a new MCP server with default settings.
    pub fn new() -> Self {
        Self {
            info: ServerInfo::default(),
        }
    }

    /// Create a new MCP server with custom name and version.
    pub fn with_info(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            info: ServerInfo {
                name: name.into(),
                version: version.into(),
            },
        }
    }

    /// Run the MCP server over stdio (blocking).
    ///
    /// Reads JSON-RPC requests from stdin and writes responses to stdout.
    /// Logs go to stderr. Connects to a Myko WebSocket server via the
    /// `MYKO_ADDRESS` env var (default `ws://localhost:5155`).
    pub fn run_stdio(&self) -> io::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async { self.run_stdio_async().await })
    }

    async fn run_stdio_async(&self) -> io::Result<()> {
        let myko_address =
            std::env::var("MYKO_ADDRESS").unwrap_or_else(|_| "ws://localhost:5155".to_string());

        eprintln!("[myko-mcp] Connecting to Myko at {}", myko_address);

        let client = Arc::new(MykoClient::new());
        client.set_address(Some(myko_address));

        let status_guard = client.connection_status().subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(addr) => {
                        eprintln!("[myko-mcp] Connected to {}", addr)
                    }
                    ConnectionStatus::Connecting(addr) => {
                        eprintln!("[myko-mcp] Connecting to {}", addr)
                    }
                    ConnectionStatus::Reconnecting(addr) => {
                        eprintln!("[myko-mcp] Reconnecting to {}", addr)
                    }
                    ConnectionStatus::Idle => eprintln!("[myko-mcp] Idle"),
                    ConnectionStatus::Disconnected => eprintln!("[myko-mcp] Disconnected"),
                }
            }
        });
        client.connection_status().own(status_guard);

        let executor = Arc::new(Executor::Client(client));
        let info = Arc::new(self.info.clone());

        // Stdio MCP can't carry per-request headers, so the same three
        // knobs as HTTP/WS come from env vars instead. Empty / unset =
        // permissive default.
        let filter = Arc::new(ClientFilters::from_strings(
            std::env::var(VISIBILITY_ALLOW_ENV).ok().as_deref(),
            std::env::var(VISIBILITY_DENY_ENV).ok().as_deref(),
            std::env::var(ARGUMENTS_ENV).ok().as_deref(),
        ));

        let (response_tx, mut response_rx) = mpsc::channel::<McpResponse>(32);

        // Stdin reader thread: read lines, parse, dispatch.
        let response_tx_clone = response_tx.clone();
        let executor_clone = executor.clone();
        let info_clone = info.clone();
        let filter_clone = filter.clone();
        std::thread::spawn(move || {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("[myko-mcp] stdin error: {}", e);
                        continue;
                    }
                };
                if line.is_empty() {
                    continue;
                }

                let request: McpRequest = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("[myko-mcp] Parse error: {}", e);
                        let response =
                            McpResponse::error(Value::Null, McpError::parse_error(e.to_string()));
                        let _ = response_tx_clone.blocking_send(response);
                        continue;
                    }
                };

                let response_tx = response_tx_clone.clone();
                let executor = executor_clone.clone();
                let info = info_clone.clone();
                let filter = filter_clone.clone();
                tokio::spawn(async move {
                    if let Some(response) =
                        dispatch::handle_request(request, &filter, &executor, &info).await
                    {
                        let _ = response_tx.send(response).await;
                    }
                });
            }
        });

        // Write responses to stdout.
        let mut stdout = io::stdout().lock();
        while let Some(response) = response_rx.recv().await {
            let json = serde_json::to_string(&response)?;
            writeln!(stdout, "{}", json)?;
            stdout.flush()?;
        }

        Ok(())
    }

    /// Get a summary of all registered items.
    pub fn summary(&self) -> McpSummary {
        let mut queries = Vec::new();
        let mut reports = Vec::new();
        let mut commands = Vec::new();

        for reg in inventory::iter::<QueryRegistration> {
            queries.push(QueryInfo {
                query_id: reg.query_id.to_string(),
                query_item_type: reg.query_item_type.to_string(),
            });
        }

        for reg in inventory::iter::<ReportRegistration> {
            reports.push(ReportInfo {
                report_id: reg.report_id.to_string(),
                output_type: reg.output_type.to_string(),
            });
        }

        for reg in inventory::iter::<CommandRegistration> {
            commands.push(CommandInfo {
                command_id: reg.command_id.to_string(),
                result_type: reg.result_type.to_string(),
            });
        }

        McpSummary {
            queries,
            reports,
            commands,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Summary Types
// ─────────────────────────────────────────────────────────────────────────────

/// Summary of registered Myko items.
#[derive(Debug, Clone)]
pub struct McpSummary {
    pub queries: Vec<QueryInfo>,
    pub reports: Vec<ReportInfo>,
    pub commands: Vec<CommandInfo>,
}

/// Query registration info.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub query_id: String,
    pub query_item_type: String,
}

/// Report registration info.
#[derive(Debug, Clone)]
pub struct ReportInfo {
    pub report_id: String,
    pub output_type: String,
}

/// Command registration info.
#[derive(Debug, Clone)]
pub struct CommandInfo {
    pub command_id: String,
    pub result_type: String,
}
