use futures::StreamExt;

use crate::combine_latest;
use crate::entities::client::GetAllClients;
use crate::prelude::*;
use crate::report::{ReportContext, ReportHandler};
use crate::{self as myko_rs};

#[myko_item]
pub struct Server {
    pub version: String,
    #[searchable]
    pub address: String,
    pub port: u16,
    pub started_at: String, // ISO DateTime
}

#[myko_query(Server)]
pub struct GetConnectedServer {}

impl QueryHandler for GetConnectedServer {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.server_ctx.host_id.to_string();
        item_id == host_id
    }
}

#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.server_ctx.host_id.to_string();
        item_id != host_id
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Manual Report Example: ServerStats
// ─────────────────────────────────────────────────────────────────────────────

/// Server statistics including connected client count.
/// This is a manually implemented report demonstrating reactive query subscriptions.
#[myko_macros::myko_report_output]
pub struct ServerStatsOutput {
    /// The server entity (if found)
    pub server: Option<Server>,
    /// Number of clients connected to this server
    pub client_count: usize,
    /// Server uptime in seconds (computed from started_at)
    pub uptime_seconds: Option<i64>,
}

/// Report that returns current server statistics.
///
/// This report demonstrates:
/// - Subscribing to multiple queries (`GetConnectedServer`, `GetAllClients`)
/// - Combining query results reactively
/// - Computing derived values (uptime)
///
/// # Example
///
/// ```rust,ignore
/// // Subscribe to server stats
/// let stats_stream = client.watch_report::<ServerStats, ServerStatsOutput>(&ServerStats::new());
///
/// // Each emission contains the latest server info and client count
/// while let Some(stats) = stats_stream.next().await {
///     println!("Server: {:?}, Clients: {}", stats.server, stats.client_count);
/// }
/// ```
#[myko_macros::myko_report(ServerStatsOutput)]
pub struct ServerStats {}

impl ReportHandler for ServerStats {
    type Output = ServerStatsOutput;

    fn compute(&self, ctx: ReportContext) -> crate::report::ReportStream<Self::Output> {
        // Combine server and client streams - emit whenever either changes
        combine_latest!(
            ctx.query(GetConnectedServer {}),
            ctx.query(GetAllClients {}),
        )
        .map(|(servers, clients)| {
            let server = servers.into_iter().next();

            // Compute uptime if we have server info
            let uptime_seconds = server.as_ref().and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.started_at)
                    .ok()
                    .map(|started| {
                        let now = chrono::Utc::now();
                        (now - started.with_timezone(&chrono::Utc)).num_seconds()
                    })
            });

            ServerStatsOutput {
                server,
                client_count: clients.len(),
                uptime_seconds,
            }
        })
        .boxed()
    }
}
