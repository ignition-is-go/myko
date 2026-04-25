use std::sync::Arc;

use hyphae::{JoinExt, MapExt, Pipeline, flat};

use crate::{
    entities::client::GetAllClients,
    prelude::*,
    report::{ReportContext, ReportHandler},
};

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
    fn test_entity(ctx: QueryTestCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.query_context.req.host_id.to_string();
        item_id == host_id
    }
}

#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryTestCtx<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.query_context.req.host_id.to_string();
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
    pub server: Option<Arc<Server>>,
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
/// ```text
/// // Client-side usage:
/// let cell = client.watch_report::<ServerStats, ServerStatsOutput>(ServerStats {});
///
/// // `cell` updates whenever server/client state changes.
/// // Read current value:
/// let latest = cell.get();
///
/// // Or subscribe reactively:
/// let _guard = cell.subscribe(|signal| {
///   // handle Signal::Value(Some(ServerStatsOutput { ... }))
/// });
/// ```
#[myko_macros::myko_report(ServerStatsOutput)]
pub struct ServerStats {}

impl ReportHandler for ServerStats {
    type Output = ServerStatsOutput;

    fn compute(&self, ctx: ReportContext) -> impl Pipeline<Arc<Self::Output>> {
        // Combine server and client cells - emit whenever either changes
        ctx.query_map(GetConnectedServer {})
            .entries()
            .join(&ctx.query_map(GetAllClients {}).entries())
            .map(flat!(|servers, clients| {
                let server = servers.first().map(|(_, server)| server.clone());

                // Compute uptime if we have server info
                let uptime_seconds = server.as_ref().and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(&s.started_at)
                        .ok()
                        .map(|started| {
                            let now = chrono::Utc::now();
                            (now - started.with_timezone(&chrono::Utc)).num_seconds()
                        })
                });

                Arc::new(ServerStatsOutput {
                    server,
                    client_count: clients.len(),
                    uptime_seconds,
                })
            }))
    }
}
