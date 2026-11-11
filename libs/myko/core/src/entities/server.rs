use std::sync::Arc;

use hyphae::{LeftJoinExt, MapExt};

use crate::{
    entities::client::{ClientServerIdRelation, GetAllClients},
    prelude::*,
    report::{ReportContext, ReportHandler},
};

#[myko_item]
#[derive(Eq)]
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
    fn test_entity(ctx: QueryTestContext<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.query_context.req.host_id.to_string();
        item_id == host_id
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn build_view(
        ctx: QueryBuildArgs<Self>,
    ) -> Option<impl MapQuery<Key = Arc<str>, Value = Arc<dyn AnyItem>>>
    where
        Self: Send + Sync + 'static,
    {
        let host_id: Arc<str> = ctx
            .query_context
            .query_context
            .req
            .host_id
            .to_string()
            .into();
        let store = ctx
            .query_context
            .registry()
            .get_or_create(Server::ENTITY_NAME_STATIC);
        Some(crate::query::build_ids_source_map(&store, &[host_id]))
    }
}

#[myko_query(Server)]
pub struct GetPeerServers {}

impl QueryHandler for GetPeerServers {
    fn test_entity(ctx: QueryTestContext<Self>) -> bool {
        let item_id = ctx.item.id.to_string();
        let host_id = ctx.query_context.req.host_id.to_string();
        item_id != host_id
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn build_view(
        ctx: QueryBuildArgs<Self>,
    ) -> Option<impl MapQuery<Key = Arc<str>, Value = Arc<dyn AnyItem>>>
    where
        Self: Send + Sync + 'static,
    {
        let host_id = ctx.query_context.query_context.req.host_id.to_string();
        let store = ctx
            .query_context
            .registry()
            .get_or_create(Server::ENTITY_NAME_STATIC)
            .as_ref()
            .clone()
            .lock();
        Some(store.select_by(move |id, _server| id.as_ref() != host_id))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Manual Report Example: ServerStats
// ─────────────────────────────────────────────────────────────────────────────

/// Server statistics including connected client count.
/// This is a manually implemented report demonstrating reactive query subscriptions.
#[myko_macros::myko_report_output]
#[derive(Eq)]
pub struct ServerStatsOutput {
    /// The server entity (if found)
    pub server: Option<Arc<Server>>,
    /// Number of clients connected to this server
    pub client_count: usize,
    /// Server uptime in seconds (computed from `started_at`)
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

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let host_id: Arc<str> = ctx.host_id().to_string().into();
        // Canonical string keys match `IdFor<Server>::MapKey`; the direct join
        // projection reads the shared relationship index without cloning clients
        // into an intermediate joined value.
        let stats_by_server = ctx
            .query_map_by_str(GetConnectedServer {})
            .left_join_fk::<ClientServerIdRelation, _>(ctx.query_map_by_str(GetAllClients {}))
            .map_joined_values(|_server_id, server, clients| (server.clone(), clients.len()))
            .materialize();

        stats_by_server.get(&host_id).map(|stats| {
            let Some((server, client_count)) = stats else {
                return Arc::new(ServerStatsOutput {
                    server: None,
                    client_count: 0,
                    uptime_seconds: None,
                });
            };
            let uptime_seconds = chrono::DateTime::parse_from_rfc3339(&server.started_at)
                .ok()
                .map(|started| {
                    let now = chrono::Utc::now();
                    now.signed_duration_since(started.with_timezone(&chrono::Utc))
                        .num_seconds()
                });
            Arc::new(ServerStatsOutput {
                server: Some(server.clone()),
                client_count: *client_count,
                uptime_seconds,
            })
        })
    }
}
