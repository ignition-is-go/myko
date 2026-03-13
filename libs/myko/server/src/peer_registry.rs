//! Peer registry for the cell-based server.
//!
//! Convergence model:
//! - Desired peers are derived from `GetAllServers` (minus self).
//! - Actual peers are active RAII handles keyed by `server_id`.
//! - Reconcile only performs set-difference operations:
//!   - create missing handles
//!   - drop extra handles
//! - Handles self-terminate on disconnect/id-mismatch and are re-created only
//!   by future snapshots.

use std::sync::Arc;

use dashmap::DashMap;
use hypha::{Cell, CellImmutable, MapExt, Signal, SubscriptionGuard, TapExt, Watchable};
use log::info;
use myko_rs::{
    entities::server::{GetAllServers, GetPeerServers, Server, ServerId},
    server::CellServerCtx,
};

mod peer_connection_handle;
use peer_connection_handle::{PeerConnectionHandle, PeerState};

/// Status of a peer connection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStatus {
    pub peer_id: String,
    pub is_connected: bool,
    pub is_alive: bool,
    pub latency_ms: Option<u64>,
    pub last_seen: Option<String>,
}

/// Peer registry for managing connections to other servers.
pub struct PeerRegistry {
    _peers_guard: SubscriptionGuard,
    _self_advertise_guard: SubscriptionGuard,
    _connections: Arc<DashMap<ServerId, PeerConnectionHandle>>,
    _remove_guards: Arc<DashMap<ServerId, SubscriptionGuard>>,
}

#[derive(Debug, Clone)]
pub struct PeerRegistryConfig {
    pub address: String,
    pub port: u16,
    pub version: String,
}

impl PeerRegistry {
    fn build_local_server(config: &PeerRegistryConfig, host_id: &ServerId) -> Server {
        Server {
            address: config.address.clone(),
            id: host_id.clone(),
            port: config.port,
            started_at: chrono::Utc::now().to_rfc3339(),
            version: config.version.clone(),
        }
    }

    fn spawn_self_advertise_guard(
        ctx: CellServerCtx,
        local_server: Server,
        self_host_id: ServerId,
    ) -> SubscriptionGuard {
        let all_servers = ctx
            .query_map(GetAllServers {}, ctx.new_server_transaction())
            .items();
        all_servers.subscribe(move |signal| {
            if let Signal::Value(servers) = signal {
                let has_self = servers.iter().any(|s| s.id == self_host_id);
                if !has_self {
                    log::info!(
                        "Local server {} missing from GetAllServers; re-advertising",
                        self_host_id
                    );
                    ctx.set(&local_server);
                }
            }
        })
    }

    fn reconcile_peer_snapshot<T>(
        peers: &T,
        host_id: &ServerId,
        local_address: &str,
        local_port: u16,
        ctx: &CellServerCtx,
        connections: &Arc<DashMap<ServerId, PeerConnectionHandle>>,
        remove_guards: &Arc<DashMap<ServerId, SubscriptionGuard>>,
    ) where
        T: AsRef<[Arc<Server>]>,
    {
        log::info!(
            "Current Peers: {}",
            peers
                .as_ref()
                .iter()
                .map(|s| format!("{}/{}:{}", s.id, s.address, s.port))
                .collect::<Vec<String>>()
                .join(", ")
        );

        for server in peers.as_ref() {
            // If another row points at our local endpoint but has a different id,
            // it is a stale incarnation and should be tombstoned.
            if server.id != *host_id && server.address == local_address && server.port == local_port
            {
                log::warn!(
                    "Deleting stale entries: {}:{}:{}",
                    server.id,
                    server.address,
                    server.port
                );
                ctx.unregister_peer_client(server.id.as_ref());
                ctx.del(server.as_ref());
                continue;
            }

            if connections.contains_key(&server.id) {
                continue;
            }

            let handle = PeerConnectionHandle::new(server.clone());

            let remove_connection_handles = connections.clone();
            let remove_state_guards = remove_guards.clone();

            let remove_server = server.clone();

            let remove_ctx = ctx.clone();

            let state_guard = handle.signal_state.subscribe(move |state| {
                if let Signal::Value(v) = state
                    && v.as_ref() == &PeerState::Delete
                {
                    log::warn!(
                        "Deleting: {}:{}:{}",
                        remove_server.id,
                        remove_server.address,
                        remove_server.port
                    );
                    remove_connection_handles.remove(&remove_server.id);
                    remove_state_guards.remove(&remove_server.id);
                    remove_ctx.unregister_peer_client(remove_server.id.as_ref());
                    remove_ctx.del(remove_server.as_ref());
                }
            });

            let server_id = server.id.clone();
            ctx.register_peer_client(server_id.clone(), handle.client());
            connections.insert(server_id.clone(), handle);
            remove_guards.insert(server_id, state_guard);
        }
    }

    fn spawn_peer_reconcile_guard(
        peer_servers: Cell<Vec<Arc<Server>>, CellImmutable>,
        host_id: ServerId,
        local_address: String,
        local_port: u16,
        ctx: CellServerCtx,
        connections: Arc<DashMap<ServerId, PeerConnectionHandle>>,
        remove_guards: Arc<DashMap<ServerId, SubscriptionGuard>>,
    ) -> SubscriptionGuard {
        peer_servers
            .tap(move |peers| {
                Self::reconcile_peer_snapshot(
                    peers,
                    &host_id,
                    &local_address,
                    local_port,
                    &ctx,
                    &connections,
                    &remove_guards,
                );
            })
            .subscribe(|_| {})
    }

    pub fn new(ctx: CellServerCtx, config: PeerRegistryConfig) -> Self {
        let server_req = ctx.new_server_transaction();

        let connections = Arc::new(DashMap::new());
        let remove_guards = Arc::new(DashMap::new());
        let peer_servers = ctx
            .query_map(GetPeerServers {}, server_req)
            .items();
        let host_id = ServerId(ctx.host_id.to_string().into());
        let server = Self::build_local_server(&config, &host_id);

        let self_advertise_guard =
            Self::spawn_self_advertise_guard(ctx.clone(), server.clone(), host_id.clone());

        let peer_sub = Self::spawn_peer_reconcile_guard(
            peer_servers,
            host_id.clone(),
            config.address.clone(),
            config.port,
            ctx.clone(),
            connections.clone(),
            remove_guards.clone(),
        );

        log::info!(
            "Publishing local server bootstrap advert: {}:{}:{}",
            server.id,
            server.address,
            server.port
        );
        ctx.set(&server);

        Self {
            _peers_guard: peer_sub,
            _self_advertise_guard: self_advertise_guard,
            _connections: connections,
            _remove_guards: remove_guards,
        }
    }

    pub fn shutdown(&self) {
        info!("PeerRegistry shutting down");
    }
}

impl Drop for PeerRegistry {
    fn drop(&mut self) {
        log::warn!("Dropping Peer Registry");
        self.shutdown();
    }
}
