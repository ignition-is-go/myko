//! Peer registry for the cell-based server.
//!
//! Manages connections to other Myko servers for federation.
//!
//! # Architecture
//!
//! ```text
//! CellServerCtx
//!        │
//!        ├──► query(GetPeerServers) ──► discovers peers reactively
//!        │
//!        └──► publish_set(Server) ──► publishes this server
//!        │
//!        ▼
//!   PeerRegistry ──► manages peer connections
//!        │
//!        ▼
//!   MykoClient ──► WebSocket connection to peer
//!        │
//!        ▼
//!   Connection monitor ──► detects disconnect, cleans up
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use hypha::{Gettable, Signal, SubscriptionGuard, Watchable};
use log::{debug, info, trace, warn};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::client::{ConnectionStatus, MykoClient};
use crate::entities::server::{GetAllServers, GetConnectedServer, GetPeerServers, Server};
use crate::item::Eventable;
use crate::query::QueryRequest;

use super::CellServerCtx;

/// Status of a peer connection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStatus {
    /// The peer's server ID
    pub peer_id: String,
    /// Whether the peer is currently connected
    pub is_connected: bool,
    /// Whether the peer is considered alive (connected and recently seen)
    pub is_alive: bool,
    /// Last measured latency in milliseconds
    pub latency_ms: Option<u64>,
    /// ISO timestamp of when the peer was last seen
    pub last_seen: Option<String>,
}

/// Internal state for a connected peer.
struct PeerConnection {
    /// WebSocket client connection
    client: MykoClient,
    /// Last time we received data from this peer
    #[allow(dead_code)]
    last_seen: Option<Instant>,
    /// Last measured latency
    #[allow(dead_code)]
    last_latency_ms: Option<u64>,
}

/// Messages for the peer registry background task.
enum PeerMessage {
    /// A peer server was discovered
    Discovered(Server),
    /// A peer server was removed
    Removed { server_id: Uuid },
    /// A peer connection was established
    Connected {
        server_id: Uuid,
        client: MykoClient,
    },
    /// A peer connection was lost
    Disconnected { server_id: Uuid },
    /// Update peer health metrics
    #[allow(dead_code)]
    UpdateHealth { server_id: Uuid, latency_ms: u64 },
    /// Shutdown the registry
    Shutdown,
}

/// Shared peer state accessible from outside the background task.
struct SharedPeerState {
    /// Connected peers
    peers: HashMap<Uuid, PeerStatus>,
    /// Peers currently being connected to
    connecting: HashSet<Uuid>,
}

/// Peer registry for managing connections to other servers.
pub struct PeerRegistry {
    /// This server's host ID
    #[allow(dead_code)]
    host_id: Uuid,
    /// Channel for sending messages to the background task
    sender: mpsc::Sender<PeerMessage>,
    /// Shared state for status queries
    state: Arc<RwLock<SharedPeerState>>,
    /// Background task handle
    _task_handle: tokio::task::JoinHandle<()>,
    /// Subscription guard for peer server discovery (keeps subscription alive)
    _peer_subscription: SubscriptionGuard,
}

/// Configuration for creating a PeerRegistry.
#[derive(Debug, Clone)]
pub struct PeerRegistryConfig {
    /// This server's address
    pub address: String,
    /// This server's port
    pub port: u16,
    /// This server's version
    pub version: String,
}

impl PeerRegistry {
    /// Create a new peer registry with CellServerCtx.
    ///
    /// The registry:
    /// - Publishes this server's Server entity
    /// - Subscribes to GetPeerServers query for peer discovery
    /// - Connects to discovered peers via WebSocket
    /// - Monitors connection health
    pub fn new(ctx: CellServerCtx, config: PeerRegistryConfig) -> Self {
        let host_id = ctx.host_id;
        // Bounded channel with high limit for backpressure safety
        let (sender, receiver) = mpsc::channel(10_000);
        let state = Arc::new(RwLock::new(SharedPeerState {
            peers: HashMap::new(),
            connecting: HashSet::new(),
        }));

        // Clean up stale servers with the same address and port (e.g., from a previous crash)
        let all_servers = ctx.query(GetAllServers {});
        for server in all_servers.get().iter() {
            if server.address == config.address
                && server.port == config.port
                && server.id.as_ref() != host_id.to_string()
            {
                info!(
                    "Removing stale server {} (same address {}:{})",
                    server.id, server.address, server.port
                );
                ctx.del(server);
            }
        }

        // Publish this server's Server entity
        let this_server = Server {
            id: host_id.to_string().into(),
            hash: host_id.to_string().into(), // Use host_id as hash for simplicity
            version: config.version.clone(),
            address: config.address.clone(),
            port: config.port,
            started_at: chrono::Utc::now().to_rfc3339(),
        };

        ctx.set(this_server.clone());
        info!("Published server {} at {}:{}", host_id, config.address, config.port);

        // Subscribe to GetPeerServers query for reactive peer discovery
        let query_sender = sender.clone();
        let peer_servers_cell = ctx.query(GetPeerServers {});

        // Track known peers to detect additions/removals
        let known_peers: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));
        let known_peers_clone = known_peers.clone();

        // Subscribe to peer server changes
        let subscription_guard = peer_servers_cell.subscribe(move |signal| {
            if let Signal::Value(servers) = signal {
                let mut known = known_peers_clone.write().unwrap();
                let current_ids: HashSet<String> = servers.iter().map(|s| s.id.to_string()).collect();

                // Find new peers
                for server in servers.iter() {
                    let id = server.id.to_string();
                    if !known.contains(&id) {
                        debug!("Discovered new peer server: {}", id);
                        if let Err(e) = query_sender.try_send(PeerMessage::Discovered(server.clone())) {
                            warn!("Peer message buffer full, dropping Discovered: {}", e);
                        }
                    }
                }

                // Find removed peers
                for id in known.iter() {
                    if !current_ids.contains(id) {
                        debug!("Peer server removed: {}", id);
                        if let Ok(uuid) = Uuid::parse_str(id) {
                            if let Err(e) = query_sender.try_send(PeerMessage::Removed { server_id: uuid }) {
                                warn!("Peer message buffer full, dropping Removed: {}", e);
                            }
                        }
                    }
                }

                *known = current_ids;
            }
        });

        // Process initial peer list
        for server in peer_servers_cell.get().iter() {
            known_peers.write().unwrap().insert(server.id.to_string());
            if let Err(e) = sender.try_send(PeerMessage::Discovered(server.clone())) {
                warn!("Peer message buffer full on init, dropping Discovered: {}", e);
            }
        }

        let task_state = state.clone();
        let task_sender = sender.clone();
        let task_ctx = ctx.clone();

        // Spawn the connection manager task
        let task_handle = tokio::spawn(async move {
            Self::run_background_task(host_id, receiver, task_state, task_sender, task_ctx).await;
        });

        info!("PeerRegistry started for host {}", host_id);

        Self {
            host_id,
            sender,
            state,
            _task_handle: task_handle,
            _peer_subscription: subscription_guard,
        }
    }

    /// Get list of connected peer IDs.
    pub fn connected_peers(&self) -> Vec<Uuid> {
        self.state
            .read()
            .unwrap()
            .peers
            .iter()
            .filter(|(_, status)| status.is_connected)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get status of a specific peer.
    pub fn peer_status(&self, peer_id: Uuid) -> Option<PeerStatus> {
        self.state.read().unwrap().peers.get(&peer_id).cloned()
    }

    /// Get status of all known peers.
    pub fn all_peer_statuses(&self) -> Vec<PeerStatus> {
        self.state.read().unwrap().peers.values().cloned().collect()
    }

    /// Check if a peer is connected.
    pub fn is_peer_connected(&self, peer_id: Uuid) -> bool {
        self.state
            .read()
            .unwrap()
            .peers
            .get(&peer_id)
            .map(|s| s.is_connected)
            .unwrap_or(false)
    }

    /// Shutdown the peer registry.
    pub fn shutdown(&self) {
        if let Err(e) = self.sender.try_send(PeerMessage::Shutdown) {
            warn!("Peer message buffer full, could not send Shutdown: {}", e);
        }
    }

    /// Background task that manages peer connections.
    async fn run_background_task(
        host_id: Uuid,
        mut receiver: mpsc::Receiver<PeerMessage>,
        state: Arc<RwLock<SharedPeerState>>,
        sender: mpsc::Sender<PeerMessage>,
        ctx: CellServerCtx,
    ) {
        // Internal state for actual connections
        let mut connections: HashMap<Uuid, PeerConnection> = HashMap::new();

        while let Some(msg) = receiver.recv().await {
            match msg {
                PeerMessage::Discovered(server) => {
                    let server_id = match Uuid::parse_str(&server.id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!("Invalid server ID {}: {}", server.id, e);
                            continue;
                        }
                    };

                    // Skip self
                    if server_id == host_id {
                        continue;
                    }

                    // Skip if already connected or connecting
                    {
                        let state_guard = state.read().unwrap();
                        if state_guard.peers.contains_key(&server_id)
                            && state_guard
                                .peers
                                .get(&server_id)
                                .map(|s| s.is_connected)
                                .unwrap_or(false)
                        {
                            trace!("Already connected to {}", server_id);
                            continue;
                        }
                        if state_guard.connecting.contains(&server_id) {
                            trace!("Already connecting to {}", server_id);
                            continue;
                        }
                    }

                    // Mark as connecting
                    state.write().unwrap().connecting.insert(server_id);

                    let peer_address = format!("ws://{}:{}/myko", server.address, server.port);
                    debug!("Connecting to peer {} at {}", server_id, peer_address);

                    // Spawn connection task
                    let sender_clone = sender.clone();
                    tokio::spawn(async move {
                        let client = MykoClient::new();
                        client.set_address(Some(peer_address.clone()));

                        // Wait for connection with timeout
                        let connected = tokio::time::timeout(Duration::from_secs(5), async {
                            loop {
                                let status = client.get_connection_status().await;
                                if matches!(status, ConnectionStatus::Connected(_)) {
                                    return true;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        })
                        .await
                        .unwrap_or(false);

                        if !connected {
                            warn!("Connection timeout: {} at {}", server_id, peer_address);
                            client.close();
                            let _ = sender_clone.send(PeerMessage::Disconnected { server_id });
                            return;
                        }

                        // Verify server ID via GetConnectedServer query
                        let query = QueryRequest::new(GetConnectedServer {});
                        let expected_id = server_id.to_string();
                        let verified = tokio::time::timeout(Duration::from_secs(3), async {
                            let stream = client.watch_query::<GetConnectedServer>(&query);
                            tokio::pin!(stream);

                            if let Some(servers) = stream.next().await {
                                debug!(
                                    "GetConnectedServer returned {} servers: {:?}",
                                    servers.len(),
                                    servers.iter().map(|s| s.id.as_ref()).collect::<Vec<_>>()
                                );
                                servers
                                    .first()
                                    .map(|s| {
                                        let matches = s.id.as_ref() == expected_id;
                                        if !matches {
                                            warn!(
                                                "Server ID mismatch: expected {}, got {}",
                                                expected_id,
                                                s.id.as_ref()
                                            );
                                        }
                                        matches
                                    })
                                    .unwrap_or(false)
                            } else {
                                warn!("GetConnectedServer returned no results (stream ended)");
                                false
                            }
                        })
                        .await
                        .unwrap_or_else(|_| {
                            warn!("GetConnectedServer timed out for {}", server_id);
                            false
                        });

                        if verified {
                            debug!("Verified peer {} at {}", server_id, peer_address);
                            let _ = sender_clone.send(PeerMessage::Connected { server_id, client });
                        } else {
                            warn!("Server ID verification failed for {}", server_id);
                            client.close();
                            let _ = sender_clone.send(PeerMessage::Disconnected { server_id });
                        }
                    });
                }

                PeerMessage::Connected { server_id, client } => {
                    info!("Connected to peer {}", server_id);

                    // Remove from connecting, add to peers
                    {
                        let mut state_guard = state.write().unwrap();
                        state_guard.connecting.remove(&server_id);
                        state_guard.peers.insert(
                            server_id,
                            PeerStatus {
                                peer_id: server_id.to_string(),
                                is_connected: true,
                                is_alive: true,
                                latency_ms: None,
                                last_seen: Some(chrono::Utc::now().to_rfc3339()),
                            },
                        );
                    }

                    // Monitor connection status reactively
                    let keepalive_client = client.clone();
                    let keepalive_sender = sender.clone();
                    tokio::spawn(async move {
                        let mut status_stream = keepalive_client.watch_connection_status();

                        while let Some(status) = status_stream.next().await {
                            match status {
                                ConnectionStatus::Connected(_) => {
                                    trace!("Peer {} connected", server_id);
                                }
                                ConnectionStatus::Disconnected => {
                                    info!("Peer {} disconnected", server_id);
                                    break;
                                }
                            }
                        }

                        keepalive_client.close();
                        if let Err(e) = keepalive_sender.try_send(PeerMessage::Disconnected { server_id }) {
                            warn!("Peer message buffer full, dropping Disconnected: {}", e);
                        }
                    });

                    connections.insert(
                        server_id,
                        PeerConnection {
                            client,
                            last_seen: Some(Instant::now()),
                            last_latency_ms: None,
                        },
                    );
                }

                PeerMessage::Disconnected { server_id } => {
                    info!("Disconnected from peer {}", server_id);

                    // Close and remove connection
                    if let Some(peer) = connections.remove(&server_id) {
                        peer.client.close();
                    }

                    // Delete the Server entity so it doesn't appear stale to other peers
                    let server_id_str: Arc<str> = server_id.to_string().into();
                    if let Some(server) = ctx.registry.get_or_create(Server::entity_name_static()).get_value(&server_id_str) {
                        ctx.del_dyn(server);
                    }
                    info!("Deleted Server entity for disconnected peer {}", server_id);

                    // Update shared state
                    {
                        let mut state_guard = state.write().unwrap();
                        state_guard.connecting.remove(&server_id);
                        state_guard.peers.remove(&server_id);
                    }
                }

                PeerMessage::Removed { server_id } => {
                    trace!("Peer {} removed", server_id);

                    // Disconnect if connected
                    if connections.contains_key(&server_id) {
                        if let Err(e) = sender.try_send(PeerMessage::Disconnected { server_id }) {
                            warn!("Peer message buffer full, dropping Disconnected: {}", e);
                        }
                    }

                    // Remove from state entirely
                    {
                        let mut state_guard = state.write().unwrap();
                        state_guard.peers.remove(&server_id);
                        state_guard.connecting.remove(&server_id);
                    }
                }

                PeerMessage::UpdateHealth {
                    server_id,
                    latency_ms,
                } => {
                    if let Some(peer) = connections.get_mut(&server_id) {
                        peer.last_seen = Some(Instant::now());
                        peer.last_latency_ms = Some(latency_ms);
                    }

                    // Update shared state
                    if let Some(status) = state.write().unwrap().peers.get_mut(&server_id) {
                        status.latency_ms = Some(latency_ms);
                        status.last_seen = Some(chrono::Utc::now().to_rfc3339());
                        status.is_alive = true;
                    }
                }

                PeerMessage::Shutdown => {
                    info!("PeerRegistry shutting down");

                    // Close all connections
                    for (_, peer) in connections.drain() {
                        peer.client.close();
                    }

                    break;
                }
            }
        }
    }
}

impl Drop for PeerRegistry {
    fn drop(&mut self) {
        self.shutdown();
    }
}
