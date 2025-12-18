//! PeerManager actor implementation.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use log::{debug, error, info, trace, warn};
use serde_json::Value;
use tokio_stream::StreamExt as TokioStreamExt;
use uuid::Uuid;

use crate::actors::command::command_manager::CommandManagerMsg;
use crate::actors::query::query_manager::QueryManagerMsg;
use crate::api::query::WrappedQuery;
use crate::client::MykoClient;
use crate::command::{AnyCommand, WrappedCommand};
use crate::context::RequestContext;
use crate::entities::server::{DeleteServer, GetConnectedServer, GetPeerServers, Server};
use crate::message::MykoMessage;
use crate::parsers::query::AnyQuery;
use crate::query::QueryRequest;
use crate::report::{AnyReport, WrappedReport};
use crate::runtime::{Actor, ActorRef, RpcReplyPort};
use crate::server::MykoServerCtx;
use crate::utils::downcast_item;

/// Arguments for spawning the PeerManager actor.
pub struct PeerManagerArgs {
    /// Server context with host ID
    pub ctx: Arc<MykoServerCtx>,
    /// This server's public address
    pub host_address: String,
    /// This server's port
    pub host_port: u16,
    /// Query manager for local GetPeerServers subscription
    pub query_manager: ActorRef<QueryManagerMsg>,
    /// Command manager for executing DeleteServer commands
    pub command_manager: ActorRef<CommandManagerMsg>,
}

/// Represents an active connection to a peer server.
struct PeerConnection {
    /// The peer's server ID
    #[allow(dead_code)]
    server_id: Uuid,
    /// WebSocket client connection
    client: MykoClient,
    /// Last time we successfully pinged this peer (for health tracking)
    last_seen: Option<std::time::Instant>,
    /// Last measured latency in milliseconds
    last_latency_ms: Option<u64>,
}

/// Status of a peer connection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStatus {
    /// The peer's server ID
    pub peer_id: String,
    /// Whether the peer is currently considered alive
    pub is_alive: bool,
    /// Last measured latency in milliseconds (None if never pinged)
    pub latency_ms: Option<u64>,
    /// ISO timestamp of when the peer was last seen
    pub last_seen: Option<String>,
}

/// Messages handled by the PeerManager actor.
pub enum PeerManagerMsg {
    /// Start peer discovery (called after AllInitComplete)
    Start,
    /// A peer server was discovered via GetPeerServers query
    PeerDiscovered(Server),
    /// A peer server was removed from GetPeerServers results
    PeerRemoved { server_id: Uuid },
    /// A peer connection was established successfully
    PeerConnected {
        server_id: Uuid,
        address: String,
        client: MykoClient,
    },
    /// A peer connection was lost
    PeerDisconnected { server_id: Uuid },
    /// Get the list of connected peer IDs
    GetConnectedPeers(RpcReplyPort<Vec<Uuid>>),
    /// Forward a query to a specific peer
    ForwardQuery {
        peer_id: Uuid,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<Result<Vec<Value>, String>>,
    },
    /// Forward a command to a specific peer
    ForwardCommand {
        peer_id: Uuid,
        command: Arc<dyn AnyCommand>,
        reply: RpcReplyPort<Result<Value, String>>,
    },
    /// Forward a report request to a specific peer
    ForwardReport {
        peer_id: Uuid,
        report: Arc<dyn AnyReport>,
        reply: RpcReplyPort<Result<Value, String>>,
    },
    /// Get the status of a specific peer
    GetPeerStatus {
        peer_id: Uuid,
        reply: RpcReplyPort<Option<PeerStatus>>,
    },
    /// Get the status of all peers
    GetAllPeerStatuses(RpcReplyPort<Vec<PeerStatus>>),
    /// Internal: Update peer health (called periodically by health check task)
    UpdatePeerHealth { peer_id: Uuid, latency_ms: u64 },
    /// Internal: Connection/verification failed - server entry already deleted
    ConnectionFailed { server_id: Uuid, address: String },
}

impl std::fmt::Debug for PeerManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerManagerMsg::Start => write!(f, "Start"),
            PeerManagerMsg::PeerDiscovered(server) => {
                write!(f, "PeerDiscovered({})", server.id)
            }
            PeerManagerMsg::PeerRemoved { server_id } => {
                write!(f, "PeerRemoved({})", server_id)
            }
            PeerManagerMsg::PeerConnected { server_id, address, .. } => {
                write!(f, "PeerConnected({}, {})", server_id, address)
            }
            PeerManagerMsg::PeerDisconnected { server_id } => {
                write!(f, "PeerDisconnected({})", server_id)
            }
            PeerManagerMsg::GetConnectedPeers(_) => write!(f, "GetConnectedPeers"),
            PeerManagerMsg::ForwardQuery { peer_id, .. } => {
                write!(f, "ForwardQuery({})", peer_id)
            }
            PeerManagerMsg::ForwardCommand { peer_id, .. } => {
                write!(f, "ForwardCommand({})", peer_id)
            }
            PeerManagerMsg::ForwardReport { peer_id, .. } => {
                write!(f, "ForwardReport({})", peer_id)
            }
            PeerManagerMsg::GetPeerStatus { peer_id, .. } => {
                write!(f, "GetPeerStatus({})", peer_id)
            }
            PeerManagerMsg::GetAllPeerStatuses(_) => write!(f, "GetAllPeerStatuses"),
            PeerManagerMsg::UpdatePeerHealth { peer_id, latency_ms } => {
                write!(f, "UpdatePeerHealth({}, {}ms)", peer_id, latency_ms)
            }
            PeerManagerMsg::ConnectionFailed { server_id, address } => {
                write!(f, "ConnectionFailed({}, {})", server_id, address)
            }
        }
    }
}

pub struct PeerManager {
    ctx: Arc<MykoServerCtx>,
    #[allow(dead_code)]
    host_address: String,
    #[allow(dead_code)]
    host_port: u16,
    /// Query manager for local GetPeerServers subscription
    query_manager: ActorRef<QueryManagerMsg>,
    /// Command manager for executing DeleteServer commands
    command_manager: ActorRef<CommandManagerMsg>,
    /// Active peer connections keyed by server ID
    peers: HashMap<Uuid, PeerConnection>,
    /// Server IDs currently being connected to (prevents duplicate connection attempts for same ID)
    connecting: HashSet<Uuid>,
    /// Self-reference for nested operations
    myself: ActorRef<PeerManagerMsg>,
}

impl PeerManager {
    fn create(args: PeerManagerArgs, myself: ActorRef<PeerManagerMsg>) -> Self {
        debug!(
            "PeerManager starting for host {}:{}",
            args.host_address, args.host_port
        );

        Self {
            ctx: args.ctx,
            host_address: args.host_address,
            host_port: args.host_port,
            query_manager: args.query_manager,
            command_manager: args.command_manager,
            peers: HashMap::new(),
            connecting: HashSet::new(),
            myself,
        }
    }

    /// Delete a Server entity via DeleteServer command (sync version)
    fn delete_server_sync(command_manager: &ActorRef<CommandManagerMsg>, server_id: Uuid, host_id: Uuid) {
        use crate::entities::server::DeleteServerArgs;
        let delete_cmd = DeleteServer::new(DeleteServerArgs {
            id: server_id.to_string().into(),
        });

        let wrapped: WrappedCommand = (&delete_cmd as &dyn AnyCommand).into();
        let req = RequestContext::internal(
            Arc::from(Uuid::new_v4().to_string()),
            host_id,
            "peer-manager",
        );

        match command_manager.call(|r| CommandManagerMsg::Execute(wrapped, req, r)) {
            Ok(_) => trace!("Deleted server entry {}", server_id),
            Err(e) => warn!("Failed to delete server {}: {:?}", server_id, e),
        }
    }
}

impl Actor for PeerManager {
    type Msg = PeerManagerMsg;
    type Args = PeerManagerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args, myself)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            PeerManagerMsg::Start => {
                trace!("Starting peer discovery");

                let myself = self.myself.clone();

                let host_id = self.ctx.host_id;
                let query_manager = self.query_manager.clone();

                // Spawn discovery thread (sync - uses crossbeam channel)
                std::thread::spawn(move || {
                    use crate::actors::query::common::QueryStreamUpdate;

                    // Subscribe to GetPeerServers for immediate discovery
                    let query: Arc<dyn AnyQuery> = Arc::new(QueryRequest::new(GetPeerServers {}));

                    let receiver = match query_manager.call(|r| QueryManagerMsg::WatchQuery(query, r)) {
                        Ok(rx) => rx,
                        Err(e) => {
                            warn!("Failed to subscribe to GetPeerServers: {:?}", e);
                            return;
                        }
                    };

                    // Track current state for reconciliation
                    let mut current_servers: std::collections::BTreeMap<Arc<str>, Server> =
                        std::collections::BTreeMap::new();

                    // Process query updates (sync iteration over crossbeam channel)
                    trace!("Discovery subscription active");
                    for update in receiver.iter() {
                        match update {
                            QueryStreamUpdate::Initial(items) => {
                                let peer_count = items.iter().filter(|(id, _)| id.as_ref() != host_id.to_string()).count();
                                debug!("Discovered {} peer servers", peer_count);
                                // Initial snapshot - populate current state and trigger connections
                                current_servers.clear();
                                for (id, item) in items {
                                    if id.as_ref() == host_id.to_string() {
                                        continue; // Skip self
                                    }
                                    if let Some(server) = downcast_item::<Server>(&item) {
                                        trace!(
                                            "  - {} at {}:{} (started: {})",
                                            server.id, server.address, server.port, server.started_at
                                        );
                                        current_servers.insert(id, server.clone());
                                        let _ = myself
                                            .send_message(PeerManagerMsg::PeerDiscovered(server));
                                    }
                                }
                            }
                            QueryStreamUpdate::Upsert(id, item) => {
                                if id.as_ref() == host_id.to_string() {
                                    continue; // Skip self
                                }
                                if let Some(server) = downcast_item::<Server>(&item) {
                                    let is_new = !current_servers.contains_key(&id);
                                    let old_started_at = current_servers
                                        .get(&id)
                                        .map(|s| s.started_at.clone());
                                    current_servers.insert(id.clone(), server.clone());

                                    if is_new {
                                        debug!(
                                            "Peer discovered: {} at {}:{}",
                                            server.id, server.address, server.port
                                        );
                                    } else {
                                        trace!(
                                            "Peer updated: {} (started: {} -> {})",
                                            server.id,
                                            old_started_at.unwrap_or_default(), server.started_at
                                        );
                                    }
                                    let _ = myself
                                        .send_message(PeerManagerMsg::PeerDiscovered(server));
                                }
                            }
                            QueryStreamUpdate::Remove(id) => {
                                if let Some(server) = current_servers.remove(&id) {
                                    debug!(
                                        "Peer removed: {} at {}:{}",
                                        server.id, server.address, server.port
                                    );
                                    if let Ok(uuid) = Uuid::parse_str(&id) {
                                        let _ = myself
                                            .send_message(PeerManagerMsg::PeerRemoved { server_id: uuid });
                                    }
                                }
                            }
                            QueryStreamUpdate::Error(msg) => {
                                error!("Query error in peer discovery: {}", msg);
                                break;
                            }
                        }
                    }

                    trace!("Discovery loop ended");
                });
            }

            PeerManagerMsg::PeerDiscovered(server) => {
                let address_key = format!("{}:{}", server.address, server.port);

                // Parse server ID
                let server_id = match Uuid::parse_str(&server.id) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!("Invalid server ID {}: {}", server.id, e);
                        return;
                    }
                };

                // Skip if this is ourselves
                if server_id == self.ctx.host_id {
                    return;
                }

                // Skip if already connected or connecting
                if self.peers.contains_key(&server_id) {
                    trace!("Already connected to {}", server_id);
                    return;
                }
                if self.connecting.contains(&server_id) {
                    trace!("Already connecting to {}", server_id);
                    return;
                }

                trace!("Connecting to {} at {}", server_id, address_key);
                self.connecting.insert(server_id);

                // Connect in background
                let myself = self.myself.clone();
                let peer_address = format!("ws://{}:{}/myko", server.address, server.port);
                let expected_server_id = server_id;
                let command_manager = self.command_manager.clone();
                let host_id = self.ctx.host_id;

                self.ctx.tokio_handle.spawn(async move {
                    let client = MykoClient::new();
                        client.set_address(Some(peer_address.clone()));

                        // Wait for connection with timeout
                        let connection_timeout = tokio::time::Duration::from_secs(5);
                        let connected = tokio::time::timeout(connection_timeout, async {
                            loop {
                                let status = client.get_connection_status().await;
                                if matches!(status, crate::client::ConnectionStatus::Connected(_)) {
                                    return true;
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            }
                        })
                        .await
                        .unwrap_or(false);

                        if !connected {
                            warn!("Connection timeout: {} at {}", expected_server_id, address_key);
                            client.close();
                            Self::delete_server_sync(&command_manager, expected_server_id, host_id);
                            let _ = myself.send_message(PeerManagerMsg::ConnectionFailed {
                                server_id: expected_server_id,
                                address: address_key,
                            });
                            return;
                        }

                        // Verify server ID via GetConnectedServer query
                        let query = QueryRequest::new(GetConnectedServer {});

                        let verification_timeout = tokio::time::Duration::from_secs(3);
                        let verification_result = tokio::time::timeout(verification_timeout, async {
                            let stream = client.watch_query(&query);
                            tokio::pin!(stream);

                            if let Some(servers) = TokioStreamExt::next(&mut stream).await {
                                let actual_id = servers.first().map(|s| s.id.to_string());
                                (actual_id.clone(), actual_id == Some(expected_server_id.to_string()))
                            } else {
                                (None, false)
                            }
                        })
                        .await
                        .unwrap_or((None, false));

                        let (actual_id, verified) = verification_result;

                        if verified {
                            trace!("Verified peer {} at {}", expected_server_id, address_key);
                            let _ = myself.send_message(PeerManagerMsg::PeerConnected {
                                server_id: expected_server_id,
                                address: address_key,
                                client,
                            });
                        } else {
                            warn!(
                                "Server ID mismatch: expected {} at {}, got {:?}",
                                expected_server_id, address_key, actual_id
                            );
                            client.close();
                            Self::delete_server_sync(&command_manager, expected_server_id, host_id);
                    let _ = myself.send_message(PeerManagerMsg::ConnectionFailed {
                        server_id: expected_server_id,
                        address: address_key,
                    });
                }
                });
            }

            PeerManagerMsg::PeerConnected {
                server_id,
                address,
                client,
            } => {
                info!("Connected to peer {} at {}", server_id, address);

                // Remove from connecting set
                self.connecting.remove(&server_id);

                // Monitor connection status - when it goes to Disconnected,
                // close the client and notify PeerManager
                let monitor_client = client.clone();
                let myself = self.myself.clone();

                self.ctx.tokio_handle.spawn(async move {
                    let mut stream = monitor_client.watch_connection_status();
                    let mut was_connected = true;

                    while let Some(status) = TokioStreamExt::next(&mut stream).await {
                        match status {
                            crate::client::ConnectionStatus::Connected(_) => {
                                was_connected = true;
                            }
                            crate::client::ConnectionStatus::Disconnected => {
                                if was_connected {
                                    debug!("Lost connection to peer {}", server_id);
                                    monitor_client.close();
                                    let _ = myself
                                        .send_message(PeerManagerMsg::PeerDisconnected { server_id });
                                    break;
                                }
                            }
                        }
                    }
                });

                self.peers.insert(
                    server_id,
                    PeerConnection {
                        server_id,
                        client,
                        last_seen: Some(std::time::Instant::now()),
                        last_latency_ms: None,
                    },
                );
            }

            PeerManagerMsg::PeerDisconnected { server_id } => {
                info!("Disconnected from peer {}", server_id);

                // Close the client to stop any reconnection attempts
                if let Some(peer) = self.peers.remove(&server_id) {
                    peer.client.close();
                }

                // Delete the Server entity via DeleteServer command
                // This will cascade delete any associated Client entities via belongs_to
                use crate::entities::server::DeleteServerArgs;
                let delete_cmd = DeleteServer::new(DeleteServerArgs {
                    id: server_id.to_string().into(),
                });

                // Convert to WrappedCommand using From impl
                let wrapped: WrappedCommand = (&delete_cmd as &dyn AnyCommand).into();

                let host_id = self.ctx.host_id;
                let req = RequestContext::internal(
                    Arc::from(Uuid::new_v4().to_string()),
                    host_id,
                    "peer-manager",
                );
                let command_manager = self.command_manager.clone();

                // Execute command in background thread
                std::thread::spawn(move || {
                    match command_manager.call(|r| CommandManagerMsg::Execute(wrapped, req, r)) {
                        Ok(_) => trace!("Deleted server entry {}", server_id),
                        Err(e) => warn!("Failed to delete server {}: {:?}", server_id, e),
                    }
                });
            }

            PeerManagerMsg::PeerRemoved { server_id } => {
                trace!("Peer {} removed from discovery", server_id);

                // If we're connected, disconnect
                if self.peers.contains_key(&server_id) {
                    let _ = self.myself.send_message(PeerManagerMsg::PeerDisconnected { server_id });
                }
            }

            PeerManagerMsg::GetConnectedPeers(reply) => {
                let peer_ids: Vec<Uuid> = self.peers.keys().cloned().collect();
                let _ = reply.send(peer_ids);
            }

            PeerManagerMsg::ForwardQuery {
                peer_id,
                query,
                reply,
            } => {
                // Check if this is for the local server
                if peer_id == self.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward query to self".to_string()));
                    return;
                }

                match self.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let query_id = query.query_id();
                        let query_tx = query.tx_id();

                        // Convert to WrappedQuery using From impl
                        let wrapped: WrappedQuery = query.into();

                        self.ctx.tokio_handle.spawn(async move {
                            trace!("Forwarding query {} to peer {}", query_id, peer_id);

                            // Set up listener for response before sending
                            let mut stream = client.get_messages();

                            // Send the query via client
                            if let Err(e) = client.send_query(wrapped) {
                                let _ = reply.send(Err(format!("Failed to send query: {}", e)));
                                return;
                            }

                            // Wait for response with matching tx
                            let timeout =
                                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                                    while let Some(msg) = TokioStreamExt::next(&mut stream).await {
                                        let Ok(myko_msg) =
                                            serde_json::from_value::<MykoMessage>(msg)
                                        else {
                                            continue;
                                        };

                                        match myko_msg {
                                            MykoMessage::QueryResponse(resp) => {
                                                if resp.tx == query_tx {
                                                    // Return all items from the response
                                                    let items: Vec<Value> = resp
                                                        .upserts
                                                        .into_iter()
                                                        .map(|w| w.item)
                                                        .collect();
                                                    return Ok(items);
                                                }
                                            }
                                            MykoMessage::QueryError(err) => {
                                                if *err.tx == *query_tx {
                                                    return Err(err.message);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    Err("Connection closed while waiting for query response"
                                        .to_string())
                                })
                                .await;

                            let result = match timeout {
                                Ok(r) => r,
                                Err(_) => Err("Query forwarding timed out".to_string()),
                            };

                            let _ = reply.send(result);
                        });
                    }
                    None => {
                        let _ = reply.send(Err(format!("Peer {} not connected", peer_id)));
                    }
                }
            }

            PeerManagerMsg::ForwardCommand {
                peer_id,
                command,
                reply,
            } => {
                // Check if this is for the local server
                if peer_id == self.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward command to self".to_string()));
                    return;
                }

                match self.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let command_id = command.command_id();
                        let command_tx = command.tx_id();

                        // Convert to WrappedCommand using From impl
                        let wrapped: WrappedCommand = command.into();

                        self.ctx.tokio_handle.spawn(async move {
                            trace!("Forwarding command {} to peer {}", command_id, peer_id);

                            // Set up listener for response before sending
                            let mut stream = client.get_messages();

                            // Send the command via client
                            if let Err(e) = client.send_command_raw(wrapped) {
                                let _ = reply.send(Err(format!("Failed to send command: {}", e)));
                                return;
                            }

                            // Wait for response with matching tx
                            let timeout =
                                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                                    while let Some(msg) = TokioStreamExt::next(&mut stream).await {
                                        let Ok(myko_msg) =
                                            serde_json::from_value::<MykoMessage>(msg)
                                        else {
                                            continue;
                                        };

                                        match myko_msg {
                                            MykoMessage::CommandResponse(resp) => {
                                                if *resp.tx == *command_tx {
                                                    return Ok(resp.response);
                                                }
                                            }
                                            MykoMessage::CommandError(err) => {
                                                if *err.tx == *command_tx {
                                                    return Err(err.message);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    Err("Connection closed while waiting for command response"
                                        .to_string())
                                })
                                .await;

                            let result = match timeout {
                                Ok(r) => r,
                                Err(_) => Err("Command forwarding timed out".to_string()),
                            };

                            let _ = reply.send(result);
                        });
                    }
                    None => {
                        let _ = reply.send(Err(format!("Peer {} not connected", peer_id)));
                    }
                }
            }

            PeerManagerMsg::ForwardReport {
                peer_id,
                report,
                reply,
            } => {
                // Check if this is for the local server
                if peer_id == self.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward report to self".to_string()));
                    return;
                }

                match self.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let report_id = report.report_id();
                        let report_tx = report.tx_id();

                        // Convert to WrappedReport using From impl
                        let wrapped: WrappedReport = report.into();

                        self.ctx.tokio_handle.spawn(async move {
                            trace!("Forwarding report {} to peer {}", report_id, peer_id);

                            // Set up listener for response before sending
                            let mut stream = client.get_messages();

                            // Send the report via client
                            if let Err(e) = client.send_report_raw(wrapped) {
                                let _ = reply.send(Err(format!("Failed to send report: {}", e)));
                                return;
                            }

                            // Wait for response with matching tx (one-shot for reports)
                            let timeout =
                                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                                    while let Some(msg) = TokioStreamExt::next(&mut stream).await {
                                        let Ok(myko_msg) =
                                            serde_json::from_value::<MykoMessage>(msg)
                                        else {
                                            continue;
                                        };

                                        match myko_msg {
                                            MykoMessage::ReportResponse(resp) => {
                                                if *resp.tx == *report_tx {
                                                    return Ok(resp.response);
                                                }
                                            }
                                            MykoMessage::ReportError(err) => {
                                                if *err.tx == *report_tx {
                                                    return Err(err.message);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    Err("Connection closed while waiting for report response"
                                        .to_string())
                                })
                                .await;

                            let result = match timeout {
                                Ok(r) => r,
                                Err(_) => Err("Report forwarding timed out".to_string()),
                            };

                            let _ = reply.send(result);
                        });
                    }
                    None => {
                        let _ = reply.send(Err(format!("Peer {} not connected", peer_id)));
                    }
                }
            }

            PeerManagerMsg::GetPeerStatus { peer_id, reply } => {
                let status = self.peers.get(&peer_id).map(|peer| {
                    let last_seen_iso = peer.last_seen.map(|instant| {
                        // Convert Instant to approximate ISO timestamp
                        let elapsed = instant.elapsed();
                        let datetime = chrono::Utc::now() - chrono::Duration::from_std(elapsed).unwrap_or_default();
                        datetime.to_rfc3339()
                    });

                    PeerStatus {
                        peer_id: peer_id.to_string(),
                        is_alive: peer.last_seen.map(|i| i.elapsed().as_secs() < 30).unwrap_or(false),
                        latency_ms: peer.last_latency_ms,
                        last_seen: last_seen_iso,
                    }
                });
                let _ = reply.send(status);
            }

            PeerManagerMsg::GetAllPeerStatuses(reply) => {
                let statuses: Vec<PeerStatus> = self
                    .peers
                    .iter()
                    .map(|(peer_id, peer)| {
                        let last_seen_iso = peer.last_seen.map(|instant| {
                            let elapsed = instant.elapsed();
                            let datetime = chrono::Utc::now() - chrono::Duration::from_std(elapsed).unwrap_or_default();
                            datetime.to_rfc3339()
                        });

                        PeerStatus {
                            peer_id: peer_id.to_string(),
                            is_alive: peer.last_seen.map(|i| i.elapsed().as_secs() < 30).unwrap_or(false),
                            latency_ms: peer.last_latency_ms,
                            last_seen: last_seen_iso,
                        }
                    })
                    .collect();
                let _ = reply.send(statuses);
            }

            PeerManagerMsg::UpdatePeerHealth { peer_id, latency_ms } => {
                if let Some(peer) = self.peers.get_mut(&peer_id) {
                    peer.last_seen = Some(std::time::Instant::now());
                    peer.last_latency_ms = Some(latency_ms);
                    trace!("Peer {} health: {}ms", peer_id, latency_ms);
                }
            }

            PeerManagerMsg::ConnectionFailed {
                server_id,
                address,
            } => {
                // Just remove from connecting set - server entry already deleted by caller
                self.connecting.remove(&server_id);
                trace!("Connection failed: {} at {}", server_id, address);
            }
        }
    }

    fn on_shutdown(&mut self) {
        trace!("PeerManager stopping");

        // Close all peer connections
        for (_, peer) in self.peers.drain() {
            peer.client.close();
        }
    }
}
