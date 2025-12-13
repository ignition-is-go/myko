//! PeerManager actor implementation.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use log::{debug, info, trace, warn};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use serde_json::Value;
use tokio::task::JoinHandle;
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
use crate::report::{AnyReport, WrappedReport};
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

/// State maintained by the PeerManager actor.
pub struct PeerManagerState {
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
    /// Handle to the peer discovery task
    discovery_handle: Option<JoinHandle<()>>,
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

pub struct PeerManager;

impl PeerManager {
    /// Delete a Server entity via DeleteServer command
    async fn delete_server(command_manager: &ActorRef<CommandManagerMsg>, server_id: Uuid, host_id: Uuid) {
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

        match ractor::call!(
            command_manager,
            CommandManagerMsg::Execute,
            wrapped,
            req
        ) {
            Ok(_) => debug!("[PEER] DELETE: {} entry removed", server_id),
            Err(e) => warn!("[PEER] DELETE FAILED: {} - {:?}", server_id, e),
        }
    }
}

impl Actor for PeerManager {
    type State = PeerManagerState;
    type Msg = PeerManagerMsg;
    type Arguments = PeerManagerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!(
            "PeerManager starting for host {}:{}",
            args.host_address, args.host_port
        );

        Ok(PeerManagerState {
            ctx: args.ctx,
            host_address: args.host_address,
            host_port: args.host_port,
            query_manager: args.query_manager,
            command_manager: args.command_manager,
            peers: HashMap::new(),
            connecting: HashSet::new(),
            discovery_handle: None,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            PeerManagerMsg::Start => {
                debug!("[PEER] Starting peer discovery");

                let myself_clone = myself.clone();
                let host_id = state.ctx.host_id;
                let query_manager = state.query_manager.clone();

                let handle = tokio::spawn(async move {
                    use crate::actors::query::common::QueryStreamUpdate;

                    // Subscribe to GetPeerServers for immediate discovery
                    let query: Arc<dyn AnyQuery> = Arc::new(GetPeerServers::new(
                        crate::entities::server::GetPeerServersArgs {},
                    ));

                    let mut receiver = match ractor::call!(
                        query_manager,
                        QueryManagerMsg::WatchQuery,
                        query
                    ) {
                        Ok(rx) => rx,
                        Err(e) => {
                            warn!("Failed to subscribe to GetPeerServers: {:?}", e);
                            return;
                        }
                    };

                    // Track current state for reconciliation
                    let mut current_servers: std::collections::BTreeMap<Arc<str>, Server> =
                        std::collections::BTreeMap::new();

                    // Process query updates
                    debug!("[PEER] WatchQuery subscription active");
                    while let Some(update) = receiver.recv().await {
                        match update {
                            QueryStreamUpdate::Initial(items) => {
                                let peer_count = items.iter().filter(|(id, _)| id.as_ref() != host_id.to_string()).count();
                                debug!("[PEER] Initial: {} peer servers", peer_count);
                                // Initial snapshot - populate current state and trigger connections
                                current_servers.clear();
                                for (id, item) in items {
                                    if id.as_ref() == host_id.to_string() {
                                        continue; // Skip self
                                    }
                                    if let Some(server) = downcast_item::<Server>(&item) {
                                        trace!(
                                            "[PEER]   - {} at {}:{} (started: {})",
                                            server.id, server.address, server.port, server.started_at
                                        );
                                        current_servers.insert(id, server.clone());
                                        let _ = myself_clone
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
                                            "[PEER] New: {} at {}:{}",
                                            server.id, server.address, server.port
                                        );
                                    } else {
                                        debug!(
                                            "[PEER] Update: {} (started: {} -> {})",
                                            server.id,
                                            old_started_at.unwrap_or_default(), server.started_at
                                        );
                                    }
                                    let _ = myself_clone
                                        .send_message(PeerManagerMsg::PeerDiscovered(server));
                                }
                            }
                            QueryStreamUpdate::Remove(id) => {
                                if let Some(server) = current_servers.remove(&id) {
                                    debug!(
                                        "[PEER] Remove: {} at {}:{}",
                                        server.id, server.address, server.port
                                    );
                                    if let Ok(uuid) = Uuid::parse_str(&id) {
                                        let _ = myself_clone
                                            .send_message(PeerManagerMsg::PeerRemoved { server_id: uuid });
                                    }
                                }
                            }
                        }
                    }

                    debug!("[PEER] Discovery loop ended");
                });

                state.discovery_handle = Some(handle);
            }

            PeerManagerMsg::PeerDiscovered(server) => {
                let address_key = format!("{}:{}", server.address, server.port);

                // Parse server ID
                let server_id = match Uuid::parse_str(&server.id) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!("[PEER] Invalid server ID {}: {}", server.id, e);
                        return Ok(());
                    }
                };

                // Skip if this is ourselves
                if server_id == state.ctx.host_id {
                    return Ok(());
                }

                // Skip if already connected or connecting
                if state.peers.contains_key(&server_id) {
                    debug!("[PEER] Already connected to {} - skipping", server_id);
                    return Ok(());
                }
                if state.connecting.contains(&server_id) {
                    debug!("[PEER] Already connecting to {} - skipping", server_id);
                    return Ok(());
                }

                debug!("[PEER] Connecting: {} at {}", server_id, address_key);
                state.connecting.insert(server_id);

                // Connect in background
                let myself_clone = myself.clone();
                let peer_address = format!("ws://{}:{}/myko", server.address, server.port);
                let expected_server_id = server_id;
                let command_manager = state.command_manager.clone();
                let host_id = state.ctx.host_id;

                tokio::spawn(async move {
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
                        warn!("[PEER] TIMEOUT: {} at {} - deleting entry", expected_server_id, address_key);
                        client.close();
                        Self::delete_server(&command_manager, expected_server_id, host_id).await;
                        let _ = myself_clone.send_message(PeerManagerMsg::ConnectionFailed {
                            server_id: expected_server_id,
                            address: address_key,
                        });
                        return;
                    }

                    // Verify server ID via GetConnectedServer query
                    let query =
                        GetConnectedServer::new(crate::entities::server::GetConnectedServerArgs {});

                    let verification_timeout = tokio::time::Duration::from_secs(3);
                    let verification_result = tokio::time::timeout(verification_timeout, async {
                        let stream = client.watch_query::<Server, GetConnectedServer>(&query);
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
                        debug!("[PEER] Verified: {} at {}", expected_server_id, address_key);
                        let _ = myself_clone.send_message(PeerManagerMsg::PeerConnected {
                            server_id: expected_server_id,
                            address: address_key,
                            client,
                        });
                    } else {
                        warn!(
                            "[PEER] MISMATCH: {} at {} (got {:?}) - deleting entry",
                            expected_server_id, address_key, actual_id
                        );
                        client.close();
                        Self::delete_server(&command_manager, expected_server_id, host_id).await;
                        let _ = myself_clone.send_message(PeerManagerMsg::ConnectionFailed {
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
                info!("[PEER] CONNECTED: {} at {}", server_id, address);

                // Remove from connecting set
                state.connecting.remove(&server_id);

                // Monitor connection status - when it goes to Disconnected,
                // close the client and notify PeerManager
                let monitor_client = client.clone();
                let myself_clone = myself.clone();
                tokio::spawn(async move {
                    use futures_signals::signal::SignalExt;

                    let mut stream = monitor_client.get_status().to_stream();
                    let mut was_connected = true;

                    while let Some(status) = TokioStreamExt::next(&mut stream).await {
                        match status {
                            crate::client::ConnectionStatus::Connected(_) => {
                                was_connected = true;
                            }
                            crate::client::ConnectionStatus::Disconnected => {
                                if was_connected {
                                    info!("[PEER] LOST: {} - stopping reconnection", server_id);
                                    monitor_client.close();
                                    let _ = myself_clone
                                        .send_message(PeerManagerMsg::PeerDisconnected { server_id });
                                    break;
                                }
                            }
                        }
                    }
                });

                state.peers.insert(
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
                info!("[PEER] DISCONNECTED: {} - deleting entry", server_id);

                // Close the client to stop any reconnection attempts
                if let Some(peer) = state.peers.remove(&server_id) {
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

                let host_id = state.ctx.host_id;
                let req = RequestContext::internal(
                    Arc::from(Uuid::new_v4().to_string()),
                    host_id,
                    "peer-manager",
                );
                let command_manager = state.command_manager.clone();

                // Execute command in background - we don't need to wait for the result
                tokio::spawn(async move {
                    match ractor::call!(command_manager, CommandManagerMsg::Execute, wrapped, req) {
                        Ok(_) => debug!("Successfully deleted disconnected server {}", server_id),
                        Err(e) => warn!("Failed to delete disconnected server {}: {:?}", server_id, e),
                    }
                });
            }

            PeerManagerMsg::PeerRemoved { server_id } => {
                debug!("Peer {} removed from GetPeerServers", server_id);

                // If we're connected, disconnect
                if state.peers.contains_key(&server_id) {
                    let _ = myself.send_message(PeerManagerMsg::PeerDisconnected { server_id });
                }
            }

            PeerManagerMsg::GetConnectedPeers(reply) => {
                let peer_ids: Vec<Uuid> = state.peers.keys().cloned().collect();
                let _ = reply.send(peer_ids);
            }

            PeerManagerMsg::ForwardQuery {
                peer_id,
                query,
                reply,
            } => {
                // Check if this is for the local server
                if peer_id == state.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward query to self".to_string()));
                    return Ok(());
                }

                match state.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let query_id = query.query_id();
                        let query_tx = query.tx_id();

                        // Convert to WrappedQuery using From impl
                        let wrapped: WrappedQuery = query.into();

                        tokio::spawn(async move {
                            debug!("Forwarding query {} to peer {}", query_id, peer_id);

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
                if peer_id == state.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward command to self".to_string()));
                    return Ok(());
                }

                match state.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let command_id = command.command_id();
                        let command_tx = command.tx_id();

                        // Convert to WrappedCommand using From impl
                        let wrapped: WrappedCommand = command.into();

                        tokio::spawn(async move {
                            debug!("Forwarding command {} to peer {}", command_id, peer_id);

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
                if peer_id == state.ctx.host_id {
                    let _ = reply.send(Err("Cannot forward report to self".to_string()));
                    return Ok(());
                }

                match state.peers.get(&peer_id) {
                    Some(peer) => {
                        let client = peer.client.clone();
                        let report_id = report.report_id();
                        let report_tx = report.tx_id();

                        // Convert to WrappedReport using From impl
                        let wrapped: WrappedReport = report.into();

                        tokio::spawn(async move {
                            debug!("Forwarding report {} to peer {}", report_id, peer_id);

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
                let status = state.peers.get(&peer_id).map(|peer| {
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
                let statuses: Vec<PeerStatus> = state
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
                if let Some(peer) = state.peers.get_mut(&peer_id) {
                    peer.last_seen = Some(std::time::Instant::now());
                    peer.last_latency_ms = Some(latency_ms);
                    debug!("Updated health for peer {}: {}ms", peer_id, latency_ms);
                }
            }

            PeerManagerMsg::ConnectionFailed {
                server_id,
                address,
            } => {
                // Just remove from connecting set - server entry already deleted by caller
                state.connecting.remove(&server_id);
                debug!(
                    "[PEER] Connection to {} at {} failed, removed from connecting set",
                    server_id, address
                );
            }

        }

        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("[PEER] Stopping");

        // Cancel discovery task
        if let Some(handle) = state.discovery_handle.take() {
            handle.abort();
        }

        // Close all peer connections
        for (_, peer) in state.peers.drain() {
            peer.client.close();
        }

        Ok(())
    }
}
