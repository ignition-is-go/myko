//! Safe local-network discovery for native Myko nodes.
//!
//! Discovery advertises only non-secret identity and declared operational
//! capabilities. It never creates trust, grants access, reveals application
//! data, or starts replication.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[cfg(not(any(target_os = "ios", target_os = "macos")))]
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdpSocket};

use myko_federation::NodeId;
use myko_iroh::{EndpointId, NativeNodeDescriptor, PairingInvitation};
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task::JoinHandle};

#[cfg(not(any(target_os = "ios", target_os = "macos")))]
use tokio::{
    net::UdpSocket,
    time::{MissedTickBehavior, interval},
};

#[cfg(any(target_os = "ios", target_os = "macos"))]
mod apple;

/// Bonjour service type used for nearby Myko node advertisements on Apple platforms.
pub const MYKO_BONJOUR_SERVICE_TYPE: &str = "_myko-node._udp";
/// IPv4 multicast group used for nearby Myko node advertisements on non-Apple platforms.
#[cfg(not(any(target_os = "ios", target_os = "macos")))]
pub const MYKO_LAN_DISCOVERY_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 75, 82);
/// UDP port used with [`MYKO_LAN_DISCOVERY_GROUP`].
#[cfg(not(any(target_os = "ios", target_os = "macos")))]
pub const MYKO_LAN_DISCOVERY_PORT: u16 = 47_826;
const LAN_DISCOVERY_PROTOCOL_VERSION: u16 = 1;
#[cfg(not(any(target_os = "ios", target_os = "macos")))]
const MAX_LAN_DISCOVERY_PACKET_BYTES: usize = 16 * 1024;
const DEFAULT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_EXPIRY: Duration = Duration::from_secs(12);

/// Operational shape of one native Myko participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantKind {
    /// A daemon-capable machine with durable history and hosted work.
    FullNode,
    /// A foreground-only native client with a stable transport identity.
    ForegroundEdge,
    /// A conventional compatibility client without a native Iroh endpoint.
    WebSocketEdge,
}

/// Explicit operational capability of one participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantCapability {
    DurableHistory,
    BackgroundTransport,
    HostWorkloads,
    LocalWorkspaces,
}

/// Explicit capability set advertised by one participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantCapabilities(BTreeSet<ParticipantCapability>);

impl ParticipantCapabilities {
    /// Capabilities of a normal durable node.
    #[must_use]
    pub fn full_node() -> Self {
        Self(BTreeSet::from([
            ParticipantCapability::DurableHistory,
            ParticipantCapability::BackgroundTransport,
            ParticipantCapability::HostWorkloads,
            ParticipantCapability::LocalWorkspaces,
        ]))
    }

    /// Capabilities of a foreground native edge.
    #[must_use]
    pub const fn foreground_edge() -> Self {
        Self(BTreeSet::new())
    }

    /// Returns whether this participant explicitly advertises a capability.
    #[must_use]
    pub fn supports(&self, capability: ParticipantCapability) -> bool {
        self.0.contains(&capability)
    }
}

/// Persistable non-secret pairing bootstrap state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingPairing {
    pub invitation: PairingInvitation,
    pub expected_inviter: NativeNodeDescriptor,
    pub comparison_code: Option<String>,
}

/// One nearby node exposed to application roster surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredNode {
    pub descriptor: NativeNodeDescriptor,
    pub display_name: String,
    pub kind: ParticipantKind,
    pub capabilities: ParticipantCapabilities,
    pub reachable: bool,
    pub last_error: Option<String>,
}

impl DiscoveredNode {
    /// Returns the authenticated network identity used for authorization.
    #[must_use]
    pub const fn endpoint_id(&self) -> EndpointId {
        self.descriptor.endpoint.id
    }

    /// Returns the immutable Myko history identity behind this endpoint.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.descriptor.node_id
    }
}

/// Non-secret metadata a node is willing to advertise on its local network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanAdvertisement {
    pub descriptor: NativeNodeDescriptor,
    pub display_name: String,
    pub kind: ParticipantKind,
    pub capabilities: ParticipantCapabilities,
}

impl LanAdvertisement {
    /// Creates the safe advertisement shape for a normal daemon.
    #[must_use]
    pub fn full_node(descriptor: NativeNodeDescriptor, display_name: impl Into<String>) -> Self {
        Self {
            descriptor,
            display_name: display_name.into(),
            kind: ParticipantKind::FullNode,
            capabilities: ParticipantCapabilities::full_node(),
        }
    }

    fn discovered(&self) -> DiscoveredNode {
        DiscoveredNode {
            descriptor: self.descriptor.clone(),
            display_name: self.display_name.clone(),
            kind: self.kind,
            capabilities: self.capabilities.clone(),
            reachable: true,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LanPacket {
    version: u16,
    advertisement: LanAdvertisement,
}

#[derive(Debug, Clone)]
struct SeenAdvertisement {
    node: DiscoveredNode,
    observed_at: Instant,
}

/// In-memory roster of time-bounded nearby advertisements.
#[derive(Debug, Default)]
pub struct LanRoster {
    nodes: BTreeMap<String, SeenAdvertisement>,
}

impl LanRoster {
    /// Incorporates one valid advertisement and reports a projection change.
    pub fn observe(&mut self, advertisement: &LanAdvertisement, now: Instant) -> bool {
        let node = advertisement.discovered();
        let key = node.endpoint_id().to_string();
        let changed = self
            .nodes
            .get(&key)
            .is_none_or(|previous| previous.node != node);
        self.nodes.insert(
            key,
            SeenAdvertisement {
                node,
                observed_at: now,
            },
        );
        changed
    }

    /// Removes observations not refreshed before `expiry`.
    pub fn expire(&mut self, now: Instant, expiry: Duration) -> bool {
        let before = self.nodes.len();
        self.nodes
            .retain(|_, seen| now.saturating_duration_since(seen.observed_at) <= expiry);
        self.nodes.len() != before
    }

    /// Removes a participant by authenticated transport identity.
    fn remove_endpoint(&mut self, endpoint: &str) -> bool {
        self.nodes.remove(endpoint).is_some()
    }

    /// Returns nearby nodes in stable endpoint-identity order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<DiscoveredNode> {
        self.nodes.values().map(|seen| seen.node.clone()).collect()
    }
}

/// Running local-network advertisement and discovery driver.
#[derive(Debug)]
pub struct LanDiscovery {
    roster: Arc<Mutex<LanRoster>>,
    updates: watch::Sender<Vec<DiscoveredNode>>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl LanDiscovery {
    /// Starts the platform-appropriate local discovery driver.
    ///
    /// # Errors
    ///
    /// Returns an error when the local discovery transport cannot be configured.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the non-Apple multicast task owns the advertisement for its lifetime"
    )]
    pub fn start(advertisement: LanAdvertisement) -> Result<Self, String> {
        Self::start_with_timing(&advertisement, DEFAULT_ANNOUNCE_INTERVAL, DEFAULT_EXPIRY)
    }

    #[cfg(any(target_os = "ios", target_os = "macos"))]
    fn start_with_timing(
        advertisement: &LanAdvertisement,
        announce_interval: Duration,
        expiry: Duration,
    ) -> Result<Self, String> {
        apple::start(advertisement, announce_interval, expiry)
    }

    #[cfg(not(any(target_os = "ios", target_os = "macos")))]
    fn start_with_timing(
        advertisement: &LanAdvertisement,
        announce_interval: Duration,
        expiry: Duration,
    ) -> Result<Self, String> {
        if announce_interval.is_zero() || expiry < announce_interval {
            return Err("LAN discovery timing is invalid".to_owned());
        }
        let group = SocketAddr::V4(SocketAddrV4::new(
            MYKO_LAN_DISCOVERY_GROUP,
            MYKO_LAN_DISCOVERY_PORT,
        ));
        let socket = bind_lan_socket()?;
        let roster = Arc::new(Mutex::new(LanRoster::default()));
        let (updates, _) = watch::channel(Vec::new());
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task_roster = Arc::clone(&roster);
        let task_updates = updates.clone();
        let local_endpoint = advertisement.descriptor.endpoint.id;
        let advertisement = advertisement.clone();
        let task = tokio::spawn(async move {
            let Ok(packet) = serde_json::to_vec(&LanPacket {
                version: LAN_DISCOVERY_PROTOCOL_VERSION,
                advertisement,
            }) else {
                return;
            };
            let mut announces = interval(announce_interval);
            announces.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut buffer = vec![0_u8; MAX_LAN_DISCOVERY_PACKET_BYTES];
            loop {
                tokio::select! {
                    _ = announces.tick() => {
                        let _sent = socket.send_to(&packet, group).await;
                        publish_expired(&task_roster, &task_updates, expiry);
                    }
                    received = socket.recv_from(&mut buffer) => {
                        let Ok((length, _sender)) = received else { break };
                        let Some(encoded) = buffer.get(..length) else { continue };
                        let Ok(packet) = serde_json::from_slice::<LanPacket>(encoded) else { continue };
                        if packet.version != LAN_DISCOVERY_PROTOCOL_VERSION
                            || packet.advertisement.descriptor.endpoint.id == local_endpoint
                            || packet.advertisement.descriptor.validate().is_err() { continue; }
                        let changed = task_roster.lock().is_ok_and(|mut roster| {
                            roster.observe(&packet.advertisement, Instant::now())
                        });
                        if changed { publish_roster(&task_roster, &task_updates); }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { break; }
                    }
                }
            }
        });
        Ok(Self {
            roster,
            updates,
            shutdown,
            task: Mutex::new(Some(task)),
        })
    }

    /// Returns the current nearby-node projection.
    #[must_use]
    pub fn snapshot(&self) -> Vec<DiscoveredNode> {
        self.roster
            .lock()
            .map(|roster| roster.snapshot())
            .unwrap_or_default()
    }

    /// Subscribes to coherent nearby-node snapshots.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Vec<DiscoveredNode>> {
        self.updates.subscribe()
    }

    /// Stops the local discovery driver.
    pub async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let task = self.task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
            task.abort();
            let _finished = task.await;
        }
    }
}

#[cfg(not(any(target_os = "ios", target_os = "macos")))]
fn bind_lan_socket() -> Result<UdpSocket, String> {
    let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MYKO_LAN_DISCOVERY_PORT);
    let socket = StdUdpSocket::bind(bind)
        .map_err(|error| format!("could not bind LAN discovery UDP socket: {error}"))?;
    socket
        .join_multicast_v4(&MYKO_LAN_DISCOVERY_GROUP, &Ipv4Addr::UNSPECIFIED)
        .map_err(|error| format!("could not join LAN discovery multicast group: {error}"))?;
    socket
        .set_nonblocking(true)
        .map_err(|error| format!("could not configure LAN discovery socket: {error}"))?;
    UdpSocket::from_std(socket)
        .map_err(|error| format!("could not start LAN discovery socket: {error}"))
}

#[cfg(not(any(target_os = "ios", target_os = "macos")))]
fn publish_expired(
    roster: &Mutex<LanRoster>,
    updates: &watch::Sender<Vec<DiscoveredNode>>,
    expiry: Duration,
) {
    let changed = roster
        .lock()
        .is_ok_and(|mut roster| roster.expire(Instant::now(), expiry));
    if changed {
        publish_roster(roster, updates);
    }
}

fn publish_roster(roster: &Mutex<LanRoster>, updates: &watch::Sender<Vec<DiscoveredNode>>) {
    let snapshot = roster
        .lock()
        .map(|roster| roster.snapshot())
        .unwrap_or_default();
    updates.send_replace(snapshot);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_edges_never_imply_hosted_workloads() {
        let capabilities = ParticipantCapabilities::foreground_edge();
        assert!(!capabilities.supports(ParticipantCapability::HostWorkloads));
        assert!(!capabilities.supports(ParticipantCapability::LocalWorkspaces));
        assert!(!capabilities.supports(ParticipantCapability::BackgroundTransport));
    }
}
