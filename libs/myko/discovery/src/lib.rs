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

use myko_federation::NodeId;
use myko_iroh::{EndpointId, NativeNodeDescriptor};
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task::JoinHandle};

#[cfg(any(target_os = "ios", target_os = "macos"))]
mod apple;
mod bonjour;
#[cfg(any(test, not(any(target_os = "ios", target_os = "macos"))))]
mod portable;

/// DNS-SD service type used for nearby Myko node advertisements on every platform.
pub const MYKO_BONJOUR_SERVICE_TYPE: &str = "_myko-node._udp";
const LAN_DISCOVERY_PROTOCOL_VERSION: u16 = 1;
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
        reason = "the discovery driver owns its complete startup configuration"
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
        portable::start(advertisement, announce_interval, expiry)
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
        if let Some(mut task) = task
            && tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _finished = task.await;
        }
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

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "requires a live multicast-capable local network"]
    async fn native_apple_and_portable_dns_sd_interoperate() -> Result<(), String> {
        use myko_iroh::{EndpointAddr, NativeNodeDescriptor, SecretKey};

        let native_advertisement = LanAdvertisement::full_node(
            NativeNodeDescriptor::new(
                NodeId::new(),
                EndpointAddr::new(SecretKey::generate().public()),
            ),
            "Native Bonjour test node",
        );
        let portable_advertisement = LanAdvertisement::full_node(
            NativeNodeDescriptor::new(
                NodeId::new(),
                EndpointAddr::new(SecretKey::generate().public()),
            ),
            "Portable DNS-SD test node",
        );
        let native = apple::start(
            &native_advertisement,
            Duration::from_secs(1),
            Duration::from_secs(4),
        )?;
        let portable = portable::start(
            &portable_advertisement,
            Duration::from_secs(1),
            Duration::from_secs(4),
        )?;

        wait_for_endpoint(
            native.subscribe(),
            portable_advertisement.descriptor.endpoint.id,
        )
        .await?;
        wait_for_endpoint(
            portable.subscribe(),
            native_advertisement.descriptor.endpoint.id,
        )
        .await?;
        native.shutdown().await;
        portable.shutdown().await;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    async fn wait_for_endpoint(
        mut updates: watch::Receiver<Vec<DiscoveredNode>>,
        expected: EndpointId,
    ) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                if updates
                    .borrow()
                    .iter()
                    .any(|node| node.endpoint_id() == expected)
                {
                    return Ok(());
                }
                updates.changed().await.map_err(|error| error.to_string())?;
            }
        })
        .await
        .map_err(|_| format!("timed out discovering DNS-SD endpoint {expected}"))?
    }
}
