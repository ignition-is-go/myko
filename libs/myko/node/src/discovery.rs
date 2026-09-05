//! Framework-owned LAN discovery configuration and live nearby-node view.

#![allow(clippy::expect_used)] // Infallible handler builders rely on validated host wiring.

use std::sync::Arc;

use hyphae::{CellImmutable, CellMap, CellMutable, MapExt as _};
use myko::{
    ApplicationHost, CommandContext, CommandError, CommandHandler, myko_report, myko_view,
    myko_view_item,
    report::{ReportContext, ReportHandler},
    view::{ViewBuildArgs, ViewHandler},
};
use myko_discovery::{DiscoveredNode, LanAdvertisement, LanDiscovery};
use myko_federation::NodeId;
use myko_iroh::NativeNodeDescriptor;
use myko_items::{myko_command, myko_item};
use tokio::sync::{mpsc, watch};

use crate::{
    FederationService, discovery_capability_id,
    peer::{PeerRoster, PeerRosterId, peer_scope},
};

/// Durable, node-local configuration for framework-owned LAN discovery.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct DiscoverySettings {
    pub display_name: String,
    #[serde(default)]
    pub hostname: String,
    pub enabled: bool,
}

myko::register_federated_item!(DiscoverySettings);

/// Changes how this node advertises itself and discovers peers on its LAN.
#[myko_command(DiscoverySettings, item = DiscoverySettings)]
pub struct ConfigureLanDiscovery {
    pub display_name: String,
    pub enabled: bool,
}

impl CommandHandler for ConfigureLanDiscovery {
    fn scope(&self, node_id: NodeId) -> PeerRosterId {
        PeerRosterId::from(node_id.to_string())
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let display_name = self.display_name.trim().to_owned();
        if display_name.is_empty() {
            return Err(CommandError::reject(
                "LAN discovery display name must not be empty",
            ));
        }
        let settings = DiscoverySettings {
            id: DiscoverySettingsId::from(context.node_id().to_string()),
            peer_roster_id: PeerRosterId::from(context.node_id().to_string()),
            display_name,
            hostname: myko_discovery::machine_hostname(),
            enabled: self.enabled,
        };
        context.emit_set(&settings)?;
        Ok(settings)
    }
}

/// Live LAN-discovery configuration for one authoritative node.
#[myko_report(Option<DiscoverySettings>, item = DiscoverySettings)]
#[derive(Copy, PartialEq, Eq)]
pub struct DiscoverySettingsReport {
    pub source_node: NodeId,
}

impl ReportHandler for DiscoverySettingsReport {
    type Output = Option<DiscoverySettings>;

    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn compute(
        &self,
        context: ReportContext,
    ) -> impl hyphae::Materialize<Arc<Self::Output>, hyphae::Definite> {
        let id = DiscoverySettingsId::from(self.source_node.to_string());
        myko::item::typed_map_arc_from_any_item::<DiscoverySettings>(
            context
                .federated_items::<DiscoverySettings>()
                .expect("validated discovery-settings federation source"),
            "DiscoverySettingsReport",
        )
        .entries()
        .map(move |settings| {
            Arc::new(
                settings
                    .iter()
                    .find(|(_, setting)| setting.id == id)
                    .map(|(_, setting)| setting.as_ref().clone()),
            )
        })
    }
}

#[derive(Clone)]
pub struct DiscoveryViewState {
    writer: CellMap<Arc<str>, Arc<DiscoveredNodeRow>, CellMutable>,
    nearby: CellMap<Arc<str>, Arc<DiscoveredNodeRow>, CellImmutable>,
}

impl std::fmt::Debug for DiscoveryViewState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiscoveryViewState")
            .finish_non_exhaustive()
    }
}

impl DiscoveryViewState {
    pub fn new() -> Self {
        let writer = CellMap::new();
        let nearby = writer.clone().lock();
        Self { writer, nearby }
    }

    pub fn publish(&self, nodes: Vec<DiscoveredNode>) {
        self.writer.replace_all(
            nodes
                .into_iter()
                .map(|node| {
                    let id = Arc::from(node.endpoint_id().to_string());
                    (Arc::clone(&id), Arc::new(DiscoveredNodeRow { id, node }))
                })
                .collect(),
        );
    }

    pub(crate) fn descriptor_for(&self, node_id: NodeId) -> Result<NativeNodeDescriptor, String> {
        self.nearby
            .snapshot()
            .into_iter()
            .map(|(_, row)| row)
            .find(|row| row.node.node_id() == node_id)
            .map(|row| row.node.descriptor.clone())
            .ok_or_else(|| "the selected node is no longer visible through discovery".to_owned())
    }
}

#[myko_view_item]
#[derive(Eq)]
pub struct DiscoveredNodeRow {
    pub id: Arc<str>,
    pub node: DiscoveredNode,
}

/// Live untrusted LAN advertisements visible to this node.
#[myko_view(DiscoveredNodeRow, item = PeerRoster)]
#[derive(PartialEq, Eq)]
pub struct NearbyNodesView {
    pub source_node: NodeId,
}

impl ViewHandler for NearbyNodesView {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![discovery_capability_id()]
    }

    fn build_cell(
        context: ViewBuildArgs<Self>,
    ) -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        context
            .resource::<DiscoveryViewState>()
            .expect("discovery view resource is installed")
            .nearby
            .clone()
    }
}

/// Retained runtime effect connecting durable settings to multicast discovery.
#[derive(Debug)]
pub struct DiscoverySupervisor {
    stopping: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl DiscoverySupervisor {
    pub fn start(
        application: &ApplicationHost,
        descriptor: NativeNodeDescriptor,
        state: DiscoveryViewState,
        network_enabled: bool,
    ) -> Result<Self, String> {
        let settings = application.watch_items::<DiscoverySettings>(
            Some(application.node().node_id()),
            Some(peer_scope(application.node().node_id())),
        )?;
        let (stopping, stop) = watch::channel(false);
        let task = tokio::spawn(run_discovery(
            descriptor,
            state,
            settings,
            network_enabled,
            stop,
        ));
        Ok(Self {
            stopping,
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) -> Result<(), String> {
        self.stopping.send_replace(true);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await
            .map_err(|error| format!("LAN discovery supervisor panicked: {error}"))?
    }
}

impl Drop for DiscoverySupervisor {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_discovery(
    descriptor: NativeNodeDescriptor,
    state: DiscoveryViewState,
    settings: myko::view::TypedViewCellMap<DiscoverySettings>,
    network_enabled: bool,
    mut stopping: watch::Receiver<bool>,
) -> Result<(), String> {
    let (change_sender, mut change_receiver) = mpsc::channel(1);
    change_sender
        .try_send(())
        .map_err(|_| "LAN discovery settings could not publish initial state".to_owned())?;
    let updates = change_sender.clone();
    let changes_guard = settings.subscribe_diffs(move |_| {
        let _sent = updates.try_send(());
    });
    let mut current = None;
    let mut discovery: Option<LanDiscovery> = None;
    let mut discovery_updates = None;
    loop {
        tokio::select! {
            changed = stopping.changed() => {
                if changed.is_err() || *stopping.borrow() {
                    break;
                }
            }
            update = change_receiver.recv() => {
                update.ok_or_else(|| "LAN discovery settings subscription closed".to_owned())?;
                        let next = settings
                            .snapshot()
                            .into_iter()
                            .next()
                            .map(|(_, settings)| settings.as_ref().clone());
                        if current != next {
                            if let Some(running) = discovery.take() {
                                running.shutdown().await;
                            }
                            discovery_updates = None;
                            state.publish(Vec::new());
                            if let Some(settings) = next
                                .as_ref()
                                .filter(|settings| network_enabled && settings.enabled)
                            {
                                let running = LanDiscovery::start(LanAdvertisement::full_node(
                                    descriptor.clone(),
                                    settings.display_name.clone(),
                                ))?;
                                state.publish(running.snapshot());
                                discovery_updates = Some(running.subscribe());
                                discovery = Some(running);
                            }
                            current = next;
                        }
            }
            changed = wait_for_discovery(&mut discovery_updates) => {
                changed?;
                if let Some(updates) = &discovery_updates {
                    state.publish(updates.borrow().clone());
                }
            }
        }
    }
    if let Some(running) = discovery {
        running.shutdown().await;
    }
    drop(changes_guard);
    drop(settings);
    Ok(())
}

async fn wait_for_discovery(
    updates: &mut Option<watch::Receiver<Vec<DiscoveredNode>>>,
) -> Result<(), String> {
    match updates {
        Some(updates) => updates.changed().await.map_err(|error| error.to_string()),
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use myko_discovery::{ParticipantCapabilities, ParticipantKind};
    use myko_iroh::{EndpointAddr, SecretKey};

    fn discovered_node(node_id: NodeId, display_name: &str) -> DiscoveredNode {
        DiscoveredNode {
            descriptor: NativeNodeDescriptor::new(
                node_id,
                EndpointAddr::new(SecretKey::generate().public()),
            ),
            display_name: display_name.to_owned(),
            hostname: "test-host".to_owned(),
            kind: ParticipantKind::FullNode,
            capabilities: ParticipantCapabilities::full_node(),
            reachable: true,
            last_error: None,
        }
    }

    #[test]
    fn discovery_resolves_the_current_descriptor_by_node_identity() -> Result<(), String> {
        let state = DiscoveryViewState::new();
        let first = discovered_node(NodeId::new(), "first");
        let selected = discovered_node(NodeId::new(), "selected");
        state.publish(vec![first, selected.clone()]);

        let descriptor = state.descriptor_for(selected.node_id())?;
        if descriptor != selected.descriptor {
            return Err("discovery resolved the wrong node descriptor".to_owned());
        }
        if state.descriptor_for(NodeId::new()).is_ok() {
            return Err("discovery resolved a node that is not currently visible".to_owned());
        }
        Ok(())
    }
}
