//! Framework-owned live projection of native peer status.

#![allow(clippy::expect_used)] // Infallible handler builders rely on validated host wiring.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use hyphae::{CellImmutable, CellMap, CellMutable};
use myko::{
    myko_view, myko_view_item,
    view::{ViewBuildArgs, ViewHandler},
};
use myko_federation::{NodeId, ReplicationSelection};
use myko_iroh::{EndpointId, NativeNodeDescriptor, PeerSupervisor, PeerSyncStatus};
use tokio::sync::watch;

use crate::{ConfiguredPeer, Peer, node_status_capability_id, peer::peer_scope};

/// One node identity and its directional replication status.
#[myko_view_item]
#[derive(Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NodeStatus {
    pub id: Arc<str>,
    pub source_node: Option<NodeId>,
    pub endpoint_id: EndpointId,
    pub local: bool,
    pub pinned: bool,
    pub replication_enabled: bool,
    pub replication_selection: ReplicationSelection,
    pub connected: bool,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct NodeStatusViewState {
    writer: CellMap<Arc<str>, Arc<NodeStatus>, CellMutable>,
    nodes: CellMap<Arc<str>, Arc<NodeStatus>, CellImmutable>,
}

impl std::fmt::Debug for NodeStatusViewState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeStatusViewState")
            .finish_non_exhaustive()
    }
}

impl NodeStatusViewState {
    pub fn new(nodes: Vec<NodeStatus>) -> Self {
        let writer = CellMap::new();
        writer.replace_all(
            nodes
                .into_iter()
                .map(|node| (Arc::clone(&node.id), Arc::new(node)))
                .collect(),
        );
        let nodes = writer.clone().lock();
        Self { writer, nodes }
    }

    pub fn publish(&self, nodes: Vec<NodeStatus>) {
        self.writer.replace_all(
            nodes
                .into_iter()
                .map(|node| (Arc::clone(&node.id), Arc::new(node)))
                .collect(),
        );
    }

    pub fn invalidate(&self, reason: impl Into<String>) {
        tracing::warn!(reason = %reason.into(), "node status projection invalidated");
        self.writer.replace_all(Vec::new());
    }
}

/// Live identity and replication state for the local node and configured peers.
#[myko_view(NodeStatus, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct NodeStatusView {
    pub source_node: NodeId,
}

impl ViewHandler for NodeStatusView {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<myko_federation::ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![node_status_capability_id()]
    }

    fn build_cell(
        context: ViewBuildArgs<Self>,
    ) -> impl myko::view::ViewBuildOutput<Item = Self::Item> {
        myko::view::LocalView::new({
            context
                .resource::<NodeStatusViewState>()
                .expect("node status resource is installed")
                .nodes
                .clone()
        })
    }
}

pub fn project_node_statuses(
    descriptor: &NativeNodeDescriptor,
    peers: &BTreeMap<EndpointId, ConfiguredPeer>,
    statuses: &[PeerSyncStatus],
) -> Vec<NodeStatus> {
    let statuses = statuses
        .iter()
        .map(|status| (status.peer.id, status))
        .collect::<BTreeMap<_, _>>();
    let mut nodes = vec![NodeStatus {
        id: Arc::from(descriptor.endpoint.id.to_string()),
        source_node: Some(descriptor.node_id),
        endpoint_id: descriptor.endpoint.id,
        local: true,
        pinned: true,
        replication_enabled: false,
        replication_selection: ReplicationSelection::All,
        connected: true,
        last_error: None,
    }];
    nodes.extend(peers.values().map(|peer| {
        let status = statuses.get(&peer.endpoint.id);
        NodeStatus {
            id: Arc::from(peer.endpoint.id.to_string()),
            source_node: peer.source_node,
            endpoint_id: peer.endpoint.id,
            local: false,
            pinned: peer.source_node.is_some(),
            replication_enabled: peer.replication_enabled,
            replication_selection: peer.replication_selection.clone(),
            connected: status.is_some_and(|current| current.connected),
            last_error: status.and_then(|current| current.last_error.clone()),
        }
    }));
    nodes
}

#[derive(Debug)]
pub struct NodeStatusProjectionGuard {
    stopping: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl NodeStatusProjectionGuard {
    pub fn start(
        descriptor: NativeNodeDescriptor,
        peers: Arc<Mutex<BTreeMap<EndpointId, ConfiguredPeer>>>,
        supervisor: Arc<PeerSupervisor>,
        state: NodeStatusViewState,
    ) -> Self {
        let (stopping, mut stop) = watch::channel(false);
        let mut updates = supervisor.subscribe_statuses();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            break;
                        }
                    }
                    changed = updates.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let Ok(peers) = peers.lock() else {
                            break;
                        };
                        let Ok(statuses) = supervisor.statuses() else {
                            break;
                        };
                        state.publish(project_node_statuses(&descriptor, &peers, &statuses));
                    }
                }
            }
        });
        Self {
            stopping,
            task: Some(task),
        }
    }

    pub async fn shutdown(mut self) {
        self.stopping.send_replace(true);
        if let Some(task) = self.task.take() {
            let _stopped = task.await;
        }
    }
}

impl Drop for NodeStatusProjectionGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
