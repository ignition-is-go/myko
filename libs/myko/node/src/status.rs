//! Framework-owned live projection of native peer status.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use myko_app::capability::{CollectionBuilding as _, ResourceScoped as _};
use myko_app::{AppError, ViewContext, ViewHandler, myko_view};
use myko_federation::NodeId;
use myko_iroh::{EndpointId, NativeNodeDescriptor, PeerSupervisor, PeerSyncStatus};
use myko_items::myko_subtype;
use tokio::sync::watch;

use crate::{ConfiguredPeer, Peer, live_state::RuntimeFeed};

/// One node identity and its directional replication status.
#[myko_subtype(derive(Eq))]
#[allow(clippy::struct_excessive_bools)]
pub struct NodeStatus {
    pub source_node: Option<NodeId>,
    pub endpoint_id: EndpointId,
    pub local: bool,
    pub pinned: bool,
    pub replication_enabled: bool,
    pub connected: bool,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct NodeStatusViewState {
    nodes: RuntimeFeed<NodeStatus>,
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
        Self {
            nodes: RuntimeFeed::new(nodes),
        }
    }

    pub fn publish(&self, nodes: Vec<NodeStatus>) {
        self.nodes.publish(nodes);
    }

    pub fn invalidate(&self, reason: impl Into<String>) {
        self.nodes.invalidate(reason);
    }
}

/// Live identity and follower state for the local node and configured peers.
#[myko_view(NodeStatus, item = Peer)]
#[derive(Copy, PartialEq, Eq)]
pub struct NodeStatusView;

impl ViewHandler for NodeStatusView {
    type Item = NodeStatus;
    type Cursor = u64;

    fn item_key(item: &Self::Item) -> Arc<str> {
        Arc::from(item.endpoint_id.to_string())
    }

    fn build(
        &self,
        context: &ViewContext,
    ) -> Result<myko_federation::LiveCollection<Self::Item, u64>, AppError> {
        let state = context.resource::<NodeStatusViewState>()?;
        context.collection_from_subscription(&state.nodes.live, Self::item_key)
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
        source_node: Some(descriptor.node_id),
        endpoint_id: descriptor.endpoint.id,
        local: true,
        pinned: true,
        replication_enabled: false,
        connected: true,
        last_error: None,
    }];
    nodes.extend(peers.values().map(|peer| {
        let status = statuses.get(&peer.endpoint.id);
        NodeStatus {
            source_node: peer.source_node,
            endpoint_id: peer.endpoint.id,
            local: false,
            pinned: peer.source_node.is_some(),
            replication_enabled: peer.following,
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
