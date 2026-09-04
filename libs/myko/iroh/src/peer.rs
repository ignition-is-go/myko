use super::{
    Arc, Connection, Duration, EndpointAddr, EndpointId, HashMap, IrohReplicationError,
    IrohReplicator, JoinHandle, LiveEvent, LogPosition, Mutex, NodeId, RecvStream,
    ReplicationCursorStore, ReplicationFrame, ReplicationSelection, authorization_error,
    read_frame, watch,
};
/// Observable state of one supervised peer follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSyncStatus {
    pub peer: EndpointAddr,
    /// Expected Myko history identity for a pinned peer, if configured.
    pub expected_source_node: Option<NodeId>,
    pub source_node: Option<NodeId>,
    pub cursor: Option<LogPosition>,
    pub connected: bool,
    pub successful_connections: u64,
    pub successful_batches: u64,
    pub last_error: Option<String>,
}

/// Handle to a background cursor-tracked peer follower.
#[derive(Debug)]
pub struct PeerSync {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
    pub(super) status: watch::Receiver<PeerSyncStatus>,
}

/// One authenticated remote subscription to best-effort Myko live events.
///
/// The stream has no replay guarantee. Consumers detect sequence gaps and
/// recover authoritative state through durable Myko queries or change streams.
#[derive(Debug)]
pub struct IrohLiveEventSubscription {
    pub(super) connection: Connection,
    pub(super) receive: RecvStream,
    pub(super) source_node: NodeId,
}

impl IrohLiveEventSubscription {
    /// Returns the stable Myko identity advertised by the serving peer.
    #[must_use]
    pub const fn source_node(&self) -> NodeId {
        self.source_node
    }

    /// Receives the next best-effort live event.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream closes or sends an unexpected frame.
    pub async fn recv(&mut self) -> Result<LiveEvent, IrohReplicationError> {
        match read_frame(&mut self.receive).await? {
            ReplicationFrame::Live { event } => Ok(*event),
            ReplicationFrame::Authorization { decision } => Err(authorization_error(decision)),
            ReplicationFrame::Error { message } => Err(IrohReplicationError::Stream(format!(
                "remote live subscription failed: {message}"
            ))),
            ReplicationFrame::Hello { .. }
            | ReplicationFrame::Batch { .. }
            | ReplicationFrame::ScopedBatch { .. }
            | ReplicationFrame::SelectedBatch { .. }
            | ReplicationFrame::ScopeCatalog { .. }
            | ReplicationFrame::Command { .. }
            | ReplicationFrame::CommandState { .. }
            | ReplicationFrame::CommandWatchReady { .. }
            | ReplicationFrame::CommandUpdate { .. }
            | ReplicationFrame::ItemState { .. }
            | ReplicationFrame::ItemFollowReady { .. }
            | ReplicationFrame::ItemUpdate { .. }
            | ReplicationFrame::HandlerState { .. }
            | ReplicationFrame::HandlerViewDelta { .. }
            | ReplicationFrame::Approval { .. } => Err(IrohReplicationError::Stream(
                "peer sent a non-live frame on a live subscription".to_owned(),
            )),
        }
    }

    /// Closes the live stream without shutting down the shared endpoint.
    pub fn close(self) {
        self.connection
            .close(0u32.into(), b"live subscription closed");
    }
}

/// Node-level supervisor for concurrent replication followers.
///
/// Peers are keyed by authenticated Iroh endpoint identity. Updating an entry
/// installs its new address or cursor policy and then shuts down the replaced
/// follower. Removing one peer does not disturb any other stream.
#[derive(Debug)]
pub struct PeerSupervisor {
    replicator: IrohReplicator,
    peers: Mutex<HashMap<EndpointId, PeerSync>>,
    status_revision: watch::Sender<u64>,
}

impl PeerSync {
    /// Returns a snapshot of replication progress and the latest transient error.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor state lock is poisoned.
    pub fn status(&self) -> Result<PeerSyncStatus, IrohReplicationError> {
        Ok(self.status.borrow().clone())
    }

    /// Subscribes to status changes for this follower.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<PeerSyncStatus> {
        self.status.clone()
    }

    /// Stops the follower and waits for its current pull or retry delay to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the background task terminated abnormally.
    pub async fn shutdown(self) -> Result<(), IrohReplicationError> {
        let _ = self.shutdown.send(true);
        self.task
            .await
            .map_err(|error| IrohReplicationError::Supervisor(error.to_string()))
    }
}

impl PeerSupervisor {
    /// Creates an empty peer supervisor over a running replication endpoint.
    #[must_use]
    pub fn new(replicator: IrohReplicator) -> Self {
        let (status_revision, _status_updates) = watch::channel(0);
        Self {
            replicator,
            peers: Mutex::new(HashMap::new()),
            status_revision,
        }
    }

    /// Starts or replaces a transient-cursor follower for one peer.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a replaced follower
    /// cannot be shut down cleanly.
    pub async fn upsert(
        &self,
        peer: EndpointAddr,
        after: Option<LogPosition>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self.replicator.follow(peer, after, retry_interval);
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a durable, source-aware follower for one peer.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if its checkpoint cannot be loaded, supervisor state is
    /// poisoned, or a replaced follower cannot be shut down cleanly.
    pub async fn upsert_persisted(
        &self,
        peer: EndpointAddr,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self
            .replicator
            .follow_persisted(peer, store, retry_interval)?;
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a durable follower with one replication selection.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be loaded, supervisor state
    /// is poisoned, or a replaced follower cannot shut down cleanly.
    pub async fn upsert_persisted_selected(
        &self,
        peer: EndpointAddr,
        selection: ReplicationSelection,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower =
            self.replicator
                .follow_persisted_selected(peer, selection, store, retry_interval)?;
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a durable follower pinned to one Myko source.
    ///
    /// Returns `true` when an existing follower with the same authenticated
    /// endpoint identity was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if its checkpoint cannot be loaded, supervisor state is
    /// poisoned, or a replaced follower cannot be shut down cleanly.
    pub async fn upsert_persisted_source(
        &self,
        peer: EndpointAddr,
        expected_source_node: NodeId,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self.replicator.follow_persisted_source(
            peer,
            expected_source_node,
            store,
            retry_interval,
        )?;
        self.replace(peer_id, follower).await
    }

    /// Starts or replaces a selected durable follower pinned to one Myko source.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be loaded, supervisor state
    /// is poisoned, or a replaced follower cannot shut down cleanly.
    pub async fn upsert_persisted_source_selected(
        &self,
        peer: EndpointAddr,
        expected_source_node: NodeId,
        selection: ReplicationSelection,
        store: Arc<dyn ReplicationCursorStore>,
        retry_interval: Duration,
    ) -> Result<bool, IrohReplicationError> {
        let peer_id = peer.id;
        let follower = self.replicator.follow_persisted_source_selected(
            peer,
            expected_source_node,
            selection,
            store,
            retry_interval,
        )?;
        self.replace(peer_id, follower).await
    }

    async fn replace(
        &self,
        peer_id: EndpointId,
        follower: PeerSync,
    ) -> Result<bool, IrohReplicationError> {
        let mut follower_status = follower.subscribe_status();
        let status_revision = self.status_revision.clone();
        tokio::spawn(async move {
            while follower_status.changed().await.is_ok() {
                status_revision.send_modify(|revision| {
                    *revision = revision.saturating_add(1);
                });
            }
        });
        let replaced = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?
            .insert(peer_id, follower);
        self.status_revision.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        let was_replaced = replaced.is_some();
        if let Some(replaced) = replaced {
            replaced.shutdown().await?;
        }
        Ok(was_replaced)
    }

    /// Stops and removes one peer follower.
    ///
    /// Returns `false` if the peer was not being followed.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or the follower cannot
    /// be shut down cleanly.
    pub async fn remove(&self, peer_id: EndpointId) -> Result<bool, IrohReplicationError> {
        let removed = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?
            .remove(&peer_id);
        let was_removed = removed.is_some();
        if was_removed {
            self.status_revision.send_modify(|revision| {
                *revision = revision.saturating_add(1);
            });
        }
        if let Some(removed) = removed {
            removed.shutdown().await?;
        }
        Ok(was_removed)
    }

    /// Returns snapshots for every currently supervised peer.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor or follower state is poisoned.
    pub fn statuses(&self) -> Result<Vec<PeerSyncStatus>, IrohReplicationError> {
        let peers = self
            .peers
            .lock()
            .map_err(|_| IrohReplicationError::Supervisor("peer lock is poisoned".to_owned()))?;
        let mut statuses = peers
            .values()
            .map(PeerSync::status)
            .collect::<Result<Vec<_>, _>>()?;
        drop(peers);
        statuses.sort_by_key(|status| status.peer.id.to_string());
        Ok(statuses)
    }

    /// Subscribes to any follower status or membership change.
    #[must_use]
    pub fn subscribe_statuses(&self) -> watch::Receiver<u64> {
        self.status_revision.subscribe()
    }

    /// Stops every peer follower while retaining the empty supervisor.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a follower cannot be
    /// shut down cleanly.
    pub async fn shutdown_all(&self) -> Result<(), IrohReplicationError> {
        let peers = {
            let mut peers = self.peers.lock().map_err(|_| {
                IrohReplicationError::Supervisor("peer lock is poisoned".to_owned())
            })?;
            std::mem::take(&mut *peers)
        };
        self.status_revision.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        for follower in peers.into_values() {
            follower.shutdown().await?;
        }
        Ok(())
    }

    /// Stops every peer follower, leaving the shared Iroh endpoint running.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned or a follower cannot be
    /// shut down cleanly.
    pub async fn shutdown(self) -> Result<(), IrohReplicationError> {
        self.shutdown_all().await
    }
}
