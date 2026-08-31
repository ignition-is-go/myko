//! Restartable native node composition for Myko 7.
//!
//! This crate combines the transport-neutral federation node, a durable Redb
//! journal, and the native Iroh transport. It owns operational identity and
//! peer-following state, but deliberately knows nothing about an application's
//! commands, projections, workspace paths, or authorization model.

#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use myko_app::{ApplicationNode, ApplicationSchema};
use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, ItemQuery, LiveSubscription, LiveSubscriptionState, Node,
    NodeError, NodeId, ScopeId, SubscriptionLiveness, live_subscription,
};
use myko_iroh::{
    EndpointAddr, EndpointId, IrohReplicationError, IrohReplicator, PeerSupervisor, PeerSyncStatus,
    load_or_create_secret_key,
};
pub use myko_iroh::{
    NativeNodeDescriptor, NativePeerReference, PairingInvitation, PairingReceipt,
    PairingReceiptSubscription,
};
use myko_redb::RedbJournal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const CONFIG_VERSION: u32 = 2;
const LEGACY_CONFIG_VERSION: u32 = 1;
const JOURNAL_FILE: &str = "node.redb";
const SECRET_FILE: &str = "iroh-secret.json";
const PEERS_FILE: &str = "peers.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPeerConfig {
    version: u32,
    peers: Vec<ConfiguredPeer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyStoredPeerConfig {
    version: u32,
    peers: Vec<EndpointAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StoredConfigHeader {
    version: u32,
}

/// One durable peer binding, optionally pinned to an expected Myko history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfiguredPeer {
    pub endpoint: EndpointAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node: Option<NodeId>,
}

impl ConfiguredPeer {
    const fn unpinned(endpoint: EndpointAddr) -> Self {
        Self {
            endpoint,
            source_node: None,
        }
    }

    fn pinned(descriptor: NativeNodeDescriptor) -> Self {
        Self {
            endpoint: descriptor.endpoint,
            source_node: Some(descriptor.node_id),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BindMode {
    Network,
    Loopback,
}

/// Failure while opening or operating a durable native Myko node.
#[derive(Debug, Error)]
pub enum DurableNodeError {
    /// The event journal could not be opened or replayed.
    #[error(transparent)]
    Node(#[from] NodeError),
    /// The native Iroh endpoint or one of its followers failed.
    #[error(transparent)]
    Iroh(#[from] IrohReplicationError),
    /// Durable JSON state was malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Durable state could not be read or committed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Durable peer state violated a node invariant.
    #[error("invalid durable node configuration: {0}")]
    Configuration(String),
    /// Shared runtime state could not be accessed.
    #[error("durable node state unavailable: {0}")]
    State(String),
}

/// A restartable Redb-backed Myko node with a native Iroh endpoint.
///
/// The data directory is the node's operational identity boundary. Opening the
/// same directory restores both the Myko event identity and Iroh transport
/// identity, then resumes every configured source-aware peer follower.
#[derive(Debug)]
pub struct DurableIrohNode {
    data_dir: PathBuf,
    node: Node,
    journal: Arc<RedbJournal>,
    replicator: IrohReplicator,
    supervisor: PeerSupervisor,
    peers: Mutex<BTreeMap<EndpointId, ConfiguredPeer>>,
    peer_updates: AsyncMutex<()>,
    retry_interval: Duration,
}

/// Runtime-owned driver for one Hyphae-backed typed item subscription.
///
/// Dropping this owner stops the receive task. Clones of [`Self::live`] remain
/// readable but no longer claim to be connected to an active transport.
pub struct ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    live: LiveSubscription<T>,
    writer: myko_federation::LiveSubscriptionWriter<T>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    /// Returns the composable Hyphae lifecycle cell.
    #[must_use]
    pub const fn live(&self) -> &LiveSubscription<T> {
        &self.live
    }
}

impl<T> Drop for ReactiveItemSubscription<T>
where
    T: hyphae::CellValue,
{
    fn drop(&mut self) {
        self.writer.invalidate("subscription owner dropped");
        self.task.abort();
    }
}

impl DurableIrohNode {
    /// Opens a durable node with normal Iroh relay and discovery configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state cannot be restored, the endpoint
    /// cannot bind, or a configured follower cannot start.
    pub async fn open(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
    ) -> Result<Self, DurableNodeError> {
        Self::open_with_policy(data_dir, retry_interval, |_| {
            Ok(Arc::new(AllowAllAccessPolicy))
        })
        .await
    }

    /// Opens a durable node and resolves its initial access policy from the
    /// restored Myko projection before the Iroh router begins serving.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state or policy cannot be restored, the
    /// endpoint cannot bind, or a configured follower cannot start.
    pub async fn open_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        resolve_policy: F,
    ) -> Result<Self, DurableNodeError>
    where
        F: FnOnce(&Node) -> Result<Arc<dyn AccessPolicy>, DurableNodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            None,
            resolve_policy,
        )
        .await
    }

    /// Opens a durable loopback-only node for local development and tests.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state cannot be restored, the endpoint
    /// cannot bind, or a configured follower cannot start.
    pub async fn open_loopback(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
    ) -> Result<Self, DurableNodeError> {
        Self::open_loopback_with_policy(data_dir, retry_interval, |_| {
            Ok(Arc::new(AllowAllAccessPolicy))
        })
        .await
    }

    /// Opens a loopback node with policy restored before the router serves.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state or policy cannot be restored, the
    /// endpoint cannot bind, or a configured follower cannot start.
    pub async fn open_loopback_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        resolve_policy: F,
    ) -> Result<Self, DurableNodeError>
    where
        F: FnOnce(&Node) -> Result<Arc<dyn AccessPolicy>, DurableNodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Loopback,
            None,
            resolve_policy,
        )
        .await
    }

    /// Opens a durable network node that serves one immutable application
    /// handler schema over the same authenticated Iroh endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, schema serving,
    /// endpoint binding, or peer restoration fails.
    pub async fn open_application_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        schema: ApplicationSchema,
        resolve_policy: F,
    ) -> Result<Self, DurableNodeError>
    where
        F: FnOnce(&Node) -> Result<Arc<dyn AccessPolicy>, DurableNodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Network,
            Some(schema),
            resolve_policy,
        )
        .await
    }

    /// Opens a durable loopback node that serves one immutable application
    /// handler schema.
    ///
    /// # Errors
    ///
    /// Returns an error if durable state, policy restoration, schema serving,
    /// endpoint binding, or peer restoration fails.
    pub async fn open_loopback_application_with_policy<F>(
        data_dir: impl AsRef<Path>,
        retry_interval: Duration,
        schema: ApplicationSchema,
        resolve_policy: F,
    ) -> Result<Self, DurableNodeError>
    where
        F: FnOnce(&Node) -> Result<Arc<dyn AccessPolicy>, DurableNodeError>,
    {
        Self::open_inner(
            data_dir.as_ref(),
            retry_interval,
            BindMode::Loopback,
            Some(schema),
            resolve_policy,
        )
        .await
    }

    async fn open_inner<F>(
        data_dir: &Path,
        retry_interval: Duration,
        bind_mode: BindMode,
        schema: Option<ApplicationSchema>,
        resolve_policy: F,
    ) -> Result<Self, DurableNodeError>
    where
        F: FnOnce(&Node) -> Result<Arc<dyn AccessPolicy>, DurableNodeError>,
    {
        fs::create_dir_all(data_dir)?;
        let secret = load_or_create_secret_key(data_dir.join(SECRET_FILE))?;
        let (node, journal) = RedbJournal::open_node_with_journal(data_dir.join(JOURNAL_FILE))?;
        let initial_policy = resolve_policy(&node)?;
        let application = schema.map(|schema| ApplicationNode::new(node.clone(), schema));
        let replicator = match (bind_mode, application) {
            (BindMode::Network, Some(application)) => {
                IrohReplicator::bind_application_with_secret_and_policy(
                    application,
                    secret,
                    initial_policy,
                )
                .await
            }
            (BindMode::Loopback, Some(application)) => {
                IrohReplicator::bind_loopback_application_with_secret_and_policy(
                    application,
                    secret,
                    initial_policy,
                )
                .await
            }
            (BindMode::Network, None) => {
                IrohReplicator::bind_with_secret_and_policy(node.clone(), secret, initial_policy)
                    .await
            }
            (BindMode::Loopback, None) => {
                IrohReplicator::bind_loopback_with_secret_and_policy(
                    node.clone(),
                    secret,
                    initial_policy,
                )
                .await
            }
        }?;
        let configured = load_peers(&data_dir.join(PEERS_FILE))?;
        if configured.contains_key(&replicator.address().id) {
            return Err(DurableNodeError::Configuration(
                "peer configuration contains this node's own Iroh identity".to_owned(),
            ));
        }
        let supervisor = PeerSupervisor::new(replicator.clone());
        for peer in configured.values() {
            if let Some(source_node) = peer.source_node {
                supervisor
                    .upsert_persisted_source(
                        peer.endpoint.clone(),
                        source_node,
                        journal.clone(),
                        retry_interval,
                    )
                    .await?;
            } else {
                supervisor
                    .upsert_persisted(peer.endpoint.clone(), journal.clone(), retry_interval)
                    .await?;
            }
        }
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            node,
            journal,
            replicator,
            supervisor,
            peers: Mutex::new(configured),
            peer_updates: AsyncMutex::new(()),
            retry_interval,
        })
    }

    /// Returns the transport-neutral event-sourced node.
    #[must_use]
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Materializes a gap-free typed query into a first-class Hyphae cell.
    ///
    /// The Redb-backed node remains authoritative storage. A runtime task
    /// advances the cell only after Myko applies each complete matching batch,
    /// so reports, views, and UI bridges never need to poll the journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial snapshot/follow boundary cannot be
    /// established or no Tokio runtime is active to own the live driver.
    pub fn watch_items_reactive_in<Q>(
        &self,
        source_node: NodeId,
        scope_id: ScopeId,
        query: Q,
    ) -> Result<ReactiveItemSubscription<Q::Output>, DurableNodeError>
    where
        Q: ItemQuery + Send + 'static,
        Q::Output: hyphae::CellValue,
    {
        let (snapshot, mut watch) = self.node.watch_items_in(source_node, scope_id, query)?;
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(snapshot.value),
            through: snapshot.through,
            liveness: SubscriptionLiveness::Current,
        });
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            DurableNodeError::Configuration(format!(
                "reactive subscription requires an active Tokio runtime: {error}"
            ))
        })?;
        let task_writer = writer.clone();
        let task = runtime.spawn(async move {
            loop {
                match watch.recv_async().await {
                    Ok(update) => task_writer.publish(update.value, Some(update.position)),
                    Err(error) => {
                        task_writer.invalidate(error.to_string());
                        break;
                    }
                }
            }
        });
        Ok(ReactiveItemSubscription { live, writer, task })
    }

    /// Returns the durable journal and replication-checkpoint store.
    #[must_use]
    pub fn journal(&self) -> Arc<RedbJournal> {
        self.journal.clone()
    }

    /// Returns the running native replication endpoint.
    #[must_use]
    pub const fn replicator(&self) -> &IrohReplicator {
        &self.replicator
    }

    /// Returns this node's authenticated Iroh address.
    #[must_use]
    pub fn address(&self) -> EndpointAddr {
        self.replicator.address()
    }

    /// Returns this endpoint paired with its stable Myko history identity.
    #[must_use]
    pub fn descriptor(&self) -> NativeNodeDescriptor {
        self.replicator.descriptor()
    }

    /// Issues an expiring one-use invitation for this durable node.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid TTL, entropy failure, poisoned pairing
    /// state, or a full bounded invitation registry.
    pub fn issue_pairing_invitation(
        &self,
        ttl: Duration,
    ) -> Result<PairingInvitation, DurableNodeError> {
        Ok(self.replicator.issue_pairing_invitation(ttl)?)
    }

    /// Redeems another node's invitation without implicitly trusting it.
    ///
    /// Call [`Self::confirm_pairing`] only after the operator compares the
    /// six-digit code shown by both endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid/replayed/expired invitations, identity
    /// mismatch, timeout, bounds, or transport failure.
    pub async fn redeem_pairing(
        &self,
        invitation: &PairingInvitation,
    ) -> Result<PairingReceipt, DurableNodeError> {
        Ok(self.replicator.redeem_pairing(invitation).await?)
    }

    /// Drains successfully authenticated inbound pairing receipts.
    ///
    /// # Errors
    ///
    /// Returns an error if shared pairing state is poisoned.
    pub fn take_pairing_receipts(&self) -> Result<Vec<PairingReceipt>, DurableNodeError> {
        Ok(self.replicator.take_pairing_receipts()?)
    }

    /// Starts a wake-up stream over the bounded inbound receipt queue.
    #[must_use]
    pub fn subscribe_pairing_receipts(&self) -> PairingReceiptSubscription {
        self.replicator.subscribe_pairing_receipts()
    }

    /// Confirms a mutually authenticated receipt and durably installs its
    /// opposite descriptor as a pinned peer follower.
    ///
    /// This is infrastructure trust only; application authorization remains a
    /// separate policy decision.
    ///
    /// # Errors
    ///
    /// Returns an error if the receipt/code is invalid, this node is not one of
    /// its endpoints, or durable peer installation fails.
    pub async fn confirm_pairing(
        &self,
        receipt: &PairingReceipt,
        comparison_code: &str,
    ) -> Result<bool, DurableNodeError> {
        receipt
            .validate()
            .map_err(DurableNodeError::Configuration)?;
        if comparison_code != receipt.comparison_code {
            return Err(DurableNodeError::Configuration(
                "pairing comparison code does not match".to_owned(),
            ));
        }
        let local = self.descriptor();
        let peer = if same_descriptor_identity(&local, &receipt.server) {
            receipt.client.clone()
        } else if same_descriptor_identity(&local, &receipt.client) {
            receipt.server.clone()
        } else {
            return Err(DurableNodeError::Configuration(
                "pairing receipt does not name this node".to_owned(),
            ));
        };
        self.upsert_peer_descriptor(peer).await
    }

    /// Returns the operational identity directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Replaces authorization for subsequently accepted native requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's shared policy state is poisoned.
    pub fn set_access_policy(&self, policy: Arc<dyn AccessPolicy>) -> Result<(), DurableNodeError> {
        self.replicator.set_access_policy(policy)?;
        Ok(())
    }

    /// Returns configured peers in stable endpoint-identity order.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration state is poisoned.
    pub fn configured_peers(&self) -> Result<Vec<EndpointAddr>, DurableNodeError> {
        self.peers
            .lock()
            .map(|peers| peers.values().map(|peer| peer.endpoint.clone()).collect())
            .map_err(|_| DurableNodeError::State("peer configuration lock is poisoned".to_owned()))
    }

    /// Returns durable peer bindings in stable endpoint-identity order.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration state is poisoned.
    pub fn configured_peer_bindings(&self) -> Result<Vec<ConfiguredPeer>, DurableNodeError> {
        self.peers
            .lock()
            .map(|peers| peers.values().cloned().collect())
            .map_err(|_| DurableNodeError::State("peer configuration lock is poisoned".to_owned()))
    }

    /// Returns live snapshots for all configured peer followers.
    ///
    /// # Errors
    ///
    /// Returns an error if supervisor state is poisoned.
    pub fn peer_statuses(&self) -> Result<Vec<PeerSyncStatus>, DurableNodeError> {
        Ok(self.supervisor.statuses()?)
    }

    /// Durably adds or updates a peer and starts its source-aware follower.
    ///
    /// Configuration is committed before the follower is installed. A process
    /// failure therefore cannot leave a running-only peer that disappears on
    /// restart. Returns `true` when an existing address was replaced.
    ///
    /// # Errors
    ///
    /// Returns an error for self-following, failed durable configuration, or a
    /// follower supervisor failure.
    pub async fn upsert_peer(&self, peer: EndpointAddr) -> Result<bool, DurableNodeError> {
        self.upsert_configured_peer(ConfiguredPeer::unpinned(peer))
            .await
    }

    /// Durably pairs with a peer and pins its expected Myko source identity.
    ///
    /// The follower authenticates the descriptor's Iroh endpoint and verifies
    /// the source identity in the replication handshake before ingesting any
    /// history. This is the preferred path for pairing and discovery systems.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported descriptor, self-following, failed
    /// durable configuration, or follower-supervisor failure.
    pub async fn upsert_peer_descriptor(
        &self,
        descriptor: NativeNodeDescriptor,
    ) -> Result<bool, DurableNodeError> {
        descriptor
            .validate()
            .map_err(DurableNodeError::Configuration)?;
        if descriptor.node_id == self.node.node_id() {
            return Err(DurableNodeError::Configuration(
                "a node cannot follow its own Myko history".to_owned(),
            ));
        }
        self.upsert_configured_peer(ConfiguredPeer::pinned(descriptor))
            .await
    }

    /// Durably installs a decoded descriptor or legacy endpoint reference.
    ///
    /// # Errors
    ///
    /// Returns the same configuration and follower errors as the corresponding
    /// pinned or unpinned upsert path.
    pub async fn upsert_peer_reference(
        &self,
        reference: NativePeerReference,
    ) -> Result<bool, DurableNodeError> {
        match reference {
            NativePeerReference::Descriptor(descriptor) => {
                self.upsert_peer_descriptor(descriptor).await
            }
            NativePeerReference::LegacyEndpoint(endpoint) => self.upsert_peer(endpoint).await,
        }
    }

    async fn upsert_configured_peer(&self, peer: ConfiguredPeer) -> Result<bool, DurableNodeError> {
        let _update = self.peer_updates.lock().await;
        if peer.endpoint.id == self.address().id {
            return Err(DurableNodeError::Configuration(
                "a node cannot follow its own Iroh identity".to_owned(),
            ));
        }
        let replaced = {
            let mut peers = self.peers.lock().map_err(|_| {
                DurableNodeError::State("peer configuration lock is poisoned".to_owned())
            })?;
            let previous = peers.insert(peer.endpoint.id, peer.clone());
            if let Err(error) = save_peers(&self.data_dir.join(PEERS_FILE), &peers) {
                if let Some(previous) = previous {
                    peers.insert(peer.endpoint.id, previous);
                } else {
                    peers.remove(&peer.endpoint.id);
                }
                drop(peers);
                return Err(error);
            }
            let replaced = previous.is_some();
            drop(peers);
            replaced
        };
        if let Some(source_node) = peer.source_node {
            self.supervisor
                .upsert_persisted_source(
                    peer.endpoint,
                    source_node,
                    self.journal.clone(),
                    self.retry_interval,
                )
                .await?;
        } else {
            self.supervisor
                .upsert_persisted(peer.endpoint, self.journal.clone(), self.retry_interval)
                .await?;
        }
        Ok(replaced)
    }

    /// Durably removes a peer and stops only its follower.
    ///
    /// Returns `false` when the peer was not configured.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration cannot be committed or the follower
    /// cannot be stopped cleanly.
    pub async fn remove_peer(&self, peer_id: EndpointId) -> Result<bool, DurableNodeError> {
        let _update = self.peer_updates.lock().await;
        let removed = {
            let mut peers = self.peers.lock().map_err(|_| {
                DurableNodeError::State("peer configuration lock is poisoned".to_owned())
            })?;
            let Some(removed_peer) = peers.remove(&peer_id) else {
                return Ok(false);
            };
            if let Err(error) = save_peers(&self.data_dir.join(PEERS_FILE), &peers) {
                peers.insert(peer_id, removed_peer);
                drop(peers);
                return Err(error);
            }
            drop(peers);
            true
        };
        if removed {
            self.supervisor.remove(peer_id).await?;
        }
        Ok(removed)
    }

    /// Stops every peer follower and then the shared Iroh endpoint.
    ///
    /// Durable configuration remains intact for the next open.
    ///
    /// # Errors
    ///
    /// Returns an error if a follower or endpoint cannot shut down cleanly.
    pub async fn shutdown(self) -> Result<(), DurableNodeError> {
        self.supervisor.shutdown().await?;
        self.replicator.shutdown().await?;
        Ok(())
    }
}

fn same_descriptor_identity(left: &NativeNodeDescriptor, right: &NativeNodeDescriptor) -> bool {
    left.node_id == right.node_id && left.endpoint.id == right.endpoint.id
}

fn load_peers(path: &Path) -> Result<BTreeMap<EndpointId, ConfiguredPeer>, DurableNodeError> {
    let encoded = match fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    };
    let header = serde_json::from_slice::<StoredConfigHeader>(&encoded)?;
    let decoded_peers = match header.version {
        CONFIG_VERSION => serde_json::from_slice::<StoredPeerConfig>(&encoded)?.peers,
        LEGACY_CONFIG_VERSION => serde_json::from_slice::<LegacyStoredPeerConfig>(&encoded)?
            .peers
            .into_iter()
            .map(ConfiguredPeer::unpinned)
            .collect(),
        version => {
            return Err(DurableNodeError::Configuration(format!(
                "unsupported peer configuration version {version}"
            )));
        }
    };
    let mut peers = BTreeMap::new();
    for peer in decoded_peers {
        if peers.insert(peer.endpoint.id, peer).is_some() {
            return Err(DurableNodeError::Configuration(
                "peer configuration contains a duplicate Iroh identity".to_owned(),
            ));
        }
    }
    Ok(peers)
}

fn save_peers(
    path: &Path,
    peers: &BTreeMap<EndpointId, ConfiguredPeer>,
) -> Result<(), DurableNodeError> {
    let stored = StoredPeerConfig {
        version: CONFIG_VERSION,
        peers: peers.values().cloned().collect(),
    };
    let encoded = serde_json::to_vec_pretty(&stored)?;
    let parent = path.parent().ok_or_else(|| {
        DurableNodeError::Configuration(format!(
            "peer configuration has no parent: {}",
            path.display()
        ))
    })?;
    let temporary = parent.join(format!(".peers-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
