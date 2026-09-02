//! Framework-owned LAN discovery configuration and live nearby-node view.

use std::sync::Arc;

use hyphae::{Signal, Watchable as _};
use myko_app::capability::{
    CollectionBuilding as _, EventPublishing as _, NodeScoped as _, Querying as _,
    ResourceScoped as _,
};
use myko_app::{
    AppError, ApplicationNode, CommandContext, CommandError, CommandHandler, HandlerSubscription,
    ReportContext, ReportHandler, ViewContext, ViewHandler, myko_report, myko_view,
};
use myko_discovery::{DiscoveredNode, LanAdvertisement, LanDiscovery};
use myko_federation::{
    LiveCollection, LiveSubscription, LogPosition, NodeId, ScopeId, SubscriptionLiveness,
};
use myko_iroh::NativeNodeDescriptor;
use myko_items::{myko_command, myko_item};
use tokio::sync::{mpsc, watch};

use crate::{
    FederationService,
    live_state::RuntimeFeed,
    peer::{PeerRoster, PeerRosterId, peer_scope},
};

/// Durable, node-local configuration for framework-owned LAN discovery.
#[myko_item(service = FederationService, scoped_by = PeerRoster)]
pub struct DiscoverySettings {
    pub display_name: String,
    pub enabled: bool,
}

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

    fn execute(
        self,
        context: CommandContext<FederationService, PeerRoster>,
    ) -> Result<Self::Output, CommandError> {
        let display_name = self.display_name.trim().to_owned();
        if display_name.is_empty() {
            return Err(CommandError::reject(
                "LAN discovery display name must not be empty",
            ));
        }
        let settings = DiscoverySettings {
            id: DiscoverySettingsId::from(context.node_id().to_string()),
            display_name,
            enabled: self.enabled,
        };
        context.emit_set(&settings)?;
        Ok(settings)
    }
}

/// Live LAN-discovery configuration for one authoritative node.
#[myko_report(Option<DiscoverySettings>, item = DiscoverySettings)]
#[derive(PartialEq, Eq)]
pub struct DiscoverySettingsReport {
    pub source_node: NodeId,
}

impl ReportHandler for DiscoverySettingsReport {
    type Output = Option<DiscoverySettings>;
    type Cursor = LogPosition;

    fn access_scope(&self) -> Option<ScopeId> {
        Some(peer_scope(self.source_node))
    }

    fn build(&self, context: &ReportContext) -> Result<LiveSubscription<Self::Output>, AppError> {
        let id = DiscoverySettingsId::from(self.source_node.to_string());
        Ok(context
            .query(
                self.source_node,
                peer_scope(self.source_node),
                GetDiscoverySettingsById { id },
            )?
            .map_value(Clone::clone))
    }
}

#[derive(Clone)]
pub struct DiscoveryViewState {
    nearby: RuntimeFeed<DiscoveredNode>,
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
        Self {
            nearby: RuntimeFeed::new(Vec::new()),
        }
    }

    pub fn publish(&self, nodes: Vec<DiscoveredNode>) {
        self.nearby.publish(nodes);
    }
}

/// Live untrusted LAN advertisements visible to this node.
#[myko_view(DiscoveredNode, item = PeerRoster)]
#[derive(Copy, PartialEq, Eq)]
pub struct NearbyNodesView;

impl ViewHandler for NearbyNodesView {
    type Item = DiscoveredNode;
    type Cursor = u64;

    fn item_key(item: &Self::Item) -> Arc<str> {
        Arc::from(item.endpoint_id().to_string())
    }

    fn build(&self, context: &ViewContext) -> Result<LiveCollection<Self::Item, u64>, AppError> {
        let state = context.resource::<DiscoveryViewState>()?;
        context.collection_from_subscription(&state.nearby.live, Self::item_key)
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
        application: &ApplicationNode,
        descriptor: NativeNodeDescriptor,
        state: DiscoveryViewState,
        network_enabled: bool,
    ) -> Result<Self, String> {
        let settings = application
            .watch_query(
                application.node().node_id(),
                peer_scope(application.node().node_id()),
                GetDiscoverySettingsById {
                    id: DiscoverySettingsId::from(application.node().node_id().to_string()),
                },
            )
            .map_err(|error| error.to_string())?;
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
    settings: HandlerSubscription<Option<DiscoverySettings>>,
    network_enabled: bool,
    mut stopping: watch::Receiver<bool>,
) -> Result<(), String> {
    let (change_sender, mut change_receiver) = mpsc::unbounded_channel();
    change_sender
        .send(())
        .map_err(|_| "LAN discovery settings could not publish initial state".to_owned())?;
    let updates = change_sender.clone();
    let changes_guard = settings.live().state().subscribe(move |signal| {
        if let Signal::Value(_) = signal {
            let _sent = updates.send(());
        }
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
                let settings_state = settings.live().current();
                match settings_state.liveness {
                    SubscriptionLiveness::Current => {
                        let next = settings_state.value.ok_or_else(|| {
                            "current LAN discovery settings report omitted its value".to_owned()
                        })?;
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
                    SubscriptionLiveness::Connecting
                    | SubscriptionLiveness::Resynchronizing { .. } => {}
                    SubscriptionLiveness::Invalid { reason } => {
                        return Err(format!("LAN discovery settings became invalid: {reason}"));
                    }
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
    settings.shutdown().await;
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
