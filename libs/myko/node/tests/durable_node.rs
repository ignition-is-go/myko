use std::{sync::Arc, time::Duration};

use myko_app::{
    CommandClient as _, CommandContext, CommandError, CommandHandler, MykoApplication,
    QueryHandler, myko_query,
};
use myko_federation::{
    AccessPolicy, AccessRequest, AllowAllAccessPolicy, BatchId, ChangeBatch, CommandClient as _,
    CommandId, CommandRequest, CommandState, MykoService, Node as FederationNode, NodeId,
    PrincipalId, ScopeId, ServiceId, SubscriptionLiveness,
};
use myko_iroh::IrohReplicator;
use myko_items::{ItemMutation, ItemProjection, ItemQuery, myko_command, myko_item, myko_service};
use myko_local::{LocalCommandClient, LocalNodeClient, LocalNodeServer};
use myko_node::{
    AddPeer, ConfigureLanDiscovery, ConfirmPairing, DiscoverySettingsReport,
    IssuePairingInvitation, NativeNodeDescriptor, NativePeerReference, Node, NodeStatus,
    NodeStatusView, PairingInvitation, PairingReceipt, PairingReceiptsView, PairingRedemptionPhase,
    PairingRedemptionReport, Peer, PeerReport, PeersView, RedeemPairingInvitation, RemovePeer,
    SetPeerFollowing, peer_id,
};

fn watch_peers(node: &Node) -> Result<myko_app::ViewSubscription<Peer>, String> {
    node.application()
        .watch_view(&PeersView {
            source_node: node.node().node_id(),
        })
        .map_err(|error| error.to_string())
}

fn watch_node_statuses(node: &Node) -> Result<myko_app::ViewSubscription<NodeStatus, u64>, String> {
    node.application()
        .watch_view(&NodeStatusView)
        .map_err(|error| error.to_string())
}

async fn add_pinned_peer(node: &Node, descriptor: NativeNodeDescriptor) -> Result<Peer, String> {
    myko_app::CommandClient::exec_command(
        node.application(),
        AddPeer {
            reference: NativePeerReference::Descriptor(descriptor),
        },
    )
    .await
    .map_err(|error| error.to_string())
}

async fn remove_peer(node: &Node, endpoint_id: myko_iroh::EndpointId) -> Result<(), String> {
    myko_app::CommandClient::exec_command(
        node.application(),
        RemovePeer {
            peer_id: peer_id(endpoint_id),
        },
    )
    .await
    .map_err(|error| error.to_string())
}

#[myko_service(ReactiveRecord)]
pub struct ReactiveService;

#[myko_item(service = ReactiveService, scope_root)]
pub struct ReactiveRecord {
    value: String,
}

#[myko_command((), item = ReactiveRecord)]
struct RemoteLifecycleCommand;

impl CommandHandler for RemoteLifecycleCommand {
    fn scope(&self, _node_id: NodeId) -> ReactiveRecordId {
        ReactiveRecordId::from("remote-command")
    }

    fn execute(
        self,
        _context: CommandContext<ReactiveService, ReactiveRecord>,
    ) -> Result<(), CommandError> {
        Ok(())
    }
}

#[myko_query(ReactiveRecord)]
struct AllReactiveRecords;

impl ItemQuery for AllReactiveRecords {
    type Item = ReactiveRecord;
    type Output = Vec<ReactiveRecord>;
    fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
        projection.values().cloned().collect()
    }
}

impl QueryHandler for AllReactiveRecords {}

#[derive(Debug)]
struct DenyAllPolicy;

impl AccessPolicy for DenyAllPolicy {
    fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
        Err("test policy denies native access".to_owned())
    }
}

fn commit_test_command(node: &FederationNode, command_type: &str) -> Result<CommandId, String> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("myko.node.test"),
        scope_id: ScopeId::new("restart"),
        principal_id: PrincipalId::new("node:test"),
        command_type: command_type.to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        Vec::new(),
    )
    .map_err(|error| error.to_string())?;
    Ok(request.id)
}

async fn wait_for_committed(node: &FederationNode, command_id: CommandId) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if node
                .command(command_id)
                .map_err(|error| error.to_string())?
                .is_some_and(|command| command.state.is_committed())
            {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| format!("command {command_id} was not replicated"))?
}

async fn wait_for(description: &str, mut condition: impl FnMut() -> bool) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if condition() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| description.to_owned())
}

#[tokio::test]
async fn typed_item_watch_drives_a_hyphae_cell_without_polling() -> Result<(), String> {
    use hyphae::{Signal, Watchable as _};

    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let node = Node::open_loopback(directory.path(), Duration::from_millis(20))
        .await
        .map_err(|error| error.to_string())?;
    let scope_id = ScopeId::new("reactive");
    let watch = node
        .watch_items_reactive_in(node.node().node_id(), scope_id.clone(), AllReactiveRecords)
        .map_err(|error| error.to_string())?;
    let (updates_tx, updates_rx) = flume::unbounded();
    let _guard = watch.live().state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _ignored = updates_tx.send(state.clone());
        }
    });
    let _initial = updates_rx.try_recv();

    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(ReactiveService::SERVICE_ID),
        scope_id: scope_id.clone(),
        principal_id: PrincipalId::new("node:test"),
        command_type: "reactive.insert".to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .node()
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let record = ReactiveRecord {
        id: ReactiveRecordId::from("record-1"),
        value: "visible".to_owned(),
    };
    node.node()
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![ItemMutation::set(&record).map_err(|error| error.to_string())?],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;

    let update = tokio::time::timeout(Duration::from_secs(2), updates_rx.recv_async())
        .await
        .map_err(|_| "reactive query cell did not publish the committed item".to_owned())?
        .map_err(|error| error.to_string())?;
    if update.value != Some(vec![record])
        || update.through.is_none()
        || update.liveness != SubscriptionLiveness::Current
    {
        return Err(format!("unexpected reactive item state: {update:?}"));
    }

    drop(watch);
    node.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn restored_policy_is_installed_before_the_router_serves() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let initial = Node::open_loopback(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let _command = commit_test_command(initial.node(), "policy-window")?;
    initial
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;

    let reopened = Node::open_loopback_with_policy(directory.path(), retry_interval, |_| {
        Ok(Arc::new(DenyAllPolicy))
    })
    .await
    .map_err(|error| error.to_string())?;
    let client = IrohReplicator::bind_loopback(FederationNode::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    client
        .verify_descriptor(&reopened.descriptor())
        .await
        .map_err(|error| error.to_string())?;
    if client.pull(reopened.address(), None).await.is_ok() {
        return Err("restored node served history before installing its policy".to_owned());
    }
    client.shutdown().await.map_err(|error| error.to_string())?;
    reopened.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn durable_node_routes_generic_remote_command_lifecycles() -> Result<(), String> {
    let local_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let remote_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let local = Node::open_loopback(local_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remote_application = MykoApplication::builder()
        .service::<ReactiveService>()
        .map_err(|error| error.to_string())?
        .build();
    let remote = Node::open_loopback_application_with_policy(
        remote_directory.path(),
        retry_interval,
        remote_application,
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await
    .map_err(|error| error.to_string())?;
    let _peer = add_pinned_peer(&local, remote.descriptor()).await?;
    let socket = local_directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_sessions(
        &socket,
        local.replicator().sessions().clone(),
        PrincipalId::new("owner:local"),
    )
    .await
    .map_err(|error| error.to_string())?;
    let commands = LocalCommandClient::new(&socket).at(remote.node().node_id());
    let submitted = commands
        .submit_command(RemoteLifecycleCommand)
        .await
        .map_err(|error| error.to_string())?;
    if submitted.source_node != remote.node().node_id()
        || !matches!(
            submitted.command.as_ref().map(|command| &command.state),
            Some(CommandState::Submitted)
        )
    {
        return Err(format!(
            "remote submit returned unexpected response: {submitted:?}"
        ));
    }
    let command_id = submitted
        .command
        .as_ref()
        .map(|command| command.request.id)
        .ok_or_else(|| "remote submit omitted its command snapshot".to_owned())?;
    let observed = commands
        .command_state(command_id)
        .await
        .map_err(|error| error.to_string())?;
    if observed
        .command
        .as_ref()
        .is_none_or(|command| command.request.id != command_id)
    {
        return Err(format!(
            "remote command lookup returned unexpected response: {observed:?}"
        ));
    }
    let cancelled = commands
        .cancel_command(command_id, "test complete".to_owned())
        .await
        .map_err(|error| error.to_string())?;
    if cancelled
        .command
        .as_ref()
        .is_none_or(|command| !command.state.is_committed())
    {
        return Err(format!(
            "terminal remote command changed after cancellation: {cancelled:?}"
        ));
    }

    server.shutdown().await.map_err(|error| error.to_string())?;
    local.shutdown().await.map_err(|error| error.to_string())?;
    remote.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn local_node_routes_remote_live_events_without_an_application_protocol() -> Result<(), String>
{
    let local_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let remote_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let local = Node::open_loopback(local_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remote = Node::open_loopback(remote_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let _peer = add_pinned_peer(&local, remote.descriptor()).await?;

    let socket = local_directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_sessions(
        &socket,
        local.replicator().sessions().clone(),
        PrincipalId::new("owner:local"),
    )
    .await
    .map_err(|error| error.to_string())?;
    let target_node = remote.node().node_id();
    let client = LocalNodeClient::new(&socket).at(target_node);
    let mut events = client
        .follow_live(vec!["agent:test".to_owned()])
        .await
        .map_err(|error| error.to_string())?;
    let payload = b"streamed remotely".to_vec();
    let _report = remote
        .replicator()
        .publish_live("agent:test", payload.clone())
        .map_err(|error| error.to_string())?;
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .map_err(|_| "routed live event did not arrive".to_owned())?
        .map_err(|error| error.to_string())?;
    if events.source_node() != target_node
        || event.source_node != target_node
        || event.topic != "agent:test"
        || event.payload != payload
    {
        return Err(format!("unexpected routed live event: {event:?}"));
    }

    server.shutdown().await.map_err(|error| error.to_string())?;
    local.shutdown().await.map_err(|error| error.to_string())?;
    remote.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn typed_peer_commands_drive_live_view_and_report() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = Node::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| format!("open pairing source: {error}"))?;
    let target = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| format!("open pairing target: {error}"))?;
    let application = target.application();
    let target_node = target.node().node_id();
    let descriptor = source.descriptor();
    let configured_peer_id = peer_id(descriptor.endpoint.id);
    let view = application
        .watch_view(&PeersView {
            source_node: target_node,
        })
        .map_err(|error| error.to_string())?;
    let report = application
        .watch_report(&PeerReport {
            source_node: target_node,
            peer_id: configured_peer_id.clone(),
        })
        .map_err(|error| error.to_string())?;

    let added = myko_app::CommandClient::exec_command(
        application,
        AddPeer {
            reference: NativePeerReference::Descriptor(descriptor),
        },
    )
    .await
    .map_err(|error| format!("issue pairing invitation: {error}"))?;
    if added.id != configured_peer_id || !added.following {
        return Err(format!("typed add returned an unexpected peer: {added:?}"));
    }
    wait_for(
        "typed peer live surfaces did not observe the added peer",
        || {
            view.live()
                .rows()
                .snapshot()
                .iter()
                .any(|(_, peer)| peer.id == configured_peer_id && peer.following)
                && report
                    .live()
                    .current()
                    .value
                    .as_ref()
                    .and_then(Option::as_ref)
                    .is_some_and(|peer| peer.id == configured_peer_id && peer.following)
        },
    )
    .await?;

    let paused = myko_app::CommandClient::exec_command(
        application,
        SetPeerFollowing {
            peer_id: configured_peer_id.clone(),
            following: false,
        },
    )
    .await
    .map_err(|error| format!("admit pairing redemption: {error}"))?;
    if paused.following {
        return Err("typed following command did not pause the peer".to_owned());
    }
    wait_for(
        "peer report did not observe the paused relationship",
        || {
            report
                .live()
                .current()
                .value
                .as_ref()
                .and_then(Option::as_ref)
                .is_some_and(|peer| !peer.following)
        },
    )
    .await?;

    myko_app::CommandClient::exec_command(
        application,
        RemovePeer {
            peer_id: configured_peer_id,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    wait_for("typed peer live surfaces did not observe removal", || {
        view.live().rows().snapshot().is_empty()
            && report
                .live()
                .current()
                .value
                .as_ref()
                .is_some_and(Option::is_none)
    })
    .await?;

    view.shutdown().await;
    report.shutdown().await;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn discovery_configuration_is_a_durable_live_framework_report() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let node = Node::open_loopback(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let report = node
        .application()
        .watch_report(&DiscoverySettingsReport {
            source_node: node.node().node_id(),
        })
        .map_err(|error| error.to_string())?;
    let configured = myko_app::CommandClient::exec_command(
        node.application(),
        ConfigureLanDiscovery {
            display_name: "phone-sized-fern".to_owned(),
            enabled: false,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if configured.display_name != "phone-sized-fern" || configured.enabled {
        return Err(format!(
            "LAN discovery command returned unexpected settings: {configured:?}"
        ));
    }
    wait_for("LAN discovery report did not observe configuration", || {
        report
            .live()
            .current()
            .value
            .as_ref()
            .and_then(Option::as_ref)
            .is_some_and(|settings| settings == &configured)
    })
    .await?;
    report.shutdown().await;
    let node_id = node.node().node_id();
    node.shutdown().await.map_err(|error| error.to_string())?;

    let reopened = Node::open_loopback(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let restored = reopened
        .application()
        .watch_report(&DiscoverySettingsReport {
            source_node: node_id,
        })
        .map_err(|error| error.to_string())?;
    if restored
        .live()
        .current()
        .value
        .as_ref()
        .and_then(Option::as_ref)
        != Some(&configured)
    {
        return Err("LAN discovery settings did not survive restart".to_owned());
    }
    restored.shutdown().await;
    reopened.shutdown().await.map_err(|error| error.to_string())
}

async fn complete_pairing_redemption(
    target: &Node,
    invitation: PairingInvitation,
) -> Result<PairingReceipt, String> {
    let redemption = myko_app::CommandClient::exec_command(
        target.application(),
        RedeemPairingInvitation { invitation },
    )
    .await
    .map_err(|error| error.to_string())?;
    let redemption_report = target
        .application()
        .watch_report(&PairingRedemptionReport {
            source_node: target.node().node_id(),
            redemption_id: redemption.id,
        })
        .map_err(|error| error.to_string())?;
    wait_for("pairing redemption report did not become terminal", || {
        redemption_report
            .live()
            .current()
            .value
            .as_ref()
            .and_then(Option::as_ref)
            .is_some_and(|redemption| redemption.phase.is_terminal())
    })
    .await?;
    let result = match redemption_report
        .live()
        .current()
        .value
        .and_then(|redemption| redemption)
        .map(|redemption| redemption.phase)
    {
        Some(PairingRedemptionPhase::Completed { receipt }) => Ok(receipt),
        Some(PairingRedemptionPhase::Failed { reason }) => Err(reason),
        phase => Err(format!("unexpected pairing redemption phase: {phase:?}")),
    };
    redemption_report.shutdown().await;
    result
}

async fn receive_pairing_receipt(
    inbound: &myko_app::ViewSubscription<PairingReceipt>,
    expected: &PairingReceipt,
) -> Result<PairingReceipt, String> {
    wait_for("source did not observe the pairing receipt view", || {
        !inbound.live().rows().snapshot().is_empty()
    })
    .await?;
    let receipts = inbound
        .live()
        .rows()
        .snapshot()
        .into_iter()
        .map(|(_, receipt)| receipt.as_ref().clone())
        .collect::<Vec<_>>();
    let [received] = receipts.as_slice() else {
        return Err(format!("source observed wrong receipts: {receipts:?}"));
    };
    if received != expected {
        return Err("pairing endpoints derived different receipts".to_owned());
    }
    Ok(received.clone())
}

async fn confirm_pairing(node: &Node, receipt: PairingReceipt) -> Result<Peer, String> {
    myko_app::CommandClient::exec_command(
        node.application(),
        ConfirmPairing {
            comparison_code: receipt.comparison_code.clone(),
            receipt,
        },
    )
    .await
    .map_err(|error| error.to_string())
}

fn require_peer_state(
    peers: &myko_app::ViewSubscription<Peer>,
    source_node: NodeId,
    following: bool,
    error: &str,
) -> Result<(), String> {
    let snapshot = peers.live().rows().snapshot();
    let valid = snapshot.first().is_some_and(|(_, peer)| {
        peer.source_node == Some(source_node) && peer.following == following
    });
    if valid { Ok(()) } else { Err(error.to_owned()) }
}

#[tokio::test]
async fn confirmed_pairing_remembers_pinned_peers_before_following() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = Node::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let inbound = source
        .application()
        .watch_view(&PairingReceiptsView)
        .map_err(|error| error.to_string())?;
    let invitation = myko_app::CommandClient::exec_command(
        source.application(),
        IssuePairingInvitation { ttl_seconds: 60 },
    )
    .await
    .map_err(|error| error.to_string())?;
    let outbound = complete_pairing_redemption(&target, invitation).await?;
    let received = receive_pairing_receipt(&inbound, &outbound).await?;
    if myko_app::CommandClient::exec_command(
        target.application(),
        ConfirmPairing {
            receipt: outbound.clone(),
            comparison_code: "000000".to_owned(),
        },
    )
    .await
    .is_ok()
    {
        return Err("target accepted the wrong comparison code".to_owned());
    }
    let _target_peer = confirm_pairing(&target, outbound.clone()).await?;
    let _source_peer = confirm_pairing(&source, received).await?;
    let source_descriptor = source.descriptor();
    let target_peers = watch_peers(&target)?;
    require_peer_state(
        &target_peers,
        source_descriptor.node_id,
        false,
        "confirmed target did not remember a paused pinned source",
    )?;
    let enabled = myko_app::CommandClient::exec_command(
        target.application(),
        SetPeerFollowing {
            peer_id: peer_id(source_descriptor.endpoint.id),
            following: true,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if !enabled.following {
        return Err("remembered pairing did not enable its follower".to_owned());
    }

    target_peers.shutdown().await;
    target
        .shutdown()
        .await
        .map_err(|error| format!("shutdown pairing target: {error}"))?;
    let reopened = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| format!("reopen pairing target: {error}"))?;
    let reopened_peers = watch_peers(&reopened)?;
    require_peer_state(
        &reopened_peers,
        source_descriptor.node_id,
        true,
        "enabled pairing relationship did not survive target restart",
    )?;
    reopened_peers.shutdown().await;
    reopened
        .shutdown()
        .await
        .map_err(|error| format!("shutdown reopened pairing target: {error}"))?;
    inbound.shutdown().await;
    source
        .shutdown()
        .await
        .map_err(|error| format!("shutdown pairing source: {error}"))
}

#[tokio::test]
async fn restart_restores_identities_peers_and_durable_cursor() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = Node::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let source_descriptor = source.descriptor();
    let source_address = source_descriptor.endpoint.clone();
    let first_command = commit_test_command(source.node(), "before-restart")?;

    let target = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let first_transport_id = target.address().id;
    let first_node_id = target.node().node_id();
    let _peer = add_pinned_peer(&target, source_descriptor.clone()).await?;
    wait_for_committed(target.node(), first_command).await?;
    target.shutdown().await.map_err(|error| error.to_string())?;

    let second_command = commit_test_command(source.node(), "while-target-offline")?;
    let reopened = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    if reopened.address().id != first_transport_id || reopened.node().node_id() != first_node_id {
        return Err("durable node identity changed across restart".to_owned());
    }
    let reopened_peers = watch_peers(&reopened)?;
    let reopened_statuses = watch_node_statuses(&reopened)?;
    let peer_rows = reopened_peers.live().rows().snapshot();
    let status_rows = reopened_statuses.live().rows().snapshot();
    if !matches!(
        peer_rows
            .iter()
            .map(|(_, peer)| peer.as_ref())
            .collect::<Vec<_>>()
            .as_slice(),
        [peer]
            if peer.endpoint == source_address
                && peer.source_node == Some(source_descriptor.node_id)
    ) || status_rows
        .iter()
        .filter(|(_, status)| !status.local)
        .count()
        != 1
    {
        return Err("configured follower was not restored".to_owned());
    }
    wait_for_committed(reopened.node(), second_command).await?;

    remove_peer(&reopened, source_address.id).await?;
    reopened_peers.shutdown().await;
    reopened_statuses.shutdown().await;
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    let after_removal = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remaining_peers = watch_peers(&after_removal)?;
    let remaining_statuses = watch_node_statuses(&after_removal)?;
    if !remaining_peers.live().rows().snapshot().is_empty()
        || remaining_statuses
            .live()
            .rows()
            .snapshot()
            .iter()
            .any(|(_, status)| !status.local)
    {
        return Err("removed peer returned after restart".to_owned());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(target_directory.path().join("iroh-secret.json"))
            .map_err(|error| error.to_string())?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o600 {
            return Err(format!(
                "Iroh secret permissions are {mode:o}, expected 600"
            ));
        }
    }

    remaining_peers.shutdown().await;
    remaining_statuses.shutdown().await;
    after_removal
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn pinned_peer_rejects_an_endpoint_serving_another_myko_history() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = Node::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let command_id = commit_test_command(source.node(), "must-not-replicate")?;
    let source_address = source.address();
    let expected_source = NodeId::new();
    let target = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let status_view = watch_node_statuses(&target)?;
    let _peer = add_pinned_peer(
        &target,
        NativeNodeDescriptor::new(expected_source, source_address.clone()),
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let statuses = status_view.live().rows().snapshot();
            if statuses.iter().any(|(_, status)| {
                status.source_node == Some(expected_source)
                    && status.last_error.as_deref().is_some_and(|error| {
                        error.contains("advertised Myko source") && error.contains("expected")
                    })
            }) {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "pinned follower did not reject the unexpected source".to_owned())??;
    if target
        .node()
        .command(command_id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("pinned follower ingested history from the wrong Myko node".to_owned());
    }

    remove_peer(&target, source_address.id).await?;
    status_view.shutdown().await;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn legacy_endpoint_only_peer_files_remain_loadable() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = Node::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let source_address = source.address();
    let encoded = serde_json::to_vec_pretty(&serde_json::json!({
        "version": 1,
        "peers": [source_address.clone()],
    }))
    .map_err(|error| error.to_string())?;
    std::fs::write(target_directory.path().join("peers.json"), encoded)
        .map_err(|error| error.to_string())?;

    let target = Node::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let peers = watch_peers(&target)?;
    let bindings = peers.live().rows().snapshot();
    if !matches!(
        bindings
            .iter()
            .map(|(_, peer)| peer.as_ref())
            .collect::<Vec<_>>()
            .as_slice(),
        [binding] if binding.endpoint == source_address && binding.source_node.is_none()
    ) {
        return Err("legacy peer configuration did not load as an unpinned binding".to_owned());
    }
    peers.shutdown().await;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}
