#![allow(clippy::implicit_clone, clippy::redundant_clone)]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use hyphae::Gettable as _;
use myko::{CommandContext, CommandError, CommandHandler, MykoApplication};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation,
    AuthorityUnavailable, AuthorizationDecision, BatchId, ChangeBatch, CommandClient as _,
    CommandId, CommandRequest, CommandState, CommandWatchingClient as _, DelegationId, MykoService,
    Node as FederationNode, NodeId, Principal, PrincipalId, ProvenanceHop, ProvenanceOperation,
    ReplicationSelection, ScopeId, ServiceId, SubscriptionLiveness,
};
use myko_iroh::{IrohReplicator, SecretKey, endpoint_principal_id};
use myko_items::{ItemMutation, myko_command, myko_item, myko_service};
use myko_local::{LocalCommandClient, LocalNodeClient, LocalNodeServer};
use myko_node::{
    AddPeer, AdvertisedServicesView, ConfigureLanDiscovery, ConfirmPairing,
    DiscoverySettingsReport, FederationService, InitiatePairing, IssuePairingInvitation,
    NativeNodeDescriptor, Node, NodeStatus, NodeStatusView, PairingInitiationPhase,
    PairingInitiationReport, PairingInvitation, PairingReceipt, PairingReceiptRow,
    PairingReceiptsView, PairingRedemptionPhase, PairingRedemptionReport, Peer, PeerReport,
    PeersView, RedeemPairingInvitation, RemovePeer, ServiceCapabilityReport, SetPeerReplication,
    SetPeerReplicationSelection, peer_id,
};

async fn open_loopback_allow(
    data_dir: impl AsRef<std::path::Path>,
    retry_interval: Duration,
) -> Result<Node, myko_node::NodeError> {
    Node::open_loopback_with_policy(data_dir, retry_interval, |_| {
        Ok(Arc::new(AllowAllAccessPolicy))
    })
    .await
}

fn watch_peers(node: &Node) -> Result<myko::view::TypedViewCellMap<Peer>, String> {
    node.application()
        .watch_view(&PeersView {
            source_node: node.node().node_id(),
        })
        .map_err(|error| error.to_string())
}

fn watch_node_statuses(node: &Node) -> Result<myko::view::TypedViewCellMap<NodeStatus>, String> {
    node.application()
        .watch_view(&NodeStatusView {
            source_node: node.node().node_id(),
        })
        .map_err(|error| error.to_string())
}

async fn add_pinned_peer(node: &Node, descriptor: NativeNodeDescriptor) -> Result<Peer, String> {
    myko_federation::CommandWatchingClient::exec_typed_command(
        node.application(),
        AddPeer {
            reference: descriptor.into(),
        },
    )
    .await
    .map_err(|error| error.to_string())
}

async fn remove_peer(node: &Node, endpoint_id: myko_iroh::EndpointId) -> Result<(), String> {
    myko_federation::CommandWatchingClient::exec_typed_command(
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

myko::register_federated_item!(ReactiveRecord);

#[myko_command(NodeId, item = ReactiveRecord)]
struct RemoteLifecycleCommand;

impl CommandHandler for RemoteLifecycleCommand {
    fn scope(&self, _node_id: NodeId) -> ReactiveRecordId {
        ReactiveRecordId::from("remote-command")
    }

    fn execute(self, context: CommandContext) -> Result<NodeId, CommandError> {
        Ok(context.node_id())
    }
}

#[derive(Debug)]
struct DenyAllPolicy;

impl AccessPolicy for DenyAllPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        Ok(AuthorizationDecision::from_rule(
            request,
            Err("test policy denies native access".to_owned()),
        ))
        .into()
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingAllowPolicy {
    requests: Arc<Mutex<Vec<AccessAttempt>>>,
}

impl AccessPolicy for RecordingAllowPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let Ok(mut requests) = self.requests.lock() else {
            return Err(AuthorityUnavailable::PolicyUnavailable).into();
        };
        requests.push(request.clone());
        AllowAllAccessPolicy.decide(request)
    }
}

fn routed_command_client(
    socket: &std::path::Path,
    destination: NodeId,
    authenticated_local_principal: Principal,
    forwarding_node: Principal,
    forwarding_node_id: NodeId,
) -> LocalCommandClient {
    let authority = AuthorityPresentation::direct(authenticated_local_principal.clone());
    LocalCommandClient::new(socket)
        .at(destination)
        .with_authority(authority)
        .with_forwarding_hop(ProvenanceHop {
            delegation_id: DelegationId::random(),
            delegator: authenticated_local_principal,
            delegate: forwarding_node,
            operation: ProvenanceOperation::NodeForward {
                node_id: forwarding_node_id.to_string(),
            },
        })
}

fn commit_test_command(node: &FederationNode, command_type: &str) -> Result<CommandId, String> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("myko.node.test"),
        scope_id: ScopeId::new("restart"),
        principal_id: PrincipalId::new("node:test"),
        authority: myko_federation::AuthorityPresentation::direct_node(PrincipalId::new(
            "node:test",
        )),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
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
async fn identity_backed_application_honors_the_startup_barrier() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let identity = SecretKey::generate();
    let endpoint_id = identity.public();
    let application = MykoApplication::builder().build();
    let (node, startup) = Node::open_application_starting_with_identity_and_policy(
        directory.path(),
        Duration::from_millis(20),
        identity,
        application,
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await
    .map_err(|error| error.to_string())?;
    if node.descriptor().endpoint.id != endpoint_id {
        return Err(
            "identity-backed node did not retain the supplied endpoint identity".to_owned(),
        );
    }
    if node.node().is_ready() {
        return Err("identity-backed application escaped its startup barrier".to_owned());
    }
    startup.ready();
    if !node.node().is_ready() {
        return Err("identity-backed application did not become ready".to_owned());
    }
    node.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn typed_item_watch_drives_a_hyphae_cell_without_polling() -> Result<(), String> {
    use hyphae::{Signal, Watchable as _};

    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let node = open_loopback_allow(directory.path(), Duration::from_millis(20))
        .await
        .map_err(|error| error.to_string())?;
    let scope_id = ScopeId::new("reactive");
    let watch = node
        .watch_items_reactive_in(
            node.node().node_id(),
            scope_id.clone(),
            GetAllReactiveRecords,
        )
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
        authority: myko_federation::AuthorityPresentation::direct_node(PrincipalId::new(
            "node:test",
        )),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
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
async fn caller_owned_identity_remains_outside_node_storage() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let identity = SecretKey::generate();
    let endpoint_id = identity.public();
    let open = |identity| {
        Node::open_loopback_application_with_identity_and_policy(
            directory.path(),
            Duration::from_millis(20),
            identity,
            MykoApplication::new(),
            |_| Ok(Arc::new(AllowAllAccessPolicy)),
        )
    };

    let node = open(identity.clone())
        .await
        .map_err(|error| error.to_string())?;
    if node.address().id != endpoint_id {
        return Err("node ignored the caller-owned native identity".to_owned());
    }
    node.shutdown().await.map_err(|error| error.to_string())?;
    if directory.path().join("iroh-secret.json").exists() {
        return Err("caller-owned native identity leaked into node storage".to_owned());
    }

    let reopened = open(identity).await.map_err(|error| error.to_string())?;
    if reopened.address().id != endpoint_id {
        return Err("caller-owned native identity changed across restart".to_owned());
    }
    reopened.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn restored_policy_is_installed_before_the_router_serves() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let initial = open_loopback_allow(directory.path(), retry_interval)
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
#[allow(clippy::too_many_lines)]
async fn durable_node_routes_generic_remote_command_lifecycles() -> Result<(), String> {
    let local_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let remote_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let local = Node::open_loopback_with_policy(local_directory.path(), retry_interval, |_| {
        Ok(Arc::new(myko_federation::AllowAllAccessPolicy))
    })
    .await
    .map_err(|error| format!("open local: {error}"))?;
    let remote_application = MykoApplication::builder()
        .service::<ReactiveService>()
        .build();
    let remote_policy = RecordingAllowPolicy::default();
    let remote = Node::open_loopback_application_with_policy(
        remote_directory.path(),
        retry_interval,
        remote_application,
        |_| Ok(Arc::new(remote_policy.clone())),
    )
    .await
    .map_err(|error| format!("open remote: {error}"))?;
    let _peer = add_pinned_peer(&local, remote.descriptor())
        .await
        .map_err(|error| format!("add peer: {error}"))?;
    let socket = local_directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_sessions(
        &socket,
        local.replicator().sessions().clone(),
        PrincipalId::new("owner:local"),
    )
    .await
    .map_err(|error| format!("spawn local server: {error}"))?;
    let owner = Principal::node(PrincipalId::new("owner:local"));
    let forwarding_node = Principal::node(endpoint_principal_id(local.address().id));
    let forwarding_hop = ProvenanceHop {
        delegation_id: DelegationId::random(),
        delegator: owner.clone(),
        delegate: forwarding_node.clone(),
        operation: ProvenanceOperation::NodeForward {
            node_id: local.node().node_id().to_string(),
        },
    };

    if LocalCommandClient::new(&socket)
        .at(remote.node().node_id())
        .submit_typed_command(RemoteLifecycleCommand)
        .await
        .is_ok()
    {
        return Err("remote route accepted a missing node-forward delegation".to_owned());
    }
    if routed_command_client(
        &socket,
        remote.node().node_id(),
        Principal::node(PrincipalId::new("node:forged")),
        forwarding_node.clone(),
        local.node().node_id(),
    )
    .submit_typed_command(RemoteLifecycleCommand)
    .await
    .is_ok()
    {
        return Err("remote route accepted a substituted authenticated principal".to_owned());
    }
    let wrong_delegate = Principal::node(PrincipalId::new("node:not-the-router"));
    if routed_command_client(
        &socket,
        remote.node().node_id(),
        owner.clone(),
        wrong_delegate,
        local.node().node_id(),
    )
    .submit_typed_command(RemoteLifecycleCommand)
    .await
    .is_ok()
    {
        return Err("remote route accepted a forged forwarding hop".to_owned());
    }

    let commands = LocalCommandClient::new(&socket)
        .at(remote.node().node_id())
        .with_authority(AuthorityPresentation::direct(owner.clone()))
        .with_forwarding_hop(forwarding_hop.clone());
    let submitted = commands
        .submit_typed_command(RemoteLifecycleCommand)
        .await
        .map_err(|error| format!("valid routed submit: {error}"))?;
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
        .map_err(|error| format!("routed state: {error}"))?;
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
        .map_err(|error| format!("routed cancel: {error}"))?;
    if cancelled
        .command
        .as_ref()
        .is_none_or(|command| !command.state.is_committed())
    {
        return Err(format!(
            "terminal remote command changed after cancellation: {cancelled:?}"
        ));
    }
    {
        let recorded = remote_policy
            .requests
            .lock()
            .map_err(|_| "recording-policy lock is poisoned".to_owned())?;
        let routed_presentation = recorded
            .iter()
            .find(|request| request.operation == AccessOperation::SubmitCommand)
            .map(|request| request.presentation.clone())
            .ok_or_else(|| "destination policy did not observe the routed submission".to_owned())?;
        drop(recorded);
        if routed_presentation.principal != owner
            || routed_presentation.executor != forwarding_node
            || routed_presentation.provenance != vec![forwarding_hop]
        {
            return Err(format!(
                "routed authority did not preserve the original principal and forwarding hop: {routed_presentation:?}"
            ));
        }
    }

    server
        .shutdown()
        .await
        .map_err(|error| format!("shutdown local server: {error}"))?;
    local
        .shutdown()
        .await
        .map_err(|error| format!("shutdown local: {error}"))?;
    remote
        .shutdown()
        .await
        .map_err(|error| format!("shutdown remote: {error}"))
}

#[tokio::test]
async fn connected_client_places_a_command_on_a_capable_peer() -> Result<(), String> {
    let local_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let remote_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let local = open_loopback_allow(local_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remote_application = MykoApplication::builder()
        .service::<ReactiveService>()
        .build();
    let remote = Node::open_loopback_application_with_policy(
        remote_directory.path(),
        retry_interval,
        remote_application,
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await
    .map_err(|error| error.to_string())?;
    let remote_node = remote.node().node_id();
    let services = remote
        .application()
        .watch_view(&AdvertisedServicesView {
            source_node: remote_node,
        })
        .map_err(|error| error.to_string())?;
    let advertised = services.snapshot();
    if !advertised
        .iter()
        .any(|(_, service)| service.is::<ReactiveService>())
        || !advertised
            .iter()
            .any(|(_, service)| service.is::<FederationService>())
    {
        return Err("node did not advertise its compiled services".to_owned());
    }

    let _peer = add_pinned_peer(&local, remote.descriptor()).await?;
    let capability = local
        .application()
        .watch_report(&ServiceCapabilityReport::for_service::<ReactiveService>(
            remote_node,
        ))
        .map_err(|error| error.to_string())?;
    wait_for("peer service catalog was not replicated", || {
        *capability.get()
    })
    .await?;

    let socket = local_directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_sessions(
        &socket,
        local.replicator().sessions().clone(),
        PrincipalId::new("owner:local"),
    )
    .await
    .map_err(|error| error.to_string())?;
    let owner = Principal::node(PrincipalId::new("owner:local"));
    let forwarding_node = Principal::node(endpoint_principal_id(local.address().id));
    let executed_by = LocalCommandClient::new(&socket)
        .with_authority(AuthorityPresentation::direct(owner.clone()))
        .with_forwarding_hop(ProvenanceHop {
            delegation_id: DelegationId::random(),
            delegator: owner,
            delegate: forwarding_node,
            operation: ProvenanceOperation::NodeForward {
                node_id: local.node().node_id().to_string(),
            },
        })
        .exec_typed_command(RemoteLifecycleCommand)
        .await
        .map_err(|error| error.to_string())?;
    if executed_by != remote_node {
        return Err(format!(
            "capability-routed command executed on {executed_by}, expected {remote_node}"
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
    let local = open_loopback_allow(local_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remote = open_loopback_allow(remote_directory.path(), retry_interval)
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
    let owner = Principal::node(PrincipalId::new("owner:local"));
    let forwarding_node = Principal::node(endpoint_principal_id(local.address().id));
    let client = LocalNodeClient::new(&socket)
        .at(target_node)
        .with_authority(AuthorityPresentation::direct(owner.clone()))
        .with_forwarding_hop(ProvenanceHop {
            delegation_id: DelegationId::random(),
            delegator: owner,
            delegate: forwarding_node,
            operation: ProvenanceOperation::NodeForward {
                node_id: local.node().node_id().to_string(),
            },
        });
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
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| format!("open pairing source: {error}"))?;
    let target = open_loopback_allow(target_directory.path(), retry_interval)
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

    let added = myko_federation::CommandWatchingClient::exec_typed_command(
        application,
        AddPeer {
            reference: descriptor.into(),
        },
    )
    .await
    .map_err(|error| format!("issue pairing invitation: {error}"))?;
    if added.id != configured_peer_id || !added.replication_enabled {
        return Err(format!("typed add returned an unexpected peer: {added:?}"));
    }
    wait_for(
        "typed peer live surfaces did not observe the added peer",
        || {
            view.snapshot()
                .iter()
                .any(|(_, peer)| peer.id == configured_peer_id && peer.replication_enabled)
                && report
                    .get()
                    .as_ref()
                    .as_ref()
                    .is_some_and(|peer| peer.id == configured_peer_id && peer.replication_enabled)
        },
    )
    .await?;

    let paused = myko_federation::CommandWatchingClient::exec_typed_command(
        application,
        SetPeerReplication {
            peer_id: configured_peer_id.clone(),
            enabled: false,
        },
    )
    .await
    .map_err(|error| format!("admit pairing redemption: {error}"))?;
    if paused.replication_enabled {
        return Err("typed replication command did not pause the peer".to_owned());
    }
    wait_for(
        "peer report did not observe the paused relationship",
        || {
            report
                .get()
                .as_ref()
                .as_ref()
                .is_some_and(|peer| !peer.replication_enabled)
        },
    )
    .await?;

    myko_federation::CommandWatchingClient::exec_typed_command(
        application,
        RemovePeer {
            peer_id: configured_peer_id,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    wait_for("typed peer live surfaces did not observe removal", || {
        view.snapshot().is_empty() && report.get().as_ref().as_ref().is_none()
    })
    .await?;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn peer_replication_selection_survives_restart() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let peer = add_pinned_peer(&target, source.descriptor()).await?;
    if peer.replication_selection != ReplicationSelection::All {
        return Err("new peer did not default to full replication".to_owned());
    }
    let selection = ReplicationSelection::Service(ServiceId::new(ReactiveService::SERVICE_ID));
    let selected = myko_federation::CommandWatchingClient::exec_typed_command(
        target.application(),
        SetPeerReplicationSelection {
            peer_id: peer.id.clone(),
            selection: selection.clone(),
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if selected.replication_selection != selection {
        return Err("typed selection command did not update the peer".to_owned());
    }
    target.shutdown().await.map_err(|error| error.to_string())?;

    let reopened = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let peers = watch_peers(&reopened)?;
    if !peers
        .snapshot()
        .iter()
        .any(|(_, peer)| peer.replication_selection == selection)
    {
        return Err("replication selection did not survive restart".to_owned());
    }
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn discovery_configuration_is_a_durable_live_framework_report() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let node = open_loopback_allow(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let report = node
        .application()
        .watch_report(&DiscoverySettingsReport {
            source_node: node.node().node_id(),
        })
        .map_err(|error| error.to_string())?;
    let configured = myko_federation::CommandWatchingClient::exec_typed_command(
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
            .get()
            .as_ref()
            .as_ref()
            .is_some_and(|settings| settings == &configured)
    })
    .await?;
    let node_id = node.node().node_id();
    node.shutdown().await.map_err(|error| error.to_string())?;

    let reopened = open_loopback_allow(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let restored = reopened
        .application()
        .watch_report(&DiscoverySettingsReport {
            source_node: node_id,
        })
        .map_err(|error| error.to_string())?;
    if restored.get().as_ref().as_ref() != Some(&configured) {
        return Err("LAN discovery settings did not survive restart".to_owned());
    }
    reopened.shutdown().await.map_err(|error| error.to_string())
}

async fn complete_pairing_redemption(
    target: &Node,
    invitation: PairingInvitation,
) -> Result<PairingReceipt, String> {
    let redemption = myko_federation::CommandWatchingClient::exec_typed_command(
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
            .get()
            .as_ref()
            .as_ref()
            .is_some_and(|redemption| redemption.phase.is_terminal())
    })
    .await?;
    match redemption_report
        .get()
        .as_ref()
        .as_ref()
        .map(|redemption| redemption.phase.clone())
    {
        Some(PairingRedemptionPhase::Completed { receipt }) => Ok(receipt),
        Some(PairingRedemptionPhase::Failed { reason }) => Err(reason),
        phase => Err(format!("unexpected pairing redemption phase: {phase:?}")),
    }
}

async fn receive_pairing_receipt(
    inbound: &myko::view::TypedViewCellMap<PairingReceiptRow>,
    expected: &PairingReceipt,
) -> Result<PairingReceipt, String> {
    wait_for("source did not observe the pairing receipt view", || {
        !inbound.snapshot().is_empty()
    })
    .await?;
    let receipts = inbound
        .snapshot()
        .into_iter()
        .map(|(_, row)| row.receipt.clone())
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
    myko_federation::CommandWatchingClient::exec_typed_command(
        node.application(),
        ConfirmPairing {
            comparison_code: receipt.comparison_code.clone(),
            receipt,
        },
    )
    .await
    .map_err(|error| error.to_string())
}

async fn complete_pairing_initiation(
    source: &Node,
    peer: NativeNodeDescriptor,
) -> Result<PairingReceipt, String> {
    let initiation = myko_federation::CommandWatchingClient::exec_typed_command(
        source.application(),
        InitiatePairing {
            peer,
            ttl_seconds: 60,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let report = source
        .application()
        .watch_report(&PairingInitiationReport {
            source_node: source.node().node_id(),
            initiation_id: initiation.id,
        })
        .map_err(|error| error.to_string())?;
    wait_for("pairing initiation report did not become terminal", || {
        report
            .get()
            .as_ref()
            .as_ref()
            .is_some_and(|initiation| initiation.phase.is_terminal())
    })
    .await?;
    match report
        .get()
        .as_ref()
        .as_ref()
        .map(|initiation| initiation.phase.clone())
    {
        Some(PairingInitiationPhase::Completed { receipt }) => Ok(receipt),
        Some(PairingInitiationPhase::Failed { reason }) => Err(reason),
        phase => Err(format!("unexpected pairing initiation phase: {phase:?}")),
    }
}

fn require_peer_state(
    peers: &myko::view::TypedViewCellMap<Peer>,
    source_node: NodeId,
    replication_enabled: bool,
    error: &str,
) -> Result<(), String> {
    let snapshot = peers.snapshot();
    let valid = snapshot.first().is_some_and(|(_, peer)| {
        peer.source_node == Some(source_node) && peer.replication_enabled == replication_enabled
    });
    if valid { Ok(()) } else { Err(error.to_owned()) }
}

#[tokio::test]
async fn confirmed_pairing_remembers_pinned_peers_before_replication() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let inbound = source
        .application()
        .watch_view(&PairingReceiptsView {})
        .map_err(|error| error.to_string())?;
    let invitation = myko_federation::CommandWatchingClient::exec_typed_command(
        source.application(),
        IssuePairingInvitation { ttl_seconds: 60 },
    )
    .await
    .map_err(|error| error.to_string())?;
    let outbound = complete_pairing_redemption(&target, invitation).await?;
    let received = receive_pairing_receipt(&inbound, &outbound).await?;
    if myko_federation::CommandWatchingClient::exec_typed_command(
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
        true,
        "confirmed target did not enable replication for the pinned source",
    )?;
    let enabled = myko_federation::CommandWatchingClient::exec_typed_command(
        target.application(),
        SetPeerReplication {
            peer_id: peer_id(source_descriptor.endpoint.id),
            enabled: true,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    if !enabled.replication_enabled {
        return Err("remembered pairing did not enable replication".to_owned());
    }
    target
        .shutdown()
        .await
        .map_err(|error| format!("shutdown pairing target: {error}"))?;
    let reopened = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| format!("reopen pairing target: {error}"))?;
    let reopened_peers = watch_peers(&reopened)?;
    require_peer_state(
        &reopened_peers,
        source_descriptor.node_id,
        true,
        "enabled pairing relationship did not survive target restart",
    )?;
    reopened
        .shutdown()
        .await
        .map_err(|error| format!("shutdown reopened pairing target: {error}"))?;
    source
        .shutdown()
        .await
        .map_err(|error| format!("shutdown pairing source: {error}"))
}

#[tokio::test]
async fn typed_pairing_initiation_is_live_and_requires_mutual_confirmation() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let source_receipts = source
        .application()
        .watch_view(&PairingReceiptsView {})
        .map_err(|error| error.to_string())?;
    let target_receipts = target
        .application()
        .watch_view(&PairingReceiptsView {})
        .map_err(|error| error.to_string())?;

    let receipt = complete_pairing_initiation(&source, target.descriptor()).await?;
    let source_receipt = receive_pairing_receipt(&source_receipts, &receipt).await?;
    let target_receipt = receive_pairing_receipt(&target_receipts, &receipt).await?;
    if !watch_peers(&source)?.snapshot().is_empty() || !watch_peers(&target)?.snapshot().is_empty()
    {
        return Err("pairing initiation implicitly trusted a peer".to_owned());
    }

    let _source_peer = confirm_pairing(&source, source_receipt).await?;
    let _target_peer = confirm_pairing(&target, target_receipt).await?;
    let source_peers = watch_peers(&source)?;
    let target_peers = watch_peers(&target)?;
    require_peer_state(
        &source_peers,
        target.node().node_id(),
        true,
        "source did not remember the confirmed target",
    )?;
    require_peer_state(
        &target_peers,
        source.node().node_id(),
        true,
        "target did not remember the confirmed source",
    )?;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn pending_pairing_receipt_survives_recipient_restart() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target_receipts = target
        .application()
        .watch_view(&PairingReceiptsView {})
        .map_err(|error| error.to_string())?;

    let receipt = complete_pairing_initiation(&source, target.descriptor()).await?;
    let _received = receive_pairing_receipt(&target_receipts, &receipt).await?;
    target.shutdown().await.map_err(|error| error.to_string())?;

    let reopened = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let reopened_receipts = reopened
        .application()
        .watch_view(&PairingReceiptsView {})
        .map_err(|error| error.to_string())?;
    let restored = receive_pairing_receipt(&reopened_receipts, &receipt).await?;
    if restored != receipt {
        return Err("recipient restart restored the wrong pairing receipt".to_owned());
    }
    if !watch_peers(&reopened)?.snapshot().is_empty() {
        return Err("recipient restart implicitly trusted the pending peer".to_owned());
    }
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn restart_restores_identities_peers_and_durable_cursor() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let source_descriptor = source.descriptor();
    let source_address = source_descriptor.endpoint.clone();
    let first_command = commit_test_command(source.node(), "before-restart")?;

    let target = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let first_transport_id = target.address().id;
    let first_node_id = target.node().node_id();
    let _peer = add_pinned_peer(&target, source_descriptor.clone()).await?;
    wait_for_committed(target.node(), first_command).await?;
    target.shutdown().await.map_err(|error| error.to_string())?;

    let second_command = commit_test_command(source.node(), "while-target-offline")?;
    let reopened = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    if reopened.address().id != first_transport_id || reopened.node().node_id() != first_node_id {
        return Err("durable node identity changed across restart".to_owned());
    }
    let reopened_peers = watch_peers(&reopened)?;
    let reopened_statuses = watch_node_statuses(&reopened)?;
    let peer_rows = reopened_peers.snapshot();
    let status_rows = reopened_statuses.snapshot();
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
        return Err("configured peer replication was not restored".to_owned());
    }
    wait_for_committed(reopened.node(), second_command).await?;

    remove_peer(&reopened, source_address.id).await?;
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    let after_removal = open_loopback_allow(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let remaining_peers = watch_peers(&after_removal)?;
    let remaining_statuses = watch_node_statuses(&after_removal)?;
    if !remaining_peers.snapshot().is_empty()
        || remaining_statuses
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
    let source = open_loopback_allow(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let command_id = commit_test_command(source.node(), "must-not-replicate")?;
    let source_address = source.address();
    let expected_source = NodeId::new();
    let target = open_loopback_allow(target_directory.path(), retry_interval)
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
            let statuses = status_view.snapshot();
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
    .map_err(|_| "pinned peer did not reject the unexpected source".to_owned())??;
    if target
        .node()
        .command(command_id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("pinned peer ingested history from the wrong Myko node".to_owned());
    }

    remove_peer(&target, source_address.id).await?;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}
