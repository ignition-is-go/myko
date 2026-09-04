use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use hyphae::{Signal, Watchable as _};
use myko::{CommandContext, CommandError, CommandHandler};
use myko_federation::{
    AccessAttempt, AccessOperation, AllowAllAccessPolicy, ApprovalId, AuthorityPresentation,
    AuthorityRealmId, AuthorizationBinding, AuthorizationDecision, BatchId, ChangeBatch,
    CommandRequest, DelegationId, ObligationId, Principal, PrincipalId, PrincipalKind,
    ProvenanceOperation, ResourceClaim, ResourceClaimKind, ServiceId, SubscriptionLiveness,
};
use myko_items::{ItemMutation, myko_command, myko_service};

#[derive(Debug)]
struct ApprovalPolicy;

impl AccessPolicy for ApprovalPolicy {
    fn authorize(&self, _request: &AccessAttempt) -> Result<(), String> {
        Ok(())
    }

    fn approve(
        &self,
        authenticated_executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        challenge_id: &ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, AuthorizationDecision> {
        let now = Utc::now();
        let binding_request = AccessAttempt::scoped(
            authenticated_executor.clone(),
            presentation.clone(),
            AccessOperation::ApproveAuthority,
            ScopeId::new("authority:test"),
        );
        Ok(ApprovalDecision {
            id: ApprovalId::new("local-approval"),
            realm_id: AuthorityRealmId::new("test"),
            challenge_id: challenge_id.clone(),
            obligation_id: ObligationId::new("test-review"),
            approver: presentation.principal.clone(),
            binding: AuthorizationBinding::from_request(&binding_request),
            approved,
            decided_at: now,
            expires_at: now + ChronoDuration::minutes(1),
            max_uses: 1,
        })
    }
}

#[derive(Debug)]
struct PresentationPolicy {
    expected: AuthorityPresentation,
    operations: Arc<Mutex<Vec<AccessOperation>>>,
}

impl AccessPolicy for PresentationPolicy {
    fn authorize(&self, request: &AccessAttempt) -> Result<(), String> {
        if request.presentation != self.expected {
            return Err("authority presentation was not preserved".to_owned());
        }
        self.operations
            .lock()
            .map_err(|_| "presentation-policy lock is poisoned".to_owned())?
            .push(request.operation);
        Ok(())
    }
}

#[myko_service(LocalRecord)]
pub struct LocalService;

#[myko::myko_item(service = LocalService, scope_root)]
pub struct LocalRecord {
    value: String,
}

#[myko::myko_query(LocalRecord, item = LocalRecord)]
#[derive(PartialEq, Eq)]
struct AllLocalRecordHandlers {}

impl myko::query::QueryHandler for AllLocalRecordHandlers {
    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new("local-scope"))
    }

    fn build_view(
        ctx: myko::query::QueryBuildArgs<Self>,
    ) -> Option<impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<dyn myko::item::AnyItem>>> {
        Some(
            ctx.federated_items::<LocalRecord>()
                .expect("test federation source is configured"),
        )
    }
}

#[myko_command(bool, item = LocalRecord)]
struct SetLocalRecord {
    id: LocalRecordId,
    value: String,
}

impl CommandHandler for SetLocalRecord {
    fn scope(&self, _node_id: NodeId) -> LocalRecordId {
        LocalRecordId::from("local-scope")
    }

    fn execute(self, context: CommandContext) -> Result<bool, CommandError> {
        context.emit_set(&LocalRecord {
            id: self.id,
            value: self.value,
        })?;
        Ok(true)
    }
}

fn local_record_application(node: Node) -> Result<ApplicationHost, LocalPeerError> {
    let application = MykoApplication::builder().service::<LocalService>().build();
    ApplicationHost::new(node, application).map_err(LocalPeerError::Protocol)
}

fn commit_record(node: &Node, scope_id: ScopeId, id: &str) -> Result<LocalRecord, LocalPeerError> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(<LocalService as myko_federation::MykoService>::SERVICE_ID),
        scope_id: scope_id.clone(),
        principal_id: PrincipalId::new("local:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("local:test")),
        resource_claims: vec![ResourceClaim::scope(
            scope_id.clone(),
            ResourceClaimKind::Primary,
        )],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "local.insert".to_owned(),
        payload: Vec::new(),
    };
    let admission = node.admit(request.clone())?;
    let record = LocalRecord {
        id: LocalRecordId::from(id),
        value: id.to_owned(),
    };
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![
                ItemMutation::set(&record)
                    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?,
            ],
        },
        Vec::new(),
    )?;
    Ok(record)
}

#[tokio::test]
async fn local_peer_drives_reactive_query_without_polling() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope_id = ScopeId::new("local-scope");
    let initial = commit_record(&node, scope_id.clone(), "record-1")?;
    let server = LocalNodeServer::spawn(
        &socket,
        node.clone(),
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let reactive = LocalItemClient::new(&socket)
        .watch_serving_items_reactive(scope_id.clone(), GetAllLocalRecords {})
        .await?;
    let (updates_tx, updates_rx) = flume::bounded(16);
    let _guard = reactive.live().state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _ignored = updates_tx.send(state.clone());
        }
    });
    let _initial_notification = updates_rx.try_recv();

    let second = commit_record(&node, scope_id.clone(), "record-2")?;
    let update = tokio::time::timeout(Duration::from_secs(2), updates_rx.recv_async())
        .await
        .map_err(|_| LocalPeerError::Protocol("local reactive update timed out".to_owned()))?
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    if update.value != Some(vec![initial.clone(), second.clone()])
        || update.liveness != SubscriptionLiveness::Current
    {
        return Err(LocalPeerError::Protocol(format!(
            "unexpected local reactive state: {update:?}"
        )));
    }

    server.shutdown().await?;
    let resynchronizing = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = updates_rx.recv_async().await.map_err(|error| {
                LocalPeerError::Protocol(format!("reactive observation ended: {error}"))
            })?;
            if matches!(
                update.liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            ) {
                return Ok::<_, LocalPeerError>(update);
            }
        }
    })
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("local reactive state did not begin resync".to_owned())
    })??;
    if resynchronizing.value != Some(vec![initial.clone(), second.clone()]) {
        return Err(LocalPeerError::Protocol(format!(
            "local reactive state did not retain stale data: {resynchronizing:?}"
        )));
    }

    let third = commit_record(&node, scope_id, "record-3")?;
    let server = LocalNodeServer::spawn(
        &socket,
        node,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let recovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let update = updates_rx.recv_async().await.map_err(|error| {
                LocalPeerError::Protocol(format!("reactive observation ended: {error}"))
            })?;
            if update.liveness == SubscriptionLiveness::Current
                && update.value == Some(vec![initial.clone(), second.clone(), third.clone()])
            {
                return Ok::<_, LocalPeerError>(update);
            }
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("local reactive state did not recover".to_owned()))??;
    if recovered.through.is_none() {
        return Err(LocalPeerError::Protocol(
            "recovered local reactive state omitted its cursor".to_owned(),
        ));
    }

    let retained = reactive.live().clone();
    drop(reactive);
    if !matches!(
        retained.current().liveness,
        SubscriptionLiveness::Invalid { ref reason } if reason == "subscription owner dropped"
    ) {
        return Err(LocalPeerError::Protocol(
            "dropping the owner did not invalidate retained state".to_owned(),
        ));
    }
    server.shutdown().await
}

#[tokio::test]
async fn local_handler_connector_follows_retained_query() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope_id = ScopeId::new("local-scope");
    let first = commit_record(&node, scope_id.clone(), "record-1")?;
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalHandlerConnector::new(&socket).client();
    let mut query = client
        .follow_query(node.node_id(), scope_id.clone(), &AllLocalRecordHandlers {})
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    if query.current().value.as_deref() != Some(std::slice::from_ref(&first)) {
        return Err(LocalPeerError::Protocol(format!(
            "local handler lost its initial query rows: {:?}",
            query.current()
        )));
    }

    let second = commit_record(&node, scope_id, "record-2")?;
    let update = tokio::time::timeout(Duration::from_secs(2), query.recv())
        .await
        .map_err(|_| LocalPeerError::Protocol("local handler update timed out".to_owned()))?
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    if update.value != Some(vec![first, second]) {
        return Err(LocalPeerError::Protocol(format!(
            "local handler returned the wrong query rows: {update:?}"
        )));
    }
    server.shutdown().await
}

#[tokio::test]
async fn local_peer_watches_command_lifecycle_without_polling() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let application = local_record_application(node.clone())?;
    let server = LocalNodeServer::spawn_application(
        &socket,
        application,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalCommandClient::new(&socket);
    let submitted = client
        .submit_typed_command(SetLocalRecord {
            id: LocalRecordId::from("lifecycle-record"),
            value: "pending".to_owned(),
        })
        .await?;
    let Some(snapshot) = submitted.command else {
        return Err(LocalPeerError::Protocol(
            "local command submission returned no state".to_owned(),
        ));
    };
    let command_id = snapshot.request.id;
    let (_initial, mut subscription) = client.watch_command(command_id).await?;
    let admission = node.claim(command_id)?;
    node.commit(
        command_id,
        ChangeBatch {
            id: BatchId::new(),
            command_id,
            service_id: snapshot.request.service_id,
            scope_id: snapshot.request.scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        Vec::new(),
    )?;
    let committed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let command = subscription.recv().await?;
            if command.state.is_committed() {
                return Ok::<_, LocalPeerError>(command);
            }
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("local command watch timed out".to_owned()))??;
    if !committed.state.is_committed() {
        return Err(LocalPeerError::Protocol(
            "local command watch returned a non-commit".to_owned(),
        ));
    }
    server.shutdown().await
}

#[tokio::test]
async fn local_client_submits_and_decodes_authenticated_approval() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let principal = Principal::node(PrincipalId::new("local:approver"));
    let sessions = FederatedSession::new(node, Arc::new(ApprovalPolicy));
    let server =
        LocalNodeServer::spawn_sessions_authenticated(&socket, sessions, principal.clone()).await?;
    let decision = LocalCommandClient::new(&socket)
        .approve_authority(ChallengeId::new("local-challenge"), true)
        .await?;
    assert!(decision.approved);
    assert_eq!(decision.approver, principal);
    assert_eq!(decision.challenge_id.as_str(), "local-challenge");
    server.shutdown().await
}

#[tokio::test]
async fn local_item_application_and_live_clients_preserve_authority_presentations()
-> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let _record = commit_record(&node, ScopeId::new("local-scope"), "presented")?;
    let application = local_record_application(node.clone())?;
    let original = Principal::new(PrincipalId::new("person:owner"), PrincipalKind::Person);
    let executor = Principal::new(PrincipalId::new("agent:desktop"), PrincipalKind::Agent);
    let presentation = AuthorityPresentation::direct(original.clone()).forward(ProvenanceHop {
        delegation_id: DelegationId::new("local-delegation"),
        delegator: original,
        delegate: executor.clone(),
        operation: ProvenanceOperation::AgentInvocation {
            agent_id: "desktop".to_owned(),
        },
    });
    let operations = Arc::new(Mutex::new(Vec::new()));
    let policy: Arc<dyn AccessPolicy> = Arc::new(PresentationPolicy {
        expected: presentation.clone(),
        operations: Arc::clone(&operations),
    });
    let sessions = FederatedSession::for_application(application, policy);
    let server = LocalNodeServer::spawn_sessions_authenticated(&socket, sessions, executor).await?;

    let (_initial, items) = LocalItemClient::new(&socket)
        .with_authority(presentation.clone())
        .watch_serving_items(ScopeId::new("local-scope"), GetAllLocalRecords {})
        .await?;
    drop(items);
    let handler = LocalHandlerConnector::new(&socket)
        .with_authority(presentation.clone())
        .client()
        .follow_query(
            node.node_id(),
            ScopeId::new("local-scope"),
            &AllLocalRecordHandlers {},
        )
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    drop(handler);
    let live = LocalNodeClient::new(&socket)
        .with_authority(presentation)
        .follow_live(vec!["presented-topic".to_owned()])
        .await?;
    drop(live);

    {
        let observed = operations.lock().map_err(|_| {
            LocalPeerError::Protocol("presentation-policy lock is poisoned".to_owned())
        })?;
        for expected in [
            AccessOperation::ReadItems,
            AccessOperation::FollowItems,
            AccessOperation::FollowHandler,
            AccessOperation::SubscribeLive,
        ] {
            if !observed.contains(&expected) {
                return Err(LocalPeerError::Protocol(format!(
                    "authority presentation was not exercised for {expected:?}"
                )));
            }
        }
    }
    server.shutdown().await
}

#[tokio::test]
async fn local_peer_executes_typed_command_to_its_result() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let application = local_record_application(node.clone())?;
    let server = LocalNodeServer::spawn_application(
        &socket,
        application.clone(),
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalCommandClient::new(&socket);
    let _dispatch = application
        .drive_commands()
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    if !client
        .exec_typed_command(SetLocalRecord {
            id: LocalRecordId::from("record-command"),
            value: "typed result".to_owned(),
        })
        .await?
    {
        return Err(LocalPeerError::Protocol(
            "typed command returned the wrong result".to_owned(),
        ));
    }
    let records = node.query_items_in(
        node.node_id(),
        &ScopeId::for_item::<LocalRecord>(&LocalRecordId::from("local-scope")),
        GetAllLocalRecords {},
    )?;
    if !matches!(records.as_slice(), [record] if record.value == "typed result") {
        return Err(LocalPeerError::Protocol(
            "typed command did not commit its item".to_owned(),
        ));
    }
    server.shutdown().await
}

#[tokio::test]
async fn local_client_waits_until_the_socket_starts_listening() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let expected_node = node.node_id();
    let reconnect_policy =
        ReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(20))
            .map_err(|error| LocalPeerError::Protocol(error.to_owned()))?;
    let client = LocalNodeClient::new(&socket).with_reconnect_policy(reconnect_policy);
    let pending = tokio::spawn(async move { client.identify().await });

    tokio::time::sleep(Duration::from_millis(80)).await;
    if pending.is_finished() {
        return Err(LocalPeerError::Protocol(
            "local client stopped retrying before the socket existed".to_owned(),
        ));
    }

    let server = LocalNodeServer::spawn(
        &socket,
        node,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let identified = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .map_err(|_| LocalPeerError::Protocol("local client did not reconnect".to_owned()))?
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))??;
    if identified != expected_node {
        return Err(LocalPeerError::Protocol(
            "reconnected local client identified the wrong node".to_owned(),
        ));
    }
    server.shutdown().await
}
