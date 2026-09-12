use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use hyphae::{Signal, Watchable as _};
use myko::{CommandContext, CommandError, CommandHandler, view::ViewHandler};
use myko_federation::{
    AccessAttempt, AccessOperation, AllowAllAccessPolicy, ApprovalId, AuthorityPresentation,
    AuthorityRealmId, AuthorityUnavailable, AuthorizationBinding, AuthorizationDecision, BatchId,
    ChangeBatch, CommandRequest, DelegationId, LiveCollectionHandle as _,
    LiveSubscriptionHandle as _, ObligationId, Principal, PrincipalId, PrincipalKind,
    ProvenanceOperation, ResourceClaim, ResourceClaimKind, ServiceId, SubscriptionLiveness,
};
use myko_items::{ItemMutation, myko_command, myko_service};

mod command_dependencies;
mod execution_control;
mod handler_authorization;
mod handler_ownership;

#[derive(Debug)]
struct ApprovalPolicy;

impl AccessPolicy for ApprovalPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        Ok(AuthorizationDecision::from_rule(request, Ok(()))).into()
    }

    fn approve<'a>(
        &'a self,
        authenticated_executor: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        challenge_id: &'a ChallengeId,
        approved: bool,
    ) -> myko_federation::AuthorityApprovalFuture<'a> {
        Box::pin(async move {
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
        })
    }
}

#[derive(Debug)]
struct PresentationPolicy {
    expected: AuthorityPresentation,
    operations: Arc<Mutex<Vec<AccessOperation>>>,
}

#[derive(Debug)]
struct RecoverableAuthorityPolicy {
    available: std::sync::atomic::AtomicBool,
    unavailable_attempts: flume::Sender<(AccessOperation, myko_federation::AuthorizationPhase)>,
}

impl AccessPolicy for RecoverableAuthorityPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        if matches!(
            request.operation,
            AccessOperation::FollowHandler
                | AccessOperation::FollowItems
                | AccessOperation::ReadItems
        ) && !self.available.load(std::sync::atomic::Ordering::SeqCst)
        {
            let _ignored = self
                .unavailable_attempts
                .try_send((request.operation, request.authorization_phase));
            return Err(AuthorityUnavailable::CoordinationUnavailable).into();
        }
        AllowAllAccessPolicy.decide(request)
    }
}

impl AccessPolicy for PresentationPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let rule = if request.presentation == self.expected {
            let Ok(mut operations) = self.operations.lock() else {
                return Err(AuthorityUnavailable::PolicyUnavailable).into();
            };
            operations.push(request.operation);
            Ok(())
        } else {
            Err("authority presentation was not preserved".to_owned())
        };
        Ok(AuthorizationDecision::from_rule(request, rule)).into()
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
    ) -> Result<Option<impl myko::query::QueryBuildOutput>, String> {
        Ok(Some(myko::query::RetainedQuery::new(
            ctx.federated_items::<LocalRecord>()?,
        )))
    }
}

#[myko::myko_view(LocalRecord, item = LocalRecord)]
#[derive(Copy, PartialEq, Eq)]
struct AllLocalRecordsView {}

impl ViewHandler for AllLocalRecordsView {
    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new("local-scope"))
    }

    fn build_cell(
        context: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = Self::Item>, String> {
        Ok(myko::view::RetainedView::new(
            context.federated_items::<LocalRecord>()?,
        ))
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
        .follow_query(
            Some(node.node_id()),
            scope_id.clone(),
            &AllLocalRecordHandlers {},
        )
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    if query.current().value.as_deref() != Some(std::slice::from_ref(&first)) {
        return Err(LocalPeerError::Protocol(format!(
            "local handler lost its initial query rows: {:?}",
            query.current()
        )));
    }

    let second = commit_record(&node, scope_id, "record-2")?;
    let update = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = query.recv().await?;
            // An unchanged publication can precede the newly committed rows.
            if update.value.as_deref() != Some(std::slice::from_ref(&first))
                || update.liveness != SubscriptionLiveness::Current
            {
                break Ok::<_, myko::client::HandlerClientError>(update);
            }
        }
    })
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
async fn retained_query_recovers_from_authority_outage_on_the_same_socket()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    let first = commit_record(&node, scope.clone(), "record-1")?;
    let (attempts_tx, attempts_rx) = flume::unbounded();
    let policy = Arc::new(RecoverableAuthorityPolicy {
        available: std::sync::atomic::AtomicBool::new(true),
        unavailable_attempts: attempts_tx,
    });
    let probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        policy.clone(),
        probe.clone(),
    )
    .await?;
    let local = LocalClientSession::new(&socket).with_reconnect_policy(ReconnectPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(20),
    )?);
    let query = local.handler_connector().client().follow_query_reactive(
        Some(node.node_id()),
        scope.clone(),
        &AllLocalRecordHandlers {},
    )?;
    let retained = query.live_collection().clone();
    let items = local
        .item_client()
        .watch_serving_items_reactive(scope.clone(), GetAllLocalRecords {})
        .await?;
    let (item_updates_tx, item_updates_rx) = flume::unbounded();
    let _item_guard = items.live().state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _ignored = item_updates_tx.send(state.clone());
        }
    });
    let (updates_tx, updates_rx) = flume::unbounded();
    let _guard = retained.state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _ignored = updates_tx.send(state.clone());
        }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while retained.current_state().liveness != SubscriptionLiveness::Current {
            updates_rx.recv_async().await?;
        }
        Ok::<_, flume::RecvError>(())
    })
    .await??;
    policy
        .available
        .store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let update = updates_rx.recv_async().await?;
            if matches!(
                update.liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            ) {
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    assert_items_resynchronizing(&item_updates_rx, &first).await?;
    assert_unavailable_read_retries(&attempts_rx).await?;
    let second = commit_record(&node, scope, "record-2")?;
    policy
        .available
        .store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let update = updates_rx.recv_async().await?;
            if update.liveness == SubscriptionLiveness::Current {
                let rows = retained.rows().snapshot();
                assert_eq!(rows.len(), 2);
                assert!(rows.iter().any(|(_, row)| row.as_ref() == &first));
                assert!(rows.iter().any(|(_, row)| row.as_ref() == &second));
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let update = item_updates_rx.recv_async().await?;
            if update.liveness == SubscriptionLiveness::Current {
                assert_eq!(update.value, Some(vec![first.clone(), second.clone()]));
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
    server.shutdown().await?;
    Ok(())
}

async fn assert_items_resynchronizing(
    updates: &flume::Receiver<Arc<myko_federation::LiveSubscriptionState<Vec<LocalRecord>>>>,
    retained: &LocalRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let update = updates.recv_async().await?;
            assert!(!matches!(
                update.liveness,
                SubscriptionLiveness::Invalid { .. }
            ));
            if matches!(
                update.liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            ) {
                assert_eq!(
                    update.value.as_deref(),
                    Some(std::slice::from_ref(retained))
                );
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    Ok(())
}

async fn assert_unavailable_read_retries(
    attempts: &flume::Receiver<(AccessOperation, myko_federation::AuthorizationPhase)>,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut handler_retried = false;
        let mut items_retried = false;
        loop {
            let (operation, phase) = attempts.recv_async().await?;
            if phase == myko_federation::AuthorizationPhase::Admission {
                handler_retried |= operation == AccessOperation::FollowHandler;
                items_retried |= operation == AccessOperation::ReadItems;
            }
            if handler_retried && items_retried {
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn one_connector_family_multiplexes_128_handler_subscriptions() -> Result<(), LocalPeerError>
{
    const SUBSCRIPTION_COUNT: usize = 128;

    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope_id = ScopeId::new("local-scope");
    let first = commit_record(&node, scope_id.clone(), "record-1")?;
    let probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        probe.clone(),
    )
    .await?;

    let connector = LocalHandlerConnector::new(&socket);
    let identified = HandlerConnector::target_node(&connector)
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(identified, node.node_id());
    let client = connector.client();
    let routed_client = client.clone().at(node.node_id());
    let mut opening = JoinSet::new();
    for index in 0..SUBSCRIPTION_COUNT {
        let client = if index % 2 == 0 {
            client.clone()
        } else {
            routed_client.clone()
        };
        let source_node = node.node_id();
        let scope_id = scope_id.clone();
        opening.spawn(async move {
            client
                .follow_query(Some(source_node), scope_id, &AllLocalRecordHandlers {})
                .await
        });
    }

    let mut subscriptions = tokio::time::timeout(Duration::from_secs(5), async {
        let mut subscriptions = Vec::with_capacity(SUBSCRIPTION_COUNT);
        while let Some(joined) = opening.join_next().await {
            let subscription = joined
                .map_err(|error| LocalPeerError::Protocol(error.to_string()))?
                .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
            assert_eq!(
                subscription.current().value.as_deref(),
                Some(std::slice::from_ref(&first))
            );
            subscriptions.push(subscription);
        }
        Ok::<_, LocalPeerError>(subscriptions)
    })
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("opening 128 handler subscriptions timed out".to_owned())
    })??;

    assert_eq!(subscriptions.len(), SUBSCRIPTION_COUNT);
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);

    drop(subscriptions.pop());
    let second = commit_record(&node, scope_id.clone(), "record-2")?;
    let committed_through = node
        .authoritative_position_in::<LocalService>(&scope_id)?
        .ok_or_else(|| LocalPeerError::Protocol("missing record commit frontier".to_owned()))?;
    let surviving = subscriptions
        .get_mut(0)
        .ok_or_else(|| LocalPeerError::Protocol("missing surviving subscription".to_owned()))?;
    let update = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = surviving.recv().await?;
            if update
                .through
                .is_some_and(|through| through >= committed_through)
            {
                return Ok::<_, HandlerClientError>(update);
            }
            assert_eq!(update.value.as_deref(), Some(std::slice::from_ref(&first)));
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("surviving handler update timed out".to_owned()))?
    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(update.liveness, SubscriptionLiveness::Current);
    assert_eq!(update.value, Some(vec![first, second]));
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);

    drop(subscriptions);
    server.shutdown().await
}

#[tokio::test]
async fn reactive_handlers_open_before_local_server_and_share_one_socket()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    let record = commit_record(&node, scope.clone(), "record-1")?;
    let local = LocalClientSession::new(&socket).with_reconnect_policy(ReconnectPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(20),
    )?);
    let client = local.handler_connector().client();
    let query =
        client.follow_query_reactive(Some(node.node_id()), scope, &AllLocalRecordHandlers {})?;
    let report = client.follow_report_reactive(&CountAllLocalRecords {})?;
    let view = client.follow_view_reactive(&AllLocalRecordsView {})?;
    assert_eq!(
        query.live_collection().current_state().liveness,
        SubscriptionLiveness::Connecting
    );
    assert_eq!(
        report.live_subscription().current().liveness,
        SubscriptionLiveness::Connecting
    );
    assert_eq!(
        view.live_collection().current_state().liveness,
        SubscriptionLiveness::Connecting
    );
    assert!(query.live_collection().rows().snapshot().is_empty());
    assert!(view.live_collection().rows().snapshot().is_empty());
    assert!(report.live_subscription().current().value.is_none());

    // The listener is absent when the initial-open tasks are scheduled.
    tokio::task::yield_now().await;
    let probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node)?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        probe.clone(),
    )
    .await?;

    let mut waiting_for = "query";
    tokio::time::timeout(Duration::from_secs(3), async {
        for (kind, collection) in [
            ("query", query.live_collection()),
            ("view", view.live_collection()),
        ] {
            waiting_for = kind;
            let (states_tx, states_rx) = flume::unbounded();
            // Revision events may precede cell settlement in a shared Hyphae batch.
            let _guard = collection.state().subscribe(move |signal| {
                if let Signal::Value(state) = signal {
                    let _ignored = states_tx.send(state.liveness.clone());
                }
            });
            while states_rx.recv_async().await? != SubscriptionLiveness::Current {
            }
            assert_eq!(
                collection.rows().snapshot(),
                vec![(Arc::from("record-1"), Arc::new(record.clone()))]
            );
        }
        waiting_for = "report";
        let live_report = report.live_subscription();
        let mut publications = live_report.watch_publications();
        let report_state = loop {
            let state = publications.recv_async().await?.state;
            if state.liveness == SubscriptionLiveness::Current {
                break state;
            }
        };
        assert_eq!(report_state.value.map(|value| value.count), Some(1));
        Ok::<_, flume::RecvError>(())
    })
    .await
    .map_err(|error| {
        format!(
            "startup wait stalled on {waiting_for}: {error}; query={:?}; report={:?}; view={:?}; accepted={}; peak_active={}",
            query.live_collection().current_state(),
            report.live_subscription().current(),
            view.live_collection().current_state(),
            probe.accepted(),
            probe.peak_active(),
        )
    })??;
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
    drop((query, report, view));
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_handler_survives_local_server_restart() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope_id = ScopeId::new("local-scope");
    let first = commit_record(&node, scope_id.clone(), "record-1")?;
    let reconnect_policy =
        ReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(20))
            .map_err(|error| LocalPeerError::Protocol(error.to_owned()))?;
    let first_probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        first_probe.clone(),
    )
    .await?;
    let local = LocalClientSession::new(&socket).with_reconnect_policy(reconnect_policy);
    let client = local.handler_connector().client();
    let mut query = client
        .follow_query(
            Some(node.node_id()),
            scope_id.clone(),
            &AllLocalRecordHandlers {},
        )
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(
        query.current().value.as_deref(),
        Some(std::slice::from_ref(&first))
    );
    let mut report = client
        .follow_report(&CountAllLocalRecords {})
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(
        report.current().value.as_ref().map(|count| count.count),
        Some(1)
    );
    let mut view = client
        .follow_view(&AllLocalRecordsView {})
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(
        view.current().value.as_deref(),
        Some(std::slice::from_ref(&first))
    );
    let reactive_view = client
        .follow_view_reactive(&AllLocalRecordsView {})
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    let (reactive_updates_tx, reactive_updates_rx) = flume::bounded(16);
    let _reactive_guard = reactive_view
        .live_collection()
        .state()
        .subscribe(move |signal| {
            if let Signal::Value(state) = signal {
                let _ignored = reactive_updates_tx.send(state.clone());
            }
        });
    let mut reactive_before_disconnect = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = reactive_updates_rx
                .recv_async()
                .await
                .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
            if state.liveness == SubscriptionLiveness::Current {
                return Ok::<_, LocalPeerError>(state);
            }
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("initial reactive view timed out".to_owned()))??;
    let submitted = local
        .command_client()
        .submit_typed_command(SetLocalRecord {
            id: LocalRecordId::from("restart-command"),
            value: "pending".to_owned(),
        })
        .await?;
    let command = submitted.command.ok_or_else(|| {
        LocalPeerError::Protocol("local command submission returned no state".to_owned())
    })?;
    let command_id = command.request.id;
    let (_initial, mut command_watch) = local.command_client().watch_command(command_id).await?;
    assert_eq!(first_probe.accepted(), 1);

    server.shutdown().await?;
    let reactive_resync = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = reactive_updates_rx.recv_async().await.map_err(|error| {
                LocalPeerError::Protocol(format!("reactive view observation ended: {error}"))
            })?;
            if update.liveness == SubscriptionLiveness::Current {
                reactive_before_disconnect = update;
                continue;
            }
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
        LocalPeerError::Protocol("reactive view did not expose the disconnected state".to_owned())
    })??;
    assert_eq!(
        reactive_resync.through, reactive_before_disconnect.through,
        "resynchronization must retain the last coherent view frontier"
    );
    let stale_submission = CommandSubmission::for_command(&SetLocalRecord {
        id: LocalRecordId::from("during-reconnect"),
        value: "must not queue".to_owned(),
    })?;
    let history_before = node.events_after(None)?;
    let disconnected = tokio::time::timeout(
        Duration::from_secs(1),
        local
            .command_client()
            .submit_submission(stale_submission.clone()),
    )
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("submission waited behind reconnecting handles".to_owned())
    })?;
    assert!(matches!(disconnected, Err(error) if error.to_string().contains("not submitted")));
    assert_eq!(node.events_after(None)?, history_before);
    let admission = node.claim(command_id)?;
    node.commit(
        command_id,
        ChangeBatch {
            id: BatchId::new(),
            command_id,
            service_id: command.request.service_id,
            scope_id: command.request.scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        Vec::new(),
    )?;
    let second = commit_record(&node, scope_id, "record-2")?;
    let second_probe = LocalServerProbe::default();
    let restarted = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        second_probe.clone(),
    )
    .await?;

    let reactive_recovered = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = reactive_updates_rx.recv_async().await.map_err(|error| {
                LocalPeerError::Protocol(format!("reactive view observation ended: {error}"))
            })?;
            if update.liveness == SubscriptionLiveness::Current {
                return Ok::<_, LocalPeerError>(update);
            }
        }
    })
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("reactive view did not recover after reconnect".to_owned())
    })??;
    assert_ne!(
        reactive_recovered.through, reactive_resync.through,
        "recovered view frontier must cover the commits made while disconnected"
    );
    assert_eq!(
        reactive_view.live_collection().current_state(),
        *reactive_recovered,
        "recovered lifecycle state must match the collection's coherent revision"
    );
    let mut reactive_recovered_rows = reactive_view.live_collection().rows().snapshot();
    reactive_recovered_rows.sort_by(|(left, _), (right, _)| left.cmp(right));
    assert_eq!(
        reactive_recovered_rows,
        vec![
            (Arc::from("record-1"), Arc::new(first.clone())),
            (Arc::from("record-2"), Arc::new(second.clone())),
        ],
        "recovered frontier must be published with the recovered rows"
    );
    assert!(node.command(stale_submission.id)?.is_none());
    local
        .command_client()
        .submit_submission(stale_submission.clone())
        .await?;
    assert!(node.command(stale_submission.id)?.is_some());

    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(
            query.recv().await?.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) {}
        while !matches!(
            report.recv().await?.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) {}
        while !matches!(
            view.recv().await?.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) {}
        Ok::<(), HandlerClientError>(())
    })
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("handler stream did not expose resynchronization".to_owned())
    })?
    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;

    let query_update = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = query.recv().await?;
            if update.liveness == SubscriptionLiveness::Current {
                return Ok::<_, HandlerClientError>(update);
            }
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("live query did not reconnect after restart".to_owned()))?
    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    let report_update = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = report.recv().await?;
            if update.liveness == SubscriptionLiveness::Current {
                return Ok::<_, HandlerClientError>(update);
            }
        }
    })
    .await
    .map_err(|_| {
        LocalPeerError::Protocol("live report did not reconnect after restart".to_owned())
    })?
    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    let view_update = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let update = view.recv().await?;
            if update.liveness == SubscriptionLiveness::Current {
                return Ok::<_, HandlerClientError>(update);
            }
        }
    })
    .await
    .map_err(|_| LocalPeerError::Protocol("live view did not reconnect after restart".to_owned()))?
    .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    assert_eq!(
        query_update.value,
        Some(vec![first.clone(), second.clone()])
    );
    assert_eq!(
        report_update.value.as_ref().map(|count| count.count),
        Some(2)
    );
    assert_eq!(view_update.value, Some(vec![first, second]));
    let command_update = tokio::time::timeout(Duration::from_secs(2), command_watch.recv())
        .await
        .map_err(|_| {
            LocalPeerError::Protocol("command watch did not reconnect after restart".to_owned())
        })??;
    assert!(command_update.state.is_committed());
    assert_eq!(second_probe.accepted(), 1);
    restarted.shutdown().await
}

#[tokio::test]
async fn dropped_handler_clients_release_connection_capacity() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalHandlerConnector::new(&socket).client();

    for _ in 0..MAX_CONNECTIONS + 8 {
        let query = client
            .follow_query(
                Some(node.node_id()),
                ScopeId::new("local-scope"),
                &AllLocalRecordHandlers {},
            )
            .await
            .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
        drop(query);
        tokio::task::yield_now().await;
    }

    server.shutdown().await
}

#[tokio::test]
async fn local_submission_rejects_incomplete_scope_without_queuing()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        probe.clone(),
    )
    .await?;
    let local = LocalClientSession::new(&socket);
    let client = local.command_client();
    let first = client
        .submit_typed_command(SetLocalRecord {
            id: LocalRecordId::from("first"),
            value: "before catch-up".to_owned(),
        })
        .await?
        .command
        .ok_or("first submission returned no command state")?;
    let source = Node::in_memory();
    commit_record(&source, first.request.scope_id, "source-record")?;
    let mut history = source.events_after(None)?.into_iter();
    let parent = history.next().ok_or("source did not record admission")?;
    let child = history.next().ok_or("source did not record commit")?;
    node.ingest(child)?;
    let before = node.events_after(None)?;
    let rejected = tokio::time::timeout(
        Duration::from_secs(2),
        client.submit_typed_command(SetLocalRecord {
            id: LocalRecordId::from("second"),
            value: "must not queue".to_owned(),
        }),
    )
    .await?;
    match rejected {
        Err(error) if error.to_string().contains("scope history is incomplete") => {}
        Err(error) => return Err(error.into()),
        Ok(_) => {
            return Err("local transport accepted a command into incomplete scope history".into());
        }
    }
    if node.events_after(None)? != before {
        return Err("rejected wire submission recorded command acceptance".into());
    }
    node.ingest(parent)?;
    let recovered = client
        .submit_typed_command(SetLocalRecord {
            id: LocalRecordId::from("second"),
            value: "explicit retry after catch-up".to_owned(),
        })
        .await?;
    if recovered.command.is_none() {
        return Err("wire submission did not recover after scope catch-up".into());
    }
    if probe.accepted() != 1 {
        return Err("rejection replaced the shared client socket".into());
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn local_peer_watches_command_lifecycle_without_polling() -> Result<(), LocalPeerError> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let application = local_record_application(node.clone())?;
    let probe = LocalServerProbe::default();
    let server = LocalNodeServer::spawn_application_with_probe(
        &socket,
        application,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
        probe.clone(),
    )
    .await?;
    let local = LocalClientSession::new(&socket);
    let identified = local.node_client().identify().await?;
    assert_eq!(identified, node.node_id());
    let client = local.command_client();
    let handler = local.handler_connector().client();
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
    let handler_subscription = handler
        .follow_query(
            Some(node.node_id()),
            ScopeId::new("local-scope"),
            &AllLocalRecordHandlers {},
        )
        .await
        .map_err(|error| LocalPeerError::Protocol(error.to_string()))?;
    let (_items, item_subscription) = local
        .item_client()
        .watch_serving_items(ScopeId::new("local-scope"), GetAllLocalRecords {})
        .await?;
    let live_subscription = local.node_client().follow_live(Vec::new()).await?;
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
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
    drop(live_subscription);
    drop(item_subscription);
    drop(handler_subscription);
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
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
            Some(node.node_id()),
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
async fn unavailable_local_server_does_not_queue_a_submission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let local = LocalClientSession::new(&socket);
    let client = local.command_client();
    let command = CommandSubmission::for_command(&SetLocalRecord {
        id: LocalRecordId::from("not-queued"),
        value: "explicit retry".to_owned(),
    })?;
    let failed = tokio::time::timeout(
        Duration::from_secs(1),
        client.submit_submission(command.clone()),
    )
    .await
    .map_err(|_| "submission waited for an unavailable server")?;
    let error = failed.err().ok_or("offline submission succeeded")?;
    assert!(error.to_string().contains("not submitted"));

    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    assert!(node.command(command.id)?.is_none());
    client.submit_submission(command.clone()).await?;
    assert!(node.command(command.id)?.is_some());
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn lost_submission_is_not_replayed_on_a_new_connection()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let listener = UnixListener::bind(&socket)?;
    let local = LocalClientSession::new(&socket);
    let client = local.command_client();
    let command = CommandSubmission::for_command(&SetLocalRecord {
        id: LocalRecordId::from("interrupted"),
        value: "explicit recovery".to_owned(),
    })?;
    let command_id = command.id;
    let peer = tokio::spawn(crate::session_mux::interrupt_submission(
        listener, command_id,
    ));
    let failed = tokio::time::timeout(
        Duration::from_secs(1),
        client.submit_submission(command.clone()),
    )
    .await
    .map_err(|_| "interrupted submission waited to be replayed")?;
    peer.await??;
    let error = failed.err().ok_or("unacknowledged submission succeeded")?;
    assert!(error.to_string().contains("acceptance is unknown"));
    assert!(error.to_string().contains(&command_id.to_string()));

    let node = Node::in_memory();
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    local.node_client().identify().await?;
    assert!(node.command(command_id)?.is_none());
    client.submit_submission(command).await?;
    assert!(node.command(command_id)?.is_some());
    server.shutdown().await?;
    Ok(())
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
