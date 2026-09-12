use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

#[derive(Debug)]
struct HandlerAuthorizationPolicy {
    allowed: AtomicBool,
    available: AtomicBool,
    denials: flume::Sender<AuthorizationDecision>,
    unavailable_attempts: flume::Sender<()>,
}

impl AccessPolicy for HandlerAuthorizationPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let protected = matches!(
            request.operation,
            AccessOperation::FollowHandler
                | AccessOperation::FollowItems
                | AccessOperation::ReadItems
        );
        if protected && !self.available.load(Ordering::SeqCst) {
            let _ignored = self.unavailable_attempts.send(());
            return Err(AuthorityUnavailable::CoordinationUnavailable).into();
        }
        if protected && !self.allowed.load(Ordering::SeqCst) {
            let decision =
                AuthorizationDecision::from_rule(request, Err("handler access revoked".to_owned()));
            let _ignored = self.denials.send(decision.clone());
            return Ok(decision).into();
        }
        AllowAllAccessPolicy.decide(request)
    }
}

#[tokio::test]
async fn handler_authorization_preserves_admission_denial() -> Result<(), Box<dyn std::error::Error>>
{
    check_handler_denial(true).await
}

#[tokio::test]
async fn handler_authorization_preserves_idle_revocation() -> Result<(), Box<dyn std::error::Error>>
{
    check_handler_denial(false).await
}

async fn check_handler_denial(denied_at_open: bool) -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    let record = commit_record(&node, scope.clone(), "protected-record")?;
    let (denials_tx, denials_rx) = flume::unbounded();
    let (attempts_tx, _attempts_rx) = flume::unbounded();
    let policy = Arc::new(HandlerAuthorizationPolicy {
        allowed: AtomicBool::new(!denied_at_open),
        available: AtomicBool::new(true),
        denials: denials_tx,
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
    let local = LocalClientSession::new(&socket);
    let client = local.handler_connector().client();
    let opened = tokio::time::timeout(
        Duration::from_secs(3),
        client.follow_query(
            Some(node.node_id()),
            scope.clone(),
            &AllLocalRecordHandlers {},
        ),
    )
    .await?;
    let error = if denied_at_open {
        match opened {
            Err(error) => error,
            Ok(_) => return Err("denied handler returned a subscription".into()),
        }
    } else {
        let mut query = opened?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while query.current().liveness != SubscriptionLiveness::Current {
                query.recv().await?;
            }
            Ok::<_, HandlerClientError>(())
        })
        .await??;
        assert_eq!(query.current().value, Some(vec![record.clone()]));
        policy.allowed.store(false, Ordering::SeqCst);
        let error = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Err(error) = query.recv().await {
                    break error;
                }
            }
        })
        .await?;
        assert!(matches!(
            query.current().liveness,
            SubscriptionLiveness::AuthorizationBlocked { .. }
        ));
        assert_eq!(query.current().value, None);
        assert_eq!(query.current().through, None);
        error
    };
    let expected = tokio::time::timeout(Duration::from_secs(3), denials_rx.recv_async()).await??;
    match error {
        HandlerClientError::Authorization(decision) => assert_eq!(*decision, expected),
        error => return Err(format!("handler lost its authorization decision: {error:?}").into()),
    }

    policy.allowed.store(true, Ordering::SeqCst);
    let mut reopened = tokio::time::timeout(
        Duration::from_secs(3),
        client.follow_query(Some(node.node_id()), scope, &AllLocalRecordHandlers {}),
    )
    .await??;
    tokio::time::timeout(Duration::from_secs(3), async {
        while reopened.current().liveness != SubscriptionLiveness::Current {
            reopened.recv().await?;
        }
        Ok::<_, HandlerClientError>(())
    })
    .await??;
    assert_eq!(reopened.current().value, Some(vec![record]));
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn handler_authorization_clears_protected_outputs_and_recovers_same_handles()
-> Result<(), Box<dyn std::error::Error>> {
    check_handler_reactive_recovery(false).await
}

#[tokio::test]
async fn handler_authorization_initial_denial_recovers_same_handles()
-> Result<(), Box<dyn std::error::Error>> {
    check_handler_reactive_recovery(true).await
}

async fn check_handler_reactive_recovery(
    denied_at_open: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    commit_record(&node, scope.clone(), "protected-record")?;
    let (denials_tx, _denials_rx) = flume::unbounded();
    let (attempts_tx, attempts_rx) = flume::unbounded();
    let policy = Arc::new(HandlerAuthorizationPolicy {
        allowed: AtomicBool::new(true),
        available: AtomicBool::new(true),
        denials: denials_tx,
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
    let items = local
        .item_client()
        .watch_serving_items_reactive(scope.clone(), GetAllLocalRecords {})
        .await?;
    policy.allowed.store(!denied_at_open, Ordering::SeqCst);
    let client = local.handler_connector().client();
    let query = client.follow_query_reactive(
        Some(node.node_id()),
        scope.clone(),
        &AllLocalRecordHandlers {},
    )?;
    let view = client.follow_view_reactive(&AllLocalRecordsView {})?;
    let report = client.follow_report_reactive(&CountAllLocalRecords {})?;
    let states = [
        ("items", items.live().map_value(|rows| !rows.is_empty())),
        (
            "query",
            query
                .live_collection()
                .as_subscription()
                .map_value(|rows| !rows.is_empty()),
        ),
        (
            "view",
            view.live_collection()
                .as_subscription()
                .map_value(|rows| !rows.is_empty()),
        ),
        (
            "report",
            report
                .live_subscription()
                .map_value(|count| count.count > 0),
        ),
    ];
    let (_guards, updates_rx) = subscribe_handler_states(&states);
    reach_denied_state(&states, &updates_rx, &policy, denied_at_open).await?;
    assert_denial_blocks_commands(&states, &local.command_client()).await;
    assert!(query.live_collection().rows().snapshot().is_empty());
    assert!(view.live_collection().rows().snapshot().is_empty());
    assert_eq!(report.live_subscription().current().value, None);
    assert_eq!(items.live().current().value, None);

    assert_denial_survives_authority_outage(&states, &policy, &attempts_rx).await?;

    commit_record(&node, scope, "newly-allowed-record")?;
    policy.allowed.store(true, Ordering::SeqCst);
    policy.available.store(true, Ordering::SeqCst);
    wait_for_handler_states(&states, &updates_rx, |state| {
        state.liveness == SubscriptionLiveness::Current && state.value == Some(true)
    })
    .await?;
    assert_eq!(query.live_collection().rows().snapshot().len(), 2);
    assert_eq!(view.live_collection().rows().snapshot().len(), 2);
    assert_eq!(items.live().current().value.map(|rows| rows.len()), Some(2));
    assert_eq!(
        report
            .live_subscription()
            .current()
            .value
            .map(|count| count.count),
        Some(2)
    );
    assert_eq!(probe.accepted(), 1);
    assert_eq!(probe.peak_active(), 1);
    server.shutdown().await?;
    Ok(())
}

async fn reach_denied_state(
    states: &[(&str, LiveSubscription<bool>)],
    updates: &flume::Receiver<()>,
    policy: &HandlerAuthorizationPolicy,
    denied_at_open: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !denied_at_open {
        wait_for_handler_states(states, updates, |state| {
            state.liveness == SubscriptionLiveness::Current && state.value == Some(true)
        })
        .await?;
        policy.available.store(false, Ordering::SeqCst);
        wait_for_handler_states(states, updates, |state| {
            matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. })
                && state.value == Some(true)
        })
        .await?;
    }
    policy.allowed.store(false, Ordering::SeqCst);
    policy.available.store(true, Ordering::SeqCst);
    wait_for_handler_states(states, updates, |state| {
        matches!(
            state.liveness,
            SubscriptionLiveness::AuthorizationBlocked { .. }
        )
    })
    .await
}

fn subscribe_handler_states(
    states: &[(&str, LiveSubscription<bool>)],
) -> (Vec<hyphae::SubscriptionGuard>, flume::Receiver<()>) {
    let (updates_tx, updates_rx) = flume::unbounded();
    let guards = states
        .iter()
        .map(|(_, live)| {
            let updates = updates_tx.clone();
            live.state().subscribe(move |_| {
                let _ignored = updates.send(());
            })
        })
        .collect();
    (guards, updates_rx)
}

async fn assert_denial_blocks_commands(
    states: &[(&str, LiveSubscription<bool>)],
    client: &LocalCommandClient,
) {
    for (name, state) in states {
        assert_eq!(
            state.current().value,
            None,
            "{name} retained protected output after denial"
        );
        let built = AtomicBool::new(false);
        let result = client
            .submit_from(state, |value| {
                built.store(true, Ordering::SeqCst);
                SetLocalRecord {
                    id: LocalRecordId::from("must-not-be-submitted"),
                    value: value.to_string(),
                }
            })
            .await;
        assert!(matches!(
            result,
            Err(LocalPeerError::Node(
                NodeError::CommandDependencyNotCurrent(
                    SubscriptionLiveness::AuthorizationBlocked { .. }
                )
            ))
        ));
        assert!(!built.load(Ordering::SeqCst));
    }
}

async fn assert_denial_survives_authority_outage(
    states: &[(&str, LiveSubscription<bool>)],
    policy: &HandlerAuthorizationPolicy,
    attempts: &flume::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    while attempts.try_recv().is_ok() {}
    policy.available.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), attempts.recv_async()).await??;
    for (name, state) in states {
        let current = state.current();
        assert!(
            matches!(
                current.liveness,
                SubscriptionLiveness::AuthorizationBlocked { .. }
            ),
            "{name} lost its denial during an authority outage"
        );
        assert_eq!(current.value, None);
        assert_eq!(current.through, None);
    }
    Ok(())
}

async fn wait_for_handler_states(
    states: &[(&str, LiveSubscription<bool>)],
    updates: &flume::Receiver<()>,
    ready: impl Fn(&LiveSubscriptionState<bool>) -> bool + Send + Sync,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !states.iter().all(|(_, live)| ready(&live.current())) {
            updates.recv_async().await?;
        }
        Ok::<_, flume::RecvError>(())
    })
    .await??;
    Ok(())
}
