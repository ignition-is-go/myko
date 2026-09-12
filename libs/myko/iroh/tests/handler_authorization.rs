use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use hyphae::Watchable as _;
use myko_federation::{
    AuthorizationBlock, AuthorizationDecision, CommandClient, LiveCollectionHandle as _,
    LiveSubscription, LiveSubscriptionHandle as _, LiveSubscriptionState, Node, NodeError,
    ReconnectPolicy, ScopeId, SubscriptionLiveness,
};
use myko_iroh::{IrohCommandClient, IrohReplicationError, IrohReplicator};

#[path = "handler_authorization/support.rs"]
mod support;
use support::*;

const TIMEOUT: Duration = Duration::from_secs(5);
type States = [(&'static str, LiveSubscription<usize>); 4];

#[tokio::test]
async fn native_revocation_clears_outputs_and_regrant_recovers_same_handles() -> TestResult {
    check_recovery(false).await
}

#[tokio::test]
async fn native_initial_handler_denial_recovers_same_handles() -> TestResult {
    check_recovery(true).await
}

#[tokio::test]
async fn native_initial_item_denial_returns_explicit_error() -> TestResult {
    let (policy, _unavailable) = ReadPolicy::new();
    policy.allowed.store(false, Ordering::SeqCst);
    let server = IrohReplicator::bind_loopback_application_with_policy(
        application(Node::in_memory())?,
        policy,
    )
    .await?;
    let remote = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let opened = tokio::time::timeout(
        TIMEOUT,
        remote
            .item_client(server.address())
            .watch_serving_items_reactive(ScopeId::new(SCOPE), GetAllRecords {}),
    )
    .await?;
    if !matches!(opened, Err(IrohReplicationError::Authorization { decision, message })
        if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
            && message.contains("native read grant revoked"))
    {
        return Err("initial item denial did not return its typed decision".into());
    }
    remote.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

async fn check_recovery(denied_at_open: bool) -> TestResult {
    let node = Node::in_memory();
    commit_record(&node, "protected")?;
    let (policy, unavailable) = ReadPolicy::new();
    let server = IrohReplicator::bind_loopback_application_with_policy(
        application(node.clone())?,
        policy.clone(),
    )
    .await?;
    let remote = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let retry = ReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(20))?;
    let items = tokio::time::timeout(
        TIMEOUT,
        remote
            .item_client(server.address())
            .with_reconnect_policy(retry)
            .watch_serving_items_reactive(ScopeId::new(SCOPE), GetAllRecords {}),
    )
    .await??;
    policy.allowed.store(!denied_at_open, Ordering::SeqCst);
    let client = remote
        .handler_connector(server.address())
        .with_reconnect_policy(retry)
        .client();
    let query = client.follow_query_reactive(
        Some(node.node_id()),
        ScopeId::new(SCOPE),
        &RecordsQuery {},
    )?;
    let report = client.follow_report_reactive(&CountAllRecords {})?;
    let view = client.follow_view_reactive(&RecordsView {})?;
    let states = [
        ("items", items.live().map_value(Vec::len)),
        (
            "query",
            query
                .live_collection()
                .as_subscription()
                .map_value(Vec::len),
        ),
        (
            "report",
            report.live_subscription().map_value(|count| count.count),
        ),
        (
            "view",
            view.live_collection().as_subscription().map_value(Vec::len),
        ),
    ];
    let (_guards, updates) = observe(&states);
    if !denied_at_open {
        check_outage_recovery(&states, &updates, &policy).await?;
    }
    policy.allowed.store(false, Ordering::SeqCst);
    policy.available.store(true, Ordering::SeqCst);
    wait_for(&states, &updates, is_denied).await?;
    assert_blocked_commands(&states, &remote.command_client(server.address())).await;
    assert!(query.live_collection().rows().snapshot().is_empty());
    assert!(view.live_collection().rows().snapshot().is_empty());
    assert_eq!(report.live_subscription().current().value, None);
    assert_eq!(items.live().current().value, None);
    while unavailable.try_recv().is_ok() {}
    policy.available.store(false, Ordering::SeqCst);
    tokio::time::timeout(TIMEOUT, unavailable.recv_async()).await??;
    for (name, live) in &states {
        assert!(
            is_denied(&live.current()),
            "{name} lost its denial on outage"
        );
    }
    commit_record(&node, "added-while-denied")?;
    policy.allowed.store(true, Ordering::SeqCst);
    policy.available.store(true, Ordering::SeqCst);
    wait_for(&states, &updates, |state| is_current_count(state, 2)).await?;
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
    drop((items, query, report, view));
    remote.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

async fn check_outage_recovery(
    states: &States,
    updates: &flume::Receiver<()>,
    policy: &ReadPolicy,
) -> TestResult {
    wait_for(states, updates, |state| is_current_count(state, 1)).await?;
    policy.available.store(false, Ordering::SeqCst);
    wait_for(states, updates, |state| {
        matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. })
            && state.value == Some(1)
    })
    .await?;
    policy.available.store(true, Ordering::SeqCst);
    wait_for(states, updates, |state| is_current_count(state, 1)).await
}

fn observe(states: &States) -> (Vec<hyphae::SubscriptionGuard>, flume::Receiver<()>) {
    let (updates, receiver) = flume::unbounded();
    let guards = states
        .iter()
        .map(|(_, live)| {
            let updates = updates.clone();
            live.state().subscribe(move |_| {
                let _ignored = updates.send(());
            })
        })
        .collect();
    (guards, receiver)
}

async fn wait_for(
    states: &States,
    updates: &flume::Receiver<()>,
    predicate: impl Fn(&LiveSubscriptionState<usize>) -> bool + Send + Sync,
) -> TestResult {
    let result = tokio::time::timeout(TIMEOUT, async {
        while !states.iter().all(|(_, live)| predicate(&live.current())) {
            updates.recv_async().await?;
        }
        Ok::<_, flume::RecvError>(())
    })
    .await;
    if result.is_err() {
        for (name, live) in states {
            eprintln!("{name}: {:?}", live.current());
        }
    }
    result??;
    Ok(())
}

fn is_current_count(state: &LiveSubscriptionState<usize>, count: usize) -> bool {
    state.liveness == SubscriptionLiveness::Current && state.value == Some(count)
}

const fn is_denied(state: &LiveSubscriptionState<usize>) -> bool {
    matches!(
        &state.liveness,
        SubscriptionLiveness::AuthorizationBlocked {
            block: AuthorizationBlock::Denied(_)
        }
    ) && state.value.is_none()
        && state.through.is_none()
}

async fn assert_blocked_commands(states: &States, client: &IrohCommandClient) {
    for (name, live) in states {
        let built = AtomicBool::new(false);
        let result = client
            .submit_from(live, |count| {
                built.store(true, Ordering::SeqCst);
                SetRecord {
                    id: RecordId::from("must-not-be-submitted"),
                    value: count.to_string(),
                }
            })
            .await;
        assert!(
            matches!(
                result,
                Err(IrohReplicationError::Ingest(
                    NodeError::CommandDependencyNotCurrent(
                        SubscriptionLiveness::AuthorizationBlocked { .. }
                    )
                ))
            ),
            "{name} did not reject a denied dependency: {result:?}"
        );
        assert!(!built.load(Ordering::SeqCst));
    }
}
