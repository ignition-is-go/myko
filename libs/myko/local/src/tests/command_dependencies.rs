use super::*;

fn dependency(
    value: Option<&str>,
    liveness: SubscriptionLiveness,
) -> LiveSubscriptionState<String> {
    LiveSubscriptionState {
        value: value.map(str::to_owned),
        through: None,
        liveness,
    }
}

fn command(value: String) -> SetLocalRecord {
    SetLocalRecord {
        id: LocalRecordId::from("from-dependency"),
        value,
    }
}

#[tokio::test]
async fn non_current_dependency_never_builds_or_submits_a_command()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node = Node::in_memory();
    let socket = directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let local = LocalClientSession::new(&socket);
    local.node_client().identify().await?;
    let client = local.command_client();
    for liveness in [
        SubscriptionLiveness::Connecting,
        SubscriptionLiveness::Resynchronizing {
            reason: "catching up".to_owned(),
        },
        SubscriptionLiveness::Invalid {
            reason: "owner dropped".to_owned(),
        },
    ] {
        let (_writer, source) = live_subscription(dependency(Some("stale"), liveness.clone()));
        let built = std::sync::atomic::AtomicBool::new(false);
        let result = client
            .submit_from(&source, |value| {
                built.store(true, Ordering::SeqCst);
                command(value)
            })
            .await;
        assert!(
            matches!(result, Err(LocalPeerError::Node(NodeError::CommandDependencyNotCurrent(state))) if state == liveness)
        );
        assert!(!built.load(Ordering::SeqCst));
        assert!(node.events_after(None)?.is_empty());
    }
    let (_writer, empty) = live_subscription(dependency(None, SubscriptionLiveness::Current));
    let result = client.submit_from(&empty, command).await;
    assert!(matches!(
        result,
        Err(LocalPeerError::Node(
            NodeError::CommandDependencyMissingValue
        ))
    ));
    assert!(node.events_after(None)?.is_empty());
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn deferred_command_rechecks_dependency_and_never_waits_for_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node = Node::in_memory();
    let socket = directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalCommandClient::new(&socket);
    let (writer, source) =
        live_subscription(dependency(Some("before"), SubscriptionLiveness::Current));
    let pending = client.submit_from(&source, command);
    writer.resynchronizing("lost serving node");
    assert!(matches!(
        pending.await,
        Err(LocalPeerError::Node(
            NodeError::CommandDependencyNotCurrent(_)
        ))
    ));

    let invoked_while_stale = client.submit_from(&source, command);
    writer.publish("recovered".to_owned(), None);
    assert!(matches!(
        invoked_while_stale.await,
        Err(LocalPeerError::Node(
            NodeError::CommandDependencyNotCurrent(_)
        ))
    ));
    assert!(node.events_after(None)?.is_empty());

    let ready_invocation = client.submit_from(&source, command);
    writer.publish("newer current value".to_owned(), None);
    let accepted = ready_invocation.await?;
    let accepted = accepted.command.ok_or("command was not accepted")?;
    let decoded: SetLocalRecord = serde_json::from_slice(&accepted.request.payload)?;
    assert_eq!(decoded.value, "newer current value");
    assert_eq!(node.events_after(None)?.len(), 1);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn command_builder_cannot_bypass_a_newly_stale_dependency()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node = Node::in_memory();
    let socket = directory.path().join("myko.sock");
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let client = LocalCommandClient::new(&socket);
    let (writer, source) =
        live_subscription(dependency(Some("current"), SubscriptionLiveness::Current));
    let result = client
        .submit_from(&source, |value| {
            writer.resynchronizing("changed while building");
            command(value)
        })
        .await;
    assert!(matches!(
        result,
        Err(LocalPeerError::Node(
            NodeError::CommandDependencyNotCurrent(_)
        ))
    ));
    assert!(node.events_after(None)?.is_empty());
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn authority_outage_blocks_composed_dependency_on_a_healthy_socket()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    commit_record(&node, scope.clone(), "record-1")?;
    let socket = directory.path().join("myko.sock");
    let (attempts, _observed) = flume::unbounded();
    let policy = Arc::new(RecoverableAuthorityPolicy {
        available: std::sync::atomic::AtomicBool::new(true),
        unavailable_attempts: attempts,
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
    let query = local.handler_connector().client().follow_query_reactive(
        Some(node.node_id()),
        scope,
        &AllLocalRecordHandlers {},
    )?;
    let rows = query.live_collection().as_subscription();
    let (_configuration_writer, configuration) =
        live_subscription(dependency(Some("suffix"), SubscriptionLiveness::Current));
    let inputs = rows.join_frontiers(&configuration);
    let client = local.command_client();
    wait_for_current(&inputs, true).await?;
    policy.available.store(false, Ordering::SeqCst);
    wait_for_current(&inputs, false).await?;
    assert_eq!(local.node_client().identify().await?, node.node_id());
    let before = node.events_after(None)?;
    let failed = client
        .submit_from(&inputs, |(rows, suffix)| {
            command(format!("{} {suffix}", rows.len()))
        })
        .await;
    assert!(matches!(
        failed,
        Err(LocalPeerError::Node(
            NodeError::CommandDependencyNotCurrent(_)
        ))
    ));
    assert_eq!(node.events_after(None)?, before);

    policy.available.store(true, Ordering::SeqCst);
    wait_for_current(&inputs, true).await?;
    assert_eq!(node.events_after(None)?, before);
    let accepted = client
        .submit_from(&inputs, |(rows, suffix)| {
            command(format!("{} {suffix}", rows.len()))
        })
        .await?;
    let accepted = accepted
        .command
        .ok_or("dependent command was not accepted")?;
    let decoded: SetLocalRecord = serde_json::from_slice(&accepted.request.payload)?;
    assert_eq!(decoded.value, "1 suffix");
    assert_eq!(probe.accepted(), 1);
    server.shutdown().await?;
    Ok(())
}

async fn wait_for_current<T, Cursor>(
    source: &LiveSubscription<T, Cursor>,
    current: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: hyphae::CellValue,
    Cursor: hyphae::CellValue,
{
    let mut publications = source.watch_publications();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = publications.recv_async().await?.state;
            if (state.liveness == SubscriptionLiveness::Current) == current {
                return Ok::<_, flume::RecvError>(());
            }
        }
    })
    .await??;
    Ok(())
}
