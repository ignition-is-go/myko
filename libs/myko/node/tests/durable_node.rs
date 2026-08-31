use std::{sync::Arc, time::Duration};

use myko_federation::{
    AccessPolicy, AccessRequest, BatchId, ChangeBatch, CommandId, CommandRequest, Node, NodeId,
    PrincipalId, ScopeId, ServiceId, SubscriptionLiveness,
};
use myko_iroh::IrohReplicator;
use myko_items::{ItemMutation, ItemProjection, ItemQuery, myko_item};
use myko_node::{DurableIrohNode, NativeNodeDescriptor};

#[myko_item(service = "myko.node.reactive", scope_root)]
pub struct ReactiveRecord {
    value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AllReactiveRecords;

impl ItemQuery for AllReactiveRecords {
    type Item = ReactiveRecord;
    type Output = Vec<ReactiveRecord>;
    const QUERY_ID: &'static str = "durable_node.all_reactive_records";

    fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output {
        projection.values().cloned().collect()
    }
}

#[derive(Debug)]
struct DenyAllPolicy;

impl AccessPolicy for DenyAllPolicy {
    fn authorize(&self, _request: &AccessRequest) -> Result<(), String> {
        Err("test policy denies native access".to_owned())
    }
}

fn commit_test_command(node: &Node, command_type: &str) -> Result<CommandId, String> {
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

async fn wait_for_committed(node: &Node, command_id: CommandId) -> Result<(), String> {
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

#[tokio::test]
async fn typed_item_watch_drives_a_hyphae_cell_without_polling() -> Result<(), String> {
    use hyphae::{Signal, Watchable as _};

    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let node = DurableIrohNode::open_loopback(directory.path(), Duration::from_millis(20))
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
        service_id: ServiceId::new("myko.node.reactive"),
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
    let initial = DurableIrohNode::open_loopback(directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let _command = commit_test_command(initial.node(), "policy-window")?;
    initial
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;

    let reopened =
        DurableIrohNode::open_loopback_with_policy(directory.path(), retry_interval, |_| {
            Ok(Arc::new(DenyAllPolicy))
        })
        .await
        .map_err(|error| error.to_string())?;
    let client = IrohReplicator::bind_loopback(Node::in_memory())
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
async fn confirmed_pairing_durably_installs_mutually_pinned_followers() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = DurableIrohNode::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let target = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let mut inbound = source.subscribe_pairing_receipts();
    let invitation = source
        .issue_pairing_invitation(Duration::from_mins(1))
        .map_err(|error| error.to_string())?;
    let outbound = target
        .redeem_pairing(&invitation)
        .await
        .map_err(|error| error.to_string())?;
    let receipts = tokio::time::timeout(Duration::from_secs(5), inbound.recv())
        .await
        .map_err(|_| "source did not observe the pairing receipt".to_owned())?
        .map_err(|error| error.to_string())?;
    let [received] = receipts.as_slice() else {
        return Err(format!("source observed wrong receipts: {receipts:?}"));
    };
    if received != &outbound {
        return Err("pairing endpoints derived different receipts".to_owned());
    }
    if target.confirm_pairing(&outbound, "000000").await.is_ok() {
        return Err("target accepted the wrong comparison code".to_owned());
    }
    let _target_replaced = target
        .confirm_pairing(&outbound, &outbound.comparison_code)
        .await
        .map_err(|error| error.to_string())?;
    let _source_replaced = source
        .confirm_pairing(received, &received.comparison_code)
        .await
        .map_err(|error| error.to_string())?;
    let source_descriptor = source.descriptor();
    if target
        .configured_peer_bindings()
        .map_err(|error| error.to_string())?
        .first()
        .and_then(|peer| peer.source_node)
        != Some(source_descriptor.node_id)
    {
        return Err("confirmed target did not persist a pinned source".to_owned());
    }

    target.shutdown().await.map_err(|error| error.to_string())?;
    let reopened = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    if reopened
        .configured_peer_bindings()
        .map_err(|error| error.to_string())?
        .first()
        .and_then(|peer| peer.source_node)
        != Some(source_descriptor.node_id)
    {
        return Err("confirmed pairing did not survive target restart".to_owned());
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
    let source = DurableIrohNode::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let source_descriptor = source.descriptor();
    let source_address = source_descriptor.endpoint.clone();
    let first_command = commit_test_command(source.node(), "before-restart")?;

    let target = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let first_transport_id = target.address().id;
    let first_node_id = target.node().node_id();
    if target
        .upsert_peer_descriptor(source_descriptor.clone())
        .await
        .map_err(|error| error.to_string())?
    {
        return Err("new peer unexpectedly replaced an existing configuration".to_owned());
    }
    wait_for_committed(target.node(), first_command).await?;
    target.shutdown().await.map_err(|error| error.to_string())?;

    let second_command = commit_test_command(source.node(), "while-target-offline")?;
    let reopened = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    if reopened.address().id != first_transport_id || reopened.node().node_id() != first_node_id {
        return Err("durable node identity changed across restart".to_owned());
    }
    if reopened
        .configured_peers()
        .map_err(|error| error.to_string())?
        != vec![source_address.clone()]
        || reopened
            .configured_peer_bindings()
            .map_err(|error| error.to_string())?
            .first()
            .and_then(|peer| peer.source_node)
            != Some(source_descriptor.node_id)
        || reopened
            .peer_statuses()
            .map_err(|error| error.to_string())?
            .len()
            != 1
    {
        return Err("configured follower was not restored".to_owned());
    }
    wait_for_committed(reopened.node(), second_command).await?;

    if !reopened
        .remove_peer(source_address.id)
        .await
        .map_err(|error| error.to_string())?
    {
        return Err("configured peer was not removed".to_owned());
    }
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    let after_removal = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    if !after_removal
        .configured_peers()
        .map_err(|error| error.to_string())?
        .is_empty()
        || !after_removal
            .peer_statuses()
            .map_err(|error| error.to_string())?
            .is_empty()
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
    let source = DurableIrohNode::open_loopback(source_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let command_id = commit_test_command(source.node(), "must-not-replicate")?;
    let source_address = source.address();
    let expected_source = NodeId::new();
    let target = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    target
        .upsert_peer_descriptor(NativeNodeDescriptor::new(
            expected_source,
            source_address.clone(),
        ))
        .await
        .map_err(|error| error.to_string())?;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let statuses = target.peer_statuses().map_err(|error| error.to_string())?;
            if statuses.first().is_some_and(|status| {
                status.expected_source_node == Some(expected_source)
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

    target
        .remove_peer(source_address.id)
        .await
        .map_err(|error| error.to_string())?;
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn legacy_endpoint_only_peer_files_remain_loadable() -> Result<(), String> {
    let source_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let retry_interval = Duration::from_millis(20);
    let source = DurableIrohNode::open_loopback(source_directory.path(), retry_interval)
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

    let target = DurableIrohNode::open_loopback(target_directory.path(), retry_interval)
        .await
        .map_err(|error| error.to_string())?;
    let bindings = target
        .configured_peer_bindings()
        .map_err(|error| error.to_string())?;
    if !matches!(
        bindings.as_slice(),
        [binding] if binding.endpoint == source_address && binding.source_node.is_none()
    ) {
        return Err("legacy peer configuration did not load as an unpinned binding".to_owned());
    }
    target.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}
