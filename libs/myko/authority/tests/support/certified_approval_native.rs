use super::*;

async fn reported_command(
    reports: &flume::Receiver<Result<CommandSnapshot, String>>,
) -> Result<CommandSnapshot, Box<dyn Error>> {
    Ok(tokio::time::timeout(StdDuration::from_mins(1), reports.recv_async()).await???)
}

fn pending_id(command: &CommandSnapshot) -> Result<ChallengeId, Box<dyn Error>> {
    match &command.state {
        CommandState::AuthorizationPending { challenge_id, .. } => Ok(challenge_id.clone()),
        state => Err(format!("expected pending approval, got {state:?}").into()),
    }
}

#[tokio::test]
async fn native_client_approves_each_round_with_native_controller_certification() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let client_transport = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let approver = Principal::node(endpoint_principal_id(client_transport.address().id));
    let [a_key, b_key] = keys();
    let (_, reader, scope) = install_obligated_grant(
        &a,
        &b,
        &anchor()?,
        &a_key,
        &b_key,
        ScopeId::new("native:multiple"),
        obligations(&approver, &["first", "second"]),
    )?;
    let command_id = CommandId::new();
    prepare_command_evidence_at(&a, reader, scope.clone(), command_id)?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    require(
        b.command(command_id)?.is_none(),
        "controller already had command before transport",
    )?;
    let harness = NativeControlHarness::start(
        a.clone(),
        b.clone(),
        authority_realm_scope(&realm()),
        scope.clone(),
    )
    .await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let non_effect = Arc::new(ScopedHistoryPolicy::new(
        endpoint_principal_id(harness.b_transport.address().id),
        vec![authority_realm_scope(&realm()), scope],
    ));
    let (runtime, policy) = PreparedAuthorityRuntime::new(coordinator, non_effect);
    a.set_command_access_policy(policy.clone())?;
    harness.a_transport.set_access_policy(policy)?;
    let client = client_transport.command_client(harness.a_transport.address());
    let (reported, reports) = flume::unbounded();
    let worker = tokio::spawn(runtime.run(move |result| {
        let _ = reported.send(result);
    }));
    let outcome = async {
        let first = pending_id(&reported_command(&reports).await?)?;
        let approval = client.approve_authority(first.clone(), true).await?;
        require(
            approval.approver == approver && approval.binding.command_id == Some(command_id),
            "native approval lost authenticated principal or command binding",
        )?;
        let pending = reported_command(&reports).await?;
        let second = next_challenge(&pending, &first, &approval.id)?;
        require(
            client.approve_authority(first, true).await? == approval,
            "native retry changed certified approval",
        )?;
        let final_approval = client.approve_authority(second, true).await?;
        require(
            final_approval.approver == approver && final_approval.binding == approval.binding,
            "second approval changed identity or exact effect binding",
        )?;
        let committed = reported_command(&reports).await?;
        require(
            committed.state.is_committed(),
            "native approvals did not release effect",
        )?;
        prepared_runtime::assert_exact_commit(&a, command_id, &saved)?;
        require(
            b.command(command_id)?.is_some(),
            "native controller never fetched command evidence",
        )?;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    worker.abort();
    let _ = worker.await;
    client_transport.shutdown().await?;
    harness.shutdown().await?;
    outcome
}
