use std::time::Duration as StdDuration;

use myko_authority::certified::PreparedAuthorityRuntime;
use myko_federation::{CommandSnapshot, CommandState, NodeError, TypedCommandAdmission};

use super::*;

pub fn saved_effect(
    node: &Node,
    command_id: CommandId,
) -> Result<Box<PreparedCommandEffect>, Box<dyn Error>> {
    match node.command(command_id)?.ok_or("missing command")?.state {
        CommandState::AuthorizationPrepared { effect } => Ok(effect),
        state => Err(format!("expected prepared command, got {state:?}").into()),
    }
}

pub fn assert_exact_commit(
    node: &Node,
    command_id: CommandId,
    saved: &PreparedCommandEffect,
) -> TestResult {
    let commits = node
        .events_after(None)?
        .into_iter()
        .filter_map(|event| match event.event {
            NodeEvent::CommandCommitted { command, batch } if command.request.id == command_id => {
                Some((batch, command.result))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if commits != vec![(saved.batch().clone(), Some(saved.result().to_vec()))] {
        return Err("runtime did not release the exact saved batch and result once".into());
    }
    Ok(())
}

async fn run_until_result(
    runtime: PreparedAuthorityRuntime,
) -> Result<CommandSnapshot, Box<dyn Error>> {
    let (tx, rx) = flume::unbounded();
    let worker = tokio::spawn(runtime.run(move |result| {
        let _ = tx.send(result);
    }));
    let result = tokio::time::timeout(StdDuration::from_secs(30), rx.recv_async()).await;
    worker.abort();
    if !worker.await.is_err_and(|error| error.is_cancelled()) {
        return Err("runtime did not stop on cancellation".into());
    }
    Ok(result???)
}

#[tokio::test]
async fn prepared_runtime_resumes_the_real_commit_boundary_after_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("runtime-a.redb");
    let b_path = directory.path().join("runtime-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (grant_head, reader, scope) = install_grant(&a, &b)?;
    let (runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    a.set_command_access_policy(policy.clone())?;
    let before = a.local_history_cut()?;
    let command_id = CommandId::new();
    a.submit(command_request(reader, scope, command_id))?;
    let TypedCommandAdmission::Execute(context) = a.begin_command(command_id)? else {
        return Err("new command did not enter its handler".into());
    };
    if !matches!(
        context.commit_bytes(b"actual handler result".to_vec()),
        Err(NodeError::AuthorityUnavailable(_))
    ) {
        return Err("effect policy did not defer the real command commit boundary".into());
    }
    let saved = saved_effect(&a, command_id)?;
    let request = a.prepared_command_access(command_id)?;
    for _ in 0..100 {
        if policy.decide(&request) != Err(AuthorityUnavailable::CoordinationUnavailable) {
            return Err("repeated dispatch did not coalesce pending wakeups".into());
        }
    }
    for event in a.events_after(before)? {
        b.ingest(event)?;
    }
    let consumed = coordinator(&a, &b)?
        .decide(
            grant_head,
            11,
            CommandId::new(),
            command_id,
            AuthorityRequestSource::new(a.clone()).prepared_command_request(command_id)?,
        )
        .await?;
    if !consumed.decision().is_permit() {
        return Err("effect was not consumed before the simulated restart".into());
    }
    drop(runtime);
    if policy.decide(&request) != Err(AuthorityUnavailable::PolicyUnavailable) {
        return Err("stopped worker left an apparently usable effect policy".into());
    }
    drop(policy);
    drop(a);
    drop(b);

    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    a.set_command_access_policy(policy.clone())?;
    let result = run_until_result(runtime).await?;
    if result.request.id != command_id || !result.state.is_committed() {
        return Err("startup did not recover the prepared command".into());
    }
    assert_exact_commit(&a, command_id, &saved)?;
    if coordinator(&a, &b)?
        .release_prepared(command_id)
        .await
        .is_ok()
    {
        return Err("already released command was treated as a new effect".into());
    }
    assert_exact_commit(&a, command_id, &saved)?;
    Ok(())
}

#[tokio::test]
async fn prepared_runtime_rechecks_a_consumed_effect_after_revocation_and_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("revoked-a.redb");
    let b_path = directory.path().join("revoked-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let command_id = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, command_id)?;
    let original = coordinator(&a, &b)?
        .decide(head, 2, CommandId::new(), command_id, request)
        .await?;
    if !original.decision().is_permit() {
        return Err("initial consumption was denied".into());
    }
    certify_grant_revocation(&a, &b, original.head())?;
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let result = coordinator(&a, &b)?.release_prepared(command_id).await?;
    if !matches!(result.state, CommandState::Rejected { .. }) {
        return Err("runtime released a historical permit despite certified revocation".into());
    }
    if a.events_after(None)?.iter().any(|event| {
        matches!(&event.event,
        NodeEvent::CommandCommitted { command, .. } if command.request.id == command_id)
    }) {
        return Err("revoked effect appended a commit".into());
    }
    Ok(())
}

#[tokio::test]
async fn prepared_runtime_recovers_its_ballot_after_quorum_outage() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("outage-a.redb");
    let b_path = directory.path().join("outage-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let [a_key, b_key, _] = keys3();
    let (_, reader, scope) = install_grant_with_anchor(&a, &b, &anchor3()?, &a_key, &b_key)?;
    let command_id = CommandId::new();
    prepare_command_evidence(&a, &b, reader, scope, command_id)?;
    let saved = saved_effect(&a, command_id)?;
    if coordinator_with_unavailable_majority(&a)?
        .release_prepared(command_id)
        .await
        .is_ok()
    {
        return Err("minority released a prepared effect".into());
    }
    if saved_effect(&a, command_id)? != saved {
        return Err("quorum outage changed or rejected the prepared effect".into());
    }
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let restored = coordinator_with_unavailable_minority(&a, &b, Arc::new(UnavailableEvidence))?;
    let result = restored.release_prepared(command_id).await?;
    if !result.state.is_committed() {
        return Err("restored quorum did not release the saved effect".into());
    }
    assert_exact_commit(&a, command_id, &saved)?;
    if coordinator(&b, &a)?
        .release_prepared(command_id)
        .await
        .is_ok()
    {
        return Err("observer executed a foreign command".into());
    }
    Ok(())
}

#[tokio::test]
async fn prepared_runtime_releases_through_native_control_and_evidence_transfer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("native-runtime-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("native-runtime-b.redb"))?;
    let (_, reader, scope) = install_grant(&a, &b)?;
    let command_id = CommandId::new();
    prepare_command_evidence_at(&a, reader, scope.clone(), command_id)?;
    let saved = saved_effect(&a, command_id)?;
    if b.command(command_id)?.is_some() {
        return Err("native receiver already had command evidence before transport".into());
    }
    let harness =
        NativeControlHarness::start(a.clone(), b.clone(), authority_realm_scope(&realm()), scope)
            .await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let (runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator, Arc::new(AllowAllAccessPolicy));
    a.set_command_access_policy(policy.clone())?;
    let result = run_until_result(runtime).await?;
    if result.request.id != command_id || !result.state.is_committed() {
        return Err("native runtime did not release the command".into());
    }
    assert_exact_commit(&a, command_id, &saved)?;
    if saved_effect(&b, command_id)? != saved {
        return Err("native controller did not fetch the exact prepared evidence".into());
    }
    harness.shutdown().await?;
    Ok(())
}
