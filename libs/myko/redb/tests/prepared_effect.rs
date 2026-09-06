use std::{error::Error, sync::Arc};

use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, ApprovalId, AuthorityPresentation, BatchId, ChallengeId,
    ChangeBatch, CommandAdmission, CommandId, CommandRequest, CommandSnapshot, CommandState,
    EventEnvelope, EventId, EventJournal, LogPosition, Node, NodeError, NodeEvent,
    PreparedCommandEffect, PrincipalId, ResourceClaim, ResourceClaimKind, ScopeId, ScopeTopology,
    ServiceId, TypedCommandAdmission,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;
type OpenedNode = (Node, Arc<RedbJournal>, Arc<dyn AccessPolicy>);

fn open(path: &std::path::Path) -> Result<OpenedNode, NodeError> {
    let (node, journal) = RedbJournal::open_node_with_journal(path)?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    Ok((node, journal, policy))
}

fn request(node: &Node, command_id: CommandId) -> CommandRequest {
    let scope_id = ScopeId::new("prepared-effect:test");
    let principal_id = PrincipalId::for_node(node.node_id());
    CommandRequest {
        id: command_id,
        service_id: ServiceId::new("prepared-effect-service"),
        scope_id: scope_id.clone(),
        principal_id: principal_id.clone(),
        authority: AuthorityPresentation::direct_node(principal_id),
        resource_claims: vec![ResourceClaim::scope(scope_id, ResourceClaimKind::Primary)],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "prepared-effect-command".to_owned(),
        payload: b"freeze-before-authorize".to_vec(),
    }
}

fn executing(node: &Node, command_id: CommandId) -> Result<CommandSnapshot, Box<dyn Error>> {
    node.submit(request(node, command_id))?;
    match node.claim(command_id)? {
        CommandAdmission::Execute(snapshot) => Ok(snapshot),
        CommandAdmission::Resume(snapshot) => {
            Err(format!("expected execute, found {snapshot:?}").into())
        }
    }
}

fn effect(snapshot: &CommandSnapshot, result: &[u8]) -> Result<PreparedCommandEffect, NodeError> {
    let batch = ChangeBatch {
        id: BatchId::new(),
        command_id: snapshot.request.id,
        service_id: snapshot.request.service_id.clone(),
        scope_id: snapshot.request.scope_id.clone(),
        causal_parents: vec![snapshot.updated_at],
        changes: Vec::new(),
    };
    PreparedCommandEffect::new(
        snapshot.updated_at,
        batch,
        result.to_vec(),
        snapshot.request.resource_claims.clone(),
        snapshot.request.application_capabilities.clone(),
        ScopeTopology::default(),
    )
}

fn committed_batch(
    journal: &RedbJournal,
    command_id: CommandId,
) -> Result<Option<(CommandSnapshot, ChangeBatch)>, NodeError> {
    Ok(journal
        .replay()?
        .into_iter()
        .find_map(|event| match event.event {
            NodeEvent::CommandCommitted { command, batch } if command.request.id == command_id => {
                Some((command, batch))
            }
            _ => None,
        }))
}

fn prepared_effect_from(command: &CommandSnapshot) -> Result<&PreparedCommandEffect, String> {
    let CommandState::AuthorizationPrepared { effect } = &command.state else {
        return Err(format!(
            "expected authorization_prepared, found {:?}",
            command.state
        ));
    };
    Ok(effect)
}

fn pending_batch_result_from(command: &CommandSnapshot) -> Result<(&ChangeBatch, &[u8]), String> {
    let CommandState::AuthorizationPending { batch, result, .. } = &command.state else {
        return Err(format!(
            "expected authorization_pending, found {:?}",
            command.state
        ));
    };
    Ok((batch, result))
}

fn prepared_event_count(journal: &RedbJournal) -> Result<usize, NodeError> {
    Ok(journal
        .replay()?
        .into_iter()
        .filter(|event| {
            matches!(
                event.event,
                NodeEvent::CommandLifecycle(CommandSnapshot {
                    state: CommandState::AuthorizationPrepared { .. },
                    ..
                })
            )
        })
        .count())
}

#[test]
fn prepared_effect_survives_reopen_and_begin_commits_without_rerun() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("prepared.redb");
    let (node, journal, policy) = open(&path)?;
    let command_id = CommandId::new();
    let execution = executing(&node, command_id)?;
    let prepared_effect = effect(&execution, b"first durable result")?;
    let prepared = node.prepare_authorization(command_id, prepared_effect.clone())?;
    if prepared_effect_from(&prepared)? != &prepared_effect {
        return Err("prepare did not retain the exact frozen effect".into());
    }
    let before = journal.replay()?;
    if before.len() != 3 {
        return Err(format!(
            "expected submitted, executing, prepared; found {}",
            before.len()
        )
        .into());
    }
    drop(policy);
    drop(node);
    drop(journal);

    let (reopened, journal, _policy) = open(&path)?;
    let recovered = match reopened.begin_command(command_id)? {
        TypedCommandAdmission::Resume(snapshot) => snapshot,
        TypedCommandAdmission::Execute(_) => {
            return Err("prepared effect was released for handler execution".into());
        }
    };
    let CommandState::CommittedLocally { batch_id, .. } = recovered.state else {
        return Err(format!(
            "prepared effect did not commit on begin, found {:?}",
            recovered.state
        )
        .into());
    };
    let Some((committed, batch)) = committed_batch(&journal, command_id)? else {
        return Err("committed event missing after prepared resume".into());
    };
    if batch_id != prepared_effect.batch().id
        || &batch != prepared_effect.batch()
        || committed.result.as_deref() != Some(prepared_effect.result())
    {
        return Err("resume committed a body different from the prepared effect".into());
    }
    let after_commit = journal.replay()?;
    let same_prepare = reopened.prepare_authorization(command_id, prepared_effect.clone())?;
    if same_prepare != recovered || journal.replay()? != after_commit {
        return Err("same prepared body after commit was not an exact idempotent retry".into());
    }
    let same_commit =
        reopened.commit_prepared_authorization(command_id, prepared_effect.effect_digest())?;
    if same_commit != recovered || journal.replay()? != after_commit {
        return Err("same prepared digest after commit was not an exact idempotent retry".into());
    }
    let changed_effect = effect(&execution, b"changed after commit")?;
    let Err(NodeError::CommandConflict(conflict)) =
        reopened.prepare_authorization(command_id, changed_effect)
    else {
        return Err("changed prepared body after commit was not rejected exactly".into());
    };
    if conflict != command_id || journal.replay()? != after_commit {
        return Err(
            "changed prepare after commit altered history or named the wrong command".into(),
        );
    }
    let Err(NodeError::CommandConflict(digest_conflict)) =
        reopened.commit_prepared_authorization(command_id, "sha256:changed-after-commit")
    else {
        return Err("changed prepared digest after commit was not rejected exactly".into());
    };
    if digest_conflict != command_id || journal.replay()? != after_commit {
        return Err(
            "changed commit after commit altered history or named the wrong command".into(),
        );
    }
    Ok(())
}

#[test]
fn raw_committed_command_cannot_be_reclassified_as_prepared() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("raw-commit.redb");
    let (node, journal, _policy) = open(&path)?;
    let command_id = CommandId::new();
    let execution = executing(&node, command_id)?;
    let prepared_effect = effect(&execution, b"raw committed result")?;
    let committed = node.commit(
        command_id,
        prepared_effect.batch().clone(),
        prepared_effect.result().to_vec(),
    )?;
    if !matches!(committed.state, CommandState::CommittedLocally { .. }) {
        return Err("raw commit did not commit fixture command".into());
    }
    let after_commit = journal.replay()?;
    let Err(NodeError::CommandNotExecuting(prepared_conflict)) =
        node.prepare_authorization(command_id, prepared_effect.clone())
    else {
        return Err("raw committed command accepted a prepared body after commit".into());
    };
    if prepared_conflict != command_id || journal.replay()? != after_commit {
        return Err(
            "raw commit prepare rejection changed history or named the wrong command".into(),
        );
    }
    let Err(NodeError::CommandNotExecuting(commit_conflict)) =
        node.commit_prepared_authorization(command_id, prepared_effect.effect_digest())
    else {
        return Err("raw committed command accepted a prepared digest after commit".into());
    };
    if commit_conflict != command_id || journal.replay()? != after_commit {
        return Err(
            "raw commit digest rejection changed history or named the wrong command".into(),
        );
    }
    Ok(())
}

#[test]
fn exact_prepare_retry_is_idempotent_but_mismatch_does_not_append() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("mismatch.redb");
    let (node, journal, _policy) = open(&path)?;
    let command_id = CommandId::new();
    let execution = executing(&node, command_id)?;
    let prepared_effect = effect(&execution, b"kept")?;
    node.prepare_authorization(command_id, prepared_effect.clone())?;
    let before_retry = journal.replay()?;
    let retried = node.prepare_authorization(command_id, prepared_effect.clone())?;
    if prepared_effect_from(&retried)? != &prepared_effect || journal.replay()? != before_retry {
        return Err("exact prepare retry changed retained history".into());
    }

    let conflicting = effect(&execution, b"different")?;
    let Err(NodeError::CommandConflict(conflict)) =
        node.prepare_authorization(command_id, conflicting)
    else {
        return Err("conflicting prepared effect was not rejected exactly".into());
    };
    if conflict != command_id || journal.replay()? != before_retry {
        return Err("conflicting prepare changed history or reported the wrong command".into());
    }
    let Err(NodeError::CommandConflict(commit_conflict)) =
        node.commit_prepared_authorization(command_id, "sha256:not-the-effect")
    else {
        return Err("wrong prepared digest was not rejected exactly".into());
    };
    if commit_conflict != command_id || journal.replay()? != before_retry {
        return Err("wrong digest commit changed history or reported the wrong command".into());
    }
    Ok(())
}

#[test]
fn challenge_and_approval_keep_the_same_prepared_body() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("challenge.redb");
    let (node, journal, _policy) = open(&path)?;
    let command_id = CommandId::new();
    let execution = executing(&node, command_id)?;
    let prepared_effect = effect(&execution, b"challenged result")?;
    node.prepare_authorization(command_id, prepared_effect.clone())?;
    let challenge = ChallengeId::new("prepared-effect:first");
    let pending = node.await_prepared_authorization(
        command_id,
        prepared_effect.effect_digest(),
        challenge.clone(),
    )?;
    let (pending_batch, pending_result) = pending_batch_result_from(&pending)?;
    if pending_batch != prepared_effect.batch() || pending_result != prepared_effect.result() {
        return Err("challenge did not retain the prepared batch/result".into());
    }
    if prepared_event_count(&journal)? != 1 {
        return Err("challenge lost the preceding prepared evidence".into());
    }
    let before_reopen = journal.replay()?;
    drop(node);
    drop(journal);

    let (reopened, journal, _policy) = open(&path)?;
    let next = ChallengeId::new("prepared-effect:second");
    let advanced = reopened.advance_authorization(
        command_id,
        &challenge,
        next.clone(),
        ApprovalId::new("approval:first"),
    )?;
    let (advanced_batch, advanced_result) = pending_batch_result_from(&advanced)?;
    if advanced_batch != prepared_effect.batch() || advanced_result != prepared_effect.result() {
        return Err("challenge advancement changed the prepared batch/result".into());
    }
    let committed =
        reopened.resume_authorization(command_id, &next, ApprovalId::new("approval:second"))?;
    let CommandState::CommittedLocally { batch_id, .. } = committed.state else {
        return Err(format!(
            "approval did not commit prepared body, found {:?}",
            committed.state
        )
        .into());
    };
    let Some((committed, batch)) = committed_batch(&journal, command_id)? else {
        return Err("committed event missing after authorization approval".into());
    };
    if batch_id != prepared_effect.batch().id
        || &batch != prepared_effect.batch()
        || committed.result.as_deref() != Some(prepared_effect.result())
        || before_reopen.len() != 4
    {
        return Err("approved command committed a body different from the prepared effect".into());
    }
    Ok(())
}

#[test]
fn old_pending_shape_reopens_and_resumes_without_fabricating_prepared_evidence() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("old-pending.redb");
    let (node, journal, _policy) = open(&path)?;
    let command_id = CommandId::new();
    let execution = executing(&node, command_id)?;
    let prepared_effect = effect(&execution, b"old pending result")?;
    let pending_position = LogPosition::new(
        execution
            .updated_at
            .sequence
            .get()
            .checked_add(1)
            .ok_or("log position overflow")?,
    );
    let pending_origin = EventId::new(node.node_id(), pending_position);
    let recorded_at = journal
        .replay()?
        .into_iter()
        .last()
        .ok_or("missing executing event")?
        .recorded_at;
    let challenge = ChallengeId::new("prepared-effect:old-pending");
    let old_state = serde_json::json!({
        "state": "authorization_pending",
        "challenge_id": &challenge,
        "batch": prepared_effect.batch(),
        "result": prepared_effect.result(),
    });
    let state = serde_json::from_value(old_state)?;
    let CommandState::AuthorizationPending { approvals, .. } = &state else {
        return Err("legacy pending JSON did not decode to AuthorizationPending".into());
    };
    if !approvals.is_empty() {
        return Err("legacy pending JSON did not use the approvals serde default".into());
    }
    let pending = CommandSnapshot {
        request: execution.request,
        state,
        result: None,
        updated_at: pending_origin,
    };
    journal.append(&EventEnvelope {
        position: pending_position,
        origin: pending_origin,
        recorded_at,
        event: NodeEvent::CommandLifecycle(pending),
    })?;
    drop(node);
    drop(journal);

    let (reopened, journal, _policy) = open(&path)?;
    if prepared_event_count(&journal)? != 0 {
        return Err("old pending replay fabricated prepared evidence".into());
    }
    let committed =
        reopened.resume_authorization(command_id, &challenge, ApprovalId::new("approval:old"))?;
    let CommandState::CommittedLocally { batch_id, .. } = committed.state else {
        return Err(format!(
            "old pending did not resume to commit: {:?}",
            committed.state
        )
        .into());
    };
    let Some((committed, batch)) = committed_batch(&journal, command_id)? else {
        return Err("old pending resume did not append a committed event".into());
    };
    if batch_id != prepared_effect.batch().id
        || &batch != prepared_effect.batch()
        || committed.result.as_deref() != Some(prepared_effect.result())
        || prepared_event_count(&journal)? != 0
    {
        return Err("old pending resume changed body or fabricated prepared evidence".into());
    }
    Ok(())
}
