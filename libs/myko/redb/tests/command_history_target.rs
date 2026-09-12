use std::error::Error;

use myko_federation::{
    AuthorityPresentation, BatchId, ChangeBatch, CommandHistoryTarget, CommandHistoryTargetError,
    CommandId, CommandRequest, CommandSnapshot, CommandState, EventId, EventJournal, LogPosition,
    Node, PrincipalId, Reconciliation, ScopeId, ScopeSelection, SelectedHistoryManifest,
    SelectedHistorySnapshot, ServiceId, StorageIncarnationId,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn commit(node: &Node, scope: &ScopeId) -> Result<CommandSnapshot, Box<dyn Error>> {
    let principal = PrincipalId::new("test:command-history");
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("records"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "put".to_owned(),
        payload: Vec::new(),
    };
    let admission = node.admit(request.clone())?;
    Ok(node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: scope.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        br#"{"value":7}"#.to_vec(),
    )?)
}

fn manifest(node: &Node, scope: &ScopeId) -> Result<SelectedHistoryManifest, Box<dyn Error>> {
    Ok(SelectedHistorySnapshot::current(node)?
        .retained_manifest(&ScopeSelection::Exact(scope.clone()))?)
}

#[test]
fn target_binds_commit_and_stays_frozen_as_history_advances() -> TestResult {
    let node = Node::in_memory();
    let scope = ScopeId::new("records:one");
    let command = commit(&node, &scope)?;
    let obligation = command.updated_at;
    let target =
        CommandHistoryTarget::from_committed(&command, manifest(&node, &scope)?, obligation)?;
    let holder = node.node_id();
    let incarnation = StorageIncarnationId::new();
    let expected = target.expected_statement(holder, incarnation)?;
    if target.command_id() != command.request.id
        || target.committed_at() != command.updated_at
        || target.obligation() != obligation
        || expected.commitment() != &target.manifest().commitment()?
    {
        return Err("target lost its immutable commit binding".into());
    }
    commit(&node, &scope)?;
    let later =
        CommandHistoryTarget::from_committed(&command, manifest(&node, &scope)?, obligation)?;
    if target.expected_statement(holder, incarnation)? != expected
        || later.expected_statement(holder, incarnation)? == expected
    {
        return Err("old target changed or later history reused the old commitment".into());
    }
    let other_obligation = CommandHistoryTarget::from_committed(
        &command,
        target.manifest().clone(),
        EventId::new(holder, LogPosition::new(999)),
    )?;
    if other_obligation.expected_statement(holder, incarnation)? == expected {
        return Err("a different obligation reused the expected statement".into());
    }
    Ok(())
}

#[test]
fn target_rejects_uncommitted_and_mismatched_command_snapshots() -> TestResult {
    let node = Node::in_memory();
    let scope = ScopeId::new("records:one");
    let command = commit(&node, &scope)?;
    let history = manifest(&node, &scope)?;
    for state in [
        CommandState::Submitted,
        CommandState::Executing,
        CommandState::Rejected {
            reason: "no".to_owned(),
        },
        CommandState::Cancelled {
            reason: "cancelled".to_owned(),
        },
    ] {
        let mut pending = command.clone();
        pending.state = state;
        if !matches!(CommandHistoryTarget::from_committed(&pending, history.clone(), command.updated_at),
            Err(CommandHistoryTargetError::NotCommitted(id)) if id == command.request.id)
        {
            return Err("uncommitted snapshot formed a command target".into());
        }
    }
    let mut wrong_id = command.clone();
    wrong_id.request.id = CommandId::new();
    let mut wrong_batch = command.clone();
    wrong_batch.state = CommandState::CommittedLocally {
        batch_id: BatchId::new(),
        position: command.updated_at,
    };
    let mut wrong_event = command.clone();
    wrong_event.state = CommandState::CommittedLocally {
        batch_id: BatchId::new(),
        position: history.events().first().ok_or("admission missing")?.origin,
    };
    let mut wrong_result = command.clone();
    wrong_result.result = Some(b"different".to_vec());
    let mut missing_result = command.clone();
    missing_result.result = None;
    let mut wrong_request = command.clone();
    wrong_request.request.payload = b"different".to_vec();
    for wrong in [
        wrong_id,
        wrong_batch,
        wrong_event,
        wrong_result,
        missing_result,
        wrong_request,
    ] {
        if !matches!(
            CommandHistoryTarget::from_committed(&wrong, history.clone(), command.updated_at),
            Err(CommandHistoryTargetError::CommitMismatch(_))
        ) {
            return Err("mismatched snapshot formed a command target".into());
        }
    }
    Ok(())
}

#[test]
fn lifecycle_counts_and_reconciliation_cannot_replace_a_commit_event() -> TestResult {
    let node = Node::in_memory();
    let scope = ScopeId::new("records:one");
    let empty = manifest(&node, &scope)?;
    let command = commit(&node, &scope)?;
    let target = CommandHistoryTarget::from_committed(
        &command,
        manifest(&node, &scope)?,
        command.updated_at,
    )?;
    for state in [
        CommandState::Replicated {
            batch_id: target.batch_id(),
            position: target.committed_at(),
            acknowledged_replicas: 100,
            required_replicas: 1,
        },
        CommandState::Reconciled {
            batch_id: target.batch_id(),
            position: target.committed_at(),
            outcome: Reconciliation::FullyVisible,
        },
    ] {
        let mut advanced = command.clone();
        advanced.state = state;
        if !matches!(
            CommandHistoryTarget::from_committed(&advanced, empty.clone(), command.updated_at),
            Err(CommandHistoryTargetError::MissingCommit(_))
        ) {
            return Err("lifecycle label replaced actual retained command history".into());
        }
        if CommandHistoryTarget::from_committed(
            &advanced,
            target.manifest().clone(),
            command.updated_at,
        )? != target
        {
            return Err("later lifecycle state changed the exact commit target".into());
        }
    }
    Ok(())
}

#[test]
fn imported_history_reopens_with_the_same_target_at_different_local_positions() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = Node::in_memory();
    let scope = ScopeId::new("records:one");
    let command = commit(&source, &scope)?;
    let target = CommandHistoryTarget::from_committed(
        &command,
        manifest(&source, &scope)?,
        command.updated_at,
    )?;
    let path = directory.path().join("holder.redb");
    let (holder, journal) = RedbJournal::open_node_with_journal(&path)?;
    commit(&holder, &ScopeId::new("records:unrelated"))?;
    for event in target.manifest().events() {
        holder.ingest(event.clone())?;
    }
    journal.verify_retained_history(target.manifest().events())?;
    let incarnation = journal.storage_incarnation()?;
    let expected = target.expected_statement(holder.node_id(), incarnation)?;
    let holder_manifest = manifest(&holder, &scope)?;
    if holder_manifest.through() == target.manifest().through()
        || holder_manifest.events().first().map(|event| event.position)
            == target
                .manifest()
                .events()
                .first()
                .map(|event| event.position)
    {
        return Err("fixture failed to give imports different recording positions".into());
    }
    let holder_target =
        CommandHistoryTarget::from_committed(&command, holder_manifest, command.updated_at)?;
    if holder_target.expected_statement(holder.node_id(), incarnation)? != expected {
        return Err("local recording positions changed the expected statement".into());
    }
    drop(holder);
    drop(journal);
    let (reopened, journal) = RedbJournal::open_node_with_journal(path)?;
    let recovered = reopened
        .command(command.request.id)?
        .ok_or("reopened commit missing")?;
    let recovered_target = CommandHistoryTarget::from_committed(
        &recovered,
        manifest(&reopened, &scope)?,
        command.updated_at,
    )?;
    journal.verify_retained_history(recovered_target.manifest().events())?;
    if recovered_target.expected_statement(reopened.node_id(), journal.storage_incarnation()?)?
        != expected
    {
        return Err("reopen changed the exact command-history assertion".into());
    }
    Ok(())
}
