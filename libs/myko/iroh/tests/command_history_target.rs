use std::error::Error;

use myko_federation::{
    AuthorityPresentation, BatchId, ChangeBatch, CommandHistoryTarget, CommandId, CommandRequest,
    CommandSnapshot, EventId, EventJournal, LogPosition, Node, NodeError, NodeId, PrincipalId,
    ScopeId, ScopeSelection, SelectedHistoryManifest, SelectedHistorySnapshot, ServiceId,
};
use myko_iroh::{
    EndpointAddr, NativeNodeDescriptor, RetainedHistorySignatureError, SecretKey,
    sign_retained_history_statement, verify_retained_history_statement,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn commit(node: &Node, scope: &ScopeId) -> Result<CommandSnapshot, Box<dyn Error>> {
    let principal = PrincipalId::new("test:iroh-command-history");
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
fn signed_command_history_target_records_once_and_reopens() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("holder.redb");
    let source = Node::in_memory();
    let scope = ScopeId::new("records:signed-target");
    let command = commit(&source, &scope)?;
    // This fixture identifies the expected assertion; it establishes no obligation policy.
    let obligation = EventId::new(NodeId::new(), LogPosition::new(41));
    let target =
        CommandHistoryTarget::from_committed(&command, manifest(&source, &scope)?, obligation)?;
    let later_command = commit(&source, &scope)?;
    let later_manifest = manifest(&source, &scope)?;

    let (holder, journal) = RedbJournal::open_node_with_journal(&path)?;
    for event in later_manifest.events() {
        holder.ingest(event.clone())?;
    }
    let expected = target.expected_statement(holder.node_id(), journal.storage_incarnation()?)?;
    let key = SecretKey::generate();
    let trusted = NativeNodeDescriptor::new(holder.node_id(), EndpointAddr::new(key.public()));
    let signed = sign_retained_history_statement(expected.clone(), &key)?;

    verify_retained_history_statement(&signed, &trusted, &expected)?;
    let wrong_history =
        CommandHistoryTarget::from_committed(&command, later_manifest.clone(), obligation)?
            .expected_statement(holder.node_id(), journal.storage_incarnation()?)?;
    if !matches!(
        verify_retained_history_statement(&signed, &trusted, &wrong_history),
        Err(RetainedHistorySignatureError::UnexpectedStatement)
    ) || !matches!(
        holder.record_retained_history_statement(signed.clone(), &later_manifest),
        Err(NodeError::InvalidRetainedHistoryStatement(_))
    ) {
        return Err("wrong history was accepted as the signed command target".into());
    }
    let wrong_obligation = CommandHistoryTarget::from_committed(
        &command,
        target.manifest().clone(),
        EventId::new(NodeId::new(), LogPosition::new(42)),
    )?
    .expected_statement(holder.node_id(), journal.storage_incarnation()?)?;
    if !matches!(
        verify_retained_history_statement(&signed, &trusted, &wrong_obligation),
        Err(RetainedHistorySignatureError::UnexpectedStatement)
    ) {
        return Err("wrong obligation was accepted as the signed command target".into());
    }

    let record = holder.record_retained_history_statement(signed.clone(), target.manifest())?;
    let recorded_history = journal.replay()?;
    drop(holder);
    drop(journal);

    let (reopened, journal) = RedbJournal::open_node_with_journal(&path)?;
    let recovered = reopened
        .command(command.request.id)?
        .ok_or("reopened holder lost the committed command")?;
    journal.verify_retained_history(target.manifest().events())?;
    let recovered_target =
        CommandHistoryTarget::from_committed(&recovered, target.manifest().clone(), obligation)?;
    let recovered_expected =
        recovered_target.expected_statement(reopened.node_id(), journal.storage_incarnation()?)?;
    verify_retained_history_statement(&signed, &trusted, &recovered_expected)?;
    if reopened.record_retained_history_statement(signed, target.manifest())? != record
        || journal.replay()? != recorded_history
        || reopened.command(later_command.request.id)?.is_none()
    {
        return Err("duplicate or reopen changed the recorded holder statement".into());
    }
    Ok(())
}
