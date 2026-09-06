use std::{error::Error, sync::Arc};

use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation, CommandId, CommandRequest,
    EventEnvelope, EventJournal, FrameworkControlEvent, IngestStatus, Node, NodeError, NodeEvent,
    PrincipalId, ReplicationSelection, RetainedHistoryStatement, ScopeId, ScopeSelection,
    SelectedHistoryManifest, SelectedHistoryManifestError, SelectedHistorySnapshot, ServiceId,
    SignedRetainedHistoryStatement, StorageIncarnationId,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn seed(node: &Node, scope: &ScopeId) -> Result<EventEnvelope, Box<dyn Error>> {
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let principal = PrincipalId::new("test:control-history");
    node.submit(CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("control-fixture"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "fixture".to_owned(),
        payload: b"accepted obligation fixture".to_vec(),
    })?;
    drop(policy);
    node.events_after(None)?
        .pop()
        .ok_or_else(|| "seed produced no event".into())
}

fn statement(
    node: &Node,
    journal: &RedbJournal,
    obligation: &EventEnvelope,
    selection: &ScopeSelection,
) -> Result<(SignedRetainedHistoryStatement, SelectedHistoryManifest), Box<dyn Error>> {
    let manifest = SelectedHistorySnapshot::current(node)?.retained_manifest(selection)?;
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        journal.storage_incarnation()?,
        obligation.origin,
        &manifest,
    )?;
    // Persistence preserves unverified bytes. Signature validation is tested in Iroh.
    Ok((
        SignedRetainedHistoryStatement::from_signature(statement, [1; 32], [2; 64]),
        manifest,
    ))
}

#[test]
fn recorded_statement_reopens_in_the_same_history_without_a_command() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("control.redb");
    let (node, journal) = RedbJournal::open_node_with_journal(&path)?;
    let scope = ScopeId::new("control:retained");
    let selection = ScopeSelection::Exact(scope.clone());
    let obligation = seed(&node, &scope)?;
    let NodeEvent::CommandLifecycle(command) = &obligation.event else {
        return Err("seed was not a command lifecycle".into());
    };
    let command_id = command.request.id;
    let (signed, manifest) = statement(&node, &journal, &obligation, &selection)?;
    let record = node.record_retained_history_statement(signed.clone(), &manifest)?;
    if record.origin.node_id != node.node_id()
        || record.event
            != NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(
                signed.clone(),
            ))
        || manifest
            .events()
            .iter()
            .any(|event| event.origin == record.origin)
    {
        return Err("record lost identity or the frozen manifest included its own issuance".into());
    }
    let expected = journal.replay()?;
    if expected.len() != 2 || node.command(command_id)?.as_ref() != Some(command) {
        return Err("recording changed the command table or failed to append exactly once".into());
    }
    let manifest_with_record =
        SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection)?;
    if manifest_with_record.events().len() != 2 {
        return Err("later manifest omitted the earlier framework control record".into());
    }
    drop(node);
    drop(journal);

    let (reopened, journal) = RedbJournal::open_node_with_journal(&path)?;
    if journal.replay()? != expected || reopened.command(command_id)?.as_ref() != Some(command) {
        return Err("restart changed retained bytes or reconstructed a spurious command".into());
    }
    journal.verify_retained_history(manifest.events())?;
    if reopened.record_retained_history_statement(signed, &manifest)? != record
        || journal.replay()? != expected
    {
        return Err("retry after restart appended another local assertion".into());
    }
    if SelectedHistorySnapshot::current(&reopened)?
        .retained_manifest(&selection)?
        .commitment()?
        != manifest_with_record.commitment()?
    {
        return Err("reopened control history produced a different commitment".into());
    }
    Ok(())
}

#[test]
fn subtree_control_requires_the_full_selection_and_cannot_establish_topology() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (source, journal) =
        RedbJournal::open_node_with_journal(directory.path().join("source.redb"))?;
    let scope = ScopeId::new("control:private-subtree");
    let exact = ScopeSelection::Exact(scope.clone());
    let subtree = ScopeSelection::Subtree(scope.clone());
    let obligation = seed(&source, &scope)?;
    let (signed, manifest) = statement(&source, &journal, &obligation, &subtree)?;
    let record = source.record_retained_history_statement(signed, &manifest)?;
    for denied in [
        ReplicationSelection::Scopes(vec![exact.clone()]),
        ReplicationSelection::Scopes(vec![ScopeSelection::Exact(ScopeId::new("other"))]),
        ReplicationSelection::Service(ServiceId::new("control-fixture")),
        ReplicationSelection::ServiceScope {
            service_id: ServiceId::new("control-fixture"),
            scope_id: scope.clone(),
        },
        ReplicationSelection::Intersection {
            requested: Box::new(ReplicationSelection::All),
            scopes: vec![exact.clone()],
        },
    ] {
        if denied.includes(&record.event)
            || denied.includes_in(
                &record.event,
                SelectedHistorySnapshot::current(&source)?.topology(),
            )
        {
            return Err(format!("narrow selector exposed a subtree statement: {denied:?}").into());
        }
    }
    if !ReplicationSelection::Scopes(vec![subtree.clone()]).includes(&record.event)
        || source
            .export_scope(scope.clone(), None)?
            .events
            .iter()
            .any(|event| event.origin == record.origin)
    {
        return Err(
            "full subtree selection or exact-scope export used the wrong control footprint".into(),
        );
    }
    if !matches!(SelectedHistorySnapshot::current(&source)?.retained_manifest(&exact),
        Err(SelectedHistoryManifestError::ControlOutsideSelection { event, .. }) if event == record.origin)
    {
        return Err(
            "exact manifest silently included or omitted an overlapping subtree control".into(),
        );
    }

    let replica = Node::in_memory();
    replica.ingest(record.clone())?;
    let snapshot = SelectedHistorySnapshot::current(&replica)?;
    if snapshot.topology().knows(&scope) || !snapshot.ready().is_empty() {
        return Err(
            "an unverified control established scope topology or bypassed its missing obligation"
                .into(),
        );
    }
    if !matches!(snapshot.retained_manifest(&subtree),
        Err(SelectedHistoryManifestError::PendingHistory(event)) if event == record.origin)
    {
        return Err("manifest ignored a pending control obligation".into());
    }
    replica.ingest(obligation)?;
    let complete = SelectedHistorySnapshot::current(&replica)?.retained_manifest(&subtree)?;
    if complete.events().len() != 2 || replica.ingest(record)? != IngestStatus::Duplicate {
        return Err("late obligation failed to release or deduplicate the control record".into());
    }
    Ok(())
}

#[test]
fn missing_retained_history_cannot_be_recorded_in_a_durable_receiver() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = Node::in_memory();
    let scope = ScopeId::new("control:missing");
    let obligation = seed(&source, &scope)?;
    let manifest = SelectedHistorySnapshot::current(&source)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let (receiver, journal) =
        RedbJournal::open_node_with_journal(directory.path().join("receiver.redb"))?;
    let statement = RetainedHistoryStatement::new(
        receiver.node_id(),
        journal.storage_incarnation()?,
        obligation.origin,
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [1; 32], [2; 64]);
    if !matches!(receiver.record_retained_history_statement(signed, &manifest),
        Err(NodeError::MissingRetainedEvent(origin)) if origin == obligation.origin)
        || !journal.replay()?.is_empty()
        || receiver.local_history_cut()?.is_some()
    {
        return Err("missing manifest history yielded a record or advanced the local cut".into());
    }
    Ok(())
}

#[test]
fn wrong_storage_incarnation_cannot_change_durable_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("wrong-incarnation.redb");
    let (node, journal) = RedbJournal::open_node_with_journal(&path)?;
    let scope = ScopeId::new("control:wrong-incarnation");
    let selection = ScopeSelection::Exact(scope.clone());
    let obligation = seed(&node, &scope)?;
    let manifest = SelectedHistorySnapshot::current(&node)?.retained_manifest(&selection)?;
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        StorageIncarnationId::new(),
        obligation.origin,
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [1; 32], [2; 64]);
    let before = journal.replay()?;
    let cut = node.local_history_cut()?;
    if !matches!(
        node.record_retained_history_statement(signed, &manifest),
        Err(NodeError::InvalidRetainedHistoryStatement(_))
    ) || journal.replay()? != before
        || node.local_history_cut()? != cut
    {
        return Err("wrong storage incarnation changed live durable state".into());
    }
    drop(node);
    drop(journal);

    let (reopened, journal) = RedbJournal::open_node_with_journal(&path)?;
    if journal.replay()? != before || reopened.local_history_cut()? != cut {
        return Err("wrong-incarnation rejection changed state after reopen".into());
    }
    Ok(())
}
