use std::{error::Error, sync::Arc};

use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation, BatchId, ChangeBatch, CommandId,
    CommandRequest, EventEnvelope, Node, NodeError, PrincipalId, ResourceClaim, ResourceClaimKind,
    ScopeId, ScopeSelection, ServiceId,
};

type TestResult = Result<(), Box<dyn Error>>;

fn request(scope: &ScopeId, claims: Vec<ResourceClaim>) -> CommandRequest {
    let principal = PrincipalId::new("test:scope-readiness");
    CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("records"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: claims,
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "records.change".to_owned(),
        payload: Vec::new(),
    }
}

fn allow(node: &Node) -> Result<Arc<dyn AccessPolicy>, NodeError> {
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(Arc::clone(&policy))?;
    Ok(policy)
}

fn dependent_history(scope: &ScopeId) -> Result<(EventEnvelope, EventEnvelope), Box<dyn Error>> {
    let source = Node::in_memory();
    let request = request(scope, Vec::new());
    let admission = source.admit(request.clone())?;
    source.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: scope.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        Vec::new(),
    )?;
    let mut events = source.events_after(None)?.into_iter();
    let parent = events.next().ok_or("source did not retain admission")?;
    let child = events.next().ok_or("source did not retain commit")?;
    Ok((parent, child))
}

#[test]
fn new_submission_into_incomplete_scope_is_not_accepted_or_queued() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let scope = ScopeId::new("records/workspace:one");
    let (parent, child) = dependent_history(&scope)?;
    node.ingest(child)?;
    let before = node.events_after(None)?;
    let command = request(&scope, Vec::new());
    if !matches!(node.submit(command.clone()), Err(NodeError::ScopeHistoryIncomplete(selection))
        if selection == ScopeSelection::Exact(scope.clone()))
    {
        return Err("new command was accepted with incomplete scope history".into());
    }
    if !matches!(node.admit(command.clone()), Err(NodeError::ScopeHistoryIncomplete(selection))
        if selection == ScopeSelection::Exact(scope))
    {
        return Err("direct admission bypassed incomplete scope history".into());
    }
    if node.command(command.id)?.is_some() || node.events_after(None)? != before {
        return Err("rejected submission changed command history".into());
    }
    node.ingest(parent)?;
    if node.command(command.id)?.is_some() {
        return Err("history arrival executed a previously rejected submission".into());
    }
    if node.submit(command.clone())?.request != command {
        return Err("explicit retry did not accept the original command identity".into());
    }
    Ok(())
}

#[test]
fn pending_history_arriving_after_preflight_is_checked_at_durable_submission() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let scope = ScopeId::new("records/workspace:one");
    let command = request(&scope, Vec::new());
    let prepared = node
        .prepare_command(command.authority.executor.id.clone(), command.clone())
        .map_err(NodeError::from)?;
    let (_, child) = dependent_history(&scope)?;
    node.ingest(child)?;
    let before = node.events_after(None)?;
    if !matches!(prepared.submit(), Err(NodeError::ScopeHistoryIncomplete(selection))
        if selection == ScopeSelection::Exact(scope))
    {
        return Err("preflight permit bypassed newly incomplete scope history".into());
    }
    if node.command(command.id)?.is_some() || node.events_after(None)? != before {
        return Err("failed admission race recorded a command".into());
    }
    Ok(())
}

#[test]
fn exact_unrelated_scope_remains_usable_but_declared_dependency_is_checked() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let blocked = ScopeId::new("records/workspace:catching-up");
    let healthy = ScopeId::new("records/workspace:ready");
    let (parent, child) = dependent_history(&blocked)?;
    node.ingest(child)?;
    node.submit(request(&healthy, Vec::new()))?;
    node.admit(request(&healthy, Vec::new()))?;
    let dependent = request(
        &healthy,
        vec![ResourceClaim::scope(
            blocked.clone(),
            ResourceClaimKind::Referenced,
        )],
    );
    if !matches!(node.submit(dependent.clone()), Err(NodeError::ScopeHistoryIncomplete(selection))
        if selection == ScopeSelection::Exact(blocked))
    {
        return Err("declared dependency on another incomplete scope was ignored".into());
    }
    node.ingest(parent)?;
    node.submit(dependent)?;
    Ok(())
}

#[test]
fn accepted_identity_recovery_is_not_a_new_submission() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let scope = ScopeId::new("records/workspace:one");
    let command = request(&scope, Vec::new());
    let accepted = node.submit(command.clone())?;
    let (_, child) = dependent_history(&scope)?;
    node.ingest(child)?;
    let before = node.events_after(None)?;
    if node.submit(command.clone())? != accepted {
        return Err("retry lost an already accepted command identity".into());
    }
    if !matches!(node.admit(command.clone())?, myko_federation::CommandAdmission::Resume(snapshot)
        if snapshot == accepted)
    {
        return Err("direct admission retried the accepted command's execution".into());
    }
    if node.events_after(None)? != before {
        return Err("identity recovery appended new acceptance".into());
    }
    let mut conflict = command;
    conflict.payload = b"different".to_vec();
    if !matches!(node.submit(conflict), Err(NodeError::CommandConflict(_))) {
        return Err("incomplete history hid a conflicting command identity".into());
    }
    Ok(())
}

#[test]
fn unresolved_topology_cannot_prove_an_event_is_outside_a_subtree() -> TestResult {
    let node = Node::in_memory();
    let _policy = allow(&node)?;
    let root = ScopeId::new("records/workspace:root");
    let possible_child = ScopeId::new("records/folder:child");
    let (_, child) = dependent_history(&possible_child)?;
    node.ingest(child)?;
    node.submit(request(&root, Vec::new()))?;
    let mut claim = ResourceClaim::scope(root.clone(), ResourceClaimKind::Referenced);
    claim.selection = ScopeSelection::Subtree(root.clone());
    if !matches!(node.submit(request(&root, vec![claim])), Err(NodeError::ScopeHistoryIncomplete(selection))
        if selection == ScopeSelection::Subtree(root))
    {
        return Err("subtree admission guessed that incomplete topology was unrelated".into());
    }
    Ok(())
}

#[test]
fn concurrent_ingestion_and_submission_have_one_history_order() -> TestResult {
    for _ in 0..32 {
        let node = Node::in_memory();
        let _policy = allow(&node)?;
        let scope = ScopeId::new("records/workspace:one");
        let (_, child) = dependent_history(&scope)?;
        let child_origin = child.origin;
        let command = request(&scope, Vec::new());
        let command_id = command.id;
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let submit_node = node.clone();
        let submit_barrier = Arc::clone(&barrier);
        let submitter = std::thread::spawn(move || {
            submit_barrier.wait();
            submit_node.submit(command)
        });
        let ingest_node = node.clone();
        let ingest_barrier = Arc::clone(&barrier);
        let ingester = std::thread::spawn(move || {
            ingest_barrier.wait();
            ingest_node.ingest(child)
        });
        barrier.wait();
        let outcome = submitter.join().map_err(|_| "submission thread panicked")?;
        ingester.join().map_err(|_| "ingestion thread panicked")??;
        let events = node.events_after(None)?;
        match outcome {
            Ok(snapshot) => {
                let accepted_at = events
                    .iter()
                    .find(|event| event.origin == snapshot.updated_at)
                    .ok_or("accepted command was not retained")?
                    .position;
                let pending_at = events
                    .iter()
                    .find(|event| event.origin == child_origin)
                    .ok_or("pending event was not retained")?
                    .position;
                if accepted_at >= pending_at {
                    return Err("new command was accepted after incomplete history arrived".into());
                }
            }
            Err(NodeError::ScopeHistoryIncomplete(selection))
                if selection == ScopeSelection::Exact(scope) =>
            {
                if node.command(command_id)?.is_some() {
                    return Err("rejected racing command was nevertheless accepted".into());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
