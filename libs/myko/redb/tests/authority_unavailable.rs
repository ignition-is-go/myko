use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use myko_federation::{
    AccessAttempt, AccessPolicy, AccessTarget, AllowAllAccessPolicy, AuthorityPresentation,
    AuthorityUnavailable, AuthorizationPhase, CommandId, CommandRequest, CommandState, Node,
    NodeError, NodeEvent, PrincipalId, ResourceClaim, ResourceClaimKind, ScopeId, ServiceId,
    TypedCommandAdmission,
};
use myko_redb::RedbJournal;

#[derive(Debug)]
struct AuthorityGate {
    available: AtomicBool,
    phase: AuthorizationPhase,
}

impl AccessPolicy for AuthorityGate {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        if request.authorization_phase == self.phase && !self.available.load(Ordering::Acquire) {
            return Err(AuthorityUnavailable::CoordinationUnavailable).into();
        }
        AllowAllAccessPolicy.decide(request)
    }
}

fn request(node: &Node) -> CommandRequest {
    let principal = PrincipalId::for_node(node.node_id());
    let scope = ScopeId::new("scope:authority-unavailable");
    CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("authority-unavailable-service"),
        scope_id: scope.clone(),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: vec![ResourceClaim::scope(scope, ResourceClaimKind::Primary)],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "authority-unavailable-command".to_owned(),
        payload: Vec::new(),
    }
}

#[derive(Debug, Default)]
struct CoordinatedAuthorityGate {
    polled: AtomicBool,
}

impl AccessPolicy for CoordinatedAuthorityGate {
    fn decide<'a>(&'a self, _request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        myko_federation::PolicyDecision::coordinated(async move {
            self.polled.store(true, Ordering::Release);
            Err(AuthorityUnavailable::HistoryUnavailable)
        })
    }
}

#[test]
fn synchronous_submission_does_not_poll_coordinated_authority() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("coordinated.redb"))?;
    let policy = Arc::new(CoordinatedAuthorityGate::default());
    node.set_command_access_policy(policy.clone())?;
    let request = request(&node);
    let id = request.id;
    if !matches!(
        node.submit(request),
        Err(NodeError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) {
        return Err("synchronous submission did not report coordination unavailable".into());
    }
    if policy.polled.load(Ordering::Acquire) {
        return Err("synchronous submission polled coordinated authority".into());
    }
    if node.command(id)?.is_some() {
        return Err("unavailable submission retained a command".into());
    }
    if !node.events_after(None)?.is_empty() {
        return Err("unavailable submission wrote journal events".into());
    }
    Ok(())
}

#[test]
fn unavailable_admission_never_records_a_rejected_command() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("admission.redb"))?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AuthorityGate {
        available: AtomicBool::new(false),
        phase: AuthorizationPhase::Admission,
    });
    node.set_command_access_policy(Arc::clone(&policy))?;
    let request = request(&node);
    let id = request.id;
    if !matches!(
        node.submit(request),
        Err(NodeError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) || node.command(id)?.is_some()
        || !node.events_after(None)?.is_empty()
    {
        return Err("unavailable admission was not a non-mutating retryable failure".into());
    }
    Ok(())
}

#[test]
fn unavailable_effect_survives_restart_and_commits_without_handler_reexecution()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("effect.redb");
    let node = RedbJournal::open_node(&path)?;
    let gate = Arc::new(AuthorityGate {
        available: AtomicBool::new(false),
        phase: AuthorizationPhase::Effect,
    });
    let policy: Arc<dyn AccessPolicy> = gate.clone();
    node.set_command_access_policy(Arc::clone(&policy))?;
    let request = request(&node);
    let id = request.id;
    if !matches!(
        node.prepared_command_access(id),
        Err(NodeError::UnknownCommand(_))
    ) {
        return Err("unknown command produced prepared authority evidence".into());
    }
    node.submit(request)?;
    if !matches!(
        node.prepared_command_access(id),
        Err(NodeError::InvalidCommandState(_))
    ) {
        return Err("unprepared command produced prepared authority evidence".into());
    }
    let TypedCommandAdmission::Execute(context) = node.begin_command(id)? else {
        return Err("new command did not execute".into());
    };
    if !matches!(
        context.commit_bytes(b"original result".to_vec()),
        Err(NodeError::AuthorityUnavailable(_))
    ) {
        return Err("unavailable effect was not returned as a retryable failure".into());
    }
    let parked = node.command(id)?.ok_or("prepared command missing")?;
    let CommandState::AuthorizationPrepared { effect } = &parked.state else {
        return Err("unavailable effect did not remain prepared".into());
    };
    let retained = node.events_after(None)?;
    drop(node);
    let reopened = RedbJournal::open_node(&path)?;
    let access = reopened.prepared_command_access(id)?;
    if access.effect_digest.as_deref() != Some(effect.effect_digest())
        || access.topology.as_ref() != Some(effect.topology_proof())
        || access.resource_claims != effect.resource_claims()
        || access.application_capabilities != effect.application_capabilities()
        || access.presentation != parked.request.authority
        || access.arguments_digest != parked.request.arguments_digest
        || !matches!(access.target, AccessTarget::KnownCommand { command_id, .. } if command_id == id)
    {
        return Err(
            "prepared authority evidence did not match the persisted command and effect".into(),
        );
    }
    reopened.set_command_access_policy(Arc::clone(&policy))?;
    if !matches!(
        reopened.begin_command(id),
        Err(NodeError::AuthorityUnavailable(_))
    ) || reopened.command(id)? != Some(parked.clone())
        || reopened.events_after(None)? != retained
    {
        return Err("unavailable retry changed the prepared command or journal".into());
    }
    gate.available.store(true, Ordering::Release);
    let TypedCommandAdmission::Resume(committed) = reopened.begin_command(id)? else {
        return Err("authority recovery reran the handler".into());
    };
    let batches = reopened
        .events_after(None)?
        .into_iter()
        .filter_map(|event| match event.event {
            NodeEvent::CommandCommitted { batch, .. } => Some(batch),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !committed.state.is_committed()
        || committed.result.as_deref() != Some(effect.result())
        || batches.as_slice() != std::slice::from_ref(effect.batch())
    {
        return Err("authority recovery did not commit the original effect exactly once".into());
    }
    Ok(())
}
