use std::{
    error::Error,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, Mutex},
};

use myko_federation::{
    AccessAttempt, AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation, AuthorizationPhase,
    CommandId, CommandRequest, CommandState, NodeEvent, PrincipalId, ResourceClaim,
    ResourceClaimKind, ScopeId, ServiceId, TypedCommandAdmission,
};
use myko_redb::RedbJournal;

#[derive(Debug)]
struct CrashAtEffectAuthorization {
    observed: Arc<Mutex<Option<AccessAttempt>>>,
}

impl AccessPolicy for CrashAtEffectAuthorization {
    fn authorize(&self, request: &AccessAttempt) -> Result<(), String> {
        if request.authorization_phase == AuthorizationPhase::Effect {
            *self.observed.lock().map_err(|error| error.to_string())? = Some(request.clone());
            resume_unwind(Box::new("crash at effect authorization"));
        }
        Ok(())
    }
}

#[test]
fn live_commit_saves_exact_effect_before_policy_and_recovers_after_crash()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("live-prepared.redb");
    let (node, journal) = RedbJournal::open_node_with_journal(&path)?;
    let observed = Arc::new(Mutex::new(None));
    let policy: Arc<dyn AccessPolicy> = Arc::new(CrashAtEffectAuthorization {
        observed: Arc::clone(&observed),
    });
    node.set_command_access_policy(Arc::clone(&policy))?;
    let command_id = CommandId::new();
    let principal_id = PrincipalId::for_node(node.node_id());
    let scope = ScopeId::new("scope:live-prepared");
    node.submit(CommandRequest {
        id: command_id,
        service_id: ServiceId::new("live-prepared-service"),
        scope_id: scope.clone(),
        principal_id: principal_id.clone(),
        authority: AuthorityPresentation::direct_node(principal_id),
        resource_claims: vec![ResourceClaim::scope(scope, ResourceClaimKind::Primary)],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "live-prepared-command".to_owned(),
        payload: Vec::new(),
    })?;
    let TypedCommandAdmission::Execute(context) = node.begin_command(command_id)? else {
        return Err("new command was not executable".into());
    };
    let result = b"exact handler result".to_vec();
    let crash = catch_unwind(AssertUnwindSafe(|| context.commit_bytes(result.clone())));
    if crash.is_ok() {
        return Err("effect authorization did not reach crash injection".into());
    }
    let attempt = observed
        .lock()
        .map_err(|error| error.to_string())?
        .clone()
        .ok_or("effect authorization was not observed")?;
    let parked = node
        .command(command_id)?
        .ok_or("prepared command missing")?;
    let CommandState::AuthorizationPrepared { effect } = parked.state else {
        return Err("policy ran before the exact effect was durable".into());
    };
    if attempt.effect_digest.as_deref() != Some(effect.effect_digest())
        || attempt.resource_claims != effect.resource_claims()
        || attempt.application_capabilities != effect.application_capabilities()
        || attempt.topology.as_ref() != Some(effect.topology_proof())
        || effect.result() != result
    {
        return Err("policy observed a different effect from the durable body".into());
    }
    if node
        .events_after(None)?
        .iter()
        .any(|event| matches!(event.event, NodeEvent::CommandCommitted { .. }))
    {
        return Err("effect committed despite crashing before authorization".into());
    }
    drop(policy);
    drop(node);
    drop(journal);

    let reopened = RedbJournal::open_node(&path)?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    reopened.set_command_access_policy(Arc::clone(&policy))?;
    let recovered = match reopened.begin_command(command_id)? {
        TypedCommandAdmission::Resume(command) => command,
        TypedCommandAdmission::Execute(_) => return Err("recovery reran the handler".into()),
    };
    if !recovered.state.is_committed() || recovered.result.as_deref() != Some(effect.result()) {
        return Err("recovered command did not commit the original result".into());
    }
    let commits = reopened
        .events_after(None)?
        .into_iter()
        .filter_map(|event| match event.event {
            NodeEvent::CommandCommitted { command, batch } if command.request.id == command_id => {
                Some(batch)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if commits.as_slice() != std::slice::from_ref(effect.batch()) {
        return Err("recovery did not commit the original batch exactly once".into());
    }
    Ok(())
}
