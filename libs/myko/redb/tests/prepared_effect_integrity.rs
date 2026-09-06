use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use myko_federation::{
    AccessAttempt, AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation,
    AuthorizationDecision, AuthorizationPhase, BatchId, ChangeBatch, CommandAdmission, CommandId,
    CommandRequest, CommandSnapshot, CommandState, EventEnvelope, EventJournal, Node, NodeError,
    NodeEvent, PreparedCommandEffect, PrincipalId, ResourceClaim, ResourceClaimKind, ScopeId,
    ScopeTopology, ServiceId, StorageIncarnationId, TypedCommandAdmission,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn request(node: &Node, command_id: CommandId, scope: &str) -> CommandRequest {
    let scope_id = ScopeId::new(scope);
    let principal_id = PrincipalId::for_node(node.node_id());
    CommandRequest {
        id: command_id,
        service_id: ServiceId::new("prepared-effect-integrity"),
        scope_id: scope_id.clone(),
        principal_id: principal_id.clone(),
        authority: AuthorityPresentation::direct_node(principal_id),
        resource_claims: vec![ResourceClaim::scope(scope_id, ResourceClaimKind::Primary)],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "prepared-effect-integrity-command".to_owned(),
        payload: b"prepared-effect-integrity".to_vec(),
    }
}

fn executing(node: &Node, command_id: CommandId) -> Result<CommandSnapshot, Box<dyn Error>> {
    node.submit(request(node, command_id, "prepared-effect-integrity:scope"))?;
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

fn prepared_effect_from(command: &CommandSnapshot) -> Result<&PreparedCommandEffect, String> {
    let CommandState::AuthorizationPrepared { effect } = &command.state else {
        return Err(format!(
            "expected authorization_prepared, found {:?}",
            command.state
        ));
    };
    Ok(effect)
}

fn tamper_prepared_effect_digest(event: &EventEnvelope) -> Result<EventEnvelope, Box<dyn Error>> {
    let mut event = event.clone();
    let NodeEvent::CommandLifecycle(command) = &mut event.event else {
        return Err("event was not a command lifecycle".into());
    };
    let CommandState::AuthorizationPrepared { effect } = &mut command.state else {
        return Err("event was not an authorization_prepared lifecycle".into());
    };
    let mut encoded = serde_json::to_value(effect.as_ref())?;
    let digest = encoded
        .get_mut("effect_digest")
        .ok_or("prepared effect did not serialize an effect_digest")?;
    *digest = serde_json::Value::String("sha256:tampered-prepared-effect".to_owned());
    **effect = serde_json::from_value(encoded)?;
    Ok(event)
}

fn prepared_event(events: &[EventEnvelope]) -> Result<&EventEnvelope, Box<dyn Error>> {
    events
        .iter()
        .find(|event| {
            matches!(
                &event.event,
                NodeEvent::CommandLifecycle(CommandSnapshot {
                    state: CommandState::AuthorizationPrepared { .. },
                    ..
                })
            )
        })
        .ok_or_else(|| "prepared lifecycle event was absent".into())
}

#[derive(Debug)]
struct FailingPreparedAppendJournal {
    inner: Arc<RedbJournal>,
    failed: AtomicBool,
}

impl FailingPreparedAppendJournal {
    const fn new(inner: Arc<RedbJournal>) -> Self {
        Self {
            inner,
            failed: AtomicBool::new(false),
        }
    }
}

impl EventJournal for FailingPreparedAppendJournal {
    fn node_id(&self) -> Result<myko_federation::NodeId, NodeError> {
        self.inner.node_id()
    }

    fn storage_incarnation(&self) -> Result<StorageIncarnationId, NodeError> {
        self.inner.storage_incarnation()
    }

    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
        self.inner.replay()
    }

    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError> {
        self.inner.append(event)?;
        if matches!(
            &event.event,
            NodeEvent::CommandLifecycle(CommandSnapshot {
                state: CommandState::AuthorizationPrepared { .. },
                ..
            })
        ) && !self.failed.swap(true, Ordering::AcqRel)
        {
            return Err(NodeError::Backend(
                "ambiguous prepared append after durable write".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CountingPermitPolicy {
    effect_decisions: Arc<AtomicUsize>,
}

impl AccessPolicy for CountingPermitPolicy {
    fn authorize(&self, _request: &AccessAttempt) -> Result<(), String> {
        Ok(())
    }

    fn decide(&self, request: &AccessAttempt) -> AuthorizationDecision {
        if request.authorization_phase == AuthorizationPhase::Effect {
            self.effect_decisions.fetch_add(1, Ordering::AcqRel);
        }
        AllowAllAccessPolicy.decide(request)
    }
}

#[test]
fn tampered_prepared_effect_digest_replays_as_corrupt_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.redb");
    let scratch_path = directory.path().join("scratch.redb");
    let (source, source_journal) = RedbJournal::open_node_with_journal(&source_path)?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    source.set_command_access_policy(Arc::clone(&policy))?;
    let command_id = CommandId::new();
    let execution = executing(&source, command_id)?;
    let prepared_effect = effect(&execution, b"valid before tamper")?;
    source.prepare_authorization(command_id, prepared_effect)?;

    let mut events = source_journal.replay()?;
    let tampered = tamper_prepared_effect_digest(prepared_event(&events)?)?;
    if !matches!(
        &tampered.event,
        NodeEvent::CommandLifecycle(CommandSnapshot {
            state: CommandState::AuthorizationPrepared { .. },
            ..
        })
    ) {
        return Err("tampered prepared event no longer deserialized as prepared".into());
    }
    for event in &mut events {
        if matches!(
            &event.event,
            NodeEvent::CommandLifecycle(CommandSnapshot {
                state: CommandState::AuthorizationPrepared { .. },
                ..
            })
        ) {
            *event = tampered.clone();
        }
    }

    let scratch = RedbJournal::open(&scratch_path)?;
    for event in &events {
        scratch.append(event)?;
    }
    if scratch.replay()?.len() != events.len() {
        return Err("scratch journal did not retain the tampered envelopes".into());
    }
    drop(scratch);
    let Err(NodeError::CorruptHistory(message)) = RedbJournal::open_node(&scratch_path) else {
        return Err("tampered prepared effect was exposed instead of rejected on replay".into());
    };
    if !message.contains("prepared command effect digest mismatch") {
        return Err(format!("unexpected corrupt history message: {message}").into());
    }
    Ok(())
}

#[test]
fn ambiguous_prepared_append_does_not_authorize_until_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("ambiguous.redb");
    let real_journal = Arc::new(RedbJournal::open(&path)?);
    let failing_journal = Arc::new(FailingPreparedAppendJournal::new(Arc::clone(&real_journal)));
    let node = Node::from_journal(failing_journal)?;
    let effect_decisions = Arc::new(AtomicUsize::new(0));
    let policy: Arc<dyn AccessPolicy> = Arc::new(CountingPermitPolicy {
        effect_decisions: Arc::clone(&effect_decisions),
    });
    node.set_command_access_policy(Arc::clone(&policy))?;

    let command_id = CommandId::new();
    node.submit(request(
        &node,
        command_id,
        "prepared-effect-integrity:ambiguous",
    ))?;
    let TypedCommandAdmission::Execute(context) = node.begin_command(command_id)? else {
        return Err("new command was not executable".into());
    };
    let result = b"ambiguous append result".to_vec();
    let Err(NodeError::Backend(message)) = context.commit_bytes(result.clone()) else {
        return Err("ambiguous prepared append did not surface the append error".into());
    };
    if !message.contains("ambiguous prepared append") {
        return Err(format!("unexpected append error: {message}").into());
    }
    if effect_decisions.load(Ordering::Acquire) != 0 {
        return Err("effect policy ran after an ambiguous prepared append".into());
    }
    let parked = real_journal
        .replay()?
        .into_iter()
        .find_map(|event| match event.event {
            NodeEvent::CommandLifecycle(command)
                if command.request.id == command_id
                    && matches!(command.state, CommandState::AuthorizationPrepared { .. }) =>
            {
                Some(command)
            }
            _ => None,
        })
        .ok_or("durable prepared command was not retained")?;
    let prepared = prepared_effect_from(&parked)?.clone();
    if prepared.result() != result {
        return Err("prepared body retained a different result".into());
    }
    drop(node);
    drop(real_journal);

    let reopened = RedbJournal::open_node(&path)?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    reopened.set_command_access_policy(Arc::clone(&policy))?;
    let recovered = match reopened.begin_command(command_id)? {
        TypedCommandAdmission::Resume(command) => command,
        TypedCommandAdmission::Execute(_) => {
            return Err("ambiguous prepared append reopened as executable".into());
        }
    };
    if !recovered.state.is_committed() || recovered.result.as_deref() != Some(prepared.result()) {
        return Err("reopen did not commit the exact prepared body".into());
    }
    let committed = reopened
        .events_after(None)?
        .into_iter()
        .find_map(|event| match event.event {
            NodeEvent::CommandCommitted { command, batch } if command.request.id == command_id => {
                Some((command, batch))
            }
            _ => None,
        })
        .ok_or("prepared body did not commit after reopen")?;
    if &committed.1 != prepared.batch() || committed.0.result.as_deref() != Some(prepared.result())
    {
        return Err("committed event differed from the prepared body".into());
    }
    Ok(())
}
