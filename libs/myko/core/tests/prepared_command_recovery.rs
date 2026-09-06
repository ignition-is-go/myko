use std::{
    collections::BTreeSet,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub use myko::prelude;
pub use myko::*;

use hyphae::{Cell, CellImmutable, CellMutable, Mutable as _};
use myko_federation::{
    AccessAttempt, AccessPolicy, AllowAllAccessPolicy, ApplicationCapability, AuthorityConstraints,
    AuthorityUnavailable, AuthorizationDecision, AuthorizationPhase, CapabilityId, CommandId,
    CommandSnapshot, CommandState, Node, NodeEvent, PrincipalId,
};
use myko_redb::RedbJournal;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Default)]
struct ExecutionCounter {
    count: Arc<Mutex<u64>>,
}

impl ExecutionCounter {
    fn record(&self) -> Result<(), CommandError> {
        let mut count = self
            .count
            .lock()
            .map_err(|_| CommandError::retry("execution counter lock is poisoned"))?;
        *count = count.saturating_add(1);
        drop(count);
        Ok(())
    }

    fn get(&self) -> TestResult<u64> {
        let count = self
            .count
            .lock()
            .map_err(|_| "execution counter lock is poisoned")?;
        Ok(*count)
    }
}

#[myko_service(RecoveryRoot)]
pub struct RecoveryService;

#[myko_item(service = RecoveryService, scope_root)]
pub struct RecoveryRoot {
    label: String,
}

#[myko::myko_command(String, item = RecoveryRoot)]
pub struct RecoveryCommand {
    root: RecoveryRootId,
    label: String,
}

impl CommandHandler for RecoveryCommand {
    fn scope(&self, _node_id: myko_federation::NodeId) -> RecoveryRootId {
        self.root.clone()
    }

    fn required_capabilities(&self) -> Vec<CapabilityId> {
        vec![execution_capability_id()]
    }

    fn execute(self, context: CommandContext) -> Result<String, CommandError> {
        context
            .resource::<ExecutionCounter>()
            .map_err(|error| CommandError::retry(error.to_string()))?
            .record()?;
        context.emit_set(&RecoveryRoot {
            id: self.root,
            label: self.label.clone(),
        })?;
        Ok(format!("result:{}", self.label))
    }
}

struct EffectGate {
    blocked: Mutex<BTreeSet<CommandId>>,
    recovered: AtomicBool,
    revision_writer: Cell<u64, CellMutable>,
    revision: Cell<u64, CellImmutable>,
}

impl std::fmt::Debug for EffectGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectGate")
            .field("recovered", &self.recovered.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl EffectGate {
    fn new() -> Self {
        let writer = Cell::new(0_u64).with_name("prepared-command-recovery-policy-revision");
        let revision = writer.clone().lock();
        Self {
            blocked: Mutex::new(BTreeSet::new()),
            recovered: AtomicBool::new(false),
            revision_writer: writer,
            revision,
        }
    }

    fn block(&self, command_id: CommandId) -> TestResult {
        self.blocked
            .lock()
            .map_err(|_| "blocked command set lock is poisoned")?
            .insert(command_id);
        Ok(())
    }

    fn recover(&self) {
        self.recovered.store(true, Ordering::Release);
        self.revision_writer.set(1);
    }

    fn blocks(&self, request: &AccessAttempt) -> Result<bool, AuthorityUnavailable> {
        if request.authorization_phase != AuthorizationPhase::Effect
            || self.recovered.load(Ordering::Acquire)
        {
            return Ok(false);
        }
        let AccessTarget::KnownCommand { command_id, .. } = &request.target else {
            return Ok(false);
        };
        let blocked = self
            .blocked
            .lock()
            .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?;
        Ok(blocked.contains(command_id))
    }
}

impl AccessPolicy for EffectGate {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        if self.blocks(request)? {
            return Err(AuthorityUnavailable::CoordinationUnavailable);
        }
        AllowAllAccessPolicy.decide(request)
    }

    fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>> {
        Some(self.revision.clone())
    }
}

fn recovery_application(counter: ExecutionCounter) -> TestResult<MykoApplication> {
    MykoApplication::builder()
        .service::<RecoveryService>()
        .resource(execution_capability(), counter)
        .map(MykoApplicationBuilder::build)
        .map_err(Into::into)
}

fn execution_capability_id() -> CapabilityId {
    CapabilityId::new("test.prepared_command_recovery.execution_counter")
}

fn execution_capability() -> ApplicationCapability {
    ApplicationCapability {
        id: execution_capability_id(),
        description: "record command handler executions".to_owned(),
        constraints: AuthorityConstraints::default(),
    }
}

fn recovery_host(
    node: Node,
    policy: Arc<dyn AccessPolicy>,
    counter: ExecutionCounter,
) -> TestResult<ApplicationHost> {
    Ok(ApplicationHost::new(node, recovery_application(counter)?)?.with_access_policy(policy)?)
}

fn command(label: &str) -> RecoveryCommand {
    RecoveryCommand {
        root: RecoveryRootId::from(format!("root:{label}")),
        label: label.to_owned(),
    }
}

fn submit(host: &ApplicationHost, label: &str) -> TestResult<CommandId> {
    let submitted = host.submit_authenticated_command(
        PrincipalId::new(format!("principal:{label}")),
        &command(label),
    )?;
    Ok(submitted.request.id)
}

fn command_state(node: &Node, command_id: CommandId) -> TestResult<Option<CommandState>> {
    Ok(node.command(command_id)?.map(|command| command.state))
}

fn command_snapshot(node: &Node, command_id: CommandId) -> TestResult<CommandSnapshot> {
    node.command(command_id)?
        .ok_or_else(|| format!("command {command_id} was not retained").into())
}

fn wait_for_state(
    node: &Node,
    command_id: CommandId,
    accepts: fn(&CommandState) -> bool,
    label: &str,
) -> TestResult {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or("deadline overflow")?;
    loop {
        if command_state(node, command_id)?.is_some_and(|state| accepts(&state)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for command to become {label}").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

const fn is_prepared(state: &CommandState) -> bool {
    matches!(state, CommandState::AuthorizationPrepared { .. })
}

const fn is_committed(state: &CommandState) -> bool {
    state.is_committed()
}

fn assert_execution_count(counter: &ExecutionCounter, label: &str, expected: u64) -> TestResult {
    let actual = counter.get()?;
    if actual != expected {
        return Err(format!("command {label} executed {actual} times, expected {expected}").into());
    }
    Ok(())
}

fn expected_result(label: &str) -> TestResult<Vec<u8>> {
    Ok(serde_json::to_vec(&format!("result:{label}"))?)
}

fn assert_prepared_result(
    node: &Node,
    command_id: CommandId,
    label: &str,
) -> TestResult<myko_federation::PreparedCommandEffect> {
    let snapshot = command_snapshot(node, command_id)?;
    let CommandState::AuthorizationPrepared { effect } = snapshot.state else {
        return Err(format!("expected prepared command, found {:?}", snapshot.state).into());
    };
    let expected = expected_result(label)?;
    if effect.result() != expected.as_slice() {
        return Err("prepared effect retained the wrong result bytes".into());
    }
    if effect.batch().changes.is_empty() {
        return Err("prepared effect retained an empty mutation batch".into());
    }
    Ok(*effect)
}

fn assert_saved_effect_released_once(
    node: &Node,
    command_id: CommandId,
    saved: &myko_federation::PreparedCommandEffect,
) -> TestResult {
    let committed = node
        .events_after(None)?
        .into_iter()
        .filter_map(|event| match event.event {
            NodeEvent::CommandCommitted { command, batch } if command.request.id == command_id => {
                Some((batch, command.result))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if committed != vec![(saved.batch().clone(), Some(saved.result().to_vec()))] {
        return Err("recovery must append exactly one copy of the saved batch and result".into());
    }
    Ok(())
}

fn assert_committed_result(node: &Node, command_id: CommandId, label: &str) -> TestResult {
    let snapshot = command_snapshot(node, command_id)?;
    let (CommandState::CommittedLocally { batch_id, .. }
    | CommandState::Replicating { batch_id, .. }
    | CommandState::ReplicationDelayed { batch_id, .. }
    | CommandState::Replicated { batch_id, .. }
    | CommandState::Reconciled { batch_id, .. }) = snapshot.state
    else {
        return Err(format!("expected committed command, found {:?}", snapshot.state).into());
    };
    let expected = expected_result(label)?;
    if snapshot.result.as_deref() != Some(expected.as_slice()) {
        return Err("committed command retained the wrong result bytes".into());
    }
    let has_batch = node.events_after(None)?.into_iter().any(|event| {
        matches!(
            event.event,
            NodeEvent::CommandCommitted { batch, .. }
                if batch.id == batch_id && !batch.changes.is_empty()
        )
    });
    if !has_batch {
        return Err(format!("committed batch {batch_id} was not retained").into());
    }
    Ok(())
}

#[test]
fn drive_commands_recovers_prepared_effect_after_restart_without_manual_dispatch() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("prepared-restart.redb");
    let gate = Arc::new(EffectGate::new());
    let label = "restart";
    let counter = ExecutionCounter::default();

    let node = RedbJournal::open_node(&path)?;
    let policy: Arc<dyn AccessPolicy> = gate.clone();
    let host = recovery_host(node.clone(), Arc::clone(&policy), counter.clone())?;
    let command_id = submit(&host, label)?;
    gate.block(command_id)?;
    let guard = host.drive_commands()?;
    wait_for_state(&node, command_id, is_prepared, "authorization prepared")?;
    let saved = assert_prepared_result(&node, command_id, label)?;
    assert_execution_count(&counter, label, 1)?;
    drop(guard);
    drop(host);
    drop(node);

    gate.recover();
    let reopened = RedbJournal::open_node(&path)?;
    let recovery_host = recovery_host(reopened.clone(), policy, counter.clone())?;
    let recovery_guard = recovery_host.drive_commands()?;
    wait_for_state(
        &reopened,
        command_id,
        is_committed,
        "committed after restart",
    )?;
    assert_committed_result(&reopened, command_id, label)?;
    assert_saved_effect_released_once(&reopened, command_id, &saved)?;
    assert_execution_count(&counter, label, 1)?;
    drop(recovery_guard);
    drop(recovery_host);
    drop(reopened);
    drop(directory);
    Ok(())
}

#[test]
fn drive_commands_keeps_running_after_unavailable_prepared_effect() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("prepared-live.redb");
    let gate = Arc::new(EffectGate::new());
    let blocked_label = "blocked";
    let permitted_label = "permitted";
    let counter = ExecutionCounter::default();

    let node = RedbJournal::open_node(&path)?;
    let policy: Arc<dyn AccessPolicy> = gate.clone();
    let host = recovery_host(node.clone(), Arc::clone(&policy), counter.clone())?;
    let blocked_id = submit(&host, blocked_label)?;
    gate.block(blocked_id)?;
    let guard = host.drive_commands()?;
    wait_for_state(&node, blocked_id, is_prepared, "authorization prepared")?;
    let saved = assert_prepared_result(&node, blocked_id, blocked_label)?;
    assert_execution_count(&counter, blocked_label, 1)?;

    let permitted_id = submit(&host, permitted_label)?;
    if let Err(error) = wait_for_state(
        &node,
        permitted_id,
        is_committed,
        "independent command committed",
    ) {
        let state = command_state(&node, permitted_id)?;
        let failure = guard.failure();
        return Err(
            format!("{error}; current state: {state:?}; driver failure: {failure:?}").into(),
        );
    }
    assert_committed_result(&node, permitted_id, permitted_label)?;
    assert_execution_count(&counter, permitted_label, 2)?;
    if let Some(failure) = guard.failure() {
        return Err(format!("driver stopped after unavailable authority: {failure}").into());
    }

    gate.recover();
    wait_for_state(
        &node,
        blocked_id,
        is_committed,
        "committed after policy recovery",
    )?;
    assert_committed_result(&node, blocked_id, blocked_label)?;
    assert_saved_effect_released_once(&node, blocked_id, &saved)?;
    assert_execution_count(&counter, blocked_label, 2)?;
    drop(guard);
    drop(host);
    drop(node);
    drop(directory);
    Ok(())
}
