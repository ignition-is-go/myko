# Candidate B: result plus evidence

## Problem

Command callers need one thing to watch, but two independent facts can change:
the execution outcome and the retained-history evidence later observed for that
command. Today `typed_completion` ends when a result is available, while
`Replicated` counts and `Reconciled` visibility do not prove durable holder
retention. This design keeps one observable command result and splits those axes
inside it, using existing retained-history statements instead of inventing a
parallel acknowledgment store.

## Usage

```rust
let observed = node.exec_typed_command_until(
    PutRecord { key, value },
    CommandMilestone::Executed,
).await?;
let output: PutRecordOutput = observed.output;

let durable = node.await_typed_command_until::<PutRecord>(
    command_id,
    CommandMilestone::Evidence(CommandEvidenceRequirement {
        obligation,
        selection,
        holders: required_holders,
    }),
).await?;
durable.evidence.require_statements(&required_holders, obligation)?;

let mut watch = node.watch_command_at(source, command_id).await?;
loop {
    let snapshot = watch.current();
    if snapshot.milestone_state(&wait_for)?.is_reached() {
        return snapshot.typed_observation::<PutRecord>()?
            .ok_or(NodeError::PendingCommand);
    }
    watch.recv().await?;
}
```

## Shape

```rust
pub struct CommandSnapshot {
    pub request: CommandRequest,
    pub outcome: CommandOutcome,
    pub evidence: CommandEvidence,
    pub updated_at: EventId,
}
pub enum CommandOutcome {
    Pending(CommandState),
    Succeeded { batch_id: BatchId, position: EventId, result: Vec<u8> },
    Rejected { reason: String },
    Cancelled { reason: String },
}
pub struct CommandEvidence {
    pub retained_statements: BTreeMap<NodeId, SignedRetainedHistoryStatement>,
    pub visibility: Option<Reconciliation>,
}
pub struct CommandEvidenceRequirement {
    pub obligation: EventId,
    pub selection: ScopeSelection,
    pub holders: BTreeSet<NodeId>,
}
pub enum CommandMilestone { Executed, Evidence(CommandEvidenceRequirement), Visible }
pub enum CommandMilestoneState { Waiting, Reached, Failed(NodeError) }
pub struct TypedCommandObservation<T> {
    pub output: T,
    pub outcome: CommandOutcomeSummary,
    pub evidence: CommandEvidence,
}
impl CommandSnapshot {
    pub fn typed_observation<C: MykoCommand>(
        &self,
    ) -> Result<Option<TypedCommandObservation<C::Output>>, NodeError>;
    pub fn milestone_state(&self, requirement: &CommandMilestone)
        -> Result<CommandMilestoneState, NodeError>;
}
pub trait CommandWatchingClient: CommandClient {
    fn exec_typed_command_until<C>(
        &self, command: C, wait_for: CommandMilestone,
    ) -> TypedCommandClientFuture<'_, TypedCommandObservation<C::Output>, Self::Error>
    where Self: Sized, C: MykoCommand;
    fn await_typed_command_until<C>(
        &self, command_id: CommandId, wait_for: CommandMilestone,
    ) -> TypedCommandClientFuture<'_, TypedCommandObservation<C::Output>, Self::Error>
    where Self: Sized, C: MykoCommand;
}
```

`CommandOutcome` owns local execution. `CommandEvidence` owns observed framework
evidence. A command may succeed with no retained statements, later gain
statements, or become visible without satisfying a holder requirement. Milestone
evaluation is pure over `CommandSnapshot`, so the existing gap-free watch still
owns admission races, lost replies, already-reached milestones, and duplicates.
Current `CommandState` variants can translate into the two axes while old fields
are migrated or kept as compatibility input.

Evidence recording reuses the existing foundation: `RetainedHistoryStatement`
binds holder, storage incarnation, obligation, selection, and commitment.
Iroh signing verifies the exact statement against an independently trusted node
descriptor, not membership, persistence, availability, or custody. Recording
already requires a durable journal, checks holder/incarnation/selection/
commitment, calls `verify_retained_history`, deduplicates, and appends a
`FrameworkControlEvent`. Candidate B only teaches command snapshots to observe
those statements; it does not add another persistence or replication-ack path.

Invariants:

- `Succeeded` is the only outcome with decoded output.
- Evidence waits require exact holder inclusion for the obligation and selection.
- Duplicate statements are idempotent; conflicts fail evidence, not outcome.
- `Visible` is separate; `Reconciled` never implies holder retention.
- `Executed` preserves current local-result behavior when selected explicitly.

## Synthesis decision

Candidate B is the split-axis candidate. It accepts a richer snapshot so callers
make one request, watch one stream, and name one milestone, while the framework
owns decoding, statement observation, duplicate handling, and restart recovery.
It rejects treating `Replicated` counts as evidence because current sources do
not authenticate a durable holder behind those counts.

## Tradeoffs accepted

- We accept a snapshot schema change in exchange for not overloading lifecycle
  labels with durability facts.
- We accept explicit `CommandMilestone` in exchange for no mesh-wide default.
- We accept that `Executed` can return before failover safety in exchange for
  preserving valid local durable execution.
- We accept leaving custody, membership, authority, and freshness to the existing
  statement contract.

## Alternatives considered

- Make `Replicated` mean required evidence: smaller surface, but it exposes
  policy and authentication through a count and would fake durability.
- Add separate `await_replication` APIs: simpler implementation, but callers
  would compose result, selected history, holder policy, and retries themselves.
- Block `typed_completion` until evidence: hides policy in the default helper
  and breaks callers that only need local durable execution.

## Open questions and risks

- Which holders satisfy a command's evidence requirement: assigned executors,
  scope coordinators, custody issuers, or a caller-provided set?
- Should conflicting retained statements fail only that milestone, or also raise
  a node-health signal?
- Which obligation event should command waits reference for lifecycle evidence?
- What stronger future milestone proves continuing custody or freshness, beyond
  exact retained inclusion?

## Next implementation step

Add pure `CommandSnapshot::milestone_state` tests proving local success, exact
statement satisfaction, duplicate idempotence, conflict failure, and that raw
`Replicated` counts do not satisfy evidence.
