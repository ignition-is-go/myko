# Candidate A: one evidence-bearing command lifecycle

## Problem

Keep `CommandSnapshot` as the durable source of truth, but make replication states
mean something stronger than the current raw counts. A milestone wait must resume
the same command, survive lost replies and restarts, and succeed immediately when
already satisfied. The design must reuse selected-history manifests and exact
journal inclusion checks without claiming that a frozen local cut proves
completeness, current availability, or failover safety.

## Usage (caller's view)

The caller chooses the milestone and required holders; no helper hides a default.

```rust
let admitted = client.submit_typed(CreateProject { name }).await?;
let local = admitted.wait(CommandMilestone::CommittedLocally).await?;
render(local.output);

let requirement = ReplicationRequirement::all_of([
    placed_holder(east_id, east_authn),
    placed_holder(west_id, west_authn),
])?;
let durable = client.await_typed_command::<CreateProject>(
    admitted.command_id,
    CommandMilestone::RetainedBy(requirement),
).await?;
assert_eq!(durable.command_id, admitted.command_id);

// Cancellation drops only this wait. Reusing the ID never resubmits the command.
let recovered = client.await_typed_command::<CreateProject>(
    command_id, CommandMilestone::RetainedBy(saved_requirement)).await?;
```

```rust
let statement = holder.retain_and_attest(target, authenticated_source).await?;
owner.record_retention(statement)?; // duplicate evidence is an idempotent no-op
```

## Shape

```rust
pub enum CommandMilestone {
    CommittedLocally,
    RetainedBy(ReplicationRequirement),
    Reconciled,
}
pub struct ReplicationRequirement { required: NonEmptySet<RequiredHolder> }
pub struct RequiredHolder { holder: NodeId, authentication: HolderAuthPolicy }
pub struct RetentionTarget {
    command_id: CommandId,
    batch_id: BatchId,
    manifest: SelectedHistoryManifest,
    obligation: EventId,
}
pub enum ReplicationProgress {
    NotStarted,
    Retaining { target: RetentionTarget },
    Delayed { target: RetentionTarget, reason: String },
    Evidenced {
        target: RetentionTarget,
        statements: BTreeMap<NodeId, RetainedStatementEventId>,
    },
}
pub struct CommittedCommand {
    batch_id: BatchId,
    position: EventId,
    replication: ReplicationProgress,
    reconciliation: Option<Reconciliation>,
}
pub enum CommandState {
    /* existing pre-commit and failure variants */
    Committed(CommittedCommand),
}
impl CommandSnapshot {
    pub fn reached(&self, milestone: &CommandMilestone)
        -> Result<bool, CommandMilestoneError>;
}
pub trait CommandWatchingClient: CommandClient {
    fn await_typed_command<C: MykoCommand>(
        &self, id: CommandId, milestone: CommandMilestone,
    ) -> TypedMilestoneFuture<'_, C::Output, Self::Error>;
}
pub trait DurableRetentionHolder {
    fn retain_and_attest(
        &self, target: RetentionTarget, peer: AuthenticatedPeer,
    ) -> RetentionFuture<'_, SignedRetainedHistoryStatement, NodeError>;
}
impl CommandOwner {
    pub fn record_retention(&self, statement: SignedRetainedHistoryStatement)
        -> Result<CommandSnapshot, NodeError>;
}
```

`RetentionTarget` is constructed once from the committed batch's frozen selected
manifest and named obligation. The holder validates policy at the boundary, then
uses existing `record_retained_history_statement`: it matches holder,
incarnation, selection, and commitment; verifies exact journal inclusion; and
durably appends the signed statement. A cache, cursor, connection, or count is
not evidence. The owner validates signature, obligation, and required-holder
eligibility, then advances the lifecycle using the recorded statement event ID.
The signed control event remains the single evidence record; replay scans it to
repair a crash between statement recording and lifecycle advancement. The map
makes duplicate delivery idempotent; conflicting holder evidence is an error.

`reached` is a pure predicate over durable state. `RetainedBy` compares named
required holders with matching statements, never `acknowledged_replicas`. Watches
evaluate `current()` before `recv()`, preserving already-reached and gap-free
behavior. Reconciliation stays orthogonal: visibility cannot satisfy retention.
This keeps a small public surface while hiding subscription races, decoding,
evidence validation, and replay behind the client and owner, per boundary
discipline and interface depth.

## Module map

- `command.rs`: lifecycle aggregate, milestone/requirement/evidence types, pure predicate.
- `node.rs`: wait loop, target construction, evidence linkage and watch publication.
- `memory.rs`: existing durable statement validation/recording boundary.
- `selected.rs`, `commitment.rs`, `attestation.rs`: manifest, commitment, statement.
- transport adapters: authenticate peers and parse signed wire statements.

## Synthesis decision

Arena synthesis pending. This candidate deliberately retains one aggregate and
strengthens its replication branch rather than adding a second status store.

## Tradeoffs accepted

- We accept evidence event references in snapshots in exchange for replayable proof.
- We accept caller-supplied holder policy in exchange for avoiding an accidental default.
- We accept that a statement records past durable inclusion, not current availability.
- We accept manifest limits in exchange for reusing the existing exact-body verifier.

## Alternatives considered

- A separate replication-status service lost: it exposes joins and consistency races to callers.
- A separate evidence query beside unchanged `CommandState` lost: two truths can disagree.
- Reusing `Replicated { counts }` lost: it hides holder identity and cannot prove durability.

## Open questions and risks

- Which authority issues `HolderAuthPolicy`, and how is its rotation represented?
- Must statements detect restoration rollback beyond `StorageIncarnationId`?
- Which selected events form the command target when one batch spans nested scopes?
- How are old statements compacted without erasing auditability?
- When may projections publish `Current`? This design intentionally does not answer it.

## Verification and next implementation step

First implement the pure `CommandSnapshot::reached` predicate plus serialization
tests for named requirements and duplicate/conflicting statements. Add a restart
test using a journal-backed snapshot: a pre-recorded matching signed statement
satisfies immediately, a raw legacy count never does, and dropping/restarting the
wait causes no submission. This proves milestone semantics and replay only; it
does not yet prove authenticated transport, placement policy,
fresh majority confirmation, continuing custody, or failover `Current`.
