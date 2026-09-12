# Candidate C: immutable achieved-milestone evidence

## Problem

Command state names several phases, but those variants are not a durable total
order: replication may be delayed, reconciliation describes visibility, and
existing replica counts have no certified acknowledgment path. The framework
needs explicit waits for selected milestones while keeping one source of truth.
The journal already durably appends immutable events; `SelectedHistoryManifest`
and `EventJournal::verify_retained_history` can validate event bodies but do not
prove custody or currentness. Therefore milestone achievement is an immutable
journal fact, projected into a command view, not a flag inferred from a state
variant or a raw count.

## Usage (caller's view)

Callers name the milestone and (for replication) the immutable target:

```rust,ignore
let local = client.wait_for_milestone::<SetRecord>(
    command_id, CommandMilestone::LocalCommit,
).await?;
let target = ReplicationTarget::from_manifest(target_id, scope, required_holders);
let durable = client.wait_for_milestone::<SetRecord>(
    command_id, CommandMilestone::Replicated { target },
).await?;
```

An application resuming after a lost reply uses the same command ID and target;
an already-recorded achievement returns immediately and never submits again:

```rust,ignore
let durable = client.wait_for_milestone::<SetRecord>(id, milestone).await?;
```

The subscription implementation consumes lifecycle views; callers neither
collect acknowledgments nor inspect transport envelopes. Wait cancellation only
cancels the wait, not the command.

## Shape

```rust,ignore
pub enum CommandMilestone {
    LocalCommit,
    Replicated { target: ReplicationTarget },
}

pub struct ReplicationTarget {
    pub id: ReplicationTargetId,       // immutable policy/epoch identity
    pub scope: ScopeId,
    pub batch: BatchId,
    pub required_holders: BTreeSet<NodeId>, // supplied explicitly, no default
}

pub struct DurableHolderEvidence {
    pub holder: NodeId,
    pub accepted: EventId,
    pub body_digest: [u8; 32],
    pub authenticated_claim: HolderSignature,
}

pub struct AchievedMilestone {
    pub milestone: CommandMilestone,
    pub evidence: Vec<DurableHolderEvidence>,
    pub recorded_at: EventId,
}

pub struct CommandLifecycleView {
    pub snapshot: CommandSnapshot,
    pub achieved: BTreeMap<MilestoneKey, AchievedMilestone>,
}

pub fn project_command_lifecycle(
    events: impl IntoIterator<Item = EventEnvelope>,
) -> Result<CommandLifecycleView, NodeError>; // not implemented

pub trait CommandWatchingClient {
    fn wait_for_milestone<C: MykoCommand>(
        &self, id: CommandId, milestone: CommandMilestone,
    ) -> MilestoneFuture<'_, C::Output, Self::Error>;
}
```

`CommandMilestone` is a set of named predicates, deliberately without `Ord`.
An authenticated coordinator/holder boundary validates the target, batch/body
digest, and signatures before the source appends a `MilestoneAchieved` lifecycle
event to the existing `EventJournal`. Projection folds those events into a set;
duplicate `(command, milestone, holder)` evidence is idempotently ignored.
Replay reconstructs the same view after restart. Lost replies and reconnects
are harmless because the wait checks the current projection before receiving.
Evidence means “recorded as achieved,” not that the holder is currently
reachable. The target is frozen by its ID and does not silently select a
coordinator set or publish `Current`.

The public waiter hides decoding, admission/watch races, projection, and
verification. It exposes only a selected milestone and typed result, keeping
transport and persistence details behind the framework boundary. Existing
`typed_completion` remains the local-result convenience and is not relabeled as
replication durability.

## Synthesis decision

This candidate is intentionally the immutable-evidence alternative for arena
comparison. It favors a small, replayable fact set over an inferred enum order;
the eventual synthesis must decide which authenticated target protocol supplies
the evidence.

## Tradeoffs accepted

- We accept one more lifecycle event per achieved milestone in exchange for
  restart-safe, auditable evidence from the existing journal.
- We accept explicit target construction in exchange for refusing to invent a
  mesh-wide holder/default policy.
- We accept retaining achieved evidence after a holder disappears in exchange
  for distinguishing recorded achievement from present availability.

## Alternatives considered

- Treating `Replicated { acknowledged_replicas, required_replicas }` as ordered
  progress exposes transport/count semantics to callers and cannot prove durable
  holders; it hides the missing authentication and loses on restart.
- Persisting a separate milestone table gives convenient reads but creates a
  second source of truth and a crash-consistency problem; journal projection
  hides that machinery without adding storage.

## Open questions and risks

- Which authority authenticates `HolderSignature`, and what challenge/freshness
  prevents copied or replayed evidence from satisfying a new target?
- Which holders belong in a target, and when may a target be considered complete?
- Should a future milestone cover reconciliation or publication, without making
  either imply replication?

## Next implementation step

Add one journal-backed `MilestoneAchieved` event and a replay test proving exact
target/evidence validation, duplicate idempotence, and immediate recovery of an
already-achieved explicit `LocalCommit` wait; leave replication policy unset.
