# Candidate A: quorum-accepted application frontier

## Problem

Assignment freshness is not application-history freshness. Today `ExecutionAssignmentCoordinator::observe` chooses a fresh no-change control transition, while `ExecutionAssignmentObservation` binds only realm and control predecessor. `SelectedHistoryManifest` and `RetainedHistoryCommitment` describe a closed *local* event set, not remote completeness. Therefore `follow_handler` cannot safely turn a replacement executor's locally coherent snapshot into `Current`, and `PreparedCommand::submit` cannot safely accept a mutation whose dependencies came from that snapshot (`core/src/server/execution_coordinator.rs`, `federation/src/execution_assignment/observation.rs`, `federation/src/selected.rs`, `federation/src/commitment.rs`, `core/src/server/federated_session.rs`, `federation/src/node.rs`).

## Usage (caller's view)

```rust
let mut view = client.follow::<Orders>(scope).await?; // existing retained handle
assert!(view.current().liveness.is_resynchronizing());
// Connector retries assigned executors; only a certified boundary may publish Current.
view.changed().await?;
assert_eq!(view.current().liveness, SubscriptionLiveness::Current);

// A command prepared from the old or resynchronizing epoch is rejected.
client.submit(command.depends_on(view.current().cursor)).await?;
```

The server startup path is similarly narrow:

```rust
let certified = startup.recover_scope(scope, assignment_head).await?;
application.install(certified.history())?;
startup.release(certified)?; // enables handler Current and dependent mutation
```

## Shape

`federation/src/history_boundary.rs` owns transport-independent domain values:

```rust
struct AcceptedHistoryFrontier {
    scope: ScopeSelection,
    tips: BTreeSet<EventId>,       // causal antichain, not local positions
    commitment: RetainedHistoryCommitment,
    event_count: u64,
}
struct HistoryBoundaryProposal {
    assignment_head: ControlHead,
    predecessor: Option<HistoryBoundaryId>,
    frontier: AcceptedHistoryFrontier,
}
struct CertifiedHistoryBoundary {
    proposal: HistoryBoundaryProposal,
    votes: Vec<SignedHistoryBoundaryVote>,
}
fn verify_boundary(
    prior: Option<&CertifiedHistoryBoundary>,
    candidate: &CertifiedHistoryBoundary,
    controllers: &ControlContext,
) -> Result<VerifiedHistoryBoundary, BoundaryError>;
```

`core/src/server/history_boundary.rs` owns coordination:

```rust
trait HistoryBoundaryEndpoint {
    async fn attest(&self, request: AttestBoundaryRequest)
        -> Result<SignedHistoryBoundaryVote, BoundaryError>;
}
async fn recover_scope(
    &self, scope: ScopeSelection, assignment: ExecutionAssignmentsAtHead,
) -> Result<CertifiedStartupHistory, StartupDesync>;
```

Each controller independently reads its persistent opaque journal, verifies that every event named by the canonical manifest is retained, that its locally persisted accepted-write index has no accepted event omitted, and that `predecessor` is its last certified boundary. It signs the structured proposal, never a caller-supplied digest alone. Application types are unnecessary; envelope identity, causal parents, scope metadata, and bytes suffice. Reuse the node journal and control electorate/signing discipline; do not create an application query store.

The load-bearing invariant is write acceptance: an application write is externally `Accepted` only after a coordinator majority durably stores its envelope and an `AcceptedWriteRecord(scope,event,parents)`. The same electorate cannot later form a majority omitting it: quorum intersection includes a controller whose monotonic accepted-write index rejects regression. This directly defeats the counterexample. If A acknowledged before majority durability, the proposed certificate **cannot** prove completeness; startup must remain desynchronized. Making majority durability part of write acceptance is an operator decision, not existing behavior.

Recovery order is fixed: authenticate assignment and boundary requests; recover the last accepted boundary; fetch/import missing envelopes; locally validate causal closure and commitment; collect majority attestations extending the prior boundary; install application state; then emit one replacement `Current`. Any unavailable majority, unknown predecessor, omitted accepted record, missing body/dependency, commitment mismatch, assignment change, or persistence failure yields `StartupDesync`, keeps the startup gate held, emits only resynchronizing state, and rejects dependent commands. Retry is idempotent by boundary identity.

The public interface hides election, evidence refresh, catch-up, and manifest comparison behind `recover_scope`; callers retain the existing `MykoClient`/subscription handle. `IrohHandlerConnector` becomes assignment-aware internally rather than exposing peer selection (`core/src/client/durable_handler.rs`, `iroh/src/client.rs`).

## Tradeoffs accepted

- We accept quorum persistence latency on acknowledged writes in exchange for a boundary that proves no acknowledged write vanished.
- We accept a compact controller-side accepted-event index in exchange for storage nodes remaining application-opaque.
- We accept blocked availability after an unsafe legacy write in exchange for never manufacturing `Current`.

## Alternative considered

A fenced handoff from the existing executor could seal its history, transfer it, then authorize B. It avoids steady-state quorum write latency, but cannot recover invisibly when A dies before sealing and exposes fencing/authority lifecycle to more callers. It is viable only if the operator prefers planned-transfer availability over crash failover.

## Open questions and risks

- Will the operator redefine successful application write acceptance as majority-durable, including cross-scope atomic events?
- What is the bootstrap boundary for pre-protocol history, and may it require all controllers rather than a majority?
- Must every assigned executor be able to reconstruct event bodies from controllers, or may attestations reference a separately proven durable replica set?

## Next implementation step

Safe executable first step: add a server-owned startup/history gate checked by `follow_handler` and command submission so an unproven replacement can emit no `Current` and accept no dependent mutation. The full milestone additionally requires native Iroh boundary messages, durable accepted-write/frontier records, quorum recovery, catch-up, and assignment-aware reconnect on the same handle.
