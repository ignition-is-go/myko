# Candidate C: quorum-durable history boundary

## Problem

`ExecutionAssignmentCoordinator::observe` produces a fresh, authenticated control observation, but `ExecutionAssignmentObservation` binds only realm, operation, and predecessor. `SelectedHistoryManifest` and `RetainedHistoryCommitment` prove a node's closed event set, not remote completeness or durability. A replacement must therefore obtain a new control decision and prove the exact application boundary that decision is allowed to serve, without inventing per-read quorum or treating a local cursor as portable.

## Usage (caller's view)

The retained owner keeps the same public subscription handle:

```rust
let sub = client.subscribe(request).await?; // no Current until startup proof
sub.next().await?;                          // State is a certified boundary
```

The node startup path is explicit:

```rust
let guard = node.startup_guard();
let current = HistoryBoundaryCoordinator::recover(&node, &assignment).await?;
node.publish_current(current)?; // atomically releases the guard
```

And command admission uses the same capability:

```rust
node.prepare(request)?.require_current()?.submit()?;
```

## Shape

Add the generic, framework-owned types in `federation::history`:

```rust
pub struct HistoryBoundary {
    pub realm: ScopeId,
    pub selection: ScopeSelection,
    pub frontier: HistoryFrontier, // coordinator-assigned monotone boundary
    pub commitment: RetainedHistoryCommitment,
}
pub struct DurableCoverage { pub node: NodeId, pub boundary: HistoryBoundary,
    pub manifest: SelectedHistoryManifest, pub signature: Signature }
pub struct CurrentHistory { boundary: HistoryBoundary, coverage: Vec<DurableCoverage> }
```

`HistoryFrontier` is a persisted coordinator value tied to accepted command/event identities, never an observer-local `LogPosition`. A node can construct a `DurableCoverage` only after reading and durably retaining every event through that frontier, then signing the resulting manifest commitment; it cannot sign caller-supplied hashes. The coordinator owns `recover(node, assignment) -> Result<CurrentHistory, StartupError>` in `core::server::history_boundary` and uses a native `ScopedRetainedEvidenceEndpoint` to fetch manifests/coverage from assigned peers. It first calls the existing fresh `observe`, then validates the selected assignment, boundary predecessor, scope, commitment, and a configured coordinator-majority of independently stored coverage records. The accepted boundary record and votes are persisted in the existing control realm (no parallel store).

At write acceptance, `PreparedCommand::submit` delegates to a coordinator-owned `submit_at_boundary`: event append, boundary advancement, and quorum durable coverage are one idempotent acceptance protocol. If quorum coverage cannot be persisted, the command is not accepted (it remains retryable), so a later startup cannot claim it. `CurrentHistory` has a private constructor and is the only input to `node.publish_current`; `follow_handler` and command admission require it. `NodeStartupGuard` remains held during recovery, and failure yields desync, blocks dependent commands, and emits no intermediate `Current`. Existing durable handler reconnect then retries the same request through the connector after a new certified state.

The public surface hides transport retries, manifest reconstruction, vote validation, and quorum accounting; callers see one startup recovery operation and one unforgeable current capability. This keeps typed projections above `federation` and storage-only peers opaque.

## Synthesis decision

This candidate chooses the smallest sound integration: make acceptance’s history boundary quorum-durable, then reuse portable manifests and durable control for failover. It deliberately does not upgrade the existing observation payload into an application certificate.

## Tradeoffs accepted

- We accept rejecting writes during a lost majority in exchange for never declaring an incompletely replicated write current.
- We accept a persisted boundary/vote record in the control realm in exchange for no second replication system.
- We accept one startup catch-up before publication in exchange for retaining the same client handle.

## Alternatives considered

Fenced transfer from the dead executor (or a hot standby) could copy a final manifest and receive a handoff fence, but it exposes transfer-session state and cannot complete when the authority is unavailable. A startup quorum merely signing each node’s local commitment loses on the A-only accepted-write counterexample: B and C agree on identical incomplete sets. Under this shape that write was never accepted, because B/C could not supply durable coverage; absent a prior boundary certificate, recovery returns desync.

## Open questions and risks

- Should the operator require a strict majority of all assigned executors, or a separately configured durability quorum?
- Is the coordinator-assigned frontier allowed to include control events, or only application events in the selected scope?
- What retention/garbage-collection rule preserves every event needed by an outstanding boundary?

## Next implementation step

Add `HistoryBoundary`/`DurableCoverage` and a failing native three-node fixture proving an A-only append cannot produce `Current`, then wire the existing persistent backend’s accepted-command path to create the first quorum boundary.
