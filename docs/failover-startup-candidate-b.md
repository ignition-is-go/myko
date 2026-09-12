# Candidate B: epoch-fenced authority transfer

## Usage (caller first)

Startup keeps the same public handle. The client still calls `connector.connect(request)` in
`core/src/client/durable_handler.rs`; internally the connector asks the assignment plane for an
executor that may serve epoch `E`:

```rust
let startup = startup_coordinator
    .recover_handler(&request, previous_revision)
    .await?;
let connector = base_connector.at(startup.executor());
let (state0, stream) = connector.connect(startup.open_request(request)).await?;
```

Server-side `FederatedSession::follow_handler` first requires a `StartupBoundary` before
`application.open_handler(...)`. If the boundary is missing or incomplete, it emits
`Resynchronizing` and never publishes an intermediate `Current`.

```rust
let gate = self.startup_gate.require_current(&principal, &handler).await?;
application.open_handler_at(&mut session, tx, handler, gate.epoch())?;
```

Writes through `Node::prepare_command(...).submit()` remain local backend appends, but the
executor records a `WriteFrontier` after each accepted append. A successor may serve only a
frontier transferred by the prior authority or covered by a previously certified boundary.

## Shape

Add `federation/src/execution_assignment/startup_boundary.rs`:

```rust
pub struct ExecutionEpoch { pub assignment_head: ControlHead, pub ordinal: u64 }
pub struct WriteFrontier { pub selection: ScopeSelection, pub commitment: RetainedHistoryCommitment }
pub struct StartupBoundary {
    pub realm: ScopeId,
    pub epoch: ExecutionEpoch,
    pub executor: NodeId,
    pub predecessor: Option<Box<StartupBoundaryId>>,
    pub frontier: WriteFrontier,
}

pub enum BoundaryAcceptance {
    PriorCertified { predecessor: StartupBoundaryId },
    TransferFromPriorExecutor { witness: SignedExecutorTransfer },
}
```

Add `core/src/server/startup_boundary.rs`:

```rust
#[async_trait::async_trait]
pub trait StartupBoundaryCoordinator: Send + Sync {
    async fn recover_handler(
        &self,
        request: &HandlerRequest,
        from: Option<HandlerStreamRevision>,
    ) -> Result<CertifiedStartupBoundary, StartupUnavailable>;
}

pub trait ExecutorTransferEndpoint: Send + Sync {
    fn prepare_transfer(&self, request: TransferPrepare) -> ControlFuture<'_, SignedExecutorTransfer>;
}
```

The control payload extends the current no-change observation rather than replacing it:
`StartupBoundaryTransition { operation, realm, assignment_predecessor, epoch, executor, frontier,
acceptance }`. Ownership stays with `federation/src/execution_assignment/*` for payload validation
and `core/src/server/execution_coordinator.rs` for quorum choice, matching today’s assignment
observer. `core/src/server/execution_control.rs` continues to authenticate controller callers; it
must not query application state.

Acceptance relation:

1. The selected assignment at `assignment_predecessor` names `executor` for the handler scope.
2. The control quorum chooses the boundary after recovering accepted assignment-control values, as
   `observe()` already does in `execution_coordinator.rs`.
3. The boundary’s `frontier` is accepted only if either it descends from the immediately prior
   certified boundary for that selection or the prior assigned executor signs a transfer witness.
4. A transfer witness is valid only if the prior executor’s durable store can prove no accepted
   command above that frontier exists for the selected scope at transfer time. This is a real
   executor/store contract, not a controller vote over caller-supplied hashes.

This addresses the A/B/C counterexample: if A accepted a write retained only on A, B and C may still
make a fresh assignment observation, but they cannot produce `TransferFromPriorExecutor` from A and
cannot derive `PriorCertified` past A’s private write. Their locally closed identical manifests are
only `SelectedHistoryManifest`/`RetainedHistoryCommitment` evidence; source says those prove local
closure and content identity, not remote completeness (`selected.rs`, `commitment.rs`). Result:
dependent commands block as desynced; no `Current`.

## Synthesis decision

Candidate B chooses fenced transfer to avoid adding a control-log round trip to every append. It is
viable only if command acceptance writes an executor-owned frontier index beside the durable command
append. The interface is deeper than a hash quorum but smaller for callers: handlers ask for a
`CertifiedStartupBoundary`, not for controller, store, and manifest details.

## Tradeoffs accepted

- We accept visible failover when the old executor is gone in exchange for not lying about history
  continuity.
- We accept an executor/store acceptance contract in exchange for avoiding per-append control quorum.
- We accept one new control payload family in exchange for reusing the existing control quorum and
  retained evidence refresh.

## Alternative considered

Quorum-durable frontier on every accepted command: `PreparedCommand::submit` would append only after
a coordinator majority retained the resulting frontier. It gives stronger invisible failover, but it
exposes coordinator availability on every mutation and turns application append latency into control
latency. It is the safer default if the operator requires invisible failover after total executor
loss.

## Open operator decision

Choose one guarantee: either approved availability means “hot standby/invisible only after prior
executor transfer or replicated frontier coverage,” or every accepted write must be quorum-durable
before acknowledgment. Candidate B implements the first.

## Next implementation step

Write a failing native-transport test beside `iroh/tests/execution_coordination/observations.rs`
where A accepts a private write, disappears, B/C observe assignment freshness, and startup refuses
`Current` without an A-signed or prior-certified frontier.
