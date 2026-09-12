# Candidate B: assigned handler failover

## Problem

The first milestone is one stable reactive handler that survives loss of its
serving node and reconnects only through another exact assigned executor whose
handler contract is compatible and whose selected history is ready for the
handler's prior visible state. Existing pieces cover parts of this but not the
whole path: retained handler owners keep one public handle and retry recoverable
opens, `ExecutionAssignmentCoordinator::observe` writes a fresh control
observation and returns historical assignments, `connect_described` binds an
open to the descriptor that was inspected, and command admission rejects locally
incomplete selected history. None of those is a lease, a global completeness
proof, a schema compatibility rule, or a storage routing contract.

## Usage

Applications keep the existing reactive API. The client is constructed with a
routing-aware connector instead of a fixed-peer connector:

```rust,ignore
let connector = AssignedHandlerConnector::new(
    base_iroh_connector,
    AssignmentObserver::new(control_coordinator),
    HandlerCompatibility::generated(),
    HandlerCatchup::selected_history(),
);
let client = MykoClient::with_handler_connector(Arc::new(connector));

let live = client.follow_view_reactive(&TasksInWorkspace { workspace })?;
```

The important caller property is that the request is frozen before routing:

```rust,ignore
let request = client.prepare_handler_request(&view).await?;
let handle = client.follow_prepared_view_reactive::<Task>(request)?;
```

`prepare_handler_request` resolves the report/view source selectors once. A
later serving-node switch may change the executor, but it never recalculates
`source_node`, `scope_id`, `service_id`, `handler_id`, or parameters from the
new executor identity.

A command path uses the same gate at the serving node:

```rust,ignore
session.submit(principal, presentation, submission, send).await?;
```

Before `prepare_command`, the session checks that its own node is still in a
fresh exact assignment observation for the command's exact scope and service.
Then the existing durable submission path performs authorization and incomplete
scope-history rejection at the one local history order.

## Shape

```rust,ignore
pub struct PreparedHandlerRequest { request: HandlerRequest, selection: ScopeSelection, service: ServiceId }
pub struct AssignedHandlerConnector<C> {
    inner: C,
    observer: Arc<dyn AssignmentObserver>,
    compatibility: Arc<dyn HandlerCompatibility>,
    catchup: Arc<dyn HandlerCatchup>,
}
pub struct RouteAttempt { request: HandlerRequest, previous: Option<HandlerCatchupTicket> }
pub struct ReadyExecutor {
    node: NodeId,
    observed_head: ControlHead,
    contract: HandlerContract,
    catchup: HandlerCatchupTicket,
}

pub trait AssignmentObserver: Send + Sync {
    async fn observe(&self) -> Result<ExecutionAssignmentsAtHead, HandlerClientError>;
}

pub trait HandlerCompatibility: Send + Sync {
    fn accepts(&self, request: &HandlerRequest, observed: &HandlerContract)
        -> Result<(), HandlerClientError>;
}

pub trait HandlerCatchup: Send + Sync {
    async fn require_ready(
        &self,
        node: NodeId,
        request: &HandlerRequest,
        previous: Option<&HandlerCatchupTicket>,
    ) -> Result<HandlerCatchupTicket, HandlerClientError>;
}
```

`PreparedHandlerRequest` is the first boundary type. It encodes that reports and
views derive data-origin selectors before routing, per boundary discipline. It
does not contain an executor. `AssignedHandlerConnector` owns candidate selection
and hides observe, describe, compatibility, and catch-up checks behind the
existing `HandlerConnector::connect` surface. `ReadyExecutor` is private to the
connector; callers receive only an initial `HandlerFrame::State` and connection.

Selection flow: freeze the `HandlerRequest`; call `observer.observe()` once for
the route attempt; require `assignments.exact(scope, service)` to be
`Some(non_empty)`; describe each assigned candidate with the unchanged request;
check directional compatibility outside `HandlerContract::validate_for`;
require portable catch-up evidence; then open with
`connect_described(request, observed_contract)`. `None` assignment, empty
assignment, `None` service, and `None` scope fail explicitly rather than becoming
global, inherited, or storage-placement policy.

The reactive owner keeps its old value while reconnecting. It may publish
`Resynchronizing` after transport loss. It must not publish a new `Current`
snapshot from a replacement executor until the replacement has passed assignment,
compatibility, catch-up, and described-open checks. This preserves the public
handle and avoids an intermediate "current" value that is only current for the
wrong executor or the wrong cut.

Portable catch-up is the unresolved semantic center. A `LogPosition` from the
old serving node cannot be compared with the replacement node's local cursor.
Closed manifests and commitments are useful evidence, but existing commitments
do not prove global completeness or set inclusion. The implementable shape is a
new portable ticket built from selected immutable event identities and content:

```rust,ignore
pub struct HandlerCatchupTicket {
    selection: ScopeSelection,
    commitment: RetainedHistoryCommitment,
    origins: BTreeSet<EventId>,
}
```

The serving node emits or makes available a ticket for each `Current` state. A
replacement executor is ready only when it can build a manifest for the same
selection, verify the previous ticket's event origins and event content are
present, and then issue its own ticket for the new state. This compares event
identity and content, not observer-local positions or unrelated local cursors.
If this origin-list ticket is too large, the open question is the accumulator
format; the milestone must not claim readiness from a digest-only commitment.

Serving-node responsibility:

```rust,ignore
pub trait ExecutionUseGate: Send + Sync {
    async fn require_serving_assignment(&self, serving_node: NodeId, scope: &ScopeId, service: &ServiceId) -> Result<(), AuthorizationFailure>;
}
```

`FederatedSession` calls this gate before inspected handler open and before new
command submission. The gate obtains a fresh assignment observation for that
use. The client-side connector improves routing and recovery, but it is not the
authority boundary. Storage-only peers remain opaque evidence sources; they are
never treated as typed executors or gateways unless they are exact assigned
executors for the requested service.

Observation-triggered publication feedback is limited by event choice. Handler
data frames do not trigger assignment observations. Observations happen only at
route/open/use boundaries and write control history, which may later appear in
retained projections. Seeing that control history does not recursively trigger
another observation. Ongoing stream revocation can be added later by watching
control history and closing the stream; it is not represented as a lease here.

## Synthesis decision

Candidate B chooses a client routing connector plus a serving-node use gate. A
client-only selector is too shallow because a stale client could open an
unassigned node, and a gateway-owned selector would make storage/forwarding
authority part of typed execution. The split keeps UX retry work on the client
and the assignment decision on the executor that is about to serve or admit work.

## Tradeoffs accepted

- We accept an explicit unsupported error for global or inherited compute policy
  in exchange for not inventing operator-intent, nested, or global semantics.
- We accept a new portable catch-up ticket in exchange for avoiding local cursor
  comparison and false digest-inclusion claims.
- We accept observing at open/use boundaries only in exchange for avoiding a
  self-feeding loop where every published retained cut writes another control
  observation.
- We accept that assignment can change immediately after the use check in
  exchange for not pretending observations are leases.

## Alternatives considered

- Client-only assigned routing: hides reconnect complexity, but exposes the
  real authority decision to clients and cannot protect command submission or
  malicious direct opens.
- Gateway-owned forwarding: hides candidate selection from clients, but exposes
  gateway trust, forwarding provenance, and storage routing as execution
  concerns. It also risks making storage holders typed gateways.
- Commitment-only catch-up: has a tiny interface, but hides the wrong
  complexity. Existing commitments prove one closed selected set, not inclusion
  of an older visible set in a replacement executor's retained history.

## Open questions and risks

- What exact portable accumulator should replace the naive `origins` set if
  handler-selected history is large?
- Where should generated client-side expected schemas come from for
  compatibility when the client is not itself an activated application build?
- Should assignment change during an already-open inspected stream close the
  stream immediately, or is use-time checking at open/reconnect enough for this
  milestone?

## First code unit

Build `PreparedHandlerRequest` and refactor `follow_report`, `follow_view`, and
their reactive forms so the `HandlerRequest` is constructed once before
connection retry. Add focused tests proving a reconnecting report/view preserves
the original `source_node` and `scope_id` when the connector's serving target
changes. This contributes directly to assigned failover without depending on the
unresolved portable catch-up accumulator.

## Evidence

Current source read: `durable_handler.rs` retains one connector/request/writer
after a successful open, but initial reactive retries rebuild reports/views;
`iroh/src/client.rs` exposes `describe` and `connect_described` as a precondition
only; `execution_coordinator.rs` fresh `observe()` chooses control history, while
`execution_assignment.rs`, `selected.rs`, `commitment.rs`, and
`command_scope_readiness.rs` explicitly limit assignment, manifest, commitment,
and command-readiness claims.
