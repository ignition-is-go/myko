# Assigned durable handler: candidate A

## Problem

Keep one reactive handle coherent while its executor disappears and reconnect it
to another explicitly assigned executor that accepts the original request and
has portable evidence for the last published dependency set. The client owns
route selection; the serving node owns use-time assignment enforcement and
readiness. Existing assignment observations are momentary control-history
writes, contracts are exact open preconditions rather than compatibility, and
local cuts or closed local manifests do not prove global completeness.

## Usage (caller's view)

Application code does not select nodes or rebuild a query:

```rust,ignore
let orders = client.follow_view_reactive(OpenOrders {
    scope: shop.clone(),
    source_node: Some(origin),
})?;

// executor A disappears: the same handle keeps its value but becomes
// Resynchronizing. Commands derived from this handle fail while it is stale.
orders.command(CloseOrder { id }).await?;
```

Framework assembly supplies control and candidate discovery once:

```rust,ignore
let connector = AssignedHandlerConnector::new(
    base_connector,
    assignments,
    candidates,
    compatibility,
);
let client = MykoClient::with_handler_connector(connector);
```

## Shape

### Retained client route

```rust,ignore
pub(crate) struct AssignedHandlerConnector {
    transport: Arc<dyn HandlerConnector>,
    assignments: Arc<dyn AssignmentObserver>,
    candidates: Arc<dyn ExecutorCandidates>,
    compatibility: Arc<dyn HandlerCompatibility>,
}

pub(crate) struct HandlerIntent {
    request: HandlerRequest,       // immutable across every attempt
    service: ServiceId,
    exact_scope: ScopeId,
    accepted: ClientHandlerSchemas,
}

#[async_trait]
pub(crate) trait AssignmentObserver: Send + Sync {
    async fn observe_exact(
        &self, scope: &ScopeId, service: &ServiceId,
    ) -> Result<ObservedExecutorSet, HandlerClientError>;
}

impl AssignedHandlerConnector {
    async fn select_and_open(
        &self, intent: &HandlerIntent, required: Option<&PortableCatchUp>,
    ) -> Result<ConnectedHandler, HandlerClientError>;
}
```

`select_and_open` obtains a fresh assignment observation, considers only the
exact configured executor set, describes candidates, applies directional schema
compatibility, asks each candidate to prove readiness, then calls
`connect_described` against the same descriptor. No assignment, missing/empty
assignment, missing schema, incompatible schema, or absent readiness evidence
fails closed. Discovery only locates an assigned identity; storage custody and
service advertisement never confer eligibility.

`HandlerIntent.request` is constructed once. Its `source_node`, scope, handler
ID, parameters, and other origin selectors survive destination changes. In
particular, a report/view selector is never regenerated from the replacement
executor. The public handle owns one writer. On loss it publishes only
`Resynchronizing`, retains the last value, and swaps the private connection after
a ready initial state arrives. It never publishes the replacement's initial
state as `Current` before readiness verification.

### Portable catch-up boundary

```rust,ignore
pub(crate) struct PortableCatchUp {
    selection: ScopeSelection,
    required_events: Vec<EventEnvelope>,
    commitment: RetainedHistoryCommitment,
}

pub(crate) enum CandidateReadiness {
    CaughtUp { verified: PortableCatchUp },
    Waiting { reason: String },
}

#[async_trait]
pub(crate) trait HandlerReadinessEndpoint: Send + Sync {
    async fn import_and_verify(
        &self, request: &HandlerRequest, required: &PortableCatchUp,
    ) -> Result<CandidateReadiness, HandlerClientError>;
}
```

Every `Current` publication that may become a failover boundary carries the
exact immutable selected event bodies on which that value depends, plus their
existing portable commitment. The new node imports those bodies, validates the
selection, dependencies, event identities, and commitment, and waits until its
handler output is current with respect to that required set. It does not compare
`LogPosition`s from different nodes. A new local manifest is not compared to the
old commitment as set inclusion, and an empty projection proves nothing.

This evidence proves only continuity from the last value, not absence of newer
remote events. Full `Current` after failover additionally needs an authoritative
scope-completeness/currentness contract. No such API exists in the inspected
source. Until that semantic decision is implemented, the honest first milestone
may reconnect and catch up but must remain `Resynchronizing`; it cannot claim the
target's fully ready replacement. The required decision is: which authenticated
source(s) certify that the executor has all history relevant to this exact scope
and service at an identified portable frontier?

### Serving-node enforcement

```rust,ignore
pub(crate) trait ExecutionGuard: Send + Sync {
    async fn admit_open(&self, scope: &ScopeId, service: &ServiceId)
        -> Result<AssignmentEpoch, ExecutionDenied>;
    async fn revalidate(&self, epoch: &AssignmentEpoch)
        -> Result<(), ExecutionDenied>;
}

pub(crate) struct AssignmentEpoch {
    node: NodeId,
    scope: ScopeId,
    service: ServiceId,
    observed_head: ControlHead,
    effective_revision: EffectiveAssignmentRevision,
}
```

`FederatedSession::follow_handler` calls `admit_open` after authorization and
before `ApplicationHost::open_handler`. The guard freshly observes the exact
assignment and rejects a node absent from it. The stream revalidates only when
the projection's **effective assignment revision** changes. That revision is
derived from assignment-set transitions; observation records leave it unchanged.
Thus an observation write cannot trigger another check or another publication.
Application history projection must likewise treat observation records as
control metadata, not handler input progress.

The client check improves routing but never substitutes for this guard. The
server ends the stream before releasing another frame if revalidation fails.
There is no lease: `AssignmentEpoch` is retained only to detect a changed
assignment set, and revalidation makes a fresh observation.

Commands initiated through this reactive handle require its liveness to be
`Current`, and carry the serving scope/service identity to the command boundary.
The serving node performs the same fresh exact-assignment check immediately
before `prepare_command`; stale or unassigned executors reject the command. This
closes ordinary stale-handle use, but observation and application append are not
atomic. Strict exclusion of a command concurrent with reassignment requires a
fencing semantic (for example, an assignment generation accepted by the command
log) that does not exist today. The design does not claim that stronger property
until that decision is made.

The public application surface remains the existing reactive handle. Routing,
compatibility, evidence import, and retries are hidden behind one connector;
only framework assembly sees the four dependencies. Boundary code validates
wire/control data, while candidate filtering is a pure function.

## Synthesis decision

Candidate A chooses client-owned selection plus server-owned enforcement. A
gateway would introduce forwarding trust and make storage nodes look executable.
It also separates continuity evidence from currentness: exact event evidence is
implementable now, while global completeness remains an explicit prerequisite
for publishing `Current`.

## Tradeoffs accepted

- We accept transferring exact required event bodies in exchange for portable,
  inspectable continuity without cross-node cursor comparison.
- We accept a semantic assignment revision beside the retained control head in
  exchange for preventing observation-triggered feedback.
- We accept remaining `Resynchronizing` after catch-up until authenticated scope
  completeness exists, in exchange for never manufacturing readiness.

## Alternatives considered

- Gateway-owned routing hides selection but exposes a mandatory trusted hop and
  forwarding policy; it loses on both storage/execution separation and depth.
- Per-frame fresh observation appears strict, but each check writes history and
  can wake the very publication being checked; semantic-change revalidation
  hides that loop behind one server guard.

## Open questions and risks

- Which existing or new authenticated protocol certifies exact-scope history
  completeness at a portable frontier?
- Must reassignment fence commands already between observation and durable
  append, and if so which control generation is recorded with the command?
- How large may a portable required-event package become before it needs a
  content-addressed transfer protocol rather than the first inline form?

## First bounded code unit

Add a pure directional `HandlerCompatibility` evaluator and an
`AssignedHandlerConnector::eligible_contracts` helper with tests proving that it
keeps the original `HandlerRequest`, considers only freshly observed exact-set
members, rejects missing schemas, and never admits storage-only advertisements.
It returns candidates, not a ready route or `Current`, but establishes the
selection boundary used by the full design without inventing completeness.
