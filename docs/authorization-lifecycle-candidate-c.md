# Authorization lifecycle candidate C: owned access epoch

## Problem

An admitted handler can later be denied, challenged, or become impossible to
evaluate. Today `drive_handler`, `drive_view`, and the local/Iroh item loops
turn authorization into terminal invalidation, and the federation writers
retain the last protected scalar or rows. The design must preserve one prepared
request (including target and selectors), let the same owned reactive handle
recover, and distinguish a policy result from transport and authority outage.
It must also make a whole-handler denial an atomic clear across scalar,
collection, and every derived map/join/union/projection.

## Usage (caller's view)

The public watch remains one owned handle; applications observe a value and
typed status, but never schedule retries or reopen a request:

```rust
let watch = client.follow_report_reactive(&report)?;
let value = watch.live().value();
let status = watch.live().access();
```

```rust
let view = client.follow_view_reactive(&view)?;
view.live().rows().watch(|rows| render(rows));
view.live().access().watch(|access| render_banner(access));
```

```rust
let item = local.item_client().follow_reactive(request, query)?;
drop(item); // cancellation closes the connection and stops all retry work
```

`access()` yields `Authorized`, `Denied(decision)`, `Challenged(challenge)`,
or `Unavailable(reason)` (plus an initial `Pending` state). Transport
resynchronization is a separate status and does not erase an authorized stale
value.

## Shape

Make the lifecycle a domain-owned module in `federation::reactive`, and have
all four drivers (core handler scalar, core view collection, local item, Iroh
item) feed it events:

```rust
pub enum AccessStatus {
    Pending,
    Authorized { decision: PermitDecision },
    Denied { decision: DenyDecision },
    Challenged { challenge: AuthorityChallenge,
                 report: AuthorizationReport },
    Unavailable { reason: AuthorityUnavailable },
}

pub struct LiveSubscriptionState<T, C> {
    pub value: Option<T>,
    pub through: Option<C>,
    pub transport: TransportStatus, // Connecting | Current | Resynchronizing | Invalid
    pub access: AccessStatus,
}

pub enum WatchEvent<T, C> {
    Snapshot { value: Option<T>, through: Option<C>, permit: PermitDecision },
    Delta { delta: T, through: C },
    Authorization(AuthorizationDecision),
    TransportUnavailable(String),
    ProtocolFailure(String),
}

pub struct AccessEpochWriter<T, C> { /* scalar or keyed collection backend */ }
impl<T, C> AccessEpochWriter<T, C> {
    pub fn apply(&mut self, event: WatchEvent<T, C>) -> Result<(), LifecycleError>;
    pub fn clear_for_authorization(&mut self, status: AccessStatus);
    pub fn resynchronizing(&mut self, reason: String);
}

pub struct PreparedWatch<R> {
    pub request: R,             // immutable target + selectors
    pub reconnect: ReconnectPolicy,
}
pub async fn run_owned_watch<R, T, C, O>(
    prepared: PreparedWatch<R>, opener: O, writer: AccessEpochWriter<T, C>,
    cancel: CancellationToken,
) -> Result<(), LifecycleError>;
```

`AccessEpochWriter::clear_for_authorization` executes one revision-gated
transaction: scalar `value = None`, or collection `CellMap::clear()`, then
publishes the denial/challenge status and lifecycle revision together. It is
idempotent. A permit can publish output only after a fresh sequence-zero
snapshot passes the existing cursor/coherence checks. An outage changes only
`transport` and/or `access = Unavailable`; it retains the last authorized
value and rows. A denial or challenge clears protected output immediately,
then the owned driver reopens the same prepared request until a permit,
cancellation, or a non-recoverable protocol error.

The driver owns retry timing and uses `select!` over cancellation, stream
events, and its backoff timer; callers have no retry token or reopen method.
Local and Iroh adapters only map wire errors into `WatchEvent`, including
authority-unavailable for item watches. `HandlerClientError::Authorization`
must carry the typed decision rather than be classified as terminal.

Composition consumes the same state boundary. `map_value` maps denied or
challenged source to `None`; joins and projections treat any denied dependency
as denied and never retain the previous tuple; collection `as_subscription`
uses the map snapshot only when access is authorized/current; union applies a
single clear revision before reconciliation. Thus derived output cannot show
an empty `Current` intermediate or stale protected tuple.

Interface depth is high: callers keep one handle and observe domain state;
transport reconnects, authorization epochs, row-map mutation, cancellation,
and revision ordering are hidden behind the writer/driver boundary. Wire
types and policy evaluation stay below adapters. Internal `AccessEpoch` is the
single source of truth for status and output publication.

## Synthesis decision

Candidate C recommends separating access status from transport status and
centralizing both transitions in an epoch writer. This is intentionally a
structurally distinct alternative to adding more variants to
`SubscriptionLiveness` and patching each existing loop. No arena synthesis has
yet selected among candidates; this package is the C input.

## Tradeoffs accepted

- We accept two status fields in exchange for making authorization outage,
  transport outage, and policy denial impossible to confuse.
- We accept a private transaction gate around collection publication in
  exchange for atomic clear-plus-status semantics.
- We accept retries after denial/challenge in the owner in exchange for no
  application retry bookkeeping and recovery through the same handle.
- We retain stale data only for transport/authority outages; policy denial and
  challenge intentionally expose no protected data.

## Alternatives considered

The smaller alternative is to extend `SubscriptionLiveness` with
`AuthorizationDenied` and `AuthorizationChallenged`, then make every driver
call `invalidate`/`publish`. It exposes per-driver retry and atomic-clear rules,
leaks collection-versus-scalar policy, and keeps joins vulnerable to retained
tuples; it has a smaller type surface but materially shallower behavior.

A second alternative is a separate `AuthorizationHandle` that callers must
manually reattach to raw `follow_*` streams. It hides reconnect internals but
exposes temporal coordination, request replay, and cancellation to every
application, violating the owned-handle contract.

## Open questions and risks

- Should `Denied` and `Challenged` retry automatically forever, or require a
  bounded policy while still keeping retry bookkeeping out of applications?
- What exact aggregate rule should apply when only one scope of a multiscope
  result is denied, and what readiness means for ordinary-read failover?
- Which coherence frontier is required before a regrant may republish a
  joined or unioned result?
- Should a challenge carry a resumable challenge action, or only typed display
  data until a future authorization API exists?

## Next implementation step

Add the typed `AccessStatus` and epoch-aware writer transitions, then route one
scalar handler through them before migrating view and local/Iroh item drivers.
