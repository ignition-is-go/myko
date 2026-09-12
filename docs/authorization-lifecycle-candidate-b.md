# Authorization lifecycle candidate B

## Problem

Retained handler and item watches currently have one terminal path for
authorization failures: the driver invalidates the same reactive handle and the
writer keeps its last value. That is wrong for revocation. A whole-handler
denial must erase protected scalar values and collection rows in the same
publication that reports the authorization state, while transport and authority
outages must keep stale output. Recovery must reuse the caller's existing owned
handle, but it may publish protected data again only after a fresh authorized
snapshot passes the existing handler or item coherence checks.

## Usage

Callers keep using the retained APIs. They observe typed status instead of
parsing strings and own no retry bookkeeping.

```rust
let report = client.follow_report_reactive(&CountLocalRecords {})?;
let state = report.live().current();
assert!(matches!(
    state.readiness,
    LiveReadiness::Blocked(AuthorizationBlock::Denied { .. })
));
assert!(state.value.is_none());
```

```rust
let view = client.follow_query_reactive(None, scope, &GetAllLocalRecords {})?;
let rev = view.live().current_revision(&view.live().subscribe_revisions());
assert!(matches!(rev.diff, Some(MapDiff::Initial { ref entries }) if entries.is_empty()));
assert!(matches!(rev.state.readiness, LiveReadiness::Blocked(_)));
```

```rust
let items = local
    .item_client()
    .watch_serving_items_reactive(scope, GetAllLocalRecords {})
    .await?;
// Socket loss retains rows. Denial clears them. Regrant republishes through the same handle.
```

Drivers report events, not policy. A retained driver owns target resolution, prepared selectors, reconnection, cancellation, and the exact request used for every recovery attempt.

```rust
RetainedDriver::new(prepared, writer, policy)
    .drive(|event| writer.apply_retained_event(event))
    .await;
```

## Shape

Use a structural lifecycle split instead of adding another string-bearing `SubscriptionLiveness` variant. Replace scalar and collection lifecycle fields with typed readiness plus payload visibility.

```rust
pub enum LiveReadiness {
    Connecting,
    Current,
    RetainingStale(RetainedStale),
    Blocked(AuthorizationBlock),
    Invalid(ProtocolInvalid),
}

pub enum RetainedStale {
    Transport { reason: String },
    AuthorityUnavailable(AuthorityUnavailable),
    Resynchronizing { reason: String },
}

pub enum AuthorizationBlock {
    Denied { decision: Arc<AuthorizationDecision> },
    Challenge { decision: Arc<AuthorizationDecision> },
}

pub struct LiveSubscriptionState<T, C = LogPosition> {
    pub value: Option<T>,
    pub through: Option<C>,
    pub readiness: LiveReadiness,
}

pub struct LiveCollectionState<C = LogPosition> {
    pub through: Option<C>,
    pub readiness: LiveReadiness,
}
```

The public writer interface becomes the policy boundary.

```rust
impl<T, C> LiveSubscriptionWriter<T, C> {
    pub fn publish_authorized(&self, value: T, through: Option<C>);
    pub fn retain_stale(&self, cause: RetainedStale);
    pub fn clear_for_authorization(&self, block: AuthorizationBlock);
    pub fn fail_protocol(&self, reason: impl Into<String>);
}

impl<T, C, K> LiveCollectionWriter<T, C, K> {
    pub fn reconcile_authorized(&self, rows: Vec<(K, Arc<T>)>, through: Option<C>) -> Result<(), String>;
    pub fn retain_stale(&self, cause: RetainedStale);
    pub fn clear_for_authorization(&self, block: AuthorizationBlock);
    pub fn fail_protocol(&self, reason: impl Into<String>);
}
```

`clear_for_authorization` is the load-bearing operation. For scalar output it sets `value: None`, `through: None`, and `readiness: Blocked(block)` in one source update. For collections it publishes `MapDiff::Initial { entries: vec![] }` and `readiness: Blocked(block)` in the same `LiveCollectionRevision`. That prevents the forbidden sequence of empty `Current` followed by denial.

Handler and item drivers share one event classifier.

```rust
pub enum RetainedEvent<T, C, K = Arc<str>> {
    AuthorizedScalar { value: T, through: Option<C> },
    AuthorizedCollection { rows: Vec<(K, Arc<T>)>, through: Option<C> },
    AuthorizedCollectionDelta { diff: MapDiff<K, Arc<T>>, through: Option<C> },
    Stale(RetainedStale),
    AuthorizationBlocked(AuthorizationBlock),
    ProtocolInvalid(String),
    Cancelled,
}
```

`HandlerClientError::Authorization` maps deny and challenge to `AuthorizationBlocked`; `AuthorityUnavailable` maps to `Stale::AuthorityUnavailable`; transport loss maps to `Stale::Transport`. `Cancelled` is only produced by owner drop and remains protocol-invalid-looking to callers who kept a cloned `LiveSubscription`.

Module map:

- `libs/myko/federation/src/reactive.rs`: owns `LiveReadiness`, `RetainedStale`, `AuthorizationBlock`, scalar writer transitions, collection writer transitions, and composition propagation.
- `libs/myko/core/src/client/durable_handler.rs`: owns `RetainedDriver` for handler reports/views, prepared request selectors, reconnect loops, and error classification.
- `libs/myko/local/src/transport.rs`: adapts local item streams into `RetainedEvent`; keeps local socket mux details private.
- `libs/myko/iroh/src/client.rs`: adapts Iroh item streams into the same retained driver. Its recoverable classifier must include authority unavailable for item streams.

Composition rules stay data-driven. `map_value` and `try_map_value` clear when the source value is absent because the value is absent, not because a string says "denied." `frontier_join_state` and `coherent_join_state` must check `Blocked(_)` before stale or invalid retention and return `value: None`; stale dependencies may keep the previous tuple. `as_subscription` returns `None` when a collection is blocked, even though it returns a sorted vector for current or stale rows. Row maps follow the empty initial diff. Union clears only when a whole source is blocked and remains unable to infer partial provenance.

## Synthesis decision

Candidate B picks writer-owned protected transitions as the base. It rejects a driver-only fix because too many call paths can reach `invalidate`, and each would have to remember whether to retain or clear. The design borrows the existing prepared-open shape from handler clients and extends it into one retained driver contract shared by local and Iroh item clients.

## Tradeoffs accepted

- We accept touching the common reactive state types in exchange for one place that can enforce atomic clear, stale retention, and recovery.
- We accept a small migration from `liveness` to `readiness` in exchange for typed denial, challenge, authority outage, transport outage, and protocol failure.
- We accept that old `Invalid` no longer means "clear nothing" by default. Writers now choose `retain_stale`, `clear_for_authorization`, or `fail_protocol` explicitly.

## Alternatives considered

- Add `AuthorizationDenied { reason: String }` to `SubscriptionLiveness`. It is shallow. Callers and joins would still need side knowledge that this one invalid state clears data while other invalid states retain it.
- Make each driver call `publish(None)` or `reconcile(empty)` before `invalidate`. That exposes ordering to every transport and can publish empty current output during revocation.
- Store a separate `authorization: Cell<AuthStatus>` beside the value cell. That creates two writers for one invariant, so readers can observe cleared data with old authorization state or stale data with new denial state.

## Open questions and risks

- What is the checkpoint for a partially denied multi-scope aggregate: all scopes current and authorized, or a policy-specific split into protected and retained rows?
- What readiness proof is required before ordinary reads can use the same failover behavior as retained reactive streams?
- Should a challenge always clear protected output, or may a future UI keep redacted metadata while approval is pending? This design clears by default.
- Do raw `follow_query`, `follow_report`, and `follow_view` streams continue to return terminal authorization errors, or do they expose the typed readiness state only through retained APIs?

## Next implementation step

Add the federation reactive types and writer methods first, then migrate `drive_handler`, `drive_view`, local item reactive, and Iroh item reactive to emit retained events through that single writer boundary.
