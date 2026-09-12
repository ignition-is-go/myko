# Candidate A: authorization-aware snapshots

## Problem

Owned reactive handles currently retain values for every non-current lifecycle state and stop on `HandlerClientError::Authorization`. Whole-handler denial instead must erase protected scalar values or collection rows, remain visibly distinct from a challenge and an authority/transport outage, and let the same handle reconnect with its already-prepared selectors. The difficult boundary is federation composition: joins and unions keep private snapshots, so clearing only the source cell/map does not revoke derived output. Raw caller-driven streams and storage-only nodes keep their existing contracts.

## Usage (caller's view)

The application owns one handle and reacts to a domain snapshot; it never schedules a retry:

```rust
let owned = client.follow_report_reactive(&UsageReport {}).await?;
match owned.live_subscription().current() {
    LiveSnapshot::Current { value, through } => render(value, through),
    LiveSnapshot::Stale { retained: Some(old), cause, .. } => render_stale(old, cause),
    LiveSnapshot::Unauthorized { block } => render_sign_in(block),
    LiveSnapshot::Connecting => render_loading(),
    LiveSnapshot::Stale { retained: None, .. } | LiveSnapshot::Failed { .. } => render_empty(),
}
```

A view observes one atomic revision. On denial its revision removes every row and reports the block together:

```rust
let owned = client.follow_view_reactive(&DevicesView {}).await?;
let revision = owned.live_collection().subscribe_revisions().recv().await?;
match revision.snapshot {
    CollectionSnapshot::Current { through } => apply(revision.diff, through),
    CollectionSnapshot::Unauthorized { block } => {
        assert!(owned.live_collection().rows().snapshot().is_empty());
        show_authorization(block);
    }
    CollectionSnapshot::Stale { cause, .. } => show_cached_rows_as_stale(cause),
    _ => {}
}
```

Derived values use the same API and cannot recover until all inputs satisfy their existing coherence rule:

```rust
let summary = devices.as_subscription().join_coherent(&policy).map_value(summarize);
// Any whole-handler auth block clears `summary`; a later permit is not published
// until both inputs are Current at the same cursor.
```

Local and Iroh item watches expose the same snapshot vocabulary; dropping any owned handle aborts its internal reconnect task as today.

## Shape

### Domain types

```rust
pub enum AuthorizationBlock {
    Denied(DenialDecision),
    Challenged { challenge: AuthorityChallenge, report: AuthorizationReport },
} // deliberately no Permit variant

pub enum RecoveryCause {
    Transport(String),
    AuthorityUnavailable(AuthorityUnavailable),
    SourceResynchronizing(String),
}

pub struct Coherent<T, C> { pub value: T, pub through: Option<C> }

pub enum LiveSnapshot<T, C = LogPosition> {
    Connecting,
    Current { value: T, through: Option<C> },
    Stale { retained: Option<Coherent<T, C>>, cause: RecoveryCause },
    Unauthorized { block: AuthorizationBlock },
    Failed { reason: String },
}

pub enum CollectionSnapshot<C = LogPosition> {
    Connecting,
    Current { through: Option<C> },
    Stale { through: Option<C>, cause: RecoveryCause },
    Unauthorized { block: AuthorizationBlock },
    Failed { reason: String },
}

pub struct LiveCollectionRevision<T, C, K> {
    pub diff: Option<MapDiff<K, Arc<T>>>,
    pub snapshot: CollectionSnapshot<C>,
}
```

The enum makes a protected value impossible in `Unauthorized`; denial and challenge are explicit typed blocks, while outages are typed stale causes. `Current` is the only publishable authoritative value state. Transport/wire decisions are parsed into `AuthorizationBlock` in connectors, per boundary-discipline.

### Writer and composition signatures

```rust
impl<T, C> LiveSubscriptionWriter<T, C> {
    pub fn publish(&self, value: T, through: Option<C>);
    pub fn retain_stale(&self, cause: RecoveryCause);
    pub fn revoke(&self, block: AuthorizationBlock); // atomically drops value/cursor
    pub fn fail(&self, reason: impl Into<String>);
}

impl<T, C, K> LiveCollectionWriter<T, C, K> {
    pub fn apply(&self, diff: MapDiff<K, Arc<T>>, through: Option<C>);
    pub fn retain_stale(&self, cause: RecoveryCause);
    pub fn revoke_all(&self, block: AuthorizationBlock);
    // one revision-gate + hyphae::batch: MapDiff::Batch(Remove*) and Unauthorized
}

enum WatchFailure {
    Recoverable(RecoveryCause),
    Authorization(AuthorizationBlock),
    Fatal(String),
}

fn classify_handler_error(error: HandlerClientError) -> WatchFailure;
fn classify_local_item_error(error: LocalPeerError) -> WatchFailure;
fn classify_iroh_item_error(error: IrohReplicationError) -> WatchFailure;

async fn drive_owned_watch<P, S, W>(prepared: P, writer: W, policy: ReconnectPolicy)
where P: PreparedReconnect<S>, W: RevocableWriter;
```

`drive_owned_watch` owns a prepared request for its full lifetime. A recoverable outage calls `retain_stale`; denial or challenge calls `revoke`, then keeps reconnecting internally with backoff. A successful reconnect first receives an authorized snapshot, validates epoch/sequence and typed payload, and only then calls `publish`; existing `Current`/cursor coherence remains the publication gate. The initial prepare phase resolves routing once and stores the full `HandlerRequest`, including source, scope, handler kind/name, params, and view selectors. Item adapters similarly retain `ItemStateRequest` plus the cloned query. No caller-visible retry token, callback, or generation exists. Drop aborts sleep/connect/receive through the existing task handle; writers are not separately shared.

Composition folds operate over the enum, not loose `Option<T> + liveness` fields:

- `map_value`/`try_map_value` map only `Current` and stale retained values; `Unauthorized` clears without invoking the transform.
- coherent/frontier joins treat any `Unauthorized` input as absorbing and clear their previous tuple. Recovery requires all dependencies `Current` plus the existing equal-cursor/frontier rule.
- `as_subscription` emits `Unauthorized` without snapshotting rows.
- projections propagate the source collection revision; `revoke_all` removals naturally clear lazy row maps.
- union clears its left/right private snapshot for the blocked side before reconciling output, and publishes output removals plus `Unauthorized` atomically. It must not infer that the other side is safe.

This is a deep interface: four writer transitions hide transport classification, retry ownership, atomic erasure, and composition recovery. Callers see only snapshots and never coordinate lifecycle methods. There is one mutable publication per source and derived folds own no independently writable authorization flags, per single-source-of-truth and separate-before-serializing-shared-state.

### Module map

```text
federation/reactive.rs       LiveSnapshot, CollectionSnapshot, writer transitions,
                             map/join/projection/union propagation
core/client/durable_handler.rs prepared durable request + shared owned-watch state machine
core/client/mod.rs           HandlerClientError -> domain WatchFailure boundary
local/transport.rs           local item prepared reconnect + classifier
iroh/client.rs               Iroh item prepared reconnect + authority-outage classification
local/tests/handler_authorization.rs end-to-end revoke/outage/regrant/cancel fixtures
federation reactive tests    atomic removal and composition non-retention/recovery
scripts/verify-query-lifecycle.sh lifecycle verification entry point
```

Storage-only nodes do not acquire these typed handler drivers. Raw `follow_query`, `follow_report`, and `follow_view` remain terminal caller-driven streams; only `follow_*_reactive` and owned item handles get the reconnect lifecycle.

## Synthesis decision

Candidate A chooses authorization-aware snapshot enums plus one reusable owned-watch state machine. Its base decision is that revocation is a data-state transition, not an error annotation: this prevents forbidden value/status combinations and forces every composition fold to handle authorization exhaustively.

## Tradeoffs accepted

- We accept a breaking replacement of the loose lifecycle structs in exchange for making unauthorized-with-data unrepresentable.
- We accept explicit authorization branches in every composition fold in exchange for guaranteed transitive erasure of private join/union snapshots.
- We accept continued background attempts after denial/challenge in exchange for same-handle recovery without application bookkeeping.
- We accept clearing an entire composed output in exchange for never guessing row- or scope-level provenance from a whole-handler decision.

## Alternatives considered

- **Epoch-bound capability cell beside existing data cells:** gate reads through a shared `AuthorizationLease` and rotate its epoch on denial. This is structurally different and minimizes data mutation, but every renderer, row projection, cached join, and union must consult the lease correctly; its broad, shallow public contract leaks policy and permits stale protected copies.
- **Terminal handle replacement:** return a new handle or retry token after denial. It simplifies drivers but exports retry sequencing and selector retention to every application, violating the owned-handle requirement and offering less interface depth.

## Open questions and risks

- At which handler planning checkpoint can Myko prove a multi-scope aggregate is wholly denied versus partially authorized, and what provenance must the plan preserve before any partial-output policy is chosen?
- Which server readiness checkpoint proves an ordinary reconnect snapshot is authorized *and* coherent enough to replace stale data; must initial and continuation admission share one typed marker?
- Should a challenge retry only on approval/authority notification rather than polling backoff, and which existing authority event can wake it without exposing retry bookkeeping?
- Must public denial details be reduced further before storing them in long-lived client state?
- Can a collection revocation with a very large removal batch meet scheduler latency limits, or does `MapDiff` need a first-class atomic `Clear` variant?

## Next implementation step

Introduce the snapshot enums and atomic scalar/collection `revoke` transitions with focused federation tests before changing any transport driver.
