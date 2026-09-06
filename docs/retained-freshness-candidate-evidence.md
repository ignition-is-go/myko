# Retained freshness candidate A: output-owned evidence

## Problem

Retained handler outputs are shared Hyphae graphs, but Myko caches only weak final maps/cells and
cannot name the durable sources that keep them correct. A cache hit skips the factory, typed map
reinsertion discards cache-entry metadata, and `RequestContext::lineage` is tracing rather than
dependency evidence. Durable sources also update asynchronously. Consequently an initial handler
frame can serialize an old output after newer accepted history exists. The locked Hyphae 3.1.1 API
offers `batch` and `no_coalesce`, but no cross-thread quiescence acknowledgement; its current
`CellMap::subscribe_diffs` snapshot-then-subscribe sequence is not an atomic handoff. This design
keeps sharing, makes evidence follow the output rather than its cache slot, and publishes only an
output-observed evidence version.

## Usage (caller's view)

Application handlers keep their current declaration style. They do not wait on sources or manage
frontiers:

```rust
fn query(&self, ctx: QueryContext) -> impl Materialize<FilteredCellMap, Definite> {
    ctx.federated_items_across_sources_selected::<Agent>(self.scope.clone())?
        .filter_map_values(|agent| agent.active.then_some(agent))
}
```

The registry and session use one internal result for query, view, and report outputs:

```rust
let prepared = application.prepare_handler(handler)?;
let mut live = prepared.subscribe_current().await?;

// The first value is coherent and carries the evidence version observed by this output.
send(live.initial_frame())?;
while let Some(frame) = live.recv().await? {
    send(frame)?;
}
```

Nested cached outputs register themselves by value, including on cache hits:

```rust
let agents: RetainedMap = ctx.query(AgentsInProject { project })?;
let counts = ctx.retain(agents).map_values(/* ... */).materialize();
```

`ctx.retain` is emitted by generated handler plumbing; it is not an application protocol. Dropping
or switching away from the nested value drops its dependency lease.

## Shape

### Core types

```rust
// server/retained_output.rs
#[derive(Clone)]
pub(crate) struct RetainedMap {
    rows: FilteredCellMap,
    evidence: OutputEvidence,
}

#[derive(Clone)]
pub(crate) struct RetainedReport {
    value: Cell<Arc<dyn AnyOutput>, CellImmutable>,
    evidence: OutputEvidence,
}

#[derive(Clone)]
pub(crate) struct OutputEvidence {
    inner: Arc<EvidenceNode>,
}

#[derive(Clone)]
pub(crate) struct PublishedMap {
    current: Cell<Arc<PublishedMapVersion>, CellImmutable>,
}

pub(crate) struct PublishedMapVersion {
    sequence: u64,
    rows: Arc<[(Arc<str>, Arc<dyn AnyItem>)]>,
    evidence: EvidenceState,
}

#[derive(Clone)]
pub(crate) struct PublishedReport {
    current: Cell<Arc<PublishedReportVersion>, CellImmutable>,
}

pub(crate) struct PreparedHandler {
    output: HandlerOutput,          // RetainedMap or RetainedReport
    authority: HandlerAuthority,
}

pub(crate) struct CurrentHandler {
    initial: NodeFrame,
    updates: flume::Receiver<NodeFrame>,
    _subscription: HandlerSubscription,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EvidenceState {
    Pending(PendingReason),
    Current(CurrentEvidence),
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CurrentEvidence {
    output_version: u64,
    dependencies: Arc<[DependencyVersion]>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DependencyId(u128);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DependencyVersion {
    id: DependencyId,
    generation: u64,
    state: DurableReadiness,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DurableReadiness {
    Complete { consumed_cut: Option<LogPosition> },
    Incomplete { consumed_cut: Option<LogPosition>, missing: MissingHistory },
    Disconnected { consumed_cut: Option<LogPosition>, reason: String },
}
```

`consumed_cut` is progress, never a completeness proof. Only a source-specific completeness rule
may construct `Complete`: local accepted history can do so at its causal cut; a foreign selection
needs durable replication coverage/custody evidence. Missing causal parents stay `Incomplete` and
are not fetched transitively by a read.

Each retained output owns its `OutputEvidence`. Cache entries remain weak and contain no duplicate
truth:

```rust
struct MapCacheEntry {
    weak: WeakRetainedMap,
    typed: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

struct ReportCacheEntry {
    weak: WeakRetainedReport,
}
```

A typed projection is another `RetainedMap` whose evidence node depends on the untyped output's
evidence. If `typed_projection` reinserts an evicted map entry, it reuses `untyped.evidence()`; it
cannot manufacture an evidence node from the cache key.

### Evidence graph

```rust
impl OutputEvidence {
    fn source(revision: Cell<SourceRevision, CellImmutable>) -> Self;
    fn derived(dependencies: DependencySet) -> Self;
    fn lease(&self) -> DependencyLease;
    fn state(&self) -> EvidenceState;
}

pub(crate) struct DependencySet {
    revisions: CellMap<DependencyId, OutputEvidence, CellImmutable>,
}

impl DependencySet {
    fn insert(&self, dependency: &OutputEvidence) -> DependencyLease;
}
```

The source driver updates rows and `SourceRevision` in one `hyphae::batch`. The revision says
`Incomplete` until the source can prove its selected history complete. Every final map immediately
materializes one persistent snapshot cell from its entries. `PublishedMap.current` is a materialized
Hyphae join of that snapshot cell and the current dependency aggregate. Reports join their final
value cell directly with the aggregate. The join maps each settled pair into a new immutable
`Published*Version` and assigns its monotonic sequence there.

This join is the concrete settlement mechanism. A dependency-version change schedules the join even
when the application-visible rows/report compare equal and emit no output diff. Because the join
also depends on the final output snapshot/value, scheduler height ordering settles that output path
before constructing the immutable publication. The server subscribes to the publication cell; it
never samples a bare map plus side metadata and never asks Hyphae to quiesce. The snapshot cell and
join are created once with the retained output, before it enters a weak cache, so cache hits cannot
miss their installation.

This is the load-bearing distinction from sampling source revisions after reading output. Sampling
could attach a concurrently newer source version that the output has not processed. The joined
node carries the exact dependency versions and immutable output snapshot observed in the same
settled propagation wave. A concurrent later source publication creates a later immutable version.

### Dynamic and cached dependencies

Factory contexts contain a `DependencySet`, not a request-global list:

```rust
pub(crate) struct RetainedBuildContext {
    request: Arc<RequestContext>,
    dependencies: DependencySet,
}

impl RetainedBuildContext {
    fn retain_map(&self, map: RetainedMap) -> TrackedMap;
    fn retain_report(&self, report: RetainedReport) -> TrackedReport;
}
```

Opening a durable source or nested retained output inserts its output-owned evidence and returns a
lease. The materialized parent output owns the leases collected during its build. A cache hit
returns the same output and evidence, so the parent registers it without rerunning the child
factory. Nested query/report calls already route through the canonical caches; their return types,
not lineage, now preserve the dependency.

For a dependency chosen at runtime, Myko's generated `switch_retained` adapter owns exactly the
selected child's lease and replaces it in the same Hyphae batch that switches the value graph.
Dropping the old branch removes its evidence edge. Arbitrary raw Hyphae `switch_map` cannot provide
this invariant and is not accepted at the registered handler boundary when its callback opens a
durable Myko source. Static transformations remain unchanged.

Evidence edges are weak toward child outputs; the lease is the strong lifetime. Thus an active
parent keeps selected dependencies alive, cache-only weak entries do not, and removed dynamic
branches do not leak or block an unrelated handler.

### Coherent subscription

```rust
impl PreparedHandler {
    async fn subscribe_current(self) -> Result<CurrentHandler, HandlerOpenError>;
}
```

`subscribe_current` installs one Myko-owned subscription on `PublishedMap.current` or
`PublishedReport.current` before accepting a Current version. It does not separately read the bare
output. When a publication contains `Current(version)`, it performs a serialized handoff:

1. lock the handler subscription state;
2. call `Cell::subscribe` on the immutable publication cell with an internal callback that can
   only buffer; the locked implementation installs this subscriber before delivering its seed;
3. reconcile transient seed/update ordering by monotonic publication sequence and choose the
   greatest buffered `Published*Version` as Initial;
4. enqueue `Initial { sequence, snapshot }` before any buffered later publication;
5. unlock, then deliver frames; later immutable versions are reduced to wire diffs or replacements
   in sequence order.

Reentrant callbacks enqueue behind the state lock; they never call transport while holding it.
In particular, a sink that mutates the source while receiving Initial cannot enter the known
Hyphae seed-before-listener gap: external delivery starts only after listener installation. A
concurrent mutation during listener installation is represented by either the subscriber seed or a
later immutable publication; one after installation is buffered. Wire diffs are derived between
two complete immutable versions, so delete/reinsert and batched updates use `NodeHandlerMapState`
semantics rather than row-key guesses. The regression
`node_handler_map_keeps_changes_made_while_delivering_its_initial_frame` remains the unit gate.
Sequence numbers belong to this output subscription, and duplicate versions coalesce. This
replaces direct `CellMap::subscribe_diffs`; it uses the locked `Cell::subscribe` ordering plus
Myko's publication sequence rather than assuming callback arrival order.
Incomplete or invalid evidence yields no Current frame. Disconnect after Current emits terminal
liveness rather than an empty snapshot.

### Signatures and migrations

```rust
// server/federated_source.rs
fn items<T>(...) -> Result<RetainedMap, String>;
fn items_across_sources_selected<T>(...) -> Result<RetainedMap, String>;

// server/context.rs
fn query_map_untyped_routed<Q>(...) -> RetainedMap;
fn view_map_untyped_routed<V>(...) -> RetainedMap;
fn report_routed<R>(...) -> RetainedReport;
fn typed_projection(..., untyped: &RetainedMap, ...) -> RetainedMap;

// server/handler_registry.rs
fn open_federated_query(...) -> Result<RetainedMap, String>;
fn open_federated_view(...) -> Result<RetainedMap, String>;
fn open_federated_report(...) -> Result<RetainedReport, String>;

// application.rs
fn prepare_handler(...) -> Result<PreparedHandler, String>;

// server/federated_session.rs
async fn follow_handler(...) { prepare_handler(...).subscribe_current().await }
```

`ClientSession` stores the returned `HandlerSubscription`; it no longer constructs an initial
snapshot from a bare `CellMap`. Wire frames may expose output sequence and liveness, but never
Hyphae cells, dependency IDs, or replication internals.

### Module map

- `server/retained_output.rs`: output/evidence types, dynamic leases, coherent subscription state.
- `server/federated_source.rs`: source revision truth and atomic rows-plus-revision publication.
- `server/context.rs`: weak caches of complete retained outputs and typed evidence preservation.
- `core/query|view|report/*`: generated/internal adapters that retain nested output leases.
- `server/handler_registry.rs`, `application.rs`: prepare a retained output without publishing it.
- `server/federated_session.rs`, `client_session.rs`: await Current evidence and perform ordered
  snapshot-to-live delivery.

The internal surface is one rich object per output. It hides cache lifetime, dependency closure,
durable readiness, settlement, and handoff behind `RetainedMap`/`RetainedReport`; callers do not
coordinate stages, per boundary-discipline and minimize-reader-load. There is one source of truth:
evidence owned by the output graph, not copied into cache entries or sessions.

## Synthesis decision

This is candidate A for arena synthesis. It deliberately chooses output-owned evidence over
placing readiness fields in every application row/value. That keeps application item schemas and
wire-neutral query APIs free of framework metadata while still making settlement reactive. The
cross-candidate judge should reject this candidate if `switch_retained` cannot cover every dynamic
durable-source construction path without an escape hatch.

## Tradeoffs accepted

- We accept changing internal query/view/report return wrappers in exchange for evidence surviving
  cache hits, typed conversions, nested outputs, and weak-cache reinsertion.
- We accept one small evidence graph beside each retained output in exchange for observing actual
  reactive settlement without an unavailable Hyphae quiescence API.
- We accept fail-closed `Incomplete` for foreign selections in exchange for never forcing
  unauthorized causal-parent replication.
- We accept a Myko-owned subscription state machine in exchange for a race-free, reentrant-safe
  initial/live handoff.
- We accept restricting dynamic durable-source switching to an instrumented adapter in exchange
  for exact removal of obsolete dependencies rather than lifetime-wide conservative unions.

## Alternatives considered

- Read source frontiers immediately before serializing a bare output. This has a smaller patch but
  exposes ordering to every caller and cannot prove the derived output incorporated the sampled
  versions.
- Rebuild a fresh graph per client. This hides freshness locally but destroys shared retained
  outputs, repeats expensive work, and still needs an atomic snapshot/live handoff.
- Wait for all cached sources globally. This is a shallow interface over the wrong ownership: an
  unrelated incomplete source can block the handler, while a dynamic nested dependency can remain
  invisible.
- Call a Hyphae settle/quiesce operation. No such public API exists in locked Hyphae 3.1.1, and
  same-thread `batch` completion is explicitly weaker under a concurrent drain.
- Store dependency metadata in `MapCacheEntry`/`ReportCacheEntry`. Weak eviction, typed reinsertion,
  and cache-hit factory elision make cache slots non-authoritative.

## Open questions and risks

- Can all registered handlers that open a durable source dynamically be routed through
  `switch_retained`, or must such construction be rejected at registration time?
- What existing replication coverage record is sufficient to construct `Complete` for each foreign
  exact/subtree selection without claiming global absence?
- Does the parent `CellMap` handoff fix provide a reusable serialized subscription primitive, or
  should `retained_output.rs` own the only implementation to avoid two subtly different protocols?
- Will evidence joins add unacceptable fanout for very large nested handler graphs, and what
  benchmark threshold should gate rollout?
- How should a wire client distinguish terminal invalidation from temporary incompleteness without
  treating either as current empty state?

## Next implementation step

Build `RetainedMap` plus a source-backed `OutputEvidence` and prove, in one focused test, that a warm
cache hit cannot emit Current until its final output observer carries the required durable source
version.
