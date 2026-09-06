# Retained freshness candidate B: value-carried readiness

## Problem

Retained handlers can reuse warm query, view, and report outputs without running
their factories again. That is good for sharing and bad for freshness if the
freshness evidence only lives in the request that happened to open the output.
The current sourced projection can build rows from a dependency-complete causal
cut, but the retained handler still needs to know whether every nested durable
source and every derived Hyphae value has caught up before it sends a `Current`
frame. This candidate keeps that evidence inside the reactive values themselves:
every cached output publishes both its data and its readiness state, and derived
outputs compute readiness from the values they already depend on.

## Usage

The caller should not coordinate source readiness manually. A retained handler
opens the same output as today and subscribes to one stream whose value already
contains readiness.

```rust
let retained = application.open_handler(handler, request)?;
let mut stream = retained.subscribe();

while let Some(frame) = stream.next().await {
    match frame.readiness {
        OutputReadiness::Current { cut } => {
            client.send_current(frame.rows, cut)?;
        }
        OutputReadiness::Pending { reason } => {
            client.send_intermediate(frame.rows, reason)?;
        }
        OutputReadiness::Failed { error } => {
            client.send_error(error)?;
        }
    }
}
```

Nested query and view callers keep their existing ergonomic surface. The only
change is that the returned retained value carries readiness alongside rows.

```rust
impl QueryHandler for AgentRoster {
    fn build(ctx: QueryBuildContext<Self>) -> Result<ReactiveMap<AgentId, AgentRow>, String> {
        let agents = ctx
            .view(AgentsAcrossSelectedSources { selection: ctx.selection() })?
            .map_values(project_agent_row);

        let permissions = ctx
            .query(PermissionProfileByAgent { scope: ctx.scope_id() })?
            .join_by_key();

        Ok(agents.join(permissions, render_roster_row))
    }
}
```

Warm cache hits preserve the same behavior. `ctx.view(...)` may return an
already-live shared output, but the returned `ReactiveMap` still includes the
readiness value that source output is publishing now. The outer output derives
its readiness from the inner values it actually reads.

```rust
let inner = server.query_map_untyped_routed(query, request, federated)?;
// Cache hit or miss, this object includes rows plus readiness.
let derived = inner.map_values(render).filter(visible_to_client);
```

Reports use the same contract:

```rust
let report: ReactiveCell<Summary> = ctx.report(CurrentAgentSummary { scope })?;
// A report is current only after its cell value and every dependency readiness
// value have settled for the same generation.
```

## Shape

Data structures first:

```rust
/// A monotonic durable point used by readiness checks. It is not a global
/// consensus frontier; it is the local cut the output claims to have projected.
pub struct FreshCut {
    pub node: myko_federation::NodeId,
    pub through: Option<myko_federation::LogPosition>,
}

/// Why an output cannot yet claim `Current`.
pub enum FreshnessBlocker {
    DurableHistoryIncomplete {
        selection: myko_federation::ScopeSelection,
        through: Option<myko_federation::LogPosition>,
    },
    SourceDisconnected {
        source_node: Option<myko_federation::NodeId>,
        scope_id: Option<myko_federation::ScopeId>,
    },
    DerivedUnsettled {
        generation: DerivedGeneration,
    },
    Failed {
        message: Arc<str>,
    },
}

/// Readiness is data, not side-channel metadata. Transformations combine it
/// exactly like they combine rows.
pub enum OutputReadiness {
    Pending {
        blockers: Arc<[FreshnessBlocker]>,
        observed: Arc<[FreshCut]>,
        generation: DerivedGeneration,
    },
    Current {
        observed: Arc<[FreshCut]>,
        generation: DerivedGeneration,
    },
    Failed {
        error: Arc<str>,
        observed: Arc<[FreshCut]>,
        generation: DerivedGeneration,
    },
}

/// A retained reactive value is the normal Hyphae value plus a readiness cell.
pub struct ReactiveMap<K, V> {
    pub rows: CellMap<K, V, CellImmutable>,
    pub readiness: Cell<OutputReadiness, CellImmutable>,
}

pub struct ReactiveCell<V> {
    pub value: Cell<V, CellImmutable>,
    pub readiness: Cell<OutputReadiness, CellImmutable>,
}
```

Cache entries store the value object, not a separate dependency registry:

```rust
struct MapCacheEntry {
    untyped: Weak<dyn AnyReactiveMap>,
    typed: DashMap<TypeId, Weak<dyn AnyReactiveMap>>,
}

trait AnyReactiveMap: Send + Sync {
    fn untyped_rows(&self) -> FilteredCellMap;
    fn readiness(&self) -> Cell<OutputReadiness, CellImmutable>;
}

struct ReportCacheEntry {
    report: Weak<dyn AnyReactiveCell>,
}
```

This preserves warm caches and typed reinsertion because the untyped object is
the owner of both rows and readiness. If a typed projection is re-created from a
live untyped map, the typed object clones the same readiness cell. No metadata is
stranded on the evicted typed entry.

Factories and build contexts return value-carried outputs:

```rust
impl MykoServerContext {
    pub(crate) fn query_map_untyped_routed<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
        federated: Option<FederatedRequest>,
    ) -> Result<Arc<dyn AnyReactiveMap>, String>;

    pub(crate) fn view_map_untyped_routed<V>(
        &self,
        view: V,
        request: Arc<RequestContext>,
        federated: Option<FederatedRequest>,
    ) -> Result<Arc<dyn AnyReactiveMap>, String>;

    pub(crate) fn report_routed<R>(
        &self,
        report: R,
        request: Arc<RequestContext>,
        federated: Option<FederatedRequest>,
    ) -> Result<Arc<dyn AnyReactiveCell>, String>;
}

impl QueryBuildContext<Q> {
    pub fn query<N>(&self, query: N) -> Result<ReactiveMap<Arc<str>, Arc<dyn AnyItem>>, String>;
    pub fn view<V>(&self, view: V) -> Result<ReactiveMap<Arc<str>, Arc<dyn AnyItem>>, String>;
    pub fn report<R>(&self, report: R) -> Result<ReactiveCell<R::Output>, String>;
}
```

Durable source accessors are just leaves in the same value graph:

```rust
pub struct FederatedMapSource {
    rows: FilteredCellMap,
    readiness: Cell<OutputReadiness, CellImmutable>,
}

pub struct SourcedMapSource<T: MykoItem> {
    rows: SourcedItemMap<T>,
    readiness: Cell<OutputReadiness, CellImmutable>,
}
```

`FederatedMapSource` maps its existing `MapRevision { frontier, liveness }` into
`OutputReadiness`. `SourcedMapSource` starts from `node.causal_snapshot()` and
publishes `Pending` when the causal snapshot or later `causal_events_through`
reports incomplete history. Reaching a local cut is not enough by itself; the
readiness value must carry the incomplete-history blocker until the source has a
dependency-complete projection.

Derived outputs combine readiness by ordinary reactive composition:

```rust
fn combine_readiness(inputs: &[Cell<OutputReadiness, CellImmutable>])
    -> Cell<OutputReadiness, CellImmutable>;

fn derive_map<A, B>(
    input: ReactiveMap<Arc<str>, A>,
    f: impl Fn(A) -> Option<B> + Send + Sync + 'static,
) -> ReactiveMap<Arc<str>, B>;

fn join_maps<A, B, C>(
    left: ReactiveMap<Arc<str>, A>,
    right: ReactiveMap<Arc<str>, B>,
    f: impl Fn(A, B) -> C + Send + Sync + 'static,
) -> ReactiveMap<Arc<str>, C>;
```

`combine_readiness` is pure: any failed input fails the output, any pending input
keeps the output pending with the union of blockers, and only all-current inputs
produce `Current`. Generations are derived from the input readiness generations
and the output's own row update generation. This does not require a new Hyphae
settlement API. It uses the same dependency graph that computes rows: the
readiness cell is another input cell. When the row computation and readiness
computation have both emitted for the same derived generation, the output may
publish `Current`.

Snapshot-to-live handoff:

```rust
impl<T: MykoItem> SourcedMapSource<T> {
    fn start(node: &Node, selection: ScopeSelection, executor: &Handle) -> Result<Self, String> {
        // 1. Read causal snapshot: (through, history, readiness evidence).
        // 2. Build rows from that exact history.
        // 3. Publish Pending unless the snapshot is dependency-complete.
        // 4. Subscribe after `through`.
        // 5. For each event, rebuild rows from causal_events_through(event.position).
        // 6. Publish rows and readiness for the same local cut/generation.
        not_implemented!()
    }
}
```

The implementation must not rely on a cross-thread scheduler acknowledgement
that Hyphae 3.1.1 does not expose. It also must not sleep. If a derived output
has not observed matching row/readiness generations, it publishes `Pending` or an
intermediate frame, not `Current`.

This is a deep interface: callers still open query/view/report outputs, while
Myko hides source liveness, causal incompleteness, cache hits, typed projection
reuse, and dynamic dependency composition behind one value type. The policy is
not spread into client sessions or Forrest.

## Module map

- `libs/myko/core/src/server/context.rs`
  - Own cache entries for value-carried `ReactiveMap` and `ReactiveCell`.
  - Ensure cache hit and typed reinsertion preserve the readiness cell.
- `libs/myko/core/src/core/query/context.rs`
  - Return nested query/view/report values that include readiness.
  - No request-only dependency accumulator.
- `libs/myko/core/src/core/view/traits.rs`
  - Same value-carrying contract for views that open federated sources.
- `libs/myko/core/src/server/federated_source.rs`
  - Publish readiness from durable `MapRevision` and sourced causal projections.
  - Do not force foreign replication; report incomplete or disconnected state as
    readiness blockers.
- `libs/myko/core/src/server/client_session.rs`
  - Gate `Current` frames on the retained output readiness value.
  - Intermediate frames may carry data, but not current liveness.
- `libs/myko/core/src/application.rs`
  - Keep application-facing handlers unchanged except for reading readiness from
    retained outputs.

## Synthesis decision

Candidate B deliberately chooses value-carried readiness over an output-owned
dependency registry. It should be the base if the synthesis favors fewer
side-channel lifetime rules and a closer fit to Hyphae's existing reactive data
model. The main graft likely needed from another candidate is any sharper
snapshot-to-live ordering proof, because this candidate refuses to invent a
scheduler settlement primitive and therefore relies on generation matching in
ordinary cells.

## Tradeoffs accepted

- We accept adding readiness cells to every retained output in exchange for
  cache hits preserving freshness without a request-local registry.
- We accept intermediate frames with data but `Pending` readiness in exchange
  for never calling an unsettled first frame `Current`.
- We accept full readiness recomputation through reactive composition in
  exchange for avoiding global cache scans and unrelated source barriers.
- We accept that a foreign read can block currentness with an incomplete-history
  reason in exchange for not forcing unauthorized transitive replication.
- We accept keeping readiness internal to Myko in exchange for Forrest remaining
  a consumer that renders liveness rather than owning federation freshness.

## Alternatives considered

- Output-owned dependency registry: deeper than request-local capture, but it
  creates a second lifetime graph beside Hyphae. It hides source details from
  callers, yet exposes a new registry ownership problem: cache eviction, typed
  reinsertion, and dynamic dependencies must all update out-of-band metadata.
- Request-local `FreshnessSet`: smallest implementation surface, but too shallow.
  It exposes temporal construction order as an invariant and loses dependencies
  on warm cache hits where factories do not run.
- Global cache scan before `Current`: hides little and blocks too much. It makes
  unrelated stale sources affect independent handlers and still cannot know
  which dynamic dependencies the current output actually read.
- Per-client retained-handler rebuild: avoids warm-cache dependency questions by
  throwing away sharing, but regresses the retained-output model and increases
  fanout cost. It also pushes freshness ownership toward clients instead of Myko.

## Open questions and risks

- What exact `DerivedGeneration` can be built from Hyphae 3.1.1 primitives
  without adding a scheduler API?
- Do existing map/filter/join helpers expose enough hooks to combine readiness
  cells alongside rows, or does Myko need a small wrapper layer for retained
  output transforms?
- How should a retained report represent partial data with pending readiness
  when the report output type has no natural empty value?
- Should `SourcedMapSource` expose unresolved causal-history blockers directly,
  or should it collapse them into a durable-source pending reason for clients?
- Can dynamic dependencies shrink safely when a branch stops reading a nested
  source, or should readiness remain conservative until the derived output's
  next full generation?
- Which exact Hyphae subscription primitive can provide snapshot-to-live
  ownership for retained handlers? `entries()` has the same snapshot-before-
  listener gap as the red handoff test, while `subscribe` must be proven to
  install the listener before any seed delivery that can reenter and mutate.

## Verification sequence

1. Warm nested cache hit: build an inner sourced query, keep it cached, open an
   outer query that hits the cache, and assert the outer first frame is not
   `Current` until the inner readiness is current.
2. Typed reinsertion: evict the typed projection while the untyped output lives,
   recreate the typed map, and assert it shares the same readiness cell.
3. Dynamic dependency switch: a query changes from reading source A to source B;
   assert A stops blocking after the next full derived generation and B blocks
   until current.
4. Snapshot/live race: reproduce `/tmp/myko-handler-handoff-red.log` by deleting
   during initial delivery, then assert no current frame is emitted from an
   `entries()`-style snapshot-before-listener path; the accepted fix must prove
   listener ownership before seed delivery under reentrant callbacks.
5. Sourced incomplete history: unresolved causal parent yields rows from the
   safe cut plus `Pending`, then flips to `Current` when the parent arrives.
6. Unrelated stale source: a stale cached source not read by the handler does not
   block the handler's current frame.
7. Forrest retained roster: reconstructed deletion absence and retained first
   current roster frame agree.
8. Two mobile nodes permission profile: current liveness is withheld until the
   permission profile source actually converges.

## Next implementation step

Introduce `ReactiveMap` and `ReactiveCell` wrappers in `server/context.rs` and
thread their readiness cells through cache hit and typed reinsertion paths before
changing client-session current-frame gating.
