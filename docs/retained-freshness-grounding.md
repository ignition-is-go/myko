# Retained-handler freshness grounding

## Scope and evidence

This work implements C11/C17-related retained-handler correctness without closing
the wider scope-continuity plan. Myko owns durable readiness and client recovery;
Forrest remains a renderer and application-semantic consumer. The architecture
must preserve the August log-first constraints recorded in scope-continuity-grounding.md.

Current evidence: `/tmp/forrest-retained-roster-current.log` reproduces a deleted
agent in the retained roster after direct reconstructed history omits it.
`/tmp/myko-sourced-final-core.log` passes 214 core tests, including sourced causal
write/deletion release and historical-cut stability. These do not prove handler
freshness.

Graph project `myko-7-current`, generation `2026-09-05T01:04:50Z`, Tier 2.
Search found compute_or_cache, query_map_untyped_routed, view_map_untyped_routed,
report_routed, typed_projection, open_handler, subscribe_node_handler_map, and
subscribe_node_handler_report without pagination. The compute_or_cache trace
identifies query/view callers but also has incorrect lexical callee matches.
Use actual source for material claims. Coverage marks context.rs,
federated_source.rs, and federation/node.rs changed; direct source was read.
Application, request, view registration/traits, query context, and client_session
have matching metadata and no recorded gaps. This is not exhaustive indexing proof.

## Traced runtime model

FederatedSession::follow_handler calls ApplicationHost::open_handler.
HandlerRegistry invokes the registered query/view/report factory. Build contexts
obtain durable rows from FederatedRuntime. Nested query/view/report calls use
MykoServerContext caches. ClientSession subscribes to the final output and keeps
its guard for the lifetime of the client subscription.

MapCacheEntry in server/context.rs stores a weak final map and weak typed
projections. ReportCacheEntry stores a weak final cell. Hyphae's materialized
graph keeps upstream computations alive through subscription guards; Myko does
not retain an enumerable durable dependency graph. The runtime separately owns
source drivers. A live cache hit skips the factory and source accessors.
typed_projection can also reconstruct an evicted cache entry from an untyped
map. Metadata stored only in the old entry would disappear on this path.

RequestContext::lineage is tracing identity. It cannot establish freshness.
Dynamic nested computations can add or remove dependencies after construction.
The required evidence must therefore survive cache hits, output lifetimes,
typed conversions, and dynamic dependency changes.

SourcedMapSource now reconstructs rows and topology from one causal cut, but it
does not publish liveness or frontier evidence. An observed local cut can include
unresolved history, so merely reaching it is not proof of complete coverage.
Ordinary source MapRevision contains rows-related diff, frontier, epoch, and
liveness, but the final derived handler may still be unsettled after a source
updates.

## Exact dependency constraints

The locked dependency is registry Hyphae 3.1.1, not necessarily the sibling
Hyphae checkout. Its cell_map.rs subscribe_diffs implementation snapshots data,
calls Initial, then subscribes to diffs while skipping that cell's initial signal.
A change between the snapshot and listener registration can be lost.

Its scheduler exposes batch and no_coalesce, not a verified public cross-thread
settlement acknowledgement. Same-thread batch completion is insufficient when
another thread owns the active drain. Do not propose an API as available without
checking the exact locked source. No edits outside Myko/Forrest are authorized.

## Required behavior and comparison rubric

Each candidate must explain and sketch these five properties:

1. First Current frame has a coherent durable cut and settled output; incomplete
   history, source failure, and disconnect cannot masquerade as current empty data.
2. Warm nested query/view/report caches, typed reinsertion, dynamic dependencies,
   and weak ownership preserve the exact dependency closure without leaks.
3. Snapshot-to-live handoff loses no updates, with explicit ordering under
   concurrent and reentrant callbacks.
4. Unrelated stale sources do not block an independent handler. A foreign read
   does not force unauthorized transitive replication.
5. The interface hides readiness mechanics inside Myko, preserves shared outputs,
   and provides an implementable verification sequence without an invented
   scheduler API, a second persistence system, sleeps, or per-client rebuilding.

Two candidates should differ structurally: output-owned dependency/evidence
objects versus readiness propagated as part of the reactive output values.
Neither direction is preselected. Write caller usage first, types/signatures,
module map, tradeoffs, alternatives, and test sequence. Bodies may be sketches;
do not claim implementation or passing tests from a design.
