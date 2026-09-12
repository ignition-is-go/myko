# Report lifecycle propagation

## Phase and problem

Ground, sketch, implementation, and bounded consumer verification are complete.
No human checkpoint was requested. Revisit the shape if callers need
repeated lifecycle-erasing adapters.

`ReportContext::federated_items` previously returned a raw map. The report cache
stored a raw cell, and native publication labeled every value current. The
regression `native_report_preserves_readiness_when_its_value_does_not_change`
failed waiting for the second frame when pending causal history left count = 1.

## Usage, before the types

An application report derives a value from a lifecycle-carrying source:

```rust,ignore
fn compute(&self, ctx: ReportContext) -> impl ReportBuildOutput<Self::Output> {
    RetainedReport::new(ctx.federated_items::<Record>()
        .expect("validated application source")
        .map_value(|records| Arc::new(Count { count: records.len() })))
}
```

The infallible handler builder still requires validated host wiring. A fallible
builder boundary remains an explicit follow-up; this unit preserves existing
construction-error behavior.
Ordinary in-process Hyphae pipelines remain accepted as local report outputs.
Nested report composition uses `map_value`, which preserves lifecycle. A direct
read uses `read_current() -> Result<Arc<T>, String>` rather than a stale value.

## Shape

`ReportBuildOutput<T>` materializes into `ReportValue<T>`. That output distinguishes
local cells from retained `LiveSubscription` publications. Registration erases
only the value type. The weak cache retains publication identity and sequence.
Native sessions forward coherent value, cursor, and liveness publications. Source
sequences remain internal. Wire sequences count emitted frames, so coalescing
cannot introduce a gap that the client would reject.

`RetainedReport<T, C>` accepts a typed cursor, including composite frontiers.
Cursor serialization happens at registration, preserving independent source
coordinates instead of selecting one arbitrary log position. Serialization
failure produces invalid lifecycle state. Retained-to-local conversion fails
explicitly at consumers that cannot represent lifecycle.

Source snapshots come from the same canonical projection update as map revisions,
not from reading separate cells at different cuts. These are application-executor
facilities. Storage-only participants still retain opaque logs without typed
handlers, application gateways, or executor eligibility.

## Synthesis decision

Use the existing retained-view pattern for reports. The previous bounded
gpt-5.5 design review supported explicit lifecycle-carrying outputs and rejected
source guessing. No additional panel or worktree is needed.

The alternatives were Hyphae dependency-graph reconstruction and a request-local
source collector. The installed Hyphae 3.1.1 graph loses map provenance at per-key
and size cells. A collector misses cached dependencies and can retain obsolete
dynamic branches. Both hide lifecycle from callers while failing to preserve it.
Explicit outputs keep that obligation in the type and cache boundary.

## Tradeoffs and remaining work

This changes internal report consumers instead of preserving an unsafe raw-cell
API. Local-only transport consumers must reject retained outputs until migrated.
Ordinary query/view propagation, generated durable reads, idle frontier progress,
assignment-aware failover, and stale-command admission remain open. `read_current`
rejects observed stale lifecycle; it does not atomically validate a command
against subsequent history or force queued reactive computation to settle.
Composite frontiers survive report registration, but general client cursor
decoding still needs migration. Source causal completeness does not prove remote
coverage or current execution authority.

Durable `CommandContext::exec_report` is not a supported proof path yet. Its
constructor has no server runtime, while the legacy helper requires one and
opens an unrouted report. A future internal reactive-read path must retain
authorization claims and causal dependency evidence, not just a cached value.

## Verification

The original failing regression now passes. The core library passes 263 tests,
including retained cache reuse, composition, unchanged-value lifecycle, and local
report backpressure. Source tests now use the existing process-wide scheduler
permit instead of perturbing other tests' immediate assertions. The expanded
baseline passes, as do all 149 federation library tests, 74 server library tests,
15 durable-node tests, and 16 benchmark-feature report tests. Strict checks pass
for core, federation, node, and server with schema and profiling, and separately
for the three benchmark-feature report test targets. Combining benchmark and
schema features still fails because `BenchManualWireValue` has no schema
implementation. The delivery record holds the verification scope and remaining
acceptance work.
