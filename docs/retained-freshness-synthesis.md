# Retained freshness synthesis

## Decision

Neither original candidate is approved for implementation as written. The
gpt-5.5 cross-judge and parent agree that both leave the central coherence proof
open. Continue from A's output-owned publication boundary, graft B's propagation
of evidence through transformations, and change the coherence unit to one
immutable value containing both payload and evidence.

The ordered source, unary transform, and subscription unit now has production
coverage, described below. Native query and view delivery now shares cached
snapshot publications and passes the initial-frame deletion regression.
Durable dependency evidence has not migrated through handler factories or caches.
This does not yet establish native retained-handler freshness.

## Comparison

Both candidates were read end to end. Scores are out of five; the parent agrees
with the cross-judge's scores.

| Grounding rubric | A: output-owned evidence | B: value-carried readiness |
|---|---:|---:|
| Coherent cut and settled first Current | 2 | 2 |
| Cached, typed, dynamic, weak ownership | 3 | 3 |
| Concurrent and reentrant initial/live handoff | 2 | 1 |
| Independence from unrelated stale sources | 4 | 4 |
| Interface depth and implementable proof | 2 | 3 |

A has the stronger ownership boundary and immutable sequenced publication.
B has the clearer verification list. A's final snapshot/evidence join and B's
separate rows/readiness cells both leave a gap between the payload computation
and the evidence attached to it.

## What survives

- A's complete output object owns its data, evidence, sequence, and lifetime.
  Weak caches refer to that object; typed reinsertion derives from the live
  untyped object, never a cache key or a request-local dependency accumulator.
- B's transformations carry evidence with the data they actually consume.
  Evidence advancement is observable even when the visible rows compare equal.
- A's client consumes immutable sequenced publications, then derives wire
  snapshots or deltas between them. Client callbacks do not run under an internal
  state lock.
- B's tests cover warm caches, typed reinsertion, dynamic branch changes,
  unrelated stale sources, causal incompleteness, and both mobile failures.

## What is rejected

- Joining a separately materialized final map snapshot with evidence is not a
  settlement proof. Hyphae 3.1.1 entries() has the same snapshot-before-listener
  gap as subscribe_diffs(). Installing it before cache insertion does not stop
  the source changing during construction.
- Same-batch dynamic rewiring is not a proven repair. The locked scheduler
  explicitly assumes stable topology for its per-tick memoized heights.
- An undefined DerivedGeneration cannot justify Current. Sequence belongs to
  the immutable publication being computed, not two independently scheduled
  cells whose correspondence is guessed afterward.
- causal_snapshot() does not report complete history. It can omit unresolved
  events; the source must obtain actual scoped completeness evidence separately
  at the same cut before constructing a Current publication.
- Locking subscription state while Cell::subscribe invokes its synchronous seed
  callback can deadlock. Register and buffer without holding a lock that the
  callback needs; send outside locks.
- Subscribe-first plus an unsequenced bare-map snapshot is not a complete
  handoff fix. Buffered older diffs can overlap a newer snapshot and regress it.
  Passing only the reentrant-delete test would not settle that ordering problem.

## Revised contract

The following signatures described the initial contract, not proposed additional
public types. Implementation reuses the existing `LiveSubscriptionState` and
`LiveSubscription`, as described in the implementation checkpoint below:

```rust
struct Observed<T> {
    sequence: OutputSequence,
    payload: T,
    evidence: ReadEvidence,
}

struct Retained<T> {
    current: Cell<Arc<Observed<T>>, CellImmutable>,
}

impl<T> Retained<T> {
    fn map<U>(&self, project: impl Fn(&T) -> U) -> Retained<U>;
    fn subscribe(&self) -> PublicationStream<T>;
}
```

Sources construct payload and evidence from one frozen read. A transformation
consumes the combined input and constructs its combined output in the same
operation. It never samples a newer side-channel readiness value after computing
older data. Derived sequences are local to their output. Evidence keeps the
identity and cut of the actual inputs, not a fabricated global frontier.

This requires migrating retained source and handler return types and their
operators. A wrapper around arbitrary existing bare maps does not enforce the
contract. Raw durable-source reads inside uninstrumented Hyphae operators cannot
silently become current retained publications.

Joining multiple inputs still needs an explicit cut-compatibility rule, and
dynamic selection must carry only the selected branch's evidence. Neither is
claimed solved by the unary map contract. Foreign completeness, authorization
changes while waiting, cache eviction, and failure propagation remain required.

## Next executable proof

Use the actual locked Hyphae dependency to build a tiny stamped source, unary
transform, and subscriber. Keep it shared per output, not rebuilt per client.

1. Publish rows and evidence together at an initial cut.
2. Subscribe, then change the source while the sink receives Initial. Observe
   the newer immutable version as Initial or a later monotonic update.
3. Deliver an older buffered publication after a newer seed. It must not regress
   the consumer or produce a delta against the wrong predecessor.
4. Advance evidence while visible output remains equal. The transformed
   publication and subscriber must still advance.
5. Fail or disconnect the source. The last payload must not remain Current.

These checks prove the publication mechanism only. The real handler regression
in client_session.rs now passes on the migrated production path, described below.
No C01-C18 row closes from a prototype.

## Executable comparison checkpoint

`libs/myko/core/tests/retained_publication_proof.rs` exercises the exact locked
Hyphae dependency. Static and same-tick deeper-switch join cases pass. A first
concurrent test with only a start barrier also passed, but did not force the
important interleaving. The strengthened test orders payload 1, then payload 2
and evidence 2, then evidence 1. The joined publication ends at `(2, 1)` and the
coherence assertion fails. `/tmp/myko-retained-publication-verified-comparison.log` records
three passing tests and this one failing test.

The comparison test publishing payload and evidence as one tuple passes under
the corresponding forced interleaving. It verifies the final publication and
the subscriber's last observed value, not only an invariant over an empty list.
This proves pairing for the prototype; it does not prove monotonic durable cuts,
output sequence filtering, handler handoff, or scoped completeness.

The counterexample has two writers to the same pair. An enforced single-writer
source could exclude that schedule, but neither a bare pair of cells nor batch()
encodes that ownership. This result does not establish that the existing daemon
has two writers to one sourced driver. It tests the proposed publication boundary,
not the cause of every retained-roster failure.

The failing split-cell prototype is deliberately still runnable and not ignored.
The expanded test target is not green. Once the accepted publication mechanism
has equivalent production coverage, rejected architecture prototypes can leave
the normal integration target while their comparison evidence remains in this
decision record.

## Ordered publication implementation checkpoint

`LiveSubscriptionState<T, C>` already holds payload, cursor, and liveness together.
The implementation reuses it instead of introducing a second retained-value API.
`LivePublication` adds a local output sequence. `LiveSubscriptionWriter` accepts
each update against its latest accepted state and drains immutable publications
in order without invoking subscriber callbacks under its acceptance lock.

Two regressions exposed an existing writer bug. Inside a reactive batch, publishing
a new value followed by a reconnect or cursor update restored the old value.
Both fail before this change and pass with the ordered writer. The accepted-state
queue prevents a deferred reactive cell from becoming the next update's input.

`watch_publications()` installs a subscription before returning the stream and
filters older or duplicate sequences. It owns the reactive guard and closes when
the cell completes or fails. `map_value()` carries the source sequence, cursor,
and liveness with the mapped value. A metadata-only change advances the publication
without invoking the value transform again.

The helper tests now wait for observed publications before asserting exact order.
Hyphae's process-global scheduler can defer another thread's callbacks. The
deliberately panicking observer runs in its own integration-test process so its
unwind cannot land on another unit test's scheduler thread. The test proves that
a subsequent update can resume publication after an observer unwinds; it does
not promise that queued work drains without a subsequent update after a panic.

Local sequence exhaustion does not wrap or panic. The writer fails the publication
cell. The raw-state constructor emits an Invalid revision at the maximum sequence.
Its observation sequence is not evidence of durable completeness or source order.

The current federation verification is 136 passing library tests, one passing
observer-panic integration test, and strict all-target core/federation Clippy.
Evidence is in `/tmp/myko-publication-verified-tests.log` and
`/tmp/myko-publication-verified-clippy.log`. An earlier core consumer run was
214 passing tests and the existing lost-deletion regression still failing in
`/tmp/myko-publication-core-consumers.log`. Later parallel core runs also exposed
immediate liveness/readiness assertions and a cascade diff-count failure. The
liveness test now waits for both actual report and view observations while
preserving its original assertions. The latest parallel result is 212 passing
tests and three failures in `/tmp/myko-publication-observed-core.log`. Those are
the retained-handler lost deletion, window readiness, and cascade diff count.
Strict core Clippy passes after the test observation change.
Running the same core tests with one test thread gives 214 passes and only the
lost-deletion failure in `/tmp/myko-publication-core-serial-diagnostic.log`.
This diagnostic does not replace or clear the parallel gate.

The stream mailbox now retains only the latest complete snapshot. A slow consumer
may skip intermediate sequences, including the initial snapshot if a newer one
arrives first. Old and duplicate sequences are rejected before replacing the
queued value. The callback never blocks waiting for the reader. This policy is
limited to complete snapshot streams; durable history and delta streams are
unchanged. The source itself still publishes every accepted sequence.
A source-to-stream test publishes 1,000 updates without consuming between writes,
checks the one-slot bound, and verifies that the reader reaches the latest
complete snapshot. Separate completion and failure tests verify that a buffered
snapshot precedes the terminal receive error.

Remaining requirements include scoped completeness,
multi-input cut compatibility, dynamic dependency changes, and migration of
handler factories, typed conversions, weak caches, and transport adapters.
The stream's final queued value can still say Current before the consumer observes EOF;
adapters must expose that
terminal condition rather than retaining Current forever. No continuity row is
closed by this unit.

## Native map handoff checkpoint

`NativeMapOutput` subscribes to the map's diff cell before taking its first
snapshot. Diff signals trigger complete snapshots captured under the publication
acceptance lock. Raw diffs are never replayed over a newer seed. Each native
session consumes ordered publications and computes deltas between the snapshots
it actually delivered. A slow session can skip snapshots without losing a deletion.

Native query and view opens share a weakly cached output keyed by host, handler,
source, scope, and parameters. Concurrent opens share one output. Tests verify
that successful and failed opens release their compute gates, the last output
owner releases a derived map, and registry-owned root maps remain available.
The root-map test uses the generated entity constant rather than a guessed name.

The original initial-frame deletion test and the output-owner test pass unchanged
in their expected contents. Async delivery tests now wait for actual frames.
The current parallel library run passes 221 core tests and 136 federation tests:
`/tmp/myko-native-handoff-core-federation.log`. Strict all-target Clippy passes
for both crates in `/tmp/myko-native-handoff-clippy.log`.

Earlier report and query readiness failures remain recorded. One green parallel
run does not prove their timing or dependency-settlement issues resolved.
Reports still use the old delivery path. A local map snapshot has no durable
frontier and does not prove scoped completeness or atomic multi-input settlement.
Terminal invalidation retains the last value; native task cleanup on terminal
source signals is not yet demonstrated. All C01-C18 rows remain open.

## Fixed-cut source publications

`SelectedHistorySnapshot` now exposes a read-only local history cut with its
dependency-complete events, topology, and pending-history assessment. A requested
cut beyond the local journal head is rejected. `has_pending_in<T>` includes all
event origins, while source-specific selected queries retain their existing
predicate. Tests cover foreign-origin pending items, unrelated exact scopes,
subtree uncertainty, and an earlier cut after a missing dependency arrives.

`SourcedMapSource` now publishes `SourcedItemSnapshot<T>` together with its local
cut and liveness through `LiveSubscription`. Pending selected history produces
`Resynchronizing`, not `Current`. Each update projects a single consumed cut.
The existing raw row map is derived from these complete publications by one
consumer task. It remains evidence-blind and cannot certify downstream settlement.
Cancellation invalidates the retained publication after its producer stops.

`LocalView`, `RetainedView`, and `ViewBuildOutput` provide explicit output
conversions. Retained conversion uses `map_value` to erase item types without
discarding the publication sequence, cut, or liveness. These wrappers are tested
but are not yet used by `ViewHandler`, `ViewCellFactory`, or the native cache.

The source and conversion changes pass 226 core and 139 federation library tests
in `/tmp/myko-snapshot-foundation-core-federation.log`. This is source-foundation
evidence, not a successful roster migration. The next production change must
preserve retained outputs through registration and require the initial output
to cover each new open's target cut. Local accepted-history completeness does
not establish remote coverage, custody, or permission to serve the result.

Strict all-target core and federation Clippy passes in
`/tmp/myko-snapshot-foundation-clippy-final.log`; scoped formatting also passes.
The separate wasm check fails with 44 unresolved-type and import errors in
`/tmp/myko-view-output-wasm-check.log`. No baseline comparison was established
for that target, so this checkpoint makes no whole-crate wasm compatibility claim.

## Registered retained roster checkpoint

`ViewHandler` and its factory now require an explicit `ViewBuildOutput`.
Registration and the native weak cache preserve the retained-publication branch.
Raw-map APIs return an error for retained outputs instead of discarding their
cut and liveness. Existing local view plans use `LocalView` explicitly.

A native open captures its local journal head before opening the handler. For a
retained output, the session withholds its initial frame until the publication
covers that target. Invalid publications can report failure without reaching the
target. Reaching the target does not promote `Resynchronizing` to `Current`.
The focused test verifies this with unchanged rows and a metadata-only advance.

Forrest's roster now maps complete sourced snapshots into roster entries with
`RetainedView`. The unchanged `local_agent_lifecycle_never_requires_pairing` test
passes, including the immediate fresh roster after deleting an agent:
`/tmp/forrest-retained-roster-lifecycle3.log`. The focused native target-cut test
passes in `/tmp/myko-retained-cut-test2.log`. Myko's lock resolves Hyphae 3.1.1;
Forrest's resolves 3.1.2. These results cover their respective locked builds.

The full library/workspace reruns and review are still pending at this checkpoint.
This migration does not prove report freshness, compatible multi-input cuts,
remote coverage, custody, or serving authority. All C01-C18 rows remain open.

The first broad library run passed 227 core tests and failed the immediate
empty-bucket observation in `live_subscription_survives_going_empty_then_repopulating`.
That test now waits up to one second for the expected cardinality before its
original assertions. Production index code is unchanged. The rerun passes 228
core and 139 federation tests in `/tmp/myko-retained-cutover-lib-tests2.log`.
Strict all-target Clippy passed in `/tmp/myko-retained-cutover-clippy.log`; a final
pass after the test adjustment is still running. Forrest's workspace run remains
pending. This is not a full Myko workspace result: the rejected split-evidence
prototype remains a failing integration test.

The Forrest workspace then caught a real remaining caller. The synchronous
roster tool requested a raw map from `FederatedRosterView`, which now correctly
rejects that conversion. The tool now uses
`ApplicationHost::snapshot_items_across_sources_selected` to read one current
local cut through the same evaluator as retained sourced publications. It checks
liveness and rejects missing payloads. The tool and retained roster share the
`AgentRosterEntry` conversion; Forrest does not fold history itself.

The new Myko API test verifies pending deletion, dependency release, and absence
of live source-cache creation. It passes in
`/tmp/myko-application-selected-snapshot-test.log`. Three messaging tests pass in
`/tmp/forrest-roster-tool-tests.log`, including immediate creation/deletion and
the original read-only bounded tool contract. Final strict checks and the Forrest
workspace rerun are pending. This trusted in-process API is not an authorization,
remote coverage, or custody proof.

The followup passes strict core/federation all-target Clippy in
`/tmp/myko-roster-snapshot-clippy.log` and both formatting checks. Independent
gpt-5.5 review accepts the bounded native and synchronous roster paths. The
latest library rerun passes 228 core tests, including the new snapshot test,
but fails an immediate ping-state assertion in
`raw_message_capture_can_be_disabled_without_affecting_dispatch`. That failure
is under investigation; `/tmp/myko-roster-snapshot-lib-tests.log` remains the
latest broad result. Forrest's second workspace run is still pending.
