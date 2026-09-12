# Query lifecycle propagation

## Phase

- [x] Reproduce ordinary query and view outputs reporting incomplete history as current.
- [x] Compare explicit retained outputs with internal dependency tracking.
- [x] Select explicit retained outputs after checking the propagation boundary.
- [x] Migrate explicit durable outputs, factories, caches, and their selected Rust consumers.
- [x] Verify native lifecycle transitions and record the remaining milestone gaps.

## Usage

An application transforms a durable snapshot without detaching its lifecycle:

```rust,ignore
fn build_view(
    ctx: QueryBuildArgs<Self>,
) -> Result<Option<impl QueryBuildOutput>, String> {
    Ok(Some(RetainedQuery::new(ctx.federated_items::<Record>()?)))
}
```

Local query builders can still return lazy Hyphae map plans. Durable builders
return retained output. Registration preserves that distinction, and local-map
conversion rejects a retained result.

Query, view, and report setup return `Result`. Generated implementations and
their callers propagate setup errors. This is a breaking Rust API change.
Forrest is not migrated while application work is shelved.

## Shape and decision

`QueryBuildOutput` materializes either a local map or a retained publication.
`QueryValue` carries that distinction through registration. The native cache
retains the existing `NativeMapOutput` publication, as retained views already do.
Durable item helpers return the source's coherent typed snapshots rather than
its raw row map. Value transformations use `LiveSubscription::map_value`, and
fallible type-erasure boundaries use `try_map_value`.

The alternative was an internal dependency collector around raw query maps.
That would preserve existing map combinators, but it needs proof that the
materialized rows correspond to the observed dependency frontier. The read-only
design review checked the locked Hyphae 3.1.1 source. Its batch scheduler does
not expose a cross-publication barrier or frontier acknowledgement. Myko's
retained publication can advance while downstream map propagation is queued.
Separately sampling those values would not establish a coherent result.

We accept a typed-output migration instead of adding a second readiness tracker.
This does not make storage nodes execute handlers or make local causal
completeness prove remote coverage, assignment, or authority.

## Cached-origin frontier progress

The persisted scope-continuity test exposed a second failure after the typed
output migration. An origin-specific cached query stopped opening after another
origin updated the same scope. Complete retained history and a fresh local
projection were both available, so transferring more history was not the fix.

`ItemProjectionWatch` processed the new local log cut but suppressed its output
when the selected origin's rows and liveness stayed unchanged. The cached
publication retained the old cursor. Native handler opening required the newer
local cut and waited indefinitely for that cached cursor to advance.

The projection driver now publishes every processed cut, including unchanged
rows. `cached_origin_query_opens_after_another_origin_advances_history` failed
with no initial frame before this change and passes afterward. The persisted
replacement-node test also passes. The lower-level regression checks unchanged
rows, current liveness, an empty diff, and the exact consumed cursor for events
from another origin and scope.

This adds cursor-only updates to retained subscriptions. They describe local log
progress, not changes to the selected items. They reveal that progress even when
no selected row changes. No extra history replay is added at the watch boundary;
the watch already projected each consumed cut before suppressing its result.
The older `ItemQueryWatch` changed-only API is unchanged. Open-time freshness
gating remains in place.

## Evidence and remaining work

Before the change, the full schema core suite passed 264 tests. Two new native
registration regressions then failed: `ProjectionRecords` and
`ProjectionRecordView` both labelled history with a missing causal parent
`Current`. These tests run through `ApplicationHost::open_handler` and the native
frame publisher, not a transport connection or assigned-executor failover.

The full core suite now passes 272 tests. New checks cover ordinary and nested query
and view opens, recovery on the same native stream, shared query publications,
and stale-read and raw-map rejection. Recovery uses the existing keyed-delta
protocol. The first expanded run incorrectly expected a second snapshot; its
four failures were corrected by checking the delta's rows, cursor, and sequence.
After adding cursor-only progress, three tests exposed an assumption that the
next publication must be the commit rather than its preceding admission. Query
and report tests now select the exact required cut before checking its value and
liveness. The queued-history test also checks the earlier admission's unchanged
rows and current liveness before requiring desync at the pending commit cut.

`bash scripts/verify-query-lifecycle.sh` reruns federation unit tests, core
unit tests and schema-enabled integration tests, local transport tests, native
handler namespaces, and persisted scope continuity. Bench-gated integration
tests do not run with this feature set. The final run passed 152 federation unit
tests, 272 core unit tests, 22 enabled core integration tests, 31 local tests,
eight native handler tests, and the persisted scope-continuity test. That is
486 passing tests. The earlier remote-read timeout led to the cached-origin
frontier fix described above.

The local restart test checks retention of the exact last observed current
frontier and advancement after recovery. It tracks intervening command-admission
publications instead of assuming the initial cursor remains current until
disconnect. The test also checks the recovered rows and passes.

Strict Clippy rejected a panic in the raw local-query API and an unchecked typed
view conversion, plus four type/documentation/test-style issues. Raw map APIs and
handler setup now return `Result` so factories can propagate failures.
Typed conversion now uses `try_map_value`, which retains the last converted value
and its cursor under `Invalid` until conversion succeeds. Its two focused tests
pass. A new report setup-failure regression exposed a leaked computation gate.
Reports now use the same drop guard as queries and views, and that regression
passes. Strict Clippy now passes for federation, core, node, server, local, and
Iroh with `myko-node/schema` and all targets. These component checks do not prove
the full milestone.

MCP finite query reads require a current result. The legacy WebSocket map
protocol cannot preserve retained lifecycle, so it rejects retained query
outputs with `QueryError`. Native subscriptions remain the retained path.
Generated local-store queries, scope readiness at command admission, and executor
selection still require implementation or separate evidence. The first milestone
and full application-builder goal remain open.

## Generated item handlers

Native opens of generated get-all, get-by-IDs, filtered queries, single-item
reports, and count reports now use the request's durable source and scope.
Their transformations use `RetainedQuery`, `RetainedReport`, and `map_value` from
the existing output design. Generated handler parameters and serialized output
shapes do not change. No new routing or readiness API is introduced.

Process-local contexts still produce explicit `LocalMap` or `LocalCell` output.
The request context selects this branch before opening a source. A failed
durable source open propagates its error; it cannot fall back to a local store.
Filtered durable reads evaluate the predicate over the selected snapshot. This
does not add indexed pushdown or a dynamic-filter subscription API.

The six initial regressions in `federated_source/tests/generated.rs` all failed
before the fix because the generated handlers labeled incomplete history
`Current`. They pass after the fix and recover through the same native stream
when the missing causal dependency arrives. The expanded cases also check an
excluded origin with a colliding ID, duplicate and empty ID lists, nonmatching
filters, and an absent scalar item. All nine expanded cases pass.

The local startup and restart tests previously expected a generated count of
zero despite committed records. They now require one record at startup and two
after the second commit and recovery. Their shared-socket, stream, drop, and
command-rejection assertions are unchanged. All 31 local transport tests pass.
The native handler-namespace suite passes eight tests, and the persisted
scope-continuity scenario also passes. These are separate component runs, not a
successful full lifecycle-script run.

The cache-ownership fixture now declares its local-map handler explicitly instead
of borrowing a generated handler. Both cache-ownership tests preserve their
assertions and use the existing shared-scheduler permit. A separate passing test
requires a generated native query without a federation runtime to return an error
and leave no cache entry or compute gate. The four enabled core integration
suites also pass all 22 tests.

Full parallel validation is not green. Runs `44948` and `75516` failed different
reactive observation assertions. After adding the missing scheduler permits to
the touched cache fixtures, run `83607` still passed 283 core tests and failed
`related_entity_windows_suppress_off_page_updates` with zero rows instead of two.
No graph or client query-map code was changed for this unit. The isolated
windowed-query test and strict Clippy passed in `38854`; that does not resolve
the full-suite failure. The lifecycle script retains its fail-fast behavior and
does not skip these tests.

Final strict Clippy in `35524` passes for macros, federation, core, node, server,
local, and Iroh with schema and all targets. The preceding lint run caught
imports after the newly added scheduler guards; moving those imports before the
first statement changed no test behavior. Targeted formatting, shell syntax,
diff whitespace, and decision-log field checks pass. A gpt-5.5 bounded review
found no source-level blocker and required the full-suite and WebAssembly gaps
to remain explicit.

The WebAssembly check in `2788` failed with 45 errors, including unavailable
native federation, application-host, and authority symbols. No clean WebAssembly
baseline was established. This unit does not claim cross-target compatibility;
bench-gated tests, the full workspace, and Apple builds were not verified.

These tests exercise registered handler execution and native frames using
in-memory history. They do not exercise a network failover, persisted custody,
authorization changes, or stale-dependent command admission. The first
milestone remains open. The later [failover startup decision](assigned-handler-design.md#agreed-startup-boundary-and-remaining-verification)
requires a majority-confirmed history boundary, whose runtime proof remains open.

## Window publication ordering

The indexed graph window and ordinary map window used their last published cell
to decide whether a value update was off-page. Hyphae can defer that cell inside
a batch. Selecting page B and then updating a row on B could therefore discard
the update based on the still-published page A. The stale row survived after the
batch settled. This was a production lost-update bug, not only a test sampling
an unfinished publication.

Both writers now keep the latest selected snapshot alongside the selection under
their existing mutex. Diff filtering uses that snapshot. Each writer releases
the mutex before publishing and retains the existing reentrant dispatch guard.
This preserves off-page suppression without requiring synchronous cell delivery.
It does not change transport routing, scope readiness, or the storage-only role.

`core::graph::tests::window_publication` reproduces the bug for indexed and map
windows with both offset and cursor selection. All four cases failed before the
fix in run `37984` and passed afterward in `21667`. The lifecycle script now
runs these cases explicitly before the broad core checks.

Run `1156` completes the full lifecycle script successfully: macro 9, federation
152, core 288, core integration 22, local transport 31, native namespaces 8, and
persisted scope continuity 1. The four focused cases also run separately. The
pre-fix parallel core run `84249` failed an off-page pointer-identity assertion.
That intermittent failure is distinct from the deterministic lost-update proof;
one green broad run does not establish that all scheduler timing failures are
resolved. The earlier WebAssembly failure remains unaddressed and was not rerun.

A repeat in `36495` confirms that limitation: core passed 287 tests and failed
`windowed_query_watch_shares_orders_and_moves_one_live_subscription` on the
derived `items()` value. The loop stopped at that first failure. No existing
test assertion or scheduler permit was changed in this unit. Strict Clippy in
`72424` passes for all seven selected packages, schema, and all targets after
making the two test-selection enums `Copy`. The earlier lint run `89536` rejected
passing those enums by value without consuming them.

## Handler authorization boundary

`HandlerClientError::Authorization` now carries the server's full
`AuthorizationDecision`. The local multiplexed handler reader and the Iroh
handler error converter preserve that decision instead of reducing it to
`Protocol`. Iroh also preserves the typed `AuthorityUnavailable` reason instead
of reducing it to `Transport`. That first boundary correction did not change
retry classification. The lifecycle extension below adds a serialized liveness
variant and changes how reactive owners handle authorization failures.

Four regressions failed before the adapter correction and pass afterward. Two
local socket tests compare the received decision with the policy's exact denial,
both at admission and after revocation without an application-data change. They
also open a new `follow_query` subscription after access returns and verify that
the shared socket was not replaced. The two Iroh tests exercise error conversion directly;
they do not prove remote subscription recovery.

Those initial tests cover the transport boundary, not the complete AB13
revocation contract. They do not establish partial aggregate revocation or
failover readiness.

The existing local ownership tests and native Iroh descriptor/open tests now
require `AuthorizationDecision::Deny` and the original rejection reason. Their
old `Protocol` assertions failed after the adapter correction. Actual contract
drift still requires `Protocol`. The lifecycle script also runs both native
contract suites so these distinctions are checked over Iroh, not only by the
new converter tests.

## Owned authorization lifecycle

`SubscriptionLiveness::AuthorizationBlocked` carries a denial or challenge.
Core handler owners and local and Iroh item owners now clear protected output
and retry the stored request after that interruption. An authority or transport
outage retains stale output only if it has not already been cleared by denial.
The same handle can recover after authorization succeeds and the existing
coherence checks report `Current`. Core handler protocol and decoding failures
remain terminal.

Scalar writers clear both value and cursor. Collection writers publish an empty
reset with blocked status in one revision. Maps and joins cannot retain their
old protected result. The current union policy blocks its whole output when a
whole input is denied. It does not claim row-level partial authorization.

The local socket regression first failed because the query retained `Some(true)`
after denial. Four local authorization tests now pass, including initial denied
handlers and the same query, report, view, and item handles through outage,
denial, and regrant. They assert one accepted socket. Two decoder regressions
also failed before blocked snapshots and deltas cleared values, cursors, and
keys. Both pass after normalization. Independent review caught and helped remove
ambiguous seed detection and an unbounded initialization loop. The final
composition uses ordered publications and a finite bootstrap capture.

The full lifecycle script passes in `81634`: core 291, core integration 22,
federation 163, local 35, macro 9, native Iroh contracts 8, native namespaces 8,
and persisted continuity 1. The focused authorization and window cases also
pass separately. Strict schema-enabled, all-target linting passes for the seven
selected packages in `33638`. The final lint cleanup removes one unnecessary
clone and splits the test fixture without dropping assertions. This successful run
does not prove that earlier intermittent scheduler failures are resolved, and
the earlier WebAssembly failure was not rerun.

See [authorization lifecycle](authorization-lifecycle-design.md) for the design
comparison and remaining API tradeoffs.

### Native authorization recovery

The three tests in
[`handler_authorization.rs`](../libs/myko/iroh/tests/handler_authorization.rs)
pass over real loopback Iroh endpoints. They retain the same query, report,
view, and item owners while a server-side test policy changes access.
An authority outage preserves the last permitted value with desync. After
recovery, idle-stream revocation clears values, cursors, and collection rows
with an explicit denial. Commands built from those denied handles fail before
their builders run. Regrant restores the original handles, including a record
added while access was denied. Initially denied handler owners also recover.
An initially denied item open returns its typed error before an owner exists.

The scenarios pass against the existing production implementation. The first
compile attempt needed missing trait imports, not a runtime fix. The lifecycle
script now includes this native test target.

The expanded lifecycle script passes in `15725`. Strict schema-enabled Iroh
linting passes with all targets in `56934`. After the lint-only test cleanup,
the three focused native tests pass again with schema in `4605` and without
schema in `90836`.

These tests use a mutable `AccessPolicy` and in-memory fixture history. They do
not prove durable identity-grant propagation, partial aggregate revocation,
server-side command freshness races, or serving-node failover. The next section
adds durable-grant coverage through retained subscriptions.

### Certified grant lifecycle

[`native_grant_subscriptions.rs`](../libs/myko/authority/tests/support/native_grant_subscriptions.rs)
opens item, query, report, and view owners together, backed by two installed
native authority controllers and Redb journals. The test queues real `RevokeAuthorityFact` and
`IssueAuthorityGrant` commands. It does not toggle the serving policy or copy
history manually.

Each owner must expose its permitted value with a cursor, then clear protected
values and cursors with an explicit denial. The test checks the underlying row
maps and report output too. A command built from each denied subscription must
fail before its builder runs. A replacement grant must restore the original
value on those same owners. A second, ungranted transport principal must remain
unable to open the view before and after regrant. Reopening the second controller
must preserve both committed authority commands and a non-genesis certified
historical head.

The earlier view-only lifecycle script passed in `88003`, including reopen.
That test took 122.68 seconds. Strict Clippy passed for the
schema-enabled authority test target in `74833`. An earlier strict run rejected
a non-Send error held across shutdown; the helper now converts it before awaiting
shutdown. No production code changed.

Latency remains unresolved. An earlier timed run took 117.87 seconds, including
28.84 seconds waiting for view recovery after the replacement grant committed.
Cleanup took 0.216 seconds. These timings identify slow phases, not their cause.

This fixture does not prove that the reopened controller can serve subscriptions,
serving-node failover, group or delegated grants, partial aggregate revocation,
or server-side command freshness races. Ungranted initial query, report, and item
opens are not part of this fixture. The broader AB13 acceptance remains open. The
[failover startup boundary](assigned-handler-design.md#agreed-startup-boundary-and-remaining-verification)
is now agreed, but these lifecycle tests do not implement or prove it.

The subsequent [historical replay optimization](authority-history-reuse-design.md)
reuses one successful fact result per immutable history snapshot and head.
Fresh quorum authorization and expiry checks are unchanged. The expanded
lifecycle verifier passes in `20931`; the native case takes 113.54 seconds with
26.31-second regrant recovery. The improvement is modest and the broader delay
remains unresolved.

The four-owner expansion failed the original 30-second readiness deadline in
`99818` and `43004`. A diagnostic run in `33761` kept that failure, then observed
all four owners become current after 42.46 seconds. A one-shot origin-filtered
query returned its protected row as current, ruling out permanent query-open
failure in that run. The delay's full cause remains unproven.

The expanded functional test now uses a 120-second phase budget for the four
streams sharing one coordinator. This is a test completion budget, not a latency
guarantee or a performance fix. Temporary probes and extra observation intervals
were removed.

The clean run `54330` failed after 395.71 seconds. All four owners initially
became current after 38.16 seconds. Revocation committed after 69.09 seconds,
and all owners reported explicit denial with cleared output after another
49.10 seconds. Their dependent command builders did not run. The replacement
grant committed after 83.92 seconds, but all four owners remained denied through
the next 120-second recovery deadline. Cleanup completed in 0.071 seconds.

The four-owner test therefore reproduces an unresolved recovery failure. It does
not prove permanent denial, successful regrant recovery, or journal reopen in
that run. The lifecycle verifier is red. The next investigation must distinguish
slow or starved certified authorization from incorrect retry/decision reuse.
No runtime fix or additional timeout increase is included here.

#### Recovery trace and duplicate hash work

The next traced run `26842` also fails, in 396.84 seconds. Its request IDs show
fresh retries receiving permits after regrant. Some admissions take about
88 to 90 seconds, leaving the owners waiting for authorized initial output at
the 120-second deadline. This disproves permanent reuse of one denied request
as the explanation for that run. It does not prove successful handle recovery.

`bash scripts/measure-authority-lifecycle.sh --trace` records request identities,
authorization outcomes, coordinator stages, and journal replay timings under
`target/agent`. `node scripts/summarize-authority-trace.mjs TRACE.log` summarizes
the captured log. Stage timings overlap and are not an exclusive CPU profile.
The second instrumented baseline, `24301`, fails in 385.57 seconds. Both traced
runs overlap brief compilation or supporting tests, so neither is an isolated
performance baseline.

The control-evidence index now reuses the preceding acceptance payload's hash
within one indexing pass when the complete slot and value match. It keeps only
borrowed references and one hash. No permission, signature check, historical
certificate, or cross-call result is added to this reuse. The focused test
fails before the change with six hashes instead of three and passes afterward.
The same test checks distinct value and epoch buckets and repeats indexing.
It tests indexing, not signature validity. The existing signed control-chain
suite supplies that adjacent safety coverage.

The post-change run `54513` fails in 371.38 seconds. All four owners become
current after 35.18 seconds. The replacement grant commits after 76.61 seconds,
but no owner recovers within the unchanged 120-second deadline. Fresh permits
still take about 81 to 82 seconds. No other owned Cargo run or profiler overlaps
this measurement. Duplicate hash reuse does not fix recovery. AB13 remains open.

#### Decode once during authority projection

`project_mutation` decoded each set to check its realm, then decoded it again
inside `ItemProjection::apply`. It now checks the realm on the materialized
item. Matching sets that `apply` ignores because of a foreign service still
fail. Schema, payload identity, scope metadata, and immutable-record checks
remain in place. The reconstruction discards its private projection on error;
this helper does not mutate a shared live projection.

The counted-deserializer regression fails before the change with two decodes
instead of one. It passes afterward. Negative cases cover a foreign service,
wrong schema version, mismatched identity, scope metadata, malformed payload,
and wrong realm even when a valid item already exists. Other cases reject
immutable replacement or deletion and malformed mutable deletion.

The authority unit, history, consumption, and controller-rotation suites pass
53 tests in `4320`. The final three focused tests pass in `68543`, and strict
authority library and test linting passes in `49484`.

The full lifecycle run `63580` fails in 367.47 seconds, compared with 371.38
seconds before this change. All four owners become current after 34.85 seconds.
Revocation commits after 62.24 seconds, followed by cleared, denied outputs
after 43.23 seconds. The replacement grant commits after 75.03 seconds, but no
owner recovers within the unchanged 120-second deadline. Fresh permits take
about 79 to 80 seconds. Cleanup takes 0.128 seconds, and reopen is not reached.
No other owned Cargo run or profiler overlaps this measurement. These runs do
not establish a material speedup or fix recovery. AB13 remains open.

The source audit also identifies a separate transfer cost to measure.
`IrohScopedEvidenceEndpoint::refresh_scopes` always passes `None` to
`pull_scope_on`, requesting each exact scope from its beginning. The existing
`ScopedReplicationCheckpoint` already binds a resume position to one source
history and scope, and `pull_scope_on` resets it when the source identity
changes. The authority coordinator also revisits complete retained event
vectors during synchronization. These paths are investigation targets, not
proven explanations of the complete delay. No checkpoint or synchronization
behavior changes in this unit.

#### Incremental scoped evidence refresh

The instrumented baseline `66057` fails in 375.72 seconds. Its 485 successful
scope refreshes apply 379 events and duplicate-check 79,822 previously retained
events. Those counts measure redundant replay, not an exclusive CPU profile or
proof of the complete recovery failure's cause.

`IrohScopedEvidenceEndpoint` now shares transient checked cursors across clones.
Each exact scope has a separate refresh lock. The lock wait and pull use the
existing timeout. A successful report updates only that scope's source-bound
checkpoint before releasing its lock. Failures and cancellation retain the prior
cursor. Every call still makes a fresh authorized request, even for empty pulls.
Recreating the adapter safely loses the optimization, not retained history.

The [design comparison](scope-evidence-refresh-design.md) records the ownership
alternatives and the scoped synthesis. Four real-wire regressions fail before
the change. Five focused tests pass afterward, including partial ingestion
failure. The final transport library and execution-evidence integration run
passes 40 tests in `25310`. Strict linting passes in `95757`.

The broader run also exposed an old test that expected terminal invalidation on
revocation. Its observed output was the agreed typed denial with cleared value
and cursor. The corrected assertion requires that state and rejects terminal
invalidation. No runtime authorization change was made for that test.

The unchanged four-owner native lifecycle `91092` passes in 231.65 seconds.
Initial readiness takes 16.31 seconds. Revocation commits in 29.87 seconds,
followed by cleared, explicitly denied outputs after 20.82 seconds. Dependent
command builders remain blocked. Regrant commits in 35.37 seconds, and all four
original handles recover their protected value after another 76.09 seconds.
The ungranted principal remains denied. Cleanup takes 0.456 seconds, and the
second controller's journal reopens with the committed authority commands and
certified history retained.

The trace records 548 successful transfers, 424 applied events, and 425 duplicate
events. Compared with the failing baseline, this run completes additional
recovery and reopen phases, so the counts are not a normalized throughput
benchmark. No other owned Cargo run or profiler overlaps the native scenario.
The formerly failing certified-grant recovery fixture now passes without a
deadline increase. Recovery latency is still high. AB13 remains open for its
remaining coverage, and startup history confirmation and serving-node failover
remain unproven.
