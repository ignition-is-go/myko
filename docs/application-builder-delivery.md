# Myko application-builder delivery evidence

This is the acceptance and audit record for the
[application-builder contract](application-builder-contract.md). All requirements
remain open until behavior is verified against the current implementation.
Documentation and existing code are not substitutes for runtime proof.

## First milestone: subscription failover

One stable typed reactive handle survives loss of its serving node. A neutral
test application uses public Myko APIs. Framework code owns recovery and command
admission. The test does not substitute application reconnect logic, manual
replication on reads, or permissive authorization for these mechanisms.

Required observations are:

1. A client receives a coherent value from serving node A for an explicitly
   placed scope. Another compatible, assigned node B can serve that scope.
2. In the case where B is not yet ready, loss of A changes the same handle to
   desynchronized while preserving its last coherent value. Mesh health reports
   degradation separately.
3. A command dependent on that stale value fails explicitly without being
   accepted, mutating data, or executing later after recovery.
4. Before B starts serving after failover, it obtains a fresh authenticated
   scope-history boundary confirmed by a strict majority of the scope's designated
   coordinators. While confirmation or catch-up is incomplete, the handle remains
   desynchronized. No intermediate value is published as current and no
   stale-dependent command is accepted. A readable local copy does not bypass
   confirmation when the majority is unavailable.
5. When B has confirmed the boundary and projected the required history, the
   original handle recovers through B. A subsequent valid command succeeds and
   its update arrives on that handle.
6. A control scope that remains ready can continue serving while the affected
   scope catches up. Shared dependencies propagate the affected scope's desync.
7. Dropping the handle releases its subscription. Repeated failover does not
   accumulate abandoned streams or duplicate command effects.

The proof must use real Myko transport and persisted scope history. Focused tests
can establish individual invariants but do not alone complete the milestone.

The startup boundary is agreed policy as of 2026-09-12, not verified runtime
behavior. An additional hot-standby case can target an invisible switch when
continuous readiness is proven. It does not replace the required cases with an
unavailable majority or a lagging replacement, or permit hiding an unproven interval.
See [assigned handler failover](assigned-handler-design.md#agreed-startup-boundary-and-remaining-verification)
for the remaining protocol and test work.

The [startup-history counterexample](failover-startup-design.md#executable-counterexample)
shows that a fresh assignment quorum can succeed while both surviving controllers
lack a typed command durably committed on the unavailable node. The original
server publishes the command's record as `Current`; after it stops, the replacement
publishes empty `Current` through the same typed query. This diagnostic uses
separate subscriptions, not automatic takeover of one stable handle. It reproduces
the missing serving gate. The write-durability relation needed to certify complete
recovery remains open.

The operator has chosen an explicit command-result lifecycle with both
`quorum_replicated` and `fully_replicated`. Callers explicitly choose which
milestone to await; neither is an implicit default. The return-value and milestone
choices are settled. Replication evidence, holder and quorum rules, and the
relationship to projection `Current` still need implementation and runtime proof.
The operator clarified that `replicated` means the durable-copy write quorum.
It does not mean every required copy or the coordinator majority approving failover.
`fully_replicated` remains the all-required-copies milestone.

The first [command lifecycle unit](command-lifecycle-design.md#verification-of-the-first-unit)
now binds an expected holder assertion to an actual committed command and frozen
selected history. Five new tests cover mismatch rejection, signed assertions,
different local recording positions, and durable reopen; seven adjacent tests also
pass. This is framework evidence plumbing, not the lifecycle waiter or a production
replication acknowledgment exchange. Holder thresholds and the serving boundary
remain open; no failover acceptance row closes from this unit.

## Full acceptance checklist

| ID | Required evidence | Status |
| --- | --- | --- |
| AB01 | Agreed contract, boundaries, and explicit unknowns recorded. | Recorded and checked against the agreed design |
| AB02 | Evidence-backed keep, change, and remove audit of each required subsystem. | In progress |
| AB03 | Neutral applications exercise nodes, nested scopes, commands, queries, reports, views, and application-owned sagas. | Open |
| AB04 | Local mesh runs without required central infrastructure. | Open |
| AB05 | Execution scales within explicit assignments; join alone does not redistribute placement. | Open |
| AB06 | Nested placement, observed durability, separate storage and execution assignments, and scope continuity through node churn work. | Open |
| AB07 | Generic storage-only binary persists and recovers opaque scope history without application execution. | Open |
| AB08 | Stable subscriptions pass every first-milestone observation above. | Open |
| AB09 | Typed internal and client dependencies propagate readiness and reject stale commands, including concurrent admission races. | Open |
| AB10 | Same command identity recovers an outcome across retries and reroutes without a second mutation. | Open |
| AB11 | Same-scope preconditions select coordination; partitions and membership changes preserve existing-majority requirements. | Open |
| AB12 | Identity-aware sharing supports groups, exact and subtree grants, restricted delegation, and distinct read and command permissions. | Open |
| AB13 | Server-enforced revocation clears protected output with explicit status; restored grants recover the same handle. | Open |
| AB14 | Typed inter-service reads and commands work across binaries in one mesh without bypassing authorization. | Open |
| AB15 | Pure application-owned saga rules execute bounded steps under service grants, with runtime resource selection per step. | Open |
| AB16 | Per-saga recovery policies expose uncertain effects honestly; every-event consumers retain replay cursors. | Open |
| AB17 | Reactive drain plans reject stale approval, replace responsibilities before removal, and block unsafe graceful removal. | Open |
| AB18 | Force eviction reports consequences and fences obsolete authority on reconnect. | Open |
| AB19 | Generated schema compatibility supports rolling updates and blocks unsafe activation while preserving interpretable history. | Open |

## Audit scope and evidence

The initial audit is bounded to durable-handler subscriptions, lifecycle state,
transport destination selection, and dependent command admission. Other acceptance
rows remain unaudited rather than presumed absent or implemented.

Audit date: 2026-09-06. Graph project `myko-7-current`, generation
`2026-09-05T01:04:50Z`. Coverage reports changed metadata for the source files
below and untracked freshness for `scope_continuity.rs`. Current source was read
directly for every finding. Graph snippets have shifted line positions, and some
call edges resolve unrelated methods with the same name. Those edges are not
evidence. This is a bounded source audit, not a repository-wide absence claim.

| Decision | Current evidence | Required change or missing proof |
| --- | --- | --- |
| Extended | [`LiveSubscriptionState` and `LiveCollectionState`](../libs/myko/federation/src/reactive.rs) bind value or collection lifecycle to a cursor. `SubscriptionLiveness` now includes a typed authorization block. | Blocked writers clear protected values and cursors atomically. These enum variants alone do not establish scope readiness. |
| Keep and extend | [`drive_handler`, `drive_view`, and `NodeHandlerSubscription::reconnect`](../libs/myko/core/src/client/durable_handler.rs) retain an owned reactive handle, report recoverable loss, and retry through the stored connector. Drop aborts the task. | Preserve ownership and retries. Prove scalar and keyed recovery, catch-up suppression, and resource release through real transport. |
| Changed authorization lifecycle | Local and Iroh handler adapters preserve `AuthorizationDecision` in `HandlerClientError::Authorization`. Owned drivers clear denied output and retry the retained request. | The local tests cover admission, idle revocation, initial denied handlers, and same-handle regrant on one socket. Native Iroh tests cover owned query, report, view, and item recovery after server-policy denial, plus explicit initial item denial. The four-owner certified-grant fixture now passes after scoped evidence refresh reuses checked cursors: all owners clear protected output and block dependent builders after revocation, recover on their original handles after regrant, and retain authority history on journal reopen. Recovery takes 76.09 seconds in the passing run, within its unchanged 120-second phase budget but still too slow. Ordered composition covers state-only revocation and identical-row regrant. Partial aggregate policy, broader load coverage, and authorization latency remain open. AB13 stays open. See [incremental scoped evidence refresh](query-lifecycle-design.md#incremental-scoped-evidence-refresh). |
| Change | [`IrohHandlerConnector::connect`](../libs/myko/iroh/src/client.rs) dials its stored `peer` and sends its stored destination. Reconnect clones the same connector and request. | This path retries the same peer. Logical scope routing must resolve another assigned, compatible, ready executor without application-managed endpoint switching. |
| Changed | [`follow_query_reactive`, `follow_report_reactive`, and `follow_view_reactive`](../libs/myko/core/src/client/durable_handler.rs) return connecting handles synchronously. Their owned tasks retry initial target resolution, then retain one prepared request across connection failures and reconnects. | Startup before the local server is covered. New report/view regressions preserve the original origin and target-derived scope after an initial open failure. This unit does not change the public API or select another assigned mesh peer. See [assigned handler failover](assigned-handler-design.md) for the proof and the agreed startup boundary whose runtime implementation remains open. |
| Keep, insufficient for AB09 | [`NodeReadiness`, `hold_startup`, and `wait_until_ready`](../libs/myko/federation/src/node.rs) provide a node-wide startup barrier. | A node-wide counter cannot by itself express scope A ready while scope B catches up. Audit existing scope and authority state before adding a second readiness mechanism. |
| Keep selected-history gating | [`selected_snapshot_state` and its retained-view tests](../libs/myko/core/src/server/federated_source.rs) inspect unresolved causal dependencies within a selection, retain ready history, and mark the output resynchronizing until dependencies arrive. | Reuse this selected-history evidence. It does not by itself establish placement, required remote coverage, compatible execution assignment, or permission to admit a dependent command. Both `application_snapshot_reports_pending_then_released_selected_history` and `registered_retained_view_gates_cached_output_and_preserves_publications` passed in the core test run. |
| Changed explicit durable outputs and standard generated item reads | [`ItemProjectionWatch`](../libs/myko/federation/src/item.rs) projects causally ready history and carries liveness through `FederatedMapSource`. Retained query, report, and view outputs preserve that lifecycle through composition, caching, and native frames. | Standard generated item queries and reports now use these outputs for native requests. Dynamic-filter, relationship, graph, and raw multi-source paths need separate evidence. Retained lifecycle alone does not establish remote coverage or execution eligibility. See [query lifecycle propagation](query-lifecycle-design.md) for current verification and limitations. |
| Keep authority checks; extend dependency contract | [`CommandSubmission` and `CommandRequest`](../libs/myko/federation/src/access.rs) carry command identity, typed payload, and authenticated claims. [`FederatedSession::submit`](../libs/myko/core/src/server/federated_session.rs) authenticates, prepares authorization, and submits. | The submission envelope has no explicit reactive dependency evidence. Payload-dependent declarations and deeper admission checks still need audit. Do not claim a complete absence of stale guards from this boundary alone. |
| Change internal report execution | [`CommandContext::from_federation`](../libs/myko/core/src/core/command/handler.rs) constructs durable execution without a server context. `exec_report` requires that context and then opens an unrouted server report. | Durable report execution needs the same typed authorization and dependency evidence as other reads. Attaching a server pointer alone would not record actual read claims or causal dependencies. Existing `exec_item_query` and `exec_selected_query` route through the federation command context, which records both. This is a source finding, not a reproduced report-command regression or proof that item-query admission covers every stale-state race. |
| Keep as baseline | [`replacement_node_materializes_scope_after_founder_and_relay_leave`](../libs/myko/node/tests/scope_continuity.rs) uses a neutral record service, persistent nodes, history transfer, remote reads, and a replacement-node command. | It opens a new subscription in `remote_records` for each check, manually transfers history, and uses `AllowAllAccessPolicy`. It is not proof of stable-handle failover, placement authority, or identity-aware sharing. Reuse its neutral domain instead of introducing Forrest dependencies. |
| Change service routing | [`FederationRouter::peer_for_service`](../libs/myko/node/src/lib.rs) selects a replication-enabled, identity-pinned peer that advertises the service. [`Peer` and `AdvertisedService`](../libs/myko/node/src/peer.rs) describe directional replication and service presence. | These records do not describe scope execution assignment or compatible schema generations. Replication eligibility is not execution eligibility. Preserve identity pinning and forwarding delegation checks, but do not use this selection rule as proof of assigned-scope failover. |
| Keep control history; add typed assignment interpretation | [`CertifiedControlChain::transitions_to`](../libs/myko/federation/src/control_chain.rs) returns ordered chosen transitions at an exact head under an independent anchor. `ControlTransition` already binds operation identity to a domain payload and rejects operation reuse in later transitions. | [`ExecutionAssignmentsAtHead`](../libs/myko/federation/src/execution_assignment.rs) now interprets explicit scope/service executor replacements. It provides historical configuration only. Current control evidence, assignment-change authorization, compatible code, and scope readiness must still reach routing. |
| Changed generated reactive sources | [`generate_get_all_query`, `generate_get_by_ids_query`, `generate_filter_query`, and the three report generators](../libs/myko/macros/src/item.rs) now use retained item snapshots for native requests. Explicit process-local contexts still use local maps and cells. | Six regressions failed before the fix because incomplete history was labeled Current, then passed with retained outputs. Expanded cases cover empty and missing results, duplicate IDs, and source exclusion. This is registered native-frame evidence, not mesh failover or stale-dependent command admission. See [generated item handlers](query-lifecycle-design.md#generated-item-handlers). |
| Changed window publication bookkeeping | Indexed graph windows and ordinary map windows retain their latest selected snapshot under the existing selection mutex. Off-page filtering no longer reads a potentially delayed output cell. | Four regressions failed with lost row updates after a batched page change and pass with the fix. The lifecycle script passed in run `1156`; this does not prove all earlier timing failures resolved or establish retained-history graph readiness. See [window publication ordering](query-lifecycle-design.md#window-publication-ordering). |
| Changed setup error propagation | [`MykoServerContext::query_map_untyped_routed`](../libs/myko/core/src/server/context.rs) now returns factory errors and rejects retained output instead of returning an empty map. Query, view, and report builders return `Result`, and their factories propagate failure. | This is a breaking Rust API change. It does not establish the complete dependency-admission design or migrate Forrest. Failed report setup now releases its computation gate through the same guard used by queries and views. |

No production code is approved for deletion by this bounded audit. Remove
application-owned reconnect and endpoint-switching workarounds from the eventual
proof path, but identify their actual callers before deleting any implementation.

## Rerunnable baseline

`bash scripts/verify-application-builder-baseline.sh` runs the execution-assignment
history tests, handler protocol and lifecycle tests, the local transport suite,
and the native retained-history
test identified above. These are
baseline checks, not completion gates for AB08 or the full goal. Test results must
be recorded separately from source findings.

The baseline also runs `myko-iroh --test execution_evidence`. Its two tests use
real native transport and Redb journals to check caller-bound assignment evidence
refresh. The controller advances after obtaining missing predecessor records,
without manually copying events in the test. Forged callers cause no import or
vote. Exact-realm history grants exclude an unrelated realm. Revoking history
access blocks the next vote without a cached-evidence fallback, and restoring
access permits an explicit retry. Unknown and duplicate source bindings fail.

Both native tests, the expanded baseline, and strict Clippy for the core and
Iroh libraries and tests passed. Targeted formatting, shell syntax, and diff
whitespace checks also passed. This proves authenticated evidence transfer only. It does not prove
current assignment authority, coordinator recovery, executor readiness, or the
stable-handle failover milestone. The storage-only role is unchanged. See
[assignment evidence refresh](execution-assignment-design.md#authenticated-assignment-evidence-refresh).

The first run passed on 2026-09-06: three handler protocol tests and one native
retained-history test. The baseline used the existing dirty worktree, including
the preserved authority and control-chain edits. It does not certify those edits
outside the behavior exercised here.

After the lifecycle fixes, verification passed:

- `cargo test -p myko --lib --target-dir target/agent -j 4`: 247 tests.
- `bash scripts/verify-application-builder-baseline.sh`: five handler tests and
  the native retained-history test.
- `cargo clippy -p myko --lib --tests --target-dir target/agent -j 4 -- -D warnings`.
- Formatting check for `durable_handler.rs`, shell syntax check, and diff
  whitespace checks. Stable rustfmt reports unsupported nightly formatting
  options but completes successfully.

## First implementation finding

`drive_view` reconciled a reconnect snapshot without checking its liveness.
`LiveCollectionWriter::reconcile` publishes `Current`, so a server snapshot marked
`Resynchronizing` became current client-side and replaced coherent rows.

The regression
`reconnecting_view_retains_coherent_rows_until_peer_is_current` failed before the
fix with `Current` versus expected `Resynchronizing`. Both received frames and
reconnect snapshots now use one exhaustive lifecycle match. Only current
snapshots reconcile rows. The regression and three adjacent protocol tests pass.

The report driver retained the desync label but replaced the coherent value with
the catch-up snapshot. Its regression,
`reconnecting_report_retains_coherent_value_until_peer_is_current`, failed before
the fix with value 2 instead of retained value 1. Report publication now retains
the previous value and cursor during connecting and resynchronizing updates.
Existing terminal-state behavior remains unchanged. Both new regressions pass.

These are component proofs of catch-up handling, not stable mesh routing. The
test connector supplies ordered connections; it does not select assigned peers
or prove server-side scope readiness. Typed command dependency admission and
automatic peer selection still need implementation audit and integration coverage.

## Initial connection is owned by the reactive handle

Reactive query, report, and view constructors return a handle in `Connecting`
without awaiting a server. The handle's task owns initial target lookup, opening,
and retry. Recoverable errors update liveness and retry with the connector's
policy. Protocol and decoding errors invalidate the handle. Dropping the handle
aborts its task. Successful reconnection still waits for a current snapshot
before replacing retained data.

This changes the Rust call shape from `follow_*_reactive(...).await?` to
`follow_*_reactive(...)?`. Code that needs an initial value must observe `Current`
rather than assume construction supplied a snapshot. The non-reactive
`follow_query`, `follow_report`, and `follow_view` methods remain asynchronous
current-then-live stream opens. Myko's callers are migrated; Forrest is untouched
and its callers will need migration when that application resumes.

`reactive_report_opened_before_server_recovers_without_a_new_handle` failed before
the change because construction returned `Transport("server is absent")`. It now
observes the initial failure, supplies a server connection, receives the value on
the original handle, and checks that dropping the owner releases the connection.

`reactive_handlers_open_before_local_server_and_share_one_socket` opens all three
handler kinds before a Unix socket listener exists. Each starts without a value,
then receives its current snapshot when the listener starts. The server observes
one shared connection. The existing local restart and 128-subscription
multiplexing tests also pass. This transport test uses the existing permissive
fixture policy; it does not prove the mesh authorization or placement contract.

Final validation after migrating initial-state assumptions:

- Six handler tests, all 13 local transport tests, and the native retained-history
  test passed through the baseline script.
- Strict Clippy passed for the core and local transport libraries and tests.
- The first full core run passed 247 tests and failed
  `core::graph::tests::generated_graph_queries_use_the_live_indexed_path` at an
  immediate post-mutation row-count assertion. The isolated rerun passed, then
  all 248 core tests passed on a full rerun without code changes. This is an
  intermittent observation, not a diagnosed or fixed graph issue.
- Formatting, shell syntax, and diff whitespace checks passed.

## Delivery order

The first implementation unit is AB08 with the AB09 admission behavior it needs.
The existing neutral record-service fixtures cover the initial transport work.
Historical scope execution assignments now use Myko's existing control history,
distinct from peer replication selection. The next routing dependency is to bind
that configuration to current control evidence, compatible execution, and scope
readiness. Observed client dependencies must also reach command admission before
AB08 can close.

Subsequent units cover storage and placement, typed
inter-service interaction, identity-aware sharing, sagas, and lifecycle workflows.
Each unit retains its acceptance rows and records rerunnable evidence. This order
does not redefine the full goal around the first passing test.

## Durable execution-assignment evidence

`ExecutionAssignmentController` now validates assignment proposals against an
anchored control history and issues evidence through `Node::vote_control` and
`Node::propose_control`. It uses their existing journal, quorum verification, and
exact-history guard. It does not extend application authorization history or
require application modules. See the [controller design and limits](execution-assignment-design.md#durable-assignment-controller).

Four Redb tests cover reopening and idempotent evidence recovery, a two-of-three
decision with accepted-value recovery, rejected foreign or malformed payloads,
and rejection by a volatile controller. The full federation and Redb suites and
strict Clippy passed. The expanded baseline also passed, including the new tests
and the native retained-history scenario. Formatting and shell checks passed.

This is a trusted local controller API, not current routing evidence. The
authenticated transport wrapper below binds framework callers to controllers.
The authenticated evidence-refresh path is described below. Coordinator recovery
and operator-intent authorization remain separate work.
AB05, AB08, and the drain and membership lifecycle rows remain open.

## Authenticated execution-control transport

`ExecutionControlEndpoint` now checks explicit principal-to-key bindings before
calling the durable assignment controller. It reuses existing control messages
and requires no application. Storage participation alone does not enable voting.
`AdministerExecution` reports this operation separately from application grant
administration and cannot be granted by `ScopeGrantPolicy`.

The five tests in `local/src/tests/execution_control.rs` exercise real Unix
sockets and Redb journals. They cover majority issuance, journal reopen with
exact no-write acceptance retry, caller impersonation and delegation denials on
all three control calls, rejected empty or ambiguous configuration, and refusal
to vote without an installed endpoint. A scope-policy check also proves that an
application admin grant does not enroll a controller. The existing baseline script includes
these through its full local transport test run.

The subsequent [control-realm routing unit](control-realm-routing.md) replaces the
single endpoint slot with explicit realm addressing. Assignment and application
authority controllers can now share a session. The
[transport limits](execution-assignment-design.md#authenticated-assignment-transport)
still record missing current-authority and executor-routing work. This does not
close AB08 or establish stable-handle failover.

Verification on 2026-09-06 passed all five focused tests, the full baseline
including 31 local tests and native retained-history recovery, and strict Clippy
for core, federation, local transport, authority, and node libraries and tests.
Targeted formatting and diff checks passed. Validation initially hit a full
filesystem; only rebuildable `target/agent/debug/incremental` caches were removed.
The interrupted new test-file write was restored before the successful reruns.

## Realm-addressed control dispatch

`ControlTarget` now addresses a realm and predecessor head in wire schema 13.
The generic `ControlEndpoint` API and `set_control_endpoint(realm, endpoint)`
registry replace the authority-named singleton. Local, Iroh, in-process, and
native node callers use the same target. Concrete endpoints validate that the
target belongs to their own anchor before voting.

The [routing design and evidence](control-realm-routing.md) record the selected
shape and its alternatives. Two new Redb-backed socket tests prove isolation
with colliding genesis heads, mixed retained history, unknown or misregistered
realms, and removal of one realm while another remains usable. Existing
controller regressions and the expanded baseline passed. Current assignment
authority and ready-executor selection remain open; this is not AB08 completion.

## Assignment coordination and interrupted-operation recovery

`ExecutionAssignmentCoordinator` now chooses assignments through configured
controller endpoints and authenticated scoped evidence sources. The
[coordinator design](execution-assignment-design.md#assignment-coordinator-design)
records the alternatives, framework boundary, and historical receipt semantics.
The implementation reuses existing quorum verification and durable voter records.
The preexisting authority coordinator is unchanged by this unit.

Five native tests in `iroh/tests/execution_coordination.rs` passed. They prove
concurrent receipt recovery, actual journal reopen without remote controllers,
preservation of a replacement executor set on old retries, minority refusal,
ballot advancement after an interrupted prepare, and accepted-value recovery
after the original proposer stops. Another case withholds the receipt until
remote evidence reaches the observer's journal, then recovers the original
ballot's result. Invalid configuration and foreign intent fail before voting.

These tests use three Redb journals, native transport, exact-realm history grants,
and no application modules or manual event copying during recovery. Strict
Clippy passed for core and Iroh libraries and tests. The expanded baseline and
three additional runs of the five-test coordination suite passed. Targeted
formatting, shell syntax, and diff whitespace checks passed. No wire version or
persistence schema changed in this unit.

This completes the tested assignment-operation recovery path, not current
assignment authority or application execution failover. Operator-intent
authorization, current-head fencing, compatible ready-executor selection, and
the stable reactive handle proof remain open. AB05, AB08, and AB09 are not closed.

## Fresh execution-assignment observations

`ExecutionAssignmentCoordinator::observe` now chooses a fresh, no-change control
operation and returns assignments at that observation point. It cannot satisfy a
new call with a historical receipt. The
[observation design](execution-assignment-design.md#fresh-assignment-observations)
records why duplicate prepare responses are insufficient and why this operation
does not provide a lease or ongoing execution permission.

Two native tests first failed against retained-only reads, then passed with
quorum-backed observation. Four observation tests now cover new operation
identities, refusal when only a minority is available, stale-observer catch-up,
accepted-assignment recovery, and recovery of an old observation without using
it as the new call's result. A Redb test rejects malformed or misbound observation
payloads before proposal and acceptance recording. The nine native coordination
tests and five durable controller tests passed.

The expanded baseline and three additional runs of the four observation tests
passed. Strict Clippy passed for core, federation, Redb, and Iroh libraries and
tests. Targeted formatting, shell syntax, and diff whitespace checks passed.

These checks remain inside Myko's explicitly configured control role. They do
not enable typed application queries on storage-only nodes. Fresh observation is
one input to future routing; compatible code, scope readiness, use-time checks,
and stable-handle failover still need integration. AB08 and AB09 remain open.

## Generated item schemas and rollout hazards

The schema audit found two executable counterexamples in
[`schema_identity.rs`](../libs/myko/items/tests/schema_identity.rs):

- Matching service identity, item name, and declared version can still hide a
  field-type change or a Serde wire rename that makes retained records undecodable.
- Bidirectional decoding can succeed while an older full-item writer drops a
  newer defaulted field. The original log entry survives, but the subsequent
  replacement loses that field in projected state.

`ItemMutation::is` now documents that it matches declared identifiers, not
serialized contracts. `AdvertisedService` documents that advertisement establishes
neither assignment nor compatibility nor per-scope readiness. These documentation
changes do not add an admission check.

The opt-in `myko-items/schema` feature now generates separate serialization and
deserialization schemas. The existing macros derive structural metadata for
items, IDs, subtypes, command inputs, and generated item-query inputs. The service
macro generates `MykoService::item_schemas` from its existing item list,
including declared identity and scope metadata. No parallel application-maintained
schema list is required.

Generation uses the existing locked Schemars 1.2.2 dependency. Its
[Serde-aware derive](https://graham.cool/schemars/deriving/attributes/) handles
nested payload structure and supported wire attributes. Custom serialization
needs an accurate schema implementation or override. Generated structure alone
does not prove custom validation semantics, safe writer activation, or migrations.

Verified component behavior:

- Seven existing item tests and six integration checks pass with schema generation
  enabled. The default build passes its seven item tests and two audit checks.
- Tests distinguish same-named schema changes, verify read/write default handling,
  and cover nested tagged enums, collections, generated scope IDs, wire renames,
  unknown-field rejection metadata, and generated query inputs.
- A negative control using the deserialization schema for both directions fails
  the defaulted-field check. Restoring separate generation passes.
- Strict Clippy passes for the item library and tests with the feature enabled.
- The native-node default build passes.
- The expanded application-builder baseline passes, including both item feature
  configurations. Targeted formatting, shell syntax, and diff checks pass.

The initial native compile failed on authority payloads without `JsonSchema`,
including `Principal`, `AuthorityGrant`, and `ScopeTopology`. These framework
contracts now derive their schemas. The `myko-node/schema` feature forwards
support through authority, Iroh, federation, core, and both macro crates.
Applications using the runtime facade enable `myko/schema`. Enabling only the
leaf `myko-items/schema` feature does not configure its callers.

The broader compile check also exposed the separate `myko::myko_item` expansion.
That facade now generates item, ID, and normal subtype schemas. Subtypes declared
with `manual(serde)` still require explicit schema support rather than deriving
a possibly unrelated Rust-field representation. Reactive handler integration is
described below, followed by durable command payload registration.

The Iroh adapter supplies `NativeNodeDescriptor`, `NativePeerReference`, pairing
invitation, and pairing receipt schemas. Its external `EndpointAddr` adapter
describes relay, IPv4, IPv6, and custom transport payloads. It distinguishes
accepted base32 keys from emitted lowercase hex, duplicate address inputs from
unique outputs, and accepted unknown struct fields from emitted fields. A pinned
`iroh-base` dependency and a compile-time type identity check bind the adapter to
the inspected external wire type. Public-key curve validity and custom decoder
semantics remain outside structural compatibility evidence.

The tests in the Iroh and native-node `generated_schema` targets compare actual
encoded payloads and decoder outcomes with JSON Schema validation. They resolve
both contracts for all 12 authority and seven federation item modules. The normal
runtime facade also has an item, ID, and nested-subtype generation test. The validator
is a development dependency with file and network resolution disabled.

An additional codec audit checks Iroh custom bytes. `serde_json::from_slice`
accepts a JSON string as bytes, but `serde_json::from_value` rejects that same
representation. The adapter conservatively describes byte arrays, which both
paths accept and Iroh emits. It does not claim to enumerate every decoder's
accepted input. Compatibility evidence must account for decoder paths and custom
validation; comparing JSON Schema documents alone cannot establish that evidence.

Native schema verification:

- All eight Iroh/native schema integration tests pass.
- All nine macro unit tests pass with schema generation enabled.
- Federation, authority, Iroh, and native-node test targets compile with schema
  generation enabled. This compile check does not claim those entire suites ran.
- Strict Clippy passes for those four libraries and the new schema test targets.
- The expanded baseline passes. The final decoder-path audit also passes its
  targeted schema tests and strict lint check. Formatting, shell syntax, and diff
  checks pass for this unit.

The next integration work is to bind activated service contracts to directional
compatibility and activation policy. Routing still lacks these checks.
These components do not prove rolling updates or stable-handle failover;
AB08 and AB19 remain open. Storage participants remain opaque log holders and
do not acquire typed application execution from this metadata.

### Reactive handler payload schemas

With `myko/schema`, service-owned query, report, and view registrations carry
`HandlerPayloadSchema` providers. `HandlerRegistry::for_services` retains those
providers only when it retains the corresponding executable registrations.
Disabled services cannot contribute handler schemas. Global and graph handlers
without a service owner retain `None`, which is missing evidence, not compatibility.

`HandlerResultSchema::Value` describes one report result.
`HandlerResultSchema::Rows` describes one query or view row value. Framework row
keys, ordering, transaction context, revisions, and stream liveness remain outside
these typed payload schemas. No transport or log encoding changed.

The runtime macros generate schemas for owned handler arguments, item filters,
report output structs, and view row structs. Custom filter schemas describe the
existing bare equality values and `$in`, `$range`, `$contains`, and `$startsWith`
operators, not their Rust enum tags. Numeric bounds accept null and unknown fields
on input but omit them on output. A test caught the initial output schema admitting
both shapes; the range adapter now distinguishes those contracts explicitly.

Native discovery row types also derive their schemas. The existing Iroh adapter
provides the endpoint-key schema used by node-status rows. The node feature
forwards schema support to discovery; applications need no duplicate DTOs or
manual schema catalog.

The native `generated_schema` target includes checks for activation filtering,
typed argument parsing, scalar versus row outputs, nested schema resolution,
filter operators, and optional bounds. These checks are part of the existing
application-builder baseline. They do not establish compatibility, readiness,
execution assignment, or successful failover.

Verification for this integration:

- All 16 Iroh and native-node schema tests pass, including the registered
  argument parser checks and framework entity-reference filters.
- All 25 existing core filter tests pass with schema generation enabled and
  disabled, including their JSON and CBOR round trips.
- `bash scripts/verify-application-builder-baseline.sh` passes with the new
  schema tests and core filter tests included. Its storage-only test still
  proves history retention without typed application handler execution.
- Native-node, authority, and Iroh test targets compile with `schema`. This is
  compile coverage, not a claim that every test in those packages ran.
- Strict Clippy passes for the native libraries, macro library, and schema test
  targets. All nine macro unit tests pass. Formatting, shell syntax, and diff
  checks pass.

The WebAssembly dependency graph now receives `myko-items/schema` directly from
`myko/schema`, rather than depending on a native-only federation dependency.
The broader `myko` WebAssembly build still fails with and without `schema`:
both checks report 45 errors involving native application host, federation,
and handler-factory APIs exposed to that target. No WebAssembly build success is
claimed. Resolving those target boundaries remains separate from native failover.

### Durable command payload schemas

`CommandHandlerRegistration` now carries the same optional payload-schema provider
as reactive registrations. With `myko/schema`, durable registration generates the
input schema from the command type and the result schema from `CommandResultType`.
The durable executor's existing trait bound requires that result type to equal
the declared command output. Command results use `HandlerResultSchema::Value`,
including arrays and unit results. Transport and command lifecycle envelopes
remain outside the application payload schema.

`HandlerRegistry` retains activated command registrations, not just their names.
`MykoApplicationBuilder::build` constructs its execution map from that retained
set. The previous second inventory-filtering loop is removed. Command execution
still uses the service-qualified key, so commands with the same name in different
services retain distinct schemas and executors. Legacy local registrations have
no durable factory or schema evidence.

The registration macro selects schema support inside Myko. It does not test a
feature flag inside the consuming application. Runtime command macros derive
input schemas for normal service-owned commands. A command declared with
`custom_serialize` must provide its own matching schema, as arbitrary custom
serialization cannot be inferred from Rust fields.

The command tests exercise both command macro families, renamed and defaulted
arguments, malformed input admission, actual typed execution, unit results,
inactive-service rejection without log mutation, and same-name commands owned by
different services. They also resolve both schema directions for every activated
native durable command. These isolated execution tests use an explicit allow-all
policy; they do not prove production grant propagation.

The baseline also runs `myko-local` with `--features myko/schema`. That consumer
has no feature named `schema`, which checks the defining-crate macro feature
selection through its existing real-socket command and subscription tests.

The payload contracts are not rollout permits. Scope binding, application
semantics, current execution assignment, and per-scope readiness still require
their own evidence. Storage-only participants do not execute these commands.

The command unit was rechecked on 2026-09-08 after its previous verification
handle disappeared. All 22 native-node and Iroh schema tests, six schema-enabled
reactive-handle tests, and strict core and macro library Clippy checks passed.
The earlier expanded baseline had also passed. These results precede the service
contract assembly below.

### Activated service contracts

`MykoApplication::service_contract(service_id)` assembles the generated item
schemas and retained query, report, view, and durable-command payload schemas for
one activated service. The resulting `ServiceContract` keeps operation kinds in
separate namespaces and retains both schema directions. This is a description of
executable payloads, not a compatibility verdict or permission to serve a scope.

Typed activation captures `MykoService::item_schemas`. The service macro generates
that method from its existing item list. The separate `MykoServiceSchema` trait
was removed and its callers migrated. Manual services without schema evidence
return `None`, which differs from an explicitly declared empty contract.
Activating only a raw service ID cannot borrow item evidence from linked code.
Activating a manual type with missing schemas also clears earlier schema evidence
under the same identity, rather than borrowing it from another type.
Adding framework services and resources preserves the existing typed activation.

The caller needs no second catalog:

```rust
let app = MykoApplication::builder().service::<Catalog>().build();
let contract = app.service_contract(Catalog::SERVICE_ID)?;
```

The design compared a separate inventory catalog with retaining schemas at typed
activation. Typed activation won because it describes this application, including
missing evidence. A global catalog could make linked but inactive code appear
available. Handler contracts come from the retained registry used by execution,
not another inventory-filtering loop. Item schemas remain on the application so
framework composition can rebuild its registry without losing typed evidence.

Assembly rejects inactive services, missing item or handler evidence, foreign or
duplicate item metadata, duplicate retained handler keys, and scalar-versus-row
result mismatches. It excludes unowned global handlers and non-durable commands.
The native tests cover generated item and handler types, raw-ID activation,
missing evidence, composition, and service-qualified same-name commands. The core
tests cover missing handler providers, wrong result shapes, and handler-key
collisions. The existing baseline runs both sets.

The negative control removed item-schema carry-over during framework composition.
`adding_framework_services_preserves_application_contracts` then failed with
`MissingItems` for `FacadeService`. The implementation was restored before the
baseline run, which passed on 2026-09-08. It includes all 28 native-node and Iroh
schema tests, three core contract guards, both item feature configurations, and
both 31-test local transport configurations. Assignment, admission, control
coordination, and native retained-history checks also passed through the script.
The full schema-enabled core library run passed all 251 tests. Strict Clippy
passed for the core library and tests, item and item-macro libraries, and native
schema and item test targets. The broader core check first caught function-pointer
casts in the new tests and unresolved schema derives in the existing integration
test facade. Explicit function types replace the casts. The facade now names its
`schemars` re-export, as it already does for `prelude`, so macro expansion does not
depend on the glob import. Both schema-enabled prepared-command recovery tests
pass and are included in the baseline. Targeted formatting, shell syntax, and
diff checks passed.

Reactive dispatch still uses name-keyed maps. This snapshot describes whichever
registrations those maps retained; it does not repair a registration shadowed by
another service with the same handler name. Service-qualified reactive dispatch
remains open. Contract exchange and directional comparison must also reach the
connector alongside current assignment and per-scope readiness. Neither this
unit nor schema equality proves writer activation, semantic migration, or AB08.
Storage-only participants still retain opaque logs without executing handlers.

## Registered service identity at subscription admission

`HandlerRegistry` now retains the generated service owner for queries, reports,
and views. `handler_authority` resolves that owner from the retained registration,
not from client parameters. `FederatedSession` includes it in the primary scope
claim when the handler declares a scope. Additional handler claims are unchanged.
Global handlers and graph registrations without an owner remain explicitly
unowned. Service activation still excludes handlers owned by inactive services.

The tests in `local/src/tests/handler_ownership.rs` cover:

- Real Unix-socket query and view subscriptions admitted by a policy that requires
  the registered service on the primary scope claim. Both failed before the fix
  with `subscription is missing its registered service owner` and passed afterward.
- Registry resolution for generated queries, reports, and views, including
  inactive-service exclusion and attempted owner substitution through parameters.
- An unowned global report remaining available without an activated service.

The existing core authority test also checks that an unowned view's primary claim
has no invented service owner. These checks establish registration-to-admission
identity, not production grant propagation or service-qualified handler names.

This is a routing prerequisite, not replacement-node selection. Historical
assignments still need current control evidence, compatible execution, and scope
readiness. Peer service advertisements alone do not prove those properties.

Verification passed: all 248 core library tests, all 24 local library tests,
the complete `verify-application-builder-baseline.sh` run, and strict Clippy for
core and local libraries and tests. Targeted formatting and diff whitespace checks
passed. The baseline's native replacement-node test still uses fresh handles and
manual history transfer, so it does not close AB08.

### Client-to-executor service identity

Typed subscription calls are unchanged. The query, report, and view macros now
derive a static service identity from the same owner declaration as the server
registration. `MykoClient` includes it in `HandlerRequest`; the serving application
rejects an omitted or different owner before subscription authorization.
`HandlerAccess` and `AuthorizationBinding` retain that service identity even for
a handler without a primary scope. The canonical transport schema is now version
12; connected peers must use that version.

The design compared deriving ownership solely at the receiving application with
carrying generated ownership from the caller. The latter supplies client-side
executor selection with a service identity and lets the executor check the
request independently. It does not require storage nodes to interpret or route
typed application requests. Storage-only nodes are log participants, not implicit
application gateways. The earlier gateway justification was incorrect.

The local ownership tests cover generated identities, missing and forged owners
for all three handler kinds, and a node without an application retaining history
while rejecting a typed subscription. That last test uses `FederatedSession::new`.
`LocalNodeServer::spawn` is an application convenience constructor, so it is not
the no-application fixture. Federation tests check distinct authorization bindings
and preserve the serialized identity of historical unowned handlers without
inventing ownership.

At this checkpoint, the registry still indexed each handler family by name.
Expected-owner validation failed closed on a mismatch but could not retain
same-named handlers from different services. The service-qualified dispatch work
below removes that limitation. Current assignment evidence and readiness remain
required for AB08.

Verification for client-to-executor identity:

- The expanded baseline passed, including all 26 local tests, both handler
  identity tests, seven wire tests, and the native retained-history scenario.
- Federation's full suite and nine macro tests passed. The macro crate's 19
  existing documentation examples remain ignored by its test configuration.
- Strict Clippy passed for core, local, federation, wire, and macro libraries and
  tests. Formatting, shell syntax, and diff whitespace checks passed.
- Two default-parallel core runs each passed 247 of 248 tests. The first failed
  `eager_watch_is_seeded_and_tracks_edges_moving_in_and_out` at its seeded-row
  assertion; its isolated rerun passed unchanged. The second failed
  `malformed_initial_snapshot_is_atomic_and_does_not_become_ready` at a readiness
  assertion. All 248 tests passed with `--test-threads=1`. These observations do
  not diagnose or fix the parallel failures, which remain unresolved.

## Service-qualified reactive dispatch

`HandlerRegistry` now resolves each query, report, and view by its service owner
and handler name. Global handlers have a separate namespace. An omitted owner
does not search owned handlers, and a missing owned handler does not fall back
to a global handler. The activated service contract iterates retained
registrations directly, preserving same-named handlers from different services.

Native and WebSocket dispatch use the qualified lookup. Rust transaction
wrappers preserve generated service identity through static traits and erased
requests. TypeScript generation emits that identity on owned handler classes;
the client carries it into requests and subscription-cache keys. Rust caches
include the concrete handler type, so same-named handlers with identical result
types cannot share computed cells. Name-only MCP reactive requests reject
ambiguous registrations rather than choosing whichever inventory entry comes
first. In-process MCP resolution considers activated registrations.

Evidence for this unit:

- `myko-node/tests/handler_namespaces.rs` passes six checks covering real local
  socket dispatch for all three reactive handler kinds, report-cache isolation,
  static and erased wire wrappers, and per-service schema retention. The
  existing 23 generated-schema checks also pass.
- Removing concrete handler identity from the server cache key makes
  `same_named_reports_with_identical_output_types_do_not_share_cache_entries`
  fail with `report cache reused another service's handler`. The fix was
  restored after this negative control.
- The TypeScript package type-checks and passes all 28 tests. Its new real
  WebSocket test observes separate query, view, and report requests for two
  services, plus a direct-callback report, without inspecting private client
  state. Repeated watches within a service still share their subscription.
- The crate-local Flux generation command cannot locate the root `flux.toml`.
  Running the configured generator directly for `libs/myko/ts/src/generated`
  succeeds with 112 registered type exports. Generated files remain generated;
  they were not hand-edited.
- The expanded `scripts/verify-application-builder-baseline.sh` passes after
  restoration of the cache fix. Strict Clippy passes for the core and server
  libraries and tests with `myko/schema,myko/codegen-ts` enabled.
- The subsequent full core run exposed an unqualified `GetAllClients` request
  in the native map-cache fixture. Its generated registration is service-owned.
  The fixture now derives both identities from the handler type and propagates
  worker errors. Its cache-sharing and owner-release checks remain intact.
  All 252 schema-enabled core tests pass after that correction. Strict Clippy
  also passes for the namespace and generated-schema integration targets.

Two report gaps surfaced while constructing the fixture. Generated
`Get{Item}ById` reports still use `StoreRegistry` rather than the durable source;
the original fixture returned no record despite a successful durable command.
The namespace fixture therefore uses explicit durable handlers, as its query
and view fixtures do. The transport also conflated an absent publication with
a present JSON null. The nullable-publication correction below addresses that
separate gap. Generated reports remain open work.

Storage-only nodes still retain opaque history without interpreting application
handlers. This unit does not select an executor, establish readiness, add
service-qualified TypeScript export names, or complete the name-only MCP API.
The connector's fixed-peer retry path still needs assignment-aware failover.
The first milestone and AB08 remain open.

A bounded gpt-5.5 source review found no blocking issue in this dispatch unit.
It did not independently run the tests. Exact duplicate registrations within
one namespace still overwrite each other; cross-service isolation does not
establish duplicate-registration rejection. MCP commands remain name-keyed;
the ambiguity check above covers reactive handlers only.

## Nullable reactive publications

`ErasedHandlerState` must distinguish `None`, meaning no published value, from
`Some(Value::Null)`, meaning a published nullable result. Default Serde decoding
mapped both to `None`. The same ambiguity affected nullable cursor values in
handler snapshots and view deltas.

The wire boundary now omits absent fields and decodes present fields as values,
including JSON null. Typed handlers and clients keep their existing nested
`Option` distinctions. No application-specific null check or replacement value
was added. The canonical protocol version is 14. Version 13 envelopes are
rejected because their null fields do not identify which meaning was intended.
This changes transport encoding, not persisted application events.

Failing-before evidence:

- `handler_state_preserves_absent_and_present_null_payloads` decoded
  `Some(Null)` as `None` for the value and cursor.
- `view_delta_preserves_absent_and_present_null_cursors` lost the present cursor.
- `nullable_report_retains_a_published_null_over_the_socket` failed with
  `published null became an absent report value` after a durable delete.

All ten wire tests and both live socket regressions pass after the fix.
The socket test follows a non-null value through deletion to a published null,
then back to a non-null value after another durable command. A second test
observes a nullable report through a listener restart on the original reactive
handle. It retains `Some(None)` during desynchronization, recovers to current,
and receives a subsequent non-null update. All eight namespace integration tests
pass, as does strict Clippy for the wire and node libraries and this test target.

The combined core and server library run with `myko/schema,myko/codegen-ts`
passes 286 core tests and 74 server tests. That run executes the service-identity
code-generation and MCP ambiguity checks from the preceding unit. The expanded
baseline passes, including both local feature configurations, execution evidence
and coordination, and the persisted scope-history scenario. Strict Clippy passes
for the core and server libraries and tests. Targeted formatting, shell syntax,
and diff whitespace checks pass. No lint exemptions were added.

The first broader baseline stopped on the new restart test's observation-channel
type mismatch. The fixture now retains the `Arc` snapshots supplied by Hyphae.
Strict checks also required sendable observer predicates and error returns
instead of panicking assertions in the corrected cache test. The final baseline,
strict checks, and focused cache test pass after those corrections.

These fixtures retain their application host and use the existing permissive
test policy. They do not prove persistence reopening, assigned-node failover,
mesh authorization, per-scope readiness, or stale-dependent command admission.
No storage participant executes an application handler in this change.

A bounded gpt-5.5 source review found no blocking issue. It reviewed the trail
and supplied execution results, not an independent test run. Protocol-version
rejection is not a migration of old decoded JSON, and Rust results do not certify
TypeScript property-presence handling or the broader failover milestone.

## Typed projection causal readiness

`Node::watch_item_projection` and `ItemProjectionWatch` now assess ready and
pending history at the same consumed local cut. Both all-source and
source-filtered projections use this assessment. The source-filtered path no
longer applies raw events before their causal dependencies arrive.

The typed snapshot and each update carry `SubscriptionLiveness`.
`FederatedMapSource` preserves it instead of assigning `Current` unconditionally.
A parent from another origin or scope can release a selected write, so the
driver checks every received event. It publishes readiness changes even when
the row diff is empty. Exact unrelated scopes ignore the pending history;
an unscoped projection checks its applicable history conservatively.

The four initial regressions failed before the production change:

- The all-source initial and live cases labeled pending history `Current`.
- The source-filtered initial case exposed a causally incomplete write.
- The source-filtered live case labeled pending history `Current`.

All four pass after the change. Three additional regressions check scope
isolation, a readiness-only release with no row diff, and queued updates whose
parent is already available at a later cut. The queued case still reports desync
at the earlier cut and publishes the released value only at its release cut.
These checks cover both source modes.

The schema-enabled `server::federated_source::tests` run passes all 21 tests.
The full schema-enabled core library passes 259 tests, and the federation library
passes all 147 tests. Strict Clippy passes for the core
and federation libraries and test targets with `myko/schema`. The rerunnable
application-builder baseline now includes the federated-source suite and passes
through its final persisted scope-history check. Targeted formatting, shell
syntax, and diff whitespace checks also pass.

This fixes the canonical projection driver, not the full handler pipeline.
At this source checkpoint, ordinary query maps, local-view maps, and report cells
still lost source liveness on their way to client publications. The report
correction below carries lifecycle through composition and caching. Ordinary
query and local-view propagation remain open. A transport-only ready flag would
not fix internal dependent reads.
An idle source also does not yet publish every unrelated consumed local cut;
that matters if a handler waits for a node-wide cut beyond its last update.

The driver now rebuilds the selected projection from ready history for either
source mode. This removes the incorrect source-filtered incremental path but
adds replay work to that path. No large-history performance result is claimed.

Local causal completeness is necessary, but it is not proof of remote history
coverage, current execution assignment, schema compatibility, or serving
authority. The storage-only role is unchanged. AB08, AB09, and the first milestone
remain open.

A bounded gpt-5.5 source and trail review found no blocking issue. It reviewed
the supplied execution results rather than rerunning the tests. Its remaining
flags are the handler lifecycle gap, the replay cost, and the distinction
between local causal completeness and permission or readiness to serve remotely.

## Report lifecycle through registration and caching

`ReportContext` now returns coherent lifecycle-carrying item snapshots for both
source APIs. `ReportBuildOutput` accepts local pipelines or retained reports.
`ReportValue` keeps that distinction through typed reads, composition, weak
caching, type erasure, and native report frames. Converting retained output to a
raw local cell fails explicitly. The older WebSocket path returns that failure
to the client; in-process MCP reads reject non-current reports.

The unchanged-count regression failed before the fix and now receives desync and
recovery while count remains 1. Cached and composed reports preserve the same
lifecycle. Synchronous reads inspect the publication rather than a separately
settled state cell. Native frames use contiguous emitted-frame numbering even
when their internal source skips publications under backpressure.

The expanded baseline passes. Full library suites pass for core with schema
at 263 tests, federation at 149 tests, and server at 74 tests. All 15 durable-node
tests and 16 benchmark-feature report tests pass. Strict library and test checks
pass for core, federation, node, and server with node schema and core profiling.
The three benchmark-feature report test targets pass strict checks separately.
Combining benchmark and schema features fails because `BenchManualWireValue`
has no schema implementation; that combination is not verified. See the
[report lifecycle design](report-lifecycle-design.md) for the alternatives and
remaining constraints.

This is application-executor behavior. Storage-only nodes remain opaque-log
participants. A current report value is not atomic command-admission evidence.
Ordinary query/view lifecycle, generated durable reads, composite-cursor client
decoding, assigned-executor failover, and full AB08/AB09 remain open.

### Application handler contract exchange

`IrohHandlerConnector::describe` now requests generated argument and result
schemas from an activated application handler. The canonical session prepares
the same typed identity, scope, claims, and capabilities as a subscription.
Admission and continuation authorization guard metadata release, including
schema-construction errors. Description does not execute the handler.

Storage-only nodes reject these requests. They retain opaque history and do not
become application executors or implicit typed-query gateways. Surviving storage
does not imply that an application executor is available.

The descriptor checkpoint introduced wire version 15. Observed handler opens
below advance it to version 16. Descriptor evidence still needs
directional compatibility checks, current assignment, and per-scope readiness
before it can support executor selection. It is not an execution permit and does
not close the stable-handle failover milestone. See the
[handler contract design](handler-contract-exchange.md) for verification and
remaining work.

### Observed contracts at handler open

`FollowHandler` now preserves an optional inspected-contract precondition through
wire decoding and prepared request interpretation. `connect_described` opens
that exact observed contract. Core checks it before executing the handler, and
holds the application slot's read guard across the instance check and synchronous
open. Application replacement wakes inspected streams and ends the old stream.

The wire and three native regressions pass, and strict Clippy passes with the
composing node's schema feature. This is an observation precondition, not a
directional compatibility rule, assignment decision, readiness assertion, or
automatic failover. See the [open-contract design](handler-open-contract.md).

## Admission while local history is incomplete

Implemented admission boundary:

- Ground: `PreparedCommand::submit` calls the backend. The reference backend
  serializes `submit`, `admit`, and history ingestion with the same state mutex.
  It recovers matching accepted command identities before considering a new one.
- Shape: keep pending-history interpretation in `SelectedHistorySnapshot` and
  call it under the backend's existing lock before accepting a new command.
  The primary scope and declared resource selections must have no unresolved
  application history. Exact unrelated scopes remain usable. Subtrees retain
  the existing conservative handling of incomplete topology.
- Alternative rejected: checking only in `Node::prepare_command` leaves a race
  between authorization and durable submission. An application-maintained ready
  flag would duplicate framework state and have the same race.
- Errors: a typed incomplete-scope error leaves no command acceptance or queued
  execution. Retrying an already accepted identity still recovers its existing
  result. History arrival does not execute a previously rejected new submission.
- Scope: this is a necessary local-history condition, not proof of mesh coverage,
  compatible assignment, current control authority, or client observation
  freshness. Those remain required for AB08 and AB09. Execution after acceptance
  also needs its own revalidation; admission alone does not close those rows.

`new_submission_into_incomplete_scope_is_not_accepted_or_queued` and
`pending_history_arriving_after_preflight_is_checked_at_durable_submission` both
failed before the change: the first accepted a command with unresolved history,
and the second accepted after that history arrived between authorization and
submission. Both now require `NodeError::ScopeHistoryIncomplete` and verify that
no command acceptance is appended. Direct `admit` has the same check.

Six focused tests now cover those cases, exact-scope isolation, declared
dependencies, conservative subtree handling, existing-ID recovery, conflicting
ID reuse, and 32 concurrent ingestion/submission races. In a race, acceptance
must precede the pending event in the actual history order or fail without an
acceptance event.

The Redb test `incomplete_scope_admission_gate_survives_reopen` persists partial
history, rejects a new submission, reopens the database, and rejects it again.
Supplying the missing parent does not accept the rejected request. An explicit
retry succeeds and remains accepted after another reopen. All 16 Redb tests pass.

`local_submission_rejects_incomplete_scope_without_queuing` exercises the public
typed command client over a Unix socket and the existing neutral record service.
The server rejects the command within the original deadline, leaves history
unchanged, and accepts an explicit retry after catch-up on the same socket. It
uses the existing permissive policy fixture, so it proves admission and transport
behavior, not identity-aware authorization.

The full federation suite passed. Its catalog-batch fixture now gives the source
complete history and omits the parent only when transferring to the target. Its
original two-command release assertions remain intact. This avoids manufacturing
new source commands while the source itself is incomplete.

The core suite exposed the same invalid source setup in `pending_catalog`, shared
by three session tests. The fixture now gives the source complete history and
withholds the parent only from the target. No subscription or catalog assertion
changed. All 248 core tests passed after this correction.

The first core run also failed
`server::native_map::tests::publishes_real_seed_then_full_snapshot_after_delete`:
the next publication still contained the initial two rows. That test passed in
the full rerun without changes to `native_map.rs`. Its intermittent failure is
unresolved; the passing rerun does not establish its cause or a fix.

The expanded baseline passed six admission tests, one Redb reopen test, eight
assignment tests, six handler tests, all 14 local tests, and the native retained
history test. These component results do not close stable-handle mesh failover.

Strict Clippy passes for core, federation, Redb, and local libraries and tests. No lint
exemptions, independent readiness registry, or new persistence mechanism were added.

## Command delivery across a local reconnect

The local supervisor previously reopened every retained request after transport
loss, including `Submit`. Its connection helper also retried forever before the
supervisor could distinguish a waiting subscription from a new submission.
Checking a reactive handle only before calling that transport cannot prevent a
command from being accepted later, after reconnect.

The caller still uses the existing typed command client. A missing server returns
a submission failure. A lost connection after writing the request returns the
command ID with an explicit unknown-acceptance message. The caller can read that
identity's durable state and explicitly retry it. A subscription continues to
reconnect on the same shared session.

The selected shape keeps this distinction inside `ClientSupervisor`: one socket
connection attempt reports success or failure, the supervisor applies backoff to
retryable reads, and `Submit` routes never enter the reconnect queue. Existing
`ClientStreamPhase` distinguishes definitely unsent requests from requests whose
acceptance is unknown. No wire schema or application-maintained connection flag
is needed.

A preflight socket probe was rejected because connection loss can race with it.
A separate socket for every command was rejected because it breaks shared-session
ownership without solving acknowledgement loss. This comparison was local; no
new review panel or worktree was created.

This transport rule does not prove reactive dependency freshness, assigned-node
routing, or durable acceptance on the server. Those remain required. It also does
not cancel work the server already accepted.

Verification for this unit:

- `unavailable_local_server_does_not_queue_a_submission` failed before the change
  because the call waited for a server. It now reports no submission, and an
  explicit retry on the same command-only client works after startup.
- `lost_submission_is_not_replayed_on_a_new_connection` failed before the change
  because a lost acknowledgement left the submission waiting for replay. A real
  Unix peer now exercises the interruption, and the client returns unknown
  acceptance with the original command ID. Reconnecting does not submit it.
- `reconnect_removes_submissions_at_every_delivery_phase` checks queued, written,
  and opened requests. Submissions terminate while the follow route remains.
- `live_handler_survives_local_server_restart` now submits while its retained view
  reports desync. The call fails without appending history, does not reappear
  after the same view recovers, and succeeds only on explicit retry. Its existing
  command watch still recovers an already accepted command on the shared socket.
- All 17 local tests and strict local Clippy passed. The protocol remains private;
  the interruption peer is a test-only helper. No lint exemptions were added.

All ten repetitions of the 17-test local suite passed. The expanded baseline also
passed, including the native retained-history test. Formatting, shell syntax, and
diff whitespace checks passed. This verifies local delivery and reconnect
behavior, not the remaining multi-node milestone.

## Client commands built from reactive dependencies

The caller uses `client.submit_from(&live_state, |value| command)`. A collection
can supply `as_subscription()`, and reactive joins carry multiple dependencies.
The command builder receives the typed value, not a separate freshness boolean.
Commands without reactive inputs keep the existing submission method.

The method belongs to the transport-neutral `CommandClient` trait. It checks the
source publication at invocation, when the returned future is first polled, and
after command construction and serialization. It reads the publication itself,
not a derived state cell that can lag the publication's callback. An invocation
made while stale remains rejected even if recovery occurs before polling. A new
invocation after recovery uses the current value.

`CommandDependencyNotCurrent` preserves the observed lifecycle variant.
`CommandDependencyMissingValue` rejects a current publication without a value.
An optional cursor is not treated as failure: runtime-backed reactive values can
be current without a durable-log cursor. No transport call occurs after these
client-side failures.

The alternative of accepting a command plus a detached readiness snapshot was
rejected because the snapshot can outlive its source's synchronization. Requiring
applications to check lifecycle themselves duplicates framework policy and misses
the deferred-future case. The selected method retains the reactive source through
the dispatch checks and does not introduce a mutable readiness store.

This method enforces published client state only. It is not a portable observation
receipt, a server-side precondition, or proof of readiness across nodes. Changes
after dispatch still require server admission and execution checks. Stable-handle
mesh failover and complete stale-command race protection remain open.

The new `local::tests::command_dependencies` module exercises the public API
against the neutral record service over real Unix sockets. Three tests failed
with an unguarded implementation and passed after the lifecycle checks:

- Non-current inputs never call the builder or append command history. A current
  input without a value also fails with its distinct typed error.
- Deferring a future does not bypass a newly stale dependency. Invoking while
  stale does not queue the command until recovery. A valid deferred invocation
  builds from the latest current value rather than an earlier detached snapshot.
- A dependency that becomes stale during command construction stops dispatch.

A fourth test joins a real retained query with another reactive value. A simulated
authority outage desynchronizes the query while `identify` still works on the same
socket. The composed dependency blocks submission without changing history.
Restoring authority recovers that same input, and an explicit new invocation
accepts a command built from its rows. The test uses a permissive policy with an
injected read-authority outage; it does not prove production grant enforcement.

The full federation suite, expanded baseline, and strict federation/local Clippy
passed. All ten repetitions of the 21-test local suite passed. Formatting and diff
whitespace checks passed. No wire schema, reactive publication implementation,
application freshness flag, or lint exemption changed in this unit.

## Historical execution assignment component

The [design rationale](execution-assignment-design.md) compares application items
with framework control payloads. The implemented shape keeps assignment history
in `myko-federation` without changing the preexisting dirty control-chain code.
Graph coverage for control-chain sources and their tests was untracked at the
recorded generation, so this extension used current source reads.

`ExecutionAssignment` proposes a complete executor set for an exact scope and
service. The canonical payload binds operation and control realm. An empty set
records explicit removal. `ExecutionAssignmentsAtHead` replays only chosen
transitions through a caller-specified certified head. Unknown assignment
versions and malformed chosen payloads fail explicitly. Other control payload
families do not change the recorded assignments.

The eight tests in `tests/execution_assignment.rs` cover:

- Replacement, removal, record-order independence, duplicate evidence, and an
  encoded-history round trip without resurrecting old assignments.
- Isolation by scope, service, and anchored realm.
- Minority accept votes and an unchosen proposal providing no assignment.
- Mismatched realm or operation, forbidden controller rotation in an assignment,
  noncanonical bytes, duplicate wire executors, unknown fields, and unknown versions.
- Controller rotation under another payload domain preserving configuration.
- Reused operation identity being rejected by the existing control chain.
- Equivalent executor sets producing the same proposal bytes.

The tests use signed retained records, not a disk backend or live controllers.
They establish historical interpretation, not durable vote issuance, current
assignment-change permission, nested effective compute policy, readiness,
compatibility, or automatic route selection. No readiness marker, new permission
grant, application-owned endpoint list, or new persistence mechanism was added.
AB05, AB08, and AB09 remain open.

Verification for this component:

- All eight assignment tests passed.
- `cargo test -p myko-federation --target-dir target/agent -j 4` passed, including
  the existing control-chain, quorum, recording, and selected-history tests.
- Strict Clippy passed for the federation library and the new test target after
  converting test assertions into fallible comparisons. No lint exemptions were added.

### Startup test observation race

The expanded baseline exposed an intermittent timeout in
`reactive_handlers_open_before_local_server_and_share_one_socket`. Diagnostics
showed that all three handles were already current on one socket, but the test
was still waiting on the query. Repeated full local-suite runs reproduced it.

The test consumed a revision notification and then checked a separate
materialized state cell. Myko emits those revision events inside a Hyphae batch.
Hyphae 3.1.1 defers cell settlement, including source cells, and shares its batch
queue across threads. The test could consume the current revision, read the old
connecting cell value, and wait for another revision that never arrives.

A temporary deterministic batch experiment reproduced the different revisions.
Changing the snapshot accessor to read the source revision did not fix it, since
source writes are deferred too. That accessor change and its incorrect
immediate-settlement test were removed. Production reactive behavior is unchanged.

The startup test now observes collection state-cell publications directly and
uses each report publication's own state. It retains the same three-second
deadline, data assertions, and one-socket assertions. Timeout diagnostics identify
the waiting handler and report all three lifecycle states. This corrects the
test's observation race; it is not evidence of a daemon reconnect fix.

The repeated suite also exposed an unchanged-snapshot assumption in
`local_handler_connector_follows_retained_query`: its first received update could
still contain the initial rows. The test now skips only unchanged current
publications and requires the committed new rows within the original two-second
deadline. Unexpected rows or non-current output still fail. The startup test
passed 100 isolated repetitions after its observation fix.

After both test corrections, all 60 repetitions of the complete 13-test local
suite passed. The expanded baseline passed eight assignment tests, six handler
tests, all 13 local tests, and the native retained-history test. Strict Clippy
passed for federation and local libraries and tests. Formatting, shell syntax,
and diff whitespace checks passed. These results do not close the mesh-failover
milestone or certify the unrelated preexisting dirty authority changes.
