# Assigned handler failover

## Phase

- [x] Ground retained reconnect, assignment checks, and scope history evidence.
- [x] Compare whole-path designs before choosing one.
- [x] Agree on framework ownership and the required failover startup history boundary.
- [ ] Implement and verify the chosen path.
- [x] Implement and verify immutable requests through initial connection retries.
- [ ] Check implementation against the design and record remaining gaps.

### Startup-boundary implementation pass

- [x] Ground the existing coordinator evidence, selected history, and serving checks.
- [x] Compare boundary-protocol sketches and identify their missing write-durability contract.
- [x] Add a native persistent counterexample for assignment freshness without application history.
- [ ] Settle the write-durability contract and implement the selected runtime path.
- [ ] Verify negative serving cases and stable-handle recovery.
- [ ] Check the implementation against the sketch, redesign if needed, and record evidence.

See [startup history boundary](failover-startup-design.md) for the counterexample,
candidate comparison, and the unresolved distinction between local commitment
and history that can safely survive serving-node failure.

## Target

One public reactive handle must survive an executing node's loss, retain its
coherent value while desynchronized, reject dependent new commands, and recover
through another assigned, compatible, ready executor. Storage holders do not
become typed executors or routing gateways. Applications do not implement retry,
placement, or freshness bookkeeping.

The last unit verified explicit retained query/view lifecycle propagation and
fixed a cached-origin frontier stall. Its 486 tests do not prove this target.

## Grounding

Graph project `myko-7-current` has generation `2026-09-05T01:04:50Z`.
The relevant files have changed or are untracked by that generation. Current
source fallback supplies the findings below, not stale call-graph edges.

### Retained connections

`core/src/client/durable_handler.rs` owns `HandlerConnector`,
`NodeHandlerSubscription`, and the stable reactive owners. Each owner keeps one
live writer and background task. Reconnection reuses the connector and fully
formed `HandlerRequest`; it does not rebuild the query or replace the public
handle. Only transport and authority-unavailable errors currently retry.

`iroh/src/client.rs` implements a fixed-peer connector. Its outer request
destination is separate from the handler's event-origin selector. Reports and
views initially derive selectors from `target_node`; established reconnects
retain those selectors. Switching executors must not silently change the data
being queried. `connect_described` binds an open to the activated descriptor
observed on that candidate, but does not itself prove compatibility or readiness.

### Assignment control

`core/src/server/execution_coordinator.rs` provides `observe`. It obtains a fresh
chosen observation through existing persisted control votes and authenticated
scope-history refresh. `ExecutionAssignmentsAtHead::exact` in federation maps an
exact scope/service pair to its configured executors. No record and an explicitly
empty set are distinct. Neither infers assignment from replication or discovery.

An observation establishes a point during its call. It is not a lease or a
reusable execution permit. Another assignment can change after it returns.
`FederatedSession` currently has access-policy admission and continuation gates,
but no assignment gate. Controller authentication and client data authorization
are separate responsibilities. Exact assignment lookup does not define nested
compute inheritance or unscoped/global-handler placement.

### Data and schema evidence

`SelectedHistorySnapshot` freezes local ready and pending history at one local
cut. `SelectedHistoryManifest` can prove a closed selected set, and its
`RetainedHistoryCommitment` excludes observer-local positions. Neither proves
that no additional remote events exist or that a replica is currently complete.
A commitment alone also does not show that a newer event set includes an older
one. `LogPosition` values from different nodes must never be compared directly.

Generated argument/result schemas have separate serialization and deserialization
contracts. `HandlerContract` transports those activated schemas and binds the
serving identity. Existing validation checks response identity and row/scalar
shape, not directional schema compatibility. Exact descriptor equality on open
guards application replacement; it is not rolling-update compatibility.

`command_scope_readiness.rs` already exercises refusal of new commands when
declared scope history is locally incomplete, including an admission race.
It does not prove command dependency validity after a serving-node switch.

## Design constraints to resolve

- The connector must preserve the original data-origin/scope selectors while
  changing the executing peer.
- Catch-up evidence must survive node changes without comparing local cursors
  or treating an empty local projection as proof of complete remote history.
- Client checks cannot replace serving-node assignment and authority checks.
- A fresh assignment observation writes control history. Retained projections
  now publish progress for every consumed local cut. Revalidating every emitted
  frame by writing another observation could therefore feed its own output back
  into the revalidation loop. The design must address this before integration.
- Freshness and publication atomicity across control and application history
  must be stated precisely. No historical snapshot may be renamed a live permit.
- Missing schemas, assignments, or readiness evidence fail explicitly. The first
  exact-scope case must not silently define inheritance or global-handler policy.

## Usage and first implementation unit

The public calls remain unchanged:

```rust,ignore
let report = client.follow_report_reactive(&summary)?;
let view = client.follow_view_reactive(&tasks)?;
```

Each handle owns request preparation and retries. After target resolution
succeeds, Myko fixes the handler kind, service, ID, data origin, scope, and
serialized parameters before opening a stream. An initial connection failure
must not cause the next attempt to evaluate the selectors against a different
executor. Established reconnects reuse that same prepared request.

Preparation itself can retry while the initial target is unavailable. Protocol
and decoding failures remain terminal. Nonreactive `follow_report` and
`follow_view` still return connection errors instead of owning a retry loop.

The implementation keeps preparation private to the retained client. It does
not add a public request wrapper, a second cache, or application-owned retry
state. This follows boundary discipline and keeps transport choice separate from
the domain selection. Request identity is implementable without deciding which
executor may claim ready history.

## Synthesis decision

The base is candidate A's immutable request and client-owned selection, with
candidate B's initial-selector fix as the first code unit. Assignment,
compatibility, readiness, and client authorization remain distinct server-side
requirements. This chooses ownership, not a completed serving protocol.

The initial three candidates all chose client-owned selection. Candidate C was
then revised into a full application-capable ingress proxy alternative, so the
comparison includes two distinct routing shapes. Storage-only nodes execute no
typed handlers in either shape.

The root scored each criterion from zero to three. Criteria, in order, are
stable ownership and selectors, honest portable readiness, server enforcement,
feedback avoidance, executable milestone coverage, and interface depth.

| Candidate | Scores | Total |
| --- | --- | --- |
| A, client selector with explicit completeness gap | 3, 3, 2, 1, 1, 2 | 12 |
| B, client selector with prepared initial request | 3, 1, 2, 2, 2, 2 | 12 |
| C, application-capable ingress proxy | 3, 3, 2, 1, 1, 1 | 11 |

The independent gpt-5.5 judge also selected A's invariants with B's first unit.
Its scores were 16, 16, and 15. The root penalized the incomplete publication
protocol and catch-up claims more heavily. Neither review accepts a complete
ready-executor implementation. The configured gpt-5.4 runner was unavailable;
the comparison used gpt-5.6-sol, gpt-5.5, and gpt-5.6-luna.

Grafts and rejections:

- Keep B's selector preservation during initial retries, not only reconnect
  after a successful first open.
- Keep A's distinction between last-seen continuity and scope completeness.
  B's prior-event ticket alone cannot establish the latter.
- Reject C's ingress proxy. It hides backend choice but adds another trusted
  application hop, proxy authorization, and a second failover state machine.
  The client connector can hide selection behind the existing public API.
- Do not implement A's proposed control-cut suppression. Current retained opens
  depend on consumed-cut progress, including control records.
- An effective-assignment revision could distinguish assignment changes from
  no-op observations. It does not establish fresh authority during a partition
  or close the race between observation and publication. The full serving gate
  still needs a protocol with executable evidence.
- Do not add a readiness trait whose only implementation trusts a boolean,
  local cursor, empty projection, or commitment. Missing proof remains missing.

## Agreed startup boundary and remaining verification

On 2026-09-12, the operator approved requiring a fresh scope-history boundary on
startup after failover. That boundary must be authenticated and confirmed by a
strict majority of the scope's designated coordinators. The replacement must
retain and project the required history before publishing `Current`. An
unreachable majority keeps recovery desynchronized despite a readable local copy.
The [application-builder contract](application-builder-contract.md#health-readiness-and-reactive-handles)
records this requirement. It is agreed policy, not an implemented serving protocol.

The boundary must identify required history across nodes. A local cursor, closed
local event set, historical assignment observation, or the client's last-seen set
does not establish that boundary. The protocol must also close the gap between
checking readiness and publishing output. Startup confirmation does not decide
ongoing freshness policy or authorize a cached observation as a live permit.

A hot standby is an assigned, compatible application executor with history and
projections already prepared and following updates. It can avoid a cold catch-up.
The best case is a switch without visible interruption, but only if Myko proves
continuous readiness through the handoff, including the required boundary.
Otherwise the same handle reports desynchronization and dependent commands fail.
Storage-only custody does not supply a ready typed executor.

The next runtime proof must distinguish a readable replacement from a ready one:

- With A unavailable and B missing accepted history, B must not publish `Current`
  even when its local history is causally closed and its projection is readable.
- With the scope's coordinator majority unavailable, B must not bypass boundary
  confirmation or silently lower quorum, even if its copy appears caught up.
- Once B confirms the boundary and projects the required history, the original
  handle must recover through B without an intermediate `Current` value.
- A prepared backup must pass the same boundary and publication checks. A test
  that merely preloads its log cannot establish interruption-free takeover.

These are acceptance cases to implement. They are not passing test results.

## Request-identity implementation evidence

The request-identity unit is implemented in `PreparedHandlerOpen` and
`retry_initial`. Both scalar and keyed drivers retain a prepared request before
retrying connection establishment. They release the temporary prepared copy once
`NodeHandlerSubscription` owns the connected request. Query preparation remains
synchronous; report and view preparation can await initial target identification.

The public reactive API was already synchronous before this unit. No public
signature changed here, and nonreactive follows still make one connection attempt.
No assigned routing or ready-executor guarantee is implemented by this unit.
The full milestone remains open.

Verification:

- The report and view regressions in
  `core/src/client/durable_handler/tests/selectors.rs` fail when preparation and
  connection are repeated together. Both failures show a different data origin
  and target-derived scope on the second request. They pass with the prepared
  request, including a later connection attempt after established stream loss.
- The initial fixture did not compile. The worker started implementation before
  an executable behavioral RED run. The root subsequently restored the original
  prepare-on-every-attempt behavior and observed both intended failures in run
  `97562`, then restored the fix. Compilation failure is not the bug proof.
- The fixtures own the existing scheduler test permit until their last connector
  reference is released. Two earlier parallel core runs failed in WebSocket
  query tests. Guarding only the new test bodies was insufficient; the corrected
  fixture lifetime matches `MockTransport` and the documented test convention.
- The 128-subscription local test now waits for the second commit's same-node
  cursor. Earlier progress publications must still contain the first record.
  The final rows, `Current` liveness, and one-socket assertions all remain.
- Final component reruns passed 491 tests: federation 152, core 274, enabled core
  integration tests 22, local transport 31, native handler namespaces 8,
  persisted scope continuity 1, and inspected native handler opens 3. Runs
  `29262` and `50375` supply these results after the respective test corrections.
  Run `50375` also passed strict Clippy across federation, core, node, server,
  local, and Iroh with `myko-node/schema` and all targets.
- The final default-parallel replay of `scripts/verify-query-lifecycle.sh`, run
  `4554`, passed all 488 tests in one invocation. The three inspected native
  handler-open tests passed separately in run `50375`.
- Targeted formatting, shell syntax, and `git diff --check` passed. The stable
  formatter warns that the repository's nightly-only import options are ignored.
  Bench-gated cases, the full workspace, and Apple builds were not verified.
- A gpt-5.5 review of the bounded source changes and actual transcript found no
  remaining review flags. The reviewer did not rerun tests or audit the full
  workspace. This does not prove the agreed startup boundary or close the milestone.

Current native assignment coordination separately passed nine tests, and the
local command-readiness tests passed six. Neither proves the missing
assigned-executor recovery protocol. No Forrest changes, new worktrees,
production daemon runs, commits, or pushes were made for this unit.
