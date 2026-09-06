# Scope continuity implementation

## Governing contract

The August first-principles foundation at `2ff388f4` governs this work. Immutable
accepted history is authoritative. Materialized state and snapshots are derived.
A scope can survive replacement of every founding node while acknowledged durable
custody of all accepted history remains continuous. One durable copy supports
orderly handoff; surviving an unexpected loss requires another acknowledged copy.

Physical resource placement does not establish permanent ownership of replicated
application state. Myko owns history, authority, custody, readiness, routing, and
client recovery. Forrest owns agent and external-effect semantics.

## Work sequence

- [x] Read the pstack principles and repository instructions.
- [x] Ground history, authority, materialization, native routing, and client recovery.
- [x] Compare independent architecture candidates and record a cross-model verdict.
- [ ] Resolve the selected candidate's safety issues without changing August's local-first command contract.
- [ ] Build the continuity and fault acceptance harness against the current baseline.
- [ ] Implement scope identity, multi-origin history coverage, reconciliation, and command recovery.
- [ ] Implement durable custody agreements, acknowledgments, readiness, and safe handoff.
- [ ] Implement membership and permission changes with stale-node fencing.
- [ ] Route commands and stable subscriptions through eligible scope replicas.
- [ ] Migrate Forrest off permanent owner-node routing for replicated agent state.
- [ ] Verify the complete fault matrix, native clients, and both workspace gates.
- [ ] Commit verified units, push both repositories, and sync the Mac without manually launching forrestd.

## Completion evidence

Each row remains open until the named behavior passes against real persistence and
the relevant transport. Unit-only or mock-only evidence cannot close an end-to-end row.

| ID | Required behavior | Authoritative verification | Status |
|---|---|---|---|
| C01 | Scope identity survives founding-node replacement | A creates; B catches up and acknowledges; A stops; C joins; B stops; reads and writes continue on C with the same scope | Open |
| C02 | Accepted history is complete and reconstructible | Reopen persisted stores and rebuild from verified snapshot plus history; compare all accepted changes and results | Open |
| C03 | Multi-origin history converges independent of delivery order | Concurrent permitted writes, reordered and duplicated transfer, disconnected writers rejoin; equal materializations and retained competing history | Open |
| C04 | Commitment and replication are distinct | Local commitment persists despite replication delay; stronger durability waits for authenticated persisted coverage | Open |
| C05 | Custody acknowledgments cannot invent coverage | Reject gaps, wrong scope, wrong origin, stale generation, unauthorized signer, and unpersisted acknowledgments | Open |
| C06 | Safe handoff cannot discard the last accepted history | Interrupt each join and leave transition with writes in flight; restarting participants recover the same obligations | Open |
| C07 | Abrupt loss respects configured durability | Lose one of two acknowledged copies; surviving scope operates and restores redundancy; loss beyond policy is explicit | Open |
| C08 | Exclusive invariants remain safe during partitions | Concurrent exclusive command submissions cannot both commit; convergent commands follow declared offline policy | Open |
| C09 | Revoked or stale membership cannot regain serving authority | Revoked node returns with old history and credentials; reads, writes, serving, and delegation obey current authority | Open |
| C10 | Command identity and results survive rerouting | Retry the same command through another replica before and after disconnect; recover recorded result without repeating committed work | Open |
| C11 | Live client handles survive replica replacement | Hold query, report, view, item, and command watches while serving nodes disappear; expose liveness and resynchronize coherent values | Open |
| C12 | Following, replication, custody, and grants remain independent | Subscription does not establish custody; pairing does not grant application access; scope selection does not leak adjacent scopes | Open |
| C13 | Control history survives churn | Membership, grant, custody, and policy decisions recover with their scopes without a permanent founding authority endpoint | Open |
| C14 | External effects are not replayed by history recovery | Forrest model and tool invocation recovery retains results or reports uncertain effects; no duplicate filesystem or provider effect | Open |
| C15 | Resource locality remains enforced | Remote root and provider account execution stays on eligible authorized hosts through application scope rerouting | Open |
| C16 | Retention preserves promised recovery | Operational pruning never removes the only history required by the custody policy; archive recovery remains verifiable | Open |
| C17 | Scope readiness is honest and scoped | One scope can serve while another catches up; stale snapshots and disconnected last values never advertise current readiness | Open |
| C18 | All consumers and validation gates agree | Myko flux checks and targeted fault tests; generated bindings as needed; Forrest fmt, workspace tests, strict Clippy; Mac build and sync | Open |

## Coordination and verification

Use the existing `/home/trevor/Code/myko-7` and `/home/trevor/Code/forrest`
checkouts. Do not create worktrees. Assign workers disjoint files and review shared
integration edits centrally. The user controls the production daemon.

The initial scope spans the federation domain, Redb journal, authority service,
native node and Iroh transport, retained core sessions, and Forrest clients and
execution. Exact changed files follow source tracing, not a preset line budget.

The architecture comparison must reject permanent-primary substitutes, snapshots
that replace accepted history, unauthenticated custody receipts, transport-local
command deduplication presented as mesh-wide idempotency, and tests that silently
keep an original node alive. A design must explain partition behavior before code.

The decision trail is `scope-continuity-decisions.tsv` in this directory.

## Current checkpoint

Native nodes can install certified authority on their existing Iroh transport
and command dispatcher. Configuration keeps the original controller electorate
separate from current authenticated routes. The node owns and stops its
authority worker. The native assembly test certifies a typed command, shuts
down both nodes, and verifies its exact result after reopening the store.

The broader run exposed a trusted-bootstrap race. An ordinary dispatcher could
claim a framework command between trusted admission and execution, then reject
it under the authority policy that bootstrap was initializing. The trusted path
now holds the existing reentrant dispatch lock across both operations. The
focused regression failed before that fix and passed afterward. This is
process-local dispatch ordering, not distributed fencing.

Forrest's production policy is still raw authority. Node-effect transitions and
provider resource reconciliation now await the existing typed command client.
Stopping the node-effect waiter preserves its pending command. Other synchronous
command callers still need migration. Mesh-host mailbox cleanup now also awaits
retained typed completion. Its focused test withholds dispatch, observes the
pending command, and verifies the exact completion result after dispatch resumes.
Remote root and provider tests require the completed dispatch mailbox to empty.
These tests do not install certified authority.
Root-task workers also await retained claims and outcomes, remain stoppable while
commands wait, and join filesystem operations already started. On restart they
resume an existing retained completion before reporting an interrupted task;
competing retained outcomes are reported without modifying history. Myko owns
typed command resumption and shares its completion logic with submit-and-watch.
The root tests use real temporary-file writes. A Redb test now closes and
reopens the store with a saved completion still pending, resumes that exact
outcome without a competing command, then reopens the completed store and checks
the exact retained result, history, and unchanged sentinel file. This proves the
single-node reopen path, not crash-at-every-instruction behavior, certified
authority, or founding-node replacement.
Final local evidence is in `/tmp/forrest-root-final-workspace.log` with 167
passing tests, `/tmp/forrest-root-final-clippy.log`, and
`/tmp/forrest-root-final-fmt-check.log`. Myko's core suite passed 245 tests and
federation library suite passed 147 tests in
`/tmp/myko-typed-resumption-core-suite.log` and
`/tmp/myko-typed-resumption-federation.log`; strict lint and formatting passed.
The added Redb test passed with all 168 local workspace tests, strict lint, and
formatting in `/tmp/forrest-root-disk-workspace.log`,
`/tmp/forrest-root-disk-clippy.log`, and `/tmp/forrest-root-disk-fmt-check.log`.
The previous production source passed all 167 tests, strict lint, and formatting
on the Mac in `/tmp/forrest-root-mac-validation.log`. The expanded five root tests,
including Redb reopen, and workspace strict lint and formatting also passed on
the Mac in `/tmp/forrest-root-disk-mac-validation.log`.
Local subscription behavior under certified
policy still needs verification before deciding whether those callers must change.
No production cutover or C01-C18 completion is claimed.

Network resource monitoring now awaits retained typed reconciliation commands.
Stopping the monitor preserves a pending update, which can finish afterward;
unchanged interface snapshots submit no commands. The unused synchronous
reconciliation wrapper is removed. Initial reconciliation still gates node open,
so authority installation must preserve startup progress independently of it.
The four focused tests and the no-default-features check passed. All 172 Forrest
workspace tests, strict workspace Clippy, and formatting passed in
`/tmp/forrest-network-workspace.log`, `/tmp/forrest-network-strict.log`, and
`/tmp/forrest-network-fmt-check.log`. These are raw-authority tests, not evidence
of certified production startup or scope churn.
Both repositories were pushed and synced to the Mac at Forrest `5edd01d` and
Myko `70489faf`. Its full 172-test workspace run, strict Clippy, and formatting
also passed in `/tmp/forrest-network-mac-validation.log`; no daemon or watcher
was manually controlled. A subsequent regression reproduced model execution
bypassing retained task-claim dispatch in `/tmp/forrest-agent-retained-claim-red.log`.
That initial failure belongs to the task-claim migration described below, not
the network-monitor checkpoint.

The task-claim regression now passes. AgentPool awaits the retained ClaimAgentTask
result before constructing and running a harness. The daemon's existing native
worker thread can stop that wait without deleting the admitted claim. The
shutdown test verifies the same pending command and zero model calls; the pool
test dispatches the saved claim and resumes execution. All 174 local workspace
tests, strict Clippy, and formatting passed in
`/tmp/forrest-agent-claim-workspace.log`,
`/tmp/forrest-agent-claim-final-strict.log`, and
`/tmp/forrest-agent-claim-fmt-check.log`.
Model/tool execution remains synchronous after claims finish, and the public
pool methods document their blocking-thread requirement. Completion, failure,
and tool lifecycle commands still need migration. This is not certified-policy
or persisted scope-recovery evidence.
The claim change is pushed and synced at Forrest `c9ecc9c` and Myko `18975f14`.
The expanded retained-completion verifier passed 32 test executions in
`/tmp/forrest-agent-claim-verifier.log`. All 174 Mac workspace tests, strict
Clippy, and formatting passed in `/tmp/forrest-agent-claim-mac-validation.log`.

The node-effect caller migration passed the full Forrest workspace tests,
formatting, and strict Clippy. Its focused verifier checks pending acceptance,
cancellation without changing the retained command, exact completion results,
remote root execution, and remote provider account use. Evidence is in
`/tmp/forrest-node-effects-workspace-tests-final.log`,
`/tmp/forrest-node-effects-workspace-strict-final.log`, and
`/tmp/forrest-node-effects-verifier-retry.log`.
Earlier link failures coincided with a full filesystem. Cargo removed generated
artifacts from the agent build directories before the successful retry.
No accepted history was removed. Earlier Mac SSH attempts timed out. The latest
sync succeeded at Myko `2ea81db4` and Forrest `4201531`, preserving the watcher
lockfile through autostash and leaving both remote worktrees clean. Mac
validation of that production source subsequently passed as recorded above.

Verification passed for the federation suite, the authority and native-node
suites, and strict lint for federation, core, authority, and native node.
Evidence is in `/tmp/myko-trusted-bootstrap-federation-green.log`,
`/tmp/myko-native-authority-assembly-gate-fixed.log`, and
`/tmp/myko-native-authority-assembly-strict-fixed.log`.
Formatting and Forrest's locked workspace all-target check also passed.
That checkpoint's Mac inspection timed out; the later successful sync is recorded above.

### Previous local-publication checkpoint

The prepared-authority worker now certifies locally accepted authority before
releasing prepared effects. It subscribes before its initial scan, wakes for
local AuthorityService commits in its realm, and retries failed publication
without requiring another command. Foreign raw authority is not selected
automatically. Publication failure postpones effect release.

Native tests verify bootstrap after a peer becomes ready, later idle revocation,
and publication recovery after a controller endpoint returns. The native node
lifecycle test now uses its existing application host and lets the worker certify
bootstrap before releasing the saved effect. Scoped history authorization and
the history startup barrier remain intact. History transports must become ready
independently of the worker, so resource initialization cannot wait for publication.
Forrest still needs production controller configuration and worker installation.

The final authority run passed 25 library, 52 coordinator, and 8 history tests
in `/tmp/myko-authority-publication-gate-final.log`. Strict lint then found two
test-only issues. Those corrections passed strict lint and the two publication
tests in `/tmp/myko-authority-publication-strict-clean.log` and
`/tmp/myko-authority-publication-final-tests.log`. Formatting and Forrest's locked
all-target check passed in `/tmp/myko-authority-publication-fmt-check.log` and
`/tmp/forrest-authority-publication-check-final.log`.

Earlier failed runs exposed a blocking test observer and lifecycle fixture
assumptions; their evidence remains in the decision trail. No full workspace,
production cutover, or C01-C18 completion is claimed. The Mac remains behind
with a modified Forrest lockfile; the latest follow-up inspection timed out.

### Previous controller-startup checkpoint

Controller prepare, propose, and accept requests now reach the explicitly
installed authority endpoint before application startup completes. The endpoint
still authenticates each caller and checks its controller binding. All other
session requests retain the application startup barrier. Iroh delegates that
ordering to the shared session instead of blocking before decoding the request.

The native Redb and Iroh regression failed on a controller-vote timeout before
the change, then passed. It checks both forged presentation and wrong proposer
rejection without history changes, and ordinary session blocking until readiness.
The coordinator suite passed 50 tests and the session suite passed 18. Strict
authority and Iroh lint passed, as did Forrest's locked all-target check.
The strengthened final regression also passed. Logs are
`/tmp/myko-startup-barrier-coordinator.log`,
`/tmp/myko-startup-barrier-session.log`,
`/tmp/myko-startup-barrier-strict.log`,
`/tmp/myko-startup-barrier-final-test.log`, and
`/tmp/forrest-startup-barrier-check.log`.

Scoped evidence refresh still waits for application readiness. Production
assembly must resolve that ordering along with controller configuration and
administration publication. This change does not complete startup certification
or production integration. All C01-C18 remain open. The latest Mac inspection
timed out without changing the remote checkout.

### Previous local-authority checkpoint

`certify_local_authority` certifies this node's accepted AuthorityService records
that are not yet selected. It recovers earlier accepted control values and then
recomputes the remaining records, including when a previous attempt selected only
a prefix. Completed retries append no additional selection. Foreign raw records
are excluded even when they have been retained in the same realm.

Native Redb/Iroh tests cover bootstrap, revocation, quorum outage, and reopen.
Additional tests cover an accepted prefix and retained foreign grants. The three
focused tests passed. The broader authority gate passed all 82 tests, strict
Clippy, and formatting. Forrest's locked all-target check also passed. Evidence
is in `/tmp/myko-local-startup-gate.log`,
`/tmp/myko-local-startup-strict-final.log`,
`/tmp/myko-local-startup-fmt-check.log`, and
`/tmp/forrest-local-startup-check.log`.
The returned head is historical evidence, not live permission or a guarantee
that foreign nodes have no newer records. Selected in-flight lifecycle records
remain lifecycle history, not completed grants. Production startup still needs
controller configuration and explicit bindings. All C01-C18 remain open.

### Previous caller-evidence checkpoint

Controller evidence refresh now has an explicit binding for each authenticated
proposer. Both authority-history and command-evidence refresh use that binding
after checking the transport principal, direct presentation, and ballot identity.
Unknown and duplicate bindings are rejected. An unbound caller can use only
locally retained evidence; it cannot fall back to another caller's endpoint.

A native test with three Redb stores and Iroh endpoints verifies that controller C
fetches A's signed history for A and B's signed history for B. A forged B request
over A's connection cannot fetch history or append a vote. All 79 selected
authority tests passed. Strict lint then caught a non-Send test temporary spanning
an await. Limiting that temporary to its synchronous setup block fixed the lint;
the two focused tests, strict lint, and formatting then passed. Evidence is in
`/tmp/myko-caller-evidence-gate.log`,
`/tmp/myko-caller-evidence-tests-final.log`,
`/tmp/myko-caller-evidence-strict-final-2.log`, and
`/tmp/myko-caller-evidence-fmt-check.log`. Forrest's locked all-target check passed
in `/tmp/forrest-caller-evidence-check.log`.

Production controller configuration must supply the required bindings. No
production cutover or C01-C18 closure is claimed by this change.

### Previous native-lifecycle checkpoint

`PreparedAuthorityRuntime::start` starts only certified effect recovery. The
native node retains ownership of its existing command dispatcher. The combined
`install` method remains available for an application that needs both tasks.

The native lifecycle test uses a Redb-backed `myko-node` and authenticated Iroh
controller endpoints. It verifies release of an exact prepared item effect, then
shuts down the authority worker. A later command still reaches preparation through
native dispatch, but its effect policy reports unavailable. The first parallel
run caught a setup race while controller binding temporarily replaced the policy.
The test now completes controller setup before submitting any application command.
The repeated authority gate passed all 77 tests, strict Clippy, and formatting.
Evidence is in `/tmp/myko-native-authority-lifecycle-gate-2.log`,
`/tmp/myko-native-authority-lifecycle-strict-final.log`, and
`/tmp/myko-native-authority-lifecycle-fmt-check.log`. Forrest's locked all-target
check passed in `/tmp/forrest-native-authority-lifecycle-check.log`.

This test does not install a production controller configuration. It establishes
the worker-ownership path needed for that installation. All C01-C18 rows remain
open, including native incoming-client and node-churn proof.

### Previous retained-selection checkpoint

The coordinator now certifies existing authority records through
`certify_selection`. Construct an `AuthoritySelection` with a stable operation
identity and retained events, then submit it to the coordinator. Exact retries
recover the original historical head, including after later choices and Redb
reopen. Reusing the operation with different records fails. Certification does
not rewrite accepted events or grant live serving permission.

Native controller tests cover quorum outage, recovery, immutable record checks,
missing bodies, causal gaps, wrong realms, and recovery of an earlier accepted
value before choosing the requested selection. The authority gate passed 75
tests, strict Clippy, and formatting. Forrest's locked workspace all-target check
also passed. Evidence is in `/tmp/myko-certified-selection-gate.log`,
`/tmp/myko-certified-selection-strict-final.log`,
`/tmp/myko-certified-selection-fmt-check.log`, and
`/tmp/forrest-certified-selection-check.log`.

This supplies the missing record-selection API for startup. Production still
uses the node-local authority policy. Controller configuration, startup worker
ownership, selection of subsequent administration records, and policy installation
remain unfinished. No C01-C18 requirement is closed by this checkpoint. The Mac
is reachable again with clean checkouts; checkpoint sync is pending.

### Previous scoped-stream checkpoint

This checkpoint extends certified access to scoped item and handler streams.
The server gives each open a new admission identity. Continuations recover that
certified admission and freshly revalidate it without spending another grant use.
Changed requests, wrong leases, revocation, and unavailable controllers do not
release application frames. Topology binding includes the requested scopes and
their ancestors, not unrelated scopes that happen to become known later.

The coordinator serializes its own proposal operations and refreshes the retained
head after waiting. This avoids competing ballots from one configured proposer;
it does not replace controller quorum checks. Item streams drain irrelevant
control records before selecting another authorization timer, avoiding feedback
that previously prevented real updates from progressing.

Handler frames pass a continuation check before forwarding. Native frame sinks
are asynchronous, and dropping a subscription aborts its producer even with a
full queue. Scalar report snapshots coalesce to the latest value under pressure;
map deltas do not use that coalescing path. The item stream drain loop yields
cooperatively so irrelevant records cannot monopolize its executor.

The expanded verification script passed 380 test executions, including all 39
coordinator tests, and the five-crate strict check. Its final node check caught
an enum-size lint caused by the larger session. Boxing the concrete command
clients and updating their two explicit trait calls fixed it. Redb/node strict,
all 15 durable-node tests, and formatting then passed. The final core server
rerun passed all 84 tests after the cooperative-yield adjustment. Evidence is
in `/tmp/myko-certified-streams-gate.log`,
`/tmp/myko-certified-streams-core-final.log`,
`/tmp/myko-certified-streams-node-strict-2.log`, and
`/tmp/myko-certified-streams-node-final.log`. These are combined results; the
original script ended at the corrected enum-size lint, not exit zero.

Forrest passed 160 locked workspace test executions, strict Clippy, and formatting.
Its final locked all-target check also passed after the client boxing change.
The Mac SSH preflight still timed out. The stream test uses native Iroh controller
coordination with an in-process incoming session, not a native incoming client.
Handler forwarding is covered by core session tests. Production Forrest still
does not install this certified policy. Other access operations, custody, the
earlier parallel core graph-window failure, and all C01-C18 rows remain open.

The following paragraphs describe the preceding committed checkpoint.

`AccessPolicy::decide` now returns one `PolicyDecision`: either an immediate result
or lazy asynchronous coordination. Synchronous local command APIs reject pending
coordination without polling it. Network access awaits the result without holding
the policy lock and rejects permission from a policy replaced during the wait.
The existing policy implementations and callers use this contract directly.

`CertifiedRuntimePolicy` now certifies initial scoped item reads, consumes the
grant through the control log, and freshly revalidates before returning permission
to the session. Unsupported item-read forms never use the fallback policy. Effects
still wake the saved-effect worker. Other operations retain their supplied policy.
The session regression fails before this change and passes afterward. Its native
controller version also verifies denial after one use and typed unavailability
without a page when one controller endpoint is removed.

Run `bash scripts/verify_policy_decisions.sh` to verify the changed policy contract,
session behavior, synchronous command boundaries, native transports, and certified
runtime. This is not a production Forrest policy replacement. Other access
operations, custody, abrupt node loss, and C01-C18 remain unfinished.

The verification script passed all 296 test executions and its five-crate strict
check. Its final Redb strict check caught an assertion-style lint in the new test.
After replacing those assertions with descriptive errors, all three Redb authority
tests and strict all-target checks for Redb and the native node passed. Formatting
also passed. Evidence is in `/tmp/myko-policy-decisions-gate-cleanup.log`,
`/tmp/myko-policy-decision-redb-final.log`, and
`/tmp/myko-policy-decision-fmt-final.log`. This combines the script run with the
corrected final check, not a claim that the entire script exited successfully.

Forrest's locked workspace tests, all-target check, and strict checks passed in
`/tmp/forrest-policy-decision-tests.log`, `/tmp/forrest-policy-decision-check.log`,
and `/tmp/forrest-policy-decision-strict.log`. The Mac SSH preflight still times
out. The earlier parallel core graph-window failure remains unresolved and is
outside this targeted gate. Coordinator string errors still map broadly to
`CoordinationUnavailable`; this checkpoint does not improve their diagnostics.

The following paragraphs describe preceding checkpoints and their limits.

Controller endpoints now certify initial scoped `ReadItems` requests without
manufacturing a prepared application command. Each voter refreshes scoped evidence
and supplies its own topology. Unsupported operations and unscoped targets remain
denied. Missing scoped evidence reports unavailable. This is not yet a live read
policy installed on application nodes.

Retrying a completed request from a later certified head now recovers the original
verified proposal and votes. It does not propose the terminal root again or consume
another grant use. The control chain owns this historical proof, including its
original electorate. Recovered decisions are not live permission. Prepared effect
release still requires fresh coordinated revalidation.

The expanded gate passes 34 selected tests, including recovery across disjoint
controller electorates, and five-crate strict checks. Formatting and Forrest's
locked workspace build check pass. The Mac SSH preflight still times out.
Production access-policy integration, custody, abrupt loss, and C01-C18 remain open.

The following paragraphs describe preceding checkpoints and their limits.

Native multi-approval coverage now drives both choices through an authenticated
Iroh client and both controller endpoints through Iroh RPCs. The second controller
starts without the prepared command and fetches its evidence over scoped native
replication. The test verifies the authenticated approver, unchanged effect binding,
immutable approval retry, advancement to the second challenge, and exact commit
once. No production policy was changed for this checkpoint.
The expanded gate passes 29 selected tests, five-crate strict checks, and formatting.
This does not yet prove abrupt node loss or certified authorization for production
reads and administration.

Saved commands now advance through multiple certified approval challenges. Recovery
walks same-command continuation records and uses the existing guarded pending-state
transition for each successor. It retains the saved batch and result, records the
approvals that justified advancement, and still freshly revalidates before commit.
Three regressions cover normal two-approval execution, reopening after the second
challenge was chosen, and catching up through two chosen rounds of a three-approval
command. The last case also checks that an unrelated challenge cannot replace the
current challenge and that retry does not append duplicate advancement.
The expanded gate passes 28 selected tests and five-crate strict checks. Formatting
and Forrest's locked build check pass. These multi-approval tests use durable local
controllers, not a native multi-node transport or abrupt process-loss test.

`PreparedAuthorityRuntime` now retries retained work five seconds after each pass
while commands remain prepared or await approval. The journal remains the work
source. An integration regression restores one controller's evidence availability
after a quorum failure and observes the exact saved effect commit once, without
another wakeup or worker restart. It failed before this change and now passes.
Waiting without a fresh approval returns the unchanged pending command, emits no
repeat report, and appends no authority decision. Public continuation and replay
validation still reject missing approval evidence. The fixed retry interval is not
a backoff policy, and this test does not prove abrupt-process-loss recovery.
The retry checkpoint passes the 25 selected tests and five-crate strict checks in
`scripts/verify_prepared_authority_runtime.sh`, workspace formatting, and Forrest's
locked workspace build check. The Mac remains unreachable over SSH.

`AccessPolicy::approve` now returns an awaited approval future. Session handlers,
the raw authority policy, the certified policy, and transport tests use that one
contract. The certified policy records the immutable approval through the
coordinator, then wakes saved-effect recovery. Quorum outages remain typed
unavailability rather than policy denials; a stopped worker also reports unavailable.

A real authenticated local-socket test approves a parked command, observes its
exact saved batch and result commit once, retries the approval without changing
it, and verifies unavailability after worker shutdown. This test exposed and fixed
a queue mismatch: handler dispatch excludes parked approvals, so authority recovery
now has its own retained prepared/pending command selection. Parked commands still
cannot re-enter the handler queue. Six focused approval tests and the local/Iroh
approval transport tests pass. The expanded rerunnable gate is
`scripts/verify_prepared_authority_runtime.sh`.

The broad parallel run exposed two test failures. A local restart test assumed its
first queued report update was the disconnect frame. It now waits for that frame
within a deadline while preserving reconnect and final-value checks; all 12 local
tests pass afterward. A core graph-window test sampled the old page after a window
change. Its isolated rerun and all 449 same-feature serial unit tests pass, but the
parallel failure remains unresolved. No graph implementation was changed.
Authority/federation/Iroh suites, the focused gate, strict Clippy, formatting, and
Forrest's locked workspace check pass. The Mac SSH preflight still times out.

Certified non-effect policy, production Forrest
installation, custody, and abrupt node-loss proof remain unfinished. Pending
evidence lookup scans retained history; no performance claim is made. All C01-C18
acceptance rows remain open.

The following paragraphs describe preceding checkpoints and their limits.

`PreparedAuthorityRuntime::install` now installs the effect policy and starts
application command dispatch and certified recovery together. Its guard owns both
workers. Explicit shutdown stops dispatch, cancels in-flight coordination, and
joins the worker without rolling back persisted votes or prepared effects.
The installer checks node identity and executor availability before changing policy.

A registered item-writing handler test blocks authority evidence refresh, joins
shutdown, reopens both Redb stores, and recovers through native Iroh. The exact
nonempty saved batch and result commit once, and the handler execution count stays
at one. A separate missing-executor test preserves the prior policy. All 66 authority
tests pass, as do both core command-recovery tests, the focused runtime and
control-chain gate, strict Clippy, and formatting.
Forrest's matching lockfile update is `abb31eb`; its locked workspace check passes.
The latest Mac SSH preflight still times out without changing remote state.

Applications can now own this effect runtime through one installation and shutdown
guard. Forrest production installation still awaits certified non-effect policy
and approval recovery. Controlled cancellation is not abrupt process-kill proof.
Custody and all C01-C18 acceptance rows remain open; Mac validation remains unfinished.

The following paragraphs describe preceding checkpoints and their limits.

`PreparedAuthorityRuntime` now connects certified coordination to saved command
effects. Its synchronous effect policy queues only wakeups and reports typed
unavailability. The async worker finds pending commands in the journal, derives
its starting head and ballot from retained evidence, recovers consumption, and
freshly revalidates before immediately committing the exact saved batch and result.
No permit returns to the command driver's retry loop. Malformed or conflicting
chosen history prevents frontier recovery instead of selecting an older valid head.

Four runtime tests cover the real command commit boundary, coalesced wakeups,
worker cancellation, consumed-effect recovery after both stores reopen, revocation,
quorum-outage ballot recovery after reopen, foreign-command rejection, and native
Iroh evidence transfer and release. Nine control-chain tests, strict Clippy, and
formatting pass. The broader authority and federation suite passes 261 tests;
the final strengthened restart assertions also pass in the focused gate.
Run `bash scripts/verify_prepared_authority_runtime.sh` to repeat that gate.

This is an effect-only adapter with an explicit separate policy for admission,
reads, replication, and administration. It is not installed in Forrest production.
Certified non-effect authorization, approval resumption, automatic runtime lifecycle
integration, custody, and all C01-C18 acceptance rows remain open. The runtime tests
do not prove graceful production drain or abrupt process-kill recovery. Mac SSH
still times out, so remote validation remains unfinished.

The following paragraphs describe preceding checkpoints and their limits.

The coordinator now certifies a fresh revalidation for each call. It follows
intervening accepted checks and revocations, rejects changed effect evidence,
and requires a controller majority. Native Iroh coverage exercises the same
path. Consuming the result rechecks time-dependent validity against its certified
snapshot only as a veto. The result cannot be cached as later permission and
does not observe revocations chosen after the call. Runtime integration must
consume it at the release boundary. Runtime policy, head and ballot ownership,
approval rounds, and custody remain unfinished.

Certified revalidation now rechecks an original permitted effect against a later
certified predecessor without creating another grant use or lease. It excludes
only the exact original use records from evaluation, retains the original
contributors, and preserves the original lease deadline. Replay validates the
new control value. Controller endpoints require fresh time and matching prepared
evidence for a new proposal, while recovering required accepted values unchanged.
Persistence tests cover restart, consumed grants, revocation, forged permits,
and lease expiry. Live coordinator release and runtime policy integration remain
unfinished; this historical protocol does not itself release a command.

The authority coordinator now tolerates unavailable minority peers during
evidence refresh. Two healthy controllers in a three-controller electorate can
certify a decision and recover the same decision after both stores reopen.
Insufficient quorum still fails, and invalid evidence stops coordination before
voting. A native Iroh test also verifies that a third controller answers before
shutdown, remains configured while the other two certify, and stays offline
through their store reopen and exact-decision recovery. Native control requests
and per-scope evidence refreshes now have configurable ten-second deadlines.
The test uses one-second failed-peer deadlines and thirty-second decision bounds.
This does not prove abrupt process loss, custody, or live effect-release permission.

Prepared commands now recover automatically through the application command
driver after durable reopen. Temporary authority unavailability defers the
command without stopping independent work. Real-Redb tests verify that recovery
appends the exact saved batch and result once without rerunning the handler.
Commands awaiting approval remain parked. The affected core and federation gate
passes 471 tests, formatting, and strict Clippy. Forrest's workspace tests,
formatting, and strict Clippy also pass against this change. A list-watch test
now waits for readiness and query resends before asserting response epochs.
This is recovery scheduling, not live certified-policy integration or custody.

The preceding native integration checkpoint at `a776937a` passed all 708 tests in
`scripts/verify_certified_consumption.sh`, formatting, and strict Clippy. This
includes the native-FFI feature set, server consumers, and the native continuity
regression. The full Myko Flux check also passes, with 14 Svelte warnings.
Forrest's frozen-source workspace tests, formatting, and strict Clippy pass.

Swift revision tests cover cancellation and reentrant publication ordering.
Three core tests now wait for their expected publications while retaining their
readiness, deletion, and persistence-order assertions. All 244 core tests pass
in the integrated feature set.

Native coordinator tests prove authenticated scoped evidence transfer and recovery
of the same chosen decision after ordinary shutdown and durable reopen. They do
not prove live effect-release permission, custody, or automatic replica replacement.
All C01-C18 rows remain Open. Forrest's matching retry fix is `ae2eaa5`.
Wire version 11 requires coordinated peer rebuilds. The latest read-only Mac
SSH preflight timed out without changing remote state, so current-wave Mac
validation remains unfinished.

The following paragraphs describe earlier checkpoints and their limits.

The prepared-effect checkpoint passes 275 affected Myko tests, the native
founder-replacement regression, formatting, and strict Clippy. Forrest passes its
full workspace tests, formatting, and strict Clippy. See
`certified-consumption-synthesis.md` for the completed local preparation and
deterministic evaluator units, and the still-open certified consumption work.
That document also records the intermittent reactive assertion and the blocked
read-only audit of interim prepared digests. No C01-C18 row is closed by this
checkpoint. The following paragraphs retain earlier checkpoints and their limits.

Fixed-cut retained manifests and versioned signed statements are implemented.
Framework control records now use the existing journal without entering command
or item projections. Focused tests cover persistence, live notification after
append, append failure, retry identity, scope filtering, and restart. At the
previous framework-control checkpoint, checks passed for the affected Myko
crates, the native continuity test, the 15-test durable-node file, and Forrest's
full test workspace and strict Clippy. That core run passed 231 library tests
and four publication experiments. This
does not claim complete Myko workspace, wasm, or Mac validation.

Local recording now checks a persisted store incarnation. Federation tests and
the 15 Redb library tests plus four control-history tests pass. The metadata tests
cover reopen, legacy upgrade, malformed identities, and orphaned durable state.
The retention verifier also rejects conflicting duplicate origins in replay.
This identity does not detect a copied or restored database, and upgrading metadata
does not validate old assertions. The native continuity test, affected-crate strict
Clippy, and scoped formatting checks pass for this increment. Forrest's full gates
above refer to the previous framework-control checkpoint, not a fresh rerun.

This is not custody issuance. Obligation authorization, eligible membership,
incarnation retirement and freshness, safe departure, and the complete fault matrix
remain open. All C01-C18 rows remain Open. The decision trail records exact test
commands and results, including failures and superseding runs.

The subsequent control-quorum checkpoint verifies signed majority evidence and
requires chosen verification to use the verified prepare result. Its 12 focused
tests and full 170-test federation suite pass, with strict Clippy and formatting.
This is not persistent voting or certified authority. The durable controller,
restart recovery, epoch activation, and portable authority projection are next.
See `custody-issuance-synthesis.md` for the boundary and remaining gates.

The durable-voter checkpoint now records authenticated controller votes through
the existing journal. Real Redb tests prove restart and legal ballot recovery.
An ambiguous append regression exposed a stale-cache vote, which now fails closed
until reopen. Federation, Redb, and wire pass 210 tests with strict Clippy and
formatting, and the native founder-replacement test passes. This does not implement
a persistent proposer, certified epoch activation, or portable authority facts.
Wire version 8 requires coordinated peer rebuilds; commit, push, Mac sync, and the
remaining continuity gates are still outstanding.

The latest proposer checkpoint persists a signed full proposal before constructing
an accept request. Real Redb tests reject same-ballot value reuse after restart
and conflicting retained proposals hidden behind a matching record. The affected
federation, Redb, and wire suite passes 218 tests, strict Clippy, and scoped
formatting. Automatic ballot allocation, a networked coordinator, and certified
authority activation remain unimplemented. Wire version 9 supersedes version 8;
the Mac has not been synced. All C01-C18 rows remain Open.

The certified-authority checkpoint now reconstructs historical authority through
an independently anchored static controller epoch. Eight integration tests cover
real Redb reopen, grant and revocation heads, selected history integrity, and proof
poisoning. The affected authority, federation, Redb, and wire suite passes 244
tests with strict Clippy and formatting. This reader does not grant live authority.
Current-head readiness, epoch rotation, coordinated consumption, and custody remain
open, as do all C01-C18 rows. See `certified-authority-synthesis.md` for the boundary.

The controller-rotation checkpoint adds a generic certified chain and replaces
authority's static quorum collector. A chosen rotation establishes a content-derived
successor epoch. Four real Redb authority tests prove disjoint controller handoff,
restart, successor-certified revocation, stale-epoch rejection, and refusal to
issue evidence over missing or malformed authority records. Nine generic tests
cover chain integrity and reverse delivery across 66 transitions; a separate Redb
test covers exact-snapshot fencing at the atomic signing boundary. The affected
four-crate suite passes 258 tests, strict Clippy and formatting. See
`control-rotation-synthesis.md` and `control-rotation-grounding.md`.

These contexts remain historical. The issuer checks its own retained history,
not network-wide currentness or remote request authorization. Controller identity
is key-only, without principal/node/store enrollment proofs. Current-head readiness,
certified consumption, custody obligations and native integration remain open.
All C01-C18 remain Open. No commit, push, Mac sync or production daemon launch has
occurred for this checkpoint; Forrest's gates above are still historical.

## Implementation history

These checkpoints describe earlier states in order. Later results supersede
earlier failures or implementation claims.

The native three-node test retains the founding history on C after A and B stop,
but fails when C serves the scope under its own node identity. This is a baseline
for replacing source-bound APIs, not evidence of custody or successful failover.

The first integrity fix rejects changed immutable content under an existing
`EventId`. Identical content relayed with a different local replay position remains
a duplicate. The federation suite passes 74 tests and strict Clippy.

The architecture comparison favors causal history with exact per-origin coverage.
Neither candidate is approved for implementation as written. The remaining design
requirements are:

- Keep normal state-only commands local and convergent, as August requires.
  Replaying accepted changes must not require deterministic command execution.
- Separate accepted event identity and provenance from command retry identity.
  Conflicting accepted commands cannot reconcile by arrival order.
- Preserve the original command identity during retry. Unknown prior execution
  must not authorize another external effect.
- Specify overlapping exclusive operations and membership changes with an actual
  coordination protocol before claiming their safety.
- Never treat expired write permission as proof that earlier accepted history
  reached another custodian.

The first causal replay unit now orders existing immutable events by their declared
parents, with stable origin identity breaking concurrent ties. All-source snapshots
and live projections use that order. Missing-parent batches remain in history but
stay out of these item projections until their parents arrive. Tests cover six
three-origin permutations, duplicates, sparse positions, transitive missing parents,
cycles, live updates, and scope-local ordering metadata.

The journal backend now maintains one incremental causal index. An append stages
metadata before persistence and installs it only after the journal accepts the event.
Each released event records the local cut at which its parent closure became
available. Reopening Redb rebuilds the same index and preserves historical cuts.
Ready events order by causal height, then immutable origin identity. All-source
watches read that shared index at their consumed cursor instead of retaining a
private history copy. They still rebuild typed item state from the ordered events.

Ordinary command queries now read the logical scope union under an explicit
unrestricted-source claim. They capture required same-service, same-scope event
dependencies from the same history snapshot used for the query. Events arriving
after the read are not claimed as observed. Source-pinned claims fail rather than
silently granting a union read. Foreign-service reads do not become blocking replay
parents, so an output-only replica need not fetch another service's private history.
This is not a complete foreign-read provenance format.

Committed lifecycle events now depend on their referenced commit in the causal
index. Tests hold every committed-status variant until both the batch and its
ancestors arrive, including reads at an earlier local cut. Public command reads,
single-command watches, and source-bound catalog pages now use causally ready
history. Watches recompute at the consumed cursor, so an unrelated late ancestor
can release a waiting command without exposing events beyond that cursor.

Catalog watches deliver every changed entry at one cursor in one atomic batch.
Clients validate the complete batch before changing state or advancing the cursor.
Empty batches, duplicate IDs, invalid entries, changed requests, and lifecycle
regressions are rejected. The session authorizes entries before emitting a batch;
the local reconnect cursor advances once per frame. This changes the wire schema
and increments its version to 6, so communicating binaries must be rebuilt together.

The retained command control table still prevents duplicate execution while history
is incomplete. Duplicate submit, admit, claim, cancel, and commit-resume paths return
`CommandHistoryIncomplete` instead of leaking a pending result. Raw control state
and public views share the terminal-state reducer. Committed results survive later
cancel/reject events; legitimate later committed lifecycle states can advance.
Command identity reconciliation across origins remains unresolved.

This does not close C03. Source-filtered and selected reads still use the old
item projection path. Topology still applies during ingestion before causal
closure. `CommandContext::query_selected` and handler reads made directly through
`Node` still need observation capture; this is separate from command lifecycle
snapshot and watch gating. In addition,
conflicting command identities still need deterministic reconciliation. Custody,
scope authority transitions, and routing remain unimplemented.

The Forrest workspace test caught a real consumer declaration mismatch:
`AppendInvocationOutput` reads its referenced invocation from logical history but
declared a source-pinned read. That claim now permits the logical read while
retaining its exact scope, invocation ID, capability, and executor checks. Both
invocation authority tests pass with `mesh-node`; the manifest now requires that
feature so an isolated invocation test cannot silently run zero tests. The
Forrest workspace rerun and strict workspace Clippy pass for this consumer fix.

The later Myko core rerun failed two reactive timing assertions despite the
earlier six-crate passing run. The query subscription test now waits for its
observable outgoing frame. Registry liveness now waits for observed transitions;
Hyphae's global scheduler explicitly permits cross-thread deferred settlement.
The production registry is unchanged. The mixed
SET/DEL fixture's explicit seed barrier passes its focused test and core Clippy.
The full core rerun after both observable barriers passes all 211 tests. Earlier
failures remain in the trail; one passing rerun is not proof of scheduler-wide
test isolation.

The next topology unit separates conflict validation over retained events from
authority derived from dependency-complete history. The unused relationship
installer has been removed because it accepted relationships without event
evidence. Selected export now freezes ready history and stops its raw cursor
before the first unresolved event, so later dependency arrival can release that
event without losing it. Tests prove that pending scope IDs remain available to
recovery inventory, do not establish parentage for authority, still participate
in reparenting rejection, and become exportable from the saved cursor when ready.
All 105 federation tests, strict federation Clippy, and 15 native durable-node
tests pass. The conservative export stop can delay unrelated scopes behind an
unresolved event. Selected-query projections, incoming topology proofs, and
scope-isolated recovery still need work. This is not full topology closure.

Selected query authorization, values, and readiness now share one explicit local
history cut. Watches use their consumed cursor rather than latest history.
Coverage receipts carry a local recording cut, so an older snapshot cannot use
a newer receipt. Relevant unresolved history reports `HistoryIncomplete`, never
authoritative absence. Missing ancestor events require conservative subtree
readiness; independent exact scopes can still be complete. Selected command
reads now use logical source-union history and capture their observations.

The live selected-history session now reuses `export_selected` from its last
safe cursor instead of maintaining a separate raw-event topology filter. Its
focused test is being verified. Incoming serialized topology proofs are still
unresolved; authenticated policy narrowing must not become reusable topology or
custody evidence.

The broader Forrest run exposed a real causal-order defect during UpdateAgent.
ProvisionAgent creates the Agent in a Catalog-scoped batch. UpdateAgent reads
that item, but the original observation filter excluded the creator because
its outer command scope was Catalog. Both ordinary and selected reads now
capture same-service batches that actually affect their output scope. A focused
regression covers that observed-read path. The separate blind-update regression
still fails: a later same-origin root update without an explicit creation parent
can sort before creation. Scoped author-order preservation and existing accepted
logs need a further design/implementation unit. Root validation remains intact;
no event logs have been reset. Current full-workspace gates are not green.

The focused mobile lifecycle rerun after the observation fix gets past root
reconstruction but fails because a deleted Agent remains in the roster. This is
further evidence that commands without read dependencies still need scoped
author ordering; it is not a passing Forrest verification. The selected-follow
and cross-scope observation regressions pass, as does strict core/federation
Clippy. The full gates remain open.

For that next unit, do not add parents inside backend commit after effect
authorization: `commit_bytes` binds the full batch, including its parents, into
the effect digest, and approval resumption retains that batch. Prepare canonical
authorship dependencies before authorization and validate them at persistence.
Existing accepted history needs an explicit replay rule rather than mutation or
discard. Any inferred author ordering must preserve sparse origin sequences and
scope independence instead of serializing every scope on the source node.

The incoming-proof investigation distinguishes response authorization from durable
topology. The current serialized `SelectedReplicationBatch.topology` has no
event provenance or signature. An authenticated source may attest its narrowing
of one request, but that assertion must not establish reusable ancestry or
custody. Carrying full establishment events can prove ancestry only when their
entire atomic batches and required history are authorized. No solution may fetch
foreign private history merely to make a selected view ready. These alternatives
still require a protocol decision and implementation.

New command contexts now prepare same-author write predecessors before effect
authorization. The scope set includes the command scope and actual mutation
placements, not read-only claims or foreign-service writes. Blind update and
delete regressions pass. A challenge/resume regression verifies that the effect
digest includes those parents and that an intervening write does not mutate the
parked batch. Raw commits and existing accepted logs require the separate inferred
ordering rule.

The first inferred scoped-author replay implementation passes all 121 federation
tests, including the previously failing raw root update. It computes author edges
from history present at the requested local cut, so a late earlier event cannot
change a previous snapshot. Index simplification, durable reopen verification,
and broader consumer validation are still underway.

The mobile lifecycle test now distinguishes reconstructed state from its retained
roster. Reconstructed history correctly omits the deleted agent, but the existing
immediate roster assertion still fails. This supersedes the earlier conclusion
that this particular remaining failure proves a write-order defect. The sourced
reactive view and initial subscription freshness need their own fix; the test has
not been weakened to accept stale data.

The scoped-order index now retains one metadata representation instead of keeping
an unused explicit-only readiness cache beside the effective graph. Cycle checks
use an iterative traversal. Reconstructing the graph at each cut preserves fixed
snapshots, but append validation and reads still scan retained history. Reopening
a long journal repeats that work and needs a measured incremental optimization.
This is a correctness checkpoint, not completion of the latency or readiness goals.

The combined library suite passes 212 core, 121 federation, and 9 Redb tests.
The Redb cases verify sparse author order across two reopens, unchanged earlier
cuts and immutable event bodies, and rejection of a combined explicit/inferred
cycle before any journal append. The sourced-map regression confirms eventual
local deletion without deleting a remote row with the same item ID. It does not
prove that a newly opened roster presents a current initial snapshot.

The next sourced-view unit should prepare the retained handler, record the exact
durable source handles it opened, await their publication cuts asynchronously,
and only then publish its initial client frame. Sourced maps need frontier and
liveness publication alongside their rows, including progress without a typed
diff. Their initial history, topology, and live subscription must share a frozen
causal snapshot. Do not wait on every globally cached source, rebuild a private
map per subscriber, or make clients sleep to hide a stale initial value. This
design still needs executable tests for a warm cache, multiple dependencies,
unrelated unavailable sources, and invalidation before the required cut.

After test-only lint cleanup, strict all-target checks pass for core, federation,
and Redb; Forrest workspace Clippy and formatting checks pass. The final
federation and Redb rerun passes 121 and 9 tests. The broader Forrest workspace
run stops at mobile with 8 passing and 2 failing tests: the stale deleted-agent
roster and a two-node Full permission profile that does not converge to the
expected value while reporting current liveness. These are unresolved consumer
failures, not evidence of a complete federation migration. Nothing has been
committed, pushed, or synced to the Mac in this checkpoint.

The sourced-map adapter now reconstructs both rows and topology from the same
dependency-complete history cut. Each consumed event rebuilds that projection,
so a newly arrived ancestor can release an older pending write or deletion.
The pending-deletion regression also proves that the earlier cut keeps its old
rows after the dependency arrives. This adds correctness coverage, not a claim
of incremental performance or complete replication coverage.

The source-only correction does not fix initial retained-handler freshness:
the focused mobile lifecycle rerun still reports a deleted agent in the roster.
The next design must retain dependency evidence with cached query, view, and
report computations. Recording only sources opened during the latest request
misses nested cache hits whose factories do not run again. Dynamic dependencies
and downstream scheduler settlement must also participate before a frame can
claim to be current. The earlier prepare/wait/publish proposal is incomplete
without this cache-owned dependency evidence.

### Retained-handler freshness work queue

- [x] Ground the cached query/view/report and scheduler publication paths.
- [x] Frame and compare two distinct cache-freshness designs using architect/arena.
- [x] Cross-judge the candidates and record the revised direction, grafts, and rejections.
- [ ] Prove the revised stamped source, transform, and subscriber contract before production migration.
- [ ] Implement the chosen shape, including weak-cache lifetime and dynamic dependencies.
- [ ] Verify warm-cache, unrelated-source, permission, and deleted-roster cases.
- [ ] Scrap and redesign if implementation requires repeated exceptions to the shape.

The cached-handler grounding is in `retained-freshness-grounding.md`. It confirms
that Myko weak caches retain final outputs while Hyphae owns the upstream graph;
there is no enumerable durable dependency closure on the cached output. Exact
registry Hyphae 3.1.1 source also exposes a separate snapshot/listener handoff
gap. A new core test synchronously deletes a row while the sink receives the
initial handler frame; the deletion is lost. The focused test fails for that
assertion in `/tmp/myko-handler-handoff-red.log`. No production handoff fix has
been made at this checkpoint, so the previous 214-test green result does not
describe the expanded test suite.

The parent and gpt-5.5 cross-judge reject both original retained-freshness
settlement mechanisms as unproven. `retained-freshness-synthesis.md` records the
comparison and the revised direction: one immutable publication must contain
both payload and the evidence from which it was computed. Separate readiness
cells or a snapshot/evidence join do not establish this. Prototype verification
and the production cache/handler migration remain open.

The executable publication comparison now passes static and same-tick switching
cases but fails the deliberately interleaved two-writer split-cell case. A paired
immutable-value variant passes the corresponding interleaving. This supports
payload/evidence atomicity as the next building block, not a claim that the
existing daemon source has multiple writers. Both the prototype comparison
target and the real initial-handler handoff test remain red.

The ordered-publication unit now reuses `LiveSubscriptionState` rather than
adding another payload/readiness type. Writers apply updates to the latest
accepted state before publishing, fixing two regressions where a batched
reconnect or cursor update restored the old payload. Unary mapping preserves
publication sequence and metadata without recomputing unchanged values.
Subscribers reject old or duplicate versions and retain one latest complete
snapshot, so slow consumers cannot grow that mailbox without bound. This is
not the policy for durable history or delta streams.

The native retained handler and cache migration remains next. Core consumer
reruns still fail the real initial-frame lost-deletion test. The latest parallel
run passes 212 tests and also fails window readiness and cascade diff-count checks.
An immediate liveness assertion now observes both actual report and view updates
before checking, matching the dependency's cross-thread scheduling contract.
The ordered writer does not supply scoped durable completeness, multi-input
compatibility, or dynamic dependency evidence on its own. All C01-C18 rows remain
open. Verification details and limitations are in `retained-freshness-synthesis.md`
and the append-only decision trail.

### Native query and view handoff checkpoint

The production initial-frame lost-deletion regression now passes. Native query
and view handlers use one shared output with listener-first snapshot observation.
Each session computes deltas between delivered snapshots. Weak-cache tests cover
concurrent reuse, derived-map release, and compute-gate cleanup after failed opens.
Registry-root maps remain owned by the registry after the output is dropped.

The current parallel library run passes 221 core and 136 federation tests.
Strict all-target Clippy passes for both crates. Logs are
`/tmp/myko-native-handoff-core-federation.log` and
`/tmp/myko-native-handoff-clippy.log`. Earlier intermittent report and query
readiness failures remain in the trail; this rerun does not establish durable
settlement. Reports, dependency evidence through cached transforms, scoped
readiness, and the remaining continuity requirements are still open.

The focused Forrest lifecycle rerun still fails after this handoff repair:
direct reconstructed history excludes the deleted agent, but a fresh roster
subscription includes it. `/tmp/forrest-native-handoff-lifecycle.log` records
the unchanged failure. Native snapshot delivery does not establish that the
retained computation has consumed the required history cut.

### Fixed-cut source foundation

Sourced projections now publish immutable rows with their consumed local cut
and liveness. Pending selected history is explicitly non-current. Raw maps are
derived from these publications for callers that have not migrated. The public
fixed-cut snapshot rejects future cuts and keeps the events and pending-history
assessment read-only. Tests cover source cancellation, metadata-only advances,
pending deletions, foreign origins, and stable historical cuts.

Explicit local-plan and retained-publication view-output conversions are in
place, but the existing view factory and native cache do not use them yet.
The next unit is that migration, including per-open target-cut enforcement and
the unchanged deleted-roster acceptance test. The current library gate passes
226 core and 139 federation tests in
`/tmp/myko-snapshot-foundation-core-federation.log`. No continuity row closes
from these source and conversion tests.

### Registered retained roster

View registration and the native cache now preserve explicit retained outputs.
Raw-map callers must handle an error rather than silently discard publication
evidence. Each fresh native open captures a target local history position and
withholds an older retained publication. A metadata-only advance can satisfy the
target while remaining `Resynchronizing`.

The native target-cut regression passes. Forrest's unchanged lifecycle test now
passes its immediate post-deletion roster assertion. Logs are
`/tmp/myko-retained-cut-test2.log` and
`/tmp/forrest-retained-roster-lifecycle3.log`. The broader tests and review are
still pending at this checkpoint. Myko tests resolve Hyphae 3.1.1 and Forrest
tests resolve 3.1.2. No continuity row closes from this bounded freshness fix.

The broad library rerun passes 228 core and 139 federation tests after a test-only
bounded observation change for a deferred query bucket update. Earlier failure
and rerun logs remain in the decision trail. The Forrest workspace run is still
pending, and the rejected split-evidence prototype still fails as an integration
test. No full-workspace or completed-continuity claim follows from this checkpoint.

The workspace run caught the roster tool still requesting a raw retained-view
map. Its synchronous read now uses the shared Myko fixed-cut evaluator, checks
liveness, and shares the roster-entry conversion with the live view. The focused
tool tests pass, including immediate create/delete. The new Myko API test passes
pending-history and no-live-cache checks. The broader rerun remains pending.

### Origin and serving endpoint correction

Reinspection found an error in the original continuity baseline. Its last
`watch_items_reactive_in(C_id, ...)` call filters C-origin events; it does not
select C as the serving endpoint. Expecting A's event from that filter was not
evidence that C could not serve replicated scope history. Independent review
confirmed this distinction. The earlier failing result remains in the trail.

The typed handler query now accepts an optional origin filter independently of
its connector's serving endpoint. Existing source-pinned callers pass `Some`;
`None` requests the logical scope. The corrected harness reads through C over
Iroh after A and B stop, commits a causally dependent C write, reopens C's store,
and checks both the logical result and separate A/C provenance filters. Its
request client never pulls history and must not contain either command.
The corrected test passes with a registered command submitted to C over Iroh.
That command reads A's previous value through the normal command query path,
emits C's replacement, and returns a typed result. Reopening C preserves that
result and the logical value. Origin-filtered reads preserve A's old value and
C's replacement separately. The request client's journal contains neither
command. Evidence: `/tmp/myko-scope-command-continuity-verified.log`.

The optional-origin caller migration passes all 11 local transport tests in
`/tmp/myko-optional-origin-local-tests.log`. The strict all-target Clippy command
for `myko`, `myko-federation`, `myko-local`, and `myko-node` completed with exit 0
in `/tmp/myko-scope-origin-clippy-final.log`. Independent gpt-5.5 review accepted
the bounded change and confirmed the typed request/authority gate remains intact.
This is an intentional Rust API change: callers choose `Some(origin)` or `None`.

The test still uses explicit pulls and `AllowAllAccessPolicy`. It does not
provide custody receipts, safe departure, membership, or authorization proof.
C01 and C02 remain open, as do the other completion rows.

The earlier roster verification also completed. Forrest's workspace tests and
strict all-target Clippy pass in `/tmp/forrest-retained-cutover-workspace2.log`
and `/tmp/forrest-retained-cutover-clippy2.log`. The final messaging rerun passes
all three tests in `/tmp/forrest-roster-final-tests.log`. Myko's core and
federation library rerun passes 229 and 139 tests respectively in
`/tmp/myko-roster-dispatch-lib-tests.log`. This does not establish a passing
Myko workspace: the rejected split-evidence prototype remains a failing
integration test, and the wider continuity requirements remain unverified.
