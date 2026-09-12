# Failover startup history boundary

## Phase

- [x] Ground the existing assignment quorum, history evidence, and command append.
- [x] Compare distinct boundary protocols and cross-review the candidates.
- [x] Reproduce the missing application-history relation with native coordination.
- [x] Agree that command results expose replication in their lifecycle and helpers
  wait for selected milestones.
- [x] Expose both `quorum_replicated` and `fully_replicated` as explicitly selected
  wait milestones in the agreed contract.
- [ ] Define replication-completion evidence and when history becomes publishable.
- [ ] Implement the accepted-history protocol and per-scope serving checks.
- [ ] Prove stable-handle recovery through a replacement without intermediate Current.

## Problem

A fresh assignment observation does not establish application-history readiness.
`ExecutionAssignmentObservation` binds operation, realm, and control predecessor.
Its controllers do not confirm an application-history boundary. A replacement's
closed local manifest cannot supply the missing fact.

The approved startup rule remains unchanged: a fresh authenticated scope-history
boundary needs the scope's coordinator majority before replacement `Current`.
The unresolved question is which durable write history that majority must cover.

## Caller usage

The existing public handle remains the owner of recovery:

```rust,ignore
let view = client.follow_view_reactive(&request)?;
// The same owner retains stale output during loss and receives replacement state.
```

Applications must not collect votes, select replacement endpoints, compare local
cursors, or maintain readiness flags. Internal dependencies use the same Myko
lifecycle. Commands based on desynchronized input still fail before acceptance.

## Executable counterexample

Run `bash scripts/verify-startup-history-boundary.sh`.

The test uses the existing three-controller Iroh fixture and three Redb journals.
It installs a neutral `BoundaryService` on A and B and assigns both executors to
the exact scope derived from `BoundaryRecordId("work")`. An explicit scope grant
authorizes A to run the registered `SetBoundaryRecord` command. The command emits
an item mutation and returns the typed record through `CommittedLocally`.

C has permission to read and subscribe to this scope, but not to copy its history.
C opens the retained `BoundaryRecords` query over native transport to A and sees
the committed record as `Current`. The query reads the exact scope across origins,
so changing the serving endpoint does not change which origin the request selects.

A's transport then shuts down. B and C make a fresh assignment observation while
both lack the application command. Their selected manifests are equally empty
and causally closed. C then opens the same typed query against B. B publishes an
empty value as `Current`, although A previously published the committed record.
Reopening A's journal recovers the exact committed command and result. No data is
deleted and the test does not manually import application history.

This fixture intentionally refreshes control history, not application history.
It proves that the existing observation and local-manifest APIs are insufficient
evidence for the proposed startup gate, and that a typed handler currently serves
the incomplete state. The test deliberately passes when it reproduces this gap.
It opens separate subscriptions to A and B; it does not implement automatic
takeover, validate a serving gate, or complete the stable-handle milestone.

## Synthesis decision

The parent read all three candidates in full. Scores are zero to five, in order:
accepted-tail safety, fresh scope evidence, framework boundaries, interface depth,
and meaningful runtime proof. These score designs, not implemented guarantees.

| Candidate | Parent scores | Result |
| --- | --- | --- |
| A, quorum-accepted frontier | 4, 3, 4, 3, 4 | Conditional crash-recovery direction |
| B, epoch-fenced transfer | 1, 2, 3, 3, 1 | Planned handoff only without a stronger write contract |
| C, quorum-durable boundary | 3, 2, 1, 1, 3 | Reject node-wide gate and invented client APIs |

The independent gpt-5.5 judge initially preferred B. The parent identified a hole:
a prior certified boundary cannot establish that A accepted no later private
write. The judge agreed on reassessment. That prior boundary is a predecessor
relation, not permission to resume `Current` after A disappears.

Use A's explicit accepted-history relation as the conditional crash-recovery
direction. Keep B's durable closing seal for planned drain while the prior
executor is available. Keep private evidence construction and per-scope checks,
but reject a reusable currentness token, a node-wide startup guard, and any
parallel application persistence system. No candidate's proposed API is installed.
The configured gpt-5.4 model was unavailable; the panel used sol, gpt-5.5, and luna.

## Proposed ownership and unresolved contract

The candidate protocol needs these responsibilities:

- Federation domain types bind the accepted history, scope, predecessor, and
  designated coordinator set without observer-local positions.
- Existing durable journals retain event bodies and evidence before acknowledgment.
  Coordinator membership does not implicitly make every controller a storage holder.
- Framework coordination confirms the boundary and obtains missing authorized
  history through existing transports. Projections remain application execution.
- Serving and command admission check per-scope evidence at publication and
  acceptance. The retained client owns reconnection, not these authority checks.

On 2026-09-12, the operator resolved the return-value question through an explicit
command lifecycle. Replication is part of that lifecycle, and helpers wait for
selected milestones. This replaces the earlier binary question about making all
multi-node calls wait or returning local completion as their only result.

The operator subsequently approved both `quorum_replicated` and `fully_replicated`.
Callers explicitly select the milestone they need; neither is an implicit default.
Selecting `replicated` means the durable-copy write quorum, not all assigned copies.
That write quorum is separate from the coordinator majority approving failover.
Coordinator votes alone do not prove that the required write history is durable.
This settles the exposed milestone distinction, not the exact durable holder set,
write-quorum rule, or projection publication policy.
Existing locally committed history must not silently be relabeled quorum-durable.
Handling that history still needs an explicit recovery rule.

The current `CommandState` already includes `CommittedLocally`, `Replicating`,
`ReplicationDelayed`, and `Replicated`. `CommandSnapshot::typed_completion` returns
an available typed result without waiting for replication. Both
`CommandWatchingClient::exec_typed_command` and `await_typed_command` use that
completion path. These types are existing implementation evidence, not proof
that the required lifecycle helpers or failover-safe replication protocol exist.

Until the history contract is settled, installing a gate that merely blocks forever
would not complete failover. Installing a gate based on two matching incomplete
copies would incorrectly weaken the requirement. This pass keeps the executable
counterexample and does not change production command or serving behavior.

## Verification

The initial submitted-command case passed in run `38187`. The earlier raw-command case
with both executors assigned and a locally committed result passed in run `55354`:
one test in 8.30 seconds. Strict Clippy for the `execution_coordination` target
passed in run `32825`. Targeted formatting, shell syntax, and diff checks passed.
The full native coordination target passed all ten tests in run `84702`, in
12.37 seconds. That fixture and its neighboring coordination tests passed
without a production behavior change.

The typed extension first failed in run `79515` because its request selected all
origins while the handler's default selected the serving node. The query now
explicitly declares all origins for the exact scope. No authorization check was
bypassed. The diagnostic passed in run `16453`, in 8.32 seconds, including the
source query, replacement query, and source journal reopen.

Strict Clippy initially rejected the test's length in run `76381`. Extracting its
scope-grant fixture kept the scenario within the limit. Final strict Clippy passed
in run `16675`; all ten coordination tests passed in run `19266`, in 12.39 seconds.
Targeted rustfmt, shell syntax, and diff checks passed. The workspace has no Flux
format task, so formatting used rustfmt directly on the edited test module.

The test is a diagnostic boundary proof, not a failing-before/passing-after fix.
No new runtime gate, wire type, or write-acknowledgment policy was implemented.
