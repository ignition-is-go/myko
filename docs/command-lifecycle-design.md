# Command-result lifecycle design

## Phase

- [x] Ground command state, typed completion, command watches, and journal evidence.
- [x] Compare distinct lifecycle and replication-evidence sketches.
- [x] Select one shape and record the rejected alternatives.
- [x] Implement the first verified unit against the selected shape.
- [x] Review the first unit against the selected shape; no new policy inferred.
- [x] Record both replication milestones with explicit caller selection.
- [ ] Produce verified quorum/full replication transitions and expose wait helpers.

## Task for candidate sketches

The operator requires command results to expose an observable lifecycle that
includes replication, with helpers that wait for selected milestones. Design the
smallest coherent framework implementation that advances this requirement and
the subscription-failover milestone. This is not permission to equate local
completion with failover safety.

Write usage first, then Rust type and signature sketches, ownership, tradeoffs,
verification, and the next implementation unit. Keep each package under 160 lines.
Use the architect rationale template. Do not write production code. Candidate
files have disjoint ownership; nobody creates worktrees or edits Forrest.

The existing lifecycle is in `libs/myko/federation/src/command.rs`:

- `CommandSnapshot` keeps a request, state, optional encoded result, and event ID.
- `CommandState` already names local commitment, replication progress, replication
  delay, counted replication, reconciliation, rejection, and cancellation.
- `typed_completion` returns an available decoded result without waiting for
  replication. `Reconciled` describes visibility, not additional durability.

The existing watching client is in `libs/myko/federation/src/node.rs`:

- `CommandSubscription` exposes `current` and `recv`.
- `CommandWatchingClient` provides a gap-free watch at the admitted source.
- `exec_typed_command` and `await_typed_command` use `complete_typed_command` and
  therefore normally finish at local commitment.
- Retry helpers must not submit a second command or interpret waiter cancellation
  as authority to cancel the command.

Source grounding found normal and prepared commits produce `CommittedLocally`.
Replication states appear in manually constructed causal-projection tests; there
is no verified production acknowledgment path that gives their counts a durable
meaning. Do not treat `Replicated { acknowledged_replicas, required_replicas }`
as certified evidence or infer durability from `Reconciled`.

Reuse `SelectedHistoryManifest` and `EventJournal::verify_retained_history` rather
than introducing another persistence system. Manifest selection is a frozen local
cut. It does not prove missing remote history, currentness, custody, or authority.
Journal verification checks exact origins, timestamps, and bodies while ignoring
receiver-local recording positions. Redb append uses immediate durability before
returning. Storage incarnation identifies a store across reopen but does not
detect restoring an old copy.

A replication acknowledgment needs a named immutable target and authenticated
durable-holder evidence. A transfer cursor, memory cache, count, or successful
connection alone cannot supply that. Distinguish recorded milestones from current
availability, and plan for lost replies, duplicate acknowledgments, restart, and
already-reached milestones. Applications must not collect acknowledgments, inspect
transport envelopes, choose replacement sockets, or maintain lifecycle flags.

Do not silently decide the remaining policy questions: which placed holders are
required, the write-quorum rule, their relationship to scope coordinators, or
when projections may publish `Current`. An explicit requirement in a test is not
a new mesh-wide default. The approved startup boundary still requires fresh
majority confirmation of the required scope history.

Propose a bounded first implementation unit with a real executable proof. Explain
what it establishes and what remains unavailable. A helper that merely trusts the
old replica counts, or a fake gate that can never recover, is not acceptable.

## Grounding evidence

Tier 2, project `myko-7-current`, stale generation `2026-09-05T01:04:50Z`.
Coverage reports changed or untracked metadata for the relevant files. Parent
read the current command, node, selected-history, journal-trait, memory commit,
and Redb append sources directly. No graph absence is an exhaustive source claim.
The preceding typed startup diagnostic proves unsafe empty replacement `Current`,
not successful failover. See `failover-startup-design.md` for its exact limits.

## Selected direction

Use B's single observable result with separate execution outcome and replication
progress. Milestones are predicates, not an ordering: reconciliation cannot erase
or imply replication, and a delayed replica does not undo local execution.
Application-facing waits name a milestone; Myko resolves its obligation, frozen
history target, eligible holders, authenticated acknowledgments, and watch routing.
Do not expose the candidates' holder lists, signing keys, or manifest construction
as application chores.

The operator approved both `quorum_replicated` and `fully_replicated` on
2026-09-12. Callers choose explicitly which milestone to await. Quorum replication
means the command's write-quorum requirement is satisfied; full replication means
every durable holder required by that same obligation has acknowledged the exact
history. Neither is an implicit default or a claim of current holder availability.
This resolves the earlier all-copies-or-quorum choice by exposing both milestones.

The operator clarified that selecting `replicated` means `quorum_replicated`, a
write quorum of durable copies. `fully_replicated` remains available for waiting
for all required copies. The durable-copy write quorum is separate from the
coordinator majority used to approve failover; coordinator votes alone cannot
satisfy the replication milestone. This names the selected wait condition, not
an implicit wait for every command.

These are agreed lifecycle names, not installed Rust enum variants or working
wait helpers. Implementation still needs an authoritative holder set and quorum
rule, authenticated durable acknowledgments, replayable milestone tracking, and
integration with the existing gap-free command watch. A three-holder, two-ack test
fixture can exercise the distinction without establishing mesh-wide policy.

The panel used the available sol, 5.5, and luna models; configured 5.4 was unavailable.
The independent 5.5 judge selected B. Parent accepted that conceptual choice but
rejected B's draft evidence predicate: matching unverified statement fields cannot
establish durable replication. B also leaked holder/obligation policy to callers.
Graft A's immutable command/history target and restart tests, and C's unordered
milestones. Reject C's extra persisted local-commit achievement and new signature
format: the existing command event and retained-history statement already own them.

## First implementation unit

Bind a `CommandHistoryTarget` to a committed snapshot, its actual immutable
`CommandCommitted` event, a closed `SelectedHistoryManifest`, and an explicit
obligation event identity. Derive expected retained-history statements from this
target using the existing commitment format. The target is framework-level local
validation, not a wire certificate, holder policy, custody agreement, or new log.

The constructor must reject uncommitted snapshots, missing commit events, and
mismatched request, result, batch, or commit identity. Use origin event identities,
never compare recording positions across nodes. Keep the manifest frozen so later
commands or acknowledgment records cannot change what an earlier wait requested.
Test exact matching, malformed inputs, raw counted states without a commit,
different obligations, imports at different local positions, and Redb reopen.

This unit does not add a replication producer or milestone waiter. Those follow
once the framework has an authoritative retention obligation and holder rule.
Adding a waiter over unverified counts would misstate its guarantee. The two
explicit wait milestones are now agreed; holder membership, the write-quorum rule,
and projection-`Current` policy are not inferred from that choice.

## Verification of the first unit

`CommandHistoryTarget` is implemented in `federation/src/command_history.rs`.
The command result schema and existing completion helpers are unchanged. Five new
tests and seven adjacent tests pass:

```sh
cargo test -p myko-redb --test command_history_target --test control_history --target-dir target/agent -j 4
cargo test -p myko-iroh --test command_history_target --test retained_history_signature --target-dir target/agent -j 4
```

The Iroh test signs a target-derived statement with a real key, checks it against
an independently constructed descriptor and expected statement, and records it
through Redb. Wrong history and obligation are rejected; replay and duplicate
recording preserve the original assertion. It opens no socket and uses an explicit
test obligation, so it does not prove a production acknowledgment exchange,
authorization, custody, placement policy, or successful failover.

Strict Clippy passes for both target tests and the adjacent signature tests.
The initial Clippy failure required `let ... else`; it was corrected and rerun.
Targeted formatting and `git diff --check` pass. Flux has no formatting task;
`cargo flux run gen` from the federation crate fails because that directory has
no `flux.toml`. No generated bindings changed: this target has no serialization or
cross-language schema. Tests validate a new contract, not a failing-before fix to
the existing command helpers. The 5.5 reviewer found no first-unit correctness
blocker; its admission-event mismatch case was added before final validation.
