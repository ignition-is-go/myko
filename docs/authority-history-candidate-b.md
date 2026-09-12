# Candidate B: operation-scoped historical replay result

## Problem

Historical authority reads currently recompute the same predecessor chain several
times inside one operation. `context_at`, `decision_at`, `plan_decision_at`, and
`plan_revalidation_at` each call `selected_at`; `decision_at` then scans the
chain again, and planning projects facts into `EvaluationState` again. The outer
`AuthorityHistoryCache` already reuses an `Arc<AuthorityHistory>` only for an
identical event snapshot, which is the right boundary for new evidence at the
same head. This candidate does not add persistent memoization. It changes each
historical operation to compute one validated replay product and pass that
product through the rest of the operation.

## Usage

Public callers keep the same API:

```rust
let head = history.retained_head()?;
let original = history.decision_at(head, &root)?;
let planned = history.plan_revalidation_at(head, op, &root, Utc::now())?;
let context = history.context_at(head)?;
```

Inside `AuthorityHistory`, those methods become thin wrappers over one private
result:

```rust
pub fn plan_revalidation_at(...) -> Result<AuthorityDecisionRevalidation, String> {
    let replay = self.replay_at(head)?;
    let original = replay.decision(root)?
        .ok_or_else(|| "authority revalidation has no original decision".to_owned())?;
    let state = replay.project_state(original.topology.clone())?;
    AuthorityDecisionRevalidation::plan(operation, &original, evaluated_at, state)
}

pub fn decision_at(...) -> Result<Option<AuthorityDecisionTransition>, String> {
    self.replay_at(head)?.decision(root)
}

pub fn context_at(...) -> Result<CertifiedControlContext, String> {
    self.replay_at(head)?.context()
}
```

`release_prepared` and `revalidate` can later get a private fast path such as
`history.replay_at(head)?.context_and_counter(history.history())`, but that is
an internal refactor, not a caller contract.

## Shape

```rust
struct HistoricalReplay {
    head: ControlHead,
    context: CertifiedControlContext,
    facts: Vec<CertifiedAuthorityFact>,
    decisions: BTreeMap<DecisionRootKey, AuthorityDecisionTransition>,
}

impl AuthorityHistory {
    fn replay_at(&self, head: ControlHead) -> Result<HistoricalReplay, String>;
    fn replay_from_transitions<'a>(
        &self,
        head: ControlHead,
        transitions: impl IntoIterator<Item = &'a ControlTransition>,
    ) -> Result<HistoricalReplay, String>;
}

impl HistoricalReplay {
    fn context(&self) -> Result<CertifiedControlContext, String>;
    fn facts(&self) -> &[CertifiedAuthorityFact];
    fn decision(
        &self,
        root: &AuthorityDecisionRoot,
    ) -> Result<Option<AuthorityDecisionTransition>, String>;
    fn project_state(&self, topology: ScopeTopology) -> Result<EvaluationState, String>;
}
```

`AuthorityFactReplay` already owns the correct transient state while applying
transitions: selected events, operation ids, command bindings, current decisions,
decision record identities, and selected output. Instead of returning only
`output`, it should return `HistoricalReplay { head, context, facts: output,
decisions }`. `replay_at` obtains `transitions_to(head)` once, applies them once,
calls `chain.context_at(head)` once, and validates fact support/realms through
`project_state(ScopeTopology::default())` before returning. The useful replay
result then answers the operation's local questions.

The invariant boundary stays unchanged: every fresh `AuthorityHistory` object
is still tied to an exact event snapshot. New evidence at the same retained head
builds a new history object, so no previous `HistoricalReplay` is reachable.
Historical replay remains tolerant for unrelated malformed evidence and strict
for the exact requested head. A valid predecessor below an invalid successor is
still readable because `replay_at(predecessor)` asks `CertifiedControlChain` only
for transitions to that predecessor and never consults later replay output.

## Synthesis decision

Candidate B deliberately chooses operation-scoped reuse over snapshot
memoization. It hides replay, transition scanning, decision extraction, and
fact validation behind one private object while leaving public APIs unchanged.
That gives most of the obvious savings in the hot methods without introducing
shared mutable state, eviction, lock ordering, or stale-head invalidation rules.

## Tradeoffs accepted

- We accept recomputing the same head across separate public calls in exchange
  for no persistent cache protocol and a small correctness surface.
- We accept cloning the returned `AuthorityDecisionTransition` in exchange for
  keeping current ownership and public return types unchanged.
- We accept projecting `EvaluationState` per requested topology in exchange for
  preserving topology as a caller-supplied planning input rather than baking it
  into replay.

## Alternatives considered

Snapshot memoization would store `HistoricalReplay` by `ControlHead` inside
`AuthorityHistory`. It hides more repeated calls, but it exposes cache
correctness inside an immutable-looking object and duplicates the existing
exact-snapshot cache boundary. It also makes tests prove eviction and
same-head-new-evidence invalidation instead of just proving pure replay results.

Returning only `Vec<CertifiedAuthorityFact>` plus helper scans is too shallow.
It keeps decision lookup and repeated projection in callers, so the public-ish
internal interface still leaks the replay structure.

## Negative tests

- Add evidence that refreshes `AuthorityHistory` at the same retained head;
  assert the new history recomputes and old replay results are unreachable.
- Build a chain where a successor has malformed retained authority payload but
  the predecessor is valid; `context_at(predecessor)` and `decision_at(predecessor)`
  still succeed while the successor fails.
- Call `plan_revalidation_at` for a root whose original decision topology differs
  from the current request topology; it must use the retained original topology.
- Certify an unknown fact type or wrong-realm payload; every wrapper using
  `replay_at` returns the same error as today.

## Next implementation step

Change `AuthorityFactReplay` to return `HistoricalReplay`, then migrate
`context_at`, `decision_at`, `plan_decision_at`, `assess_at`, and
`plan_revalidation_at` one by one with focused exact-snapshot and predecessor
tests plus the existing lifecycle measurement script.
