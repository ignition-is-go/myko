# Candidate C: bounded materialization per history snapshot

## Usage (caller's view)

Callers keep their current API and do not manage a cache:

```rust
let history = authority.history_for_exact_snapshot().await?;
let context = history.context_at(head)?;
let prior = history.decision_at(head, &root)?;
let plan = history.plan_revalidation_at(head, operation, &root, now)?;
```

Each call still validates the requested certified head and applies its own
topology, request, time, binding, and expiry inputs. Calls on one
`Arc<AuthorityHistory>` reuse only that head's validated materialization. A new
event snapshot creates a new history object and memo.

## Problem

`AuthorityHistory::selected_at` walks all transitions and rebuilds authority
facts for every caller. Projection then decodes those facts into an
`EvaluationState`. `context_at`, `decision_at`, and `plan_revalidation_at`
repeat both costs. The outer cache gives an immutable exact event snapshot, but
`refresh` creates a new object when evidence arrives at the same retained head.

## Shape

Add private, bounded state to `AuthorityHistory`:

```rust
struct HistoricalMaterialization {
    head: ControlHead,
    facts: Arc<[CertifiedAuthorityFact]>,
    // Projection has already decoded and realm-validated mutable records.
    projected: Arc<AuthorityFacts>,
}

struct MaterializationMemo {
    entries: VecDeque<HistoricalMaterialization>, // capacity: fixed constant
}

struct AuthorityHistory {
    anchor: AuthorityAnchor,
    history: Vec<EventEnvelope>,
    chain: CertifiedControlChain,
    memo: Mutex<MaterializationMemo>,
}
```

`AuthorityFacts` is the existing owned fact shape in `facts.rs`; it gains no
transport fields. The memo is bounded per history snapshot, not per process or
node. A small FIFO (for example, 16 heads) avoids an LRU protocol.

The private helper supplies both replay output and decoded facts:

```rust
fn materialized_at(&self, head: ControlHead)
    -> Result<HistoricalMaterialization, String>;

fn materialized_at(...) {
    if let Some(hit) = self.memo.lock().get(head) {
        return Ok(hit.clone());
    }
    let transitions = self.chain.transitions_to(head)?;
    let facts = Arc::from(self.selected_from_transitions(transitions)?);
    validate_facts(&facts, self.realm_id())?;
    let projected = Arc::new(project_facts_to_authority_facts(
        &facts, self.realm_id())?);
    self.memo.lock().insert_bounded(head, facts, projected);
    Ok(entry)
}
```

`context_at` calls `materialized_at(head)` before `chain.context_at(head)`.
`decision_at` calls it before scanning transitions. Planning calls it, then
attaches supplied topology before evaluation. Keep the current validation order:
`transitions_to` rejects an invalid chosen head, replay preserves causal,
canonical, realm, and duplicate checks, and projection preserves decode and
fact checks. Do not memoize a permit, quorum result, currentness claim, or
time-dependent evaluation.

Concurrent misses may replay twice. That is intentional: a per-entry lock or
single-flight mechanism would add coordination without changing correctness.
The insertion rechecks the key and keeps the first completed value. `refresh`
constructs an empty memo. Therefore new evidence at an unchanged head cannot
reuse old conclusions, while a valid predecessor below an invalid successor
remains independently readable.

## Rationale

This shape targets transition replay and repeated JSON decoding. It hides cache
ownership and eviction while preserving the public `AuthorityHistory` API. The
outer snapshot is the freshness boundary; the head key is the historical one.

Incremental materialization across refreshed snapshots could reuse prefixes,
but it needs proof that every retained event identity and transition dependency
is unchanged, plus invalidation for malformed successors. That complexity sits
in `refresh` and risks making predecessor reads depend on later evidence. A
call-local replay is easier to prove but repeats the full cost. This memo avoids
both cross-snapshot invalidation and a new caller protocol.

## Tradeoffs accepted

- We accept duplicate work on simultaneous cache misses in exchange for no
  single-flight state machine.
- We accept eviction and occasional replay in exchange for a fixed memory
  bound.
- We accept cloning projected facts per evaluation in exchange for keeping
  topology and time out of shared state.

## Alternatives considered

Incremental prefix materialization would store replay checkpoints and extend
them during `refresh`. It hides more CPU work but exposes dependency and
invalidation complexity, especially when same-head evidence invalidates a
successor. It lost on proof burden.

Call-local replay requires no new state and has the smallest implementation
surface, but every caller repeats transition parsing and projection decoding.
It lost on the measured repeated-call path.

## Concrete negative tests

- Add evidence that leaves the retained head unchanged. The next
  `history_for_exact_snapshot` must return a different `Arc` and recompute the
  requested result.
- Add a malformed chosen successor after a valid predecessor. `context_at` on
  the predecessor must succeed; the successor must fail after predecessor reuse.
- Mutate topology, request binding, or evaluation time between calls. The
  historical facts may be reused, but each result must reflect the new input
  and expiry checks.
- Corrupt a selected payload and verify the first and repeated calls return
  the same decode error rather than a cached partial result.
- Measure the native lifecycle script with unchanged build and report phase
  timings alongside exact-head reuse tests.

## Synthesis decision

Not applicable. This is the independent candidate C sketch.

## Open questions and risks

- Is a capacity of 16 heads sufficient for observed coordinator fan-out, or
  should the implementation expose a test-only capacity constant?
- Does `AuthorityFacts` need a dedicated projection helper, or can the current
  `project_facts` be split without widening module visibility?

## Next implementation step

Refactor projection to produce one validated `AuthorityFacts`, then add the
private bounded memo and exact-head reuse tests before changing measurements.
