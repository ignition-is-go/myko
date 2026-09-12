# Candidate A: snapshot-owned historical memoization

## Problem

`AuthorityHistoryCache` reuses an `Arc<AuthorityHistory>` only while the node event snapshot is byte-for-byte unchanged. Inside that immutable snapshot, `selected_at` still replays the certified chain for every call, `decision_at` scans it again, and `project_facts` repeatedly decodes the same authority items. The optimization must preserve canonicality, realm, topology, replay error, expiry, and fresh quorum checks. In particular, evidence added at the same retained head must invalidate old conclusions, while a valid predecessor must remain readable below an invalid successor.

## Usage (caller's view)

Public calls do not change. Callers neither name nor manage the memo.

```rust
let history = coordinator.history_for_exact_snapshot().await?;

history.context_at(head)?;
let prior = history.decision_at(head, &root)?;
let plan = history.plan_revalidation_at(head, operation, &root, Utc::now())?;
```

All other public signatures stay unchanged. Live access still synchronizes evidence, obtains the exact snapshot, coordinates quorum, binds the request, and checks expiry at use time. A memoized historical result is not permission.

## Shape

Add a private, snapshot-owned memo to `AuthorityHistory` in `certified/history.rs`:

```rust
pub struct AuthorityHistory {
    anchor: AuthorityAnchor,
    history: Vec<EventEnvelope>,
    chain: CertifiedControlChain,
    historical: Mutex<BTreeMap<ControlHead, Arc<Result<HistoricalHead, String>>>>,
}

struct HistoricalHead {
    facts: Arc<[CertifiedAuthorityFact]>,
    projected: ProjectedAuthorityFacts,
    decisions: BTreeMap<DecisionRootKey, AuthorityDecisionTransition>,
}

struct ProjectedAuthorityFacts {
    // EvaluationState fields except ScopeTopology.
}

impl HistoricalHead {
    fn evaluation_state(&self, topology: ScopeTopology) -> EvaluationState;
}

impl AuthorityHistory {
    fn historical_at(&self, head: ControlHead) -> Result<Arc<HistoricalHead>, String>;
    fn compute_historical_at(&self, head: ControlHead) -> Result<HistoricalHead, String>;
}
```

`historical_at` checks the map under a short lock. On a miss, it computes outside the lock and installs the result unless another thread won the race. Duplicate concurrent computation is acceptable. Both successes and errors are memoized because the snapshot is immutable.

The computation keeps the current validation order:

```text
transitions = chain.transitions_to(head)?
facts, decisions = replay transitions against retained events
validate supported types, canonical mutations, item realms, and immutable identities
decode one topology-neutral ProjectedAuthorityFacts
return HistoricalHead
```

Historical queries obtain the same `HistoricalHead`. `decision_at` uses its decision index. Projection-dependent calls clone the projected collections into an `EvaluationState` and add the caller-supplied topology. Topology stays explicit without becoming a cache key. Time and expiry remain call inputs.

`validate_transition_at` does not insert speculative heads. It initially keeps its existing full replay path. Only heads accepted by `chain.transitions_to` enter the map, so memory is bounded by certified heads in the snapshot. `from_events` and `refresh` create an empty memo. New evidence at the same head creates a new `AuthorityHistory` and cannot see the prior memo.

The private interface hides replay, indexing, decoding, synchronization, and invalidation. Callers retain only their existing domain inputs, per boundary-discipline and minimize-reader-load.

## Synthesis decision

Candidate A recommends per-snapshot, per-head memoization because snapshot identity already defines the correct invalidation boundary. The topology-neutral projection keeps the cache bounded without weakening topology checks.

## Tradeoffs accepted

- We accept one memo entry per requested certified head in exchange for eliminating repeat replay and decode work within a snapshot.
- We accept possible duplicate work on a concurrent miss in exchange for never holding the memo lock during replay.
- We accept collection clones when adding topology in exchange for avoiding an unbounded `(head, topology)` cache.

## Alternatives considered

A nonmemoizing single-pass query object could make `plan_revalidation_at` replay once and share the result locally. Repeated public calls would still replay and decode, and internal callers would acquire lifetime and sequencing concerns. Snapshot memoization hides more work behind the unchanged API.

An eager prefix replay for every head could reduce first-access cost. An invalid successor could block construction of valid predecessor results, and measurements do not show that cold replay dominates.

## Open questions and risks

- Does cloning the topology-neutral projection remain material after replay and decoding are removed from repeat calls?
- Does `CertifiedControlChain` expose a reliable certified-head count for a hard debug assertion on memo bounds?
- Do measurements show enough improvement to justify caching errors as well as successes?

## Negative tests and measurement

- Append malformed evidence without advancing the retained head. Assert that a new `Arc` cannot reuse the old success.
- Query a valid predecessor, then query an invalid successor, then query the predecessor again. Assert success, the same error, and success.
- Vary topology at one head and assert different evaluations where topology matters. Assert that the memo contains one head entry.
- Repeat `decision_at`, `context_at`, and `plan_revalidation_at` at one head. Instrument test-only replay and decode counters and assert one historical computation.
- Assert canonicality, wrong-realm, unknown-item, malformed-payload, and expiry failures stay unchanged.
- Run `scripts/measure-authority-lifecycle.sh` in the same debug profile. Compare with the passing 121.64/122.10 second lifecycle and 28.83/28.93 second same-view regrant samples. Report runs, variance, and profiler overhead without attributing the baseline delay to the sampled stacks.

## Next implementation step

Add `HistoricalHead` and a test-only computation counter, then route `context_at` and `decision_at` through `historical_at` before changing other callers.
