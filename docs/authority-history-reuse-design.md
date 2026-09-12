# Historical authority replay reuse

## Plan

- [x] Ground the historical calculation and live authorization boundary.
- [x] Compare distinct designs for reducing repeated historical replay.
- [x] Select the design and record the tradeoffs.
- [x] Implement and verify unchanged authorization results.
- [x] Check whether implementation evidence requires a different design.

### Design comparison

- [x] Frame the task and criteria.
- [x] Produce independent sketches.
- [x] Cross-judge completed sketches.
- [x] Pick the base.
- [x] Incorporate useful alternatives.
- [x] Verify the synthesized design.

## Problem

The native certified-grant lifecycle test takes about two minutes in the debug
build. Two current measurements passed in 121.64 and 122.10 seconds. Recovery
after regrant took 28.83 and 28.93 seconds. These tests exercise the existing
production path, including fresh controller coordination and persisted history.

CPU samples show JSON parsing, copying, and cryptographic work. The captured
call stacks are incomplete, even with a larger stack buffer. These samples do
not establish which caller accounts for the total delay. Debugger attachment
was denied by the environment.

Current source shows that `AuthorityHistory::selected_at` rebuilds authority
facts from the requested historical chain. `context_at`, `decision_at`, and
`plan_revalidation_at` call it repeatedly. `AuthorityHistoryCache` already reuses
an immutable history snapshot, but the fact replay is repeated within it.

`scripts/measure-authority-lifecycle.sh` reruns the lifecycle assertions and
prints the existing phase timings. Performance comparisons must use the same
build profile and report any profiler overhead.

## Design criteria

The design comparison grades these properties separately:

- New evidence at an unchanged head still invalidates historical conclusions.
- Historical reuse cannot bypass fresh quorum authorization or expiry checks.
- Memory and locking remain bounded by the owning history snapshot.
- Callers do not acquire a new cache-management protocol.
- A repeatable measurement and adversarial tests distinguish reuse from skipped validation.

## Scope

This work does not choose ordinary-read failover readiness. It does not change
the rule that storage-only nodes retain logs without running typed handlers.
No latency threshold or production performance guarantee is inferred from the
debug fixture.

## Grounded flow

`AuthorityHistory` owns an immutable anchor, event vector, and certified chain.
`selected_at(head)` walks the chain from genesis, indexes retained events by
origin, and applies every transition to a fresh `AuthorityFactReplay`.
`context_at`, `decision_at`, `plan_decision_at`, and `plan_revalidation_at` repeat
that calculation. Projection then decodes the selected authority records into
`EvaluationState`.

The existing outer cache returns the same `Arc<AuthorityHistory>` only for an
equal event snapshot. Changed evidence creates a new history object even when
the head is unchanged. Any historical result reuse must follow that ownership.
Historical calculations must preserve replay and decode errors, including valid
predecessors below an invalid successor.

Live authorization remains outside this boundary. `authorize_scoped_access`
synchronizes evidence and coordinates a fresh request-specific decision or
revalidation. Time, request binding, approvals, topology, and uses remain inputs
to that decision. A reused historical calculation is never a permit.

The direct-source review covers `certified/history.rs`, `certified/mod.rs`,
`certified/coordinator/access.rs`, `certified/coordinator/history_cache.rs`,
and `federation/src/control_chain.rs`. Graph generation
`2026-09-05T01:04:50Z` does not track these current paths reliably.

## Usage and chosen shape

Public callers keep `context_at`, `decision_at`, and `plan_revalidation_at`.
The existing private `selected_at(head)` call retains its result and error
semantics. Its implementation can reuse facts from its last successful head.

```rust
struct SelectedFacts {
    head: ControlHead,
    facts: Arc<[CertifiedAuthorityFact]>,
}

pub struct AuthorityHistory {
    // Existing immutable anchor, events, and certified chain.
    selected: Mutex<Option<SelectedFacts>>,
}

impl AuthorityHistory {
    fn selected_at(&self, head: ControlHead)
        -> Result<Vec<CertifiedAuthorityFact>, String>;
    // Check one cached head under the lock. On a miss, release the lock,
    // replay through the existing function, then install only successful facts.
}
```

`from_events` and `refresh` start with an empty entry. Errors and speculative
`validate_transition_at` results are not stored. Computation occurs outside the
lock; simultaneous misses may duplicate work. A competing successful head may
replace the entry without changing either result. Callers receive owned facts
as before, so no cache-management protocol or public API is added.
The implementation uses an immutable `Arc` internally so that cloning the
returned fact vector occurs after releasing the lock. This refines the initial
vector-storage sketch without changing the ownership or reuse boundary.

## Synthesis decision

The main agent read all three sketches. The gpt-5.5 cross-judge scored them as
follows, from 1 to 5 in the order of the design criteria above.

| Candidate | New evidence | Fresh authorization | Memory and locking | Caller interface | Verification |
| --- | --- | --- | --- | --- | --- |
| A, per-head snapshot memo | 5 | 4 | 2 | 5 | 5 |
| B, operation-local replay | 5 | 5 | 5 | 5 | 4 |
| C, bounded snapshot memo | 5 | 4 | 4 | 5 | 4 |

The synthesis keeps B's existing replay semantics and borrows snapshot ownership
from A and a fixed memory bound from C. The first implementation stores one
successful fact result. It does not add B's broader replay-result object or
decision index because those are not needed for this experiment.

A's per-head map could retain quadratic fact copies. C's capacity of 16 has no
measurement behind it. Both proposals eagerly project decoded state, which can
change error timing for callers that previously needed only selected facts.
Those additions are rejected. So are cross-snapshot prefix reuse, cached
errors, cached permissions, and any change to fresh quorum or expiry checks.

The implementation accepts fact-vector clones and duplicate concurrent misses
to keep ownership and locking small. If the real lifecycle measurement does
not improve, the repeated replay hypothesis is insufficient and the memo must
be reconsidered. The unavailable configured gpt-5.4 runner was omitted; sol,
gpt-5.5, and luna supplied the completed candidates.

## Verification and remaining limits

The replay-count regression failed before implementation in `51265` with
`the same snapshot and head replayed 2 times`. It passes after the change.
The counter exists only in the unit-test build. Two integration tests also
verify that same-head snapshots cannot share changed facts or missing-body
failures. Those correctness tests passed before the optimization and afterward.

Final source validation in `58145` passes 30 authority unit tests, 10 historical
tests, 6 consumption tests, and 4 controller-rotation tests. Strict authority
library and test linting passes in `54618`. An earlier lint failure concerned
unchecked arithmetic in the test-only counter; it was corrected without
removing assertions.

The expanded lifecycle verifier passes in `20931`. It includes those authority
checks and the native retained-view scenario, followed by the existing core,
federation, transport, namespace, and persisted-continuity suites. The native
scenario takes 113.54 seconds, including the second controller's journal reopen.
Recovery after the replacement grant commits takes 26.31 seconds. No other
owned Cargo run or profiler overlaps this final measurement.

The initial vector-storage experiment passed in 114.89 seconds with 26.35-second
recovery. That measurement overlapped compilation and other tests. The earlier
121.64 and 122.10-second baselines each included an eight-second CPU sample.
The preceding unprofiled lifecycle run in `88003` took 122.68 seconds.
These observations show a modest improvement in this debug fixture, not a
production latency guarantee or an explanation of the remaining delay.

The deterministic test proves that unchanged historical calculations are
reused. The evidence supports keeping the bounded memo, but does not justify
broader caches or changes to authorization coordination. Independent gpt-5.5
review found no source blocker. Serving-node failover, ordinary-read readiness,
and the broader application-builder milestone remain open.
