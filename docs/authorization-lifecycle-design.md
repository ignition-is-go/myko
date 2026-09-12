# Authorization lifecycle

## Decision

Use the candidates' common typed authorization block and atomic writer transition.
Keep the existing owned watch entry points, prepared requests, and publication
streams. Add no independent public authorization flag. The first implementation keeps
the existing state structs and extends their lifecycle enum; it does not claim
that all invalid public state combinations are statically unrepresentable.

## Usage and shape

Applications keep one `follow_*_reactive` owner and render its lifecycle. A
blocked lifecycle distinguishes denial from a challenge and carries the server's
domain evidence. A blocked scalar has no value or cursor. A blocked collection
publishes an empty reset and blocked lifecycle in one revision.

```rust,ignore
pub enum AuthorizationBlock {
    Denied(Box<DenyDecision>),
    Challenge { challenge: Box<AuthorityChallenge>, report: Box<AuthorizationReport> },
}
pub enum SubscriptionInterruption {
    Resynchronizing { reason: String },
    AuthorizationBlocked { block: AuthorizationBlock },
}
// SubscriptionLiveness gains AuthorizationBlocked { block: AuthorizationBlock }.
// Both existing writer types gain interrupt(SubscriptionInterruption).
```

`AuthorizationBlock` cannot contain a permit. Interruptions cannot declare data
current. Transport adapters classify failures into an interruption or a terminal
error; the shared writer decides whether to retain or clear. Repeated denial is
idempotent. A subsequent outage cannot restore cleared payload. Successful opens
still pass the existing authorization and coherence checks before publication.

## Composition and recovery

Scalar maps preserve explicit absence. Both joins clear their cached tuple when
a whole dependency is authorization-blocked. This is not partial aggregate
policy: that tuple depends on the now-unavailable whole input. Collection row
maps follow removal diffs. Scalar projections of blocked collections expose no
value instead of inventing an authorized empty result.

Union must not infer row authorization from a whole-handler decision. Its blocked
output must expose no protected payload; any future partial-result API needs
explicit authorization provenance. Recovery recomputes from the currently
allowed inputs and existing coherence rules, never from a pre-denial cache.

Core handler drivers and local/Iroh item drivers retain their owned request and
retry after an authorization block or outage. Core handler protocol and decoding
failures remain terminal. Drop cancels the task as before. Raw caller-driven streams keep
their explicit error behavior; this does not turn them into background owners.

## Synthesis and tradeoffs

Three completed designs were read end to end. The configured fourth model was
unavailable, so the panel used gpt-5.6-sol, gpt-5.5, and gpt-5.6-luna. The independent
gpt-5.5 judge favored A's sum-state model and B's practical writer boundary.

The judge scored each design from 1 to 5 against the agreed criteria.

| Criterion | A | B | C |
| --- | --- | --- | --- |
| Atomic clearing | 5 | 4 | 4 |
| Outage distinction | 5 | 5 | 4 |
| Ownership and recovery | 4 | 5 | 4 |
| Framework boundaries | 4 | 5 | 3 |
| API size | 4 | 4 | 2 |

The implementation takes A's permit-free block type and destructive authorization
transition, B's atomic empty reset and shared interruption classification, and
C's insistence that outage and denial remain distinct. It rejects C's independent
access/transport fields. A full replacement of the public state structs is
deferred: first prove transitive clearing and recovery through the existing
publication pipeline. This is an explicit departure from the judge's preferred
public sum type, not a claim that the existing structs encode absence statically.

No generic event bus, new cancellation token, or parallel snapshot cache is needed.
The existing writers already own atomic publication, and existing task ownership
already owns cancellation. Reuse those mechanisms.

Composition tests and review found that comparing an incoming reset with its
initial row values cannot identify an old publication. Regrant can legitimately
restore identical rows. Collection revisions therefore carry a private ordered
publication, and composition tracks the last accepted sequence. The public
revision type is unchanged. Materialization must not wait for the reactive
scheduler or for publishers to become idle.

## Verification and open work

The first socket regression failed with `query retained protected output after
denial`. The corrected local suite passes four tests, including same-handle
recovery and rejection of commands built from blocked dependencies. Core tests
cover blocked snapshot and delta normalization, plus suppression of protected
catch-up output. The federation regressions cover maps, both joins, collections,
state-only revocation, and identical-row regrant. Independent review caught the
seed ambiguity and an unbounded initialization loop. Both have been removed.

`scripts/verify-query-lifecycle.sh` includes these tests and passes for the final
implementation in `81634`. Strict component validation passes in `33638`.
The subsequent native Iroh test target passes three scenarios for idle
revocation, same-owner regrant, initially denied handler recovery, and explicit
initial item denial. It uses server-side test policy changes, not persisted
identity grants. See [native authorization recovery](query-lifecycle-design.md#native-authorization-recovery).
A subsequent [certified grant lifecycle test](query-lifecycle-design.md#certified-grant-lifecycle)
proves one retained view through real revocation and regrant with two native
controllers. The second controller retains both authority commands after its
journal reopens. That check does not prove post-reopen serving or failover.
Partial authorization provenance, the remaining certified-grant handler matrix,
authorization latency, ordinary-read failover readiness, and the broader mesh
milestone remain open.
