# Certified consumption design

## Problem

A certified historical grant is not permission to spend it now. The live command
path also loses the exact effect if it crashes after authorization and before
commit. We need one recoverable authorization decision bound to one durable effect,
with every subsequent decision seeing the uses already chosen by controllers.

## Usage

Application handlers keep calling `commit_bytes`. Federation saves the prepared
effect before invoking policy. Restart resumes that saved effect without invoking
the application handler again. The authority coordinator owns quorum recovery,
so callers do not coordinate a read barrier, reservation, and materialization.

The intended authority interface takes a stable request identity and trusted
request evidence. It returns a decision for that identity, or a coordination
failure. A coordination failure is not an obligation challenge.

## Shape

Federation owns `PreparedCommandEffect` and its command lifecycle. The saved body
includes the original batch, result, actual claims, capabilities, and prospective
topology. Its digest retains the batch ID and causal parents. The command identity
is stable independently of these bytes. A changed body under that identity is a
conflict, not a new request.

Every enforced command effect pays one additional local durable append before
policy evaluation. Trusted framework commands keep their direct commit path.
This preparation does not require a quorum. The additional append is necessary
because policy decides whether an effect consumes authority only after seeing it.

The existing serialized `AuthorizationPending` shape keeps its original batch,
result, challenge, and approvals. Those events already exist in accepted logs.
New commands retain the complete effect in a preceding `AuthorizationPrepared`
event. Challenge advancement checks its batch and result against that evidence.
Older pending events retain their original local approval semantics, without
invented claims or a retroactive certified identity. This preserves immutable
history while deleting the old loose-batch authorization API.

Authority owns the request-specific control payload. Stable identity comprises
realm, request identity, and authorization phase. The full authorization binding,
effect digest, lease request, and explicit trusted topology are separate evidence.
`AccessAttempt` skips topology during serialization, so its default JSON is not a
complete certified request codec.

An authorization round fixes evaluation time and deterministic identity seed
before evaluating. Challenge and lease IDs derive from that seed. Grant,
delegation, and approval use IDs derive from the decision ID and contributor ID.
Neither derivation hashes an outcome that still contains random identifiers.

One chosen control transition contains the decision and its typed use, challenge,
lease, and audit records. It does not fabricate `CommandCommitted` events.
`AuthorityHistory` folds these records alongside selected ordinary authority
records in control-chain order, using the existing item projection and evaluator.
Selection validation must reject duplicate or overwritten immutable records across
both sources. The next decision sees the use as soon as the transition is chosen.

Exact retries recover the chosen result. Challenge fulfillment starts a subsequent
round tied to the same effect and certified approval evidence. It must not either
return the old challenge forever or allow a second permit for the same effect.

## Synthesis decision

Use candidate A's direct request-specific transition. Independent review by
gpt-5.5 agreed that candidate B's retained-history scan adds no freshness proof.
The signed predecessor and chosen successor already bind the decision's read
basis. Reject the barrier wrapper and `RecoveryAlias`.

Replace A's two-step consumption and later record materialization with typed
records in the chosen transition. Independent review accepted this correction.
It removes a realm-wide pending gap and avoids synthetic application commands.
Keep both candidates' requirement to durably prepare the exact effect first.

During implementation, review caught that replacing the fields of the existing
pending variant would make accepted journals unreadable. Restore that durable
fact format and retain new preparation evidence separately in the same journal.
A new versioned pending variant would add another long-lived state without
improving the exact-body guarantee. Resetting logs is not an acceptable migration.

The interface hides quorum recovery and record planning. Application callers own
their request identity and effects, not control-chain stages. Generic federation
code does not import authority record types.

## Constraints still requiring implementation proof

- A controller re-evaluates the proposed intent against its certified predecessor
  and checks the exact planned records before signing. A signed client assertion
  of topology is insufficient.
- A certified realm cannot decide that a local projection is safe merely because
  that stale projection reports an unbounded permit. Any local fast path needs an
  explicit authority validity contract. Ordinary convergent writes remain local
  first; bounded consumption and control transitions coordinate.
- Controller clocks, expiration, and recovery must agree. New request time is not
  client supplied. Recovering an old accepted proposal cannot discard the value
  Paxos requires, even after its lease expires. Choosing that value does not make
  an expired certificate usable. The implementation must distinguish recovery
  safety from permission at use time.
- A certificate authorizes the exact prepared effect. It is not a reusable
  credential for another read or an exactly-once guarantee for physical effects.
- Native transport, authenticated controller endpoints, quorum coordination,
  client request identities, and live enforcement remain required. A manually
  driven historical test does not satisfy those requirements.

`AccessPolicy::decide` now returns a typed `Result` that separates denial from
temporary authority unavailability. Prepared effects survive unavailable
authorization without a durable rejection. Session responses carry typed retry
status, and retained handler clients resubscribe. Real storage and local socket
tests cover those paths. Retained item recovery and in-process submission error
propagation are being verified alongside the controller integration. Unavailable
authority is an error, not a fourth authorization decision.

## Implementation sequence

- [x] Ground the current evaluator, policy, and command lifecycle.
- [x] Compare two designs and obtain independent review.
- [x] Select direct transitions with atomic typed records.
- [x] Save and recover prepared effects through the actual command path.
- [x] Make evaluator identities and record planning deterministic.
- [ ] Replay and validate request-specific certified records.
- [ ] Implement coordinator and live enforcement, including approval rounds.
- [ ] Prove competing uses, mismatch, crash, revocation, rotation, and expiry.
- [ ] Verify affected crates and Forrest callers, then audit C01-C18.

The first two implementation units pass their focused persistence and evaluator
tests. They do not make live authorization certified. All C01-C18 requirements
remain open.

## Verified checkpoint and recovery limits

The native integration checkpoint has four passing coordinator tests,
including authenticated Iroh control requests and authorized exact-scope history
transfer. The native test shuts down normally, reopens both Redb stores, and
recovers the same chosen decision without another grant use. Removing manual
endpoint cleanup exposed a retained-router ownership cycle. The production
evidence adapter now retains the node and endpoint without retaining the router.
The same recovery test and authority strict Clippy pass after that fix.

This proves recovery of a historical decision, not permission to release an
effect now. Live certified policy enforcement, approval rounds, custody, and the
C01-C18 fault matrix remain unfinished. The integrated native-FFI gate also
exposed dropped Swift collection revisions. Writer event delivery now passes
15 Swift native-FFI tests and 19 reactive tests, including cancellation and
reentrant publication-order regressions. Focused strict Clippy also passes.
The complete integrated gate now passes all 708 tests, formatting, and strict
Clippy, including the native continuity regression and server consumers.

Forrest now treats typed authority outages as retryable invocation failures and
keeps authorization denial permanent. Its regression, frozen-source workspace
tests, formatting, and strict Clippy pass. The matching fix is `ae2eaa5`.
Wire version 11 requires coordinated peer rebuilds. Mac verification remains
unfinished because SSH to the other machine timed out.

The following results describe the earlier committed checkpoint.

The current checkpoint passes 275 affected Myko tests, strict Clippy, formatting,
and the native founder-replacement regression. Forrest passes its full workspace
tests, formatting, and strict Clippy. The decision trail records the commands and
logs. The native regression uses explicit transfers and `AllowAllAccessPolicy`.
It does not prove certified custody or automatic client rerouting.

Prepared-effect digests now use ordered topology collections. A regression failed
before this fix, and mobile restart and replication tests pass afterward. Original
pending-command JSON still decodes and resumes without invented prepared evidence.

Prepared records written by the interim unordered-digest implementation require
separate verification. Changing collection order cannot authenticate an old
digest. The local journal contains prepared records, but its read-only audit was
blocked by an open database. Filesystem snapshot cloning is unavailable. No
journal was reset or rewritten, and no production daemon was stopped or started
manually. A marker in raw database bytes does not establish a live-record mismatch.

When the journal is closed, run the read-only audit with:

```sh
cargo run -p myko-redb --example audit_prepared_effects --target-dir target/agent -- /path/to/node.redb
```

The audit reports counts without printing application payloads. A digest mismatch
requires raw-byte recovery analysis, not deletion or re-execution of the command.

One reactive collection-diff assertion failed during an affected-crate run. The
same source passed subsequent focused and full-library runs. No reactive fix was
made, and the intermittent failure remains unexplained.

Run `bash scripts/verify_certified_consumption.sh` to repeat affected-crate tests,
strict Clippy, formatting checks, and the existing native continuity regression.
That native test uses explicit transfers and does not prove native certified
consumption. Forrest workspace checks and the full C01-C18 fault matrix remain
separate required gates.
