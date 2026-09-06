# Candidate A: causal batches with embedded custody control

Status: reviewed candidate, not an approved implementation specification. The
comparison preferred its causal history model, but the coordinated-by-default
handler contract conflicts with August's local-first requirement. Identity,
cross-slot coordination, and expired-writer history also need correction. See the
current checkpoint in `scope-continuity-plan.md` before using this candidate.

## Problem

Myko currently persists an ordered node log and deduplicates imported `EventId`s, but a source cursor
describes only that source's observation order. Scope continuity instead needs one stable scope identity,
the complete immutable union of accepted batches, deterministic materialization independent of delivery
order, and custody facts that survive every founding node. This design keeps node logs as transport and
replay indexes, makes a per-scope causal DAG authoritative, and records authority, membership, custody,
and command outcomes in that same history. It preserves local multi-writer commitment for declared
convergent work; it coordinates only command identity, exclusive invariants, and administrative epochs
that cannot be made safe by merge.

## Usage (caller's view)

Application authors use normal handlers, which are coordinated by default. The offline path is a separate,
restricted mutation language; it cannot call user code, read ambient state, or perform effects.

```rust
let rename = ConvergentProposal::at(command_id, caller_frontier)
    .set_lww(Card::NAME, card_id, "new name");

#[myko_command(consistency = "exclusive", resource = "seat/{seat_id}")]
struct ReserveSeat { seat_id: SeatId }

let scope = ScopeId::for_item(&project_id);
let receipt = client.apply(scope.clone(), rename, Durability::Copies(2)).await?;
assert_eq!(receipt.scope_id(), &scope);
receipt.await_durability().await?; // authenticated persisted coverage, not connectivity
```

A durable node joins and leaves a scope through one continuity interface. Catch-up, moving-frontier
tracking, authority checks, and fencing remain behind it.

```rust
continuity.join(scope.clone(), CustodyPolicy::copies(2)).await?;
assert!(continuity.readiness(&scope).is_serving());
continuity.leave(scope.clone()).await?; // returns only after obligations moved or fails safely
```

Native callers keep stable handles while routing changes.

```rust
let cards = client.watch_query(scope.clone(), Cards::all()).await?;
let result = client.resume_command(scope, command_id).await?;
// `cards` transitions Ready -> Resynchronizing -> Ready without replacement.
```

Forrest supplies stable command IDs and resource-local execution constraints, but never an owner node:

```rust
mesh.execute_in_scope(agent_scope, command_id, RequiredHost::ProviderAccount(account)).await?;
```

## Shape

### Core data structures

```rust
// This is the evolution of the existing journal envelope, not a second authoritative log.
struct EventEnvelope {
    position: LogPosition, origin: EventId, recorded_at: DateTime<Utc>, event: NodeEvent,
    record_id: RecordId, scope_id: ScopeId, causal_parents: BTreeSet<RecordId>,
    admitted_under: AuthorityEpoch, author_signature: Signature,
}
struct CommandCommit {
    identity: CommandIdentity, execution: ExecutionProof,
    batch: ChangeBatch, result: Vec<u8>,
}
struct CommandIdentity { scope_id: ScopeId, command_id: CommandId, request_digest: Digest,
                         caller_frontier: CausalFrontier }
enum ExecutionProof {
    ConvergentProposal { canonical_proposal: Vec<u8> },
    Coordinated { certificate: CoordinationCertificate },
}
struct CausalFrontier(BTreeSet<RecordId>); // maximal known records; implies their ancestors
struct ExactOriginCoverage { origin: NodeId, present_sequences: IntervalSet<LogPosition> }
struct ScopeCoverage { scope_id: ScopeId, epoch: AuthorityEpoch, frontier: CausalFrontier,
                       origins: BTreeMap<NodeId, ExactOriginCoverage>, history_root: Digest }
struct CustodyReceipt { custodian: NodeId, coverage: ScopeCoverage, journal_generation: Digest,
                        issued_at: Timestamp, signature: Signature }
struct ScopeCustodyState { policy: CustodyPolicy, active: BTreeSet<NodeId>, receipts: BTreeMap<NodeId,
                           CustodyReceipt>, transition: Option<Handoff> }
struct ScopeReadiness { phase: ReadinessPhase, verified: CausalFrontier,
                        authority_epoch: AuthorityEpoch, durability: DurabilityHealth }
struct BallotState { promised: Ballot, accepted: Option<AcceptedValue> }
struct AcceptedValue { ballot: Ballot, value_digest: Digest, value: CoordinatedDecision }
struct PrepareCertificate { ballot: Ballot, replies: BTreeMap<NodeId, SignedPromise> }
struct CoordinationCertificate { scope_id: ScopeId, epoch: AuthorityEpoch, slot: ControlSlot,
    ballot: Ballot, value_digest: Digest, accepts: BTreeMap<NodeId, SignedAccept> }
```

`RecordId` rather than arrival position names causal truth. `EventEnvelope.position` remains the local
journal cursor and `EventId` remains provenance during migration; neither is a scope frontier. Parents
must exist or arrive in the same atomic ingest bundle before a record becomes applicable. Unknown-parent
records are durably quarantined, never acknowledged as covered. `EventId.sequence` is not required to be
contiguous: imported events consume today's local positions and scoped export omits unrelated records, so
numeric holes are legitimate. The grafted per-origin exact set says which envelopes are physically held, while
DAG parent closure says whether a frontier is complete; neither treats a numeric hole as missing history.
`history_root` commits to `(origin, exact set, record hashes)`. `RecordId` hashes the canonical unsigned envelope;
the author signature covers that ID.

### Public signatures and depth

```rust
trait Continuity {
    async fn join(&self, scope: ScopeId, policy: CustodyPolicy) -> Result<(), ContinuityError>;
    async fn leave(&self, scope: ScopeId) -> Result<(), ContinuityError>;
    fn readiness(&self, scope: &ScopeId) -> ScopeReadiness;
    fn route(&self, request: ScopeRequest) -> Result<RouteCandidates, RouteError>;
}
trait EventJournal { // evolves the existing trait and Redb log
    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError>;
    fn append_causal(&self, events: &[EventEnvelope]) -> Result<DurableCoverage, NodeError>;
    fn persist_and_attest(&self, expected: ScopeCoverage) -> Result<CustodyReceipt, NodeError>;
    fn install_snapshot(&self, snapshot: VerifiedSnapshot) -> Result<(), NodeError>;
    fn prune(&self, permit: RetentionPermit) -> Result<PruneReport, NodeError>;
}
```

These are deep interfaces: callers ask for a domain outcome, continuity transition, or eligible route;
they never orchestrate pages, peers, signatures, or handoff phases. Wire messages are private adapters.
Validation of signatures, canonical encoding, scope equality, parent closure, epochs, and receipts occurs
at storage/transport boundaries; causal reduction and conflict resolution are pure functions.

### Admission and convergence algorithm

```text
admit(identity, operation):
  authenticate principal and exact scope; verify digest and caller frontier fixed in identity
  if command index has ID: reject a different digest, otherwise return recorded outcome
  require local verified history to dominate identity.caller_frontier
  if operation is ConvergentProposal:
    validate generated-schema field/operator combinations; mechanically fold canonical mutation bytes
    derive commit/result/RecordId solely from identity and proposal (no arbitrary handler or I/O)
  else:
    obtain current-epoch certificate for (identity, invariant keys)
    execute once at the certificate's agreed frontier; embed certificate
  atomically append record + origin/parent/command/scope-coverage indexes
  publish CommittedLocally; replicate; advance lifecycle from signed custody receipts
ingest(bundle):
  authenticate sender, records, scope, epoch and certificates
  reject conflicting reuse of command ID; verify every parent is durable or in bundle
  append records and all indexes atomically, in topological order
  materialize deterministic topological folds; concurrent mutations use schema merge strategy,
    with (causal dominance, strategy result, RecordId) as the total deterministic tie-break
```

The resulting `ChangeBatch.causal_parents` is the complete relevant state frontier observed by the handler,
not merely the preceding `Executing` lifecycle event. Read tracking narrows a scope frontier only when the
schema can prove untouched records irrelevant; otherwise the full verified frontier is captured.

The caller chooses the causal frontier once and it is hashed into command identity; a retry cannot silently
adopt a replica's newer frontier. Only built-in CRDT/LWW/set/counter proposals whose encoders and folds Myko
implements use offline admission. A Rust macro cannot prove arbitrary user code deterministic, so every existing
normal handler—including reads, random/time, external effects, and application validation—is coordinated until
explicitly migrated to this restricted API. Same-ID proposals therefore collapse to identical bytes globally;
same ID with any different digest/frontier is a conflict.

For a normal handler the chosen decision names one executor and input frontier; only that executor's signed
outcome can fill the command slot. A crash before a state-only outcome may certify a new attempt only after the
old executor durably releases it. An external-effect claim is never reassigned without a recorded `NotRun`;
loss after invocation but before `EffectOutcome` yields durable `Uncertain`, which recovery returns instead of
rerunning the effect. This preserves C10/C14 without claiming impossible exactly-once provider behavior.

### Coordination and authority epochs

Coordination is Multi-Paxos-shaped but has no required leader: any current voter can propose a value for a
scope-local `ControlSlot`. Each voter durably stores `BallotState` in Redb before replying. A prepare at a higher
`(counter, proposer_id)` persists `promised`; the proposer must carry forward the highest accepted value from a
quorum. An accept request carries that prepare certificate; voters verify its quorum and that its value is the
highest accepted reply (or the proposal if none), then persist only when its ballot is at least the promise.
A voter rejects a different digest at an already accepted ballot. A decision certificate is valid only when
all signatures bind the same scope, epoch, slot, ballot, value digest, voter-set digest, and authority frontier,
and distinct eligible accepts form the epoch's quorum. Competing values cannot both certify because quorums
intersect and a voter never accepts below its durable promise. Crash recovery replays promises and accepts;
retrying a chosen slot returns its chosen value. Slots serialize only command identity/invariant keys and control
decisions, not convergent application writes.

`ControlSlot::Command(command_id)` is deterministic scope-global deduplication. Exclusive keys use a certified
per-key sequence whose decision cites the preceding key decision; a reservation is valid only if that predecessor
is chosen and the pure invariant reducer permits it. Multi-key operations sort keys and certify one atomic
decision containing all predecessor heads, so they cannot deadlock or partially reserve. Administrative slots
form a single certified chain per epoch, which gives membership changes an unambiguous order.

```text
propose(slot, value):
  choose ballot above observed promises; collect prepare replies from quorum
  form prepare certificate; value = highest accepted reply's value, else requested value
  collect durably persisted signed accepts from quorum; validate certificate; append decision event

change_membership(old, new):
  certify JointBegin(old_digest, new_digest) under old quorum
  while joint, every certificate needs a quorum of old AND a quorum of new
  new voters catch up through JointBegin and persist ballot/control history
  certify JointFinish(new_digest) under both quorums; subsequent slots use new only
```

The same joint rule changes custody voters, grants, policy, and revocation. It permits any proposer and replica
replacement without creating a primary. Conflicting membership proposals occupy the same slot and resolve by
the ballot rule; a loser retries after the chosen membership. Loss of either joint quorum makes administration
unavailable rather than weakening safety.

Partition authority is explicit. Authenticated mesh nodes may hold an epoch-scoped
`OfflineConvergentWriter` grant and use only the restricted proposal API. Foreign users, delegated principals,
normal handlers, exclusive work, grants, and administration require a current coordination certificate unless
policy deliberately issues a bounded revocation-latency capability. Revocation is therefore not magically
instantaneous across a partition: already authorized offline proposals remain admissible under their old epoch.
To seal an epoch, every old offline writer must durably append `WriterSealed(final_frontier)` and erase its
admission capability, or its explicitly bounded capability must expire. Until then the writer remains a history
and handoff obligation, readiness is degraded, pruning cannot pass its frontier, and `JointFinish` cannot remove
it. An indefinitely disconnected, indefinitely authorized writer makes final revocation/handoff unavailable;
that is the safety cost of offline writes. Once sealed, old-epoch records beyond the signed final frontier are
rejected, and stale nodes cannot serve, attest, vote, or delegate. Causally prior accepted records are never
retroactively erased.

### Custody, handoff, snapshots, and retention

`persist_and_attest` is the only receipt constructor. In one Redb immediate-durability transaction it
verifies ancestry closure, stores missing records and indexes, advances scope coverage, then stores the
receipt payload before signing it with the authenticated node key. Peers verify scope, epoch, signer
eligibility, journal generation, history root, and that the advertised frontier dominates the promised cut.
There is no second receipt store and no inference from reachability. A signature is an authenticated custody
claim, not cryptographic proof that an adversarial disk retained bytes: the baseline fault model trusts authorized
custodians not to lie and uses independent receipts plus periodic hash/challenge audits for fault detection. A
Byzantine durability target would require erasure-coded proofs or trusted hardware and is not claimed here.

```text
join(candidate):
  certify JoinIntent in scope control history; stream snapshot + missing DAG + concurrent tail
  candidate verifies snapshot frontier/root, installs it, and persists every descendant received
  candidate emits receipt covering JoinReady's parent cut; certify ActivateCustodian
  only then count candidate for new-write durability and advertise scoped readiness
leave(node):
  certify DrainIntent; node stops new command admission but continues ingest/replication
  collect WriterSealed from every removed old-epoch offline writer (or wait for bounded expiry)
  if removal would violate policy, activate replacement first
  leaving node persists final tail and replacement receipt must dominate DrainIntent's moving cut
  certify RemoveCustodian in a newer epoch; old epoch credentials are fenced
```

Writes in flight either precede each writer's sealed frontier and must appear in the dominating receipt, or use
the new epoch and custody set. An unsealed unreachable writer prevents final removal, so late accepted history
cannot be discarded. A crash at any phase replays the transition from immutable control records;
the old custodian remains obligated until `RemoveCustodian`. Removing the last policy-satisfying copy is
rejected. One copy permits an orderly handoff but is explicitly not abrupt-loss tolerant.

A snapshot contains scope ID, authority epoch, causal frontier, materialization hash, history root, schema
version, exact per-origin coverage, and signer. Install succeeds only after verifying its frontier and coverage
root against reachable history/archive; it cannot cross an unsealed epoch or stand in for missing parent bodies.
tail records remain authoritative. Pruning requires a certified `RetentionPermit` proving that enough active
archives have receipts covering the entire pruned downward closure. Operational replicas may drop bodies
only after this proof; command results, control history, content hashes, and archive locators remain. Archive
loss below policy makes the scope degraded and blocks further pruning—it never rewrites the recovery promise.

## Module map and migration

- `federation/src/access.rs`: add `RecordId`, epochs, policies, execution class, and durability target; preserve `ScopeId`; generate bindings from Rust.
- `federation/src/command.rs`: make recovery scope-based; add request digest, read frontier, and execution proof; derive replication states from receipts.
- `federation/src/history.rs`: evolve `EventEnvelope` and `EventJournal` with causal metadata, bundles, exact coverage, snapshots, receipts, and reconciliation; keep local checkpoints only for pulling.
- `federation/src/memory.rs`: index scope/record/command, validate closure, topologically materialize, quarantine gaps, and derive scoped custody/readiness.
- `federation/src/node.rs`: admit through scope history, remove foreign-origin execution rejection, and expose continuity, recovery, and stable watches.
- `redb/src/lib.rs`: add record, parent, frontier, command, snapshot, control, and receipt tables with atomic append/coverage and post-durability attestation.
- `redb/src/lib.rs` also owns atomic per-`(scope, epoch, slot, voter)` promise/accepted rows; no in-memory vote may be emitted before that transaction commits.
- `authority/src/{domain,commands,evaluator,facts}.rs`: materialize realms from control records/current epochs while preserving executor/resource binding.
- `node/src/{peer,status,lib}.rs`: advertise signed scoped readiness/custody/coverage; replace `peer_for_service` with eligible scope candidates.
- `core/src/server/federated_session.rs`: give `NodeRequestRouter` scope, command ID, durability, and locality; preserve the authority chain across retries.
- `iroh/src/protocol.rs`: add private causal/coverage/receipt/readiness frames and retain pins/cursor revalidation; `client.rs` retargets live cells by frontier.
- Forrest `apps/forrestd` mesh clients: remove owner addresses, declare host locality, record effect result/uncertainty, and resume by scope/command ID.

Migration is vertical and executable: (1) evolve the existing envelope/journal plus Redb indexes and replay parity;
(2) causal ingest/materialization behind existing replication; (3) scope command recovery and certificates; (4) custody,
handoff, snapshots, and retention; (5) scope-aware advertisements/router/client retargeting; (6) Forrest clients;
(7) delete source-owned command and service-only routing APIs after every caller moves. At no stage is a second
authoritative `ScopeHistory` written beside the node journal: old envelopes are decoded into the new form during
replay/migration, and one atomic Redb transaction remains the source of truth.

## Fault acceptance matrix

- C01/C13: A->B->C founding-node replacement, including authority/control replay, with A then B truly stopped.
- C02/C16: reopen each Redb store; rebuild from verified snapshot+tail and archive after pruning; byte-compare
  accepted batches/results and materialization hashes.
- C03: three writers, permuted/duplicated bundles and missing-parent quarantine; require equal frontiers/state
  while all competing records remain inspectable.
- C04/C05: delay replication; require local commit, then reject forged, wrong-scope/origin/epoch/signer/gapped or
  crash-before-commit receipts; only persisted authenticated coverage reaches `Replicated`.
- C06/C07: fault after every join/drain durable step with concurrent writes; restart; then abruptly lose one of
  two acknowledged stores, restore redundancy, and explicitly fail beyond policy.
- C08/C09: partition voters and crash after promise/accept; prove one certificate, joint-quorum transitions,
  offline-writer sealing/late-history retention, revoked-node fencing, and no stale readiness after reunion.
- C10/C14/C15: submit one ID through two replicas during disconnect; prove one outcome; crash before/after real
  Forrest filesystem/provider effects, retaining result or uncertainty, and enforce eligible execution host.
- C11/C17: keep query/report/view/item/command handles alive through replica death; observe Resynchronizing and
  coherent recovery; keep one scope Ready while another catches up and never label stale values current.
- C12: independently vary follow, custody, pairing, grants, and selected scope; assert no implied privilege or
  adjacent-scope disclosure.
- C18: run Myko causal/durable/native transport faults and flux gates, generated binding checks, Forrest mesh
  tests/fmt/workspace/strict Clippy, then Mac build/sync without launching the production daemon.

## Synthesis decision

Cross-judging selected this causal-record DAG as the base because parent closure gives one delivery-order-
independent acceptance rule without leaking origin streams to callers. It grafts candidate B's exact per-origin
physical coverage into receipts and snapshots, but rejects its new per-scope counter: existing origin positions
have legitimate holes. It also rejects treating a signed receipt as proof of an honest disk.

## Tradeoffs accepted

- We accept content hashing, parent indexes, and causal-gap storage in exchange for delivery-order independence.
- We accept quorum unavailability for identity/exclusive/admin operations in exchange for partition safety.
- We accept a restricted built-in mutation language for offline commands in exchange for global deduplication.
- We accept retained archive infrastructure in exchange for verifiable complete history with bounded hot stores.

## Alternatives considered

- A permanent primary or command-home node offers simple ordering but loses multi-writer availability and scope
  continuity when that endpoint is absent; its small implementation exposes ownership and failover to callers.
- One global total-order consensus log hides merge logic but coordinates every write and makes unrelated scopes
  share failure. Per-scope causal history keeps the public surface equally small without that coupling.
- Bare per-origin streams plus a materialization frontier simplify append paths, but make every receipt, snapshot,
  and recovery consumer reason about an unbounded origin map; this candidate hides that behind DAG coverage.

## Open questions and risks

- Which generated fields receive LWW, observed-remove set, and commutative-counter proposal operators first?
- Should control sets use majorities only, or policy-selected threshold sets whose intersection is validated?
- How are archive locators replaced without making an external catalog a second authoritative system?
- What canonical merge strategies are safe to expose in generated cross-language schemas?

## Next implementation step

First evolve `EventEnvelope` with `record_id`, `scope_id`, `causal_parents`, and `admitted_under`, and replace
arrival-order `apply_event` in `federation/src/memory.rs` with `apply_causal(&[EventEnvelope])`. The first gate is
a C03 unit test in `federation/src/tests.rs`: three origins, legitimate origin-sequence holes, causal children
delivered before parents, every permutation plus duplicates, and assertions for identical materialization,
frontier, exact coverage, retained competitors, and restart replay. Redb persistence follows only after it passes.
