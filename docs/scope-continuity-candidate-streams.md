# Candidate: per-origin streams with certified frontiers

## Problem

A scope must remain usable after every founding node disappears, while accepted
history remains reconstructible and multiple writers can be offline. The current
`EventEnvelope` mixes immutable origin identity with an observer's local replay
position; `ReplicationBatch` is therefore a transport cursor, not a scope union.
This candidate makes those facts explicit: every origin owns an append-only
immutable stream, and a replica serves only a materialization certified by a
frontier over the streams it has durably verified. Authority, custody, routing,
and client recovery remain Myko concerns; Forrest supplies agent/effect policy.

## Usage (caller's view)

The caller submits the same request to whichever eligible replica is reachable:

```rust
let receipt = scope.submit(request).await?;
let value = scope.read::<AgentState>().await?;
let watch = scope.watch::<AgentState>(cursor).await?;
```

```rust
// A retry is safe because command identity is scope-global, not transport-local.
let receipt = router.for_scope(scope_id).submit(request.clone()).await?;
assert_eq!(receipt, router.for_scope(scope_id).submit(request).await?);
```

```rust
// Exclusive operations expose the unavailable case instead of pretending to
// commit during a partition; convergent writes may use the local policy.
scope.submit(exclusive_request).await?; // Err(Unavailable::Coordination)
scope.submit(convergent_request).await?; // accepted only if local policy permits
```

## Shape

### Data model

```rust
pub struct OriginStream {
    pub origin: NodeId,
    pub entries: BTreeMap<ScopeOriginSeq, AcceptedEntry>,
    pub coverage: ExactCoverage<ScopeOriginSeq>,
}
pub struct AcceptedEntry {
    pub id: EventId,                 // (origin, seq), immutable
    pub command: CommandId,
    pub scope: ScopeId,
    pub parents: Vec<EventId>,       // complete causal dependencies
    pub payload: NodeEvent,
}
pub struct Frontier {
    pub streams: BTreeMap<NodeId, OriginSeq>,
    pub control_epoch: ControlEpoch,
}
pub struct Materialization {
    pub frontier: Frontier,
    pub state: ScopeState,
    pub retained_conflicts: Vec<ConflictRecord>,
}
pub struct CustodyReceipt {
    pub scope: ScopeId,
    pub frontier: Frontier,
    pub holder: NodeId,
    pub generation: MembershipGeneration,
    pub signature: ReceiptSignature,
}
```

`ScopeOriginSeq`, `ControlEpoch`, `MembershipGeneration`, and
`ReceiptSignature` are opaque validated newtypes. `ScopeOriginSeq` is allocated
by an origin within a scope; it is not `EventEnvelope.position`, which advances
when imported events are appended locally. `ExactCoverage` records ranges (or
individual holes) rather than assuming `max(sequence)` means contiguous
coverage. `Frontier` is a partial vector clock, never a single global
position. A frontier is certified only when each included entry, its parents,
and the control epoch are persisted and authenticated. Missing streams remain
missing; they are not inferred from connectivity, snapshots, or an advertised
service. A `ChangeBatch` records the observed frontier of every parent stream,
not only the executing event's own parent.

### Deep scope capability

```rust
pub trait ScopeReplica {
    async fn submit(&self, request: CommandRequest) -> Result<CommandReceipt, Unavailable>;
    async fn read(&self, at: ReadAt) -> Result<Materialization, Unavailable>;
    async fn watch(&self, from: Frontier) -> Result<ScopeWatch, Unavailable>;
    async fn frontier(&self) -> Result<CertifiedFrontier, Unavailable>;
}

pub trait ScopeStore {
    fn append_atomically(&mut self, entry: AcceptedEntry,
                         custody: Option<CustodyReceipt>) -> Result<(), StoreError>;
    fn import(&mut self, stream: OriginStream) -> Result<ImportOutcome, StoreError>;
    fn certify(&self, requested: &Frontier) -> Result<CertifiedFrontier, StoreError>;
    fn recover(&mut self) -> Result<Recovery, StoreError>;
}

pub fn merge(entries: impl IntoIterator<Item = AcceptedEntry>)
    -> Result<Materialization, MergeError>;
pub fn authorize_control(event: ControlEvent, epoch: ControlEpoch,
    proof: &ControlProof) -> Result<ControlEpoch, ControlError>;
pub fn choose_replica(view: &[ReplicaView], operation: Operation,
    required: &Frontier) -> Result<NodeId, Unavailable>;
```

`ScopeReplica` hides stream selection, durable receipt verification, replay,
rerouting, and watch resynchronization. Callers see only domain values and
explicit availability errors. `merge` is pure: topologically apply causal
parents, then deterministically order concurrent non-exclusive mutations by
`(scope, command, event id)`; retain losing alternatives as history. Exclusive
commands and membership/grant/revocation events require a certified control
epoch and quorum policy recorded in control history. If the required quorum or
parent history is unavailable, return `Unavailable::Coordination` or
`Unavailable::History`, respectively. No operation promises exactly-once
external effects: command results are deduplicated; Forrest effect recovery
returns `Committed`, `Uncertain`, or `NotRun`.

### Module ownership and flow

* `federation::stream`: `OriginStream`, event IDs, causal validation, import
  gaps, and deterministic merge; it knows no transport.
* `redb::scope_store`: atomically appends accepted entries, origin indexes,
  custody receipts, and control records; recovery verifies signatures and gaps.
* `authority::control`: validates epoch transitions, grants, revocation, and
  quorum proofs against control history; stale generations cannot serve or
  delegate.
* `node::scope_replica`: implements `ScopeReplica`, chooses eligible replicas,
  and performs durable reroute/readiness checks.
* `core::server::federated_session` and `iroh::client`: preserve live handles,
  switch serving replica, install a fresh certified frontier, and mark watches
  resynchronizing. Forrest consumes the capability and never owns scope state.

Replication exchanges `(scope, origin, exact_coverage, parents, entries)` plus
signed persisted coverage. A receiver rejects wrong scope/origin, gaps,
unauthorized signer, stale generation, or a receipt for unpersisted data. Join
first imports and certifies required history; leave transfers a signed custody
receipt in the same durable transaction as the final acknowledged coverage.
Only then is the old holder removed from the control epoch. Crashes replay the
atomic journal and reconstruct obligations idempotently.

### Migration seams

First add scope-origin IDs, exact coverage, and observed-frontier parents while
retaining local replay positions only for transport cursors. Then migrate Redb
indexes and atomic receipt records; rebuild materializations from streams, not
snapshots. Next move `AuthorityRealm` grants, bootstrap membership, revocation,
and executor binding into replicated control history with signed epochs; the
authenticated executor check remains at the boundary, but no grant points at a
founding authority endpoint. Finally make `NodeRequestRouter` ask
`choose_replica`, and let native clients reconnect through that result while
keeping one live handle. Each seam is dual-read/verified before its old path is
removed; no compatibility path can authorize stale generations.

### Availability limits

Reads and convergent writes are available on any authorized replica whose
requested frontier is certified locally. Exclusive writes, revocation, grants,
custody transfer, and pruning require the recorded control quorum and are
unavailable during a partition that cannot reach it. A scope may serve while a
different scope catches up. A lost last copy reports explicit history loss;
there is no silent recovery from a snapshot.

## Synthesis decision

This is candidate B: choose per-origin immutable streams plus certified
materialization frontiers as the base. It deliberately rejects a global event
DAG/central index as the primary shape; a DAG may be derived for diagnostics,
but stream ownership makes independent writers and custody gaps visible. The
candidate also borrows the useful control-epoch separation from that alternative
without introducing a permanent primary or a lease that is not durably voted.

## Tradeoffs accepted

* We accept vector-frontier metadata and causal gap handling in exchange for
  truthful multiwriter recovery.
* We accept deterministic conflict policy plus retained losing history in
  exchange for convergence without erasing provenance.
* We accept unavailable exclusive operations during partitions in exchange for
  preserving safety.
* We accept a more involved atomic store record in exchange for receipts that
  cannot claim data that was not persisted.
* We accept replica switching and client resync boundaries in exchange for
  stable handles without pretending continuity of an unverified cursor.

## Alternatives considered

### Global event DAG with one scope index

Each node appends vertices to a shared DAG and replicas exchange a global
frontier. It hides causal traversal but exposes callers to DAG completeness,
index rebuild, and conflict traversal; central indexing becomes a bottleneck
and makes custody proofs harder to scope. It lost because per-origin streams
make gaps and independently durable copies directly checkable.

### Epoch-ledger with a designated sequencer per epoch

An epoch leader serializes all writes and signs snapshots/frontiers. It gives a
small read API but exposes partition-dependent leader elections and risks
becoming a permanent-primary substitute; offline convergent writers still need
another merge path. It lost because control epochs are needed only for
exclusive/authority decisions, not every application event.

## Open questions and risks

* Which deterministic merge policies should each Forrest agent event type
  register, and which must be exclusive?
* What minimum custody quorum is deployable for each durability policy?
* Should archived streams use the same signature envelope or a separately
  auditable archive key?
* How much frontier metadata can native clients retain before pagination is
  required?

## Fault tests (C01–C18)

Implement the matrix in `scope-continuity-plan.md` against real Redb and Iroh:
C01 replacement continuity; C02 reopen/rebuild completeness; C03 reordered,
duplicated multi-origin convergence; C04 local commit versus delayed
replication; C05 forged/gapped/stale receipt rejection; C06 interrupted join /
leave; C07 one-copy loss and restoration; C08 partitioned exclusive safety;
C09 stale/revoked node fencing; C10 rerouted command result identity; C11 live
watch resync; C12 independent following/replication/custody/grants; C13 control
history churn; C14 uncertain external effects without replay; C15 resource
locality; C16 retention/archive recovery; C17 scoped readiness; C18 Myko flux,
Forrest gates, generated bindings, and Mac sync.

## Next implementation step

Add the opaque frontier/receipt types and pure stream merge in
`federation::stream`, then write C03/C05 permutation and forgery tests before
wiring Redb persistence or native rerouting.
