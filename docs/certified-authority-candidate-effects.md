# Certified authority candidate, typed effects

## Problem

Myko already has durable signed proposer and acceptor records for crash-fault
control decisions. Those records can prove that a value was chosen for one
realm slot, but they do not yet make raw foreign `AuthorityService` facts usable
after the founding authority node disappears. The current `AuthorityPolicy`
loads facts from `application.node_id()` and opens per-item retained sources
against that local origin. A node that merely retained another node's raw
authority history must still deny those facts, which is the right current safety
behavior.

The next design needs a portable authority source that survives founder loss
without making the founder permanent, without inventing a second grant store,
and without coordinating normal application commands. The key decision in this
candidate is that the certified control value contains whole typed authority
effects. It does not point at existing event records and ask policy code to
decide later whether those rows count.

## Usage

### Certifying a founder grant

The proposer constructs one stable authority operation and turns it into a typed
effect batch before starting the existing prepare, proposal, and accept flow.

```rust
use myko_authority::certified::{
    AuthorityCertification, AuthorityOperation, CertifiedAuthoritySource,
};
use myko_federation::control_quorum::{ControlBallot, ControlQuorumVerifier};

let operation = AuthorityOperation::issue_grant(
    realm_id.clone(),
    AuthorityOperationId::from_command(command_id),
    grant,
    AuthorizationBinding::from_request(&request),
)?;

let value = AuthorityCertification::value_for_operation(
    &certified_head,
    operation,
    current_authority_snapshot,
)?;

let prepare = verifier.prepare_request(next_ballot)?;
let promises = coordinator.collect_prepare(prepare)?;
let prepared = verifier.verify_prepare(next_ballot, &promises)?;
let proposal = prepared.proposal_request(&value)?;
let signed_proposal = node.propose_control(&proposal, controller_key)?;
let accepts = coordinator.collect_accepts(verifier.accept_request(&signed_proposal)?)?;
let chosen = prepared.verify_chosen(&signed_proposal.message.value, &accepts)?;

let certified = AuthorityCertification::from_chosen(
    anchor_or_epoch_chain,
    chosen,
    signed_proposal,
    accepts,
)?;
certified_source.install(certified)?;
```

The ordinary grant command may still exist as raw history, but policy does not
use it because it was not selected by a certified predecessor chain.

### Evaluating policy after the founder is gone

`AuthorityPolicy` asks for an authority source at a required head. The local-only
source is the current default. The certified source becomes usable for a realm
only after its static anchor, chosen heads, and readiness against the requested
head verify.

```rust
let source = CertifiedAuthoritySource::open(
    application.node(),
    StaticAuthorityAnchor::for_realm(realm_id.clone()),
)?;

let policy = AuthorityPolicy::with_source(
    application.clone(),
    realm_id.clone(),
    Arc::new(source),
);

let decision = policy.decide_at(&attempt, RequiredAuthorityHead::current());
```

The evaluator still consumes `EvaluationState`. It does not know whether facts
came from local rows or certified effects. The policy must not treat an old
certified snapshot as current authority.

### Atomic bounded uses

A grant with `max_uses` is not consumed by appending a raw `GrantUse` row to a
replacement node. The use is another certified authority operation with a stable
operation ID and one typed effect batch.

```rust
let operation = AuthorityOperation::record_evaluation(
    realm_id.clone(),
    AuthorityOperationId::from_decision(decision_id),
    request.clone(),
    outcome.clone(),
)?;

let certified_use = coordinator.choose_authority_operation(operation)?;
certified_source.install(certified_use)?;
```

If the proposer crashes after the value is chosen, a retry proposes the same
operation ID and same typed effect batch. If it tries to use the same ID for a
different batch, certification rejects it before policy state changes.

## Shape

### Domain types

```rust
pub mod certified;

pub struct StaticAuthorityAnchor {
    pub realm: AuthorityRealmKey,
    pub initial_epoch: ControlEpochId,
    pub initial_head: ControlHead,
    pub controllers: Vec<ControllerId>,
    pub bootstrap_principal: Principal,
}

pub struct CertifiedAuthoritySource {
    node: Node,
    anchor: StaticAuthorityAnchor,
    heads: BTreeMap<ControlHead, CertifiedAuthorityHead>,
    facts: CertifiedAuthorityFacts,
    current: CertifiedAuthorityReadiness,
}

pub enum CertifiedAuthorityReadiness {
    Current(ControlHead),
    KnownButStale(ControlHead),
    Incomplete {
        required: ControlHead,
        latest: Option<ControlHead>,
    },
}

pub struct CertifiedAuthorityHead {
    pub slot: ControlSlot,
    pub head: ControlHead,
    pub predecessor: ControlHead,
    pub epoch: ControlEpochId,
    pub effects: CertifiedAuthorityEffects,
    pub proposal: SignedControlProposal,
    pub accepts: Vec<SignedControlVote>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityOperation {
    pub id: AuthorityOperationId,
    pub realm: AuthorityRealmKey,
    pub predecessor: ControlHead,
    pub epoch: ControlEpochId,
    pub binding: AuthorizationBinding,
    pub effects: CertifiedAuthorityEffects,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifiedAuthorityEffects {
    pub realm: Option<AuthorityRealm>,
    pub capabilities: Vec<CapabilityRegistrationEffect>,
    pub grants: Vec<GrantEffect>,
    pub delegations: Vec<DelegationEffect>,
    pub obligations: Vec<ObligationEffect>,
    pub challenges: Vec<ChallengeEffect>,
    pub approvals: Vec<ApprovalEffect>,
    pub leases: Vec<LeaseEffect>,
    pub uses: Vec<AuthorityUseEffect>,
    pub audits: Vec<DecisionAuditEffect>,
}

pub enum GrantEffect {
    Issue(GrantRecord),
    Revoke {
        id: AuthorityGrantId,
        revoked_at: DateTime<Utc>,
    },
}

pub enum AuthorityUseEffect {
    Grant(GrantUse),
    Delegation(DelegationUse),
    Approval(ApprovalUse),
}
```

The effects are authority-domain values, not event envelopes. They are serialized
as `ControlValue` only at the control boundary. Policy code never receives a
half-decoded quorum value.

### Function signatures

```rust
impl AuthorityOperation {
    pub fn issue_grant(
        realm: AuthorityRealmKey,
        id: AuthorityOperationId,
        grant: AuthorityGrant,
        binding: AuthorizationBinding,
    ) -> Result<Self, CertifiedAuthorityError>;

    pub fn record_evaluation(
        realm: AuthorityRealmKey,
        id: AuthorityOperationId,
        request: AccessAttempt,
        outcome: EvaluationOutcome,
    ) -> Result<Self, CertifiedAuthorityError>;

    pub fn rotate_epoch(
        realm: AuthorityRealmKey,
        id: AuthorityOperationId,
        next_epoch: CertifiedControllerSet,
        binding: AuthorizationBinding,
    ) -> Result<Self, CertifiedAuthorityError>;
}

impl AuthorityCertification {
    pub fn value_for_operation(
        predecessor: &CertifiedAuthorityHead,
        operation: AuthorityOperation,
        current: &EvaluationState,
    ) -> Result<ControlValue, CertifiedAuthorityError>;

    pub fn from_chosen(
        anchor: &StaticAuthorityAnchor,
        chosen: ChosenControlQuorum,
        proposal: SignedControlProposal,
        accepts: Vec<SignedControlVote>,
    ) -> Result<CertifiedAuthorityHead, CertifiedAuthorityError>;
}

impl CertifiedAuthoritySource {
    pub fn open(
        node: &Node,
        anchor: StaticAuthorityAnchor,
    ) -> Result<Self, CertifiedAuthorityError>;

    pub fn install(
        &self,
        head: CertifiedAuthorityHead,
    ) -> Result<CertifiedAuthoritySnapshot, CertifiedAuthorityError>;

    pub fn snapshot(&self) -> Result<EvaluationState, CertifiedAuthorityError>;

    pub fn snapshot_at(
        &self,
        required: RequiredAuthorityHead,
    ) -> Result<EvaluationState, CertifiedAuthorityError>;

    pub fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>>;
}

impl AuthorityPolicy {
    pub fn with_source(
        application: ApplicationHost,
        realm_id: AuthorityRealmKey,
        source: Arc<dyn AuthoritySource>,
    ) -> Self;
}

pub trait AuthoritySource: Send + Sync {
    fn snapshot(
        &self,
        realm_id: &AuthorityRealmKey,
        required: RequiredAuthorityHead,
    ) -> Result<EvaluationState, AuthoritySourceError>;
    fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>>;
}
```

### Module map

`libs/myko/authority/src/certified/mod.rs` owns public source and operation
types.

`libs/myko/authority/src/certified/effects.rs` owns typed effect validation,
application to `CertifiedAuthorityFacts`, and conversion into `EvaluationState`.

`libs/myko/authority/src/certified/chain.rs` owns anchor and predecessor-chain
verification. It decodes `ControlValue`, checks the chosen certificate with the
existing `ControlQuorumVerifier`, derives the content head, and verifies that
the value's predecessor equals the slot predecessor.

`libs/myko/authority/src/certified/source.rs` owns `CertifiedAuthoritySource`.
It reads retained `FrameworkControlEvent::ControlProposal` and
`FrameworkControlEvent::ControlVote` records from the realm scope, builds the
latest certified chain, and materializes facts from typed effects.

`libs/myko/authority/src/policy.rs` keeps policy evaluation. Its dependency
changes from private `AuthorityFactSources` to an `AuthoritySource` trait. The
existing local source implements the trait with today's behavior. The certified
source implements the same trait.

No new grant database appears. Certified facts are a projection of chosen Myko
control history, just as materialized items are a projection of accepted command
history.

## Rationale

Typed effects put the authority decision in the chosen value itself. That hides
the risky part behind one interface: "install this certified head." Callers do
not pass event IDs, row IDs, source nodes, and a pile of filter knobs to make a
foreign grant count. They pass a chosen control value and receive an
`EvaluationState`.

This also keeps validation at the right boundaries. `control_quorum` validates
signatures, quorum, ballot recovery, and exact slot. `certified::chain`
validates anchor, epoch, predecessor, operation ID, and effect encoding.
`certified::effects` validates authority semantics against the previous
authority state. `AuthorityPolicy` evaluates an already materialized state and
does not learn transport or quorum details.

Stable operation identity is load-bearing. `AuthorityOperationId` must be inside
the chosen value and must be unique at one predecessor. A retry after crash can
reconstruct the same value. A conflicting same-ID effect at the same predecessor
is a certification error, not a second grant or another use. This is what makes
bounded `max_uses` consumption safe without coordinating ordinary application
commands.

The static external anchor is intentionally small: realm, initial epoch,
initial head, controller keys, and bootstrap principal. It is the one thing the
system does not derive from imported Myko history. Every later controller set is
a certified typed effect chosen by the current epoch. Future epoch rotation is
therefore a normal authority operation, but activation waits for predecessor
verification. A losing rotation cannot install its controllers merely because
its records arrived first.

Currentness is separate from historical evaluation. A node may have enough
certified history to answer "what was true at head H" while still lacking the
latest certified head needed for live `AccessPolicy` decisions. Live policy must
fail closed with a not-current denial when the required head is unknown or still
causally incomplete. Historical proof is useful for replay and audit; it is not
permission to serve current grants.

Storage and authority stay separate. Durable vote records show that a local
controller persisted a promise, accepted value, or proposal before responding.
They do not by themselves authorize the effect. The certified source must still
verify the chosen value against the authority chain and typed rules. A copied or
restored store may preserve the same storage incarnation, so this design does
not treat storage identity as freshness proof.

## Synthesis decision

This is candidate B's independent shape. It prefers typed authority effects in
the certified control value over selecting existing authority event records. The
winning part is the narrow caller interface: install a certified head and read
an evaluation snapshot. The cost is a new effect codec and validator, but that
cost sits in one authority-owned module instead of leaking into policy,
replication, and control-quorum callers.

## Tradeoffs accepted

- We accept duplicating authority row shapes as effect variants in exchange for
  a value that remains usable after the proposer and founder source disappear.
- We accept that raw foreign `AuthorityService` facts stay inert in exchange for
  not granting policy power to copied history without a certified predecessor
  chain.
- We accept that epoch rotation is not automatic on record arrival in exchange
  for fencing losing rotations and stale controller sets.
- We accept that `AuthorityPolicy` gains an injected source in exchange for
  keeping the evaluator unchanged and transport-neutral.
- We accept that storage incarnation is only an input to custody and vote
  durability checks in exchange for not pretending it detects rollback.
- We accept that bounded-use permits require another certified operation in
  exchange for preventing two replacement nodes from consuming the same shared
  allowance independently.

## Alternatives considered

Selecting existing event records lost. It exposes too much to callers: source
node, event filtering, service rows, causal cuts, and questions about whether a
raw row is certified. It also leaves proposer liveness in the design because the
chosen value can become a pointer to bytes that the new authority source has not
retained.

A second grant database lost. It might be simple to query, but it splits the
source of truth from Myko history. Recovery then has to reconcile two durable
systems. That is exactly the failure shape the continuity plan rejects.

Coordinating ordinary authority commands by default lost. It would make every
normal write pay for control safety. The only coordinated path here is the
authority control operation that changes what policy may trust. Application
state and ordinary non-control commands remain local-first.

Treating retained raw facts as authority after receipt lost. Custody evidence
answers "who retained these bytes." It does not answer "which grants are active."
Authority activation needs a certified effect chain, not a storage promise.

## Open questions and risks

- What is the exact static anchor distribution format, and where does a client
  obtain it before any Myko authority history is trusted?
- Should the first implementation support only `IssueAuthorityGrant`, or also
  certified `EvaluateAuthority` use records so `max_uses` can be proven in the
  same unit?
- How should `AuthorityOperationId` be derived for framework-issued effects:
  command ID, decision ID, or a new framework operation ID with explicit retry
  bytes?
- Which custody obligation must be attached before an epoch rotation activates
  a successor controller set?
- What non-rollback witness, if any, is required before a controller with an old
  storage incarnation can vote in the current epoch after a long outage?
- What source announces or discovers the required current head for live policy
  without letting an attacker pin a stale head as "current"?

## Integration call chain

1. `AuthorityPolicy::decide_at` asks its `AuthoritySource` for an
   `EvaluationState` at the required certified head.
2. `CertifiedAuthoritySource::snapshot` returns state built from the latest
   certified head under the static anchor and verified epoch chain. For live
   policy, `snapshot_at` rejects stale or incomplete heads.
3. `CertifiedAuthoritySource::install` accepts only a `CertifiedAuthorityHead`.
   It never takes raw grants or event envelopes.
4. `AuthorityCertification::from_chosen` verifies the existing
   `ChosenControlQuorum`, proposer record, accept votes, anchor, epoch,
   predecessor, operation ID, and typed effect bytes.
5. `certified::effects::apply` updates `CertifiedAuthorityFacts`, rejects
   duplicate operation IDs at one predecessor, rejects same-ID conflicting
   effects, and records bounded uses atomically with the permit audit.
6. Existing `evaluate(&EvaluationState, &AccessAttempt, now)` makes the final
   permit, challenge, or deny decision.

## Next implementation step

Build `certified::effects` and one focused positive test that a certified
`IssueAuthorityGrant` chosen by the existing durable proposer and acceptor path
permits after the founder Redb node is closed and the successor reopens from
retained control history.
