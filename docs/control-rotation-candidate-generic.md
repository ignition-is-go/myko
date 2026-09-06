# Control rotation candidate, generic chain

## Problem

Myko now persists signed control proposals and votes in ordinary federation
history. `ControlQuorumVerifier` can verify one slot when a caller supplies the
slot and controller keys, and `Node::vote_control` can durably sign that request.
That is enough for static historical authority, but not for founder-free
rotation. Today an external caller can still construct a verifier for an
arbitrary slot and ask a local node to vote if it owns the matching key. The
missing layer is a generic certified control chain that derives the next epoch
from the prior chosen rotation, rejects stale or losing epochs, and only then
hands authority a verified value to decode.

This candidate puts controller-chain and epoch traversal in
`myko_federation::control`. Authority becomes a payload consumer. It decodes
selected authority records from a verified chosen value, rather than owning the
rules for controller succession.

## Usage

### Build a successor context after rotation

The caller starts from an independently provisioned anchor. Imported history can
help prove the chain, but it cannot create the anchor.

```rust
use myko_authority::{AuthorityHistory, AuthoritySelection};
use myko_federation::control::{
    CertifiedControlChain, ControlAnchor, RequiredControlHead,
};

let anchor = ControlAnchor::new(
    realm_scope.clone(),
    genesis_epoch,
    genesis_head,
    founding_controllers,
)?;

let chain = CertifiedControlChain::replay(
    replacement_node.events_after(None)?,
    anchor,
)?;

let successor = chain.context_at(RequiredControlHead::Exact(rotation_head))?;
let authority = AuthorityHistory::from_certified(
    &successor,
    AuthoritySelection::realm(realm_id.clone()),
)?;
let decision = authority.assess_at(grant_head, &attempt, now, topology)?;
```

If the old controllers choose a rotation, the replacement node no longer needs
the old stores to evaluate authority after it retains the certified authority
history. Safe custody handoff is still a separate requirement. The next decision
is verified against `successor.verifier()`, whose controller keys come from the
chosen rotation value, not from the caller.

### Propose a rotation

The control layer owns the operation shape. Authority only supplies the payload
records that the current epoch authorizes.

```rust
let rotation = chain.rotation_value(
    current_head,
    ControlRotation {
        operation_id,
        successor_controllers,
        retained_authority_history: retained_manifest.commitment()?,
    },
)?;

let round = chain.begin_round(current_head, next_ballot)?;
let promises = coordinator.collect_prepare(round.prepare_request())?;
let prepared = round.verify_prepare(&promises)?;
let proposal = prepared.proposal_request(&rotation)?;
let signed = node.propose_control(&proposal, controller_key)?;
let accepts = coordinator.collect_accepts(round.accept_request(&signed)?)?;
let chosen = prepared.verify_chosen(&signed.message.value, &accepts)?;

chain.install_chosen(chosen, signed, accepts)?;
let successor = chain.context_at(RequiredControlHead::LatestKnown)?;
```

`install_chosen` rejects a losing rotation, a value whose derived successor epoch
does not match its bytes, and a proposal for an old epoch at the new predecessor.

### Fence local vote issuance

Raw `Node::vote_control` remains a low-level durable signer. Production callers
do not build `ControlQuorumVerifier` directly.

```rust
let context = chain.context_at(RequiredControlHead::LatestKnown)?;
let request = context.prepare_request(next_ballot)?;
let vote = node.vote_certified_control(&request, controller_key)?;
```

`vote_certified_control` checks that the requested slot is the current certified
context for this realm before delegating to the existing durable vote append.
This is local issuance fencing. It is separate from certification rejection:
imported stale votes may remain retained history, but they cannot create a live
successor context.

## Shape

### Generic federation types

```rust
pub mod control;

pub struct ControlAnchor {
    pub realm: ScopeId,
    pub genesis_epoch: ControlEpochId,
    pub genesis_head: ControlHead,
    pub controllers: CertifiedControllerSet,
}

pub struct CertifiedControllerSet {
    pub epoch: ControlEpochId,
    pub controllers: Vec<ControllerId>,
}

pub struct CertifiedControlChain {
    anchor: ControlAnchor,
    heads: BTreeMap<ControlHead, CertifiedControlHead>,
    current: Option<ControlHead>,
}

pub struct CertifiedControlHead {
    pub slot: ControlSlot,
    pub head: ControlHead,
    pub predecessor: ControlHead,
    pub epoch: ControlEpochId,
    pub value: CertifiedControlValue,
    pub proposal: SignedControlProposal,
    pub accepts: Vec<SignedControlVote>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifiedControlValue {
    pub operation_id: ControlOperationId,
    pub predecessor: ControlHead,
    pub payload: ControlPayload,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlPayload {
    AuthorityRecords(CertifiedRecordSelection),
    Rotate(ControlRotation),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRotation {
    pub operation_id: ControlOperationId,
    pub successor: CertifiedControllerSet,
    pub retained_authority_history: RetainedHistoryCommitment,
}

pub struct CertifiedControlContext<'a> {
    chain: &'a CertifiedControlChain,
    head: CertifiedControlHead,
    verifier: ControlQuorumVerifier,
}

pub struct CertifiedControlVoteRequest<'a> {
    context_head: ControlHead,
    realm: ScopeId,
    inner: ControlVoteRequest<'a>,
}
```

`CertifiedControllerSet::epoch` is derived, not caller-chosen:

```rust
impl CertifiedControllerSet {
    pub fn derive(
        predecessor: ControlHead,
        controllers: impl IntoIterator<Item = ControllerId>,
    ) -> Result<Self, ControlChainError>;
}
```

The derivation signs nothing. It hashes the predecessor and canonical controller
list under a versioned domain. The chosen rotation value must carry exactly that
derived epoch. This prevents a proposal from smuggling one controller list under
another epoch ID.

### Chain API

```rust
impl CertifiedControlChain {
    pub fn replay(
        history: impl IntoIterator<Item = EventEnvelope>,
        anchor: ControlAnchor,
    ) -> Result<Self, ControlChainError>;

    pub fn install_chosen(
        &mut self,
        chosen: ChosenControlQuorum,
        proposal: SignedControlProposal,
        accepts: Vec<SignedControlVote>,
    ) -> Result<CertifiedControlHead, ControlChainError>;

    pub fn context_at(
        &self,
        required: RequiredControlHead,
    ) -> Result<CertifiedControlContext<'_>, ControlChainError>;

    pub fn begin_round(
        &self,
        predecessor: ControlHead,
        ballot: ControlBallot,
    ) -> Result<CertifiedControlRound<'_>, ControlChainError>;

    pub fn rotation_value(
        &self,
        predecessor: ControlHead,
        rotation: ControlRotation,
    ) -> Result<ControlValue, ControlChainError>;
}

pub enum RequiredControlHead {
    Exact(ControlHead),
    LatestKnown,
}

impl CertifiedControlContext<'_> {
    pub fn verifier(&self) -> &ControlQuorumVerifier;

    pub fn prepare_request(
        &self,
        ballot: ControlBallot,
    ) -> Result<CertifiedControlVoteRequest<'_>, ControlChainError>;

    pub fn accept_request(
        &self,
        proposal: &SignedControlProposal,
    ) -> Result<CertifiedControlVoteRequest<'_>, ControlChainError>;

    pub fn decode_payload<T: ControlPayloadDecoder>(
        &self,
        head: ControlHead,
    ) -> Result<T::Output, ControlChainError>;
}
```

`ControlQuorumVerifier::new` stays available for tests and low-level tools, but
production issuance uses `CertifiedControlContext`. That is the line between
"this certificate is invalid" and "this local node must not sign that request."

### Authority integration

```rust
pub struct AuthorityHistory {
    chain_head: ControlHead,
    selected: Vec<EventEnvelope>,
    projection: EvaluationState,
}

pub struct AuthoritySelection {
    realm: AuthorityRealmKey,
    required_history: RetainedHistoryCommitment,
}

impl ControlPayloadDecoder for AuthoritySelection {
    type Output = AuthorityHistory;

    fn decode(
        &self,
        value: &CertifiedControlValue,
        retained: &RetainedHistoryReader,
    ) -> Result<Self::Output, AuthorityCertificationError>;
}

impl AuthorityHistory {
    pub fn from_certified(
        context: &CertifiedControlContext<'_>,
        selection: AuthoritySelection,
    ) -> Result<Self, AuthorityCertificationError>;

    pub fn assess_at(
        &self,
        head: ControlHead,
        attempt: &AccessAttempt,
        now: DateTime<Utc>,
        topology: ScopeTopology,
    ) -> Result<AuthorizationDecision, AuthorityCertificationError>;
}
```

Authority decodes `ControlPayload::AuthorityRecords` into exact retained
`AuthorityService` records and their command-lifecycle parents. It reuses the
existing `ItemProjection` and evaluator. It does not project raw all-origin
authority rows. It also does not become `AccessPolicy` currentness. A historical
context can answer at head H while still being stale for live serving.

## Module map

`libs/myko/federation/src/control_chain.rs` owns `ControlAnchor`,
`CertifiedControlChain`, `CertifiedControlContext`, rotation derivation, and
chain replay over `FrameworkControlEvent::ControlProposal` and
`FrameworkControlEvent::ControlVote`.

`libs/myko/federation/src/control_quorum.rs` keeps single-slot majority
verification. It should not learn authority payloads or epoch traversal.

`libs/myko/federation/src/node.rs` adds one production-facing method:
`vote_certified_control(&CertifiedControlVoteRequest, &SigningKey)`. It
delegates to the existing durable `vote_control` only after the certified
context proves that the slot is current for the realm.

`libs/myko/authority/src/certified/history.rs` owns
`AuthoritySelection`, selected-record decoding, and `AuthorityHistory`.

`libs/myko/authority/src/policy.rs` remains live local policy until a later
current-head readiness protocol exists. This candidate does not wire historical
authority into live policy.

No module adds a second journal. Control records stay in `NodeEvent::FrameworkControl`.

## Rationale

The generic chain layer owns the one thing that must be shared by every future
control payload: which epoch is allowed to decide the next slot. If authority
owns that traversal, custody and membership would need to duplicate it later.
That smells like two maps, two sets of bugs, and one future evening lost to a
very smug split-brain test.

The public interface is intentionally small. Callers replay a chain, ask for a
certified context, and decode a payload. They do not pass controller lists into
authority projection or ask authority to guess whether an epoch is active.
`ControlQuorumVerifier` remains the single-slot checker. `CertifiedControlChain`
is the multi-slot checker.

A losing rotation is rejected because its derived head is not the predecessor of
any installed successor. An uncertified rotation is rejected because no chosen
quorum exists for its value. A stale-epoch decision at the new predecessor is
rejected because `context_at` derives the verifier from the predecessor's chosen
rotation, not from the request.

Retaining full certified authority history is required before a successor
context is usable for authority payloads. The rotation value binds the retained
history commitment. The authority decoder then verifies exact retained bodies
before producing `AuthorityHistory`. This is still not custody handoff. It is
only enough to avoid using a successor controller context whose authority input
bytes are missing.

Local vote fencing is deliberately separate. Certification rejects bad imported
evidence during replay. Issuance fencing stops this node from signing a stale or
arbitrary slot through the raw durable vote path. Both are needed.

## Synthesis decision

This is candidate B. It moves certified controller-chain traversal into the
generic federation control layer and leaves authority to decode selected payload
records. It rejects the alternative where authority owns epochs because that
would make every future control use reimplement succession.

## Tradeoffs accepted

- We accept a new generic chain module in exchange for one epoch traversal rule
  shared by authority, custody, and later membership control.
- We accept that `ControlQuorumVerifier::new` remains low-level in exchange for
  preserving tests and protocol tools. Production vote issuance must go through
  a certified context.
- We accept historical authority as a separate result from live `AccessPolicy`
  in exchange for not treating an old certified chain as current authority.
- We accept requiring retained authority-history commitments in rotation values
  in exchange for successor contexts that cannot activate over missing input
  bytes.
- We accept dropping compatibility wrappers for internal callers in exchange for
  making stale verifier construction visible at compile time during migration.

## Alternatives considered

Authority-owned epoch traversal lost. It gives authority the smallest immediate
diff, but it leaks controller-chain rules into an application service. Custody
and membership would need their own versions, and stale epoch rejection would
depend on each payload decoder.

Raw verifier construction everywhere lost. It keeps the existing API pleasant
for tests, but production callers can ask a node to sign any slot whose
controller key matches. That is fine as a primitive and wrong as a serving
contract.

Choosing rotations as ordinary authority records lost. It would reuse existing
item projection, but the controller set that decides authority would itself be
authorized by application rows selected after the fact. The trust direction is
backwards.

Treating latest retained head as current lost. It gives easy reads after reopen,
but a partitioned node could miss a revocation or newer rotation and still serve
old grants. Historical context is not live readiness.

## Open questions and risks

- What external distribution format pins the initial `ControlAnchor` before any
  Myko history is trusted?
- Should `RequiredControlHead::LatestKnown` stay test-only until a current-head
  freshness protocol exists?
- Does the first rotation test need a separate retained-history statement for
  the authority records, or is exact local journal verification enough for the
  bounded historical unit?
- How should `vote_certified_control` identify which realm context is current
  when the same node participates in several realms?
- Which later custody rule retires old controllers without treating rotation as
  proof that accepted history was safely handed off?

## Next implementation step

Build `control_chain` with static-anchor replay, rotation-derived successor
epochs, and one Redb test where old controllers choose a rotation, old stores
close, disjoint new controllers choose the next authority-record value, and
losing or stale-epoch evidence is rejected.
