# Certified authority candidate: selected AuthorityService records

## Tracing before rationale

Current authority is local-origin only. `AuthorityPolicy::new` takes the local
`ApplicationHost`, reads `application.node_id()`, derives the authority realm
scope, and opens `AuthorityFactSources` for exactly that source and scope in
`libs/myko/authority/src/policy.rs:39` and
`libs/myko/authority/src/facts.rs:67`. `AuthorityGrantsView::source_node` returns
that pinned source in `libs/myko/authority/src/domain.rs:120`.

The read path waits for the local source cut before evaluating. `AuthorityPolicy`
calls `authoritative_position_in`, then `current_state`, then the pure
`evaluate` function in `policy.rs:307` and `policy.rs:365`. The command path is
separate. `load_state` in `facts.rs:153` reads AuthorityService facts again from
`node.node_id()`. For durable evaluation, `EvaluateAuthority::execute` in
`commands.rs:483` emits `GrantUse`, `DelegationUse`, `ApprovalUse`,
`ChallengeRecord`, `LeaseRecord`, and `DecisionAudit` in `commands.rs:523` through
`commands.rs:575`. That is not just a read projection. It consumes bounded
authority.

The control layer now gives us durable ordering evidence, not authority by
itself. `FrameworkControlEvent` stores retained statements, signed control votes,
and signed control proposals in `libs/myko/federation/src/control.rs:19`.
`ControlQuorumVerifier::accept_request` verifies a signed proposal and its
prepare proof in `control_quorum.rs:175`. `PreparedControlQuorum::proposal_request`
builds a full proposal request in `control_quorum.rs:321`. `Node::propose_control`
persists that proposal in `node.rs:3602`, and the memory backend shows the same
journal-first rule in `memory.rs:290`. A chosen quorum gives a content head, not
a live policy.

The existing Redb baseline matters. Retaining a foreign grant and reopening a
replacement node still denies with `grant_coverage` in
`libs/myko/authority/src/tests.rs:822`. The fresh parent run is
`/tmp/myko-certified-authority-baseline.log`. This design preserves that
boundary. All C01-C18 continuity requirements remain open.

## Problem

Myko needs portable authority that survives the founder disappearing, but raw
foreign AuthorityService facts must stay inert. A node that merely retained
another node's grant history cannot treat that grant as local authority. At the
same time, a certified old chain is only a historical proof. It does not prove
the current authority head. The design must keep the existing AuthorityService
facts and evaluator, add a certified selection boundary, and make consuming
decisions use the control protocol instead of an uncoordinated local use record.

## Usage

Bootstrapping a realm installs an independent anchor. The anchor is not imported
from history.

```rust
let anchor = StaticAuthorityAnchor {
    realm_id,
    genesis_head,
    initial_epoch,
    controller_keys,
    anchor_signature,
};
let certified = CertifiedAuthority::open(application.clone(), anchor)?;
```

A coordinator chooses exact AuthorityService history. The proposal value carries
the selected immutable events, not a new grant language.

```rust
let selection = certified.prepare_selection(vec![grant_event, obligation_event])?;
let proposal = coordinator.propose(selection.control_value())?;
let chosen = coordinator.choose(proposal)?;
certified.retain_chosen(chosen, selection)?;
```

Live authorization requires current-head readiness. Evaluating at an old certified
head is useful for audit and recovery, but it cannot permit a live command.

```rust
let current = certified.require_current_head(&realm_id)?;
let policy = AuthorityPolicy::new_certified(application, realm_id, certified.clone(), current)?;
policy.authorize(&attempt)?;
```

Bounded use goes through the same chain. The decision ID is stable for one command
effect, so a retry reuses the same AuthorityService use and audit records.

```rust
let intent = CertifiedAuthorityConsumptionId::for_effect(command_id, effect_digest, &attempt);
let pending = certified.prepare_consumption(intent, attempt)?;
let chosen = coordinator.choose(pending.control_value())?;
let permit = policy.finish_consuming_decision(chosen, pending)?;
```

## Shape

The certified chain selects existing AuthorityService event bodies. It does not
define grants, obligations, revocations, approvals, or uses a second time.

```rust
pub struct StaticAuthorityAnchor {
    pub realm_id: AuthorityRealmKey,
    pub genesis_head: ControlHead,
    pub initial_epoch: AuthorityControllerEpoch,
    pub controller_keys: Vec<ControllerId>,
    pub anchor_signature: AnchorSignature,
}

pub struct AuthorityControllerEpoch {
    pub epoch_id: ControlEpochId,
    pub predecessor: ControlHead,
    pub controllers: Vec<AuthorityController>,
}

pub struct AuthorityController {
    pub principal: Principal,
    pub node_id: NodeId,
    pub controller_key: ControllerId,
    pub storage_incarnation: StorageIncarnationId,
}

pub struct CertifiedAuthorityEvent {
    pub event_id: EventId,
    pub body_hash: AuthorityEventHash,
    pub event: NodeEvent,
}

pub enum CertifiedAuthorityTransition {
    SelectRecords {
        predecessor: CertifiedAuthorityHead,
        records: Vec<CertifiedAuthorityEvent>,
    },
    RotateEpoch {
        predecessor: CertifiedAuthorityHead,
        records: Vec<CertifiedAuthorityEvent>,
        successor: AuthorityControllerEpoch,
    },
}

pub struct CertifiedAuthorityHead {
    pub realm_id: AuthorityRealmKey,
    pub epoch_id: ControlEpochId,
    pub control_head: ControlHead,
    pub selected_records_hash: AuthorityEventSetHash,
}

pub enum CertifiedAuthorityReadiness {
    Historical(CertifiedAuthorityHead),
    Current(CertifiedAuthorityHead),
}
```

`CertifiedAuthorityEvent` accepts only AuthorityService command effects for the
realm scope. The projector validates the event ID, body hash, service ID, scope,
causal dependencies, and retained coverage before it lets the event feed the
existing item projection. The normal Myko `ItemProjection` reconstructs
`GrantRecord`, `DelegationRecord`, `ObligationRecord`, `GrantUse`,
`DecisionAudit`, and the other current facts from the selected bodies. The
evaluator sees the same domain records it sees today.

`CertifiedAuthorityReadiness::Current` is the only value `AuthorityPolicy` accepts
for live `AccessPolicy` decisions. A historical head can answer
`evaluate_at_head` for audit, but that result is labelled historical and cannot
authorize a command, replication, custody issuance, or bounded-use consumption.
Current means the node has verified the anchor, every chosen predecessor link,
the latest known certified head for the realm, and the required retained event
bodies through that head. If the node cannot prove current readiness, it denies
with a current-head error instead of returning a stale permit.

Bounded consumption uses deterministic AuthorityService event bodies. For one
command effect, derive:

```rust
pub struct CertifiedAuthorityConsumptionId {
    pub realm_id: AuthorityRealmKey,
    pub head: CertifiedAuthorityHead,
    pub binding: AuthorizationBinding,
    pub command_id: CommandId,
    pub effect_digest: EffectDigest,
    pub phase: AuthorizationPhase,
}

pub struct PendingCertifiedConsumption {
    pub id: CertifiedAuthorityConsumptionId,
    pub request: AccessAttempt,
    pub decision_records: Vec<CertifiedAuthorityEvent>,
}
```

`EvaluateAuthority` should split into a pure decision builder and a record builder.
The record builder emits deterministic `GrantUseId`, `DelegationUseId`,
`ApprovalUseId`, `ChallengeRecordId`, `LeaseRecordId`, and `DecisionAuditId` from
`CertifiedAuthorityConsumptionId`. Those normal AuthorityService events are then
selected by a control transition. If the process crashes after the local events
exist but before certification, they remain raw and inert for portable policy.
Retrying uses the same IDs and same event bodies. If the bodies differ, the
ordinary immutable command conflict rejects the retry.

Epoch rotation is a certified transition. The successor controllers cannot act
because their proposal arrived, because their node retained a grant, or because a
foreign envelope claimed origin. They can act only after the predecessor epoch
chooses the rotation and the local node projects that certified head as current.
Retired incarnations remain useful historical signers, but they cannot sign for a
post-rotation predecessor.

The first public API should stay small.

```rust
impl CertifiedAuthority {
    pub fn open(
        host: ApplicationHost,
        anchor: StaticAuthorityAnchor,
    ) -> Result<Self, CertifiedAuthorityError>;

    pub fn retain_chosen(
        &self,
        chosen: ChosenControlQuorum,
        transition: CertifiedAuthorityTransition,
    ) -> Result<CertifiedAuthorityHead, CertifiedAuthorityError>;

    pub fn facts_at(
        &self,
        head: &CertifiedAuthorityHead,
    ) -> Result<EvaluationState, CertifiedAuthorityError>;

    pub fn require_current_head(
        &self,
        realm_id: &AuthorityRealmKey,
    ) -> Result<CertifiedAuthorityReadiness, CertifiedAuthorityError>;

    pub fn prepare_consumption(
        &self,
        id: CertifiedAuthorityConsumptionId,
        request: AccessAttempt,
    ) -> Result<PendingCertifiedConsumption, CertifiedAuthorityError>;
}

impl AuthorityPolicy {
    pub fn new_certified(
        application: ApplicationHost,
        realm_id: AuthorityRealmKey,
        authority: CertifiedAuthority,
        current: CertifiedAuthorityReadiness,
    ) -> Result<Self, AppError>;

    pub fn evaluate_at_head(
        authority: &CertifiedAuthority,
        head: &CertifiedAuthorityHead,
        request: AccessAttempt,
    ) -> Result<HistoricalAuthorizationDecision, AppError>;

    pub fn finish_consuming_decision(
        &self,
        chosen: ChosenControlQuorum,
        pending: PendingCertifiedConsumption,
    ) -> Result<AuthorizationDecision, AppError>;
}
```

`require_current_head` must not return `Current` merely because the node has the
highest head it has seen. It needs an independently established freshness rule,
or it must fail closed. That rule can be a later protocol unit, but the live
policy constructor must require its result from day one.

## Module map

`myko_federation::control_quorum` stays generic. It verifies and persists
proposals and votes, but it never decodes AuthorityService facts.

`myko_federation::authority` owns transport-neutral anchor, epoch, head, selected
record, and consumption identity types. It should not know `AuthorityService`
queries.

`myko_authority::certified` owns `CertifiedAuthority`, selected-record validation,
the certified fact projector, and current-head readiness. It calls the existing
AuthorityService generated queries or item projection after the certified filter.

`AuthorityPolicy` gets a constructor that takes `CertifiedAuthorityReadiness::Current`.
The old constructor can remain local-origin for existing local-first behavior.
The policy must not silently fall back from certified to raw local or raw foreign
facts.

`myko_redb` needs no authority database. It stores the same ordinary command
events and framework control records in the existing journal.

## Synthesis decision

I choose selected AuthorityService records as the base. The typed-effect option
looked tempting because control values would be smaller and easier to inspect,
but it would define a second meaning for grants and use records. That is exactly
where authority bugs breed. Selecting exact existing event bodies gives a deeper
interface: callers ask for a certified head, while the projector hides chain
verification, event filtering, retained coverage, and item reconstruction.

## Tradeoffs accepted

- We accept larger control values in exchange for not depending on a proposer to
  keep the proposed bytes online.
- We accept a certified projection step in exchange for leaving AuthorityService
  as the only grant and obligation model.
- We accept that historical evaluation cannot permit live work in exchange for
  avoiding stale founder authority after replacement.
- We accept quorum coordination for bounded use records in exchange for correct
  max-use semantics across replicas.

## Alternatives considered

Typed authority effects lost. It hides AuthorityService event details from the
control layer, but exposes a second authority model to implementers and reviewers.

Origin allowlists lost. They are shallow and convenient, but they trust the thing
the baseline already proved unsafe: a foreign record that happens to be retained
or wrapped under a useful source.

Snapshot certification lost. It would make reads cheap, but snapshots replace the
accepted history that Myko uses as authority. Recovery would no longer explain
which command effects produced the facts.

## Open questions and risks

- How does a node learn that a certified head is the latest current head without
  a permanent founder or a stale peer?
- Which AuthorityService item should carry controller epoch records, or should
  epoch metadata remain control-only with a hash linked from selected records?
- How much selected history can one control value carry before we need chunked
  certified selections with the same head semantics?
- What is the exact denial code for "certified but not current" so callers do not
  treat it as ordinary grant absence?

## Next implementation step

Build `CertifiedAuthorityEvent` validation and a projector that reconstructs
AuthorityService facts from one chosen head, then prove the foreign-grant baseline
still denies before certification and permits only at an explicitly current
certified head.
