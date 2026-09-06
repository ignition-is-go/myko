# Control rotation candidate: authority-owned epoch context

## Problem

The historical authority chain now proves selected `AuthorityService` records at
an explicit head. It still uses one static controller epoch. The next step is to
let an old certified controller set choose a successor set, then let that
successor certify later authority history after every old controller store is
closed. The rotation must not become a live-current proof, a custody
acknowledgment, or a permission to serve. It is only the next historical verifier
context.

The current code matters. `AuthorityHistory::replay` reconstructs chosen heads
from existing `FrameworkControlEvent::ControlProposal` and `ControlVote` records
in `libs/myko/authority/src/certified/history.rs`. `AuthorityHistory::assess_at`
projects only selected records through the existing authority item projection in
`certified/mod.rs`. `ControlQuorumVerifier` verifies one supplied slot and
controller set. `ControlVoteRequest` and `ControlProposalRequest` persist votes
and proposals through `Node`, but they do not know whether a slot is the current
certified authority slot. That last part is a sharp little trap. Certification
can reject stale votes; generic vote issuance cannot prevent them by itself.

## Usage

A caller asks the certified authority layer for the context at the head it wants
to extend. That context contains the only epoch that can certify the next
transition.

```rust
let anchor = AuthorityAnchor::new(realm, epoch, genesis, old_controllers)?;
let history = AuthorityHistory::replay(&node, anchor)?;
let old = history.context_at(grant_head)?;

let rotation = old.rotate(
    operation_id,
    retained_authority_events,
    AuthorityControllerSet::new(vec![
        AuthorityControllerBinding::new(b_principal, b_node, b_key, b_store)?,
        AuthorityControllerBinding::new(c_principal, c_node, c_key, c_store)?,
    ])?,
)?;
```

The old epoch chooses the rotation. A disjoint successor set is fine because the
old quorum signs the transition.

```rust
let ballot = old.ballot(7, old_controller_a)?;
let prepare = old.prepare_request(ballot)?;
let promises = collect_old_epoch_promises(prepare)?;
let prepared = old.verify_prepare(ballot, &promises)?;
let proposal = old.propose(prepared, &rotation, &old_controller_a_key)?;
let accept = old.accept_request(&proposal)?;
let accepts = collect_old_epoch_accepts(accept)?;
let rotation_head = old.verify_chosen(&proposal, &accepts)?;
```

After replay, the new context is derived from the rotation head. The caller does
not pass an epoch ID or controller list for the successor. Those are inside the
chosen rotation value.

```rust
let history = AuthorityHistory::replay(&successor_node, anchor)?;
let next = history.context_at(rotation_head)?;

let next_selection = next.select_records(next_operation, retained_revocation_events)?;
let next_head = choose_with_successor_quorum(next, next_selection)?;
let assessment = history.assess_at(next_head, &attempt, now, topology)?;
```

Historical assessment still does not authorize live work.

```rust
let assessment = history.assess_at(next_head, &attempt, now, topology)?;
if assessment.requires_certified_effect() {
    return Err("historical permit still needs a certified consuming effect");
}
```

## Shape

Put rotation in the existing `myko_authority::certified` domain. Do not add a
parallel ACL, epoch table, or store. The chain value becomes one enum:

```rust
pub enum AuthorityTransition {
    SelectRecords {
        operation: AuthorityOperationId,
        records: AuthoritySelection,
    },
    RotateControllers {
        operation: AuthorityOperationId,
        records: AuthoritySelection,
        successor: AuthorityControllerSet,
    },
}
```

`AuthoritySelection` keeps the current selected-record rules: full immutable
origin, recorded time, event body, same-realm lifecycle support, committed facts,
and Myko causal replay. Rotation does not get a shortcut. Before a successor
context exists, the node must retain and validate the full selected authority
history through the rotation head. A rotation certificate without the selected
records is only evidence that some bytes were chosen.

The successor epoch ID is derived, not supplied:

```rust
pub struct AuthorityControllerSet {
    controllers: Vec<AuthorityControllerBinding>,
}

pub struct AuthorityControllerBinding {
    principal: Principal,
    node_id: NodeId,
    controller: ControllerId,
    store: StorageIncarnationId,
}

pub struct CertifiedAuthorityEpoch {
    id: ControlEpochId,
    predecessor: ControlHead,
    controllers: AuthorityControllerSet,
}

impl CertifiedAuthorityEpoch {
    pub fn derive(
        realm: &AuthorityRealmKey,
        predecessor: ControlHead,
        controllers: AuthorityControllerSet,
    ) -> Result<Self, String>;
}
```

The derived bytes include realm, predecessor head, sorted controller bindings,
and a versioned domain string. This prevents reusing a friendly epoch ID with a
different controller set. It also binds principal, node, controller key, and
store incarnation in one place. Disjoint successors work because their authority
comes from the predecessor epoch's chosen transition, not from their own claim.

`AuthorityHistory` should expose one deep API:

```rust
impl AuthorityHistory {
    pub fn context_at(&self, head: ControlHead) -> Result<AuthorityContext, String>;
}

pub struct AuthorityContext {
    realm: AuthorityRealmKey,
    predecessor: ControlHead,
    epoch: CertifiedAuthorityEpoch,
}

impl AuthorityContext {
    pub fn select_records(
        &self,
        operation: AuthorityOperationId,
        records: &[EventEnvelope],
    ) -> Result<AuthorityTransition, String>;

    pub fn rotate(
        &self,
        operation: AuthorityOperationId,
        records: &[EventEnvelope],
        successor: AuthorityControllerSet,
    ) -> Result<AuthorityTransition, String>;

    pub fn verifier(&self) -> Result<ControlQuorumVerifier, String>;

    pub fn prepare_request(
        &self,
        ballot: ControlBallot,
    ) -> Result<ControlVoteRequest<'_>, String>;

    pub fn accept_request(
        &self,
        proposal: &SignedControlProposal,
    ) -> Result<ControlVoteRequest<'_>, String>;

    pub fn verify_chosen(
        &self,
        proposal: &SignedControlProposal,
        accepts: &[SignedControlVote],
    ) -> Result<ControlHead, String>;
}
```

`context_at` walks from genesis to the requested head, applying transitions in
certified order. `SelectRecords` keeps the same epoch. `RotateControllers`
changes the epoch only for children of the rotation head. For every transition it
enforces chain-wide operation ID uniqueness and selected event identity rules.
If two chosen successors exist for one predecessor, the requested branch fails.
If a losing rotation has no accept majority, it is inert.

Stale epoch evidence is rejected at certification. Suppose old epoch A chooses a
rotation at head H into epoch B. A later proposal for predecessor H under epoch A
must not certify, even if generic nodes durably signed it. `context_at(H)` returns
epoch B, and `AuthorityContext::accept_request` builds a verifier for epoch B and
predecessor H. Old-epoch records remain inert history. Lower-ballot checks still
belong to the durable voter state machine; epoch correctness belongs to the
certified authority context.

## Framework-owned issuance boundary

Certification and issuance are different gates. `Node::vote_control` currently
accepts any `ControlVoteRequest` made from any external verifier. That generic
API is useful for the lower protocol, but it cannot know whether the verifier was
derived from a certified authority context. So this design adds a framework-owned
authority issuance path and treats raw generic votes as untrusted input:

```rust
pub struct AuthorityControllerSession<'a> {
    node: &'a Node,
    context: AuthorityContext,
}

impl AuthorityControllerSession<'_> {
    pub fn vote_prepare(
        &self,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String>;

    pub fn propose(
        &self,
        prepared: PreparedControlQuorum<'_>,
        transition: &AuthorityTransition,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String>;

    pub fn vote_accept(
        &self,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String>;
}
```

This wrapper is the API used by authority coordinators. It checks that the key is
bound to the context's epoch and store binding before calling `Node`. It does not
make raw `Node::vote_control` impossible. Tests should prove raw stale records can
be appended but cannot certify a post-rotation authority head.

## Module map

`myko_authority::certified::history` owns chain replay, transition decoding,
`context_at`, and chain-wide operation and selected-record uniqueness.

`myko_authority::certified::rotation` should own `AuthorityControllerSet`,
`AuthorityControllerBinding`, and `CertifiedAuthorityEpoch::derive`. It is pure
domain code.

`myko_authority::certified::session` should own the framework-issued controller
wrapper around `Node::vote_control` and `Node::propose_control`.

`myko_federation::control_quorum` stays generic. It should not learn
AuthorityService semantics or epoch rotation rules.

`myko_redb` stays a journal backend. It stores the same control and application
records, with no authority database.

## Rationale

The smallest deep interface is `AuthorityHistory::context_at(head)`. It hides
anchor traversal, rotation application, selected history checks, stale epoch
fencing, and verifier construction. Callers extend a certified context; they do
not assemble controller lists from retained records.

Putting rotation beside selected history is better than bolting flags onto the
control quorum layer. Control quorum answers one question: did this electorate
choose this value for this slot? Authority certification answers another: which
electorate is valid for the next slot? Mixing those would either trust imported
controller lists or make generic Paxos code depend on AuthorityService.

Historical-only stays explicit. A context at H says, "under the certified chain,
this was the verifier after H." It does not say H is current, the old stores left
safely, or a custodian acknowledged retained history. That restraint is annoying,
but it keeps stale revocations from turning into permits.

## Alternatives considered

Caller-supplied successor epoch IDs lost. It is easy to implement, but it lets
two different controller sets reuse one friendly ID. Content-derived epoch IDs
make that illegal by construction.

Embedding rotation in `control_quorum` lost. It would hide less from the caller,
not more, because authority code would still need to validate selected history
and current epoch. The generic quorum module would inherit domain rules it cannot
verify.

Treating raw durable votes as authority-issued lost. It matches today's low-level
API, but it trusts the wrong boundary. Raw votes are evidence to certify or
reject, not proof that the authority subsystem asked for them.

## Open questions and risks

- What non-forgeable type should represent `AuthorityOperationId` so selection
  and rotation share one chain-wide identity namespace without tying it to a
  retrying command's changing predecessor?
- Should store incarnation checks in `AuthorityControllerSession` be hard
  failures at issuance time, certification time, or both?
- How should successor contexts report "selected history missing" differently
  from "rotation lost" so repair code can fetch records without retrying a dead
  ballot?
- What protocol later proves a head is current rather than merely historical?

## Next implementation step

Implement `AuthorityTransition` and `AuthorityHistory::context_at`, then add a
Redb test where old controllers choose a disjoint successor set, close old
stores, reopen a successor, certify a later selected record under the successor
epoch, and reject old-epoch certification at the post-rotation predecessor.
