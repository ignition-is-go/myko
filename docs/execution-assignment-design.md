# Execution assignments and durable control

This is an implementation design for one dependency of subscription failover,
not a claim that routing or the application-builder milestone is complete.

## Usage

Framework control code proposes replacement executor sets. A history consumer
reads those sets at an explicit certified head:

```rust
let proposal = ExecutionAssignment::new(
	operation, realm, scope.clone(), service.clone(), [node_a, node_b],
);
let transition = proposal.transition()?;
// The existing control protocol chooses and retains the transition.
let assignments = ExecutionAssignmentsAtHead::replay(&chain, head)?;
let configured = assignments.exact(&scope, &service);
```

An empty executor set records removal of all explicit executors. No entry means
no assignment was recorded at that head. Neither result infers assignment from
discovery, service advertisement, replication configuration, or storage custody.

## Problem and existing flow

The node router currently selects a replication-enabled peer advertising the
service. That establishes neither a scope execution assignment nor readiness.
Assignments belong to Myko control state, separate from application data.

`ControlTransition` already carries an opaque payload and operation identity.
The existing quorum protocol signs and chooses its encoded value.
`CertifiedControlChain::replay` validates retained evidence under an independently
provisioned anchor. `transitions_to(head)` returns chosen transitions in order.
The new projection interprets assignment payloads from that sequence. It creates
no separate log, quorum protocol, persistence engine, or transport dependency.

## Shape

The federation crate owns the proposal and historical projection in
`execution_assignment.rs`:

```rust
struct ExecutionAssignment { /* private validated payload */ }
impl ExecutionAssignment {
	fn new(operation: CommandId, realm: ScopeId, scope: ScopeId,
		service: ServiceId, executors: impl IntoIterator<Item = NodeId>) -> Self;
	fn transition(&self) -> Result<ControlTransition, String>;
}

struct ExecutionAssignmentsAtHead { /* private realm, head, exact entries */ }
impl ExecutionAssignmentsAtHead {
	fn replay(chain: &CertifiedControlChain, head: ControlHead) -> Result<Self, String>;
	fn realm(&self) -> &ScopeId;
	fn head(&self) -> ControlHead;
	fn exact(&self, scope: &ScopeId, service: &ServiceId) -> Option<&BTreeSet<NodeId>>;
}
```

The private wire representation binds realm, operation, scope, service, and the
complete sorted executor set. Constructor input is normalized as a set.
Canonical decoding rejects unknown fields, duplicate wire executors,
unsupported assignment versions, and operation or realm
mismatches. Assignment payloads use retain transitions, not controller rotations.
Other payload domains remain available to their existing consumers.

Repeated evidence has no additional effect. The existing control chain rejects
reuse of an operation ID in a subsequent transition. A retry recovers the
original operation's evidence instead of choosing it a second time. Repeating
old evidence after a later replacement must not restore the old executor set.

The historical projection has no method for selecting a ready route or producing
an authorization permit. Per type-system discipline, those guarantees must not
be represented by a historical assignment type. The private payload and replay
logic keep wire validation out of routing callers.

## Synthesis decision

The earlier bounded reviewer and the main agent compared two shapes. The chosen
shape uses typed payloads in the existing certified control chain. Generated
framework items in the application log would make ordinary application mutation
and application schema registration part of assignment control. That is the
wrong boundary for the agreed design.

The reviewer suggested a schema marker if compatibility evidence was unavailable.
That suggestion is rejected. An assignment must not manufacture compatibility
evidence. Compatibility remains a separate routing requirement.

## Limits and open questions

The projection keeps historical configuration distinct from live authority.
The durable controller described below records votes, but neither API establishes
operator permission, scope existence, storage placement, or currentness.

Which current control evidence and operator grant authorize assignment changes?
How does the runtime bind assigned executors to compatible service generations
and per-scope readiness? These remain integration requirements.

Nested scopes inherit durable placement under the agreed contract. This exact
record projection does not decide whether compute assignments inherit, override,
or aggregate. What effective compute policy applies to nested scopes remains
explicitly open. `exact` is a historical lookup, not a final eligibility API.

## Implementation phases

- [x] Ground: inspect control replay, peer routing, and the agreed contract.
- [x] Sketch: compare application items with typed control payloads.
- [x] Agree: select control payloads within the agreed framework boundary.
- [x] Implement: canonical payload, exact-head replay, and signed-history tests.
- [x] Scrap check: reuse the chain's operation-identity rejection rather than
  duplicate it. No readiness or compatibility marker was added.

The implementation follows the sketch. The eight signed-history tests pass.
These tests serialize and replay framework records but do not prove a disk
write, live controller protocol, or routing behavior. The next integration must
bind this configuration to current control evidence and scope readiness before
using it to choose a serving node.

## Durable assignment controller

Framework controller code can now issue assignment evidence through a node's
existing journal without loading an application:

```rust
let controller = ExecutionAssignmentController::new(node, anchor);
let promise = controller.prepare(head, ballot, &key)?;
// The coordinator gathers a prepare majority and recovers any accepted value.
let proposal = controller.propose(head, ballot, &promises, &value, &key)?;
let accepted = controller.accept(head, &proposal, &key)?;
```

`execution_assignment/controller.rs` owns this local trusted API. Each operation
captures one local history snapshot, derives its certified electorate, and checks
assignment history at the requested head. Proposal and acceptance also validate
the canonical payload, realm, operation identity, and prohibition on assignment
payloads changing controller membership. The exact snapshot is bound to the
existing atomic voting boundary, which rejects a local history change before
issuing a signature. `Node::vote_control` and `Node::propose_control` own
persistence, ballot recovery, and duplicate response handling.

The design compared extending `AuthorityController` with an execution-specific
controller using those same recording APIs. The latter keeps placement out of
application authorization history. `AuthorityFactReplay::apply` intentionally
accepts authority payload families; changing it to interpret placements would mix
the two responsibilities. No authority files were changed for this controller.

Four Redb tests in `tests/execution_controller.rs` exercise actual journal writes:

- Reopen preserves the chosen assignment and exact proposal and acceptance
  responses. Retrying those responses appends nothing. Reusing a chosen operation
  after its successor head is rejected.
- One of three controllers cannot propose or establish an assignment. Two can
  recover an accepted value under a new proposer without replacing it with a
  different assignment.
- Foreign-realm, unsupported-domain, and malformed proposals are rejected before
  persistence. A valid controller signature does not bypass payload checks on
  acceptance.
- A volatile node cannot issue a promise.

This local controller does not allocate ballots, authenticate network callers,
authorize an operator's intent, distribute evidence, or attest currentness.
The Redb tests coordinate calls directly and transfer retained evidence explicitly.
They are not an automatic mesh protocol or a routing proof.

## Authenticated assignment transport

`myko::server::ExecutionControlEndpoint` wraps the local controller with explicit
principal-to-controller-key bindings. It checks the authenticated executor, exact
principal kind, direct provenance, and ballot proposer before prepare, propose,
or accept can reach the durable controller. Empty and duplicate caller bindings
are rejected at construction. Denials use `AdministerExecution`, not application
grant administration. An application scope grant cannot authorize this operation.

The endpoint uses existing `ControlPrepare`, `ControlPropose`, and `ControlAccept`
messages. No application handlers or new journal are involved. Installation is
explicit; opening a storage node does not install a voting endpoint.

Five tests in `local/src/tests/execution_control.rs` cover caller configuration,
socket denials before persistence, two-of-three voting with actual Redb journals,
refusal to vote without an installed endpoint, and rejection of application
scope grants as controller authority. The majority test reopens a
journal and retries acceptance through a new server, requiring the original
signed response and unchanged history. The denial cases exercise all three calls
with unknown identities, mismatched kinds, forged executors, delegated principals,
forwarding provenance, and wrong key bindings. All servers have no application
loaded and use a deny-all application access policy.

Transport now uses `set_control_endpoint(realm, endpoint)` and the generic
`ControlEndpoint` trait. Every call carries a required `ControlTarget` containing
the realm and predecessor head. Authority and execution controllers can share one
session and journal without treating each other's payloads as their own history.
Unknown realms have no fallback. See [control-realm routing](control-realm-routing.md)
for the design comparison and isolation tests. The socket tests explicitly copy
retained evidence; they do not prove remote evidence refresh or coordinator recovery.

Authenticated controller participation is not operator-intent authorization,
current assignment authority, executor readiness, schema compatibility, or a
routing permit. Those checks must be connected before assignments select a
serving node. Storage custody and application execution remain separate.

## Authenticated assignment evidence refresh

An execution controller can bind each registered caller to a
`ScopedRetainedEvidenceEndpoint`. The transport endpoint authenticates the caller
before refreshing its configured control realm. Prepare, propose, and accept
then use the existing durable controller over the refreshed local journal.
Unknown callers and duplicate source bindings are rejected during configuration.
Callers without a source binding use only locally retained evidence.

The existing `IrohScopedEvidenceEndpoint` supplies this transfer over native
transport. Its source independently checks history access. A configured source
failure stops the control request, even when the local journal could answer it.
Typed availability failures survive the boundary. Invalid transfer evidence maps
to `HistoryUnavailable`. Successfully imported records can remain after a failed
refresh, but no vote follows that failure.

The native tests in `iroh/tests/execution_evidence.rs` use two Redb journals and
no application modules. The proposer gathers a chosen assignment through actual
controller calls and scoped history pulls. The other controller initially lacks
the complete predecessor evidence. A later authenticated prepare refreshes that
evidence and advances without manual event copying. Forged callers cannot cause
an import, another realm remains excluded, and revoked history access blocks a
new vote until access is restored.

Refresh supplies retained evidence, not a current-head certificate, read barrier,
lease, or ready-executor permit. The coordinator below adds operation recovery;
currentness and routing remain separate requirements. Installing this controller role does
not make storage participation imply voting or typed application execution.

## Assignment coordinator design

Implementation phases for this unit:

- [x] Ground the authority coordinator, execution controller, and history transfer.
- [x] Compare coordinator shapes and select the boundary below.
- [x] Agree within the recorded contract; no new operator or routing guarantees.
- [x] Implement and exercise interrupted assignment recovery over native transport.
- [x] Check the implemented shape against the sketch and record verification.

The framework caller supplies one assignment operation and receives a historical
receipt:

```rust
let receipt = coordinator.assign(assignment.clone()).await?;
let recovered = coordinator.assign(assignment).await?;
assert_eq!(receipt.head(), recovered.head());
```

`ExecutionAssignmentCoordinator` lives beside the authenticated execution
endpoint in core's native server module. Construction binds an observer journal,
control anchor, direct caller, proposer key identity, and unique controller peers.
Each peer supplies the existing `ControlEndpoint` and can attach an authenticated
scoped evidence source into the observer. The assignment call hides ballot
selection, quorum collection, accepted-value recovery, and receipt reconstruction.
It does not authorize an operator's intent or return a ready route.

The existing authority flow refreshes history, recovers an operation, obtains
promises, selects any previously accepted value, proposes, collects accept votes,
and refreshes retained evidence again. Its planning and recovery bind application
authority requests. Assignment coordination instead validates the exact assignment
operation and encoded value. Both use `ControlQuorumVerifier`, durable controller
recording, and `CertifiedControlChain`. Neither uses an application handler.

Two shapes were compared locally. Extracting a generic driver from
`AuthorityDecisionCoordinator` would move its request-specific planning and
recovery across a new callback interface. An assignment coordinator over the
existing voter interfaces exposes fewer orchestration details and preserves the
dirty authority implementation. That is the selected shape. The shared protocol
and persistence remain in federation; this is not a second quorum algorithm.

One coordinator serializes its own assignment calls. Persisted voter rules remain
the safety boundary across concurrent coordinators and restarts. Each round
derives its ballot from retained signed member votes at the exact slot. A fresh
prepare quorum can require recovery of another operation before the requested
operation advances. A repeated operation with different bytes fails explicitly.
An already chosen operation returns its original receipt without new votes,
including after a later assignment replaced it.

The returned head must be reconstructible from the observer's retained evidence.
A partial round can persist votes even when the call fails. An explicit retry
recovers those votes; there is no application mutation, automatic background
queue, new persistence engine, or implication of current routing authority.

Operator-intent authorization, current-head fencing, effective nested compute
policy, compatible execution, and readiness remain open integration questions.

The implementation matches this sketch. `ExecutionAssignmentReceipt` keeps the
assignment and chosen head private and exposes read-only accessors. Peer calls
and evidence pulls run concurrently with per-request timeouts. The coordinator
accepts unavailable peers only when the remaining votes still verify as a quorum.
An invalid evidence transfer or explicit controller denial fails the call.
Eight rounds bound recovery of other accepted operations. Calls do not become
background jobs when the retry limit or a quorum check fails.

Five tests in `iroh/tests/execution_coordination.rs` pass, as does strict Clippy
for core and Iroh libraries and tests. These tests use three actual Redb journals,
native transport, exact-realm history grants, and no application modules:

- Concurrent retries return the same receipt. Reopening the journals and stopping
  both remote controllers still permits receipt recovery without new records.
  Retrying an older operation leaves the newer executor set unchanged.
- A minority cannot choose. Restoring a second controller allows an explicit
  retry with a ballot above the interrupted prepare.
- After the proposer stops, two surviving controllers recover its accepted
  assignment before choosing the next assignment. Recovery uses scoped history
  transfer, not manual event copying.
- A reply quorum without retained remote evidence cannot return a receipt.
  Restoring evidence transfer recovers the original ballot's result.
- Missing or duplicate proposer configuration fails. A foreign-realm assignment
  fails without changing any controller's journal.

The interruption fixture deliberately issues only part of the existing protocol
before stopping its proposer. The coordinator, not fixture code, drives recovery.
This is framework assignment recovery, not application subscription failover.

## Fresh assignment observations

Phases for this unit:

- [x] Trace authority admission and continuation revalidation.
- [x] Compare fresh prepare evidence with a chosen observation record.
- [x] Select the existing fresh-operation pattern without lease semantics.
- [x] Implement the observation payload, controller checks, and coordinator call.
- [x] Verify stale observers, quorum loss, and accepted-assignment recovery.

The framework calls `coordinator.observe().await?` to obtain
`ExecutionAssignmentsAtHead` from a control operation chosen during that call.
Unlike `assign`, this call never satisfies a new invocation with a previously
chosen operation. The historical result type deliberately remains unchanged:
the observed head is not a reusable live permit or a readiness assertion.

The existing authority path uses `authorize_scoped_access`, then `revalidate`,
which creates a fresh operation identity and chooses its value through the
quorum. Stream continuation invokes another revalidation. Its result is not
cacheable permission. Assignment observation follows this fresh-operation
pattern without importing application grant evaluation into execution control.

Two alternatives were compared locally. A prepare-only barrier avoids another
chosen record, but persisted duplicate promises can answer reused ballots. The
existing prepare wire format has no fresh observation nonce, and the coordinator
does not provide a globally fresh ballot allocation guarantee. A new chosen
observation record instead uses the existing operation identity, ordering,
accepted-value recovery, and durable evidence rules. This is the selected shape.
It adds a control-log record per observation, not an application event.

`ExecutionAssignmentObservation` binds its operation, realm, and exact predecessor.
Execution controllers accept it only against that predecessor. It preserves
controller membership and executor sets. The coordinator first finishes any
previously accepted value that the prepare quorum requires, then chooses its own
new observation. Its returned assignments come from that chosen head, not a
later unverified local snapshot.

This establishes an observation point during the call. Another assignment may
change immediately afterward. Subscription routing and command admission still
need their own use-time checks, scope readiness, and compatible code. No lease,
ongoing execution fence, or atomicity across application and control logs is
introduced by this unit.

The two initial native regressions failed against retained-only reads: one reused
the previous head, and the stale observer missed its replacement executor set.
Both pass through `observe`. Four observation tests now cover fresh operation
identities, quorum loss without cached fallback, a stale observer obtaining the
replacement, recovery of an accepted assignment, and recovery of an old
observation before choosing the new call's observation. The complete nine-test
native coordination target passes.

The Redb controller test rejects foreign realms, wrong predecessors, mismatched
operations, controller rotations, noncanonical payloads, and unsupported versions
before proposal or acceptance recording. It then chooses a valid observation
and verifies that no executor set was manufactured. All five durable controller
tests pass. Observation records use the existing opaque control payload format;
the wire version and persistent envelope schema are unchanged.

The expanded baseline and three additional observation-suite runs passed. Strict
Clippy passed for core, federation, Redb, and Iroh libraries and tests. Targeted
formatting and diff checks passed. The implementation follows the selected
fresh-operation design and adds no cached readiness flag or live-permit type.
