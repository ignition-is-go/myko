# Certified consumption candidate A

## Problem

Myko can now prove historical authority records through `AuthorityHistory` and can rotate controller sets through the generic certified control chain. That proves what authority facts existed at an exact head. It does not yet spend a bounded grant, approval, challenge, or lease for a live caller. The hard part is the live boundary: a controller must not hand out two permits for one remaining use, and a node must not treat an old historical permit as a reusable access credential. The existing local path in `AuthorityPolicy::evaluate` records `GrantUse`, `DelegationUse`, `ApprovalUse`, `LeaseRecord`, `ChallengeRecord`, and `DecisionAudit` through a trusted local `EvaluateAuthority` command, but that command uses fresh random IDs and local authority state. The certified path must reuse the evaluator and item records, but choose the request-specific decision as one opaque transition in the existing certified control chain.

## Usage

An application command does not ask authority after it has already consumed authority. It first freezes the exact effect body, then asks certified authority to choose the authorization transition for that body.

```rust
let prepared = context.prepare_effect(result)?;
let certificate = authority.authorize_effect(
    CertifiedHead::after(local_certified_head),
    prepared.intent(),
    AuthorityClock::system(),
)?;
prepared.commit_with_certificate(certificate)?;
```

`prepare_effect` is a framework operation, not an application command. It stores the `ChangeBatch`, result bytes, actual resource claims, actual capabilities, prospective topology proof, and the command snapshot that produced `command.updated_at`. Retry reloads this stored body. It must not call `BatchId::new()` again.

Reads and other non-command admissions use an explicit request identity. A caller that wants a consuming read supplies or obtains a stable `AccessRequestId`; the node serves the read only after it verifies a certificate for the same request binding.

```rust
let intent = CertifiedAccessIntent::read(
    AccessRequestId::new("client-stable-read-17"),
    access_attempt,
    topology.proof_for(&requested_scopes),
)?;
let certificate = authority.authorize_access(head, intent, AuthorityClock::system())?;
node.query_items_with_authority(query, &certificate)?;
```

Controllers use the existing durable prepare, propose, and accept calls. The authority wrapper builds one transition value and lets Paxos decide the winner for the predecessor.

```rust
let history = AuthorityHistory::replay(&node, anchor)?;
let context = history.context_at(predecessor)?;
let intent = CertifiedAccessIntent::effect(prepared_effect)?;
let transition = CertifiedConsumption::evaluate(history, predecessor, intent, clock.now())?;
let value = transition.control_value()?;

let promises = controller.prepare(predecessor, ballot, key)?;
let proposal = controller.propose(predecessor, ballot, &promises, &value, key)?;
let vote = controller.accept(predecessor, &proposal, key)?;
```

The same chosen operation is the linearization point. If two max-uses-one intents race at the same predecessor, only one transition can be chosen. The loser recovers the chosen value, then retries after that head. The retry sees the first use in selected authority history and denies or challenges according to the normal evaluator.

## Shape

Add one authority-owned transition payload. Keep the generic chain opaque.

```rust
pub struct CertifiedAccessIntent {
    id: AuthorityIntentId,
    realm: AuthorityRealmKey,
    request: AccessAttempt,
    phase: CertifiedAuthorizationPhase,
    topology_proof: ScopeTopology,
    prepared_effect: Option<PreparedEffectBody>,
}

pub struct PreparedEffectBody {
    command_id: CommandId,
    command_updated_at: EventId,
    batch: ChangeBatch,
    result: Vec<u8>,
    actual_claims: Vec<ResourceClaim>,
    actual_capabilities: Vec<ApplicationCapability>,
    effect_digest: String,
}

pub struct CertifiedConsumption {
    operation: CommandId,
    intent: CertifiedAccessIntent,
    evaluated_at: DateTime<Utc>,
    outcome: CertifiedAuthorityOutcome,
}

pub struct CertifiedAuthorityCertificate {
    head: ControlHead,
    operation: CommandId,
    intent_id: AuthorityIntentId,
    outcome_id: AuthorityOutcomeId,
    decision: AuthorizationDecision,
}
```

`AuthorityIntentId` is stable across retries and rotations. For command effects, it hashes the realm, the access binding, the authorization phase, and the serialized `PreparedEffectBody`. It does not hash the retry predecessor, the current controller set, the selected grants, or the certified head. For reads, it hashes the explicit `AccessRequestId`, the realm, the binding, and the topology proof. A read without that identity cannot consume bounded authority.

`PreparedEffectBody` is the answer to the current `commit_bytes` problem. Today, `prepare_batch` allocates a fresh `BatchId`, includes `command.updated_at`, and adds current local causal predecessors. Then `commit_bytes` hashes the whole batch, claims, capabilities, and result into `effect_digest`. If a node crashes after a certified permit but before `node.commit`, rerunning the handler creates a different batch and digest. The new path stores the exact prepared effect before certified authorization. `commit_with_certificate` commits only that stored body, and an exact retry either recovers the committed command or uses the same pending body.

`CertifiedConsumption::evaluate` owns the authority decision. It takes `AuthorityHistory`, a predecessor head, the intent, and a controller-supplied evaluation time. It calls `AuthorityHistory::selected_at(predecessor)`, projects the same `EvaluationState` used by `assess_at`, merges the intent topology proof, and calls the existing `evaluate`. It replaces random IDs with deterministic IDs derived from `AuthorityOutcomeId`.

```rust
impl CertifiedConsumption {
    pub fn evaluate(
        history: &AuthorityHistory,
        predecessor: ControlHead,
        intent: CertifiedAccessIntent,
        clock: AuthorityDecisionTime,
    ) -> Result<Self, String>;

    pub fn control_value(&self) -> Result<ControlValue, String>;
    pub fn from_transition(transition: &ControlTransition) -> Result<Self, String>;
}
```

`AuthorityOutcomeId` hashes the intent ID, predecessor head, operation ID, evaluated time, and canonical evaluator result before record IDs are assigned. Record IDs then derive from that outcome:

- `DecisionAuditId = decision/{outcome_id}`
- `GrantUseId = grant-use/{outcome_id}/{grant_id}`
- `DelegationUseId = delegation-use/{outcome_id}/{delegation_id}`
- `ApprovalUseId = approval-use/{outcome_id}/{approval_id}`
- `ChallengeId = challenge/{outcome_id}/{obligation_id}`
- `LeaseId = lease/{outcome_id}`

These IDs are stable for the chosen outcome. A later retry after rotation finds the same intent in `AuthorityHistory::transitions_to(head)` and returns the existing certificate instead of choosing a second consumption transition. A mismatched request that reuses the intent ID fails because the payload hashes the full binding and prepared body.

The chosen transition does not directly mutate a second database. Its payload contains the canonical authority records that would have been emitted by `EvaluateAuthority`: use records, a challenge record, a lease record, and one decision audit. `AuthoritySelection` then retains those ordinary `AuthorityService` command events in the next chain step, the same way current historical authority selection works. The selected facts remain the only projected authority state.

That gives two steps:

1. Choose `CertifiedConsumption` as the exclusive decision for the request.
2. Commit the corresponding deterministic authority records as ordinary `AuthorityService` events and retain them with `AuthoritySelection`.

The decision certificate is usable after step 1 for the exact prepared effect, but the next unrelated consuming authorization must evaluate from a head that includes step 2. If step 2 is missing, `CertifiedConsumption::evaluate` refuses to sign another consuming transition. Controllers may only recover the previous consumption, complete its deterministic authority-record retention, or sign non-consuming transitions. This keeps max-uses-one grants from being spent twice during the gap between the chosen consumption and the selected records that make that consumption visible to the evaluator.

## Time semantics

The caller does not choose `evaluated_at`. The proposer supplies an `AuthorityDecisionTime`, and each controller signs only when the time is within its local `ClockWindow` and is not earlier than the durable prepared-effect timestamp. This is a crash-fault rule, not a global clock proof. Grant expiry and approval expiry are evaluated at `evaluated_at`. Revocations and grant changes are evaluated by selected authority history at the predecessor head. If a revocation is chosen before the request's first consumption decision, the decision sees the revocation and denies. If a permit was already chosen before the revocation, the certificate remains valid only for its exact prepared body.

Leases use the same deterministic outcome IDs. The lease interval starts at `evaluated_at`. A node verifies the certificate again before committing or serving a read and rejects an expired lease at use time.

## Live enforcement

`AuthorityPolicy::decide` remains the local fast path for non-consuming decisions. When `requires_durable_evaluation` is false, ordinary convergent application writes do not enter quorum. When the evaluator reports a consuming permit, challenge, or lease, the node moves through a certified path.

`CommandContext::commit_bytes` must change in this order:

1. Build the batch, claims, capabilities, topology proof, result, and digest once.
2. Persist `CommandState::AuthorizationPrepared { intent_id, body }`.
3. Ask certified authority to choose or recover `CertifiedConsumption`.
4. Verify that the returned certificate matches the stored body.
5. Commit the stored batch with `node.commit`.

`resume_authorization` and `advance_authorization` use the same stored body. They do not rerun the application handler, do not recompute `effect_digest`, and do not allocate a new batch ID.

Read enforcement needs a parallel method:

```rust
pub fn decide_certified(
    &self,
    head: ControlHead,
    intent: CertifiedAccessIntent,
) -> Result<CertifiedAuthorityCertificate, AuthorityError>;

pub fn verify_certificate(
    &self,
    head: ControlHead,
    certificate: &CertifiedAuthorityCertificate,
    intent: &CertifiedAccessIntent,
) -> Result<AuthorizationDecision, AuthorityError>;
```

The verifier checks the generic chain proof, decodes the consumption payload, checks the intent hash, checks deterministic record IDs, and rejects stale epochs through `AuthorityHistory::context_at`. It does not replay a physical command effect. Command commit verifies the certificate against the already stored body and commits that body once.

## Module map

- `authority::certified::consumption` owns `CertifiedAccessIntent`, `PreparedEffectBody`, `CertifiedConsumption`, deterministic IDs, and payload codecs.
- `authority::certified::issuer` adds `decide`, `recover`, and `verify_certificate` on top of `AuthorityController`.
- `authority::certified::history` exposes transition lookup by `AuthorityIntentId` and keeps selected-record projection rules.
- `authority::commands` reuses `EvaluateAuthority` logic through a shared deterministic record builder. It stops owning random IDs for certified consumption.
- `federation::node` owns prepared-effect command state and exact-body commit or resume.

## Synthesis decision

This candidate chooses request-specific certified transitions. The design has a smaller live interface than a read-barrier plus reservation protocol because the existing control chain already serializes one successor per predecessor. It also keeps the authority state in existing `AuthorityService` records. The costly part is moving `commit_bytes` to persist a prepared effect before authority. That cost is necessary because the current batch and digest are not stable across retry.

## Tradeoffs accepted

- We accept a new prepared-effect command state in exchange for exact retry recovery after a crash.
- We accept controller clock windows in exchange for keeping expiry checks deterministic enough for crash-fault control.
- We accept a two-step consumption-plus-retention flow in exchange for keeping `AuthorityService` records as the only projected authority facts.
- We accept that a certificate authorizes only one exact prepared body in exchange for preventing old permits from becoming live credentials.

## Alternatives considered

- A latest-head read barrier plus local `EvaluateAuthority` lost because it makes freshness a read-side claim. It cannot stop two controllers from spending one remaining use unless it adds a second reservation system.
- Precomputing `GrantUse` records in the proposal lost because selected records are facts, not authority to spend. It also fails when the prepared command effect changes after a crash.
- Reusing `AuthorizationDecision::Challenge` as a fake certified-authority command lost because it exposes internal authority coordination as an application challenge and blurs obligation approval with quorum consumption.

## First tests

The first durable test should use two Redb controller nodes and a max-uses-one grant. Prepare two different `PreparedEffectBody` values at the same predecessor. Drive both through real `AuthorityController` prepare, propose, and accept calls. Verify that only one consumption transition becomes chosen, that the losing intent recovers the chosen value instead of receiving a permit, and that retrying the loser at the chosen head denies after the deterministic `GrantUse` is retained.

The next tests should cover crash after chosen consumption before command commit, retry after controller rotation, revocation chosen before the first request, and mismatched reuse of an `AuthorityIntentId` with a different prepared body.

## Open questions and risks

- What `ClockWindow` is acceptable for grant expiry and lease issuance in a crash-fault deployment?
- Which non-consuming transitions, if any, may proceed while a prior consumption waits for deterministic authority-record retention?
- Where should read certificates be cached so repeated delivery of the same read response does not consume authority again?

## Next implementation step

Add `PreparedEffectBody` and `CommandState::AuthorizationPrepared`, then change `CommandContext::commit_bytes` to persist the exact body before any certified consuming authorization starts.
