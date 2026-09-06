# Certified consumption candidate B: quorum read barrier plus reservation

## Problem

Bounded authority consumption needs a certified current-enough decision, not just a certified historical grant. A one-use grant must not authorize two distinct effects through competing controller sets, and a retry after a lost reply, restart, or rotation must recover the same reservation. Current local `AuthorityPolicy::evaluate` can decide a durable path, but `EvaluateAuthority` emits random use, challenge, lease, and audit IDs before the outer effect commits. Current `AuthorityHistory` proves historical selected authority facts and controller rotation, but it deliberately does not prove live currentness or spend shared bounded capacity. The awkward part is the gap: a caller needs to know whether a predecessor head is still safe to decide under, and if not, recover the certified successor before proposing any consumption.

## Usage

Effect admission first asks for a certified consumption reservation. The request identity is stable across retry and does not include the predecessor, controller set, wall clock, or connection.

```rust
let prepared = context.prepare_pending_effect()?;
let intent = CertifiedConsumptionIntent::from_prepared_effect(&prepared)?;

let barrier = controller.read_barrier(
    &anchor,
    intent.realm_id(),
    intent.request_id(),
    observed_head,
)?;

match controller.reserve_consumption(&anchor, barrier, intent, &signing_key)? {
    CertifiedConsumption::Permit(reservation) => {
        verify_live_authorization(&reservation, &binding)?;
        execute_effect_once(reservation.effect_identity())?;
        controller.record_effect_result(reservation, effect_result_digest)?;
    }
    CertifiedConsumption::Challenge(challenge) => return Err(challenge.into()),
    CertifiedConsumption::Deny(report) => return Err(report.into()),
}
```

A retry uses the same `RequestIdentity` and `AuthorizationBinding`. If a value was already accepted for that identity, the controller returns that chosen result. If the retry changes the binding, phase, requested lease, deterministic challenge identity, or deterministic lease identity, it is a mismatched reuse and fails.

```rust
let recovered = controller.reserve_consumption(&anchor, barrier, same_intent, &signing_key)?;
assert_eq!(recovered.request_id(), command_id.into());
assert_eq!(recovered.binding_digest(), original_binding_digest);
```

If an old controller tries to decide after a rotation or revocation was already chosen, `read_barrier` must discover the predecessor no longer remains the certified head for this request path. The caller receives `CertifiedConsumptionError::StaleHead { successor }`, imports or retains the missing control history, and retries against the successor. It does not receive a fake `LatestKnown` permit.

`CommandContext::commit_bytes` needs one extra stop before authorization. Today `prepare_batch` allocates a fresh `BatchId`, records `updated_at`, captures local causal predecessors, and `commit_bytes` hashes the whole batch plus actual claims, capability uses, and result into `effect_digest`. If a process consumes a certified one-use permit and crashes before `node.commit`, a handler retry can build a different batch and digest. The migration is therefore: prepare and durably park the exact pending effect first, derive the consumption intent from that parked batch digest, then resume the same parked batch after the certified reservation is recovered. Do not drop causal parents or batch binding to make retry easier.

## Shape

The barrier design separates “I have read a certified predecessor and there is no earlier chosen reservation for this intent under the known chain” from “this request may consume authority.” The second fact is only true after a reservation value is chosen by the certified controllers for the barrier head.

```rust
pub struct RequestIdentity(CommandId);

pub struct CertifiedConsumptionIntent {
    request_id: RequestIdentity,
    realm: AuthorityRealmKey,
    phase: AuthorizationPhase,
    prepared: PreparedEffectBinding,
    requested_lease: Option<DeterministicLeaseRequest>,
    challenge_identity: Option<ChallengeIdentity>,
}

pub struct PreparedEffectBinding {
    command_id: CommandId,
    batch_id: BatchId,
    binding: AuthorizationBinding,
    effect_digest: EffectDigest,
    actual_claims_digest: ClaimsDigest,
    capability_digest: CapabilityDigest,
}

pub struct CertifiedReadBarrier {
    realm: AuthorityRealmKey,
    request_id: RequestIdentity,
    predecessor: ControlHead,
    observed_chain_digest: ChainDigest,
    selected_authority_digest: AuthorityStateDigest,
    prior_decision: Option<ControlHead>,
}

pub enum ConsumptionTransition {
    Reservation(ConsumptionReservation),
    RecoveryAlias {
        request_id: RequestIdentity,
        chosen: ControlHead,
    },
}

pub enum ConsumptionReservation {
    Permit(CertifiedPermitReservation),
    Challenge(CertifiedChallengeReservation),
    Deny(CertifiedDenyReservation),
}

pub struct CertifiedPermitReservation {
    request_id: RequestIdentity,
    phase: AuthorizationPhase,
    binding_digest: BindingDigest,
    evaluated_at: EvaluationInstant,
    grant_uses: Vec<DeterministicGrantUse>,
    delegation_uses: Vec<DeterministicDelegationUse>,
    approval_uses: Vec<DeterministicApprovalUse>,
    lease: Option<DeterministicLeaseRecord>,
    audit: DeterministicDecisionAudit,
    effect_identity: EffectIdentity,
}
```

Module map:

- `authority/src/certified/consumption.rs`: owns request identity, barrier, reservation payload codec, deterministic use IDs, and mismatch validation.
- `authority/src/certified/controller.rs` or existing `issuer.rs`: adds `read_barrier`, `reserve_consumption`, and recovery helpers over `AuthorityHistory` plus `ControlQuorumVerifier`.
- `authority/src/evaluator.rs`: gains a pure deterministic evaluation mode that accepts an `EvaluationSeed` and returns planned records instead of random IDs.
- `authority/src/policy.rs`: calls certified consumption only for phases that can spend bounded authority, approvals, challenges, or leases. Ordinary convergent writes still use local-first command flow.
- `core/src/server/context.rs` and the command resume path: prepare, persist, and resume the exact batch/effect bytes before certified consumption rather than rerunning the handler to create a new digest.
- `federation/src/control_chain.rs`: remains generic. It certifies control transitions and rotations, but does not know authority semantics.

`read_barrier` is not a cache freshness oracle. It validates retained certified history to an exact predecessor, scans already chosen consumption transitions for the stable request identity, and returns either the prior chosen decision or a barrier tied to that predecessor. `reserve_consumption` then proposes one `ConsumptionTransition::Reservation` whose value includes the barrier digest and the full deterministic evaluation result. Paxos recovery rules force later retries with the same request identity to adopt the already accepted value. If the adopted value’s request identity matches but its parked effect digest, binding digest, actual claims, capability uses, or phase differ, the caller gets a mismatch error and must not execute.

Why not directly choose the request as a normal control transition? Because the request alone cannot prove the read basis. A stale controller could sign a request against a predecessor that omitted a previously chosen revocation, rotation, or consumption. The barrier puts the read set into the value being chosen: predecessor head, selected authority digest, and request identity. The chosen reservation is therefore replayable and comparable without pretending to know the live latest head.

Deterministic identities replace random evaluator output:

```rust
pub struct EvaluationSeed {
    request_id: RequestIdentity,
    transition_operation: CommandId,
    phase: AuthorizationPhase,
}

impl EvaluationSeed {
    pub fn grant_use_id(&self, grant: &AuthorityGrantId) -> GrantUseId;
    pub fn challenge_id(&self, obligation: &ObligationId) -> ChallengeId;
    pub fn lease_id(&self) -> LeaseId;
    pub fn decision_id(&self) -> DecisionAuditId;
}
```

Challenge and lease clocks are explicit. The reservation records the evaluator’s `evaluated_at` from a validated local time source used for policy expiry, and the chosen value includes the resulting expiry. A client-supplied time is never accepted as authority. If the deployment cannot make a clock claim strong enough for offline leases, the reservation may certify a challenge or deny, but must not mint an offline lease by assumption.

## Synthesis decision

This is candidate B. The base is a fresh read barrier plus reservation because it keeps currentness and consumption separate while still using the existing signed control journal. It deliberately rejects “directly choose the request” as too shallow: that hides the read-basis problem and leaves callers to guess whether the predecessor was stale. It also rejects a second grant-use database because the accepted Myko log remains the source of truth.

## Tradeoffs accepted

- We accept an extra control decision for bounded consumption in exchange for preventing two controllers from spending the same one-use allowance.
- We accept explicit retry identity management in exchange for deterministic recovery across restart, lost replies, and rotation.
- We accept that a barrier can become stale before reservation in exchange for not inventing latest-currentness.
- We accept that liveness can stop under partition or disjoint epochs until enough certified history is retained in exchange for avoiding unsound lease assumptions.
- We accept deterministic challenge and lease IDs in exchange for replayable chosen outcomes.
- We accept that effect execution is still not exactly-once in exchange for a clear boundary: the reservation authorizes one effect identity, while the effect adapter prevents nonreplay or compensates externally.

## Alternatives considered

- Direct request-as-transition: smaller interface, but callers still need to know whether the request was evaluated against the right predecessor. It hides little policy and leaks currentness into every caller.
- Precomputed use-record transition only: seems compatible with current facts, but it certifies output records without certifying the read barrier that made them safe. It also does not handle accepted-but-unknown recovery cleanly.
- Lease-based currentness: attractive for liveness, unsafe for partitions and clocks. Expiry can fence future decisions under a known rule; it cannot prove old unseen accepted decisions do not exist.
- Second consumption ledger: easier to query, but it creates another source of truth beside the accepted Myko log and duplicates authority history rules.

## Open questions and risks

- What is the minimum clock contract allowed for offline leases, and should the first implementation deny offline leases until that contract exists?
- Should `RequestIdentity` be supplied by command admission for command effects and by an explicit nonce for query/read admissions?
- How should physical effect adapters persist `effect_identity` and result digest so a recovered reservation cannot replay a non-idempotent external action?
- What retention rule makes old controllers reliably discover a previously chosen rotation or revocation before issuing a stale reservation?
- How much authority state digest detail is necessary for useful diagnostics without leaking selected fact bodies into generic control logs?

## Next implementation step

Add `CertifiedConsumptionIntent`, `CertifiedReadBarrier`, and deterministic evaluation output types, then write durable tests for two controllers racing to reserve one max-uses-1 grant and for retry recovery after a lost accepted value.
