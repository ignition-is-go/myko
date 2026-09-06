# Custody issuance candidate B

## Usage

The caller asks the holder to accept one frozen custody obligation. The holder does not pass a permit or a boolean from the caller into the storage layer. It derives the statement locally, asks the installed authority policy for a durable custody reservation, signs with its authenticated node key, verifies the local journal, and only then appends a framework control acknowledgment.

```rust
let snapshot = SelectedHistorySnapshot::current(receiver.node())?;
let manifest = snapshot.retained_manifest(&obligation.selection)?;

let receipt = custody.issue(CustodyIssuanceRequest {
    authenticated_executor: endpoint_principal_id(peer_descriptor.endpoint.id),
    presentation: AuthorityPresentation::direct(Principal::node(
        endpoint_principal_id(peer_descriptor.endpoint.id),
    )),
    trusted_holder: peer_descriptor,
    obligation_event: obligation_event_id,
    manifest,
    idempotency: CustodyIssuanceId::for_obligation_and_holder(
        obligation_event_id,
        receiver.node().node_id(),
        receiver.node().storage_incarnation()?.unwrap(),
    ),
})?;
```

Departure code consumes only the returned acknowledgment event:

```rust
handoff.note_acknowledgment(receipt.event.origin)?;
if handoff.required_acknowledgments_satisfied()? {
    handoff.complete_departure()?;
}
```

Verification re-derives all trusted inputs before reading the signed statement:

```rust
let expected = RetainedHistoryStatement::new(
    holder.node_id,
    holder.storage_incarnation,
    obligation_event,
    &required_manifest,
)?;
verify_retained_history_statement(&ack.signed, &holder.descriptor, &expected)?;
authority_policy.verify_custody_authorization(&ack.authorization, &expected_binding)?;
```

## Problem

The current code can record a `SignedRetainedHistoryStatement`, but the docs and source both say that value is only an assertion. `Node::record_retained_history_statement` checks local holder identity, store incarnation, selected commitment, and durable event bodies before appending. It does not interpret the obligation, authorize the signer, or prove membership. `AuthorityPolicy` evaluates local-origin authority facts only, and durable evaluation may consume one-shot grants before the outer custody append exists. Candidate B treats custody issuance as two durable facts with one idempotency key: a portable authority reservation in `AuthorityService`, then a framework control acknowledgment in the existing journal.

## Shape

Add transport-neutral custody types in `libs/myko/federation/src/custody.rs` and re-export them from `myko_federation`.

```rust
pub struct CustodyObligation {
    pub id: CustodyObligationId,
    pub realm_id: AuthorityRealmId,
    pub issuer: Principal,
    pub selection: ScopeSelection,
    pub required_history: RequiredHistoryManifest,
    pub eligible_holders: Vec<CustodyHolder>,
    pub surviving_copy_count: NonZeroUsize,
    pub authority_cut: PortableAuthorityCut,
    pub retired_incarnations_cut: RetiredIncarnationCut,
}

pub struct CustodyHolder {
    pub node_id: NodeId,
    pub principal_id: PrincipalId,
    pub signing_key: [u8; 32],
}

pub struct RequiredHistoryManifest {
    pub selection: ScopeSelection,
    pub events: Vec<EventEnvelope>,
}

pub struct CustodyAcknowledgment {
    pub signed: SignedRetainedHistoryStatement,
    pub authorization: CustodyAuthorizationRef,
}

pub struct CustodyAuthorizationRef {
    pub issuance_id: CustodyIssuanceId,
    pub decision_id: String,
    pub binding_digest: String,
    pub authority_cut: PortableAuthorityCut,
}

pub struct CustodyReservationRecord {
    pub id: CustodyIssuanceId,
    pub binding: AuthorizationBinding,
    pub binding_digest: String,
    pub decision_id: String,
    pub reserved_at: DateTime<Utc>,
}
```

Extend `FrameworkControlEvent` instead of adding another store:

```rust
pub enum FrameworkControlEvent {
    RetainedHistoryStatement(SignedRetainedHistoryStatement),
    CustodyObligation(CustodyObligation),
    CustodyAcknowledgment(CustodyAcknowledgment),
    StorageIncarnationRetired(StorageIncarnationRetired),
}
```

The old `RetainedHistoryStatement` variant remains inert compatibility evidence. New custody code emits `CustodyAcknowledgment`, which contains the same signed statement plus the authority reference that made issuance legal. Command consumers continue to skip framework controls.

The custody API belongs beside `Node`, not inside application commands:

```rust
pub struct CustodyIssuer<P, S> {
    node: Node,
    policy: Arc<P>,
    signer: S,
}

impl<P, S> CustodyIssuer<P, S>
where
    P: CustodyAuthorityPolicy,
    S: RetainedHistorySigner,
{
    pub fn issue(&self, request: CustodyIssuanceRequest)
        -> Result<IssuedCustodyReceipt, CustodyIssuanceError>;
}

pub trait CustodyAuthorityPolicy: AccessPolicy {
    fn reserve_custody_issuance(
        &self,
        attempt: CustodyAuthorizationAttempt,
    ) -> Result<CustodyAuthorizationRef, AuthorizationDecision>;

    fn verify_custody_authorization(
        &self,
        reference: &CustodyAuthorizationRef,
        binding: &AuthorizationBinding,
    ) -> Result<(), AuthorizationDecision>;
}
```

`AccessOperation` gains `AcceptCustody`, and `FederationPermission` gains `Custody`. `AccessTarget` gains:

```rust
Custody {
    obligation: EventId,
    holder: NodeId,
    storage_incarnation: StorageIncarnationId,
    selection: ScopeSelection,
    commitment: RetainedHistoryCommitment,
}
```

The resource claim uses `Custody`, not `ReadHistory` or `Write`. Ordinary commands stay local-first. Custody issuance is framework control work and never becomes a fake typed application command with an accidental `Write` claim.

## Obligation meaning

`CustodyObligation` is persisted as a framework control event before any acknowledgment can reference it. The `RetainedHistoryStatement.obligation` field continues to point at that event origin. Validation loads the exact event by origin from retained history and requires the event body to be `FrameworkControlEvent::CustodyObligation`. A generic `Obligation` in `AuthorityService` can require human approval, but it cannot define storage custody by itself.

The required history is exact and non-circular. `RequiredHistoryManifest.events` excludes the obligation event and every later acknowledgment. The validator receives the obligation event origin from the caller's retained history lookup and rejects any required set containing that origin. Later handoff completion can require a later obligation to retain earlier acknowledgments, but each obligation names a finite predecessor set.

`SelectedHistoryManifest` remains the way to build a local retained set. It already fixes one local cut, rejects pending relevant history, rejects atomic cross-scope leakage, and checks dependencies. The issuer compares the caller-supplied manifest to the obligation's required event bodies before signing. An origin maximum is never accepted as coverage.

## Portable authority

Do not change `AuthorityPolicy` from "local origin only" to "all imported facts." That would trust foreign grants merely because they arrived. Candidate B adds an authority source chain inside `AuthorityService`: `AuthoritySourceEpoch { realm_id, epoch, previous, sources, quorum, required_custody }`. The bootstrap realm starts at the local source. Changing sources is an ordinary authorized authority command requiring `Admin` over the realm and custody receipts for the authority realm history named by `required_custody`.

`AuthorityPolicy::new_portable(application, realm_id, locator)` opens fact sources only for the active epoch sources proven by that chain. It still evaluates `AuthorityGrant`, `AuthorityDelegation`, `Obligation`, uses, approvals, and leases through the existing evaluator. The locator is not an ACL. It is the verified answer to "which origins are authoritative for this realm at this cut?" If the chain is missing, conflicted, lacks required custody, or depends on a retired incarnation, the policy denies with `authority_projection_not_current`.

This lets authority survive founding nodes without making the founder permanent. It also avoids trusting imported grants blindly, because imported facts count only after the source epoch chain proves that their origin is currently authoritative.

## Module ownership

`myko-federation` owns the custody domain types, `FrameworkControlEvent` variants, `AccessOperation::AcceptCustody`, and `Node::record_custody_acknowledgment`. `myko-authority` owns `AuthoritySourceEpoch`, `CustodyReservationRecord`, and `AuthorityPolicy::new_portable`, because those records are authority facts and must use the existing evaluator, use counters, approvals, and revocations. `myko-iroh` owns only signing and descriptor verification. `myko-redb` changes only through the existing `EventJournal` contract and persistence tests.

## Identity and trust

Custody binds four identities that the current code keeps separate:

- `NodeId` identifies the Myko event origin and holder in the statement.
- `PrincipalId` identifies the authenticated executor used by `AccessAttempt`.
- The Ed25519 key signs `RetainedHistoryStatement::signing_bytes`.
- `StorageIncarnationId` identifies the durable store that retained the bytes.

For native Iroh, `NativeNodeDescriptor` binds `NodeId` to endpoint key, and `endpoint_principal_id(endpoint.id)` binds the authenticated stream to a `PrincipalId`. The issuer verifies that `CustodyHolder.node_id`, `principal_id`, and `signing_key` match those independently trusted values before any receipt field is read as evidence. Deserializing a signed wrapper, verifying a signature, or seeing a receipt field never establishes authority by itself.

Retired incarnations are control facts. `StorageIncarnationRetired` names `(node_id, storage_incarnation)` and the authority cut that retired it. Issuance rejects an incarnation retired at or before the authorization cut. Reopening a Redb file cannot prove freshness, and this design does not claim rollback detection without surviving retained control evidence.

## Authorization, crashes, and retries

Authorization takes effect when `reserve_custody_issuance` durably records a `DecisionAudit` and authority-use records for the exact `CustodyIssuanceId` and binding digest. Custody takes effect later, only when `Node::record_custody_acknowledgment` appends the framework control event.

If the process crashes before reservation, retry evaluates normally. If it crashes after reservation but before append, retry with the same `CustodyIssuanceId` returns the existing reservation even if a grant was one-shot and is now consumed. That reservation authorizes only the same obligation, holder, incarnation, key, selection, commitment, and required event count. A different digest must reauthorize and may be denied.

If append succeeds and the response is lost, retry finds the exact `CustodyAcknowledgment` in local history and returns it. If append fails, no receipt is released and the sender's obligation remains unresolved. Revocation after reservation blocks new reservations, but it does not rewrite the meaning of the exact pre-revocation reservation. Revocation before the reservation cut denies.

## Synthesis decision

Candidate B chooses the reservation-plus-append shape. It keeps interface depth high: callers ask for custody issuance once, while the callee hides obligation lookup, statement construction, signature binding, durable authority reservation, journal verification, append, and retry lookup. The rejected shapes made callers assemble too much authority context or smuggled custody into application commands.

## Tradeoffs accepted

- We accept two durable writes in exchange for keeping `AuthorityService` and the existing event journal as the only sources of truth.
- We accept exact retry reservations in exchange for a clear answer to the authorization-consumed append-crash gap.
- We accept that rollback detection needs retained retirement evidence in exchange for not claiming that `StorageIncarnationId` proves freshness alone.

## Alternatives considered

- Make `record_retained_history_statement` call `AccessPolicy::decide` directly. It hides little and exposes too much to callers, because they must know how to build the right `AccessAttempt` and recovery story.
- Model custody as a typed authority command. That reuses command durability, but it grants application `Write` semantics and violates the local-first command contract.
- Trust all imported `AuthorityService` facts. It is easy to code and wrong: imported grants would cross the current trust boundary without a source epoch proof.

## Open questions and risks

- Should `AuthoritySourceEpoch` require one source or a quorum when multiple authority origins are active?
- How long may an unused custody reservation remain retryable before an administrator must supersede it?
- Does safe departure count a reserved but unappended acknowledgment? Candidate B says no.

## Next implementation step

Build `custody.rs`, the two new `FrameworkControlEvent` variants, `AccessOperation::AcceptCustody`, and a Redb test that creates a real obligation, retains exact history, reserves authority once, appends one acknowledgment, reopens, retries, and receives the same event; native Iroh tests then cover wrong key, wrong principal, wrong holder, wrong obligation, retired incarnation, and denial before append.
