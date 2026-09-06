# Candidate A: certified custody issuance

## Usage (caller's view)

The coordinator creates a real storage obligation from a frozen source manifest. It never constructs an authorization decision.

```rust
let snapshot = SelectedHistorySnapshot::current(source.node())?;
let manifest = snapshot.retained_manifest(&ScopeSelection::Exact(project_scope))?;

let obligation = authority.create_custody_obligation(
    AuthenticatedSession::native(operator_connection),
    CustodyObligationDraft::for_holder(manifest, holder_binding, CustodyTerm::UntilReleased),
)?;
```

After replication delivers the obligation event and every required immutable event, the holder issues through its installed policy and native signing identity. There is no public API that accepts `PermitDecision`, an `authorized: bool`, or a deserialized "verified" value.

```rust
let receipt = holder_authority.issue_custody(
    AuthenticatedSession::native(holder_connection),
    holder_node,
    &holder_iroh_identity,
    obligation.event_id(),
)?;
// Success means the signed statement is already durable in holder_node's journal.
```

A verifier counts the receipt only against independently loaded control and authority state.

```rust
let counted = authority.verify_current_custody(
    receipt.event_id(),
    obligation.event_id(),
    required_authority_head,
)?;
```

`counted` proves acceptance of this fixed obligation. It grants neither serving authority nor membership and says nothing about later writes.

## Problem

`Node::record_retained_history_statement` already checks the local holder, persisted store incarnation, selection, commitment, and exact durable bodies before appending a `FrameworkControlEvent`. It deliberately does not authorize the obligation or signer. `AuthorityPolicy` can consume limited grants durably, but it currently evaluates only local-origin `AuthorityService` facts. Treating all imported grants as trusted would let replication invent authority; retaining the local-origin rule forever would make a founding node permanent. Custody issuance needs a portable authority cut, an obligation with stored meaning, and recovery across the authority-commit and receipt-append boundary.

The existing `retained_foreign_grants_do_not_become_local_authority_after_restart` regression is the compatibility line: raw retained authority from A must remain inert on B, including after B reopens. Only the certified projection below may cross that line.

## Shape

### Domain types

```rust
pub struct RequiredEvent {
    pub origin: EventId,
    pub immutable_digest: [u8; 32], // origin, recorded_at, and complete NodeEvent
}

pub struct RequiredHistory {
    pub selection: ScopeSelection,
    pub events: Vec<RequiredEvent>,  // canonical EventId order, no duplicates
    pub commitment: RetainedHistoryCommitment,
}

pub struct CustodianBinding {
    pub node_id: NodeId,
    pub node_principal: PrincipalId,       // must equal PrincipalId::for_node(node_id)
    pub endpoint_principal: PrincipalId,   // must equal endpoint_principal_id(key)
    pub ed25519_key: [u8; 32],
    pub storage_incarnation: StorageIncarnationId,
}

pub struct CustodyObligation {
    pub id: CustodyObligationId,
    pub realm_id: AuthorityRealmId,
    pub required: RequiredHistory,
    pub holder: CustodianBinding,
    pub term: CustodyTerm, // first version: UntilReleased
    pub creation_authorization: AuthorityMutationId,
}

pub struct CustodyIssuanceAuthorization {
    pub id: CustodyIssuanceId,
    pub obligation_event: EventId,
    pub holder: CustodianBinding,
    pub statement_digest: [u8; 32],
    pub authorized_principal: PrincipalId,
    pub authority_head: AuthorityHead,
}
```

`RequiredHistory` is the exact pre-obligation set. The obligation event is not in its own requirement. A statement references that event and commits to the pre-obligation set, so the dependency is finite and noncircular. A later obligation naturally includes earlier obligations and receipts when they fall inside its frozen selection. The journal loads each listed origin, checks its immutable digest, rebuilds `SelectedHistoryManifest`, and recomputes the existing commitment. Origin maxima and local replay positions are irrelevant.

The obligation means that this exact holder and store must retain the exact set until a later, authority-certified `CustodyRelease` names the obligation and proves the surviving-copy rule. Expiry, grant revocation, disconnect, and removal from serving membership do not erase the storage promise.

### Portable authority prerequisite

Authority becomes portable through a certified history of the existing `AuthorityService`, not through another ACL. Each realm has a locally provisioned `RealmTrustAnchor { realm_id, genesis_head }`. The anchor identifies the realm, not a live founder. A certified `AuthorityEpoch` names controller verification keys and a threshold. A `CertifiedAuthorityMutation` binds the previous head, epoch, stable command ID, complete effect digest, and resulting head, with a threshold of signatures from the previous epoch. Epoch rotation uses the same rule, so every founding endpoint may disappear.

Controllers durably vote at most once for a proposed successor to one head. This is selective coordination for authority control only. Imported `AuthorityService` events remain inert until their command and effects appear in the single certified chain rooted at the trust anchor. `AuthorityFactSources::open_certified` may read all origins, but an internal, non-serializable `CertifiedAuthorityProjection` exposes only facts selected by that chain. `AuthorityPolicy::current_state` reads this projection and its certified head. It must never switch its current `Some(local_node)` filter to an unverified `None` filter.

Grants and revocations, custodian key bindings, incarnation retirement, obligation-creation authorizations, and issuance authorizations are all `AuthorityService` facts certified this way. Ordinary application commands remain local-first. A partition without the authority threshold can continue commands allowed by existing policy, but it cannot create a new obligation, rotate authority, retire an incarnation, or issue a new custody receipt.

### Signatures and flow

```rust
impl AuthorityPolicy {
    pub fn create_custody_obligation(
        &self,
        session: AuthenticatedSession,
        draft: CustodyObligationDraft,
    ) -> Result<CustodyObligationRecord, CustodyError>;

    pub fn issue_custody<S: CustodyStatementSigner>(
        &self,
        session: AuthenticatedSession,
        holder: &Node,
        signer: &S,
        obligation: EventId,
    ) -> Result<CustodyReceipt, CustodyError>;
}

pub trait CustodyStatementSigner {
    fn public_key(&self) -> [u8; 32];
    fn sign(&self, statement: RetainedHistoryStatement)
        -> Result<SignedRetainedHistoryStatement, SigningError>;
}

impl Node {
    fn required_manifest(&self, required: &RequiredHistory)
        -> Result<SelectedHistoryManifest, NodeError>;
    fn append_custody_obligation(&self, obligation: CustodyObligation)
        -> Result<EventEnvelope, NodeError>;
}
```

The native session derives `endpoint_principal_id` from the authenticated Iroh connection. The policy resolves that endpoint through the certified `CustodianBinding`, derives `PrincipalId::for_node(node_id)`, and evaluates a new `AccessOperation::IssueCustody` requiring a new `FederationPermission::Custody`. `ReadHistory`, pairing, possession of bytes, and an application `Write` grant do not cover it. The policy also checks that the obligation event is the expected control variant, its creation authorization is certified, its stored requirement matches the requested statement, the binding is current, and the incarnation is not retired. The signer supplies cryptography only; its key must equal the independently certified binding.

Authorization linearizes when the quorum certifies the `CustodyIssuanceAuthorization` mutation. The mutation command atomically consumes limited grants and stores the exact issuance record under a deterministic ID. Only then does the holder sign and call the existing durable recording boundary. The public result is released after append succeeds.

Retries use the deterministic obligation and issuance IDs. A crash before the authority mutation commits consumes nothing. A crash after local commit but before certification resumes certification of the same mutation. A crash after certification but before receipt append reuses the persisted authorization without consuming authority again. A crash after append returns the existing identical local control event. Conflicting reuse of either ID fails. Revocation before certification wins; revocation afterward blocks later issuances but cannot strand this authorized append. Retirement after issuance leaves the receipt as historical evidence but prevents it from counting as current custody. A restored old database cannot obtain a new authorization from the current quorum. Detecting rollback of the same key and incarnation still requires a surviving quorum or an external non-rollback witness.

Obligation creation uses the same recovery rule: certify its creation authorization, then append the obligation by stable ID. A crash between those writes resumes the authorized append; it does not re-evaluate or consume the grant again.

### Module ownership

- `myko-federation` owns `RequiredHistory`, `CustodyObligation`, exact journal reconstruction, and new `FrameworkControlEvent::CustodyObligation`. The existing statement, manifest, control journal, and low-level recorder stay intact.
- `myko-authority` owns certified authority heads, epochs, identity and retirement facts, `Custody` evaluation, grant consumption, authorization records, and the two high-level methods. It remains the only authority system.
- `myko-iroh` implements `CustodyStatementSigner` and constructs `AuthenticatedSession` only from the live authenticated connection and local secret key. Wire payloads remain untrusted data.

This is a deep interface: callers provide a draft or an obligation ID, while the callee hides history reconstruction, certified-cut selection, identity binding, grant consumption, signing, persistence, and recovery.

## Synthesis decision

Candidate A chooses a certified authority chain plus durable issuance intent. It rejects a local-only policy exception because it cannot survive founder replacement, and rejects a receipt-side proof bundle because callers could replay or assemble authority evidence outside the policy's current-state check.

## Tradeoffs accepted

- We accept quorum latency for authority and custody control in exchange for revocation ordering and portable authority; ordinary commands do not pay it.
- We accept an explicit event-digest list in the first format in exchange for exact, testable coverage. A later compact set must preserve identical semantics.
- We accept failure to issue during an authority partition in exchange for never guessing whether a revocation or incarnation retirement won.

## Alternatives considered

Local-origin issuance has a small API but preserves a permanent authority node, so it is not viable. Reading every replicated grant hides little and exposes every caller to forged-origin facts. A self-contained capability token simplifies offline issuance, but revocation and retired-incarnation checks then depend on stale token fields and cannot linearize against current authority.

## Open questions and risks

- What controller threshold and fault assumption will the first `AuthorityEpoch` support?
- Which external witness, if any, will deployments use when rollback detection must survive loss of every current controller?

## Next implementation step

Build one vertical slice with a genesis epoch, one certified controller rotation, deterministic obligation and issuance authorization commands, and the two framework control appends. Its Redb and native Iroh test must stop the founder and issue on the successor, preserve `retained_foreign_grants_do_not_become_local_authority_after_restart`, reject missing or changed events, wrong scope/key/principal/incarnation, `ReadHistory` alone, forged permits, and uncertified grants, then inject an append failure and prove after restart that retry produces one grant use and one receipt event. Add pre-certification revocation and retired-receipt counting as denial cases in the same slice.
