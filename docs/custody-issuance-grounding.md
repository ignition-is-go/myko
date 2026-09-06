# Custody issuance integration

This work continues `scope-continuity-plan.md`; it does not narrow its C01-C18
requirements. The storage-incarnation checkpoint is verified. This document
grounds the next design, not a custody implementation claim.

## Current boundary

`Node::record_retained_history_statement` checks the local holder, persisted store
identity, selected commitment, and exact durable bodies before appending through
the existing journal. Its input remains an unverified signed assertion. It does
not validate an obligation, grant, signer, or current custodian membership.
At this initial grounding, `FrameworkControlEvent` contained only that statement
variant. The subsequent durable-voter checkpoint adds signed control votes.

The Iroh signing adapter signs the statement bytes. Verification requires an
independently trusted `NativeNodeDescriptor` and expected statement. Neither the
receipt's own fields nor successful signature verification establish custody.
`endpoint_principal_id` maps an authenticated endpoint to its principal. A
`NodeId`, endpoint key, and `PrincipalId` are distinct identities.

`Node::prepare_command` calls `command_authorization`, which constructs an
`AccessAttempt`, binds the authenticated executor, and consults the installed
`AccessPolicy`. No policy means denial. `AuthorityPolicy::decide` calls
`evaluate`; its retained facts must cover the captured local authority cut.
Some decisions require durable `EvaluateAuthority` execution, such as consuming
limited-use grants. A plain permit is not a durable custody decision or a fence
against a later revocation.

The ordinary command dispatcher holds a process-local reentrant lock through
claim, handler, effect authorization, and commit. Nested authority evaluation
runs in that interval. Custody code outside it does not inherit that protection.
Even inside it, the authority-use batch commits before the outer effect batch.
A crash can consume a one-shot grant without persisting the effect. The issuance
design must define recovery rather than claiming atomic cross-service writes.
Ordinary typed commands also normalize their primary claim to require `Write`;
using one as a framework-control wrapper would add unintended application powers.

`AuthorityPolicy` explicitly excludes foreign authority projections. Its fact
sources and freshness check select the local node's origin. Changing that filter
to all origins would let imported authority records cross the existing trust
boundary. Keeping that filter as the scope's permanent authority would contradict
founder replacement. Custody issuance must resolve this distinction explicitly.

`AccessOperation` and `FederationPermission` have no custody operation or
permission. Read-history access, pairing, and possession of bytes cannot substitute
for an agreement to retain them. Existing `AuthorityService` facts must remain the
authority system; do not add a competing application ACL or receipt database.

## Constraints for the design

- Keep immutable accepted history authoritative and ordinary commands local-first.
- Use the existing journal and explicit framework control records.
- Bind an acknowledgment to an actual persisted obligation, exact selection,
  required immutable history, holder, signing identity, and storage incarnation.
- Check the obligation's meaning, not merely that some event has its ID.
- Do not trust a caller-built `PermitDecision`, boolean callback, deserialized
  verified wrapper, or receipt fields as their own authority proof.
- State when authorization takes effect and how retries, revocation, and a crash
  between authorization and append recover without inventing a promise.
- A fixed history acknowledgment must not imply current membership, serving
  authority, all-writer completeness, or permission to discard another copy.
- Retired incarnation and rollback checks require independent control evidence.
- Authority must remain recoverable after founding nodes disappear.
- Test real Redb persistence and the native transport, including denial paths.

## Evidence and phases

Graph project `myko-7-current`, Tier 2, generation `2026-09-05T01:04:50Z` is stale.
Targeted source reads supersede it. Relevant paths are federation `access.rs`,
`node.rs`, `memory.rs`, `control.rs`, `selected.rs`, authority `policy.rs` and
`domain.rs`, and Iroh `attestation.rs` and `identity.rs`.
The governing August document is
`superpowers/specs/2026-08-22-myko-federation-first-principles.md`, especially
sections 4.5, 5.2, 6, and 8.

- [x] Read pstack principles and current source.
- [x] Finish the authorization flow review.
- [x] Compare issuance candidates and cross-judge them. Both converge on a durable
  intent followed by append; neither supplies a complete voting protocol.
- [x] Synthesize the obligation and authorization boundary, including the required
  ballot-recovery gate in `custody-issuance-synthesis.md`.
- [x] Implement and test the signed quorum boundary. This verifies evidence only,
  not persistent votes, certified authority, or custody.
- [x] Persist controller votes in the existing journal and test real Redb reopen,
  legal recovery, failed appends, and ambiguous commits. This is the acceptor only.
- [x] Persist signed ballot-to-proposal bindings and remove direct prepare-to-accept
  construction. Test conflicting reuse, restart, concurrent calls, invalid proof,
  and ambiguous appends. This does not allocate ballots or coordinate peers.
- [ ] Implement the selected boundary and its real persistence tests.
- [ ] Verify native integration, denials, retry behavior, and strict checks.
- [ ] Audit the evidence and retain all unresolved continuity requirements.

No human checkpoint is requested. Do not create worktrees, delete event logs,
or manually launch the production daemon.
