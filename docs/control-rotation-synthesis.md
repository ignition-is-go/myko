# Certified rotation synthesis

## Usage

```rust
let chain = CertifiedControlChain::replay(&events, anchor)?;
let context = chain.context_at(predecessor)?;
let rotation = ControlTransition::rotate(operation, successor_keys, payload)?;
let value = rotation.control_value()?;
let verifier = context.verifier()?;
// Existing durable prepare, propose, accept APIs choose value.
let next = CertifiedControlChain::replay(&updated_events, anchor)?;
let successor = next.context_at(rotation_head)?;
```

`context_at` proves the configuration after an exact historical head. It does
not prove that head is globally current. Authority validates all selected record
bodies and their causal history before exposing its corresponding context.

## Shape

Federation owns a generic `control_chain` module. `ControlAnchor` pins realm,
initial epoch, genesis and controller keys. `ControlTransition` contains a stable
operation ID, an opaque payload, and either preservation or replacement of the
controller set. Replacement keys are canonical, distinct and validated. The
successor epoch is derived from the chosen rotation head under a versioned domain;
that head already binds realm, predecessor, old epoch and full transition bytes.

`CertifiedControlChain::replay(&[EventEnvelope], ControlAnchor)` validates immutable
identities and reachable chosen decisions. `context_at(ControlHead)` returns a
private-field historical configuration. `transitions_to(ControlHead)` exposes
the certified ordered opaque transitions for domain validation. There is no
latest-head API. An unchosen branch does not activate keys. Valid duplicate proof
dominates malformed duplicate proof; conflicting chosen successors fail closed.

Authority owns its payload codec, operation/record identity checks, exact retained
bodies, causal closure and projection. It migrates its existing static quorum
replay to the generic chain rather than retaining two chain implementations.

Local issuance must additionally compare its validated history with the journal
under the existing append mutex before signing. This rejects changes between
validation and append; it is not a network freshness proof. An authority issuer
must revalidate on retry. Raw low-level signing remains untrusted protocol input.
Persisted proposals and recovered accepted values retain existing Paxos semantics.

## Synthesis decision

Independent gpt-5.5 review scored both candidates 7/10. Use the generic candidate
for reusable controller succession. Graft the authority candidate's exact-head
interface and domain-owned record validation. Delete generic AuthorityRecords,
authority-specific commitments, LatestKnown, and current-serving claims.

Controller key enrollment is the existing generic identity boundary. Principal,
node and storage bindings require their own certified enrollment semantics; this
unit must not imply those exist merely by adding unverified fields.

## Verification contract

Use real Redb durable prepare/propose/accept calls. Transfer retained history to a
disjoint successor electorate, close old stores, reopen successors, and choose a
later authority transition. Reject old-epoch decisions after rotation, losing
rotations, conflicting chosen successors and incomplete selected authority history.
Test atomic local issuance separately. Neither test proves custody discharge,
global currentness, rollback resistance, or live permission.

All C01-C18 remain open. No production daemon, worktree, commit, push or Mac sync.
