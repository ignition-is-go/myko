# Certified controller rotation

## Required behavior

A chosen rotation under the old controller quorum establishes the controller set
for the next predecessor slot. The successor may be disjoint from the old set.
After transferring and reopening the required history, the successor must verify
that chain and produce the next certified decision while the old stores are closed.
An unchosen rotation must not activate its proposed keys. A request using the old
epoch at the post-rotation predecessor must be rejected.

The anchor remains independently provisioned. Rotation changes controllers, not
the trust root. The successor epoch identity must bind the certified rotation,
so a caller cannot reuse an old epoch label with a different controller set.
The grant and lifecycle history selected before rotation remains necessary for
historical authority reconstruction after rotation.

## Current paths and boundaries

`AuthorityAnchor` currently contains one static epoch and controller set.
`AuthorityHistory::replay` gathers proposals and accept votes under that static
configuration, verifies full prepare and accept evidence, and records chosen heads.
`selected_at` walks the requested chain to genesis and validates operation IDs,
exact retained records, and causal closure. `assess_at` projects the selected
AuthorityService records and returns a labelled historical result.

`ControlQuorumVerifier::new` validates an externally established configuration,
not an authority chain. Its request factories feed durable proposer and voter
operations. The voter reconstructs promises and accepted values by controller and
exact slot before signing. `Node::vote_control` persists the result before release.
Those generic APIs do not currently refuse a caller-constructed authority epoch.

The implementation must distinguish two claims: rejecting uncertified historical
evidence, and preventing the framework's authority controller from issuing a vote
under an invalid context. A historical-reader test alone cannot prove issuance
fencing. The new boundary must derive the expected slot and controllers from a
verified predecessor, never from a requested controller list or epoch label.

## Design comparison

Compare authority-owned traversal with generic federation control-chain traversal.
Both must reuse the existing journal, signing, prepare recovery, and head hashing.
Judge them on configuration derivation, issuance fencing, complete history/restart,
layer ownership, and interface size. The generic candidate must earn its migration
cost by removing repeated rules, not by adding a second chain of wrappers.

The historical reader remains distinct from live policy. Neither a rotation
certificate nor a historical context proves current-head freshness, discharges a
storage obligation, or supplies a custody acknowledgment. Controller rollback and
full-copy key reuse remain separate unresolved risks.

## Work sequence

- [x] Revalidate source and previous verified checkpoint.
- [x] Compare two candidate shapes and obtain independent review.
- [x] Implement certified epoch traversal and predecessor-derived context.
- [x] Connect authority controller issuance to that context.
- [x] Verify disjoint handoff and restart, losing rotations, stale epochs, and
  incomplete predecessor history with actual durable proposer and acceptor APIs.
- [x] Run affected gates and audit the decision trail.

## Evidence

The preceding checkpoint passed 244 affected-crate tests and strict checks.
That checkpoint has no rotation support. Relevant files are authority's
`certified/history.rs` and `certified/mod.rs`, plus federation's `control_quorum.rs`,
`control_quorum/voter.rs`, and `control_quorum/proposal.rs`.
Graph project `myko-7-current`, Tier 2, generation `2026-09-05T01:04:50Z` is stale.
Targeted search returned no symbols; coverage reports these new files untracked.
Direct current-source reads supply the evidence.

All C01-C18 requirements in `scope-continuity-plan.md` remain Open.

## Implemented boundary

The generic chain now indexes proposals by predecessor and acceptance evidence by
chosen-value head and ballot. It advances one certified child at a time and does
not repeatedly rescan earlier proposals as the chain grows. A reverse-delivery
test reconstructs 64 retained transitions, a rotation and a successor decision.
This is a bounded behavior test, not a throughput benchmark.

Authority uses that chain and validates its own selected records. Controller
issuance validates the predecessor history and candidate payload, then binds the
same local snapshot to the durable request. The existing backend checks it under
the append mutex. Real Redb tests cover stale snapshots at that atomic boundary;
the actual authority wrapper is exercised for handoff and payload/history denial.
There is no deterministic injected wrapper interleaving test: that would require
a production hook or a wide fake backend. This limits the test claim, not the
scope of the remaining work.

The current affected suite passes 258 tests. Strict checks are recorded in
`/tmp/myko-controller-rotation-verified-clippy-command-2.log`; the similarly named
file without `-2` is a superseded failure. Controller identities are keys only.
No controller-to-principal, node or storage enrollment proof is implied.

The native founder-and-relay replacement regression also passes in
`/tmp/myko-controller-rotation-native-regression.log`. It uses explicit transfers
and AllowAll policy. It does not exercise certified controller rotation over the
network and does not prove custody.
