# Durable custody and departure

This design refines the custody portion of the August foundation. It is not an
implementation claim. Ordinary convergent commands remain local-first. The
completion requirements remain in [scope-continuity-plan.md](scope-continuity-plan.md).

## What a receipt proves

A receipt states that one identified storage holder durably accepted an exact
set of immutable events before issuing the receipt. It does not prove continued
availability, current serving authority, or that an adversarial holder kept the
bytes. Eligible custodians are trusted to obey the storage contract.

The receipt binds the custody agreement, exact scope selection, holder identity,
storage incarnation, and a manifest of immutable event identities and content.
The signature also binds the receipt format version. The manifest includes the
required command results and control history, not merely the current item values.

An origin maximum is not an exact manifest. Origin positions also cover other
scopes, and observing position 20 does not prove that a required position 12 was
received. Coverage comparison must compare the required event set and content.
Local replay positions are useful for local recovery but never compare coverage
between holders. A compact representation must preserve these properties.

The manifest identifies a frozen obligation, not all history that might exist
elsewhere. Missing causal parents prevent readiness. Pending accepted history
remains a retention obligation even while it cannot contribute to a projection.
Cross-scope dependencies require explicit authorization and retention treatment;
receipt generation cannot silently export adjacent private history.

A receipt covers history preceding its own issuance. It does not recursively
claim to contain itself. Later receipts may cover earlier receipt records. The
handoff completion rule must name which control records the successor retains;
it cannot require an infinite sequence of receipts acknowledging receipts.

## Where persistence is enforced

`EventJournal::append` already requires durability before success. Redb uses an
immediate-durability transaction. `InMemoryBackend` without a journal has no
durable storage obligation and must not issue a durable receipt.

The storage boundary must verify the manifest against retained immutable bodies,
persist the attestation record, and finish the durable write before releasing
the receipt. An append error cannot publish a receipt or advance custody state.
An acknowledgment recorded on the sender before receiver persistence is invalid.

Receipt storage belongs in Myko's existing history system. `NodeEvent` now has
an explicit framework control variant alongside its command variants. Command
consumers skip control records. No fake application command or independent
receipt database is involved. The recording and signing operations described
below do not issue custody receipts.

`NodeId` is not itself a signing key. A transport-neutral receipt needs an
authenticated binding between the holder's node identity and its verification
key. Possession of that key does not establish a scope custody grant.

An incarnation identifier distinguishes intentionally replaced stores. It cannot
detect restoration of an old snapshot containing the same identifier and key.
Such rollback requires reconciliation with a surviving witness or an external
non-rollbackable mechanism. Reopening a file alone cannot prove freshness.

## How departure accounts for writes

Custody changes coordinate only the necessary control transition. They must not
turn normal state-only command admission into a quorum operation.

1. The departing node persists its drain intent and stops new local admission
   for the affected scope. Accepted work must finish or retain a recoverable
   obligation. The node continues replication while it drains.
2. The successor acquires and persists the required history. A receipt for an
   earlier cut remains valid for that cut but cannot cover a later accepted tail.
3. Every writer removed by the transition seals its accepted history. Its final
   manifest includes all work accepted before sealing. Continuing writers retain
   their own custody obligations and must have a valid path to the successor set.
4. The control transition verifies the successor's receipts against the closed
   obligations and the configured surviving-copy policy.
5. Only a persisted, recoverable completion authorizes the departing node to
   release custody. Crashes before completion leave the obligation in force.

Fencing or permission expiry prevents future acceptance under old authority. It
does not prove that earlier accepted history reached another holder. If an
unreachable writer may hold the only copy of such history, a lossless completion
cannot be certified. The system must retain that unresolved obligation rather
than relabel the history as never accepted.

Concurrent leave requests cannot each count the other departing node as the
survivor. Membership and custody completion need an intersecting control decision
protocol with persisted promises and recovery. The earlier candidate's proposed
protocol is not approved wholesale, especially its restrictions on normal
commands. This remains a design gate before safe-departure code.

## Executable acceptance sequence

The implementation must extend the real Redb and Iroh continuity test, not
replace it with a simulated state machine. The following cases are required:

- Omit one required event while retaining a higher origin position. Reject the
  receipt despite equal maxima.
- Retain the same event IDs with changed immutable content. Reject the receipt.
- Move identical events to different local replay positions. Preserve coverage.
- Reorder and duplicate transfer. Produce the same exact covered set.
- Fail the receiver's durable write. Return no receipt and retain the sender's
  obligation. Reopen both stores and verify the same result.
- Accept another command after the first receipt. Refuse departure until its
  accepted history is also covered.
- Let a writer accept offline, expire its permission, and remain unreachable.
  Preserve the unresolved history obligation.
- Interrupt each join, drain, receipt, and completion boundary. Reopen the stores
  and finish without losing accepted history or repeating external effects.
- Race two departures. Keep the configured number of actual durable copies.
- Substitute the wrong scope, agreement, signer, or known retired incarnation.
  Reject the acknowledgment without granting serving authority.

## Implemented storage precondition

`EventJournal::verify_retained_history` now checks every supplied event against
durable replay. It compares immutable origin, timestamp, and body while ignoring
the holder's local replay position. Missing events and conflicting immutable
content produce distinct errors. Repeated and reordered requirements preserve
the result. An empty requirement is valid set inclusion, not evidence of a
complete scope.

The verifier rejects conflicting duplicate origins in durable replay instead of
silently retaining the last body. Identical immutable duplicates do not change
the retained set.

The Redb test checks these properties after reopening real stores. It explicitly
retains a higher same-origin event while omitting a lower required event, and
requires verification to fail. The native continuity test freezes nonempty scope
history before each transfer and verifies the receiver's durable journal. It
also verifies C's full frozen scope history after restart, alongside the existing
logical value, origin-filtered values, and typed command result assertions.

This helper trusts the supplied required set and uses full durable replay. It
does not itself derive scope closure or implement an efficient manifest format.
Receipt signing and safe departure remain behind that boundary.

Independent representation review favors explicit framework control events in
`NodeEvent`, with command consumers migrated to handle non-command events.
Custody records must not become executable application work. The control variant
now exists. The membership protocol and departure operation remain unimplemented.

## Signed-statement implementation checkpoint

`SelectedHistoryManifest::commitment` hashes the selected immutable event set with
a version-one encoding. The digest binds the selection, event origins, timestamps,
and complete bodies. It excludes observer-local positions and the local recording
cut. Events sort by origin, and JSON object keys sort recursively. This encoding
is specific to Myko, not an implementation of RFC 8785.

`RetainedHistoryStatement` binds that commitment to a holder, storage incarnation,
and obligation event. Its signing bytes have a separate version-one domain.
`SignedRetainedHistoryStatement` stores the statement, Ed25519 verification key,
signature, and explicit `ed25519_statement_v1` format in `myko-federation`.
Deserialization rejects unsupported formats, unknown fields, and incorrect byte
lengths. Construction and deserialization do not verify the signature.

The Iroh adapter signs these bytes and verifies them against an independently
expected statement and authenticated node descriptor. Tests cover wrong keys,
changes to every statement field, transplanted signatures, serialization, and
fixed encoding vectors. Nine integration tests, 139 federation library tests,
31 Iroh library tests, and strict affected-crate Clippy pass at this checkpoint.

These records are signed assertions, not durable custody receipts. Issuance must
still verify the obligation, signer eligibility, and retained history, then
persist the control event before releasing a receipt. No membership, safe-leave,
or all-writers completeness guarantee follows from a valid signature.

## Framework control recording

`NodeEvent::FrameworkControl` retains `FrameworkControlEvent` in the existing
journal. Its first record type contains a signed retained-history statement.
Command catalogs and item projections ignore this record. It does not establish
scope existence or topology. Causal replay waits for its referenced obligation,
but that dependency alone does not prove retention of the committed event set.

`Node::record_retained_history_statement` requires a durable journal and verifies
the local holder, persisted storage incarnation, selection, commitment, and exact
retained manifest bodies before appending. Subscribers receive the record only
after a successful append. Exact retries return the original locally authored
record, including after restart.
An append failure leaves the local cursor, projections, and live delivery unchanged.

The recording API deliberately accepts unverified signature bytes. It does not
authorize the obligation, verify membership, or establish that an incarnation has
not been retired. A custody issuer must establish these conditions before using
the record as an acknowledgment. Imported assertions remain inert evidence.

`EventJournal::storage_incarnation` requires each journal adapter to expose a
persisted store identity. Redb initializes that identity in an immediate-durability
metadata transaction and preserves it across reopen. Legacy metadata with a valid
node identity gains a store identity without changing accepted history. This
upgrade does not certify assertions already in the log. A malformed identity or
an established journal missing its node identity causes initialization to fail.
`Node::storage_incarnation` returns `None` for the volatile reference backend.

Matching this metadata binds new local recording to the opened store. It does
not detect a copied or restored database, both of which can retain the same
identity. Incarnation retirement and freshness still need independently retained
control evidence. They remain prerequisites for custody issuance.

Replication checks a control record's full `ScopeSelection`. An exact-root grant
does not expose a subtree statement. Service-only replication excludes controls
because they are not application-service events. An overlapping but narrower
retained manifest fails instead of silently dropping part of a control record.
The wire schema is version 7 because old peers cannot decode the new event variant.

Verification now includes four Redb control-history tests, five recording and
verifier tests, and 15 Redb library tests. Previous checks also cover 17 authority
tests and the real exact-scope session stream. The native continuity test and
15-test durable-node file passed at the framework-control checkpoint.
An earlier loaded run hit a pairing report timeout. Its isolated rerun and the
full-file rerun pass, but the scheduling explanation remains an inference.

The affected Myko strict checks pass. Forrest's full test and strict Clippy gates
passed at the framework-control checkpoint and were not rerun for the storage
incarnation increment. The decision trail retains earlier failures and superseding results.
Full custody and safe departure remain open in the continuity plan.

## Fixed-cut manifest implementation checkpoint

`SelectedHistorySnapshot::retained_manifest` now derives the retained event set
for a `ScopeSelection` at the snapshot's local recording cut. The returned
`SelectedHistoryManifest` exposes read-only selection, cut, and event accessors.
The native continuity test uses this method instead of a handwritten event filter,
and verifies the source journal before transferring the required set.

The manifest includes replaced and deleted state history and command lifecycle
records, not just events that contribute to current rows. Its retention scope
calculation is separate from replication authorization: reading another scope
does not make a command part of that scope's write history. Existing replication
permission checks remain unchanged.

Relevant unresolved history produces a typed error. Subtree selection rejects
pending history conservatively because an absent parent link cannot establish
that a pending write is outside the subtree. Exact selection can ignore unrelated
pending history. An atomic event spanning an unselected scope produces an error
rather than a partial event. Declared dependencies, committed lifecycle
references, and implicit same-author scoped predecessors must remain inside the
selected set.

An exact nested scope may therefore require a wider selection to retain its
parent-spanning establishment event. The current single-selection API does not
solve independently authorized cross-scope custody. A cut of `None` means the
empty prefix, even if the node has since accepted events. Neither an empty
manifest nor a complete local prefix proves that another writer has no unseen
accepted history.

Seven integration tests pass in `/tmp/myko-selected-manifest-exact-set.log`.
The fixed-cut test compares the complete retained event sets before and after
deletion, so preserving only the final value cannot satisfy it. The native
replacement-node test passes in `/tmp/myko-scope-manifest-native-final.log`.
All 139 federation library tests pass in
`/tmp/myko-selected-manifest-federation-lib.log`, and strict node Clippy passes in
`/tmp/myko-scope-manifest-node-clippy-final.log` after the visibility correction.
Forrest's workspace tests and strict Clippy pass in
`/tmp/forrest-scope-manifest-workspace.log` and
`/tmp/forrest-scope-manifest-clippy.log`.

Framework control records, signer binding, and safe-departure coordination remain
unimplemented. This checkpoint does not close a custody completion row.
