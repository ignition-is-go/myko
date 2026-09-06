# Custody issuance synthesis

Status: selected direction with a required control-protocol implementation gate.
No custody issuer, portable authority projection, or voting protocol is implemented
by this document. All C01-C18 requirements remain open.

## Decision

Use candidate A's certified authority history and durable issuance intent as the
base. Use candidate B's framework-owned issuer facade and explicit acknowledgment
record. A caller supplies an obligation ID, not a permit, a signer identity taken
from the request, or a hand-built manifest advertised as verified.

The two candidates converged on authorization followed by receipt append. They
did not provide independently complete coordination protocols. The cross-judge
preferred A, but initially overrated its coordination rule. The parent identified
the split-vote counterexample below; the judge then reduced that score from 2/2
to 1/2. Neither candidate is approved as written.

Reject B's ordinary administrator command as sufficient to transfer authority
sources. Concurrent source changes require an ordered control decision. Also
reject A's hard requirement that an authority principal use a formatted node-ID
string. A certified binding must connect the actual principal, node, endpoint
key, and store without changing the meaning of existing principal IDs.

Do not retain an old API merely as a compatibility layer. Low-level stored
assertions may remain useful evidence, but only the new validated path can
produce an acknowledgment that custody accounting counts.

## What is already executable

`retained_foreign_grants_do_not_become_local_authority_after_restart` uses two
real Redb stores. A authorizes its grant, closes, and B retains A's exact history.
After B reopens, its own grant permits access while A's retained grant fails with
`grant_coverage`. The local positive control rules out an unavailable policy as
the reason for denial. The 18-test authority suite and strict Clippy pass.

The baseline's first run expected the wrong denial code. The evaluator already
returned the correct denial; the fixture was corrected, not production behavior.
The decision trail preserves that failure and the superseding run.

## Recoverable control decisions

The first implementation must use a crash-fault majority protocol. Controllers
are enrolled by an independently trusted realm anchor and certified epoch chain.
An epoch contains unique controller identities and keys. Its quorum is strictly
more than half its controllers, not a caller-selected threshold. Two controllers
therefore require both for new control decisions. This does not change ordinary
local-first application commitment.

Permanent one-vote-per-head state is insufficient. Three controllers can each
vote for a different successor, leaving no majority and no legal next action.
A higher ballot must recover earlier accepted values safely.

```rust
struct AuthoritySlot {
    realm: AuthorityRealmId,
    epoch: AuthorityEpochId,
    predecessor: AuthorityHead,
}
struct AuthorityBallot {
    counter: u64,
    proposer: ControllerId,
}
struct AcceptedAuthorityValue {
    ballot: AuthorityBallot,
    value: AuthorityTransitionValue,
}
struct AuthorityVoteState {
    promised: Option<AuthorityBallot>,
    accepted: Option<AcceptedAuthorityValue>,
}
```

The slot binds every request, reply, signature, persisted vote, and certificate.
The proposal contains the stable operation identity and complete canonical
authority effect, not only a digest whose bytes may disappear with the proposer.
The resulting head is derived from the slot and value; it is not caller-chosen.

1. A controller accepts a prepare at a higher ballot and durably records its
   promise before replying. An equal-ballot retry returns the current accepted
   value without lowering the promise. A lower ballot is rejected.
2. A prepare certificate contains authenticated replies from distinct enrolled
   controllers for the same slot and ballot. The proposer must adopt the value
   with the highest accepted ballot. Only a certificate with no accepted value
   permits a new proposal. Conflicting values at the same highest accepted ballot
   are invalid evidence, not a tie to resolve arbitrarily.
3. An accept request carries that prepare certificate. A controller verifies the
   certificate, the required value choice, the slot, and its own promised ballot.
   It never accepts a different value at the same ballot. It persists the full
   accepted value before replying.
4. A chosen certificate requires a majority of authenticated accepts for the
   same slot, ballot, and value. Importing records or collecting promises alone
   does not establish a chosen head.
5. After a crash or split minority votes, a proposer gathers a fresh prepare
   majority at a higher ballot and completes the required accepted value.
   Progress requires a reachable majority and an eventual stable proposer.

An acceptor need not have sent a separate prepare reply at the accept ballot.
It rejects an accept below its retained promise; a valid higher accept persists
the new promise and accepted value together. Prepare and accept majorities can
have different members. Requiring a prior same-ballot prepare would needlessly
exclude a replacement acceptor. This follows the acceptor rule in
[Paxos Made Simple, section 2.2](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf).

Cryptographic validation belongs at the protocol boundary. A deserialized
certificate is not a verified certificate. Public callers must not construct
the verified epoch or quorum types from a boolean or a claimed controller list.
Controller promises and accepts belong in local controller-origin framework
records in the existing durable Myko history, with replies withheld on append
failure. Appending these records cannot require an already chosen authority
transition or its certified projection. They are not application commands and
must not create a separate receipt or voting database. Replay must authenticate
the controller record, not trust a peer envelope merely claiming a local origin.

## Epoch changes and freshness

A rotation is a chosen control value under the established epoch, not an
arrival-order update to a source list. The successor configuration can act only
after verifying that chosen rotation and retaining the required authority history.
Every subsequent slot cites the certified predecessor and the configuration
selected by it. Old-epoch requests for a post-rotation predecessor are rejected.
Installing a losing or uncertified rotation cannot activate its controllers.

The implementation must prove this fencing during competing rotations and
restart before claiming reconfiguration. A successful rotation certificate alone
does not discharge the retiring holders' storage obligations. That still requires
the safe-departure protocol and exact surviving durable coverage.

The protocol assumes enrolled controllers honor durable promises through ordinary
crash recovery. It does not make majority voting Byzantine-safe or detect a
restored database that erased a promise while retaining its key. Controller
rollback recovery needs independently retained evidence or a non-rollback witness.
Unknown freshness cannot be converted into current authority by reopening a file.

## Obligation and issuance semantics

An authorized obligation binds its exact pre-obligation immutable event set,
selection, holder node, authority principal, signing key, and storage incarnation.
The obligation does not include itself in that set. Issuance must also retain the
obligation record and its validated authorization, so a receipt cannot outlive
the evidence explaining what was promised.

AuthorityService remains the source of grants, revocations, uses, and decisions.
Its portable projection admits only facts selected by a certified authority chain
rooted in the realm anchor. Raw foreign facts remain inert. An explicit custody
operation and permission distinguish making a retention promise from reading
history or writing application state.

Issuance authorization consumes bounded authority and records the exact issuance
intent in one certified authority transition. A crash before receipt append
retries that same binding and consumes no second use. Another binding cannot use
the reservation. A holder derives and signs the statement, verifies durable
retention, and appends the acknowledgment before releasing it.

Only a persisted acknowledgment can count as receipt evidence. A reservation
alone cannot discharge a sender's obligation. Revocation prevents new issuance
authorizations, but does not erase an already certified storage obligation.
An acknowledgment completed after incarnation retirement can at most explain
historical custody under a pre-retirement authorization; it cannot count as
current custody. Counting requires a retirement-aware certified authority head,
not the head advertised by the receipt itself.

## First production unit and gates

Build the persistent control-vote state machine, certificate validation, and
certified-head recovery before adding a custody issuance endpoint. Keep ordinary
application commands outside this coordination path.

The unit must exercise actual journal writes and real signing identities:

- Persist a promise, reopen, and reject a lower ballot.
- Persist an accepted full value, reopen, and recover it in a prepare reply.
- Lose a reply after majority acceptance, then choose the same value at a higher
  ballot after restart.
- Recover competing accepted minorities through a legal prepare/accept trace.
  Do not seed impossible protocol state through an unchecked test-only append.
- Inject journal failure and observe no successful vote response or state change.
- Reject duplicate signers, insufficient quorums, wrong slot, wrong epoch, forged
  signatures, and a proposal that ignores the highest accepted value.
- Race two rotations and reject every action under the losing successor epoch.
- Preserve the foreign-grant baseline and add a positive certified-history case
  before replacing AuthorityPolicy's local-origin projection.

Then wire obligation creation and recoverable issuance through this boundary,
and extend the native founder-replacement test to issue an actual acknowledgment
on the successor. Until those gates pass, this is a design and an executable
trust-boundary baseline, not completed custody.

## Certificate verification checkpoint

`myko_federation::control_quorum` now verifies signed promise and accept evidence
against an independently supplied slot and controller configuration. It validates
Ed25519 keys, signatures, distinct membership, strict majority, phase, and exact
realm, epoch, predecessor, and ballot. Prepare verification rejects contradictory
accepted reports and retains the highest accepted full value.

Chosen verification is a method on `PreparedControlQuorum`, which borrows its
original verifier. It cannot substitute another slot or electorate and checks the
required recovery value before accepting a chosen certificate. This is stronger
than checking a majority of accept signatures alone. Under the crash-fault Paxos
assumption, such signatures establish choice if controllers enforce their rules;
the API additionally checks consistency with the supplied prepare evidence.
The durable controller must still enforce those rules before emitting each vote.

The new mismatch regression failed against the initially separate chosen verifier
and passes after this API change. Twelve certificate tests and the entire
federation suite pass, totaling 170 tests. Strict all-target Clippy and scoped
formatting pass. The first lint run failed on signature-codec module visibility;
the visibility was corrected without a lint allowance. Logs are recorded in the
decision trail.

This module does not persist or issue votes. Its wire containers are unverified
until checked. Configuration construction does not validate an authority chain,
and chosen payload bytes do not authorize or execute an authority transition.
The next unit must append authenticated local control votes through the existing
journal, with no reply before durable append, then prove restart and legal ballot
recovery. Epoch activation, portable AuthorityService projection, obligations,
custody issuance, native fault tests, and C01-C18 remain open. No fresh Forrest,
Mac, or complete Myko workspace gate is claimed by this checkpoint.

## Durable voter checkpoint

`ControlQuorumVerifier::prepare_request` and
`PreparedControlQuorum::accept_request` construct sealed local requests.
`Node::vote_control` reconstructs the controller's retained promises and accepted
full values under the existing backend lock. It appends a signed
`FrameworkControlEvent::ControlVote` before broadcasting or returning the reply.
An exact retained reply is reused without another append. Volatile nodes refuse
to vote. The key must belong to one durable controller, not independent stores
that can vote concurrently with the same identity.

Replay authenticates matching own-controller votes regardless of the event
wrapper's claimed origin. It rejects conflicting accepted values at one ballot.
The generic ingest path rejects invalid vote signatures before append. Votes
have an exact realm selection and no fake custody obligation or application
command. Retained-history statements still depend on their real obligation.

A fault test found that an append can commit and still return an error, leaving
the live cache behind durable history. Before computing a response, the voter now
compares journal replay with the live cache. A mismatch returns
`NodeError::DurableHistoryChanged` and requires reopening the node. The regression
failed before this guard and passes after it, including recovery of the original
promise on reopen. A failure before append remains retryable without a reopen.
This check currently replays the journal for each control vote. It does not add
that work to ordinary application commands, and it is not online reconciliation.

The nine real Redb voter tests cover persisted promises, full accepted values,
lost replies after majority acceptance, legal competing minorities through
completed recovery and reopen, exact conflict errors, signature rejection, and
foreign-wrapped own votes. The replacement-acceptor test uses an A/B prepare
majority followed by an A/C accept majority, then reopens C and checks its implicit
promise. Independent review initially proposed requiring a separate same-ballot
promise, then withdrew that suggestion after checking this legal Paxos trace.

The final federation, Redb, and wire run passes 210 tests. This includes the
11-test control recording file and four retained-history Redb tests. Strict
all-target Clippy and scoped formatting pass. The native founder-replacement test
also passes, but does not exercise a networked control coordinator. Initial
compile and lint failures, the ambiguous-commit regression, and superseding runs
are recorded in the decision trail.

The wire schema is now version 8 for the new framework record. Peers must be
rebuilt together; nothing has been committed, pushed, or synced to the Mac at
this checkpoint. No production daemon was manually launched.

This is a durable acceptor, not a completed authority protocol. A persistent
proposer must bind each ballot to one proposal and recover without reusing it for
different values. Certified predecessor and epoch activation, controller rotation
fencing, independently provisioned realm anchors, portable AuthorityService facts,
custody obligations and issuance, safe departure, rollback handling, and the native
fault matrix remain required. Raw chosen value bytes still do not authorize or
execute an authority transition. All C01-C18 rows remain open.

## Durable proposer binding checkpoint

The proposer now persists a signed full proposal before issuing an accept request.
`PreparedControlQuorum::proposal_request` retains the verified prepare votes and
required recovery value. `Node::propose_control` binds the slot and ballot to that
value and proof in the existing journal. The former direct
`PreparedControlQuorum::accept_request` constructor has been removed.
`ControlQuorumVerifier::accept_request` checks the proposer signature and complete
prepare proof against its independently supplied slot and electorate.

The original Redb regression demonstrated that one prepared ballot could issue
different values. The persisted binding now rejects that reuse, including after
reopen. An exact retry returns the original signed proposal without appending.
Replay checks every matching retained proposal, including foreign-wrapped records,
so an earlier matching record cannot hide a later conflicting proposal. The same
journal/cache check used by the voter prevents a second proposal after an ambiguous
append until the node reopens.

The affected federation, Redb, and wire suite passes 218 tests, including 14 control
recording tests, 15 certificate tests, and two real Redb proposer tests. Strict
all-target Clippy and scoped formatting pass. The decision trail retains the
original failing regression and the import and redundant-clone lint failures.
The native founder-replacement test also passes after rebuilding the consumers.
That test does not exercise control coordination over the network.

This is durable ballot binding, not automatic ballot allocation or a networked
coordinator. Controller-key exclusivity across stores and rollback fencing remain
operational assumptions. Signed proposal bytes do not prove durable storage to a
remote verifier or authorize an authority transition. Certified epochs, portable
authority, custody issuance, and all C01-C18 requirements remain open. The wire
version is now 9. Peers have not been rebuilt or synced for this version.
