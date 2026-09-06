# Certified authority selection

## Decision

Use the records candidate. Its projector consumes the existing AuthorityService
domain rather than introducing parallel grant, revocation, and use-effect types.
Graft the effects candidate's self-contained chosen value and narrow historical
read interface. The selection includes origin, recorded time, and the complete
immutable event body. Observer-local replay positions do not enter signed bytes.

The independent gpt-5.5 judge scored records 7/10 and effects 5/10 across trust,
immutable replay, domain reuse, bounded-use safety, and interface size. Both
candidates left freshness and retry identity incomplete. Neither draft is a live
authority protocol as written.

## First implementation

```rust
let anchor = AuthorityAnchor::new(realm, epoch, genesis, controller_keys)?;
let history = AuthorityHistory::replay(&replacement_node, anchor)?;
let assessment = history.assess_at(head, &attempt, time, topology)?;
```

The anchor is provisioned independently of imported records. Replay reconstructs
chosen transitions using that anchor's electorate, the signed full proposal,
embedded prepare quorum, and matching accept majority. Each transition extends
the previous content-derived head. Chosen value bytes select exact retained
AuthorityService commits and their command-lifecycle dependencies. Missing or changed bodies cannot be replaced by the
copy embedded in a proposal.

The historical projection uses the existing `ItemProjection` and authority
evaluator. Its result type is not `AccessPolicy`. It records whether a decision
would require a certified authority effect, but it neither consumes an allowance
nor releases a live permit. The existing policy remains local-origin and never
silently falls back from missing certified evidence to raw foreign facts.

The first implementation uses a static controller epoch. It must not expose a
`Current` constructor based on the highest head retained locally. A chain can
prove historical authority without knowing that another node has a later
revocation. Epoch rotation and a protocol for current-head readiness are still
required for live use and founder-free controller replacement.

## Retry and consumption constraints

Reject reused selection-operation identities across the certified chain, not only
within one predecessor slot. Selected event and command identities cannot acquire
another meaning under a later head. Immutable use and audit IDs cannot be replaced
or deleted by later selected records.

This is not complete bounded-use consumption. A future stable consumption identity
must bind realm, original command/effect identity, request binding, and phase.
The transition's expected predecessor remains separate from that retry identity.
Advancing to another head cannot authorize a second consumption for the same
effect. That corrects both candidates' per-predecessor identity proposals.

## Verification

The fresh pre-change foreign-grant baseline passes in
`/tmp/myko-certified-authority-baseline.log`. The new test must close the founder,
retain control evidence and exact authority records on a successor, reopen it,
and obtain a historical permit while live local policy still denies.
Missing bodies, changed timestamps, insufficient accept votes, and an unrelated
anchor electorate must fail. Further chain-conflict and semantic tests are
required before treating this as verified.

The implementation checkpoint below records the superseding test results. Custody,
live authority, epoch rotation, full workspace validation, and all C01-C18 remain open.

## Integration correction

The first positive run failed because the selected commits lacked their lifecycle
parents. `CommandContext::prepare_batch` includes `command.updated_at` in every
batch's causal parents. The selection must therefore include same-realm lifecycle
records, preserve their exact immutable bodies, and order records using Myko's
existing causal dependency rules. Multiple lifecycle records may share a command
ID. Only repeated commits or inconsistent command requests are conflicts.
The authority projector ignores lifecycle records after the chain validates them.
This supersedes the drafts' committed-record-only selection.

The initial fixture also omitted installing the local authority policy and failed
before exercising certification. It now installs the policy for source commands,
then removes that policy reference before dropping the founder. Reopening the
founder store confirms the test did not silently retain its live journal.

## Verified historical checkpoint

Eight integration tests now pass. The Redb fixture chooses a grant head and a
successor revocation head, closes the founder store, and reopens the successor.
Historical assessment permits at the grant head and denies at the revocation head.
Live local policy denies the foreign grant throughout. Bounded historical grants
carry an explicit uncommitted-consumption flag and do not mutate the journal.

Negative tests cover missing selected bodies, changed recording time, insufficient
accept votes, an unrelated anchor electorate, and operation-ID reuse across heads.
Unenrolled proposals do not poison a valid head. Independent review found that a
signed malformed duplicate prepare proof could overwrite valid evidence for the
same head. The regression failed before the fix and now passes with either arrival
order. Valid proof dominates malformed duplicate evidence.

The final affected authority, federation, Redb, and wire suite passes 244 tests.
Strict all-target Clippy, scoped formatting, and diff whitespace checks pass.
The native founder-replacement regression also passed earlier in this unit. It
does not test the certified authority protocol over the network. The decision
trail preserves the fixture, missing-parent, poisoning, compile, and lint failures.

`causal_replay` is now exported from federation for the certified reader to reuse
the existing dependency rules. `ControlSlot::head_for` owns the head-byte contract
used by both chosen-quorum and authority code. No new wire record or second
persistence system was introduced. Wire version 9 remains unsynced.

The independent reviewer approved this bounded historical implementation. There is
no cross-origin item-scope fault-matrix claim, current-head protocol, epoch rotation,
consumption certificate, live policy integration, or custody issuance yet. No fresh
Forrest or complete Myko workspace gate is claimed. Nothing was committed, pushed,
or synced, and no production daemon was launched.
