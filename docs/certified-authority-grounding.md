# Certified authority integration

## Required behavior

An independently provisioned realm anchor establishes controller keys, an initial
epoch, and a predecessor head. Imported controller lists cannot establish trust.
A chosen control transition selects exact authority history under that anchor.
Only the selected history can contribute to portable authority evaluation.
The same retained grant must deny before certification and permit after a valid
certified chain is retained and reopened on a replacement node.

This unit must preserve the existing foreign-grant denial test. Copying a database,
following a view, pairing, or retaining an arbitrary grant cannot activate authority.
Ordinary application commands remain local-first. Control transitions and bounded
authority consumption require the exclusive protocol already selected in
`custody-issuance-synthesis.md`.

## Current call paths

`AuthorityPolicy::new` opens `AuthorityFactSources` for the local node ID and realm
scope. Its retained maps feed `AuthorityFacts::with_topology` and then the existing
`evaluate` function. `AuthorityPolicy::current_state` waits for the local source's
authoritative cut before taking that snapshot. The command-side `load_state`
independently reads the same realm through local-origin queries.

For a decision requiring durable effects, `AuthorityPolicy::evaluate` executes
`EvaluateAuthority` through the existing application host. Bounded grant uses,
delegation uses, approvals, challenges, and leases cannot safely move to a portable
read projection while their writes remain uncoordinated local commands. The new
shape must make that distinction explicit rather than returning an unconsumed
permit or consuming a separate local copy of a shared allowance.

`ControlQuorumVerifier` currently takes an external slot and controller list.
It validates keys and evidence, not the authority of that list. A signed proposal
contains the value and prepare proof. `Node::propose_control` persists the proposal,
and `Node::vote_control` persists promises and accepts in the existing journal.
`PreparedControlQuorum::verify_chosen` verifies matching accepts and yields a sealed
`ChosenControlQuorum`. Its content-derived head does not activate authority.

`FrameworkControlEvent` contains retained statements, signed proposals, and signed
votes. These records do not enter application item projections. There is currently
no certified authority chain projector or portable fact-selection policy.

## Design comparison

Compare two complete shapes before implementation:

1. A certified chain selects exact immutable existing AuthorityService event bodies.
2. A certified chain selects typed authority effects and projects those through the
   existing authority domain.

Score each candidate on independently rooted trust, complete binding and replay,
reuse of the existing authority domain, atomic bounded-use semantics, and a small
caller interface that supports epoch changes without a permanent founder.
Reject unsigned origin allowlists, raw all-origin authority projection, snapshot
replacement of history, a second grant database, and self-certified controllers.

## Work sequence

- [x] Read principles, architect, arena, and current integration source.
- [x] Trace retained and command-side authority evaluation.
- [x] Compare two independent candidate shapes and cross-judge them.
- [x] Implement the historical chain boundary and real persistence test.
- [x] Connect historical certified fact selection without weakening raw-foreign denial.
- [x] Verify historical restart, invalid evidence, and explicit unconsumed bounded-use
  assessments with affected-crate checks.
- [ ] Establish current-head readiness, epoch activation, and coordinated consumption
  before connecting live policy.

## Evidence scope

Graph project `myko-7-current`, Tier 2, generation `2026-09-05T01:04:50Z` is stale.
The authority policy, domain, and test paths report changed metadata. The new
control files are untracked by the graph. Direct source reads supersede the graph
for these paths. `facts.rs`, `commands.rs`, and `lib.rs` have matching metadata.
The existing Redb foreign-grant test is in `authority/src/tests.rs`.
The previous proposer checkpoint passed 218 affected-crate tests and one native
continuity test. These results do not prove this new authority integration.

All C01-C18 requirements in `scope-continuity-plan.md` remain Open.
