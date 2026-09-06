# Certified authority consumption

## Required behavior

A bounded grant with one remaining use must not authorize two distinct effects
through competing controllers. Retrying the same effect after lost replies,
restart or controller rotation must recover the same decision and use identity.
A different request cannot reuse that identity. A revocation chosen before an
authorization decision must prevent a stale controller from issuing a new permit.
An old historical permit is not a reusable current-access credential.

The accepted log remains the truth. Reuse the existing control journal, certified
chain, evaluator and authority use/audit types. Do not introduce another receipt
database, duplicated authority rules, fake application commands for framework
control, or a quorum requirement for ordinary convergent application writes.
The authority decision may coordinate an exclusive allowance; that does not make
the eventual external effect exactly once or justify blindly retrying it.

## Current implementation

`AuthorityPolicy::evaluate` reads its local-origin authority projection. It returns
pure decisions for continuation and unbounded cases. Bounded consumption,
challenges and leases enter `EvaluateAuthority` through a trusted framework command.
That command reloads local authority state, runs `evaluate`, emits GrantUse,
DelegationUse and ApprovalUse records according to authorization phase, records
any challenge/lease, and appends DecisionAudit. It returns a decision before the
outer effect commits. Its decision and use identifiers are random per invocation.

`AuthorizationBinding::from_request` binds principal, executor, provenance,
operation, command identity/type, resources, capabilities, argument and effect
digests and effect topology proof. A stable retry identity must additionally keep
different authorization phases distinct. A changing retry predecessor is not an
effect identity. Query/read admissions without a command ID need an explicit
request identity; do not invent one from a changing connection or timestamp.

The live command path has a crash gap before that binding is stable.
`CommandContext::prepare_batch` allocates a new BatchId and captures the executing
command event plus current causal predecessors. `commit_bytes` hashes the entire
batch, actual claims/capabilities and result, calls `policy.decide`, then commits.
Only the challenge branch parks the exact batch in AuthorizationPending. Certified
consumption therefore needs durable preparation before authorization and recovery
of that prepared body, not another handler run with a new batch or fewer causal
bindings. Ordinary non-consuming writes must retain their local-first behavior.

AccessAttempt topology is skipped by serde. Any certified request codec must
explicitly bind the required topology evidence and preserve its trusted origin;
deserializing a client's asserted topology must not grant it authority.

`evaluate` currently creates random challenge and lease IDs. Replay of a chosen
intent therefore requires deterministic identity inputs or a validated recorded
outcome. Choosing a client-supplied past evaluation time must not bypass expiry.
Clock assumptions and lease behavior must be explicit.

`AuthorityHistory` now reconstructs exact selected records and typed authority
state through generic certified controller rotations. `AuthorityController`
checks predecessor and proposed payload validity, then binds its replay snapshot
at the atomic durable signing boundary. It does not yet re-evaluate an effect
against the predecessor, coordinate a request, authorize a remote issuer call,
or prove network-wide freshness. Choosing precomputed local use records alone
does not establish at-most-once consumption of a shared allowance.

## Design fork

Compare a request-specific authorization transition chosen directly in the
existing control chain with a fresh read-barrier plus reservation protocol.
Both designs must explain how stale controllers encounter a previously chosen
revocation or rotation, how they recover an already accepted competing value,
and where the application verifies and spends the returned result. Neither may
rename a latest retained head to current or return a historical assessment as
an AccessPolicy permit.

Require caller-first types and a migration path for existing local evaluation,
deterministic uses/audits, challenge and lease behavior, and real durable tests.
Choose the design with the smallest interface that contains these invariants,
not the one with the smallest passing historical-only test.

## Work sequence

- [x] Read current policy, command, evaluator, binding and certified issuer paths.
- [x] Complete independent traced explanation and compare two designs.
- [x] Synthesize a request/decision contract with independent review.
- [ ] Implement certified evaluation, stable consumption and retry recovery.
- [ ] Verify competing effects, duplicate and mismatched retries, revocation,
  restart and rotation through durable controllers.
- [ ] Connect live decision enforcement and verify its actual caller path.
- [ ] Run affected gates and audit remaining scope-continuity gaps.

## Evidence and limitations

Source: authority/src/policy.rs:307, commands.rs:467, evaluator.rs:581 and :645,
facts.rs:191, domain.rs:69 and :181; federation/src/authority.rs:452;
authority/src/certified/history.rs and issuer.rs. Paths are under libs/myko.
Graph project myko-7-current has generation 2026-09-05T01:04:50Z. The targeted
search returned EvaluateAuthority, requires_durable_evaluation and
AuthorizationBinding. Changed/untracked coverage required direct reads. The
graph's policy caller edge was useful; outbound standard-method names resolved
to unrelated code and were not used as evidence.

Previous checkpoint: 258 affected tests, strict Clippy, formatting and one native
continuity regression pass. It proves historical controller handoff, not certified
consumption or live authority. All C01-C18 remain Open. Work stays in existing
myko-7 and forrest checkouts, without running the production daemon.
