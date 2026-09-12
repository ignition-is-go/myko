# Failover startup protocol design task

## Shared delegate instructions

Work directly in `/home/trevor/Code/myko-7`. You are not alone in this checkout.
Do not revert other edits. Each candidate owns only its assigned Markdown file.
No production or test edits, Cargo runs, worktrees, daemons, commits, pushes, or
child agents. Read the architect runner prompt and rationale template supplied
by the parent. Return at most 800 words, with concrete type signatures and source
references. The parent owns implementation and verification.

## Required result

Design the history-boundary protocol for a stable subscription to recover through
another assigned application executor. The operator requires a fresh authenticated
history boundary confirmed by the scope's coordinator majority before replacement
`Current`. Missing majority or incomplete history means desync and blocked
dependent commands. A hot standby can avoid cold catch-up, but invisible failover
requires proven continuity. Storage-only nodes retain opaque logs, not typed
queries. Do not invent ongoing per-read quorum or application-owned freshness.

The first milestone includes a real native transport, persistent history, the
same public handle, no intermediate `Current`, and no stale-dependent mutation.
Write caller usage first, then types, owning modules, protocol, alternatives, and
one executable next step. Distinguish agreed behavior from a missing operator
decision. Prefer reusing the current log and control protocol to a parallel store.

## Grounding

Graph project `myko-7-current`, generation `2026-09-05T01:04:50Z`, Tier 2.
Index status is ready, but relevant files are changed or not tracked. Searches
for assignment coordinator, selected manifest, commitment, and control transitions
returned no current symbols. HandlerConnector's old snippet is displaced and its
zero call edges are not absence evidence. Parent checked coverage for every file
below and read current source as fallback. Use source, not stale graph lines.

- `core/src/server/execution_coordinator.rs`: `observe` chooses a fresh no-change
  control operation after recovering accepted control values. `choose` uses
  persisted prepare, propose, and accept records. Refresh imports only the
  assignment-control realm. The result is historical assignment configuration.
- `core/src/server/execution_control.rs`: authenticated principal-to-proposer
  bindings precede caller-bound control-realm evidence refresh and signing.
- `federation/src/execution_assignment/observation.rs`: the signed payload binds
  only operation, realm, and predecessor. It has no application scope history.
- `federation/src/execution_assignment/controller.rs`: controllers validate that
  payload family and predecessor, not application-history completeness.
- `federation/src/selected.rs`: a selected manifest proves closure of one local
  event set. Empty or locally closed history is not remote completeness.
- `federation/src/commitment.rs`: a portable hash binds selection and immutable
  event identities/bodies, excluding observer-local positions. It is not a
  completeness, authority, or durability certificate.
- `federation/src/node.rs`: `PreparedCommand::submit` calls the local backend.
  Command authorization and local causal closure do not themselves establish
  remote durable acknowledgment. Inspect any stronger acceptance path before
  claiming the coordinator knows all accepted application history.
- `core/src/client/durable_handler.rs`: retained owners already keep one prepared
  request and retry through a connector. Local cursors cannot compare two nodes.
- `iroh/src/client.rs`: `IrohHandlerConnector` still dials one fixed peer.
- `core/src/server/federated_session.rs`: `follow_handler` checks authorization
  and optional activated-schema identity, without an assignment/history gate.
- `iroh/tests/execution_coordination/observations.rs`: a reusable three-node native
  persistent fixture proves fresh control observations and missing-quorum refusal.

All paths above are relative to `libs/myko/`.

## Counterexample every candidate must address

A accepts an application write that B and C have not retained. A becomes
unavailable. B and C can agree on identical closed local history and a fresh
control observation. Why does the proposed startup certificate not label their
incomplete history `Current`? A quorum signing caller-supplied hashes is not an
answer. State the precise write-acceptance, durable coverage, or prior-boundary
relation that rules this out. If it needs an undecided guarantee, identify it
instead of silently choosing that guarantee.

Compare at least one structurally different alternative, such as coupling the
accepted history frontier to quorum durability versus a fenced transfer from an
existing authority. Do not merely rename the observation payload.
