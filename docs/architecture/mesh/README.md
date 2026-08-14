# Myko Mesh — Architecture Reference

**Status:** Normative reference · **Created:** 2026-07-26 · **Source design:**
[`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md`](../../superpowers/specs/2026-07-25-myko-mesh-node-architecture.md)

---

## What these documents are

The design spec argues the mesh into existence: it weighs alternatives, records why each decision was
made, and cites the code that motivated it. It is the document to read when you want to know *why*.

These documents are the complement. They state, without argument, **what an implementation must do**:
byte layouts, algorithms, type definitions, state machines, and numbered invariants that tests can be
written against. Rationale appears only where it prevents a reader from "simplifying" a constraint
that is load-bearing.

**The design spec remains authoritative on intent.** If a normative statement here contradicts the
spec, the spec wins and this document is a bug. Section references of the form `spec §8.4` point back.

## Invariant numbering

Every document defines invariants with a document-scoped prefix. `NM-3` is the third invariant in
`01-node-model.md`. They exist so the test suite (spec §16) and the conformance vectors (spec §9.6)
can cite them, and so a reviewer can ask "which invariant does this violate?" and get an answer.

Invariant IDs are **stable and never reused**. A withdrawn invariant is struck through in place.

## Reading order

| # | Document | Answers |
|---|---|---|
| 01 | [Node model](01-node-model.md) | What is a node? What may it claim, and what follows from the claim? |
| 02 | [Type identity and schema](02-type-identity-and-schema.md) | How is a type named across services and versions? Where does field metadata come from? |
| 03 | [Record format](03-record-format.md) | The byte-level state-change record, the content hash, HLC encoding |
| 04 | [Merge semantics](04-merge-semantics.md) | How two records for one entity become one state; CRDT shapes; OCC |
| 05 | [Scopes and capabilities](05-scopes-and-capabilities.md) | Multitenancy, the authorization boundary, the management plane |
| 06 | [Transport and planes](06-transport-and-planes.md) | The transport contract, planes, handshake, envelopes, the gateway protocol |
| 07 | [State and log](07-state-and-log.md) | The two storage subsystems, their indexes, retention, checkpoints |
| 08 | [Replication and subscriptions](08-replication-and-subscriptions.md) | Anti-entropy, gossip, query-driven replication, hydration, bootstrap |
| 09 | [Commands and routing](09-commands-and-routing.md) | Dispatch, consistency modes, optimistic execution, idempotency |
| 10 | [Crate layout and migration](10-crate-layout-and-migration.md) | Where the code goes; what moves; wasm gating; the dyn-dispatch budget |

01–04 are the substrate: nothing else is implementable without them. 05–09 are the mesh proper. 10 is
the map from these documents onto the repository.

## Start here if you are new to this work

[**Handoff — 2026-07-30**](../../superpowers/handoffs/2026-07-30-myko-mesh-architecture-and-m1.md).
It covers branch state, what is verified versus assumed, the M1 investigation and its three
corrections, cross-repo context, and repo conventions. Read it before editing anything here.

## Implementation plans

Architecture says what to build; plans say in what order, with what exit criteria.

- [Mesh roadmap](../../superpowers/plans/2026-07-26-myko-mesh-roadmap.md) — all 14 phases, gated
- [Phase 1 — item field schemas](../../superpowers/plans/2026-07-26-mesh-phase-1-item-field-schemas.md)
- [Phase 2 — benchmarks, simulation harness, M1](../../superpowers/plans/2026-07-26-mesh-phase-2-benchmarks-and-sim-harness.md)
- [Phase 3 — the wire break](../../superpowers/plans/2026-07-26-mesh-phase-3-wire-break.md)

## The v1 scope, in one paragraph

**Mesh peers are native Rust processes** — servers and desktop applications. They speak QUIC (iroh
binding), gossip, anti-entropy, and hold scopes authoritatively. **Everything else attaches through a
Gateway** over WSS: browsers, polyglot services, devices, scripts. An attached node is a real node —
local store, local projections, optimistic execution, an outbox — but its sole peer is its gateway,
and it has no routable mesh identity. Browser peering and polyglot peering are designed
(spec §10.2, §10.3) and **not built**; nothing in v1 depends on either existing.

This is why cross-language conformance splits in two tiers (03 §7), why the recommended binding can
use `iroh-blobs` for bulk transfer, and why the relay question does not gate the browser story.

## Status of the open items

| Item | Blocks | State |
|---|---|---|
| **M1** — resident-memory amplification | the local-first story, and sizing any `Stateful` node | **Resolved 2026-07-27.** The memory is **genuinely live** and concentrated in **derived/query cells** — ~254 KB/item at rack scale. Not allocator retention (rack runs jemalloc; RSS/live 1.33×). See [`M1-findings.md`](M1-findings.md). Fixes live outside this repo: rship `lv-fc26`, hyphae PR #20, myko `lv-4a87`. |
| **M2** — gossip topic count per scope | nothing; a measurement | Open |
| **M3** — iroh FFI gossip exposure | nothing under the v1 scope | Moot unless polyglot peering is pursued |
| **Q1** — `Handler` without state | nothing; likely a placement question | Open |

**M1's resolution makes [01 NM-8](01-node-model.md) stronger, not weaker.** A node's memory cannot be
predicted from its filter — and the reason is worse than "unmeasured": at rack scale the cost is
~254 KB/item and its independent variable is **live derived-cell count**, which filter cardinality
does not determine. **No document here may be used to size a deployment**, and the claim that a narrow
filter bounds memory remains unproven until the derived-cell term is fixed upstream.
