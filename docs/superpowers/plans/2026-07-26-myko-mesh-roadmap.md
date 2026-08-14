# Myko Mesh — Implementation Roadmap

**Date:** 2026-07-26 · **Branch:** `feat/iroh-integration` · **Status:** Planning

**Spec:** [`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md`](../specs/2026-07-25-myko-mesh-node-architecture.md)
**Architecture:** [`docs/architecture/mesh/`](../../architecture/mesh/README.md)

---

## What this document is

The architecture set says *what* to build. This says **in what order, with what exit criteria, and
what happens if a phase's result contradicts the design.**

Fourteen phases. Three have detailed task-by-task plans (phases 1–3, linked below); the rest have
deliverables, exit criteria, and dependencies here.

> **Detailed plans are written when a phase is next, not now.** A task-level plan for phase 12 written
> before phases 3–5 exist would be fiction — it would specify against a store, a log, and a routing
> table that do not have shapes yet. The exit criteria below are what make each phase's plan writable
> when its turn comes.

## Sequencing principles

1. **Land every wire break in one phase** (10 CL-12). Each break is a migration for live consumers,
   and the wire break is the least reversible step in the plan.
2. **Measure before committing the format.** Phase 2 exists because phase 3 cannot be undone.
3. **Design storage layouts in, never retrofit.** The log ships indexed and compacted (07 SL-13); the
   Merkle index ships keyed per `(item_type, scope)` (05 SC-21). Both retrofits are rewrites.
4. **Highest-risk migration last.** The `ws:m:*` cutover touches every client port and nothing depends
   on it, so it goes at the end.
5. **Validate on a real workload before release.** Internal demo services first, then a scoped rship
   migration, then coordinated release.

## Dependency graph

```
   0  prereqs ─────────────────────────────────────────────────────┐
      │                                                            │
   1  field schemas + merge mapping                                │
      │                                                            │
   2  benchmarks + sim harness + M1  ◄── M1 DONE; bench+sim not started │
      │                                                            │
   3  THE WIRE BREAK  (records, HLC, OCC, qualified types, scope)   │
      │                                                            │
      ├──► 4  split state from log ──┬──► 12 time travel           │
      │                              │                              │
      └──► 5  state store + scopes ──┴──► 8  NodeScoped ──► 9 QDR ──┤
              │                                    │                │
              └──► 6 manifests ──► 7 planes ───────┴──► 10 routing  │
                                                          │         │
                                                          └──► 11 conflicts + offline
                                                                    │
                                                        13 rship migration + perf
                                                                    │
                                                        14 gateway cutover ◄──────┘
```

---

## Phase 0 — Prerequisites

**Status:** mostly landed. **Blocks:** everything.

| Deliverable | State |
|---|---|
| Single apply chokepoint: `apply_event_batch` → `emit_grouped` → `apply_effects` | **Landed** (PR #25) |
| `Origin::Remote` apply mode | **Regressed — must be restored.** Only `Local \| Cascade` remain (`server/context.rs:59`); wire-ingested events currently apply as `Local`, cascading and producing. Restored with the planes in phase 7 (10 CL-15). |
| `feat/iroh-dataplane` disposition | **Rewrite, do not land.** Wall-clock whole-entity LWW does not match 04. Its two survivable ideas reappear as 03 RF-3 and 04 MG-25 (10 CL-16). |

**Exit:** the apply chokepoint is the only path into the store, and the team has agreed
`feat/iroh-dataplane` is not merging.

---

## Phase 1 — Item field schemas and merge-strategy mapping

**Plan:** [`2026-07-26-mesh-phase-1-item-field-schemas.md`](2026-07-26-mesh-phase-1-item-field-schemas.md)

Additive macro change. It precedes the wire break because it determines **what the record must
carry**.

**Deliverables:** `FieldSchema` on `ItemRegistration` (02 TI-12); `field_id` as collision-checked
FNV-1a (02 TI-14); merge strategy selected from the declared type (04 MG-8); `namespace` derived from
`module_path!()`'s first segment plus the override (02 TI-3, TI-4); `#[myko_field]` attribute
(02 TI-15).

**Exit criteria:**

- Every `#[myko_item]` type emits a complete field schema, verified by a test that walks `inventory`
  and asserts the schema matches the struct for a representative entity set.
- A field-name collision within a type is a **compile error** naming both fields.
- `namespace` equality replaces `codegen/mod.rs`'s substring filter; `lv-ea59` closes.
- **No behavioural change.** `cargo test` and the rship build both pass unmodified.

**Risk:** low. Purely additive; nothing reads the new data yet.

---

## Phase 2 — Benchmarks, simulation harness, and M1

**Plan:** [`2026-07-26-mesh-phase-2-benchmarks-and-sim-harness.md`](2026-07-26-mesh-phase-2-benchmarks-and-sim-harness.md)

**This phase is a gate, not a task.** Two things must come out of it before phase 3 may start.

> **M1 is DONE** (2026-07-27). Deliverable 1 below is complete; deliverables 2 and 3 are not started.
> The answer: **the memory is genuinely live and concentrated in derived/query cells**, ~254 KB/item
> at rack scale — *not* allocator retention (rack runs jemalloc, RSS/live 1.33×) and not primarily
> `source_rows`. Evidence and the full correction history:
> [`M1-findings.md`](../../architecture/mesh/M1-findings.md).

**Deliverables:**

1. ~~**M1 resolved**~~ — **DONE.** Harness at `libs/myko/core/examples/amplification.rs`; reproduced
   on main @ `0ed566f9`. Remaining fixes are **upstream of this repo**: rship `lv-fc26`, hyphae
   PR #20, myko `lv-4a87`. Nothing in this roadmap blocks on them.
2. **Synthetic rship-shaped benchmark** measuring record size, merge-metadata overhead, typed-store
   ingest, and field-addressed encode/decode against today's JSON path (10 CL-18). **Not started.**
3. **`myko-sim`** — the deterministic simulation harness (spec §16): simulated transport, seeded
   scheduling, fault injection for partition, duplication, reorder, delay, and clock skew.
   **Not started.**

**Exit criteria:**

- [x] **M1 has an answer**, and the design consequence is recorded in 01 NM-8 and 10 CL-20. The
      outcome was the **first** fork — *inherent amplification, not a sweep bug* — so: 08 §4's filter
      model is load-bearing rather than an optimization, browser nodes need hard subscription budgets
      (phase 9), and a disk-backed store would not help, because the memory is not in the store.
- [ ] Field-addressed encoding is **measured**, not assumed, against the current path.
- [ ] `myko-sim` can run a two-node partition-and-heal scenario deterministically from a seed.

**Why the harness lands here and not with phase 3:** convergence properties are unfalsifiable as
claims and cheap as seeded property tests. Landing the harness first means **the wire break arrives
with its properties**, rather than acquiring them later.

**M1 came back "inherent", so this applies:** phase 3 still proceeds — the record format does not
change — but **phase 9's scope grows** (subscription budgets, eviction policy) and the local-first
story must be re-scoped before phase 13 promises it to anyone.

---

## Phase 3 — The wire break

**Plan:** [`2026-07-26-mesh-phase-3-wire-break.md`](2026-07-26-mesh-phase-3-wire-break.md)

**Everything at once** (10 CL-12): the three-layer encoding, HLC with tiebreak and drift bound,
per-field merge metadata, OCC preconditions, opaque payload, `NodeId` origin, qualified `item_type`,
scope id, `actor`, explicit tombstones, envelope moves, and the record rename.

**Deliverables:** `Record` and `EntityState` (03, 04 §2); the merge algorithm (04 §3) including
PN-Counter, ORSWOT, and LWW-Map; two-tier conformance vectors (03 §7); the migration converter
(10 CL-13).

**Exit criteria:**

- Tier-1 vectors pass in Rust; the vector directory is consumable by a non-Rust binding without a Rust
  dependency (10 CL-3).
- Tier-2 hash vectors pass in Rust.
- Merge determinism, per-field independence, and **precondition-travel** properties pass under
  `myko-sim` seeds. (Precondition-travel is the falsifier for apply-time OCC: two replicas receiving
  the same two records in opposite orders must end identical. OCC *read-set tracking* is phase 5 —
  it needs the store's read seam.)
- The converter round-trips a copy of a real Postgres log, and is idempotent.
- Bench numbers from phase 2 are met or the deviation is explained and accepted.

**Rollback:** none after release. This is the point of no return; that is why phase 2 gates it.

---

## Phase 4 — Split state from log

**Depends on:** 3. **Blocks:** 12.

Independent retention paths and the role bits of 01 §2.

**Deliverables:** `myko-log` crate; the `(scope, type, id) → time-ordered versions` index (07 SL-14);
per-`(key, field)` compaction (07 SL-17); merge metadata in the log (07 SL-18); checkpoints
(07 §6); the two retention policies as separate configuration (07 SL-19); the local-only writer
(07 SL-7, SL-8).

**Exit criteria:**

- The log is **indexed and compacted from the start** — verified by a test that compacts a
  multi-field write history and asserts latest-per-`(key, field)` survives.
- Replay from a compacted log reconstructs state whose subsequent merges are correct (07 SL-18) — not
  merely whose values look right.
- The five local-only operations (07 §3) all route through `local_only`, and a test asserts each emits
  nothing.
- Startup warns when no unbounded-retention node is configured (07 SL-23).

**Risk:** medium. Retrofitting the index or compaction later is a rewrite (07 SL-13), so the exit
criteria are strict on purpose.

---

## Phase 5 — State store and scope partitioning

**Depends on:** 3. **Blocks:** 6, 8.

**Deliverables:** the typed store holding `EntityState` keyed `(qualified_type, scope, id)`
(07 SL-9, SL-10); per-field merge wired into the apply path; OCC read-set tracking in
`CommandContext` (04 §5); tombstones and GC (04 §6); **a Merkle index keyed per `(item_type, scope)`**
with scope-intersection negotiation (05 SC-21, SC-22); scope in the header end to end (05 §2);
cross-scope reference rejection (05 SC-6); eviction as a local-only purge (05 SC-24).

**Exit criteria:**

- Scope isolation properties pass: anti-entropy never transfers data for an unserved scope, eviction
  emits no records, cross-scope references are rejected.
- A read-then-written field rejects on precondition mismatch; a blind write does not; a rejected write
  surfaces a retryable error.
- Merkle roots are per `(item_type, scope)` — **not per type with scope retrofitted** (05 SC-21).

**Risk:** high. This is where the store's shape is fixed. **Do not build a per-type Merkle index and
retrofit scope; that is a rewrite.**

---

## Phase 6 — Manifests and membership

**Depends on:** 5.

Schema discovery and a routing table **with no routing semantics yet** — the table is built and
observable, but `execute_command_job` does not consult it.

**Deliverables:** `NodeManifest` derived from inventory (01 NM-15); manifest gossip; the routing table
(09 §3); schema compatibility comparison (02 §3) with per-type verdicts.

**Exit criteria:**

- The manifest is built entirely by walking `inventory`; nothing in it is hand-maintained.
- Two nodes with skewed crate versions pair successfully, replicate compatible types, and report the
  incompatible one with both crate versions (02 TI-9, TI-14).
- Unknown-field retention round-trips through a skewed pair with stable content hashes on both sides
  (02 TI-10) — this is the test that catches the anti-entropy churn hazard.

---

## Phase 7 — ALPN planes

**Depends on:** 6. **Restores:** `Origin::Remote` (phase 0's outstanding item).

`myko/mesh/1` first. This makes peer participation real.

**Deliverables:** `myko-mesh` transport trait and `myko-iroh` binding (10 CL-2); the handshake
(06 §4); intern tables (03 RF-7, 06 TP-15); framing and envelopes (06 §5); **`Origin::Remote`
restored and driven by the plane** (04 MG-16, 06 TP-9); anti-entropy sessions over the plane
(08 §2).

Design `myko/serve/1` against 06 TP-22's envelope, **not** as a port of `MykoMessage`.

**Exit criteria:**

- A record arriving on the replication plane applies with no cascade, no produce, and no saga — tested
  by asserting a parent DEL replicated to a peer produces zero child DELs on that peer (04 MG-17).
- Declaring a plane and then rejecting its stream terminates the connection (01 NM-17).
- An unknown intern id fails the record rather than being guessed (06 TP-15).
- Two-node partition and heal converges under `myko-sim` with conflict detection firing (08 RP-11).

---

## Phase 8 — `NodeScoped` (D1)

**Depends on:** 5. **Blocks:** 9, and the optimistic half of 10.

**Start with the un-gate spike** (10 CL-7). The spike produces a real error list, and **the phase's
scope is set from that list**, not from the architecture document.

**Deliverables:** `MykoServerContext` → `MykoNodeContext`, `ServerScoped` → `NodeScoped`; the module
un-gated for wasm with only genuinely native internals gated; absent subsystems as `Option` fields
returning `Result`/`Option` instead of `unreachable!()`; `Viewing`/`PeerAccess`/`Replaying` created
for wasm (10 CL-6); the scheduler seam (10 CL-8); `CommandHandler::execute` real on wasm.

**Exit criteria:**

- `cargo check --target wasm32-unknown-unknown -p myko` passes.
- A command handler executes on wasm against a local store and returns a real result.
- **No new `dyn` on the capability call path** (10 CL-4, CL-17) — verified by the phase-2 benchmark
  re-run, not by inspection.

**Risk:** medium-high, and **the risk is unquantified until the spike runs.**

---

## Phase 9 — Query-driven replication

**Depends on:** 8. **This is what makes browser nodes viable.**

**Deliverables:** materialization filters (01 §4); the filter derived from live subscriptions
(08 RP-15); the union-is-the-unit store-once/send-once path (08 RP-16, RP-17); per-predicate
hydration gates (08 RP-24); the exactness ladder (08 RP-18); bootstrap and warm start (08 §6, §7).

**Exit criteria:**

- The first evaluation of a newly registered predicate **blocks until backfill completes**; a store
  never serves a predicate it has not finished hydrating (08 RP-25).
- An entity matching three of a peer's live predicates is stored once and sent once (08 RP-16).
- Cancelling one of two matching subscriptions does not evict the entity (08 RP-17).
- A node stale past the tombstone window discards and cold-bootstraps rather than reconciling — tested
  for a **live** partitioned node, not only a restarting one (01 NM-20).

**M1 came back "inherent", so this is now in scope, not conditional:** this phase also delivers hard
subscription budgets per attached node and an eviction policy under memory pressure. Note the budget's
unit is **live derived cells**, not entities — that is what the rack measurement showed dominates.

---

## Phase 10 — Command routing

**Depends on:** 7 (transport) and 8 (optimistic execution).

**Deliverables:** ingress routing at `execute_command_job` (09 CR-13); consistency modes and routing
keys (09 §6); node-addressed dispatch (09 §3); edge-owned direct publish (09 §7); idempotency dedup
(09 §8); hop limits and visited sets (09 CR-15).

**Exit criteria:**

- Ingress routes by `(command_id, scope)`; nested same-service commands stay in-process; hop limits
  terminate loops.
- A retried command returns the **recorded** result; a side-effect-marked handler never executes twice
  (09 CR-36).
- Only the owner direct-writes an owned entity; a non-owner direct write is a protocol error
  (09 CR-29).
- A sticky command with no resolvable routing key **fails to compile** (09 CR-24).

---

## Phase 11 — Conflict recording and offline

**Depends on:** 10.

**Deliverables:** the command outbox (09 CR-18); the provisional overlay as a real store layer
(07 SL-12, 09 CR-19); partition-heal detection during anti-entropy (04 MG-28); owned-entity replay
detection (04 MG-29); replicated heal summaries (04 MG-31).

**Exit criteria:**

- A superseded prediction produces **no conflict record**; the overlay drops atomically; sagas never
  fired on provisional records (09 CR-17, CR-20).
- Offline replay detects its own losses via the outbox; partition heal detects during anti-entropy;
  the heal summary replicates while the detail does not.
- An owned-entity replay conflict is recorded as an **ownership violation**, distinctly from an
  ordinary conflict (04 MG-29).

---

## Phase 12 — Time travel

**Depends on:** 4 (the log index) and 10 (routing).

Routed historical reads first, then `RestoreEntityTree`.

**Deliverables:** `ListVersions`, `GetTreeAsOf`, `RestoreEntityTree { root, as_of, mode }`; the
transient historical read path (08 RP-42); as-of closure computation (08 RP-43); cascade suppression
(08 RP-45); the `RESTORE` / `RESTORE_EXACT` rights split (05 SC-11).

**Exit criteria:**

- A historical read **never lands in the local store** (08 RP-42) — tested by asserting the store is
  byte-identical before and after a `GetTreeAsOf`.
- Exact-mode restore does not delete entities inside the computed closure (08 RP-45).
- The closure is upward-closed, checked before emission (08 RP-46).
- In-gap reads fail structurally because no peer advertises coverage (01 NM-9).

---

## Phase 13 — Scoped rship migration and perf validation

**Depends on:** 11.

A scoped rship migration for **real-workload** performance testing, then a coordinated release. rship
is myko's perf-validation consumer; myko-side benches alone have not been conclusive before.

**Exit criteria:**

- rship runs on the mesh stack for the migrated scope with no regression against the phase-2 baseline.
- A flamegraph confirms the capability-path dyn budget (10 CL-17) held.
- M1's answer is re-verified against the real deployment. (Already done once at rack scale on 2026-07-27
  — re-verify that the upstream fixes, rship `lv-fc26` and hyphae PR #20, actually moved the number.)

---

## Phase 14 — Gateway cutover, retiring `ws:m:*`

**Depends on:** 13. **Nothing depends on this.**

**Deliverables:** `ws_handler` and `autosocket` **re-pointed** to carry 06 TP-22's envelope for the
Gateway role (06 §6) — not deleted; attached nodes migrated off `ws:m:*`; `MykoMessage` deleted; the
14-variant subscription protocol collapsed (06 TP-22).

**Exit criteria:**

- A browser attaches over WSS, holds a local store, subscribes, executes optimistically, and syncs
  through its gateway.
- A gateway-attached non-Rust node round-trips records and commands with correct merge results
  (tier-1 conformance).
- Gateway failover: an attached node survives losing its gateway and reconciles on reconnect
  (01 §6).
- `MykoMessage` no longer exists in the tree.

**Risk:** highest migration risk in the plan — every client port changes protocol. Sequenced last for
exactly that reason. It is a protocol migration on an existing transport, not a transport replacement
(06 TP-28).

---

## Cross-cutting: the test suite

The properties below run inside `myko-sim` from phase 2 onward, each landing with the phase that makes
it meaningful.

| Property | Lands in |
|---|---|
| Merge determinism; strategy selection is deterministic | 3 |
| Per-field independence; OR-Set adds both stick; counters sum | 3 |
| OCC: read-then-written rejects, blind writes do not | 5 |
| Scope isolation; eviction emits nothing; cross-scope refs rejected | 5 |
| Log contiguity: a gap disqualifies the range; checkpoint opens a new one; in-gap reads fail | 4 |
| Recovery: state rebuilds from a compacted log with merge metadata intact | 4 |
| Conformance: every binding reproduces the tier-1 vectors | 3 (Rust), 14 (bindings) |
| Routing: ingress by `(command_id, scope)`; hop limits terminate | 10 |
| Idempotency: retried command returns the recorded result | 10 |
| Hydration gate: first evaluation blocks until backfill completes | 9 |
| Warm start: a stale node refuses writes; past the window it cold-bootstraps | 9 |
| Edge ownership: only the owner direct-writes | 10 |
| Rebase: no conflict record; overlay drops atomically; sagas never fire | 11 |
| Conflict recording: outbox detection, heal detection, summary replicates | 11 |
| Polyglot: a gateway-attached non-Rust node round-trips correctly | 14 |

---

## Open items, and what they gate

| Item | Gates | Resolved by |
|---|---|---|
| **M1** — resident-memory amplification | the local-first story; sizing any `STATEFUL` node; phase 9's scope | **RESOLVED 2026-07-27.** Memory is genuinely live in derived/query cells, ~254 KB/item. Fixes are upstream (rship `lv-fc26`, hyphae PR #20). Phase 9's scope grows as a result. |
| **M2** — gossip topic count per scope | nothing | Phase 2, cheaply |
| **M3** — iroh FFI gossip exposure | nothing under the v1 scope | Moot unless polyglot peering is pursued (06 §9) |
| **Q1** — `Handler` without state | nothing; likely a placement question | Phase 10, if it comes up |

## Related levi tasks

- `lv-4a87` (P1, perf) — query/view cache memory scales with materializations × source size, not
  matches. **Quantified at ~208 B/(N·P)** and reproduced on main — but scoped to views/reports/
  hand-written operator chains; generated queries mostly bypass hyphae's `MapState`. Contributes to
  M1; does not dominate it. Still open.
- `lv-ea59` (P2, codegen) — codegen crate filter uses substring match on `module_path`, over-matching
  sibling crates. **Closed as a side effect of phase 1** (10 CL-11).
