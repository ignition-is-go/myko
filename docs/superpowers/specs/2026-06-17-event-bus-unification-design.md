# Event Bus Unification — One Apply Pipeline, Explicit Origins

**Status:** Design / proposal (revised)
**Date:** 2026-06-17
**Layer:** myko framework (core `server/context.rs`, `server/relationship_manager.rs`; server `postgres.rs`, `peer_persister.rs`, `ws_handler.rs`)

## Why

The mutation path has grown into **nine near-identical pipelines**, each re-implementing the same steps and differing only on three axes — typed vs type-erased, single vs batch, locally-emitted vs ingested. The cost isn't just duplication: **every cross-cutting concern has to be added in nine places**, and the loop-avoidance rules are scattered across per-call flags, a host-id check in one consumer, and an `EventOptions::from_peer` flag that's defined but never wired.

That scattering is not a stylistic complaint — it has produced and is hiding real correctness bugs. Three of them, established by walking the current code:

1. **The cascade is silently capped at one level.** `prevent_relationship_updates` is set on every cascade product, and `context.rs` gates the *entire* `forward_*` call on it (`del_dyn_with_options` at context.rs:721; batch at :768). So a cascade-deleted child never runs its own cascade — **grandchildren are not deleted at runtime**, only reaped by the startup orphan sweep (`establish_relations` → `cleanup_belongs_to_orphans`). The flag is doing two unrelated jobs: loop-guard (wanted) and depth-cap (an accident).

2. **owns_many orphan removal never happens at runtime.** `forward_set` (relationship_manager.rs:282) does no owns_many work at all — only `index_belongs_to_child` + `handle_ensure_for`. So editing a parent to drop a child from its owned array leaves the now-unreferenced child alive until the next boot's `cleanup_owns_many_orphans`. The relationship attribute promises cascade-delete and orphan-removal; today only the first fires at runtime, and only one level deep (bug #1).

3. **The peer-replication path re-cascades and re-produces.** The DB consumer applies tailed events to the store only (no cascade, no produce). The peer ingest path (`MykoMessage::Event` → `apply_event` → `apply_event_batch_immediate`) instead *re-cascades and re-produces* (context.rs ~1000, ~1019). `EventOptions::from_peer` exists to distinguish this but is never set or checked — two servers peered bidirectionally can echo.

The common root cause: relationship invariants that should hold *continuously* are deferred to the boot-time reconciler, and loop-safety is expressed as scattered per-path flags instead of one explicit policy. Centralizing the apply pipeline is the lever that fixes all three at once.

> **Note on the mutation-index bug (historical):** an earlier framing of this doc cited the restore-point mutation index being wired into only some paths. That has since been centralized — `note_mutation` is called from all four `produce_*` methods (context.rs:1134/1146/1158/1174) and `apply_remote_event` records remote events directly (postgres.rs ~926). The index is now consistent; this redesign keeps it that way structurally rather than fixing it. It is no longer a motivating bug, just confirmation that the produce layer is the right chokepoint.

Goal: collapse the nine pipelines into **one apply path whose loop-safety and relationship effects are explicit and centralized**, fixing the three bugs above, without reintroducing echoes and without losing the typed fast path's performance.

## Current architecture (what exists today)

### The repeated pipeline

Every mutation runs: **reduce** (store insert/remove) → **search** (index) → **relationships** (cascade) → **persist** (produce). Entry points (all in `core/src/server/context.rs`):

| Entry point | World | Shape |
|---|---|---|
| `set_with_options` / `del_with_options` | typed `T: Eventable` | single |
| `batch_set_with_options` / `batch_del_with_options` | typed | batch |
| `set_dyn_with_options` / `del_dyn_with_options` | erased `Arc<dyn AnyItem>` | single |
| `batch_set_dyn_with_options` / `batch_del_dyn_with_options` | erased | batch |
| `del_by_id_with_options` | erased | single |
| `apply_event_batch` → `apply_event_batch_immediate` | erased (JSON→item) | batch, **ingest** |

The body is structurally identical; only the reduce front-end legitimately differs (typed `Arc` insert vs JSON parse vs per-type batched diff). **Reduce always removes from the store before the cascade runs** (e.g. `del_dyn_with_options`: remove at :715, cascade at :722) — this ordering is load-bearing for the fix below.

### The produce layer

`produce_set` / `produce_del` / `produce_set_dyn` / `produce_del_dyn` (context.rs:1132-1182) build an `MEvent` stamped with this host's `host_id` as `source_id`, call `note_mutation`, then fan to:
- **persisters** (`persisters.resolve(type).persist(event)`) — Postgres, `BlackholePersister` (ephemeral), or `PeerPersister` (peer-replicated),
- the optional **`event_sink`** (peer broadcast).

`prevent_persist` skips this whole method — i.e. it skips *both* the persister and the sink; there is no "persist but don't broadcast."

### The consumer

`postgres.rs::run_consumer_loop` bootstraps then tails via LISTEN/NOTIFY, calling `apply_remote_event` per row. That function **applies to the store only** (+ records the mutation index) — no cascade, no produce — and skips its own events via `source_id == host_id` (postgres.rs:919).

### The relationship cascade (today)

- `belongs_to` maintains reverse membership indexes (`belongs_to_children_by_parent`, `belongs_to_parent_by_child`); cascade-delete finds children by FK scan (`find_children_by_fk`).
- `owns_many` maintains **no membership index**; `find_parents_containing` (relationship_manager.rs:712) is a full `store.entries()` scan. Cascade-delete enumerates children from the parent's id array (`extract_ids`, :584). On child delete it rewrites parent arrays (`handle_owns_many_child_delete`); on child *set* it does nothing.
- All cascade products are published with `prevent_relationship_updates: true` (`publish_*_cascade`, :1104-1168), which is the only recursion guard — no depth counter.

## The loop-avoidance invariants (must survive)

1. **`source_id` / `host_id`** — stamped at `produce_*`, checked by the consumer so a host doesn't re-apply its own DB-tailed events. Breaks `persist → NOTIFY → apply → persist → ∞`.
2. **The consumer never `produce`s.** Tailed events are applied, not re-emitted. The originating host's cascade products are *also* in the log and arrive on their own; re-cascading on apply would double them.
3. **Recursion termination.** A cascade must not loop on a cyclic schema (A→B→A) or re-process an already-handled node. Today this is the blanket `prevent_relationship_updates`; below it becomes the natural store-as-visited-set.
4. **Per-type durability** — ephemeral entity types skip the durable backend via `BlackholePersister` at the persister router, *not* via a per-event flag. (`prevent_persist` was the per-event version; it is dead — see "Dead flags".)
5. **Reduce batching** — the ingest path emits one store diff per type per op, not per item. Any unification must keep this accumulator.

## Proposal

### One post-reduce pipeline, keyed by `Origin`

The nine pipelines differ, for loop-safety purposes, only in **where the event came from** and **what op it is**, which determine two booleans: *should it cascade?* and *should it produce?* Unify at the **post-reduce** boundary so the typed fast path keeps its direct `Arc` insert (no JSON round-trip):

```rust
enum Origin {
    /// A command handler / server module emitting a new mutation here,
    /// OR a client event ingested over the WebSocket (not yet durable here).
    Local,
    /// A relationship cascade product — a consequence of another mutation here.
    Cascade,
    /// An event that originated elsewhere and is ALREADY durable elsewhere
    /// (DB tail, or a peer-replicated event). Apply only.
    Remote,
}
```

> **No `Local { produce: false }` variant** — see "Dead flags" below. `prevent_persist` is never set anywhere, so a store-only-but-local origin would have zero callsites. The only real "don't durably store this" need is *per entity type* (ephemeral entities), and that is already served by `BlackholePersister` at the persister-router layer (`persister.rs:174`, `:203`) — orthogonal to `Origin`. `Local` therefore always produces.

```rust
// Specialized, kept for performance — the only legitimate per-entry difference:
fn reduce_typed<T: Eventable>(&self, item) -> Arc<dyn AnyItem>   // direct Arc insert
fn reduce_erased(&self, item: Arc<dyn AnyItem>)                  // JSON-parsed insert
fn reduce_batch(&self, ...)                                      // per-type accumulated diff

// One shared tail every entry point calls after its reduce:
fn apply_effects(&self, item: &Arc<dyn AnyItem>, change: MEventType, origin: Origin) {
    self.search_index.update(item, change);
    self.note_mutation(item, change);            // mutation index — ONE place
    if origin.should_cascade(change) { self.relationship_manager.forward(item, change, self); }
    if origin.should_produce()       { self.produce(item, change); }
}
```

The nine entry points shrink to: pick a `reduce_*`, then call `apply_effects(.., origin)`. Every loop gate and cross-cutting concern lives in exactly one function.

### The effect table (corrected)

`should_cascade` depends on **op as well as origin** — this is the key correction. A DEL cascade product *must keep cascading* (that's the grandchild fix); the owns_many array-fixup SET must not.

| Origin | reduce | search | index | cascade | produce | notes |
|---|---|---|---|---|---|---|
| `Local` — command / client WS event | ✓ | ✓ | ✓ | ✓ | ✓ | client events cascade + persist here; they arrive as `reduce_erased`. Per-type durability is the persister router's job, not this column. |
| `Cascade` (DEL product) | ✓ | ✓ | ✓ | **✓** | ✓ | **transitive** — descends to grandchildren; terminates via store-visited-set |
| `Cascade` (owns_many SET array-fixup) | ✓ | ✓ | ✓ | ✗ | ✓ | only trims a parent's id array; no structural descent |
| `Remote` — DB tail / peer-replicated event | ✓ | ✓ | ✓ | ✗ | ✗ | skip entirely if `source_id == host_id` |

Each loop-break is now one explicit cell:
- `Remote` doesn't produce → DB-tail and peer echoes both broken (fixes bug #3).
- `Remote` skips own `source_id` → DB-tail self-echo.
- `Cascade` termination is no longer "don't descend" — see below.

### Fix #1: transitive cascade via store-as-visited-set

The reason the blanket flag was "safe" was that it never descended. We can descend safely **for free** because of the reduce-before-cascade ordering:

- All cascade finders read the store — `find_children_by_fk` (relationship_manager.rs:483), `find_parents_containing` (:712). Reduce has already *removed* the node from the store before its cascade runs.
- So the store is a **monotonically shrinking visited set**: a node reached again by a cyclic schema is already gone and finds nothing. Transitive delete terminates with no depth counter and no separate visited set.
- The "parent resurrection" hazard (a cascade-deleted child's `handle_owns_many_child_delete` re-SETting its parent) is already avoided: the parent was reduced out of the store first, so `find_parents_containing` returns nothing for it.

Concretely: **drop `prevent_relationship_updates` from `publish_del_cascade*`** (let DEL products run `forward_del`), and **keep it only on `publish_set_cascade*`** (the owns_many array-fixup, which must not descend). In `Origin` terms, `should_cascade(change)` is `true` for `Cascade` + DEL and `false` for `Cascade` + the SET fixup.

### Fix #2: runtime orphan removal — on `belongs_to`, not `owns_many`

The original instinct here was to give `owns_many` a reverse membership index so orphan removal (child unreferenced by any parent → child deleted) fires at runtime instead of only at boot. **That plan is dropped** in light of the `owns_many` deprecation (below): building net-new index infrastructure for a feature we're removing is the wrong investment.

Instead, runtime orphan removal lands on `belongs_to`, the pattern we're keeping — where it's *simpler*, because the child's FK is already authoritative and `belongs_to` already maintains the reverse indexes (`belongs_to_children_by_parent` / `belongs_to_parent_by_child`):

1. A `belongs_to` orphan is a child whose parent FK points at a parent that no longer exists. Today this is reaped only by `cleanup_belongs_to_orphans` at boot.
2. The reverse index already tells us, on a parent DEL, exactly which children dangle — and Fix #1 already makes that DEL cascade transitively. So "delete the parent → children cascade-deleted → grandchildren too" is the runtime mechanism; the boot sweep demotes to a backstop with no new index needed.
3. The remaining boot-only case (a child written with an FK to a never-existent parent) can be closed at runtime by checking parent existence on the child's SET path inside `apply_effects` — cheap, and it reuses the index `belongs_to` already has.

For `owns_many` specifically: **do not** add the reverse index or runtime orphan removal. Leave its existing boot-time `cleanup_owns_many_orphans` sweep as-is for the deprecation window, and steer all new ownership modeling to `belongs_to`.

### `owns_many` is deprecated (remove eventually)

`owns_many` is an antipattern and is being phased out. The reasons are exactly the soft spots this redesign keeps tripping over:

- **Denormalized + drift-prone.** The parent holds an `id` array that is the supposed source of truth, but the framework only ever *removes* from it (on child DEL) — it **never adds on child create** (`forward_set` does no owns_many work). So membership is maintained half by the framework, half by application code, and drifts.
- **Needs a boot-time sweep to stay correct.** Because additions aren't managed and the array can drift, correctness leans on `cleanup_owns_many_orphans` at startup — eventual consistency where the relationship should be invariant.
- **Cascade enumeration rides the array**, so it's only as complete as the array is current (vs `belongs_to`, which scans authoritative FKs).

**Replacement:** `belongs_to` on the child. Invert ownership — instead of `Scene { node_ids: Vec<Id> }` with `#[owns_many(BindingNode)]`, the child carries `BindingNode { scene_id }` with `#[belongs_to(Scene)]`. The child FK is authoritative, cascade is an FK scan (no array to drift), orphan-removal is "FK dangles," and there is nothing to maintain on create. This is the pattern the rest of this design optimizes for.

**Deprecation mechanics (separate, follow-up work — not part of this refactor):**
- Add a `#[deprecated]`-style compile-time warning emitted by the `#[owns_many]` arm of the `myko_item` macro (`macros/src/relationship.rs`), spanned at the user's attribute.
- Mark it deprecated in the macro rustdoc (`macros/src/lib.rs`), the relationship module docs, and CLAUDE.md's relationship-attributes section; document the `belongs_to`-inversion migration.
- **Caveat:** the project builds clippy with `-D warnings`, so a hard `#[deprecated]` warning becomes a build error for any *downstream* consumer (e.g. rship) still using `owns_many`. There are **zero** in-repo `#[owns_many]` usages, so this repo is unaffected, but coordinate with downstream before turning the warning on — or land it behind a deprecation that downstreams can `#[allow(deprecated)]` during migration.

### Fix #3 + the WS client/peer split (the `from_peer` reframing)

`MykoMessage::Event` is a **single ingest path carrying two different kinds of event** (ws_handler.rs:1127):
- **Client-emitted** (the log says "from client"; `client/mod.rs:888`): not yet durable here, and the client sent only the root mutation — no cascade products. This must **cascade and produce** → `Origin::Local{produce:true}`.
- **Peer-replicated**: already durable and already cascaded at the origin → `Origin::Remote`.

So the handler needs a **discriminator** to tell them apart — which is exactly what `EventOptions::from_peer` was meant to be. It is therefore **unfinished, not dead**: revive it (or derive it from the connection type at the ws boundary) and thread it into `Origin`. Routing all WS ingest to `Remote` (as an earlier draft implied) would *drop client mutations on the floor* — they'd never persist or propagate.

> **Open question — transitive peer relay.** `Remote` not producing also stops a host from *re-broadcasting* a peer's event to its other peers. Under a full mesh that's purely the echo fix. If any deployment relies on multi-hop relay (A↔B, B↔C, C not peered with A), `Remote` needs a "relay to peer sink without cascade/persist" sub-case. **Confirm the peer topology before implementing step 4 below.**

### Where each caller lands

- Command `emit_set`/`emit_del`, batch, dyn, reconcile/server-ownership rebakes, **and client WS events** → `Origin::Local`.
- `publish_del_cascade*` → `Origin::Cascade` (DEL — descends). `publish_set_cascade*` (owns_many fixup) → `Origin::Cascade` (SET — does not descend).
- `apply_remote_event` (DB tail) **and** peer-replicated `MykoMessage::Event` → `Origin::Remote`.

### Dead flags (delete, don't migrate)

Confirmed by grep across the whole repo — these are read but never written, so they carry no behavior and should be removed rather than folded into `Origin`:

- **`EventOptions::prevent_persist`** — checked in 11 places (context.rs), set to `true` in **zero**. Its documented purpose (skip durable backend for backend-sourced events) is handled structurally by `Origin::Remote`; per-type ephemerality is handled by `BlackholePersister`.
- **`EventOptions::from_peer`** — defined and TS-exported, never set or checked. The peer/client discrimination it was meant to provide is reintroduced properly in Fix #3.
- **`MEvent::from_item_with_options`** (wire/event/mod.rs:96) — the only constructor that populates `MEvent.options`; never called. Every `produce_*` emits `options: None`, which is *why* both flags above are dead even on the wire. Remove it (and consider dropping `MEvent.options` entirely once the ingest stops reading it).

## Migration plan

1. **Pure refactor (behavior-preserving):** add `Origin` + `apply_effects`; re-express existing methods in terms of it, keeping `Cascade` non-descending for now. Verify against the existing suite.
2. **Fix #1 — transitive cascade (behavior change):** drop `prevent_relationship_updates` from `publish_del_cascade*`, keep it on `publish_set_cascade*`. Add tests: 3-level `belongs_to` delete (grandchild gone *without* restart); 3-level `owns_many` delete; cyclic-schema termination; owns_many parent-delete with no parent resurrection.
3. **Fix #2 — runtime `belongs_to` orphan removal (behavior change):** make dangling-FK children delete at runtime via the existing reverse index + transitive DEL cascade, demoting `cleanup_belongs_to_orphans` to a backstop. Tests: delete a parent → children and grandchildren gone without restart; child written with an FK to a non-existent parent → removed on its SET. **Do not** build owns_many machinery here.
4. **Fix #3 — WS client/peer split (behavior change):** revive `from_peer` (or connection-type discriminator); route client events → `Local`, peer events → `Remote`. Add a peer↔peer echo test; resolve the transitive-relay open question first.
5. **Cleanup:** fold typed/dyn/batch entries onto the shared tail; delete duplicated bodies; **delete the dead flags** (`prevent_persist`, `from_peer`, `from_item_with_options`) — they are read-only with no producers, so removal is behavior-preserving (regenerate TS bindings after).
6. **Deprecate `owns_many` (separate follow-up, not gating the refactor):** emit the macro deprecation warning + docs + CLAUDE.md note, with the `belongs_to`-inversion migration. Coordinate with downstream (`-D warnings`) before flipping it on. The eventual *removal* of `owns_many` is out of scope here — this only marks it.

## Invariants checklist (regression guard)

- [ ] DB tail does not re-persist this host's own events (`source_id == host_id` skip).
- [ ] DB tail / peer apply never `produce` (no `persist → tail → persist`, no `peer → peer` echo).
- [ ] **Client** WS events still cascade and persist (not mis-routed to `Remote`).
- [ ] Cascade terminates on a cyclic schema with no depth counter (store-visited-set).
- [ ] **DEL cascades reach grandchildren at runtime** (not deferred to boot).
- [ ] owns_many array-fixup SET does **not** trigger structural descent (while owns_many still exists).
- [ ] **`belongs_to` orphans (dangling FK) are deleted at runtime**, not only at boot.
- [ ] No new owns_many infrastructure added (it is deprecated, not extended).
- [ ] Ingest still emits one store diff per type per op (reduce batching preserved).
- [ ] Mutation index + search updated on **all** applied mutations, local and remote, in one place.
- [ ] Typed local writes still avoid a JSON round-trip.

## Non-goals

- Changing the wire format, the persister abstraction, or the reactive store/cell layer.
- Removing the typed world — typed `Eventable` stays for compile-time safety and the fast reduce path.
- Multi-hop peer relay semantics beyond resolving whether it is required (see open question).
- *Removing* `owns_many` — this design only marks it deprecated and steers new work to `belongs_to`; the actual removal and any downstream migration are separate.
