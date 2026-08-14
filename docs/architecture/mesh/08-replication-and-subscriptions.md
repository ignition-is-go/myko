# 08 — Replication, Subscriptions, and Time Travel

**Normative.** Source: spec §5.3, §12, §14, §15.2–15.7. Invariant prefix `RP`.

---

## 1. Three convergence mechanisms, ranked

| Mechanism | Role | If it stops |
|---|---|---|
| **Anti-entropy** (§2) | **authoritative** — periodic Merkle comparison and repair | the mesh stops converging |
| **Gossip** (§3) | a **latency optimization** on top of anti-entropy | convergence gets slower, nothing breaks |
| **Query-driven replication** (§4) | how *filtered* nodes get data at all | filtered nodes cannot serve their own projections |

> **RP-1** — **Anti-entropy is the authoritative convergence path.** Even if every gossip message were
> dropped, periodic anti-entropy converges the mesh. No design decision may make correctness depend on
> gossip delivery.

RP-1 is what makes the polyglot-peer extension cheap (06 §9) and what lets gossip be tuned
aggressively for latency without risking correctness.

## 2. Anti-entropy

### The tree

> **RP-2** — One Merkle tree per **`(qualified_type, scope)`** (05 SC-21). Never per type alone.

> **RP-3** — A **leaf** hashes `(entity_id, content_hash, entity_hlc)` where `content_hash` is 03
> RF-18 and `entity_hlc` is the maximum HLC across the entity's field entries and its tombstone.
> Including the HLC lets a descent report *which side is ahead* without fetching the entity.

> **RP-4** — Leaves are placed by the first bytes of `BLAKE3(entity_id)`, giving a balanced tree
> independent of id shape. Interior nodes hash their children's hashes in fixed child order. Fanout is
> 16; depth is bounded by the type's cardinality.

> **RP-5** — **Empty types cost no tree.** A `(type, scope)` with no entities has no root and is
> omitted from the root exchange. This is why 156 declared types is not 156 trees — in the measured
> `rack` deployment, 107 types are unpopulated and 10 types hold 97% of records.

> **RP-6** — Tombstoned entities **remain leaves** until GC (04 MG-25). A node that has GC'd a
> tombstone and a node that never saw the entity are indistinguishable — which is exactly why 01
> NM-20 forces a cold bootstrap past the GC window rather than a reconcile.

### The session

```
Initiator                                            Responder
    │                                                    │
    │── ScopeIntersect { scopes[] } ────────────────────►│   RP-7
    │◄─ ScopeIntersect { scopes[] } ─────────────────────│
    │                                                    │
    │        for each shared scope, both complete:       │   RP-8
    │── Roots { (type, scope, root_hash, count)[] } ────►│
    │◄─ Roots { ... } ───────────────────────────────────│
    │                                                    │
    │        for each differing root:                    │
    │── Descend { type, scope, path } ──────────────────►│
    │◄─ Children { hashes[] } ───────────────────────────│
    │              … until leaf divergence …             │
    │                                                    │
    │── Fetch { type, scope, entity_ids[] } ────────────►│
    │◄─ RecordBatch (03 records, full state) ────────────│   RP-9
    │                                                    │
```

> **RP-7** — **Sessions negotiate the scope intersection first** and reconcile only shared scopes
> (05 SC-22). A node never accepts repair data for a scope it does not serve (05 SC-23) — checked at
> the receiver, independent of what the sender believed.

> **RP-8** — **Both sides must be complete** for a `(service, scope)` to compare it (01 NM-7). A node
> that is filtered there skips the pair silently; it converges by §4 instead. Filtered-vs-complete
> comparison would report permanent, legitimate divergence.

> **RP-9** — Repair transfers **full entity state**, not deltas. A delta for an entity the receiver
> may not hold is exactly 03 RF-16's unapplicable case, and anti-entropy is where that case is most
> likely.

> **RP-10** — Repair applies as `Origin::Remote` (04 MG-16): merge and index, **no cascade, no
> produce, no saga**. A repair that cascaded would re-delete children the originating node already
> tombstoned, on every node that repaired.

> **RP-11** — **Conflict detection lives here.** When an incoming value beats a *locally originated*
> one, that is a partition-heal conflict (04 MG-28): record the detail locally, and contribute to the
> replicated heal summary (04 MG-31).

## 3. Gossip

> **RP-12** — **One gossip topic per scope** (05 SC-27), carrying record batches for realtime
> dissemination. Membership is per topic, so partitioning falls out of topic membership rather than
> needing a filter.

> **RP-13** — Gossip delivery is **at-least-once with duplication**, and overlaps anti-entropy and
> pairwise streams. This is precisely why merge must be idempotent (04 MG-5) and why op-based CRDTs
> are ruled out (04 MG-10).

> **RP-14** — Gossip requires iroh-gossip and therefore Rust, native or wasm. Under the v1 scope this
> is the only role bit with a language qualifier, and it applies to **peers only** — gateway-attached
> nodes receive realtime updates from their gateway over WSS (06 §6) and never join a swarm.

**M2** measures topic-count scaling: a node serving 1000 scopes holds 1000 HyParView/PlumTree
memberships. Cheaply testable; blocks nothing.

## 4. Query-driven replication

> **RP-15** — **The filter is derived, not declared.** A node's filter is **the union of its live
> subscriptions**, maintained automatically.

Every typed read already passes through the capability seam the framework can see:
`query_map` and its variants (`libs/myko/core/src/core/capability.rs:129`), `view` (`:387`), `report`
(`:245`), and `exec_query` (`core/command/handler.rs:81`) all take structured params.

This bounds memory by **working-set size rather than tenant size**, which is what makes browser nodes
viable regardless of organization size — subject to M1 (01 NM-8).

### One store, N derived views

> **RP-16** — **The union is the unit: an entity matching many overlapping predicates is stored once
> and sent once.**

RP-16 is what distinguishes this from result-set replication, where each subscription streams its own
results and an entity matching three queries arrives three times. Today's WS protocol is the latter;
this is not.

- **Stored once.** Query membership is *derived* by evaluating each predicate over the store —
  precisely what hyphae already does incrementally. Query results are views holding references, not
  copies.
- **Sent once.** When an entity changes, the serving node asks "does this match any of the peer's live
  predicates?" — one check against the union, one send, regardless of how many of the peer's queries
  match.

> **RP-17** — **Refcounting governs eviction, not storage.** The refcount counts *matching
> predicates*, so cancelling one subscription does not evict an entity another still matches. It never
> counts copies, because there are none.

This is less new machinery than it appears: myko already evaluates queries over `CellMap`s with
incremental diffs and pushes them to subscribers. **The change is that results land in a real local
store other projections can run over**, rather than being consumed by one subscription.

### The exactness ladder

> **RP-18** — **The union is exact only for predicates that are data.** The degradation ladder is
> explicit, and every rung **over-fetches; none under-serves**:

| Predicate shape | Registers as | Why |
|---|---|---|
| Generated `XQuery` filters | **exact predicate** | canonicalized, serializable structures with a per-item `matches` |
| Hand-written `test_entity` | **whole-type subscription** | arbitrary code; not a per-item data predicate |
| `build_view` join plans | **whole-type subscription** for each input type | membership depends on *other* entities |
| `registry()` / `search()` consumers | **complete nodes only** | no predicate exists to register |

> **RP-19** — **Report interest means the report's *inputs***, recorded at the capability seam during
> the report's first materialization — **not** read from hyphae's dependency graph. That graph is
> runtime-only, weak, and unlabeled: built for glitch-free invalidation, not introspection.

> **RP-20** — Two known registration gaps, both resolving to "route instead" (§5): `switch_map`-nested
> queries register only after the first tick, and `registry()` / `search()` record nothing.

### Three paths bypass the query hook

| Path | Hazard |
|---|---|
| `Searching::search()` (`capability.rs:232`) | full-text index to ids, no predicate — **benign**: the follow-up lookup registers |
| `entity_snapshot` (`server/context.rs`) | point lookup by id — **benign**, and trivially expressible as a predicate |
| `RegistryScoped::registry()` (`capability.rs:80`) | raw store access by runtime-determined type name — **a correctness hazard** |

> **RP-21** — **`registry()` cannot be policed at the capability seam.** The relationship manager does
> not use the capability at all: it reads the `ctx.registry` field directly
> (`libs/myko/core/src/core/relationship/relationship_manager.rs:481` et seq.). `query_snapshot`
> (behind every handler's `exec_query`), search-index maintenance, and belongs_to bucket backfill share
> the same complete-store assumption.

> **RP-22** — **Completeness is the actual guard, not the seam.** A parent DEL executed on a filtered
> node would emit child DELs for *only the children that node happens to hold* — silently
> under-applying, with no error and nothing detectable locally. 09 §2 removes this by construction:
> authoritative execution happens only on nodes complete for the scope, so a cascade always walks a
> complete graph.

Deletion is the most visible instance of a general problem: handlers validate against state, and *any*
validation against a filtered view is silently wrong.

> **RP-23** — Query-driven replication **relocates rather than removes the RAM question.** Something
> must still evaluate subscriptions against the complete set, so a `STATEFUL` node holding the whole
> scope must exist. The hard-bounded case (browsers) is solved; the server case becomes a machine you
> can size — after M1.

## 5. Hydration gates and placement

### Registration must gate the first evaluation

Between registering a predicate and finishing its hydration from the serving peer, the local store
holds a subset — and nothing in the store layer knows it. `select`, `snapshot`, and `exec_query` all
compute happily over whatever is present, and `StoreRegistry::get_or_create` makes a missing type
indistinguishable from an empty one.

> **RP-24** — **Registering a predicate returns a per-predicate readiness gate, and the first
> evaluation blocks on it.** Same shape as the per-scope gate of 01 NM-19. Without it, the
> registration window returns exactly the silent-incomplete results this model exists to prevent.

```rust
pub struct Registration {
    pub predicate_id: PredicateId,
    /// Resolves when backfill for this predicate has completed and indexed.
    pub ready: ReadinessGate,
}
```

> **RP-25** — A store MUST NOT serve a predicate it has not finished hydrating. This is a property
> test (spec §16), not a convention.

### The placement decision

> **RP-26** — Projections **stop being protocol and become a placement decision** — the same handler
> code runs locally or remotely. Commands and events stay on the wire: commands have side effects and
> need validation at a node that owns them; records *are* the replication substrate.

> **RP-27** — **Coverage is not the question — cost is.** Because every typed read passes through the
> query hook, evaluating a query *registers* its predicate, which extends the filter, which brings the
> data. A projection is never silently under-served (01 NM-6).

> **Decision rule: register-and-materialize when the predicted result set is small and reused; route
> when it is large or one-shot.**

Routing wins when:

- **Selectivity is low** — replicating a million rows to compute ten inverts the economics.
- **Computation is shared** — `report_cache` and `compute_gates`
  (`libs/myko/core/src/server/context.rs:246,249`) exist so N subscribers share one computation.
  Local-only evaluation multiplies that by subscriber count.
- **Cold start dominates** — a first-time predicate must replicate before it can answer.

> **RP-28** — **Subsumption is an optimization, not a correctness requirement.** It avoids re-fetching
> when `age > 10` is registered after `age > 5`. Getting it wrong over-fetches; it never produces
> wrong answers.

## 6. Bootstrap

> **RP-29** — **Bootstrap a state node from an existing `STATEFUL` peer**, not from the log:
> O(current state) rather than O(history), with merge metadata already resolved.

```
snapshot at a watermark  →  subscribe to the live tail  →  readiness gate held closed
                                                            until both are current
```

Anti-entropy alone would converge eventually, but from empty it degenerates to "pull everything."

> **RP-30** — **A filtered node bootstraps by declaring its subscriptions** and receiving matching
> entities from a peer whose filter subsumes its own (01 NM-6).

> **RP-31** — Bulk transfer for snapshot and backfill uses the bulk path of 06 TP-8 — `iroh-blobs` in
> the recommended binding, the bulk plane in a non-iroh one. Attached nodes bootstrap over the
> gateway's WSS carrying the same `ServeMsg` envelope (06 TP-23).

## 7. Warm start and staleness

> **RP-32** — **Persisted state carries a watermark and is stale until reconciled, never
> authoritative.** On restart the mesh has moved on.

> **RP-33** — **Catch-up comes from peers**, via anti-entropy from the watermark. A node's own log
> records only changes *it* wrote, so replaying it does not catch up.

> **RP-34** — **The readiness gate blocks writes, not merely reads** (01 NM-19). A stale read is
> temporary; a stale write is permanent loss.

> **RP-35** — **Past the tombstone GC window, discard and cold-bootstrap** — and this binds live nodes
> too, not only restarting ones (01 NM-20, 04 MG-26).

> **RP-36** — Readiness is **per scope**. A node caught up on org 5 serves org 5 while org 12 still
> syncs. This applies to browser nodes equally.

## 8. Offline operation

> **RP-37** — **Offline nodes accept writes.** A node may be its own state-durability target for
> locally-originated writes, holding them in a **local outbox** and replaying on reconnect — commands
> in the general case (09 §5), records for edge-owned entities (09 §7).

> **RP-38** — The accepted risk is **surfaced, not prevented**: unsynced local writes are lost if the
> device is lost, and clearing site data discards both pending writes and unresolved conflicts. This
> shows as a pending-write count. Preventing it would mean refusing offline writes, which is the thing
> being enabled (07 SL-34).

> **RP-39** — The outbox does double duty: it is also what makes offline conflict detection possible
> (04 MG-29, MG-30).

## 9. Time travel

### Restore is a forward write, never a rewind

> **RP-40** — Restoration reads state as of *T* and **re-writes it with a current HLC**. Merge handles
> it natively, no distributed coordination is needed, and the restore appears in the log as an
> ordinary write. This is a payoff of 07 SL-1: because state is primary and the log derived, restore
> is *just a write*.

Contrast 07 SL-32: **recovery** replays in place preserving original timestamps. Restore and recovery
must not share code paths carelessly.

### Inspection is a routed read

> **RP-41** — History lives on `LOGGED` nodes, so **windback routes** — the paradigm case for §5's
> placement decision. Two requirements: the manifest advertises contiguous ranges so a requester can
> pick a covering peer (01 NM-14), and the log is indexed by `(scope, type, id)` (07 SL-14).

> **RP-42** — **Historical reads return transient projections and MUST NOT replicate into the local
> store.** The store is keyed `(type, scope, id)` and holds *current* state; a historical version
> shares that key. Through the replication path it would either be rejected as older — correct but
> useless — or, if re-stamped to land, **silently perform a restore instead of a preview**. History
> travels on a separate read path. This is a safety constraint, not a preference.

### Restoring an entity tree

> **RP-43** — **Tree membership changed over time.** The closure is computed *as of T*, walking
> relationships in the **historical** state, not the current one.

| Mode | Behavior | Risk |
|---|---|---|
| **Merge** | SET the T-closure to T-state; leave newer entities alone | non-destructive, but not "how it was" |
| **Exact** | SET the T-closure *and* DEL anything not present at T | genuinely restores; **destroys post-T work** |

> **RP-44** — **Mode is a required parameter.** Defaulting to Exact would be a data-loss footgun.
> Exact additionally requires the `RESTORE_EXACT` right (05 SC-11).

> **RP-45** — **Cascades MUST be suppressed.** Exact mode emits DELs, and `#[belongs_to]` cascades a
> parent DEL to children while `#[owns_many]` deletes them outright (07 §2). Through the normal path
> the cascade rule fires *on top of* the computed closure and deletes entities the restore intended to
> keep. The restore has already computed the exact desired end state, so it applies as an
> **authoritative batch with cascade rules suppressed** (07 SL-7).

> **RP-46** — **The closure must be upward-closed.** Restoring a child whose parent is absent leaves
> an orphan the next cascade evaluation may remove. Checkable before the batch is emitted, and
> checked.

> **RP-47** — Restore is **atomic locally, eventually consistent globally**. Batch emission is
> first-class; there is no distributed transaction.

Scope containment holds naturally via immutable binding (05 SC-5) — **assert it anyway**.
Concurrency is handled by 04 §5's preconditions.

### Shape and UX

`RestoreEntityTree { root, as_of, mode }` is a **framework-provided command**, so 09 §2 applies:

> **RP-48** — It routes to a node **complete for the scope** that can also reach a `LOGGED` peer
> covering `as_of`. Completeness is doubly required: the T-closure is computed by walking
> relationships, and a filtered node would compute a truncated tree.

| Operation | Cost | UI moment |
|---|---|---|
| `ListVersions { root, before?, limit }` | cheap — index scan | render the timeline immediately |
| `GetTreeAsOf { root, as_of }` | expensive | preview on selection |
| `RestoreEntityTree { root, as_of, mode }` | a write | on confirm |

### Attribution

> **RP-49** — **Attribution needs an identity, not a node.** Three separate concepts, all required:
> **node** (`origin`), **connection** (`client_id`), and **identity** (`actor`, 03 §3). Audit, undo
> attribution, and sub-scope filtering (05 SC-19) all need the third, and only the third.

---

## Invariant index

| ID | One line |
|---|---|
| RP-1 | Anti-entropy is authoritative; gossip is an optimization |
| RP-2 | One Merkle tree per `(qualified_type, scope)` |
| RP-3 | Leaf hashes `(entity_id, content_hash, entity_hlc)` |
| RP-4 | Leaf placement by `BLAKE3(entity_id)`; fanout 16 |
| RP-5 | Empty types cost no tree |
| RP-6 | Tombstoned entities remain leaves until GC |
| RP-7 | Negotiate the scope intersection first |
| RP-8 | Both sides must be complete to compare |
| RP-9 | Repair transfers full entity state, not deltas |
| RP-10 | Repair applies as `Remote` — no cascade, no produce |
| RP-11 | Partition-heal conflict detection lives in anti-entropy |
| RP-12 | One gossip topic per scope |
| RP-13 | Gossip is at-least-once with duplication |
| RP-14 | Gossip is Rust-only and peer-only |
| RP-15 | The filter is derived from live subscriptions |
| RP-16 | Union is the unit: stored once, sent once |
| RP-17 | Refcounting governs eviction, not storage |
| RP-18 | Exactness ladder — over-fetch, never under-serve |
| RP-19 | Report interest is recorded at the seam, not read from hyphae's graph |
| RP-20 | Two registration gaps resolve to "route instead" |
| RP-21 | `registry()` bypasses the capability seam entirely |
| RP-22 | Completeness is the guard, not the seam |
| RP-23 | Filtering relocates the RAM question; it does not remove it |
| RP-24 | Registration returns a readiness gate; first evaluation blocks |
| RP-25 | Never serve an unhydrated predicate |
| RP-26 | Projections are a placement decision, not protocol |
| RP-27 | Coverage is never the question; cost is |
| RP-28 | Subsumption is an optimization, not correctness |
| RP-29 | Bootstrap from a `STATEFUL` peer, not the log |
| RP-30 | Filtered nodes bootstrap by declaring subscriptions |
| RP-31 | Bulk uses `iroh-blobs` / the bulk plane |
| RP-32 | Persisted state is stale until reconciled |
| RP-33 | Catch-up comes from peers, not the local log |
| RP-34 | The readiness gate blocks writes |
| RP-35 | Past the GC window, discard and cold-bootstrap — live nodes too |
| RP-36 | Readiness is per scope |
| RP-37 | Offline nodes accept writes into an outbox |
| RP-38 | The device-loss risk is surfaced, not prevented |
| RP-39 | The outbox enables offline conflict detection |
| RP-40 | Restore is a forward write |
| RP-41 | Historical inspection routes to a covering `LOGGED` peer |
| RP-42 | Historical reads must not replicate into the local store |
| RP-43 | The restore closure is computed as of T |
| RP-44 | Restore mode is required; Exact needs a stronger right |
| RP-45 | Restore suppresses cascades |
| RP-46 | The closure must be upward-closed |
| RP-47 | Restore is atomic locally, eventually consistent globally |
| RP-48 | `RestoreEntityTree` routes to a complete node with log reach |
| RP-49 | Node, connection, and identity are three concepts; audit needs identity |
