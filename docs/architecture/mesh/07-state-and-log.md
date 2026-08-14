# 07 — State and Log

**Normative.** Source: spec §6, §7, §15. Invariant prefix `SL`.

---

## 1. Two subsystems, one primary

> **SL-1** — **State is primary. The log is a durable, append-only artifact produced by state
> changes.**

| | Is | Merge | Anti-entropy | Retention |
|---|---|---|---|---|
| **State store** | a KV store with per-field merge and tombstones (04 §2) | yes | yes (08 §2) | latest per `(key, field)`, forever until tombstone GC |
| **Log** | append-only, per-origin | **none needed** | **none** — shipping it is bulk archival | history depth, bounded by time with a size cap |

> **SL-2** — The log is **outside convergence**. It needs no merge and no anti-entropy because it is
> append-only and per-origin. Two nodes' logs are not expected to be equal and are never compared.

> **SL-3** — `MEvent` is renamed. It is a **state-change record** (03), not an event.

### Why the "event-sourced" framing has to go

The mismatch is not cosmetic. It implies guarantees the system does not provide — silently — conflates
two retention policies that must differ (§5), and invites applying LWW to a log where LWW is
meaningless.

### What the log is kept for

**Audit trail**, **point-in-time replay**, **recovery** (§7), and **sagas** — which react to a *local*
mutation stream, a sound pattern distinct from distributed-log consumption.

> **SL-4** — Causal ordering and cross-origin sequencing are **not provided**, and nothing in §1's
> list needs them. Per-field merge converges without them.

## 2. Entities are source; derivation lives one layer up

> **SL-5** — **Entities never derive from one another.** An entity's state is source data, changed only
> by an explicit write. Queries select, views project, reports aggregate — all functions *of* entities,
> never inputs *to* them.

Relationship attributes look like an exception and are not:

| Attribute | Is |
|---|---|
| `#[belongs_to(Scene)]` | a reactive rule that **emits real DELs** for children when a parent is deleted |
| `#[owns_many(BindingNode)]` | the same, plus a real write updating the parent when a child is deleted |
| `#[ensure_for(Project)]` | a reactive rule that **creates a real entity** per dependency |

> **SL-6** — Relationship effects are **rules that write**. Each produces ordinary source data —
> records with their own HLC and actor, replicated like any other record, and independently mutable
> afterwards. None makes one entity's state a function of another's.

The alternative — treating a child's tombstone as *derived* from its parent's — would place entities
in both layers at once, and an explicit SET on a derived-tombstoned child would have no defined
meaning. Being writes, relationship effects follow 09 §2: **they run where the state they read is
complete.**

## 3. Local-only operations

Five operations share one hazard shape: a node modifies its own storage, and routing that through the
replication path would corrupt the mesh.

| Operation | Would otherwise |
|---|---|
| Scope eviction (05 §7) | delete an organization's data everywhere |
| Log truncation (§5) | destroy history everywhere |
| Restore batch (08 §9) | cascade beyond the computed closure |
| Log checkpoint (§6) | replicate as a mesh-wide write |
| Conflict record (04 §7) | replicate a node-local observation |

> **SL-7** — **"Local operation that emits nothing" is a first-class, named concept in the store
> API**, not five independently-remembered special cases.

```rust
impl EntityStoreHandle {
    /// Mutates local storage only. Produces no records, fires no sagas, runs no
    /// cascades, and appends nothing to the replication outbound queue.
    ///
    /// The closure receives a writer that is *structurally incapable* of
    /// emitting — there is no emit method on it.
    pub fn local_only<R>(&self, f: impl FnOnce(&LocalWriter) -> R) -> R;
}
```

> **SL-8** — The local-only writer MUST NOT expose an emit path. Enforcement is by type, not by
> discipline — the whole point of SL-7 is that "remember not to emit" has failed five times already.

## 4. The state store

Today: `StoreRegistry` holds one `EntityStore` per entity type
(`libs/myko/core/src/store/registry.rs`), where `EntityStore = CellMap<Arc<str>, Arc<dyn AnyItem>>` —
a hyphae reactive map keyed by entity id. Apply is arrival-order overwrite via `store.insert_many`.

Four changes:

> **SL-9** — **Keyed by `(qualified_type, scope, id)`**, not by bare type name (02 TI-5, 05 SC-1).
> `StoreRegistry::get_or_create(entity_type)` becomes `get_or_create(qualified_type, scope)`.

> **SL-10** — **The store holds `EntityState` (04 §2), not `Arc<dyn AnyItem>` alone.** Per-field HLCs,
> origins, strategies, and opaque unknown fields (02 TI-10) have nowhere else to live. The typed
> `Arc<dyn AnyItem>` projection is derived from `EntityState` and cached alongside it, because every
> query and view consumes the typed form.

> **SL-11** — **A readiness state is attached to each `(service, scope)` and to each registered
> predicate.** `StoreRegistry::get_or_create` currently makes a missing type indistinguishable from an
> empty one; with hydration gates (08 §5) that ambiguity is a correctness bug. A store that has not
> finished hydrating MUST NOT answer.

> **SL-12** — **The provisional overlay is a real store layer** (09 §5), not bookkeeping. The reactive
> graph reads *through* it: overlay first, then base. Dropping an overlay is one operation and emits
> nothing (SL-7).

```
     query / view / report
              │
              ▼
      ┌───────────────┐
      │   Overlay     │   provisional records from optimistic execution
      │  (per outbox  │   — dropped atomically on rebase (09 §5)
      │   entry)      │
      └───────┬───────┘
              │ miss
              ▼
      ┌───────────────┐
      │  Base store   │   EntityState per (qualified_type, scope, id)
      │  CellMap      │   + readiness — SL-11
      └───────────────┘
```

### Renamed fields

02 TI-15 leaves the read-through-versus-migrate-forward choice open. **This document chooses
migrate-forward:** on the next write to a renamed field, the store writes the new `field_id` and
field-tombstones the old one. Read-through remains for values not yet rewritten. Compaction (§5) then
retires the old id naturally, and the content hash stabilizes on the new id across the mesh within one
write cycle instead of never.

## 5. The log

> **SL-13** — The log is built **indexed and compacted from the start**. Retrofitting either is far
> more expensive than designing them in.

### Index

> **SL-14** — The log is indexed by **`(scope, qualified_type, entity_id) → time-ordered versions`**.

Today `replay_to_store(until)` rebuilds an entire `StoreRegistry` from the whole log. That is far too
coarse once tree inspection is routine (08 §9).

> **SL-15** — **Historical operations are explicitly allowed to be slow.** That buys real
> simplification: no prefetch, no aggressive caching, simple request/response rather than streaming,
> and an index that may favour **compactness over lookup latency**.

### Type dictionary

> **SL-16** — The log stores the **qualified name or its stable hash**, never a connection intern id
> (03 RF-8). A per-log type dictionary bounds the storage cost of names; it is local to the log file
> or table and is not a wire concept.

### Compaction

> **SL-17** — **Compaction is per-`(key, field)`, not per-key** (03 §4). Retain the latest surviving
> value for each field.

> **SL-18** — **The log must store merge metadata**, not just values. Replay that drops per-field HLCs,
> origins, and CRDT state produces a state that merges *incorrectly* thereafter — a silent, permanent
> corruption that only shows up on the next concurrent write.

### Two retention policies

> **SL-19** — These are **two policies and must be configured separately:**

| Concept | Bounded by | Purpose |
|---|---|---|
| **History depth** | time, with a size cap as a safety valve | how far back windback reaches |
| **Latest per `(key, field)`** | never, until tombstone GC | makes state recovery possible at all (§7) |

> **SL-20** — **The horizon governs history depth only.** Evicting the most recent version of a live
> key makes state recovery impossible, **silently**.

### The horizon is adjustable

> **SL-21** — A `LOGGED` node's horizon is **policy, configured per scope**. Widen it and the node
> backfills; reads it previously routed become local.
>
> - **Expand = backfill** — bulk, resumable, cold-path, *transfer plus index build*. It MUST extend an
>   existing contiguous range (01 NM-9), not create islands. It requires the scope capability:
>   historical data is not less sensitive.
> - **Contract = local truncation** — a local-only operation (SL-7).

> **SL-22** — Advertise `horizon_actual`, never `horizon_target` (01 NM-14).

> **SL-23** — **Operational floor: at least one archival node with unbounded retention, preferably
> two.** If every node has a finite horizon and all roll forward, **history is permanently lost**.
> This is easy to miss because every individual horizon looks locally reasonable. A deployment with no
> unbounded-retention node MUST warn at startup.

## 6. Checkpoints

> **SL-24** — A **checkpoint** is a log-only record (03 `record_type = 2`) capturing the full state of
> a `(scope, type, id)` set at a watermark, **preserving each field's original HLC, origin, and
> actor**.

> **SL-25** — A checkpoint makes its range **self-sufficient**: state rebuilds from it forward with
> nothing earlier (01 NM-9).

> **SL-26** — **Periodic checkpointing is a general precondition**, not a repair-only tool. §7's
> recovery and the gap repair of SL-28 need the same mechanism, and a log without checkpoints cannot
> serve as a recovery source at all.

> **SL-27** — **A checkpoint is log-only and MUST NOT replicate** (SL-7). Writing current state as
> ordinary SETs would stamp fresh HLCs and actors — corrupting attribution — and worse, those records
> would replicate, turning log repair into a mesh-wide write.

## 7. Failure modes

### Losing all `LOGGED` nodes

> **SL-28** — **State nodes keep running.** The log is outside convergence (SL-2). What is lost is
> **history, not data**.

On return, the log is **silently non-reconstructable unless repaired**: replay would produce pre-gap
state plus post-gap changes with the gap's changes missing.

> **SL-29** — Repair is a **marked checkpoint**, never "the next events" — and it **opens a new
> contiguous range rather than repairing the old one**. The node then advertises
> `[inception .. gap_start]` and `[checkpoint .. now]`, and in-gap reads fail structurally (01 NM-9,
> 08 §7).

> **SL-30** — Checkpoint **after** state nodes have converged, never from an arbitrary node
> mid-partition.

### Losing all `STATEFUL` nodes

> **SL-31** — Recovery requires the log to retain the **latest surviving value per `(key, field)`
> regardless of age** (SL-19). Without it, an entity last written outside the horizon has no record at
> all, and replay yields a state where it is *absent* rather than stale.

> **SL-32** — **Recovery replays in place, preserving original HLCs, origins, and actors** — the exact
> opposite of restore's forward write (08 §9). Re-stamping would present as a mass rewrite and clobber
> surviving replicas. **Do not share code paths between recovery and restore carelessly.**

### Write admission

> **SL-33** — Write admission **decomposes into two independent questions**, both asked per scope
> (05 SC-25):
>
> - **State durability** — is a reachable `Durable(Stateful)` target available for this scope?
> - **History durability** — is a reachable `Durable(Logged)` target available for this scope?
>
> Whether a scope requires the second is **per-scope policy**: compliance scopes demand it,
> operational scopes accept gaps. The history-durability ack is what anchors log contiguity
> (01 NM-10).

> **SL-34** — **Fail-closed write admission is rejected.** Refusing local writes unless a durability
> target is reachable would make an offline node inert, contradicting first-class origins — and it
> would not buy what it appears to. It only blocks writes in a component that *cannot persist at all*,
> so two partition halves that each retain durability both pass the check and both accept writes.
> Divergence across a partition happens regardless; 04's merge plus 04 §7's conflict recording is the
> actual answer.

## 8. Schema requirements by role

> **SL-35** — Schema is **required for `STATEFUL`, not required for `LOGGED`**:

| Role | Schema | Because |
|---|---|---|
| `STATEFUL` | **required, per service** | CRDT merge, relationship cascades, index maintenance, and query evaluation are all type-specific |
| `LOGGED` | **not required** | indexes on header fields only (SL-14); outside convergence; serves raw records for the requester to parse |

A generic, schema-free **archival appliance remains possible**; a generic *state* store does not.
LWW merge itself is schema-free (03 RF-13) — the schema is needed for everything *else* a state node
does. A node lacking the schema could relay and LWW-merge but could not serve a single query, which is
the whole point of holding state.

> **SL-36** — **A `LOGGED` node can retain history for types nothing currently materializes.** Because
> it never parses bodies, it accumulates records for services no node in the mesh links today — so
> when such a service is deployed, its history is already there rather than starting empty. A
> `STATEFUL` node cannot do this; it can only hold what it links.

Today an unknown `item_type` is silently dropped: `parse_item` returns `None`
(`libs/myko/core/src/server/context.rs`) and every ingest path skips it
(`libs/myko/server/src/postgres.rs`). SL-36 requires that the log ingest path stop doing this.

> **SL-37** — **The cost of SL-35 is stated plainly:** a new service cannot obtain *state* durability
> from nodes that predate it; those nodes need redeploying with the new crates. For within-org
> federation that is a redeploy you would do anyway. **For an open federated mesh spanning parties you
> do not control, this is the decision to revisit.**

## 9. Performance note

Parse-free is faster only for pure store-and-forward. Once a node merges, queries, indexes, or
cascades, parse-free *defers* the cost and pays it at read time repeatedly rather than at write time
once. For `LOGGED`, parse-free is genuinely optimal: header-only indexing, bodies returned verbatim.

---

## Invariant index

| ID | One line |
|---|---|
| SL-1 | State is primary; the log is a derived artifact |
| SL-2 | The log is outside convergence — no merge, no anti-entropy |
| SL-3 | `MEvent` renames to a state-change record |
| SL-4 | No causal ordering or cross-origin sequencing is provided |
| SL-5 | Entities never derive from one another |
| SL-6 | Relationship effects are rules that write |
| SL-7 | "Local operation that emits nothing" is a named store-API concept |
| SL-8 | The local-only writer has no emit method — enforced by type |
| SL-9 | Store keyed by `(qualified_type, scope, id)` |
| SL-10 | The store holds `EntityState`, not just the typed item |
| SL-11 | Readiness is attached to stores and predicates |
| SL-12 | The provisional overlay is a real store layer |
| SL-13 | Build the log indexed and compacted from the start |
| SL-14 | Log index: `(scope, type, id) → time-ordered versions` |
| SL-15 | Historical operations may be slow — buy simplicity with it |
| SL-16 | The log stores qualified names, never intern ids |
| SL-17 | Compaction is per-`(key, field)` |
| SL-18 | The log must store merge metadata |
| SL-19 | History depth and latest-per-field are two policies |
| SL-20 | The horizon governs history depth only |
| SL-21 | Horizon expand = backfill extending contiguity; contract = local truncation |
| SL-22 | Advertise `horizon_actual` |
| SL-23 | At least one unbounded-retention node; warn if none |
| SL-24 | Checkpoints preserve original HLC, origin, actor |
| SL-25 | A checkpoint makes its range self-sufficient |
| SL-26 | Periodic checkpointing is a general precondition |
| SL-27 | Checkpoints are log-only and never replicate |
| SL-28 | Losing all `LOGGED` nodes loses history, not data |
| SL-29 | Gap repair opens a new range; in-gap reads fail structurally |
| SL-30 | Checkpoint after convergence, not mid-partition |
| SL-31 | Recovery needs latest-per-`(key, field)` regardless of age |
| SL-32 | Recovery replays in place; restore writes forward. Separate code paths |
| SL-33 | Write admission decomposes into state and history durability, per scope |
| SL-34 | Fail-closed write admission is rejected |
| SL-35 | Schema required for `STATEFUL`, not for `LOGGED` |
| SL-36 | A `LOGGED` node retains history for types nothing materializes |
| SL-37 | Cost: new services get no state durability from older nodes |
