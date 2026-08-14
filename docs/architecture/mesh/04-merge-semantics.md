# 04 — Merge Semantics

**Normative.** Source: spec §7.1, §8, §10.5.2. Invariant prefix `MG`.

---

## 1. Why whole-entity LWW fails

Today's record carries the entire entity with one timestamp. Two users editing different fields — A
changes `name`, B changes `description` — each emit a full-entity SET; one wins and the other is
**silently discarded**. The loss is an artifact of granularity: nothing about those two edits actually
conflicts.

This is currently masked because writes funnel through one server, commands serialize, and handlers
read-modify-write against uncontended state. **The mesh removes every one of those protections.**
Concurrent edits become the ordinary case, degrading in proportion to how well the mesh succeeds.

> **MG-1** — Convergence compares **stored state**, not log position. Myko is not event-sourced: the
> accurate description is *a replicated key-value store with per-field merge, a local event bus for
> sagas, and a durable changelog*.

> **MG-2** — Nothing carries a global sequence number. Per-origin ordering is not required for
> convergence and is not provided at the state layer. (The log layer's per-origin `log_seq`, 03 §3,
> exists for gap detection and is never consulted by merge.)

**Ruled out, with reasons:** version vectors (an entry per writer, and every client is a writer —
unbounded growth); entity-home routing (an offline node could not write at all); consensus (kills
partition tolerance).

## 2. Stored entity state

```rust
// myko-core::mesh::state

pub struct EntityState {
    pub type_id: QualifiedName,
    pub scope_id: ScopeId,
    pub entity_id: EntityId,
    /// Entity-level tombstone. `Some(hlc)` means deleted at that HLC — 03 RF-9.
    pub deleted: Option<Hlc>,
    /// Ascending by field_id — 03 RF-11. A BTreeMap, so merge is a merge-join.
    pub fields: BTreeMap<u32, FieldState>,
    /// Memoized 03 RF-18. Invalidated on any mutation. Never serialized.
    content_hash: OnceCell<[u8; 32]>,
}

pub struct FieldState {
    pub hlc: Hlc,
    /// The origin that wrote the winning value — the RF-3 tiebreak input.
    pub origin: NodeId,
    pub tombstone: bool,
    pub strategy: MergeStrategy,
    /// Canonical CBOR. For CRDT strategies this is the *CRDT state*, §3.
    pub value: Bytes,
}
```

> **MG-3** — `FieldState.origin` is stored, not merely received. It is the RF-3 tiebreak input, and
> dropping it would make merge non-deterministic on exact HLC ties.

The existing in-process `ContentHash` memoizer is **not** a starting point for `content_hash`: its
cache resets on `Clone` and it has no cross-process stability. `OnceCell` here memoizes the RF-18 hash
and is invalidated on mutation.

## 3. Per-field merge

> **MG-4** — Each field carries its own HLC and merges independently. Concurrent edits to `name` and
> `description` both survive as independent registers.

> **MG-5** — Merge is **commutative, associative, and idempotent**. Applying the same record twice, or
> applying two records in either order, produces identical `EntityState` — verified by the
> merge-determinism property test (spec §16).

MG-5 is not optional politeness: the mesh delivers **at-least-once with duplication** (gossip,
anti-entropy, and pairwise streams overlap), so a non-idempotent merge corrupts state under normal
operation, not just under fault.

### The algorithm

```
merge_record(state: &mut EntityState, rec: &Record):

    # 1. Entity-level tombstone is a register under the same total order (RF-9)
    if rec.record_type == Delete:
        if (rec.hlc, rec.origin) > (state.deleted_hlc_or_min, state.deleted_origin_or_min):
            state.deleted = Some(rec.hlc)
        # A DEL does not clear fields. A later SET must be able to beat it per-field.
        return

    if rec.record_type == Set:
        # A SET newer than the tombstone revives the entity (RF-9).
        if state.deleted is Some(d) and (rec.hlc, rec.origin) > (d, state.deleted_origin):
            state.deleted = None

    # 2. Field entries — a linear merge-join, both sides ascending by field_id (RF-11)
    for entry in rec.fields:            # ascending, unique
        strategy = schema_strategy(state.type_id, entry.field_id)
                     .unwrap_or(entry.tag)          # TI-10: unknown field carries its tag
        if strategy_known and strategy != entry.tag:
            reject_record(StrategyMismatch)         # RF-14
            return

        match state.fields.entry(entry.field_id):
            Vacant  => insert FieldState::from(entry)
            Occupied(cur) => match strategy:
                Lww        => if (entry.hlc, rec.origin) > (cur.hlc, cur.origin) {
                                  cur = FieldState::from(entry)
                              }
                PnCounter  => cur.value = pn_merge(cur.value, entry.value)      # §3.2
                              cur.hlc   = max(cur.hlc, entry.hlc)
                OrSet      => cur.value = orswot_merge(cur.value, entry.value)  # §3.3
                              cur.hlc   = max(cur.hlc, entry.hlc)
                LwwMap     => cur.value = lwwmap_merge(cur.value, entry.value)  # §3.4
                              cur.hlc   = max(cur.hlc, entry.hlc)

    state.invalidate_content_hash()
```

> **MG-6** — For CRDT strategies the HLC is **advanced to the max, not used to select**. Selecting by
> HLC would discard one side's state and defeat the CRDT. The HLC on a CRDT field exists for
> staleness reporting and log ordering only.

> **MG-7** — A DEL does **not** clear field state. Fields are retained (subject to tombstone GC, §6)
> so that a later SET beating the tombstone revives a coherent entity rather than a one-field
> fragment.

### 3.1 Strategy selection

> **MG-8** — The merge strategy is **selected from the declared Rust type by the macro**, not written
> by hand. `#[myko_field(merge = ...)]` overrides it; changing either is a schema-incompatible change
> (02 TI-8, 03 RF-14).

| Declared type | Strategy | Tag | Rationale |
|---|---|---|---|
| `String`, `i*`, `u*`, `f*`, `bool`, `Uuid`, enums, `Arc<str>` | **LWW** | 0 | No coherent merge exists for an opaque scalar. LWW is *correct* here, not lossy. |
| `Option<T>` | strategy of `T` | — | `None` is a field tombstone (03 RF-10). |
| `Counter<i64>` (newtype) | **PN-Counter** | 1 | Both intents are "+1". |
| `Set<T>`, `BTreeSet<T>`, `HashSet<T>` | **OR-Set** | 2 | Both additions must stick. |
| `BTreeMap<K, V>`, `HashMap<K, V>` | **LWW-Map** | 3 | Different keys both survive. |
| `Vec<T>` | **LWW** | 0 | Ordered sequences are not sets; see §3.5. |
| Nested `#[myko_item]` struct | **LWW** on the whole value | 0 | Nested entities are references, not embedded state (07 §1). |
| Collaborative text | — | — | **Out of scope.** §3.5. |

> **MG-9** — **`Vec<T>` is LWW, deliberately.** Treating a `Vec` as a set loses order and duplicates;
> treating it as a sequence needs a sequence CRDT, which §3.5 excludes. A field that genuinely wants
> set semantics declares a set type.

> **Sets deserve priority in the implementation order.** `{Alice}` with concurrent adds of Bob and
> Carol resolves under whole-entity LWW to `{Alice, Bob}` *or* `{Alice, Carol}` — a concurrent add
> **silently revokes** the other person. On a permissions list that is a security-adjacent correctness
> bug, not a merge nicety.

### 3.2 State-based CRDTs, and the actor bound

> **MG-10** — **Every CRDT here is state-based.** Delivery forces it: the mesh is at-least-once with
> duplication and provides no causal ordering, so an op-based "+1" delivered twice double-counts.
> Op-based CRDTs are ruled out wholesale.

State-based counters and sets carry **per-actor entries**, which is the unbounded-writer growth §1
rejects in version vectors. The bound:

> **MG-11** — **The CRDT actor set is bounded to durable nodes.** An actor id is the `NodeId` of a
> node that is `Durable(Stateful)` and complete for the scope. Edge and attached nodes mutate counters
> and sets **via commands** (09 §2), never by direct write, so actor entries grow with **mesh size,
> not client count**.

> **MG-12** — A node MUST reject a CRDT field state containing an actor id that is not, per its
> current membership view, a durable node for the scope. Rejection is loud and does not silently drop
> the record: it fails the record and reports a protocol violation (05 §6 — under the v1 trust model
> this is a correctness check against bugs, not a defence against a hostile peer).

### PN-Counter

```
value:  CBOR map { actor_id (32 bytes) -> [p: uint, n: uint] }
read:   sum(p for all actors) - sum(n for all actors)
merge:  per actor, p = max(p_a, p_b), n = max(n_a, n_b)
incr:   local actor's p += delta        (delta > 0)
decr:   local actor's n += delta        (delta > 0)
```

Merge is per-actor max, so it is idempotent and order-free. Canonical CBOR (03 RF-17) sorts the map by
actor bytes, so the encoding is deterministic.

### 3.3 OR-Set — ORSWOT

Plain OR-Set carries a tag per add and never shrinks. **ORSWOT** (Observed-Remove Set Without
Tombstones) carries a version vector instead, bounding size by `actors + elements`:

```
value:  CBOR map {
            "c" -> context:  map { actor_id -> counter: uint }      # every dot ever observed
            "e" -> entries:  map { element (canonical CBOR) -> map { actor_id -> counter } }
        }

add(x):     counter = context[me] + 1
            context[me] = counter
            entries[x]  = { me: counter }           # replaces this actor's prior dot for x

remove(x):  delete entries[x]                       # context retains the dots — that is the record

merge(a, b):
    for each element x:
        dots_a = a.entries[x] or {}
        dots_b = b.entries[x] or {}
        # keep a dot if the other side has it, or has not yet observed it
        kept = { (actor, n) in dots_a : dots_b has (actor, n)  or  b.context[actor] < n }
             ∪ { (actor, n) in dots_b : dots_a has (actor, n)  or  a.context[actor] < n }
        if kept is non-empty: out.entries[x] = kept
    out.context = per-actor max(a.context, b.context)
```

> **MG-13** — ORSWOT is **add-wins**: a concurrent add and remove of the same element resolves to
> present. This is the correct default for the collaborator/permission lists that motivate the
> strategy — a concurrent add must never be silently revoked (§3.1).

> **MG-14** — Element identity is the **canonical CBOR encoding** of the element (03 RF-17). Two
> elements are the same element iff their canonical bytes are equal.

### 3.4 LWW-Map

```
value:  CBOR map { key (canonical CBOR) -> [hlc: 8 bytes, origin: 32 bytes, v: value | null] }
merge:  per key, keep the entry with the greater (hlc, origin) — RF-3
        `null` is a key tombstone, subject to the same GC window as entity tombstones (§6)
read:   keys whose entry is not a tombstone
```

Nested maps recurse: an LWW-Map whose values are maps merges per key, and the value's own strategy
applies one level down.

### 3.5 Collaborative text is out of scope

> **MG-15** — **Sequence CRDTs are excluded from this design.** They are op-based, causally dependent,
> and cannot ride an opaque-value merge-join — every property this merge layer relies on. Collaborative
> text gets its own delta protocol when it lands, layered above, not a fifth strategy tag.

Per-field LWW plus the three structured strategies is therefore **nearly complete**: most fields are
scalars where LWW is right, and the remaining shapes are identifiable from the declared type.

## 4. Apply modes

The apply path is one chokepoint — `apply_event_batch` → `emit_grouped` → `apply_effects`
(`libs/myko/core/src/server/context.rs`) — and its `Origin` enum is the single policy point for what a
mutation is allowed to trigger. Today it holds two variants:

```rust
// libs/myko/core/src/server/context.rs:59 — today
pub(crate) enum Origin { Local, Cascade }
```

`Origin::Remote` existed in PR #25 and **has since been removed**; wire-ingested events currently apply
as `Local`, which means they cascade and re-produce. That regression must be reversed with the
replication plane.

> **MG-16** — The apply mode is a **four-variant policy point**:

```rust
pub(crate) enum Origin {
    /// A command handler or server module emitting a new mutation here.
    /// Cascades, produces, fires sagas, appends to the log.
    Local,
    /// A relationship cascade product. Cascades on DEL only; produces.
    Cascade,
    /// Arrived on the replication plane (06 §2). Merges + indexes.
    /// NO cascade, NO produce, NO saga, NO re-broadcast.
    Remote,
    /// An optimistic prediction (09 §5). Lands in the provisional overlay.
    /// NO cascade, NO produce, NO saga, NO log append.
    Provisional,
}
```

| Mode | Merge + index | Cascade | Produce (re-emit) | Sagas | Log append | Where it lands |
|---|---|---|---|---|---|---|
| `Local` | yes | yes | yes | yes | yes | base store |
| `Cascade` | yes | DEL only | yes | yes | yes | base store |
| `Remote` | yes | **no** | **no** | **no** | yes | base store |
| `Provisional` | yes | **no** | **no** | **no** | **no** | overlay |

> **MG-17** — **`Remote` must not cascade.** The originating node already emitted any cascade writes as
> ordinary records (07 §1). Re-running cascades on every receiver would duplicate them, and choosing
> one receiver to be responsible would be a leader election this design avoids.

> **MG-18** — **The mode is determined by the plane the record arrived on, never by a wire flag**
> (06 §2). If a single plane carried both kinds, the discriminator would return to the wire and every
> record would have to declare its own origin.

## 5. Optimistic concurrency control

Per-field merge fixes **structural** conflicts. It does nothing for **semantic** ones: a handler reads
`seat.occupied == false` and writes `occupied = true`; two run concurrently on load-balanced nodes,
both read `false`, both write, merge picks one — and the loser's client believes it got the seat.

### The mechanism: runtime read-set tracking

`CommandContext` is already the single funnel for both sides — reads via `exec_query_first`,
`exec_query`, `exec_report`; writes via `emit_set`, `emit_del`.

> **MG-19** — The context **records which entity fields a handler observed**, and on emit the
> framework **automatically attaches a precondition to each written field that was also read**,
> carrying the HLC observed at read time.

> **MG-20** — **A field written without being read is a blind write and gets no precondition.** This is
> correct: blind writes are intentional.

No declaration, no macro static analysis (which cannot see the read set anyway), no per-handler
discipline. A rejected write returns a clean, retryable error.

> **MG-21** — Reactive `query_map` subscriptions are **not** part of a command's read set. Only
> snapshot reads inside the command are. A subscription is a standing interest, not an observation at
> a point in time.

> **MG-22** — An explicit opt-out exists for high-throughput blind-write paths, declared on the
> command.

### Execution time only — and why

> **MG-23** — **Preconditions are checked where the command executes. Never at apply time.**

An apply-time precondition is order-dependent, and the divergence is permanent:

```
A writes  occupied=true  @ hlc=10, precondition: occupied.hlc == 5
B writes  occupied=true  @ hlc=11, precondition: occupied.hlc == 5

Replica C receives A then B:
    A: precondition holds (occupied.hlc is 5)  -> apply, occupied.hlc = 10
    B: precondition fails (occupied.hlc is 10) -> reject
    C's state: A's write.

Replica D receives B then A:
    B: precondition holds -> apply, occupied.hlc = 11
    A: precondition fails -> reject
    D's state: B's write.

C and D have diverged, and no further message reconciles them.
```

So: **the emitted record replicates unconditionally and merges as pure LWW.** `precondition_hlc`
travels for audit and conflict inspection (03 RF-15), not as a gate.

> **MG-24** — The guarantee OCC provides is **exactly as strong as the executing node's view**.
> Strengthening it is routing's job, not merge's — see the per-command consistency modes of 09 §6.

## 6. Tombstones and GC

> **MG-25** — A DEL leaves a **timestamped tombstone** in the store and in the anti-entropy index, so
> a peer learns the entity was *deleted* rather than *absent*. Anti-entropy cannot distinguish those
> two states without it, and would push the entity back.

> **MG-26** — Tombstones GC on a configurable window, default **30 days**. The same window is the
> staleness threshold past which a node must discard local state and cold-bootstrap rather than
> reconcile (01 NM-20) — they are one number and must be configured as one.

> **MG-27** — Tombstone GC applies to **entity tombstones, field tombstones (03 RF-10), and LWW-Map
> key tombstones (§3.4)** alike. ORSWOT has no tombstones by construction (§3.3), which is why it was
> chosen.

## 7. Conflicts: recorded, not replicated

Two situations produce genuine divergence, and one that resembles it does not.

> **MG-28** — **Partition heal.** Both sides committed successfully with their own durability.
> Detection happens **during anti-entropy repair**, when an incoming value beats a *locally
> originated* one.

> **MG-29** — **Owned-entity replay** (09 §7). Edge-owned entities are the one case where offline
> replay ships *records* rather than commands, so the outbox comparison against merged state survives
> there. The owner is the single writer, so a replay loss is never an ordinary race — it means an
> ownership violation or an administrative write (08 §9's restore) touched an owned entity. **Rare,
> and recorded loudly.**

> **MG-30** — **Command replay is not a conflict.** The general outbox holds commands and replays by
> re-execution; a result differing from the prediction is a **rebase**. The prediction never committed
> anywhere and MUST NOT be logged as data loss. Rebases surface as a local UX event ("your change was
> adjusted"), never a conflict record.

The two detection mechanisms — anti-entropy comparison and outbox replay comparison — do not
generalize to each other. **Both are needed.**

### Where records go

| What | Where | Bounded by |
|---|---|---|
| Per-conflict detail — losing value, winner, both HLCs, both actors | **local log**, unreplicated (07 §2) | number of conflicts |
| Per-heal summary — window, counts, which nodes hold the detail | **replicated** | number of heals |

> **MG-31** — The replicated unit is the **heal**, not the conflict. This gives discoverability without
> flooding the mesh on a large heal. The summary is small and append-only — an OR-Set (§3.3).

> **MG-32** — Resolution needs no new mechanism. The log is a reflog: inspect what was discarded and
> restore it as a **forward write** (08 §9).

A consequence worth stating: **browser nodes want a narrow `LOGGED` role** — just their own conflicts,
not full history. `LOGGED` is not purely a server-tier role.

---

## Invariant index

| ID | One line |
|---|---|
| MG-1 | Convergence compares stored state, not log position |
| MG-2 | No global sequence at the state layer |
| MG-3 | `FieldState.origin` is stored — it is the tiebreak input |
| MG-4 | Per-field HLC; fields merge independently |
| MG-5 | Merge is commutative, associative, idempotent |
| MG-6 | CRDT fields advance the HLC to max, never select by it |
| MG-7 | A DEL does not clear field state |
| MG-8 | Strategy is selected from the declared type by the macro |
| MG-9 | `Vec<T>` is LWW, deliberately |
| MG-10 | All CRDTs are state-based; op-based ruled out by delivery |
| MG-11 | CRDT actors are bounded to durable nodes |
| MG-12 | Reject CRDT state naming a non-durable actor |
| MG-13 | ORSWOT is add-wins |
| MG-14 | Element identity is canonical CBOR bytes |
| MG-15 | Sequence CRDTs / collaborative text are out of scope |
| MG-16 | Four apply modes: Local, Cascade, Remote, Provisional |
| MG-17 | `Remote` must not cascade |
| MG-18 | Apply mode comes from the plane, never a wire flag |
| MG-19 | Read-set tracking auto-attaches preconditions to read-then-written fields |
| MG-20 | Blind writes get no precondition |
| MG-21 | Subscriptions are not part of a command's read set |
| MG-22 | Explicit OCC opt-out exists per command |
| MG-23 | Preconditions check at execution time, never apply time |
| MG-24 | OCC is exactly as strong as the executing node's view |
| MG-25 | DEL leaves a timestamped tombstone |
| MG-26 | Tombstone GC window == the cold-bootstrap staleness threshold |
| MG-27 | GC covers entity, field, and map-key tombstones |
| MG-28 | Partition-heal conflicts detected during anti-entropy |
| MG-29 | Owned-entity replay loss is an ownership violation, recorded loudly |
| MG-30 | A rebase is not a conflict and is never logged as data loss |
| MG-31 | The replicated unit is the heal, not the conflict |
| MG-32 | Resolution is a forward write; the log is a reflog |
