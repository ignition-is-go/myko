# Restore Points — Linear Time-Travel over the Event Log

**Status:** Design
**Date:** 2026-06-17
**Layer:** myko framework (core + server), consumed by rship

## Summary

Add **restore points**: lightweight, named bookmarks into the event log that let a
user inspect, diff, and (later) restore project state as it was at a past moment.

The key realization that shapes this whole design: **the Postgres event log is
already the version-control substrate.** Every mutation is durably recorded with a
monotonic id and timestamp, and `ExportEntityTree { as_of }` already replays the log
to reconstruct any past state. So restore points need almost no new machinery — a
restore point is a *pointer into history*, not a copy of state.

This is deliberately a **linear** model (Time-Machine / undo-history), not git: no
branches, no merge. A single authoritative timeline per server is enough for the real
workflows, and divergent branches + 3-way merge are the only parts that would require
genuinely new structure. They are out of scope.

## Non-goals

- **Branching / merge.** Out of scope. The event log is one linear timeline; divergent
  streams + 3-way entity merge are a separate, much larger project.
- **Replacing `Snapshot`.** `Snapshot` (rship, `libs/entities/core/src/snapshot.rs`)
  stays. It serves a different need: a heavyweight, **self-contained** full-tree copy
  for export/import/backup and for capturing *live runtime overlay* (emitter pulses via
  its pre-capture/post-export hooks) that isn't in the authored event log. Restore points
  are the lightweight, in-project, log-backed counterpart.
- **The full restore/apply engine.** Reconstructing and *re-emitting* a past tree back
  to live state (undelete, rollback) is a follow-up. This spec lands the bookmarks +
  inspection + diff; restore-as-write builds on `as_of` reconstruction later.

## Design decisions and rationale

### 1. A restore point is a pointer, not a copy

`Snapshot` stores the entire serialized `EntityTreeExport` in `data: Value` — an
O(tree-size) JSON copy per snapshot. A restore point stores only an anchor; the log
reconstructs the state on demand via `as_of`.

| | `Snapshot` (exists) | `RestorePoint` (this spec) |
|---|---|---|
| stores | full materialized tree copy | an anchor timestamp |
| create cost | O(tree size) | O(1) |
| reconstruct | deserialize the blob | `as_of` replay from the log |
| depends on | nothing (portable) | the event log staying intact |

### 2. On-log framework entity → realtime sync for free

`RestorePoint` is a normal on-log `#[myko_item]`. This is the decisive tradeoff: by
living on the event log it inherits myko's entire reactive read/sync surface
(`GetAllRestorePoints`, `…ByQuery`, live `CellMap`s, windowing) at zero cost. An
off-log side table would have forced us to rebuild that machinery (a provider, dedicated
wire verbs, a LISTEN/NOTIFY→Cell bridge) just to make a list update live.

Precedent: `Snapshot` is already exactly this — an on-log `#[myko_item]` emitted via
`ctx.emit_set`. Restore points follow the same pattern, minus the heavy `data` blob.

The only "con" of being on-log is that the bookmark's own creation is itself an event,
and history is immutable (a deleted restore point's creation event remains). For
low-frequency human bookmarks this is negligible, and it is the same property every other
entity already has.

### 3. Anchor on **timestamp**, not event id

`MEvent` (`libs/myko/core/src/wire/event/mod.rs`) has **no `id` field** — `id` is a
`BIGSERIAL PRIMARY KEY` assigned by Postgres on insert (`libs/myko/server/src/postgres.rs`),
and the producer's INSERT uses no `RETURNING`. So a command handler never learns its own
event id; "self-anchor to my own event id" is impossible.

Anchoring on a server-stamped RFC3339 timestamp is both necessary and *better*:

- **Reuses the existing path.** `replay_to_store(until: &str)` and
  `ExportEntityTree.as_of: Option<String>` already take a timestamp. No new replay code,
  full API consistency with `CreateSnapshot`.
- **Deterministic despite ties.** The replay query is
  `WHERE created_at <= until ORDER BY item_type, item_id, id DESC` — within the same
  instant it still picks the highest `id`, i.e. "the latest event at that moment." Correct.
- **Trustworthy.** The command stamps the *server's* `now()` (never a client clock).

Event-id anchoring remains a future precision upgrade (`RelationRegistration`/replay
already order by `id`; exposing `MAX(id)` to the handler + an `id <=` predicate is a
contained change) but buys nothing for human restore points today.

### 4. Write path: a command (raw client events are the narrow exception)

The real mutation primitive in myko is the raw `MEvent` — clients *can* push events
directly (`MykoMessage::Event` / `EventBatch` → `ctx.apply_event` in `ws_handler.rs`).
But restore-point creation must be **server-stamped** (the authoritative `now()`), so it
is a `CreateRestorePoint` **command**, not a direct client event. This is exactly where
the validated command path earns its keep. Direct client events stay reserved for their
narrow existing uses.

### 5. `root_type` + `root_id`, no `scope`

`Snapshot` carries both a `scope_id` (`belongs_to(Project)`, for cascade + listing) and a
`root_type`/`root_id` (what sub-tree was copied) because those genuinely differ — you can
snapshot a `Scene` while scoping its lifecycle to the `Project`.

A restore point needs no separate scope:

- There is no copied sub-tree; the bookmark is a global moment. `root_type`/`root_id` only
  say *what this bookmark is about* (and what to diff/reconstruct against). The root **is**
  the scope — a project-level point sets `root = project`, a scene-level point sets
  `root = scene`.
- Listing ("points for X") = `GetRestorePointsByQuery` on `root_id`/`root_type`.
- As a **framework** entity, `RestorePoint` cannot name rship's `Project` anyway (myko-core
  is upstream of the rship layer), so a typed `belongs_to(Project)` is not available here.
  `root_id`/`root_type` are plain `Arc<str>` — opaque to myko, meaningful to rship.

### 6. Restore points are **never** auto-deleted

No cleanup saga. A restore point deliberately **outlives** the entities it references —
that is the entire point: it must survive the deletion of an item so the item can be
restored later. The event log still holds the full history to reconstruct a long-deleted
root. Restore points are removed only by an explicit `DeleteRestorePoint`. A restore point
may therefore reference a `root_id` that no longer exists live; that is expected and fine.

### 7. Restoring resurrects deleted entities

Applying a restore point converges live state to `as_of(at_timestamp)` — and that
**includes bringing back entities that have since been deleted**. This is the headline
behavior of a restore (think `git checkout <old>`: the working tree fully matches the old
state, deletions and all), and it is the concrete reason for decision 6.

Crucially, resurrection is **not** a special case — it falls out of the reconstruction.
The replay query selects the latest event per entity *as of T* and keeps only those whose
latest event is a `SET` (`postgres.rs`):

- alive at T, **deleted after T** → latest event ≤ T is a `SET` → **present** in `as_of(T)`
  (this is the resurrection set);
- deleted *before* T → latest event ≤ T is a `DEL` → correctly absent.

So `as_of(T)` already *is* "everything alive at T." See the Restore section below for the
convergence rules and the ordering/id constraints resurrection imposes.

## Data model (myko-core)

```rust
#[myko_item]
pub struct RestorePoint {
    /// Display name for this restore point.
    #[searchable]
    pub name: String,

    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,

    /// Entity type this point is anchored to (e.g. "Project", "Scene"). Opaque to myko.
    pub root_type: Arc<str>,

    /// Entity id this point is anchored to. Opaque to myko; may not exist live.
    pub root_id: Arc<str>,

    /// RFC3339 timestamp, stamped server-side at creation. The `as_of` anchor.
    pub at_timestamp: String,
    // id / hash added by #[myko_item]
}
```

## Commands (myko-core)

```rust
#[myko_command(RestorePointId)]
pub struct CreateRestorePoint {
    pub name: String,
    #[serde(default)] pub description: Option<String>,
    pub root_type: Arc<str>,
    pub root_id: Arc<str>,
    // NOTE: no timestamp field — the handler stamps server now().
}
```

Handler: stamp `at_timestamp = Utc::now().to_rfc3339()`, build the `RestorePoint`,
`ctx.emit_set(&rp)`. `DeleteRestorePoint` / `DeleteRestorePoints` are generated by
`#[myko_item]`; no custom delete logic (see decision 6 — no cascade).

## Reading "what changed since a restore point"

This is the headline UX: *fast, live visualization of what changed since the restore
point.* It is a new generic, type-erased report in myko-core, a sibling to
`ExportEntityTree`.

```rust
#[myko_report(EntityChanges)]
pub struct EntityChangesSince {
    pub root_type: Arc<str>,
    pub root_id: Arc<str>,
    pub since_timestamp: String,
}

// EntityChanges: Vec<{ entity_type, id, change: Added | Removed | Modified(Vec<FieldDiff>) }>
```

### Why a log-delta, not a tree-reconstruction diff

Two ways to compute the changeset, with very different cost:

- **Tree-reconstruction diff** — reconstruct `as_of(T)` and `as_of(now)`, diff every entity
  in the tree. **O(tree size)** — re-materializes thousands of entities to surface the ten
  that moved. (This is what the existing client-side `entity-diff.ts` does on two
  `ExportEntityTree` payloads.)
- **Log-delta** (chosen) — range-scan the events table:
  `WHERE created_at > since_timestamp`, filtered to the root's descendant id set. **O(number
  of changes)** — touches only entities that actually changed, because that is precisely
  what the log records. For field-level "before", reconstruct just those few entities
  `as_of(T)`.

The log-delta reaches Postgres the same way `as_of` replay already does (a report reaching
a server-side provider via `ReportContext`'s `server_ctx`), so it needs no new access path —
only the new query. Field diffing reuses reflection over the JSON (as `entity-diff.ts`
already does); no per-entity generated diff code.

### Subtree scoping (the one detail to nail)

The events table keys by `(item_type, item_id)`, not by a denormalized scope. To restrict
"changes within this root's tree" you resolve the root's descendant id set (cheap, from the
relationship registry / current tree) and filter the range scan to it.

Edge case: entities **deleted** since `T` are not in the current tree, so the membership
filter must also admit `DEL` events whose `item_id` belonged to the tree *at T*. Decide the
exact membership rule (membership-at-either-endpoint) during implementation.

## Reactivity

Free. Because `RestorePoint` is on-log, the list and any "N changes since" badge update
through myko's normal reactive query/report machinery — no provider, no NOTIFY bridge.
(`EntityChangesSince` itself reads the event store directly, so if a *live* count is wanted
it recomputes on the same event-stream signals the stores already observe; a static
on-demand fetch is also fine for a v1.)

## Layer map

| layer | adds |
|---|---|
| **myko-core** | `RestorePoint` entity; `CreateRestorePoint` / generated deletes; `EntityChangesSince` report. Reuses existing `as_of` timestamp replay. |
| **myko-server** | the log-delta query backing `EntityChangesSince` (sibling to the `replay_to_store` history query in `postgres.rs`). |
| **rship** | UI: list restore points (live), create/delete, and render `EntityChangesSince` anchored on a point's `at_timestamp`. rship supplies `root_type`/`root_id` (e.g. a `Project`). |

## Restoring (apply) — follow-up phase

Restore-as-write re-emits a reconstructed past tree back to live state. This spec lands
the bookmarks + inspection + diff; the apply engine is a follow-up, but its semantics are
pinned here because they justify decisions 6 and 7.

**Convergence.** Applying restore point `T` makes the live tree equal `as_of(T)`:

- entity in `as_of(T)` but **currently deleted** → re-emit `SET` (**resurrect**);
- entity in `as_of(T)` with **different current fields** → re-emit `SET` with the T-era
  data (**revert**);
- entity **not** in `as_of(T)` but currently alive → emit `DEL` (**remove** post-T creations);
- otherwise unchanged → no-op.

The diff that drives this is the same comparison `EntityChangesSince` computes, run between
`as_of(T)` and current — so the apply engine is "turn the diff into an ordered event stream."

**Constraints the resurrection case imposes:**

- **Restore in place, not clone.** Re-emit with the **original ids** from `as_of(T)`
  (the reconstructed entities keep their id/hash), so existing references heal automatically.
  This is *not* the id-remapping clone path — a resurrected entity must reclaim its old id.
- **Ordering / referential integrity.** Resurrects must respect the relationship graph:
  a resurrected child whose parent was also deleted needs its parent resurrected first
  (topological order over `belongs_to`/`owns_many`/`ensure_for`, parents before children;
  removals last). This is the main correctness work of the apply engine.
- **Scope.** Apply is bounded to the restore point's `(root_type, root_id)` descendant set,
  using the same subtree resolution as `EntityChangesSince` (including the deleted-member
  edge case noted there).

## Sibling feature: Undo Transaction

A restore point converges the whole subtree to a past *moment*. An **undo transaction**
reverses a single *changeset*. They are two callers of the same converge engine and should
be built together.

The substrate already exists: `tx` groups the events of one command (an atomic changeset —
a "commit"), and `EventsForTransaction` reads them back. So both features reduce to
"converge to a target state by re-emitting events in topological order," differing only in
how the target is derived:

| | target state |
|---|---|
| restore point | `as_of(T)` over `(root_type, root_id)` |
| undo transaction | current state, with tx `X`'s touched entities set back to their pre-`X` state |

Like restore, an undo is **itself a new forward transaction** — it does not rewrite
history, so it is naturally redoable (undo the undo) with no special redo storage beyond a
client-side pointer.

### The full-snapshot constraint

myko events are **full-entity snapshots, not field deltas** (`MEvent.item: Value` is the
whole entity). There is no delta to invert — undoing means *setting an entity back to a
prior full state*. That is only unambiguous if nothing newer touched the entity, which
splits the feature into an easy half and a hard half:

- **Linear undo / redo (most-recent transactions)** — clean and the high-value win. For
  each entity the latest tx touched, restore it to its immediately-prior event (`DEL` if the
  tx created it, resurrect if the tx deleted it). A plain undo/redo stack. **Ship this.**
- **Reverting an arbitrary middle-of-history tx** — has conflict semantics. Entities the tx
  touched that **no later tx touched** revert cleanly; entities a later tx *also* modified
  cannot be cleanly reverted (setting them to the pre-tx value clobbers the newer change —
  the events record full states, not contributions). Requires explicit conflict surfacing
  (skip / overwrite / user choice). Same family as merge, scoped to one changeset. **Opt-in
  advanced mode; do not let it block linear undo.**

### Granularity note

A `tx` is whatever one command emitted, which can be a broad cascade (a delete that cascades
to many children is a single tx). Undo reverses the **whole** tx — usually what's wanted, but
the unit is "per command," not "per field."

### Shared engine

Both features want one primitive — call it the **converge engine**: given a target set of
`(entity_type, id) -> Option<value>` (Some = SET to this value, None = DEL) bounded to a
scope, diff against current and emit the minimal SET/DEL stream in dependency order
(parents before children, removals last; resurrected entities reclaim their original ids,
never remapped). Restore points feed it `as_of(T)`; undo feeds it the pre-`X` states of
tx `X`'s entities.

## Future work

- **Event-id anchoring** — exact, race-free anchors via `MAX(id)` + an `id <=` replay
  predicate, if timestamp precision ever proves insufficient.
- **Branching + merge** — only if multi-author offline divergence becomes a requirement.
  This is the part the linear/event-log model does *not* give for free.
