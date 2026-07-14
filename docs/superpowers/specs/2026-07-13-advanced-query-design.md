# Advanced Query: array-valued field matching (IN semantics)

- **Status**: proposed — ready to implement
- **Date**: 2026-07-13
- **Origin**: rship-side audit (2026-07-13) of query-chain workarounds; requested by Trevor
- **Scope**: myko + myko-macros + codegen/TS bindings. rship adoption is follow-up work tracked rship-side.

## Problem

`GetXsByQuery(PartialX)` matches every `Some` field by single-value equality. There is no way to
express "field IN {a, b, c}" for anything other than primary ids (`GetXsByIds`). An rship-wide
audit found **~45 distinct call sites** working around this, in two shapes:

1. **N query cells / N queries, one per value** — loops or `join_vec`ed `query_map` cells that
   differ only in one field's value. In reactive code this multiplies resting hyphae subscribers
   and adds `switch_map` + `join_vec` scaffolding whose re-knit cost is pure overhead.
2. **Whole-table fetch + local set filter** — `ByQuery(Default::default())` or `GetAllX` watched
   broadly, then filtered by set membership in the closure. The reactive cell re-ticks on **every**
   write to the table, not just matching rows.

Three rship code comments narrate the dilemma explicitly (being forced to pick between "unfiltered
query re-ticked this world cell on EVERY value write anywhere in the system" and "N per-anchor
cells"): `libs/entities/comp-engine/src/runtime/input/candidates.rs:73-78` and `:308-313`,
`libs/entities/comp-engine/src/runtime/input/contributor.rs:119-127`. The array-valued field match
is precisely the missing middle point.

## Constraints (set by Trevor)

- **Do NOT change the generated `PartialX` structs or `GetXsByQuery`.** Existing call sites,
  wire shapes, and TS bindings stay byte-identical.
- This is a **new query type** with **new query-context methods** — working name
  `query_filtered(...)` and friends (final names in §3).
- The pure-Partial query pattern gets deprecated **eventually**, after adoption proves the new
  shape. Deprecation is explicitly out of scope for this work.

## Design

### 1. Per-type field filters

Filters are **custom per data type**, not one generic enum. Selection is driven by a trait with an
associated type, so the macro never has to sniff field types syntactically — it just emits
`Option<<FieldType as Filterable>::Filter>` and the compiler resolves the right filter:

```rust
pub trait Filter<T> {
    fn matches(&self, value: &T) -> bool;
}

pub trait Filterable: Sized {
    type Filter: Filter<Self>;
}
```

Concrete filter types:

```rust
// IDs — ALWAYS exact match (single or set). No partial/range matching, ever.
// Eq/In on ids is what the index routing (§4) can serve.
pub enum IdFilter<T> {
    Eq(T),
    In(Vec<T>),
}

// Number-like (all integer + float primitives)
pub enum NumericFilter<T> {
    Eq(T),
    In(Vec<T>),
    Range { min: Option<T>, max: Option<T> },   // inclusive bounds; both None is invalid
}

// Strings (String / Arc<str> non-id fields)
pub enum StringFilter {
    Eq(Arc<str>),
    In(Vec<Arc<str>>),
    Contains(Arc<str>),      // substring, case-sensitive (case-insensitive: open question)
    StartsWith(Arc<str>),
}

// Bools — equality only. In(Vec<bool>) is either an Eq or match-everything, so it doesn't exist.
impl Filterable for bool { type Filter = bool; }   // Filter<bool> for bool: `self == value`

// Enums and other exact-only types — Eq plus set membership (useful for e.g. state IN [Armed, Building])
pub enum EqFilter<T> {
    Eq(T),
    In(Vec<T>),
}
```

**Guiding principle: each type's filter exposes exactly the operations that are meaningful for
that type, and nothing more.** Bool is bare equality; ids never partial-match or range; strings
never range; numbers never substring. If an operation would be degenerate (reducible to Eq or
match-all), it doesn't get a variant.

`Filterable` impls: numeric primitives → `NumericFilter`; `String`/`Arc<str>` → `StringFilter`;
`bool` → `bool`; generated entity id newtypes (`XId`) → `IdFilter` (emitted by `#[myko_item]`
alongside the id type itself — this is what enforces "ids are always exact"); fallback for enums
and other opaque types → `EqFilter` via per-type impls or a macro-emitted impl. Filters get
`From<T>` (→ `Eq`) and, where `In` exists, `From<Vec<T>>` for call-site ergonomics.

Semantics and invariants:

- `Eq`/`In` use plain `==` — same equality semantics as today's Partial matching; **no numeric
  cross-type coercion** (that's an rship `BindingValue` concern, not myko's).
- **Canonicalization** (required for query-cache identity): `In` values sorted + deduped,
  `In([x])` → `Eq(x)`, `Range{min:Some(a), max:Some(a)}` → `Eq(a)`, so equivalent filters
  hash/compare equal and share one reactive cell.
- `In([])` is legal and **matches nothing** — document this loudly; it is the correct behavior
  for "scope to this (possibly empty) derived set" call sites, and a silent footgun otherwise.
- `Option<T>` entity fields: filter applies to the inner value; a `None` field matches no filter.
  Explicit null-matching (`IsNull`/`IsSet`) is an open question — don't block on it.
- Room to grow (NOT now): `NotEq`, case-insensitive string modes, `EndsWith`. Choose a wire
  representation that tolerates adding variants.

### 2. Generated query type (in `#[myko_item]`)

Per entity `X`, generate alongside the existing set:

```rust
pub struct XFilter {             // every field: Option<<FieldType as Filterable>::Filter>
    pub some_field: Option<<SomeType as Filterable>::Filter>,
    ...
}
pub struct GetXsByFilter(pub XFilter);
```

plus the `matches` impl (mirror of `myko-macros/src/partial_matches.rs`, with `Filter::matches`
per field). Only the Get variant for now — no Count/Delete advanced variants until asked for.

### 3. Context surface

Method names are FINAL. Each existing context method gets a `_filtered` twin taking
`GetXsByFilter(XFilter)`:

- `ctx.query_filtered(...)` (⇄ `query`)
- `ctx.query_map_filtered(...)` (⇄ `query_map`)
- `ctx.exec_query_filtered(...)` (⇄ `exec_query`)
- `ctx.exec_query_first_filtered(...)` (⇄ `exec_query_first`)
- reactive (phase 2, §5): `ctx.query_live(filter_cell)` — one argument; the entity/query type is
  inferred from `Cell<XFilter>`, so `GetXsByFilter` is not passed separately. No `_live` variants
  of the exec/one-shot forms (a cell parameter is meaningless outside a reactive graph).
- client side: no new method — `watch_query(GetXsByFilter(...))` (TS + Rust client) rides the
  existing path as a normal registered query type; verify end to end.

If the plumbing allows the new query structs to flow through the *existing* ctx methods, the
`_filtered` methods can be thin wrappers — but keep the distinct names as the public seam so the
eventual Partial deprecation is a mechanical rename (`query_map` → `query_map_filtered`).

### 4. Routing — the critical requirement

**An `In` on a `#[belongs_to]` field MUST route through `BelongsToSourceIndex` as a union of K
buckets** (one per array value), not fall back to whole-table scan-and-match. The per-type filter
design makes this total: belongs_to fields are id newtypes, id newtypes get `IdFilter`, and
`IdFilter` has only `Eq`/`In` — so **every expressible filter on an indexed field is
index-servable by construction**. There is no filter a caller can write on a belongs_to field
that forces a scan. If it scans, the
reactive cell re-ticks on every table write and the feature rebuilds the exact problem it exists
to solve, under a new name. This is the difference between a major win and a footgun — treat a
scan fallback on an indexed field as a bug, not a degraded mode.

Details:

- Compound keys: today the bucket key is the ordered values of the populated belongs_to fields
  (`CompoundKey = Vec<Arc<str>>` in `core/query/registration.rs`). One `In` field → K keys.
  Multiple `In` belongs_to fields → cartesian product of keys; either cap the product size or
  document the blow-up and log when it exceeds a threshold.
- Reactive correctness: an item whose fk mutates INTO the queried set must appear in the cell
  (and tick it); mutating OUT must remove it — same guarantees as today's single-bucket routing,
  including the bucket_for TOCTOU class fixed in PR #36.
- Non-indexed fields: linear predicate matching is fine (same as Partial today). For large `In`
  arrays build a `HashSet` above a small length threshold inside `matches`.

### 5. Phase 2 — reactive filter parameters: `query_live(Cell<XFilter>)`

The value-based query still leaves one piece of scaffolding at rship call sites: when the filter's
key set is itself derived reactively (ids from query A feeding query B), the caller must wrap the
query in `switch_map` to re-issue it on set changes. Phase 2 removes that by accepting the filter
**as a cell**:

```rust
ctx.query_live(filter_cell)   // filter_cell: Cell<XFilter> (or impl Watchable<XFilter>); entity inferred
```

Decided shape: the reactive unit is the **whole `XFilter`** — NOT per-field `Cell<Filter>` inside
the struct. `XFilter` stays a plain serializable value type; callers build `filter_cell` with
ordinary `map`/`join` (which is exactly the cell they feed into `switch_map` today); myko diffs
the incoming filter per field internally so unchanged fields cost nothing.

Why this is strictly better than `switch_map` for the dominant case:

- The dataflow graph persists; filter changes flow as values. Internally, an `In` set change on an
  indexed field becomes an **incremental bucket diff** against `BelongsToSourceIndex` — subscribe
  the added keys, unsubscribe the removed ones, emit membership changes as diffs — instead of
  switch_map's full tear-down/re-knit of the inner graph.
- It eliminates a whole hazard class: state held downstream of the query (e.g. `state_transition`)
  survives filter changes, because nothing is torn down. (The "state machine resets when
  switch_map re-fires" bug shape becomes unrepresentable at these sites.)

Known costs, accepted:

- **Server-side only.** A cell can't cross the wire; TS/remote clients stay on the value-based
  query (re-subscribe on parameter change, as today). Every switch_map site in the rship audit is
  server-side reactive code, so this covers the actual pain.
- **No value-identity cache sharing.** The query cache dedups by canonical query value; a cell is
  object identity. `query_live` cells are per-call-site. Sites whose filter is genuinely static
  should keep using the value-based form to retain cross-coordinator sharing.
- **Scan predicates rescan on filter change.** `Range`/`Contains` changes re-evaluate the scoped
  table (indexed `Eq`/`In` get the bucket-diff path). Document loudly: do not feed a
  per-frame-changing `Range` into `query_live`.

**Both forms are kept permanently** — `query_live` does not subsume the value-based methods:

- One-shot imperative queries (`exec_query_*` in command handlers) have no reactive graph to hold
  a cell; wrapping a one-shot filter in a constant cell is noise.
- Static filters need **value identity** to share the query cache across call sites/coordinators;
  a cell is object identity, and myko cannot know a cell will never tick. Reports whose filter is
  fixed for their lifetime should use the value form to keep that sharing.
- The value type/query must exist anyway for the wire (TS + remote `watch_query`).

Call-site rule of thumb: filter derived from other cells → `query_live`; filter fixed for the
lifetime of the report/command → value-based. The eventual deprecation targets the *Partial*
pattern only — both filter forms are permanent.

Sequencing: ship the value-based query first; `query_live` reuses the same matching + routing
internals and adds only the per-field diff + incremental bucket subscription layer.

### 6. Codegen / TS / wasm

- Generate TS types for `XFilter`, each filter type (`IdFilter`, `NumericFilter`,
  `StringFilter`, `EqFilter`), and the query class; register via the normal `register_ts_export`
  path. Never hand-written (rship rule). The per-type filters give TS callers honest unions —
  an id field's filter type simply has no `contains`/`range` arm to misuse.
- Wire: new types, so no back-compat constraint on their own shape — but pick the serde
  representation deliberately; explicit tagging recommended (safer than untagged against
  `Vec<...>`-typed fields and against adding variants later).
- The rship entity layer compiles to wasm32 — new generated code must not regress wasm gating.

## Acceptance criteria

1. Unit: per-type filter matching — `IdFilter` Eq/In/empty-In; `NumericFilter` Range bounds
   (inclusive, open-ended min-only/max-only, degenerate `min==max`); `StringFilter`
   Contains/StartsWith; bool bare equality. Canonicalization: permuted `In` arrays produce
   identical query identity; `In([x])` ≡ `Eq(x)`; `Range{a,a}` ≡ `Eq(a)`. Compile-fail (or
   simply API-absence) check that id fields expose no partial/range operations and bool exposes
   no `In`.
2. Routing: entity with `#[belongs_to]`, advanced query with `In` on the fk — verify via query
   runtime metrics that writes to non-matching buckets do NOT tick the cell, and K-bucket union
   returns exactly the union. Include a 2-belongs_to compound-key case.
3. Reactive: item mutated into / out of the `In` set appears/disappears with a tick.
4. Cache: two call sites issuing equivalent advanced queries share one cell
   (`query_factories_by_id` counters).
5. TS: codegen emits the types; a TS `watchQuery(new GetXsByFilter(...))` round-trips.
6. `cargo check` on wasm32 target for a consumer crate with the macro applied.
7. Phase 2 (`query_live`): mutating the filter cell's `In` set updates results via incremental
   bucket subscription (verify with query runtime metrics: no full re-registration, only delta
   buckets touched); downstream cell state survives a filter change (no graph teardown); a
   `Range` filter change re-evaluates correctly.

## Motivating call sites (rship, for API-shape validation)

The audit's highest-leverage targets — use these to sanity-check the ergonomics before finalizing:

| Site | Today | With advanced query |
| --- | --- | --- |
| `comp-engine/runtime/input/discovery.rs:114-172, 248-300` | one `GetBindingNodesByQuery{scene_id: Some(s)}` cell per active scene, then one values cell per node, nested `switch_map` + `join_vec` | `scene_id: In(active)` + `node_id: In(nodes)` → 2 cells per coordinator, one nesting level deleted |
| `comp-engine/runtime/input/candidates.rs:96-98` | ALL BindingNodeConnections into every engine's world cell, filtered locally by consumer set | `end_node_id: In(consumers)` |
| `candidates.rs:79-92`, `contributor.rs:128-145`, `comp_engine.rs:212-243`, `kind_resolution.rs:37-54` | 2–4 sibling cells differing only in `anchor_id`, joined | `anchor_id: In([...])` → 1 cell each |
| `assignment/src/instance_assign.rs:58, 150, 253` | `GetAllInstances` + `(cluster_id, service_id)` set filter (dispatch + reconcile backbone) | `cluster_id: In(assigned)` + local service check (composite-tuple IN is a documented non-goal) |
| `engine/src/node_executor/mod.rs:92-118` | ~12 permanent watchers, one per `node_type` | `node_type: In(handled)` → 1 diff stream |
| `runtime/src/target_swap_candidates.rs:80-97` | live UI report watching the ENTIRE BindingNodeValue table, set-filtered | `node_id: In(self.node_ids)` |
| `nodes/src/binding_node.rs:418-560` (CloneBindingNodes) | 4 per-node `exec_query` loops (one duplicated) | 4 array queries total |
| `runtime/src/scene_tag_build.rs:113-128` | per-name `GetTagsByQuery{name: Some(n)}` loop — a **string field**, never coverable by the ByIds API | `name: In(names), scope_id: Eq(x)` |

Recurring shapes the API must handle well: `Eq` on one field combined with `In` on another
(appears 3×+); `In` on non-id string fields; `In` sets derived reactively (callers will wrap in
`switch_map` — that's expected and fine, the win is inside).

## Non-goals

- Changing `PartialX` / `GetXsByQuery` in any way.
- Composite-tuple membership (`(a, b) IN {...}`), ranges, negation, OR trees.
- Deprecating the Partial pattern (later, separate work, after rship adoption).
- Advanced variants of Count/Delete queries.
