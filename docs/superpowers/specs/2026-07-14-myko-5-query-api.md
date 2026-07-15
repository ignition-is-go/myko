# myko 5.0: the filter pattern becomes the query pattern

- **Status**: proposed — needs sign-off on naming + the `CountXs`/`PartialX` scope
  decision (§3.2) before implementation starts
- **Date**: 2026-07-14
- **Origin**: direction change from Trevor, relayed via marshal from quiet-ember's
  session (rship, currently testing against this branch)
- **Supersedes**: `docs/superpowers/specs/2026-07-13-advanced-query-design.md`
  (phase 1 + phase 2 of that spec are both already shipped, on this branch,
  `feat/advanced-query-live`; that document's phase 1 explicitly deferred this
  exact rename — see "Non-goals" — as separate, later work. This is that work.)
- **Scope**: myko + myko-macros + generated TS bindings. **Breaking, myko 5.0.0.**
  rship is currently pinned to published 4.24.2 (dropped its local path override)
  and will not track this branch live — free to break anything.

## Why now

Phase 1 (`docs/.../2026-07-13-advanced-query-design.md`) shipped the per-type
filter pattern (`Filterable`/`Filter<T>`/`IdFilter`/`NumericFilter`/
`StringFilter`/`EqFilter`) additively, alongside the original `PartialX` /
`GetXsByQuery(PartialX)` query pattern, specifically to avoid a breaking change
while the design proved itself. Its own §3 named the seam explicitly: `_filtered`
suffixes exist "so the eventual Partial deprecation is a mechanical rename."

That adoption proof happened over the course of this session: rship hit and
worked through the full `Filterable` gap taxonomy (opaque JSON payloads,
`Vec`/`HashMap`/tuple container fields, foreign numeric newtypes via
`ordered_float`, and finally domain subtypes via `#[myko_subtype]`'s new
auto-impl). The filter pattern is now the *more* capable, *more* ergonomic
surface — `GetXsByFilter`/`XFilter` already has everything `GetXsByQuery`/
`PartialX` has (see §3.1) plus `In`/`Range`/`Contains` and a reactive
`query_live` path `PartialX` never had. Keeping both permanently means two
per-entity query types, two per-entity "matches" structs, and four
`_filtered`-suffixed wrapper methods that exist purely to avoid a name
collision with the type that should have had the good name.

Direction from Trevor (verbatim, relayed): "Stop shoehorning advanced queries
in as additive; make the API what it should be." This spec reclaims the
canonical names for the canonical (filter-based) pattern and deletes the
now-legacy `PartialX`-as-query pattern outright.

## 1. Final naming (needs sign-off — this is the one open naming decision)

| Today (phase 1, additive) | 5.0 (canonical) |
| --- | --- |
| `XFilter` (per-entity filter struct) | **`XQuery`** |
| `GetXsByFilter(XFilter)` | **`GetXsByQuery(XQuery)`** — reclaims the name freed by deleting the old `GetXsByQuery(PartialX)` |
| `ctx.query_filtered(...)` / `query_map_filtered(...)` / `exec_query_filtered(...)` / `exec_query_first_filtered(...)` | **deleted** — the plain `ctx.query(...)` / `query_map(...)` / `exec_query(...)` / `exec_query_first(...)` take over unchanged (see §3.3, they're already fully generic, zero code changes needed to the methods themselves) |
| `ctx.query_live(filter_cell: Cell<XFilter>)` | **unchanged**, just follows the `XFilter → XQuery` rename (`Cell<XQuery>`) — orthogonal to this break per Trevor's direction |
| `PartialX` / `Partial{Entity}` struct, `#[derive(partially::Partial)]`, `PartialMatches` derive | **deleted entirely** — see §3.2, not just its query role |
| `IdFilter<T>`, `NumericFilter<T>`, `StringFilter`, `EqFilter<T>`, `Unfilterable`, `Filterable`, `Filter<T>`, `CanonicalFilter`, `BelongsToRoute`, `LiveFilterQuery` | **unchanged** — these are the underlying mechanism, not entity-facing naming, and aren't confusingly named today |

Rationale for `XQuery` over keeping `XFilter`: once `GetXsByQuery` no longer
means "by partial value-equality," the wrapped payload should read as *the
query*, not *a filter passed to a query* — `GetXsByQuery(XQuery)` reads as one
coherent noun phrase; `GetXsByQuery(XFilter)` reads like a type mismatch on
first glance. Trevor's own suggested shape in the direction-change message
used `XQuery` — this spec adopts it as the default, flagged for explicit
confirmation since he left exact naming open.

## 2. Final per-entity generated surface

For an entity `X` with fields `some_field: SomeType`, `parent_id: ParentId`
(`#[belongs_to]`):

```rust
// The ONLY per-entity query-parameter type. Was XFilter.
#[derive(Clone, Default, PartialEq, Debug, Serialize, Deserialize, TS)]
pub struct XQuery {
    pub id: Option<IdFilter<XId>>,
    pub parent_id: Option<IdFilter<ParentId>>,
    pub some_field: Option<<SomeType as Filterable>::Filter>,
}
impl XQuery {
    pub fn matches(&self, item: &X) -> bool { /* unchanged body, renamed receiver */ }
    pub fn canonicalize(self) -> Self { /* unchanged */ }
    #[cfg(not(target_arch = "wasm32"))]
    pub fn belongs_to_route(&self) -> Option<BelongsToRoute> { /* unchanged */ }
}
register_ts_export!(XQuery);

#[cfg(not(target_arch = "wasm32"))]
impl query::LiveFilterQuery for XQuery {
    type Item = X;
    fn entity_type() -> &'static str { "X" }
    fn matches(&self, item: &X) -> bool { XQuery::matches(self, item) }
    fn belongs_to_route(&self) -> Option<BelongsToRoute> { XQuery::belongs_to_route(self) }
}

// The ONLY per-entity "get by query" type. Reclaims the name.
#[myko_non_hash_cache_key]
#[myko_manual_cache_key]
#[myko_query(X)]
pub struct GetXsByQuery(pub XQuery);

impl CacheKey for GetXsByQuery {
    fn cache_key(&self, state: &mut dyn Hasher) {
        cache::write_serde_cache_key(&self.0.clone().canonicalize(), state);
    }
}
impl QueryHandler for GetXsByQuery {
    fn test_entity(ctx) -> bool { ctx.query.0.matches(&ctx.item) }
    #[cfg(not(target_arch = "wasm32"))]
    fn build_view(ctx) -> Option<FilteredCellMap> { /* unchanged K-bucket union routing */ }
}

// CountXs now takes XQuery too (see §3.2) instead of PartialX.
#[myko_non_hash_cache_key]
#[myko_report(XCount)]
pub struct CountXs(pub XQuery);
impl ReportHandler for CountXs {
    type Output = XCount;
    fn compute(&self, ctx: ReportContext) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let query = GetXsByQuery(self.0.clone());   // was: GetXsByQuery(PartialX) via the OLD type
        let source = ctx.query_map_by_str(query);
        source.size().map(...)
    }
}
```

Call-site shape, unchanged method names, new type:

```rust
ctx.query(GetXsByQuery(XQuery { some_field: Some(v.into()), ..Default::default() }))
ctx.query_map(GetXsByQuery(XQuery { parent_id: Some(vec![a, b].into()), ..Default::default() }))
ctx.exec_query(GetXsByQuery(XQuery { ...}))
ctx.exec_query_first(GetXsByQuery(XQuery { ...}))
ctx.query_live(filter_cell)   // filter_cell: impl Watchable<XQuery> — unchanged, just the renamed type
```

`GetAllXs`, `GetXsByIds`, `DeleteX`, `DeleteXs`, `GetXById` and the rest of
`#[myko_item]`'s auto-generated surface are **untouched** — this only replaces
the partial-match query and its count twin.

## 3. What's being deleted, and why each deletion is safe

### 3.1 `GetXsByQuery(PartialX)` (the query role)

Straight deletion. `GetXsByFilter`/`XFilter` (renaming to `GetXsByQuery`/
`XQuery`) is already a strict superset of what `PartialX`-as-query could
express: every field `PartialX` could pin with `Some(v)` (exact equality),
`XQuery` can pin identically via `Some(v.into())` → `Eq(v)`, plus `In`/`Range`/
`Contains` that `PartialX` never had. There is no `PartialX`-as-query call
shape that doesn't have a direct `XQuery` equivalent.

### 3.2 `PartialX` the struct, and `PartialMatches` — the scope decision that needs sign-off

Trevor's message said "only the query role is being replaced" and asked for
an audit of other `PartialX` uses before deleting it. That audit (this
session, full results in the git history / available on request) found
**exactly one other role**: `CountXs(PartialX)` (`item.rs:684-712`) — a
report, generated unconditionally per entity — constructs a `GetXsByQuery`
internally (`item.rs:704`, `#get_by_partial_ident(self.0.clone())`) and runs
it via `ctx.query_map_by_str(query).size()`. No partial-update/PATCH/setter
role exists anywhere in myko: `setter.rs`'s codegen does full-clone +
single-field override, never touches `PartialX`; the `partially` crate's own
merge/`apply_to` machinery (its actual intended use case) is never invoked
anywhere in the codebase — `partially::Partial` is used purely as a "generate
an all-`Option` sibling struct" code generator here, nothing more.

**Recommendation** (needs explicit confirmation, this is the one place this
spec goes beyond exactly what was asked): delete `PartialX`/`PartialMatches`
entirely, and migrate `CountXs(PartialX)` → `CountXs(XQuery)` — mechanically
identical to today's body, just swapping the inner type and reusing the new
`GetXsByQuery` (see §2's `CountXs` example). This is the only way to
literally delete `PartialX` per "zero parallel types"; leaving `PartialX`
alive solely to back `CountXs` would mean 5.0 still generates two per-entity
structs with overlapping purpose, which is the exact thing this break exists
to eliminate. If Trevor wants `CountXs` to keep taking a flat partial-equality
struct for some reason not surfaced in this audit, say so explicitly and
`PartialX`/`PartialMatches` survive scoped to that one role only.

### 3.3 The four `_filtered` methods

`query_filtered`/`query_map_filtered`/`exec_query_filtered`/
`exec_query_first_filtered` (`core/src/core/query/context.rs:101-114`,
`core/src/server/context.rs:1281-1299`, `core/src/core/command/handler.rs:
368-377,405-411`) are, today, **literal one-line pure delegations** to their
non-suffixed twins (`{ self.query_map(query, request) }`, etc.) — generic
over `Q: QueryParams`/`QueryFactory` with zero `XFilter`-specific type
constraints. They exist solely as a naming seam so `GetXsByFilter` calls
didn't collide with `GetXsByQuery(PartialX)` calls while both patterns were
live. Deleting them and using the plain names directly for the (renamed)
canonical query requires **no changes to `query`/`query_map`/`exec_query`/
`exec_query_first` themselves** — they already accept anything implementing
`QueryParams`, which `GetXsByQuery(XQuery)` will continue to. This is a
delete-four-functions-and-one-test change, not a reimplementation
(`core/tests/advanced_query_filter_test.rs`'s
`query_map_filtered_is_a_working_alias_for_query_map` test is deleted
alongside them — its entire premise no longer exists once there's only one
name).

### 3.4 `query_live` — unaffected

Per Trevor's direction (point 2): stays exactly as shipped, just follows the
`XFilter → XQuery` rename mechanically (`Cell<XFilter>` → `Cell<XQuery>` in
its signature and doc comments). No design changes.

## 4. In-repo call sites this touches (from this session's audit — not exhaustive of downstream)

Real Rust call sites needing updates when this lands, inside this repo:

- `libs/myko/core/src/bench_entities.rs` — `GetBenchItemsByQuery(PartialBenchItem{...})` (1 site, `#[cfg(feature = "bench")]`).
- `libs/myko/core/tests/query_cache_leak_test.rs` — 5 sites, same pattern, same feature gate.
- `libs/myko/core/tests/advanced_query_filter_test.rs` — the `_filtered`-alias test, deleted.
- `item.rs`'s own `CountXs` codegen (§3.2).
- Every `#[myko_item]` entity's generated surface changes shape regardless of
  explicit callers (2 production entities — `Client`, `Server` — plus ~10
  bench/test fixtures across this session's added fixtures).

No hits in `myko-server`, `myko-leptos`, or the non-Cargo binding trees
(`ts`/`py`/`cpp`/`csharp`) — those only ever consume the *generated* per-entity
surface, which regenerates automatically.

## 5. Wire / TS impact

- `GetXsByFilter`'s TS class generation is unaffected structurally — the
  generic `generate_query_class` codegen pass (`core/src/codegen/mod.rs:451`)
  iterates `inventory::iter::<QueryRegistration>()` with no
  `GetXsByQuery`-vs-`GetXsByFilter` special-casing. Renaming the Rust-side
  `#[myko_query(X)]` struct from `GetXsByFilter` to `GetXsByQuery` is
  sufficient; no codegen-source changes needed.
- The OLD `GetXsByQuery` TS class (currently `Omit<PartialClient, 'tx' |
  'createdAt'>` — flat field-to-value args) disappears; the class that
  survives under that name has the NEW (nested-filter) constructor shape
  (`Omit<ClientQuery, 'tx' | 'createdAt'>` — roughly `{ someField?: {kind:
  "eq", value: T} | {kind: "in", value: T[]} | ... }` per filter type's
  `#[serde(tag = "kind", content = "value")]` wire tagging).
- `PartialClient.ts`-style files disappear if §3.2's full deletion is
  confirmed.
- **TS bindings in this checkout are currently stale for the phase-1 feature
  already** — `GetClientsByFilter`/`ClientFilter` don't exist yet in either
  generated output directory because `cargo flux run gen` hasn't been re-run
  since phase 1 landed. Regenerating is a required step of this work, not
  optional — do it after the Rust-side rename compiles clean, in the usual
  hot-reload flow (per CLAUDE.md: don't run typegen manually, the user runs it
  in hot-reload mode).

## 6. Migration table (mechanical, for rship's scripted rename)

| Old form | New form |
| --- | --- |
| `PartialX { field: Some(v), ..Default::default() }` used as `GetXsByQuery(PartialX{...})` | `XQuery { field: Some(v.into()), ..Default::default() }` used as `GetXsByQuery(XQuery{...})` |
| `ctx.query_filtered(GetXsByFilter(XFilter{...}))` | `ctx.query(GetXsByQuery(XQuery{...}))` |
| `ctx.query_map_filtered(GetXsByFilter(XFilter{...}), req)` | `ctx.query_map(GetXsByQuery(XQuery{...}), req)` |
| `ctx.exec_query_filtered(GetXsByFilter(XFilter{...}))` | `ctx.exec_query(GetXsByQuery(XQuery{...}))` |
| `ctx.exec_query_first_filtered(GetXsByFilter(XFilter{...}))` | `ctx.exec_query_first(GetXsByQuery(XQuery{...}))` |
| `ctx.query_map(GetXsByFilter(XFilter{...}), req)` *(already-plain filter calls from phase 1 adoption)* | `ctx.query_map(GetXsByQuery(XQuery{...}), req)` — rename the two type names only, method call unchanged |
| `ctx.query_live(filter_cell)` where `filter_cell: Cell<XFilter>` | `ctx.query_live(filter_cell)` where `filter_cell: Cell<XQuery>` — rename the type only |
| `CountXs(PartialX{...})` | `CountXs(XQuery{...})` |
| TS: `new GetXsByQuery({ field: value })` | TS: `new GetXsByQuery({ field: { kind: "eq", value } })` (or `{ kind: "in", value: [...] }` for a set) |
| TS: `import type { PartialX } from "./PartialX"` | TS: `import type { XQuery } from "./XQuery"` |

Field-level filter construction (`IdFilter`/`NumericFilter`/`StringFilter`/
`EqFilter`) is **unchanged** by this migration — only the outer per-entity
type names move. Anywhere rship already adopted `GetXsByFilter`/`XFilter`
during this session (the 14 call-site migrations quiet-ember mentioned as
stashed) only needs the two type-name renames in the table above, not new
filter-construction logic.

## 7. Explicitly unchanged / out of scope

- `query_live` design and implementation (§3.4).
- The filter primitive types and traits (`Filterable`, `Filter<T>`,
  `CanonicalFilter`, `IdFilter`, `NumericFilter`, `StringFilter`, `EqFilter`,
  `Unfilterable`) and this session's additions (`impl_filterable_eq!`,
  `impl_filterable_opaque!`, container blanket impls, the `ordered-float`
  feature, `#[myko_subtype]`'s auto-`Filterable` impl and `manual(serde,
  ts)`). All of it is reused as-is by the renamed `XQuery`/`GetXsByQuery`.
- Advanced (filter-based) variants of `DeleteX`/`DeleteXs` — still out of
  scope, same as phase 1's non-goals. `CountXs` is in scope only because it
  already, incidentally, depended on the type being deleted (§3.2) — it is
  not gaining new filter capability it didn't already have via `PartialX`
  equality matching, just being kept alive on the surviving type.
- `rship`'s own compat types (`BindingValue`/`SchemaRef`/`JsonSchemaNode`
  conversions) — rship-side work per quiet-ember, not this spec's concern.

## 8. Acceptance criteria

1. `PartialX`/`Partial{Entity}` structs, the `PartialMatches` derive, and the
   old `GetXsByQuery(PartialX)` codegen no longer exist anywhere in
   `#[myko_item]`'s output (confirmed §3.2's sign-off resolves in favor of
   full deletion).
2. `XQuery`/`GetXsByQuery(XQuery)` codegen is byte-for-byte the old
   `XFilter`/`GetXsByFilter(XFilter)` codegen with only the two identifiers
   renamed — no behavioral changes to matching, canonicalization, or
   `belongs_to_route` routing.
3. `query_filtered`/`query_map_filtered`/`exec_query_filtered`/
   `exec_query_first_filtered` no longer exist; `query`/`query_map`/
   `exec_query`/`exec_query_first` accept `GetXsByQuery(XQuery)` with no
   changes to those four methods' own source.
4. `CountXs(XQuery)` compiles and behaves identically to today's
   `CountXs(PartialX)` for every filter shape `PartialX` could express
   (`Eq`-only matching).
5. `query_live(Cell<XQuery>)` — same acceptance criteria as phase 1/2's
   original spec, unchanged, just the renamed type.
6. `cargo test`/`clippy -D warnings`/`cargo fmt --check` clean on native +
   `wasm32-unknown-unknown`, matching this session's established verification
   bar.
7. `cargo flux run gen` (hot-reload, user-run) regenerates TS with no
   `PartialX`/`GetXsByFilter`/`_filtered` remnants; the class that survives as
   `GetXsByQuery` has the nested-filter constructor shape.
8. Version bumped to `5.0.0` across the workspace (myko, myko-macros,
   myko-server, myko-leptos — currently lockstep at `4.24.2`) — handled by
   the existing CI stamping on merge, not a manual edit as part of this spec's
   implementation.

## 9. Sequencing (proposed, not yet executed)

quiet-ember suggested branching `feat/myko-5` off `feat/advanced-query-live`
so the 4.x-additive branch stays intact as a fallback if 5.0 needs to be
shelved. That's a reasonable, low-risk default (branching is cheap and
reversible) but hasn't been created yet — this spec is the deliverable
requested *before* any code changes, per the original ask. Implementation
order, once the naming (§1) and `PartialX` scope (§3.2) decisions are
confirmed:

1. `item.rs`: rename `XFilter`→`XQuery`, `GetXsByFilter`→`GetXsByQuery`;
   delete the old `GetXsByQuery(PartialX)`/`PartialX` codegen; migrate
   `CountXs` to `XQuery`.
2. Delete the four `_filtered` context methods + their test.
3. Update in-repo call sites (§4).
4. Full verification sweep (native + wasm32 test/clippy/fmt).
5. Regenerate TS bindings (user-run, hot-reload).
6. Post the final migration table (§6, already drafted here) for rship's
   scripted rename once the surface is confirmed stable.
