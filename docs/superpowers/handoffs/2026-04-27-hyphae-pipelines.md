# Handoff: hyphae lazy pipeline / MapQuery migration

**Date:** 2026-04-27
**Branch:** `feat/hyphae-pipelines`
**Status:** Functionally complete, 4 commits unpushed, ready to push or squash.

---

## TL;DR

Migrated myko to hyphae's lazy pipeline API. `Cell::map(...)` → `MapPipeline` requires `.materialize()` to produce a `Cell`; `CellMap` operators like `.select(...)`, `.inner_join(...)` return uncompiled `MapQuery` plans. Trait return types on `ReportHandler::compute`, `QueryHandler::build_view`, and `ViewHandler::build_cell` are widened to `impl Pipeline<...>` / `impl MapQuery<...>` so trait-method bodies can compose multiple operators without intermediate cell allocations between them — the framework materializes once at the registration / cache boundary.

WeakCell remains the cache handle (per hyphae guidance). A SharedPipeline-based cache experiment was committed and reverted in this branch's history.

---

## Branch state

```
4dcb9f7e Revert "feat(server): cache reports as SharedPipeline handles"
c98c419d feat(server): cache reports as SharedPipeline handles            ← reverted by 4dcb9f7e
49d3506b fix(core): adapt client_session tests to MapQuery select API
279a597e feat(core): widen Query/View handlers to MapQuery return type
a8eee4e3 feat(core): adopt new hyphae MapPipeline API
```

- 5 commits ahead of `main`
- 4 commits ahead of `origin/feat/hyphae-pipelines` (push pending)
- Net diff: 27 files, +218 / -125
- The c98c419d + 4dcb9f7e pair nets to zero. Squashable if cleaner history is preferred.

Verification:
- `cargo check --target-dir target/claude --workspace` — clean
- `cargo test --target-dir target/claude --workspace` — 56 tests pass, 0 fail
- `cargo clippy --target-dir target/claude --workspace -- -D warnings` — clean

---

## What changed

### 1. Pipeline migration (`a8eee4e3`)

Local hyphae replaced eager `Cell::map(...) → Cell<U>` with lazy `MapPipeline<S, T, U, F>` that requires `.materialize()` to produce a `Cell`. `Cell`-only methods (`.with_name`, `.deduped`, `.join`, `.subscribe`) only exist on `Watchable`/`Cell` — they cannot be called directly on a `MapPipeline`.

Approach: `.materialize()` is added at boundaries where a `Cell` is required. `Pipeline` is added to the `myko::prelude` so `.materialize()` is available wherever `prelude::*` is used (which includes the macro-generated entity code).

Files touched: `client/entity_sync.rs`, `client/mod.rs`, `core/query/context.rs`, `core/report/cell.rs`, `core/report/registration.rs`, `core/view/context.rs`, `entities/framework_reports.rs`, `core/report/handler.rs`, `prelude.rs`, `macros/src/item.rs`.

Bonus: clippy `ptr_arg` fix for `macros::gate_ts_attrs` that surfaced under `--all-features`.

### 2. ReportHandler::compute widening (part of `a8eee4e3`)

`fn compute(...) -> Cell<Arc<Self::Output>, CellImmutable>` → `fn compute(...) -> impl Pipeline<Arc<Self::Output>>`.

Reports can now compose `ctx.report(InnerReport).map(...)` chains without materializing the intermediate cells. The framework materializes once in `CellServerCtx::report` (the cache boundary).

Updated every `impl ReportHandler` site (~18 across `entities/client.rs`, `entities/server.rs`, `entities/framework_reports.rs`, `bench_entities.rs`, `core/report/export_tree.rs`, `search/entity_search.rs`).

### 3. QueryHandler/ViewHandler widening (`279a597e`)

`fn build_view(ctx) -> Option<FilteredCellMap>` → `fn build_view(ctx) -> Option<impl MapQuery<Arc<str>, Arc<dyn AnyItem>>>`.

`fn build_cell(ctx) -> TypedViewCellMap<Self::Item>` → `fn build_cell(ctx) -> impl MapQuery<Arc<str>, Arc<Self::Item>>`.

Implementors can now chain `.inner_join(...).project_map(...).select_cell(...)` inside `build_view`/`build_cell` bodies; framework materializes once at the cache boundary in `cell_factory`. `MapQuery` added to `myko::prelude`.

Wrapper impls in `core/query/request.rs` and `core/view/request.rs` materialize at the wrapper boundary so the `impl Trait` return type infers cleanly.

### 4. Test adaptations (`49d3506b`)

`SelectExt::select` consumes `self` by value and returns a `SelectPlan` (a `MapQuery`). Test setup in `core/server/client_session.rs` deref-clones `Arc<EntityStore>` into a `CellMap` value, then materializes the SelectPlan before passing to `subscribe_query`. Stale `FilteredCellMap` import in `query/request.rs` removed.

### 5. SharedPipeline cache experiment (`c98c419d` + reverted by `4dcb9f7e`)

Tried converting `CellServerCtx::report`'s cache from `WeakCell<T, CellImmutable>` to `SharedPipeline<T>`. Public API of `report()` would have changed from `Cell<...>` to `SharedPipeline<...>`.

Rationale was attractive: SharedPipeline keeps the recipe alive, defers upstream subscription until the first consumer materializes, and (unlike SharedMapQuery) **is re-installable** after drain — `Pipeline::install(&self, ...)` versus `MapQuery::install(self, ...)` is the underlying asymmetry.

Hyphae guidance was: **WeakCell is the right handle for caching**. Reverted.

---

## Architectural decisions

### Why widen trait return types instead of materializing in the trait?

The widening enables **fewer intermediate materializations inside trait method bodies**. A `build_view` doing `a.inner_join(b).project_map(...)` goes from 3 cell allocations (one per operator) to 1 (single materialize at the cache boundary). Without the widening, every operator inside the body would have allocated its own intermediate `CellMap`.

Tradeoff: implementors return `impl MapQuery<...>` — slightly more abstract type. Concrete `CellMap` and `Cell` values still satisfy the bound via blanket impls on `ReactiveMap` and `Watchable`, so trivial impls returning a pre-built map continue to work unchanged.

### Why is the cache materialization not "wasted laziness"?

Three layers of laziness, only the middle one is consumed by the cache:

```
build_view body:    [op] → [op] → [op]              — lazy MapQuery chain (preserved)
                                       \
cache boundary:                         materialize → CellMap (cached, shared)
                                                              /
consumer:                                          [op] → [op] → materialize → leaf
                                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                                   lazy again — CellMap is itself a MapQuery
                                                   via the ReactiveMap blanket
```

The materialization at the cache boundary is the price of consolidation — without it, every consumer reruns the entire upstream plan from scratch. Consumer-side laziness is preserved because `CellMap: MapQuery` (blanket impl).

### What does the cache cost in cross-query fusion?

Real loss, documented honestly. When Q1's `build_view` calls `ctx.query_map(Q2)`:

```
Q2's plan: a.inner_join(b)  ──materialize──→  CellMap_Q2
                                                  ↓
Q1's plan: CellMap_Q2.inner_join(c)  ──materialize──→  CellMap_Q1
```

Two CellMaps, two diff propagation closures, two subscriber lists. The operators **cannot fuse across the cache boundary**. The fused alternative would be one big plan, one materialize, one subscription per root source — but Q2's work would not be shared with other queries that use it.

This is the **fusion vs. sharing tradeoff**:
- **Cache (current):** Q2's work is shared across N consumers. Q1 specifically pays an extra hop.
- **Fuse (no inner cache):** tightest pipeline. But every other query that uses Q2 reruns it from scratch.

These are at odds. Pure overhead when Q2 has only one consumer; pure win when Q2 has many.

### Why was SharedPipeline rejected for the report cache?

Per hyphae guidance, **WeakCell is the right caching handle**. SharedPipeline was an attractive-looking alternative because it defers upstream subscription and is re-installable, but the canonical answer is the WeakCell pattern.

### Why is SharedMapQuery not a fit for caching at all?

`MapQuery::install(self, ...)` consumes the plan. SharedMapQuery wraps the upstream plan in a `Box<dyn FnOnce>` that's `slot.take()`d on first install. After all subscribers drop, upstream guards are released; the upstream slot is **None forever**. Future installs see a stale snapshot, no live updates.

Compare to `SharedPipeline`: holds the original `P: Pipeline<T>` by value via `UpstreamWrap(P)`, calls `self.0.install(sink)` (note `&self`) on each fresh install wave. Re-installable.

The asymmetry is in hyphae's underlying trait definitions:
- `PipelineInstall::install(&self, ...) -> SubscriptionGuard`
- `MapQueryInstall::install(self, ...) -> Vec<SubscriptionGuard>`

By design — MapQuery deliberately consumes self so chained plans compose without cloning, and explicitly is **not Clone** to prevent accidentally duplicating join/projection work.

---

## Open questions / future work

### Benchmark fuse-vs-share on real workloads

The fusion vs. sharing tradeoff is real. The current cache always materializes between dependent queries (Q1 calls `ctx.query_map(Q2)` → two separate diff pipelines). For repeat consumers of Q2, the cache wins. For one-shot consumers, fusion would be tighter.

Would be valuable to measure on representative workloads before deciding whether to:

1. **Add a separate uncached API path:** `ctx.query_plan(Q)` → `impl MapQuery<...>` (uncached, fuses with caller). `ctx.query_map(Q)` stays cached. Implementors choose. Most explicit; matches hyphae's "you choose where the share-points are" philosophy.
2. **Always uncached for inner queries:** drop the `query_cache`, only cache the WS-layer top-level subscriptions. Lose all inner-query sharing — probably the wrong default for a CQRS framework where queries are heavily reused.
3. **Smart cache:** materialize only when a query has 2+ consumers. Hard to detect dynamically; probably not worth the complexity.

Option 1 is the cleanest shape if benchmarks justify the API expansion. **Don't speculatively implement** — wait for evidence.

### SharedMapQuery's one-shot upstream

If hyphae adds a re-installable upstream variant of `SharedMapQuery` (one that holds the plan by value and re-installs on each fresh wave, mirroring `SharedPipeline`'s design), the query/view caches could revisit using share handles. Not currently a priority — WeakCell works.

### `register_ts_export!` MykoProtocol entry

The CBOR migration (separate branch `feat/ditch-msgpack`, PR #7) added `crate::client::MykoProtocol` to the `register_ts_export!` macro so the typegen binary refreshes `MykoProtocol.ts`. Not relevant to this branch, but mentioning since it's recent context that landed nearby.

### TS generated files staleness

Whenever Rust types change, `cargo flux run gen` (or the user's hot-reload codegen) needs to refresh `libs/myko/ts/src/generated/`. The `flux.toml` `gen` task on `feat/ditch-msgpack` was fixed to pass `--features codegen`; that fix is in PR #7 against `dev`. This branch doesn't have that fix — if you need to run typegen here before that PR merges, use:

```bash
cargo run --target-dir target/claude --features codegen -p myko --bin typegen -- libs/myko/ts/src/generated
```

---

## Files touched on this branch

```
.cargo/config.toml                                                 — local hyphae path override
libs/myko/core/Cargo.toml
libs/myko/core/src/bench_entities.rs
libs/myko/core/src/client/entity_sync.rs
libs/myko/core/src/client/mod.rs
libs/myko/core/src/core/query/context.rs
libs/myko/core/src/core/query/registration.rs
libs/myko/core/src/core/query/request.rs
libs/myko/core/src/core/query/traits.rs
libs/myko/core/src/core/report/cell.rs
libs/myko/core/src/core/report/export_tree.rs
libs/myko/core/src/core/report/handler.rs
libs/myko/core/src/core/report/registration.rs
libs/myko/core/src/core/view/context.rs
libs/myko/core/src/core/view/registration.rs
libs/myko/core/src/core/view/request.rs
libs/myko/core/src/core/view/traits.rs
libs/myko/core/src/entities/client.rs
libs/myko/core/src/entities/framework_reports.rs
libs/myko/core/src/entities/server.rs
libs/myko/core/src/prelude.rs
libs/myko/core/src/search/entity_search.rs
libs/myko/core/src/server/client_session.rs
libs/myko/core/src/server/context.rs
libs/myko/macros/src/item.rs
libs/myko/macros/src/lib.rs
libs/myko/server/src/peer_registry.rs
libs/myko/server/src/peer_registry/peer_connection_handle.rs
libs/myko/server/src/ws_handler.rs
```

---

## Recommended next actions

1. Decide whether to push as-is (preserves the SharedPipeline experiment + revert in history as an audit trail of the design conversation) or squash to drop the experiment pair (cleaner history; experiment was unpushed so no rebase pain).
2. If pushing as-is: `git push origin feat/hyphae-pipelines`. PR target: `dev` per CLAUDE.md.
3. If squashing: `git rebase -i HEAD~5` and drop both `c98c419d` and `4dcb9f7e`. Net effect identical to current; just no experiment trace.
4. After merge, consider scheduling the fuse-vs-share benchmark (see Open Questions). No code action until that evidence exists.
