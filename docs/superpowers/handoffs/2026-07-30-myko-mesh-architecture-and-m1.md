# Handoff: Myko Mesh architecture docs, phase plans, and the M1 investigation

**Date:** 2026-07-30
**Branch:** `feat/iroh-integration` @ `1c22edbc`
**Status:** Documentation + one measurement harness. **All work uncommitted.** No production code changed.
**Audience:** an incoming agent with no prior context on this work.

---

## TL;DR

Two deliverables, both complete:

1. **A normative architecture reference for the myko mesh** — `docs/architecture/mesh/`, 11 documents
   decomposing a 1,744-line design spec into implementable form with **291 numbered invariants**, plus
   a 4-phase-plan set (roadmap + phases 1–3) in `docs/superpowers/plans/`.
2. **M1 resolved** — a memory harness (`libs/myko/core/examples/amplification.rs`) that quantified
   myko's resident-memory amplification. **M1 is real:** the memory is genuinely live and concentrated
   in **derived/query cells**, ~254 KB/item at rack scale. It is *not* allocator retention and *not*
   primarily the mechanism first suspected.

**Nothing here is committed.** `git status` shows `docs/architecture/`, four plan files,
`libs/myko/core/examples/`, and a 4-line addition to `libs/myko/core/Cargo.toml`.

**The single most important thing to read before acting:**
[`docs/architecture/mesh/M1-findings.md` §8](../../architecture/mesh/M1-findings.md) — it records three
conclusions that were stated confidently and later refuted. Re-deriving them wastes days.

---

## Branch state

```
1c22edbc docs(specs): revise mesh architecture after adversarial review   ← HEAD
f9bc3b9f docs: replace federation and wire specs with mesh node architecture
34f56a48 feat(server): restore peer count, entity snapshot, and view cache accessors
```

- **64 commits ahead** of `origin/main`, **6 behind**. The 6 behind include **myko v5.0.2**, which
  shipped an opt-in glibc `malloc_trim` probe (`MYKO_MALLOC_TRIM_INTERVAL_SECS`, PR #44). **This branch
  does not have it.**
- Working tree — all untracked/modified, nothing staged:

```
 M libs/myko/core/Cargo.toml              # +4 lines: [[example]] amplification
?? docs/architecture/                     # 12 files, ~3,600 lines
?? docs/superpowers/plans/2026-07-26-*.md # 4 files, ~2,450 lines
?? libs/myko/core/examples/               # amplification.rs, 403 lines
```

**Verification status:**

| Check | Result |
|---|---|
| `cargo check --target-dir target/claude -p myko --features bench --example amplification` | clean |
| `cargo clippy ... --example amplification -- -D warnings` | clean |
| `rustfmt --edition 2024 --check` on the example | clean |
| Harness run (release) | reproducible to 3 s.f. across 3 runs |
| Full workspace test sweep | **not run** — no production code was changed, so no regression surface |

> **Pre-existing, unrelated:** `cargo fmt --check` reports diffs in
> `libs/myko/core/benches/advanced_query_filters.rs`. That is not from this work. It is caused by
> nightly-only rustfmt settings (`imports_granularity`, `group_imports`) that stable cannot apply.

---

## Part 1 — The architecture doc set

### What it is, and why it is separate from the spec

`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md` (1,744 lines) is the **design
spec**: it argues the mesh into existence, weighs alternatives, and records *why* each decision was
made. It went through an adversarial review on 2026-07-26 and remains **authoritative on intent**.

`docs/architecture/mesh/` is the complement: **what an implementation must do**, without argument.
Byte layouts, algorithms, type definitions, state machines. If a normative statement there contradicts
the spec, **the spec wins and the doc is a bug**.

### Files

| File | Lines | Covers |
|---|---:|---|
| `README.md` | 81 | Index, conventions, v1 scope, open-item status |
| `01-node-model.md` | 393 | Role bits, filters, completeness, gateway, manifest, lifecycle |
| `02-type-identity-and-schema.md` | 265 | Crate-qualified names, version skew, `FieldSchema`, field ids |
| `03-record-format.md` | 353 | **Byte-level** wire record, HLC, canonical CBOR, content hash |
| `04-merge-semantics.md` | 435 | Merge algorithm, CRDT state shapes, apply modes, OCC |
| `05-scopes-and-capabilities.md` | 270 | Multitenancy, signed grants, management plane |
| `06-transport-and-planes.md` | 315 | Transport contract, planes, handshake, gateway protocol |
| `07-state-and-log.md` | 339 | Store/log split, indexing, compaction, checkpoints, retention |
| `08-replication-and-subscriptions.md` | 412 | Anti-entropy, gossip, query-driven replication, time travel |
| `09-commands-and-routing.md` | 337 | Dispatch, consistency modes, optimistic execution, idempotency |
| `10-crate-layout-and-migration.md` | 227 | Target crate split, `NodeScoped` refactor, macro changes |
| `M1-findings.md` | 230 | **The memory investigation — read §8** |

### The invariant convention — this matters for editing

Every document defines invariants with a document-scoped prefix: `NM-` (node model), `TI-` (type
identity), `RF-` (record format), `MG-` (merge), `SC-` (scopes), `TP-` (transport), `SL-` (state/log),
`RP-` (replication), `CR-` (commands), `CL-` (crate layout).

> **Invariant IDs are stable and never reused.** A withdrawn invariant is struck through **in place**,
> keeping its number. Renumbering would silently invalidate every cross-reference and every future
> test that cites one.

Each document ends with an index table listing all of its invariants. **Body and index must match.**
Verify with:

```bash
for f in docs/architecture/mesh/0*.md docs/architecture/mesh/10*.md; do
  body=$(grep -ohP '^> \*\*\K[A-Z]{2}-\d+' "$f" | sort -u | wc -l)
  index=$(grep -cP '^\| [A-Z]{2}-\d+ \|' "$f")
  echo "$(basename "$f"): body=$body index=$index"
done
# and check for duplicates across the whole set:
grep -ohP '^> \*\*\K[A-Z]{2}-\d+' docs/architecture/mesh/*.md | sort | uniq -d
```

Last verified 2026-07-26: all 10 documents matched; zero duplicates.

### Link checking

All internal links were verified to resolve. Re-check after edits:

```bash
for f in docs/architecture/mesh/*.md docs/superpowers/plans/2026-07-26-*.md; do
  d=$(dirname "$f")
  grep -oP '\]\(\K[^)#]+(?=[#)])' "$f" | while read -r l; do
    case "$l" in http*) ;; *) [ -e "$d/$l" ] || echo "BROKEN: $f -> $l";; esac
  done
done
```

---

## Part 2 — The implementation plans

`docs/superpowers/plans/`:

| File | Lines | Status |
|---|---:|---|
| `2026-07-26-myko-mesh-roadmap.md` | 448 | 14 phases: deliverables, exit criteria, dependencies, risk |
| `2026-07-26-mesh-phase-1-item-field-schemas.md` | 826 | Task-by-task, ready to execute |
| `2026-07-26-mesh-phase-2-benchmarks-and-sim-harness.md` | 566 | Task-by-task; **M1 portion already done** |
| `2026-07-26-mesh-phase-3-wire-break.md` | 610 | Task-by-task, ready to execute |

**Phases 4–14 deliberately have no task-level plan.** A plan for phase 12 written now would specify
against a store, log, and routing table that have no shape yet. The roadmap's exit criteria are what
make each plan writable when its turn comes. **Do not pre-write them.**

Plan style follows the existing house convention in
`docs/superpowers/plans/2026-04-24-cbor-wire-migration-rust.md`: file-structure table, type-consistency
list, then `### Task N` blocks of `- [ ] **Step N:**` items with exact code, verification commands, and
a commit step.

### Phase status

| Phase | State |
|---|---|
| 0 — prereqs | Mostly landed. **One outstanding:** `Origin::Remote` apply mode was introduced in PR #25 and later removed; only `Local \| Cascade` remain (`server/context.rs:59`). Wire-ingested events currently apply as `Local` — cascading and producing. Must be restored in phase 7. |
| 1 — field schemas | Planned, not started |
| 2 — bench + sim + M1 | **M1 done.** Benchmark and `myko-sim` not started. |
| 3 — wire break | Planned, not started |
| 4–14 | Roadmap-level only |

### One decision worth knowing

`feat/iroh-dataplane` (a separate branch) carries partial convergence work using wall-clock timestamps
and whole-entity LWW. **It does not match the design and is to be rewritten, not landed.** Its two
survivable ideas already reappear in the docs: the `(ts, source_id)` total order as `03 RF-3`'s
`(hlc, origin)`, and tombstones in the stamp index as `04 MG-25`.

---

## Part 3 — M1, and the three corrections

**Read `docs/architecture/mesh/M1-findings.md` in full.** This is the summary.

### The question

A `rack` deployment holding ~82k records across 156 types — 40–170 MB of data at rest — was observed
at process RSS in the tens of GB. That is 100–1000× amplification. Until understood, a myko node's
memory could not be predicted from what it holds.

### The harness

`libs/myko/core/examples/amplification.rs` (403 lines). A counting global allocator wrapping `System`,
tracking live bytes, over the real store shape (`CellMap<Arc<str>, Arc<dyn AnyItem>>`), sweeping entity
count × distinct live predicate count, plus a teardown test and a `malloc_trim` probe.

**It opens no socket.** Pure in-process measurement — no server, no listener, no port.

```bash
cargo run --release --target-dir target/claude -p myko --features bench --example amplification
```

Requires the `bench` feature (for `bench_entities::BenchItem`) and the `[[example]]` block already
added to `libs/myko/core/Cargo.toml`.

### What it measured

| Result | Value |
|---|---|
| Per-predicate cost | **~208 B per entity per live predicate**, exactly linear in `N × P`, plus ~100 KB fixed per predicate |
| Teardown | **0.0% retained** — dropping 5,015 MB of predicates left 0.05 MB |
| RSS vs live heap (glibc) | **889×** — 5,167 MB resident against 5.81 MB live, collapsing to 13 MB on `malloc_trim(0)` |

Reproduced identically on `myko` main @ `0ed566f9` by another session, so the numbers are not an
artifact of this branch.

### The answer

**M1 is real. The memory is genuinely live.** At rack scale: **27,383 items across 144 stores against
6.96 GB live ≈ 254 KB/item**, concentrated in **derived and query cells** — 13,195 `query_cells`,
topped by `GetCuePlaybacksByQuery` and `GetBindingNodeValuesByQuery`.

Corroborated by rship `lv-fc26` (~4M cells / ~29 GB of binding-node join/map runtimes at rack scale)
and hyphae **PR #20** (a `height_dependents` dedup leak).

### ⚠ Three corrections — do not re-derive these

**1. The 889× RSS result does not explain rack.**
`rship_server` sets `tikv-jemallocator` as its `#[global_allocator]`
(`rship/apps/server/src/main.rs:38`) — adopted *because of* the glibc behaviour. Under jemalloc at
rack scale, **RSS/live is 1.33×**: ~75% of RSS is genuinely live. `malloc_trim` measures nothing there.
**Do not re-propose `malloc_trim` probing for rship or rack.** The discriminator there is jemalloc
`allocated` vs `resident` via `mallctl`, already logged every 10 s in rship's dev profile.

**2. The 208 B/(N·P) figure does not cover myko's generated queries.**
The harness measures `select`, which installs hyphae's `MapState` with its per-stage `source_rows`
mirror. Generated queries mostly take other paths (`query/registration.rs`):

| Generated query | Path | Source mirror? |
|---|---|---|
| `Get*ById` | `store.get(id)` — per-key observation cell | **no** |
| `Get*sByIds` | `build_ids_source_map` — small `CellMap` of just those ids | narrow |
| `Get*sByQuery` pinning a `belongs_to` | `build_belongs_to_source_map` → pre-narrowed index bucket | narrow |
| `Get*sByQuery` pinning nothing | scan mode (`LiveDiffScope::FullStore`) — matches-only result | **no** |

`MapState` *does* appear in **views** (`ViewFactory` / `build_view`), **reports** using `MapExt`
operators, `capability.rs:206`'s `query_diff -> .diffs().map(..).materialize()`, and hand-written
operator chains. **Quote 208 B/(N·P) for those, never for generated queries.**

**3. Cache sweep lag and hyphae-layer predicate leak are both ruled out.**
Teardown is driven by `SubscriptionGuard` drop, not by the caller-driven cache sweep. The caches hold
`Weak` refs, so a stale entry is ~100 B, not ~1 MB. Teardown returns 100%.

### The generalisable lesson

Not "RSS lies." It is: **an instrument is only valid for the configuration it was calibrated against.**
`malloc_trim` is a glibc instrument and rship runs jemalloc. A `select`-based harness measures the
`MapState` path and generated queries take four other paths. **Check the configuration before
transferring the number.**

### Where M1's remaining work lives — not in this repo

| Item | Repo | State |
|---|---|---|
| `lv-fc26` — ~4M cells / ~29 GB binding-node join/map runtimes | **rship** | Open; the dominant term |
| hyphae **PR #20** — `height_dependents` dedup leak | **hyphae** | Open |
| `lv-4a87` — `source_rows` scales with source size, not matches | **myko** | Open, P1, quantified |
| `malloc_trim` probe (glibc only) | **myko** | **Shipped in v5.0.2**, PR #44 — not on this branch |

Nothing in the mesh roadmap blocks on any of them.

---

## Verified vs assumed

Treat this table as the trust boundary. Everything in the docs was written against the tree at
`1c22edbc`.

**Verified by reading code or running it:**

- Harness numbers (three runs, plus independent reproduction on main).
- `ItemRegistration` has no field schema — `core/item/traits.rs:79`.
- `CommandRegistration.args: &'static [OperationArgField]` exists — `core/command/registration.rs:12`.
- `crate_name: module_path!()` — `macros/src/item.rs:777`, and identically in `command.rs:139`,
  `query.rs:74`, `view.rs:138`, `report.rs:68`.
- Codegen's substring filter — `codegen/mod.rs:158` and 7 sibling call sites (`lv-ea59`).
- `Origin { Local, Cascade }` only — `server/context.rs:59`.
- Apply chain `apply_event_batch → emit_grouped → apply_effects` — `server/context.rs`.
- Store is arrival-order overwrite via `store.insert_many` — no timestamp comparison anywhere.
- `ClientSession` holds `all_items` + `visible_items` per subscription — `client_session.rs:147`.
- hyphae `MapState` has `source_rows` / `source_output_keys` / `output_cache` —
  `traits/collections/internal/map_runtime.rs:19`.
- `source_output_keys.insert` is guarded by `if !desired_keys.is_empty()` — non-matching rows get no
  `HashSet`.
- `select`'s `install` returns `Vec<SubscriptionGuard>` — teardown is guard-driven.
- `execute_command_job` at `ws_handler.rs:1383`, errors on handler miss at `:1447`.
- `ServerScoped::__server_ctx` and the `server` module are wasm-gated — `capability.rs:87`,
  `lib.rs:91`.
- `CommandHandler::execute` is a hardcoded wasm error — `command/handler.rs:198`.
- Only 3 files in `core` touch `thread::spawn`/`tokio`: `query/registration.rs`, `client/mod.rs`,
  `server/context.rs`.

**Assumed, stated as such in the docs, and NOT verified:**

- **The `NodeScoped` refactor has never been compiled.** `10 CL-7` says so explicitly. The claim is
  "blockers appear few and localized," not "it builds." **Phase 8's first task is an un-gate spike to
  produce a real error list, and the phase scope comes from that list, not from the document.**
- Byte-size estimates in `03`/`04` (~38 B/entry etc.) were **layout arithmetic and measured 5.5× low**.
  Treat any un-measured size figure in the docs the same way.
- iroh capability claims (`06 §9`) are from published docs retrieved 2026-07-25, not from API
  exercise. M3 in the spec tracks this; it is moot under the v1 scope.
- The FNV-1a `field_id` collision-check design assumes a real collision pair can be found for the
  compile-fail test. Phase 1 Task 9 Step 2 says: **do not fabricate one** — brute-force it, or change
  the test's shape and say so.

---

## Cross-repo map

All on host `malcolm`, all checkouts under `/home/trevor/Code/`:

| Path | Branch | Relevance |
|---|---|---|
| `myko-iroh-integration` | `feat/iroh-integration` | **this work** |
| `myko` | `main` | canonical myko; has v5.0.2 |
| `rship` | `feat/bump-myko-5.0.1` | perf-validation consumer; `lv-fc26` lives here |
| `rship-myko-6` | `feat/myko-6-migration` | migration work |
| `../hyphae/hyphae` | — | reactive framework; PR #20 |

**Convention: one agent session per repo.** Cross-repo fixes are handed to that repo's session rather
than applied directly to another checkout. This work followed that — the main-branch reproduction was
requested from the `myko` session, not done here.

---

## Suggested next actions, in order

1. **Decide what to commit.** Everything is uncommitted. Natural split:
   - `docs(architecture): add normative mesh architecture reference` — `docs/architecture/mesh/`
     minus `M1-findings.md`
   - `docs(plans): add mesh roadmap and phase 1-3 plans` — the four plan files
   - `bench(core): add M1 amplification harness` — `examples/amplification.rs` + `Cargo.toml`
   - `docs(mesh): record M1 findings and corrections` — `M1-findings.md`

   Repo rules: Conventional Commits; **do not add a co-author trailer; do not mention the tool that
   generated the commit.**

2. **Rebase on `origin/main`** — 6 commits behind, including v5.0.2. Low risk: this branch's changes
   are docs plus one new example file.

3. **Execute phase 1** (`2026-07-26-mesh-phase-1-item-field-schemas.md`). Purely additive, low risk,
   closes `lv-ea59` as a side effect. **One caution carried in the plan:** replacing codegen's
   substring filter with namespace equality is a real behaviour change — a crate named `myko`
   currently over-matches `myko_server` and `myko_leptos`. Diff generated TypeScript on a real
   consumer before releasing.

4. **Then phase 2's remaining two deliverables** — the encoding benchmark and `myko-sim`. Both gate
   phase 3, which is the irreversible one.

---

## Repo conventions the incoming agent needs

- **Always** pass `--target-dir target/claude` to every cargo command. Other tools hold locks on the
  default target dir.
- Prefer `cargo check` over `cargo build`.
- **Check `.bacon-locations` for current clippy errors before running clippy yourself** — bacon keeps
  it updated. Fix in order; later errors are often resolved by fixing the first.
- **Do not run the app or type generation.** The user runs both in hot-reload mode. If generated
  TypeScript needs checking, ask them to run `cargo flux run gen` and report the diff.
- **Task tracking is `levi`**, a git-aware issue tracker whose state lives in the repo
  (`refs/levi/events`). Every read command takes `--json`. `levi ls --json`, `levi show <id> --json`,
  `levi add "title" -p p1 -l label`, `levi comment <id> "text"`, `levi close <id>` (commit first — the
  close anchors at HEAD).
- **Logic belongs in Rust.** Cross-language duplication must be generated, never hand-maintained.
- Lines under 120 chars. Comments explain *why*, not *what*, and carry initials: `// NOTE(ts): ...`.

### Open levi tasks touching this work

```
lv-4a87 [P1] query/view cache memory scales with materializations x source size, not matches
lv-ea59 [P2] codegen crate filter uses substring match on module_path, over-matches sibling crates
lv-3816 [P2] Viewing capability is native-only — hand-written build_cell bodies can't compile to wasm32
lv-46ae [P2] ExportEntityTree wasm-gated at module level
lv-a1a6 [P3] QueryHandler::build_view has no wasm arm
```

The last three are all `NodeScoped`/phase-8 surface and should be read before that phase starts.
