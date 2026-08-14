# M1 — Resident-Memory Amplification: Findings

**Status:** Resolved in premise; the mechanism is real and located elsewhere ·
**Measured:** 2026-07-26 (local), 2026-07-27 (main + rship production-scale) ·
**Harness:** `libs/myko/core/examples/amplification.rs`

> ## ⚠ Read this before anything below
>
> **This document's original headline was refuted on 2026-07-27, two days after it was written.** The
> refutation is in §8. The body of §1–§7 is preserved as written, because the reasoning is still
> correct *within its scope* and the scope limits are the lesson — but two claims must not be carried
> forward:
>
> 1. **"RSS is not live heap (889×)" does not explain rack.** `rship_server` sets
>    **tikv-jemallocator** as its `#[global_allocator]` — adopted precisely *because of* the glibc
>    behaviour measured here. Under jemalloc at rack scale, **RSS/live is 1.33×**: ~75% of RSS is
>    genuinely live. `malloc_trim` measures nothing there.
> 2. **The ~208 B/(N·P) figure does not apply to myko's generated queries.** It measures hyphae's
>    `MapState` path, which generated `Get*sByQuery` queries mostly **do not use** (§8.2).
>
> **M1 is real.** The memory is genuinely live, it is ~254 KB/item at rack scale, and it lives in
> derived/query cells — not in allocator retention and not in `source_rows` on the generated-query
> path.

---

## 1. What was run

A counting global allocator wrapping `System`, tracking live bytes, over the real store shape
(`CellMap<Arc<str>, Arc<dyn AnyItem>>` — myko's `EntityStore`) with distinct materialized `select`
predicates installed against it. No server, no socket, no network. Single-threaded on the measured
path.

Reproducible: three runs agreed to three significant figures.

```
cargo run --release --target-dir target/claude -p myko --features bench --example amplification
```

## 2. Result A — the per-predicate coefficient

| N (entities) | P (predicates) | store MB | predicates MB | B per entity per predicate |
|---:|---:|---:|---:|---:|
| 1,000 | 100 | 0.30 | 30.21 | 316.7 |
| 1,000 | 1,000 | 0.30 | 302.04 | 316.7 |
| 10,000 | 100 | 2.40 | 218.46 | 229.1 |
| 10,000 | 1,000 | 2.40 | 2,184.42 | 229.1 |
| 25,000 | 100 | 5.65 | 495.96 | 208.0 |
| 25,000 | 1,000 | 5.65 | 4,960.79 | 208.1 |

**Cost is exactly linear in `N × P`.** The `P = 100` and `P = 1000` columns agree to four significant
figures at every N, so there is no super-linearity and no interaction term.

The per-entity figure falls with N (317 → 229 → 208) because there is a **fixed per-predicate
overhead** amortized over the entity count. Fitting `cost_per_predicate = a + b·N` across the three
points gives roughly **b ≈ 200 B per entity** and **a ≈ 100 KB per predicate**, though the fit is
loose (~7% residual) and the fixed term should not be quoted precisely.

> **The predicted coefficient was 38 B/entity/predicate. The measured one is ~208 B — 5.5× higher.**
> The structural model (`source_rows` + `output_cache` + `source_output_keys`) was right about the
> *shape* and wrong about the *magnitude*. The gap is mostly hashbrown's capacity doubling — tables
> sit at up to 2× their occupied size — plus per-match costs larger than the layout arithmetic
> suggested.

**Consequence for the rack numbers.** At 208 B/entity/predicate over `Action` @ 25,457, one predicate
costs ~5.3 MB. Ten GB of *live heap* would need **~1,900 live predicates**; 30 GB needs ~5,700. With
8 clients that is 240–710 live predicates per client — high, but **not implausible for a rich
control-system UI**, where the original 38 B estimate demanded an implausible ~10,000.

## 3. Result B — teardown is clean

| | live heap | RSS |
|---|---:|---:|
| baseline (N=25,457, P=0) | 5.76 MB | 5,116 MB |
| loaded (P=1,000) | 5,015.21 MB | 5,173 MB |
| dropped (P=0) | 5.81 MB | 5,167 MB |

**0.0% of installed memory was retained** after dropping every predicate. Guard-driven teardown in
hyphae's map runtime returns everything.

> **This kills the predicate-leak hypothesis at the hyphae layer** — which had been promoted to
> leading candidate on the strength of the arithmetic, before it was measured.
>
> **Scope limit, stated plainly:** the harness drops its predicates *explicitly*. It therefore proves
> hyphae releases on drop; it proves **nothing** about whether myko-layer structures hold predicates
> alive past their subscription. A myko-level retention bug remains possible and this harness cannot
> see it.

## 4. Result C — RSS is not live heap

The decisive measurement. After teardown, with **5.81 MB** of live heap:

```
dropped  (N=25457, P=0)      live      5.81 MB   rss   5167.55 MB
trimmed  (malloc_trim)       live      5.81 MB   rss     13.33 MB   (pages returned)
```

**`malloc_trim(0)` collapsed RSS from 5,167 MB to 13 MB.** The process was referencing 5.81 MB and
resident at 5,167 MB — an **889× ratio**, entirely glibc arena retention. Nothing was leaked; the
allocator had simply not returned the pages.

This is visible throughout the sweep, not only at teardown: RSS climbs to 2,254 MB during the
`N=10,000, P=1,000` row and then **stays at ~2,201 MB for every subsequent row**, including
`N=25,000, P=0` where live heap is 5.65 MB.

> **The spec ranked this hypothesis 4th and characterised it as "real, but not 1000×." That is
> wrong — it is ~889× here, under precisely myko's allocation pattern:** many medium-sized,
> similarly-shaped hashbrown tables allocated and freed in waves. That pattern fragments a glibc
> arena about as badly as anything can.

> **Scope, added 2026-07-30:** this result is real **for a default-glibc process**. It does not
> transfer to `rship_server`, which runs jemalloc — see §8.1.

## 5. Hypothesis table — superseded

*Kept for the record. The live version is §8.4.* As of 2026-07-26 this ranked allocator retention (4)
leading, `source_rows` (1) confirmed-but-insufficient, predicate leak (5) killed at the hyphae layer,
and cache sweep lag (2) ruled out. §8 reverses the ranking of 4 and relocates 1.

## 6. Corroboration on main

Re-run by another session on `myko` main @ `0ed566f9` (2026-07-27): **all three local results
reproduced identically** — ~208 B/(N·P), 0.0% retained after teardown, 889× RSS/live under glibc.

So the numbers are not an artifact of `feat/iroh-integration`. That was the question the cross-check
was for, and it is settled.

## 7. Design consequences

- **[01 NM-8](01-node-model.md) stands, and is now better supported.** A node's memory cannot be
  predicted from its filter. §8.3 makes this stronger, not weaker: at rack scale the cost is
  ~254 KB/item and lives in derived cells, so filter cardinality is not even the right independent
  variable.
- **A disk-backed store is a non-fix.** The memory is in reactive structure, not in the store: at
  `N=25,000, P=1,000` the store is 5.65 MB and the predicates are 4,961 MB — an 878× ratio *within
  the harness itself*. Confirmed independently at rack scale (§8.3).
- **Any RSS-based memory claim is inadmissible without a live-heap cross-check** — but the
  cross-check is **allocator-specific**, and that qualifier is the part I originally got wrong. Under
  glibc it is `malloc_trim`; under jemalloc it is `allocated` vs `resident` via `mallctl`. Applying
  the glibc instrument to a jemalloc process measures nothing.

---

# 8. Refutation and relocation (2026-07-27)

Two independent findings landed after §1–§7 were written. Together they say **M1 is real, and neither
of this document's original mechanisms explains it.**

## 8.1 The allocator hypothesis does not apply to rack

`rship_server` sets **`tikv-jemallocator` as its `#[global_allocator]`**
(`rship/apps/server/src/main.rs:38`) — adopted *because of* the glibc behaviour measured in §4. The
rack figure was therefore observed under jemalloc, where glibc arena retention cannot occur and
`malloc_trim` is a no-op.

jemalloc stats at loaded steady state, production-scale data on a local bench:

| | |
|---|---|
| RSS | 9,267 MB |
| `je_allocated` (live) | 6,960 MB |
| `je_resident` | 9,108 MB |
| **RSS / live** | **1.33×** — ~75% of RSS is genuinely live |

`background_thread` purging was already on; ticks flat. **The memory is real.**

> This is the correction that matters most. §4's 889× is a true statement about a glibc process and
> a **false lead** for the deployment it was offered to explain. I proposed the `malloc_trim` probe
> for rack; that proposal was wrong.

## 8.2 The 208 B/(N·P) figure does not cover generated queries

The harness measures `select`, which installs hyphae's `MapState` (with its per-stage `source_rows`
mirror). **myko's generated queries mostly do not take that path.** Per the correction on `lv-4a87`,
`query/registration.rs` has its own machinery:

| Generated query | Path | Source mirror? |
|---|---|---|
| `Get*ById` | `store.get(id)`, a per-key observation cell | **no** |
| `Get*sByIds` | `build_ids_source_map` — a small `CellMap` of just those ids | narrow |
| `Get*sByQuery` pinning a `belongs_to` | `build_belongs_to_source_map` → a pre-narrowed index bucket | narrow |
| `Get*sByQuery` pinning nothing | scan mode (`LiveDiffScope::FullStore`) — subscribes to whole-store diffs, maintains a **matches-only** result | **no** |

Where `MapState` *does* appear: **views** (`ViewFactory` / `build_view`), **reports** using `MapExt`
operators, `capability.rs:206`'s `query_diff -> .diffs().map(..).materialize()`, and any hand-written
hyphae operator chain. Each chained stage carries its own `MapState`, so plan depth still multiplies —
but over that stage's input, which for a `belongs_to`-routed query is a narrow bucket.

**So the harness's 208 B/(N·P) is a correct measurement of the view/report/hand-written-chain layer,
and must not be extrapolated to the generated-query layer.**

## 8.3 Where the memory actually is

At rack scale: **27,383 items across 144 stores against 6.96 GB live ≈ 254 KB/item.** The
amplification is in **derived and query cells** — 13,195 `query_cells`, topped by
`GetCuePlaybacksByQuery` and `GetBindingNodeValuesByQuery`.

Corroborated by two companion findings:

- **rship `lv-fc26`** — ~4M cells / ~29 GB of binding-node join/map runtimes at rack scale.
- **hyphae PR #20** — a `height_dependents` dedup leak.

## 8.4 Live hypothesis table

| Hypothesis | Status |
|---|---|
| **Derived/query-cell runtimes dominate** (join/map plan stages, per-cell overhead) | **Confirmed and leading.** ~254 KB/item at rack scale. Attack surface: `lv-fc26`, hyphae PR #20. |
| `height_dependents` dedup leak in hyphae | **Confirmed**, fix in hyphae PR #20. |
| `source_rows` per stage (`lv-4a87`) | **Real and quantified at ~208 B/(N·P)** — but scoped to views/reports/hand-written chains (§8.2), not generated queries. Contributes; does not dominate. |
| Allocator retention | **Refuted for rack** (jemalloc, 1.33×). True and large for *glibc* processes (§4). |
| Predicate leak at the hyphae layer | **Killed** — teardown returns 100%. |
| Cache sweep lag | **Ruled out.** |

## 8.5 What shipped

**myko v5.0.2** (PR #44, merged 2026-07-27) adds an **opt-in glibc `malloc_trim` probe** —
`MYKO_MALLOC_TRIM_INTERVAL_SECS`, in `telemetry.rs`, which **refuses at startup under jemalloc** via a
`dlsym` check for `_rjem_mallctl`. Useful for glibc deployments only. It is a diagnostic, not a fix,
and it is *not* the instrument for rship.

> **This branch (`feat/iroh-integration`) predates v5.0.2** and does not contain the probe.

## 8.6 Do not repeat

- **Do not re-propose `malloc_trim` probing for rship or rack.** The discriminator there is jemalloc
  `allocated` vs `resident`, already logged every 10 s in rship's dev profile.
- **Do not cite 208 B/(N·P) as a generated-query cost.** Cite it for views, reports, and hand-written
  operator chains.
- **Do not size a myko node from this document.** The rack coefficient is ~254 KB/item and its
  independent variable is live derived-cell count, which nothing here predicts.
