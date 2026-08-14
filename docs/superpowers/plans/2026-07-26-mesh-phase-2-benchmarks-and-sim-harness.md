# Mesh Phase 2 — Benchmarks, Simulation Harness, and M1

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. **Task 1 is a gate.** If M1 comes back
> "inherent per-subscription amplification," stop and re-read §"If M1 says inherent" before starting
> Task 5 — the design consequence changes phase 9's scope and must be recorded before phase 3 ships.

**Goal:** Answer M1, measure the proposed wire format against the current one, and land the
deterministic simulation harness — **all three before the wire break, which is the least reversible
step in the plan.**

**Architecture:** Three independent workstreams, sequenced so the cheapest disqualifying answer comes
first. M1 is resolved by a heap profile against a live node, not by another count. The encoding
benchmark is a Criterion bench in `myko-core` behind the existing `bench` feature, comparing
field-addressed encode/decode against today's JSON and CBOR paths. `myko-sim` is a new dev crate:
simulated transport, seeded scheduling, and fault injection, with no dependency on any real
transport.

**Tech Stack:** Rust, Criterion (existing bench harness), `heaptrack`, `myko-sim` (new crate),
`rand_chacha` for seeded determinism.

**Spec:** [`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md`](../specs/2026-07-25-myko-mesh-node-architecture.md) §16, §17
**Architecture:** [`01 — Node model`](../../architecture/mesh/01-node-model.md) (NM-8) ·
[`08 — Replication`](../../architecture/mesh/08-replication-and-subscriptions.md) (RP-23) ·
[`10 — Crate layout`](../../architecture/mesh/10-crate-layout-and-migration.md) (CL-18, CL-20)
**Roadmap:** [phase 2](2026-07-26-myko-mesh-roadmap.md#phase-2--benchmarks-simulation-harness-and-m1)

**Related:** `lv-4a87` (P1, perf) — query/view cache memory scales with materializations × source
size, not matches. **This is M1 hypothesis 1**; Task 1 resolves or refutes it.

**Depends on:** [phase 1](2026-07-26-mesh-phase-1-item-field-schemas.md) — the encoding benchmark
needs `FieldSchema` to build field-addressed records at all.

---

## File Structure

**Files created:**

| File | Responsibility |
|------|----------------|
| `libs/myko/core/examples/amplification.rs` | **The M1 harness.** Counting global allocator; sweeps entity count × live-predicate count; teardown test. An example, not a bench — Criterion measures time, this is a memory question. |
| `libs/myko/core/benches/record_encoding.rs` | Criterion bench: field-addressed vs JSON vs CBOR, encode and decode, at three entity shapes. |
| `libs/myko/core/benches/merge_apply.rs` | Criterion bench: typed-store ingest and per-field merge under the four strategies. |
| `libs/myko/core/src/bench_entities.rs` | *(modified)* Add wide, narrow, and CRDT-bearing entity shapes. |
| `libs/myko-sim/` | New dev crate — the deterministic simulation harness. |
| `libs/myko-sim/src/lib.rs` | `Sim`, `SimNode`, `Fault`, seeded scheduler. |
| `libs/myko-sim/src/transport.rs` | In-memory `MeshTransport` with injectable partition, delay, duplication, reorder. |
| `libs/myko-sim/src/clock.rs` | Virtual clock with per-node skew, driving HLC generation. |
| `libs/myko-sim/tests/partition_heal.rs` | The gate scenario: two nodes, partition, divergent writes, heal, converge. |
| `docs/architecture/mesh/M1-findings.md` | The M1 answer, its evidence, and the design consequence. |

**Files modified:**

| File | Changes |
|------|---------|
| `Cargo.toml` (workspace) | Add `libs/myko-sim` to members; add `rand_chacha`, `criterion` (if not already workspace-level). |
| `libs/myko/core/Cargo.toml` | Register the two new benches under `required-features = ["bench"]`. |
| `docs/architecture/mesh/README.md` | Update the M1 row in "Status of the open items". |
| `docs/architecture/mesh/01-node-model.md` | Resolve or restate NM-8 with the finding. |

---

## Workstream A — M1

**This is the gate.** Everything else in the phase can proceed in parallel, but phase 3 does not start
until Task 1 produces an answer and Task 2 records it.

### Task 1: Reproduce the amplification locally, then heap-profile the deployment

**Both halves are needed, and they answer different questions.** A local harness measures the
*coefficient* — how much RAM one live predicate over N entities actually costs — deterministically and
without a deployment. Only the deployment can tell you its *predicate count*. Neither alone resolves
M1: the harness without the deployment gives a model with an unknown input; the deployment without
the harness gives a number with no model to attribute it to.

The measurement that motivated M1, for reference (`rack` deployment, 2026-07-26,
myko 4.0.0-canary.79, 8 connected clients): **82,164 records** across 156 declared types, 49 of them
non-empty, top 10 types holding 96.8%, largest type `Action` @ 25,457, **~40–170 MB at rest**,
**observed process RSS in the tens of GB**. That is 100–1000× amplification.

#### The allocation model, read off the code

Verified against `hyphae/src/traits/collections/internal/map_runtime.rs` and
`libs/myko/core/src/server/client_session.rs:147`. Per **distinct live predicate** installed over a
type with `N` source entities and `M` matches, `MapState` holds three maps:

| Structure | Scales with | Cost |
|---|---|---|
| `source_rows: FxHashMap<Arc<str>, Arc<dyn AnyItem>>` | **N — every entity of the type, matching or not** | two fat pointers = 32 B payload, ~38 B/entry at load factor |
| `output_cache: FxHashMap<OK, OV>` | M | ~38 B/entry |
| `source_output_keys: FxHashMap<SK, FxHashSet<OK>>` | M | ~128 B/entry — a `HashSet` struct **plus its own heap allocation**, per match |

Plus, per **WS subscription**, `QuerySubscriptionState` holds `all_items` and `visible_items` — two
more `HashMap<Arc<str>, Arc<dyn AnyItem>>` over matches, ~76 B/match.

So:

```
heap ≈ base + entities + Σ_predicates (38·N + 166·M) + Σ_subscriptions (76·M)
```

**`source_rows` is the term that scales with source size rather than matches** — exactly `lv-4a87`.

> **M1 IS RESOLVED — this whole task is history.** Steps 1–2 ran on 2026-07-26 and the deployment-side
> question was answered on 2026-07-27. The harness is at
> `libs/myko/core/examples/amplification.rs`; the findings, **including the refutation of this task's
> own leading hypothesis**, are in [`M1-findings.md`](../../architecture/mesh/M1-findings.md).
>
> Measured: **~208 B per entity per live predicate** (5.5× the layout arithmetic below, exactly linear
> in `N × P`, scoped to the hyphae `MapState` path — *not* generated queries); **teardown returns
> 100%**; and RSS overstating live heap **889× under the default glibc allocator only** — which does
> **not** transfer to rack, where jemalloc gives RSS/live of 1.33× and the memory is genuinely live.
>
> Steps 3–7 below are **superseded**. Kept for the reasoning trail; do not execute them. The live
> hypothesis table is at the end of this task.

#### What the arithmetic already rules out

- **Hypothesis 1 (per-subscription derived maps) does not reach the observed number on its own.** One
  predicate over `Action` @ 25,457 costs `38 × 25,457 ≈ 970 KB`. Reaching 10 GB needs **~10,000 live
  predicates on the largest type**; 30 GB needs ~31,000. With 8 clients that is >1,200 live predicates
  *per client*, which is implausible for a UI. The mechanism is real and is a genuine bug worth fixing
  — it is not, by itself, a 1000× explanation.
- **Hypothesis 2 (cache sweep lag) is nearly ruled out by construction.** Teardown of a map runtime is
  driven by `SubscriptionGuard` drop (`select.rs:41` returns `Vec<SubscriptionGuard>`), **not** by the
  cache sweep. The caches hold `Weak` refs, so a dead cache entry retains a `String` key and a `Weak`
  — on the order of 100 B, not 1 MB. A million stale entries would be ~100 MB. **Sweep lag cannot
  produce tens of GB.**

> **Both leading hypotheses are arithmetically insufficient.** That is a real finding and it changes
> the profile's job: do not go in expecting to confirm hypothesis 1. Go in expecting to find either
> (a) a **predicate leak** — guards retained past subscription end by something process-global, making
> the effective predicate count grow with *uptime* rather than client count; the process-global
> belongs-to source index (`query/registration.rs:1049`) is one such structure and there may be
> others — or (b) something not on the original list at all. Add "predicate leak / unbounded predicate
> growth over uptime" as **hypothesis 5** and treat it as the leading candidate.

- [ ] **Step 1: Build the local amplification harness**

A `myko-core` example (not a Criterion bench — **Criterion measures time, and this is a memory
question**) with a counting global allocator:

```rust
/// Wraps System, tracking live bytes. Gives live-heap numbers that are immune
/// to allocator retention — which is exactly what separates hypothesis 4 (RSS
/// vs live heap) from everything else.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting { /* add on alloc, sub on dealloc */ }
```

Sweep two axes independently over a single entity type:

| Axis | Values | Isolates |
|---|---|---|
| `N` — entities in the store | 1k, 10k, 25k, 100k | the base and the `38·N` per-predicate term |
| `P` — distinct live predicates | 0, 1, 10, 100, 1000 | whether cost is linear in `N·P` |

Report live bytes and RSS at each point, plus `live_bytes / (N·P)` — which should converge on the
~38 B/entity/predicate the model predicts. **If it does not, the model above is wrong and that is the
finding.**

- [ ] **Step 2: Run the teardown test — the hypothesis-5 discriminator**

At `N = 25k, P = 1000`: drop every predicate, call `sweep_dead_cache_entries()`, force a settle, and
re-measure.

- **Live bytes return to the `P = 0` baseline** → teardown works; the cost is genuinely bounded by
  *live* predicates, and the deployment must actually have ~10⁴ of them.
- **Live bytes stay high** → **predicate leak confirmed**, and it is the answer. The effective
  predicate count grows with uptime, not with client count, which is exactly the shape that turns a
  ~1 MB-per-predicate mechanism into tens of GB on a long-lived server.

This single measurement is the highest-value step in the phase. Do it before the heap profile.

- [ ] **Step 3: Run the two free checks against the deployment**

Both are one call each.

Ask the user to run, against a live server:

```
ctx.view_cache_len()          // server/context.rs:403
ctx.view_cache_live_count()   // server/context.rs:408
```

A large gap between them means the caller-driven sweep (`sweep_dead_cache_entries`,
`server/context.rs:421`) is not being called often enough — or at all. **Verify the host app calls it
at all** before profiling anything.

Also confirm the deployed build's hyphae features. The `trace` feature carries the per-cell memory
(~0.63 GB in a prior measurement); `profiling` only adds hot-path CPU. The workspace pins
`hyphae = { version = "^2.0", features = ["scheduler"] }` — confirm the *deployed* build did not add
more. `cargo tree -e features` is unreliable here; use a negative control (build without the feature
and compare) instead.

- [ ] **Step 4: Count the deployment's live predicates — the model's missing input**

This is the number that turns the Step 1 coefficient into a prediction, and **nothing else in this
task substitutes for it.**

`query_cache_len()` / `view_cache_len()` / `report_cache_len()` and their `*_live_count()` pairs
already give it (`server/context.rs:373-410`). Ask the user for all six against the live `rack` server,
plus **uptime**.

Then check the prediction:

```
predicted_live_heap ≈ live_predicates × 38 B × (mean source-type cardinality)
```

- **Predicted ≈ observed** → the model holds; the fix is `source_rows`, i.e. `lv-4a87`.
- **Predicted ≪ observed** → the memory is somewhere the model does not describe. **Stop
  hypothesising and go to Step 6.**
- **`*_len()` ≫ `*_live_count()`, and `_len` grows with uptime** → hypothesis 5. Cross-check against
  Step 2's teardown result.

- [ ] **Step 5: Test whether RSS scales with client count**

Cheap discriminator, and it separates "per-client subscriptions" from "grows with uptime". Ask the user
to record RSS at 0, 1, 4, and 8 connected clients on an otherwise-idle node with a fixed dataset.

- **RSS scales with client count** → live subscriptions dominate; hypothesis 1.
- **RSS roughly flat but high** → the cost is not per-client. Combined with Step 4's uptime figure,
  that points at hypothesis 5.

- [ ] **Step 6: Run `heaptrack` against a node under normal load**

```bash
heaptrack ./target/release/<server-binary>
# … drive normal load …
heaptrack_print heaptrack.<binary>.<pid>.gz | head -100
```

Record: peak heap vs RSS (separating hypothesis 4 — allocator retention — from real live heap), the
top allocation sites by size and by count, and the share attributable to hyphae cells, to
`source_rows` clones, and to the three caches.

- [ ] **Step 7: Write down which hypothesis the evidence supports**

**M1 IS RESOLVED.** Final state as of 2026-07-27 — see
[`M1-findings.md` §8](../../architecture/mesh/M1-findings.md) for the evidence:

| Hypothesis | Status |
|---|---|
| **Derived/query-cell runtimes dominate** | **Confirmed and leading.** ~254 KB/item at rack scale (27,383 items / 6.96 GB live), concentrated in 13,195 `query_cells`. Attack surface: rship `lv-fc26` (~4M cells / ~29 GB of binding-node join/map runtimes), hyphae **PR #20** (`height_dependents` dedup leak). |
| `source_rows` per stage (`lv-4a87`) | **Real, quantified at ~208 B/(N·P)**, reproduced on main @ `0ed566f9`. **Scoped to views/reports/hand-written operator chains** — generated queries mostly bypass hyphae's `MapState`. Contributes; does not dominate. |
| Allocator retention | **REFUTED for rack.** `rship_server` runs `tikv-jemallocator`; RSS/live is **1.33×**, so ~75% of RSS is genuinely live. True and large (889×) for *glibc* processes only. |
| Predicate leak at the hyphae layer | **Killed** — teardown returns 100%. |
| Cache sweep lag | **Ruled out.** |

> **Three of my pre-measurement calls were wrong, and the record should say so.** Hypothesis 5
> (predicate leak) was promoted to "leading" on arithmetic and the measurement killed it. Hypothesis 4
> (allocator retention) was then promoted to leading on the local 889× result — and was refuted two
> days later, because the deployment it was meant to explain runs a different allocator. And the
> 208 B/(N·P) coefficient was quoted without checking which code paths actually reach hyphae's
> `MapState`; most generated queries do not.
>
> **The generalisable lesson is not "RSS lies."** It is: *an instrument is only valid for the
> configuration it was calibrated against.* `malloc_trim` is a glibc instrument; rship runs jemalloc.
> A `select`-based harness measures the `MapState` path; generated queries take four other paths.
> **Check the configuration before transferring the number.**

**Do not stop at "probably 1."** The design consequence forks on the answer, so the answer needs
evidence, not a ranking. And if the evidence supports **none** of the five, say so and widen the
search — an unexplained 1000× is a worse outcome than an inconvenient explanation.

### Task 2: Record the finding and its design consequence

**Files:** Create `docs/architecture/mesh/M1-findings.md`; modify `README.md`, `01-node-model.md`

- [ ] **Step 1: Write `M1-findings.md`**

Structure: the measurement, the method, the numbers, which hypothesis is supported and which are
ruled out (with the evidence for each ruling), and the consequence.

- [ ] **Step 2: Record the consequence in the architecture set**

The fork, from the spec:

| Finding | Consequence |
|---|---|
| **Inherent per-subscription amplification** | 08 §4's filter model becomes **load-bearing rather than an optimization**. Browser nodes need hard subscription budgets. **A disk-backed store would not help at all, because the memory is not in the store.** |
| **Sweep lag or fragmentation** | A bug with a fix. The design stands as written and the current implementation gets its RAM back. |

Update 01 NM-8 to state the finding rather than the open question, and update the README's open-items
table. **Leave the invariant id NM-8 in place** — invariant ids are stable and never reused.

- [ ] **Step 3: Resolve or update `lv-4a87`**

```bash
levi comment lv-4a87 "M1 heap profile: <finding>. See docs/architecture/mesh/M1-findings.md"
# and, if the profile confirms it and a fix lands:
levi close lv-4a87
```

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/mesh
git commit -m "docs(mesh): record the M1 heap-profile finding and its design consequence"
```

### Task 3: M2 — gossip topic count

Cheap, blocks nothing, and worth doing while the profiling setup is warm.

- [ ] **Step 1: Measure the per-topic cost of iroh-gossip membership**

A standalone binary joining N topics on one endpoint, sampling RSS at N = 1, 10, 100, 1000. §5.7 puts
a topic per scope, so a node serving 1000 organizations holds 1000 HyParView/PlumTree memberships.

- [ ] **Step 2: Record the number in `M1-findings.md` under an M2 heading**

If per-topic cost makes 1000 scopes untenable, that is a design input for phase 7 — topic sharing
across low-traffic scopes — not a phase-2 blocker.

---

## Workstream B — Encoding and merge benchmarks

**Purpose: the wire break must be measured before it commits, not after.**

### Task 4: Widen the bench entity set

**Files:** Modify `libs/myko/core/src/bench_entities.rs`

- [ ] **Step 1: Add three shapes**

The existing bench entities were built for `dyn_vs_typed` and the advanced-query filter benches. The
encoding bench needs shapes chosen for the thing being measured — **the ratio of per-field metadata to
payload**, which is the whole question about field-addressed encoding.

| Shape | Why |
|---|---|
| **Narrow** — 3 scalar fields | Worst case for field-addressed encoding: 13 bytes of per-field metadata against tiny values. If it loses anywhere, it loses here. |
| **Wide** — 50 scalar fields | The case field-addressing exists for: a one-field edit ships one field instead of fifty. |
| **CRDT-bearing** — a set, a map, a counter, plus scalars | Measures CRDT state overhead in the value bytes, which no scalar shape shows. |

All three behind the existing `bench` feature, matching how `bench_entities.rs` is already gated.

- [ ] **Step 2: Verify they compile under the feature**

```bash
cargo check --target-dir target/claude -p myko --features bench
```

### Task 5: The encoding benchmark

**Files:** Create `libs/myko/core/benches/record_encoding.rs`; modify `libs/myko/core/Cargo.toml`

- [ ] **Step 1: Register the bench**

In `libs/myko/core/Cargo.toml`, below the existing `[[bench]]` entries (`:84`, `:89`):

```toml
[[bench]]
name = "record_encoding"
harness = false
required-features = ["bench"]
```

- [ ] **Step 2: Write the bench**

Four axes, measured for each of the three shapes:

| Axis | Compares |
|---|---|
| **Full-entity encode** | field-addressed record vs `serde_json` (today's `MEvent.item`) vs `ciborium` |
| **Full-entity decode** | the same three |
| **Single-field update encode** | field-addressed (1 field) vs today's whole-entity JSON — **the headline number** |
| **Record size on the wire** | bytes, for all of the above |

> **Guard the existing win.** `ItemRegistration::serialize_json` (`core/item/traits.rs:90`) is a typed
> shim that sidesteps `erased_serde`'s vtable on the JSON emit path — a measured ~20% improvement.
> **The JSON baseline in this bench must use that path**, not `erased_serde`, or field-addressed
> encoding will look better than it is (10 CL-18).

- [ ] **Step 3: Run and record**

```bash
cargo bench --target-dir target/claude -p myko --features bench --bench record_encoding
```

Record results in `M1-findings.md` under an "Encoding baseline" heading, with the exact commit sha.
These are the numbers phase 3's exit criteria are checked against.

- [ ] **Step 4: Interpret against the design claim**

The design's claim is that field-addressed encoding is a net win because **writes carry only changed
fields**. That claim is:

- **Confirmed** if single-field-update encoding and size beat whole-entity JSON by a wide margin on
  the wide shape, and full-entity encoding is not materially worse.
- **Qualified** if the narrow shape regresses meaningfully. That is acceptable — narrow entities are
  cheap in absolute terms — but it must be **written down**, not discovered later.
- **Contradicted** if full-entity encode/decode regresses materially on realistic shapes. **Stop and
  reopen §9 of the spec.** This is exactly the outcome phase 2 exists to catch before phase 3 makes it
  permanent.

### Task 6: The merge/apply benchmark

**Files:** Create `libs/myko/core/benches/merge_apply.rs`

- [ ] **Step 1: Bench per-field merge against today's arrival-order overwrite**

Today's apply is `store.insert_many` inside `emit_grouped` — arrival-order overwrite with no
comparison at all (`server/context.rs`). Per-field merge is strictly more work; the question is how
much.

Measure: LWW merge-join at 3/50 fields; PN-Counter merge at 1/10/100 actors; ORSWOT merge at
10/100/1000 elements; LWW-Map merge at 10/100 keys; and the batch-apply path end to end at realistic
batch sizes.

- [ ] **Step 2: Record and sanity-check the actor bound**

The ORSWOT and PN-Counter numbers are what justify bounding the actor set to durable nodes rather than
letting every client be an actor. If per-actor cost is negligible at 100 actors, note it — it widens
the design's options later. If it is not, that bound is load-bearing and should be stated as such.

- [ ] **Step 3: Commit both benches**

```bash
git add libs/myko/core/benches libs/myko/core/src/bench_entities.rs libs/myko/core/Cargo.toml
git commit -m "bench(core): measure field-addressed encoding and per-field merge before the wire break"
```

---

## Workstream C — `myko-sim`

### Task 7: Create the crate

**Files:** Create `libs/myko-sim/`; modify workspace `Cargo.toml`

- [ ] **Step 1: Scaffold and add to the workspace**

```toml
# Cargo.toml (workspace)
members = [
  "libs/myko/core",
  "libs/myko/macros",
  "libs/myko/server",
  "libs/myko/leptos",
  "libs/autosocket",
  "libs/myko-sim",          # <- new
]
```

`myko-sim` depends on `myko` and `rand_chacha`, and on **no real transport**. It is a dev-facing crate
and is not published.

- [ ] **Step 2: Define the harness surface**

```rust
/// A deterministic multi-node simulation. Every source of nondeterminism —
/// scheduling, clock, fault injection, message order — derives from `seed`,
/// so a failing run is reproducible from the seed alone.
pub struct Sim {
    seed: u64,
    nodes: Vec<SimNode>,
    clock: VirtualClock,
    faults: FaultSchedule,
}

pub enum Fault {
    /// Sever both directions between two node sets until `heal_at`.
    Partition { a: NodeSet, b: NodeSet, heal_at: SimTime },
    /// Deliver each message n times.
    Duplicate { factor: u8 },
    /// Deliver messages out of order within a window.
    Reorder { window: Duration },
    /// Add latency, optionally asymmetric.
    Delay { min: Duration, max: Duration },
    /// Offset one node's wall clock — drives HLC skew and the drift bound.
    ClockSkew { node: NodeId, offset: i64 },
}
```

> **Every fault in this list is one the design makes a claim about.** Duplication is why op-based
> CRDTs are ruled out; reorder is why merge must be commutative; clock skew is why HLC exists and why
> the drift bound exists. The harness is not generic fault injection — it is the falsifier for
> specific claims.

- [ ] **Step 3: Implement the in-memory transport**

Implement `MeshTransport` (10 CL-2) over in-process channels, with the fault schedule applied at
delivery. **No `tokio::time`, no wall clock** — all timing is virtual, or the harness is not
deterministic.

- [ ] **Step 4: Implement the virtual clock**

Per-node wall clock with configurable offset, driving HLC generation. The clock advances only when the
scheduler says so, so a seeded run replays identically.

### Task 8: The gate scenario

**Files:** Create `libs/myko-sim/tests/partition_heal.rs`

- [ ] **Step 1: Write the two-node partition-and-heal test**

```
1. Two nodes, both complete for scope S, converged on entity E.
2. Partition.
3. A writes E.name; B writes E.description  — different fields.
4. A writes E.status = "x"; B writes E.status = "y"  — same field, genuine conflict.
5. Heal.
6. Assert:
     - name and description BOTH survive          (per-field independence)
     - status resolves by (hlc, origin) on BOTH   (deterministic total order)
     - both nodes' content hashes are equal       (convergence)
     - a conflict record exists locally on the losing side, unreplicated
     - the heal summary replicates
7. Re-run with the same seed: byte-identical outcome.
8. Re-run with 100 different seeds: every one converges.
```

Steps 6's last two assertions will not pass until phase 11 lands conflict recording. **Write them now
and mark them `#[ignore]` with the phase they unblock in the ignore reason** — the scenario is the
harness's own acceptance test, and a scenario that only asserts what already works does not prove the
harness can catch anything.

- [ ] **Step 2: Verify determinism explicitly**

```bash
cargo test --target-dir target/claude -p myko-sim partition_heal -- --nocapture
cargo test --target-dir target/claude -p myko-sim partition_heal -- --nocapture   # identical output
```

If the two runs differ, **the harness is not usable and the phase is not done.** Nondeterminism in a
harness built to prove convergence is worse than no harness — it produces flakes that get muted.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml libs/myko-sim
git commit -m "test(sim): add the deterministic simulation harness with partition/heal coverage"
```

---

## Exit criteria

- [ ] **The amplification is reproduced locally** — `examples/amplification.rs` shows live heap as a
      function of entity count × live-predicate count, with a fitted per-entity-per-predicate
      coefficient.
- [ ] **The teardown test has run**, and predicate leak (hypothesis 5) is either confirmed or killed.
- [ ] **M1 has an answer**, backed by the harness *and* a heap profile, recorded in `M1-findings.md`,
      with the design consequence written into 01 NM-8 and the README's open-items table.
- [ ] **M2 measured** and recorded.
- [ ] **Field-addressed encoding is measured**, not assumed, against today's path — including the
      typed-`serialize_json` baseline, not `erased_serde`.
- [ ] **Per-field merge cost is measured** for all four strategies.
- [ ] **`myko-sim` runs the two-node partition-and-heal scenario deterministically from a seed**, with
      byte-identical output across runs, and converges across 100 seeds.
- [ ] The bench numbers are recorded **with the commit sha**, so phase 3 can be checked against them.

## If M1 says "inherent"

Before phase 3 starts, these must be recorded — not implemented, recorded:

1. **08 §4's filter model becomes load-bearing.** Reword RP-15 and RP-23 from "bounds memory by
   working-set size" to a claim the profile actually supports.
2. **Phase 9 grows**: hard subscription budgets per attached node, and an eviction policy under memory
   pressure. Add both to the roadmap's phase-9 deliverables.
3. **A disk-backed store is off the table as a mitigation** — the memory is not in the store. Say so
   explicitly, because it is the intuitive fix and it does not work.
4. **The gateway capacity model (01 §6) needs a number.** "Clients × their subscriptions" becomes a
   sizing constraint with a coefficient rather than a shape.

**Phase 3 still proceeds either way** — the record format does not change on this finding. What
changes is what the design is allowed to promise.
