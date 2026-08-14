# 10 — Crate Layout, `NodeScoped`, and Migration

**Normative.** Source: spec §13, §18, plus repository ground truth. Invariant prefix `CL`.

---

## 1. Today

```
Cargo.toml (workspace, v5.0.1, edition 2024)
  libs/myko/core        myko            — the framework; compiles to wasm
  libs/myko/macros      myko-macros     — #[myko_item], #[myko_command], …
  libs/myko/server      myko-server     — WS handler, Postgres, peer registry, MCP
  libs/myko/leptos      myko-leptos     — Leptos/web-ui integration
  libs/autosocket       autosocket      — reconnecting WS transport (native + wasm)

  libs/myko/{ts,py,python,cpp,csharp,ui-svelte,ui-vue,debug}   — ports and bindings
```

Inside `myko-core`:

```
src/lib.rs
  cache            client          codegen (native + feature)   codegen_types
  core/            entities        operation_index (native)     search (native)
  server/ (native) store/          utils/                       wire/
```

`core/` holds `capability.rs`, `reflection.rs`, `request.rs`, and the `command/`, `query/`, `view/`,
`report/`, `item/`, `relationship/`, `saga/`, `common/` modules. `server/` — the entire module, gated
`#[cfg(not(target_arch = "wasm32"))]` at `lib.rs:91` — holds `MykoServerContext`, the apply pipeline
(`apply_event_batch` → `emit_grouped` → `apply_effects`), and the caches.

## 2. Target layout

> **CL-1** — **New crates, not new modules in `core`.** The mesh subsystems have distinct dependency
> profiles (iroh, blake3, blobs) and distinct wasm stories; folding them into `core` would force every
> consumer to carry them.

| Crate | Contains | Depends on | wasm |
|---|---|---|---|
| **`myko`** (`core`) | records (03), merge (04), state store (07 §4), capabilities traits, reflection, `MykoNodeContext` | — | **yes** |
| **`myko-mesh`** *(new)* | manifests, roles, routing table, anti-entropy (08 §2), planes and envelopes (06), handshake, capability grants (05 §4) | `myko` | yes (no transport) |
| **`myko-iroh`** *(new)* | the iroh binding of 06 TP-1: endpoint, ALPN registration, gossip, blobs bulk | `myko-mesh`, `iroh` | native only in v1 |
| **`myko-log`** *(new)* | the log subsystem (07 §5): index, compaction, checkpoints, backfill | `myko` | yes (IndexedDB backend) |
| **`myko-server`** | gateway (WSS termination), Postgres log backend, MCP, HTTP surface | `myko-mesh`, `myko-log` | native only |
| **`myko-sim`** *(new, dev)* | deterministic simulation harness (spec §16) | `myko-mesh` | native only |
| **`myko-macros`** | unchanged role; gains field schemas (02 §4), `edge_owned`, `routing_key`, `namespace`, `myko_field` | — | n/a |
| **`myko-debug`** | the JSON record renderer (03 RF-25) | `myko` | yes |

> **CL-2** — **`myko-core` must not depend on `iroh`.** The transport contract (06 TP-1) is satisfied
> by a trait in `myko-mesh`; `myko-iroh` implements it. A datacenter deployment, a simulation run, and
> a future WebRTC binding all substitute at that seam.

```rust
// myko-mesh::transport — the whole contract, 06 TP-2..TP-4
#[async_trait]
pub trait MeshTransport: Send + Sync + 'static {
    async fn dial(&self, peer: NodeId, plane: Plane) -> Result<Connection>;
    async fn accept(&self) -> Result<(NodeId, Plane, Connection)>;
    fn local_id(&self) -> NodeId;
}
```

> **CL-3** — **The conformance vectors (03 §7) live outside every crate**, in
> `conformance/vectors/`, so `libs/myko/ts`, `py`, `cpp`, and `csharp` consume the same directory
> without a Rust dependency. Generation is a Rust binary; consumption is language-agnostic files.

## 3. The `NodeScoped` refactor (D1)

Clients become full nodes: `ServerScoped` is re-rooted as `NodeScoped`, and wasm gets a real reduced
backing instead of `unreachable!()`.

### Current shape, verified

- Eight capability traits descend from `ServerScoped` (`core/capability.rs:87`), whose sole accessor
  is `__server_ctx() -> &Arc<MykoServerContext>` — that method and the whole `server` module
  (`lib.rs:91`) are `#[cfg(not(target_arch = "wasm32"))]`.
- `RegistryScoped` (`:74`) and `RequestScoped` (`:51`) sit **outside** `ServerScoped` and are ungated.
- On wasm, capability bodies are `unreachable!()` stubs via the `wasm_native!` macro (`:113`) — except
  `Viewing` (`:385`), `PeerAccess` (`:408`), and `Replaying` (`:421`), which are **whole-trait**
  native-gated and must therefore be *created* on wasm rather than un-stubbed.
- `CommandHandler::execute` is a hardcoded error on wasm
  (`core/command/handler.rs:198`, "not supported on wasm32").
- `MykoClient` holds a socket and tx-keyed dispatch maps with **no store at all**.

### The plan

> **CL-4** — **Reduce the context; do not trait-object it.** Making `__node()` return
> `&Arc<dyn NodeRuntime>` puts dynamic dispatch on **every capability call**, and prior benchmarking
> found the existing dyn boundary already accounts for a meaningful slice of rship's hot path
> (~10%). The refactor must not widen it.

1. Rename `MykoServerContext` → `MykoNodeContext`; `ServerScoped` → `NodeScoped`.
2. Un-gate the module for wasm, gating only genuinely native internals.
3. **Subsystems a node lacks become `Option` fields** — the struct already does this for `event_sink`
   and `history_replay`.
4. **Capability methods needing an absent subsystem return `Result`/`Option`** instead of
   `unreachable!()`.

> **CL-5** — Step 4 is **strictly better than today's behaviour even natively**: a panic-stub becomes
> a typed absence. No caller loses information.

> **CL-6** — `Viewing`, `PeerAccess`, and `Replaying` are **created for wasm**, not un-stubbed. They
> have no wasm bodies today because the whole trait is gated, so there is nothing to un-gate.

### Feasibility, honestly stated

> **CL-7** — **This has not been compiled.** The claim is "blockers appear few and localized," not
> "it builds." **The first task of the phase is an un-gate spike producing a real error list**, and
> the phase's scope is set from that list, not from this document.

Supporting evidence: across all of `core`, only three files reference `thread::spawn` or `tokio` —
`core/query/registration.rs`, `client/mod.rs` (already wasm-gated), and `server/context.rs`. Postgres
persisters live in `myko-server`.

> **CL-8** — Thread-spawning in `server/context.rs` is the known thing to isolate, behind a
> **scheduler seam**: a small trait with a native thread-pool implementation and a wasm
> `spawn_local` one. `core/query/registration.rs` gets the same treatment.

## 4. Macro changes, consolidated

Every macro-side change the mesh requires, in one place. All are additive except where noted.

| Change | Macro | Doc |
|---|---|---|
| `fields: &'static [FieldSchema]` on `ItemRegistration` | `item.rs` | 02 TI-12 |
| `field_id` = FNV-1a 32, collision-checked at expansion | `item.rs` | 02 TI-14 |
| Merge strategy selected from the declared type | `item.rs` | 04 MG-8 |
| `namespace` derived from `module_path!()` first segment | all registration macros | 02 TI-3 |
| `#[myko_item(namespace = "...")]` override | `item.rs`, `command.rs` | 02 TI-4 |
| `#[myko_field(merge = ..., renamed_from = "...")]` | new attribute | 02 TI-15, 04 MG-8 |
| `#[myko_item(edge_owned)]` | `item.rs` | 09 CR-28 |
| `#[scoped_by(Organization)]` | `relationship.rs` | 05 SC-4 |
| `#[routing_key]` on a command arg; compile error if sticky and unresolvable | `command.rs` | 09 CR-24 |
| `optimistic` opt-in marker on commands | `command.rs` | 09 CR-21 |
| Consistency declaration on commands | `command.rs` | 09 CR-27 |
| Cross-scope reference rejection where statically knowable | `relationship.rs` | 05 SC-6 |

> **CL-9** — `ItemArgs` (`libs/myko/macros/src/item.rs:13`) currently parses two options
> (`ingest_buffer_ms`, `post_deserialize`) and **hard-errors on anything else**. Every new option
> above must be added there or the attribute fails to parse — this is the single choke point and is
> easy to miss when adding an attribute in a different file.

## 5. Codegen impact

> **CL-10** — Generated TypeScript identifiers stay **unqualified** (02 TI-5). The qualified name is a
> wire and registry key; `GetAllTargets` and `TargetQuery` do not change name.

> **CL-11** — The crate filter in `libs/myko/core/src/codegen/mod.rs` uses
> `x.crate_name.contains(&crate_name)` at eight call sites, over-matching sibling crates (tracked as
> `lv-ea59`). **02 TI-3's precomputed `namespace` field replaces it with an equality check** — fixing
> the bug as a side effect of the mesh work rather than as separate churn.

## 6. Migration

> **CL-12** — **Land every wire break in one phase.** Each break is a migration for live consumers,
> and the wire break is the least reversible step in the plan.

> **CL-13** — A **migration converter** ships with the wire break, re-encoding existing history:
> RFC3339 → HLC, whole-entity JSON → field entries, bare `item_type` → the reserved default namespace
> (02 TI-6). It runs against the Postgres log offline and is idempotent.

> **CL-14** — Converted records carry `origin` = the converting deployment's node id and preserve the
> original `created_at` as the HLC physical component with logical 0. Attribution that never existed
> is not invented: `actor` is absent on converted records, not defaulted to a placeholder identity.

### Phase-0 prerequisite: restore `Origin::Remote`

> **CL-15** — The event-bus unification (PR #25) supplies the single apply chokepoint this design
> depends on — `apply_event_batch` → `emit_grouped` → `apply_effects`. **One regression must be
> reversed:** the `Origin::Remote` apply mode PR #25 introduced has since been removed. Only
> `Local | Cascade` remain (`libs/myko/core/src/server/context.rs:59`), so **wire-ingested events
> currently apply as `Local` — cascading and producing.** The remote mode returns with the planes
> (04 MG-16, 06 TP-9).

> **CL-16** — `feat/iroh-dataplane` carries partial convergence work targeting wall-clock timestamps
> and whole-entity resolution. It **does not match 04 and is to be rewritten, not landed.** Its two
> survivable ideas reappear here: the `(ts, source_id)` total order as 03 RF-3's `(hlc, origin)`, and
> tombstones in the stamp index as 04 MG-25.

## 7. Performance budget

Constraints the implementation must respect, carried from prior measurement.

> **CL-17** — **No new `dyn` on the capability call path** (CL-4). The existing boundary is ~10% of
> rship's hot path; the mesh must not add a second.

> **CL-18** — **Record encode/decode must not regress the JSON emit path** already won by the typed
> `serialize_json` shim on `ItemRegistration` (`core/item/traits.rs:90`), which sidesteps
> `erased_serde`'s vtable. Field-addressed encoding replaces that path; the phase-2 benchmark measures
> it before the wire break commits (roadmap phase 2).

> **CL-19** — **`Arc<str>` interning on hot fields stays** (06 §8). `intern_entity_type`
> (`wire/event/mod.rs`) becomes qualified-name interning; the mechanism does not change.

> **CL-20** — **M1 gates any memory claim.** `query/view/report` cache growth already scales with
> materializations × source size rather than matches (`lv-4a87`), and 08 RP-23 relocates rather than
> removes the RAM question. Until the phase-2 heap profile lands, no document in this set may be cited
> to size a node.

---

## Invariant index

| ID | One line |
|---|---|
| CL-1 | Mesh subsystems are new crates, not `core` modules |
| CL-2 | `myko-core` must not depend on `iroh`; the seam is a trait |
| CL-3 | Conformance vectors live outside every crate |
| CL-4 | Reduce the context; do not trait-object it |
| CL-5 | Typed absence beats today's panic-stub, even natively |
| CL-6 | `Viewing`/`PeerAccess`/`Replaying` are created for wasm, not un-stubbed |
| CL-7 | D1 has not been compiled; the un-gate spike sets the scope |
| CL-8 | Thread spawning goes behind a scheduler seam |
| CL-9 | `ItemArgs` hard-errors on unknown options — the single choke point |
| CL-10 | Generated TS identifiers stay unqualified |
| CL-11 | The precomputed `namespace` replaces codegen's substring filter (`lv-ea59`) |
| CL-12 | Land every wire break in one phase |
| CL-13 | A migration converter ships with the wire break |
| CL-14 | Converted records do not invent attribution |
| CL-15 | `Origin::Remote` must be restored with the planes |
| CL-16 | `feat/iroh-dataplane` is rewritten, not landed |
| CL-17 | No new `dyn` on the capability call path |
| CL-18 | Do not regress the typed-serialize emit win |
| CL-19 | `Arc<str>` interning stays |
| CL-20 | M1 gates every memory claim |
