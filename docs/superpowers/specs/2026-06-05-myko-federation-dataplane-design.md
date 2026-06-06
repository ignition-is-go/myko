# Myko Federated Realtime Dataplane — Design

**Date:** 2026-06-05
**Status:** Draft for review
**Scope:** Causal sync substrate + conflict detection/surfacing (sub-projects #1 + #2). Command federation/delegation (#3) is explicitly deferred to a later spec.

---

## 1. Goal

Make a myko-server able to **federate its data with other myko-servers** over unreliable links: nodes pair voluntarily, replicate in realtime when connected, keep operating independently during an internet drop, and **converge automatically on reconnect** — surfacing genuine concurrent-edit conflicts to a human rather than silently losing data.

The result is, in effect, a **distributed, realtime, conflict-aware data layer** built into myko-server. Consuming apps (rship first) get it by enabling it and pairing — they keep defining data the normal way with `#[myko_item]`.

### Driving properties

- **Leaderless mesh of equal peers.** No node is authoritative. "The cloud" is just a well-connected peer you choose to pair with (often for backup). It is not special in the protocol.
- **Opt-in and modular.** Off by default and dormant until a node is paired. A node not using it behaves exactly as today.
- **Self-healing.** Convergence does not depend on any message stream being perfectly reliable; nodes periodically compare ground truth and repair divergence (anti-entropy).
- **Survives partitions + restarts.** Durability is local (each node's own Postgres); a node can crash mid-partition and lose nothing it had committed locally.
- **Honest about conflicts.** Concurrent edits to the same entity across a partition are *detected* (not guessed at) and *surfaced to a human*, not auto-merged.

## 2. Non-goals

- **No automatic merge / CRDT semantics.** Conflicts are surfaced for human resolution. (A future per-type merge strategy could be layered on, but is out of scope.)
- **No command federation/delegation.** Routing/delegating commands across nodes is sub-project #3, a separate spec built on this substrate.
- **No strong consistency / consensus.** This is eventual consistency with explicit conflict surfacing, not Raft/Paxos.
- **No central discovery service.** Membership bootstraps from explicit runtime pairing (§7).
- **No generic table/document API.** The layer federates typed `#[myko_item]` entities, the myko-native way.

## 3. Where it lives

Federation is a **first-class, opt-in myko-server capability**, not a separately deployed service. All of it — data plane and control plane — lives in the framework. `Peer`, `Conflict`, and the pairing/resolution commands ship as **framework-provided entities/commands** (consistent with the existing built-in `Server` entity).

It is delivered primarily as **a new persister** plugged into the existing `PersisterRouter`, plus two fresh hooks in the write/apply pipeline.

### 3.1 Persistence is a per-node choice

myko-server already routes events through `PersisterRouter` to persisters (`PostgresProducerHandle`, `PeerPersister`). The dataplane is **another persister** (`DataplanePersister` + its consumer side). A node's persistence config becomes a deliberate choice:

| Config | Behavior |
|---|---|
| Postgres + Dataplane | Durable locally **and** federated — the offline-resilient edge, or the always-on cloud node |
| Dataplane only | No local durable store; leans on the mesh. A thin/cache participant |
| Postgres only | Exactly today's behavior; federation dormant |

**Durability ⇐ local Postgres. Reach/backup ⇐ dataplane persister.** Mix per node. A dataplane-only node that gets partitioned can serve from memory but has no durable source of truth and loses unsynced state on restart — so "withstands internet drops" is a property of nodes running *both* persisters. This is stated as an explicit expectation, not a limitation to fix.

### 3.2 The four internal pieces

These were considered as candidate public "extension seams"; the decision is to keep them as **internal modular structure** of the federation feature (no public seam contract for its own sake), except where an existing seam already fits.

1. **Write-path metadata hook** — in `CellServerCtx::set_with_options` / `del_with_options`: stamp the entity's version vector on outgoing writes. *(New hook.)*
2. **Apply-path classification hook** — in `apply_event_batch`: classify an incoming event as ancestor (fast-forward), descendant (ignore), or concurrent (conflict) **before** the store's reduce/LWW discards a version. *(New hook — this is the load-bearing one; conflict detection is impossible after LWW has run.)*
3. **Derived-index maintenance** — the Merkle index is maintained from the event stream the same way `core/search/index.rs` maintains the search index. *(Reuses the existing pattern; generalize if convenient.)*
4. **Peer protocol over existing connections** — anti-entropy runs over the connections `PeerRegistry` already manages. *(Reuses `PeerRegistry`; `PersisterRouter` is the existing seam for the data-movement half.)*

## 4. Core data model

### 4.1 Node identity

Reuses the existing `source_id` (host UUID) already present on `MEvent`. This is the key into version vectors.

### 4.2 Version vectors (causality)

Each federated entity carries a **version vector** `VV: {node_id → counter}`. On each local SET/DEL, the writing node increments its own component. Comparing two VVs classifies the relationship:

- **Equal** → same version, nothing to do.
- **A dominates B** (A ≥ B componentwise, A ≠ B) → A is strictly newer; fast-forward, **no conflict**.
- **Concurrent** (neither dominates) → genuine concurrent edit → **conflict**.

This is what makes "surface to human" possible: it distinguishes "newer version" from "conflicting version," which wall-clock + the global Postgres `id` cannot do across a partition (each node has an independent `id` sequence).

`MEvent` gains: the entity's post-write `VV` (and, if needed for ordering, a per-node monotonic sequence). It already carries `source_id`, `created_at`, `tx`.

### 4.3 Merkle index (divergence discovery)

A tree over the keyspace, leaves keyed by `(item_type, id)` hashing `(id, VV, content_hash)`. **`content_hash` is the existing per-item `hash: Arc<str>` field** — every `#[myko_item]` already carries it, so leaves are nearly free.

- Built **per `item_type`** (matches how `StoreRegistry` already partitions by type), rolling up to a per-type root.
- Two nodes compare roots; equal ⇒ that type is in sync (cheap). Unequal ⇒ descend only differing branches to find exact divergent keys.
- Maintained incrementally as events apply.

### 4.4 Tombstones

DEL leaves a **tombstone** (a DEL event with a VV) retained in the Merkle index, so a peer that still has the entity learns it was *deleted*, not merely *absent* (otherwise anti-entropy would resurrect it). Tombstones are garbage-collected on a boring default retention window (e.g. configurable, default ~30 days). GC strategy is intentionally **not** a focus of this design; the default is "retain N days," with the understanding that a node partitioned longer than the window could resurrect deleted data. Membership-aware GC is a possible future refinement.

## 5. Data plane

### 5.1 Local write path

1. Command handler emits SET/DEL via `CommandContext`.
2. Write-path hook (§3.2.1) reads the entity's current VV, increments this node's component, stamps it on the event and the stored entity.
3. Store reduce + relationship cascade (unchanged).
4. `PersisterRouter` fans the event to configured persisters: local Postgres (durability) and `DataplanePersister` (federation).

### 5.2 Realtime fast path (connected)

When peers are connected, `DataplanePersister` pushes new events over the existing `PeerRegistry` connections immediately — low-latency propagation, same spirit as today's `PeerPersister`, but carrying VVs and routed through apply-path classification on the receiving end.

### 5.3 Anti-entropy backstop (authoritative)

Periodically (and on reconnect), paired nodes run an anti-entropy session over their connection:

1. Exchange per-type Merkle roots.
2. For differing types, descend the trees to enumerate divergent `(item_type, id)` keys.
3. For each divergent key, exchange the entity + VV and run apply-path classification (§5.4).

Anti-entropy is the **self-healing source of truth**: even if every fast-path message were dropped, periodic anti-entropy still converges the mesh. The fast path is a latency optimization on top.

### 5.4 Apply-path classification (incoming events)

When an event arrives (fast path, anti-entropy, or the dataplane consumer feeding `apply_event_batch`), classify *before* reduce:

- **Ancestor of local** → ignore (we're already ahead).
- **Descendant of local** → fast-forward apply (normal reduce).
- **Concurrent with local** → **conflict** → §6.

### 5.5 Transitive convergence

Anti-entropy runs **pairwise**. Convergence across the whole mesh is *emergent*: if A↔B and B↔C are paired but A and C never pair, A's writes still reach C through B. No node needs global membership knowledge for data to converge.

## 6. Conflict model

### 6.1 Detection

A conflict is exactly a **concurrent** VV relationship (§4.2) on the same entity. No heuristics.

### 6.2 Deterministic conflict identity

In a mesh, the *same* conflict is detected independently by multiple nodes. To avoid N duplicate conflict records, the **`Conflict` entity id is derived deterministically** from the conflicting inputs (the two VVs + content hashes). Every node independently computes the *same* id, so the `Conflict` object itself converges through the dataplane like any other entity.

### 6.3 The `Conflict` entity

A framework-provided `#[myko_item]` holding:

- The conflicting entity's `(item_type, id)`.
- Both (or all) concurrent versions: content + VV + `source_id` + `created_at`.
- The common-ancestor version where derivable (for context in the resolution UI).
- Resolution status (pending / resolved).

While a conflict is pending, the entity remains readable (a provisional/last-applied version is shown); the `Conflict` record signals that a human decision is owed. (Exact provisional-read semantics — show provisional vs freeze — is a spec-level detail flagged for the implementation plan.)

### 6.4 Resolution

A framework-provided command lets an operator resolve a `Conflict` — pick a version (or supply a merged value). Resolution emits a normal SET whose VV **dominates all conflicting versions** (so it propagates as an unambiguous fast-forward and the conflict closes everywhere via the same dataplane).

## 7. Control plane — membership & pairing

### 7.1 Runtime pairing, persisted locally

A node runs standalone. At runtime it is pointed at a peer (`wss://…`) via a command/API. The two nodes handshake, exchange identities, and **each persists the relationship in its own local Postgres** in a **local, non-synced `peer` table** (distinct from the synced `Server` metadata). On restart, each node reads its local peer table and auto-redials.

This dissolves the bootstrap chicken-and-egg that the old "discover peers via the synced store" approach had once each node has its *own* Postgres: first contact is imperative (you hand it an address); thereafter the node **self-seeds from its own local peer table**, never from synced data.

### 7.2 Scoped pairings

Each pairing declares **what it covers** — a scope over entity types (with "everything" as one setting). The Merkle/anti-entropy machinery runs per-scope. Scope expression mechanism (e.g. a set of `item_type`s on the `Peer` record, and/or a `#[federated]` marker on entity types) is a spec-level detail for the plan.

### 7.3 Membership scope

Each node knows its **directly-paired** peers (from the local peer table). Data converges transitively without wider knowledge (§5.5). Wider membership knowledge (peer-list gossip) is **not required** for v1 and is left as a future refinement; its only real benefit is membership-aware tombstone GC and auto-healing around dead intermediaries (§4.4).

### 7.4 `Server` metadata

The existing `Server` entity continues as synced *metadata about* connected peers (version, capabilities, last-seen) — useful once connected, but **not** the discovery mechanism.

## 8. Failure handling

| Scenario | Behavior |
|---|---|
| Internet drop (node with local Postgres) | Keeps operating fully; writes accumulate durably in local Postgres. |
| Internet drop (dataplane-only node) | Serves from memory; unsynced state is lost on restart (§3.1). Expected. |
| Reconnect | Anti-entropy session runs, divergence discovered via Merkle descent, events exchanged, conflicts surfaced. |
| Process restart | Node reloads from local Postgres (if present) and re-dials peers from the local `peer` table. |
| Concurrent edit across partition | Detected as concurrent VV → `Conflict` entity (deterministic id) → human resolution. |
| Long partition > tombstone window | Possible resurrection of deleted data (accepted v1 trade-off; tune window). |
| Dropped fast-path messages | Anti-entropy backstop converges anyway. |

## 9. Integration points (existing code)

- `libs/myko/server/src/peer_registry.rs` / `peer_connection_handle.rs` — connection management; anti-entropy + pairing extend this.
- `libs/myko/server/src/peer_persister.rs` — evolves into `DataplanePersister` (VVs, fast path, conflict-aware).
- `libs/myko/core/src/server/persister.rs` — `PersisterRouter`; dataplane registers here.
- `libs/myko/core/src/server/context.rs` (`set_with_options` / `del_with_options`, `apply_event_batch`) — the two new hooks (§3.2.1, §3.2.2).
- `libs/myko/core/src/wire/event/mod.rs` (`MEvent`) — add VV (+ per-node sequence).
- `libs/myko/core/src/search/index.rs` — pattern to mirror for the Merkle index.
- `libs/myko/core/src/entities/server.rs` — `Server` metadata stays; add framework `Peer` + `Conflict` entities alongside.
- `libs/myko/macros` — possible `#[federated]` marker for scoping which entity types participate.

## 10. Testing strategy

- **VV classification unit tests** — ancestor/descendant/concurrent across synthetic vectors.
- **Merkle index tests** — incremental maintenance correctness; root equality iff identical sets; descent finds exactly the divergent keys; tombstones present.
- **Two-node convergence** — real myko-servers (per project convention: real entities + macros), partition + concurrent writes + reconnect; assert convergence and exactly the expected `Conflict`s.
- **Transitive convergence** — three nodes A↔B↔C (no A–C link); assert A's writes reach C.
- **Deterministic conflict identity** — same conflict detected on two nodes yields one converged `Conflict` (matching ids).
- **Resolution** — resolving emits a dominating VV that closes the conflict mesh-wide.
- **Restart durability** — node restart reloads from local Postgres and re-dials from the local `peer` table; no data loss.
- **Persistence configs** — Postgres+Dataplane, Dataplane-only, Postgres-only behave per §3.1.

## 11. Deferred / future refinements

- Command federation/delegation (sub-project #3) — separate spec.
- Per-type automatic merge strategies / CRDT fields (beyond human resolution).
- Membership gossip (peer-list exchange) → membership-aware tombstone GC + auto-healing around dead intermediaries.
- Provisional-read vs freeze semantics for pending conflicts (decide in the plan).
- Scope-expression mechanism details (`#[federated]` marker and/or per-pairing type sets).

## 12. Build phasing (high level; detailed plan via writing-plans)

1. **Causal substrate** — VVs on entities/events, write-path stamp hook, apply-path classification hook (fast-forward only, no conflicts yet), `DataplanePersister` fast path. Two-node fast-forward replication works.
2. **Anti-entropy** — Merkle index + per-type roots + descent + reconnect session. Self-healing convergence works.
3. **Conflicts** — concurrent detection, deterministic `Conflict` entity, resolution command. Human-surfaced conflicts work.
4. **Pairing/control plane** — runtime pair command, local `peer` table, auto-redial, scoped pairings, persistence-config matrix.
