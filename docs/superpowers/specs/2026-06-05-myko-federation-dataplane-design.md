# Myko Federated Realtime Dataplane — Design (iroh transport + LWW)

**Date:** 2026-06-05
**Revised:** 2026-06-18 — pivoted to an **iroh-based transport** and an **LWW conflict model**; the version-vector + human-conflict-surfacing model is retired (see "Revision note").
**Status:** Draft for review
**Scope:** Causal-free sync substrate over iroh (sub-project #1). Command federation/delegation (#3) is explicitly deferred to a later spec.

---

## Revision note (what changed and why)

The original (2026-06-05) design resolved concurrent edits with **per-entity version vectors** and **surfaced conflicts to a human**, backed by a Merkle anti-entropy index. That required adding a VV to `MEvent` (a wire change) and a `Conflict` entity + resolution command.

We have pivoted to:

1. **Last-writer-wins (LWW) by `created_at`** as the convergence rule — **no wire change** (`created_at` already rides on `MEvent`). Concurrent cross-partition edits are resolved deterministically; one side wins. There is **no** concurrent-edit detection and **no** human conflict surfacing.
2. **iroh** as the transport/identity substrate — NodeId-addressed QUIC with NAT traversal and relay fallback, replacing raw `wss://` peer connections.
3. **iroh-gossip** as the realtime dissemination path (automatic multi-hop forwarding), replacing the manual full-mesh fan-out.

**Retired from the prior design:** version vectors on the wire (§ old 4.2), concurrent-VV classification (old §5.4), the `Conflict` entity + resolution command (old §6), and human conflict surfacing.
**Retained, simplified:** the persistence-config matrix (durable vs stateless nodes), a Merkle anti-entropy backstop (now keyed on `content_hash` only, resolving via LWW rather than VV), tombstones, runtime pairing + local peer table, and transitive convergence.

> **Accepted trade-off:** LWW silently drops one side of a genuine concurrent edit across a partition. If a future workload needs conflict-honesty, the **hybrid** path (LWW default, VV opt-in per entity type) can be layered on this same foundation without redoing it (§14).

---

## 1. Goal

Make a myko-server able to **federate its data with other myko-servers** over unreliable links: nodes pair voluntarily, replicate in realtime when connected, keep operating independently during an internet drop, and **converge automatically on reconnect** via LWW — without manual event forwarding and without requiring public IPs.

The result is a **distributed, realtime, eventually-consistent data layer** built into myko-server. Consuming apps (rship first) get it by enabling it and pairing — they keep defining data the normal way with `#[myko_item]`.

### Driving properties

- **Leaderless mesh of equal peers.** No node is a protocol authority. Durability is a per-node property, not a special role (§8). "The cloud" is just a well-connected, always-on peer.
- **Opt-in and modular.** Off by default, dormant until a node is paired. A node not using it behaves exactly as today.
- **Self-healing.** Convergence does not depend on any single message arriving; nodes periodically compare ground truth and repair divergence (anti-entropy, §7.3).
- **Connectivity without public IPs.** iroh handles NAT traversal, hole-punching, and relay fallback; you pair by **NodeId**, not address (§5, §9).
- **Survives partitions + restarts.** Durability is local (a node's own Postgres). A node with local durability can crash mid-partition and lose nothing it had committed locally; a stateless node's guarantees are explicitly weaker (§8).
- **Deterministic convergence.** Final state is a function of the events (LWW by `created_at` + stable tiebreaker), not of delivery order (§6).

## 2. Non-goals

- **No version vectors / concurrent-edit detection / human conflict surfacing.** Retired (see Revision note). Convergence is LWW.
- **No automatic merge / CRDT field semantics.** A per-type merge or VV-opt-in could be layered later (§14), out of scope now.
- **No command federation/delegation.** Sub-project #3, a separate spec on this substrate.
- **No strong consistency / consensus.** Eventual consistency via LWW, not Raft/Paxos.
- **No central discovery service.** Membership bootstraps from explicit runtime pairing (§9).
- **No generic table/document API.** Federates typed `#[myko_item]` entities the myko-native way.

## 3. Prerequisites (must land before this)

This design rides on two foundational changes; neither is optional.

### 3.1 Prereq 0 — Event Bus Unification

See `2026-06-17-event-bus-unification-design.md`. Collapses the nine mutation pipelines into **one `apply_effects` path keyed by an `Origin` enum** (`Local` / `Cascade` / `Remote`). This design depends on it for three reasons:

1. **It is the single apply chokepoint** where the LWW guard (§6) and the dataplane's ingest classification live — instead of being smeared across nine pipelines.
2. **It makes "sagas only fire on local events" structural.** `Origin::Remote` does not `produce`, and `produce_*` is what feeds the saga `event_sink`; so peer-replicated events never reach sagas. This is the loop/echo correctness the dataplane relies on.
3. **Its deferred line-167 "transitive peer relay" open question is answered here:** gossip forwards automatically (§7.2), so `Origin::Remote` stays apply-only and needs **no** manual relay sub-case.

### 3.2 Prereq 1 — Deterministic LWW apply + tombstones

The current apply path overwrites by **arrival order** with no comparison (`context.rs` reduce; `created_at` is carried but never read), and DEL removes from the store with **no tombstone**. That is not convergent — two nodes receiving the same conflicting writes in different orders diverge permanently, and a stale SET after a DEL resurrects the entity.

Land deterministic LWW in the unified `apply_effects`/`reduce_*`:

- **SET wins iff `incoming.created_at > stored.created_at`**, with a stable tiebreaker (`hash`, then `source_id`) on equal timestamps. This guard *also* provides idempotency (a re-delivered event isn't "newer" ⇒ no-op) and dedup (gossip/anti-entropy overlap is harmless).
- **DEL leaves a timestamped tombstone**; a SET older than the tombstone is suppressed (no resurrection). Tombstones GC on a retention window (§6.3).

**No wire change** — `created_at` already exists on `MEvent`; the store retains the winning `created_at` per key for comparison.

## 4. Where it lives

Federation is a **first-class, opt-in myko-server capability**, not a separate service. `Peer` and the pairing commands ship as **framework-provided entities/commands** (consistent with the built-in `Server` entity). It is delivered as:

- a new persister (`DataplanePersister`) plugged into the existing `PersisterRouter`, plus
- an **iroh endpoint** owned by the server (§5), carrying the gossip fast path and the anti-entropy/bootstrap protocols, plus
- the two ingest/LWW hooks that live in the unified `apply_effects` (§3).

### 4.1 Persistence is a per-node choice (node roles)

| Config | Role | Behavior |
|---|---|---|
| Postgres + Dataplane | **Durable node** | Durable locally **and** federated — the offline-resilient edge, or an always-on cloud node. Can serve anti-entropy and snapshot bootstrap to others. |
| Dataplane only | **Stateless / P2P node** | No local durable store and **no persistent state across restart**. A thin compute/cache participant; leans entirely on the mesh for durability and on durable peers for bootstrap (§8). |
| Postgres only | Today | Federation dormant; exactly current behavior. |

**Durability ⇐ local Postgres. Reach ⇐ dataplane.** A stateless node that is partitioned can serve from memory but loses unsynced state on restart; its writes are durable only once a durable peer has them (§8.2). This is an explicit expectation, not a limitation to fix.

## 5. Transport & identity (iroh)

The server owns one **iroh `Endpoint`** (single UDP socket, multiplexing all peer connections). Protocols are dispatched by ALPN via iroh's `Router`.

- **Identity = NodeId.** Each node has a persisted ed25519 keypair; its **NodeId is its `source_id`** — cryptographically verifiable, replacing the host-UUID. This is the only thing a stateless node persists (see §8.1).
- **Connectivity.** QUIC + TLS 1.3, NAT traversal / hole-punching, **relay fallback** when direct fails. Peers are dialed by NodeId; **no public IP or port-forwarding required**.
- **Crates.**
  - `iroh` (core) — endpoint, router, dialing. **Required.**
  - `iroh-gossip` — realtime dissemination fast path (§7.2). **Required.**
  - `iroh-blobs` — content-addressed snapshot transfer for stateless-node bootstrap (§8.3). **Required for stateless nodes.**
- **New dependency surface.** Pulls in the quinn/QUIC stack (quinn, rustls, ed25519, relay client) — currently zero QUIC deps in the repo. iroh 1.0 is wire-stable across minor/language versions.

> The existing `PeerRegistry` connection-management role is reframed onto the iroh endpoint; pairing (§9) supplies NodeIds instead of `wss://` URLs.

## 6. Conflict model: LWW

### 6.1 Resolution rule

For two writes to the same `(item_type, id)`, the winner is **`max` by `created_at`**, tiebroken by `(hash, source_id)` for equal timestamps. Deterministic ⇒ every node converges to the same value regardless of arrival order or path. Implemented once in `apply_effects` (§3.2).

### 6.2 Why this is sufficient here

Receivers apply replicated events as **inert state** — sagas are local-only (Prereq 0), so no node re-derives effects from a peer's event. State replication only needs per-entity convergence, which LWW provides. Per-origin ordering / sequencing is therefore **not** required, and no sequence number goes on the wire.

### 6.3 Tombstones

DEL leaves a **tombstone** (the DEL's `created_at`) retained in the store and the anti-entropy index, so a peer that still holds the entity learns it was *deleted*, not merely *absent* (else anti-entropy resurrects it). GC on a configurable retention window (default ~30 days); a node partitioned longer than the window may resurrect deleted data (accepted trade-off). Membership-aware GC is a future refinement.

### 6.4 Clock caveat

`created_at` is **wall-clock**. LWW correctness degrades under clock skew (the higher wall-clock wins, which may not be the truly-later write). Keep node clocks NTP-synced. The skew-proof alternative (a Hybrid Logical Clock) is a wire change and is explicitly out of scope for this pivot.

## 7. Data plane

### 7.1 Local write path

1. Command handler emits SET/DEL via `CommandContext` (`Origin::Local`).
2. Unified `apply_effects`: reduce (LWW guard) → search → relationship cascade → produce.
3. `produce` fans the event to configured persisters: local Postgres (durability) and `DataplanePersister` (federation).

No VV stamp; nothing added to the event beyond what `produce_*` already emits.

### 7.2 Realtime fast path — iroh-gossip (automatic forwarding)

`DataplanePersister` **broadcasts each new event to a gossip topic** (§7.4). iroh-gossip (HyParView + PlumTree) then **disseminates it across the whole swarm itself** — each node forwards to its eager peers and lazy-pushes digests to the rest, *inside the protocol, below the application*. **We do not manually forward to peers.** A receiving node simply gets each message once (gossip-deduplicated) on its subscription and runs it through `apply_effects` as `Origin::Remote`.

Consequences:
- **No manual fan-out loop** (the current `PeerPersister` pattern is retired).
- **`Origin::Remote` stays apply-only** — it must **not** re-broadcast (gossip already forwarded) and must **not** produce/cascade. This is exactly the unification doc's line-167 resolution.
- **Loop safety** = gossip's message-id dedup ∪ the `source_id == host_id` skip.

### 7.3 Anti-entropy backstop (authoritative convergence)

Gossip is best-effort; a node offline past the gossip cache window, or a permanently-missed update to an entity never touched again, won't be repaired by gossip alone. So paired durable nodes periodically (and on reconnect) run **anti-entropy** over their iroh connection:

1. Exchange per-`item_type` **Merkle roots**. The tree's leaves are keyed by `(item_type, id)` hashing `(id, content_hash, created_at)` — **`content_hash` is the existing `hash: Arc<str>` field**, so leaves are nearly free and **no wire change** is needed. Tombstones are present as leaves.
2. Equal roots ⇒ that type is in sync (cheap). Unequal ⇒ descend only differing branches to enumerate exact divergent keys.
3. For each divergent key, exchange entity + `created_at` and apply the **LWW** rule (§6.1). (No VV classification — just LWW.)

Anti-entropy is the **self-healing source of truth**: even if every gossip message were dropped, periodic anti-entropy converges the mesh. The gossip fast path is a latency optimization on top.

### 7.4 Topics & scope

A gossip **TopicId** per replication **scope** (per pairing scope / set of `item_type`s, see §9.2). Keeping each topic's swarm small bounds gossip hop-count in practice and prevents flooding unrelated events mesh-wide. Anti-entropy runs per-scope to match.

### 7.5 Apply-path (incoming events)

All inbound events — gossip fast path, anti-entropy repair, or DB tail — enter the **one** `apply_effects` path as `Origin::Remote`: LWW reduce, search + mutation-index update, **no cascade, no produce**, skip if `source_id == host_id`.

## 8. Node roles & durability

### 8.1 The durable tier — multiple replicas, no consensus

Durability is a per-node capability, not a privileged role — but a mesh needs **more than one** durable node. A single durable node is a single point of failure on four axes: **durability** (its disk is the only copy of history), **partition** (only its side can commit or bootstrap), **bootstrap** (it is the sole snapshot source, §8.5), and **operations** (no rolling restart without a durability gap). Under the strict-CP rule below this is even sharper: one durable node down freezes writes *mesh-wide*. So **run ≥2 durable nodes as a hard floor, placed one per operational domain** — for rship, a durable node at each edge site **plus** the always-on cloud node.

Multiple durable nodes are cheap because the model is **leaderless replication, not consensus**: each durable node independently persists everything it sees and converges with the others via the same gossip + anti-entropy + LWW path as any peer. There is **no leader election, no quorum-for-safety, and no split-brain hazard** — two durable nodes accepting writes across a partition reconcile deterministically via LWW on heal. So **"how many durable nodes" is purely a durability-SLA / replication-factor choice**, never a correctness constraint. Quorum enters only as an acknowledgment *preference* (§8.3), not a safety requirement.

### 8.2 The durable-node guarantee — deployment + strict-CP admission

You **cannot** topologically guarantee that every connected component contains a durable node — an arbitrary partition can isolate stateless-only nodes from all durable nodes. The guarantee is therefore enforced in two layers:

1. **Deployment policy (the real guarantee).** Place durable nodes so every operational domain that must keep operating contains one. Enforced at deploy time, not by the protocol.
2. **Strict-CP write admission (mandatory enforcement).** Each node advertises a `durable` capability flag in its peer metadata, and every node continuously evaluates from membership + liveness whether its component contains a **reachable durable node**. **A node admits a locally-originated write (`Origin::Local`) only if that check passes; otherwise the write is refused and the component is read-only** until a durable node is reachable. The check is **fail-closed**: when reachability is unknown (startup, membership flux, stale liveness), writes block.

This makes the system **CP on the durability axis — we never accept a write we cannot make durable.** Note the *deliberate* axis asymmetry: convergence is AP/eventual via LWW (including silent loss of one side of a concurrent edit, §6), while *write admission* is CP w.r.t. durable reachability. Different axes, intentionally different choices.

Implications:
- **A durable node is its own durable peer**, so it never self-blocks unless its local store fails. Only stateless nodes — and any fully durable-less component — get blocked.
- **Blocking applies to origination only.** Inbound replicated events (`Origin::Remote`, gossip or anti-entropy) **always** apply — convergence is never blocked, only new local writes.
- **A stateless node with no reachable durable peer is inert** — it can neither originate writes nor bootstrap (no snapshot source). Consistent with stateless nodes being dependent participants.

### 8.3 Commit semantics for locally-originated writes

Because admission already guarantees a reachable durable target, there is **no fire-and-forget mode**. Once admitted, a write commits per a configurable acknowledgment level:

- **wait-for-1-durable-ack (default / floor)** — committed once any durable peer acks persistence; survives the origin crashing.
- **wait-for-N-durable-acks** — survives losing up to N−1 durable nodes after commit.

On a stateless origin there is no crash-surviving outbox, so an *admitted-but-not-yet-acked* write lives only in RAM and is lost if the origin crashes before the ack. (Admission guarantees a durable target *exists*; the ack guarantees the write *arrived* there.)

### 8.4 Stateless node identity

A stateless (Dataplane-only) node persists **nothing across restart except its 32-byte iroh keypair** (so its NodeId / `source_id` is stable and it isn't re-paired as a stranger each boot). Everything else — its materialized store — is rebuilt on boot (§8.5).

> Alternative considered: fully-ephemeral identity (new keypair per boot). Rejected as the default because it churns the peer table and orphans `source_id`s; persisting 32 bytes is the pragmatic choice. Configurable if truly zero-disk is required.

### 8.5 Stateless bootstrap (cold start)

An empty-on-boot node does not "catch up" event-by-event — it **rebuilds its whole working set** (and per §8.2 it can only do so while a durable peer is reachable):

1. Pull a **materialized snapshot** of the in-scope state from a durable peer via **iroh-blobs** (content-addressed, resumable, deduplicated), at a known watermark.
2. **Subscribe to the gossip topic** and apply the live tail from that watermark forward.
3. **Readiness gate:** do not serve queries until snapshot + live tail are current, or the node answers from empty/stale state.

### 8.6 Saga placement

Deferred/long-running sagas (follow-on effects seconds–minutes after the command — these exist) must run on **durable** nodes, for two reasons: a stateless restart loses in-flight saga state with nothing to resume from, **and** under strict-CP a stateless node that loses durable reachability has its saga follow-on writes **refused** mid-chain. A durable node never self-blocks, so deferred chains complete there. Alternative: event-source saga progress into the durable log for replay. Decide in the plan.

## 9. Control plane — membership & pairing

### 9.1 Runtime pairing by NodeId, persisted locally

A node runs standalone. At runtime it is pointed at a peer's **NodeId** (via a command/API). The two nodes handshake over iroh (identity is the NodeId, cryptographically verified — no separate auth needed), and **each persists the relationship in its own local store** in a **local, non-synced `peer` table**. On restart, a durable node reads its peer table and auto-redials; a stateless node redials from its (re-supplied or configured) bootstrap peers.

This dissolves the bootstrap chicken-and-egg of "discover peers via the synced store": first contact is imperative (you hand it a NodeId), and thereafter a durable node self-seeds from its own local peer table.

### 9.2 Scoped pairings → topics

Each pairing declares **what it covers** — a scope over entity types (with "everything" as one setting). Each scope maps to a gossip **TopicId** (§7.4) and a per-scope anti-entropy session. Scope-expression mechanism (a set of `item_type`s on the `Peer` record and/or a `#[federated]` marker on entity types) is a plan-level detail.

### 9.3 Membership scope & transitive convergence

A node knows its **directly-paired** peers. Data converges transitively two ways: gossip forwards multi-hop automatically within a topic (§7.2), and pairwise anti-entropy repairs across the union of pairings (§7.3). No node needs global membership knowledge. The `Server` entity stays as synced *metadata about* peers (version, capabilities, last-seen), **not** the discovery mechanism.

## 10. Failure handling

| Scenario | Behavior |
|---|---|
| Internet drop (durable node) | Keeps operating fully (it is its own durable peer); writes accumulate durably in local Postgres. |
| Internet drop (stateless node, durable peer still reachable) | Keeps writing; admitted writes commit via durable-ack (§8.3). |
| Internet drop (stateless node, no durable peer reachable) | **Read-only** — writes refused (strict-CP admission, §8.2); serves stale local reads; converges on reconnect. |
| Partition isolates a durable-less component | Entire component is **read-only** (fail-closed); inbound applies still flow; rejoins + converges on heal. |
| Single durable node down (only one deployed) | **Mesh-wide write freeze** for stateless nodes until a durable node returns — why ≥2 is a floor (§8.1). |
| Reconnect | Gossip resumes; anti-entropy session repairs divergence via Merkle descent + LWW. |
| Process restart (durable) | Reloads from local Postgres; re-dials peers from local `peer` table. |
| Process restart (stateless) | Empty; re-bootstraps via snapshot (blobs) + gossip tail (§8.5), iff a durable peer is reachable. |
| Concurrent edit across partition | Resolved by LWW (`created_at`); **one side silently wins** (no conflict surfaced — accepted trade-off). |
| Long partition > tombstone window | Possible resurrection of deleted data (accepted; tune window). |
| Dropped gossip messages | Anti-entropy backstop converges anyway. |
| NAT / no public IP | iroh hole-punches; relay fallback if direct fails. |

## 11. Integration points (existing code)

- `libs/myko/server/src/peer_registry.rs` / `peer_connection_handle.rs` — connection management reframed onto the iroh endpoint; pairing + anti-entropy extend this.
- `libs/myko/server/src/peer_persister.rs` — **retired** as a manual fan-out; replaced by `DataplanePersister` broadcasting to a gossip topic.
- `libs/myko/core/src/server/persister.rs` — `PersisterRouter`; dataplane registers here. `BlackholePersister` still handles per-type ephemerality.
- `libs/myko/core/src/server/context.rs` — the unified `apply_effects` (Prereq 0) hosts the LWW guard (Prereq 1) and `Origin::Remote` ingest.
- `libs/myko/core/src/wire/event/mod.rs` (`MEvent`) — **no change** (LWW uses existing `created_at`; identity uses NodeId as `source_id`). Confirm the dead `EventOptions`/`from_peer` cleanup from Prereq 0 has landed.
- `libs/myko/core/src/search/index.rs` — pattern to mirror for the Merkle anti-entropy index (keyed on existing `hash`).
- `libs/myko/core/src/entities/server.rs` — `Server` metadata stays; add framework `Peer` entity alongside. (No `Conflict` entity — retired.)
- `libs/myko/macros` — possible `#[federated]` marker for scoping participating types.
- **New:** an iroh endpoint module (endpoint + router + ALPN protocols for gossip, anti-entropy, blobs/snapshot), and a stable keypair store.
- **New:** a membership/liveness + `durable`-capability subsystem feeding a cached "durable-reachable" flag, and a **write-admission gate** at command entry (before `Origin::Local` reaches `apply_effects`/produce) that fail-closes when the flag is unset (§8.2).

## 12. Testing strategy

- **LWW determinism** — same conflicting writes applied in different orders converge to identical state; tiebreaker is deterministic.
- **Tombstone correctness** — DEL then stale SET does not resurrect; tombstone GC respects the window.
- **Idempotency** — re-delivered event (gossip ∪ anti-entropy overlap) is a no-op, no spurious diff.
- **Gossip dissemination** — broadcast on one node reaches all topic members with **no application-level forwarding**; multi-hop (A→B→C, A and C not directly paired) still delivers.
- **Anti-entropy** — Merkle root equality iff identical sets; descent finds exactly the divergent keys; partition + reconnect converges with no gossip.
- **Two-node convergence** — real myko-servers (real entities + macros), partition + concurrent writes + reconnect; assert LWW-converged state.
- **Stateless bootstrap** — cold node rebuilds via snapshot + gossip tail; readiness gate holds reads until current.
- **Stateless durability** — write with wait-for-durable-ack survives origin crash; fire-and-forget does not (asserted as expected).
- **iroh connectivity** — NAT'd peers connect; relay fallback path exercised; NodeId pairing + auto-redial.
- **Saga locality** — sagas never fire on `Origin::Remote` events (structural via Prereq 0).
- **Strict-CP admission** — writes refused when no durable node is reachable (fail-closed on startup/flux); admitted + committed (durable-ack) when a durable peer is reachable; inbound `Origin::Remote` applies still flow while a component is read-only.
- **Durable-tier redundancy** — with ≥2 durable nodes, losing one retains full state on the others; loss tolerance matches the configured ack level; a durable node never self-blocks its own writes.

## 13. Build phasing (high level; detailed plan via writing-plans)

0. **Prereq 0 — Event Bus Unification** lands (`2026-06-17` doc). One `apply_effects`, `Origin`, dead-flag cleanup.
1. **Prereq 1 — Deterministic LWW + tombstones** in `apply_effects`. Convergence works locally; no wire change.
2. **iroh transport** — endpoint/router/ALPN, NodeId identity as `source_id`, NodeId pairing + local `peer` table + auto-redial. Two durable nodes connect over QUIC with NAT traversal.
3. **Gossip fast path** — `DataplanePersister` broadcasts to a per-scope topic; `Origin::Remote` apply-only ingest. Realtime replication with automatic forwarding.
4. **Anti-entropy** — Merkle index (on existing `hash`) + per-type roots + descent + reconnect session, repairing via LWW. Self-healing convergence.
5. **Durability tier + stateless node role** — `durable`-capability advertisement, membership/liveness + the cached durable-reachable flag, the **strict-CP write-admission gate** (fail-closed), durable-ack commit levels; then the Dataplane-only profile: iroh-blobs snapshot bootstrap + readiness gate, saga-placement rule. Multi-durable-node redundancy validated here.
6. **Pairing/control plane polish** — scoped pairings → topics, persistence-config matrix tests.

## 14. Deferred / future refinements

- **Hybrid conflict-honesty** — LWW default, **VV opt-in per entity type** (`#[federated(conflicts)]`-style) for types where silent loss is unacceptable, reintroducing concurrent detection + a `Conflict` entity only where needed. The LWW foundation here does not preclude it.
- **Hybrid Logical Clocks** — skew-proof ordering if wall-clock LWW proves insufficient (wire change).
- **Command federation/delegation** (sub-project #3) — separate spec.
- **Membership gossip** (peer-list exchange) → membership-aware tombstone GC + auto-healing around dead intermediaries.
- **iroh custom transports** (e.g. Bluetooth/Tor) for non-IP links.
