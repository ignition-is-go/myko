# 01 — Node Model

**Normative.** Source: spec §1, §2, §4. Invariant prefix `NM`.

---

## 1. Definitions

**Node** — one process holding an ed25519 keypair, a set of linked services, a set of served scopes,
and a set of role bits. There is exactly one process model; "server" and "client" are configurations
of it, not kinds.

**Service** — a set of entity types with their queries, reports, views, commands, and handlers,
identified by the crate namespace that defines them (02 §1). **Bound at compile time**: a node's
services are what its binary links.

**Scope** — a vertical multitenancy slice, identified by a scope-root entity id (05 §1). **Bound at
runtime**: a node's scopes are what its capabilities grant and its config selects.

**Data set** — a node's data set is `services it links × scopes it serves`. An entity is addressed by
`(service.Type, scope, id)`.

**Attached node** — a node whose sole peer is a Gateway (§6). It has a keypair but no routable mesh
address.

**Peer** — a node that dials and is dialled over the mesh transport (06 §1). Under the v1 scope, every
peer is a native Rust process.

---

## 2. Role bits

```rust
// myko-core::mesh::role

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Roles: u16 {
        /// Materializes state in memory, bounded by a filter per (service, scope) — §4.
        const STATEFUL = 1 << 0;
        /// Holds contiguous history over an advertised range — §5.
        const LOGGED   = 1 << 1;
        /// Executes some set of command_ids.
        const HANDLER  = 1 << 2;
        /// Originates commands and subscribes to results. Any node may hold this.
        const ORIGIN   = 1 << 3;
        /// Terminates a non-mesh transport (WSS) and bridges attached nodes — §6.
        const GATEWAY  = 1 << 4;
        /// Forwards mesh transport only.
        const RELAY    = 1 << 5;
    }
}
```

Roles are held **per `(service, scope)`**, not globally. A node may be `STATEFUL` for
`billing`/org-5 and hold no role at all for `identity`/org-12.

**`Gateway` and `Relay` are different mechanisms.** A relay forwards mesh traffic between two peers
that remain each other's counterparties. A gateway *terminates* a different transport and **is** the
attached node's counterparty.

### Reference configurations

| Configuration | Roles |
|---|---|
| Today's myko server | `STATEFUL(*) + LOGGED + HANDLER + ORIGIN`, `Durable(LOGGED)` |
| Today's myko client | `ORIGIN` |
| Browser editor (attached) | `STATEFUL(filter) + ORIGIN + LOGGED(own conflicts only)` |
| Archival appliance | `LOGGED + RELAY`, `Durable(LOGGED)`, no schema |
| Gateway | `GATEWAY + STATEFUL(*) + HANDLER` for its served scopes |

> **NM-1** — A node's role set is runtime-advertised state, not a compile-time type parameter. Roles
> may change at runtime; the manifest (§7) and the handshake (06 §4) both re-state them.

> **NM-2** — Nothing in the protocol privileges one node over another on the basis of role. Tiering
> (§3) is an emergent consequence of the eligibility rules in §4, not a caste.

## 3. Durability is a qualifier

`Durable` means **survives restart**, and qualifies each holding independently:

```rust
pub struct Holding {
    pub role: Roles,     // exactly one of STATEFUL | LOGGED
    pub durable: bool,
}
```

`Durable(STATEFUL)` and `Durable(LOGGED)` are separate facts. "State in memory, log on disk" is a
real configuration and is myko's today: the in-memory `StoreRegistry`
(`libs/myko/core/src/store/registry.rs`) is `STATEFUL`; Postgres persisting and replaying records
(`libs/myko/server/src/postgres.rs`) is `Durable(LOGGED)`.

### Being relied upon is a separate claim

A browser with IndexedDB genuinely *is* `Durable(STATEFUL)` over a narrow filter — and must never be a
mesh durability target, because the user clears the cache.

> **NM-3** — Persistence is a local fact. **Being a durability target is a claim made to peers**, held
> per scope, advertised in the manifest and asserted in the handshake. A node may be complete and
> durable and still decline the claim.

> **NM-4** — A node MUST be complete (§4) for a scope to advertise itself as a durability target for
> that scope. Completeness is necessary, not sufficient.

## 4. The materialization filter, and completeness

`STATEFUL` is RAM-bounded, so **every stateful node declares a filter per `(service, scope)`** bounding
what it materializes.

```rust
pub enum Filter {
    /// Everything in this (service, scope). The degenerate case.
    All,
    /// The union of live registered predicates — 08 §4.
    Predicates(PredicateSet),
}
```

There is one mechanism across the whole spectrum. `Filter::All` is not a different kind of node.

> **NM-5** — A node is **complete** for `(service, scope)` iff its filter there is `All`.
> Completeness is **derived from the filter**, never stored as a separate flag.

The derivation matters: a flag would have to be checked by every consumer before trusting a node for
anti-entropy or bootstrap, and forgetting the check would fail silently.

> **NM-6** — **Subsumption relates two peers, never a node to its own queries.** Node A may serve node
> B for a region iff A's filter subsumes B's. A node never checks subsumption against itself:
> evaluating a query *registers* its predicate and extends the filter (08 §4), so a local projection
> is never under-served.

### Eligibility

| Capability | Requires |
|---|---|
| Serve anti-entropy | **complete** over the compared range |
| Be a durability target | **complete** for the scope |
| Bootstrap another node | filter **subsumes** the target's |
| Execute a command **authoritatively** | **complete** for the scope (09 §2) |
| Run a command optimistically | nothing — always allowed, always provisional (09 §5) |
| Evaluate a projection locally | always possible; the question is cost, not coverage (08 §5) |

> **NM-7** — Anti-entropy requires completeness on **both** sides. Two nodes with different filters
> over one scope will never match Merkle roots, legitimately. Reconciling over a filter *intersection*
> would require query algebra and is not attempted. Filtered nodes converge instead by having their
> filter re-evaluated by a complete peer (08 §4).

> **NM-8** — A `Stateful` node's memory cost is **not predictable from its filter**. M1 resolved
> (2026-07-27) *against* predictability: the dominant cost is **derived/query cells**, measured at
> ~254 KB/item at rack scale, whose independent variable is live derived-cell count rather than
> entity count. Filter cardinality does not determine it. **No sizing decision may cite the filter
> mechanism as a bound.** See [`M1-findings.md`](M1-findings.md).

### Sharding (named, not designed)

If a scope exceeds one node's memory, nodes with **disjoint filters whose union is `All`** can cover it
between them. That brings genuine distributed-database problems — bootstrap assembles from several
nodes, durability becomes a property of the set, cross-filter queries scatter-gather — and is recorded
as an option, not a design.

## 5. `Logged` and contiguity

> **NM-9** — `LOGGED` requires **contiguity within each advertised range, anchored by a checkpoint or
> by inception**. Formally: for any queryable time *T* inside an advertised range, there is a
> checkpoint at or before *T* with unbroken log from it to *T*. A node holding disjoint runs
> advertises **separate ranges** or drops the claim.

A checkpoint makes its range **self-sufficient** — state rebuilds from it forward with nothing
earlier (07 §6).

> **NM-10** — Contiguity is defined against **admitted writes**, not wall-clock time. The checkable
> claim is: *the range holds every write acked by a history-durability target for the scope*
> (07 §7). Writes a scope's policy never required to be history-durable were never inside the
> guarantee.

Two mechanisms make NM-10 checkable:

1. **Durability acks** anchor the definition — the ack is the moment a write enters the guarantee.
2. **Log records carry a per-origin sequence** (03 §4). State convergence stays sequence-free; only the
   log layer sequences, so a single origin's stream is directly gap-detectable. This matters most for
   edge-owned streams (09 §7).

Consequences, all of which are behaviours rather than reminders:

- **In-gap historical reads fail structurally.** The routing layer finds no peer advertising coverage
  (08 §7). Correctness comes from routing, not from remembering to check a marker.
- **Backfill must extend contiguity, not create islands** (07 §5).
- **Audit gains a statable guarantee** — claiming `LOGGED` means history is complete for the range.

## 6. The Gateway role

**A `Gateway` exposes a WebSocket server so that nodes which decline the mesh transport can still be
nodes.** Everything past the gateway is mesh-native.

Its clientele — **lazy nodes**, any participant for which "open a WebSocket" is the whole transport
budget: browsers (which cannot hole-punch over QUIC), polyglot services that will not link
`iroh-ffi`, and scripts, devices, and plugins.

### Termination, not proxying

> **NM-11** — **The attached node's mesh relationship is with the gateway.** It subscribes to the
> gateway; the gateway serves from its own state and routes the attached node's commands onward. The
> gateway does not forward traffic on the attached node's behalf, and an attached node **never
> addresses another node**.

NM-11 is why an attached node needs no routable identity, and therefore no relay infrastructure. It
also follows from rules that already existed: browsers are spokes, filtered nodes do not execute
authoritatively (09 §2), and a filtered node's subscriptions are evaluated by a complete peer (08 §4).

There is exactly one exception, and it is narrow:

> **NM-12** — A gateway injects an attached **edge-owner's** records into the replication plane on the
> owner's behalf, preserving `origin = the attached node` (09 §7). This is the only case a node
> re-broadcasts a record it did not originate. Termination still holds: the gateway is the owner's
> on-ramp, not its proxy for reaching anyone.

### Requirements

> **NM-13** — `GATEWAY` implies `STATEFUL(All) + HANDLER` for every scope it serves. Both follow from
> existing rules — it evaluates its clients' subscriptions (needs completeness, NM-7 and 08 §4) and
> executes their commands authoritatively (needs completeness, 09 §2).

- **Run more than one, with client failover.** An attached node survives gateway loss because it is
  `Durable(Stateful)` locally and reconciles on reconnect (08 §8).
- **Capacity is `clients × their subscriptions`** — which lands on M1 (NM-8).

### Identity without addressing

An attached node keeps an ed25519 keypair. It needs one for capability presentation (05 §4), `actor`
attribution (03 §3), and outbox correlation (09 §5). It does **not** get a routable address, which
satisfies the transport contract's identity requirement (06 §1) without iroh's addressing layer.

## 7. The manifest

The manifest is the **gossiped discovery hint**: what a node reports about itself, third-party and
possibly stale.

```rust
// myko-core::mesh::manifest

pub struct NodeManifest {
    pub node_id: NodeId,                       // ed25519 public key
    pub generation: u64,                       // monotonic; bumped on any change
    pub services: Vec<ServiceEntry>,           // linked crates + versions — 02 §3
    pub scopes: Vec<ScopeEntry>,
    pub commands: Vec<CommandEntry>,
    pub entities: Vec<EntityEntry>,
}

pub struct ScopeEntry {
    pub scope_id: ScopeId,
    pub roles: Roles,
    pub durable: DurableFlags,                 // per-holding, §3
    pub complete: bool,                        // derived: filter == All — NM-5
    pub durability_target: DurabilityTarget,   // { state: bool, history: bool } — NM-3
    pub log_ranges: Vec<LogRange>,             // §5; horizon_actual only — NM-14
}

pub struct CommandEntry {
    pub command_id: QualifiedName,             // 02 §1
    pub args: Vec<FieldSchema>,                // 02 §4
    pub result_type: QualifiedName,
    pub description: Option<String>,
    pub consistency: ConsistencyDecl,          // 09 §6
    pub side_effecting: bool,                  // 09 §5
}

pub struct EntityEntry {
    pub entity_type: QualifiedName,
    pub fields: Vec<FieldSchema>,              // includes merge strategy per field — 04 §3
    pub edge_owned: bool,                      // 09 §7
}
```

> **NM-14** — `log_ranges` advertises `horizon_actual` — materialized **and indexed** — never
> `horizon_target`. A node backfilling from 30 to 180 days must not advertise 180 while sitting at 60.

> **NM-15** — The manifest is **derived, not authored**: built by walking `inventory` registrations at
> startup. Nothing in it is hand-maintained, so it cannot go stale relative to the compiled binary.
> The machinery already exists — `libs/myko/core/src/core/reflection.rs` and
> `core/command/registration.rs` capture operation arg schemas at macro-expansion time; 02 §4 adds the
> entity field schemas that are currently missing.

### Manifest versus handshake

They answer different questions and are separate on purpose. Conflating them would create two sources
of truth needing a tiebreak rule.

| | Says | Nature |
|---|---|---|
| **Gossiped manifest** | "node X reports it serves scope 5 and handles `CreateInvoice`" | third-party, possibly stale — a **discovery hint** telling you where to look |
| **Control handshake** (06 §4) | what this peer will accept, right now | first-party, immediate — a **binding contract**, enforceable against it |

Divergence is well-defined rather than tiebroken:

> **NM-16** — If the manifest claims more than the handshake offers, **the manifest was stale**:
> update it and look elsewhere. This is ordinary during membership churn and is not an error.

> **NM-17** — If a peer declares a plane in the handshake and then rejects a stream on it, that is a
> **protocol violation**: terminate the connection and mark the peer unreliable.

A transport may filter earlier as an optimization — the iroh binding maps planes to ALPNs so an
unsupported plane fails at connection setup (06 §3) — but the handshake remains authoritative.

## 8. Topology

The mesh is not flat. **A partial node structurally depends on a complete one** — something must
evaluate its subscriptions against the full set. Three independent constraints produce the same shape:

1. **Query-driven replication** (08 §4) — partial nodes need a complete node to evaluate against.
2. **Polyglot** — non-Rust nodes cannot join a gossip swarm; they gateway-attach.
3. **Browser transport** — no QUIC hole-punching; browsers are spoke-shaped.

So: **a peer mesh among complete nodes, with a spoke layer of partial and edge nodes**, attached
either natively (a filtered iroh peer) or through a gateway.

> **NM-18** — Tiers are **soft**. A node changes tier by changing roles at runtime. No protocol
> message, wire field, or routing decision may take "which tier is this" as an input; every such
> decision takes completeness (NM-5), role bits, and capabilities instead.

## 9. Node lifecycle

```
                    ┌─────────────┐
                    │   Created   │  keypair minted or loaded
                    └──────┬──────┘
                           │ provisioning: pair against the deployment
                           │ authority, receive scope grants — 05 §4
                    ┌──────▼──────┐
                    │  Enrolled   │  mesh.Node entity exists; grants held
                    └──────┬──────┘
                           │ build manifest from inventory — NM-15
                    ┌──────▼──────┐
              ┌────►│  Starting   │  dial peers, handshake, compare schemas
              │     └──────┬──────┘
              │            │ per scope: bootstrap (08 §7) or warm-start
              │            │ reconcile (08 §8)
              │     ┌──────▼──────┐
              │     │  Hydrating  │  per-scope readiness gate CLOSED:
              │     │  (per scope)│  reads block, writes REJECTED — NM-19
              │     └──────┬──────┘
              │            │ watermark caught up
              │     ┌──────▼──────┐
              │     │    Ready    │  serving; gate open for this scope
              │     │  (per scope)│
              │     └──────┬──────┘
              │            │ partition / restart
              │            ▼
              │     ┌─────────────┐   last reconcile older than the
              └─────┤   Stale     ├─► tombstone GC window? discard local
   reconcile  │     └─────────────┘   state, cold-bootstrap — 08 §8
   succeeds ──┘
```

> **NM-19** — **Readiness is per scope, and the gate blocks writes, not merely reads.** A node caught
> up on org 5 serves org 5 while org 12 still syncs. A stale read is temporary; a stale **write** is
> permanent loss — a node returning with T1 state whose user modifies a value it never saw change at
> T2 lands a T3 write that clobbers T2. Preconditions (04 §5) catch this; the gate prevents it
> arising.

> **NM-20** — Any node whose last successful reconcile for a scope is older than the tombstone GC
> window discards local state for that scope and cold-bootstraps — **restart or not**. A node
> partitioned past the window has the identical resurrection hazard without ever restarting: peers GC
> the tombstone, and heal-time anti-entropy reads the surviving copy as "peer is missing X" and pushes
> the deleted entity back.

---

## Invariant index

| ID | One line |
|---|---|
| NM-1 | Roles are runtime state, not compile-time types |
| NM-2 | No protocol privilege by role; tiering is emergent |
| NM-3 | Persistence is local; being a durability target is a claim |
| NM-4 | Durability target requires completeness (necessary, not sufficient) |
| NM-5 | Complete ⟺ filter is `All`; derived, never a flag |
| NM-6 | Subsumption relates peers, never a node to itself |
| NM-7 | Anti-entropy requires completeness on both sides |
| NM-8 | Filter does not bound memory until M1 resolves |
| NM-9 | `LOGGED` requires contiguity anchored by checkpoint or inception |
| NM-10 | Contiguity is defined against history-durability acks |
| NM-11 | Gateway terminates; attached nodes never address other nodes |
| NM-12 | Sole re-broadcast exception: gateway injects an attached edge-owner's records |
| NM-13 | `GATEWAY` implies `STATEFUL(All) + HANDLER` for served scopes |
| NM-14 | Advertise `horizon_actual`, never `horizon_target` |
| NM-15 | The manifest is derived from inventory, never authored |
| NM-16 | Manifest over-claim means stale manifest, not error |
| NM-17 | Declaring a plane then rejecting its stream is a protocol violation |
| NM-18 | Tiers are soft; no decision may take tier as an input |
| NM-19 | Readiness is per-scope and gates writes, not merely reads |
| NM-20 | Past the tombstone window, discard and cold-bootstrap — restart or not |
