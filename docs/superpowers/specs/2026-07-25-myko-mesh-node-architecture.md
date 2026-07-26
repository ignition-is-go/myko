# Myko Mesh — Node Architecture & Cross-Service Federation

**Date:** 2026-07-25 · **Revised:** 2026-07-26 after adversarial review — per-command consistency
modes (§11.5), edge-owned entities (§11.6), idempotency (§11.7), the management plane (§5.4), the v1
trust model (§5.5), hydration gates (§12.2), wire/merge fixes throughout, and the **v1 peer scope**
(§1.2): mesh peers are native processes; browsers and polyglot attach via the Gateway (§2.7), with
browser/polyglot peering kept as compatible extensions (§10.2, §10.3).
**Status:** Design. One open item (§17, M1 — resident-memory amplification) is unresolved and gates
the local-first story; the rest are measurements.

---

## 1. Goal and shape

Myko today has two kinds of process: a **server** (owns state, executes handlers) and a **client**
(owns a socket, executes nothing). This design replaces that binary with **one process model carrying
a set of role bits**, so a mesh can span many services and many tenants, route commands to whoever can
execute them, and make data durable wherever storage exists.

### 1.1 Two axes: service and scope

| | Is | Unit | Bound at |
|---|---|---|---|
| **Service** | a set of entity types with their queries, reports, views, commands, and handlers | code — a crate namespace (§3) | **compile time** — what a node links |
| **Scope** | a vertical multitenancy slice through a service | data — a scope-root entity (§5) | **runtime** — capabilities and config |

An entity is addressed by `(service.Type, scope, id)`. A node's data set is **services it links ×
scopes it serves**.

**Scope identity extends exactly as far as the scope-root entity is shared.** When a scope spans
services, those services must share the entity representing it — which means linking the crate that
defines it, and therefore (§3.1) getting the same qualified type and the same schema by construction.

So scope sharing needs no new mechanism: it is type sharing. Services linking a common
`identity.Organization` share the scope namespace, and a capability grant for org 5 spans all of them.
Services that declare their own scope roots have genuinely separate scope namespaces, and a grant in
one means nothing in the other.

### 1.2 The topology is tiered, softly

The mesh is not flat. **A partial node structurally depends on a complete one** — something must
evaluate its subscriptions against the full set. Three independent constraints produce the same shape:

1. **Query-driven replication** (§12.3) — partial nodes need a complete node to evaluate against.
2. **Polyglot** (§10.2) — non-Rust nodes cannot join a gossip swarm; they attach through a gateway
   (§2.7) or, exceptionally, hang off a Rust node as ALPN spokes.
3. **Browser transport** (§10.3) — no hole-punching, relay-only as a peer; browsers are spoke-shaped
   and gateway-attached by default (§2.7).

So: **a peer mesh among complete nodes, with a spoke layer of partial and edge nodes** — attached
either natively (a filtered iroh peer) or through a gateway (§2.7).

**v1 scope: mesh peers are native processes** — server-hosted and desktop applications. Browsers and
non-Rust services attach through gateways (§2.7). Nothing in the wire precludes a browser or polyglot
peer — §10.2 and §10.3 keep both as compatible extensions — but **no v1 machinery depends on one
existing**, which is what lets the cross-language hash burden collapse (§9.6) and lets the
recommended binding use iroh-blobs for bulk (§10.3).

The tiers are **soft** — an emergent consequence of role bits, not an architectural caste. A node
changes tier by changing roles at runtime; nothing in the protocol privileges one node over another.

### 1.3 The inversion

**A node's role set is a runtime-advertised property, not a compile-time one.** Myko's capability
system (`core/capability.rs`) grants handler capabilities via sealed trait impls decided at compile
time — excellent for scoping what a *handler* may do, and precisely the wrong axis for what a *node*
is. The two coexist: compile-time capabilities scope handler contexts; runtime role bits scope mesh
participation.

## 2. The node model

### 2.1 Role bits

| Role | Means | Backed by |
|---|---|---|
| **Stateful** | materializes state in memory, bounded by a **filter** per `service × scope` (§2.6) | linked item parsers (typed, §6) |
| **Logged** | holds **contiguous history — no gaps** — over an advertised range | log store (schema-free, §6) |
| **Handler** | executes some set of `command_id`s | linked command handlers |
| **Origin** | originates commands and subscribes to results | any node |
| **Gateway** | terminates a **non-mesh transport** (WSS) and bridges attached nodes into the mesh (§2.7) | a listening socket + `Stateful(*)` for the scopes it serves |
| **Relay** | forwards mesh transport only | iroh endpoint |

`Gateway` and `Relay` are different things: a relay forwards mesh traffic between peers, while a
gateway **terminates** a different transport and is the attached node's sole peer.

Today's server is `Stateful(*) + Logged + Durable(Logged) + Handler + Origin`; today's client is
`Origin` alone. A browser editor is `Stateful(filter) + Origin + Logged(own conflicts)`. An archival
appliance is `Logged + Durable(Logged) + Relay`.

### 2.2 Durability is a qualifier, not a role

**`Durable` means survives restart**, qualifying each *holding* independently: `Durable(Stateful)` and
`Durable(Logged)` are separate. "State in memory, log on disk" is a real configuration and it is
myko's today — the in-memory `StoreRegistry` is `Stateful`, Postgres persisting and replaying records
is `Durable(Logged)`.

### 2.3 Being relied upon is an advertisement, not a capability

A browser node with IndexedDB genuinely *is* `Durable(Stateful)` over a narrow filter — yet must never
be a mesh durability target, because the user clears the cache. **Persistence is a local fact; being a
durability target is a claim made to peers.**

Nodes advertise targets per scope in the manifest (§4.2) for discovery, and assert them in the
handshake (§4.3), which is where the claim becomes binding. Being complete for the scope is a
precondition (§2.6) — but not sufficient, since a node may be complete and still decline to be relied
upon.

### 2.4 Definitional details that carry weight

**Completeness is a property of the filter, not a separate kind of node** — see §2.6.

**`Logged` requires contiguity — a gap disqualifies the claim.** Precisely: contiguous within an
advertised range, **anchored by a checkpoint or by inception**. What is required is that for any
queryable *T* there is a checkpoint at or before *T* with unbroken log to *T*. A checkpoint makes its
range **self-sufficient** — state rebuilds from it forward with nothing earlier. A node holding
disjoint runs advertises separate ranges, or drops the claim. Consequences:

- **In-gap historical reads fail structurally** — the routing layer (§14.2) finds no peer advertising
  coverage. Correctness comes from the routing mechanism, not from remembering to check a marker.
- **Backfill must extend contiguity, not create islands** (§14.3).
- **Audit gains a statable guarantee:** claiming `Logged` means history is complete for the advertised
  range.

**Contiguity is defined against admitted writes, not against wall-clock time.** Records carry no
global sequence (§7.1), and an origin can vanish holding records it never shipped — so "no gaps"
cannot mean "every record any node ever produced," which is unverifiable. The claim that can be
checked is: **the range holds every write acked by a history-durability target for the scope**
(§15.4). Writes a scope's policy never required to be history-durable were never inside the
guarantee. Two mechanisms make it checkable: durability acks anchor the definition, and log records
(only — state convergence stays sequence-free) carry a **per-origin sequence**, so a single origin's
stream is gap-detectable directly — which matters most for edge-owned streams (§11.6).

### 2.5 Language qualifier

Every role is language-agnostic **except the realtime dissemination fast path**, which needs
iroh-gossip and therefore Rust, native or wasm (§10). Under the v1 scope (§1.2) the polyglot path is
gateway attachment (§2.7) — no iroh, no mesh plane. A polyglot node holding roles as a *peer*,
converging via anti-entropy plus pairwise replication, is the §10.2 extension.

### 2.6 The materialization filter

`Stateful` is RAM-bounded, so **every node declares a filter per `(service, scope)` bounding what it
materializes.** This is one mechanism across the whole spectrum — `filter = *` is simply the
degenerate case, not a different kind of node.

A browser editor filters to the handful of entities its screen projects. A reporting service filters
to what its reports read. A cloud node takes `*`. **A server that needs only part of a scope no longer
has to hold all of it.**

> **How much a filter actually saves is unquantified** (§17, M1). Measured resident memory runs
> 100–1000× the data at rest, so cost tracks *reactive structure* rather than entity count. If that
> amplification is per-subscription, narrowing the filter helps proportionally and this mechanism is
> essential; if it is fixed overhead, filtering saves less than it appears. **Resolve M1 before sizing
> anything against this.**

**Completeness is derived, not declared:** a node is *complete* for a `(service, scope)` when its
filter is `*` there. Deriving it rather than carrying a separate flag matters — a flag would have to be
checked by every consumer before trusting a node for anti-entropy or bootstrap, and forgetting would
fail silently.

**Subsumption relates peers, not a node to its own queries.** Node A can serve node B for a region iff
**A's filter subsumes B's**. A node never needs this check against itself — evaluating a query
*registers* its predicate and extends the filter (§12.3), so local projections are never
under-served. Complete nodes subsume everyone, which is *why* they form the mesh tier (§1.2) rather
than by stipulation.

Eligibility follows from completeness:

| Capability | Requires |
|---|---|
| Serve anti-entropy | **complete** over the compared range |
| Be a durability target | **complete** for the scope |
| Bootstrap another node | filter **subsumes** the target's |
| Execute a command **authoritatively** | **complete** for the scope (§11.1) — any node may run it optimistically (§11.3) |
| Evaluate a projection locally | always possible — evaluating registers the predicate (§12.2); the question is cost, not coverage |

**Anti-entropy requires completeness on both sides**, and for a concrete reason: two nodes with
different filters over one scope will never match Merkle roots — legitimately, exactly the failure
§5.3 identifies for scopes, one level down. Reconciling over a filter *intersection* would mean query
algebra, which is hard in general. Filtered nodes instead converge by having their filter re-evaluated
by a complete peer (§12.3).

**A sharding path out of M1.** If a scope exceeds one node's memory, nodes with disjoint filters whose
union is `*` can cover it between them — an alternative to a disk-backed store. That brings real
distributed-database problems (bootstrap assembles from several nodes, durability becomes a property
of the set, cross-filter queries scatter-gather), so it is named here as an option, not designed.

### 2.7 The Gateway role

**A `Gateway` exposes a WebSocket server so that nodes which decline the full mesh transport can still
be nodes.** Everything past the gateway is mesh-native.

This is the on-ramp for **lazy nodes** — any participant for which "open a WebSocket" is the entire
transport budget:

- **Browsers**, which cannot hole-punch over QUIC (§10.3) and gain nothing from mesh addressing.
- **Polyglot services** that would rather not link `iroh-ffi`.
- **Scripts, devices, plugins** — anything spoke-shaped.

#### Termination, not proxying

**The attached node's mesh relationship *is* with the gateway.** It subscribes to the gateway, and the
gateway serves from its own state and routes commands onward (§11.2). The gateway does not forward
traffic on the attached node's behalf or give it mesh-wide reach.

Everything already pointed here: browsers are spokes (§1.2), filtered nodes do not execute commands
(§11.1), and a filtered node's subscriptions are evaluated by a complete peer (§12.3). Under
termination an attached node **never addresses another node** — which is precisely why it needs no
routable identity, and therefore no relay infrastructure.

#### Requirements

- **`Gateway` implies `Stateful(*) + Handler`** for the scopes it serves. It evaluates its clients'
  subscriptions and executes their commands authoritatively, so both follow from existing rules rather
  than adding new ones.
- **Run more than one**, with client failover. An attached node survives a gateway loss because it is
  `Durable(Stateful)` locally and §15.7's warm start reconciles the staleness on reconnect.
- **Capacity is `clients × their subscriptions`**, which lands back on M1 (§17).

#### Identity without addressing

An attached node keeps an ed25519 keypair — needed for capabilities (§5.4), `actor` attribution
(§9.3), and outbox correlation (§11.3) — but no routable address. That satisfies §10.0's first
transport requirement without iroh's addressing layer.

#### What this resolves

**Polyglot, more than cost.** A spoke-shaped service in any language attaches over WSS and never
touches `iroh-ffi`. So the gossip-exposure question (§17, M3) stops being load-bearing: it applies
only to nodes that want to be mesh *peers*, and §10.2 already established polyglot nodes cannot fully
be that. **The gateway is the answer to the polyglot problem, not a workaround for it** — iroh is
required only for the peer mesh.

It also sharpens D1 (§13). An attached node is a full node in the **capability** sense — local state,
local projections, optimistic execution, outbox, conflict detection — but not in the
**addressability** sense. Both halves are real; only the second needs the mesh transport.

## 3. Type identity

`item_type` today is the bare struct identifier (`macros/src/item.rs:82`, emitted as
`ENTITY_NAME_STATIC` at `:969`), threaded into registrations, store keys, Merkle leaves, and routing.
Two services both defining `User` collide silently everywhere at once.

### 3.1 Qualify by defining crate

**Entity types and command ids are qualified by the crate that defines them** — `identity.User`.

The namespace is owned by the **defining crate**, never the consuming node or service. This makes type
sharing structural rather than conventional: if an org has a shared `identity` crate, every service
linking it emits `identity.User` **automatically** — same qualified name, same compiled type, same
schema by construction. Two services each defining their own `User` get distinct names automatically.
Collision-avoidance and type-sharing become one mechanism.

A node-level or service-level identifier would break this: two services sharing `identity` would stamp
`billing.User` and `crm.User` and fragment exactly the type they meant to share.

### 3.2 Why not canonical-names-by-convention

Convergence is per-field merge (§8) with no semantic merge across independently declared types. If
service A knows `User{id, name}` and service B knows `User{id, name, email}` — the commonest shape of a
shared type — writes from A **destroy B's `email`**. Sharing the defining crate makes divergent schemas
impossible by construction, which is why it is the mechanism rather than a convention.

### 3.3 Version skew and compatibility

Crate-qualification converts *collision* into *version skew*: `identity@1.2` and `identity@1.3` both
emit `identity.User` but may differ. Nodes compare schemas at pairing using **compatibility rules, not
equality** — additive-optional fields compatible; removal and type changes not. Exact-match-or-reject
would tear down replication during every rolling deploy, since skew mid-rollout is normal.

**Crate version travels in the manifest, not the type key** — in the key it would fragment every type
on every release; in the manifest it lets a rejected pairing report *why*.

**Skewed nodes must retain unknown fields.** When `identity@1.2` receives a record carrying a field
added in `1.3`, it stores the entry opaquely — merged by HLC, never interpreted, **included in the
content hash**. Dropping it would make the node's hash permanently disagree with every `1.3` peer's
for the same entity, converting benign skew into endless anti-entropy repair churn during exactly the
rolling deploys these compatibility rules exist to survive. Retention is cheap: below the schema
layer, field entries are already opaque bytes (§9.5).

### 3.4 Mechanics

- **Take the first segment of `module_path!()`.** The field named `crate_name` actually holds
  `module_path!()` (`macros/src/item.rs:777`, and identically in `command.rs:139`, `query.rs:74`,
  `view.rs:138`, `report.rs:68`) — a full module path, not a crate name. Codegen compensates by
  substring-matching (`.contains(&crate_name)`, `codegen/mod.rs:158`). Namespacing on the raw value
  would make **moving a struct between modules inside its own crate a wire-breaking change**.
- **Provide a namespace override** — `#[myko_item(namespace = "identity")]` — so a crate rename or
  cross-crate move is a refactor rather than a forced migration.
- **The qualified name is a wire/registry key, not an identifier.** It keys the record header, store
  entries, Merkle leaves, and routing; the unqualified struct name stays the Rust/TS identifier, and
  generated names (`GetAllTargets`, `TargetQuery`) stay unqualified.
- **Migration:** a bare `item_type` belongs to a default namespace.

## 4. Discovery and the manifest

### 4.1 Most of this already exists in the binary

`core/reflection.rs` captures — **at macro-expansion time, embedded in the binary** — field names,
Rust types, and optionality for every query/view/report/command argument struct (`OperationArgField`).
`CommandRegistration` adds `command_id`, `result_type`, `crate_name`, and the doc comment. This
already backs the MCP `search()` index.

Its module doc is explicit that it was designed to be independent of codegen and to **never go stale
relative to the compiled binary** — exactly what a gossiped manifest needs.

**The one gap: entity field schemas.** `ItemRegistration` (`core/item/traits.rs:79`) captures
`entity_type`, `crate_name`, and function pointers — but no field schema. **Required change:** add an
`args`-equivalent field list, mirroring `#[myko_command]`. Mechanical and additive; the macro has the
field list at expansion time. This also supplies the per-field merge metadata of §8.2.

### 4.2 The manifest

- `node_id` — iroh NodeId
- `roles` — §2.1 bits, plus `Durable(...)` qualifiers, **per `(service, scope)`**
- `services` — linked services and crate versions (§3.3)
- `commands` — `command_id` → arg schema, result type, description
- `entities` — `entity_type` → field schema + merge strategy per field (§8.3)
- `scopes` — scope ids served
- `durability_targets` — scopes for which this node advertises itself as relied-upon (§2.3)
- `log_ranges` — contiguous ranges covered (§2.4), advertising `horizon_actual` — materialized *and
  indexed* — never `horizon_target` (§14.3)

The manifest is **derived, not authored**: built by walking inventory at startup.

### 4.3 Discovery hint versus binding contract

The manifest and the handshake answer different questions, and conflating them would create two
sources of truth needing a tiebreak rule. They are separate on purpose:

| | Says | Nature |
|---|---|---|
| **Gossiped manifest** | "node X reports it serves scope 5 and handles `CreateInvoice`" | third-party, possibly stale — a **discovery hint** telling you where to look |
| **Control handshake** | what this peer will actually accept, right now | first-party, immediate — a **binding contract**, enforceable against it |

**The handshake opens with a plane declaration**: each side states the planes it serves (§10.5.3).
Opening a stream on an undeclared plane is a protocol error. The handshake also carries capability
presentation (§5.4) and schema comparison (§3.3).

Divergence is well-defined rather than a tiebreak:

- **Manifest claims more than the handshake offers** — the manifest was stale. Update it and look
  elsewhere. Ordinary, expected during membership churn.
- **A peer declares a plane and then rejects a stream on it** — protocol violation. Terminate the
  connection and mark the peer unreliable.

A transport may filter earlier as an optimization — the iroh binding maps planes to ALPNs so an
unsupported plane fails at connection setup (§10.5.1) — but the handshake remains authoritative.

## 5. Scopes and multi-tenancy

A crate declares a **scope root** — `Organization` — and every entity belongs to exactly one scope. A
mesh hosts many organizations, each node opted in to a subset.

**A scope crossing services requires those services to share the scope-root entity**, i.e. to link the
crate defining it (§1.1). This is enforced by the same mechanism as any other shared type, so there is
nothing extra to build — but it does mean *scope-root crates are the most load-bearing shared crates
in a deployment*, and their versioning discipline (§3.3) matters more than most.

### 5.1 Scope lives in the record header

**The scope id is in the record header, not the entity body.** A `Logged` node that cannot parse
`billing.Invoice` must still decide whether the record belongs to a scope it serves; if scope were in
the body, schema-free storage and scope partitioning would be mutually exclusive.

Scope may be *logically* derived from a relationship — `#[scoped_by(Organization)]` fits the existing
`#[belongs_to]` family — but it is **denormalized onto the header at emit time**.

**Entity→scope binding is immutable.** Moving between scopes would be a cross-partition transaction
with no atomicity across nodes serving one side. Changing org is delete + recreate. This costs little
because the mutable half lives elsewhere (§5.4).

### 5.2 References

- **Cross-scope references are forbidden.** An entity in org 1 referencing org 2 is a **tenancy
  violation**, not a partitioning inconvenience. Enforced in the relationship macros.
- **Cross-service references within a scope are normal** — billing's `Invoice` referencing identity's
  `User`, both in org 5. This is how services compose, and it resolves through §3.1: link `identity`,
  subscribe to its types, resolve locally. Don't link it and you hold an opaque id — explicit and fine.

### 5.3 Anti-entropy must be scope-aware

Per-`item_type` Merkle roots are **actively wrong** under partitioning: nodes serving orgs {1,2} and
{2,3} never match roots, legitimately. Naive anti-entropy reads that as divergence, "repairs" it by
exchanging everything, and pushes org-1 data onto a node not authorized to hold it.

- **Merkle trees are keyed per `(item_type, scope)`.**
- **Sessions negotiate the scope intersection first**, reconciling only shared scopes.
- **A node never accepts repair data for a scope it does not serve** — fail safe.

### 5.4 Access is granted by signed capability

Entity→scope binding is immutable; **identity→scope access is mutable**. Nothing in the data plane
moves when access changes — only what a node may replicate.

Putting the mapping in the replicated data plane fails both ways: in a global scope every node learns
every org's membership; inside the scope it grants, you need the grant to replicate the scope
containing it.

**iroh supplies the primitive.** Node identity is an ed25519 keypair, so a grant is a **signed
capability**: *"the authority for scope 5 asserts NodeId X may replicate scope 5, until T."* Presented
during the control-plane handshake (§10.5.1) and verified by signature. No replicated ACL, no
bootstrap paradox.

**The authority is the management plane, and the framework provides it.** Nodes and grants are
themselves myko entities — `mesh.Node`, `mesh.ScopeGrant` — in a management scope, mutated by
framework commands: `GrantScopeAccess { node, scope, ttl }`, `RevokeScopeAccess`, node enrollment and
retirement. The handler for that scope holds the **deployment authority keypair** and emits the
signed capability as the command's effect — issuance, node inventory, and rotation are ordinary
command handling rather than a parallel PKI. Bootstrap is the one genuinely new mechanism: the first
node mints the deployment keypair; every later node is provisioned by pairing against it.

**Revocation is "stop renewing."** Short TTLs avoid distributed revocation lists. This tensions
against offline operation — a partitioned node cannot renew — so **TTL is configurable per scope**,
since sensitivity and offline requirements vary together.

### 5.5 Partitioning is stronger than RLS

**A node cannot leak what it never received.** Physical absence beats read-time filtering, and buys
data locality, blast-radius reduction, and jurisdictional compliance. This is what makes local-first
projection safe: **the replication boundary is the authorization boundary**. Myko has no
authorization model today — verified, not assumed — and this keeps that from becoming permanent.

Two qualifications:

- **Asymmetric.** *Never-granted* is strong. *Revoked* is weak — the node had the data, and revocation
  cannot retract what was delivered to a node you do not control. **Grant conservatively.**
- **Complete only on single-identity nodes.** A node's scope set is the **union of identities it
  serves**. A shared server holding org 5 for user A holds it while serving user B, who lacks access.
  **Shared nodes need identity-level filtering on top.**

**The v1 trust model is explicit: nodes within a deployment are operator-trusted.** Capabilities
govern *what replicates where* — tenancy, locality, blast radius — not defence against a malicious
node. `actor` (§14.4) is asserted by the origin and trusted; the direct-write rule (§11.6) is a
protocol rule, not an enforced barrier. An open mesh spanning parties that do not trust each other
would need record-level write authorization and attestable attribution on top — the same boundary §6
already draws for schema, restated for security.

### 5.6 Eviction

On losing access a node purges that scope from its store and log.

> **Local eviction must never emit DEL records.** The entities exist for every other node; routing
> eviction through the deletion path would replicate tombstones mesh-wide and **destroy the
> organization's data everywhere**. Eviction is a store-level purge producing no records (§7.4).

Anti-entropy handles the aftermath for free: sessions negotiate scope intersection first and the node
no longer claims that scope.

### 5.7 Consequences

- **Write admission is per-scope** — "is a durability target reachable *for this scope*?"
- **Routing keys on scope** — `(command_id, scope) → nodes`.
- **Gossip topics per scope**, so partitioning falls out of topic membership (scaling: §17, M2).
- **A global scope** holds framework entities and shared reference data — **not scope-root records**.
  Replicating the org roster to every node would leak the tenant list §5.4 keeps out of the data
  plane; a scope root lives in the scope it defines, and nodes discover scopes through grants, not
  enumeration.

## 6. Durability and schema

Today an unknown `item_type` is silently dropped: `parse_item` returns `None`
(`server/context.rs:444`) and every ingest path skips it (`postgres.rs:218`).

| Role | Schema | Rationale |
|---|---|---|
| **`Stateful`** | **required, per service** | CRDT merge, cascades, indexing, and query evaluation are all type-specific (§9.5) |
| **`Logged`** | **not required** | index keys on `(scope, item_type, id)`, all header fields; outside convergence (§7); serves raw records for the requester to parse |

A generic, schema-free **archival appliance remains possible**; a generic *state* store does not.

**A useful consequence: a `Logged` node can retain history for types nothing currently materializes.**
Because it never parses bodies, it accumulates records for services no node in the mesh links today —
so when such a service is deployed, its history is already there rather than starting empty. A
`Stateful` node cannot do this; it can only hold what it links.

**Why state nodes must know the schema.** Not for LWW merge — §9.5's field-addressed records make
that schema-free, since merging is a merge-join on `field_id` comparing timestamps and copying value
bytes without interpreting them. The schema is required for everything *else* a state node does:
**CRDT merge** (counter increment, set union, sequence merge — all type-specific), **relationship
cascades**, **index maintenance**, and **query evaluation**. A node lacking the schema could relay and
LWW-merge but could not serve a single query, which is the whole point of holding state.

**On performance.** Parse-free is faster only for pure store-and-forward. Once a node merges, queries,
indexes, or cascades, parse-free *defers* the cost and pays at read time repeatedly rather than write
time once. For `Logged`, parse-free is genuinely optimal: header-only indexing, bodies returned
verbatim.

**Cost.** A new service cannot obtain *state* durability from nodes predating it; those nodes need
redeploying with the new crates. For within-org federation that is a redeploy you would do anyway.
**For an open federated mesh spanning parties you do not control, this is the decision to revisit.**

## 7. State and log

### 7.1 Myko is not event-sourced

- **Convergence compares stored state, not log position** — a last-writer-wins register per field
  (§8). Today's apply path consults no timestamp at all — `store.insert` is arrival-order overwrite
  (`context.rs:912`) — so current behavior is *weaker* than even whole-entity LWW.
- **Merkle leaves hash `(id, content_hash, timestamp)`** — current state, not history. Greenfield: no
  Merkle machinery exists today, and the in-process `ContentHash` memoizer (cache reset on `Clone`,
  no cross-process stability) is not a starting point for §9.5's hash.
- **A write carries field values, not domain intent** — state transfer, not "the user changed their
  email."
- **Nothing carries a sequence number.** Per-origin ordering is not required for convergence and is
  not provided.

The accurate description: **a replicated key-value store with per-field merge, a local event bus for
sagas, and a durable changelog.**

The mismatch implies guarantees the system does not provide (silently), conflates two retention
policies that must differ (§14.1), and invites applying LWW to a log where it is meaningless.

### 7.2 State is primary; the log is derived

> **State is primary. The log is a durable, append-only artifact produced by state changes.**

- The **typed state store is primary** — a KV store with per-field merge and tombstones.
- The **log is separate**, with its own retention, compaction, and archival path. Append-only and
  per-origin, so it needs **no** merge and no anti-entropy — shipping it is bulk archival.
- **`MEvent` is renamed** (§9) — it is a state-change record.

### 7.3 What is kept

**Audit trail**, **point-in-time replay**, **recovery**, and **sagas** — which react to a *local*
mutation stream, a sound pattern distinct from distributed-log consumption. Causal ordering and
per-origin sequencing are not provided, and nothing here needs them: per-field merge (§8) converges
without them.

### 7.4 Local operations that must not propagate

Five operations share one hazard shape: a node modifies its own storage, and routing that through the
replication path would corrupt the mesh.

| Operation | Would otherwise |
|---|---|
| Scope eviction (§5.6) | delete an org's data everywhere |
| Log truncation (§14.3) | destroy history everywhere |
| Restore batch (§14.3) | cascade beyond the computed closure |
| Log checkpoint (§15.4) | replicate as a mesh-wide write |
| Conflict record (§8.5) | replicate a node-local observation |

**"Local operation that emits nothing" is a first-class, named concept in the store API**, not five
independently-remembered special cases.

### 7.5 Entities are source; views and reports are derived

**Entities never derive from one another.** An entity's state is source data, changed only by an
explicit write. Derivation lives one layer up: queries select, views project, reports aggregate — all
functions *of* entities, never inputs *to* them.

This keeps the layering total, and it is worth stating because relationship attributes look like an
exception and are not:

| Attribute | Is |
|---|---|
| `#[belongs_to(Scene)]` | a reactive rule that **emits real DELs** for children when a parent is deleted |
| `#[owns_many(BindingNode)]` | the same, plus a real write updating the parent when a child is deleted |
| `#[ensure_for(Project)]` | a reactive rule that **creates a real entity** per dependency |

Each produces ordinary source data — records with their own HLC and actor, replicated like any other,
and independently mutable afterwards. None makes one entity's state a function of another's.

The alternative — treating a child's tombstone as *derived* from its parent's — would place entities
in both layers at once, and an explicit SET on a derived-tombstoned child would have no defined
meaning. Relationship effects are therefore **rules that write**, and being writes, they follow §11.1:
they run where the state they read is complete.

## 8. Conflict resolution

### 8.1 Why whole-entity LWW fails

Today's record carries the entire entity with a single timestamp. Two users editing different fields —
A changes `name`, B changes `description` — each emit a full-entity SET, one wins, and the other is
**silently discarded**. The loss is an artifact of granularity, not a real disagreement: nothing about
those two edits actually conflicts.

Today this is masked: writes funnel through one server, commands serialize, handlers read-modify-write
against uncontended state. **This design removes every one of those protections.** Concurrent edits
become the ordinary case, degrading in proportion to how well the mesh succeeds.

### 8.2 Per-field merge

**Each field carries its own timestamp.** Concurrent edits to `name` and `description` both survive as
independent registers. The macro generates this from §4.1's field schemas — generated, not
hand-written merge code.

**Ruled out:** version vectors (an entry per writer, and every client is a writer — unbounded growth);
entity-home routing (an offline node could not write at all); consensus (kills partition tolerance).

### 8.3 Same-field conflicts: coherent only for structured fields

LWW loses information because `SET x = v` discards **intent**:

| Field shape | Concurrent merge | Mechanism |
|---|---|---|
| Opaque scalar (`name: String`) | **no coherent merge exists** | LWW is *correct*, not lossy |
| Counter (`error_count`) | both intents are "+1" | PN-Counter |
| Set (`collaborators`) | both additions must stick | OR-Set |
| Collaborative text | different paragraphs both survive | sequence CRDT — **deferred**, see below |
| Nested map (`metadata`) | different keys both survive | recursed LWW-Map |

Per-field LWW is therefore **nearly complete** — most fields are scalars where it is right. The gap is
three shapes, all identifiable from the **declared field type**, so the macro selects the strategy
automatically.

> **Sets deserve priority.** `{Alice}` plus concurrent adds of Bob and Carol resolves under
> whole-entity LWW to `{Alice, Bob}` *or* `{Alice, Carol}` — a concurrent add **silently revokes** the
> other person. On a permissions list that is a security-adjacent correctness bug.

**Every strategy is state-based; delivery forces it.** The mesh delivers at-least-once with
duplication (gossip, anti-entropy, and pairwise streams overlap), and §7.3 provides no causal
ordering — an op-based "+1" delivered twice double-counts, so op-based CRDTs are ruled out wholesale.
State-based counters and sets carry per-actor entries, which is the unbounded-writer growth §8.2
rejects in version vectors — so **the actor set is bounded to durable nodes**: edge nodes mutate
counters and sets via commands (§11.1), never direct writes, and entries grow with mesh size, not
client count. Sequence CRDTs do not fit this shape at all — op-based, causally dependent, unable to
ride an opaque-value merge-join — so **collaborative text is out of scope** here and gets its own
delta protocol when it lands.

### 8.4 Optimistic concurrency

Per-field merge fixes **structural** conflicts. It does nothing for **semantic** ones: a handler reads
`seat.occupied == false`, writes `occupied = true`; two run concurrently on load-balanced nodes, both
read `false`, both write, merge picks one — and the loser's client believes it got the seat.

**OCC: reject the write at execution time unless the field's version still matches the one read.**
Four independent motivations — restore (§14.3), stale warm-start (§15.7), offline replay (§8.5), and
load-balanced read-modify-write (§11.5).

**Mechanism: runtime read-set tracking in `CommandContext`.** It is already the single funnel for both
sides — reads via `exec_query_first` / `exec_query` / `exec_report`, writes via `emit_set` /
`emit_del`. The context records which entity fields a handler observed; on emit, the framework
**automatically attaches preconditions for fields the handler both read and wrote**.

> **Rule:** precondition each written field on the version observed at read time, if that field was
> read. A field written without being read is a blind write and gets no precondition — correct, since
> blind writes are intentional.

No declaration, no macro static analysis (which cannot see the read set anyway), no per-handler
discipline. A rejected write returns a clean error the handler can retry. An explicit opt-out exists
for high-throughput blind-write paths. Reactive `query_map` subscriptions are not part of a command's
read set — only snapshot reads inside commands.

**Preconditions are checked where the command executes — never at apply time.** An apply-time
precondition is order-dependent: replica C receiving A's write then B's keeps A, replica D receiving
B's then A's keeps B, and the mesh diverges permanently. So the emitted record replicates
unconditionally and merges as pure LWW; `precondition_hlc` (§9.3) travels for audit and conflict
inspection, not as a receiver-side gate. The guarantee is exactly as strong as the executing node's
view — which is what §11.5's per-command consistency modes exist to strengthen.

### 8.5 Conflicts are recorded, not replicated

Two situations produce genuine divergence — and one that resembles divergence is not:

- **Partition heal.** Both sides committed successfully with their own durability; detection happens
  **during anti-entropy repair**, when an incoming value beats a *locally originated* one.
- **Owned-entity replay** (§11.6). Edge-owned entities are the one case where offline replay ships
  *records* rather than commands; the outbox comparison against merged state survives exactly there.
  The owner is the single writer, so a replay loss is never an ordinary race — it means an ownership
  violation or an administrative write (restore, §14) touched an owned entity. Rare, and recorded
  loudly.
- **Command replay is not conflict.** The general outbox holds commands (§11.3, §15.6) and replays by
  re-execution; a result differing from the prediction is a **rebase** — the prediction never
  committed anywhere and must not be logged as data loss. Rebases surface as a local UX event ("your
  change was adjusted"), not a conflict record.

Anti-entropy detection and replay detection do not generalize to each other; both mechanisms are
needed.

**Where records go:**

| What | Where | Bounded by |
|---|---|---|
| Per-conflict detail — losing value, winner, timestamps, actors | **local log**, unreplicated (§7.4) | number of conflicts |
| Per-heal summary — window, counts, which nodes hold detail | **replicated** | number of heals |

Making the replicated unit the *heal* rather than the *conflict* gives discoverability without
flooding the mesh on a large heal. The summary is small and append-only — an OR-Set, a shape §8.3
already handles.

**Resolution needs no new mechanism.** The log is a reflog: inspect what was discarded, and restore it
as a forward write (§14.1).

Consequence: **browser nodes want a narrow `Logged` role** — just their own conflicts, not full
history — so `Logged` is not purely a server-tier role.

## 9. The wire

The state-change record is redesigned. Current shape (`wire/event/mod.rs:18`):

```rust
pub struct MEvent {
    pub item: Value,                 // fully-parsed JSON tree
    pub change_type: MEventType,
    pub item_type: Arc<str>,
    pub created_at: Arc<str>,        // RFC3339 string
    pub tx: Arc<str>,                // UUID string
    pub source_id: Option<Arc<str>>,
}
```

### 9.1 What the encoding has to do

Five requirements, all falling out of decisions made elsewhere:

1. **Header readable without a decoder library.** A `Logged` node indexes on `(scope, item_type, id)`
   (§6) and must never parse the body.
2. **Body opaque and skippable** for nodes without the schema.
3. **Per-field metadata** — an HLC per field (§8.2), an optional OCC precondition (§8.4), and merge
   strategy for structured types (§8.3).
4. **Deterministic content hash**, since Merkle leaves hash it (§5.3) — required to agree across every
   implementation that **serves anti-entropy**, i.e. complete peers. Attached nodes (§2.7) never
   compute it: filtered nodes converge by re-evaluation, not comparison (§2.6). Under the v1 scope
   (§1.2) that makes hash agreement a single-implementation concern (§9.6).
5. **Implementable in Rust, TypeScript, Python, Swift, Kotlin, C, C++, C#** (§10.1) — because attached
   nodes decode records into local stores and encode their own writes (§2.7), the record format is
   polyglot even where the mesh planes are not.

Requirement 3 is the one that reshapes everything. Once every field carries its own timestamp and
possibly a precondition, **the record is already field-granular** — and shipping whole entities on top
of field-granular metadata is pure waste.

### 9.2 Three layers

| Layer | Encoding | Who must implement it |
|---|---|---|
| **Header** | fixed field order, length-prefixed — no library | every node |
| **Field entries** | varint-framed, custom | every node that merges or stores |
| **Field values** | canonical CBOR | only nodes with the schema |

**A `Logged` node needs no CBOR library at all** — it reads the header with a trivial cursor (fixed
field order, length prefixes, fixed-width integers), skips the field section by length, and stores
the bytes. True fixed offsets would require fixed-width ids, and entity ids today are arbitrary
strings — if ids later become fixed-width, the header tightens in a version bump. Either way the
archival appliance role, and the peer minimum, stay dramatically cheap to implement.

### 9.3 The record

**Header** — fixed layout:

- `version`, `record_type` (set / delete / checkpoint)
- `scope_id` (§5.1)
- `type_id` — the qualified type (§3), interned **per connection**: the handshake exchanges the
  qualified-name ↔ id mapping, control frames extend it mid-stream, and intern ids never outlive the
  connection. **The log stores the qualified name (or its stable hash), never an intern id** — an
  archive must be readable without the connection that wrote it
- `entity_id`
- `origin` — NodeId, non-optional; a record always has an origin
- `actor` — the identity responsible (§14.4), distinct from origin (a node) and connection
- `record_hlc` — the record's own timestamp

**Field section** — a sequence of entries, each:

- `field_id` (u32)
- `hlc` — this field's own timestamp, fixed width
- `flags` — tombstone, has-precondition, merge strategy
- optional `precondition_hlc` (§8.4)
- `len` (varint) + `value` bytes, canonical CBOR

**Hybrid Logical Clocks** replace `created_at` throughout. Wall-clock resolution degrades under skew,
making convergence depend on NTP discipline across every node. (Precision about today: nothing
compares `created_at` at all — the apply path is arrival-order overwrite, §7.1 — and where a
comparison *would* be added, lexicographic RFC3339 is fragile: format drift (`Z` vs `+00:00`) or
trailing-zero variance changes both ordering and equality, costing ~30 bytes and a parse where a
fixed-width integer compare would do.)

**Total order:** ties break on `(hlc, origin NodeId)` — deterministic everywhere, and part of the
conformance surface (§9.6); `feat/iroh-dataplane` independently arrived at the same `(ts, source_id)`
shape. **Drift bound:** a node rejects records whose HLC physical component leads its own clock past
a configured bound, so one broken clock cannot win every LWW race for hours.

**Delete is a register too.** `record_type = delete` is the entity-level tombstone with its own HLC,
competing with SETs under the same total order — a later SET beats an earlier DEL (recreate; the
delta-for-unknown-entity fallback of §9.5 covers partial recreates), and a later DEL beats earlier
SETs. Per-field tombstone flags mark *field* removal within a live entity and do not interact with
entity deletion. One rule, no special cases.

> **Scope limit.** HLC fixes *causally-related* writes misordered by clock skew. Genuinely concurrent
> writes still resolve by arbitrary tiebreak — HLC is no substitute for §8.

### 9.4 Field ids are name hashes

`field_id` is a 32-bit hash of the field name, **collision-checked at macro-expansion time** within
each type.

This avoids protobuf-style manual numbering and its bookkeeping — no registry, no "never reuse a
number" discipline, and ids stay stable when fields are reordered. A rename is a new field, which is
correct: renaming a field *is* a schema change. It must not be a *silent* one, though — without an
affordance, every stored value is orphaned under the old id. `#[myko_field(renamed_from = "old")]`
ships with the feature; whether it reads through the old id or migrates forward on next write is an
implementation decision, but the bare footgun is not acceptable.

### 9.5 What field-addressing buys

**Writes carry only changed fields.** A one-field edit to a fifty-field entity sends one field. This
removes the whole-entity clobber at the wire level, not merely at the merge level. A creation sends
every field, so full-state transfer is just the degenerate case.

**LWW merge needs no schema.** Merging is a merge-join on `field_id` comparing HLCs and copying value
bytes — the receiver never interprets a value. *CRDT* merge (counters, OR-Sets, sequences) still needs
the schema, which is why state nodes remain schema-scoped (§6).

**OCC lands naturally**, since preconditions were already per-field.

**Content hash is well-defined:** hash over field entries in `field_id` order with canonical CBOR
values. Deterministic across languages, which Merkle comparison requires.

**Two consequences to carry:**

- **Log compaction is per-`(key, field)`**, not per-key (§15.1) — retain the latest surviving value for
  each field.
- **A delta for an unknown entity cannot be applied.** A node receiving a partial update for an entity
  it has never seen requests full state, or waits for anti-entropy to repair. This needs an explicit
  fallback path, not a silent drop.

### 9.6 Per-language support

CBOR is chosen for values specifically because coverage is universal: `ciborium` (Rust), `cbor-x`
(TypeScript), `cbor2` (Python), `tinycbor` (C/C++ — MIT, ~2 KLoC, no STL or allocator assumptions),
with mature libraries in Swift, Kotlin, and C#. Where a binding lacks one, the emitted subset is small
enough that a single-purpose decoder is on the order of 500 lines.

The header and field framing need no library anywhere — a byte cursor and varints.

**Conformance is a deliverable, not a hope — in two tiers.** The wire break ships a test-vector
suite run in every binding's CI. **Tier 1, every binding:** record encode/decode and HLC/LWW
merge-join results — what attached nodes actually do. **Tier 2, anti-entropy servers only:**
content-hash agreement over canonical CBOR. Under the v1 scope (§1.2) tier 2 binds a single
implementation — the native Rust one — which reduces the cross-language canonical-form problem
(canonical output is not `cbor-x`/`cbor2`'s default; floats and map-key ordering are the classic
divergences) from a correctness cliff to a checklist the polyglot-peer extension (§10.2) would
re-open. Hashed positions avoid floats where possible regardless, and a binding that cannot emit
canonical form re-encodes through the ~500-line single-purpose encoder.

### 9.7 Debuggability

JSON is **not** a co-equal wire encoding. It is a **rendering**: a debug tool decodes a record to JSON
for inspection, using the schema when available and field-id-keyed output when not. This keeps
inspectability without a second encoder on the hot path, and without every node having to implement
two formats.

### 9.8 Envelope, not record

`tx` is request-scoped and meaningless remotely; schema version is per-batch. Both ride the batch
envelope. Per-plane envelopes carry transport/session metadata; the record carries only what is
intrinsic to the mutation.

### 9.9 Retire the WebSocket *protocol*, not the transport

The thing to retire is `MykoMessage` (`wire/message.rs:42`) and the `ws:m:*` protocol — a
client-server message set with 20 variants, 14 of them one subscription protocol instantiated three
times (§12.4). It is replaced by the record format above and the per-plane envelopes of §10.5.4.

**WebSocket survives as a transport.** The `Gateway` role (§2.7) terminates WSS and bridges attached
nodes into the mesh, which is how browsers and other lazy nodes participate without iroh at all. So
`ws_handler` and autosocket are **re-pointed, not deleted** — they carry the mesh protocol instead of
`ws:m:*`.

That eliminates the relay question for the browser tier entirely: a gateway is a myko node you already
run, so there is no relay to host, pay for, or treat as an availability ceiling. iroh relays remain
relevant only to the **peer** mesh (§10.3).

This is still the largest migration risk in the plan — every client port changes protocol — but it is
a protocol migration on an existing transport rather than a transport replacement. Sequenced last
(§18).

### 9.10 What explicitly stays

The **capability trait system** (re-rooted to `NodeScoped` in §11, otherwise untouched),
**`inventory`-based registration**, **`Arc<str>` interning on hot fields**, **the reflection
machinery**, and **hyphae**.

## 10. Transport

### 10.0 The transport contract

**The protocol is transport-agnostic.** Everything above the byte stream — record encoding (§9), merge
semantics (§8), manifests (§4), anti-entropy (§5.3), routing (§11), subscriptions (§12) — is defined
without reference to any particular transport.

A conforming transport must provide exactly three things:

1. **Authenticated peer identity by public key.** Ed25519. The remote's key is verified during
   connection establishment; it is the subject of capabilities (§5.4) and the value of `origin` on
   every record (§9.3).
2. **Reliable, ordered, bidirectional byte streams**, many per connection, independently
   flow-controlled.
3. **Plane multiplexing** — every stream belongs to exactly one plane, established at stream open
   (§10.5.2 depends on this and nothing more).

QUIC + TLS 1.3 satisfies all three, and so does anything equivalent. **iroh is the recommended
binding, not a dependency** — it additionally solves NAT traversal, hole-punching, and relay fallback,
which are genuinely hard and matter for edge and browser nodes. A datacenter-only deployment could
speak this protocol over plain QUIC.

This separation is deliberate and has a practical payoff: **it is what keeps the polyglot story open.**
Non-Rust iroh bindings expose only endpoints and streams (§10.1), so binding the protocol to iroh
would push every other language through `iroh-ffi`. Spec'd against the contract above, a Python
implementation can use `aioquic` and a Go one `quic-go` — losing NAT traversal, which datacenter peers
do not need anyway.

**Sections 10.1–10.5 are the iroh binding.** They are the recommended realization of the contract,
not part of it.

### 10.1 What each language gets

Researched 2026-07-25 against iroh 1.0 (shipped 2026-06-15), which formalizes **wire-protocol
stability across minor versions *and languages***.

| | Endpoint / ALPN / streams | iroh-gossip | iroh-blobs |
|---|---|---|---|
| **Rust (native)** | yes | yes | yes |
| **Rust → wasm (browser)** | yes, **relay-only** | **yes** (since 0.33) | **no** (tracking issue open) |
| **Python / Swift / Kotlin / JS(NAPI) / C** | yes | **not exposed** | **not exposed** |

Official bindings: Rust, Python, Swift, Kotlin, JavaScript, C (Go community-maintained). The C binding
(`iroh-c-ffi`) is the route for myko's **C++ and C# ports**.

**The FFI is deliberately minimal:** endpoints, protocols, connections & multipath, custom relays.
Gossip and blobs are not on its feature matrix.

> **Under the v1 scope (§1.2), only the native-Rust row is load-bearing.** The rest informs the
> extensions below and the gateway decision — and M3 (§17) verifies the gossip-absence claim only if
> the polyglot-peer extension is ever pursued.

### 10.2 Polyglot peers (extension, not v1)

Reimplementing HyParView + PlumTree in five languages is rejected — that is precisely the manual
fan-out gossip exists to avoid. **A polyglot peer participates over the ALPN planes instead.**

This costs latency, not correctness, because **anti-entropy (§5.3) is the authoritative convergence
path and gossip is a latency optimization on top of it.** Even if every gossip message were dropped,
periodic anti-entropy converges the mesh. Further, **direct pairwise replication over an ALPN stream
needs no gossip at all** — a polyglot node paired to a Rust node gets realtime updates, losing only
transitive delivery and onward relaying. **Realtime spokes on a Rust hub.**

Both paths are for polyglot nodes that need to be *peers* — holding a scope authoritatively,
anti-entropying, being a durability target. Under the v1 scope (§1.2) no such node exists: polyglot
services gateway-attach (§2.7), which needs none of this machinery. This section is the design that
makes the extension cheap if a consumer ever needs it — note it also re-activates the cross-language
content-hash requirement (§9.6).

### 10.3 Browser peers (extension, not v1)

Under the v1 scope (§1.2) browsers are always gateway-attached (§2.7); no wasm-iroh work exists in
the plan. This section records what browser *peering* would look like, because the facts were
researched and nothing in the wire precludes it.

**iroh core and iroh-gossip both compile to wasm**, so a browser peer is technically a genuine
realtime participant. Two constraints:

1. **Relay-only, always.** No arbitrary UDP, so no QUIC hole-punching; every browser iroh connection
   traverses a relay over WebSocket (still end-to-end encrypted). Relay capacity is infrastructure to
   own or pay for — which is why most browsers should be gateway-attached instead (§2.7, §9.9);
   wasm-iroh is for the browser that genuinely needs to be a mesh *peer*. (Pedantically: browsers
   *can* hole-punch via WebRTC data channels — ICE/STUN, with TURN fallback for the NAT pairs that
   resist. iroh does not speak WebRTC, so browser peering that way would be a second binding of
   §10.0's contract, losing iroh-gossip and still needing signaling plus TURN. Named here as the
   known extension path, not built: even a WebRTC browser peer is a filtered node that cannot
   anti-entropy or execute authoritatively, so §1.2's tiering — not transport — is what keeps
   browsers in the spoke tier.)
2. **No iroh-blobs in wasm.** A browser peer would need the **custom-ALPN snapshot path** — the
   portable bulk mechanism §10.0's contract defines. Under the v1 scope this pressure is gone:
   **every peer is native Rust, so the recommended binding uses iroh-blobs for bulk** (snapshot
   §15.3, backfill §15.2), and the custom-ALPN path remains the transport-agnostic definition —
   what a non-iroh binding, or this extension, would implement. Attached nodes bootstrap over the
   gateway's WSS carrying the same §12.4 envelope.

### 10.4 Build notes (browser-peer extension)

No v1 work depends on these; they are the recorded build facts for §10.3.

- wasm requires `iroh = { version = "1", default-features = false }`.
- No NPM package; the pattern is an app-specific Rust wrapper via wasm-bindgen. `myko-core` already
  compiles to wasm, so **myko is the wrapper**.
- **JavaScript has two iroh paths**: browsers get wasm (relay-only, gossip available), Node/Deno/Bun get
  NAPI (direct connections, no gossip). Not feature-equivalent.

### 10.5 The myko ALPN

No ALPN usage exists in the codebase — greenfield.

#### 10.5.1 ALPN = plane, as an optimization over the handshake

The normative plane declaration is in the control handshake (§4.3). This binding **additionally** maps
each plane to an ALPN string, so iroh's `Router` filters at connection establishment rather than one
round trip in — a node that does not serve a plane simply does not register its ALPN, and dialing it
fails immediately rather than being rejected after the handshake.

Strictly additive: the handshake declaration remains authoritative, and an implementation that only
honours the handshake is conforming.

#### 10.5.2 The plane determines origin — no wire flag, ever

- Records on the **replication** plane apply as remote — apply + index only; no cascade, no produce,
  no re-broadcast. The originating node already emitted any cascade writes (§7.5); re-running them on
  every receiver would duplicate them.
- Records produced by a command on the **serve** plane are local-origin.

**This is the load-bearing use of planes**, and it needs only §10.0's third contract requirement —
every stream belongs to exactly one plane, fixed at open. Whether the plane is named by ALPN or by a
stream header is irrelevant.

The hard constraint: if a single plane carries both kinds, the discriminator returns to the wire and
records must each declare their own origin — reintroducing a per-record flag that connection-level
planes make unnecessary.

#### 10.5.3 Two planes

ALPN is negotiated at connection setup, so **one ALPN = one connection per peer pair**. A 1:1 split
onto role bits would mean six QUIC connections per peer.

| ALPN | Carries | Lifecycle | Who registers |
|---|---|---|---|
| `myko/mesh/1/<network>` | control + handshake/manifest, replication, anti-entropy | long-lived | every participating node |
| `myko/serve/1/<network>` | routed commands, subscriptions, bulk transfer | request-scoped | `Handler` / state / `Logged` nodes |

The **network id** prevents two unrelated deployments sharing a public relay from ever negotiating —
defence-in-depth against misconfiguration, since NodeId pairing already handles authorization.

Two planes captures most of the value: the coarse split is visible at dial time, and **the planes
version independently**. `myko/mesh/1` is the **peer minimum**; the *polyglot* minimum is the gateway
protocol (§2.7, §9.9) — the same record format and §12.4 envelope over WSS, no planes at all.

**Bulk transfer likely warrants its own plane.** State snapshot (§15.2) and log backfill (§14.3) are
both cold-path, resumable, bulk workloads unlike request/response RPC. Splitting later is compatible —
and in the iroh binding under the v1 scope, bulk rides **iroh-blobs** (§10.3), with the bulk plane as
the transport-agnostic realization a non-iroh binding would implement.

#### 10.5.4 Framing and encoding

Length-delimited envelope (`u32` prefix + payload), specified precisely because polyglot implementers
depend on it.

- **Carry the state-change record as-is** on the replication plane — §9's three-layer encoding already
  gives every node header access without a decoder library, so the plane adds only framing.
- **Do not reuse `MykoMessage`** — its `ws:m:*` variants are a client-server WebSocket protocol, and the
  mesh plane needs messages with no WS equivalent (Merkle roots, manifests, descent requests). Define
  per-plane envelopes.
- **Control-plane and RPC messages use canonical CBOR**, matching §9's value encoding rather than
  introducing a second format.

Per CLAUDE.md's generation rule, per-plane schemas are **generated** from the Rust definitions using
the machinery behind §4.

## 11. Commands and routing

### 11.1 Authoritative execution requires completeness

> **Optimistic execution is always allowed and always provisional. Authoritative execution requires a
> node complete for the target scope** (§2.6).

Handlers validate against state — uniqueness checks, existence checks, cross-entity preconditions — and
every one gives a **wrong answer against a filtered view, silently**. Relationship cascades are the
most visible instance, not a separate problem: they walk the graph through `RegistryScoped::registry()`
(§12.3) and would reach only the children a filtered node happens to hold.

Consequences:

- **`Handler` implies complete** for the scopes whose *state* it authoritatively mutates.
- **Routing targets narrow** to nodes that both hold the handler *and* are complete for the scope.
- **Any node may run a handler optimistically** for local feedback (§11.3) — the result is a
  prediction, superseded by the authoritative run.

This reinforces the tiered topology (§1.2) with a rule rather than a tendency: **the mesh tier decides,
the spoke tier predicts.**

> **Scope of the rule: it governs authoritative state mutation, not side effects.** Handlers do two
> separable things — they *decide* (validate against state, then write) and they *act* (render
> something, move a light, play audio). Only deciding needs a complete view. See §11.2 for
> node-addressed commands, where the acting is the point.

> **Rejected: carving out cascades.** "Run commands locally but cascades on a complete node" fixes the
> visible symptom and leaves validation broken. It also needs a cascade *owner* — since replicated
> records apply as remote with no cascade (§10.5.2), having complete nodes cascade on inbound records
> would make **every** complete node cascade the same deletion independently. Convergent under merge,
> but wasteful, and choosing one responsible node is a leader election this design avoids.

#### Two dispatch modes: scope-routed and node-addressed

Not every command is a state mutation looking for an owner. **Some are addressed at a specific node
because that node is the thing being acted on** — show this scene in that browser, drive that fixture,
play on that device. A control system is largely made of these.

**Dispatch mode is a property of the command, not of the node:**

| Mode | Routed by | Requires completeness | Example |
|---|---|---|---|
| **Scope-routed** | `(command_id, scope) → owner` | **yes** (§11.1) | `CreateInvoice` |
| **Node-addressed** | `node_id → that node` | **no** | `SetActiveScene { node }` |

Node-addressed commands are exempt because **they are not making authoritative state decisions** —
they act on the target node itself or on state that node is the source of truth for. A filtered node
executing one is not validating against a partial view of shared state; there is nothing global to
validate against. **A browser can therefore be controlled through the mesh** despite holding a narrow
filter.

Two constraints keep this honest:

- **Any shared-state writes a node-addressed handler produces still follow §11.1.** They go out as
  ordinary writes through the optimistic-then-authoritative path (§11.3). The exemption covers the
  acting, not the deciding.
- **Delivery is at-most-once and unacknowledged by default.** A node-addressed command is not a state
  mutation, so it does not converge and it is not replayed by anti-entropy. If the target is offline,
  it does not happen. Anything needing durability should write state and let the target react to it.

**Gateways route in both directions** (§2.7): they carry attached nodes' commands into the mesh *and*
deliver node-addressed commands back out to them.

### 11.2 Route at ingress, execute locally

Commands are a **primary integration path** between services — a client sends `CreateInvoice` and it
routes to whichever node owns billing. But **handlers rarely dispatch nested commands across service
boundaries**; a handler's nested commands are normally same-service and stay in-process.

Therefore:

- **`CommandHandler::execute` stays synchronous.** No breakage across myko or rship.
- **The routing table is primary** — `(command_id, scope) → complete nodes holding that handler`, built
  from manifests (§4.2).
- **One interposition point:** `ws_handler.rs:1383 execute_command_job`, which today scans local
  inventory and errors on a miss (`:1447`). It becomes "resolve owner; if it's me *and I am complete
  for this scope*, proceed exactly as today; else forward."
- **Cross-service nested calls get an explicit async API** — visibly different, so the network boundary
  is legible in handler code, and rare by convention rather than prohibition.

### 11.3 Optimistic execution

**The same handler runs twice** — a general model, not an offline special case. D1 (§13) is what makes
it available: `CommandHandler::execute` becomes real on wasm, so the identical Rust handler runs on
both sides.

1. **Optimistically, on the originating node** — against whatever state it holds, for immediate
   feedback. The result is a *prediction* and may be wrong, since validation ran against a partial
   view.
2. **Authoritatively, on a node complete for the scope** — the real result.

The prediction is then **rebased** on the authoritative outcome. Offline (§15.6) is simply the case
where step 2 is deferred until reconnect; online, the round trip is short and the rebase usually a
no-op.

Three consequences:

- **A rebase is not a merge conflict.** §8.5's conflict recording is for concurrent writes that both
  committed; a superseded prediction never committed anywhere and must not be logged as data loss.
- **The outbox holds commands, not records** (§15.6) — the command is what gets re-executed. Records
  produced optimistically are provisional and discarded on rebase.
- **Provisional state is an overlay, not a merge.** LWW cannot un-apply, so optimistic records land in
  a provisional layer the reactive graph reads *through*; rebase drops the overlay and applies the
  authoritative records — never compensating writes. Sagas and relationship rules do not fire on
  provisional apply (the same no-produce discipline as remote apply, §10.5.2). The overlay is a real
  piece of machinery (§13, §18 phase 11), not bookkeeping.
- **Handlers with external side effects must not run optimistically.** Sending an email or charging a
  card cannot happen twice. This needs a marker on the handler, and the default must be safe: **opt in
  to optimistic execution, never opt out.**

This is the one place a handler's result is allowed to differ between nodes, and it is intentional.

**Prediction accuracy is bounded by the filter, and that is acceptable.** A filtered node predicts
correctly for entities it holds and cannot predict effects on those it does not — which are, by
definition, not on its screen. A predicted cascade reaches only the children it holds; the
authoritative run reaches all of them, and the rebase reconciles the difference.

### 11.4 Loop safety

`RequestContext.lineage` is an in-process call chain with no hop count or TTL. Cross-node routing adds
a hop limit and a visited-node set.

### 11.5 Consistency is declared per command

When multiple complete nodes advertise a `command_id` for a scope, routing needs a rule — and no one
rule fits every command. OCC alone cannot exclude: preconditions are execution-time-only (§8.4,
deliberately — apply-time preconditions break convergence), so two load-balanced nodes can both read
`occupied == false` inside the replication window, both pass, and both emit. Serialization has to
come from routing, and how much a command needs is the command's own business:

- **`routing = sticky`** — the default *mechanism*: the origin rendezvous-hashes `(scope,
  routing_key)` over eligible nodes, so same-entity commands serialize at one node in steady state.
  Costs nothing (the hash is a local computation) and degrades to any-eligible on failover. Sticky
  routing needs a **routing key** — the argument identifying the target entity, marked
  `#[routing_key]`, defaulting to the conventional id argument where unambiguous.
- **`occ = enforced`** — confirm-then-write: the origin treats the write as committed only once the
  executing node confirms preconditions. A real compare-and-set in steady state; honestly best-effort
  during failover races, and the response says which it was.
- **`routing = any, occ = best_effort`** — maximum availability for commands that tolerate it:
  idempotent updates, blind writes, telemetry.

Seat-booking buys serialization; telemetry buys availability. The declaration lives on the command
and travels in the manifest (§4.2), so origins route accordingly. Horizontal scaling is preserved —
distinct entities hash to distinct owners — with no rebalance story needed beyond the hash.

### 11.6 Edge-owned entities: the direct-write exception

Some entities have a natural single writer at the edge — a device's own status, a sensor's readings,
a session's presence. Routing those through a command to a complete node adds a round trip to learn
what only the edge node knows.

**A type may be declared edge-owned** — `#[myko_item(edge_owned)]` — giving each entity an owning
node, stamped at creation. **The owner direct-publishes records for its own entities on the
replication plane**: the single sanctioned bypass of command validation, and the inversion of the old
model where direct event publish was the general path. Consequences:

- **Single writer by construction** — no concurrent-write races, LWW trivially sound, OCC
  unnecessary.
- **The replication-plane write rule becomes crisp:** a node may direct-write only records for
  entities it owns; every other write arrives as a command (§11.1). Under the v1 trust model (§5.5)
  this is a protocol rule rather than an enforced defence — but it is *stated*, so an open-mesh
  future hardens a rule instead of discovering a hole.
- **Per-origin log streams are gap-checkable** where it matters most: the owner's records carry the
  log-layer sequence (§2.4), so `Logged` contiguity is verifiable exactly for the streams edge nodes
  produce.
- **Offline replay ships records, not commands, for owned entities** — the one place §8.5's
  record-level comparison survives, and where a loss signals an ownership violation rather than an
  ordinary conflict.
- **Gateway-attached owners publish through their gateway** (§2.7): the record's `origin` is the
  attached node, and the gateway injects it into the replication plane on the owner's behalf — the
  one case a node re-broadcasts a record it did not originate. Termination stands: the gateway is the
  owner's on-ramp, not its proxy for reaching anyone.

### 11.7 Idempotency across routing

A routed command that times out gets retried, and a retry after partial execution must not
double-execute — counters (§8.3) make the hazard concrete. **Every routed command carries an
idempotency key** (origin NodeId + origin-local id); handler nodes keep a dedup window keyed on it
and return the recorded result on replay. The §11.3 side-effect marker gates *retry* re-execution the
same way it gates optimistic execution: a handler that cannot run twice is exactly-once-or-error,
never silently re-run.

## 12. The operation model

### 12.1 Projections are pure functions of local state

Queries, views, and reports compute over the entity store. They became wire operations for one reason:
**the client had no store**. §13 removes that — a node with a local store runs `query_map` with
identical semantics.

Commands and events stay on the wire: commands have side effects and need validation at a node that
owns them; events *are* the replication substrate.

### 12.2 Placement, with a decision procedure

Projections **stop being protocol and become a placement decision** — the same handler code runs
locally or remotely.

**Coverage is not the question — cost is.** Because every typed read passes through the query hook
(§12.3), evaluating a query *registers* its predicate, which extends the filter, which brings the
data. A projection is therefore never silently under-served; the only question is whether registering
is cheaper than routing.

**Registration must gate the first evaluation.** Between registering a predicate and finishing its
hydration from the serving peer, the local store holds a subset — and nothing in the store layer
knows it: `select`, `snapshot`, and `exec_query` all compute happily over whatever is present, and
`StoreRegistry::get_or_create` makes a missing type indistinguishable from an empty one. So
registration returns a **per-predicate readiness gate** — the same shape as §15.7's per-scope gate —
and the first evaluation blocks on it. Without the gate, the registration window returns exactly the
silent-incomplete results this model exists to prevent.

> **Register-and-materialize when the predicted result set is small and reused; route when it is large
> or one-shot.**

Routing wins when: **selectivity** is low (replicating a million rows to compute ten inverts the
economics); **computation is shared** (`report_cache` and `compute_gates`, `server/context.rs:246,249`,
exist so N subscribers share one computation — local-only multiplies that by subscriber count); or
**cold start** dominates, since a first-time predicate must replicate before it can answer.

Subsumption is then an **optimization, not a correctness requirement** — it avoids re-fetching when
`age > 10` is registered after `age > 5`. Getting it wrong over-fetches; it does not produce wrong
answers.

### 12.3 Query-driven replication

**The filter is derived, not declared.** Every typed read passes through the query hook —
`query_map` and its variants (`capability.rs:129`), `view` (`:387`), `report` (`:245`), and
`exec_query` (`command/handler.rs:81`) all take structured params the framework sees. So a node's
filter is simply **the union of its live subscriptions**, maintained automatically rather than
configured.

This bounds memory by working-set size rather than tenant size, which is what makes browser nodes
viable regardless of organization size.

#### One store, N derived views

**The union is the unit — an entity matching many overlapping queries is stored once and sent once.**
This distinguishes the design from result-set replication, where each subscription streams its own
results and an entity matching three queries arrives three times. Today's WS protocol is the latter;
this is not.

- **Stored once.** Query membership is *derived* by evaluating each predicate over the store —
  precisely what hyphae already does incrementally. Query results are views holding references, not
  copies.
- **Sent once.** When an entity changes, the serving node asks "does this match any of the peer's live
  predicates?" — one check against the union, one send, regardless of how many of the peer's queries
  match.
- **Refcounting governs eviction, not storage.** The refcount counts *matching predicates*, so
  cancelling one subscription does not evict an entity another still matches. It never counts copies,
  because there are none.

**The union is exact only for predicates that are data.** Generated `XQuery` filters are
canonicalized, serializable structures with a per-item `matches` — those register exactly.
Hand-written `test_entity` is arbitrary code, and `build_view` join plans make membership depend on
*other* entities — not per-item predicates at all. The ladder is explicit: **data predicates → exact
filter; opaque-code queries → whole-type subscription; `registry()` and search consumers → complete
nodes only.** Degrading over-fetches; it never under-serves.

It is also less new machinery than it appears: myko already evaluates queries over `CellMap`s with
incremental diffs and pushes them to subscribers. **The change is that results land in a real local
store other projections can run over**, rather than being consumed by one subscription.

**Report interest means the report's *inputs*** — recorded at the capability seam, not read from
hyphae's graph. (Hyphae's dependency graph is runtime-only, weak, and unlabeled — built for
glitch-free invalidation, not introspection — so "derive from the graph" is not mechanical.) The
framework records each `ctx.query_map` / `view` / `report` call's item type during the report's first
materialization. Two known gaps, both resolving to "route instead": `switch_map`-nested queries
register only after first tick, and `registry()` / `search()` record nothing. For low-selectivity
reports, route regardless (§12.2).

#### Three paths bypass the query hook

- **`Searching::search()`** (`capability.rs:232`) — full-text index to ids, no predicate. Benign: the
  follow-up lookup registers.
- **`entity_snapshot`** (`server/context.rs:358`) — point lookup by id. Benign, and trivially
  expressible as a predicate.
- **`RegistryScoped::registry()`** (`capability.rs:80`) — raw store access by runtime-determined type
  name. The relationship manager does not even use the capability: it reads the `ctx.registry` field
  directly (`relationship_manager.rs:481` et seq.), so policing the capability seam would miss the
  largest bypass — completeness (§11.1) is the actual guard. `query_snapshot` (behind every handler's
  `exec_query`), search-index maintenance, and belongs_to bucket backfill share the same
  complete-store assumption.

> **The third is a correctness hazard on filtered nodes.** Cascade rules walk the graph through
> `registry()` (§7.5), so a parent DEL executed on a filtered node would emit child DELs for **only the
> children that node happens to hold** — silently under-applying, with no error and nothing detectable
> locally. The narrower the filter, the more it misses.
>
> **§11.1 removes it by construction:** authoritative execution happens only on nodes complete for the
> scope, so a cascade always walks a complete graph. Deletion is the most visible instance of a general
> problem — handlers validate against state, and *any* validation against a filtered view is silently
> wrong.

**It relocates rather than removes the RAM question:** something must still evaluate subscriptions
against the complete set, so a `Stateful` node holding the whole scope must exist. The hard-bounded
case (browsers) is solved; the server case becomes a machine you can size (§17, M1).

### 12.4 Collapse the enum

`MykoMessage` carries 14 structurally identical variants — `{Query, View, Report}` ×
`{subscribe, response, cancel, window, error}`. One generic envelope (`Subscribe{kind, id, params}` /
`Update{id, payload}` / `Cancel{id}` / `Error{id}` / `Window{id, …}`) collapses them with no loss.

One envelope serves both the `myko/serve` plane and the gateway's WSS protocol (§2.7, §9.9): the
transports differ, the message set does not.

## 13. The `NodeScoped` refactor (D1)

Clients become full nodes: `ServerScoped` is re-rooted as `NodeScoped`, and wasm gets a real reduced
backing instead of `unreachable!()`.

**Current shape.** Eight capability traits descend from `ServerScoped` (`capability.rs:87`), whose
sole accessor is `__server_ctx() -> &Arc<MykoServerContext>` — that method and the whole `server`
module (`lib.rs:91`) are `#[cfg(not(target_arch = "wasm32"))]`. (`RegistryScoped` and `RequestScoped`
sit outside `ServerScoped`, ungated.) On wasm, capability bodies are `unreachable!()` stubs via
`wasm_native!` — except `Viewing`, `PeerAccess`, and `Replaying`, which are whole-trait native-gated
and must be *created* on wasm rather than un-stubbed — and `CommandHandler::execute` is a hardcoded
error (`command/handler.rs:198`). `MykoClient` holds a socket and tx-keyed dispatch maps with **no store at
all** (`client/mod.rs:240`).

**Reduce the context, don't trait-object it.** Making `__node()` return `&Arc<dyn NodeRuntime>` puts
dynamic dispatch on every capability call, and prior benchmarking found the existing dyn boundary
already accounts for a meaningful slice of rship's hot path. Instead:

- Rename `MykoServerContext` → `MykoNodeContext`; `ServerScoped` → `NodeScoped`.
- Un-gate the module for wasm, gating only genuinely native internals.
- Subsystems a node lacks become `Option` fields — the struct already does this for `event_sink` and
  `history_replay`.
- Capability methods needing an absent subsystem return `Result`/`Option` instead of `unreachable!()` —
  strictly better than today's panic-stub, even natively.

**Feasibility:** most of the struct looks wasm-compatible already. Across all of `core`, only three
files reference `thread::spawn` or `tokio`: `query/registration.rs`, `client/mod.rs` (already
wasm-gated), and `server/context.rs`. Postgres persisters live in `myko-server`.

**This has not been compiled** — the claim is "blockers appear few and localized," not "it builds."
First task is an un-gate spike for a real error list; thread-spawning in `server/context.rs` is the
known thing to isolate behind a scheduler seam.

## 14. Time travel

### 14.1 Restore is a forward write, never a rewind

Restoration reads state as of *T* and re-writes it with a **current** timestamp: merge handles it
natively, no distributed coordination is needed, and the restore appears in the log as an ordinary
write. A payoff of §7.2 — because state is primary and the log derived, restore is *just a write*.

### 14.2 Inspection is a routed read

History lives on `Logged` nodes, so **windback routes** — the paradigm case for §12.2.

**Historical reads return transient projections and must not replicate into the local store.** The
store is keyed `(type, id)` and holds *current* state; a historical version shares that key. Through
the replication path it would either be rejected as older (correct but useless) or, if re-stamped to
land, **silently perform a restore instead of a preview**. History travels on a **separate read path**
— a safety constraint, not a preference.

Two requirements: the manifest advertises **contiguous ranges** so a requester can pick a covering
peer; and **the log is indexed by `(scope, item_type, id)` → time-ordered versions.** Today
`replay_to_store(until)` rebuilds an entire `StoreRegistry` from the whole log — far too coarse once
tree inspection is routine.

### 14.3 Restoring an entity tree

**Tree membership changed over time.** The closure is computed *as of T*, walking relationships in the
**historical** state.

| Mode | Behavior | Risk |
|---|---|---|
| **Merge** | SET the T-closure to T-state; leave newer entities alone | non-destructive, not "how it was" |
| **Exact** | SET the T-closure *and* DEL anything not present at T | genuinely restores; **destroys post-T work** |

Mode is a **required parameter** — defaulting to Exact would be a data-loss footgun.

> **Cascades must be suppressed.** Exact mode emits DELs, and `#[belongs_to]` cascades a parent DEL to
> children while `#[owns_many]` deletes them outright (§7.5). Through the normal path the cascade rule
> fires *on top of* the computed closure and deletes entities the restore intended to keep. The restore
> has already computed the exact desired end state, so it applies as an **authoritative batch with
> cascade rules suppressed** (§7.4).
>
> The closure should also be **upward-closed** — restoring a child whose parent is absent leaves an
> orphan that the next cascade evaluation may remove. Checkable before the batch is emitted.

**Atomic locally, eventually consistent globally** — batch emission is first-class, but there is no
distributed transaction.

**Scope containment** holds naturally via immutable binding (§5.1), but assert it. **Authorization**
requires a capability for the target scope, Exact arguably a stronger one than Merge. **Concurrency**
is handled by §8.4's preconditions.

### 14.4 Shape and UX

`RestoreEntityTree { root, as_of, mode }` is a **framework-provided command**, so §11.1 applies — it
routes to a node **complete for the scope** that can also reach a `Logged` peer covering `as_of`.
Completeness is doubly required here: the T-closure is computed by walking relationships (§14.3), and
a filtered node would compute a truncated tree.

**Historical operations are explicitly allowed to be slow.** That buys real simplification: no
prefetch, no aggressive caching, simple request/response rather than streaming, and a log index that
can favour **compactness over lookup latency**.

| Operation | Cost | UI moment |
|---|---|---|
| `ListVersions { root, before?, limit }` | cheap — index scan | render the timeline immediately |
| `GetTreeAsOf { root, as_of }` | expensive | preview on selection |
| `RestoreEntityTree { root, as_of, mode }` | a write | on confirm |

**Attribution needs an identity, not a node.** Three separate concepts, all needed: **node**
(`source_id`), **connection** (`client_id`), and **identity** (`actor`). Audit, undo attribution, and
sub-scope RLS all need the third.

## 15. Retention and lifecycle

### 15.1 Two distinct retention policies

| Concept | Bounded by | Purpose |
|---|---|---|
| **History depth** | time, with a size cap as safety valve | how far back windback reaches |
| **Latest per `(key, field)`** | never, until tombstone GC | makes state recovery possible at all (§9.5) |

> **The horizon governs history depth only.** Evicting the most recent version of a live key makes
> state recovery impossible, **silently** (§15.5).

**Tombstones:** DEL leaves a timestamped tombstone in the store and anti-entropy index, so a peer
learns the entity was *deleted* rather than absent. GC on a configurable window (~30 days) — also the
threshold past which warm start must not reconcile (§15.7).

### 15.2 The horizon is adjustable

A `Logged` node's horizon is **policy**, configured per scope. Widen it and the node backfills; reads
it previously routed become local.

- **Expand = backfill** — bulk, resumable, cold-path, *transfer plus index build*. **Must extend an
  existing contiguous range** (§2.4), not create islands. Requires the scope capability — historical
  data is not less sensitive.
- **Contract = local truncation** — one of the §7.4 operations that must emit nothing.

**Advertise `horizon_actual`, never `horizon_target`** — a node backfilling from 30 to 180 days must
not advertise 180 while sitting at 60.

> **Operational floor: at least one archival node with unbounded retention**, preferably two. If every
> node has a finite horizon and all roll forward, **history is permanently lost**. Easy to miss,
> because every individual horizon looks locally reasonable.

### 15.3 Bootstrapping a state node

**From an existing `Stateful` peer** — O(current state) rather than O(history), with merge metadata
already resolved. Snapshot at a watermark → subscribe to the live tail → readiness gate until both are
current, using the bulk-transfer path. Anti-entropy alone would converge eventually, but from empty it
degenerates to "pull everything."

A filtered node bootstraps by declaring its subscriptions (§12.3) and receiving matching entities from a peer whose filter subsumes its own (§2.6).

### 15.4 Losing all `Logged` nodes

**State nodes keep running** — the log is outside convergence. What is lost is **history, not data**.

**Write admission** decomposes: **state durability** needs a reachable `Durable(Stateful)` target;
**history durability** needs a reachable `Durable(Logged)` target. Whether a scope requires the second
is **per-scope policy** — compliance scopes demand it, operational scopes accept gaps.

**On return, the log is silently non-reconstructable unless repaired.** Replay would produce pre-gap
state plus post-gap changes with the gap's changes missing.

> **Repair is a marked checkpoint, never "the next events."** Writing current state as ordinary SETs
> would stamp fresh timestamps and actors, corrupting attribution — and worse, those records would
> **replicate**, turning log repair into a mesh-wide write (§7.4). The checkpoint is **log-only**,
> preserving original per-field timestamps and actors.

**The checkpoint opens a new contiguous range; it does not repair the old one** — the node advertises
`[inception .. gap_start]` and `[checkpoint .. now]`, and in-gap reads fail structurally (§2.4).
Checkpoint **after** state nodes have converged, not from an arbitrary node mid-partition.

### 15.5 Recovering state when all state nodes are lost

Requires the log to retain the **latest surviving value per `(key, field)` regardless of age** (§15.1, §9.5) — compaction, or periodic
checkpoints making recovery "latest checkpoint + tail." Without it, an entity last written outside the
horizon has no record at all, and replay yields a state where it is *absent* rather than stale.

- **The log must store merge metadata**, not just values — replay dropping per-field timestamps and
  CRDT tags produces a state that merges *incorrectly* thereafter.
- **Recovery replays in place, preserving original timestamps and actors** — the exact opposite of
  §14.1's forward write. Re-stamping would present as a mass rewrite and clobber surviving replicas.
  **Do not share code paths carelessly.**

**The checkpoint mechanism §15.4 needs is the same one §15.5 needs.** Periodic checkpointing is a
**general precondition** for the log to serve as a recovery source.

### 15.6 Offline operation

**Offline nodes accept writes.** A node may be its own state-durability target for locally-originated
writes, holding them in a **local outbox** and replaying on reconnect — commands in the general case
(§11.3), records for edge-owned entities (§11.6).

The accepted risk is explicit: **unsynced local writes are lost if the device is lost**, and clearing
site data discards both pending writes and unresolved conflicts. This is surfaced as a pending-write
count, not prevented — preventing it would mean refusing offline writes, which is the thing being
enabled.

The outbox does double duty: it is also what makes offline conflict detection possible (§8.5).

**Rejected: fail-closed write admission** — refusing local writes unless a durability target is
reachable. It would make an offline node inert, contradicting first-class origins. It also would not
buy what it appears to: it only blocks writes in a component that *cannot persist at all*, so two
partition halves that each retain durability both pass the check and both accept writes. Divergence
across a partition happens regardless, and §8's merge plus §8.5's conflict recording is the actual
answer to it.

### 15.7 Warm start — persisted state is stale

Persisted state carries a **watermark**. On restart the mesh has moved on, so **persisted state is
stale until reconciled**, never authoritative. **Catch-up comes from peers**, via anti-entropy from the
watermark — a node's own log records only changes *it* wrote.

> **The gate blocks writes, not merely reads.** A stale read is temporary; a stale **write** is
> permanent loss. A node returns holding T1 state, a user reads a stale value, modifies it, and the
> write lands at T3 — clobbering the T2 change the node never saw. §8.4's preconditions catch this;
> the readiness gate prevents it arising.

**Staleness threshold:** if the watermark is older than the tombstone GC window, local state may
resurrect deleted entities — past that point, **discard local state and cold-bootstrap** (§15.3).
**The threshold binds live nodes too:** a node partitioned longer than the window has the identical
hazard without restarting — peers GC the tombstone, and heal-time anti-entropy reads its surviving
copy as "peer is missing X" and pushes the entity back. Any node whose last successful reconcile for
a scope is older than the tombstone window discards and cold-bootstraps that scope, restart or not.

**Readiness is per-scope.** A node caught up on org 5 serves org 5 while org 12 still syncs. Applies to
browser nodes equally.

## 16. Testing strategy

The properties below run inside a **deterministic simulation harness** — simulated transport, seeded
scheduling, fault injection for partition, duplication, reorder, delay, and clock skew, in the
turmoil/madsim family. Convergence properties are unfalsifiable as claims and cheap as seeded
property tests; the harness lands with phase 2 so the wire break arrives with its properties (§18).

- **Merge determinism** — conflicting writes applied in different orders converge identically; strategy
  selection per field type is deterministic.
- **Per-field independence** — concurrent edits to distinct fields both survive; OR-Set adds both
  stick; counters sum.
- **OCC** — a read-then-written field rejects on precondition mismatch; a blind write does not; a
  rejected write surfaces a retryable error.
- **Conflict recording** — offline replay detects its own losses via outbox; partition heal detects
  during anti-entropy; heal summary replicates while detail does not.
- **Hydration gate** — the first evaluation of a newly registered predicate blocks until backfill
  completes; a store never serves a predicate it has not finished hydrating.
- **Edge ownership** — only the owner direct-writes an owned entity; a non-owner direct write is a
  protocol error; an owned-entity replay conflict is recorded as an ownership violation.
- **Rebase** — a superseded prediction produces no conflict record; the provisional overlay drops
  atomically; sagas never fire on provisional records.
- **Idempotency** — a retried command returns the recorded result; a side-effect-marked handler never
  executes twice.
- **Conformance** — every language binding reproduces the wire test-vector content hashes
  byte-for-byte (§9.6).
- **Scope isolation** — anti-entropy never transfers data for an unserved scope; eviction emits no
  records; cross-scope references are rejected.
- **Log contiguity** — a gap disqualifies the range; checkpoint opens a new one; in-gap reads fail.
- **Recovery** — state rebuilds from a compacted log with merge metadata intact; recovery preserves
  original timestamps where restore does not.
- **Warm start** — a stale node refuses writes until reconciled; past the tombstone window it
  cold-bootstraps instead.
- **Routing** — ingress routes by `(command_id, scope)`; nested same-service commands stay in-process;
  hop limits terminate loops.
- **Polyglot** — a gateway-attached non-Rust node round-trips records and commands through its
  gateway with correct merge results (tier-1 conformance, §9.6). (The peer-extension variant — ALPN
  pairing, anti-entropy, no gossip — tests with §10.2 if that extension lands.)

## 17. Open items

**M1 blocks the local-first story and is a live problem in the current implementation.** M2, M3 and Q1
are measurements and verifications that block nothing.

### M1 — resident-memory amplification

**Not "how much data" — "how much RAM per unit of data."** Measured against the `rack` deployment
(2026-07-26, myko 4.0.0-canary.79, 8 connected clients):

| | |
|---|---|
| Total records | **82,164** across 156 declared entity types |
| Non-empty types | **49** — 107 types declared but unpopulated |
| Concentration | top 10 types hold **96.8%** of all records |
| Largest type | `Action` @ 25,457 |
| Data at rest | ~40–170 MB depending on record size |
| **Observed process RSS** | **tens of GB** |

That is a **100–1000× amplification**, and it means the binding constraint on `Stateful` was never
entity count. A dataset that trivially fits in a browser tab costs a server tens of gigabytes to hold
*reactively*. Until this is understood, the memory cost of a node cannot be predicted from its filter,
and §12.3's claim that filtering bounds memory is unquantified.

**Hypotheses, in order of suspicion:**

1. **Per-subscription derived maps.** If each live query/view materializes per-entry reactive structure
   rather than referencing source cells, cost is `subscriptions × matching entities`, not `entities`.
   That shape produces three orders of magnitude. **Testable: does RSS scale with client count?**
   Code-verified shape: each scan-mode query holds `source_rows` — a full Arc-clone of every entity
   of the type, matching or not (`hyphae:map_runtime.rs:13`) — plus ~3 entries per match, and each WS
   subscription adds two more maps per match (`client_session.rs:147`). Pointer-level, roughly
   100–200 B per entity per distinct live predicate — which reaches tens of GB only with *thousands*
   of distinct live predicates. If the deployment does not have those, suspicion moves down this
   list.
2. **Cache sweep lag.** `query_cache` / `view_cache` / `report_cache` hold weak refs with an explicit
   dead-entry sweep — and the sweep is **caller-driven** (`sweep_dead_cache_entries`,
   `context.rs:415`); first verify the host app calls it at all. Compare `view_cache_len` against
   `view_cache_live_count` on a live server — one call, and the accessors already exist.
3. **hyphae per-cell overhead**, or a memory-heavy feature (`trace`) enabled in the deployed build.
4. **RSS vs live heap** — allocator retention under sustained `Arc` churn. Real, but not 1000×.

**The instrument is a heap profile, not another count** — `heaptrack` against a node under normal load
replaces all four hypotheses with an answer.

**Design consequences fork on the result:**

- **Inherent per-subscription amplification** → §12.3's filter model becomes load-bearing rather than
  an optimization, browser nodes need hard subscription budgets, and a disk-backed store would not
  help at all, because the memory is not in the store.
- **Sweep lag or fragmentation** → a bug with a fix; this design stands as written and the current
  implementation gets its RAM back.

**A note on the data shape.** Two-thirds of declared types are empty, and 10 types hold 97% of records.
Extreme skew means a filter over two or three types would do nearly all the work — and empty types cost
no Merkle tree (§5.3), which softens the per-`(item_type, scope)` index concern at 156 types.

**M2 — gossip topic count.** §5.7 puts a topic per scope; iroh-gossip maintains HyParView/PlumTree
membership **per topic**, so a node serving 1000 orgs holds 1000 memberships. Cheaply testable.

**M3 — iroh FFI gossip exposure.** §10.1's claim that non-Rust bindings lack gossip rests on the
published feature matrix rather than per-language API references. **Moot under the v1 scope (§1.2):**
polyglot services gateway-attach and never touch iroh. Becomes a real verification only if the
polyglot-peer extension (§10.2) is pursued — check per-language API references then, including the
undocumented "iroh-services binding" the published matrix lists.

**Q1 — `Handler` without state.** Executing commands against state a node does not hold is undefined.
Likely coherent only if the handler's reads route, which makes it a placement question (§12.2).

## 18. Phasing

**Validation strategy:** benchmark with **internal myko demo services** before committing the wire
format, then perform a **scoped rship migration** for real-workload perf testing, then a **coordinated
release**. The early benchmark exists because the wire break is the least reversible step.

**Sequencing principle: land every wire break in one phase.** Each break is a migration for live
consumers.

0. **Prereqs.** Event-bus unification (landed, PR #25) supplies the single apply chokepoint this design
   depends on (`apply_event_batch` → `emit_grouped` → `apply_effects`). One regression to reverse:
   the `Origin::Remote` apply mode PR #25 introduced has since been removed — only `Local | Cascade`
   remain, and wire-ingested events currently apply as `Local`, cascading and producing; the remote
   mode returns with the planes (§10.5.2). Partial convergence work exists on `feat/iroh-dataplane`
   targeting wall-clock timestamps and whole-entity resolution; it does not match §8 and should be
   **rewritten rather than landed** — its two survivable ideas, the `(ts, source_id)` total order and
   tombstones in the stamp index, reappear as §9.3's HLC tiebreak and §15.1's tombstones.
1. **Item field schemas + merge strategy mapping** (§4.1, §8.3) — additive macro change. Determines
   what the record must carry, so it precedes the wire break.
2. **Benchmark services + simulation harness** — synthetic rship-shaped load measuring record size,
   merge metadata overhead, typed-store ingest, and field-addressed encoding (§9); also answers
   **M1**. Alongside: the deterministic simulation harness (§16), so the wire break lands with its
   convergence properties as seeded tests.
3. **The wire break — all at once** (§9): the three-layer encoding, HLC (with tiebreak and drift
   bound), per-field merge metadata, OCC preconditions, opaque payload, NodeId `source_id`, qualified
   `item_type`, scope id, `actor`, explicit tombstones, envelope moves, record rename. Ships with the
   two-tier conformance vectors (§9.6 — encode/decode + merge for every binding; hash agreement
   Rust-only under §1.2's scope) and the migration converter for live deployments — existing history
   re-encoded (RFC3339 → HLC, whole-entity JSON → field entries, bare `item_type` → default
   namespace).
4. **Split state from log** (§7.2) — independent retention, role bits (§2). Build the log **indexed**
   and **compacted per `(key, field)`** from the start (§15.1, §15.5); retrofitting either is far more
   expensive than designing them in.
5. **State store + scope partitioning** (§5, §6) — typed store, per-field merge, OCC, tombstones, and a
   Merkle index keyed per `(item_type, scope)` with scope-intersection negotiation. **Do not build a
   per-type Merkle index and retrofit scope** — that is a rewrite.
6. **Manifests + membership** (§4.2) — schema discovery and a routing table with no routing semantics.
7. **ALPN planes** (§10.5) — `myko/mesh/1` first; this makes peer participation real (polyglot's
   ordinary path is the gateway, §2.7, which lands with phase 14). Design `myko/serve/1` against
   §12.4's envelope, not a port of `MykoMessage`.
8. **`NodeScoped`** (§13) — start with the un-gate spike.
9. **Query-driven replication** (§12.3) — materialization filters (§2.6), subscription-defined
   working sets, per-predicate hydration gates (§12.2). This is what makes browser nodes viable.
10. **Command routing** (§11) — ingress routing, consistency modes and routing keys (§11.5),
    edge-owned direct publish (§11.6), idempotency dedup (§11.7), hop limits.
11. **Conflict recording + offline** (§8.5, §15.6) — command outbox, provisional overlay (§11.3),
    detection, heal summaries.
12. **Time travel** (§14) — routed historical reads, then `RestoreEntityTree`.
13. **Scoped rship migration + perf validation**, then coordinated release.
14. **Retire the `ws:m:*` protocol** (§9.9) — last; highest-risk migration, nothing depends on it.
    This is the **Gateway cutover**: `ws_handler` and autosocket are re-pointed to carry §12.4's
    envelope for the Gateway role (§2.7), not deleted, and attached nodes migrate off `ws:m:*`.

## 19. Sources

**iroh capability claims in §10** (retrieved 2026-07-25): [iroh 1.0](https://www.iroh.computer/blog/v1) ·
[WASM/browser support](https://docs.iroh.computer/deployment/wasm-browser-support) ·
[language bindings](https://docs.iroh.computer/languages) ·
[iroh-ffi](https://n0-computer.github.io/iroh-ffi/) ·
[iroh-blobs wasm issue](https://github.com/n0-computer/iroh-blobs/issues/90) ·
[iroh 0.33](https://www.iroh.computer/blog/iroh-0-33-0-browsers-and-discovery-and-0-RTT-oh-my)
