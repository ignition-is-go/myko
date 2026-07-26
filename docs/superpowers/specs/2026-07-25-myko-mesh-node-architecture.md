# Myko Mesh — Node Architecture & Cross-Service Federation

**Date:** 2026-07-25
**Status:** Design. Open items are measurements and one external verification (§17), not unresolved
architecture.

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
2. **Polyglot** (§10.2) — non-Rust nodes cannot join a gossip swarm, so they hang off a Rust node.
3. **Browser transport** (§10.3) — relay-only, no hole-punching; edge connectivity is asymmetric anyway.

So: **a peer mesh among complete nodes, with a spoke layer of partial and edge nodes.**

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
| **Stateful** | holds a **complete current snapshot in memory** for its `service × scope` set | linked item parsers (typed, §6) |
| **Cached** | holds a **subscription-defined subset** — the union of its live query results (§12.3) | linked item parsers |
| **Logged** | holds **contiguous history — no gaps** — over an advertised range | log store (schema-free, §6) |
| **Handler** | executes some set of `command_id`s | linked command handlers |
| **Origin** | originates commands and subscribes to results | any node |
| **Relay** | forwards transport only | iroh endpoint |

`Stateful` and `Cached` are mutually exclusive per `(service, scope)`: a node either has the complete
set or a subset. **Only `Stateful` can serve anti-entropy, bootstrap peers, or be a durability
target** — a `Cached` node has no complete Merkle coverage to compare.

Today's server is `Stateful + Logged + Durable(Logged) + Handler + Origin`; today's client is `Origin`
alone. A browser editor is `Cached + Origin + Logged(own conflicts)`. An archival appliance is
`Logged + Durable(Logged) + Relay`.

### 2.2 Durability is a qualifier, not a role

**`Durable` means survives restart**, qualifying each *holding* independently: `Durable(Stateful)` and
`Durable(Logged)` are separate. "State in memory, log on disk" is a real configuration and it is
myko's today — the in-memory `StoreRegistry` is `Stateful`, Postgres persisting and replaying records
is `Durable(Logged)`.

### 2.3 Being relied upon is an advertisement, not a capability

A browser node with IndexedDB genuinely *is* `Durable(Cached)` — yet must never be a mesh durability
target, because the user clears the cache. **Persistence is a local fact; being a durability target is
a claim made to peers.** Nodes advertise targets per scope (§4.2); the ALPN set is the authoritative
form of that claim (§10.5.1).

### 2.4 Definitional details that carry weight

**"Complete" means complete for the node's `service × scope` set.** No node holds everything, so
unqualified "complete" would make `Stateful` unachievable.

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

**`Stateful` is RAM-bounded**, which is why `Cached` exists (§12.3). Whether a complete node can hold a
whole scope is the one unmeasured question (§17, M1).

### 2.5 Language qualifier

Every role is language-agnostic **except the realtime dissemination fast path**, which needs
iroh-gossip and therefore Rust, native or wasm (§10). Polyglot nodes hold any role and converge via
anti-entropy plus direct pairwise replication (§10.2).

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

**The ALPN set is the authoritative role advertisement** (§10.5.1) — the manifest says what a node
claims; registered ALPNs are what it will accept. Where they disagree, ALPN wins.

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
- **A global scope** holds scope-root records, framework entities, and shared reference data.

## 6. Durability and schema

Today an unknown `item_type` is silently dropped: `parse_item` returns `None`
(`server/context.rs:444`) and every ingest path skips it (`postgres.rs:218`).

| Role | Schema | Rationale |
|---|---|---|
| **`Stateful` / `Cached`** | **required, per service** | CRDT merge, cascades, indexing, and query evaluation are all type-specific (§9.5) |
| **`Logged`** | **not required** | index keys on `(scope, item_type, id)`, all header fields; outside convergence (§7); serves raw records for the requester to parse |

A generic, schema-free **archival appliance remains possible**; a generic *state* store does not.

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

- **Convergence compares stored state, not log position** — a last-writer-wins register per field.
- **Merkle leaves hash `(id, content_hash, timestamp)`** — current state, not history.
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
| Collaborative text | different paragraphs both survive | sequence CRDT (Yjs/Automerge) |
| Nested map (`metadata`) | different keys both survive | recursed LWW-Map |

Per-field LWW is therefore **nearly complete** — most fields are scalars where it is right. The gap is
three shapes, all identifiable from the **declared field type**, so the macro selects the strategy
automatically.

> **Sets deserve priority.** `{Alice}` plus concurrent adds of Bob and Carol resolves under
> whole-entity LWW to `{Alice, Bob}` *or* `{Alice, Carol}` — a concurrent add **silently revokes** the
> other person. On a permissions list that is a security-adjacent correctness bug.

### 8.4 Optimistic concurrency

Per-field merge fixes **structural** conflicts. It does nothing for **semantic** ones: a handler reads
`seat.occupied == false`, writes `occupied = true`; two run concurrently on load-balanced nodes, both
read `false`, both write, merge picks one — and the loser's client believes it got the seat.

**OCC: apply a write only if the field still holds the value that was read.** Four independent
motivations — restore (§14.3), stale warm-start (§15.7), offline replay (§8.5), and load-balanced
read-modify-write (§11.3).

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

### 8.5 Conflicts are recorded, not replicated

Two situations produce genuine divergence:

- **Offline replay.** A node holds an **outbox** of un-acked writes; on reconnect it compares them
  against merged state and detects its own losses locally.
- **Partition heal.** Both sides committed successfully with their own durability, so neither has a
  pending outbox — detection instead happens **during anti-entropy repair**, when an incoming value
  beats a *locally originated* one.

Outbox detection does not generalize to partition heal; both mechanisms are needed.

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
4. **Deterministic content hash**, since Merkle leaves hash it (§5.3) and must agree across languages.
5. **Implementable in Rust, TypeScript, Python, Swift, Kotlin, C, C++, C#** (§10.1).

Requirement 3 is the one that reshapes everything. Once every field carries its own timestamp and
possibly a precondition, **the record is already field-granular** — and shipping whole entities on top
of field-granular metadata is pure waste.

### 9.2 Three layers

| Layer | Encoding | Who must implement it |
|---|---|---|
| **Header** | fixed layout, fixed offsets | every node |
| **Field entries** | varint-framed, custom | every node that merges or stores |
| **Field values** | canonical CBOR | only nodes with the schema |

**A `Logged` node needs no CBOR library at all** — it reads the header at fixed offsets, skips the
field section by length, and stores the bytes. That makes the archival appliance role, and the
polyglot minimum, dramatically cheaper to implement.

### 9.3 The record

**Header** — fixed layout:

- `version`, `record_type` (set / delete / checkpoint)
- `scope_id` (§5.1)
- `type_id` — the qualified type (§3), interned
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
making convergence depend on NTP discipline across every node; and `created_at` is an RFC3339 **string
compared lexicographically**, which is fragile — format drift (`Z` vs `+00:00`) or trailing-zero
variance changes both ordering and equality — costing ~30 bytes and a parse where a fixed-width
integer compare would do.

> **Scope limit.** HLC fixes *causally-related* writes misordered by clock skew. Genuinely concurrent
> writes still resolve by arbitrary tiebreak — HLC is no substitute for §8.

### 9.4 Field ids are name hashes

`field_id` is a 32-bit hash of the field name, **collision-checked at macro-expansion time** within
each type.

This avoids protobuf-style manual numbering and its bookkeeping — no registry, no "never reuse a
number" discipline, and ids stay stable when fields are reordered. A rename is a new field, which is
correct: renaming a field *is* a schema change.

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

The header and field framing need no library anywhere — fixed offsets and varints.

### 9.7 Debuggability

JSON is **not** a co-equal wire encoding. It is a **rendering**: a debug tool decodes a record to JSON
for inspection, using the schema when available and field-id-keyed output when not. This keeps
inspectability without a second encoder on the hot path, and without every node having to implement
two formats.

### 9.8 Envelope, not record

`tx` is request-scoped and meaningless remotely; schema version is per-batch. Both ride the batch
envelope. Per-plane envelopes carry transport/session metadata; the record carries only what is
intrinsic to the mutation.

### 9.9 Retire the WebSocket transport

Once browser nodes run wasm iroh and polyglot nodes speak ALPN planes, the WS stack — `MykoMessage`
(`wire/message.rs:42`), `ws_handler`, autosocket, `ws:m:*` — is a **second transport for no remaining
capability gain**. Retiring it collapses two protocols, two identity models, and two reconnection
strategies into one. Largest simplification available; largest migration risk. Sequenced last (§18).

### 9.10 What explicitly stays

The **capability trait system** (re-rooted to `NodeScoped` in §11, otherwise untouched),
**`inventory`-based registration**, **`Arc<str>` interning on hot fields**, **the reflection
machinery**, and **hyphae**.

## 10. Transport and polyglot participation

Researched 2026-07-25 against iroh 1.0 (shipped 2026-06-15), which formalizes **wire-protocol
stability across minor versions *and languages***.

### 10.1 What each language gets

| | Endpoint / ALPN / streams | iroh-gossip | iroh-blobs |
|---|---|---|---|
| **Rust (native)** | yes | yes | yes |
| **Rust → wasm (browser)** | yes, **relay-only** | **yes** (since 0.33) | **no** (tracking issue open) |
| **Python / Swift / Kotlin / JS(NAPI) / C** | yes | **not exposed** | **not exposed** |

Official bindings: Rust, Python, Swift, Kotlin, JavaScript, C (Go community-maintained). The C binding
(`iroh-c-ffi`) is the route for myko's **C++ and C# ports**.

**The FFI is deliberately minimal:** endpoints, protocols, connections & multipath, custom relays.
Gossip and blobs are not on its feature matrix.

> **Verify before building on this** (§17, M3). The gossip-absence claim rests on the published feature
> matrix rather than per-language API references, and it is the most load-bearing external fact here.

### 10.2 The mesh splits along the gossip line

Reimplementing HyParView + PlumTree in five languages is rejected — that is precisely the manual
fan-out gossip exists to avoid. **Polyglot nodes participate over the ALPN planes instead.**

This costs latency, not correctness, because **anti-entropy (§5.3) is the authoritative convergence
path and gossip is a latency optimization on top of it.** Even if every gossip message were dropped,
periodic anti-entropy converges the mesh. Further, **direct pairwise replication over an ALPN stream
needs no gossip at all** — a polyglot node paired to a Rust node gets realtime updates, losing only
transitive delivery and onward relaying. **Realtime spokes on a Rust hub.**

### 10.3 Browser nodes

**iroh core and iroh-gossip both compile to wasm**, so a browser node is a genuine realtime
participant. Two constraints:

1. **Relay-only, always.** No UDP, so no hole-punching; every browser connection traverses a relay over
   WebSocket (still end-to-end encrypted). Relay capacity is infrastructure to own or pay for.
2. **No iroh-blobs in wasm.** Browser and polyglot nodes both need a **custom-ALPN snapshot path**, so
   one mechanism serves both and **iroh-blobs demotes to a native-Rust optimization**.

### 10.4 Build notes

- wasm requires `iroh = { version = "1", default-features = false }`.
- No NPM package; the pattern is an app-specific Rust wrapper via wasm-bindgen. `myko-core` already
  compiles to wasm, so **myko is the wrapper**.
- **JavaScript has two iroh paths**: browsers get wasm (relay-only, gossip available), Node/Deno/Bun get
  NAPI (direct connections, no gossip). Not feature-equivalent.

### 10.5 The myko ALPN

No ALPN usage exists in the codebase — greenfield.

#### 10.5.1 ALPN = plane, and the ALPN set is the role advertisement

iroh's `Router` dispatches by ALPN, so **a node advertises roles by which ALPNs it registers**. The
manifest says what a node *claims*; the ALPN set is what it will *accept*, verifiable by dialing.
**Where they disagree, ALPN wins.**

#### 10.5.2 The plane determines origin — no wire flag, ever

- Records on the **replication** plane apply as remote — apply + index only; no cascade, no produce, no
  re-broadcast.
- Records produced by a command on the **serve** plane are local-origin.

A hard constraint: if a plane carries both, the discriminator returns to the wire and the dataplane
spec's §5.1 win is lost.

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
version independently**. `myko/mesh/1` is the **polyglot minimum**.

**Bulk transfer likely warrants its own plane.** State snapshot (§15.2) and log backfill (§14.3) are
both cold-path, resumable, bulk workloads unlike request/response RPC. Splitting later is compatible.

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

### 11.1 Route at ingress, execute locally

Commands are a **primary integration path** between services — a client sends `CreateInvoice` and it
routes to whichever node owns billing. But **handlers rarely dispatch nested commands across service
boundaries**; a handler's nested commands are normally same-service and stay in-process.

Therefore:

- **`CommandHandler::execute` stays synchronous.** No breakage across myko or rship.
- **The routing table is primary** — `(command_id, scope) → nodes`, built from manifests (§4.2).
- **One interposition point:** `ws_handler.rs:1383 execute_command_job`, which today scans local
  inventory and errors on a miss (`:1447`). It becomes "resolve owner; if it's me, proceed exactly as
  today; else forward."
- **Cross-service nested calls get an explicit async API** — visibly different, so the network boundary
  is legible in handler code, and rare by convention rather than prohibition.

### 11.2 Loop safety

`RequestContext.lineage` is an in-process call chain with no hop count or TTL. Cross-node routing adds
a hop limit and a visited-node set.

### 11.3 Ownership: load-balance freely

When multiple nodes advertise a `command_id` for a scope, any may handle it. Horizontal scaling, no
rebalance story.

**This makes §8.4's OCC mandatory rather than optional.** Two concurrent commands touching the same
entity execute on different nodes; each reads, computes, writes. If both write the same field,
per-field merge does not help — they genuinely conflict. OCC is what turns that into a clean rejection
and retry.

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

> **A projection evaluates locally iff it is covered by a live subscription; otherwise it routes.**

That is a concrete decision procedure, not a policy. The store knows what it has because it knows what
it subscribed to; coverage is checkable — trivially for identical queries, tractably for simple filter
subsumption.

Remote execution is also right when: **selectivity** is low (replicating a million rows to compute ten
inverts the economics); **computation is shared** (`report_cache` and `compute_gates`,
`server/context.rs:246,249`, exist so N subscribers share one computation — local-only multiplies by
subscriber count); or **cold start** dominates.

### 12.3 Query-driven replication

**A `Cached` node's working set is defined by its live subscriptions**, not by its scope. Declare the
queries, views, and reports you need; matching entities replicate; projection happens locally.

This bounds memory by working-set size rather than tenant size, which is what makes browser nodes
viable regardless of organization size.

It is also less new machinery than it appears: myko already evaluates queries over `CellMap`s with
incremental diffs and pushes them to subscribers. **The change is that results land in a real local
store other projections can run over**, rather than being consumed by one subscription.

Three requirements:

- **Refcount entities across subscriptions**, so cancelling one does not evict entities another needs.
- **§12.2's coverage rule is load-bearing, not an optimization.** Without it, a query outside the
  subscription set returns incomplete results indistinguishable from "no matches" — silent and
  undetectable.
- **Report interest means the report's *inputs***, derivable from hyphae's dependency graph. For
  low-selectivity reports the answer is "compute remotely" instead.

**It relocates rather than removes the RAM question:** something must still evaluate subscriptions
against the complete set, so a `Stateful` node holding the whole scope must exist. The hard-bounded
case (browsers) is solved; the server case becomes a machine you can size (§17, M1).

### 12.4 Collapse the enum

`MykoMessage` carries ~12 variants that are structurally identical — `{Query, View, Report}` ×
`{subscribe, response, cancel, window, error}`. One generic envelope (`Subscribe{kind, id, params}` /
`Update{id, payload}` / `Cancel{id}` / `Error{id}` / `Window{id, …}`) collapses them with no loss.

## 13. The `NodeScoped` refactor

Clients become full nodes: `ServerScoped` is re-rooted as `NodeScoped`, and wasm gets a real reduced
backing instead of `unreachable!()`.

**Current shape.** Every capability trait descends from `ServerScoped` (`capability.rs:87`), whose sole
accessor is `__server_ctx() -> &Arc<MykoServerContext>` — and that method, the whole `server` module
(`lib.rs:91`), and every capability body are `#[cfg(not(target_arch = "wasm32"))]`. On wasm,
capabilities are `unreachable!()` stubs and `CommandHandler::execute` is a hardcoded error
(`command/handler.rs:198`). `MykoClient` holds a socket and tx-keyed dispatch maps with **no store at
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

> **Cascades must be suppressed.** Exact mode emits DELs, and `#[belongs_to]` cascades parent DEL to
> children while `#[owns_many]` deletes them outright. Through the normal path the cascade fires *on
> top of* the computed closure and deletes entities the restore intended to keep. The restore computes
> the exact desired end state, so it applies as an **authoritative batch with cascades suppressed**
> (§7.4).

**Atomic locally, eventually consistent globally** — batch emission is first-class, but there is no
distributed transaction.

**Scope containment** holds naturally via immutable binding (§5.1), but assert it. **Authorization**
requires a capability for the target scope, Exact arguably a stronger one than Merge. **Concurrency**
is handled by §8.4's preconditions.

### 14.4 Shape and UX

`RestoreEntityTree { root, as_of, mode }` is a **framework-provided command**, routed to a node serving
the scope that can reach a `Logged` peer covering `as_of`.

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

A `Cached` node bootstraps by declaring subscriptions (§12.3) and receiving matching entities.

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
writes, holding them in a **local outbox** and replaying on reconnect.

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

**Readiness is per-scope.** A node caught up on org 5 serves org 5 while org 12 still syncs. Applies to
browser nodes equally.

## 16. Testing strategy

- **Merge determinism** — conflicting writes applied in different orders converge identically; strategy
  selection per field type is deterministic.
- **Per-field independence** — concurrent edits to distinct fields both survive; OR-Set adds both
  stick; counters sum.
- **OCC** — a read-then-written field rejects on precondition mismatch; a blind write does not; a
  rejected write surfaces a retryable error.
- **Conflict recording** — offline replay detects its own losses via outbox; partition heal detects
  during anti-entropy; heal summary replicates while detail does not.
- **Coverage rule** — a query outside the subscription set routes rather than evaluating locally
  against a partial store.
- **Scope isolation** — anti-entropy never transfers data for an unserved scope; eviction emits no
  records; cross-scope references are rejected.
- **Log contiguity** — a gap disqualifies the range; checkpoint opens a new one; in-gap reads fail.
- **Recovery** — state rebuilds from a compacted log with merge metadata intact; recovery preserves
  original timestamps where restore does not.
- **Warm start** — a stale node refuses writes until reconciled; past the tombstone window it
  cold-bootstraps instead.
- **Routing** — ingress routes by `(command_id, scope)`; nested same-service commands stay in-process;
  hop limits terminate loops.
- **Polyglot** — a non-Rust node paired to a Rust hub receives realtime updates and converges via
  anti-entropy with no gossip.

## 17. Open items

These are measurements and one external verification. No architecture is blocked on them.

**M1 — entity cardinality per scope.** `Stateful` means materialized in memory, so a complete node
cannot serve a scope larger than its RAM. §12.3 removes this constraint for edge nodes but relocates it
to the complete tier. **Measure with internal benchmark services** (`bench_entities.rs` is precedent)
before sizing the complete tier. If organizations prove very large, the escape is a disk-backed state
store — assessed as viable but substantial: realtime updates are safe because hyphae evaluates
predicates **incrementally** (O(n) at subscribe, O(changes) steady-state), but `EntityStore` **is** a
hyphae `CellMap` (`store/entity_store.rs:36`), so store and reactive graph are one object, and
`select()` over a partial resident map returns silently incomplete results. That path needs
index-driven query evaluation, and whether hyphae's `CellMap` can front a partial backing store is
unverified.

**M2 — gossip topic count.** §5.7 puts a topic per scope; iroh-gossip maintains HyParView/PlumTree
membership **per topic**, so a node serving 1000 orgs holds 1000 memberships. Cheaply testable.

**M3 — iroh FFI gossip exposure.** §10.1's claim that non-Rust bindings lack gossip rests on the
published feature matrix rather than per-language API references. Verify before phase 6.

**Q1 — `Handler` without state.** Executing commands against state a node does not hold is undefined.
Likely coherent only if the handler's reads route, which makes it a placement question (§12.2).

## 18. Phasing

**Validation strategy:** benchmark with **internal myko demo services** before committing the wire
format, then perform a **scoped rship migration** for real-workload perf testing, then a **coordinated
release**. The early benchmark exists because the wire break is the least reversible step.

**Sequencing principle: land every wire break in one phase.** Each break is a migration for live
consumers.

0. **Prereqs.** Event-bus unification (landed, PR #25) supplies the single apply chokepoint this design
   depends on. Partial convergence work exists on `feat/iroh-dataplane` targeting wall-clock timestamps
   and whole-entity resolution; it does not match §8 and should be **rewritten rather than landed**.
1. **Item field schemas + merge strategy mapping** (§4.1, §8.3) — additive macro change. Determines
   what the record must carry, so it precedes the wire break.
2. **Benchmark services** — synthetic rship-shaped load measuring record size, merge metadata
   overhead, typed-store ingest, and field-addressed encoding (§9). Also answers **M1**.
3. **The wire break — all at once** (§9): the three-layer encoding, HLC, per-field merge metadata, OCC preconditions, opaque
   payload, NodeId `source_id`, qualified `item_type`, scope id, `actor`, explicit tombstones,
   envelope moves, record rename.
4. **Split state from log** (§7.2) — independent retention, role bits (§2). Build the log **indexed**
   and **compacted per `(key, field)`** from the start (§15.1, §15.5); retrofitting either is far more
   expensive than designing them in.
5. **State store + scope partitioning** (§5, §6) — typed store, per-field merge, OCC, tombstones, and a
   Merkle index keyed per `(item_type, scope)` with scope-intersection negotiation. **Do not build a
   per-type Merkle index and retrofit scope** — that is a rewrite.
6. **Manifests + membership** (§4.2) — schema discovery and a routing table with no routing semantics.
7. **ALPN planes** (§10.5) — `myko/mesh/1` first; this makes polyglot participation real. Design
   `myko/serve/1` against §12.4's envelope, not a port of `MykoMessage`.
8. **`NodeScoped`** (§13) — start with the un-gate spike.
9. **Query-driven replication** (§12.3) — `Cached` role, subscription-defined working sets, coverage
   rule. This is what makes browser nodes viable.
10. **Command routing** (§11) — ingress routing, hop limits, load balancing.
11. **Conflict recording + offline** (§8.5, §15.6) — outbox, detection, heal summaries.
12. **Time travel** (§14) — routed historical reads, then `RestoreEntityTree`.
13. **Scoped rship migration + perf validation**, then coordinated release.
14. **Retire the WebSocket transport** (§9.9) — last; highest-risk migration, nothing depends on it.

## 19. Sources

**iroh capability claims in §10** (retrieved 2026-07-25): [iroh 1.0](https://www.iroh.computer/blog/v1) ·
[WASM/browser support](https://docs.iroh.computer/deployment/wasm-browser-support) ·
[language bindings](https://docs.iroh.computer/languages) ·
[iroh-ffi](https://n0-computer.github.io/iroh-ffi/) ·
[iroh-blobs wasm issue](https://github.com/n0-computer/iroh-blobs/issues/90) ·
[iroh 0.33](https://www.iroh.computer/blog/iroh-0-33-0-browsers-and-discovery-and-0-RTT-oh-my)
