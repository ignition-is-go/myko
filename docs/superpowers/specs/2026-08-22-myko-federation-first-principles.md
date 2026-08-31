# Myko Federation — First-Principles Foundation

**Date:** 2026-08-22
**Status:** Beginning of a design. This document records the requirements and architectural decisions
that should constrain the next specification. It intentionally does not select a transport, wire
encoding, consensus protocol, merge algorithm, or storage engine.

---

## 1. Why restart the design

Earlier federation work moved too quickly from desired behavior to mechanisms: peer registries,
gossip, iroh, ALPNs, role advertisements, LWW records, Merkle trees, and partitioning strategies.
Several of those mechanisms may still be useful, but choosing them first obscured the product Myko
is meant to provide.

This document starts with the application model and its invariants. Infrastructure choices must be
derived from these constraints rather than becoming constraints themselves.

### 1.1 Current alpha implementation boundary

The Myko 7 alpha keeps the foundation transport-neutral in `myko-federation`.
`myko-items` is the transport-neutral application-schema layer: consuming
applications declare `#[myko_item(service = "...", ...)]` entities (including
`scope_root` and `scoped_by` placement), and the macro generates stable typed
IDs plus basic typed queries. Owning service is part of the item schema and
every immutable `ItemMutation`; command contexts and raw-batch validation reject
cross-service mutations before commit. Typed item requests infer service from
the entity rather than accepting a caller-supplied string. `Node::query_items_in`
performs source/service/scope filtering and current-state materialization behind
a typed query API, so applications do not need to replay federation envelopes
or decode stored payloads. `Node::watch_items_in` extends the same typed query from an
initial replay cursor into gap-free live updates; it applies an entire atomic
batch before publishing each recomputed value.

The alpha also owns the service-side command boundary. `Node::begin_command`
admits only commands whose immutable origin is the executing node and returns a
typed `CommandContext`. The context lets application code query scoped items,
emit typed set/delete mutations, and commit one atomic batch plus a serialized
result without constructing federation envelopes, cursors, or batch IDs.
Applications can declare the stable payload/result contract with
`#[myko_command(service = "...", name = "...", result = Type)]`.
`DeclaredCommand` then owns request encoding, and
`Node::begin_declared_command` validates the wire identity and decodes the body
before the command is claimed. A schema mismatch therefore cannot strand the
command in an executing state.

`DeclaredCommandContext` is an owned execution capability, not a callback-only
borrow. A long-running application handler may carry it with its resident task
or session while awaiting model output, tools, or operator approval, then use
that same context to emit typed items and commit, reject, or retry. If the
process dies instead, the durable backend requeues the abandoned local claim;
application code does not reconstruct an atomic batch from lifecycle history.

`Node::pending_local_commands` and `dispatch_declared` own admission ordering,
local-origin filtering, decoding, atomic commit, and malformed-body rejection.
A handler explicitly classifies failures as terminal `Reject` or transient
`Retry`; retries append a durable reason and return the command to the ordered
pending set. Applications therefore do not rescan raw lifecycle envelopes or
conflate temporary local dependency failures with invalid domain commands.
`Node::watch_pending_declared` and the service-wide
`watch_pending_local_service_commands` extend that pending set into a gap-free
work feed. Myko materializes restart catch-up at one history boundary, then
delivers new local submissions and retries from its lossless event stream.
Foreign replicated submissions remain projections and never become executable
work, while supervisors can use bounded receives for cooperative shutdown
without timer-polling durable history.

The transport-neutral `CommandClient` contract supplies the same unclaimed
submit/query/cancel surface for an embedded `Node` and an authenticated
`IrohCommandClient`. Native clients select Iroh without changing the
application command API. WebSocket remains an optional short-lived edge
adapter rather than the framework's client or node foundation.
`Node::watch_command` and `IrohCommandClient::watch_command` extend that
surface with one current-then-live lifecycle stream. The node reconstructs the
initial state from the same bounded history prefix used to establish the
subscription, preventing a query-to-subscribe gap. Open native command streams
are reauthorized on policy revision and close immediately after revocation, so
clients can await terminal state instead of polling command queries.

Scope-level command state uses the parallel `CommandStateClient` contract.
`CommandStateRequest` collects bounded pages of current lifecycle state for one
declared service/scope/command type against a fixed serving-log ceiling. Entries
retain both source admission order and the serving position of their latest
authoritative transition; stale replicated transitions cannot regress the
catalog. `CommandStateSnapshot::declared` decodes application bodies and typed
results without exposing event envelopes. `CommandStateStream` and
`IrohCommandClient::watch_commands` continue the completed snapshot through a
filtered replay-then-live `FollowCommands` stream. Every page and open stream is
authorized independently, policy revocation closes the stream, and reconnect
starts from a fresh cursor-stable snapshot.

The parallel `ItemClient` contract supplies bounded current-state reads for an
embedded `Node` and an authenticated `IrohItemClient`. A request identifies the
declared item schema and scope, with service generated from that schema, and may either name an already
replicated source or select the serving node's own authoritative state. The
response is paginated by stable item ID. Its first page fixes an immutable log
ceiling, and every continuation carries that ceiling, so concurrent commits do
not create gaps or duplicates in the collected snapshot. Each page is bounded,
schema-validated, and authorized independently. Myko's client collector checks
source, server, schema, cursor, ordering, and page limits before rehydrating the
normal generated typed query; applications do not replay history or repair
pagination. This one-shot surface is for initial screens and short-lived
consumers. Callers that want incremental processing may consume pages directly.
Each current-state entry also retains the authoritative source-log position and
mutation index of its latest set. `ItemProjection::values_by_last_change` makes
that framework metadata available to typed application queries without adding
transport cursors to `#[myko_item]` payloads. Snapshot collection, embedded
watches, and native stream updates all preserve the same ordering metadata;
mutations from one atomic batch remain in their original order.
For native non-replica clients, `ItemQueryStream` turns that collected snapshot
into a transport-neutral typed materializer. `IrohItemClient` opens a
`FollowItems` request strictly after the snapshot cursor; the server establishes
a replay-then-live subscription before acknowledging the stream and sends only
matching schema mutations from complete atomic batches. Command envelopes,
unrelated item bodies, and raw scope history do not cross the client API. Each
policy revision reauthorizes the open stream, while a disconnect is recovered
by collecting a fresh snapshot and subscribing again. Embedded typed watches or
scoped replay-then-follow streams remain available when a durable local replica
is actually desired.

`myko-redb` is an optional immutable journal and cursor store, `myko-iroh` is an
optional authenticated native transport, and `myko-node` composes those two
adapters into a restartable long-running node with stable Myko and Iroh
identities plus durable peer followers. `myko-websocket-gateway` is a separate
short-lived edge adapter; it is not required by the node, journal, replication,
command, query, or subscription foundations. An application may supervise that
gateway over the same `Node` without changing native peer identity or durable
state. These mechanisms implement part of the constraints below without
changing their first-principles status.

Native pairing binds both stable identities in a versioned
`NativeNodeDescriptor`: the cryptographic Iroh endpoint and the Myko source log
it is expected to serve. A bounded identity handshake lets short-lived clients
verify the descriptor before application authorization or command submission.
Pinned durable followers verify the same source during every replication
handshake and ingest nothing on mismatch. Endpoint-only peer files remain a
legacy unpinned mode that deliberately retains replacement-history reset
semantics. Myko's default pairing exchange uses a separate bounded Iroh ALPN,
an expiring one-use bearer whose server retains only a verifier, an HMAC
transcript over both stable identities, and a shared six-digit comparison code.
Successful redemption neither grants application authority nor persists a
follower; those remain explicit application/operator decisions. The outer
ticket, QR, file, and discovery encoding remain pluggable.

This crate split is an implementation boundary, not the intended application
experience. Myko remains an application framework: its public facade should
compose schema registration, command execution, typed reactive queries,
persistence, federation, and subscriptions. Redb, Iroh, cursors, mutation
encoding, and replay are framework concerns unless an application explicitly
replaces an adapter.

Hyphae is the framework's first-class reactive graph. A durable adapter such as
Redb owns restartable history and materialization inputs; it does not replace
live cells, dependency tracking, reports, or derived views with request-time
reads. UI integrations such as `myko-gpui`, `myko-leptos`, and `myko-ratatui`
own lifecycle and rerender bridging rather than visual components or a second
state store. `myko-app` is the application boundary for explicitly registered
query, report, and view handlers. It retains the gap-free dependency drivers,
materializes their Hyphae pipelines once, and gives every transport the same
type-erased wire lifecycle without making the transport own the handler.

The intended system supports:

- long-running and short-lived nodes;
- durable state for long-lived data;
- a complete history of every accepted change;
- communication among services with different responsibilities;
- replication among instances of the same service for high availability and failover;
- graph-structured data, including edges with their own identity, values, and state;
- a peer-to-peer mesh without a mandatory central coordinator;
- application-defined multitenancy, ownership, and sharing;
- row-level security;
- realtime queries, views, and change delivery;
- low request latency, high throughput, and horizontal scalability.

High replication bandwidth is an acceptable tradeoff when it removes data-location routing from the
command path and lets capable replicas execute work locally.

## 2. Working system model

The system is composed of applications, services, nodes, scopes, principals, graph records, commands,
and subscriptions.

### 2.1 Application

An application defines its graph schema, scope-root types, commands, typed queries and views, access
policies, and service organization. Concepts such as `Organization`, `User`, and `Project` are
application types, not hard-coded Myko tenant types.

### 2.2 Service

A service is the boundary within which a command may atomically change authoritative graph state.
It owns:

- a set of node and edge types;
- commands and business invariants;
- atomic change batches;
- durable history for its changes;
- replication among its replicas.

Services are semantic atomicity boundaries, not necessarily one-to-one with binaries, processes,
crates, or deployment units. An application may package several services in one binary or distribute
one service capability across several deployments without changing the service boundary.

An item schema declares exactly one owning service. Myko includes that service
in mutation identity and rejects a command batch that attempts to mutate an
item owned by another service. Cross-service work therefore uses an explicit
command or typed view rather than accidentally creating a second current-state
namespace for the same Rust entity.

### 2.3 Node

A node is one running participant in the mesh. Nodes may be long-running or short-lived and may
combine several capabilities. A node may host several services and participate in several scopes.

### 2.4 Replica

A replica is a node that holds authoritative state for a service and scope also held by other nodes.
Replicas do not own disjoint data shards. Every writable replica that is current and authorized for a
scope may accept and execute commands locally.

### 2.5 Principal

A principal is the authenticated identity behind an operation. A principal may represent a user,
agent, service, API credential, or node. Myko derives the principal from the authenticated connection;
untrusted request input cannot choose it.

## 3. Typed property graph

Canonical application state is a typed property graph.

```text
Node:
  (service, scope, type, id) -> typed fields

Edge:
  (service, scope, edge_type, source, target, edge_id) -> typed fields
```

Edges are first-class records rather than hidden relationship metadata. They have:

- stable identity;
- typed stored values;
- an owning service and scope;
- their own lifecycle and history;
- indexes and query participation;
- access policies;
- realtime updates;
- conflict behavior.

An edge may reference endpoints in another scope, but the edge itself belongs to exactly one scope.
Cross-scope references do not implicitly replicate the referenced record or everything reachable
from it.

## 4. Complete immutable history

Every accepted authoritative mutation produces an immutable, durable historical record. Current
graph state is a materialization of that history, not the only surviving representation of truth.

Commands alone are insufficient history. Re-executing an old command under changed code, changed
dependencies, or nondeterministic conditions may produce a different result. History must preserve
the canonical graph changes accepted from the command.

### 4.1 Atomic change batch

A command produces one immutable change batch containing all affected nodes and edges within one
service and one scope.

```text
Command
   |
   v
Validate service invariants
   |
   v
Commit immutable change batch
   +--> materialized graph state
   +--> indexes and reactive queries
   +--> durable history
   +--> realtime replication
```

Replicas and observers apply the complete batch or none of it. Realtime consumers must not observe
half of a command.

Different scopes can have different authorized replica sets. A batch spanning scopes could not be
atomically visible to a peer authorized for only one side, so cross-scope work uses causally linked
commands or a durable workflow. Cross-service work follows the same rule.

### 4.2 Undo

Undo never deletes or rewrites history. It appends a compensating command and change batch that
references the operation it compensates.

```text
A: name = "Old"
B: name = "New"
C: undo B -> name = "Old"
```

All three changes remain available. Undo normally targets an atomic command batch so domain and graph
invariants are preserved. A service may validate or customize an inverse when an operation cannot be
mechanically reversed.

### 4.3 Time travel

Time travel reconstructs transient graph state at a historical position. It is a read operation and
does not enter normal replication.

```text
state at P = snapshot at or before P + changes through P
```

Historical positions must describe distributed causality correctly; wall-clock timestamps alone are
not sufficient to identify a consistent historical cut.

### 4.4 Forward restore

Restore never rewinds shared history. It:

1. reconstructs the selected graph closure at a historical position;
2. compares it with current state;
3. creates a proposed present-day change batch;
4. validates current service invariants;
5. commits the restoration as a new forward change.

The changes between the historical position and the restoration remain permanently visible.

### 4.5 Snapshots and retention

Snapshots accelerate bootstrap and historical reads but do not replace history. The complete history
must remain available on an authorized, redundant durability set. Operational replicas may retain a
recent window rather than the complete archive as long as complete history remains durably reachable.

## 5. Multi-writer replication

Horizontal scalability requires more than allowing every replica to receive an HTTP or socket
request. Every current, authorized replica must be able to execute a command against its local graph
without forwarding it according to data ownership.

```text
User or agent
      |
      v
Any capable replica
      |
      +--> execute against local state
      +--> durably append change batch
      +--> apply locally
      +--> replicate to peers
```

Within a scope there is no permanent primary and no record-to-node ownership partition. Replicas
exchange immutable changes and deterministically arrive at the same visible state.

### 5.1 Concurrent accepted changes

A command may reach local commitment and later have some or all of its visible values superseded by
a concurrent accepted command. Local commitment means the change permanently entered history; it
does not guarantee that its value remains the current winner after reconciliation.

All competing changes remain available for inspection, undo, time travel, and forward restoration.
The command lifecycle must make partial or complete supersession visible.

The exact causal metadata, deterministic ordering, and merge strategies remain to be specified.

### 5.2 Selective coordination

Some business invariants cannot be preserved by asynchronous merge. Two replicas cannot both promise
the last seat, allocate the same unique name, or spend the same exclusive resource merely by choosing
a deterministic winner afterward.

The default path remains local and convergent. Commands that protect an exclusive invariant may use
explicit, narrowly scoped coordination before local commitment. Coordination is selected because of
the invariant, not because a node owns the affected data.

## 6. Observable command lifecycle

A command has a durable, observable, multi-state response rather than one ambiguous terminal reply.
A representative lifecycle is:

```text
Submitted
  -> Executing
  -> CommittedLocally
  -> Replicating
  -> Replicated
  -> Reconciled
```

It may instead become `Rejected` or `Cancelled` before commitment, or
`ReplicationDelayed` after commitment. Cancellation is itself durable and
idempotent. It prevents a later local claim or commit, while a command that
already committed remains committed if cancellation loses that terminal-state
race.

`CommittedLocally` is the decisive business transition:

- validation succeeded;
- the atomic change batch is durable locally;
- the change is permanently part of history;
- the domain result is available;
- local realtime observers may see it.

A replication failure after this point cannot turn the command into a rejection. Doing so would make
client retries duplicate already committed work.

`Replicated` describes a concrete durability target, such as durable acknowledgment by a requested
number of eligible replicas. It must not make an unprovable claim of permanent global convergence.

`Reconciled` may report that a command is fully visible, partially superseded, or fully superseded.

Every command has a stable ID. A caller can disconnect, reconnect through another node, and resume
observing its lifecycle. Re-submitting the same ID is idempotent and does not execute the command
twice.

## 7. Node capabilities

Node roles remain useful when treated as composable capabilities derived from requirements rather
than rigid node classes.

### 7.1 State

Holds complete current state for each advertised `(service, scope)` and can maintain typed queries and
views. State is complete relative to the granted scope, not the entire mesh.

### 7.2 History

Durably retains immutable change history over an explicitly advertised range and serves replay, undo,
bootstrap, and time travel.

### 7.3 Handler

Contains command implementations. A state-dependent handler executes only when the corresponding
State capability is present and current.

### 7.4 Subscriber

Consumes typed live views or durable change streams without becoming an authoritative state replica.

### 7.5 Relay

Provides mesh connectivity without interpreting application data.

A typical long-running service replica is `State + Handler + recent History`. An archival node may be
`History` only. A browser or short-lived agent is generally a `Subscriber`.

Roles vary by service and scope on one physical node. They do not introduce command routing based on
data ownership.

## 8. State-node lifecycle

A State node progresses through explicit readiness states:

```text
Discovered
  -> Bootstrapping
  -> CatchingUp
  -> Ready
  -> Degraded or Isolated
  -> Draining
```

### 8.1 Bootstrap without a gap

The node discovers authorized peers, selects a State peer for a snapshot, and selects History peers
for missing changes and verification. It begins buffering realtime changes before installing the
snapshot:

1. establish realtime replication after snapshot frontier `F`;
2. download and verify the snapshot at `F`;
3. install the snapshot;
4. replay buffered and historical changes after `F`;
5. reconcile with reachable peers;
6. advertise readiness.

### 8.2 Scoped readiness

Readiness is tracked per `(service, scope)`. A node may be ready for Project A while Project B is still
bootstrapping. It cannot advertise State readiness merely because stale local data exists.

### 8.3 Degraded operation

Loss of replication peers changes an explicit health state rather than silently invalidating local
state. Commands whose policy permits local durability may continue to commit and report
`ReplicationDelayed`. Commands requiring coordination or replicated durability wait or fail cleanly.

### 8.4 Restart and drain

A restarting node loads its local snapshot and frontier, catches up, reconciles, and only then becomes
ready. A draining node stops advertising handlers, finishes active work, replicates outstanding
changes where possible, and withdraws its capability advertisements. No ownership transfer is
needed.

## 9. Cross-service communication

Services have different responsibilities and may perform work beyond materializing graph state. They
also need typed, live access to information owned by other services.

Cross-service communication has three explicit forms.

### 9.1 Typed live queries and views

The owning service publishes typed query and view contracts. A consumer receives:

1. a consistent initial result;
2. a cursor identifying the source history position;
3. typed additions, updates, and removals;
4. explicit liveness and resynchronization state.

The consumer maintains the result only as live subscription state. It does not persist a foreign
authoritative replica or treat a disconnected last value as current.

In Rust, the value, cursor, and liveness revision are exposed coherently through
a Hyphae cell. Transport adapters drive that cell from the same typed
snapshot/follow contract; application reports, views, and UI adapters compose
the cell without polling the transport or persistence backend. The cursor type
is part of the handler contract: a direct query commonly uses one
`LogPosition`, while a joined view may expose a composite frontier covering
several durable dependencies.

A service may also publish typed provisional progress as coalescible live state. This progress is
not immutable history and has no replay guarantee. Each source supplies a monotonic sequence per
live topic so a filtered subscriber can detect loss under bounded backpressure without confusing
unrelated topics for gaps. After a gap or reconnect, the subscriber must query or resume a durable
stream to recover authoritative state.

Views may compose typed live views from several services:

```text
Presence live view ----+
                       +--> reactive computation --> derived live view
Capability live view --+
```

The derived view is recomputed live and is not written into authoritative graph state.

If the computation determines that authoritative state should change, it issues an explicit typed
command, including when the target is the same service. Reactive recomputation never silently mutates
the graph.

### 9.2 Typed durable change streams

Durable change streams deliver every committed transition and resume from history. They are distinct
from live state views, whose intermediate states may be coalesced under backpressure.

Durable streams are suitable for operational effects such as notifications, external API calls,
audit export, and explicit workflows. Consuming a durable stream does not implicitly create graph
state; authoritative mutation still requires a command.

Consumer and peer cursors are node-local operational metadata, not replicated authoritative graph
state. A cursor advances durably only after the corresponding immutable batch was ingested or its
effect checkpointed. A crash before cursor persistence may replay idempotent work; persisting the
cursor first could omit history and is forbidden. Cursor storage is transport-neutral and keyed by a
transport namespace plus stable peer or consumer identity. A peer checkpoint also records the
source node identity whose position space it describes. If the same transport identity begins serving
a different source history, the consumer replaces the checkpoint with that identity and resumes from
the new history's beginning; it must never reuse a position from the old source.

A subscriber granted one scope does not need the source's complete log. A scope-filtered durable
stream omits unrelated event bodies while retaining a watermark in the source log's position space.
Included event positions may therefore contain gaps, and an empty batch may still advance the
watermark across unrelated activity. The consumer keeps that cursor per `(source, scope)`, validates
that every included event belongs to the requested scope, and never treats it as a cursor for another
scope or for full-node replication. The cursor value therefore carries both source and scope identity,
not merely a numeric position. If the same transport peer begins serving a different source history,
the consumer discards the old position and replays the scope from its beginning.

A short-lived client also needs to discover which scopes it may choose without first knowing their
identifiers. Scope discovery is a bounded, lexically ordered, cursor-paginated projection of scope
identifiers—not event bodies. A transport evaluates the same exact-scope read policy independently
for every candidate before disclosure. Thus discovery cannot be used to enumerate unauthorized
scope names, and selecting a returned scope still requires a separate authorized scoped query or
subscription. A client rejects pagination that changes source node identity mid-scan.

### 9.3 Typed commands

A command asks the owning service to perform an authoritative operation. Cross-service workflows use
causally linked service-local commands rather than hidden distributed transactions.

Application handlers receive a Myko-owned command context rather than direct
access to the journal protocol. They express domain decisions as typed item
mutations and one result; Myko validates and commits those mutations atomically,
records the lifecycle transition, and enforces the execution placement policy.
An application should not need to assemble `ChangeBatch` values or inspect raw
event envelopes for ordinary command execution.

Handler failure has two explicit meanings. `Reject` records a terminal domain
decision. `Retry` records a non-terminal reason and releases the claim so the
same stable command can be dispatched again without restarting the node.

```rust
#[myko_command(service = "planning", name = "planning.rename", result = bool)]
struct RenameProject {
    project: ProjectId,
    title: String,
}
```

The stable ID, scope, and principal wrap this typed body in a
`DeclaredCommand`; they are admission metadata, not repeated application
payload fields.

When a service makes a durable decision using a foreign live view, its history records the exact
source revisions it observed. This preserves provenance without copying the foreign graph into its
own authoritative state.

## 10. Scopes

`ScopeId` is the application-defined unit of trust, federation, replication, history access, and
atomic visibility for a related portion of the graph. A scope is not synonymous with an organization
or tenant.

An application may define several scope-root types:

```rust
#[myko_item(scope_root)]
struct Organization { /* ... */ }

#[myko_item(scope_root)]
struct Project { /* ... */ }

#[myko_item(scope_root)]
struct UserPrivateSpace { /* ... */ }
```

Every root instance creates a stable, distinct scope:

```text
Organization 1 -> ScopeId(org-1)
Organization 2 -> ScopeId(org-2)
Project A      -> ScopeId(project-a)
Project B      -> ScopeId(project-b)
Alice Private  -> ScopeId(alice-private)
```

Records declare how they are scoped:

```rust
#[myko_item(scoped_by = Organization)]
struct OrganizationMember { /* ... */ }

#[myko_item(scoped_by = Project)]
struct Task { /* ... */ }
```

A record belongs to one scope while retaining typed references to records in other scopes. Scope-root
relationships do not implicitly merge scopes or grant access transitively.

### 10.1 Scopes may span services

The same `ScopeId(project-a)` may organize related state in several services:

```text
Planning service:   Project A tasks             -> project-a scope
Document service:   Project A documents         -> project-a scope
Automation service: Project A automation rules  -> project-a scope
```

The common scope controls federation grants across those services. Each service still owns its types,
commands, atomic batches, and history.

## 11. Capability-scoped federation

Mesh reachability does not imply transitive data access.

```text
Company A <--> Company B <--> Company C
```

This topology does not authorize A to receive C's data, C to receive A's data, or B to re-share data
between them. Graph state federates only through explicit, non-transitive, scope-specific grants.

```rust
ScopeGrant {
    scope_id: project_a,
    grantee: organization_2,
    permissions: {
        read,
        subscribe,
        write,
        history,
        reshare,
        admin,
    },
}
```

The precise grant representation remains open, but the semantic properties are fixed:

- grants target one scope;
- permissions are explicit;
- resharing is denied unless explicitly granted;
- connecting to a peer does not inherit that peer's grants;
- replication occurs only among peers with compatible authorization;
- accepting replicated writes requires write permission, not merely network reachability;
- history access may cover current state only, changes since sharing, a bounded window, or complete
  history;
- revocation stops future replication, subscriptions, commands, history access, and delegation.

Transport adapters present authenticated request metadata to one transport-neutral access-policy
contract before exposing history or applying a mutation. That metadata includes the transport
principal, operation, exact service/scope when known, command identity and claimed command principal,
or exact live topics. A permissive bootstrap policy may be used for local development, but it is an
explicit policy rather than an implicit property of an authenticated connection. Durable grant
materialization and policy evaluation remain application/node concerns shared by every transport.
Long-lived transport streams re-evaluate their original authenticated request
when the installed policy changes. A newly denied stream closes explicitly;
revocation is not limited to rejecting the principal's next connection.

Revocation cannot make an untrusted party forget plaintext it previously received. Granting State
access is therefore an infrastructure trust decision. Cooperative local eviction removes a node's
copy and emits no graph deletion.

### 11.1 Federation closure

A scope grant does not mean blindly traversing and copying every reachable record. The application
schema defines the scope's replication closure:

- scoped descendants and internal edges are included;
- external references remain references;
- selected foreign information is obtained through typed live views;
- sensitive child data may require another scope grant.

This limits proliferation while preserving typed cross-scope relationships.

## 12. Row-level security

Scope federation decides which machines may physically possess a scope. Row-level security decides
which authenticated principals may observe or mutate individual records inside that scope.

```text
Federation:
  May node B store Project A?

Row-level policy:
  May Alice read Task 42 inside Project A?
```

These layers cannot substitute for each other. RLS cannot protect plaintext from the operator of an
authorized State node.

### 12.1 Application-defined authorization graph

Applications define ordinary typed records for organizations, users, membership, ownership, and
sharing:

```text
User --member_of--> Organization
Organization --owns--> Project
Organization --has_access_to { permissions }--> Project
```

Sharing relationships are first-class stateful edges with their own values, commands, history, undo,
and realtime behavior.

Myko supplies generic primitives rather than a mandatory organization schema:

- authenticated `Principal`;
- `ScopeRoot` and stable `ScopeId`;
- typed access policies;
- access capabilities such as read, create, update, delete, subscribe, history, and share;
- enforcement across all graph access paths;
- an optional conventional organization model may be built on these primitives.

### 12.2 Typed reactive policies

Applications declare access policies over typed graph relationships. The exact Rust API remains to be
designed; conceptually:

```rust
#[myko_policy(for = Task)]
fn task_access(principal: PrincipalRef, task: TaskRef, graph: PolicyGraph) -> Access {
    if graph.exists::<ProjectMember>((task.project(), principal.user())) {
        Access::READ | Access::SUBSCRIBE
    } else {
        Access::NONE
    }
}
```

Policies participate in reactive dependency tracking. When membership or sharing changes, active
subscriptions immediately add or remove newly visible rows without reconnecting.

Policies must compile into indexed, incremental graph predicates. Invoking arbitrary policy code once
per candidate row would violate the throughput requirement.

### 12.3 Enforcement

The same policy model applies to:

- typed queries and views;
- initial subscription results and every subsequent update;
- command admission and affected records;
- graph nodes and graph edges independently;
- durable change streams;
- history, undo, and time travel;
- derived cross-service views at the publishing service.

Current read access must not automatically expose every historical value. History requires explicit
permission. The safe default evaluates current authorization before historical access; applications
may deliberately define historical, as-of authorization where required.

RLS belongs in Myko's graph and query layer, not solely in a particular SQL backend, because current
state and subscriptions may be maintained in memory or by other storage engines.

## 13. Performance principles

The architecture protects latency and throughput by construction:

- capable State replicas execute commands against local data;
- no record-owner lookup or data-location forwarding occurs on the ordinary command path;
- realtime queries and access policies are maintained incrementally;
- replication is asynchronous unless a command requests stronger durability;
- immutable changes are batched for persistence and dissemination;
- control traffic, replication, live views, and durable streams apply independent backpressure;
- joining nodes bootstrap from snapshots and replay only the required suffix;
- scope grants prevent unrelated data from proliferating across the mesh;
- global coordination is avoided except for explicitly exclusive invariants.

Concrete throughput, latency, bootstrap, recovery, and subscription-scale targets must be established
before selecting the final wire and storage designs.

## 14. Current invariants

The following statements summarize the foundation established so far:

1. Every accepted authoritative change remains permanently available in immutable history.
2. Undo and restore append forward changes; neither rewrites history.
3. Graph nodes and stateful edges are equal first-class records.
4. A command atomically changes records within one service and one scope.
5. Every current, authorized writable replica may execute commands locally.
6. Data is not assigned to replicas through ownership-based sharding.
7. Concurrent accepted changes remain in history even when their visible values are superseded.
8. Coordination is reserved for invariants that cannot be reconciled after the fact.
9. Command completion is a durable, observable lifecycle rather than one boolean response.
10. Cross-service state context comes from typed live queries and views.
11. Live derivations do not become authoritative stored copies.
12. Authoritative mutation occurs only through explicit commands.
13. Durable change streams and coalescible live state subscriptions are distinct contracts.
14. Applications may define multiple scope-root types.
15. A ScopeId is a federation and trust boundary, not necessarily an organization or tenant.
16. Federation grants are explicit, scope-specific, and non-transitive.
17. RLS is an application-defined, typed, reactive graph policy enforced by Myko.
18. Infrastructure trust and end-user authorization are separate layers.

## 15. Next design work

The next specification work should define the immutable change model while preserving every invariant
above. It must cover:

- identifiers for commands, batches, changes, records, scopes, services, nodes, and principals;
- causal metadata and historical positions;
- representation of field and edge changes;
- deterministic reconciliation of concurrent changes;
- explicit deletion and delete-versus-update behavior;
- partial and complete supersession reporting;
- preconditions and selective coordination;
- atomic application and persistence of batches;
- snapshot frontier and bootstrap verification;
- history indexes for time travel and graph restoration;
- schema evolution and generated cross-language representations.

Only after that model is understood should the design select replication topology, transport, wire
encoding, storage layout, anti-entropy mechanism, and membership protocol.

## 16. Open questions

These questions are intentionally unresolved. They are ordered roughly by dependency so future work
can resume from the top without reopening the settled invariants in §14.

### 16.1 Change and causality model

1. What is the canonical shape of an immutable change batch and an individual record change?
2. What causal metadata lets replicas distinguish predecessors from concurrent changes without
   unbounded metadata growth as short-lived nodes come and go?
3. How is a stable historical position or consistent cut represented across several writers?
4. Does history expose a causal graph directly, a deterministic linearization, or both?
5. What deterministic ordering selects the visible value for concurrent scalar writes?
6. Which merge strategies are built in for counters, sets, maps, sequences, and other structured
   fields, and how does an application select or extend them?
7. Is reconciliation defined per field, record, edge, or command batch when several concurrently
   changed values interact?
8. How are partial and complete supersession represented and reported back through a command's
   observable lifecycle?
9. What are the semantics of concurrent deletion and update, recreation after deletion, and deletion
   of a node with live edges?
10. How does schema evolution preserve replay and interpretation of old changes indefinitely?

### 16.2 Atomicity and coordination

1. How does the storage API atomically persist a change batch, its current-state materialization, and
   the local command lifecycle transition?
2. Which invariants can use optimistic preconditions while remaining locally executable, and which
   require coordination before `CommittedLocally`?
3. What coordination primitive protects exclusive invariants without introducing permanent record
   ownership?
4. How is coordination scoped, timed out, recovered after failure, and represented in history?
5. How are causally linked cross-service or cross-scope commands observed and debugged as one business
   workflow without claiming distributed atomicity?

### 16.3 Scope model

1. How is a `ScopeId` created, and is it derived from or merely associated with its root record?
2. Can a scope root itself live inside another scope, and what semantics does nesting have beyond an
   ordinary typed reference?
3. May a record ever move between scopes? If so, is movement a delete-and-create workflow or a
   framework-supported migration?
4. How does a service attach records to a scope whose root is defined and stored by another service?
5. How are scope kinds and their schemas identified across services and language bindings?
6. How does the schema declare the replication closure of a scope without accidentally traversing
   arbitrary graph references?
7. Are there explicit system and public scopes, and who is authorized to create or modify them?
8. What application operation creates, changes, and revokes scope grants, and where is the grant's
   authoritative history stored?
9. Can a grant cover several services participating in the same scope while assigning different
   permissions to each service?
10. How are delegated and reshared grants constrained, traced to their issuing authority, and revoked?
11. What history becomes visible when a scope is shared: current state, changes since sharing, a
    bounded interval, or complete history, and how is that choice encoded?
12. What does cooperative eviction remove locally, and how does a node prove that it no longer
    advertises or serves the revoked scope?

### 16.4 Replica membership, durability, and readiness

1. How does a node discover the authorized replica set for a `(service, scope)` without making one
   central registry mandatory?
2. Which changes to membership require an epoch, and how are concurrent membership changes resolved?
3. What does it mean for a node to be caught up in a multi-writer system whose reachable peers may
   have different causal frontiers?
4. What exact readiness criteria permit a Handler to begin accepting commands?
5. May an isolated State node accept all convergent commands, only explicitly offline-capable
   commands, or none by default?
6. How does a command declare its requested durability target, and what is the default?
7. Does a durability target count local storage, distinct physical machines, distinct operators,
   geographic zones, or some declared failure-domain policy?
8. How are complete-history availability and redundancy continuously verified?
9. How are snapshots created without pausing writes, authenticated, chunked, resumed, and checked
   against their causal frontier?
10. How does a joining node choose among State and History peers without trusting one peer's snapshot
    blindly?
11. How much recent history must an operational State replica retain locally?
12. How are role and readiness advertisements scoped and withdrawn during crash, partition, and
    graceful drain?

### 16.5 Command lifecycle

1. What are the exact framework states, transitions, and terminal versus non-terminal conditions for
   a command?
2. Which lifecycle transitions are durably stored, and which are derived observations?
3. How does a caller resume command observation through another node?
4. Where is command-id idempotency recorded, how long is it retained, and how does it converge across
   replicas receiving the same command concurrently?
5. What constitutes a concrete `Replicated` receipt as membership changes?
6. When and how is later supersession attached to a previously completed command?
7. How are commands that wait for selective coordination represented without holding an unbounded
   request connection open?

### 16.6 Typed cross-service dataflow

1. How does a service publish and version its typed query, view, durable-stream, and command contracts?
2. How are a live view's initial result and subsequent updates tied to one gap-free source position?
3. What liveness states distinguish current, disconnected, resynchronizing, and invalid subscription
   data inside application code?
4. Which live-view updates may be coalesced, and how does a consumer discover that intermediate states
   were skipped?
5. What ordering and delivery guarantees do durable change streams provide across concurrent writers?
6. Where are durable stream cursors stored, and what idempotency support is available for external
   side effects?
7. How are derived views composed across services without deadlocks, dependency cycles, or unbounded
   fan-out?
8. How does a durable decision record the foreign view revisions and interpreted values that informed
   it?
9. How does subscription failover choose another service replica and resume without duplicating or
   omitting observable changes?

### 16.7 Row-level security

1. What Rust API expresses typed policies while keeping them analyzable, indexable, and reactive?
2. Which access capabilities are framework-standard, and how may applications add business-specific
   permissions?
3. How are policy dependencies discovered so membership and sharing changes invalidate affected live
   results immediately?
4. How are policies applied before query work to avoid both per-row overhead and unauthorized timing,
   count, identifier, and error side channels?
5. How are command-level authorization and record-level authorization composed for create, update,
   delete, and edge mutations?
6. How is access to an edge determined when its endpoints live in different scopes?
7. Does historical access use current policy, policy evaluated at the historical position, or an
   explicitly selected mode?
8. How do services enforce RLS on foreign typed views without exposing their internal authorization
   graph?
9. How are policy definitions versioned so replay, audit, and authorization decisions remain
   explainable after code changes?
10. Which administrative operations may bypass ordinary RLS, and how are they isolated and audited?

### 16.8 Transport, replication, and storage mechanisms

1. Which peer-to-peer transport satisfies native, browser, and multi-language requirements?
2. Is realtime dissemination gossip-based, direct fan-out, tree-based, or adaptive by replica-set
   size?
3. What anti-entropy index compares scoped current state and complete history efficiently?
4. How are realtime changes, snapshots, history transfer, commands, views, and control traffic
   separated for backpressure and prioritization?
5. What canonical wire encoding remains evolvable and can be generated for every supported language?
6. Which storage abstractions support current graph state, immutable history, snapshots, command
   lifecycle, and policy indexes without coupling Myko to Postgres?
7. How are malicious, malformed, unauthorized, or resource-exhausting replication messages rejected
   before expensive decoding and application?
8. How are node identity, scope grants, authenticated principals, encryption keys, and key rotation
   represented and distributed?

### 16.9 Scale targets and validation

1. What command throughput and commit-latency targets must one replica and one scope sustain?
2. How many writable replicas may participate in one hot scope?
3. How many scopes, services, principals, and live subscriptions may one node serve?
4. What replication lag, failover time, and reconnect time are acceptable?
5. How large may current state and complete history become, and what bootstrap time is acceptable per
   gigabyte?
6. What are the expected ratios of graph nodes to edges and of stored facts to derived live results?
7. Which workload represents the worst expected RLS graph traversal?
8. How should benchmarks model concurrent writers, partitions, reconnects, scope sharing, revocation,
   and subscription backpressure?
9. Which existing Myko and downstream workloads should validate the design before committing the wire
   format?
