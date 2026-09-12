# Assigned reactive handler: candidate C — ingress-owned routing

## Problem

The durable client retains one connector and `HandlerRequest`, while
`IrohHandlerConnector::at` changes only the serving destination. The target
milestone needs one stable reactive handle through loss of an executor, without
turning storage holders into typed executors or claiming that assignment,
manifests, commitments, or local cursors prove readiness. This candidate puts
selection and proxying in an application-capable ingress node. The ingress can
fail over to another assigned ingress; it owns backend choice and never exposes
that choice to applications.

## Usage (caller's view)
The application API remains unchanged:

```rust,ignore
let handle = client.follow_query_reactive(Some(origin), scope, &query)?;
let value = handle.live_collection().items();
```

The framework opens through an assigned ingress. The ingress opens the backend
under the same inspected contract and returns a proxied stream:

```rust,ignore
let (frame, stream) = ingress.open_described(request.clone(), contract).await?;
```

On ingress loss, the retained owner publishes `Resynchronizing`, reconnects to
another assigned ingress, and receives a fresh snapshot before publishing
`Current`. Commands pass through the ingress gate immediately before dispatch:

```rust,ignore
ingress.dispatch(command, dependency, provenance).await?;
```

## Shape
The node layer owns an `AssignedIngressConnector`; core still owns the stable
reactive owner and publication rules. An ingress must have an application
loaded, so it is not a storage-only node.

```rust,ignore
struct HandlerSelectors { source_node: Option<NodeId>, scope_id: ScopeId }
struct IngressAttempt { ingress: NodeId, contract: HandlerContract }

trait AssignedIngress {
    async fn observe_and_open(&self, request: HandlerRequest,
        observed: Option<HandlerContract>)
        -> Result<(HandlerFrame, Box<dyn HandlerConnection>), IngressError>;
    async fn dispatch(&self, command: Command, dependency: Dependency,
        provenance: Provenance) -> Result<CommandResult, IngressError>;
}
struct AssignedIngressConnector { /* ingress candidates + observer */ }
impl AssignedIngressConnector {
    async fn open(&self, request: HandlerRequest)
        -> Result<IngressAttempt, HandlerError>;
}
```

The client connector obtains a fresh exact-scope/service assignment
observation, then tries only assigned ingress nodes. It carries the original
`source_node` and `scope_id` unchanged; serving ingress and backend identities
are routing metadata, never replacement selectors. The ingress authenticates
the caller and forwarding provenance, checks the exact assignment at use time,
describes its application, checks directional compatibility, obtains portable
catch-up evidence, and selects an assigned compatible ready backend. It echoes
the inspected descriptor into backend open, so application replacement cannot
silently race inspection. Exact descriptor equality is an open precondition,
not rolling-update compatibility.

The ingress proxies ordered, validated typed frames, preserving stream
epochs/sequences and propagating `Resynchronizing`; it cannot emit `Current`
while its backend is unready or
while it is catching up. A new ingress connection starts a fresh backend epoch
and must provide a complete initial snapshot before the client publication
boundary. The client retains the last coherent value during the gap.

Assignment observations are sampled once per ingress open and once per command,
never once per frame. They are scoped to that operation and discarded after the
use; they are not leases, permits, or application events. This avoids a loop in
which every consumed frame writes control history and triggers another
observation. The ingress repeats assignment, authorization, compatibility, and
readiness checks immediately before backend use because observations can stale
immediately.

Catch-up evidence is an opaque portable bundle passed through existing scoped
evidence transport: source identities, closed manifest/commitment references,
and a backend-issued readiness boundary. It never compares `LogPosition`s from
different nodes. Existing manifests and commitments establish neither global
completeness nor set inclusion; absent an additional authorized readiness
boundary the ingress returns `InsufficientEvidence`, never `Current`.

Dependent commands are rejected as `DependentStateUnavailable` unless the
dependency is current under the same selectors and use-time ingress/backend
checks. A retained value can remain readable during resynchronization. No
empty projection is treated as complete, and no nested/global policy is
invented: only exact configured assignments are eligible.

The ingress owns extra state: assignment head, backend identity, contract
descriptor, selectors, provenance, epoch translation, catch-up evidence, and a
bounded retry state machine. Every hop adds authorization and failure handling.
A client trusts the ingress to
preserve selectors and frame ordering; the ingress trusts only authenticated
backend responses and must not turn a storage role into execution. Backend
selection is hidden, but the trust surface and latency are larger than a
client-direct connector.

The unresolved semantic decision is the exact executor-issued, authorized fact
that certifies per-scope readiness. Until specified, ingress must fail closed;
a checkpoint, local cursor, closed history, coordinator variable, or assignment
record alone is insufficient.

## Synthesis decision
This is a deliberate server-owned alternative for comparison, not a claim that
it should win. It hides backend choice behind an application-capable ingress and
can fail over when the backend changes. It preserves selectors and stable client
ownership, but adds a trusted proxy, an additional hop, stream translation, and
two failover layers. The client-side selector shape likely wins because it keeps
the transport path shorter and avoids granting ingress nodes routing authority;
this candidate makes that rejection concrete.

## Tradeoffs accepted
- We accept ingress latency and duplicated reconnect state for hidden backend
  selection and centralized provenance checks.
- We accept trusting application-capable ingress nodes for frame integrity in
  exchange for keeping storage nodes out of the typed path.
- We accept fail-closed gaps when readiness evidence is absent for honesty.

## Alternatives considered
Client-side selector plus executor gate has fewer hops and less shared route
state; its connector must perform candidate selection, but applications still
see one handle. It hides comparable complexity behind a smaller trust surface,
so it is the likely base.

Gateway forwarding through any replication peer was rejected: it exposes
storage custody as an accidental typed gateway and lacks application authority.

## Open questions and risks

- What exact readiness attestation may an ingress forward and an executor sign?
- How are stream epochs mapped without allowing duplicate or intermediate
  `Current` frames?
- Which ingress identity is authorized to proxy each exact scope/service?

## Next implementation step

Build one bounded ingress proxy test with a fake backend: verify preserved
selectors/provenance, stale-assignment refusal, ordered epoch restart,
`InsufficientEvidence`, and no intermediate `Current` before native transport.
