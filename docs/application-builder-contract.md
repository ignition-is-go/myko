# Myko application-builder contract

Status: agreed design, recorded 2026-09-06. This describes requirements, not a
claim that the current implementation satisfies them. The
[delivery checklist](application-builder-delivery.md) records evidence and gaps.
Failover startup readiness and command-result lifecycle were agreed on 2026-09-12.
Undecided details remain explicit below.

This contract defines the current goal. Earlier scope-continuity plans remain
historical evidence, not the acceptance definition for this goal. Forrest is
shelved while Myko proves these requirements with its own test applications.

## Application model and framework boundary

Myko is a federated application-builder toolkit. Its application-facing building
blocks are nodes, scopes and entities, reactive queries, reports and views,
commands, and application-owned sagas over reactive state.

A node is an application instance whose binary registers services and modules.
There is no separate application-membership layer or automatic application root
scope. A logical operation addresses a scope, stable service identity, and
operation. Myko selects a compatible, assigned, ready executor. Administrative
operations can explicitly address a node.

Myko owns durable accepted history, identity, authorization, placement,
coordination, routing, subscription lifecycle, and generic execution machinery.
Applications own domain events, command semantics, projections, saga rules,
resource semantics, and external side effects. Internal service calls and client
calls use the same typed mechanisms and authorization rules.

Applications do not implement a second persistence system, replication system,
reconnect loop, or freshness protocol beside Myko. Forrest-specific agent,
message, model, and tool behavior does not belong in the framework proof.

## Four guarantees

- Local operation needs no mandatory centralized or server-style infrastructure.
- Execution scales horizontally within an explicitly managed trusted mesh.
- Meshes share application data through fine-grained, identity-aware grants.
- Services within a mesh use typed reads and commands to interact across
  application binaries.

## Durable placement and execution

The accepted event log is the truth. A durable replica holds the accepted history
for its placed scopes, not necessarily every scope in the application or mesh.
Custody includes retention responsibility. Possessing copied bytes alone does
not establish current custody or authority.

A scope's identity and accepted history do not depend on its founding node.
Scopes survive node churn when durable history remains continuously available
and responsibility transfers preserve the required authority. A surviving copy
does not by itself satisfy an operation's coordinator or execution requirements.

Operators or applications explicitly manage scope placement. Joining the mesh
does not automatically redistribute scopes. Nested scopes follow their parent's
placement. A scope's fault tolerance follows from its actual durable placement;
it is not a separately configured tolerance number.

Myko supplies a generic storage-only binary. Application nodes share its baseline
data plane. Storage-only nodes persist explicitly placed history, enforce
framework integrity and authorization, and report durable coverage. They need no
application modules and treat application payloads as opaque. They do not execute
application commands, projections, or sagas.

Storage-only participation does not imply an application-query gateway role.
Clients and executing applications resolve eligible compute nodes for typed
subscriptions. Storage participants supply retained history without interpreting
application handler names or payload types.

Storage placement and execution assignment are separate. Within a trusted mesh,
execution assignment manages compute resources rather than creating security
isolation. Principal and group permissions still apply. An executor needs an
assignment, compatible service code, readiness, and eligible resources.

Durable storage can remain available while application execution is unavailable.
If no compatible, assigned executor is ready, retained logs do not make typed
queries available. Existing clients retain their last coherent value marked
desynchronized until an eligible executor catches up and resumes service.

These operations have distinct meanings:

| Operation | Meaning |
| --- | --- |
| Subscribe | Follow reactive output without acquiring durable placement. |
| Place | Assign durable scope-history storage. |
| Share | Grant access without implying a replica or custody transfer. |

Remote reads use long-running reactive subscriptions to eligible serving nodes.
Releasing a handle releases its subscription. A remote read does not implicitly
copy scope history into a durable local replica.

## Health, readiness, and reactive handles

Local-first does not permit every partitioned node to keep writing independently.
A configured mesh is a dependency. Member loss makes the mesh degraded, even
when enough nodes remain to continue work. Mesh health and per-scope operation
readiness are separate. Degraded does not necessarily mean desynchronized.

An operation proceeds only when its readiness and authority requirements hold.
Otherwise it fails loudly. A scope catching up does not serve intermediate state
or execute commands against incomplete data. Another ready scope can continue
serving. A projection that depends on several scopes inherits their required
readiness conditions.

A logical subscription survives serving-node loss. When serving continuity is
lost or unproven, the handle retains its last coherent value and explicitly
reports desynchronization. Myko reconnects through another eligible, ready serving
node. A socket connection alone does not restore synchronization; coherent
current state does.

After failover, a replacement must obtain a fresh, authenticated scope-history
boundary confirmed by a strict majority of that scope's designated coordinators
before it starts serving the scope as `Current`. The replacement must retain and
project the required history through that boundary without publishing intermediate
state. A readable local copy or the client's last observed history is insufficient.
If the majority is unreachable, recovery stays desynchronized even when the
replacement has a readable copy. Missing coordinators do not lower quorum.

This requirement applies per scope at startup after failover. It does not decide
ongoing readiness policy for an already-current executor or require a quorum
round trip for every emitted value. The evidence format and the protocol that
checks the boundary before publication still need implementation and proof.

An assigned application executor can prepare as a hot standby by retaining the
scope's history, building its projections, and following new events. This avoids
starting catch-up from scratch. Seamless takeover is the best-case target, not an
unconditional guarantee. Keeping a handle `Current` through a switch requires
proof of continuous readiness, including the required history boundary. Otherwise
the handle reports desynchronization. A storage-only replica cannot execute the
projections needed for this ready-to-serve backup.

New commands dependent on desynchronized state fail loudly and immediately.
They are not silently queued for execution after recovery. This rule applies to
clients and internal saga execution. Already accepted commands and physical
effects in flight retain their identities and recovery obligations.

Types encode reactive dependencies and lifecycle distinctions. Runtime checks
close races between observation and execution. Cross-service reactive reads
propagate updates, desynchronization, and authorization changes automatically.
Applications do not maintain parallel freshness booleans.

## Commands and coordination

Command results expose one lifecycle with distinct local completion and replication
milestones. The replication milestones are `quorum_replicated` and
`fully_replicated`. Callers explicitly choose which milestone a wait helper awaits;
neither replication milestone is an implicit default.

`quorum_replicated` requires durable acknowledgments satisfying the command's write
quorum. `fully_replicated` requires acknowledgments from all durable replicas
required by the same replication obligation. Both refer to the exact committed
history, not connection counts, discovered peers, or cached projections.

Selecting the `replicated` milestone means `quorum_replicated`: a write quorum of
durable copies, not every assigned copy. `fully_replicated` remains the stronger
milestone. This write quorum is separate from the coordinator majority that
approves failover. Coordinator votes alone are not durable-copy acknowledgments.

Myko owns the obligation, holder membership, acknowledgment verification, and
lifecycle tracking. Applications choose a wait milestone, not a list of sockets or
acknowledgment collectors. Recorded achievement does not prove that those holders
remain available now.

The exact holder and quorum rules and when projections may publish `Current`
still need definition and proof. Those rules
must preserve the startup-history and stale-state requirements above. Existing
local results must not silently become claims of failover-safe durability.

Commands are idempotent by default. Retrying, reconnecting, or rerouting the same
logical command ID recovers its existing outcome without repeating its mutation.
A new invocation has a new ID. This is not an exactly-once guarantee for arbitrary
external side effects.

Commands can declare same-scope preconditions. A concurrency-sensitive
precondition selects the required coordination automatically, without a second
exclusive flag. Cross-scope atomic preconditions are not ordinary preconditions.
Cross-scope workflows use independently committed commands and sagas, with
application-defined compensation where appropriate.

Exclusive operations require a strict majority of their designated coordinator
set, not a majority of every mesh member or storage holder. Odd counts are useful
but not required. Four coordinators require three votes, so neither side of a
two-versus-two partition can proceed. Two coordinators require both votes.

Coordinator membership changes require the existing majority. Missing nodes do
not lower quorum automatically. Permanent majority loss requires explicit
disaster recovery. Ordinary operations still require their declared authority
and readiness; absence of an exclusive precondition is not permission to write
on every partition.

## Safe draining and removal

Each node has a reactive drain-plan view describing affected placements,
responsibilities, proposed destinations, blockers, and resulting health. The
operator approves a concrete plan. Execution validates its revision and
preconditions immediately before acting and rejects stale plans loudly.

Draining adds responsibilities to replacement nodes until losing the target is
effectively a no-op. This includes durable catch-up, execution readiness,
coordinator membership transitions, and handling active saga steps. Copying can
continue while writes arrive, but every consequential transition checks current
requirements and preserves accepted history.

Scopes placed solely on the target require replacement placement. Without an
eligible destination, graceful removal is blocked. Nested scopes follow their
parent. Force eviction is distinct and must disclose lost coverage or availability.
An evicted node cannot reconnect and resume obsolete assignments or authority.

## Schema compatibility and rolling updates

Stable service identities and generated type schemas define compatibility.
Semver labels alone do not prove compatibility. Myko generates schema checks and
handles compatible routing and rollout under configured policy, exposing reactive
status and errors without application-written version-routing state machines.

Compatible versions can coexist during a rolling update. Activation of
incompatible contracts or production writes remains blocked until required
participants support them. Historical accepted events remain interpretable;
rollouts do not silently rewrite or delete them.

Schema generation cannot infer semantic migrations or external-effect behavior.
Applications provide necessary migrations or adapters; Myko schedules their
execution. Detailed compatibility and activation rules remain an implementation
design task, not a reason to equate matching semver with safety.

## Application-owned sagas and runtime resources

Sagas are long-lived rules, pure over reactive state. Reactive evaluation does
not itself perform external effects. Bounded work and side-effect execution are
separate from evaluation. Application state and progress are durable; sagas do
not depend on a client's connection or require implicit persisted call stacks.

Execution scales dynamically within explicitly assigned eligible nodes. Resource
types are declared in code, while resource instances and availability are data
that can change at runtime. Each step selects resources independently. Stable
typed work identity is part of the design; detailed scheduler APIs remain open.

Recovery policy is configurable per saga, including automatic at-least-once
retry, takeover after confirmed stop, and explicit recovery from uncertain
effects. Myko does not promise exactly-once physical effects.

Reactive state provides the latest coherent value, not a task for every update
notification. Processing every event requires a distinct replayable history
stream and retained cursor.

Sagas belong to application services and execute under application-service
grants. Data may belong to principals, groups, or scopes. Ownership of input data
does not implicitly delegate its owner's authority to a saga.

## Identity and mesh-to-mesh sharing

Users, agents, and groups have stable identities across meshes. An agent is a
distinct principal and needs explicit delegation to act for a user. Routing to
another node preserves the originating principal and cannot broaden authority.

A receiving mesh recognizes identity issuers and delegation but grants access
independently. Identity recognition alone grants nothing. External principals do
not need membership in the receiving infrastructure mesh. Groups have stable
identities and reactive membership.

Grants can target an exact scope or a subtree. Reads and subscriptions, command
invocation, and onward delegation are distinct permissions. Onward delegation is
off by default and cannot exceed the original grant. Sharing does not imply
durable replication or custody.

The serving node knows and reactively enforces the subscriber's grants, group
membership, and delegation. Revocation affects a subscription even if application
data does not change. Removed access clears protected scalar values or removes
affected collection entries and reports an explicit typed authorization change.
It must not look like an authorized empty result.

Transport loss retains stale data. Revocation removes unauthorized data. The same
handle can recover when access is restored, but publishes no protected payload
until authorization and coherence are reestablished. Revocation cannot erase
data already disclosed to another operator.

## Remaining unknowns

These details need decisions or evidence before implementation claims them:

- The protocol and evidence for the agreed failover startup boundary, plus ongoing
  readiness and authority requirements for other ordinary operations.
- Evidence, holder membership, and write-quorum rules for the two explicit command
  replication milestones, and their relationship to projection `Current`.
- Executor selection, placement policy APIs, and coherent failover across cursors.
- Typed command dependencies and server-side validation across concurrent changes.
- Partial revocation semantics for aggregates and derived multi-scope results.
- Identity issuer recognition, group authority, and grant propagation protocols.
- Generated compatibility rules, activation participants, and migration contracts.
- Saga work identity, resource capacity reservation, and recovery-policy APIs.
- History retention and recovery when continuity or a coordinator majority is lost.

These unknowns do not weaken the agreed failure behavior or authorize an
application-specific substitute for framework behavior.
