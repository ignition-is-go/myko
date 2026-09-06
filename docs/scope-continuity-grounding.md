# Scope continuity source grounding

## Baseline and evidence

Myko baseline is `902c6c02`. Both working trees were clean at the start.
Graph project `myko-7-current`, Tier 2, generation `2026-09-05T01:04:50Z`.
Coverage has no recorded gaps in the cited federation, Redb, authority, native
node, and transport paths. Peer and durable-node test metadata changed; direct
source reads supplied the fallback. Graph coverage is not proof of completeness.

The governing requirements and complete verification matrix live in
`scope-continuity-plan.md`. Historical rationale was checked in the original
August plan at `2ff388f4`, before its later alpha progress notes.

## Authoritative history and command execution

- `federation/src/access.rs`: `EventId` is origin node plus origin sequence.
  `CommandRequest` contains command ID, service, scope, principal, authority, and
  declared claims. Origin identifies provenance, not the future owner of a scope.
- `federation/src/history.rs`: an envelope has a local replay position and an
  immutable origin. Selected export and checkpoints describe a serving source's
  local cursor, not a scope's union of multiple writers.
- `federation/src/node.rs`: `NodeBackend` owns submit, claim, commit, lifecycle,
  ingestion, and history. `begin_command_with_authorization` rejects execution of
  a command whose first event originated elsewhere. Replicated submissions are
  deliberately not pending executable work.
- `federation/src/memory.rs`: submit and commit recover identical command IDs and
  reject conflicting requests. Ingest deduplicates immutable origins but applies
  imported events in arrival order. Lifecycle ordering has a deterministic origin
  tie-break. It is not a causal merge of application state.
- `federation/src/command.rs`: batches carry causal parents, but current validation
  and ingestion do not enforce a complete causal dependency graph.
- `redb/src/lib.rs`: journal append atomically writes the event and origin index
  with immediate durability. Checkpoints are separate records. Scope custody and
  persisted coverage are not currently one atomic journal operation.
- `InMemoryBackend::from_journal` rebuilds projections and requeues abandoned
  local claims. Node readiness is an overall startup gate, not scoped readiness.

## Authority

`authority/src/domain.rs` defines a node-local `AuthorityRealm`, immutable bootstrap
principal, grants, delegations, approvals, leases, use records, and decision audit
items. Its grant view names a source node. The evaluator binds the presented
executor to the authenticated transport principal and checks explicit resource
claims. Preserve that binding and exact-scope enforcement while removing dependence
on a founding node's authority endpoint. Membership, revocation, and lease state
need a consistency rule stronger than arbitrary arrival-order merging.

## Native routing and client recovery

- `core/src/server/federated_session.rs`: `NodeRequestRouter` currently resolves
  an unhandled command by service alone and routes an envelope to a node.
- `node/src/lib.rs`: `FederationRouter::peer_for_service` selects a remembered
  replication-enabled peer advertising the service. It does not evaluate scoped
  custody, coverage, authority, or serving readiness. This retained framework
  router is the integration boundary, not a second Forrest router.
- `node/src/peer.rs`: local durable peer configuration controls followers.
  `AdvertisedService` records compiled handlers. Advertisements are not grants or
  proofs of readiness.
- `iroh/src/protocol.rs`: follower identity pins and source-reset handling protect
  against interpreting one source's cursor as another's. Persisted cursors are
  explicitly stale until revalidated. Preserve these protections when adding
  scope-level multi-source recovery.
- `core/src/server/federated_session.rs`: selected history is filtered against
  current authority at snapshot and during streaming. Revocation closes streams.
- `federation/src/item.rs`: item requests already distinguish source from serving
  node, but native clients currently connect to one endpoint.
- `iroh/src/client.rs`: reactive item reconnect retains the same live cell,
  marks it resynchronizing, and installs a fresh snapshot/follow boundary.
  `federation/src/reactive.rs` carries source-aware composite frontiers. Extend
  those owners to select another eligible replica instead of replacing handles.

## Existing executable test seams

- `node/tests/durable_node.rs`: restart preserves identity, configured peers,
  selected replication, and durable cursors. Pinned-source mismatch is rejected.
  `connected_client_places_a_command_on_a_capable_peer` tests native routing.
- `iroh/src/tests.rs`: selected persisted cursors require revalidation; unpinned
  source replacement restarts the history boundary.
- `federation/src/tests.rs`: replicated origins ingest once. Extend this with
  permutations and multi-origin causal state, not only transport idempotency.
- Forrest `apps/forrestd/tests/mesh_execution.rs` and `mesh_recovery.rs` exercise
  actual remote root/model execution and uncertain-effect recovery.

## Design comparison constraints

Produce two structurally different designs compatible with the same final
requirements. Neither may substitute a permanent primary for multiwriter scopes.
Explain global command identity under concurrent delivery, causal conflicts,
revocation during partitions, safe last-custodian handoff, and durable receipt
authentication. Being connected is not a durability acknowledgment.

Separate causal provenance from local transport cursors. Separate convergent
application changes from coordination of exclusive or authority-changing commands.
Do not promise availability across a partition when the required coordination or
durable history is inaccessible. Keep all authoritative decisions in Myko history.

Design candidates are documentation only, in separate files of this checkout.
They must include caller usage, types and signatures, module ownership, migration
steps, tradeoffs, and the exact fault tests that distinguish correct behavior.
