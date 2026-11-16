# Operation provenance on Myko 6

This is the first increment toward user-scoped undo and entity-tree restoration.
It adds correlation evidence to existing event persistence, using current Myko 6.
No code from the unfinished Myko 7 rewrite is required.

## Recorded evidence

Command contexts retain their originating request transaction. All synchronous
command emission methods, including raw imports, mixed batches, replacements,
graph reconciliation, and relationship cascades, carry provenance to persistence.
Queued causal mutations capture their own metadata before their caller's scope
ends. Scope guards restore previous attribution on return or unwinding and are
isolated by server instance and thread.

The envelope records the originating request transaction, command ID, request
lineage, and optional **unverified connection ID**.
Connection attribution is not an authenticated user identity. The originating
command context supplies the command ID and lineage; these are not a complete
nested execution trace. Events emitted by a command carry the originating
`RequestContext.tx` in `MEvent.tx`, including custom persisters and the saga event
sink. Nested commands, synchronous relationship cascades, and queued causal
mutations preserve it. Raw imports emitted by a command use that command's tx,
not a label supplied inside the imported event. The transaction is correlation,
not an authorization boundary or proof of an atomic commit.

## Transaction propagation

The originating command's `tx` follows its queries, reports, nested commands,
and emitted events. Query and report contexts already forward the request;
command event construction must retain that tx instead of minting a fresh one
for every write. A constructor may still generate a fresh tx for a standalone
server mutation with no originating command scope.

`RequestContext::child` preserves tx. Client command-response callbacks and live
query/view/report subscription cancellation continue using their existing tx
values; event propagation does not change their routing. `tx` is the causal
grouping identity; no separate execution identifier is generated. It is neither
authentication nor an atomic-transaction receipt.

Macro-declared sagas retain the triggering event's tx in an explicit command
wrapper across their asynchronous stream. Their reads and subsequent writes
therefore remain in that causal transaction. Handwritten saga pipelines can use
`CausedSagaCommand::new(command, event.tx.clone())` for the same contract; commands
with no declared originating event start an independent internal transaction.
This does not propagate authenticated user authority or prove that asynchronous
work has finished.

## Compatibility and storage

`MEvent`, its Rust struct literals, and its JSON/CBOR wire shape remain unchanged.
`EventRecord` flattens the original event and adds optional `provenance` in durable
JSON. Old event readers ignore that extra member. Old records deserialize with
`provenance: None`; absence means unknown provenance, not a trusted system actor.
Inbound raw events cannot establish trusted provenance through this envelope.

`Persister::persist_record` defaults to forwarding the original event to existing
`persist` implementations with the originating command tx already applied. Such
persisters discard the sidecar and report `supports_provenance() == false`. A custom persister must override both
methods to retain it. Applications must inspect that capability for each routed
backend before relying on metadata. Null, blackhole, and peer persisters do not
retain provenance.

PostgreSQL retains the envelope in its existing JSONB column, including retries;
no SQL schema migration is needed. PostgreSQL rejects NUL-bearing provenance
before enqueueing it, so invalid caller metadata cannot poison the retry queue;
this does not undo state already changed before persistence was attempted. `load_records_after_id` reads envelopes without
stripping metadata. Events remain indexed by `tx`; authenticated user lookup
still needs an explicit API. This bounded, row-ID-based read is a scan
primitive for incremental consumers, not a scalable interactive operation history
or evidence that an operation is complete. An indexed query contract is a future
increment. Existing history/replay APIs retain their `MEvent` contracts
and therefore do not expose the sidecar. Peer replication and asynchronous saga
work do not propagate the operation envelope across processes or tasks; saga
commands retain their event tx separately.

## Explicit limits and next steps

This change supplies no atomicity, commit-before-response, authenticated actor,
rollback, before-image, or undo guarantee. A failing command may already have
changed state or queued events. Transient writes stay transient. Direct server
mutations outside a command emission scope retain unknown provenance.

Durability barriers remain tasks `lv-3f3a` and `lv-3a27`. Compaction remains
`lv-54eb` and must preserve any history guarantees introduced later. Subsequent
increments need verified actor attribution, complete operation boundaries,
historical relationship/topology evidence, and conflict-checked append-only
restore before user-scoped or subtree undo can be advertised.

## Validation

Run `cargo flux run test-operation-provenance` and
`cargo flux run check-operation-provenance` with `RUST_TEST_THREADS=1`.
Run `cargo flux run test-postgres-provenance` for the PostgreSQL regression.
The task starts a disposable local PostgreSQL cluster on a private Unix socket,
runs the persistence test, and removes the database when it exits. PostgreSQL
server tools must be installed. The same task runs in the native CI quality gate.
