# Operation provenance on Myko 6

This is the first increment toward user-scoped undo and entity-tree restoration.
It adds correlation evidence to existing event persistence, using current Myko 6.
No code from the unfinished Myko 7 rewrite is required.

## Recorded evidence

Each `CommandContext` lazily creates a server-generated operation ID. Clones and
nested commands using that context retain it. Each independent context gets a
new ID even when callers reuse the same request `tx`. All synchronous command
emission methods, including raw imports, mixed batches, replacements, graph
reconciliation, and relationship cascades, carry this metadata to persistence.
Queued causal mutations capture their own metadata before their caller's scope
ends. Scope guards restore previous attribution on return or unwinding and are
isolated by server instance and thread.

The envelope records the operation ID, caller-supplied request transaction label,
command context ID, request lineage, and optional **unverified connection ID**.
Connection attribution is not an authenticated user identity. The originating
command context supplies the command ID and lineage; these are not a complete
nested execution trace. `MEvent.tx` retains its existing per-event value, including
in custom persisters and the saga event sink. The envelope does not overwrite it
with either the operation ID or the request label. Request labels are never an
authorization boundary or proof of a complete transaction.

## Why retain a separate execution ID?

`RequestContext::child` preserves request `tx` for nested tracing. The same field
also identifies client command-response callbacks (`client/mod.rs`) and live
query/view/report subscriptions and cancellation (`server/client_session.rs`).
Those are request/subscription lifetimes, not server-issued execution identities.
Separately, `MEvent` constructors currently mint their own transaction values;
saga macros log those values and saga dispatch creates a fresh internal request.

This increment preserves all those existing transaction values and routes. The
sidecar's `requestTx` is the link back to the existing command tracing label.
Its distinct `operationId` identifies a particular command-context execution even
when two clients, retries, or repeated calls reuse the same request label. It
must not become a subscription key or replace command-response correlation. It
is not an authentication boundary or an atomic-transaction receipt. Async saga
causation needs a later explicit contract; neither identifier proves that all
downstream work has finished.

## Compatibility and storage

`MEvent`, its Rust struct literals, and its JSON/CBOR wire shape remain unchanged.
`EventRecord` flattens the original event and adds optional `provenance` in durable
JSON. Old event readers ignore that extra member. Old records deserialize with
`provenance: None`; absence means unknown provenance, not a trusted system actor.
Inbound raw events cannot establish trusted provenance through this envelope.

`Persister::persist_record` defaults to forwarding the original event to existing
`persist` implementations without changing event fields. Such persisters discard the sidecar and
report `supports_provenance() == false`. A custom persister must override both
methods to retain it. Applications must inspect that capability for each routed
backend before relying on metadata. Null, blackhole, and peer persisters do not
retain provenance.

PostgreSQL retains the envelope in its existing JSONB column, including retries;
no SQL schema migration is needed. PostgreSQL rejects NUL-bearing provenance
before enqueueing it, so invalid caller metadata cannot poison the retry queue;
this does not undo state already changed before persistence was attempted. `load_records_after_id` reads envelopes without
stripping metadata. Operation IDs currently exist only in JSONB, with no operation
index or operation/user lookup API. This bounded, row-ID-based read is a scan
primitive for incremental consumers, not a scalable interactive operation history
or evidence that an operation is complete. An indexed query contract is a future
increment. Existing history/replay APIs retain their `MEvent` contracts
and therefore do not expose the sidecar. Peer replication and asynchronous saga
work do not propagate the operation envelope across processes or tasks.

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
The opt-in real PostgreSQL test uses an isolated disposable database:
`MYKO_PROVENANCE_TEST_POSTGRES=<connection string> cargo flux run test-postgres-provenance`.
The integration test truncates only its dedicated `provenance_test_events` table;
never point it at a shared or production database.
