# Authenticated transaction history and undo

`tx` identifies the originating command and all of its causal work. Queries,
reports, nested commands, emitted events, synchronous relationship cascades, and
macro-declared saga commands preserve it. No additional operation ID is generated.
Standalone server mutations without command context receive their own tx.
Request/response and subscription routing retain their existing identifiers.

## Attribution

`CommandContext` captures the stable application user from the live authenticated
`RequestContext`/`AuthSession`. The optional durable `EventRecord.provenance`
envelope records `actorUserId`, `requestTx`, `commandId`, lineage, connection
attribution, and, for a compensation, `undoOfTx`. A connection ID is never a user.
Neither request arguments nor inbound raw-event JSON can establish this metadata.
Legacy and unattributed records stay unknown rather than becoming system users.

Clones and nested commands retain their context. Queued causal mutations retain
metadata before the caller's scope ends. Macro-declared sagas carry an opaque
`CommandCausation` token across their asynchronous command stream: this preserves
the originating user for audit without granting that user's authentication or
permissions to an internal saga. Handwritten sagas must retain
`ctx.begin_event(&event)` with `CausedSagaCommand::from_causation(...)` to participate
in the same contract. A transaction with undrained saga deliveries is not eligible
for undo. Dropped/failed delivery paths release their reservations.

## Durable history

PostgreSQL preserves provenance in the existing JSONB event envelope. Legacy
`MEvent` readers ignore the additive field. `Persister::persist_record` still
forwards the event to legacy persisters, but such backends cannot support undo.

The provider exposes `transaction_history(tx)` and
`latest_user_transaction(user)`. A repeatable-read snapshot contains every event
in the transaction, each touched entity's state immediately before its first
write, its final write, and evidence of subsequent changes. Reads and decoding
fail explicitly; missing or malformed history never becomes an empty successful
reversal plan. `EventsForTransaction` now returns `Result<Vec<MEvent>, String>`
and checks the authenticated caller owns the entire transaction.

Domain commands can call `undo_latest_matching` with a trusted selection policy.
It pages the caller's completed original transactions using `user_transaction_page`
and a stable first-event cursor, without an arbitrary lookback cutoff. Selection,
policy evaluation and reversal share the local mutation authority. A conflict in
the first matching transaction is returned, never skipped. `entity_event_at`
resolves historical ownership at a log revision, including tombstones, so domain
policies need not infer ownership from today's live entities.

A producer barrier acknowledges all prior queued events. The local runtime tracks
active command contexts and outstanding saga deliveries. Once that causal work
finishes, PostgreSQL stores a completion record in `<events_table>_transactions`,
separate from entity state, containing the existing tx, user and first/last event
IDs. A later write with the same tx invalidates the completion. User lookup is
indexed by `(user_id, first_id DESC)`, so late saga writes cannot reorder an older
user action above a newer action. Incomplete transactions remain ineligible after
restart, even though their partial events remain readable for diagnosis.

## Entity opt-in

Entity definitions explicitly opt into reversal with `#[myko_item(undoable)]`
(or `undoable = true`, compatible with other item options). The generated
`UndoableRegistration` metadata is separate from parser registration; unmarked
entities default to false (`is_undoable_entity` reads the registration). Unknown and unregistered
types are excluded. Presence, navigation, external/executor state and runtime
status should remain unmarked; this is a framework-enforced policy, not a UI filter.

Latest/log selection skips transactions containing only excluded entity types.
A transaction mixing opted-in and excluded entities remains visible but is blocked
as a whole. Explicit transaction undo enforces the same rule. New excluded writes
still participate in revision and dependency conflict checks; opting out never
makes those later writes safe to overwrite. The flag is current entity metadata,
not a client-supplied event field or permission bypass.

## Undo commands

- `UndoLastTransaction {}` selects the authenticated user's latest completed,
  original, unreversed transaction, returning `None` when none exists.
- `UndoTransaction { transaction_id }` reverses a specified transaction owned by
  the authenticated user. It rejects unknown/mixed authors, active/incomplete
  transactions, an invalid or identical undo tx, and an already reversed target.

- `GetUndoTransactions { limit }` previews up to 100 eligible user transactions,
  newest first. Each entry reports the safety of undoing the entire prefix through
  it, including metadata for uncompensated later revisions (transaction, verified
  author, entity and timestamp, without value payloads).
- `UndoTransactions { transaction_ids }` checks that the supplied newest-first IDs
  remain a contiguous eligible prefix, then preflights their combined inverse.
  Duplicate IDs, empty/oversized ranges and stale selections fail before writes.

Domain adapters share one policy between `undo_log_matching` and
`undo_range_matching`. Newer selected actions are excluded from older revision
checks, but intervening foreign actions remain conflicts. A virtual entity overlay
checks each original against the state produced by reversing newer selections.
The final dependency/representation checks run against the combined plan; older
selected edits can restore dependencies missing from a shorter prefix. Each
original gets its own durable receipt under the one compensation transaction.

For every affected entity, undo restores its previous payload or absence. Deleted
entities reclaim their original IDs. Granular entities allow another user's rename
to survive a position undo. Uncompensated later revisions reject the whole plan,
even when they wrote an equal value. Both durable revisions and normalized live
state are checked. Compensated later edits are recognized so repeated undo can
walk the user's stack after restart.

Validation also checks relationship/graph deletions, owning-parent fixups and the
final dependency state. A cascade must not silently delete a newer child or rewrite
an unplanned parent. Unknown types, missing dependencies, incompatible persistence
routes, transient live divergence and history failures reject before inverse
writes. A local read/write coordination gate excludes concurrent authoritative
mutations during the check and application, while ordinary mutations remain
concurrent with each other. Contexts without history avoid this gate.

The inverse writes are normal events under a new tx, with an on-log
`TransactionUndo` receipt. Receipts remain effective even if their live entity is
removed. Only receipts with server-stamped compensation provenance count;
ordinary entity writes cannot forge them. Successful undo acknowledges persistence. An undo transaction itself is
not selected or accepted for undo in this first version; redo and scene/time
rollback remain separate API work. The history change representation and checked
mixed replacement path provide their underlying primitives.

## Live undo previews

`undo_log_matching` accepts either a command or report context through
`UndoReadContext`. The read-only path does not construct command activities or
acquire event-publishing capabilities, so a mounted preview cannot make its user
appear busy to actual undo commands.

The PostgreSQL backend exposes a shared per-user `undo_list_source`: a disposable
in-memory projection of the latest 200 completed, opted-in transactions. It
hydrates metadata and bounded field previews from existing history once, then
consumes producer commits and completion notifications. No timer, full preview rebuild, before-image lookup, or
new durable summary table is involved in ordinary list updates. Opening the UI
reveals its already mounted subscription.

The projection keeps bounded entity keys and known later writers. Oversized
metadata is marked truncated; all rows defer full validation until apply. A
repeatable-read hydration snapshot is coordinated with local commit sequencing,
so queued events are neither missed nor applied twice across hydration. Cache
eviction does not expire durable history. Successful undo can refill older rows;
the apply path still reads fresh history and checks the complete selected range.

Field previews are transient data carried by `persist_record_with_preview`,
not serialized event/provenance fields. Authoritative mutation paths already hold
the prior entity; they capture display data before it is released, and the
producer forwards it only after successful persistence. The projection coalesces
first-before/final-after snapshots until transaction completion. Nested object
fields and array indices use JSON Pointer paths. Absent values remain distinct
from explicit JSON null. Create/delete operations retain their field data.

Display limits are 32 entities, 128 changed fields and 32 KiB of field text per
transaction, with 512-byte value previews and a 64-KiB pending snapshot budget.
Missing, oversized or incomplete input is marked partial, never presented as a
complete inverse. Cold hydration reads bounded first-before/final-after values
from existing log rows once; live updates perform no additional database reads.
Immutable diff vectors are shared across cached snapshots. A list adapter can
omit those vectors and serve a deduplicated per-transaction detail stream from
the same cell, avoiding retransmission of 200 full diffs on every edit. The event
schema and durable tables are unchanged.

## Scope and failure semantics

This is append-only causal reversal, not a general ACID command transaction or a
distributed writer lock. Existing command mutations can change live state before
persistence fails. The coordination gate makes conflict rejection all-or-nothing
within one authoritative runtime; it does not provide serializable multi-server
writes to a shared database. A failure/crash during inverse persistence may leave
partial forward events. Without a durable completion they are not treated as a
successful undo, and subsequent reversal fails closed against divergent state.

All routed events of an eligible transaction must use the configured attributed
history backend. Peer replication does not transport the sidecar or authenticated
actor across processes. Complete log retention is required: compaction must preserve
prior-state and completion evidence before undo history can be retained across it.
No automatic event-history retention policy or force-overwrite option is introduced.

## Validation

Use `RUST_TEST_THREADS=1` for all reactive tests:

- `cargo flux run test-operation-provenance`
- `cargo flux run check-operation-provenance`
- `cargo flux run test-transaction-undo`
- `cargo flux run lint-transaction-undo`
- `cargo flux run test-postgres-provenance`
- `cargo flux run check --selection transaction-clients`

The PostgreSQL tasks create a disposable cluster on a private Unix socket and
remove it on exit. The undo suite covers two-user granular edits across restart,
strict conflicts without partial reversal, ownership, legacy/mixed actors,
resurrection, relationship side effects, repeated undo, active/incomplete work,
asynchronous saga attribution and competing undo attempts. It runs in native CI.
