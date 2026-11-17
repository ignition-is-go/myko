# Non-owning references and representation migrations

`relationship::ReferenceRegistration` registers a pure extractor from an entity
payload to typed `graph::EntityRef` targets. This describes non-owning intent:
ordinary target deletion does not cascade to the referencing entity, and export
ownership traversal does not automatically include referenced resources.

Checked undo validates restored targets against its final overlay, including
other entities restored by the same reversal. Undoing target creation rejects
referencing entities outside the inverse plan. Extraction currently scans
registered stores during dependency validation; it does not create indexed graph
edges or enforce target availability on ordinary writes. Applications can keep
unresolved references while a resource is absent.

Application invariants register `MutationValidatorRegistration`. Validators see a
`ProposedState` containing final upserts/deletes and may reject publication before
writes. They run under the local transaction coordinator's exclusive authority
for affected types. Reversal policies can request validation when any entity is
deleted, to validate legacy payloads that have no explicit reference registration.
Replay preserves historical states and bypasses application write validation.

Representation migrations can register a pure, idempotent
`MutationRewriterRegistration` to canonicalize authoritative/import mutations
before validation. Rewriters return a mixed replacement plan. All resulting events
retain the current originating transaction and provenance. Nonconvergent rewrites
fail before publication. Transient writes and undo compensation bypass rewriting;
compensation must restore the recorded physical state, including an old format.

`EntityQuerySourceRegistration` supplies a logical source for generated entity
queries, id reports, snapshots and live filters. Its context retains the
originating request/transaction. The application owns the compatibility projection
and reactive lifetime. `EntityHistoryAliasRegistration` allows a logical entity
history to include multiple physical types sharing a stable id; original events
and physical type names remain unchanged.

These hooks do not provide distributed locking or durable multi-event ACID.
They share the existing single-authority and persistence-failure limitations of
checked undo. Reader support must reach every participating peer before enabling
new-format writes. Applications define their migration and undo boundaries without changing
historical log records.

## Starting an undo epoch after migration

An application may deliberately start undo history with the current data model.
After migrating and durably flushing current state, call
`entities::undo::begin_undo_history(&server, migration_id)`. It appends an internally
attributed boundary once per migration id. PostgreSQL selection excludes
transactions that began before the latest boundary; explicit transaction undo
rejects them too. Old event records remain readable and unchanged. Repeating the
same migration id after restart does not advance the boundary, even if its live
marker row was later deleted. Ordinary imported/entity writes do not establish a
boundary without the internal provenance used by this server-only helper.
