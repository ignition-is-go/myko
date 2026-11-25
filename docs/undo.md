# Transaction undo

Myko can reverse an authenticated user's own committed transactions. Undo is append-only: it emits new events that restore the prior state and records a `TransactionUndo` receipt, so history is never rewritten and a transaction cannot be reversed twice.

## Opting entities in

Only entity types declared with `#[myko_item(undoable)]` participate. `is_undoable_entity(entity_type)` reports the registration. A transaction that touched an entity which has not opted in cannot be undone, and neither can anything recorded before it.

## Commands

- `UndoTransaction { transaction_id }` reverses one of the caller's transactions.
- `UndoLastTransaction {}` reverses the caller's latest transaction that has not already been reversed, and returns `None` when nothing is eligible.

Both require an authenticated user. Planning is conflict-checked: if a later transaction changed any affected entity, the whole plan is rejected. There is no implicit overwrite or force mode.

Applications that scope undo to a domain (for example, one editor's edits) use `undo_log_matching` to list eligible transactions and `undo_range_matching` to reverse a checked range, each with a predicate over the transaction history.

## Live history

The undo log is maintained in memory as a bounded live list with field-level diffs cached from committed mutations, so read-only history previews update without replaying the event store. `UndoHistoryBoundary` marks where history becomes durable after startup migrations; transactions before the boundary are not offered for undo.
