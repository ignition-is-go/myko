# MykoSwift

`MykoSwift` is Myko's non-visual lifecycle integration for native Swift apps.
It plays the same role as `myko-ratatui`, `myko-gpui`, and `myko-leptos`:
application code supplies typed commands, queries, reports, and views while the
integration owns subscription lifetime and UI invalidation mechanics.

The directory contains both sides of the native boundary:

- the Rust `myko-swift` crate adapts any typed Hyphae subscription to
  cancellable synchronous `current`/`next` calls suitable for UniFFI, including
  lossless keyed collection revisions;
- the Swift `MykoSwift` package consumes those calls off the main actor,
  delivers revisions on the main actor, rejects stale work after restart, and
  provides structured-concurrency bridging for blocking native calls,
  serialized foreground/background node lifecycle, heterogeneous subscription
  ownership, reusable keyed collection materialization, and device-only
  Keychain storage for opaque node identities and secrets.

With the Rust crate's `embedded-node` feature, `EmbeddedNodeHost` also owns the
durable node identity, platform-supplied Iroh identity, asynchronous runtime,
and serialized foreground state. A concrete application supplies only its
typed runtime construction and shutdown callbacks; it does not reimplement
identity restoration, active-node locking, or foreground lifecycle mechanics.

The `native-ffi` feature adds Myko's generated Swift federation component.
`MykoFederation` exposes framework-owned LAN discovery, pairing, remembered
peers, directional replication, and their live subscriptions. A concrete app
implements `NativeApplicationAccess` once for its composed application node;
it does not duplicate those commands, records, projections, or subscription
objects. UniFFI library-mode generation can emit this component beside the
application component from the same linked native library and XCFramework.

Concrete applications still own exported records and projections for their
domain entities. The Rust-side
`export_blocking_subscription!` macro generates each concrete UniFFI
subscription object's uniform `current`/`next`/`cancel` surface. Applications
do not own transport routing, subscription retention, revision waiting, or
cancellation.

Keyed query and view results use `export_blocking_collection_subscription!`.
Its initial update is a typed reset and every later update retains the native
insert, update, remove, or batch semantics without rebuilding the collection.
The Swift application applies those projected `upserts` and `removedIDs` to a
`MykoCollectionState`; it owns only domain-to-presentation mapping and sorting.

An embedded Apple application normally owns one `MykoNodeSession`, registers
its typed `MykoSubscriptionBinding` values with the session's
`MykoSubscriptionGroup`, and uses the session's `MykoOperationScope` for
node-bound commands. The session serializes native start and stop, activates
declared subscriptions only after startup, cancels them before stop or failure
is published, and rejects stale command completions after a node transition.
The application maps lifecycle and subscription updates into its presentation
model; its generated adapter supplies concrete records and operations without
reimplementing native concurrency or lifecycle mechanics.

Long-lived query, report, and view bindings can be registered declaratively
with `MykoSubscriptionGroup.register`. Activating the group opens every
registered subscription; cancelling it preserves those declarations so a
foregrounded node reopens fresh streams without application-owned restart
wiring. Dynamic subscription families use `MykoKeyedSubscriptionGroup`: stable
keys retain their live bindings while removed keys are cancelled and new keys
are opened. This mirrors Myko's keyed reactive collections instead of making
each application tear down and rebuild every child subscription after a parent
revision.
