# MykoSwift

`MykoSwift` is Myko's non-visual lifecycle integration for native Swift apps.
It plays the same role as `myko-ratatui`, `myko-gpui`, and `myko-leptos`:
application code supplies typed commands, queries, reports, and views while the
integration owns subscription lifetime and UI invalidation mechanics.

The directory contains both sides of the native boundary:

- the Rust `myko-swift` crate adapts any typed Hyphae subscription to
  cancellable synchronous `current`/`next` calls suitable for UniFFI;
- the Swift `MykoSwift` package consumes those calls off the main actor,
  delivers revisions on the main actor, rejects stale work after restart, and
  provides structured-concurrency bridging for blocking native calls,
  serialized foreground/background node lifecycle, heterogeneous subscription
  ownership, and device-only Keychain storage for opaque node identities and
  secrets.

Concrete applications still own their exported records and projection from
domain entities into presentation data. They do not own transport routing,
subscription retention, revision waiting, or cancellation.

An embedded Apple application normally owns one `MykoNodeLifecycle`, registers
its typed `MykoSubscriptionBinding` values with a `MykoSubscriptionGroup`, and
uses a `MykoOperationScope` for node-bound commands. The operation scope moves
blocking UniFFI work off the main actor and rejects stale completions after a
node stop or restart. The application maps lifecycle and subscription updates
into its presentation model; its generated adapter supplies concrete records
and operations without reimplementing native concurrency or lifecycle
mechanics.
