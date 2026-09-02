# MykoSwift

`MykoSwift` is Myko's non-visual lifecycle integration for native Swift apps.
It plays the same role as `myko-ratatui`, `myko-gpui`, and `myko-leptos`:
application code supplies typed commands, queries, reports, and views while the
integration owns subscription lifetime and UI invalidation mechanics.

The directory contains both sides of the native boundary:

- the Rust `myko-swift` crate adapts any typed Hyphae subscription to
  cancellable synchronous `current`/`next` calls suitable for UniFFI;
- the Swift `MykoSwift` package consumes those calls off the main actor,
  delivers revisions on the main actor, and rejects stale work after restart.

Concrete applications still own their exported records and projection from
domain entities into presentation data. They do not own transport routing,
subscription retention, revision waiting, or cancellation.
