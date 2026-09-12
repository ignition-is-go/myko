# Authorization lifecycle grounding

## Contract

Whole-handler denial must clear protected scalar values and collection rows,
publish an explicit authorization status, and permit the same owned reactive
handle to recover when access returns. Transport or authority unavailability
retains stale output. Recovery cannot republish protected output until the
server has authorized it and its existing coherence checks pass.

Partial multi-scope aggregate policy and ordinary-read failover readiness are
still undecided. Do not invent either. Storage-only nodes remain log participants,
not typed-handler executors. All changes belong in Myko, not Forrest.

## Current path

The server sends an `AuthorizationDecision` at admission or continuation and
closes the denied logical stream. Local mux and Iroh clients now preserve that
decision as `HandlerClientError::Authorization`. The core `drive_handler` and
`drive_view` tasks still treat it as terminal and call `invalidate`, which retains
protected output. `retry_initial` and established reconnect loops only retry
transport and authority-unavailable errors.

The request is prepared once after target resolution. Reconnection must retain
that request and its selectors. Local logical streams share one socket.
Raw `follow_query`, `follow_report`, and `follow_view` subscriptions are
caller-driven streams, distinct from the owned `follow_*_reactive` handles.

The local and Iroh item subscription drivers in `transport.rs` and `client.rs`
have the same retained-value issue. The local driver retries authority outages;
the Iroh item driver's recoverable classification currently omits that reason.

## Composition path

`libs/myko/federation/src/reactive.rs` owns the shared lifecycle and its writers.
Scalar invalidation and resynchronization retain the previous value and cursor.
Collection invalidation and resynchronization publish no row diff.

Scalar `map_value` and `try_map_value` clear on explicit source `None`. Both
`frontier_join_state` and `coherent_join_state` retain their own previous tuple
on invalid or noncurrent dependencies, so source clearing alone is insufficient.
Collection `as_subscription` snapshots rows regardless of liveness. Lazy row
maps follow removals. Union owns left and right snapshots, applies their diffs,
and reconciles the merged rows. Revocation needs one atomic row-removal and
lifecycle revision, never an empty `Current` followed by denial.

A whole-handler denial contains no row-level authorization provenance. Any
decision about preserving unrelated parts of a partially denied aggregate must
remain explicit rather than being inferred by generic composition.

## Verification

Use the existing neutral local record application and live socket fixtures in
`libs/myko/local/src/tests/handler_authorization.rs`. Exercise scalar reports,
queries, views, retained clones, initial denial, idle revocation, outage,
regrant, and cancellation. Add focused federation composition tests and extend
`scripts/verify-query-lifecycle.sh`. Preserve all existing dirty work. Use
`target/agent`, four Cargo jobs, no new worktrees, production daemons, commits,
pushes, or Forrest edits.

Graph Tier 2 discovery used `myko-7-current`, generation
`2026-09-05T01:04:50Z`. Coverage reports changed metadata and stale symbol ranges.
Current source reads, not stale snippets, ground these findings. The how
explainer traced scalar maps, both joins, collection snapshots, union, and
revision publication in `reactive.rs`. The main agent traced both core drivers
and the local and Iroh item drivers.
