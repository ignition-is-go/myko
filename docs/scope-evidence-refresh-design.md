# Incremental scoped evidence refresh

## Plan

- [x] Ground the adapter, checked cursor, and local ingestion path.
- [x] Sketch distinct ownership alternatives.
- [x] Select and record the synthesized design.
- [x] Implement and verify the selected design.
- [x] Reconsider the design if implementation exposes a mismatch.

### Design comparison

- [x] Frame the problem and criteria.
- [x] Produce independent sketches.
- [x] Cross-judge completed sketches.
- [x] Pick the base.
- [x] Incorporate useful alternatives.
- [x] Verify the result against real transport behavior.

## Problem

Before this change, the four-owner certified-grant recovery fixture remained red
after two small replay optimizations. Its last run before transfer instrumentation
took 367.47 seconds and did not restore the item, query, report, or view owner
within 120 seconds after regrant committed.

`IrohScopedEvidenceEndpoint::refresh_scopes` always passed `None` as the checked
cursor to `IrohReplicator::pull_scope_on`. Every refresh therefore requested the
exact scope from its beginning. The existing pull API already supported a
checkpoint bound to one source history and one scope. Repeated full transfers
were one measured cost; they did not explain all the latency.

## Scope and criteria

This work belongs to Myko's existing replication adapter. It does not grant
serving authority, change command durability, or define failover readiness.
Applications keep the existing `refresh_scopes` interface. No second persistence
or replication protocol is introduced.

The design comparison grades these properties separately:

- Repeated successful refreshes transfer new history without comparing positions
  from different sources or scopes.
- Source replacement, failure, cancellation, and partial ingestion cannot advance
  a checkpoint past history retained locally.
- Clones and concurrent refreshes have explicit ownership and bounded contention.
- Permission checks remain fresh for each request; retained cursors are not permits.
- Real transport tests distinguish incremental refresh from duplicate full replay.

## Grounding

The focused source explanation traces `refresh_scopes` through
`pull_scope_on`, `fetch_scoped_batch`, and `Node::ingest_scoped_batch`.
The adapter currently has no cursor state. Clones copy the node, endpoint,
remote address, and timeout. Each requested scope gets a separate timed pull.

The checked pull rejects cross-scope cursors. If the remote source identity
changes, it discards the position and refetches the same scope from the
beginning. Positions belong to one source's complete log and may advance across
unrelated scopes. They are not per-scope ordinals or cross-node ordering.

Ingestion validates the batch and every event's scope, applies events
idempotently, then records replication coverage. Only a successful result
produces `ScopedReplicationReport::checkpoint()`. A failed batch can leave an
applied prefix. A later failed scope does not undo earlier successful scopes.
Retries from the last successful checkpoint recheck matching duplicate origins,
bodies, and timestamps. A conflicting retained event still fails.

The design must decide how clones and concurrent refreshes own transient cursor
state. Cursor loss may cause a full replay. It must not require a new durable
checkpoint store, and a retained checkpoint must never serve as authority.

The existing native tests cover exact-scope filtering with cursor advancement,
changed-source replay, and cross-scope cursor rejection. New adapter tests must
exercise automatic checkpoint reuse rather than pass checkpoints manually.

The graph generation is `2026-09-05T01:04:50Z`; the parent and focused explainer
used direct-source verification for the untracked or changed paths. Main paths
are `iroh/src/evidence_client.rs`, `iroh/src/protocol.rs`,
`core/src/server/retained_evidence.rs`, `federation/src/history.rs`,
`federation/src/node.rs`, `federation/src/memory.rs`, and `iroh/src/tests.rs`.

## Synthesis decision

The parent read all three candidates in full. Candidate A serializes the whole
adapter. Candidates B and C allow overlapping pulls and coordinate publication
afterward. The configured GPT-5.4 runner was unavailable; the three available
runners were GPT-5.6-sol, GPT-5.5, and GPT-5.6-luna.

The GPT-5.5 cross-judge initially preferred B with C's conditional publication.
The parent rejected concurrent duplicate pulls for this high-fanout workload.
The measured baseline finished in 375.72 seconds with 485 successful transfers,
379 applied events, and 79,822 duplicate events. Recovery remained red.

The synthesis keeps A's serialized successful transition but limits ownership
to each exact scope. It takes the independent-scope concurrency sought by B
and C without their cursor comparison or stale-publication machinery. The
cross-judge reviewed this concrete revision and endorsed it.

The parent scores A/B/C respectively on the five criteria above as
4/5/4, 5/4/4, 2/3/3, 5/5/5, and 4/4/4. All preserve the permission boundary.
A loses on contention and unbounded lock wait. B and C permit duplicate pulls
and need publication rules that per-scope ownership removes.

### Caller usage and private shape

Callers keep the same interface:

```rust
let evidence = IrohScopedEvidenceEndpoint::new(local, remote);
evidence.refresh_scopes(&[a.clone(), b.clone()]).await?;
let background = evidence.clone();
tokio::try_join!(
	background.refresh_scopes(std::slice::from_ref(&a)),
	evidence.refresh_scopes(std::slice::from_ref(&b)),
)?;
```

The adapter gains a shared map from `ScopeId` to a shared Tokio mutex holding
`Option<ScopedReplicationCheckpoint>`. The map also uses a Tokio mutex. No new
public type, forwarding method, persistence store, or cursor API is added.

For each requested scope, the existing timeout contains the whole operation:

1. Look up or create its checkpoint cell, then release the map guard.
2. Acquire that scope's guard and call the existing checked pull with its cursor.
3. On success, assign `Some(report.checkpoint())` with no intervening await.
4. Release the scope guard before advancing to the next scope.

Only callers refreshing the same scope wait for each other. Every caller still
makes a fresh authorized request, including when the prior pull found no events.
Timeout, cancellation, or ingestion error leaves the previous checkpoint intact.
No operation holds two scope guards or waits for a scope guard while holding the
map guard. Source replacement uses the existing checked full replay, not cursor
ordering across source histories.

We accept one transient cell per visited scope for the adapter's lifetime.
Recreating the adapter loses this optimization and safely replays from the
beginning. Scope cardinality remains a memory-use consideration; this work adds
no eviction policy or new durability guarantee.

## Verification

The wire-observer tests use real Iroh connections, `FederatedSession`, permission
checks, scoped export, and local node ingestion. Only the test protocol observes
batches; production has no test callback. Required checks cover repeated pulls,
clones, source replacement, fresh denial, and lock cancellation or timeout.

The first test build used a nonexistent `IrohReplicator::node()` getter; using
the existing internal node field corrected the fixture. The four behavioral
tests then failed against the unchanged adapter: all cursors were `None`, and a
waiting clone opened a duplicate transfer.

After implementation, three passed. The cancellation fixture incorrectly waited
for the cancelled server request to finish sending its batch. It now checks the
successful retry's observed batches and unchanged cursor, without requiring a
cancelled request to finish. All five tests pass, including a separate partial
ingestion conflict that retains a valid prefix and retries from the last
successful checkpoint. Broader transport checks and the native recovery run
remain to be collected.

The broader library run first passed 37 of 38 tests. Its old reactive-item
revocation test waited for terminal `Invalid`, but a diagnostic run observed
`AuthorizationBlocked::Denied` with cleared value and cursor. That is the agreed
contract. The test now requires that typed denial and cleared protected output,
and fails immediately on terminal invalidation. The diagnostic print is removed.
All 38 library tests and both execution-evidence integration tests then pass.

Strict linting rejected the long introductory doc paragraph, a scope guard held
past its last use, and test `expect` calls. The implementation now drops the
guard immediately after publication. The test protocol uses Tokio mutexes and
matches cancellation errors without `expect`. No assertion or timeout was
weakened to satisfy linting. The final 40 tests and strict linting pass. No
implementation mismatch required another architecture pass.

The post-change native run `91092` passes in 231.65 seconds, with all four
original handles recovering 76.09 seconds after regrant commits. Revocation
clears the protected outputs and blocks dependent command builders. An ungranted
principal remains denied, and the second controller's journal reopens with the
committed revoke and replacement-grant commands and certified authority history.
The test does not change its 120-second phase deadline.

The trace records 548 successful scope transfers, 424 applied events, and 425
duplicates. These are whole-run counts, not normalized throughput comparisons:
the baseline failed before recovery and reopen. The measured duplicate work
falls sharply and the formerly failing recovery phase passes. Recovery latency
remains high. Neither this run nor the adapter tests establish serving-node
failover or a fresh startup history boundary. The full goal remains open.
