# Candidate C: optimistic per-scope checkpoints

## Problem

`IrohScopedEvidenceEndpoint` currently owns the local `Node`, Iroh endpoint,
remote address, and timeout, but no cursor state; `refresh_scopes` therefore
pulls every scope from `None`. The public
`ScopedRetainedEvidenceEndpoint::refresh_scopes(&[ScopeId])` contract must stay
unchanged. A scoped checkpoint is bound to source node and scope, and its
`through` position is a source-local whole-log cut (it may skip events from
other scopes). The source-node mismatch rule already forces a full refetch, and
`Node::ingest_scoped_batch` validates, applies idempotently, and records
coverage before returning success. This candidate adds only in-memory adapter
state and never holds a lock across network transfer.

## Usage (caller's view)

The caller remains unaware of checkpoints or concurrency:

```rust
let evidence = IrohScopedEvidenceEndpoint::new(local_replicator, peer);
evidence.refresh_scopes(&[scope_a.clone(), scope_b.clone()]).await?;
```

Existing authority refresh code continues to pass a borrowed scope slice:

```rust
endpoint.refresh_scopes(std::slice::from_ref(&realm)).await?;
```

Two controllers may safely share the same endpoint and refresh overlapping
scopes. They may perform duplicate pulls; both successful ingests are
idempotent, and only a completion based on the current checkpoint can publish
the next one.

## Shape

Add private state keyed by exact scope, with a short critical section only:

```rust
type ScopeCheckpoints = Arc<std::sync::Mutex<HashMap<ScopeId, ScopedReplicationCheckpoint>>>;

pub struct IrohScopedEvidenceEndpoint {
    node: Node,
    endpoint: iroh::Endpoint,
    remote: EndpointAddr,
    request_timeout: Duration,
    checkpoints: ScopeCheckpoints,
}

impl IrohScopedEvidenceEndpoint {
    pub fn new(local: IrohReplicator, remote: EndpointAddr) -> Self;
    pub const fn with_request_timeout(self, timeout: Duration) -> Self;
}
```

The trait method keeps its existing signature and remains sequential across the
requested slice:

```rust
fn refresh_scopes<'a>(&'a self, scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a>;
```

Its per-scope operation is conceptually:

```text
for scope in scopes:
    expected = checkpoints.lock().get(scope).cloned()
    report = timeout(pull_scope_on(node, endpoint, remote, scope, expected)).await?
    candidate = ScopedReplicationCheckpoint::new(report.source_node, report.scope_id,
                                                  report.through)
    lock checkpoints
    if checkpoints.get(scope) == expected:
        checkpoints.insert(scope, candidate)
    // If another pull won, discard candidate; retained history is still valid.
return Ok(())
```

The lock is never held while connecting, reading, ingesting, or waiting on the
timeout. A missing map entry represents “no successful checkpoint”; it is not
an authority or readiness claim. A failed pull never publishes state, while a
successful report is safe to publish because ingest records coverage before
returning it. The adapter still exposes no cursor API, durable store, new
replication authority, or cross-source maximum.

This is a deep enough adapter boundary: callers issue one refresh operation and
the adapter hides checkpoint selection, source replacement, timeout mapping,
and stale-publication policy. Only the requested scopes and ordinary
`RetainedEvidenceError` remain visible. The short lock is a state-publication
mechanism, not a transfer scheduler (per separate-before-serializing-shared-
state and boundary-discipline).

## Synthesis decision

This is candidate C’s proposed base. It deliberately chooses optimistic
per-scope state over an adapter-wide async mutex: transfer latency cannot block
unrelated scopes, and duplicate pulls are acceptable because ingest is
idempotent. No other candidate is incorporated here.

## Tradeoffs accepted

- We accept duplicate in-flight pulls in exchange for no adapter-wide lock
  spanning network I/O.
- We accept process-local, restart-lost checkpoints in exchange for no new
  durable persistence or public cursor contract.
- We accept sequential iteration within one call in exchange for preserving
  partial-success semantics and simple error behavior.
- We accept a tiny synchronous lock around map access in exchange for a safe
  compare-and-publish point.

## Alternatives considered

An adapter-wide `tokio::sync::Mutex` around each full refresh would serialize
all scopes and make checkpoint publication straightforward, but exposes no
additional caller capability while coupling unrelated transfers to the
slowest peer. It loses on interface depth and concurrency behavior.

Per-call local checkpoints avoid shared state entirely, but cannot resume a
later call and reduce every refresh to a full pull. They hide less policy than
this candidate and provide no continuity benefit.

## Verification plan

- Refresh the same scope twice and assert the second request resumes from the
  published `through` position while retaining the existing idempotent result.
- Arrange overlapping delayed pulls where the older completion arrives last;
  assert the checkpoint map keeps the newer successful checkpoint and that a
  subsequent pull does not regress.
- Replace the source node between calls; assert the stale checkpoint is
  discarded by `pull_scope_on`, the scope is replayed from `None`, and only a
  successful replay is published.
- Force a timeout or ingest error; assert no checkpoint is inserted or
  replaced, while earlier scopes in the same call remain retained.

## Open questions and risks

- Should equal or incomparable source-local positions be treated as an
  unconditional replacement, or should publication require a source-aware
  monotonicity check?
- Is duplicate bandwidth acceptable under the expected controller fan-out, or
  should a future design add per-scope single-flight state without changing
  this public interface?
- Should endpoint cloning share the checkpoint map (the proposed `Arc`) or
  intentionally start a fresh map per clone?

## Next implementation step

Add the private per-scope map and two short-lock helpers (`snapshot` and
`publish_if_unchanged`), then cover stale-completion and source-replacement
cases with focused Iroh tests.

Evidence: `libs/myko/iroh/src/evidence_client.rs`; scoped pull protocol in
`libs/myko/iroh/src/protocol.rs:1320-1369`; checkpoint/report types and ingest
ordering in `libs/myko/federation/src/history.rs:430-515` and
`libs/myko/federation/src/node.rs:4035-4075`.
