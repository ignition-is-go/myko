# Candidate A: adapter-wide serialized transient checkpoints

## Usage (caller's view)

The public contract does not change. A controller constructs the adapter, clones it as
needed, and asks for exact scopes; successful later calls automatically resume from the
last complete source-local cut observed by any clone.

```rust
let evidence = IrohScopedEvidenceEndpoint::new(local, remote)
    .with_request_timeout(Duration::from_secs(5));

evidence.refresh_scopes(&[orders.clone(), invoices.clone()]).await?;
// Sends the stored checkpoint for `orders`; no cursor is supplied by the caller.
evidence.refresh_scopes(&[orders.clone()]).await?;
```

Clones are one logical adapter, not independent sessions:

```rust
let background = evidence.clone();
tokio::spawn(async move { background.refresh_scopes(&[orders]).await });
evidence.refresh_scopes(&[invoices]).await?;
```

These calls serialize. This candidate deliberately favors a single, unambiguous owner
of checkpoint transitions over parallel pulls to the same remote.

## Problem

Today every timed `pull_scope_on` receives `None`, even though the protocol already
accepts a checkpoint bound to source node, exact scope, and a source-local complete log
cut. The adapter is cloneable but owns only node, endpoint, remote, and timeout, so adding
automatic resumption requires shared transient state with defined concurrency and
cancellation behavior. Only a successful ingest/report may advance it: ingest validates
scope, applies idempotently, then records coverage, while an error can leave an applied
prefix. A source-node mismatch is handled inside `pull_scope_on` by refetching that scope
from `None`.

## Shape

Keep the implementation in `libs/myko/iroh/src/evidence_client.rs`; do not add a module
or change `ScopedRetainedEvidenceEndpoint`.

```rust
#[derive(Debug, Clone)]
pub struct IrohScopedEvidenceEndpoint {
    node: Node,
    endpoint: iroh::Endpoint,
    remote: EndpointAddr,
    request_timeout: Duration,
    refresh: Arc<tokio::sync::Mutex<RefreshState>>,
}

#[derive(Debug, Default)]
struct RefreshState {
    // Each value is valid only for its map key and its recorded source node.
    checkpoints: HashMap<ScopeId, ScopedReplicationCheckpoint>,
}

impl RefreshState {
    fn checkpoint(&self, scope: &ScopeId) -> Option<ScopedReplicationCheckpoint>;
    fn record_success(&mut self, report: &ScopedReplicationReport);
}
```

`new` creates one `Arc<Mutex<RefreshState>>`; derived `Clone` shares it. The private map
models the dominant lookup directly: exact scope to the latest successful checkpoint.
`record_success` constructs the value from the authoritative report, rather than merging
positions:

```rust
self.checkpoints.insert(report.scope_id.clone(), ScopedReplicationCheckpoint {
    source_node: report.source_node,
    scope_id: report.scope_id.clone(),
    position: report.through,
});
```

`refresh_scopes` keeps one adapter-wide guard for the whole requested slice:

```rust
let mut state = self.refresh.lock().await;
for scope in scopes {
    let checkpoint = state.checkpoint(scope);
    let report = timeout(
        self.request_timeout,
        IrohReplicator::pull_scope_on(
            &self.node, &self.endpoint, self.remote.clone(), scope.clone(), checkpoint,
        ),
    ).await.map_err(timeout_error)?.map_err(replication_error)?;
    debug_report(&report);
    state.record_success(&report); // no await between success and commit
}
Ok(())
```

The mutex is the ownership boundary, not a public coordination API. It hides clone,
concurrency, and checkpoint policy behind the unchanged single-method interface. Holding
it across transport awaits intentionally serializes all refresh work for this adapter.
Cancellation or timeout drops the guard and cannot advance the in-flight scope because
the only mutation follows a successful report with no intervening await. Earlier scopes
in the same call remain advanced; the failing/cancelled scope and later scopes do not.
Retries are safe because node ingestion is idempotent, including the case where an error
left a prefix locally. Source replacement is not inferred by comparing positions:
`pull_scope_on` detects source mismatch, replays from the beginning, and the successful
report atomically replaces that scope's `(source_node, position)` pair.

Per boundary discipline, wire and batch validation remain in protocol/node ingestion.
The adapter trusts `ScopedReplicationReport`; it adds no authority/readiness policy,
permission bypass, persistence, public checkpoint API, or cross-source position maximum.

## Synthesis decision

Candidate only. This shape is intentionally the serialized-state option for later arena
comparison; no synthesis claim is made here.

## Tradeoffs accepted

- We accept head-of-line blocking across unrelated scopes in exchange for one checkpoint
  writer and cancellation semantics that require no leases, generations, or merge rules.
- We accept losing resume state when the adapter is recreated in exchange for no new
  durability store or lifecycle coupling.
- We accept holding a Tokio mutex across network I/O in exchange for making concurrent
  clones observationally equivalent to one sequential adapter.

## Alternatives considered

- Per-scope locks permit unrelated scopes to refresh concurrently, but add an index of
  coordination objects and more lifecycle policy; the unchanged public interface remains
  deep, but the private ownership model is materially harder to audit.
- Checkpoint checkout followed by an unlocked pull avoids head-of-line blocking, but
  exposes the implementation to stale completion ordering and requires generations or a
  source-aware commit protocol.
- Storing cursors in `Node` could share progress beyond this adapter, but conflates local
  replication coverage with transport-specific remote identity and introduces durability
  and ownership questions outside the requested boundary.

## Open questions and risks

- Is serializing slow or unreachable scope pulls across all clones acceptable for the
  expected controller workload?
- Can a controller retain many short-lived scope identifiers long enough for the transient
  map's unbounded growth to matter, and if so what lifecycle event can evict them safely?
- Should duplicate scope IDs in one request intentionally perform a second incremental
  pull, or should the adapter deduplicate the input while preserving error order?

## Test strategy

- Prove a second successful refresh sends the first report's checkpoint and fetches only
  later events; verify distinct scopes never reuse each other's checkpoint.
- Clone the adapter, start overlapping refreshes, and use a controllable peer to prove the
  second request does not reach the network until the first completes.
- Timeout and abort an in-flight pull after a partial ingest, then retry and prove the old
  checkpoint is reused, duplicates are harmless, and only success advances it.
- Change the peer's advertised source node at the same endpoint; prove the old checkpoint
  triggers a full refetch and the map is replaced with the new source/report pair.
- Return wrong-scope data and ingest/backend errors; prove the cursor is unchanged and
  later scopes in the slice are not attempted.

## Next implementation step

Add `RefreshState` and focused adapter tests that capture the checkpoint passed to a
controllable scoped-pull seam before changing the production refresh loop.
