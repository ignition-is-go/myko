# Candidate B: shared transient scoped checkpoints

## Problem

`IrohScopedEvidenceEndpoint::refresh_scopes` refreshes exact scopes from one authenticated peer, but today every scope pull passes `None` as the scoped checkpoint. That preserves correctness, yet repeated refreshes replay source history from the beginning even though the protocol already returns a source-local scoped cursor. The design must keep the public `ScopedRetainedEvidenceEndpoint` interface unchanged, keep the adapter as the owner of Iroh node/endpoint/remote/timeout policy, and avoid moving readiness, authority, or persistence semantics out of `Node::ingest_scoped_batch`.

## Usage

Consumers continue to construct and call the endpoint exactly as they do today:

```rust
let evidence = IrohScopedEvidenceEndpoint::new(local_transport, remote)
    .with_request_timeout(Duration::from_secs(10));
evidence.refresh_scopes(&[scope_a.clone(), scope_b.clone()]).await?;
```

Authority coordination remains unchanged. The coordinator asks for retained evidence before replaying local retained history:

```rust
if let Some(evidence) = retained_evidence.as_ref() {
    evidence.refresh_scopes(&scopes).await?;
}
let decision = AuthorityDecisionRevalidation::from_retained_payload(payload)?;
```

`Clone` becomes intentionally useful without adding a caller-visible cursor API:

```rust
let evidence = Arc::new(IrohScopedEvidenceEndpoint::new(local, remote));
let first = evidence.clone();
let second = evidence.clone();
tokio::try_join!(
    first.refresh_scopes(&[scope_a.clone()]),
    second.refresh_scopes(&[scope_a.clone(), scope_b.clone()]),
)?;
```

Both clones consult the same transient checkpoint table. A successful refresh can shorten later pulls; a cancelled, timed-out, or failed refresh leaves the table unchanged.

## Shape

Add one private shared field to the adapter:

```rust
#[derive(Debug, Clone)]
pub struct IrohScopedEvidenceEndpoint {
    node: Node,
    endpoint: iroh::Endpoint,
    remote: EndpointAddr,
    request_timeout: Duration,
    checkpoints: Arc<ScopedEvidenceCheckpoints>,
}

#[derive(Debug, Default)]
struct ScopedEvidenceCheckpoints {
    inner: tokio::sync::Mutex<HashMap<ScopeId, ScopedReplicationCheckpoint>>,
}
```

`IrohScopedEvidenceEndpoint::new` initializes `checkpoints: Arc::default()`. `Clone` then naturally shares the transient table because the field is an `Arc`. No checkpoint appears in the constructor, trait method, or public adapter API.

The private helper owns the only synchronization policy:

```rust
impl ScopedEvidenceCheckpoints {
    async fn checkpoint_for(&self, scope: &ScopeId) -> Option<ScopedReplicationCheckpoint>;

    async fn advance_after_success(&self, report: &ScopedReplicationReport);
}
```

`refresh_scopes` stays the single caller-facing operation:

```rust
for scope in scopes {
    let checkpoint = self.checkpoints.checkpoint_for(scope).await;
    let report = timeout(
        self.request_timeout,
        IrohReplicator::pull_scope_on(
            &self.node,
            &self.endpoint,
            self.remote.clone(),
            scope.clone(),
            checkpoint,
        ),
    ).await??;

    self.checkpoints.advance_after_success(&report).await;
}
```

`advance_after_success` stores `report.checkpoint()`, keyed by `report.scope_id`. The report is produced only after the node validates the scoped batch, idempotently ingests each event, records replication coverage, and returns success. That keeps checkpoint advancement behind the same trust boundary as the existing local coverage update.

The checkpoint remains source-local and scope-local: `source_node + scope_id + through`. It is not a per-scope ordinal, not a local position, and not a global ordering across scopes. `pull_scope_on` already rejects a wrong-scope checkpoint before network I/O and refetches from `None` if the remote advertises a different `source_node`, so stale shared state is self-correcting at the transport boundary.

Concurrency is deliberately last-writer-wins over successful reports, with a guard against local regression for the same source:

```rust
match existing {
    Some(old) if old.source_node == next.source_node && old.position > next.position => keep old,
    _ => store next,
}
```

If a concurrent refresh begins from an older checkpoint and succeeds after a newer one, it must not move the shared table backwards for the same source. If the source node changed, store the successful report's checkpoint; the protocol has already replayed from `None` when needed. Cancellation and timeout do not call `advance_after_success`, so partial local ingest may exist without an advertised adapter cursor. The next refresh may replay a prefix, which is acceptable because ingestion is idempotent and safer than claiming a cursor after an interrupted operation.

## Synthesis decision

This candidate is intentionally distinct from persistent or caller-managed cursor designs: the adapter owns a shared, transient checkpoint cache and hides it behind unchanged `refresh_scopes` usage. It has good interface depth because one existing public method now also manages resume state, clone sharing, source changes, timeout/cancel behavior, and stale-cursor recovery without making callers coordinate those concerns.

## Tradeoffs accepted

- We accept losing checkpoints on process restart in exchange for no persistence schema, recovery, readiness, or authority change.
- We accept a short mutex around a small map in exchange for simple clone-safe semantics and no per-scope task registry.
- We accept duplicate replay after failure or cancellation in exchange for success-only cursor advancement.
- We accept one checkpoint per exact scope in exchange for avoiding cross-scope max cursors that could skip source-local log gaps.

## Alternatives considered

- Persist checkpoints in the node or redb cursor store. This hides replay work across restart, but exposes storage, transport-peer identity, and readiness policy outside the adapter's current responsibility.
- Add checkpoint parameters or return values to `ScopedRetainedEvidenceEndpoint::refresh_scopes`. This gives callers control, but makes them understand source-local scoped cursors and weakens the existing deep interface.
- Compute a shared max cursor across all requested scopes. This is invalid because scoped export advances through unrelated source events without disclosing them; each exact scope needs its own `source_node + scope_id + through` cursor.
- Keep checkpoints per clone. This is simpler internally, but wastes the fact that controllers commonly retain and clone the adapter and would still replay after clone fan-out.

## Open questions and risks

- Should the checkpoint map be bounded or pruned if a long-lived adapter sees unbounded scope cardinality?
- Should same-source regression protection treat `None` as lower than any concrete position, matching durable checkpoint rules?
- Should tracing include whether a checkpoint was used and whether it advanced, so replay behavior is visible during rollout?

## Next implementation step

Add the private `ScopedEvidenceCheckpoints` helper and wire `refresh_scopes` to read a checkpoint before each pull and store `report.checkpoint()` only after successful `pull_scope_on`.
