# Causal loop detection without eager serialization

The server prevents identical transitions from recurring within a causal root.
Transition identity consists of entity type, entity ID, SET/DEL operation and a
hash of its serialized content. A new root starts with empty history. Distinct
payloads for one entity remain valid; replay of an earlier payload is suppressed.
The depth and derived-mutation budgets remain separate safeguards.

Previously every candidate mutation allocated a JSON byte buffer and hashed the
entire payload before testing membership, including the first and only use of an
entity identity in that root. This imposed serialization on typed mutations even
when no persistence or saga consumer needed a wire representation.

The candidate separates cheap identity discovery from content comparison:

1. The first `(type, ID, operation)` retains its immutable `Arc<dyn AnyItem>`
   snapshot without serialization.
2. Replay of the same snapshot is suppressed without serialization.
3. A different snapshot with the same identity computes the original content
   fingerprints for both values. The first snapshot is then released and the
   hashes are retained for subsequent comparisons.
4. Later occurrences use the original content-hash membership rule, including
   the existing serialization-failure fallback. Root completion releases all
   history.

This changes no method signatures, wire formats, persistence ordering, queue
ordering, loop budgets or graph-coordination policy. As with reactive storage
and relationship old-value comparison, entity snapshots must remain immutable
and their serialization must be stable. Values must be replaced through the
mutation API rather than changed through shared interior state.

The tradeoff is an additional reference to each first snapshot for the causal
root's lifetime. A repeated identity releases that snapshot once hashed. There
is no process-wide content cache and no retention between roots. Heavy repeated-
identity workloads still pay serialization and can add hash-table bookkeeping;
this is not a universal constant-factor improvement.

Four regression checks cover:

- 1,000 unique identities with zero payload serializations;
- same-allocation replay with zero serializations;
- first-repeat fingerprinting, distinct-state acceptance, old-state suppression,
  SET/DEL separation and release of the first retained snapshot;
- 5,000 deterministic transitions compared against the previous eager algorithm,
  including multiple entity types and explicit serialization failures.

Validation uses `RUST_TEST_THREADS=1 cargo flux run test-core-ci`,
`cargo flux run check-relationship-index`, and
`cargo flux run check-event-production-format` from the repository root.
The core CI run uses the repository's locked reactive dependency; external
consumer validation must separately identify its exact dependency origins.

End-to-end performance is workload-dependent. Do not transfer serialization
operation counts into an application speedup claim. Consumer-specific fixtures,
measurements and rollout decisions belong outside this repository.
