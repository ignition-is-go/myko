# Profiling work events

With the `profiling` feature, the `myko::work` tracing target emits TRACE events
that consumers may aggregate without installing additional store subscribers:

- `store_reduce`: a single-row reducer call, or a uniform grouped store reducer
  call, labeled by entity type. `rows` is submitted row count, not changed rows.
  Mixed replacement reduction is not covered by this event.
- `reactive_predicate`: a predicate call in the standard source-filter and fallback
  query factories, labeled by query type. Custom query implementations are not
  exhaustively covered.
- `index_route`: a belongs-to routing attempt; it need not find a live bucket.
- `wire_send`: a successful WebSocket send, labeled by message kind; `bytes` is
  the serialized payload size. This includes command responses and control
  traffic, not just reactive value delivery.

Events describe executed operations, not causal identities or transaction IDs.
Subscribers define capture boundaries. Diagnostic aggregation can change
scheduling; compare counters across repetitions and measure production CPU in
separate runs with aggregation disabled. Existing Hyphae fanout spans identify
store diff emissions without adding observers that would change demand.
