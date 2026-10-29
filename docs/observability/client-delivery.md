# Per-client delivery diagnostics

Use this when clients remain connected but reactive control values arrive unevenly.
The hooks run in `myko-server`, so Rship picks them up when it consumes a release
containing this change. No wire protocol or Unreal plugin change is required.

## Enable

Set `MYKO_WS_DIAGNOSTICS=1` in the **server process environment** before starting
it. The setting is read once. Diagnostics are off by default. With diagnostics
enabled, counters are flushed to OTel every five seconds and on teardown;
periodic diagnostic logs remain off. No payload values, property names, subscription arguments,
or transaction IDs are logged or exported.

For metrics, configure the host's existing OTLP pipeline with
`OTEL_EXPORTER_OTLP_ENDPOINT` (or the existing `MYKO_TRACING_ENDPOINT` fallback).
For a short log-based investigation, also set `MYKO_WS_DIAGNOSTICS_LOGS=1`
and enable `myko_server::delivery=debug` in the tracing filter. This emits one
client summary and at most five operation summaries (ranked by encoded bytes)
per connection per five-second window and on teardown. Metrics still include
every tracked operation. Leave this switch unset for metrics-only operation.
Without an exporter, explicitly enabled structured summaries still work. The setting does not
configure or repair collection: verify that the server instance appears in the
metrics backend before interpreting an empty dashboard as zero activity.

Import [client-delivery-dashboard.json](client-delivery-dashboard.json) into
Grafana, choose the Prometheus data source, and filter by `instance`, `peer`, and
`client_id`. The peer is the TCP address visible to the server, which can be a
proxy rather than the ultimate client. Join it to the existing `Client` entity
or connection log to identify an executor. `client_id` changes on reconnect;
`peer` retains the IP across those sessions. Connection logs include the port.

## Read optional debug summaries

`client delivery summary` contains active subscriptions, requested subscriptions
not yet marked ready, equivalent initial subscription requests, observed queue
high-water mark, maximum queue/materialization/serialization/send times, and
socket write failures. Failed subscription builds can remain in the not-ready
count; it is not a count of only runnable jobs. Equivalent requests are extra
active transactions with the same operation and initial arguments/window/rate,
excluding `tx`. They are candidates for investigation, not proof of a bug.

`operation delivery summary` is grouped by connection, operation kind, and
report/query/view name. Fields are **counts over `window_s`**, not rates:

| Field | Meaning |
| --- | --- |
| `produced` | Report/query/view callbacks attempting to enqueue a response, before view sampling/coalescing |
| `enqueue_failed` | Those enqueue attempts rejected by the bounded channel |
| `messages`, `bytes` | Frames successfully written to the WebSocket sink, and encoded application bytes (JSON or CBOR; excludes TCP/WebSocket overhead) |
| `unchanged` | Fully inspected responses whose report value or affected item values match retained fingerprints; initial snapshots are excluded |
| `property_occurrences` | JSON leaves included in inspected report values or query/view upsert items, counting repeats |
| `changed_properties` | Leaves added or changed relative to the previous value **in this subscription**; initial leaves count as added |
| `removed_properties` | Leaves removed from a report/item, including known deleted query/view items |
| `unique_properties` | Distinct `(subscription, item, leaf path)` identities sent in this window, including unchanged leaves |
| `upserts`, `deletes` | Item operations inspected in query/view diffs; the legacy mirror is not counted again |
| `max_produced_gap_ms` | Largest interval between callbacks producing updates for a subscription |
| `max_gap_ms` | Largest interval between successful sends on any one subscription to this operation, including the interval since the previous window |
| `incomplete_inspections` | Responses or uniqueness accounting that exceeded inspection limits; property/duplicate counts are partial |

A property here is a scalar JSON leaf (array positions count separately); empty
objects/arrays count as one leaf. This is transport-level accounting, **not**
a count of Unreal UProperties applied on the game thread. Query/view envelopes,
sequence numbers, window ordering and transaction IDs do not count as properties.
Window-order-only updates are not classified as unchanged.

Property equality uses 64-bit path/value fingerprints. Collisions are possible;
these diagnostics are evidence for investigation, not a deduplication decision.
They never suppress an update. The baseline is reset after incomplete inspection,
so the next complete response cannot be labeled unchanged against stale state.
Cancellation releases the subscription baseline. A fresh query/view sequence-0
snapshot resets its item baseline. Unattributed frames are labeled explicitly.

For example, `messages=1000 property_occurrences=50000 changed_properties=20
unchanged=980 window_s=5` means 200 responses/sec carried 10,000 leaves/sec,
while only four leaf changes/sec were observed. Find that client's operation,
then inspect its subscription usage and report dependencies.

## Locate the delay

1. Rank `myko_ws_delivery_messages_total` and `myko_ws_delivery_bytes_total` by
   client, peer and operation. Compare produced and sent rates over a sustained
   interval: the counters are not atomically sampled, and window boundaries can
   temporarily skew their difference. View sampling intentionally reduces sends.
2. Compare unchanged rate and property occurrence/change rates. High traffic
   with few changes points toward redundant publication or oversized resends.
3. Check equivalent subscriptions and connection opens/closes. Reconnects create
   new initial snapshots, so high initial property counts can be a consequence.
4. Compare `produced_gap` with send `gap` to separate uneven production from
   delivery delay. Correlate `gap` with `queue_wait`, `materialize`, `serialize`, and `send`.
   Queue wait measures callback enqueue to dequeue for deferred report/query/view
   frames. It excludes deliberate time held in the view sampler and does not
   cover the priority/normal command queues. `queue_peak` samples all three send
   queue lengths plus producer observations of the deferred queue; it is an
   observed high-water mark, not an exact continuous gauge.
5. Materialization measures conversion of a deferred report output to JSON;
   serialization measures JSON/CBOR frame encoding. Send time includes the sink
   send/flush wait. Successful send means accepted by the local socket path,
   **not acknowledged or applied by Unreal**. Low server times with choppy Unreal
   application requires receive/decode/game-thread hooks at that endpoint.

Histograms use seconds and have sub-millisecond through five-second buckets.
The client-level `inspection` histogram measures the additional diagnostic work
on the writer path. `untracked` and `inspection_incomplete` counters expose
coverage limits even when optional debug logs are disabled.

Operation-tagged histograms include subscription setup, production gap, queue wait, report
materialization, serialization, send, and inter-send gap. Connection-level gauge
metrics include active subscriptions, equivalent initial subscriptions, and queue
peak. Counters are exported on each five-second flush (and teardown), so use a
rate window longer than both this window and the collector interval.

## Bounds and operational cost

Detailed inspection is opt-in because it performs extra item serialization,
fingerprinting, locking, and metric recording on the writer path. Use it during
an investigation and compare against diagnostics-off performance; it can affect
the workload being measured. There is no per-message logging.

Per connection, tracking is capped at 256 subscriptions and 256 operation groups
per window, 4,096 leaves per inspected value/update, 65,536 retained leaves,
and 65,536 unique property identities per window. Stored operation, transaction
and item identifiers are limited to 1,024 bytes. Property inspection skips
encoded frames over 256 KiB and JSON nesting beyond 64 levels. Counts above
these limits increment `untracked` or `incomplete_inspections`; they never silently
claim completeness. Values are never retained, only fingerprints and item IDs.
OTel labels omit transaction IDs, property paths, argument fingerprints and item
IDs. They include connection IDs, so scope the capture in busy environments and
respect the metrics backend's series limits.

The dashboard is an importable template, not an installed dashboard. Prometheus
names assume the usual OTLP translation (`.` → `_`, counters → `_total`, seconds
histograms → `_seconds_bucket`). Adjust names if your collector changes them.
