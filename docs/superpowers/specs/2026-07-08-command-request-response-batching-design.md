# Command Request/Response: real responses + batching

- **Status:** Design only — **DO NOT BUILD YET.** Commit in myko when there's a moment.
- **Date:** 2026-07-08
- **Author:** hardy-lynx (rship) — for azure-jaguar (myko owner)
- **Domain:** `libs/myko/core` (wire + client), `libs/myko/server` (ws_handler + correlation)

## Motivation

Two coupled problems on the command path:

1. **Commands to clients are fire-and-forget, and we want them to stop being that.**
   `send_command_request_to` (`server/src/client_registry.rs:51`) records `tx →
   (command_id, Instant)` in `outbound_commands_by_tx` purely for a timing
   `trace!`. Nothing awaits or consumes the response. We want command dispatch to
   eventually carry a **real return value** back to a waiting caller (e.g. an
   executor reporting the actual result of an action, an RPC-style call).

2. **We deserialize many response frames for no reason.**
   Every inbound `ws:m:command-response` is parsed into a full
   `CommandResponse { response: Value, tx }` (`server/src/ws_handler.rs:1278`),
   only for the handler to read `tx`, emit a `trace!`, and drop `response`
   (`ws_handler.rs:1290-1302`). Two wastes stacked:
   - **(W1) payload deser nobody consumes** — the whole `response` Value is parsed
     to be thrown away.
   - **(W2) per-frame envelope tax** — each frame re-pays the
     `#[serde(tag="event", content="data")]` tag-parse + dispatch. This is the
     exact tax `EventBatch` already amortized ~7:1 on the event path
     (`wire/message.rs:74`, `client/mod.rs:909`); the command-response path never
     got the equivalent.

The forward-looking goal (real request/response) **inverts W1** — once a caller
awaits the payload, the deser is justified — and batching kills W2 regardless.
This design covers both so they land coherently rather than as two retrofits.

## Non-goals

- No change to the inbound command (server→client) batch: `CompactBatchTargetAction`
  already packs many action assignments into one `ws:m:command` frame with payload
  dedup (`rship/.../multi_action.rs:825`). This design is about the **response**
  direction and about making individual commands awaitable.
- No parallel schema versioning. If a wire body changes incompatibly, that is an
  SDK major-version bump, per the repo's "no V1/V2 variants" rule.

## Design

### 1. Wire type (`libs/myko/core/src/wire/message.rs`)

Add one variant, mirroring `EventBatch`:

```rust
#[serde(rename = "ws:m:command-response-batch")]
CommandResponseBatch(Vec<CommandResponse>),
```

`CommandResponse` is unchanged (`{ response: Value, tx: String }`,
`wire/command.rs:15`). TS regen picks it up via the existing `#[ts(export)]` on
`MykoMessage`. Add the metrics-label arm in `server/src/ws_timing.rs:73`.

Backward-compatible: old SDKs keep sending singular `CommandResponse`; the server
accepts both. Nothing is forced to adopt the batch.

### 2. Server receive loop (`server/src/ws_handler.rs`)

Add a `CommandResponseBatch(resps)` arm next to the existing `CommandResponse`
arm (`:1278`) that runs the same correlate-by-`tx` body over each element. Because
`tx` is unique per request, intra-batch ordering is irrelevant — each element
resolves independently. Pure loop over the existing body; no new logic.

Do the same for the **client** receive side (`client/src/... client/mod.rs:673`)
*iff* we also batch the server→client direction (§5). For the executor→server
direction only §2 server-side is needed.

### 3. Kill W1 now — deser-skip fast path (independent of everything else)

This is a **standalone present-tense win** and can land before the request/response
migration. While correlation is telemetry-only, the server needs only `tx`, not
`response`. Options, cheapest first:

- **(a)** Give the server a lightweight `CommandResponseTx { tx: String }` shape it
  can deserialize instead of the full `CommandResponse` when no response sink is
  registered — `response` is left as un-parsed input. Requires the inbound
  dispatch to peek the `event` tag and choose the shape before parsing `data`.
- **(b)** Change `CommandResponse.response` to `Box<RawValue>` (serde_json
  `RawValue`) so the payload is captured but not parsed until a consumer asks.
  Zero-copy-ish; parses lazily only when a sink consumes it. This composes
  naturally with §4 (sink present → parse; absent → drop unparsed).

**(b) is preferred** — it makes W1 disappear automatically as a function of
"is anyone waiting for this response," which is exactly the §4 predicate. CBOR path
needs the analogous treatment (ciborium has no `RawValue`; may need a
`serde_bytes`/deferred-decode shim, or accept that the fast path is JSON-only
initially — call this out at build time).

### 4. Graduate correlation to a real response sink (the "not fire-and-forget" core)

Today `outbound_commands_by_tx: Mutex<HashMap<tx, (command_id, Instant)>>`
(`ws_handler.rs:1286`). To make a command awaitable, the value must optionally
carry a **response sink**:

```rust
struct OutboundCommand {
    command_id: Arc<str>,
    started: Instant,
    sink: Option<ResponseSink>, // None = fire-and-forget (today's behavior)
}
```

`ResponseSink` mirrors what the **client already has** — `command_response_handlers:
Mutex<HashMap<String, CommandResponseHandler>>` where `CommandResponseHandler =
Box<dyn FnOnce(Result<Value, String>) + Send>` (`client/mod.rs:62, 258`). The
server side is the missing mirror image. A `tokio::sync::oneshot` or a
one-shot cell is the natural sink; the dispatcher awaits it.

Requirements this introduces:

- **Timeout + cleanup.** A dead/slow executor must not strand awaiters or leak map
  entries. Every sink-carrying entry needs a deadline; on expiry, resolve the sink
  `Err("timeout")` and remove. (Fire-and-forget entries keep today's remove-on-
  response / eventual-sweep behavior.) A periodic sweep or a per-entry timer.
- **Backpressure — no bounded channels.** Per the repo rule ("fix the root cause,
  don't cap queue sizes"), the bound is the **timeout**, not a fixed-size pending
  map. An awaiting caller either gets a response or a timeout error; it never
  blocks unboundedly and the map self-drains via deadlines.
- **Correlation is unchanged for the batch.** A `CommandResponseBatch` resolves N
  sinks by `tx`; each element is independent.

`send_command_request_to` gains a sibling that returns an awaitable
(`send_command_request_awaited` → `oneshot::Receiver<Result<Value, String>>` or a
cell), registering a sink. The existing fire-and-forget entrypoint stays as-is
(`sink: None`).

### 5. Executor-side response batcher (client, `libs/myko/core/src/client`)

Mirror `send_event_batch` (`client/mod.rs:909`). `CommandResponder::respond_ok`
(`client/mod.rs:87`) currently encodes and sends immediately. To batch:

- Buffer responses into a `Mutex<Vec<CommandResponse>>` on `MykoClientInner`.
- **Flush policy — needs a decision.** Options: time-window (per-tick, like the UE
  event flush), count-threshold, or explicit `client.flush_responses()`.
  Fire-and-forget and RPC-with-generous-timeout both tolerate up to one window of
  added latency, so a **time window** (single-digit ms) is the safe default. Make
  it configurable; default conservative.
- `send_command_response_batch(Vec<CommandResponse>)` send helper (copy
  `send_event_batch`, including the `is_empty()` short-circuit and
  `send_or_queue`).
- Expose either a `respond_ok_batched` variant or a client batching-mode flag so
  existing `respond_ok` callers transparently buffer.

### 6. Symmetric server→client batching — optional, defer

Only if the **server** batches responses to UI clients (server produces
`CommandResponse` at `ws_handler.rs:1392`). Needs per-connection response buffering
on the writer + the client-side `ws:m:command-response-batch` receive arm from §2.
Not on the executor perf path. Punt unless a UI-facing command becomes high-volume.

## Suggested landing order

1. **§3 (RawValue deser-skip)** — standalone, kills W1 immediately, no protocol
   change. Safe to land first and independently.
2. **§1 + §2** — wire variant + server receive loop. Backward-compatible; unlocks
   the ingest win the instant a responding SDK emits batches.
3. **§4** — response sink + timeout/backpressure. The actual "not fire-and-forget"
   capability. Bigger; do when the first real awaiting caller exists.
4. **§5** — executor batcher + flush policy. Cross-SDK: any responding SDK (UE via
   arctic-badger, TS SDK) must emit the new frame to benefit; server accepting it is
   backward-compatible.

## Cross-SDK note

Same adoption shape as `EventBatch`: the server accepting `CommandResponseBatch` is
backward-compatible, but the **win only materializes when a responding SDK emits
it**. Coordinate the emit side (UE = arctic-badger, TS SDK) once §1/§2 land.

## Open decisions for azure-jaguar

- §3: RawValue (JSON) vs a `{tx}`-only fast shape; CBOR lazy-decode story.
- §4: oneshot vs cell for the sink; sweep-timer vs per-entry deadline.
- §5: flush trigger (time-window default; window length).
