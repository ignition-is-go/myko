# CBOR wire-protocol migration

**Status:** Spec — awaiting implementation plan
**Date:** 2026-04-24
**Scope:** Replace MessagePack with CBOR as the binary encoding for the myko WebSocket wire protocol; delete the explicit protocol-switch handshake; preserve JSON as a co-equal wire encoding.

---

## 1. Goal and non-goals

### Goal

Replace `rmp_serde` (MessagePack) with `ciborium` (CBOR) as the binary encoding for the myko WebSocket wire protocol, unblocking the adjacently-tagged-enum bug that currently forces JSON-only round-trips for some message variants. Preserve JSON as a co-equal wire encoding. Replace the explicit protocol-switch handshake with implicit capability-via-demonstration negotiation derived from incoming WebSocket frame type.

### Non-goals

- No schema/IDL adoption (no protobuf, no FlatBuffers, no `.proto` files).
- No abandonment of serde — every existing `#[derive(Serialize, Deserialize)]` is preserved untouched.
- No change to consumer-facing macro ergonomics — `myko_item` and downstream crates author Rust types exactly as today.
- No performance overhaul of payload encoding. Consumer-defined commands, queries, reports, and events stay schemaless and encode as map-shaped CBOR data, equivalent in size and shape to msgpack.
- No changes to the `MykoMessage` envelope shape, the type registry, or downstream-crate code.

---

## 2. Why CBOR (and not the alternatives)

The constraint set:

1. Binary encoding on the hot path (perf in TS-on-web, Python, and Unreal C++).
2. JSON kept as a co-equal wire encoding (debug, devtools, external integrations).
3. Adjacently-tagged enums (`#[serde(tag = "event", content = "data")]`) must round-trip without hand-rolled `Serialize`/`Deserialize` impls.
4. Cross-language client coverage (Rust, TS, Python, Unreal C++, C#).
5. No IDL — consumers author plain Rust types as today.
6. Open to dropping serde if needed, but no derive-based alternative meets all the other constraints.

That set is fundamentally what serde provides; the bug is in `rmp_serde` specifically, not in serde. The realistic options for the binary path are therefore other serde-compatible binary formats, ranked against the constraint set:

| Format | Cross-language | Enum handling | JSON-compatible data model | Notes |
|--------|----------------|---------------|----------------------------|-------|
| **CBOR (`ciborium`)** | Mature in all 5 langs | Clean (tagged enums = string-keyed maps) | Yes (CBOR data model is a strict superset of JSON's) | **Recommended.** |
| MessagePack (`rmp_serde`) | Mature | **Broken** for adjacently-tagged enums | Yes | Status quo; the blocker. |
| MessagePack (alternative crate) | Mature | Unknown without testing | Yes | Fallback if ciborium also fails the gate. |
| Postcard / Bincode 2.x | Rust-only | Clean | No | Disqualified by cross-language requirement. |
| Protobuf | Best | Via `oneof` | Yes (proto3 JSON mapping) | Disqualified: requires IDL, no perf payoff for payload-dominated traffic, downstream `build.rs` cost. |

CBOR via `ciborium` is the only option that satisfies all six constraints simultaneously.

---

## 3. Per-language libraries

### Rust (server, Rust client, Leptos client)

`ciborium` — RFC 8949 compliant, serde-compatible, BSD-2-Clause. Used for both encode and decode.

### TypeScript (web hot path, plus svelte/vue wrappers)

`cbor-x` — fastest CBOR encoder/decoder in JavaScript, browser-compatible, no Node-only dependencies, MIT-licensed, actively maintained.

### Python

`cbor2` — de-facto Python CBOR library, pure-Python with optional C extension shipping prebuilt wheels for major platforms.

### Unreal C++

`tinycbor` (Intel, MIT) — pure C, ~2 KLoC, no exceptions, no STL, no allocator assumptions. Compiles cleanly inside an Unreal Module under UBT and runs in Shipping builds.

Integration shape: a thin Unreal Module that wraps `tinycbor` and converts between CBOR bytes and Unreal's `TSharedPtr<FJsonValue>` runtime tree (the same intermediate the existing JSON path uses via `FJsonSerializer`). Higher layers of the Unreal client that consume incoming messages do not change; only the bytes-to-`FJsonValue` layer flips between `FJsonSerializer` (text frames) and the new CBOR module (binary frames).

**Fallback if `tinycbor` integration causes friction:** hand-roll a minimal CBOR decoder against the specific subset of CBOR types myko emits. CBOR's wire encoding is intentionally simple (major type in top 3 bits, varint length, payload); a single-purpose decoder of roughly 500 lines is realistic.

### C#

Out of hot path per current usage. Migration can lag the others; a C# CBOR library (e.g. `PeterO.Cbor`) is integrated when prioritized.

---

## 4. Format negotiation: capability-via-demonstration

The current explicit handshake (`SWITCH_TO_MSGPACK` magic-string text frame, `MykoMessage::ProtocolSwitch` confirmation reply, `use_binary: AtomicBool` on the session) is deleted in favor of implicit negotiation derived from WebSocket frame type.

### Server-side rules

- Each WebSocket session tracks `outgoing_format: AtomicU8`, initialized to **JSON**.
- On every received `Message::Binary` frame, the server promotes `outgoing_format` to **CBOR**. The transition is sticky — `outgoing_format` never downgrades back to JSON within a session.
- `Message::Text` frames do not change `outgoing_format`.
- All server-originated messages (responses, events, broadcasts) encode using the current `outgoing_format`.

### Client-side rules

- Client picks one format at startup (constructor argument or `set_protocol(MykoProtocol::CBOR)`) and uses it for all outgoing frames.
- Client decodes incoming frames based on WebSocket frame type: `WsFrame::Text` → JSON, `WsFrame::Binary` → CBOR.

### Properties this guarantees

- A client that has never demonstrated CBOR capability never receives CBOR. Pure-JSON clients (during migration, debugging via `wscat`, external integrations) receive JSON for the entire lifetime of their connection.
- A pure-CBOR client demonstrates capability on its first frame and receives CBOR thereafter.
- A hybrid client that occasionally emits a JSON frame stays in CBOR mode (text frames do not demote), so debug tools piggybacking on a real client connection do not accidentally degrade the connection's encoding.
- The "client always sends first" invariant means the JSON default is essentially unreachable in practice. It remains as a cheap defensive fallback (one atomic load); if it ever fires for a server-pushed message, JSON is universal and harmless.

### State machine

```
                   receive Message::Binary
                         │
                         ▼
   ┌────────┐                    ┌────────┐
   │  JSON  │ ─────────────────▶ │  CBOR  │ ◀── self-loop on
   └────────┘                    └────────┘     any received frame
        ▲
        │ self-loop on Message::Text
```

JSON state: text frames keep the session in JSON; the first binary frame transitions to CBOR. CBOR state: any received frame keeps the session in CBOR — the transition is one-way.

---

## 5. Code surfaces

### Rust (`libs/myko/core` and `libs/myko/server`)

| File | Change |
|------|--------|
| `core/src/wire/message.rs` | Delete `MykoMessage::ProtocolSwitch` variant. |
| `core/src/client/mod.rs` | Rename `MykoProtocol::MSGPACK` → `MykoProtocol::CBOR`. Replace `rmp_serde::to_vec` / `from_slice` with `ciborium::ser::into_writer` / `de::from_reader` in `encode_protocol`, `decode_message`, and the message-send paths. Remove the `// Force JSON until msgpack report-response round-trip is diagnosed` workaround at the existing line ~317 once gate test passes. |
| `core/src/wire/command.rs` | Rename `EncodedCommandMessage::Msgpack(Vec<u8>)` → `Cbor(Vec<u8>)`. Switch the `MykoProtocol::MSGPACK` arm in `encode_command_message` to ciborium. |
| `core/src/wire/event/mod.rs` | Rename `from_mp` to `from_cbor` (or `decode_binary`). Replace rmp_serde imports and calls with ciborium. |
| `core/src/server/protocol.rs` | Rename `message_to_msgpack` → `message_to_cbor`. Switch implementation to ciborium. |
| `core/src/server/client_session.rs` | Replace the `rmp_serde::from_slice` call (currently ~line 931) with ciborium. |
| `server/src/ws_handler.rs` | Delete the `SWITCH_TO_MSGPACK` magic-string branch (currently ~lines 712-725). Delete the `priority_tx.try_send(MykoMessage::ProtocolSwitch ...)` confirmation send. Delete the dead-letter logging arm for `MykoMessage::ProtocolSwitch` (currently ~lines 1395-1402). Replace `use_binary: AtomicBool` with `outgoing_format: AtomicU8`; update on every `Message::Binary` receive (one-way to CBOR); use for all outgoing encodes. |
| `Cargo.toml` files | Add `ciborium` dependency. Remove `rmp_serde` once all call sites are converted. |

### Other-language clients

| Crate / package | Change |
|-----------------|--------|
| `libs/myko/ts` | Replace existing msgpack library with `cbor-x`. |
| `libs/myko/py` | Replace existing msgpack library with `cbor2`. |
| `libs/myko/cpp` (Unreal) | Add Unreal Module wrapping `tinycbor` and exposing CBOR ⟷ `TSharedPtr<FJsonValue>` conversion mirroring the existing `FJsonSerializer` API. |
| `libs/myko/csharp` | Defer; integrate when prioritized. |
| `libs/myko/svelte`, `libs/myko/vue` | No direct change — wrap `libs/myko/ts` and inherit. |
| `libs/myko/leptos` | No direct change — uses Rust client, inherits. |

---

## 6. Migration ordering

1. **Gate test.** Reproduce the current `rmp_serde` failure case as a unit test, run it through `ciborium::ser::into_writer` → `ciborium::de::from_reader`, assert round-trip equality. The most likely candidate is `MykoMessage::ReportResponse` based on the existing `// Force JSON until msgpack report-response round-trip is diagnosed` comment, but the test must reproduce whatever specific shape is currently broken. **If the gate test fails, the migration halts here and the design is reopened.**
2. **Rust server + Rust client + Leptos** — single PR. CI passes; Leptos client continues working against the Rust server. Removes the protocol-switch handshake. Hard cut from msgpack on the Rust side.
3. **TypeScript client** (`libs/myko/ts`) — cascades automatically to svelte and vue.
4. **Python client** (`libs/myko/py`).
5. **Unreal C++ client** (`libs/myko/cpp`) — last, because the `tinycbor` Module integration carries the most unknown integration friction. Falls back to hand-rolled decoder if `tinycbor` proves problematic in UBT or with Unreal's allocator setup.
6. **C# client** — when prioritized; off hot path.

Each step is independently shippable. Until a given language client is migrated, it continues sending JSON; the server's capability-via-demonstration logic keeps it on JSON for that session. There is no interop cliff during the rollout.

---

## 7. Backward-compat policy: hard cut, no msgpack alias

Reasoning:

- **No production users are successfully on msgpack today.** The current msgpack path is broken for at least one variant (the `// Force JSON until msgpack report-response round-trip is diagnosed` workaround in `core/src/client/mod.rs` is direct evidence) and every working client is on JSON. There is no installed base to preserve compatibility with.
- Keeping a deprecated msgpack alias means keeping a known-broken path alive in the codebase for no benefit.
- Cross-version skew during the rollout is bounded: every myko language client lives in this monorepo and ships in lockstep. Any client at a version that doesn't yet support CBOR simply continues using JSON; the server's capability-via-demonstration negotiation accommodates this without any fallback path.
- The protocol-switch handshake message is being deleted regardless, so there is no "switch to old binary format" path to preserve.

A stricter posture (one release with both msgpack and CBOR supported, then a release that drops msgpack) is possible but doubles the wire-test matrix for a release cycle of paranoia that the absence of an installed base does not justify.

---

## 8. Testing

- **Gate test (permanent).** Round-trip the previously-broken `MykoMessage` variant through ciborium. Lives as a regression test for the lifetime of the binary path.
- **Parameterized integration tests.** Existing wire-protocol integration tests are parameterized over `MykoProtocol::JSON` and `MykoProtocol::CBOR`, covering query / report / view / command / event flows.
- **Negotiation behavior test.** Server-side test confirming `outgoing_format` starts as JSON, transitions to CBOR on the first received binary frame, and never transitions back regardless of subsequent text-frame traffic.
- **JSON-only client test.** Test confirming a session that sends only text frames receives only text frames for its entire lifetime.
- **Per-client smoke tests.** Each language client runs a small round-trip test (subscribe to a query, receive an upsert, send a command, receive a response) over CBOR against the Rust server before that client's migration PR merges.

---

## 9. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Ciborium handles the failing case round-trip but with unexpected size or perf cost. | Gate test plus a small benchmark in the same PR comparing CBOR encode/decode time and bytes-on-wire against the current msgpack path on representative messages. |
| `tinycbor` integration into Unreal hits unforeseen UBT, exception, or allocator friction. | Unreal client scheduled last in the migration order. Hand-rolled minimal CBOR decoder is the documented fallback. |
| Downstream apps holding wire frames in transit during a deploy window. | Frame type is self-describing per-frame. JSON is always safe mid-deploy. No coordination required. |
| A hidden second adjacently-tagged enum variant fails differently in ciborium than rmp_serde. | The gate test is one variant; before merge, the parameterized integration test sweep over JSON vs CBOR exercises the full set of `MykoMessage` and `QueryChange` variants. |

---

## 10. Open questions

- **What is the exact shape of the `rmp_serde` failure?** The `// Force JSON until msgpack report-response round-trip is diagnosed` comment in `core/src/client/mod.rs` indicates the problem manifests on `ReportResponse` round-trip, but the underlying cause (which field, which serde representation, which type interaction) has not been diagnosed. The gate test must reproduce the specific failure; if the diagnosis turns up something that affects ciborium too, the design is reopened.
- **Does the Unreal C++ port have an existing JSON entry point on `TSharedPtr<FJsonValue>`** that the new CBOR module can plug behind, or does it parse JSON directly into game-side types? If the latter, the integration shape changes — to be confirmed during the Unreal migration step. (Does not block Rust/TS/Py migration.)
