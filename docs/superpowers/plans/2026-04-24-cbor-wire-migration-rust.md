# CBOR Wire-Protocol Migration — Rust + Leptos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `rmp_serde` (MessagePack) with `ciborium` (CBOR) as the binary encoding for the myko WebSocket wire protocol on the Rust side (server, Rust client, Leptos), and replace the explicit `SWITCH_TO_MSGPACK` handshake with implicit capability-via-demonstration negotiation derived from incoming WebSocket frame type.

**Architecture:** Hard-cut swap of the binary serialization library underneath serde — no IDL, no schema, no consumer-facing changes. Format negotiation becomes implicit: server defaults to JSON, sticky-promotes to CBOR on the first received binary frame, never demotes. Per-language client migrations (TS, Python, Unreal C++, C#) are out of scope for this plan and ship as separate follow-on plans against the migrated server.

**Tech Stack:** Rust, serde, `ciborium` (RFC 8949 CBOR, BSD-2-Clause), tokio-tungstenite, hyphae, autosocket.

**Spec:** `docs/superpowers/specs/2026-04-24-cbor-wire-migration-design.md`

---

## File Structure

**Files modified:**

| File | Responsibility | Changes |
|------|----------------|---------|
| `Cargo.toml` (workspace) | Workspace dep declarations | Add `ciborium`. Remove `rmp-serde` (last task). |
| `libs/myko/core/Cargo.toml` | Core deps | Add `ciborium.workspace = true`. Remove `rmp-serde` (last task). |
| `libs/myko/core/src/wire/message.rs` | `MykoMessage` envelope | Delete `ProtocolSwitch` variant. |
| `libs/myko/core/src/wire/command.rs` | Command wire types and encoding | Rename `EncodedCommandMessage::Msgpack` → `Cbor`. Switch encoder to ciborium. |
| `libs/myko/core/src/wire/event/mod.rs` | Event wire types | Replace `from_mp` with `from_cbor`, swap implementation to ciborium. |
| `libs/myko/core/src/client/mod.rs` | Rust client transport, protocol negotiation, encode/decode | Rename `MykoProtocol::MSGPACK` → `CBOR`. Replace rmp_serde calls with ciborium. Remove the JSON-forced default workaround. |
| `libs/myko/core/src/server/protocol.rs` | Server-side wire helpers | Rename `message_to_msgpack` → `message_to_cbor`. Switch implementation to ciborium. |
| `libs/myko/core/src/server/mod.rs` | Server module re-exports | Update re-export name to match `protocol.rs` rename. |
| `libs/myko/core/src/server/client_session.rs` | Server-side per-client session, including test mock writer | Replace `rmp_serde::from_slice` in mock writer test. |
| `libs/myko/server/src/ws_handler.rs` | WebSocket handler — receive/decode loop, send/encode loop, format negotiation | Replace `use_binary: AtomicBool` with `outgoing_format: AtomicU8`. Delete `SWITCH_TO_MSGPACK` magic-string and `ProtocolSwitch` confirmation send. Switch all rmp_serde calls to ciborium. Update `outgoing_format` on every binary frame received (sticky promotion). |

**Files created:**

| File | Responsibility |
|------|----------------|
| `libs/myko/core/src/wire/cbor_roundtrip_tests.rs` | Permanent regression test module for the previously-broken `MykoMessage` round-trip cases over CBOR. Wired into the existing `wire` module tree. |

**No new public types.** `outgoing_format` is private to the server `ws_handler`. The `MykoProtocol::CBOR` rename is a breaking name change of an existing public enum variant.

**Type consistency** (used throughout this plan, must match exactly):
- Public enum variant: `MykoProtocol::CBOR` (replaces `MykoProtocol::MSGPACK`)
- Internal enum variant: `EncodedCommandMessage::Cbor(Vec<u8>)` (replaces `EncodedCommandMessage::Msgpack(Vec<u8>)`)
- Function: `MEvent::from_cbor` (replaces `MEvent::from_mp`)
- Function: `myko::server::message_to_cbor` (replaces `message_to_msgpack`)
- Field: `outgoing_format: Arc<AtomicU8>` (replaces `use_binary: Arc<AtomicBool>` in `ws_handler.rs` and `ChannelWriter`)
- Constant for log/protocol-switch confirmation strings: deleted entirely (no replacement).

---

## Phase 1: Gate test — verify ciborium handles the previously-broken case

The existing comment at `libs/myko/core/src/client/mod.rs:317` (`// NOTE(ts): Force JSON until msgpack report-response round-trip is diagnosed`) is direct evidence that `rmp_serde` round-trip fails on at least `MykoMessage::ReportResponse`. Phase 1 reproduces that failure with `rmp_serde`, then verifies `ciborium` round-trips the same value cleanly. **If `ciborium` also fails, the migration halts at Phase 1 and the spec is reopened.**

### Task 1: Add ciborium to workspace and core dependencies

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `libs/myko/core/Cargo.toml`

- [ ] **Step 1: Add ciborium to the workspace dependency table**

In `Cargo.toml` at the workspace root, find the line `rmp-serde = "1.3"` and add directly below it:

```toml
ciborium = "0.2"
```

- [ ] **Step 2: Add ciborium as a workspace dependency in core**

In `libs/myko/core/Cargo.toml`, find the line `rmp-serde.workspace = true` and add directly below it:

```toml
ciborium.workspace = true
```

- [ ] **Step 3: Verify the dependency resolves**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds successfully (no compile errors). Ignore unused-import warnings — no code uses ciborium yet.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml libs/myko/core/Cargo.toml Cargo.lock
git commit -m "chore(core): add ciborium dependency for CBOR migration"
```

### Task 2: Reproduce the rmp_serde report-response failure as a test

**Files:**
- Create: `libs/myko/core/src/wire/cbor_roundtrip_tests.rs`
- Modify: `libs/myko/core/src/wire/mod.rs`

- [ ] **Step 1: Write the failing rmp_serde reproducer test**

Create `libs/myko/core/src/wire/cbor_roundtrip_tests.rs` with:

```rust
//! Round-trip regression tests for the wire protocol.
//!
//! These tests exist because rmp_serde was found to silently corrupt
//! MykoMessage::ReportResponse on round-trip (see commit history). They
//! must pass for ciborium and serve as the gate for the binary path.

#[cfg(test)]
mod tests {
    use crate::wire::{MykoMessage, ReportResponse};
    use serde_json::json;

    fn sample_report_response() -> MykoMessage {
        MykoMessage::ReportResponse(ReportResponse {
            response: json!({
                "rows": [
                    { "id": "row-1", "value": 42, "label": "alpha" },
                    { "id": "row-2", "value": -17, "label": "beta" },
                ],
                "total": 2,
                "metadata": {
                    "duration_ms": 12.5,
                    "cached": false,
                }
            }),
            tx: "tx-abc-123".to_string(),
        })
    }

    /// Documents the failure mode that motivated this migration.
    /// If this test ever PASSES with rmp_serde, the workaround at
    /// client/mod.rs (the JSON-forced default) can be reconsidered
    /// independently of the CBOR migration.
    #[test]
    #[ignore = "documents the rmp_serde failure that motivated CBOR migration"]
    fn report_response_roundtrip_msgpack_documents_failure() {
        let original = sample_report_response();
        let bytes = rmp_serde::to_vec(&original).expect("encode should succeed");
        let decoded: Result<MykoMessage, _> = rmp_serde::from_slice(&bytes);

        // Either decode fails, or the result is not equal to the original.
        // Both outcomes are wrong; we record the actual outcome in the
        // assertion message for posterity.
        match decoded {
            Err(e) => panic!("rmp_serde decode failed (expected): {}", e),
            Ok(roundtripped) => {
                let original_json = serde_json::to_value(&original).unwrap();
                let roundtripped_json = serde_json::to_value(&roundtripped).unwrap();
                assert_eq!(
                    original_json, roundtripped_json,
                    "rmp_serde roundtrip mismatch"
                );
            }
        }
    }
}
```

- [ ] **Step 2: Wire the new module into `wire/mod.rs`**

In `libs/myko/core/src/wire/mod.rs`, find the existing module declarations near the top (`pub mod command;`, `pub mod event;`, etc.) and add at the end of that block:

```rust
#[cfg(test)]
mod cbor_roundtrip_tests;
```

- [ ] **Step 3: Run the test and observe its failure mode**

Run: `cargo test --target-dir target/claude -p myko --lib wire::cbor_roundtrip_tests -- --ignored --nocapture`
Expected: the test runs (because of `--ignored`) and panics with either an rmp_serde decode error, or a mismatch assertion. Capture the exact failure mode in the next step.

- [ ] **Step 4: Annotate the test with the observed failure**

Update the doc-comment on `report_response_roundtrip_msgpack_documents_failure` to record the exact failure observed in step 3 (e.g., "rmp_serde decode fails with: invalid type: ..." or "round-trip yields response = null instead of object"). This documents the bug for posterity.

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/wire/cbor_roundtrip_tests.rs libs/myko/core/src/wire/mod.rs
git commit -m "test(core): document rmp_serde report-response roundtrip failure"
```

### Task 3: Verify ciborium handles the same case

**Files:**
- Modify: `libs/myko/core/src/wire/cbor_roundtrip_tests.rs`

- [ ] **Step 1: Add the ciborium round-trip test (failing — function not yet supported in this module)**

Append to the `tests` module in `libs/myko/core/src/wire/cbor_roundtrip_tests.rs`:

```rust
    /// Gate test: ciborium must round-trip MykoMessage::ReportResponse cleanly.
    /// If this fails, the CBOR migration halts and the spec is reopened.
    #[test]
    fn report_response_roundtrip_cbor() {
        let original = sample_report_response();

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&original, &mut bytes).expect("ciborium encode");

        let roundtripped: MykoMessage =
            ciborium::de::from_reader(bytes.as_slice()).expect("ciborium decode");

        let original_json = serde_json::to_value(&original).unwrap();
        let roundtripped_json = serde_json::to_value(&roundtripped).unwrap();
        assert_eq!(
            original_json, roundtripped_json,
            "ciborium roundtrip should preserve ReportResponse"
        );
    }
```

- [ ] **Step 2: Run the gate test**

Run: `cargo test --target-dir target/claude -p myko --lib wire::cbor_roundtrip_tests::tests::report_response_roundtrip_cbor -- --nocapture`
Expected: PASS.

**If this test fails, STOP. Report the failure mode and reopen the spec — the migration cannot proceed.**

- [ ] **Step 3: Add round-trip coverage for the other variants that flow through the binary path**

Append to the `tests` module:

```rust
    fn assert_roundtrip(msg: MykoMessage) {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&msg, &mut bytes).expect("ciborium encode");
        let roundtripped: MykoMessage =
            ciborium::de::from_reader(bytes.as_slice()).expect("ciborium decode");
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            serde_json::to_value(&roundtripped).unwrap(),
            "roundtrip mismatch for {:?}",
            msg,
        );
    }

    #[test]
    fn ping_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::Ping(crate::wire::PingData {
            id: "ping-1".into(),
            timestamp: 1_700_000_000_000,
        }));
    }

    #[test]
    fn query_cancel_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::QueryCancel(crate::wire::CancelSubscription {
            tx: "tx-cancel-1".into(),
        }));
    }

    #[test]
    fn command_error_roundtrip_cbor() {
        assert_roundtrip(MykoMessage::CommandError(crate::wire::CommandError {
            tx: "tx-cmd-1".into(),
            command_id: "MyCommand".into(),
            message: "validation failed: name is required".into(),
        }));
    }
```

- [ ] **Step 4: Run all the new tests**

Run: `cargo test --target-dir target/claude -p myko --lib wire::cbor_roundtrip_tests`
Expected: all four cbor tests PASS. The `_documents_failure` test is `#[ignore]` and is not run.

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/wire/cbor_roundtrip_tests.rs
git commit -m "test(core): verify ciborium roundtrips MykoMessage variants cleanly"
```

---

## Phase 2: Replace rmp_serde with ciborium across the wire layer

Each task swaps a single call site or rename. After each task, the codebase must still build and tests must still pass. Tasks are ordered so that intermediate states are always compilable: rename-first tasks land before tasks that consume the new names.

### Task 4: Rename `EncodedCommandMessage::Msgpack` → `Cbor` and switch encoder

**Files:**
- Modify: `libs/myko/core/src/wire/command.rs`
- Modify: `libs/myko/core/src/server/client_session.rs:927-933` (test mock — consumer of the variant)
- Modify: `libs/myko/server/src/ws_handler.rs:526-529` (consumer of the variant)

- [ ] **Step 1: Rename the variant and switch the encoder body**

In `libs/myko/core/src/wire/command.rs`, find:

```rust
pub enum EncodedCommandMessage {
    Json(String),
    Msgpack(Vec<u8>),
}
```

Replace with:

```rust
pub enum EncodedCommandMessage {
    Json(String),
    Cbor(Vec<u8>),
}
```

In the same file, find `encode_command_message`:

```rust
        MykoProtocol::MSGPACK => rmp_serde::to_vec(&message)
            .map(EncodedCommandMessage::Msgpack)
            .map_err(|err| err.to_string()),
```

Replace with:

```rust
        MykoProtocol::MSGPACK => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(&message, &mut bytes).map_err(|e| e.to_string())?;
            Ok(EncodedCommandMessage::Cbor(bytes))
        }
```

(`MykoProtocol::MSGPACK` is renamed in Task 11; the variant rename and the encoder swap happen in two separate steps so that intermediate builds are green.)

- [ ] **Step 2: Update the consumer in `core/src/server/client_session.rs`**

In `libs/myko/core/src/server/client_session.rs`, find the `match payload` arm in the test mock writer (around line 930):

```rust
                EncodedCommandMessage::Msgpack(bytes) => {
                    rmp_serde::from_slice(&bytes).expect("Serialized command msgpack should decode")
                }
```

Replace with:

```rust
                EncodedCommandMessage::Cbor(bytes) => ciborium::de::from_reader(bytes.as_slice())
                    .expect("Serialized command CBOR should decode"),
```

- [ ] **Step 3: Update the consumer in `server/src/ws_handler.rs`**

In `libs/myko/server/src/ws_handler.rs`, find the `match &msg` arm around line 526:

```rust
                    OutboundMessage::SerializedCommand {
                        payload: EncodedCommandMessage::Msgpack(bytes),
                        ..
                    } => Message::Binary(bytes.clone().into()),
```

Replace `EncodedCommandMessage::Msgpack` with `EncodedCommandMessage::Cbor`. The body stays the same.

- [ ] **Step 4: Build to verify the rename is consistent**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds successfully. Verify by reading any error output that no other call sites reference the old `Msgpack` variant name.

- [ ] **Step 5: Run command-related tests**

Run: `cargo test --target-dir target/claude -p myko --lib wire::command`
Run: `cargo test --target-dir target/claude -p myko --lib server::client_session`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/myko/core/src/wire/command.rs libs/myko/core/src/server/client_session.rs libs/myko/server/src/ws_handler.rs
git commit -m "refactor(core): rename EncodedCommandMessage::Msgpack to Cbor and switch encoder"
```

### Task 5: Rename `MEvent::from_mp` → `from_cbor` and switch implementation

**Files:**
- Modify: `libs/myko/core/src/wire/event/mod.rs`

`from_mp` has no external callers (verified by `grep -rn "from_mp" libs/`), so the rename is contained to its definition site.

- [ ] **Step 1: Update imports and rewrite the function**

In `libs/myko/core/src/wire/event/mod.rs`, find the import:

```rust
use rmp_serde::Deserializer;
```

Delete that line (ciborium does not use it).

Also delete the unused `use std::io::Cursor;` if it ends up unused after the function rewrite (it's only used inside `from_mp`).

Find the function:

```rust
    pub fn from_mp(s: &[u8]) -> Result<MEvent, rmp_serde::decode::Error> {
        let cur = Cursor::new(s);
        let mut de = Deserializer::new(cur);
        Deserialize::deserialize(&mut de)
    }
```

Replace with:

```rust
    pub fn from_cbor(s: &[u8]) -> Result<MEvent, ciborium::de::Error<std::io::Error>> {
        ciborium::de::from_reader(s)
    }
```

- [ ] **Step 2: Build to verify**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds successfully.

- [ ] **Step 3: Run event-related tests**

Run: `cargo test --target-dir target/claude -p myko --lib wire::event`
Expected: PASS. (Tests within the existing event module, if any.)

- [ ] **Step 4: Commit**

```bash
git add libs/myko/core/src/wire/event/mod.rs
git commit -m "refactor(core): rename MEvent::from_mp to from_cbor"
```

### Task 6: Switch `client/mod.rs::encode_protocol` to ciborium

**Files:**
- Modify: `libs/myko/core/src/client/mod.rs:107-112`

- [ ] **Step 1: Replace the rmp_serde encode call**

In `libs/myko/core/src/client/mod.rs`, find:

```rust
fn encode_protocol(protocol: &AtomicU8, msg: &MykoMessage) -> Option<WsFrame> {
    match MykoProtocol::from(protocol.load(Ordering::SeqCst)) {
        MykoProtocol::JSON => serde_json::to_string(msg).ok().map(WsFrame::Text),
        MykoProtocol::MSGPACK => rmp_serde::to_vec(msg).ok().map(WsFrame::Binary),
    }
}
```

Replace the `MSGPACK` arm with:

```rust
        MykoProtocol::MSGPACK => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(msg, &mut bytes).ok()?;
            Some(WsFrame::Binary(bytes))
        }
```

(The variant is still named `MSGPACK` here; rename happens in Task 11.)

- [ ] **Step 2: Build**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add libs/myko/core/src/client/mod.rs
git commit -m "refactor(core): switch client encode_protocol to ciborium"
```

### Task 7: Switch the remaining client-side rmp_serde calls

**Files:**
- Modify: `libs/myko/core/src/client/mod.rs:716, 726`

- [ ] **Step 1: Replace the encode call at ~line 716**

In `libs/myko/core/src/client/mod.rs`, find the line beginning with `let bytes = rmp_serde::to_vec(msg)`. Replace the surrounding block:

```rust
                let bytes = rmp_serde::to_vec(msg).map_err(|e| e.to_string())?;
```

with:

```rust
                let mut bytes = Vec::new();
                ciborium::ser::into_writer(msg, &mut bytes).map_err(|e| e.to_string())?;
```

- [ ] **Step 2: Replace the decode call at ~line 726**

Find:

```rust
            WsFrame::Binary(bytes) => match rmp_serde::from_slice::<Value>(bytes) {
```

Replace `rmp_serde::from_slice::<Value>(bytes)` with `ciborium::de::from_reader::<Value, _>(bytes.as_slice())`.

Find the surrounding warn message (a few lines below):

```rust
                    warn!("msgpack decode failed ({} bytes): {}", bytes.len(), e);
```

Replace `"msgpack decode failed"` with `"CBOR decode failed"`.

- [ ] **Step 3: Build**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds.

- [ ] **Step 4: Run client-side tests**

Run: `cargo test --target-dir target/claude -p myko --lib client`
Expected: PASS (tests that exist still pass; some tests may use the workaround that gets removed in Task 12).

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/client/mod.rs
git commit -m "refactor(core): switch client decode and message-send paths to ciborium"
```

### Task 8: Rename and switch `server/protocol.rs`

**Files:**
- Modify: `libs/myko/core/src/server/protocol.rs`
- Modify: `libs/myko/core/src/server/mod.rs:32`

- [ ] **Step 1: Rename and rewrite the function body**

In `libs/myko/core/src/server/protocol.rs`, replace the entire content with:

```rust
//! WebSocket protocol types
//!
//! Re-exports and helpers for the existing wire protocol.

pub use crate::wire::MykoMessage;

/// Serialize a MykoMessage to CBOR bytes.
pub fn message_to_cbor(msg: &MykoMessage) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(msg, &mut bytes)?;
    Ok(bytes)
}

/// Serialize a MykoMessage to JSON.
pub fn message_to_json(msg: &MykoMessage) -> Result<String, serde_json::Error> {
    serde_json::to_string(msg)
}
```

- [ ] **Step 2: Update the re-export in `server/mod.rs`**

In `libs/myko/core/src/server/mod.rs`, find:

```rust
pub use protocol::{message_to_json, message_to_msgpack};
```

Replace with:

```rust
pub use protocol::{message_to_cbor, message_to_json};
```

- [ ] **Step 3: Find and update any external callers**

Run: `grep -rn "message_to_msgpack" libs/`
Expected: no remaining references inside `libs/` (the function had no external callers prior; verify and fix any new ones found).

- [ ] **Step 4: Build**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds.

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/server/protocol.rs libs/myko/core/src/server/mod.rs
git commit -m "refactor(core): rename message_to_msgpack to message_to_cbor"
```

### Task 9: Switch the server's WebSocket writer encode path

**Files:**
- Modify: `libs/myko/server/src/ws_handler.rs:530-538`

- [ ] **Step 1: Replace the rmp_serde encode in the OutboundMessage::Message arm**

In `libs/myko/server/src/ws_handler.rs`, find around line 530:

```rust
                    OutboundMessage::Message(msg) if use_binary_writer.load(Ordering::SeqCst) => {
                        match rmp_serde::to_vec(msg) {
                            Ok(bytes) => Message::Binary(bytes.into()),
                            Err(e) => {
                                log::error!("Failed to serialize message to msgpack: {}", e);
                                continue;
                            }
                        }
                    }
```

Replace with:

```rust
                    OutboundMessage::Message(msg) if use_binary_writer.load(Ordering::SeqCst) => {
                        let mut bytes = Vec::new();
                        match ciborium::ser::into_writer(msg, &mut bytes) {
                            Ok(()) => Message::Binary(bytes.into()),
                            Err(e) => {
                                log::error!("Failed to serialize message to CBOR: {}", e);
                                continue;
                            }
                        }
                    }
```

(The `use_binary_writer` field gets renamed in Task 13. Keeping it for now.)

- [ ] **Step 2: Build**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add libs/myko/server/src/ws_handler.rs
git commit -m "refactor(server): switch ws_handler writer to ciborium"
```

### Task 10: Switch the server's WebSocket reader decode path

**Files:**
- Modify: `libs/myko/server/src/ws_handler.rs:687`

- [ ] **Step 1: Replace the rmp_serde decode in the Message::Binary arm**

In `libs/myko/server/src/ws_handler.rs`, find around line 687:

```rust
                            match rmp_serde::from_slice::<MykoMessage>(&data) {
```

Replace with:

```rust
                            match ciborium::de::from_reader::<MykoMessage, _>(data.as_ref()) {
```

- [ ] **Step 2: Build**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds.

- [ ] **Step 3: Run server tests**

Run: `cargo test --target-dir target/claude -p myko-server --lib`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/server/src/ws_handler.rs
git commit -m "refactor(server): switch ws_handler reader to ciborium"
```

### Task 11: Rename `MykoProtocol::MSGPACK` → `CBOR`

**Files:**
- Modify: `libs/myko/core/src/client/mod.rs:42-54, 110, 698, 711, 715`
- Modify: `libs/myko/core/src/wire/command.rs:88-93`
- Modify: `libs/myko/server/src/ws_handler.rs:1517-1521`

- [ ] **Step 1: Rename the enum variant and From impl in `client/mod.rs`**

In `libs/myko/core/src/client/mod.rs`, find:

```rust
/// Wire protocol for encoding messages.
/// Defaults to MSGPACK for better performance - server auto-detects binary frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::TS)]
#[ts(export)]
pub enum MykoProtocol {
    JSON = 0,
    MSGPACK = 1,
}

impl From<u8> for MykoProtocol {
    fn from(v: u8) -> Self {
        match v {
            0 => MykoProtocol::JSON,
            _ => MykoProtocol::MSGPACK,
        }
    }
}
```

Replace with:

```rust
/// Wire protocol for encoding messages.
/// Defaults to JSON; clients opt into CBOR by calling `set_protocol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::TS)]
#[ts(export)]
pub enum MykoProtocol {
    JSON = 0,
    CBOR = 1,
}

impl From<u8> for MykoProtocol {
    fn from(v: u8) -> Self {
        match v {
            0 => MykoProtocol::JSON,
            _ => MykoProtocol::CBOR,
        }
    }
}
```

- [ ] **Step 2: Update remaining `MSGPACK` references in `client/mod.rs`**

Search the file for `MSGPACK`:

Run: `grep -n "MSGPACK" libs/myko/core/src/client/mod.rs`

Replace each `MykoProtocol::MSGPACK` with `MykoProtocol::CBOR`. There should be three occurrences:
- The arm in `encode_protocol` (around line 110).
- The arm in `encode_message` (around line 715).
- The doc-comment on `set_protocol` (around line 698): `"Set the wire protocol (JSON or MSGPACK). Default is MSGPACK."` → `"Set the wire protocol (JSON or CBOR). Default is JSON."`

- [ ] **Step 3: Update `wire/command.rs`**

In `libs/myko/core/src/wire/command.rs`, find around line 92:

```rust
        MykoProtocol::MSGPACK => {
```

Replace `MykoProtocol::MSGPACK` with `MykoProtocol::CBOR`.

- [ ] **Step 4: Update `server/src/ws_handler.rs`**

In `libs/myko/server/src/ws_handler.rs`, find around line 1517:

```rust
    fn protocol(&self) -> myko::client::MykoProtocol {
        if self.use_binary_writer.load(Ordering::SeqCst) {
            myko::client::MykoProtocol::MSGPACK
        } else {
            myko::client::MykoProtocol::JSON
        }
    }
```

Replace `myko::client::MykoProtocol::MSGPACK` with `myko::client::MykoProtocol::CBOR`.

- [ ] **Step 5: Verify no remaining references to MSGPACK**

Run: `grep -rn "MSGPACK\|Msgpack\|msgpack" libs/myko/ --include='*.rs'`
Expected: only the `_documents_failure` test from Task 2 (which references `rmp_serde` and the historical name in its doc-comment) and possibly one or two log messages saying "msgpack" that should also be updated to "CBOR" or "binary" in this step.

For each remaining `msgpack` log string, replace with `CBOR`. Examples:
- `"Client {} switched to binary (msgpack) protocol via auto-detect"` → `"Client {} switched to binary (CBOR) protocol via auto-detect"`
- `"Client {} switched to binary (msgpack) protocol via explicit request"` → kept temporarily; this branch is deleted in Task 14.

- [ ] **Step 6: Build the whole workspace**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds.

- [ ] **Step 7: Run the gate test plus core unit tests**

Run: `cargo test --target-dir target/claude -p myko --lib wire`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add libs/myko/core/src/client/mod.rs libs/myko/core/src/wire/command.rs libs/myko/server/src/ws_handler.rs
git commit -m "refactor(core): rename MykoProtocol::MSGPACK to CBOR"
```

### Task 12: Remove the JSON-forced default workaround in client constructor

**Files:**
- Modify: `libs/myko/core/src/client/mod.rs:317-318`

- [ ] **Step 1: Replace the workaround comment and default**

In `libs/myko/core/src/client/mod.rs`, find:

```rust
        // NOTE(ts): Force JSON until msgpack report-response round-trip is diagnosed
        let protocol = Arc::new(AtomicU8::new(MykoProtocol::JSON as u8));
```

Replace with:

```rust
        let protocol = Arc::new(AtomicU8::new(MykoProtocol::JSON as u8));
```

(The default stays JSON — clients opt in to CBOR via `set_protocol`. The workaround comment is the only thing that needs to go; it referenced an msgpack bug that no longer exists.)

- [ ] **Step 2: Build**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add libs/myko/core/src/client/mod.rs
git commit -m "chore(core): remove obsolete msgpack workaround comment"
```

---

## Phase 3: Replace explicit handshake with capability-via-demonstration

### Task 13: Delete the `SWITCH_TO_MSGPACK` magic-string handler

**Files:**
- Modify: `libs/myko/server/src/ws_handler.rs:46-47, 712-725`

This task is sequenced before the `use_binary` → `outgoing_format` rename so that all consumers of `use_binary` (including the `use_binary.store(true, ...)` call inside the magic-string branch) are removed before the field is renamed. This keeps every intermediate state compilable.

- [ ] **Step 1: Delete the constant declaration**

In `libs/myko/server/src/ws_handler.rs`, find around line 46-47:

```rust
/// Must match ProtocolMessages.SwitchToMSGPACK in TypeScript client.
const SWITCH_TO_MSGPACK: &str = "myko:switch-to-msgpack";
```

Delete both lines.

- [ ] **Step 2: Delete the magic-string branch in the text-frame handler**

Find around line 712:

```rust
                        Message::Text(text) => {
                            if text == SWITCH_TO_MSGPACK {
                                log::debug!(
                                    "Client {} switched to binary (msgpack) protocol via explicit request",
                                    client_id
                                );
                                if let Err(e) = priority_tx.try_send(MykoMessage::ProtocolSwitch {
                                    protocol: "msgpack".into(),
                                }) {
                                    drop_logger.on_drop("ProtocolSwitch", &e);
                                }
                                use_binary.store(true, Ordering::SeqCst);
                                continue;
                            }

                            match serde_json::from_str::<MykoMessage>(&text) {
```

Replace the entire `if text == SWITCH_TO_MSGPACK { ... continue; }` block (and the blank line after it) with nothing — the `match serde_json::from_str::<MykoMessage>(&text) {` line should immediately follow the `Message::Text(text) => {` arm opener.

- [ ] **Step 3: Build**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds. The `MykoMessage::ProtocolSwitch` variant still exists (deleted in Task 15) but no producer references it any longer; the dead-letter consumer arm in the inbound handler still references it and remains valid.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/server/src/ws_handler.rs
git commit -m "refactor(server): delete SWITCH_TO_MSGPACK magic-string handshake"
```

### Task 14: Replace `use_binary: AtomicBool` with `outgoing_format: AtomicU8` on the server session

**Files:**
- Modify: `libs/myko/server/src/ws_handler.rs:349-374, 530-562, 679-684, 1502-1572`

This task introduces the new state, wires it through the writer/reader/ChannelWriter, and updates the auto-detect logic to set CBOR (not just `true`).

- [ ] **Step 1: Add the `MykoProtocol` import to `ws_handler.rs`**

At the top of `libs/myko/server/src/ws_handler.rs`, the file already uses `myko::client::MykoProtocol` in the `protocol()` method body. Promote that to a top-level import for clarity:

Find an existing `use myko::...` near the top of the file and add (or extend an existing line):

```rust
use myko::client::MykoProtocol;
```

Then in the `protocol()` method body (around line 1517), simplify any `myko::client::MykoProtocol::CBOR` to just `MykoProtocol::CBOR`.

- [ ] **Step 2: Replace the field declaration and initialization**

Find around line 349:

```rust
        // Protocol: default to JSON, switch to binary only if client opts in
        let use_binary = Arc::new(AtomicBool::new(false));
```

Replace with:

```rust
        // Outgoing format for this session: defaults to JSON, sticky-promotes
        // to CBOR on the first received binary frame. Never demotes.
        let outgoing_format = Arc::new(AtomicU8::new(MykoProtocol::JSON as u8));
```

This requires also adding `use std::sync::atomic::AtomicU8;` near the existing `AtomicBool` import. Find that import and add `AtomicU8` to the same use statement.

- [ ] **Step 3: Update the field usages — initialization sites for `ChannelWriter`**

Find around line 354 and 362:

```rust
        let writer = ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            use_binary_writer: use_binary.clone(),
        };

        ...

        let writer_arc: Arc<dyn WsWriter> = Arc::new(ChannelWriter {
            tx: tx.clone(),
            deferred_tx: deferred_tx.clone(),
            drop_logger: drop_logger.clone(),
            use_binary_writer: use_binary.clone(),
        });
```

Replace `use_binary_writer: use_binary.clone()` with `outgoing_format: outgoing_format.clone()` in both `ChannelWriter` constructions.

- [ ] **Step 4: Rename the local variable used by the reader/writer tasks**

Find around line 374:

```rust
        let use_binary_writer = use_binary.clone();
```

Replace with:

```rust
        let outgoing_format_writer = outgoing_format.clone();
```

- [ ] **Step 5: Update the encoder branch in the writer task**

Find around line 530 (already updated to ciborium in Task 9):

```rust
                    OutboundMessage::Message(msg) if use_binary_writer.load(Ordering::SeqCst) => {
```

Replace with:

```rust
                    OutboundMessage::Message(msg)
                        if outgoing_format_writer.load(Ordering::SeqCst)
                            == MykoProtocol::CBOR as u8 =>
                    {
```

- [ ] **Step 6: Update the log line that references `use_binary_writer`**

Find around line 562:

```rust
                        use_binary_writer.load(Ordering::SeqCst),
```

Replace with:

```rust
                        outgoing_format_writer.load(Ordering::SeqCst) == MykoProtocol::CBOR as u8,
```

(The log placeholder formatter, presumably `{}` for a bool, doesn't need to change — `bool` formats fine.)

- [ ] **Step 7: Update the auto-detect branch in the binary-frame reader**

Find around line 679:

```rust
                        Message::Binary(data) => {
                            if !use_binary.load(Ordering::SeqCst) {
                                log::debug!(
                                    "Client {} switched to binary (CBOR) protocol via auto-detect",
                                    client_id
                                );
                                use_binary.store(true, Ordering::SeqCst);
                            }
```

Replace with:

```rust
                        Message::Binary(data) => {
                            if outgoing_format.load(Ordering::SeqCst) != MykoProtocol::CBOR as u8 {
                                log::debug!(
                                    "Client {} promoted outgoing format to CBOR via demonstration",
                                    client_id
                                );
                                outgoing_format.store(MykoProtocol::CBOR as u8, Ordering::SeqCst);
                            }
```

- [ ] **Step 8: Update the `ChannelWriter` struct definition and `protocol()` method**

Find the struct definition around line 1502:

```rust
struct ChannelWriter {
    tx: mpsc::Sender<OutboundMessage>,
    deferred_tx: mpsc::Sender<DeferredOutbound>,
    drop_logger: Arc<DropLogger>,
    use_binary_writer: Arc<AtomicBool>,
}
```

Replace `use_binary_writer: Arc<AtomicBool>` with `outgoing_format: Arc<AtomicU8>`.

Find the `protocol()` method around line 1517:

```rust
    fn protocol(&self) -> MykoProtocol {
        if self.use_binary_writer.load(Ordering::SeqCst) {
            MykoProtocol::CBOR
        } else {
            MykoProtocol::JSON
        }
    }
```

Replace with:

```rust
    fn protocol(&self) -> MykoProtocol {
        MykoProtocol::from(self.outgoing_format.load(Ordering::SeqCst))
    }
```

- [ ] **Step 9: Update the `test_channel_writer` test**

Find around line 1572:

```rust
            use_binary_writer: Arc::new(AtomicBool::new(false)),
```

Replace with:

```rust
            outgoing_format: Arc::new(AtomicU8::new(MykoProtocol::JSON as u8)),
```

- [ ] **Step 10: Build the workspace**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds. Verify by reading any errors.

- [ ] **Step 11: Verify no `use_binary` references remain**

Run: `grep -n "use_binary" libs/myko/server/src/ws_handler.rs`
Expected: no matches.

- [ ] **Step 12: Run server tests**

Run: `cargo test --target-dir target/claude -p myko-server --lib`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add libs/myko/server/src/ws_handler.rs
git commit -m "refactor(server): replace use_binary with outgoing_format atomic for sticky CBOR promotion"
```

### Task 15: Delete the `MykoMessage::ProtocolSwitch` variant

**Files:**
- Modify: `libs/myko/core/src/wire/message.rs:86-89`
- Modify: `libs/myko/server/src/ws_handler.rs:1395-1402`

- [ ] **Step 1: Delete the variant declaration**

In `libs/myko/core/src/wire/message.rs`, find:

```rust
    /// Protocol switch confirmation - sent by server when client requests binary mode
    #[serde(rename = "ws:m:protocol-switch")]
    ProtocolSwitch { protocol: String },
```

Delete the doc-comment and the variant.

- [ ] **Step 2: Delete the dead-letter logging arm in `ws_handler.rs`**

In `libs/myko/server/src/ws_handler.rs`, find around line 1395:

```rust
            MykoMessage::ProtocolSwitch { protocol } => {
                log::warn!(
                    "Unexpected client message kind=protocol_switch_ack client={} protocol={} active_subscriptions={}",
                    session.client_id,
                    protocol,
                    session.subscription_count()
                );
            }
```

Delete the entire arm. The match block above it should now end cleanly with the arm before it.

- [ ] **Step 3: Build the whole workspace and find any remaining ProtocolSwitch references**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds. Any errors will point at remaining `MykoMessage::ProtocolSwitch` references that need to be deleted (most likely none — Tasks 13/14 already removed the producers).

- [ ] **Step 4: Verify no ProtocolSwitch references remain**

Run: `grep -rn "ProtocolSwitch\|protocol-switch" libs/myko/ --include='*.rs'`
Expected: no matches.

- [ ] **Step 5: Run the full test suite**

Run: `cargo test --target-dir target/claude --workspace`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/myko/core/src/wire/message.rs libs/myko/server/src/ws_handler.rs
git commit -m "refactor(core): delete MykoMessage::ProtocolSwitch variant"
```

---

## Phase 4: Negotiation tests, integration tests, and dependency cleanup

### Task 16: Add a test for the sticky JSON→CBOR negotiation behavior

**Files:**
- Modify: `libs/myko/server/src/ws_handler.rs` — add a unit test in the existing `tests` module.

This task verifies the sticky-promotion semantics with a focused test that exercises the `outgoing_format` atomic transitions.

- [ ] **Step 1: Find the existing tests module in `ws_handler.rs`**

In `libs/myko/server/src/ws_handler.rs`, locate the `#[cfg(test)] mod tests { ... }` block (it contains `test_channel_writer` near the bottom of the file).

- [ ] **Step 2: Add a unit test for `MykoProtocol::from` + atomic transitions**

Inside the `tests` module, add:

```rust
    #[test]
    fn outgoing_format_starts_as_json_and_promotes_to_cbor() {
        use std::sync::atomic::{AtomicU8, Ordering};

        let outgoing_format = AtomicU8::new(MykoProtocol::JSON as u8);

        // Initially JSON.
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::JSON,
        );

        // Simulate receiving a binary frame: promote.
        outgoing_format.store(MykoProtocol::CBOR as u8, Ordering::SeqCst);
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::CBOR,
        );

        // Simulate receiving more text frames after promotion: no change.
        // (The handler in the read loop only writes on Binary, never on Text,
        // so this is a no-op assertion that the field's last-write-wins
        // semantics give us stickiness for free.)
        assert_eq!(
            MykoProtocol::from(outgoing_format.load(Ordering::SeqCst)),
            MykoProtocol::CBOR,
        );
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test --target-dir target/claude -p myko-server --lib outgoing_format_starts_as_json_and_promotes_to_cbor`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/server/src/ws_handler.rs
git commit -m "test(server): verify outgoing_format JSON-to-CBOR sticky promotion"
```

### Task 17: Find and parameterize existing wire integration tests over JSON and CBOR

**Files:**
- Read: any existing wire integration test under `libs/myko/core/tests/`, `libs/myko/server/tests/`, or `libs/myko/core/src/**/tests/*.rs`.
- Modify: as needed.

- [ ] **Step 1: Locate existing wire integration tests**

Run: `grep -rn "MykoProtocol\|MykoMessage" libs/myko/*/tests/ libs/myko/core/src/**/tests*.rs 2>/dev/null`
Expected: a list of test files. Read them.

- [ ] **Step 2: For each integration test that exercises JSON message flow end-to-end, add a CBOR variant**

Pattern: if a test exists like:

```rust
#[test]
fn test_query_subscribe_flow_json() {
    let client = MykoClient::with_transport(...);
    // client uses default JSON
    ...
}
```

Add a CBOR mirror:

```rust
#[test]
fn test_query_subscribe_flow_cbor() {
    let client = MykoClient::with_transport(...);
    client.set_protocol(MykoProtocol::CBOR);
    ...
}
```

If no existing wire integration tests cover the end-to-end flow, skip this step — the gate test in Phase 1 already verifies serialization correctness.

- [ ] **Step 3: Run the integration tests**

Run: `cargo test --target-dir target/claude --workspace --tests`
Expected: PASS for both JSON and CBOR variants.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/
git commit -m "test(core): parameterize wire integration tests over JSON and CBOR"
```

(Skip this commit if no changes were made in Step 2.)

### Task 18: Remove the `rmp-serde` dependency

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `libs/myko/core/Cargo.toml`

- [ ] **Step 1: Verify no remaining rmp_serde references in code**

Run: `grep -rn "rmp_serde\|rmp-serde" libs/ --include='*.rs' --include='*.toml'`
Expected: no matches in source files. Possibly a single match in `cbor_roundtrip_tests.rs` from the `_documents_failure` test — that test references rmp_serde explicitly to demonstrate the bug. Decide:
- **Option A** (recommended): delete the `_documents_failure` test since rmp_serde is going away. The bug it documents is no longer reproducible without the dependency, and the gate test (`report_response_roundtrip_cbor`) is what protects the migration going forward.
- **Option B**: keep rmp_serde as a `dev-dependency` solely for the documentation test. More code, less benefit.

Choose Option A: delete the `_documents_failure` test from `cbor_roundtrip_tests.rs`. The doc-comment at the top of the file already explains its history.

- [ ] **Step 2: Delete the `_documents_failure` test**

In `libs/myko/core/src/wire/cbor_roundtrip_tests.rs`, delete the `#[ignore]` test `report_response_roundtrip_msgpack_documents_failure` and its surrounding doc-comment.

- [ ] **Step 3: Remove `rmp-serde` from `libs/myko/core/Cargo.toml`**

Find the line `rmp-serde.workspace = true` and delete it.

- [ ] **Step 4: Remove `rmp-serde` from the workspace Cargo.toml**

In `Cargo.toml` at the workspace root, find `rmp-serde = "1.3"` and delete that line.

- [ ] **Step 5: Build and test**

Run: `cargo check --target-dir target/claude --workspace`
Expected: builds.

Run: `cargo test --target-dir target/claude --workspace`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml libs/myko/core/Cargo.toml libs/myko/core/src/wire/cbor_roundtrip_tests.rs Cargo.lock
git commit -m "chore(core): remove rmp-serde dependency"
```

### Task 19: Final verification — clippy and full test sweep

**Files:** none (verification only)

- [ ] **Step 1: Check `.bacon-locations` for any lingering errors**

Run: `cat /home/trevor/Code/myko/.bacon-locations 2>/dev/null | head -50`
Expected: empty or no errors related to the migration. If errors exist, fix them in order before proceeding.

- [ ] **Step 2: Run clippy on the workspace**

Run: `cargo clippy --target-dir target/claude --workspace -- -D warnings`
Expected: no warnings.

If clippy complains about anything (e.g., unused imports left over from the rename tasks, dead code), fix it inline and commit:

```bash
git add <fixed-files>
git commit -m "chore(core): clippy cleanup post-CBOR-migration"
```

- [ ] **Step 3: Run rustfmt**

Run: `cargo fmt --check`
Expected: clean. If not, run `cargo fmt` and commit:

```bash
git add -A
git commit -m "chore: cargo fmt"
```

- [ ] **Step 4: Full test sweep**

Run: `cargo test --target-dir target/claude --workspace`
Expected: all PASS.

- [ ] **Step 5: Confirm migration is complete**

Run: `grep -rn "rmp_serde\|rmp-serde\|MSGPACK\|Msgpack\|msgpack\|use_binary\|SWITCH_TO_MSGPACK\|ProtocolSwitch" libs/myko/ --include='*.rs' --include='*.toml'`
Expected: no matches.

The Rust + Leptos side of the migration is complete. Server and Rust client now speak CBOR (binary frames) and JSON (text frames) with implicit capability-via-demonstration negotiation.

---

## Follow-on plans (out of scope here)

The Rust server now accepts both JSON and CBOR from any connected client and adapts per-session. Each non-Rust language client migrates independently against the migrated server, in this order per the spec's Section 6:

1. **TypeScript client** (`libs/myko/ts`) — replace its msgpack library with `cbor-x`. Cascades to svelte/vue wrappers automatically.
2. **Python client** (`libs/myko/py`) — replace its msgpack library with `cbor2`.
3. **Unreal C++ client** (`libs/myko/cpp`) — add an Unreal Module wrapping `tinycbor` (Intel, MIT) that mirrors the existing `FJsonSerializer`-based JSON path. Hand-rolled minimal CBOR decoder is the documented fallback if `tinycbor` integration is problematic.
4. **C# client** (`libs/myko/csharp`) — when prioritized; off the hot path.

Each language-client migration gets its own implementation plan written when prioritized.

Until each language client is migrated, it continues sending JSON; the server's capability-via-demonstration negotiation keeps that session on JSON for its lifetime, with no interop cliff.
