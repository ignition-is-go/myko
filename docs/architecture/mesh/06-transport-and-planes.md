# 06 — Transport and Planes

**Normative.** Source: spec §9.9, §10, §12.4. Invariant prefix `TP`.

---

## 1. The transport contract

> **TP-1** — **The protocol is transport-agnostic.** Record encoding (03), merge semantics (04),
> manifests (01 §7), anti-entropy (08 §2), routing (09), and subscriptions (08 §4) are all defined
> without reference to any particular transport.

A conforming transport provides exactly three things:

> **TP-2** — **Authenticated peer identity by public key.** Ed25519. The remote's key is verified
> during connection establishment; it is the subject of capabilities (05 §4) and the value of `origin`
> on every record (03 §3).

> **TP-3** — **Reliable, ordered, bidirectional byte streams**, many per connection, independently
> flow-controlled.

> **TP-4** — **Plane multiplexing**: every stream belongs to exactly one plane, fixed at stream open.
> Nothing else about plane naming is contractual.

QUIC + TLS 1.3 satisfies all three, and so does anything equivalent.

> **TP-5** — **iroh is the recommended binding, not a dependency.** It additionally solves NAT
> traversal, hole-punching, and relay fallback — genuinely hard, and relevant for edge nodes. A
> datacenter-only deployment could speak this protocol over plain QUIC; a Python implementation could
> use `aioquic` and a Go one `quic-go`, losing NAT traversal that datacenter peers do not need.

TP-5 is what keeps the polyglot story open: non-Rust iroh bindings expose only endpoints and streams,
so binding the protocol to iroh would push every other language through `iroh-ffi`.

**Sections 3–5 are the iroh binding** — the recommended realization, not part of the contract.

## 2. Planes

> **TP-6** — There are **two planes**:

| Plane | Carries | Lifecycle | Registered by |
|---|---|---|---|
| **`mesh`** | control + handshake/manifest, replication, anti-entropy | long-lived | every participating node |
| **`serve`** | routed commands, subscriptions, bulk transfer | request-scoped | `HANDLER` / stateful / `LOGGED` nodes |

A 1:1 split onto role bits would mean six connections per peer pair, since a plane is negotiated at
connection setup. Two planes captures most of the value: the coarse split is visible at dial time, and
**the planes version independently**.

> **TP-7** — `mesh` is the **peer minimum**. The *polyglot* minimum is the gateway protocol (§6) — the
> same record format and the same subscription envelope over WSS, with no planes at all.

> **TP-8** — **Bulk transfer may be split into its own plane later, compatibly.** State snapshot
> (08 §7) and log backfill (07 §5) are cold-path, resumable, bulk workloads unlike request/response
> RPC. In the iroh binding under the v1 scope, bulk rides **iroh-blobs**; the bulk plane remains the
> transport-agnostic realization a non-iroh binding would implement.

### The plane determines apply mode

> **TP-9** — **This is the load-bearing use of planes.** Records arriving on the **replication** stream
> of the `mesh` plane apply as `Origin::Remote` (04 §4): merge and index only, no cascade, no produce,
> no saga, no re-broadcast. Records produced by a command executing on the `serve` plane are
> local-origin.

> **TP-10** — **No wire flag, ever.** If a single plane carried both kinds, the discriminator would
> return to the wire and every record would have to declare its own origin — reintroducing exactly
> what connection-level planes make unnecessary.

TP-9 needs only TP-4. Whether the plane is named by ALPN or by a stream header is irrelevant to it.

## 3. ALPN (iroh binding)

No ALPN usage exists in the codebase — this is greenfield.

```
myko/mesh/1/<network>
myko/serve/1/<network>
```

> **TP-11** — ALPN mapping is an **optimization over the handshake, strictly additive**. iroh's
> `Router` filters at connection establishment, so a node that does not serve a plane simply does not
> register its ALPN and dialling it fails immediately rather than after a round trip. **The handshake
> declaration remains authoritative** (01 NM-16, NM-17), and an implementation that honours only the
> handshake is conforming.

> **TP-12** — The **network id** prevents two unrelated deployments sharing a public relay from ever
> negotiating. It is defence-in-depth against misconfiguration; NodeId pairing already handles
> authorization.

## 4. The handshake

Opened as the first stream of the `mesh` plane, before any other stream on the connection.

```
Dialer                                          Listener
  │                                                │
  │ ── Hello { protocol_version, node_id,          │
  │            network, planes[], manifest,        │
  │            grants[] } ────────────────────────►│
  │                                                │  verify grants (05 SC-10)
  │                                                │  compare schemas (02 §3)
  │◄────────── HelloAck { node_id, planes[],       │
  │              manifest, grants[],               │
  │              schema_verdicts[],                │
  │              intern_table[] } ─────────────────│
  │                                                │
  │ ── InternTableAck { intern_table[] } ─────────►│
  │                                                │
  │        both sides: connection ESTABLISHED      │
  │                                                │
  │◄────── control frames (InternExtend,           │
  │         ManifestUpdate, Goodbye) ─────────────►│
```

> **TP-13** — **The handshake opens with a plane declaration.** Each side states the planes it serves.
> Opening a stream on an undeclared plane is a protocol error (01 NM-17).

> **TP-14** — The handshake carries, in one round trip: plane declaration, manifest exchange (01 §7),
> capability presentation and verification (05 §4), and schema comparison with per-type verdicts
> (02 §3). A rejected pairing reports *which type* and *which crate versions* — never a bare failure.

> **TP-15** — **The intern table is established in the handshake and extended by control frames**
> (03 RF-7). Intern ids are connection-scoped and never persisted (03 RF-8). Either side may extend
> its own direction's table; a receiver seeing an unknown intern id MUST fail the record, not guess.

> **TP-16** — Schema comparison failing for a type removes **that type** from replication on this
> connection (02 TI-9). The connection proceeds for every compatible type.

### Control messages

```rust
pub enum MeshControl {
    Hello(Hello),
    HelloAck(HelloAck),
    InternTableAck { entries: Vec<InternEntry> },
    /// Mid-stream extension of this direction's intern table — TP-15.
    InternExtend { entries: Vec<InternEntry> },
    /// The sender's manifest changed; generation is monotonic (01 §7).
    ManifestUpdate { manifest: NodeManifest },
    /// Renewed or newly issued grants (05 §5).
    GrantUpdate { grants: Vec<SignedGrant> },
    /// Orderly shutdown, so the peer does not treat it as a fault.
    Goodbye { reason: GoodbyeReason },
}
```

## 5. Framing and envelopes

> **TP-17** — Every plane frames messages as **`u32` big-endian length prefix + payload**. Specified
> precisely because polyglot implementers depend on it.

> **TP-18** — **Control and RPC messages are canonical CBOR** (03 RF-17), matching the record's value
> encoding rather than introducing a second format.

> **TP-19** — **Records ride as-is on the replication stream.** 03's three-layer encoding already gives
> every node header access without a decoder library, so the plane adds only framing and the batch
> envelope. The record is not re-wrapped in CBOR.

```rust
/// One frame on the replication stream.
pub struct RecordBatch {
    /// Request/trace correlation. Request-scoped — 03 RF-24.
    pub tx: Option<Uuid>,
    /// Schema version of the sender, per batch — 03 RF-24.
    pub schema_generation: u64,
    /// Length-prefixed 03 records, concatenated.
    pub records: Bytes,
}
```

> **TP-20** — **Do not reuse `MykoMessage`.** Its `ws:m:*` variants are a client-server WebSocket
> protocol; the mesh plane needs messages with no WS equivalent (Merkle roots, tree descent requests,
> manifests). Per-plane envelopes are defined fresh.

> **TP-21** — Per-plane message schemas are **generated** from the Rust definitions using the
> reflection machinery of 01 §7, per the repository's cross-language generation rule. They are not
> hand-maintained in any binding.

### The subscription envelope

`MykoMessage` (`libs/myko/core/src/wire/message.rs:42`) carries 20 variants, **14 of them one
subscription protocol instantiated three times** — `{Query, View, Report}` × `{subscribe, response,
cancel, window, error}`.

> **TP-22** — One generic envelope collapses all fourteen with no loss:

```rust
pub enum ServeMsg {
    Subscribe { id: SubId, kind: OpKind, params: CborValue },
    Update    { id: SubId, payload: CborValue },
    Window    { id: SubId, window: WindowSpec },
    Cancel    { id: SubId },
    Error     { id: SubId, error: OpError },

    // Not part of the collapse — genuinely distinct operations.
    Command   { id: ReqId, command: CommandFrame },   // 09 §3
    Result    { id: ReqId, result: CommandResult },
}

pub enum OpKind { Query, View, Report }
```

> **TP-23** — **One envelope serves both the `serve` plane and the gateway's WSS protocol.** The
> transports differ; the message set does not.

## 6. The gateway protocol

> **TP-24** — A `GATEWAY` (01 §6) exposes WSS carrying **the same record format (03) and the same
> `ServeMsg` envelope (TP-22)** — with no planes, no ALPN, and no iroh.

Because there are no planes, TP-9's discriminator is unavailable, and the gateway supplies it
structurally instead:

> **TP-25** — On the gateway protocol, apply mode is determined by **frame direction and type**:
> records the gateway pushes to an attached node apply as `Origin::Remote`; records an attached
> **edge-owner** pushes to the gateway are injected into the replication plane preserving
> `origin = the attached node` (01 NM-12). An attached node pushing a record for an entity it does not
> own is a protocol error, not a merge input.

> **TP-26** — The gateway authenticates the attached node by its ed25519 keypair over the WSS
> connection and verifies its grants (05 SC-10) exactly as a peer handshake would. Attachment is not a
> weaker trust path than peering; it is a narrower one.

## 7. Retiring `ws:m:*`

> **TP-27** — **The WebSocket *protocol* retires; the WebSocket *transport* survives.** `MykoMessage`
> and the `ws:m:*` message set are replaced by 03's records and this document's envelopes.
> `ws_handler` (`libs/myko/server/src/ws_handler.rs`) and `autosocket` are **re-pointed, not deleted** —
> they carry the gateway protocol (§6).

This eliminates the relay question for the browser tier entirely: a gateway is a myko node you already
run, so there is no relay to host, pay for, or treat as an availability ceiling. iroh relays remain
relevant only to the **peer** mesh.

> **TP-28** — This is the largest migration risk in the plan — every client port changes protocol —
> and it is sequenced **last** (roadmap phase 14). Nothing else depends on it. It is a protocol
> migration on an existing transport, not a transport replacement.

## 8. What explicitly stays

The capability trait system (re-rooted to `NodeScoped`, 10 §3, otherwise untouched),
`inventory`-based registration, `Arc<str>` interning on hot fields, the reflection machinery, and
hyphae.

## 9. Not built: peer extensions

Recorded because the facts were researched and nothing in the wire precludes them. **No v1 machinery
depends on either.**

### Polyglot peers

Reimplementing HyParView + PlumTree in five languages is rejected — that is precisely the manual
fan-out gossip exists to avoid. A polyglot peer would instead participate **over the plane streams
without gossip**, which costs latency and not correctness: **anti-entropy is the authoritative
convergence path and gossip is a latency optimization on top of it.** Even if every gossip message
were dropped, periodic anti-entropy converges the mesh. Further, **direct pairwise replication over a
`mesh`-plane stream needs no gossip at all** — a polyglot node paired to a Rust node gets realtime
updates, losing only transitive delivery and onward relaying.

Pursuing it re-activates the cross-language content-hash requirement (03 RF-21) and makes M3 a real
verification again.

### Browser peers

iroh core and iroh-gossip both compile to wasm, so a browser peer is technically a realtime
participant. Two constraints: **relay-only always** (no arbitrary UDP, so no QUIC hole-punching; every
browser iroh connection traverses a relay over WebSocket, still end-to-end encrypted), and **no
iroh-blobs in wasm** (a browser peer would need TP-8's transport-agnostic bulk path).

Pedantically, browsers *can* hole-punch — via WebRTC data channels with ICE/STUN and TURN fallback.
iroh does not speak WebRTC, so that route would be a second binding of TP-1's contract, losing
iroh-gossip and still needing signaling plus TURN. **Even a WebRTC browser peer is a filtered node
that cannot anti-entropy (01 NM-7) or execute authoritatively (09 §2)** — so it is tiering, not
transport, that keeps browsers in the spoke tier.

Build notes if the extension is ever taken up: wasm requires
`iroh = { version = "1", default-features = false }`; there is no NPM package and the pattern is an
app-specific Rust wrapper via wasm-bindgen (`myko-core` already compiles to wasm, so **myko is the
wrapper**); and JavaScript has two non-equivalent iroh paths — browsers get wasm (relay-only, gossip
available), Node/Deno/Bun get NAPI (direct connections, no gossip).

---

## Invariant index

| ID | One line |
|---|---|
| TP-1 | The protocol is transport-agnostic |
| TP-2 | Transport must authenticate peers by ed25519 public key |
| TP-3 | Transport must provide many reliable ordered streams per connection |
| TP-4 | Every stream belongs to exactly one plane, fixed at open |
| TP-5 | iroh is the recommended binding, not a dependency |
| TP-6 | Two planes: `mesh` and `serve` |
| TP-7 | `mesh` is the peer minimum; the gateway protocol is the polyglot minimum |
| TP-8 | Bulk may split into its own plane later, compatibly |
| TP-9 | The plane determines apply mode |
| TP-10 | No per-record origin flag, ever |
| TP-11 | ALPN is an additive optimization; the handshake is authoritative |
| TP-12 | Network id prevents cross-deployment negotiation |
| TP-13 | The handshake opens with a plane declaration |
| TP-14 | One round trip: planes, manifests, grants, schema verdicts |
| TP-15 | Intern tables are connection-scoped; unknown ids fail the record |
| TP-16 | Schema failure removes one type, not the connection |
| TP-17 | `u32` big-endian length prefix on every plane |
| TP-18 | Control and RPC messages are canonical CBOR |
| TP-19 | Records ride as-is; no re-wrapping |
| TP-20 | Do not reuse `MykoMessage` for mesh planes |
| TP-21 | Per-plane schemas are generated, not hand-maintained |
| TP-22 | One subscription envelope replaces 14 variants |
| TP-23 | The same envelope serves the `serve` plane and the gateway |
| TP-24 | The gateway speaks records + `ServeMsg` over WSS, no planes |
| TP-25 | On the gateway, apply mode comes from direction and frame type |
| TP-26 | Attached nodes authenticate and present grants like peers |
| TP-27 | The WS protocol retires; the WS transport survives |
| TP-28 | The `ws:m:*` cutover sequences last |
