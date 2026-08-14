# 03 — The Record Format

**Normative.** Source: spec §9. Invariant prefix `RF`.

This document is the wire contract. Every binding implements it; the conformance vectors (§7) are the
arbiter. **All multi-byte integers are big-endian**, so byte-wise comparison equals numeric comparison
— which HLC ordering (§2) and canonical map ordering (§5) both rely on.

---

## 0. What it replaces

```rust
// libs/myko/core/src/wire/event/mod.rs:18 — the current shape
pub struct MEvent {
    pub item: Value,                 // fully-parsed JSON tree — whole entity, every write
    pub change_type: MEventType,
    pub item_type: Arc<str>,         // bare struct name — 02 §0
    pub created_at: Arc<str>,        // RFC3339 string
    pub tx: Arc<str>,                // UUID string, request-scoped
    pub source_id: Option<Arc<str>>,
}
```

Every field of it is wrong for a mesh: whole-entity payload (04 §1), unqualified type (02 §0), a
wall-clock string as the ordering key (§2), a request-scoped `tx` on a replicated record (§6), and an
optional origin.

The replacement is named **`Record`**. `MEvent` retires with the `ws:m:*` protocol (06 §7).

## 1. Requirements

1. **Header readable without a decoder library.** A `LOGGED` node indexes on
   `(scope, type, entity_id)` and must never parse the body.
2. **Body opaque and skippable** for nodes without the schema.
3. **Per-field metadata** — an HLC per field, an optional OCC precondition, and a merge strategy tag.
4. **Deterministic content hash** — Merkle leaves hash it (08 §2).
5. **Implementable in Rust, TypeScript, Python, Swift, Kotlin, C, C++, C#** — attached nodes decode
   records into local stores and encode their own writes, so the *record* is polyglot even where the
   mesh planes are not.

Requirement 3 reshapes everything else. Once every field carries its own timestamp and possibly a
precondition, **the record is already field-granular**, and shipping whole entities on top of
field-granular metadata is pure waste.

## 2. Three layers

| Layer | Encoding | Who must implement it |
|---|---|---|
| **Header** | fixed field order, length-prefixed — no library | every node |
| **Field entries** | varint-framed, custom | every node that merges or stores |
| **Field values** | canonical CBOR (§5) | only nodes with the schema |

> **RF-1** — A `LOGGED` node needs **no CBOR library at all**. It reads the header with a byte cursor,
> skips the field section by its length prefix, and stores the bytes verbatim.

True fixed offsets would require fixed-width ids, and entity ids today are arbitrary strings. If ids
later become fixed-width, the header tightens in a version bump. Either way the archival-appliance
role and the peer minimum stay cheap to implement.

### Hybrid Logical Clocks

```rust
/// 8 bytes, big-endian. Byte comparison == happens-before-or-concurrent ordering.
///
///   bits 63..16  physical: milliseconds since the Unix epoch (48 bits, good to year 10889)
///   bits 15..0   logical:  monotonic counter, reset whenever physical advances
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hlc(pub u64);
```

Update rule on send:

```
now = wall_clock_ms()
if now > self.physical { self = Hlc { physical: now, logical: 0 } }
else                   { self = Hlc { physical: self.physical, logical: self.logical + 1 } }
```

Update rule on receive of `remote`:

```
now = wall_clock_ms()
p   = max(self.physical, remote.physical, now)
if p == self.physical == remote.physical  { l = max(self.logical, remote.logical) + 1 }
else if p == self.physical                { l = self.logical + 1 }
else if p == remote.physical              { l = remote.logical + 1 }
else                                      { l = 0 }
self = Hlc { physical: p, logical: l }
```

> **RF-2** — HLCs replace `created_at` throughout. Nothing in the system compares RFC3339 strings.

Precision about what is being replaced: **nothing compares `created_at` today at all** — the apply
path is arrival-order overwrite (`libs/myko/core/src/server/context.rs`, `store.insert_many` inside
`emit_grouped`), so current behaviour is weaker than even whole-entity LWW. Where a comparison *would*
be added, lexicographic RFC3339 is fragile: format drift (`Z` vs `+00:00`) or trailing-zero variance
changes both ordering and equality, at ~30 bytes and a parse where a fixed-width integer compare would
do.

> **RF-3** — **Total order** is `(hlc, origin)`, with `origin` compared as 32 raw bytes,
> lexicographically. Deterministic in every implementation, and part of the conformance surface.

> **RF-4** — **Drift bound.** A node MUST reject a record whose HLC physical component leads its own
> wall clock by more than a configured bound (default 5 minutes), so one broken clock cannot win every
> LWW race for hours. Rejection is loud: logged with the origin, and surfaced as a peer-health signal.

> **RF-5** — HLC fixes *causally-related* writes misordered by clock skew. **Genuinely concurrent
> writes still resolve by arbitrary tiebreak.** HLC is no substitute for the per-field merge and CRDTs
> of 04.

## 3. The header

Fixed field order. `varstr` is `varint length` followed by that many UTF-8 bytes.

| # | Field | Encoding | Notes |
|---|---|---|---|
| 1 | `version` | `u8` | Record format version. `1` at first ship. |
| 2 | `record_type` | `u8` | `0` = set · `1` = delete · `2` = checkpoint (07 §6) |
| 3 | `header_flags` | `u8` | bit 0: `type_id` is interned (else inline) · bit 1: `actor` present · bit 2: `log_seq` present · bits 3–7 reserved, MUST be zero |
| 4 | `scope_id` | `varstr` | 05 §2 — always present, never derived at the receiver |
| 5 | `type_id` | `u32` if interned, else `varstr` `namespace` + `varstr` `name` | 02 §1, §4 below |
| 6 | `entity_id` | `varstr` | |
| 7 | `origin` | 32 bytes | ed25519 public key. **Non-optional** — a record always has an origin. |
| 8 | `actor` | `varstr`, present iff flag bit 1 | The identity responsible (08 §9). Distinct from `origin` (a node) and from the connection. |
| 9 | `record_hlc` | 8 bytes | §2 |
| 10 | `log_seq` | `u64`, present iff flag bit 2 | Per-origin log-layer sequence (01 NM-10). **Log layer only** — never consulted by state convergence. |
| 11 | `field_section_len` | `varint` | Byte length of §4, so a schema-free node skips it in one jump. |

> **RF-6** — Every header field is present in every record in this order. There are no optional
> positions except those gated by `header_flags`, and a reader that does not understand a flag bit
> MUST reject the record rather than guess.

### Type id interning

> **RF-7** — `type_id` is interned **per connection**. The handshake exchanges the qualified-name ↔ id
> mapping and control frames extend it mid-stream. **Intern ids never outlive the connection.**

> **RF-8** — **The log stores the qualified name (or its stable hash), never an intern id.** An
> archive must be readable without the connection that wrote it.

A record read back from the log and re-sent is re-interned against the new connection. Storage cost of
the name is bounded by the log's own type dictionary (07 §3), not paid per record.

### Deletes

> **RF-9** — **Delete is a register.** `record_type = delete` is an entity-level tombstone carrying its
> own HLC, competing with SETs under the total order of RF-3: a later SET beats an earlier DEL
> (recreate), a later DEL beats earlier SETs. There is no special case.

> **RF-10** — Per-field tombstone flags (§4) mark **field** removal within a live entity. They do not
> interact with entity deletion, and an entity-level DEL does not require per-field tombstones.

A recreate after a DEL is a partial-state hazard: the SET may carry only the fields that changed. The
unknown-entity fallback of RF-16 covers it.

## 4. The field section

```
varint  field_count
repeat field_count times:
    varint  field_id            (u32 value, varint-encoded)
    8 bytes hlc
    u8      entry_flags
    8 bytes precondition_hlc    present iff entry_flags bit 1
    varint  value_len
    bytes   value               canonical CBOR, value_len bytes; absent iff tombstone
```

`entry_flags`:

| Bit | Meaning |
|---|---|
| 0 | **tombstone** — this field is removed. `value_len` MUST be 0 and no value bytes follow. |
| 1 | **has_precondition** — an 8-byte `precondition_hlc` follows the flags byte (04 §5). |
| 2–4 | **merge strategy tag** — 04 §3. `0` LWW · `1` PN-Counter · `2` OR-Set · `3` LWW-Map · `4`–`7` reserved. |
| 5–7 | reserved, MUST be zero |

> **RF-11** — Entries are written in **ascending `field_id` order**, and a reader MUST reject a record
> whose entries are unordered or contain a duplicate `field_id`. Ordering makes merge a linear
> merge-join and makes the content hash (§6) definitionally order-free.

> **RF-12** — **Writes carry only changed fields.** A one-field edit to a fifty-field entity sends one
> field entry. A creation sends every field, so full-state transfer is the degenerate case, not a
> separate message type.

> **RF-13** — **LWW merge needs no schema.** Merging two records is a merge-join on `field_id`
> comparing HLCs and **copying value bytes without interpreting them**. Only CRDT merge (strategy tags
> 1–3) requires the schema, which is why state nodes remain schema-scoped (01 §2, 04 §3).

> **RF-14** — The merge strategy tag on the wire is **advisory and must agree with the receiver's
> schema**. A receiver holding a schema MUST use its own schema's strategy and MUST reject the record
> if the tag disagrees — a strategy mismatch is the incompatibility of 02 TI-8, caught at the record
> rather than silently merging a counter as a register. A receiver *without* the schema (TI-10)
> carries the tag through unexamined.

> **RF-15** — `precondition_hlc` **travels for audit and conflict inspection only**. It is never a
> receiver-side gate. Apply-time preconditions break convergence — see 04 §5, which is the reason.

> **RF-16** — **A delta for an unknown entity cannot be applied.** A node receiving a partial update
> for an entity it has never seen MUST take the explicit fallback path: request full state from the
> sender, or wait for anti-entropy to repair. **Silent drop is a protocol violation.**

## 5. Canonical CBOR

Field values are CBOR (RFC 8949) in **deterministic encoding**, RFC 8949 §4.2.1, with the following
tightened for cross-language reproducibility:

> **RF-17** — Canonical form for myko values:
>
> 1. Integers, lengths, tags, and simple values use the **shortest form** that encodes them.
> 2. **Definite lengths only.** No indefinite-length strings, arrays, or maps.
> 3. Map keys are sorted by the **byte-wise lexicographic order of their encoded key bytes**.
> 4. Duplicate map keys are a **decode error**, not last-wins.
> 5. **No floats unless the Rust type is `f32`/`f64`.** Where a float is unavoidable it is encoded in
>    the shortest form that round-trips exactly (RFC 8949 §4.2.2), `NaN` is the canonical quiet NaN
>    `0xf97e00`, and `-0.0` encodes as `-0.0` (it is **not** normalized to `+0.0`).
> 6. No CBOR tags other than those myko explicitly assigns. Tag assignments live in the conformance
>    vectors; an unassigned tag is a decode error.

Canonical output is not the default of `cbor-x` (TypeScript) or `cbor2` (Python). Under the v1 scope
that costs little (§7), but the rule is stated so that a binding which cannot emit canonical form
knows to re-encode through the single-purpose encoder rather than hope.

### Library coverage

`ciborium` (Rust), `cbor-x` (TypeScript), `cbor2` (Python), `tinycbor` (C/C++ — MIT, ~2 KLoC, no STL
or allocator assumptions), with mature libraries in Swift, Kotlin, and C#. Where a binding lacks one,
the emitted subset is small enough that a single-purpose encoder/decoder is on the order of 500 lines.

**The header and field framing need no library anywhere** — a byte cursor and varints.

## 6. The content hash

The content hash identifies **merged entity state**, not a record. Two nodes that have converged on an
entity MUST produce identical bytes. It is the leaf input for Merkle comparison (08 §2).

> **RF-18** — `content_hash` = BLAKE3-256 over the following byte sequence, in exactly this order:

```
"myko.entity.v1\0"                  15 bytes, domain separator
varstr   namespace                  the qualified type — 02 §1, never an intern id
varstr   name
varstr   scope_id
varstr   entity_id
u8       is_deleted                 1 if the entity is tombstoned, else 0
8 bytes  entity_tombstone_hlc        present iff is_deleted
varint   field_count                live and field-tombstoned entries; see RF-19
repeat, ascending field_id:
    varint   field_id
    8 bytes  hlc
    u8       tombstone               1 or 0
    u8       merge_strategy          the receiver's schema value, or the carried tag if unknown
    varint   value_len               0 if tombstone
    bytes    value                   canonical CBOR
```

> **RF-19** — **What is hashed, precisely:**
>
> - **Included:** every field entry the node holds for the entity, live or field-tombstoned —
>   *including fields it has no schema for* (02 TI-10). Excluding unknown fields would make a skewed
>   node's hash permanently disagree with its peers'.
> - **Excluded:** `precondition_hlc`, `origin`, `actor`, `log_seq`, `record_hlc`, and any
>   connection-scoped or provisional state (09 §5). These are properties of *how state arrived*, not
>   of the state.

> **RF-20** — Field-tombstoned entries are hashed with their HLC and `tombstone = 1`, **not omitted**.
> A node that has seen a field's deletion and a node that has never seen the field are in different
> states and must hash differently — otherwise the deletion silently fails to replicate through
> anti-entropy.

> **RF-21** — Content-hash agreement is required of every node that **serves anti-entropy** — i.e.
> complete peers (01 NM-7). Attached nodes never compute it: filtered nodes converge by
> re-evaluation, not comparison.

RF-21 is what collapses the cross-language hash burden. Under the v1 scope every anti-entropy server
is the native Rust implementation, so canonical-CBOR agreement binds **one** implementation.

## 7. Conformance

> **RF-22** — Conformance is a **deliverable**, shipped with the wire break as a test-vector suite run
> in every binding's CI. Two tiers:
>
> - **Tier 1 — every binding.** Record encode/decode round-trip, header parse without a CBOR library,
>   HLC arithmetic (both update rules of §2), total order (RF-3), and LWW merge-join results
>   (RF-13). This is what attached nodes actually do.
> - **Tier 2 — anti-entropy servers only.** Content-hash agreement over canonical CBOR (§5, §6).
>   Under the v1 scope this binds the native Rust implementation alone.

The vector format is a directory of `.bin` inputs with a `.json` expectation manifest, so a binding can
run it without linking myko. Vector categories:

| Category | Asserts |
|---|---|
| `header/` | byte-exact header encode; parse of every flag combination; rejection of reserved bits (RF-6) |
| `varint/` | boundary values at 1/2/3/4/5-byte widths |
| `hlc/` | send and receive update rules; drift-bound rejection (RF-4) |
| `order/` | `(hlc, origin)` total order across ties (RF-3) |
| `record/` | full record round-trip; unordered and duplicate `field_id` rejection (RF-11) |
| `merge/` | pairwise LWW merge-join outcomes, including field tombstones and unknown fields |
| `crdt/` | PN-Counter, OR-Set, LWW-Map state merges (04 §3) |
| `cbor/` | canonical encoding of each supported value shape (RF-17) |
| `hash/` | **tier 2** — content hash of each merged-state fixture (RF-18–RF-20) |

> **RF-23** — Every invariant in this document that a test can falsify has at least one vector.
> Adding an invariant without a vector is incomplete work.

## 8. Envelope, not record

> **RF-24** — `tx` and schema version do **not** ride the record. `tx` is request-scoped and
> meaningless remotely; schema version is per-batch. Both belong on the batch envelope (06 §5). **The
> record carries only what is intrinsic to the mutation.**

## 9. Debuggability

> **RF-25** — **JSON is not a co-equal wire encoding.** It is a *rendering*: a debug tool decodes a
> record to JSON for inspection, using the schema when available and emitting `field_id`-keyed output
> when not.

This keeps inspectability without a second encoder on the hot path, and without every node
implementing two formats. `myko-debug` owns the renderer (10 §2).

---

## Invariant index

| ID | One line |
|---|---|
| RF-1 | A `LOGGED` node needs no CBOR library |
| RF-2 | HLCs replace `created_at` everywhere |
| RF-3 | Total order is `(hlc, origin)`, origin compared as raw bytes |
| RF-4 | Reject records whose HLC leads local time past the drift bound |
| RF-5 | HLC does not resolve genuine concurrency; 04 does |
| RF-6 | Fixed header order; unknown flag bits reject the record |
| RF-7 | `type_id` interning is per-connection |
| RF-8 | The log stores the qualified name, never an intern id |
| RF-9 | Delete is a register competing under the same total order |
| RF-10 | Field tombstones are independent of entity deletion |
| RF-11 | Field entries are ascending and unique by `field_id` |
| RF-12 | Writes carry only changed fields |
| RF-13 | LWW merge needs no schema |
| RF-14 | Wire merge tag is advisory; disagreement with the schema rejects |
| RF-15 | `precondition_hlc` is audit metadata, never a receiver gate |
| RF-16 | A delta for an unknown entity takes the fallback path, never a silent drop |
| RF-17 | Canonical CBOR profile |
| RF-18 | Content hash = BLAKE3-256 over the specified byte sequence |
| RF-19 | Unknown fields hashed; `precondition_hlc`/origin/actor excluded |
| RF-20 | Field tombstones are hashed, not omitted |
| RF-21 | Hash agreement binds anti-entropy servers only |
| RF-22 | Two-tier conformance vectors ship with the wire break |
| RF-23 | Every falsifiable invariant has a vector |
| RF-24 | `tx` and schema version ride the envelope, not the record |
| RF-25 | JSON is a rendering, not an encoding |
