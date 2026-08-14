# Mesh Phase 3 — The Wire Break

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. **This phase is not reversible after
> release.** Do not start it until [phase 2](2026-07-26-mesh-phase-2-benchmarks-and-sim-harness.md)
> has produced an M1 answer and encoding baseline numbers. Every task below is checked against those
> numbers at the end.

**Goal:** Replace `MEvent` with the three-layer state-change record, land per-field merge with its
four strategies, ship the two-tier conformance vectors, and provide the migration converter for
existing history.

**Architecture:** Everything that breaks the wire lands in **one phase** — each break is a migration
for live consumers, so shipping them separately means N migrations instead of one. Scope boundary:
this phase delivers the record **types, codecs, and merge algorithms** plus their vectors and the
converter. Wiring merge into the live apply path, OCC read-set tracking, and the Merkle index are
**phase 5**; the replication plane and `Origin::Remote` are **phase 7**.

**Tech Stack:** Rust, `ciborium` (already a workspace dep), `blake3` (new), `myko-sim` (from phase 2),
Criterion.

**Spec:** [`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md`](../specs/2026-07-25-myko-mesh-node-architecture.md) §9, §8
**Architecture:** [`03 — Record format`](../../architecture/mesh/03-record-format.md) (all of it) ·
[`04 — Merge semantics`](../../architecture/mesh/04-merge-semantics.md) §2–§4 ·
[`02 — Type identity`](../../architecture/mesh/02-type-identity-and-schema.md) (TI-5, TI-6, TI-10) ·
[`10 — Crate layout`](../../architecture/mesh/10-crate-layout-and-migration.md) (CL-3, CL-13, CL-14, CL-16, CL-18)
**Roadmap:** [phase 3](2026-07-26-myko-mesh-roadmap.md#phase-3--the-wire-break)

**Depends on:** phase 1 (`FieldSchema`, `field_id`, `MergeStrategy`), phase 2 (`myko-sim`, baselines).

---

## File Structure

**Files created:**

| File | Responsibility |
|------|----------------|
| `libs/myko/core/src/wire/record/mod.rs` | `Record`, `RecordType`, `HeaderFlags`, `EntryFlags`; encode/decode entry points. |
| `libs/myko/core/src/wire/record/header.rs` | Fixed-order header codec — no CBOR library on this path. |
| `libs/myko/core/src/wire/record/entries.rs` | Field-section codec: varint framing, ordering and duplicate enforcement. |
| `libs/myko/core/src/wire/record/varint.rs` | LEB128 varint read/write with explicit width bounds. |
| `libs/myko/core/src/wire/record/hlc.rs` | `Hlc`, both update rules, drift-bound check. |
| `libs/myko/core/src/wire/record/canonical.rs` | Canonical CBOR encode/validate per RF-17. |
| `libs/myko/core/src/wire/record/hash.rs` | Content hash per RF-18. |
| `libs/myko/core/src/mesh/state.rs` | `EntityState`, `FieldState`. |
| `libs/myko/core/src/mesh/merge.rs` | The merge algorithm of 04 §3. |
| `libs/myko/core/src/mesh/crdt/pn_counter.rs` | PN-Counter state and merge. |
| `libs/myko/core/src/mesh/crdt/orswot.rs` | ORSWOT state and merge. |
| `libs/myko/core/src/mesh/crdt/lww_map.rs` | LWW-Map state and merge. |
| `conformance/vectors/` | The two-tier test-vector suite (RF-22). |
| `conformance/README.md` | How a binding consumes the vectors without a Rust dependency. |
| `libs/myko/core/src/bin/gen_vectors.rs` | Generates `conformance/vectors/` from the Rust implementation. |
| `libs/myko/core/tests/conformance.rs` | Runs the vectors against the Rust implementation. |
| `libs/myko-sim/tests/merge_properties.rs` | Seeded convergence properties. |
| `libs/myko/server/src/bin/migrate_wire.rs` | The history converter (CL-13). |

**Files modified:**

| File | Changes |
|------|---------|
| `libs/myko/core/src/wire/event/mod.rs` | `MEvent` gains a deprecation note and a `From<MEvent> for Record` used only by the converter. **Not deleted** — `ws:m:*` still carries it until phase 14. |
| `libs/myko/core/src/wire/mod.rs` | Register the `record` module. |
| `libs/myko/core/src/lib.rs` | Register the `mesh` module. |
| `libs/myko/core/Cargo.toml` | Add `blake3`. |
| `Cargo.toml` (workspace) | Add `blake3`. |

**Type consistency** (must match exactly):

- `myko::wire::record::Record` — the state-change record. **Never `MeshEvent`, never `StateRecord`.**
- `myko::wire::record::Hlc(pub u64)` — packed 48-bit physical ms + 16-bit logical.
- `myko::mesh::state::EntityState` / `FieldState` — merged state, per 04 §2.
- `myko::mesh::merge::merge_record(&mut EntityState, &Record) -> MergeOutcome`.
- Domain separators, byte-exact: `b"myko.entity.v1\0"` (content hash), `b"myko.grant.v1\0"` (grants,
  phase 5).

---

## Phase 1: Primitives

Each of these is pure, independently testable, and has conformance vectors. Land them before anything
composes them.

### Task 1: Varint

**Files:** Create `wire/record/varint.rs`

- [ ] **Step 1: Implement LEB128 read/write**

```rust
/// Unsigned LEB128. Bounded to 5 bytes for u32 and 10 for u64 — a longer
/// encoding is a decode error, not a wrapped value.
pub fn write_varint(out: &mut Vec<u8>, mut v: u64);
pub fn read_varint(cur: &mut Cursor<'_>) -> Result<u64, DecodeError>;
```

> **Reject over-long encodings.** `0x80 0x80 0x80 0x80 0x80 0x00` is a valid-looking encoding of zero
> and must be an error, not a zero. A permissive decoder makes the content hash
> implementation-dependent, since two byte strings would decode to the same record.

- [ ] **Step 2: Test boundaries**

Round-trip 0, 127, 128, 16383, 16384, `u32::MAX`, `u64::MAX`; reject over-long and truncated inputs.

### Task 2: HLC

**Files:** Create `wire/record/hlc.rs`

- [ ] **Step 1: Implement `Hlc` with both update rules**

Layout: bits 63..16 physical milliseconds since the Unix epoch, bits 15..0 logical counter.
Big-endian on the wire, **so byte comparison equals numeric comparison** — every ordering path
depends on this.

Copy the two update rules verbatim from [03 §2](../../architecture/mesh/03-record-format.md#hybrid-logical-clocks).
Do not re-derive them.

- [ ] **Step 2: Implement the total order (RF-3)**

```rust
/// (hlc, origin), origin compared as 32 raw bytes lexicographically.
pub fn total_order(a: (Hlc, &NodeId), b: (Hlc, &NodeId)) -> Ordering;
```

- [ ] **Step 3: Implement the drift bound (RF-4)**

Reject a record whose HLC physical component leads local wall clock by more than the configured bound
(default 5 minutes). **Rejection is loud**: logged with the origin, surfaced as a peer-health signal.
A silent drop here means one broken clock quietly wins every LWW race and nobody finds out.

- [ ] **Step 4: Test logical-counter overflow**

16 bits is 65,536 ticks within one millisecond. Decide and test the behaviour: **carry into the
physical component** (the standard choice — it keeps the clock monotonic and merely runs slightly
ahead) rather than wrapping, which would make time go backwards.

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/wire/record
git commit -m "feat(wire): add varint and hybrid logical clock primitives"
```

### Task 3: Canonical CBOR

**Files:** Create `wire/record/canonical.rs`

- [ ] **Step 1: Implement encode and validate**

`ciborium` does not emit canonical form by default. Two functions:

```rust
/// Encode a value in canonical form per RF-17.
pub fn to_canonical<T: Serialize>(v: &T) -> Result<Vec<u8>, EncodeError>;
/// Verify bytes are already canonical. Used on ingest to reject
/// non-canonical input rather than silently re-hashing it differently.
pub fn is_canonical(bytes: &[u8]) -> Result<(), DecodeError>;
```

The six rules, from [03 §5](../../architecture/mesh/03-record-format.md#5-canonical-cbor): shortest
form; definite lengths only; map keys sorted by byte-wise lexicographic order of their **encoded key
bytes**; duplicate map keys are a decode error; float rules (shortest round-tripping form, canonical
quiet NaN `0xf97e00`, `-0.0` **not** normalized to `+0.0`); no unassigned tags.

- [ ] **Step 2: Test each rule with a positive and a negative case**

Especially the two classic cross-language divergences: **map-key ordering** and **float encoding**.
These are the ones that will break a TypeScript or Python binding later, so the vectors must pin them
now.

- [ ] **Step 3: Commit**

### Task 4: Content hash

**Files:** Create `wire/record/hash.rs`; modify `Cargo.toml` files

- [ ] **Step 1: Add `blake3`**

Workspace `Cargo.toml`, near the other serialization deps:

```toml
blake3 = "1"
```

- [ ] **Step 2: Implement RF-18 exactly**

Hash the byte sequence in
[03 §6](../../architecture/mesh/03-record-format.md#6-the-content-hash) — in that order, with the
`b"myko.entity.v1\0"` separator.

Three things the implementation must get right, each of which is a silent-divergence bug if missed:

- **Unknown fields are included** (RF-19). A skewed node that excludes them hashes differently from
  every newer peer forever, converting benign version skew into endless anti-entropy churn.
- **Field tombstones are hashed with `tombstone = 1`, not omitted** (RF-20). A node that saw a
  deletion and a node that never saw the field are in different states and must hash differently, or
  the deletion silently fails to replicate.
- **`precondition_hlc`, `origin`, `actor`, `log_seq`, and `record_hlc` are excluded.** They describe
  *how state arrived*, not the state.

- [ ] **Step 3: Test the three above explicitly, as named tests**

```rust
#[test] fn unknown_fields_are_included_in_the_hash() { … }
#[test] fn field_tombstones_hash_differently_from_absent_fields() { … }
#[test] fn precondition_and_origin_do_not_affect_the_hash() { … }
```

Name them after the invariant, so a future reader sees why they exist without opening the spec.

- [ ] **Step 4: Commit**

---

## Phase 2: The record codec

### Task 5: Header codec

**Files:** Create `wire/record/header.rs`, `wire/record/mod.rs`

- [ ] **Step 1: Define the types**

```rust
pub enum RecordType { Set = 0, Delete = 1, Checkpoint = 2 }

bitflags! {
    pub struct HeaderFlags: u8 {
        const TYPE_INTERNED = 1 << 0;
        const HAS_ACTOR     = 1 << 1;
        const HAS_LOG_SEQ   = 1 << 2;
        // bits 3..7 reserved — RF-6 requires rejecting them if set
    }
}
```

- [ ] **Step 2: Implement encode/decode in the exact field order of RF-6**

`version`, `record_type`, `header_flags`, `scope_id`, `type_id`, `entity_id`, `origin` (32 bytes),
`actor` (flagged), `record_hlc` (8 bytes), `log_seq` (flagged), `field_section_len`.

- [ ] **Step 3: Reject reserved flag bits (RF-6)**

A reader that does not understand a flag bit **rejects the record** rather than guessing. This is what
makes a future flag addition safe.

- [ ] **Step 4: Implement the schema-free skip path (RF-1)**

```rust
/// Parse the header and return the body byte range, without touching CBOR.
/// This is the whole `Logged` node interface — a byte cursor and varints.
pub fn parse_header_only(bytes: &[u8]) -> Result<(Header, Range<usize>), DecodeError>;
```

- [ ] **Step 5: Test it with CBOR unavailable**

Add a test that parses a header from bytes and asserts on `(scope, type, entity_id)` **without
constructing any CBOR value**. The point of RF-1 is that an archival appliance needs no CBOR library;
a test that happens to link one does not prove it.

### Task 6: Field-section codec

**Files:** Create `wire/record/entries.rs`

- [ ] **Step 1: Implement `EntryFlags` and the entry codec**

Per [03 §4](../../architecture/mesh/03-record-format.md#4-the-field-section): bit 0 tombstone, bit 1
has_precondition, bits 2–4 merge strategy tag, bits 5–7 reserved.

> The strategy tag values **are** `MergeStrategy`'s discriminants from phase 1. Convert with a checked
> `TryFrom<u8>`, never a cast — an unassigned tag value (4–7) must be a decode error.

- [ ] **Step 2: Enforce ordering and uniqueness (RF-11)**

Reject a record whose entries are unordered or contain a duplicate `field_id`. Ordering is what makes
merge a linear merge-join and makes the content hash definitionally order-free — a permissive decoder
silently gives up both.

- [ ] **Step 3: Enforce the tombstone/value relationship**

`tombstone` set ⟹ `value_len == 0` and no value bytes. Both directions are errors.

- [ ] **Step 4: Round-trip tests, then commit**

```bash
git add libs/myko/core/src/wire/record
git commit -m "feat(wire): add the three-layer state-change record codec"
```

---

## Phase 3: Merge

### Task 7: `EntityState` and LWW merge

**Files:** Create `mesh/state.rs`, `mesh/merge.rs`; modify `lib.rs`

- [ ] **Step 1: Define `EntityState` and `FieldState`**

Copy from [04 §2](../../architecture/mesh/04-merge-semantics.md#2-stored-entity-state). `fields` is a
`BTreeMap<u32, FieldState>` so merge is a merge-join; `content_hash` is a `OnceCell` invalidated on
mutation.

> **`FieldState.origin` is stored, not merely received** (MG-3). It is the RF-3 tiebreak input;
> dropping it makes merge non-deterministic on exact HLC ties, which a seeded property test will find
> and a hand-written test will not.

- [ ] **Step 2: Implement `merge_record`**

Copy the algorithm from
[04 §3](../../architecture/mesh/04-merge-semantics.md#the-algorithm). Three things it must do that are
easy to get wrong:

- **A DEL does not clear field state** (MG-7). Fields are retained so a later SET beating the tombstone
  revives a coherent entity rather than a one-field fragment.
- **CRDT fields advance the HLC to max, never select by it** (MG-6). Selecting by HLC discards one
  side's state and defeats the CRDT.
- **A wire tag disagreeing with the schema rejects the record** (RF-14), rather than merging a counter
  as a register.

- [ ] **Step 3: Implement the unknown-entity fallback (RF-16)**

```rust
pub enum MergeOutcome {
    Applied,
    /// Partial update for an entity never seen. The caller must request full
    /// state or wait for anti-entropy — RF-16. Silent drop is a protocol violation.
    NeedsFullState { type_id: QualifiedName, entity_id: EntityId },
    Rejected(RejectReason),
}
```

Returning an enum rather than `Result<(), _>` is deliberate: `NeedsFullState` is not an error and must
not be handled by the error path.

- [ ] **Step 4: Test commutativity, associativity, and idempotence (MG-5) directly**

Not as a property test yet — as three explicit unit tests with hand-built records, so a failure points
at the algorithm rather than at the generator.

### Task 8: The three CRDTs

**Files:** Create `mesh/crdt/{pn_counter,orswot,lww_map}.rs`

- [ ] **Step 1: PN-Counter**

State `{ actor -> [p, n] }`, read `sum(p) - sum(n)`, merge per-actor max. Canonical CBOR sorts the map
by actor bytes, so the encoding is deterministic for free.

- [ ] **Step 2: ORSWOT**

Copy the algorithm from
[04 §3.3](../../architecture/mesh/04-merge-semantics.md#33-or-set--orswot). It is **add-wins**
(MG-13), element identity is **canonical CBOR bytes** (MG-14), and it carries a version-vector context
rather than per-add tombstones — which is what bounds its size to `actors + elements`.

> Test the concurrent add/remove case explicitly. `{Alice}` with concurrent adds of Bob and Carol must
> yield `{Alice, Bob, Carol}`. Under whole-entity LWW it yields one or the other, silently revoking a
> person — the security-adjacent bug this strategy exists to prevent.

- [ ] **Step 3: LWW-Map**

Per-key `(hlc, origin, value | null)`, merge per key by RF-3, `null` is a key tombstone under the same
GC window as entity tombstones.

- [ ] **Step 4: Enforce the actor bound (MG-11, MG-12)**

Reject CRDT state naming an actor that is not, per the current membership view, a durable node for the
scope. Under the v1 trust model this is a correctness check against bugs, not a defence against a
hostile peer — but it is the check that keeps actor sets from growing with client count.

> Membership is not available until phase 6. Implement the check behind a
> `MembershipView` trait with a permissive `AllowAll` impl for now, and **file a follow-up to wire the
> real view in phase 6** rather than leaving a `TODO`:
> `levi add "wire real MembershipView into CRDT actor-bound check" -p p2 --dep <phase-6-task>`

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/mesh
git commit -m "feat(mesh): add EntityState, per-field merge, and the three state-based CRDTs"
```

---

## Phase 4: Conformance vectors

### Task 9: Generate the vectors

**Files:** Create `conformance/`, `libs/myko/core/src/bin/gen_vectors.rs`

- [ ] **Step 1: Place them outside every crate (CL-3)**

```
conformance/
  README.md
  vectors/
    header/    varint/    hlc/     order/
    record/    merge/     crdt/    cbor/
    hash/                          # tier 2
```

Each category is `NNN-name.bin` inputs plus a `manifest.json` of expectations. **No Rust dependency**
— `libs/myko/ts`, `py`, `cpp`, and `csharp` consume the directory directly.

- [ ] **Step 2: Write the generator**

`gen_vectors.rs` produces the directory from the Rust implementation. Deterministic: fixed seeds,
fixed node ids, fixed timestamps. **No `Uuid::new_v4()`, no `Utc::now()`** anywhere in it, or the
vectors churn on every run and the diff becomes unreviewable.

- [ ] **Step 3: Cover every falsifiable invariant (RF-23)**

Walk 03's invariant index and confirm each has at least one vector. The categories map to it directly.
**An invariant without a vector is incomplete work** — note any that resist vectoring and say why in
`conformance/README.md`.

- [ ] **Step 4: Write `conformance/README.md` for a non-Rust implementer**

It must be sufficient on its own: the two tiers (RF-22), the manifest format, how to run a subset, and
what tier 2 requires that tier 1 does not. The audience is someone writing the Python binding who has
never read a Rust file in this repo.

### Task 10: Run the vectors against Rust

**Files:** Create `libs/myko/core/tests/conformance.rs`

- [ ] **Step 1: Load and assert every vector**

Both tiers pass in Rust. This is circular for the generator's own output — which is why Step 2 exists.

- [ ] **Step 2: Hand-write at least five vectors independently**

Byte sequences written by hand from 03's tables, **not** produced by the encoder. If the encoder has a
field-order bug, generated vectors encode the bug and pass. Hand-written vectors are the only thing
that catches it. Cover: a minimal SET, a SET with all header flags set, a DEL, a field tombstone, and
one over-long varint that must be rejected.

- [ ] **Step 3: Commit**

```bash
git add conformance libs/myko/core/src/bin/gen_vectors.rs libs/myko/core/tests/conformance.rs
git commit -m "test(wire): add the two-tier conformance vector suite"
```

---

## Phase 5: Convergence properties

### Task 11: Seeded property tests in `myko-sim`

**Files:** Create `libs/myko-sim/tests/merge_properties.rs`

- [ ] **Step 1: Merge determinism**

Conflicting writes applied in different orders converge identically; strategy selection per field type
is deterministic. Generate random record sequences from a seed, apply in N random permutations, assert
identical `content_hash`.

- [ ] **Step 2: Per-field independence**

Concurrent edits to distinct fields both survive; OR-Set adds both stick; counters sum.

- [ ] **Step 3: Precondition semantics**

`precondition_hlc` **travels and does not gate** (RF-15, MG-23). The falsifying test is the divergence
scenario from 04 §5, run as a simulation:

```
A writes occupied=true @hlc=10, precondition occupied.hlc == 5
B writes occupied=true @hlc=11, precondition occupied.hlc == 5
Replica C receives A then B; replica D receives B then A.
Assert: C and D have IDENTICAL state.
```

If preconditions were checked at apply time, C keeps A's write and D keeps B's, permanently. **This
test is the reason apply-time preconditions are forbidden**, and it must exist before any code is
tempted to add them.

> **Scope boundary:** OCC *read-set tracking* in `CommandContext` is phase 5. This phase proves only
> that the record-level precondition travels and does not gate.

- [ ] **Step 4: Duplication and reorder under the phase-2 fault harness**

Every record delivered twice and out of order; assert convergence. This is what justifies ruling out
op-based CRDTs (MG-10) — and if it passes trivially, check that the harness is actually injecting the
faults.

- [ ] **Step 5: Commit**

---

## Phase 6: The migration converter

### Task 12: Convert existing history

**Files:** Create `libs/myko/server/src/bin/migrate_wire.rs`; modify `wire/event/mod.rs`

- [ ] **Step 1: Implement `From<MEvent> for Record`**

Three transformations (CL-13):

| From | To |
|---|---|
| `created_at` RFC3339 string | `Hlc` — parsed millis as the physical component, logical 0 |
| `item: Value` (whole-entity JSON) | field entries, one per JSON object key, each with the record's HLC |
| bare `item_type` | the reserved default namespace (TI-6) |

> **Do not invent attribution** (CL-14). `origin` is the converting deployment's node id; **`actor` is
> absent**, not defaulted to a placeholder identity. A converted record honestly says "we do not know
> who did this," which is true, rather than asserting a lie that audit will later trust.

- [ ] **Step 2: Handle the field-id assignment carefully**

A converted record's field ids come from the **current** schema's `field_id` for each JSON key. A key
with no matching schema field is retained as an **unknown field** (TI-10) under
`field_id(key)` — not dropped. History predating a field rename will carry the old id and resolve
through the rename chain (TI-15).

- [ ] **Step 3: Make it idempotent and resumable**

It runs against a production Postgres log offline. Track a converted-watermark; re-running from any
point must produce identical output. **Test resumption explicitly** by killing and restarting
mid-run — a converter that is only idempotent in theory is a converter that corrupts a log at 3am.

- [ ] **Step 4: Dry-run mode and a diff report**

Default to dry-run. Report: records converted, records with unknown fields, records with unparseable
`created_at`, and the resulting size delta. **Require an explicit `--commit` flag to write.**

- [ ] **Step 5: Test against a real log copy**

Ask the user for a copy of a real Postgres log, or a dump. Verify: every record converts, the record
count matches, no record loses a field, and re-running produces zero changes.

- [ ] **Step 6: Deprecate `MEvent` without deleting it**

`ws:m:*` still carries `MEvent` until phase 14. Add a module-level note pointing at `Record` and the
phase-14 cutover; **do not** add `#[deprecated]`, which would produce warnings across the whole tree
for eleven phases.

- [ ] **Step 7: Commit**

```bash
git add libs/myko/server/src/bin/migrate_wire.rs libs/myko/core/src/wire/event/mod.rs
git commit -m "feat(server): add the wire migration converter for existing history"
```

---

## Phase 7: Gate

### Task 13: Check against the phase-2 baselines

- [ ] **Step 1: Re-run the encoding and merge benches**

```bash
cargo bench --target-dir target/claude -p myko --features bench --bench record_encoding
cargo bench --target-dir target/claude -p myko --features bench --bench merge_apply
```

- [ ] **Step 2: Compare against the recorded phase-2 numbers**

The phase-2 baselines were recorded with a commit sha in `M1-findings.md`. Compare directly.

- **Met** → proceed.
- **Deviated** → the deviation is **explained and accepted in writing**, in `M1-findings.md`, before
  release. Not "probably noise" — an explanation.
- **Full-entity encode/decode materially regressed on realistic shapes** → **stop and reopen spec §9.**
  This is the outcome phase 2 existed to catch, and catching it late is still catching it.

- [ ] **Step 3: Full sweep**

Check `.bacon-locations` for outstanding clippy errors before running clippy, and fix in order.

```bash
cargo check --target-dir target/claude --workspace
cargo clippy --target-dir target/claude --workspace -- -D warnings
cargo fmt --check
cargo test --target-dir target/claude --workspace -- --nocapture
cargo test --target-dir target/claude -p myko-sim -- --nocapture
```

- [ ] **Step 4: Confirm the exit criteria**

From the roadmap:

- [ ] Tier-1 vectors pass in Rust, and the vector directory is consumable without a Rust dependency.
- [ ] Tier-2 hash vectors pass in Rust.
- [ ] Merge determinism, per-field independence, and precondition-travel properties pass under
      `myko-sim` seeds.
- [ ] The converter round-trips a copy of a real Postgres log, and is idempotent and resumable.
- [ ] Bench numbers are met, or the deviation is explained and accepted in writing.

- [ ] **Step 5: Tag the wire version**

`Record.version = 1` is now frozen. Note in the release notes that any subsequent change to the header
layout, the field-entry layout, the canonical CBOR profile, or the content-hash input is a **version
bump**, not an edit.

---

## What this phase deliberately does not do

| Deferred | To | Why |
|---|---|---|
| Wiring merge into the live apply path | phase 5 | The store's keying changes to `(qualified_type, scope, id)` there; doing it here would mean doing it twice. |
| OCC read-set tracking in `CommandContext` | phase 5 | Needs the store's read seam, which phase 5 defines. This phase only proves the precondition travels and does not gate. |
| The Merkle index | phase 5 | Must be keyed per `(item_type, scope)` from the start; scope handling lands there. |
| `Origin::Remote` | phase 7 | The apply mode is determined by the plane (MG-18), and there is no plane yet. |
| Deleting `MEvent` | phase 14 | `ws:m:*` still carries it. |
| Tombstone GC | phase 5 | The GC window is shared with the cold-bootstrap threshold (MG-26) and configured as one number, with the store. |
