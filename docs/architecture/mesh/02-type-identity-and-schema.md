# 02 — Type Identity and Schema

**Normative.** Source: spec §3, §4.1. Invariant prefix `TI`.

---

## 0. The problem being fixed

`item_type` today is the bare struct identifier. The macro emits it as `ENTITY_NAME_STATIC`
(`libs/myko/macros/src/item.rs:969`) and threads it into `ItemRegistration.entity_type`, store keys
(`StoreRegistry::get_or_create(entity_type)`), and every wire record. **Two services both defining
`User` collide silently, everywhere at once** — one store, one Merkle leaf set, one routing entry.

## 1. Qualified names

> **TI-1** — Entity types and command ids are qualified by the **crate that defines them**:
> `identity.User`, `billing.CreateInvoice`.

```rust
// myko-core::mesh::name

/// `namespace.name`. Interned; comparison is pointer-first.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    pub namespace: Arc<str>,   // "identity"
    pub name: Arc<str>,        // "User"
}
```

> **TI-2** — The namespace is owned by the **defining crate**, never by the consuming node or service.

TI-2 is the whole mechanism. If an organization has a shared `identity` crate, every service linking
it emits `identity.User` **automatically** — same qualified name, same compiled type, same schema by
construction. Two services each defining their own `User` get distinct names automatically.
Collision-avoidance and type-sharing become one thing.

A node-level or service-level identifier would break it: two services sharing `identity` would stamp
`billing.User` and `crm.User` and fragment exactly the type they meant to share.

### Why not canonical-names-by-convention

Convergence is per-field merge (04) with no semantic merge across independently declared types. If
service A knows `User{id, name}` and service B knows `User{id, name, email}` — the commonest shape of
a shared type — a write from A **destroys B's `email`**. Sharing the defining crate makes divergent
schemas impossible by construction. That is why it is a mechanism and not a convention.

## 2. Deriving the namespace

The field named `crate_name` on every registration does **not** hold a crate name. It holds
`module_path!()` — a full module path:

- `libs/myko/macros/src/item.rs:777` — `crate_name: module_path!()`
- identically in `command.rs:139`, `query.rs:74`, `view.rs:138`, `report.rs:68`

Codegen compensates by substring-matching it (`x.crate_name.contains(&crate_name)`,
`libs/myko/core/src/codegen/mod.rs:158` and seven sibling call sites — this over-matches sibling
crates and is tracked as `lv-ea59`).

> **TI-3** — The namespace is the **first path segment of `module_path!()`**, not the raw value.
> Namespacing on the raw value would make **moving a struct between modules inside its own crate a
> wire-breaking change**.

```rust
// Evaluated at macro-expansion time; emitted as a &'static str.
fn namespace_of(module_path: &str) -> &str {
    module_path.split("::").next().unwrap_or(module_path)
}
```

> **TI-4** — An explicit override exists: `#[myko_item(namespace = "identity")]` and the equivalent on
> `#[myko_command]`. A crate rename or a cross-crate move is then a refactor rather than a forced
> data migration.

> **TI-5** — **The qualified name is a wire and registry key, not a Rust identifier.** It keys the
> record header (03 §3), store entries, Merkle leaves (08 §2), and routing (09 §3). The unqualified
> struct name remains the Rust and TypeScript identifier, and generated operation names
> (`GetAllTargets`, `TargetQuery`, `DeleteTarget`) stay unqualified.

> **TI-6** — **Migration:** a bare, unqualified `item_type` read from existing history or an
> unmigrated peer belongs to a reserved default namespace. The phase-3 converter rewrites stored
> history into explicit namespaces.

## 3. Version skew

Crate-qualification converts *collision* into *version skew*: `identity@1.2` and `identity@1.3` both
emit `identity.User` and may differ in fields.

> **TI-7** — **Crate version travels in the manifest, never in the type key.** In the key it would
> fragment every type on every release. In the manifest it lets a rejected pairing report *why*.

> **TI-8** — Nodes compare schemas at pairing using **compatibility rules, not equality**.
> Exact-match-or-reject would tear down replication during every rolling deploy, since skew
> mid-rollout is normal.

### Compatibility rules

Evaluated per entity type present on both sides, in the handshake (06 §4).

| Change from the older schema to the newer | Verdict |
|---|---|
| Field added, and it is `Option<T>` or has a default | **compatible** |
| Field added, non-optional, no default | **incompatible** — the older node cannot construct it |
| Field removed | **incompatible** |
| Field type changed (including `T` → `Option<T>`) | **incompatible** |
| Field renamed **with** `#[myko_field(renamed_from = "old")]` | **compatible** — same `field_id` chain (§5) |
| Field renamed **without** the attribute | seen as *removed + added* → **incompatible** |
| Merge strategy changed for a field | **incompatible** — 04 §3 |
| Docs, ordering, attribute changes not listed above | **compatible** |

```rust
pub enum SchemaVerdict {
    Compatible,
    Incompatible { entity_type: QualifiedName, reason: IncompatibilityReason },
}
```

> **TI-9** — An incompatible verdict **fails the pairing for that entity type only**, not the
> connection. The pair replicates every compatible type and reports the incompatible one with the
> crate versions of both sides. A connection-level failure would make one lagging type block an
> entire deployment's replication.

### Unknown-field retention

> **TI-10** — **A node receiving a field it has no schema for MUST store the entry opaquely, merge it
> by HLC like any other field, and include it in the content hash.**

Dropping it would make the node's content hash permanently disagree with every newer peer's hash for
the same entity, converting benign skew into **endless anti-entropy repair churn during exactly the
rolling deploys TI-8 exists to survive**.

Retention is cheap: below the schema layer, field entries are already opaque bytes (03 §4). The
receiver copies `(field_id, hlc, flags, value_bytes)` verbatim.

> **TI-11** — An opaquely-retained field is **never interpreted**: it does not participate in CRDT
> merge (which needs the strategy from the schema), never appears in a query predicate, and is not
> surfaced to handler code. It is carried and hashed, nothing more.

## 4. Entity field schemas — the one gap

`core/reflection.rs` already captures, **at macro-expansion time and embedded in the binary**, the
field names, Rust types, and optionality of every query/view/report/command argument struct:

```rust
// libs/myko/core/src/core/reflection.rs
pub struct OperationArgField {
    pub name: &'static str,
    pub rust_type: &'static str,   // as written, e.g. "Option<String>"
    pub optional: bool,
}
```

`CommandRegistration` (`core/command/registration.rs:5`) carries `args: &'static [OperationArgField]`
alongside `command_id`, `result_type`, `crate_name`, and the doc comment. Its module doc is explicit
that it was designed to be independent of codegen and to **never go stale relative to the compiled
binary** — exactly what a gossiped manifest needs (01 NM-15).

**`ItemRegistration` has no equivalent.** Today (`core/item/traits.rs:79`) it carries
`entity_type`, `crate_name`, and four function pointers, and nothing describing the entity's fields.

> **TI-12** — `ItemRegistration` gains a field schema list, mirroring `CommandRegistration::args`. It
> is the source for the manifest's `EntityEntry.fields` (01 §7), for per-field merge strategy
> selection (04 §3), and for `field_id` assignment (§5).

```rust
// libs/myko/core/src/core/reflection.rs — extended

pub struct FieldSchema {
    pub name: &'static str,
    pub rust_type: &'static str,
    pub optional: bool,
    /// 32-bit hash of `name` — §5. Precomputed at macro-expansion time.
    pub field_id: u32,
    /// How concurrent writes to this field merge — 04 §3. Selected from
    /// `rust_type` by the macro, overridable with `#[myko_field(merge = ...)]`.
    pub merge: MergeStrategy,
    /// Set by `#[myko_field(renamed_from = "old")]`. Read-through chain — §5.
    pub renamed_from: Option<&'static str>,
}

pub struct ItemRegistration {
    pub entity_type: &'static str,
    pub crate_name: &'static str,          // module_path!() — namespace via TI-3
    pub namespace: &'static str,           // TI-3/TI-4, precomputed
    pub fields: &'static [FieldSchema],    // TI-12
    pub parse: ItemParseFn,
    pub parse_bytes: ItemParseBytesFn,
    pub serialize_json: ItemSerializeJsonFn,
}
```

The change is **mechanical and additive**: the macro already has the full field list at expansion time
(`libs/myko/macros/src/item.rs` builds a `field_types: HashMap<String, syn::Type>` from
`input_struct.fields` a few lines below the registration it emits).

> **TI-13** — `OperationArgField` and `FieldSchema` stay **distinct types**. Argument structs have no
> `field_id`, no merge strategy, and no rename chain, because arguments do not merge and are not
> stored. Unifying them would put four meaningless fields on every command argument.

## 5. Field ids

> **TI-14** — `field_id` is a **32-bit hash of the field name**, computed at macro-expansion time and
> **collision-checked within each type at expansion time**. A collision is a compile error naming both
> fields.

This avoids protobuf-style manual numbering and its bookkeeping: no registry, no "never reuse a
number" discipline, and ids stay stable when fields are reordered.

**Hash function:** FNV-1a 32-bit over the field name's UTF-8 bytes. Chosen because it is trivial to
reimplement identically in every binding (the two-line loop is in the conformance vectors, 03 §7), has
no seed or endianness ambiguity, and collision resistance is irrelevant here — collisions are detected
exhaustively at compile time within the only namespace that matters, a single type's field set.

```rust
pub const fn field_id(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x01000193);
        i += 1;
    }
    hash
}
```

### Renames

A rename produces a new `field_id`, which is **correct** — renaming a field *is* a schema change. It
must not be a *silent* one: without an affordance every stored value is orphaned under the old id.

> **TI-15** — `#[myko_field(renamed_from = "old")]` ships **with** the feature, not after it. The
> attribute records the previous name; the derived `FieldSchema.renamed_from` gives the merge layer a
> read-through chain from the old id to the new.

Read-through versus migrate-forward-on-next-write is an implementation choice (07 §4 prefers
migrate-forward, since compaction then retires the old id naturally). The bare footgun — a rename
that silently orphans data — is not acceptable in either.

> **TI-16** — A rename chain is at most one hop deep per schema version. Renaming `a → b → c` across
> two releases requires the intermediate release to have been deployed, or an explicit
> `renamed_from = "a"` on `c`.

---

## Invariant index

| ID | One line |
|---|---|
| TI-1 | Types and command ids are crate-qualified: `identity.User` |
| TI-2 | The defining crate owns the namespace |
| TI-3 | Namespace = first segment of `module_path!()`, not the raw value |
| TI-4 | `namespace = "..."` override exists for renames and moves |
| TI-5 | The qualified name is a wire/registry key, not a Rust identifier |
| TI-6 | Bare `item_type` migrates into a reserved default namespace |
| TI-7 | Crate version rides the manifest, never the type key |
| TI-8 | Schemas compare by compatibility rules, not equality |
| TI-9 | Incompatibility fails one type, not the connection |
| TI-10 | Unknown fields are retained opaquely, merged, and hashed |
| TI-11 | Opaquely-retained fields are never interpreted |
| TI-12 | `ItemRegistration` gains a field schema list |
| TI-13 | `FieldSchema` and `OperationArgField` stay distinct |
| TI-14 | `field_id` = FNV-1a 32 of the name, collision-checked at expansion |
| TI-15 | `renamed_from` ships with the feature |
| TI-16 | Rename chains are one hop deep per schema version |
