# Mesh Phase 1 — Item Field Schemas and Merge-Strategy Mapping

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Work them in order —
> later tasks depend on types introduced by earlier ones.

**Goal:** Give every `#[myko_item]` entity a complete, compile-time field schema — name, Rust type,
optionality, a stable 32-bit `field_id`, and a merge strategy — and give every registration a derived
`namespace`. Nothing consumes this data yet; phase 3 does.

**Architecture:** Purely additive macro and reflection work. `ItemRegistration` gains
`fields: &'static [FieldSchema]` mirroring `CommandRegistration::args`, plus a precomputed
`namespace`. Merge strategy is inferred from the declared Rust type and overridable per field. Field
ids are FNV-1a hashes of field names, collision-checked at macro-expansion time within each type.

**Tech Stack:** Rust, `syn` / `quote` / `proc-macro2`, `inventory`, `trybuild` (new dev-dependency for
compile-fail tests).

**Spec:** [`docs/superpowers/specs/2026-07-25-myko-mesh-node-architecture.md`](../specs/2026-07-25-myko-mesh-node-architecture.md) §4.1, §8.3
**Architecture:** [`02 — Type identity and schema`](../../architecture/mesh/02-type-identity-and-schema.md) (TI-3, TI-4, TI-12, TI-14, TI-15) ·
[`04 — Merge semantics`](../../architecture/mesh/04-merge-semantics.md) (MG-8, MG-9) ·
[`10 — Crate layout`](../../architecture/mesh/10-crate-layout-and-migration.md) (CL-9, CL-11)
**Roadmap:** [phase 1](2026-07-26-myko-mesh-roadmap.md#phase-1--item-field-schemas-and-merge-strategy-mapping)

**Closes:** `lv-ea59` (codegen crate filter uses substring match on `module_path`).

---

## File Structure

**Files modified:**

| File | Responsibility | Changes |
|------|----------------|---------|
| `libs/myko/core/src/core/reflection.rs` | Compile-time reflection metadata | Add `MergeStrategy`, `FieldSchema`, `field_id()`, `namespace_of()`. Keep `OperationArgField` untouched (TI-13). |
| `libs/myko/core/src/core/item/traits.rs` | `ItemRegistration` | Add `namespace` and `fields`. |
| `libs/myko/core/src/core/command/registration.rs` | `CommandRegistration` | Add `namespace`, `result_type_namespace`. |
| `libs/myko/core/src/core/query/registration.rs` | `QueryRegistration` | Add `namespace`. |
| `libs/myko/core/src/core/view/registration.rs` | `ViewRegistration` | Add `namespace`. |
| `libs/myko/core/src/core/report/registration.rs` | `ReportRegistration` | Add `namespace`, `output_type_namespace`. |
| `libs/myko/core/src/codegen/mod.rs` | TS codegen crate filtering | Replace 8 `crate_name.contains(&crate_name)` filters with `namespace == crate_name` equality. |
| `libs/myko/macros/src/lib.rs` | Shared macro helpers | Add `entity_field_schema_tokens()`, `namespace_tokens()`, `const_field_id()`. Keep `field_metadata_tokens()` as-is. |
| `libs/myko/macros/src/item.rs` | `#[myko_item]` | Parse `namespace` option in `ItemArgs`; strip `#[myko_field]`; emit `namespace` + `fields` on `ItemRegistration`. |
| `libs/myko/macros/src/command.rs` | `#[myko_command]` | Emit `namespace` + `result_type_namespace`. |
| `libs/myko/macros/src/query.rs` | `#[myko_query]` | Emit `namespace`. |
| `libs/myko/macros/src/view.rs` | `#[myko_view]` | Emit `namespace`. |
| `libs/myko/macros/src/report.rs` | `#[myko_report]` | Emit `namespace`, `output_type_namespace`. |
| `libs/myko/core/Cargo.toml` | Core deps | Add `trybuild` as a dev-dependency. |

**Files created:**

| File | Responsibility |
|------|----------------|
| `libs/myko/macros/src/field_attr.rs` | Parse and strip `#[myko_field(merge = ..., renamed_from = "...")]`. |
| `libs/myko/core/src/core/reflection_tests.rs` | Schema-completeness and strategy-selection tests over real macro-expanded entities. |
| `libs/myko/core/tests/compile_fail/field_id_collision.rs` | `trybuild` fixture: two fields whose names collide under FNV-1a. |
| `libs/myko/core/tests/compile_fail/field_id_collision.stderr` | Expected compiler output. |
| `libs/myko/core/tests/compile_fail.rs` | `trybuild` runner. |

**Type consistency** (used throughout; must match exactly):

- `myko::reflection::MergeStrategy` — `#[repr(u8)]`, variants `Lww = 0`, `PnCounter = 1`, `OrSet = 2`,
  `LwwMap = 3`. The discriminants are the on-wire merge-strategy tag (03 §4, `entry_flags` bits 2–4)
  and must not be reordered or renumbered — a change is a wire break.
- `myko::reflection::FieldSchema` — fields in this order: `name`, `rust_type`, `optional`, `field_id`,
  `merge`, `renamed_from`.
- `myko::reflection::field_id(&str) -> u32` — `const fn`, FNV-1a 32.
- `myko::reflection::namespace_of(&str) -> &str` — `const`-compatible first-segment split.
- Registration field name is `namespace` everywhere (never `crate_namespace` or `ns`).

---

## Phase 1: Reflection primitives in core

No macro changes yet. This phase is compile-only and adds no behaviour.

### Task 1: Add `MergeStrategy`, `FieldSchema`, `field_id`, `namespace_of`

**Files:** Modify `libs/myko/core/src/core/reflection.rs`

- [ ] **Step 1: Append the new types to `reflection.rs`**

The file today is 23 lines: a module doc and `OperationArgField`. Append below `OperationArgField`,
leaving it untouched (TI-13 — argument structs have no `field_id`, no merge strategy, and no rename
chain, because arguments do not merge and are not stored):

```rust
/// How concurrent writes to one field are reconciled.
///
/// Discriminants are the on-wire merge-strategy tag (see the mesh record
/// format, `entry_flags` bits 2–4). **Never reorder or renumber** — a change
/// here is a wire break.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MergeStrategy {
    /// Last-writer-wins register. Correct — not merely tolerable — for opaque
    /// scalars, where no coherent merge exists.
    Lww = 0,
    /// State-based PN-Counter. Both concurrent intents are "+n".
    PnCounter = 1,
    /// State-based observed-remove set (ORSWOT). Concurrent adds both stick.
    OrSet = 2,
    /// Per-key LWW map. Concurrent writes to different keys both survive.
    LwwMap = 3,
}

/// One field of a `#[myko_item]` entity, captured by the macro directly from
/// the struct's field list.
///
/// Distinct from [`OperationArgField`] on purpose: arguments do not merge, are
/// not stored, and have no stable id, so four of these fields would be
/// meaningless on them.
#[derive(Debug, Clone, Copy)]
pub struct FieldSchema {
    pub name: &'static str,
    /// Rust type as written on the field (e.g. `"Option<String>"`).
    pub rust_type: &'static str,
    /// `true` if the field's type is `Option<...>`.
    pub optional: bool,
    /// FNV-1a 32 of `name`, precomputed at macro-expansion time and
    /// collision-checked within the type.
    pub field_id: u32,
    /// Selected from `rust_type` by the macro; overridable with
    /// `#[myko_field(merge = ...)]`.
    pub merge: MergeStrategy,
    /// Set by `#[myko_field(renamed_from = "old")]`. Gives the merge layer a
    /// read-through chain from the old id to this one.
    pub renamed_from: Option<&'static str>,
}

/// FNV-1a 32-bit hash of a field name.
///
/// Chosen because it is trivial to reimplement identically in every language
/// binding (no seed, no endianness ambiguity), and because collision resistance
/// is irrelevant here: collisions are detected exhaustively at compile time
/// within the only namespace that matters — a single type's field set.
pub const fn field_id(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

/// The namespace a type belongs to: the first segment of `module_path!()`.
///
/// The registration field named `crate_name` actually holds `module_path!()` —
/// a full module path, not a crate name. Namespacing on the raw value would
/// make moving a struct between modules *inside its own crate* a wire-breaking
/// change, so the first segment is what identifies the defining crate.
pub fn namespace_of(module_path: &str) -> &str {
    match module_path.find("::") {
        Some(i) => &module_path[..i],
        None => module_path,
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check --target-dir target/claude -p myko`
Expected: builds successfully. Warnings about unused items are expected — nothing reads them yet.

- [ ] **Step 3: Add unit tests for the two pure functions**

Append to `reflection.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::{field_id, namespace_of};

    #[test]
    fn field_id_is_fnv1a_32() {
        // Canonical FNV-1a 32 test vectors. These same values appear in the
        // conformance suite, so every binding must reproduce them.
        assert_eq!(field_id(""), 0x811c_9dc5);
        assert_eq!(field_id("a"), 0xe40c_292c);
        assert_eq!(field_id("foobar"), 0xbf9c_f968);
    }

    #[test]
    fn field_id_is_order_independent_and_stable() {
        assert_eq!(field_id("name"), field_id("name"));
        assert_ne!(field_id("name"), field_id("description"));
    }

    #[test]
    fn namespace_takes_the_first_segment() {
        assert_eq!(namespace_of("identity::users::model"), "identity");
        assert_eq!(namespace_of("identity"), "identity");
        assert_eq!(namespace_of(""), "");
    }
}
```

Run: `cargo test --target-dir target/claude -p myko reflection -- --nocapture`
Expected: 3 tests pass. **If the FNV vectors fail, stop** — the constants are wrong and every
downstream `field_id` would be, too.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/core/src/core/reflection.rs
git commit -m "feat(core): add FieldSchema, MergeStrategy, field_id, and namespace_of to reflection"
```

---

## Phase 2: Namespace on every registration

This phase is independently valuable — it closes `lv-ea59` — and it is a smaller, lower-risk exercise
of the same "add a field to every registration" mechanic that phase 3 repeats for `fields`.

### Task 2: Add `namespace` to the five registration structs

**Files:** Modify `core/item/traits.rs`, `core/command/registration.rs`,
`core/query/registration.rs`, `core/view/registration.rs`, `core/report/registration.rs`

- [ ] **Step 1: Add the field to each struct**

In each of the five, add directly below the existing `crate_name` field:

```rust
    /// Namespace this type belongs to: the first segment of `crate_name`
    /// (which holds `module_path!()`), or the `namespace = "..."` override.
    /// Precomputed at macro-expansion time.
    pub namespace: &'static str,
```

In `report/registration.rs`, also add below `output_type_crate`:

```rust
    /// Namespace of the output type — first segment of `output_type_crate`.
    pub output_type_namespace: &'static str,
```

In `command/registration.rs`, also add below `result_type_crate`:

```rust
    /// Namespace of the result type — first segment of `result_type_crate`.
    pub result_type_namespace: &'static str,
```

> **This breaks any code constructing these structs literally.** They are constructed only by the
> macros in `libs/myko/macros`, which Task 3 updates in the same commit. If `cargo check` reports a
> literal construction anywhere else, that is a finding worth reporting — not something to patch
> around.

- [ ] **Step 2: Confirm the expected breakage**

Run: `cargo check --target-dir target/claude -p myko`
Expected: **errors** — "missing field `namespace`" at the macro-emitted construction sites. This is
the gate: if it compiles, the field was added somewhere unused.

### Task 3: Emit `namespace` from the macros

**Files:** Modify `libs/myko/macros/src/lib.rs`, `item.rs`, `command.rs`, `query.rs`, `view.rs`,
`report.rs`

- [ ] **Step 1: Add the namespace helper to `macros/src/lib.rs`**

Next to `field_metadata_tokens` (`lib.rs:200`), add:

```rust
/// Tokens computing the namespace at *runtime const-eval* from `module_path!()`,
/// or the literal override when one is given.
///
/// `module_path!()` cannot be split at macro-expansion time — the proc macro
/// does not know its own call-site module path — so the split happens in the
/// expansion, via `myko::reflection::namespace_of`.
pub(crate) fn namespace_tokens(
    override_name: Option<&str>,
    krate: &syn::Path,
) -> proc_macro2::TokenStream {
    match override_name {
        Some(ns) => quote! { #ns },
        None => quote! { #krate::reflection::namespace_of(module_path!()) },
    }
}
```

> `namespace_of` is `fn`, not `const fn`, because `&str` slicing is not yet stable in const context
> for this shape. The registration field is therefore initialised at inventory-collection time, not
> as a compile-time constant. That is fine: `inventory` already runs constructors at startup, and
> nothing here is on a hot path.
>
> **If `namespace_of` is later made `const fn`**, change the field type to a const-evaluated
> expression and nothing else moves.

- [ ] **Step 2: Emit it from `item.rs`**

In `libs/myko/macros/src/item.rs`, in the `item_registration` quote block (currently at `:774`), add
below `crate_name: module_path!(),`:

```rust
            namespace: #namespace_tokens,
```

and bind `namespace_tokens` above the quote block:

```rust
    let namespace_tokens = crate::namespace_tokens(args.namespace.as_deref(), krate);
```

(`args.namespace` is added in Task 5. Until then, pass `None` and revisit — or do Task 5 first.)

- [ ] **Step 3: Emit it from `command.rs`, `query.rs`, `view.rs`, `report.rs`**

Same pattern in each registration `quote!` block. In `command.rs` (`:135`) add both:

```rust
                namespace: #krate::reflection::namespace_of(module_path!()),
                result_type_namespace: #krate::reflection::namespace_of(module_path!()),
```

> **`result_type_namespace` is not always the same as `namespace`.** `result_type_crate` is emitted as
> `module_path!()` today, which is the *command's* module — meaning it is already wrong whenever the
> result type lives in another crate. Emitting the same value preserves exactly today's behaviour and
> does not introduce a new bug. **File a follow-up** rather than fixing it here:
> `levi add "command result_type_crate is the command's module_path, not the result type's" -p p2 -l codegen`

In `report.rs`, `output_type_crate` has the same property — mirror it identically.

- [ ] **Step 4: Verify**

Run: `cargo check --target-dir target/claude -p myko && cargo check --target-dir target/claude --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add libs/myko/core/src/core libs/myko/macros/src
git commit -m "feat(core): derive a namespace on every registration from module_path"
```

### Task 4: Replace codegen's substring filter with namespace equality

**Files:** Modify `libs/myko/core/src/codegen/mod.rs`

- [ ] **Step 1: Replace the eight filters**

`crate_name` there is `CARGO_PKG_NAME` with `-` replaced by `_` (`codegen/mod.rs:149-150`), which is
exactly the first segment of `module_path!()` for that crate. So the substring test becomes equality:

| Line | From | To |
|---|---|---|
| `:158` | `.filter(\|x\| x.crate_name.contains(&crate_name))` | `.filter(\|x\| x.namespace == crate_name)` |
| `:161`, `:164`, `:167`, `:170`, `:284` | same | same |
| `:210` | `.filter(\|r\| r.output_type_crate.contains(&crate_name))` | `.filter(\|r\| r.output_type_namespace == crate_name)` |
| `:218` | `.filter(\|c\| c.result_type_crate.contains(&crate_name) && c.result_type != "()")` | `.filter(\|c\| c.result_type_namespace == crate_name && c.result_type != "()")` |

- [ ] **Step 2: Verify generation output is unchanged for a single-crate case, and narrower for siblings**

Run the existing codegen tests: `cargo test --target-dir target/claude -p myko codegen -- --nocapture`
Expected: pass.

> **This is a behaviour change, and it is the point.** A crate named `myko` previously matched
> registrations from `myko_server`, `myko_leptos`, and any crate whose module path contained `myko`.
> If a downstream crate was silently relying on the over-match to get types generated, its generated
> output will shrink. **Check `cargo flux run gen` output diff on a real consumer before releasing**,
> and note the change in the commit body.

- [ ] **Step 3: Commit and close the levi task**

```bash
git add libs/myko/core/src/codegen/mod.rs
git commit -m "fix(codegen): filter registrations by namespace equality, not module_path substring

The crate filter matched any registration whose module_path *contained* the
consuming crate's name, so a crate named 'myko' pulled in registrations from
'myko_server' and 'myko_leptos'. Registrations now carry a precomputed
namespace (the first module_path segment), compared by equality."
levi close lv-ea59
```

---

## Phase 3: Entity field schemas

### Task 5: Add the `namespace` option to `ItemArgs`

**Files:** Modify `libs/myko/macros/src/item.rs`

- [ ] **Step 1: Extend `ItemArgs`**

`ItemArgs` (`item.rs:13`) currently holds two options and **hard-errors on anything else**
(`:33-37`). It is the single choke point for every `#[myko_item(...)]` option, and forgetting it means
the attribute simply fails to parse.

```rust
pub struct ItemArgs {
    pub ingest_buffer_ms: Option<u64>,
    pub post_deserialize: Option<ExprPath>,
    /// Overrides the namespace derived from `module_path!()`, so a crate
    /// rename or cross-crate move is a refactor rather than a forced
    /// data migration.
    pub namespace: Option<String>,
}
```

In the `Parse` impl, before the `else` that errors, add:

```rust
            } else if ident == "namespace" {
                let value: syn::LitStr = input.parse()?;
                args.namespace = Some(value.value());
```

- [ ] **Step 2: Wire it into the `namespace_tokens` call from Task 3 Step 2**

- [ ] **Step 3: Add a test entity using the override**

In an existing macro-integration test module, declare an entity with
`#[myko_item(namespace = "custom_ns")]` and assert its registration's `namespace` is `"custom_ns"`.

- [ ] **Step 4: Verify and commit**

```bash
cargo test --target-dir target/claude -p myko
git add libs/myko/macros/src/item.rs
git commit -m "feat(macros): add namespace override to myko_item"
```

### Task 6: The `#[myko_field]` attribute

**Files:** Create `libs/myko/macros/src/field_attr.rs`; modify `libs/myko/macros/src/lib.rs`,
`item.rs`

- [ ] **Step 1: Create `field_attr.rs`**

Mirror the shape of the existing `setter::collect_setter_fields` / `strip_setter_attrs` pair, which
`item.rs` already calls at `:76` and `:110` — the collect-before-strip ordering is load-bearing and
already established.

```rust
//! `#[myko_field(...)]` — per-field mesh metadata.
//!
//! Two options, both affecting how the field merges across the mesh:
//!   merge        — override the strategy inferred from the declared type
//!   renamed_from — record the field's previous name, so the merge layer has a
//!                  read-through chain from the old field_id to this one

use std::collections::HashMap;
use syn::{Field, ItemStruct};

#[derive(Default, Clone)]
pub struct FieldAttrs {
    /// `"Lww" | "PnCounter" | "OrSet" | "LwwMap"` — validated at parse time.
    pub merge: Option<String>,
    pub renamed_from: Option<String>,
}

/// Collect per-field attrs, keyed by field name. Call BEFORE stripping.
pub fn collect_field_attrs(input: &ItemStruct) -> HashMap<String, FieldAttrs> { /* … */ }

/// Remove `#[myko_field(...)]` so it does not reach the emitted struct.
pub fn strip_field_attrs(field: &mut Field) { /* … */ }
```

An unrecognised option, or a `merge` value outside the four variant names, must be a
`syn::Error` at the attribute's span — not a silent default.

- [ ] **Step 2: Register the module and call it from `item.rs`**

`mod field_attr;` in `macros/src/lib.rs`. In `myko_item_impl`, collect **before** the existing
attribute-stripping loop (which begins at `:107`), and strip **inside** it, next to
`setter::strip_setter_attrs(field);`.

- [ ] **Step 3: Verify an entity with `#[myko_field]` compiles and the attribute does not leak**

Expected: `#[myko_field]` on a field compiles; a bogus option is a compile error naming the option.

- [ ] **Step 4: Commit**

```bash
git add libs/myko/macros/src/field_attr.rs libs/myko/macros/src/lib.rs libs/myko/macros/src/item.rs
git commit -m "feat(macros): add myko_field attribute for merge strategy and rename chain"
```

### Task 7: Emit `FieldSchema` from `#[myko_item]`

**Files:** Modify `libs/myko/macros/src/lib.rs`, `item.rs`; modify `core/item/traits.rs`

- [ ] **Step 1: Add `fields` to `ItemRegistration`**

In `libs/myko/core/src/core/item/traits.rs:79`, below `namespace`:

```rust
    /// Entity's own fields, captured at macro-expansion time. Source for the
    /// mesh manifest's entity schema, per-field merge strategy selection, and
    /// field_id assignment.
    pub fields: &'static [crate::reflection::FieldSchema],
```

- [ ] **Step 2: Add `entity_field_schema_tokens` to `macros/src/lib.rs`**

Next to `field_metadata_tokens`. It differs in four ways: it computes `field_id`, checks for
collisions, infers the merge strategy, and threads `renamed_from`.

```rust
pub(crate) fn entity_field_schema_tokens(
    fields: &[(syn::Ident, syn::Type)],
    attrs: &HashMap<String, crate::field_attr::FieldAttrs>,
    krate: &syn::Path,
) -> syn::Result<proc_macro2::TokenStream> {
    let mut seen: HashMap<u32, String> = HashMap::new();
    let mut entries = Vec::new();

    for (ident, ty) in fields {
        let name = ident.to_string();
        let id = const_field_id(&name);

        // TI-14: a collision inside one type is a compile error naming both fields.
        if let Some(other) = seen.insert(id, name.clone()) {
            return Err(syn::Error::new(
                ident.span(),
                format!(
                    "field_id collision: `{name}` and `{other}` both hash to {id:#010x}. \
                     Rename one of them (and add #[myko_field(renamed_from = \"...\")])."
                ),
            ));
        }

        let field_attrs = attrs.get(&name).cloned().unwrap_or_default();
        let strategy = field_attrs
            .merge
            .clone()
            .unwrap_or_else(|| infer_merge_strategy(ty));
        let strategy_ident = format_ident!("{}", strategy);
        let rust_type = quote!(#ty).to_string();
        let optional = is_option_type(ty);
        let renamed = match &field_attrs.renamed_from {
            Some(old) => quote!(Some(#old)),
            None => quote!(None),
        };

        entries.push(quote! {
            #krate::reflection::FieldSchema {
                name: #name,
                rust_type: #rust_type,
                optional: #optional,
                field_id: #id,
                merge: #krate::reflection::MergeStrategy::#strategy_ident,
                renamed_from: #renamed,
            }
        });
    }

    Ok(quote! { &[ #(#entries),* ] })
}

/// Compile-time twin of `myko::reflection::field_id`. **These two must stay
/// byte-identical** — the macro stamps ids the runtime never recomputes, so a
/// divergence would be invisible until two nodes disagreed on the wire.
fn const_field_id(name: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in name.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}
```

- [ ] **Step 3: Add `infer_merge_strategy`**

Selection is by the **outermost** type constructor, with `Option<T>` unwrapped first (a `None` is a
field tombstone, not a different strategy).

```rust
fn infer_merge_strategy(ty: &syn::Type) -> String {
    let inner = unwrap_option(ty).unwrap_or(ty);
    match outer_ident(inner).as_deref() {
        Some("Counter")                                => "PnCounter",
        Some("HashSet") | Some("BTreeSet") | Some("Set") => "OrSet",
        Some("HashMap") | Some("BTreeMap")              => "LwwMap",
        // Vec is LWW deliberately: treating it as a set loses order and
        // duplicates, and treating it as a sequence needs a sequence CRDT,
        // which is out of scope for this design.
        _                                               => "Lww",
    }
    .to_string()
}
```

`unwrap_option` already exists in `item.rs` (`:50`, documented for the advanced-query filter codegen);
reuse it rather than writing a second one.

- [ ] **Step 4: Call it from `item.rs`**

`filter_fields: Vec<(syn::Ident, syn::Type)>` is already snapshotted at `item.rs:~120` — "every field,
**including the `id` field just pushed**". That is exactly the list the schema needs, so no second
traversal is required.

Bind above the `item_registration` quote and propagate the `syn::Result` (return
`e.to_compile_error().into()` on error, matching how the rest of the macro surfaces errors):

```rust
    let field_schema_tokens = match crate::entity_field_schema_tokens(&filter_fields, &field_attrs, krate) {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };
```

and add to the registration:

```rust
            fields: #field_schema_tokens,
```

- [ ] **Step 5: Verify**

Run: `cargo check --target-dir target/claude --workspace`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add libs/myko/core/src/core/item/traits.rs libs/myko/macros/src
git commit -m "feat(macros): emit per-field schemas with field_id and merge strategy on ItemRegistration"
```

---

## Phase 4: Tests and the no-behaviour-change gate

### Task 8: Schema-completeness and strategy-selection tests

**Files:** Create `libs/myko/core/src/core/reflection_tests.rs`; modify `core/mod.rs`

- [ ] **Step 1: Write the tests against real macro-expanded entities**

Per the repository's testing rule, use real entities with macros — not hand-built registrations.
`libs/myko/core/src/bench_entities.rs` (behind the `bench` feature) and the existing test entities in
`core/` are the candidates; prefer whichever is already compiled into the default test build.

```rust
//! Field-schema tests. These assert the macro's output against the struct
//! definitions it was expanded from, so a field added without a schema entry
//! fails here rather than silently producing an incomplete manifest.

#[test]
fn every_item_registration_has_a_field_schema() {
    for reg in inventory::iter::<ItemRegistration> {
        assert!(
            !reg.fields.is_empty(),
            "{} has an empty field schema — every entity has at least `id`",
            reg.entity_type
        );
        assert!(
            reg.fields.iter().any(|f| f.name == "id"),
            "{} is missing the `id` field the macro appends",
            reg.entity_type
        );
    }
}

#[test]
fn field_ids_are_unique_within_every_type() {
    for reg in inventory::iter::<ItemRegistration> {
        let mut seen = std::collections::HashMap::new();
        for f in reg.fields {
            if let Some(prev) = seen.insert(f.field_id, f.name) {
                panic!("{}: {} and {} share field_id {:#010x}", reg.entity_type, prev, f.name, f.field_id);
            }
        }
    }
}

#[test]
fn field_ids_match_the_runtime_hash() {
    // The macro stamps ids the runtime never recomputes. If the two
    // implementations ever diverge, this is the only place it shows up.
    for reg in inventory::iter::<ItemRegistration> {
        for f in reg.fields {
            assert_eq!(f.field_id, myko::reflection::field_id(f.name), "{}.{}", reg.entity_type, f.name);
        }
    }
}

#[test]
fn merge_strategy_follows_the_declared_type() {
    // Assert against a known entity rather than every entity, so the test
    // states the rule instead of restating the codebase.
    let reg = lookup_item_registration("BenchTarget").expect("bench entity registered");
    let by_name = |n: &str| reg.fields.iter().find(|f| f.name == n).unwrap();
    assert_eq!(by_name("name").merge, MergeStrategy::Lww);
    assert_eq!(by_name("id").merge, MergeStrategy::Lww);
    // Extend with an entity carrying a set/map/counter field once one exists.
}

#[test]
fn namespace_is_the_first_module_path_segment() {
    for reg in inventory::iter::<ItemRegistration> {
        assert!(!reg.namespace.contains("::"), "{}: namespace {} is a module path", reg.entity_type, reg.namespace);
        assert!(reg.crate_name.starts_with(reg.namespace) || reg.namespace == "myko",
            "{}: namespace {} is not a prefix of crate_name {}", reg.entity_type, reg.namespace, reg.crate_name);
    }
}
```

> The `namespace == "myko"` escape in the last test covers the override case. If a test entity uses
> `#[myko_item(namespace = "custom_ns")]` (Task 5 Step 3), widen the assertion to skip entities whose
> namespace is not a prefix **and** whose registration used an override — or drop the prefix check and
> keep only the "no `::`" assertion, which is the invariant that actually matters.

- [ ] **Step 2: Add a set/map/counter entity if none exists**

`merge_strategy_follows_the_declared_type` is worthless if every entity in the tree is all scalars.
Add a test-only entity with `BTreeSet<String>`, `BTreeMap<String, String>`, and a `Vec<String>` field
and assert `OrSet`, `LwwMap`, and **`Lww`** respectively — the `Vec` case is the one most likely to be
"fixed" into `OrSet` by someone later.

- [ ] **Step 3: Run and commit**

```bash
cargo test --target-dir target/claude -p myko reflection -- --nocapture
git add libs/myko/core/src/core/reflection_tests.rs libs/myko/core/src/core/mod.rs
git commit -m "test(core): assert entity field schemas, field_id uniqueness, and strategy selection"
```

### Task 9: Compile-fail test for field-id collisions

**Files:** Create `libs/myko/core/tests/compile_fail.rs`,
`tests/compile_fail/field_id_collision.rs`, `tests/compile_fail/field_id_collision.stderr`;
modify `libs/myko/core/Cargo.toml`

- [ ] **Step 1: Add `trybuild` as a dev-dependency**

Workspace root `Cargo.toml`, in `[workspace.dependencies]`:

```toml
trybuild = "1.0"
```

`libs/myko/core/Cargo.toml`, under `[dev-dependencies]`:

```toml
trybuild.workspace = true
```

- [ ] **Step 2: Find an actual FNV-1a 32 collision pair**

Do **not** invent one. Write a throwaway binary that brute-forces short lowercase identifiers until
two hash equal, and use that pair:

```bash
cargo run --target-dir target/claude --example find_fnv_collision
```

If no short pair is findable in reasonable time, **change the test's shape rather than fabricating
one**: assert the collision path with a unit test that calls `entity_field_schema_tokens` directly
with two synthesised same-id fields, and drop the `trybuild` fixture. Note which route was taken in
the commit body.

- [ ] **Step 3: Write the fixture and runner, generate the `.stderr`**

```bash
TRYBUILD=overwrite cargo test --target-dir target/claude -p myko compile_fail
```

Then **read the generated `.stderr`** and confirm it names both colliding fields and suggests
`renamed_from` — the whole point of the error is that it is actionable.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml libs/myko/core/Cargo.toml libs/myko/core/tests
git commit -m "test(macros): compile-fail coverage for field_id collisions within a type"
```

### Task 10: No-behaviour-change gate

- [ ] **Step 1: Full workspace check and clippy**

Check `.bacon-locations` for outstanding clippy errors **before** running clippy yourself, and fix in
order — later errors are often resolved by fixing the first.

```bash
cargo check --target-dir target/claude --workspace
cargo clippy --target-dir target/claude --workspace -- -D warnings
cargo fmt --check
```

- [ ] **Step 2: Full test sweep**

```bash
cargo test --target-dir target/claude --workspace -- --nocapture
```

Expected: everything that passed before this plan still passes. **Phase 1 changes no runtime
behaviour** — the only intended behavioural difference in the whole plan is codegen's narrower crate
filter (Task 4).

- [ ] **Step 3: Verify codegen output on a real consumer**

Do not skip this because the tests pass. Task 4 narrowed a filter that consumers may have been
relying on.

Ask the user to run type generation in their hot-reload session and diff the generated TypeScript.
**Do not run `cargo flux run gen` yourself** — the user runs code and type generation.

- [ ] **Step 4: Confirm the exit criteria**

From the roadmap:

- [ ] Every `#[myko_item]` type emits a complete field schema (Task 8).
- [ ] A field-name collision within a type is a compile error naming both fields (Task 9).
- [ ] `namespace` equality replaces the substring filter; `lv-ea59` closed (Task 4).
- [ ] No behavioural change: `cargo test` and the rship build both pass unmodified.

---

## What this phase deliberately does not do

- **Nothing reads `FieldSchema` yet.** The manifest (phase 6) and the record encoder (phase 3) are its
  consumers. Adding a consumer here would couple this additive change to a wire change.
- **No `edge_owned`, `routing_key`, or `scoped_by`.** They belong to phases 10 and 5 respectively.
  Adding attribute options ahead of the code that honours them creates attributes that silently do
  nothing.
- **No `Counter<T>` newtype.** `infer_merge_strategy` matches the name so the mapping is complete and
  testable, but the type itself ships with the CRDT implementations in phase 3.
