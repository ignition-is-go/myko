//! Per-type field filters for advanced (array-valued / `IN`) query matching.
//!
//! See `docs/superpowers/specs/2026-07-13-advanced-query-design.md` for the
//! full design. Each data type exposes exactly the filter operations that
//! are meaningful for it — ids are always exact (`Eq`/`In`), numbers add
//! `Range`, strings add substring/prefix matching, bools are bare equality.
//! Selection is driven by the [`Filterable`] trait's associated type, so
//! `#[myko_item]` never has to sniff field types syntactically.

use std::{collections::HashMap, hash::Hash, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{TS, common::with_id::WithId, core::item::Eventable};

/// A filter that can test whether a value of type `T` matches.
pub trait Filter<T> {
    fn matches(&self, value: &T) -> bool;
}

/// Normalizes a filter to its canonical form — required for query-cache
///
/// identity (see spec §1): sorted+deduped `In`, `In([x])` -> `Eq(x)`,
/// `Range{a,a}` -> `Eq(a)`. Implemented uniformly across every filter type,
/// including `bool` (a no-op — bare equality has nothing to canonicalize),
/// so `#[myko_item]`'s generated `XQuery::canonicalize` can canonicalize
/// every field the same way regardless of which filter type it holds.
pub trait CanonicalFilter: Sized {
    #[must_use]
    fn canonicalize(self) -> Self;
}

/// Associates a type with the filter type that can express queries over it.
///
/// Implemented for every field type `#[myko_item]` can generate a filter
/// for — numeric primitives, `String`/`Arc<str>`, `bool`, entity id
/// newtypes, and (via a per-type impl or macro-emitted default) opaque/enum
/// types through [`EqFilter`].
///
/// Deliberately no `Filter<Self>` bound on the associated type: the
/// `Option<T>` impl below sets `type Filter = T::Filter`, which implements
/// `Filter<T>`, not `Filter<Option<T>>` — the macro-generated `matches`
/// code unwraps the `Option` itself (a `None` field never matches, see
/// that impl's doc comment) rather than dispatching through `Filter`
/// uniformly, so this trait only needs to name the right filter *type*.
pub trait Filterable: Sized {
    type Filter;
}

/// Ordered foreign-key values for a `BelongsToSourceIndex` bucket. A single
///
/// `#[belongs_to]` field yields a 1-element key (the pre-compound-routing
/// shape); an entity with 2+ `#[belongs_to]` fields queried with more than
/// one set at once yields one element per field that was `Some`, in the
/// entity's declared field order. Two queries that populate different SETS
/// of `belongs_to` fields land in different buckets even for the same entity
/// type — the bucket only ever holds items matching every field in the key.
///
/// `SmallVec` with inline capacity 2: keys are built per item per diff and
/// cloned on every routing decision / key-set diff, and virtually all
/// entities have 1–2 `#[belongs_to]` fields — inline storage makes those
/// clones allocation-free.
pub type CompoundKey = smallvec::SmallVec<[Arc<str>; 2]>;

/// Extracts the compound foreign-key values (see [`CompoundKey`]) an item
///
/// contributes for one specific field combination. Position `i` in the
/// returned key corresponds to field `i` of that combination. Returns
/// `None` only on a downcast failure (item is the wrong entity type) —
/// `#[belongs_to]` fields feeding this are all non-optional, so a
/// correctly-typed item always has a value for every field in play.
///
/// Deliberately a distinct type from
/// [`crate::core::relationship::FkExtractor`]: that alias backs
/// `RelationshipManager`'s own single-field child-index tracking and
/// `export_tree`'s traversal, both unrelated to query routing — widening it
/// in place would ripple into those unrelated subsystems for no reason.
pub type CompoundFkExtractor = fn(&dyn std::any::Any) -> Option<CompoundKey>;

/// A `belongs_to` routing decision for one `XQuery` instance — which fields
///
/// are pinned, the compound keys to union-route through
/// `BelongsToSourceIndex`, and the extractor needed to build/maintain that
/// index. Returned by a generated `XQuery`'s `belongs_to_route()` method
/// (see `#[myko_item]`'s codegen); shared between the value-based query's
/// `build_view` (routes once per query construction) and `query_live`'s
/// incremental per-tick diffing (routes on every filter change, diffing
/// keys against the previous tick's route) — one routing rule, two
/// consumers, so they can never disagree on what's index-servable.
///
/// A plain data type deliberately kept here in the ungated `filter` module
/// rather than the native-only `registration` module: [`LiveFilterQuery`]
/// (below) needs to name it in a trait bound that must resolve on wasm32
/// too, even though the actual `query_live` engine that consumes it stays
/// server-only.
pub struct BelongsToRoute {
    pub field_names: &'static [&'static str],
    pub keys: Vec<CompoundKey>,
    pub extract_fk: CompoundFkExtractor,
}

/// Sentinel `field_names` for the primary-key route, so `query_live`'s
///
/// route-shape tracking (`prev_route_field_names`) distinguishes id mode
/// from every `belongs_to` field combination (a real field named "(id)" is
/// impossible — parentheses aren't valid in a Rust identifier).
pub const ID_ROUTE_FIELD_NAMES: &[&str] = &["(id)"];

/// The full routing decision for one `XQuery` instance, in priority order:
///
/// - [`QueryRoute::Ids`] — the filter pins `id` (`Eq`/`In`). The primary
///   key is the strongest index in the system (the store itself is an
///   id-keyed map of per-key cells), and an id pin bounds the result to
///   ≤ N rows outright, so it wins over any `belongs_to` combination also
///   present — those fields still narrow via `matches` on the ≤ N rows.
/// - [`QueryRoute::BelongsTo`] — no id pin; route on `#[belongs_to]`
///   buckets as before.
///
/// Returned by a generated `XQuery`'s `query_route()` method; `None` from
/// that method means scan mode. Same one-rule/two-consumers contract as
/// [`BelongsToRoute`]: `build_view` and `query_live` both dispatch on this.
pub enum QueryRoute {
    /// Primary-key route: subscribe the store's per-id cells for exactly
    /// these ids (empty = `In([])` = matches nothing, an empty result —
    /// NOT a fall-through to scan).
    Ids(Vec<Arc<str>>),
    BelongsTo(BelongsToRoute),
}

/// Bridges a generated `XQuery` type to its entity, so `query_live` can be
///
/// generic over just `impl Watchable<F>` — no `GetXsByQuery` wrapper
/// needed; "the entity/query type is inferred from `Cell<XQuery>`" (spec
/// §5). Implemented by `#[myko_item]`'s codegen for every generated
/// `XQuery`, delegating to that type's own already-generated `matches`/
/// `belongs_to_route` methods.
///
/// The trait declaration lives in this ungated module so it resolves on
/// wasm32 (needed for `ReportContext::query_live`'s wasm32 stub signature,
/// which must type-check even though its body just panics) — only the
/// `query_live` engine itself (`registration.rs`) is native-only.
pub trait LiveFilterQuery: Clone + PartialEq + Send + Sync + 'static {
    type Item: Eventable
        + WithId
        + DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static;

    fn entity_type() -> &'static str;
    fn matches(&self, item: &Self::Item) -> bool;
    /// The full routing decision (id → `belongs_to` → scan); see
    /// [`QueryRoute`]. `None` means scan mode.
    fn query_route(&self) -> Option<QueryRoute>;
}

/// Sort + dedup an `In` value set. Order doesn't affect matching, only
/// query-cache identity, so two callers passing the same set in different
/// orders (or with duplicates) must produce the same canonical filter.
fn canonical_in_values<T: Ord + Clone>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values.dedup();
    values
}

/// Same as [`canonical_in_values`], but for `PartialOrd`-only types
/// (floats) that don't implement `Ord` because of `NaN`. Sorts via
/// `partial_cmp` with `Equal` as the NaN fallback — deterministic for the
/// non-NaN elements, which is what canonicalization actually needs; `NaN`
/// in a filter value set is already a degenerate case no ordering can fix.
fn canonical_in_values_partial<T: PartialOrd + PartialEq + Clone>(mut values: Vec<T>) -> Vec<T> {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    values.dedup_by(|a, b| a == b);
    values
}

// ─────────────────────────────────────────────────────────────────────────
// IdFilter — ids are ALWAYS exact match (single or set), never partial or
// ranged. This is what makes every filter on a #[belongs_to] field
// index-servable by construction (see registration.rs's union-of-buckets
// routing): there is no IdFilter variant that requires a table scan.
// ─────────────────────────────────────────────────────────────────────────

// Wire (see `wire_serde` below): `Eq` is the bare value — identical to the
// pre-6.0 `PartialX { field: value }` shape, so old payloads stay compatible
// — and every other operator is a `$`-sigilled object. `$` can't collide with
// a bare value: field/variant names are Rust identifiers, never `$`-prefixed.
#[derive(Debug, Clone, PartialEq, Eq, TS)]
// `bound` is required because `#[ts(type = …)]` on a generic drops the
// auto-added `T: TS` bound but still emits it internally (ts-rs 11); it's a
// no-op without the `typegen-typescript` feature (myko's `TsNoop` claims the attr).
#[ts(type = "T | { \"$in\": Array<T> }", bound = "T: ts_rs::TS")]
pub enum IdFilter<T> {
    Eq(T),
    In(Vec<T>),
}

impl<T: Clone> IdFilter<T> {
    /// The set of ids this filter matches, if finite and enumerable without
    /// touching the store — always true for `IdFilter`, since it never
    /// contains a scan-only variant. Used to route `In` through
    /// `BelongsToSourceIndex` as a bucket union instead of a table scan.
    pub fn key_values(&self) -> Vec<T> {
        match self {
            Self::Eq(value) => vec![value.clone()],
            Self::In(values) => values.clone(),
        }
    }
}

impl<T: Ord + Clone> CanonicalFilter for IdFilter<T> {
    /// Sort + dedup `In`, collapse a 1-element `In` to `Eq`.
    fn canonicalize(self) -> Self {
        match self {
            Self::In(values) => {
                let values = canonical_in_values(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => Self::Eq(only),
                    Err(values) => Self::In(values),
                }
            }
            Self::Eq(value) => Self::Eq(value),
        }
    }
}

impl<T: Eq + Hash> Filter<T> for IdFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            Self::Eq(expected) => value == expected,
            // In([]) matches nothing — this is correct behavior for
            // "scope to this (possibly empty) derived set" call sites, not
            // a bug. Document loudly (see spec §1) rather than special-case.
            Self::In(values) => in_matches(values, value),
        }
    }
}

impl<T> From<T> for IdFilter<T> {
    fn from(value: T) -> Self {
        Self::Eq(value)
    }
}

impl<T> From<Vec<T>> for IdFilter<T> {
    fn from(values: Vec<T>) -> Self {
        Self::In(values)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// NumericFilter — Eq / In / Range (inclusive bounds).
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(
    type = "T | { \"$in\": Array<T> } | { \"$range\": { min?: T, max?: T } }",
    bound = "T: ts_rs::TS"
)]
pub enum NumericFilter<T> {
    Eq(T),
    In(Vec<T>),
    /// Inclusive bounds; both `None` is invalid (rejected at construction
    /// time is future work — for now it degenerates to "match everything,"
    /// same as an unset filter, since myko trusts callers not to write it).
    Range {
        min: Option<T>,
        max: Option<T>,
    },
}

impl<T: PartialOrd + PartialEq + Clone> CanonicalFilter for NumericFilter<T> {
    /// Sort + dedup `In`, collapse 1-element `In` to `Eq`, collapse
    /// `Range{min: Some(a), max: Some(a)}` to `Eq(a)`.
    fn canonicalize(self) -> Self {
        match self {
            Self::In(values) => {
                let values = canonical_in_values_partial(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => Self::Eq(only),
                    Err(values) => Self::In(values),
                }
            }
            Self::Range {
                min: Some(a),
                max: Some(b),
            } if a == b => Self::Eq(a),
            Self::Eq(value) => Self::Eq(value),
            range @ Self::Range { .. } => range,
        }
    }
}

impl<T: PartialEq + PartialOrd> Filter<T> for NumericFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            Self::Eq(expected) => value == expected,
            Self::In(values) => values.iter().any(|v| v == value),
            Self::Range { min, max } => {
                min.as_ref().is_none_or(|min| value >= min)
                    && max.as_ref().is_none_or(|max| value <= max)
            }
        }
    }
}

impl<T> From<T> for NumericFilter<T> {
    fn from(value: T) -> Self {
        Self::Eq(value)
    }
}

impl<T> From<Vec<T>> for NumericFilter<T> {
    fn from(values: Vec<T>) -> Self {
        Self::In(values)
    }
}

macro_rules! impl_numeric_filterable {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Filterable for $ty {
                type Filter = NumericFilter<$ty>;
            }
        )+
    };
}

impl_numeric_filterable!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64
);

// OrderedFloat<T> is a foreign (ordered_float crate) newtype, so orphan
// rules block downstream crates from ever implementing Filterable for it
// themselves — it has to live here, same reasoning as the container
// blanket impls below. Unlike those, it's genuinely numeric (that's the
// whole point of the newtype: giving floats a total order), so NumericFilter
// — including Range — is the right treatment, not Unfilterable. Only f32/f64
// get impls, matching ts-rs's own `ordered-float-impl` feature (which only
// implements TS for those two concrete instantiations, not generic T).
#[cfg(feature = "ordered-float")]
impl_numeric_filterable!(
    ordered_float::OrderedFloat<f32>,
    ordered_float::OrderedFloat<f64>
);

// ─────────────────────────────────────────────────────────────────────────
// StringFilter — Eq / In / Contains / StartsWith. Never Range (meaningless
// for strings without a locale-aware ordering myko doesn't want to own).
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(
    type = "string | { \"$in\": Array<string> } | { \"$contains\": string } | { \"$startsWith\": string }"
)]
pub enum StringFilter {
    Eq(Arc<str>),
    In(Vec<Arc<str>>),
    /// Substring, case-sensitive.
    Contains(Arc<str>),
    StartsWith(Arc<str>),
}

impl CanonicalFilter for StringFilter {
    /// Sort + dedup `In`, collapse a 1-element `In` to `Eq`.
    fn canonicalize(self) -> Self {
        match self {
            Self::In(values) => {
                let values = canonical_in_values(values);
                match <[Arc<str>; 1]>::try_from(values) {
                    Ok([only]) => Self::Eq(only),
                    Err(values) => Self::In(values),
                }
            }
            Self::Eq(value) => Self::Eq(value),
            Self::Contains(value) => Self::Contains(value),
            Self::StartsWith(value) => Self::StartsWith(value),
        }
    }
}

impl Filter<Arc<str>> for StringFilter {
    fn matches(&self, value: &Arc<str>) -> bool {
        match self {
            Self::Eq(expected) => value == expected,
            Self::In(values) => in_matches(values, value),
            Self::Contains(needle) => value.contains(needle.as_ref()),
            Self::StartsWith(prefix) => value.starts_with(prefix.as_ref()),
        }
    }
}

// Support String-typed entity fields (not just Arc<str>) matching against
// the same Arc<str>-based filter — avoids a parallel StringFilter<String>.
impl Filter<String> for StringFilter {
    fn matches(&self, value: &String) -> bool {
        match self {
            Self::Eq(expected) => value.as_str() == expected.as_ref(),
            Self::In(values) => values.iter().any(|v| v.as_ref() == value.as_str()),
            Self::Contains(needle) => value.contains(needle.as_ref()),
            Self::StartsWith(prefix) => value.starts_with(prefix.as_ref()),
        }
    }
}

impl From<Arc<str>> for StringFilter {
    fn from(value: Arc<str>) -> Self {
        Self::Eq(value)
    }
}

impl From<Vec<Arc<str>>> for StringFilter {
    fn from(values: Vec<Arc<str>>) -> Self {
        Self::In(values)
    }
}

// String/&str conveniences so call sites migrating off `PartialX { field:
// Some(string) }` can stay `Some(string.into())` regardless of whether the
// entity field is `String` or `Arc<str>`.
impl From<String> for StringFilter {
    fn from(value: String) -> Self {
        Self::Eq(Arc::from(value))
    }
}

impl From<&str> for StringFilter {
    fn from(value: &str) -> Self {
        Self::Eq(Arc::from(value))
    }
}

impl From<Vec<String>> for StringFilter {
    fn from(values: Vec<String>) -> Self {
        Self::In(values.into_iter().map(Arc::from).collect())
    }
}

impl From<Vec<&str>> for StringFilter {
    fn from(values: Vec<&str>) -> Self {
        Self::In(values.into_iter().map(Arc::from).collect())
    }
}

impl Filterable for Arc<str> {
    type Filter = StringFilter;
}

impl Filterable for String {
    type Filter = StringFilter;
}

/// An optional entity field (`Option<T>`) is filtered by `T`'s own filter
/// type — the macro-generated `matches` impl special-cases `None` (never
/// matches, regardless of the filter) rather than needing an `Option`-aware
/// filter type. This impl exists purely so `<Option<T> as Filterable>::Filter`
/// resolves to the same type as `<T as Filterable>::Filter` for the
/// generated `XQuery` struct's field declarations.
impl<T: Filterable> Filterable for Option<T> {
    type Filter = T::Filter;
}

// ─────────────────────────────────────────────────────────────────────────
// bool — bare equality. In(Vec<bool>) would always reduce to Eq or
// match-everything, so it doesn't exist as a variant; `bool` itself is its
// own filter type.
// ─────────────────────────────────────────────────────────────────────────

impl Filter<Self> for bool {
    fn matches(&self, value: &Self) -> bool {
        self == value
    }
}

impl Filterable for bool {
    type Filter = Self;
}

impl CanonicalFilter for bool {
    /// No-op — bare equality has nothing to canonicalize.
    fn canonicalize(self) -> Self {
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────
// EqFilter — fallback for enums and other exact-only opaque types: Eq plus
// set membership, no substring/range operations.
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(type = "T | { \"$in\": Array<T> }", bound = "T: ts_rs::TS")]
pub enum EqFilter<T> {
    Eq(T),
    In(Vec<T>),
}

impl<T: Ord + Clone> CanonicalFilter for EqFilter<T> {
    /// Sort + dedup `In`, collapse a 1-element `In` to `Eq`.
    fn canonicalize(self) -> Self {
        match self {
            Self::In(values) => {
                let values = canonical_in_values(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => Self::Eq(only),
                    Err(values) => Self::In(values),
                }
            }
            Self::Eq(value) => Self::Eq(value),
        }
    }
}

impl<T: PartialEq> Filter<T> for EqFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            Self::Eq(expected) => value == expected,
            Self::In(values) => values.iter().any(|v| v == value),
        }
    }
}

impl<T> From<T> for EqFilter<T> {
    fn from(value: T) -> Self {
        Self::Eq(value)
    }
}

impl<T> From<Vec<T>> for EqFilter<T> {
    fn from(values: Vec<T>) -> Self {
        Self::In(values)
    }
}

/// Marks `$ty` filterable via [`EqFilter`] (`Eq`/`In`, no partial/range
///
/// operations) — the fallback for enums and other exact-only opaque types
/// per spec §1. For a downstream crate's own entity-field enums (state
/// machines, tagged values, etc.) that want the same treatment
/// `#[myko_item]`'s generated id/numeric/string fields already get
/// automatically via a built-in `Filterable` impl.
///
/// `$ty` must already satisfy `Debug + Clone + PartialEq + Eq + Ord +
/// Serialize + Deserialize + TS` (`ts_rs`'s `TS`, or `myko::TS`'s no-op form
/// when the `typegen-typescript` feature is off) — this macro only wires up the
/// `Filterable` impl, it doesn't derive anything on `$ty` itself.
#[macro_export]
macro_rules! impl_filterable_eq {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl $crate::query::Filterable for $ty {
                type Filter = $crate::query::EqFilter<$ty>;
            }
        )+
    };
}

/// Marks `$ty` filterable via [`Unfilterable`] — the escape hatch for a
///
/// downstream crate's own opaque payload structs (arbitrary nested data with
/// no query-meaningful equality, the same shape `serde_json::Value` needed
/// [`Unfilterable`] for above). Unlike [`impl_filterable_eq!`], `$ty` need
/// not implement `Eq`/`Ord`/`Hash` — `Unfilterable`'s `Filter`/`CanonicalFilter`
/// impls are unconditional over any `T`, so this macro only needs `$ty` to
/// exist as a type; the generated `XQuery` field is structurally always
/// `None`, same as any other `Unfilterable` field.
#[macro_export]
macro_rules! impl_filterable_opaque {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl $crate::query::Filterable for $ty {
                type Filter = $crate::query::Unfilterable;
            }
        )+
    };
}

/// Membership test for `In` filters — a plain linear scan, deliberately.
///
/// The spec-§4 "build a `HashSet` above a length threshold" approach was
/// tried and is strictly worse here: `matches` gets one membership question
/// per call, so building the set (an allocation plus hashing all n values)
/// always costs more than n direct comparisons. A set/binary-search only
/// wins if it can be amortized across calls, which requires either caching
/// alongside the filter (impossible in the serde/TS-mirrored enum) or a
/// canonicalized-at-intake guarantee that match-path filters are sorted —
/// neither holds today. TODO(ts): revisit with binary search if
/// canonicalization is ever enforced at query intake.
pub fn in_matches<T: PartialEq>(values: &[T], value: &T) -> bool {
    values.iter().any(|v| v == value)
}

// ─────────────────────────────────────────────────────────────────────────
// Unfilterable — the escape hatch for genuinely opaque payload fields
// (e.g. serde_json::Value), where the spec's own principle ("each type's
// filter exposes exactly the operations meaningful for it, and nothing
// more") means the right operation set is NONE. A degenerate EqFilter<T>
// doesn't work here either: an opaque JSON blob has no meaningful equality
// as a query predicate, and it doesn't implement Ord, which In's
// canonicalization sort needs. Uninhabited (zero variants), so
// `Option<Unfilterable>` — the field type #[myko_item] generates for a
// Filterable::Filter = Unfilterable field — can only ever be `None`: the
// field is structurally unfilterable while the containing XQuery still
// compiles, derives, and (de)serializes normally.
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum Unfilterable {}

impl<T> Filter<T> for Unfilterable {
    fn matches(&self, _value: &T) -> bool {
        false
    }
}

impl CanonicalFilter for Unfilterable {
    fn canonicalize(self) -> Self {
        match self {}
    }
}

impl Filterable for serde_json::Value {
    type Filter = Unfilterable;
}

// Container blanket impls — `Vec<T>`, `HashMap<K, V>`, and small tuples are
// foreign types (from `std`), so a downstream crate can never implement our
// local `Filterable` trait for them itself (orphan rule); these have to
// live here or fields of these shapes can never appear on an entity at all.
// Unfilterable is the only sound choice for the same reason `serde_json::Value`
// gets it above: "list/map of T" and "tuple of heterogeneous fields" have no
// query-meaningful equality/ordering as a single field-level predicate,
// independent of what `T`/`K`/`V` themselves support.
impl<T> Filterable for Vec<T> {
    type Filter = Unfilterable;
}

impl<K, V, S> Filterable for HashMap<K, V, S> {
    type Filter = Unfilterable;
}

impl<A, B> Filterable for (A, B) {
    type Filter = Unfilterable;
}

impl<A, B, C> Filterable for (A, B, C) {
    type Filter = Unfilterable;
}

impl<A, B, C, D> Filterable for (A, B, C, D) {
    type Filter = Unfilterable;
}

// Register the four filter types for TS export, once per generic type here
// (not per-entity — unlike XQuery, which is a concrete struct generated
// fresh per entity by #[myko_item], these are ts-rs *generic* TS types:
// `export type IdFilter<T> = ...`. ts-rs derives the TS output treating the
// Rust generic parameter as a TS generic, so which concrete Rust type
// triggers the underlying `TS::export()` call doesn't affect the emitted
// `.ts` file's content — Arc<str>/i64 just need to satisfy each type's own
// derive bounds (Clone/PartialEq/Eq/Serialize/Deserialize/TS). `bool`
// (bare equality's own filter type) needs no registration: it maps
// directly to the TS `boolean` primitive, not a named type declaration.
crate::register_typegen_type!(
    IdFilter<Arc<str>>,
    NumericFilter<i64>,
    StringFilter,
    EqFilter<Arc<str>>,
    Unfilterable
);

// ─────────────────────────────────────────────────────────────────────────
// Wire format. `Eq` serializes as the bare value — byte-identical to the
// pre-6.0 `PartialX { field: value }` shape, so old wire payloads keep
// deserializing (bare value == eq) even though the Rust API is a hard break.
// Every richer operator is a single-key `$`-sigilled object (`{ "$in": … }`,
// `{ "$range": … }`, …). The sigil can never collide with a bare value: a
// bare value is either a scalar (different JSON type from an object) or a
// Rust-derived object whose keys are identifiers — never `$`-prefixed.
// Deserialization tries the sigilled operator first, then falls back to
// bare==eq, via a self-describing-format untagged choice (myko's wire is
// JSON and CBOR, both self-describing; this would not work over bincode).
// ─────────────────────────────────────────────────────────────────────────
mod wire_serde {
    use serde::{Deserializer, Serializer, ser::SerializeMap};

    use super::{Arc, Deserialize, EqFilter, IdFilter, NumericFilter, Serialize, StringFilter};

    fn serialize_op<S: Serializer, T: Serialize + ?Sized>(
        s: S,
        key: &str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(1))?;
        m.serialize_entry(key, value)?;
        m.end()
    }

    // IdFilter / EqFilter share a wire: bare == eq, `{ "$in": [...] }`.
    macro_rules! eq_in_wire {
        ($filter:ident) => {
            impl<T: Serialize> Serialize for $filter<T> {
                fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                    match self {
                        $filter::Eq(v) => v.serialize(s),
                        $filter::In(vs) => serialize_op(s, "$in", vs),
                    }
                }
            }
            impl<'de, T: Deserialize<'de>> Deserialize<'de> for $filter<T> {
                fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                    #[derive(Deserialize)]
                    #[serde(bound(deserialize = "T: Deserialize<'de>"))]
                    enum Op<T> {
                        #[serde(rename = "$in")]
                        In(Vec<T>),
                    }
                    #[derive(Deserialize)]
                    #[serde(untagged, bound(deserialize = "T: Deserialize<'de>"))]
                    enum Wire<T> {
                        Op(Op<T>),
                        Bare(T),
                    }
                    Ok(match Wire::deserialize(d)? {
                        Wire::Op(Op::In(vs)) => $filter::In(vs),
                        Wire::Bare(v) => $filter::Eq(v),
                    })
                }
            }
        };
    }
    eq_in_wire!(IdFilter);
    eq_in_wire!(EqFilter);

    impl Serialize for StringFilter {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            match self {
                Self::Eq(v) => v.serialize(s),
                Self::In(vs) => serialize_op(s, "$in", vs),
                Self::Contains(v) => serialize_op(s, "$contains", v),
                Self::StartsWith(v) => serialize_op(s, "$startsWith", v),
            }
        }
    }
    impl<'de> Deserialize<'de> for StringFilter {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            enum Op {
                #[serde(rename = "$in")]
                In(Vec<Arc<str>>),
                #[serde(rename = "$contains")]
                Contains(Arc<str>),
                #[serde(rename = "$startsWith")]
                StartsWith(Arc<str>),
            }
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Wire {
                Op(Op),
                Bare(Arc<str>),
            }
            Ok(match Wire::deserialize(d)? {
                Wire::Op(Op::In(vs)) => Self::In(vs),
                Wire::Op(Op::Contains(v)) => Self::Contains(v),
                Wire::Op(Op::StartsWith(v)) => Self::StartsWith(v),
                Wire::Bare(v) => Self::Eq(v),
            })
        }
    }

    impl<T: Serialize> Serialize for NumericFilter<T> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            match self {
                Self::Eq(v) => v.serialize(s),
                Self::In(vs) => serialize_op(s, "$in", vs),
                Self::Range { min, max } => {
                    struct Bounds<'a, T> {
                        min: &'a Option<T>,
                        max: &'a Option<T>,
                    }
                    impl<T: Serialize> Serialize for Bounds<'_, T> {
                        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                            let len = usize::from(self.min.is_some())
                                .saturating_add(usize::from(self.max.is_some()));
                            let mut m = s.serialize_map(Some(len))?;
                            if let Some(v) = self.min {
                                m.serialize_entry("min", v)?;
                            }
                            if let Some(v) = self.max {
                                m.serialize_entry("max", v)?;
                            }
                            m.end()
                        }
                    }
                    serialize_op(s, "$range", &Bounds { min, max })
                }
            }
        }
    }
    impl<'de, T: Deserialize<'de>> Deserialize<'de> for NumericFilter<T> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            #[serde(bound(deserialize = "T: Deserialize<'de>"))]
            enum Op<T> {
                #[serde(rename = "$in")]
                In(Vec<T>),
                #[serde(rename = "$range")]
                Range {
                    #[serde(default)]
                    min: Option<T>,
                    #[serde(default)]
                    max: Option<T>,
                },
            }
            #[derive(Deserialize)]
            #[serde(untagged, bound(deserialize = "T: Deserialize<'de>"))]
            enum Wire<T> {
                Op(Op<T>),
                Bare(T),
            }
            Ok(match Wire::deserialize(d)? {
                Wire::Op(Op::In(vs)) => Self::In(vs),
                Wire::Op(Op::Range { min, max }) => Self::Range { min, max },
                Wire::Bare(v) => Self::Eq(v),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! require_ok {
        ($value:expr) => {
            match {
                let result = $value;
                assert!(
                    result.is_ok(),
                    "unexpected error: {:?}",
                    result.as_ref().err()
                );
                result
            } {
                Ok(value) => value,
                Err(_) => return,
            }
        };
    }

    #[test]
    fn id_filter_eq_and_in() {
        let eq: IdFilter<i64> = IdFilter::Eq(5);
        assert!(eq.matches(&5));
        assert!(!eq.matches(&6));

        let in_: IdFilter<i64> = IdFilter::In(vec![1, 2, 3]);
        assert!(in_.matches(&2));
        assert!(!in_.matches(&4));
    }

    #[test]
    fn id_filter_empty_in_matches_nothing() {
        let empty: IdFilter<i64> = IdFilter::In(vec![]);
        assert!(!empty.matches(&0));
        assert!(!empty.matches(&1));
    }

    #[test]
    fn id_filter_canonicalization() {
        // Permuted + duplicated In arrays canonicalize identically.
        let a = IdFilter::In(vec![3, 1, 2]).canonicalize();
        let b = IdFilter::In(vec![1, 2, 3, 2, 1]).canonicalize();
        assert_eq!(a, b);
        assert_eq!(a, IdFilter::In(vec![1, 2, 3]));

        // In([x]) collapses to Eq(x).
        let single = IdFilter::In(vec![7]).canonicalize();
        assert_eq!(single, IdFilter::Eq(7));
    }

    #[test]
    fn numeric_filter_range_inclusive_bounds() {
        let range = NumericFilter::Range {
            min: Some(1i64),
            max: Some(10),
        };
        assert!(range.matches(&1));
        assert!(range.matches(&10));
        assert!(range.matches(&5));
        assert!(!range.matches(&0));
        assert!(!range.matches(&11));
    }

    #[test]
    fn numeric_filter_range_open_ended() {
        let min_only = NumericFilter::Range {
            min: Some(5i64),
            max: None,
        };
        assert!(min_only.matches(&5));
        assert!(min_only.matches(&1000));
        assert!(!min_only.matches(&4));

        let max_only = NumericFilter::Range {
            min: None,
            max: Some(5i64),
        };
        assert!(max_only.matches(&5));
        assert!(max_only.matches(&-1000));
        assert!(!max_only.matches(&6));
    }

    #[test]
    #[cfg(feature = "ordered-float")]
    fn ordered_float_is_filterable_via_numeric_filter() {
        // Mirrors a downstream crate's own Keyframe-style entity field
        // (e.g. rship's `position: OrderedFloat<f64>`) — OrderedFloat is
        // foreign to both myko and rship, so this impl has to live here.
        use ordered_float::OrderedFloat;

        fn assert_filterable<T: Filterable>() {}
        assert_filterable::<OrderedFloat<f64>>();
        assert_filterable::<Option<OrderedFloat<f64>>>();

        let range = NumericFilter::Range {
            min: Some(OrderedFloat(1.0)),
            max: Some(OrderedFloat(5.0)),
        };
        assert!(range.matches(&OrderedFloat(3.0)));
        assert!(!range.matches(&OrderedFloat(6.0)));

        let eq = NumericFilter::Eq(OrderedFloat(2.5));
        assert!(eq.matches(&OrderedFloat(2.5)));
        assert!(!eq.matches(&OrderedFloat(2.6)));
    }

    #[test]
    fn numeric_filter_degenerate_range_canonicalizes_to_eq() {
        let degenerate = NumericFilter::Range {
            min: Some(3i64),
            max: Some(3),
        };
        assert_eq!(degenerate.canonicalize(), NumericFilter::Eq(3));
    }

    #[test]
    fn numeric_filter_in_canonicalization() {
        let a = NumericFilter::In(vec![3i64, 1, 2]).canonicalize();
        assert_eq!(a, NumericFilter::In(vec![1, 2, 3]));
        let single = NumericFilter::In(vec![9i64]).canonicalize();
        assert_eq!(single, NumericFilter::Eq(9));
    }

    #[test]
    fn string_filter_contains_and_starts_with() {
        let contains = StringFilter::Contains(Arc::from("mid"));
        assert!(contains.matches(&Arc::from("something-midway")));
        assert!(!contains.matches(&Arc::from("nope")));

        let starts = StringFilter::StartsWith(Arc::from("pre"));
        assert!(starts.matches(&Arc::from("prefix")));
        assert!(!starts.matches(&Arc::from("suffix")));
    }

    #[test]
    fn string_filter_in_canonicalization_dedupes_and_sorts() {
        let a =
            StringFilter::In(vec![Arc::from("b"), Arc::from("a"), Arc::from("b")]).canonicalize();
        assert_eq!(a, StringFilter::In(vec![Arc::from("a"), Arc::from("b")]));
    }

    #[test]
    fn bool_filter_is_bare_equality() {
        assert!(true.matches(&true));
        assert!(!true.matches(&false));
        assert!(false.matches(&false));
    }

    #[test]
    fn eq_filter_eq_and_in() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum State {
            Armed,
            Building,
            Idle,
        }

        let filter: EqFilter<State> = EqFilter::In(vec![State::Armed, State::Building]);
        assert!(filter.matches(&State::Armed));
        assert!(filter.matches(&State::Building));
        assert!(!filter.matches(&State::Idle));
    }

    #[test]
    fn impl_filterable_eq_macro_wires_up_filterable() {
        // Mirrors a downstream crate's own entity-field enum (e.g.
        // rship's BindingValue) that isn't an id/numeric/string field and
        // has no built-in Filterable impl — impl_filterable_eq! is how it
        // opts into EqFilter without hand-writing the impl.
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
        enum ConnectionState {
            Connected,
            Disconnected,
        }
        crate::impl_filterable_eq!(ConnectionState);

        fn assert_filterable<T: Filterable>() {}
        assert_filterable::<ConnectionState>();

        let filter: <ConnectionState as Filterable>::Filter =
            EqFilter::Eq(ConnectionState::Connected);
        assert!(filter.matches(&ConnectionState::Connected));
        assert!(!filter.matches(&ConnectionState::Disconnected));
    }

    #[test]
    fn in_matches_large_set_membership() {
        let values: Vec<i64> = (0..64).collect();
        assert!(in_matches(&values, &0));
        assert!(in_matches(&values, &63));
        assert!(!in_matches(&values, &-1));
        assert!(!in_matches(&values, &64));
    }

    #[test]
    fn eq_serializes_as_the_bare_value() {
        // The whole point of the wire design: `Eq` IS the bare value, so the
        // pre-6.0 `PartialX { field: value }` payload is still a valid query.
        assert_eq!(
            require_ok!(serde_json::to_value(IdFilter::Eq(Arc::<str>::from("abc")))),
            serde_json::json!("abc")
        );
        assert_eq!(
            require_ok!(serde_json::to_value(NumericFilter::Eq(42_i64))),
            serde_json::json!(42)
        );
        assert_eq!(
            require_ok!(serde_json::to_value(StringFilter::Eq(Arc::from("hi")))),
            serde_json::json!("hi")
        );
    }

    #[test]
    fn operators_serialize_as_sigilled_objects() {
        assert_eq!(
            require_ok!(serde_json::to_value(IdFilter::In(vec![
                Arc::<str>::from("a"),
                Arc::from("b")
            ]))),
            serde_json::json!({ "$in": ["a", "b"] })
        );
        assert_eq!(
            require_ok!(serde_json::to_value(StringFilter::Contains(Arc::from("x")))),
            serde_json::json!({ "$contains": "x" })
        );
        assert_eq!(
            require_ok!(serde_json::to_value(StringFilter::StartsWith(Arc::from(
                "p"
            )))),
            serde_json::json!({ "$startsWith": "p" })
        );
        assert_eq!(
            require_ok!(serde_json::to_value(NumericFilter::Range {
                min: Some(1_i64),
                max: Some(9),
            })),
            serde_json::json!({ "$range": { "min": 1, "max": 9 } })
        );
        // A half-open range omits the absent bound rather than emitting null.
        assert_eq!(
            require_ok!(serde_json::to_value(NumericFilter::Range {
                min: Some(1_i64),
                max: None,
            })),
            serde_json::json!({ "$range": { "min": 1 } })
        );
    }

    #[test]
    fn old_bare_wire_deserializes_as_eq() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct Enumish {
            variant: String,
        }

        // WIRE-COMPAT ACID TEST: a pre-6.0 payload (bare value per field)
        // must deserialize to `Eq` under every active filter type.
        let id: IdFilter<Arc<str>> = require_ok!(serde_json::from_value(serde_json::json!("abc")));
        assert_eq!(id, IdFilter::Eq(Arc::from("abc")));

        let n: NumericFilter<i64> = require_ok!(serde_json::from_value(serde_json::json!(42)));
        assert_eq!(n, NumericFilter::Eq(42));

        let s: StringFilter = require_ok!(serde_json::from_value(serde_json::json!("hi")));
        assert_eq!(s, StringFilter::Eq(Arc::from("hi")));

        // And an EqFilter over an object-shaped value (the enum case): a bare
        // object with no `$` key is Eq, never mistaken for an operator.
        let e: EqFilter<Enumish> = require_ok!(serde_json::from_value(
            serde_json::json!({ "variant": "connected" })
        ));
        assert_eq!(
            e,
            EqFilter::Eq(Enumish {
                variant: "connected".into()
            })
        );
    }

    #[test]
    fn sigilled_wire_deserializes_as_operators() {
        let id: IdFilter<Arc<str>> = require_ok!(serde_json::from_value(
            serde_json::json!({ "$in": ["a", "b"] })
        ));
        assert_eq!(id, IdFilter::In(vec![Arc::from("a"), Arc::from("b")]));

        let s: StringFilter = require_ok!(serde_json::from_value(
            serde_json::json!({ "$contains": "x" })
        ));
        assert_eq!(s, StringFilter::Contains(Arc::from("x")));

        let n: NumericFilter<i64> = require_ok!(serde_json::from_value(
            serde_json::json!({ "$range": { "min": 1, "max": 9 } })
        ));
        assert_eq!(
            n,
            NumericFilter::Range {
                min: Some(1),
                max: Some(9)
            }
        );
    }

    #[test]
    fn every_filter_round_trips_both_forms() {
        fn rt<F>(f: F)
        where
            F: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            let json = require_ok!(serde_json::to_value(&f));
            let back: F = require_ok!(serde_json::from_value(json));
            assert_eq!(f, back);
            drop(f);
        }
        rt(IdFilter::Eq(Arc::<str>::from("a")));
        rt(IdFilter::In(vec![Arc::<str>::from("a"), Arc::from("b")]));
        rt(StringFilter::Eq(Arc::from("a")));
        rt(StringFilter::Contains(Arc::from("a")));
        rt(StringFilter::StartsWith(Arc::from("a")));
        rt(StringFilter::In(vec![Arc::from("a")]));
        rt(NumericFilter::Eq(1_i64));
        rt(NumericFilter::In(vec![1_i64, 2]));
        rt(NumericFilter::Range {
            min: Some(1_i64),
            max: Some(9),
        });
        rt(NumericFilter::Range {
            min: Some(1_i64),
            max: None,
        });
    }

    #[test]
    fn wire_round_trips_over_cbor_too() {
        // The untagged deserialize needs a self-describing format; myko's
        // wire is JSON AND CBOR (see wire/protocol.rs message_to_cbor), so
        // prove both operator and bare forms survive a ciborium round trip.
        fn rt_cbor<F>(f: F)
        where
            F: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            let mut bytes = Vec::new();
            require_ok!(ciborium::ser::into_writer(&f, &mut bytes));
            let back: F = require_ok!(ciborium::de::from_reader(bytes.as_slice()));
            assert_eq!(f, back);
            drop(f);
        }
        rt_cbor(IdFilter::Eq(Arc::<str>::from("a")));
        rt_cbor(IdFilter::In(vec![Arc::<str>::from("a"), Arc::from("b")]));
        rt_cbor(StringFilter::Contains(Arc::from("x")));
        rt_cbor(NumericFilter::Range {
            min: Some(1_i64),
            max: Some(9),
        });
    }

    #[test]
    fn old_partial_query_payload_still_deserializes() {
        // The end-to-end promise: a whole pre-6.0 `PartialClient`-shaped
        // query — bare values per field — deserializes into the new
        // `ClientQuery` with eq semantics on each supplied field, and
        // omitted fields stay unconstrained (None).
        use crate::entities::client::ClientQuery;
        let q: ClientQuery = require_ok!(serde_json::from_value(serde_json::json!({
            "address": "192.168.1.5:54320",
        })));
        assert_eq!(
            q.address,
            Some(StringFilter::Eq(Arc::from("192.168.1.5:54320")))
        );
        assert_eq!(q.server_id, None);
        assert_eq!(q.windback, None);
        assert_eq!(q.id, None);
    }

    #[test]
    fn unfilterable_field_is_always_none() {
        // Nothing constructs an Unfilterable value — the only inhabitant
        // of Option<Unfilterable> is None, and it stays that way through a
        // full serde round-trip.
        let none: Option<Unfilterable> = None;
        let json = require_ok!(serde_json::to_value(&none));
        assert_eq!(json, serde_json::Value::Null);
        let round_tripped: Option<Unfilterable> = require_ok!(serde_json::from_value(json));
        assert_eq!(round_tripped, None);
    }

    #[test]
    fn unfilterable_field_rejects_a_populated_value() {
        // Deserializing anything OTHER than null/absent into
        // Option<Unfilterable> must fail — there is no valid Unfilterable
        // payload to construct, so a filter that tries to pin this field
        // is rejected rather than silently accepted and ignored.
        let attempted = serde_json::json!({"kind": "eq", "value": "anything"});
        let result: Result<Option<Unfilterable>, _> = serde_json::from_value(attempted);
        assert!(result.is_err());
    }

    #[test]
    fn serde_json_value_is_filterable_via_unfilterable() {
        fn assert_filterable<T: Filterable>() {}
        assert_filterable::<serde_json::Value>();
    }

    #[test]
    fn container_types_are_filterable_via_unfilterable() {
        // Vec<T>/HashMap<K, V>/tuples are foreign (std) types — a
        // downstream crate can never impl our local Filterable trait for
        // them itself (orphan rule), so these blanket impls have to live
        // here or a field of any of these shapes blocks #[myko_item]'s
        // XQuery derive entirely.
        fn assert_filterable<T: Filterable>() {}
        assert_filterable::<Vec<String>>();
        assert_filterable::<HashMap<String, i64>>();
        assert_filterable::<(String, i64)>();
        assert_filterable::<(String, i64, bool)>();
        assert_filterable::<(String, i64, bool, Arc<str>)>();

        let none: Option<<Vec<String> as Filterable>::Filter> = None;
        let json = require_ok!(serde_json::to_value(&none));
        assert_eq!(json, serde_json::Value::Null);
    }

    #[test]
    fn impl_filterable_opaque_macro_wires_up_filterable() {
        // Mirrors a downstream crate's own opaque payload struct (e.g.
        // rship's Snapshot-adjacent types) that isn't Eq/Ord/Hash and has
        // no query-meaningful equality — impl_filterable_opaque! is how it
        // opts into Unfilterable without hand-writing the impl.
        #[derive(Debug, Clone, PartialEq)]
        struct OpaquePayload;
        crate::impl_filterable_opaque!(OpaquePayload);

        fn assert_filterable<T: Filterable>() {}
        assert_filterable::<OpaquePayload>();

        let none: Option<<OpaquePayload as Filterable>::Filter> = None;
        let json = require_ok!(serde_json::to_value(&none));
        assert_eq!(json, serde_json::Value::Null);
    }
}
