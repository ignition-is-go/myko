//! Per-type field filters for advanced (array-valued / `IN`) query matching.
//!
//! See `docs/superpowers/specs/2026-07-13-advanced-query-design.md` for the
//! full design. Each data type exposes exactly the filter operations that
//! are meaningful for it — ids are always exact (`Eq`/`In`), numbers add
//! `Range`, strings add substring/prefix matching, bools are bare equality.
//! Selection is driven by the [`Filterable`] trait's associated type, so
//! `#[myko_item]` never has to sniff field types syntactically.

use std::{collections::HashSet, hash::Hash, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::TS;

/// A filter that can test whether a value of type `T` matches.
pub trait Filter<T> {
    fn matches(&self, value: &T) -> bool;
}

/// Normalizes a filter to its canonical form — required for query-cache
/// identity (see spec §1): sorted+deduped `In`, `In([x])` -> `Eq(x)`,
/// `Range{a,a}` -> `Eq(a)`. Implemented uniformly across every filter type,
/// including `bool` (a no-op — bare equality has nothing to canonicalize),
/// so `#[myko_item]`'s generated `XFilter::canonicalize` can canonicalize
/// every field the same way regardless of which filter type it holds.
pub trait CanonicalFilter: Sized {
    fn canonicalize(self) -> Self;
}

/// Associates a type with the filter type that can express queries over it.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
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
            IdFilter::Eq(value) => vec![value.clone()],
            IdFilter::In(values) => values.clone(),
        }
    }
}

impl<T: Ord + Clone> CanonicalFilter for IdFilter<T> {
    /// Sort + dedup `In`, collapse a 1-element `In` to `Eq`.
    fn canonicalize(self) -> Self {
        match self {
            IdFilter::In(values) => {
                let values = canonical_in_values(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => IdFilter::Eq(only),
                    Err(values) => IdFilter::In(values),
                }
            }
            other => other,
        }
    }
}

impl<T: Eq + Hash> Filter<T> for IdFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            IdFilter::Eq(expected) => value == expected,
            // In([]) matches nothing — this is correct behavior for
            // "scope to this (possibly empty) derived set" call sites, not
            // a bug. Document loudly (see spec §1) rather than special-case.
            IdFilter::In(values) => in_matches(values, value),
        }
    }
}

impl<T> From<T> for IdFilter<T> {
    fn from(value: T) -> Self {
        IdFilter::Eq(value)
    }
}

impl<T> From<Vec<T>> for IdFilter<T> {
    fn from(values: Vec<T>) -> Self {
        IdFilter::In(values)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// NumericFilter — Eq / In / Range (inclusive bounds).
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum NumericFilter<T> {
    Eq(T),
    In(Vec<T>),
    /// Inclusive bounds; both `None` is invalid (rejected at construction
    /// time is future work — for now it degenerates to "match everything,"
    /// same as an unset filter, since myko trusts callers not to write it).
    Range {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<T>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<T>,
    },
}

impl<T: PartialOrd + PartialEq + Clone> CanonicalFilter for NumericFilter<T> {
    /// Sort + dedup `In`, collapse 1-element `In` to `Eq`, collapse
    /// `Range{min: Some(a), max: Some(a)}` to `Eq(a)`.
    fn canonicalize(self) -> Self {
        match self {
            NumericFilter::In(values) => {
                let values = canonical_in_values_partial(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => NumericFilter::Eq(only),
                    Err(values) => NumericFilter::In(values),
                }
            }
            NumericFilter::Range {
                min: Some(a),
                max: Some(b),
            } if a == b => NumericFilter::Eq(a),
            other => other,
        }
    }
}

impl<T: PartialEq + PartialOrd> Filter<T> for NumericFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            NumericFilter::Eq(expected) => value == expected,
            NumericFilter::In(values) => values.iter().any(|v| v == value),
            NumericFilter::Range { min, max } => {
                min.as_ref().is_none_or(|min| value >= min)
                    && max.as_ref().is_none_or(|max| value <= max)
            }
        }
    }
}

impl<T> From<T> for NumericFilter<T> {
    fn from(value: T) -> Self {
        NumericFilter::Eq(value)
    }
}

impl<T> From<Vec<T>> for NumericFilter<T> {
    fn from(values: Vec<T>) -> Self {
        NumericFilter::In(values)
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

// ─────────────────────────────────────────────────────────────────────────
// StringFilter — Eq / In / Contains / StartsWith. Never Range (meaningless
// for strings without a locale-aware ordering myko doesn't want to own).
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
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
            StringFilter::In(values) => {
                let values = canonical_in_values(values);
                match <[Arc<str>; 1]>::try_from(values) {
                    Ok([only]) => StringFilter::Eq(only),
                    Err(values) => StringFilter::In(values),
                }
            }
            other => other,
        }
    }
}

impl Filter<Arc<str>> for StringFilter {
    fn matches(&self, value: &Arc<str>) -> bool {
        match self {
            StringFilter::Eq(expected) => value == expected,
            StringFilter::In(values) => in_matches(values, value),
            StringFilter::Contains(needle) => value.contains(needle.as_ref()),
            StringFilter::StartsWith(prefix) => value.starts_with(prefix.as_ref()),
        }
    }
}

// Support String-typed entity fields (not just Arc<str>) matching against
// the same Arc<str>-based filter — avoids a parallel StringFilter<String>.
impl Filter<String> for StringFilter {
    fn matches(&self, value: &String) -> bool {
        match self {
            StringFilter::Eq(expected) => value.as_str() == expected.as_ref(),
            StringFilter::In(values) => values.iter().any(|v| v.as_ref() == value.as_str()),
            StringFilter::Contains(needle) => value.contains(needle.as_ref()),
            StringFilter::StartsWith(prefix) => value.starts_with(prefix.as_ref()),
        }
    }
}

impl From<Arc<str>> for StringFilter {
    fn from(value: Arc<str>) -> Self {
        StringFilter::Eq(value)
    }
}

impl From<Vec<Arc<str>>> for StringFilter {
    fn from(values: Vec<Arc<str>>) -> Self {
        StringFilter::In(values)
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
/// generated `XFilter` struct's field declarations.
impl<T: Filterable> Filterable for Option<T> {
    type Filter = T::Filter;
}

// ─────────────────────────────────────────────────────────────────────────
// bool — bare equality. In(Vec<bool>) would always reduce to Eq or
// match-everything, so it doesn't exist as a variant; `bool` itself is its
// own filter type.
// ─────────────────────────────────────────────────────────────────────────

impl Filter<bool> for bool {
    fn matches(&self, value: &bool) -> bool {
        self == value
    }
}

impl Filterable for bool {
    type Filter = bool;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum EqFilter<T> {
    Eq(T),
    In(Vec<T>),
}

impl<T: Ord + Clone> CanonicalFilter for EqFilter<T> {
    /// Sort + dedup `In`, collapse a 1-element `In` to `Eq`.
    fn canonicalize(self) -> Self {
        match self {
            EqFilter::In(values) => {
                let values = canonical_in_values(values);
                match <[T; 1]>::try_from(values) {
                    Ok([only]) => EqFilter::Eq(only),
                    Err(values) => EqFilter::In(values),
                }
            }
            other => other,
        }
    }
}

impl<T: PartialEq> Filter<T> for EqFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            EqFilter::Eq(expected) => value == expected,
            EqFilter::In(values) => values.iter().any(|v| v == value),
        }
    }
}

impl<T> From<T> for EqFilter<T> {
    fn from(value: T) -> Self {
        EqFilter::Eq(value)
    }
}

impl<T> From<Vec<T>> for EqFilter<T> {
    fn from(values: Vec<T>) -> Self {
        EqFilter::In(values)
    }
}

/// Marks `$ty` filterable via [`EqFilter`] (`Eq`/`In`, no partial/range
/// operations) — the fallback for enums and other exact-only opaque types
/// per spec §1. For a downstream crate's own entity-field enums (state
/// machines, tagged values, etc.) that want the same treatment
/// `#[myko_item]`'s generated id/numeric/string fields already get
/// automatically via a built-in `Filterable` impl.
///
/// `$ty` must already satisfy `Debug + Clone + PartialEq + Eq + Ord +
/// Serialize + Deserialize + TS` (ts_rs's `TS`, or `myko::TS`'s no-op form
/// when the `ts-export` feature is off) — this macro only wires up the
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

/// Above this length, `matches` builds a `HashSet` for O(1) membership
/// instead of a linear scan per call. Chosen to be well above the common
/// case (a handful of ids/values) where a `Vec` scan is faster than hashing.
pub const IN_HASH_THRESHOLD: usize = 16;

/// Membership test that switches to a `HashSet` above [`IN_HASH_THRESHOLD`]
/// elements — used by generated `matches` impls for non-indexed `In`
/// filters over larger value sets (per spec §4: "For large `In` arrays
/// build a `HashSet` above a small length threshold inside `matches`").
pub fn in_matches<T: Eq + Hash>(values: &[T], value: &T) -> bool {
    if values.len() > IN_HASH_THRESHOLD {
        values.iter().collect::<HashSet<_>>().contains(value)
    } else {
        values.iter().any(|v| v == value)
    }
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
// field is structurally unfilterable while the containing XFilter still
// compiles, derives, and (de)serializes normally.
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum Unfilterable {}

impl<T> Filter<T> for Unfilterable {
    fn matches(&self, _value: &T) -> bool {
        match *self {}
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

// Register the four filter types for TS export, once per generic type here
// (not per-entity — unlike XFilter, which is a concrete struct generated
// fresh per entity by #[myko_item], these are ts-rs *generic* TS types:
// `export type IdFilter<T> = ...`. ts-rs derives the TS output treating the
// Rust generic parameter as a TS generic, so which concrete Rust type
// triggers the underlying `TS::export()` call doesn't affect the emitted
// `.ts` file's content — Arc<str>/i64 just need to satisfy each type's own
// derive bounds (Clone/PartialEq/Eq/Serialize/Deserialize/TS). `bool`
// (bare equality's own filter type) needs no registration: it maps
// directly to the TS `boolean` primitive, not a named type declaration.
crate::register_ts_export!(
    IdFilter<Arc<str>>,
    NumericFilter<i64>,
    StringFilter,
    EqFilter<Arc<str>>
);

#[cfg(test)]
mod tests {
    use super::*;

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
    fn in_matches_large_set_uses_hashset_path_correctly() {
        let values: Vec<i64> = (0..(IN_HASH_THRESHOLD as i64 + 10)).collect();
        assert!(in_matches(&values, &0));
        assert!(in_matches(&values, &(IN_HASH_THRESHOLD as i64 + 9)));
        assert!(!in_matches(&values, &-1));
        assert!(!in_matches(&values, &(IN_HASH_THRESHOLD as i64 + 10)));
    }

    #[test]
    fn filter_serde_uses_explicit_tag() {
        let filter = IdFilter::In(vec![Arc::<str>::from("a"), Arc::from("b")]);
        let json = serde_json::to_value(&filter).unwrap();
        assert_eq!(json["kind"], "in");
        assert_eq!(json["value"], serde_json::json!(["a", "b"]));

        let round_tripped: IdFilter<Arc<str>> = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, filter);
    }

    #[test]
    fn unfilterable_field_is_always_none() {
        // Nothing constructs an Unfilterable value — the only inhabitant
        // of Option<Unfilterable> is None, and it stays that way through a
        // full serde round-trip.
        let none: Option<Unfilterable> = None;
        let json = serde_json::to_value(&none).unwrap();
        assert_eq!(json, serde_json::Value::Null);
        let round_tripped: Option<Unfilterable> = serde_json::from_value(json).unwrap();
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
}
