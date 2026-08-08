//! Benchmark-only entities using the myko macros.
//!
//! This module is only compiled when the `bench` feature is enabled.
//! It provides entities that use the full macro stack for realistic
//! performance testing.
//!
//! The `#[myko_item]` macro auto-generates:
//! - `GetAllBenchItems` - query all items
//! - `GetBenchItemsByIds` - query by ID list
//! - `GetBenchItemsByQuery` - query by per-field filter
//! - `CountAllBenchItems` / `CountBenchItems` - count reports
//!
//! We add a custom `GetBenchItemsByCategory` for category-based filtering,
//! and `SwitchMapReport` for testing `switch_map` + `query_map` cache cleanup.

use std::sync::Arc;

use hyphae::SwitchMapExt;

use crate::prelude::*;

/// A simple entity for benchmarking with category-based filtering.
#[myko_item]
pub struct BenchItem {
    #[searchable]
    pub name: String,
    #[searchable]
    pub category: String,
    pub value: i64,
}

// Tree-shaped entity lives in a sub-module because `myko_item` re-imports
// hyphae traits at module scope and two invocations in the same module collide.
pub use tree::BenchTreeItem;
mod tree {
    use std::sync::Arc;

    use crate::prelude::*;

    /// Tree-shaped entity for benchmarking cross-store-get + downcast patterns.
    ///
    /// This models a filtered-tree view's lineage walk. The hot-path question is:
    /// inside a project closure that does N parent-pointer hops per item, how much
    /// of the cost is the dyn-boundary downcast?
    #[myko_item]
    pub struct BenchTreeItem {
        pub name: String,
        pub parent_id: Option<Arc<str>>,
        pub depth: i64,
    }
}

// Compound-key belongs_to test fixture — needed because no PRODUCTION
// entity has 2+ #[belongs_to] fields (confirmed during the rship-qtu
// investigation this feature partly grew out of). Exercises the K-bucket
// union routing's cartesian-product path (advanced-query-design spec §4
// acceptance criterion 2's "2-belongs_to compound-key case"), which a
// single-belongs_to entity like Client can't reach.
pub use compound_a::{BenchParentA, BenchParentAId};
mod compound_a {
    use crate::prelude::*;

    #[myko_item]
    pub struct BenchParentA {
        pub name: String,
    }
}

pub use compound_b::{BenchParentB, BenchParentBId};
mod compound_b {
    use crate::prelude::*;

    #[myko_item]
    pub struct BenchParentB {
        pub name: String,
    }
}

pub use compound_child::{
    BenchCompoundChild, BenchCompoundChildQuery, GetBenchCompoundChildsByQuery,
};
mod compound_child {
    use super::{BenchParentA, BenchParentAId, BenchParentB, BenchParentBId};
    use crate::prelude::*;

    #[myko_item]
    pub struct BenchCompoundChild {
        #[belongs_to(BenchParentA)]
        pub parent_a_id: BenchParentAId,
        #[belongs_to(BenchParentB)]
        pub parent_b_id: BenchParentBId,
        pub value: i64,
    }
}

// Opaque JSON payload field fixture — mirrors rship's Snapshot entity
// (json payload field), which hit `Filterable` not being implemented for
// `serde_json::Value` before the Unfilterable marker filter existed.
// #[myko_item] must still generate a compiling, derivable XQuery with a
// `payload: Option<Unfilterable>` field that can only ever be `None`.
pub use snapshot::BenchSnapshot;
mod snapshot {
    use crate::prelude::*;

    #[myko_item]
    pub struct BenchSnapshot {
        pub payload: serde_json::Value,
    }
}

// Container-field fixture — mirrors rship's Class A Filterable gap (Vec/
// HashMap/tuple entity fields), which hit the same "no orphan-rule
// workaround exists downstream" problem as serde_json::Value did before
// the blanket container impls existed. #[myko_item] must generate a
// compiling, derivable XQuery with `Unfilterable`-backed fields for all
// three container shapes.
pub use containers::BenchContainerFields;
mod containers {
    use std::collections::HashMap;

    use crate::prelude::*;

    #[myko_item]
    pub struct BenchContainerFields {
        pub tags: Vec<String>,
        pub metadata: HashMap<String, i64>,
        pub coordinates: (i64, i64),
    }
}

// OrderedFloat fixture — mirrors rship's Keyframe entity (`position:
// OrderedFloat<f64>`), which hit the same "foreign type, orphan rules
// block a downstream Filterable impl" problem as the container fields
// above, but is genuinely numeric (Range-filterable), not Unfilterable.
#[cfg(feature = "ordered-float")]
pub use keyframe::BenchKeyframe;
#[cfg(feature = "ordered-float")]
mod keyframe {
    use ordered_float::OrderedFloat;

    use crate::prelude::*;

    #[myko_item]
    pub struct BenchKeyframe {
        pub position: OrderedFloat<f64>,
    }
}

// #[myko_subtype] fixtures — proves the macro's auto-generated Filterable
// impl (added because writing impl_filterable_eq!/impl_filterable_opaque!
// by hand for every domain subtype was exactly the class of adoption
// friction rship kept hitting) picks EqFilter when the consumer derived
// Eq + Ord, and falls back to Unfilterable otherwise, without either case
// needing a manual Filterable impl of its own.
pub use subtypes::{BenchSubtypeFields, BenchSubtypeOrdered, BenchSubtypeUnordered};
mod subtypes {
    use crate::prelude::*;

    #[myko_subtype(derive(Eq, Ord, PartialOrd))]
    pub struct BenchSubtypeOrdered {
        pub label: String,
    }

    #[myko_subtype(derive(Eq))]
    pub struct BenchSubtypeUnordered {
        pub note: String,
    }

    #[myko_item]
    pub struct BenchSubtypeFields {
        pub ordered: BenchSubtypeOrdered,
        pub unordered: BenchSubtypeUnordered,
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::query::{EqFilter, Filter, Filterable, Unfilterable};

        #[test]
        fn eq_and_ord_subtype_gets_eq_filter() {
            let filter: <BenchSubtypeOrdered as Filterable>::Filter =
                EqFilter::Eq(BenchSubtypeOrdered {
                    label: "a".to_string(),
                });
            assert!(filter.matches(&BenchSubtypeOrdered {
                label: "a".to_string(),
            }));
            assert!(!filter.matches(&BenchSubtypeOrdered {
                label: "b".to_string(),
            }));
        }

        #[test]
        fn default_subtype_falls_back_to_unfilterable() {
            fn assert_unfilterable<T>()
            where
                T: Filterable<Filter = Unfilterable>,
            {
            }
            assert_unfilterable::<BenchSubtypeUnordered>();
        }
    }
}

// Custom-wire fixture — mirrors rship's BindingValue. Myko owns the opaque
// generated representation, so the subtype supplies no backend trait boilerplate.
pub use manual_wire::BenchManualWireValue;
mod manual_wire {
    use crate::prelude::*;

    #[myko_subtype(derive(Default, Eq), manual(serde), export(as = "unknown"))]
    pub struct BenchManualWireValue {
        pub raw: String,
    }

    impl serde::Serialize for BenchManualWireValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(&self.raw)
        }
    }

    impl<'de> serde::Deserialize<'de> for BenchManualWireValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Self {
                raw: String::deserialize(deserializer)?,
            })
        }
    }

    #[myko_item]
    pub struct BenchManualWireHolder {
        pub value: BenchManualWireValue,
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::query::{Filterable, Unfilterable};

        #[test]
        fn manual_wire_value_serializes_as_a_plain_string() {
            let value = BenchManualWireValue {
                raw: "hello".to_string(),
            };
            let json = serde_json::to_value(&value);
            assert!(json.is_ok(), "serialize manual wire value");
            let Ok(json) = json else {
                return;
            };
            assert_eq!(json, serde_json::Value::String("hello".to_string()));

            let round_tripped = serde_json::from_value::<BenchManualWireValue>(json);
            assert!(round_tripped.is_ok(), "deserialize manual wire value");
            let Ok(round_tripped) = round_tripped else {
                return;
            };
            assert_eq!(round_tripped.raw, "hello");
        }

        #[test]
        fn manual_wire_value_still_gets_the_filterable_auto_impl() {
            fn assert_unfilterable<T>()
            where
                T: Filterable<Filter = Unfilterable>,
            {
            }
            assert_unfilterable::<BenchManualWireValue>();
        }
    }
}

/// Query to get `BenchItems` filtered by category (custom query beyond auto-generated ones).
#[myko_query(BenchItem)]
pub struct GetBenchItemsByCategory {
    pub category: String,
}

impl QueryHandler for GetBenchItemsByCategory {
    fn test_entity(ctx: QueryTestContext<Self>) -> bool {
        ctx.item.category == ctx.query.category.as_str()
    }
}

/// Report that reproduces the `CuePaused` memory leak pattern:
/// `switch_map` on an outer query, with a nested `query_map` inside.
///
/// The outer watches all items matching a category. On each change,
/// `switch_map` creates a new inner `query_map(GetBenchItemsByIds)` to
/// look up the matching items by ID. This is the exact pattern that
/// leaks in production.
#[myko_report(Vec<String>)]
pub struct SwitchMapReport {
    pub category: String,
}

impl ReportHandler for SwitchMapReport {
    type Output = Vec<String>;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let category = self.category.clone();

        // Outer: watch all items matching the category
        let items = ctx
            .query_map(GetBenchItemsByQuery(BenchItemQuery {
                category: Some(StringFilter::Eq(category.into())),
                ..Default::default()
            }))
            .items()
            .materialize();

        // switch_map + nested query_map — the leak pattern
        items.switch_map(move |items| {
            if items.is_empty() {
                return Cell::new(Arc::new(Vec::<String>::new())).lock();
            }

            let ids: Vec<BenchItemId> = items.iter().map(|item| item.id.clone()).collect();

            // Inner: look up by IDs (different IDs each time = different cache key)
            ctx.query_map(GetBenchItemsByIds { ids })
                .items()
                .materialize()
                .map(|items| {
                    Arc::new(
                        items
                            .iter()
                            .map(|item| item.name.clone())
                            .collect::<Vec<_>>(),
                    )
                })
                .materialize()
        })
    }
}

#[cfg(test)]
mod typed_search_tests {
    //! End-to-end test that exercises the macro-generated
    //! `impl Searchable for BenchItem` against the typed `SearchIndex<T>`.

    use super::*;
    use crate::search::typed::{Score, SearchIndex, SearchOptions};

    fn item(id: &str, name: &str, category: &str) -> BenchItem {
        BenchItem {
            id: id.into(),
            name: name.to_string(),
            category: category.to_string(),
            value: 0,
        }
    }

    #[test]
    fn macro_generated_impl_indexes_and_finds() {
        let mut index = SearchIndex::<BenchItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.insert(&item("2", "video camera", "hardware"));
        index.insert(&item("3", "lighting fixture", "props"));

        let mixer = index.search("mixer", SearchOptions::default());
        assert_eq!(mixer.len(), 1);
        assert_eq!(mixer.first().map(|hit| hit.id.0.as_ref()), Some("1"));
        assert_eq!(mixer.first().map(|hit| hit.score), Some(Score::Exact));

        let hardware = index.search("hardware", SearchOptions::default());
        assert_eq!(hardware.len(), 2);
    }

    #[test]
    fn macro_generated_field_names_match_searchable_attrs() {
        use crate::search::typed::Searchable;
        // Mirrors the order of `#[searchable]` on BenchItem (name, category).
        // `value: i64` is *not* searchable so it must not appear here.
        assert_eq!(BenchItem::searchable_field_names(), &["name", "category"]);
    }

    #[test]
    fn matched_field_resolves_against_macro_generated_order() {
        let mut index = SearchIndex::<BenchItem>::new();
        index.insert(&item("1", "alpha", "beta"));

        let name_hits = index.search("alpha", SearchOptions::default());
        assert!(!name_hits.is_empty(), "expected name hit");
        let Some(name_hit) = name_hits.first() else {
            return;
        };
        assert_eq!(name_hit.matched_field, 0, "alpha is the name field");

        let category_hits = index.search("beta", SearchOptions::default());
        assert!(!category_hits.is_empty(), "expected category hit");
        let Some(cat_hit) = category_hits.first() else {
            return;
        };
        assert_eq!(cat_hit.matched_field, 1, "beta is the category field");
    }

    #[test]
    fn build_typed_registry_picks_up_macro_emitted_registration() {
        // Walks the inventory submissions and constructs a per-type
        // SearchIndex<T> for every entity whose macro emitted register_typed.
        let registry = crate::search::build_typed_registry();
        assert!(
            registry.entity_types().any(|t| t == "BenchItem"),
            "BenchItem should be registered via inventory; got: {:?}",
            registry.entity_types().collect::<Vec<_>>()
        );

        registry.insert(&item("1", "audio mixer", "hardware"));
        let hits = registry.search("BenchItem", "mixer", SearchOptions::default());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits.first().map(|hit| hit.id.as_ref()), Some("1"));
    }

    #[test]
    fn macro_generated_typed_search_report_exists() {
        // The macro emits a `Search{T}` report and `Search{T}Result` per
        // entity with `#[searchable]`. This test just smoke-checks that the
        // types exist with the expected shape — actually invoking
        // `compute()` requires a full ReportContext (heavyweight setup).
        let report = SearchBenchItem {
            query: "audio".to_string(),
            limit: 25,
        };
        assert_eq!(report.query, "audio");
        assert_eq!(report.limit, 25);

        // SearchBenchItemResult.ids is `Vec<BenchItemId>` (typed) — not the
        // legacy `Vec<Arc<str>>` that EntitySearchResult uses.
        let result = SearchBenchItemResult {
            ids: vec![BenchItemId::from(std::sync::Arc::<str>::from("1"))],
        };
        assert_eq!(result.ids.len(), 1);
    }
}
