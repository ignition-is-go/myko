//! Benchmark-only entities using the myko macros.
//!
//! This module is only compiled when the `bench` feature is enabled.
//! It provides entities that use the full macro stack for realistic
//! performance testing.
//!
//! The `#[myko_item]` macro auto-generates:
//! - `GetAllBenchItems` - query all items
//! - `GetBenchItemsByIds` - query by ID list
//! - `GetBenchItemsByQuery` - query by partial match
//! - `CountAllBenchItems` / `CountBenchItems` - count reports
//!
//! We add a custom `GetBenchItemsByCategory` for category-based filtering,
//! and `SwitchMapReport` for testing switch_map + query_map cache cleanup.

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

    /// Tree-shaped entity for benchmarking cross-store-get + downcast patterns
    /// like `FilteredTargetTree`'s lineage walk in rship. The hot-path question is:
    /// inside a project closure that does N parent-pointer hops per item, how much
    /// of the cost is the dyn-boundary downcast?
    #[myko_item]
    pub struct BenchTreeItem {
        pub name: String,
        pub parent_id: Option<Arc<str>>,
        pub depth: i64,
    }
}

/// Query to get BenchItems filtered by category (custom query beyond auto-generated ones).
#[myko_query(BenchItem)]
pub struct GetBenchItemsByCategory {
    pub category: String,
}

impl QueryHandler for GetBenchItemsByCategory {
    fn test_entity(ctx: QueryTestCtx<Self>) -> bool {
        ctx.item.category == ctx.query.category.as_str()
    }
}

/// Report that reproduces the CuePaused memory leak pattern:
/// switch_map on an outer query, with a nested query_map inside.
///
/// The outer watches all items matching a category. On each change,
/// switch_map creates a new inner query_map(GetBenchItemsByIds) to
/// look up the matching items by ID. This is the exact pattern that
/// leaks in production.
#[myko_report(Vec<String>)]
pub struct SwitchMapReport {
    pub category: String,
}

impl ReportHandler for SwitchMapReport {
    type Output = Vec<String>;

    fn compute(&self, ctx: ReportContext) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let category = self.category.clone();

        // Outer: watch all items matching the category
        let items = ctx
            .query_map(GetBenchItemsByQuery(PartialBenchItem {
                category: Some(category),
                ..Default::default()
            }))
            .items();

        // switch_map + nested query_map — the leak pattern
        items.switch_map(move |items| {
            if items.is_empty() {
                return Cell::new(Arc::new(Vec::<String>::new())).lock();
            }

            let ids: Vec<BenchItemId> = items.iter().map(|item| item.id.clone()).collect();

            // Inner: look up by IDs (different IDs each time = different cache key)
            ctx.query_map(GetBenchItemsByIds { ids })
                .items()
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
        assert_eq!(mixer[0].id.0.as_ref(), "1");
        assert_eq!(mixer[0].score, Score::Exact);

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

        let name_hit = &index.search("alpha", SearchOptions::default())[0];
        assert_eq!(name_hit.matched_field, 0, "alpha is the name field");

        let cat_hit = &index.search("beta", SearchOptions::default())[0];
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
        assert_eq!(hits[0].id.as_ref(), "1");
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
