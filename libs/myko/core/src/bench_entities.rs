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
    pub name: String,
    pub category: String,
    pub value: i64,
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
        })
    }
}
