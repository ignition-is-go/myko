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
//! We add a custom `GetBenchItemsByCategory` for category-based filtering.

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
