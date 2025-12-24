//! EntitySearch report for full-text search across entities.
//!
//! This report searches for entities matching a query string and returns the matching IDs.
//!
//! # Example
//!
//! ```rust,ignore
//! // Type-safe constructor with default limit (100)
//! let search = EntitySearch::for_type::<Target>("audio mixer");
//!
//! // With custom limit
//! let search = EntitySearch::for_type_with_limit::<Target>("audio mixer", 50);
//!
//! let result = report_manager.call(ReportManagerMsg::Execute, search).await?;
//! // result contains EntitySearchResult { ids: vec!["id1".into(), "id2".into(), ...] }
//! ```
//!

use std::sync::Arc;

use crate as myko_rs;
use crate::{
    item::Eventable,
    report::{ReportContext, ReportHandler},
};

/// Result of an entity search.
#[myko_macros::myko_report_output]
pub struct EntitySearchResult {
    /// IDs of entities matching the search query
    pub ids: Vec<Arc<str>>,
}

/// Search for entities by full-text query.
///
/// Returns matching entity IDs up to the specified limit.
/// If the entity type is not indexed for search, returns an empty result.
#[myko_macros::myko_report(EntitySearchResult)]
pub struct EntitySearch {
    /// Entity type to search (e.g., "Target", "Scene")
    pub entity_type: String,
    /// Search query string
    pub query: String,
    /// Maximum number of results to return (default: 100)
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

impl EntitySearch {
    /// Create a new EntitySearch for a specific entity type with default limit (100).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let search = EntitySearch::for_type::<Target>("audio mixer");
    /// ```
    pub fn for_type<T: Eventable>(query: &str) -> Self {
        Self::for_type_with_limit::<T>(query, 100)
    }

    /// Create a new EntitySearch for a specific entity type with custom limit.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let search = EntitySearch::for_type_with_limit::<Target>("audio mixer", 50);
    /// ```
    pub fn for_type_with_limit<T: Eventable>(query: &str, limit: usize) -> Self {
        Self {
            entity_type: T::entity_name_static().to_string(),
            query: query.to_string(),
            limit,
        }
    }
}

impl ReportHandler for EntitySearch {
    type Output = EntitySearchResult;

    fn compute(&self, ctx: ReportContext) -> hypha::Cell<Self::Output, hypha::CellImmutable> {
        // Perform search via ReportContext (sync call)
        let ids = ctx.search(&self.entity_type, &self.query, self.limit);

        // Create an immutable cell with the search result
        // Note: This report returns a single result and doesn't update reactively.
        // For reactive search, you would need to subscribe to entity changes.
        hypha::Cell::new(EntitySearchResult { ids }).lock()
    }
}
