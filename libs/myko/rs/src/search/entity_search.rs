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

use {crate as myko_rs};

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::item::Eventable;
use crate::report::{ReportContext, ReportHandler};

/// Result of an entity search.
#[derive(Debug, Clone, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
pub struct EntitySearchResult {
    /// IDs of entities matching the search query
    pub ids: Vec<Arc<str>>,
}

crate::register_ts_export!(EntitySearchResult);

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
            entity_type: T::entity_name_static(),
            query: query.to_string(),
            limit,
        }
    }
}

impl ReportHandler for EntitySearch {
    type Output = EntitySearchResult;

    fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Self::Output> + Send>> {
        Box::pin(async_stream::stream! {
            // Parse args
            let args: EntitySearch = match ctx.args() {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed to parse EntitySearch args: {}", e);
                    yield EntitySearchResult { ids: vec![] };
                    return;
                }
            };

            // Perform search via server context (sync call)
            let ids = ctx.server_ctx.search(&args.entity_type, &args.query, args.limit);

            yield EntitySearchResult { ids };

            // Note: This report returns a single result and doesn't update reactively.
            // For reactive search, you would need to subscribe to events and re-search
            // when relevant entities change. That's left for future enhancement.
        })
    }
}
