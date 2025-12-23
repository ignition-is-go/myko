//! Query registration via inventory.

use std::any::Any;
use std::sync::Arc;

use hypha::SelectExt;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::{common::with_id::WithId, store::StoreRegistry};

use super::super::item::Eventable;
use super::{
    cell::FilteredCellMap, context::MykoServerCtx, request::QueryRequest,
    traits::{AnyQuery, QueryHandlerCtx, QueryParams},
};

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Type alias for query parse function.
pub type QueryParseFn = fn(Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and host_id, returns a FilteredCellMap.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Uuid, // host_id for server context
) -> Result<FilteredCellMap, String>;

// ─────────────────────────────────────────────────────────────────────────────
// QueryRegistration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(QueryRegistration);

/// Registration entry for a query type.
/// Collected via inventory for automatic discovery.
pub struct QueryRegistration {
    /// Query identifier (e.g., "GetAllTargets")
    pub query_id: &'static str,
    /// Entity type this query returns (e.g., "Target")
    pub query_item_type: &'static str,
    /// Crate where this query is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing query from JSON
    pub parse: QueryParseFn,
    /// Factory for creating reactive cell from query
    pub cell_factory: QueryCellFactory,
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryFactory - Static methods for query types
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating query registration data.
///
/// This trait has a blanket implementation for all types implementing `QueryParams`,
/// so user-defined queries automatically get `parse` and `cell_factory` methods.
pub trait QueryFactory: QueryParams {
    /// Parse JSON into this query type.
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

    /// Create a reactive cell for this query.
    fn cell_factory(
        query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        host_id: Uuid,
    ) -> Result<FilteredCellMap, String>;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let query = serde_json::from_value::<QueryRequest<Q>>(value)?;
        Ok(Arc::new(query))
    }

    fn cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        host_id: Uuid,
    ) -> Result<FilteredCellMap, String> {
        // Downcast to the QueryRequest wrapper
        let any_ref: &dyn Any = any_query.as_ref();
        let request: QueryRequest<Q> = any_ref
            .downcast_ref::<QueryRequest<Q>>()
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Failed to downcast query to QueryRequest<{}>",
                    Q::query_id_static()
                )
            })?;

        // Extract the inner query params
        let query: Arc<Q> = Arc::new(request.query);

        // Get the store for this entity type
        let store = registry.get_or_create(&Q::query_item_type_static());

        log::debug!(
            "Creating query cell for {} with host_id={}",
            Q::query_id_static(),
            host_id
        );

        // Create server context with actual host_id for test_entity
        let server_ctx = Arc::new(MykoServerCtx::new(host_id, registry.clone()));

        // Use store.select() to get a FilteredCellMap of matching items
        Ok(store.select(move |item| {
            if let Some(typed_item) = item.as_any().downcast_ref::<Q::Item>() {
                let ctx = QueryHandlerCtx {
                    item: Arc::new(typed_item.clone()),
                    query: query.clone(),
                    server_ctx: server_ctx.clone(),
                };
                Q::test_entity(ctx)
            } else {
                false
            }
        }))
    }
}
