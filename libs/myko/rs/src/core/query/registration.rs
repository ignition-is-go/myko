//! Query registration via inventory.

use std::any::Any;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{
    common::with_id::WithId,
    registry::{
        item::Eventable,
        query::{CapturedQueryParser, MykoQueryParser},
    },
    store::StoreRegistry,
};

use super::{
    cell::FilteredCellMap,
    context::MykoServerCtx,
    request::QueryRequest,
    traits::{QueryHandlerCtx, QueryParams},
};

use crate::registry::query::AnyQuery;

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and host_id, returns a FilteredCellMap.
/// FilteredCellMap wraps a CellMap and maintains its subscription to the store.
/// Serialization to JSON happens at the WebSocket layer, not here.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Uuid, // host_id for server context
) -> Result<FilteredCellMap, String>;

/// Data needed to register a query.
pub struct RegisterQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub parser: Arc<dyn MykoQueryParser>,
    pub cell_factory: QueryCellFactory,
}

/// Type alias for the query factory function pointer.
pub type QueryFactoryFn = fn() -> RegisterQueryData;

/// Registration entry for a query type.
/// Collected via inventory for automatic discovery.
pub struct QueryRegistration {
    /// Query identifier (e.g., "GetAllTargets")
    pub query_id: &'static str,
    /// Entity type this query returns (e.g., "Target")
    pub query_item_type: &'static str,
    /// Crate where this query is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Factory function that creates the registration data
    pub factory: QueryFactoryFn,
}

inventory::collect!(QueryRegistration);

// ─────────────────────────────────────────────────────────────────────────────
// QueryFactory - Creates registration data for cell-based queries
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating query registrations.
///
/// This trait has a blanket implementation for all types implementing `QueryParams`,
/// so user-defined queries automatically get a `create_registration()` method.
pub trait QueryFactory: QueryParams {
    /// Create the registration data (cell factory + parser) for this query type.
    fn create_registration() -> RegisterQueryData;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn create_registration() -> RegisterQueryData {
        let parser: Arc<dyn MykoQueryParser> = Arc::new(CapturedQueryParser::<QueryRequest<Q>>::new());

        RegisterQueryData {
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
            parser,
            cell_factory: |any_query: Arc<dyn AnyQuery>, registry: Arc<StoreRegistry>, host_id: Uuid| -> Result<FilteredCellMap, String> {
                // Downcast to the QueryRequest wrapper (parser creates QueryRequest<Q>)
                let any_ref: &dyn Any = any_query.as_ref();
                let request: QueryRequest<Q> = any_ref
                    .downcast_ref::<QueryRequest<Q>>()
                    .cloned()
                    .ok_or_else(|| format!("Failed to downcast query to QueryRequest<{}>", Q::query_id_static()))?;

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
                let server_ctx = Arc::new(MykoServerCtx::new(
                    host_id,
                    registry.clone(),
                ));

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
            },
        }
    }
}
