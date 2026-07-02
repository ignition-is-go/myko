//! Query types and registration.

#[cfg(not(target_arch = "wasm32"))]
pub mod cell;
mod context;
#[cfg(not(target_arch = "wasm32"))]
mod registration;
mod request;
mod traits;

// Re-export all public types
#[cfg(not(target_arch = "wasm32"))]
pub use cell::FilteredCellMap;
#[cfg(not(target_arch = "wasm32"))]
pub use context::QueryCellContext;
pub use context::QueryContext;
#[cfg(not(target_arch = "wasm32"))]
pub use registration::{
    QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration, QueryRuntimeMetrics,
    QueryRuntimePerIdMetrics, build_belongs_to_source_map, build_ids_source_map,
    filter_query_over_source, query_runtime_metrics, query_runtime_metrics_by_id,
    sweep_all_belongs_to_source_indexes,
};
pub use request::QueryRequest;
#[cfg(not(target_arch = "wasm32"))]
pub use traits::QueryBuildCellCtx;
pub use traits::{
    AnyQuery, Query, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestCtx,
};
