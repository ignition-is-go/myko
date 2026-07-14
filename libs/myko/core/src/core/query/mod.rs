//! Query types and registration.

#[cfg(not(target_arch = "wasm32"))]
pub mod cell;
mod context;
mod filter;
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
pub use filter::{
    BelongsToRoute, CanonicalFilter, CompoundFkExtractor, CompoundKey, EqFilter, Filter,
    Filterable, IN_HASH_THRESHOLD, IdFilter, LiveFilterQuery, NumericFilter, StringFilter,
    Unfilterable, in_matches,
};
#[cfg(not(target_arch = "wasm32"))]
pub use registration::{
    QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration, QueryRuntimeMetrics,
    QueryRuntimePerIdMetrics, UNION_KEYS_WARN_THRESHOLD, build_belongs_to_source_map,
    build_belongs_to_union_source_map, build_ids_source_map, cartesian_product,
    filter_query_over_source, query_live, query_runtime_metrics, query_runtime_metrics_by_id,
    sweep_all_belongs_to_source_indexes,
};
pub use request::QueryRequest;
#[cfg(not(target_arch = "wasm32"))]
pub use traits::QueryBuildCellCtx;
pub use traits::{
    AnyQuery, Query, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestCtx,
};
