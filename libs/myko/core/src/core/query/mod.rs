//! Query types and registration.

pub mod cell;
mod context;
mod filter;
mod registration;
mod request;
mod traits;

// Re-export all public types
pub use cell::{FilteredCellMap, WindowedQuerySnapshot, WindowedQuerySource};
pub use context::{QueryBuildContext, QueryContext};
pub use filter::{
    BelongsToRoute, CanonicalFilter, CompoundFkExtractor, CompoundKey, EqFilter, Filter,
    Filterable, ID_ROUTE_FIELD_NAMES, IdFilter, LiveFilterQuery, NumericFilter, QueryRoute,
    StringFilter, Unfilterable, in_matches,
};
pub use registration::{
    QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration, QueryRuntimeMetrics,
    QueryRuntimePerIdMetrics, UNION_KEYS_WARN_THRESHOLD, build_belongs_to_source_map,
    build_belongs_to_union_source_map, build_ids_source_map, cartesian_product,
    filter_query_over_source, filter_typed_source, query_live, query_runtime_metrics,
    query_runtime_metrics_by_id, sweep_all_belongs_to_source_indexes,
};
pub use request::QueryRequest;
pub use traits::{
    AnyQuery, Query, QueryBuildArgs, QueryHandler, QueryId, QueryIdStatic, QueryItemType,
    QueryParams, QueryTestContext,
};
