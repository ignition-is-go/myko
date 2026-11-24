//! Query types and registration.

pub mod cell;
mod context;
mod filter;
mod registration;
mod request;
mod source;
mod traits;
pub use source::{EntityQuerySourceRegistration, has_entity_source, registered_entity_source};

// Re-export all public types
pub use crate::server::{EntityHistory, HistoryEvent};
pub use cell::{FilteredCellMap, WindowedQuerySnapshot, WindowedQuerySource};
pub use context::{QueryBuildContext, QueryContext};
pub use filter::{
    BelongsToRoute, CanonicalFilter, CompoundFkExtractor, CompoundKey, ContainsFilter, EqFilter,
    EqualityKeys, Filter, Filterable, ID_ROUTE_FIELD_NAMES, IdFilter, IndexKey, IndexKeysExtractor,
    KeyExtractor, KeyFilter, LiveFilterQuery, NumericFilter, QueryRoute, StringFilter,
    Unfilterable, in_matches, serde_index_key,
};
pub use registration::{
    QueryCellFactory, QueryFactory, QueryParseFn, QueryRegistration, QueryRuntimeMetrics,
    QueryRuntimePerIdMetrics, QueryWindowCellFactory, UNION_KEYS_WARN_THRESHOLD,
    belongs_to_route_snapshot, build_belongs_to_source_map, build_belongs_to_union_source_map,
    build_ids_source_map, build_index_union_source_map, cartesian_product,
    filter_query_over_source, filter_typed_source, query_live, query_live_in_context,
    query_runtime_metrics, query_runtime_metrics_by_id, route_snapshot_counts,
    sweep_belongs_to_source_indexes,
};
pub use request::QueryRequest;
pub use traits::{
    AnyQuery, Query, QueryBuildArgs, QueryHandler, QueryId, QueryIdStatic, QueryItemType,
    QueryParams, QueryTestContext, QueryWindowBuildArgs,
};
