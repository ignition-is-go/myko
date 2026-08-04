//! Prelude module - commonly used types for convenience imports.

pub use chrono::Utc;
// Re-export hyphae cell types for reports
pub use hyphae::{
    Cell, CellImmutable, CellMutable, CountByExt, Gettable, GroupByExt, MapExt, MapQuery,
    MaterializeDefinite, MaterializeEmpty, Mutable, Pipeline, ProjectCellExt, ProjectMapExt,
    SelectCellExt, SelectExt, Watchable,
};
pub use myko_macros::*;
pub use uuid::Uuid;

// Re-export TS for derive macros — conditional on the `ts-export` feature
// at the lib.rs level, so downstream derives go through the same switch.
pub use crate::TS;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::client::entity_sync::{EntityStoreSync, EntityStoreSyncOptions};
// Handler capability traits — bring the scoped methods (tx/registry, emit_* +
// execute_command on command handlers, and on native
// query_map/report/view/search/peer_*/replay_store) into scope for handler
// bodies. See `core::capability`.
pub use crate::core::capability::{
    CommandSending, EventPublishing, Querying, RegistryScoped, Reporting, RequestScoped, Searching,
    ServerScoped,
};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::core::capability::{PeerAccess, Replaying, Viewing};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::query::FilteredCellMap;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::query::QueryBuildArgs;
pub use crate::{
    cache::{
        CacheKey, serde_content_hash, write_hash_cache_key, write_serde_cache_key, write_str_key,
    },
    client::MykoClient,
    command::{
        AnyCommand, CommandContext, CommandHandler, CommandHandlerRegistration, CommandId,
        CommandIdStatic, CommandParams, CommandRequest, CommandResultType,
    },
    common::{
        to_value::ToValue,
        with_id::{WithId, WithTypedId},
        with_transaction::WithTransaction,
    },
    core::{
        common::content_hash::ContentHash,
        item::{
            AnyItem, Eventable, IngestBufferPolicy, IngestBufferRegistration, ItemParseFn,
            ItemRegistration,
        },
    },
    query::{
        AnyQuery, EqFilter, Filter, Filterable, IdFilter, NumericFilter, Query, QueryHandler,
        QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestContext, StringFilter,
        Unfilterable, in_matches,
    },
    report::{
        AnyReport, CountResult, MykoReport, Report, ReportContext, ReportHandler, ReportId,
        ReportIdStatic, ReportOutputType, ReportParams,
    },
    utils::downcast_item,
    view::{
        AnyView, FilteredViewCellMap, TypedViewCellMap, ViewBuildArgs, ViewHandler, ViewId,
        ViewIdStatic, ViewItemType, ViewParams, ViewRequest,
    },
    wire::{
        CancelSubscription, CommandError, CommandResponse, ErasedWrappedItem, MEvent, MEventType,
        MykoMessage, PingData, QueryChange, QueryError, QueryResponse, ReportError, ReportResponse,
        WrappedCommand, WrappedItem, WrappedQuery, WrappedReport,
    },
};
// Server-only re-exports (tokio-free types only; MykoServer lives in myko-server)
#[cfg(not(target_arch = "wasm32"))]
pub use crate::{
    query::{QueryBuildContext, QueryFactory, QueryParseFn, QueryRegistration},
    report::{ReportFactory, ReportParseFn, ReportRegistration},
    search::{
        EntitySearch, EntitySearchResult, SearchIndex, SearchableRegistration, iter_searchable,
    },
    server::MykoServerContext,
    view::{ViewFactory, ViewRegistration},
};
