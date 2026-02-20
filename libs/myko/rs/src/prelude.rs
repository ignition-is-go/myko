//! Prelude module - commonly used types for convenience imports.

pub use chrono::Utc;
// Re-export hypha cell types for reports
pub use hypha::{
    Cell, CellImmutable, CellMutable, Gettable, MapExt, Mutable, SelectExt, Watchable,
};
pub use myko_macros::*;
// Re-export ts_rs::TS for derive macros
pub use ts_rs::TS;
pub use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
pub use crate::query::FilteredCellMap;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::query::QueryBuildCellCtx;
pub use crate::{
    client::{
        MykoClient,
        entity_sync::{EntityStoreSync, EntityStoreSyncOptions},
    },
    command::{
        AnyCommand, CommandContext, CommandHandler, CommandHandlerRegistration, CommandId,
        CommandIdStatic, CommandParams, CommandRequest, CommandResultType,
    },
    common::{to_value::ToValue, with_id::WithId, with_transaction::WithTransaction},
    core::item::{AnyItem, Eventable, ItemParseFn, ItemRegistration},
    query::{
        AnyQuery, Query, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams,
        QueryTestCtx,
    },
    report::{
        AnyReport, CountResult, MykoReport, Report, ReportContext, ReportHandler, ReportId,
        ReportIdStatic, ReportOutputType, ReportParams,
    },
    utils::downcast_item,
    view::{
        AnyView, FilteredViewCellMap, TypedViewCellMap, ViewBuildCellCtx, ViewHandler, ViewId,
        ViewIdStatic, ViewItemType, ViewParams, ViewRequest,
    },
    wire::{
        CancelSubscription, CommandError, CommandResponse, MEvent, MEventType, MykoMessage,
        PingData, QueryChange, QueryError, QueryResponse, ReportError, ReportResponse,
        WrappedCommand, WrappedItem, WrappedQuery, WrappedReport,
    },
};
// Server-only re-exports (tokio-free types only; CellServer lives in myko-server)
#[cfg(not(target_arch = "wasm32"))]
pub use crate::{
    query::{QueryCellContext, QueryFactory, QueryParseFn, QueryRegistration},
    report::{ReportFactory, ReportParseFn, ReportRegistration},
    search::{
        EntitySearch, EntitySearchResult, SearchIndex, SearchableRegistration, iter_searchable,
    },
    server::CellServerCtx,
    view::{ViewFactory, ViewRegistration},
};
