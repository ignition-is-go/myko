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

pub use crate::{
    client::MykoClient,
    command::{
        AnyCommand, CommandContext, CommandHandler, CommandHandlerRegistration, CommandId,
        CommandIdStatic, CommandParams, CommandRequest, CommandResultType,
    },
    common::{to_value::ToValue, with_id::WithId, with_transaction::WithTransaction},
    core::item::{AnyItem, Eventable, ItemParseFn, ItemRegistration},
    mcp::McpServer,
    query::{
        AnyQuery, Query, QueryFactory, QueryHandler, QueryId, QueryIdStatic, QueryItemType,
        QueryParseFn, QueryRegistration, QueryTestCtx,
    },
    report::{
        AnyReport, CountResult, MykoReport, Report, ReportContext, ReportFactory, ReportHandler,
        ReportId, ReportIdStatic, ReportOutputType, ReportParseFn, ReportRegistration,
    },
    search::{EntitySearch, EntitySearchResult, SearchIndex, SearchableRegistration, iter_searchable},
    server::{CellServer, CellServerBuilder, CellServerCtx},
    utils::downcast_item,
    wire::{
        CancelSubscription, CommandError, CommandResponse, MEvent, MEventType, MykoMessage,
        PingData, QueryError, QueryResponse, ReportError, ReportResponse, WrappedCommand,
        WrappedItem, WrappedQuery, WrappedReport,
    },
};
