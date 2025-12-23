//! Prelude module - commonly used types for convenience imports.

pub use crate::{
    client::MykoClient,
    command::{
        AnyCommand, CommandContext, CommandHandler, CommandHandlerRegistration, CommandId,
        CommandIdStatic, CommandParams, CommandRequest, CommandResultType,
    },
    common::{to_value::ToValue, with_id::WithId, with_transaction::WithTransaction},
    query::{
        Query, QueryFactory, QueryHandler, QueryHandlerCtx, QueryId, QueryIdStatic, QueryItemType,
        QueryRegistration, RegisterQueryData,
    },
    registry::{
        item::{AnyItem, CapturedItemParser, Eventable, ItemRegistration},
        query::AnyQuery,
    },
    report::{
        AnyReport, CountResult, MykoReport, Report, ReportContext, ReportFactory, ReportHandler,
        ReportId, ReportIdStatic, ReportOutputType, ReportRegistration, RegisterReportData,
    },
    search::{iter_searchable, EntitySearch, EntitySearchResult, SearchableRegistration},
    server::{CellServer, CellServerBuilder, CellServerCtx},
    utils::downcast_item,
    wire::{
        CancelSubscription, CommandError, CommandResponse, MEvent, MEventType, MykoMessage,
        PingData, QueryError, QueryResponse, ReportError, ReportResponse, WrappedCommand,
        WrappedItem, WrappedQuery, WrappedReport,
    },
};

// Re-export hypha cell types for reports
pub use hypha::{Cell, CellImmutable, CellMutable, Gettable, MapExt, Mutable, Watchable};

pub use chrono::Utc;
pub use myko_macros::*;
pub use uuid::Uuid;

// Re-export ts_rs::TS for derive macros
pub use ts_rs::TS;
