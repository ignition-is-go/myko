//! Wire protocol types for WebSocket communication.
//!
//! This module contains all the types used for the WebSocket wire protocol:
//! - `MykoMessage` - the main message enum
//! - `MEvent` - event types
//! - Query/Report/Command request/response/error types
//! - Helper functions for wrapping requests

pub mod command;
pub mod event;
pub mod item;
pub mod message;
pub mod query;
pub mod report;
mod shared;
pub mod view;

#[cfg(test)]
mod cbor_roundtrip_tests;

pub use command::{
    CommandError, CommandResponse, EncodedCommandMessage, WrappedCommand, encode_command_message,
    wrap_command_request,
};
pub use event::{MEvent, MEventType, intern_entity_type};
pub use item::{ErasedWrappedItem, WrappedItem};
pub use message::{CancelSubscription, MessageEventRegistration, MykoMessage, PingData};
pub use query::{
    ClientQueryChange, ClientQueryResponse, QueryChange, QueryError, QueryResponse, QueryResult,
    QueryWindow, QueryWindowUpdate, WrappedQuery, wrap_query,
};
pub use report::{ReportError, ReportResponse, WrappedReport, wrap_report};
pub use view::{ViewError, ViewResponse, ViewWindowUpdate, WrappedView, wrap_view};

// Ensure core wire types are exported to TS bindings for downstream packages.
crate::register_typegen_type!(
    event::MEventType,
    event::MEvent,
    message::CancelSubscription,
    message::PingData,
    message::MykoMessage,
    command::CommandResponse,
    command::WrappedCommand,
    command::CommandError,
    // NOTE(ts): QueryResponse and QueryChange no longer derive TS — manual TS export needed later
    query::WrappedQuery,
    query::QueryError,
    query::QueryWindow,
    query::QueryWindowUpdate,
    item::WrappedItem,
    report::ReportResponse,
    report::WrappedReport,
    report::ReportError,
    view::WrappedView,
    view::ViewError,
    view::ViewWindowUpdate,
    serde_json::Value
);
