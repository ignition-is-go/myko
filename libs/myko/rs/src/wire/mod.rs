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

// Re-export deprecated function for backwards compat
#[allow(deprecated)]
pub use command::wrap_command;
pub use command::{CommandError, CommandResponse, WrappedCommand, wrap_command_request};
pub use event::{EventOptions, MEvent, MEventType};
pub use item::WrappedItem;
pub use message::{CancelSubscription, MessageEventRegistration, MykoMessage, PingData};
pub use query::{QueryError, QueryResponse, QueryResult, WrappedQuery, wrap_query};
pub use report::{ReportError, ReportResponse, WrappedReport, wrap_report};
