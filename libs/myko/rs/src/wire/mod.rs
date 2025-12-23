//! Wire protocol types for WebSocket communication.
//!
//! This module contains all the types used for the WebSocket wire protocol:
//! - `MykoMessage` - the main message enum
//! - Query/Report/Command request/response/error types
//! - Helper functions for wrapping requests

pub mod command;
pub mod message;
pub mod query;
pub mod report;

pub use command::{wrap_command_request, CommandError, CommandResponse, WrappedCommand};
pub use message::{CancelSubscription, MessageEventRegistration, MykoMessage, PingData};
pub use query::{wrap_query, QueryError, QueryResponse, QueryResult, WrappedQuery};
pub use report::{wrap_report, ReportError, ReportResponse, WrappedReport};

// Re-export deprecated function for backwards compat
#[allow(deprecated)]
pub use command::wrap_command;
