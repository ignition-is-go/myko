//! Cell-based WebSocket module
//!
//! Provides WebSocket infrastructure backed by hypha cells for reactive
//! query and report subscriptions.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    ClientSession (per WebSocket)                │
//! │  - Holds SubscriptionGuards (dropped on disconnect)             │
//! │  - Direct WebSocket write (no actor routing)                    │
//! │                                                                 │
//! │  tx_1 -> SubscriptionGuard (query cell subscription)            │
//! │  tx_2 -> SubscriptionGuard (report cell subscription)           │
//! │  tx_3 -> ...                                                    │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! When a client disconnects, the ClientSession drops, all SubscriptionGuards
//! are dropped, and subscriptions are automatically cleaned up.

mod client_session;
mod protocol;

pub use client_session::{ClientSession, WsWriter};
pub use protocol::{
    message_to_json, message_to_msgpack, CancelSubscription, MykoMessage, QueryError,
    QueryResponse, ReportError, ReportResponse,
};
