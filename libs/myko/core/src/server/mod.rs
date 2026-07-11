//! Server-side types for the cell-based Myko server.
//!
//! This module contains the tokio-free server types:
//! - `CellServerCtx` — server context for queries, reports, and event publishing
//! - `HandlerRegistry` — registry of item/query/report handlers from inventory
//! - `ClientRegistry` — global WebSocket client writer registry
//! - `ClientSession` — per-connection subscription management
//! - `RelationshipManager` — cascade operations for entity relationships
//! - `Persister` trait — abstraction for event persistence
//! - Protocol serialization helpers
//!
//! The full server runtime (WebSocket accept loop, Postgres, peer registry) lives
//! in the `myko-server` crate.

pub mod client_registry;
mod client_session;
mod context;
pub mod dispatch_metrics;
pub mod entity_set_stats;
mod handler_registry;
pub mod history_replay;
pub mod persister;
mod protocol;
mod relationship_manager;
pub mod report_cache_stats;

pub use client_registry::{client_registry, init_client_registry, try_client_registry};
pub use client_session::{ClientSession, PendingQueryResponse, WsWriter};
pub use context::CellServerCtx;
pub(crate) use context::Origin;
pub use handler_registry::HandlerRegistry;
pub use history_replay::HistoryReplayProvider;
pub use persister::{
    BlackholePersister, NullPersister, PersistError, PersistHealth, Persister, PersisterRouter,
};
pub use protocol::{message_to_cbor, message_to_json};
pub use relationship_manager::RelationshipManager;
