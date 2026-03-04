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
//! The full server runtime (WebSocket accept loop, Kafka, peer registry) lives
//! in the `myko-server` crate.

pub mod client_registry;
mod client_session;
mod context;
mod handler_registry;
pub mod persister;
mod protocol;
mod relationship_manager;

pub use client_registry::{client_registry, init_client_registry, try_client_registry};
pub use client_session::{ClientSession, PendingQueryResponse, WsWriter};
pub use context::CellServerCtx;
pub use handler_registry::HandlerRegistry;
pub use persister::{BlackholePersister, NullPersister, Persister, PersisterRouter};
pub use protocol::{message_to_json, message_to_msgpack};
pub use relationship_manager::RelationshipManager;
