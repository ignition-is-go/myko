//! Server-side types for the cell-based Myko server.
//!
//! This module contains the tokio-free server types:
//! - `MykoServerContext` — server context for queries, reports, and event publishing
//! - `HandlerRegistry` — registry of item/query/report handlers from inventory
//! - `ClientRegistry` — global connected-client sink registry
//! - `ClientSession` — per-connection subscription management
//! - `RelationshipManager` — cascade operations for entity relationships
//! - `Persister` trait — abstraction for event persistence
//! - Protocol serialization helpers
//!
//! The full server runtime (WebSocket accept loop, Postgres, peer registry) lives
//! in the `myko-server` crate.
//!
//! # wasm32
//!
//! This module *compiles* for wasm32 so that `core::capability` can be
//! target-independent — every handler capability reads through
//! `ServerScoped::__server_ctx()`, so a native-only `MykoServerContext` would
//! force a `#[cfg]` onto every capability and, transitively, onto every
//! hand-written handler in consumer entity crates (which do compile to wasm32
//! via the leptos UI cdylibs). Compiling here is what keeps that boilerplate
//! out of consumer apps.
//!
//! It is NOT expected to *run* there. A few call sites compile on wasm32 but
//! panic if executed — `thread::spawn`/`thread::sleep` in `context.rs`
//! (ingest-buffer flush window), `report_cache_stats.rs` and
//! `entity_set_stats.rs` (periodic stats windows), and `Instant::now()` in
//! `persister.rs` and `client_session.rs` (no monotonic clock on
//! wasm32-unknown-unknown). Nothing reaches them today, because constructing a
//! `MykoServerContext` needs a persister and handler registry that no wasm
//! build sets up. If server code ever genuinely runs on wasm, these are the
//! sites to fix first: the timers want a `spawn_after` shim and the clocks
//! want `web_time::Instant`.

pub mod client_registry;
mod client_session;
mod context;
pub mod dispatch_metrics;
pub mod entity_set_stats;
#[cfg(not(target_arch = "wasm32"))]
mod federated_session;
#[cfg(not(target_arch = "wasm32"))]
pub mod federated_source;
mod handler_registry;
pub mod history_replay;
pub mod persister;
mod protocol;
mod relationship_manager;
pub mod report_cache_stats;

pub use client_registry::{client_registry, init_client_registry, try_client_registry};
pub use client_session::{ClientSession, PendingQueryResponse, SessionSink};
pub(crate) use context::Origin;
pub use context::{CausalDiagnostics, CausalLimits, MykoServerContext, MykoServerRuntime};
#[cfg(not(target_arch = "wasm32"))]
pub use federated_session::{
    FederatedSession, NodeFrameStream, NodeRequestRouter, NodeRouteFuture,
};
#[cfg(not(target_arch = "wasm32"))]
pub use federated_source::{SourcedItem, SourcedItemKey, SourcedItemMap, SourcedItemSnapshot};
#[cfg(not(target_arch = "wasm32"))]
pub use handler_registry::HandlerAuthority;
pub use handler_registry::HandlerRegistry;
pub use history_replay::{
    CommittedHistoryEvent, EntityHistory, HistoryEntityKey, HistoryEvent, HistoryPage,
    HistoryReplayProvider,
};
pub use persister::{
    BlackholePersister, NullPersister, PersistError, PersistHealth, Persister, PersisterRouter,
};
pub use protocol::{message_to_cbor, message_to_json};
pub use relationship_manager::RelationshipManager;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native_map;
