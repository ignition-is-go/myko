//! # Myko RS - Event-Sourcing CQRS Framework
//!
//! `myko-rs` is an actor-based event-sourcing framework for building real-time,
//! distributed systems with strong consistency guarantees.
//!
//! ## Core Concepts
//!
//! | Concept | Description |
//! |---------|-------------|
//! | **Item** | Base entity with `id` and content-hash (`hash`) for optimistic concurrency |
//! | **Event** | Immutable record of state change: `SET` (create/update) or `DEL` (delete) |
//! | **Query** | Request for live data stream, returns reactive `Observable<T[]>` |
//! | **Report** | Computed/derived data request, returns `Observable<T>` |
//! | **Command** | Intent to mutate state, returns result of operation |
//! | **Saga** | Stateful stream processor that reacts to events and emits commands |
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                             CellServer                                   │
//! │                                                                          │
//! │  WebSocket ──► WsHandler ──► CellServerCtx ──► StoreRegistry             │
//! │      │              │             │                   │                  │
//! │      │              │             │             CellMap<id, item>        │
//! │      │              ▼             │                   │                  │
//! │      │        KafkaProducer       │                   ▼                  │
//! │      │              │             │         Query/Report cells           │
//! │      │              ▼             │                   │                  │
//! │      │           Kafka ◄─────────────── KafkaConsumer                    │
//! │      │                                                                   │
//! │      ◄────────────────────── (subscription updates)                      │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! Define an entity using the `#[myko_item]` attribute macro:
//!
//! ```rust,ignore
//! use myko_rs::prelude::*;
//!
//! #[myko_item]
//! pub struct Target {
//!     pub name: String,
//!     pub category: Option<String>,
//!     // id: Arc<str> and hash: Arc<str> added automatically
//! }
//! ```
//!
//! The macro auto-generates:
//! - `GetAllTargets`, `GetTargetsByIds`, `GetTargetsByQuery` queries
//! - `CountAllTargets`, `CountTargets`, `GetTargetById` reports
//! - `DeleteTarget`, `DeleteTargets` commands
//! - `PartialTarget` struct for partial matching
//! - Registration with the [`inventory`] system
//!
//! ## Module Guide
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`client`] | WebSocket client for connecting to Myko servers |
//! | [`core`] | Core types: command, query, report, saga, item, relationship |
//! | [`wire`] | Wire protocol types: MykoMessage, MEvent, responses, errors |
//! | [`server`] | CellServer and server context |
//! | [`store`] | Entity store and registry |
//!
//! ## Performance
//!
//! Myko-rs is optimized for high-throughput, low-latency scenarios:
//!
//! - **Hypha cells**: Reactive queries and reports using the hypha cell library
//! - **Lock-free stores**: CellMap for concurrent entity access
//! - **MessagePack serialization**: Binary format for efficient WebSocket communication
//! - **Optional Kafka**: Run in-memory for development, add Kafka for production persistence
//!
//! See `libs/myko/rs/OPTIMIZATION.md` for detailed performance guidelines.

// Main module structure
pub mod client;
pub mod codegen;
pub mod core;
pub mod entities;
pub mod search;
pub mod server;
pub mod store;
pub mod utils;
pub mod wire;

pub mod prelude;

#[cfg(feature = "bench")]
pub mod bench_entities;

// Re-export core modules at top level for backwards compatibility
pub use core::{command, common, item, query, relationship, report, request, saga};

// Re-export crates for use in macros
pub use hypha; // For cell-based queries/reports in #[myko_item]
pub use inventory;
pub use inventory::submit; // For myko_rs::submit! macro
pub use ts_rs::{self, TS};
// Re-export wire types at top level for backwards compatibility
pub use wire::event; // For #[derive(myko_rs::TS)]

/// Helper macro for submitting message event registrations
#[macro_export]
macro_rules! submit_message_event {
    ($variant:ident, $event:expr) => {
        inventory::submit!($crate::wire::MessageEventRegistration {
            variant_name: stringify!($variant),
            event_value: $event,
        });
    };
}
