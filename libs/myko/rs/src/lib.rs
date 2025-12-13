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
//! │                             MykoServer                                    │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
//! │  │EventManager │  │QueryManager │  │ReportManager│  │ CommandManager  │  │
//! │  │             │  │             │  │             │  │                 │  │
//! │  │ EventHandler│  │QueryHandler │  │ReportHandler│  │ CommandHandler  │  │
//! │  │ EventHandler│  │QueryRunner  │  │ReportRunner │  │ (user-defined)  │  │
//! │  │ ...         │  │...          │  │...          │  │                 │  │
//! │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘  │
//! │                          │                                               │
//! │                    ┌─────┴─────┐                                         │
//! │                    │ EventBus  │ ← Lock-free broadcast                   │
//! │                    └─────┬─────┘                                         │
//! │            ┌─────────────┼─────────────┐                                 │
//! │            ▼             ▼             ▼                                 │
//! │     ┌──────────┐  ┌──────────┐  ┌────────────────────┐                   │
//! │     │SagaRunner│  │SagaRunner│  │RelationshipManager │                   │
//! │     └──────────┘  └──────────┘  └────────────────────┘                   │
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
//! | [`actors`] | Actor system: event, query, report, command managers |
//! | [`client`] | WebSocket client for connecting to Myko servers |
//! | [`command`] | Command types and traits |
//! | [`event`] | Event types (`MEvent`, `MEventType`) |
//! | [`item`] | Base item trait and types |
//! | [`query`] | Query types and traits |
//! | [`relationship`] | Entity relationship system (`#[belongs_to]`, `#[owns_many]`, `#[ensure_for]`) |
//! | [`report`] | Report types and traits |
//! | [`saga`] | Reactive event processors with stream operators |
//! | [`server`] | Server context and configuration |
//!
//! ## Performance
//!
//! Myko-rs is optimized for high-throughput, low-latency scenarios:
//!
//! - **Direct actor references**: EventHandlers bypass Server routing for O(1) message delivery
//! - **Lock-free EventBus**: Broadcast to sagas without blocking event processing
//! - **MessagePack serialization**: Binary format for efficient WebSocket communication
//! - **Optional Kafka**: Run in-memory for development, add Kafka for production persistence
//!
//! See `libs/myko/rs/OPTIMIZATION.md` for detailed performance guidelines.

pub mod actors;
pub mod api;
pub mod client;
pub mod command;
pub mod common;
pub mod context;
pub mod entities;
pub mod event;
pub mod item;
pub mod message;
pub mod parsers;
pub mod query;
pub mod relationship;
pub mod report;
pub mod saga;
pub mod search;
pub mod server;
pub mod utils;

pub mod prelude;
pub mod type_gen;

#[cfg(feature = "bench")]
pub mod bench_entities;
// Re-export crates for use in macros
pub use inventory;
pub use inventory::submit;  // For myko_rs::submit! macro
pub use ts_rs;
pub use ts_rs::TS;  // For #[derive(myko_rs::TS)]

/// Helper macro for submitting message event registrations
#[macro_export]
macro_rules! submit_message_event {
    ($variant:ident, $event:expr) => {
        inventory::submit!($crate::message::MessageEventRegistration {
            variant_name: stringify!($variant),
            event_value: $event,
        });
    };
}
