//! Event processing system for the Myko actor framework.
//!
//! This module provides the core event infrastructure for handling entity state changes
//! in a distributed, event-sourced architecture.
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────┐
//!                     │    EventManager     │ ← Coordinates all event handlers
//!                     └──────────┬──────────┘
//!                                │
//!        ┌───────────────────────┼───────────────────────┐
//!        ▼                       ▼                       ▼
//! ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
//! │EventHandler │         │EventHandler │         │EventHandler │
//! │  (Target)   │         │  (Scene)    │         │ (Binding)   │
//! └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
//!        │                       │                       │
//!        └───────────────────────┼───────────────────────┘
//!                                ▼
//!                         ┌─────────────┐
//!                         │  EventBus   │ ← Lock-free broadcast to sagas
//!                         └─────────────┘
//! ```
//!
//! # Components
//!
//! - [`EventManager`](event_manager::EventManager): Central coordinator that routes events
//!   to type-specific handlers and signals initialization completion.
//!
//! - [`EventHandler`](event_handler::EventHandler): Per-entity-type actor that maintains
//!   in-memory state, handles persistence to Kafka, and forwards updates to QueryManager.
//!
//! - [`EventBus`]: High-throughput lock-free broadcast channel for distributing events
//!   to saga subscribers without blocking the main event path.
//!
//! # Data Flow
//!
//! 1. Client sends a command that results in an event (SET or DEL)
//! 2. EventManager receives the event and:
//!    - Publishes to EventBus for saga subscribers
//!    - Routes to the appropriate EventHandler by item type
//! 3. EventHandler:
//!    - Parses the item using its registered parser
//!    - Produces to Kafka (if configured)
//!    - Updates its in-memory store
//!    - Notifies QueryManager directly (bypasses Server routing for performance)
//!
//! # Initialization Sequence
//!
//! 1. EventManager spawns EventHandlers for each registered entity type
//! 2. Each EventHandler spawns a KafkaConsumer to replay events from topic
//! 3. When KafkaConsumer catches up, EventHandler signals `PersisterCaughtUp`
//! 4. EventHandler forwards `RepoInitComplete` to EventManager
//! 5. When all handlers are ready, EventManager sends `AllInitComplete` to Server

pub mod common;
pub mod event_bus;
pub mod event_handler;
pub mod event_manager;

pub use event_bus::{EventBus, EventBusStream, EventBusSubscriber};
