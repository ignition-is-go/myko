//! # Crossbeam-based Actor Runtime
//!
//! A sync actor system built on crossbeam channels for high-throughput,
//! low-latency message processing with true OS-thread parallelism.
//!
//! ## Design Principles
//!
//! - **Sync handlers**: No async, borrow checker works naturally
//! - **OS threads**: True parallelism, not just concurrency
//! - **Composable primitives**: Build complex topologies from simple parts
//! - **Per-key ordering**: Events for the same entity processed in order
//!
//! ## Primitives
//!
//! | Primitive | Parallelism | Ordering | Use Case |
//! |-----------|-------------|----------|----------|
//! | `Actor` | None | Total | Single-threaded state machine |
//! | `Pool` | Yes | None | Embarrassingly parallel work |
//! | `Sharded` | Yes | Per-key | Parallel with per-entity ordering |
//! | `Broadcast` | Fan-out | Per-subscriber | Multi-consumer dispatch |
//!
//! ## Example
//!
//! ```rust,ignore
//! use myko_rs::runtime::{Sharded, Sink};
//!
//! // Process events with per-entity ordering, parallel across entities
//! let events = Sharded::spawn(num_cpus::get(), |event: Event| {
//!     // This runs on OS threads, borrow checker works normally
//!     process_event(event);
//! });
//!
//! // Send with routing key
//! events.send_keyed(&event.entity_id, event)?;
//! ```

mod actor;
mod broadcast;
mod error;
mod pool;
mod sharded;
mod sink;

pub use actor::{Actor, ActorHandle, ActorRef, CallError};
pub use broadcast::{Broadcast, BroadcastBuilder};
pub use error::SendError;
pub use pool::{Pool, PoolHandle};
pub use sharded::{Sharded, ShardedHandle};
pub use sink::{KeyedSink, Sink};

/// Type alias for RPC reply ports, compatible with ractor API style.
/// Use this in message enums for request-response patterns.
pub type RpcReplyPort<T> = crossbeam::channel::Sender<T>;

/// Spawn functions for convenience.
pub mod spawn {
    pub use super::actor::spawn_fn;
    pub use super::pool::{spawn as pool, spawn_with_state as pool_with_state};
    pub use super::sharded::{spawn as sharded, spawn_with_state as sharded_with_state};
}
