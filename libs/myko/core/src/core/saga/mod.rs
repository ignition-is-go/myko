//! Saga framework for reactive event processing.
//!
//! Sagas are stateful stream processors that react to events and emit commands
//! on state transitions. Each saga is an actor that owns its accumulated state.
//!
//! # Overview
//!
//! - **Event-driven**: Sagas subscribe to a stream of events
//! - **Stateful**: Sagas can accumulate state across events using `scan`, `pairwise`, etc.
//! - **Reactive**: Sagas emit commands when state transitions occur
//! - **Actor-based**: Each saga runs as an isolated actor with its own state
//!
//! # Example
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use myko::event::MEventType;
//! use myko::saga::{EventStream, SagaStreamExt};
//!
//! let events: EventStream = futures::stream::empty().boxed();
//! let _pipeline = events
//!     .of_item_type("Scene")
//!     .of_change_type(MEventType::SET)
//!     .pairwise();
//! ```
//!
//! # Stream Operators
//!
//! The `SagaStreamExt` trait provides RxJS-like operators:
//!
//! | Operator | Purpose |
//! |----------|---------|
//! | `of_item_type(name)` | Filter by item type |
//! | `of_change_type(SET/DEL)` | Filter by change type |
//! | `pairwise()` | Compare prev/current for transitions |
//! | `scan(initial, f)` | Accumulate state across events |

#[cfg(not(target_arch = "wasm32"))]
mod context;
mod stream;
mod traits;

#[cfg(not(target_arch = "wasm32"))]
pub use context::{SagaContext, SagaError};
pub use stream::{
    OfChangeType, OfItemType, Pairwise, SagaStreamExt, Scan, is_change_type, is_item_type,
};
pub use traits::{
    AnySaga, CommandStream, EventStream, Saga, SagaCommand, SagaHandler, SagaRegistration,
};
