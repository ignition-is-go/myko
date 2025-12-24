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
//! ```ignore
//! use myko_rs::saga::{Saga, SagaContext, EventStream, CommandStream};
//! use myko_rs::saga::stream::SagaStreamExt;
//!
//! #[myko_saga]
//! pub struct StatusTransitionSaga;
//!
//! impl Saga for StatusTransitionSaga {
//!     type State = ();
//!
//!     fn name() -> &'static str {
//!         "StatusTransitionSaga"
//!     }
//!
//!     fn build(events: EventStream, _ctx: Arc<SagaContext>) -> CommandStream {
//!         Box::pin(events
//!             .of_item_type("Scene")
//!             .of_change_type(MEventType::SET)
//!             .pairwise()
//!             .filter_map(|(prev, curr)| async move {
//!                 // Detect status changes
//!                 if prev.item["status"] != curr.item["status"] {
//!                     Some(NotifyStatusChange { ... }.into())
//!                 } else {
//!                     None
//!                 }
//!             }))
//!     }
//! }
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

mod context;
mod stream;
mod traits;

pub use context::{SagaContext, SagaError};
pub use stream::{
    OfChangeType, OfItemType, Pairwise, SagaStreamExt, Scan, is_change_type, is_item_type,
};
pub use traits::{AnySaga, CommandStream, EventStream, Saga, SagaRegistration};
