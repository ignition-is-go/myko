//! Core saga traits and types for reactive event processing.
//!
//! Sagas are stateful stream processors that react to events and emit commands
//! on state transitions. Each saga is an actor that owns its accumulated state.

use std::sync::Arc;

use crate::{command::WrappedCommand, event::MEvent};

/// Type alias for the event stream input to sagas
pub type EventStream = futures::stream::BoxStream<'static, MEvent>;

/// Type alias for the command stream output from sagas
pub type CommandStream = futures::stream::BoxStream<'static, WrappedCommand>;

/// Trait for saga handlers that process events and emit commands.
///
/// Sagas are stateful stream processors that:
/// - Subscribe to a filtered stream of events
/// - Accumulate state across events (via `scan`, `pairwise`, etc.)
/// - Emit commands when state transitions occur
///
/// # Example
///
/// ```ignore
/// #[myko_saga]
/// pub struct CleanupSaga;
///
/// impl Saga for CleanupSaga {
///     type State = ();
///
///     fn name() -> &'static str {
///         "CleanupSaga"
///     }
///
///     fn build(events: EventStream, _ctx: Arc<SagaContext>) -> CommandStream {
///         Box::pin(events
///             .of_items::<LogEntry>()
///             .of_type(ChangeType::Set)
///             .filter_map(|event| async move {
///                 // Cleanup logic
///                 None
///             }))
///     }
/// }
/// ```
pub trait Saga: Send + Sync + 'static {
    /// Accumulated state type for this saga.
    /// Use `()` for stateless sagas.
    type State: Default + Send + Sync + 'static;

    /// Human-readable name for logging and debugging
    fn name() -> &'static str
    where
        Self: Sized;

    /// Build the event processing pipeline.
    ///
    /// This method receives a stream of all events and should filter/transform
    /// them into commands to execute.
    fn build(
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> CommandStream
    where
        Self: Sized;
}

/// Type-erased saga trait for dynamic dispatch.
///
/// This allows storing different saga types in a single collection
/// without knowing the concrete types at compile time.
pub trait AnySaga: Send + Sync + 'static {
    /// Get the saga's name
    fn name(&self) -> &'static str;

    /// Build the event processing pipeline
    fn build_boxed(
        &self,
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> CommandStream;

    /// Create the initial state for this saga
    fn create_state(&self) -> Box<dyn std::any::Any + Send + Sync>;
}

/// Blanket implementation of AnySaga for all Saga types
impl<S: Saga> AnySaga for S {
    fn name(&self) -> &'static str {
        S::name()
    }

    fn build_boxed(
        &self,
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> CommandStream {
        S::build(events, ctx)
    }

    fn create_state(&self) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(S::State::default())
    }
}

/// Registration entry for saga discovery via inventory.
///
/// The `#[myko_saga]` proc macro generates these registrations automatically.
pub struct SagaRegistration {
    /// Unique identifier for this saga
    pub saga_id: &'static str,

    /// Factory function to create an instance of the saga
    pub create: fn() -> Arc<dyn AnySaga>,
}

// Collect all saga registrations via inventory
inventory::collect!(SagaRegistration);
