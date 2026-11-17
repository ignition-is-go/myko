//! Core saga traits and types for reactive event processing.
//!
//! Sagas are stream processors that react to events and emit typed commands.

use std::sync::Arc;

use futures::StreamExt;

use crate::{
    command::{CommandContext, CommandError, CommandHandler, CommandIdStatic},
    event::{MEvent, MEventType},
    item::Eventable,
};

/// Type alias for the event stream input to sagas.
pub type EventStream = futures::stream::BoxStream<'static, MEvent>;

/// Type alias for type-erased saga commands.
pub type CommandStream = futures::stream::BoxStream<'static, Box<dyn SagaCommand>>;

/// Type-erased executable command produced by a saga.
pub trait SagaCommand: Send + Sync + 'static {
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn execute_boxed(self: Box<Self>, ctx: CommandContext) -> Result<(), CommandError>;
    fn command_name(&self) -> &'static str;

    /// Transaction of the event that caused this command, when known.
    fn originating_tx(&self) -> Option<&Arc<str>> {
        None
    }
}

/// Retains an event's transaction across the asynchronous saga command stream.
pub struct CausedSagaCommand<C> {
    command: C,
    tx: Arc<str>,
    causation: Option<crate::store::CommandCausation>,
}

impl<C> CausedSagaCommand<C> {
    pub const fn from_causation(
        command: C,
        tx: Arc<str>,
        causation: crate::store::CommandCausation,
    ) -> Self {
        Self {
            command,
            tx,
            causation: Some(causation),
        }
    }

    pub const fn new(command: C, tx: Arc<str>) -> Self {
        Self {
            command,
            tx,
            causation: None,
        }
    }
}

impl<C: SagaCommand> SagaCommand for CausedSagaCommand<C> {
    fn execute_boxed(self: Box<Self>, mut ctx: CommandContext) -> Result<(), CommandError> {
        // The stream may outlive its event handler and run on another thread.
        Arc::make_mut(&mut ctx.req).tx = self.tx;
        if let Some(causation) = &self.causation {
            ctx.inherit_causation(causation);
        }
        Box::new(self.command).execute_boxed(ctx)
    }

    fn command_name(&self) -> &'static str {
        self.command.command_name()
    }

    fn originating_tx(&self) -> Option<&Arc<str>> {
        Some(&self.tx)
    }
}

impl<C> SagaCommand for C
where
    C: CommandHandler + CommandIdStatic + Send + Sync + 'static,
{
    fn execute_boxed(self: Box<Self>, ctx: CommandContext) -> Result<(), CommandError> {
        let _ = (*self).execute(ctx)?;
        Ok(())
    }

    fn command_name(&self) -> &'static str {
        C::COMMAND_ID
    }
}

/// Trait for saga handlers that process events and emit commands.
pub trait Saga: Send + Sync + 'static {
    /// Accumulated state type for this saga. Use `()` for stateless sagas.
    type State: Default + Send + Sync + 'static;

    /// Typed command emitted by this saga.
    type Command: SagaCommand;

    /// Human-readable name for logging and debugging.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Build the event processing pipeline.
    fn build(
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> futures::stream::BoxStream<'static, Self::Command>
    where
        Self: Sized;
}

/// Ergonomic handler trait for macro-declared sagas.
///
/// `#[myko_saga(...)]` can generate `Saga` + registration boilerplate while this trait
/// contains the typed business logic.
pub trait SagaHandler: Send + Sync + 'static {
    /// Event item type consumed by this saga.
    type EventItem: Eventable;
    /// Command type emitted by this saga.
    type Command: SagaCommand;
    /// Event change type consumed by this saga.
    const EVENT_TYPE: MEventType;
    /// Handle one matching event and optionally emit a command.
    fn handle(
        item: Self::EventItem,
        event: MEvent,
        ctx: Arc<super::context::SagaContext>,
    ) -> Option<Self::Command>;
}

/// Type-erased saga trait for dynamic dispatch.
pub trait AnySaga: Send + Sync + 'static {
    /// Get the saga's name.
    fn name(&self) -> &'static str;

    /// Build the event processing pipeline.
    fn build_boxed(
        &self,
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> CommandStream;

    /// Create the initial state for this saga.
    fn create_state(&self) -> Box<dyn std::any::Any + Send + Sync>;
}

impl<S: Saga> AnySaga for S {
    fn name(&self) -> &'static str {
        S::name()
    }

    fn build_boxed(
        &self,
        events: EventStream,
        ctx: Arc<super::context::SagaContext>,
    ) -> CommandStream {
        Box::pin(S::build(events, ctx).map(|cmd| -> Box<dyn SagaCommand> { Box::new(cmd) }))
    }

    fn create_state(&self) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(S::State::default())
    }
}

/// Registration entry for saga discovery via inventory.
pub struct SagaRegistration {
    /// Unique identifier for this saga.
    pub saga_id: &'static str,
    /// Factory function to create an instance of the saga.
    pub create: fn() -> Arc<dyn AnySaga>,
    /// Entity type this saga listens for (for dispatch filtering).
    pub event_entity_type: &'static str,
    /// Change type this saga listens for (for dispatch filtering).
    pub event_change_type: MEventType,
}

inventory::collect!(SagaRegistration);
