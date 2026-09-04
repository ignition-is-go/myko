//! Command types and registration.

mod handler;
mod registration;
mod request;
mod traits;

// Re-export handler types (available on all platforms for entity impls)
pub use handler::{
    CommandContext, CommandExecutorAdapter, CommandExecutorFactory, CommandHandler,
    CommandHandlerRegistration, DynCommandExecutor,
};
#[cfg(not(target_arch = "wasm32"))]
pub use handler::{DurableCommandExecutor, durable_command_executor};
// Re-export registration types (server-only)
pub use registration::CommandRegistration;
// Re-export request type
pub use request::CommandRequest;
// Re-export traits
pub use traits::{
    AnyCommand, CommandId, CommandIdStatic, CommandParams, CommandResultType, MykoCommand,
};

// Re-export wire types for backwards compatibility
pub use crate::wire::{CommandError, CommandResponse, WrappedCommand, wrap_command_request};
