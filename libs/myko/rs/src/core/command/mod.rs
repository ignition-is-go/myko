//! Command types and registration.

mod handler;
mod registration;
mod request;
mod traits;

// Re-export handler types
pub use handler::{
    CommandContext, CommandExecutorAdapter, CommandExecutorFactory, CommandHandler,
    CommandHandlerRegistration, DynCommandExecutor,
};

// Re-export registration types
pub use registration::CommandRegistration;

// Re-export request type
pub use request::CommandRequest;

// Re-export traits
pub use traits::{
    AnyCommand, CommandId, CommandIdStatic, CommandParams, CommandResultType, MykoCommand,
};

// Re-export wire types for backwards compatibility
pub use crate::wire::{wrap_command_request, CommandError, CommandResponse, WrappedCommand};
