//! Command trait definitions.

use std::{fmt::Debug, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{client::MykoClient, common::with_transaction::WithTransaction, wire::WrappedCommand};

// ─────────────────────────────────────────────────────────────────────────────
// Core Command Traits
// ─────────────────────────────────────────────────────────────────────────────

pub trait CommandId {
    fn command_id(&self) -> Arc<str>;
}

/// Static command ID for registration
pub trait CommandIdStatic {
    /// The command ID as a const string (usable in static contexts)
    const COMMAND_ID: &'static str;

    /// Get the command ID (convenience method, defaults to COMMAND_ID)
    fn command_id_static() -> &'static str {
        Self::COMMAND_ID
    }
}

/// Result type for a command
pub trait CommandResultType {
    type Result: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
}

/// Type-erased command trait for dynamic dispatch.
/// All commands implement this via the `#[myko_command]` macro.
pub trait AnyCommand: WithTransaction + CommandId + Debug + Send + Sync + 'static {
    /// Serialize this command to a JSON Value.
    fn to_value(&self) -> Value;
}

// A command that can be sent; implementors provide a response type via the macro.
pub trait MykoCommand<T> {
    fn handle(&self, client: &MykoClient) -> impl std::future::Future<Output = Result<T, String>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// CommandParams - Marker trait for command parameter structs (inner type)
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for command parameter structs.
///
/// This is implemented by the user-defined command struct (e.g., `CreateTarget`).
/// It combines identity traits without requiring transaction metadata.
///
/// The full `Command` trait is implemented on `CommandRequest<C>` where `C: CommandParams`.
pub trait CommandParams:
    Serialize
    + DeserializeOwned
    + Clone
    + Send
    + Sync
    + CommandId
    + CommandIdStatic
    + CommandResultType
    + Debug
    + 'static
{
}

// Blanket impl for any type that satisfies the bounds
impl<T> CommandParams for T where
    T: Serialize
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + CommandId
        + CommandIdStatic
        + CommandResultType
        + Debug
        + 'static
{
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversions
// ─────────────────────────────────────────────────────────────────────────────

// Conversion from Arc<dyn AnyCommand> to WrappedCommand
impl From<&dyn AnyCommand> for WrappedCommand {
    fn from(command: &dyn AnyCommand) -> Self {
        WrappedCommand {
            command: command.to_value(),
            command_id: command.command_id().to_string(),
        }
    }
}

impl From<Arc<dyn AnyCommand>> for WrappedCommand {
    fn from(command: Arc<dyn AnyCommand>) -> Self {
        WrappedCommand::from(command.as_ref())
    }
}

impl From<&Arc<dyn AnyCommand>> for WrappedCommand {
    fn from(command: &Arc<dyn AnyCommand>) -> Self {
        WrappedCommand::from(command.as_ref())
    }
}
