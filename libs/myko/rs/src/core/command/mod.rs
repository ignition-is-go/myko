mod handler;

use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

use crate::{client::MykoClient, common::with_transaction::WithTransaction};

pub use handler::{
    BoxFuture, CommandContext, CommandHandler, CommandHandlerFactory, CommandHandlerRegistration,
};

// ─────────────────────────────────────────────────────────────────────────────
// CommandRequest<C> - Generic wrapper that adds tx to any command params
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps command parameters with transaction metadata.
///
/// This type adds `tx` (transaction ID) to any command parameters struct.
/// Uses `#[serde(flatten)]` to serialize as a flat structure.
///
/// # Example
///
/// ```ignore
/// // Command params (what user defines):
/// #[myko_command(Target)]
/// pub struct CreateTarget {
///     pub name: String,
/// }
///
/// // Create a request:
/// let request = CommandRequest::new(CreateTarget { name: "foo".into() });
///
/// // Serializes to: { "tx": "...", "name": "foo" }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommandRequest<C> {
    pub tx: Arc<str>,
    #[serde(flatten)]
    pub command: C,
}

impl<C> CommandRequest<C> {
    /// Create a new command request with auto-generated tx.
    pub fn new(command: C) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            command,
        }
    }

    /// Create a new command request with a specific tx.
    pub fn with_tx(command: C, tx: Arc<str>) -> Self {
        Self { tx, command }
    }
}

impl<C: Default> Default for CommandRequest<C> {
    fn default() -> Self {
        Self::new(C::default())
    }
}

/// Convert command params directly into a CommandRequest.
/// This only works for types that implement CommandParams (actual command param structs),
/// not for CommandRequest itself, which avoids ambiguity with From<&CommandRequest<C>>.
impl<C: CommandParams> From<C> for CommandRequest<C> {
    fn from(command: C) -> Self {
        Self::new(command)
    }
}

/// Convert a reference to a CommandRequest into an owned CommandRequest by cloning.
impl<C: Clone> From<&CommandRequest<C>> for CommandRequest<C> {
    fn from(request: &CommandRequest<C>) -> Self {
        request.clone()
    }
}

impl<C: Send + Sync + 'static> WithTransaction for CommandRequest<C> {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone()
    }
}

impl<C: CommandId> CommandId for CommandRequest<C> {
    fn command_id(&self) -> Arc<str> {
        self.command.command_id()
    }
}

impl<C: CommandIdStatic> CommandIdStatic for CommandRequest<C> {
    fn command_id_static() -> &'static str {
        C::command_id_static()
    }
}

impl<C: CommandResultType> CommandResultType for CommandRequest<C> {
    type Result = C::Result;
}

impl<C: CommandId + Serialize + Debug + Send + Sync + 'static> AnyCommand for CommandRequest<C> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("CommandRequest should serialize to JSON")
    }
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

/// Static command ID for registration
pub trait CommandIdStatic {
    fn command_id_static() -> &'static str;
}

/// Result type for a command
pub trait CommandResultType {
    type Result: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
}

// Registration for type generation (separate from handler registration)
inventory::collect!(CommandRegistration);

#[derive(Debug)]
pub struct CommandRegistration {
    pub command_id: &'static str,
    pub result_type: &'static str,
    pub result_type_crate: &'static str,
    pub crate_name: &'static str,
}

// A command that can be sent; implementors provide a response type via the macro.
pub trait MykoCommand<T> {
    fn handle(&self, client: &MykoClient) -> impl std::future::Future<Output = Result<T, String>>;
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommandResponse {
    pub response: Value,
    pub tx: String,
}

impl CommandResponse {
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WrappedCommand {
    pub command: Value,
    pub command_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommandError {
    pub tx: String,
    pub command_id: String,
    pub message: String,
}

pub trait CommandId {
    fn command_id(&self) -> Arc<str>;
}

/// Type-erased command trait for dynamic dispatch.
/// All commands implement this via the `#[myko_command]` macro.
pub trait AnyCommand: WithTransaction + CommandId + Debug + Send + Sync + 'static {
    /// Serialize this command to a JSON Value.
    fn to_value(&self) -> Value;
}

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

/// Wrap a CommandRequest into a WrappedCommand for sending.
///
/// The CommandRequest already contains the tx via `#[serde(flatten)]`,
/// so this just serializes and extracts the command_id.
pub fn wrap_command_request<C: CommandId + Serialize + Clone>(
    request: &CommandRequest<C>,
) -> Result<WrappedCommand, serde_json::Error> {
    let json = serde_json::to_value(request)?;

    Ok(WrappedCommand {
        command: json,
        command_id: request.command_id().to_string(),
    })
}

/// Legacy wrap_command that takes tx separately.
/// Prefer using `wrap_command_request` with `CommandRequest<C>` instead.
#[deprecated(note = "Use wrap_command_request with CommandRequest instead")]
pub fn wrap_command<C: CommandId + Serialize + Clone>(
    tx: String,
    command: &C,
) -> Result<WrappedCommand, serde_json::Error> {
    // Create a CommandRequest and delegate
    let request = CommandRequest::with_tx(command.clone(), tx.into());
    wrap_command_request(&request)
}
