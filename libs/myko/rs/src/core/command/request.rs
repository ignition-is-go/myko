//! CommandRequest wrapper type.

use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

use super::traits::{AnyCommand, CommandId, CommandIdStatic, CommandParams, CommandResultType};
use crate::common::with_transaction::WithTransaction;

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
    const COMMAND_ID: &'static str = C::COMMAND_ID;
}

impl<C: CommandResultType> CommandResultType for CommandRequest<C> {
    type Result = C::Result;
}

impl<C: CommandId + Serialize + Debug + Send + Sync + 'static> AnyCommand for CommandRequest<C> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("CommandRequest should serialize to JSON")
    }
}
