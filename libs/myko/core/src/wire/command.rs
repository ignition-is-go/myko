//! Wire protocol types for commands.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::message::WS_EVENT_COMMAND;
use crate::{
    TS,
    client::MykoProtocol,
    core::command::{CommandId, CommandRequest},
};

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

pub enum EncodedCommandMessage {
    Json(String),
    Cbor(Vec<u8>),
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WrappedCommandRef<'a, C> {
    command: &'a CommandRequest<C>,
    command_id: &'a str,
}

#[derive(Serialize)]
struct CommandMessageRef<'a, C> {
    event: &'static str,
    data: WrappedCommandRef<'a, C>,
}

pub fn encode_command_message<C: CommandId + Serialize>(
    protocol: MykoProtocol,
    request: &CommandRequest<C>,
) -> Result<EncodedCommandMessage, String> {
    let command_id = request.command_id();
    let message = CommandMessageRef {
        event: WS_EVENT_COMMAND,
        data: WrappedCommandRef {
            command: request,
            command_id: command_id.as_ref(),
        },
    };

    match protocol {
        MykoProtocol::JSON => serde_json::to_string(&message)
            .map(EncodedCommandMessage::Json)
            .map_err(|err| err.to_string()),
        MykoProtocol::CBOR => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(&message, &mut bytes).map_err(|e| e.to_string())?;
            Ok(EncodedCommandMessage::Cbor(bytes))
        }
    }
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
