use serde::{Deserialize, Serialize, ser::Error};
use serde_json::Value;

use crate::client::MykoClient;

// A command that can be sent; implementors provide a response type via the macro.
pub trait MykoCommand<T> {
    fn handle(&self, client: &MykoClient) -> impl std::future::Future<Output = Result<T, String>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedCommand {
    pub command: Value,
    pub command_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandError {
    pub tx: String,
    pub message: String,
}

pub trait CommandId {
    fn command_id(&self) -> String;
}

pub fn wrap_command<C: CommandId + Serialize + Clone>(
    tx: String,
    command: &C,
) -> Result<WrappedCommand, serde_json::Error> {
    let mut json = serde_json::to_value(command.clone())?;

    let obj_mut = json.as_object_mut();

    if obj_mut.is_none() {
        return Err(serde_json::Error::custom("Could not convert to object"));
    }

    let obj = obj_mut.unwrap();

    obj.insert("tx".to_string(), tx.into());

    Ok(WrappedCommand {
        command: json,
        command_id: command.command_id(),
    })
}
