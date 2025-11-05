use crate::{actors::repo_manager::RepoManagerMsg, event::MEvent, server::MykoServer};
use partially::Partial;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseItem {
    pub id: String,
}

impl TryFrom<MEvent> for BaseItem {
    fn try_from(value: MEvent) -> Result<Self, Self::Error> {
        serde_json::from_value(value.item_json()).map_err(|e| e.into())
    }

    type Error = serde_json::Error;
}

pub trait Eventable<T, PT: Clone>:
    Partial<Item = PT> + Serialize + DeserializeOwned + Clone + Send + Sync + Sized
{
    fn id(&self) -> String;
    fn hash(&self) -> String;
    fn entity_name(&self) -> String;
    fn entity_name_static() -> String;
    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        server
            .server
            .send_message(crate::actors::server::ServerMsg::RepoManagerMsg(
                RepoManagerMsg::RegisterRepo(Self::entity_name_static().into()),
            ))
            .map_err(anyhow::Error::msg)?;

        Ok(())
    }
}
