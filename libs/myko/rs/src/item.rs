use partially::Partial;
use ractor::Actor;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    actors::repo::{Repo, RepoArgs},
    event::MEvent,
};

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
    fn register() -> impl std::future::Future<Output = Result<(), ractor::ActorProcessingErr>> + Send
    {
        async {
            Actor::spawn(
                None,
                Repo,
                RepoArgs {
                    entity_name: Self::entity_name_static().into(),
                },
            )
            .await?;
            Ok(())
        }
    }
}
