use crate::{
    actors::repo::{Repo, RepoArgs},
    item::Eventable,
};
use partially::Partial;
use ractor::Actor;
use serde::{Deserialize, Serialize};

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Client {
    id: String,
    hash: String,
}

impl Eventable<Client, PartialClient> for Client {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn hash(&self) -> String {
        self.hash.clone()
    }

    fn entity_name(&self) -> String {
        Self::entity_name_static()
    }

    fn entity_name_static() -> String {
        "Client".into()
    }

    async fn register() -> Result<(), ractor::ActorProcessingErr> {
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
