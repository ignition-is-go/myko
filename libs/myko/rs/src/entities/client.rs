use crate::{actors::repo_manager::RepoManagerMsg, item::Eventable, server::MykoServer};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
