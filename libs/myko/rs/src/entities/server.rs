use crate::{actors::repo_manager::RepoManagerMsg, item::Eventable, server::MykoServer};
use partially::Partial;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Partial, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[partially(derive(Clone, Serialize, Deserialize, Default))]
pub struct Server {
    pub id: String,
    pub hash: String,
    pub version: String,
    pub address: String,
    pub port: u16,
    pub started_at: String, // ISO DateTime
}

impl Eventable<Server, PartialServer> for Server {
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
        "Server".into()
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
