use crate::{
    actors::event::event_manager::EventManagerMsg,
    parsers::item::{CapturedItemParser, MykoItemParser},
    prelude::AnyItem,
    server::MykoServer,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{any::Any, sync::Arc};
use ts_rs::TS;

inventory::collect!(ItemRegistration);

pub struct ItemRegistration {
    pub entity_type: &'static str,
    pub crate_name: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: Arc<str>,
}

pub trait Eventable:
    AnyItem + MykoAutoQueries + Serialize + DeserializeOwned + Clone + Sized + Any
{
    fn entity_name(&self) -> String;
    fn entity_name_static() -> String;
    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        let parser: Arc<dyn MykoItemParser> = Arc::new(CapturedItemParser::<Self>::new());

        server
            .server
            .send_message(crate::actors::server::ServerMsg::RepoManagerMsg(
                EventManagerMsg::RegisterRepo(Self::entity_name_static().into(), parser),
            ))
            .map_err(anyhow::Error::msg)?;

        Self::register_auto(server)?;

        Ok(())
    }
}

pub trait MykoAutoQueries: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    fn register_auto(server: &Arc<MykoServer>) -> Result<(), anyhow::Error>;
}
