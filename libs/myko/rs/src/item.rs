use crate::{actors::repo_manager::RepoManagerMsg, event::MEvent, server::MykoServer};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use uuid::Uuid;

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

pub trait Eventable:
    Serialize + DeserializeOwned + Clone + Send + Sync + Sized + Any + 'static
{
    fn id(&self) -> String;
    fn hash(&self) -> String;
    fn entity_name(&self) -> String;
    fn entity_name_static() -> String;
    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        server
            .server
            .send_message(crate::actors::server::ServerMsg::RepoManagerMsg(
                RepoManagerMsg::RegisterRepo(
                    Self::entity_name_static().into(),
                    Box::new(EntityController::<Self>::new()),
                    TypeId::of::<Self>(),
                ),
            ))
            .map_err(anyhow::Error::msg)?;

        Ok(())
    }
}

pub struct EntityController<T: Eventable> {
    hash_map: HashMap<Arc<str>, Arc<T>>,
}

pub trait MykoEntityController: Send + Sync + 'static {
    fn set(
        &mut self,
        key: Arc<str>,
        value: Value,
    ) -> Result<Arc<dyn Any + Send + Sync>, anyhow::Error>;
    fn del(&mut self, key: &Arc<str>);
    fn len(&self) -> usize;
}

impl<T: Eventable + Send + Sync + 'static> MykoEntityController for EntityController<T> {
    fn set(
        &mut self,
        key: Arc<str>,
        item: Value,
    ) -> Result<Arc<dyn Any + Send + Sync>, anyhow::Error> {
        let item = match item {
            Value::Object(mut obj) => {
                let hash = obj.get("hash");
                if hash.is_none() {
                    obj.insert(
                        "hash".to_string(),
                        Value::String(Uuid::new_v4().to_string()),
                    );
                }
                Value::Object(obj)
            }
            _ => {
                anyhow::bail!("Invalid value type");
            }
        };
        let item = serde_json::from_value::<T>(item)?;

        let item_arc = Arc::new(item);
        self.hash_map.insert(key, item_arc.clone());

        Ok(item_arc as Arc<dyn Any + Send + Sync>)
    }

    fn del(&mut self, key: &Arc<str>) {
        self.hash_map.remove(key);
    }

    fn len(&self) -> usize {
        self.hash_map.len()
    }
}

impl<T: Eventable> EntityController<T> {
    pub fn new() -> Self {
        Self {
            hash_map: HashMap::new(),
        }
    }
}
