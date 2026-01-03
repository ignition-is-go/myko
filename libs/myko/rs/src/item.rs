use partially::Partial;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: String,
}

pub trait WithId {
    fn id(&self) -> String;
}

pub trait Eventable<T, PT: Clone>:
    Partial<Item = PT> + Serialize + DeserializeOwned + Clone + Send + Sync + Sized
{
    type T;

    fn id(&self) -> String;
    fn hash(&self) -> String;
    fn entity_name(&self) -> String;
}
