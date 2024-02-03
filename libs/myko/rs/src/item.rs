use std::any::Any;

use partially::Partial;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MItem {
    id: String,
    hash: String,
}

#[wasm_bindgen]
impl MItem {
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> String {
        self.id.to_string()
    }

    #[wasm_bindgen(getter)]
    pub fn hash(&self) -> String {
        self.hash.clone()
    }
}

pub trait Eventable<T, PT: Clone>:
    Partial<Item = PT> + Serialize + DeserializeOwned + Clone + Send + Sync + Sized
{
    type T;

    fn id(&self) -> String;
    fn hash(&self) -> String;
    // fn matches(&self, query: &PartialT) -> bool;
}

pub fn matches<T: Eventable<T, PT> + PartialEq, PT: Clone>(item: &T, query: &PT) -> bool {
    let before = item.clone();
    let q = query.clone();

    let mut after = item.clone();

    after.apply_some(q);

    after == before
}
