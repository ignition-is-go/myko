use serde::{Deserialize, Serialize};
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

pub trait Eventable<T> {
    type T;
    fn id(&self) -> String;
    fn hash(&self) -> String;
    fn from_json(json: Value) -> Result<T, serde_json::Error>;
}
