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
    #[wasm_bindgen(constructor)]
    pub fn new(id: String, hash: String) -> Self {
        Self { id, hash }
    }

    #[wasm_bindgen(getter, js_name = "id")]
    pub fn get_id(&self) -> String {
        self.id.to_string()
    }

    #[wasm_bindgen(getter, js_name = "hash")]
    pub fn get_hash(&self) -> String {
        self.hash.clone()
    }
}

impl MItem {
    pub fn id(&self) -> String {
        self.id.to_string()
    }

    pub fn hash(&self) -> String {
        self.hash.clone()
    }

    pub fn from_json(json: Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(json)
    }
}
