use std::{io::Cursor, u8};

use crate::{item::Eventable, utils::remove_whitespace};
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MEventType {
    SET,
    DEL,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEvent {
    item: Value,

    #[serde(rename = "changeType")]
    change_type: MEventType,

    #[serde(rename = "itemType")]
    item_type: String,

    #[serde(rename = "createdAt")]
    created_at: String,

    tx: String,

    #[serde(rename = "sourceId")]
    source_id: Option<String>,
}

impl MEvent {
    pub fn from_str(s: &str) -> Result<MEvent, serde_json::Error> {
        serde_json::from_str(&remove_whitespace(s))
    }

    pub fn from_mp(s: &[u8]) -> Result<MEvent, ()> {
        let cur = Cursor::new(s);
        let mut de = Deserializer::new(cur);
        let event: MEvent = match Deserialize::deserialize(&mut de) {
            Ok(event) => event,
            Err(_e) => {
                return Err(());
            }
        };
        Ok(event)
    }

    pub fn item_json(&self) -> Value {
        self.item.clone()
    }

    pub fn from_item<T, PT: Clone>(
        item: &impl Eventable<T, PT>,
        change_type: MEventType,
        tx: String,
    ) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type,
            item_type: "MItem".to_string(),
            created_at: "2021-01-01T00:00:00Z".to_string(),
            tx,
            source_id: None,
        }
    }
}

#[wasm_bindgen]
impl MEvent {
    #[wasm_bindgen(getter, js_name = "itemType")]
    pub fn item_type(&self) -> String {
        self.item_type.clone()
    }

    #[wasm_bindgen(getter, js_name = "createdAt")]
    pub fn created_at(&self) -> String {
        self.created_at.clone()
    }

    #[wasm_bindgen(getter, js_name = "changeType")]
    pub fn change_type(&self) -> MEventType {
        self.change_type
    }

    #[wasm_bindgen(getter)]
    pub fn tx(&self) -> String {
        self.tx.clone()
    }

    #[wasm_bindgen(getter, js_name = "sourceId")]
    pub fn source_id(&self) -> Option<String> {
        self.source_id.clone()
    }
}
