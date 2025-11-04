use std::io::Cursor;

use crate::{item::Eventable, utils::remove_whitespace};
use chrono::Utc;
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MEventType {
    SET,
    DEL,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEvent {
    item: Value,

    #[serde(rename = "changeType")]
    change_type: MEventType,

    #[serde(rename = "itemType")]
    item_type: String,

    #[serde(rename = "createdAt", default = "utc_now_iso")]
    created_at: String,

    #[serde(default = "generate_random_uuid")]
    tx: String,

    #[serde(rename = "sourceId")]
    source_id: Option<String>,
}

fn generate_random_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn utc_now_iso() -> String {
    Utc::now().to_rfc3339()
}

impl MEvent {
    pub fn from_str_trim(s: &str) -> Result<MEvent, serde_json::Error> {
        serde_json::from_str(&remove_whitespace(s))
    }

    pub fn from_mp(s: &[u8]) -> Result<MEvent, rmp_serde::decode::Error> {
        let cur = Cursor::new(s);
        let mut de = Deserializer::new(cur);
        Deserialize::deserialize(&mut de)
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
            item_type: item.entity_name().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx,
            source_id: None,
        }
    }

    pub fn change_type(&self) -> MEventType {
        self.change_type
    }

    pub fn item_type(&self) -> String {
        self.item_type.to_string()
    }
}
