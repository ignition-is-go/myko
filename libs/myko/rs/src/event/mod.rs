use crate::{item::Eventable, utils::remove_whitespace};
use chrono::Utc;
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Cursor;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
pub enum MEventType {
    SET,
    DEL,
}

/// Options that can be attached to an event
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EventOptions {
    /// When true, relationship cascades are skipped for this event.
    /// Used to prevent infinite loops during cascade processing.
    #[serde(default)]
    pub prevent_relationship_updates: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct MEvent {
    pub item: Value,

    pub change_type: MEventType,

    pub item_type: String,

    #[serde(default = "utc_now_iso")]
    pub created_at: String,

    #[serde(default = "generate_random_uuid")]
    pub tx: String,

    pub source_id: Option<String>,

    /// Optional event options (e.g., prevent_relationship_updates)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<EventOptions>,
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

    pub fn from_item(item: &impl Eventable, change_type: MEventType, tx: String) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type,
            item_type: item.entity_name().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx,
            source_id: None,
            options: None,
        }
    }

    /// Create an event with options
    pub fn from_item_with_options(
        item: &impl Eventable,
        change_type: MEventType,
        tx: String,
        options: Option<EventOptions>,
    ) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type,
            item_type: item.entity_name().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx,
            source_id: None,
            options,
        }
    }

    /// Check if relationship updates should be prevented for this event
    pub fn prevent_relationship_updates(&self) -> bool {
        self.options
            .as_ref()
            .map(|o| o.prevent_relationship_updates)
            .unwrap_or(false)
    }

    pub fn change_type(&self) -> MEventType {
        self.change_type
    }

    pub fn item_type(&self) -> String {
        self.item_type.to_string()
    }
}
