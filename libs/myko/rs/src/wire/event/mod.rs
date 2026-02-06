use std::io::Cursor;

use chrono::Utc;
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::item::Eventable;

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
    /// When true, the event is not persisted to Kafka.
    /// Used for events from Kafka (to avoid re-publishing).
    #[serde(default)]
    pub prevent_persist: bool,
    /// When true, this event was replicated from a peer server.
    /// Used to prevent re-broadcasting to peers and avoid cascade loops.
    #[serde(default)]
    pub from_peer: Option<bool>,
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
    /// Parse an MEvent from a JSON string.
    ///
    /// NOTE: The name `from_str_trim` is historical - it no longer trims whitespace from
    /// the input. JSON parsers handle structural whitespace correctly, and blindly removing
    /// whitespace was destroying string values (e.g., "hello world" → "helloworld").
    pub fn from_str_trim(s: &str) -> Result<MEvent, serde_json::Error> {
        serde_json::from_str(s)
    }

    pub fn from_mp(s: &[u8]) -> Result<MEvent, rmp_serde::decode::Error> {
        let cur = Cursor::new(s);
        let mut de = Deserializer::new(cur);
        Deserialize::deserialize(&mut de)
    }

    pub fn item_json(&self) -> Value {
        self.item.clone()
    }

    pub fn from_item(item: &impl Eventable, change_type: MEventType, source_id: &str) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type,
            item_type: item.entity_type().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
            options: None,
        }
    }

    /// Create an event with options
    pub fn from_item_with_options(
        item: &impl Eventable,
        change_type: MEventType,
        source_id: &str,
        options: Option<EventOptions>,
    ) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type,
            item_type: item.entity_type().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
            options,
        }
    }

    /// Create a DEL event for an entity type and ID
    pub fn del(entity_type: &str, id: &str, source_id: &str) -> MEvent {
        MEvent {
            item: serde_json::json!({ "id": id }),
            change_type: MEventType::DEL,
            item_type: entity_type.to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
            options: None,
        }
    }

    /// Create a SET event from a JSON value
    pub fn set_from_value(entity_type: &str, value: Value, source_id: &str) -> MEvent {
        MEvent {
            item: value,
            change_type: MEventType::SET,
            item_type: entity_type.to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
            options: None,
        }
    }

    /// Check if relationship updates should be prevented for this event
    pub fn prevent_relationship_updates(&self) -> bool {
        self.options
            .as_ref()
            .map(|o| o.prevent_relationship_updates)
            .unwrap_or(false)
    }

    /// Check if this event was replicated from a peer server
    pub fn is_from_peer(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|o| o.from_peer)
            .unwrap_or(false)
    }

    pub fn change_type(&self) -> MEventType {
        self.change_type
    }

    pub fn item_type(&self) -> String {
        self.item_type.to_string()
    }
}
