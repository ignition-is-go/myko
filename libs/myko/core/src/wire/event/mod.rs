use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{TS, common::to_value::ToValue, core::item::AnyItem, item::Eventable};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
pub enum MEventType {
    SET,
    DEL,
}

/// Options that can be attached to an event
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EventOptions {
    /// When true, relationship cascades are skipped for this event.
    /// Used to prevent infinite loops during cascade processing.
    #[serde(default)]
    pub prevent_relationship_updates: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
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

    pub fn from_cbor(s: &[u8]) -> Result<MEvent, ciborium::de::Error<std::io::Error>> {
        ciborium::de::from_reader(s)
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
        }
    }

    /// Create a DEL event from a typed entity.
    pub fn del(item: &impl Eventable, source_id: &str) -> MEvent {
        MEvent {
            item: serde_json::to_value(item).unwrap(),
            change_type: MEventType::DEL,
            item_type: item.entity_type().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
        }
    }

    /// Create a DEL event from a dynamic item.
    pub fn del_from_any(item: &Arc<dyn AnyItem>, source_id: &str) -> MEvent {
        MEvent {
            item: item.to_value(),
            change_type: MEventType::DEL,
            item_type: item.entity_type().to_string(),
            created_at: Utc::now().to_rfc3339(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: Some(source_id.to_string()),
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
        }
    }

    /// Create a SET event carrying an explicit LWW stamp (`created_at` +
    /// `source_id`). Used when re-emitting an applied write so the broadcast
    /// stamp matches the one the origin actually applied (LWW convergence).
    pub fn set_with_stamp(
        entity_type: &str,
        value: Value,
        created_at: &str,
        source_id: Option<&str>,
    ) -> MEvent {
        MEvent {
            item: value,
            change_type: MEventType::SET,
            item_type: entity_type.to_string(),
            created_at: created_at.to_string(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: source_id.map(|s| s.to_string()),
        }
    }

    /// Create a DEL event carrying an explicit LWW stamp.
    pub fn del_with_stamp(
        item: &Arc<dyn AnyItem>,
        created_at: &str,
        source_id: Option<&str>,
    ) -> MEvent {
        MEvent {
            item: item.to_value(),
            change_type: MEventType::DEL,
            item_type: item.entity_type().to_string(),
            created_at: created_at.to_string(),
            tx: uuid::Uuid::new_v4().to_string(),
            source_id: source_id.map(|s| s.to_string()),
        }
    }

    pub fn change_type(&self) -> MEventType {
        self.change_type
    }

    pub fn item_type(&self) -> String {
        self.item_type.to_string()
    }

    /// Strip null bytes from all string fields and the item Value tree.
    /// PostgreSQL rejects `\0` in text/jsonb columns, and binary protocol
    /// executors (ATEM, Crestron, AMX, etc.) can produce strings containing
    /// null bytes from raw protocol data.
    pub fn sanitize_null_bytes(&mut self) {
        fn sanitize_string(s: &mut String) {
            if s.as_bytes().contains(&0) {
                *s = s.replace('\0', "");
            }
        }

        fn sanitize_value(v: &mut Value) {
            match v {
                Value::String(s) => sanitize_string(s),
                Value::Array(arr) => arr.iter_mut().for_each(sanitize_value),
                Value::Object(map) => {
                    // NOTE(ts): serde_json::Map keys are Strings but we can't
                    // mutate keys in place. Rebuild the map if any key has nulls.
                    let has_bad_key = map.keys().any(|k| k.as_bytes().contains(&0));
                    if has_bad_key {
                        let entries: Vec<_> = std::mem::take(map)
                            .into_iter()
                            .map(|(k, mut v)| {
                                sanitize_value(&mut v);
                                (k.replace('\0', ""), v)
                            })
                            .collect();
                        *map = entries.into_iter().collect();
                    } else {
                        map.values_mut().for_each(sanitize_value);
                    }
                }
                _ => {}
            }
        }

        sanitize_string(&mut self.item_type);
        sanitize_string(&mut self.created_at);
        sanitize_string(&mut self.tx);
        if let Some(ref mut sid) = self.source_id {
            sanitize_string(sid);
        }
        sanitize_value(&mut self.item);
    }
}
