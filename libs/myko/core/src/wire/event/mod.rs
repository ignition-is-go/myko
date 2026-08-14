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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct MEvent {
    pub item: Value,

    pub change_type: MEventType,

    // NOTE(ts): the string fields are Arc<str>, not String — an event is
    // cloned across subsystems (persister, sink, buffers, sessions) far
    // more often than it is built, and Arc makes every one of those clones
    // a refcount bump. Serialized form is unchanged (plain JSON strings).
    pub item_type: Arc<str>,

    #[serde(default = "utc_now_iso")]
    pub created_at: Arc<str>,

    #[serde(default = "generate_random_uuid")]
    pub tx: Arc<str>,

    pub source_id: Option<Arc<str>>,
}

/// Interned `Arc<str>` for a `&'static str` entity type — one allocation
///
/// per distinct type for the process lifetime, then refcount bumps. Every
/// produced event stamps `item_type`; without interning each stamp would
/// copy the type name into a fresh Arc.
pub fn intern_entity_type(entity_type: &'static str) -> Arc<str> {
    static INTERNED: std::sync::OnceLock<dashmap::DashMap<&'static str, Arc<str>>> =
        std::sync::OnceLock::new();
    INTERNED
        .get_or_init(dashmap::DashMap::new)
        .entry(entity_type)
        .or_insert_with(|| Arc::from(entity_type))
        .clone()
}

fn generate_random_uuid() -> Arc<str> {
    Arc::from(uuid::Uuid::new_v4().to_string())
}

fn utc_now_iso() -> Arc<str> {
    Arc::from(Utc::now().to_rfc3339())
}

impl MEvent {
    /// Parse an `MEvent` from a JSON string.
    ///
    /// NOTE: The name `from_str_trim` is historical - it no longer trims whitespace from
    /// the input. JSON parsers handle structural whitespace correctly, and blindly removing
    /// whitespace was destroying string values (e.g., "hello world" → "helloworld").
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn from_str_trim(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn from_cbor(s: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::de::from_reader(s)
    }

    #[must_use]
    pub fn item_json(&self) -> Value {
        self.item.clone()
    }

    pub fn from_item(item: &impl Eventable, change_type: MEventType, source_id: &str) -> Self {
        Self {
            item: serde_json::to_value(item).unwrap_or_else(|error| {
                tracing::error!(%error, "failed to serialize event item");
                Value::Null
            }),
            change_type,
            item_type: intern_entity_type(item.entity_type()),
            created_at: Arc::from(Utc::now().to_rfc3339()),
            tx: Arc::from(uuid::Uuid::new_v4().to_string()),
            source_id: Some(Arc::from(source_id)),
        }
    }

    /// Create a DEL event from a typed entity.
    pub fn del(item: &impl Eventable, source_id: &str) -> Self {
        Self {
            item: serde_json::to_value(item).unwrap_or_else(|error| {
                tracing::error!(%error, "failed to serialize deleted event item");
                Value::Null
            }),
            change_type: MEventType::DEL,
            item_type: intern_entity_type(item.entity_type()),
            created_at: Arc::from(Utc::now().to_rfc3339()),
            tx: Arc::from(uuid::Uuid::new_v4().to_string()),
            source_id: Some(Arc::from(source_id)),
        }
    }

    /// Create a DEL event from a dynamic item.
    pub fn del_from_any(item: &Arc<dyn AnyItem>, source_id: &str) -> Self {
        Self {
            item: item.to_value(),
            change_type: MEventType::DEL,
            item_type: intern_entity_type(item.entity_type()),
            created_at: Arc::from(Utc::now().to_rfc3339()),
            tx: Arc::from(uuid::Uuid::new_v4().to_string()),
            source_id: Some(Arc::from(source_id)),
        }
    }

    /// Create a SET event from a JSON value
    #[must_use]
    pub fn set_from_value(entity_type: &str, value: Value, source_id: &str) -> Self {
        Self {
            item: value,
            change_type: MEventType::SET,
            item_type: Arc::from(entity_type),
            created_at: Arc::from(Utc::now().to_rfc3339()),
            tx: Arc::from(uuid::Uuid::new_v4().to_string()),
            source_id: Some(Arc::from(source_id)),
        }
    }

    #[must_use]
    pub const fn change_type(&self) -> MEventType {
        self.change_type
    }

    #[must_use]
    pub fn item_type(&self) -> String {
        self.item_type.to_string()
    }

    /// Strip null bytes from all string fields and the item Value tree.
    /// `PostgreSQL` rejects `\0` in text/jsonb columns, and binary protocol
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
                        *map = std::mem::take(map)
                            .into_iter()
                            .map(|(k, mut v)| {
                                sanitize_value(&mut v);
                                (k.replace('\0', ""), v)
                            })
                            .collect();
                    } else {
                        map.values_mut().for_each(sanitize_value);
                    }
                }
                _ => {}
            }
        }

        fn sanitize_arc(s: &mut Arc<str>) {
            if s.as_bytes().contains(&0) {
                *s = Arc::from(s.replace('\0', ""));
            }
        }

        sanitize_arc(&mut self.item_type);
        sanitize_arc(&mut self.created_at);
        sanitize_arc(&mut self.tx);
        if let Some(ref mut sid) = self.source_id {
            sanitize_arc(sid);
        }
        sanitize_value(&mut self.item);
    }
}
