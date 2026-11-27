//! Client event frames parsed straight into typed items.
//!
//! Decoding a JSON event frame as a `MykoMessage` builds a `serde_json::Value`
//! tree for every item, which is then converted to the registered type. Here
//! each item stays a slice of the frame and is deserialized directly into its
//! type. The two steps that edited the tree run without one: null-byte
//! stripping sends the rare item containing `\u0000` down the `Value` path,
//! and framework-owned fields are filled on the item's top-level members
//! only. An item the direct parse rejects is retried through the `Value`
//! path, so exactly the items that path accepts are accepted.

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use myko::{
    core::item::AnyItem,
    relationship::{
        iter_client_id_registrations, iter_fallback_to_id_registrations,
        iter_server_owned_registrations,
    },
    server::MykoServerContext,
    wire::{MEvent, MEventType},
};
use serde::Deserialize;
use serde_json::value::RawValue;

/// Strip null bytes and fill framework-owned fields of a client event.
pub fn prepare_incoming_event(event: &mut MEvent, client_id: &str, host_id: uuid::Uuid) {
    event.sanitize_null_bytes();
    if event.change_type != MEventType::SET {
        return;
    }
    let Some(obj) = event.item.as_object_mut() else {
        return;
    };
    let item_type = &*event.item_type;

    // #[myko_client_id] fields carry the connection's client id.
    if let Some(reg) = iter_client_id_registrations().find(|reg| reg.entity_type == item_type) {
        obj.insert(
            reg.field_name_json.to_owned(),
            serde_json::Value::String(client_id.to_owned()),
        );
    }

    // #[server_owned] fields default to this server's id.
    if let Some(reg) = iter_server_owned_registrations().find(|reg| reg.entity_type == item_type) {
        let field = reg.field_name_json;
        if obj
            .get(field)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty()
        {
            obj.insert(
                field.to_owned(),
                serde_json::Value::String(host_id.to_string()),
            );
        }
    }

    // #[fallback_to_id] fields default to the entity's own id.
    for reg in iter_fallback_to_id_registrations().filter(|reg| reg.entity_type == item_type) {
        let field = reg.field_name_json;
        if matches!(obj.get(field), None | Some(serde_json::Value::Null))
            && let Some(id) = obj.get("id").and_then(|v| v.as_str()).map(String::from)
        {
            obj.insert(field.to_owned(), serde_json::Value::String(id));
        }
    }
}

/// A wire event whose item is still text of the frame.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawEvent<'a> {
    #[serde(borrow)]
    item: &'a RawValue,
    pub change_type: MEventType,
    #[serde(borrow)]
    pub item_type: Cow<'a, str>,
    // Typed as `MEvent` types them so the same frames are rejected; ingest
    // does not read them.
    #[serde(borrow, default, rename = "createdAt")]
    _created_at: Cow<'a, str>,
    #[serde(borrow, default, rename = "tx")]
    _tx: Cow<'a, str>,
    #[serde(borrow, default, rename = "sourceId")]
    _source_id: Option<Cow<'a, str>>,
}

/// The events of a JSON frame, if it is one.
pub struct EventFrame<'a> {
    /// `ws:m:event-batch` rather than `ws:m:event`.
    pub batch: bool,
    pub events: Vec<RawEvent<'a>>,
}

/// The events of a JSON `ws:m:event` or `ws:m:event-batch` frame, or `None`
/// for any other frame, which the caller decodes as a `MykoMessage`.
pub fn parse_event_frame(text: &str) -> Option<EventFrame<'_>> {
    // Clients write the tag first; a frame that leads with its data, or is
    // not an event frame, is left to the full decoder.
    let head = text.as_bytes().get(..64).unwrap_or(text.as_bytes());
    if !head.windows(11).any(|window| window == b"\"ws:m:event") {
        return None;
    }
    let mut frame: EventFrame<'_> = serde_json::from_str(text).ok()?;
    // Policy sees the type that will be applied.
    for event in &mut frame.events {
        event.item_type = strip_null_bytes(std::mem::take(&mut event.item_type));
    }
    Some(frame)
}

/// Reads `{"event": <tag>, "data": <events>, ...}` in one pass, refusing a
/// frame whose data precedes its tag or whose tag is not an event tag.
impl<'de: 'a, 'a> Deserialize<'de> for EventFrame<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<'a>(std::marker::PhantomData<&'a ()>);
        impl<'de: 'a, 'a> serde::de::Visitor<'de> for Visitor<'a> {
            type Value = EventFrame<'a>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a websocket event frame")
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                use serde::de::{Error, IgnoredAny};
                let mut batch = None;
                let mut events = None;
                while let Some(key) = map.next_key::<Cow<'de, str>>()? {
                    match &*key {
                        "event" if batch.is_none() => {
                            batch = Some(match &*map.next_value::<Cow<'de, str>>()? {
                                "ws:m:event" => false,
                                "ws:m:event-batch" => true,
                                _ => return Err(M::Error::custom("not an event frame")),
                            });
                        }
                        "data" if events.is_none() => {
                            events = Some(match batch {
                                Some(true) => map.next_value::<Vec<RawEvent<'a>>>()?,
                                Some(false) => vec![map.next_value::<RawEvent<'a>>()?],
                                None => return Err(M::Error::custom("data before event tag")),
                            });
                        }
                        "event" | "data" => return Err(M::Error::custom("duplicate field")),
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                match (batch, events) {
                    (Some(batch), Some(events)) => Ok(EventFrame { batch, events }),
                    _ => Err(M::Error::custom("incomplete event frame")),
                }
            }
        }
        deserializer.deserialize_map(Visitor(std::marker::PhantomData))
    }
}

impl RawEvent<'_> {
    /// The event's typed item, prepared as [`prepare_incoming_event`]
    /// prepares a decoded event. `None`, with a warning, when the type is
    /// unknown or the item does not parse as it.
    pub fn into_item(self, ctx: &MykoServerContext, client_id: &str) -> Option<Arc<dyn AnyItem>> {
        let item_type = self.item_type;
        let Some(parse_bytes) = ctx.handler_registry.item_bytes_parser(&item_type) else {
            tracing::warn!("Unknown entity type or parse error for ingest: {item_type}");
            return None;
        };
        let raw = self.item.get();
        // JSON text can carry a null byte only as this escape.
        if !raw.contains("\\u0000") {
            let filled = if self.change_type == MEventType::SET {
                fill_framework_fields(&item_type, raw, client_id, ctx.host_id)
            } else {
                Filled::Unchanged
            };
            let parsed = match &filled {
                Filled::Unchanged => parse_bytes(raw.as_bytes()).ok(),
                Filled::Text(text) => parse_bytes(text.as_bytes()).ok(),
                Filled::NeedsValue => None,
            };
            if parsed.is_some() {
                return parsed;
            }
        }
        let mut event = MEvent {
            item: serde_json::from_str(raw).unwrap_or(serde_json::Value::Null),
            change_type: self.change_type,
            item_type: Arc::from(&*item_type),
            created_at: Arc::from(""),
            tx: Arc::from(""),
            source_id: None,
        };
        prepare_incoming_event(&mut event, client_id, ctx.host_id);
        let item = ctx.parse_item(&event.item_type, event.item);
        if item.is_none() {
            tracing::warn!("Unknown entity type or parse error for ingest: {item_type}");
        }
        item
    }
}

fn strip_null_bytes(text: Cow<'_, str>) -> Cow<'_, str> {
    if text.as_bytes().contains(&0) {
        Cow::Owned(text.replace('\0', ""))
    } else {
        text
    }
}

enum Filled {
    /// The type owns no framework fields.
    Unchanged,
    /// The item with its framework fields filled.
    Text(String),
    /// The item's members cannot be read without a `Value`.
    NeedsValue,
}

/// Fill `item`'s framework-owned fields on its top-level members, as
/// [`prepare_incoming_event`] fills them on a `Value`.
fn fill_framework_fields(
    item_type: &str,
    item: &str,
    client_id: &str,
    host_id: uuid::Uuid,
) -> Filled {
    let client_field = iter_client_id_registrations()
        .find(|reg| reg.entity_type == item_type)
        .map(|reg| reg.field_name_json);
    let server_field = iter_server_owned_registrations()
        .find(|reg| reg.entity_type == item_type)
        .map(|reg| reg.field_name_json);
    let mut fallback_fields = iter_fallback_to_id_registrations()
        .filter(|reg| reg.entity_type == item_type)
        .map(|reg| reg.field_name_json)
        .peekable();
    if client_field.is_none() && server_field.is_none() && fallback_fields.peek().is_none() {
        return Filled::Unchanged;
    }
    fill_members(
        item,
        client_field,
        server_field,
        fallback_fields,
        client_id,
        host_id,
    )
    .map_or(Filled::NeedsValue, Filled::Text)
}

/// `None` for an item whose members need a `Value`: one that is not an
/// object, or has a key written with escapes.
fn fill_members<'a>(
    item: &'a str,
    client_field: Option<&'a str>,
    server_field: Option<&'a str>,
    fallback_fields: impl Iterator<Item = &'a str>,
    client_id: &str,
    host_id: uuid::Uuid,
) -> Option<String> {
    let string = |text: &str| serde_json::value::to_raw_value(text).ok().map(Cow::Owned);
    let Ok(borrowed) = serde_json::from_str::<BTreeMap<&str, &RawValue>>(item) else {
        return None;
    };
    let mut members: BTreeMap<&str, Cow<'_, RawValue>> = borrowed
        .into_iter()
        .map(|(key, value)| (key, Cow::Borrowed(value)))
        .collect();
    if let Some(field) = client_field {
        members.insert(field, string(client_id)?);
    }
    if let Some(field) = server_field
        && members
            .get(field)
            .is_none_or(|value| !value.get().starts_with('"') || value.get() == "\"\"")
    {
        members.insert(field, string(&host_id.to_string())?);
    }
    for field in fallback_fields {
        if members.get(field).is_none_or(|value| value.get() == "null")
            && let Some(id) = members.get("id").filter(|id| id.get().starts_with('"'))
        {
            let id = id.clone();
            members.insert(field, id);
        }
    }
    serde_json::to_string(&members).ok()
}
