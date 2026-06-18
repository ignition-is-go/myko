//! Wire protocol types for items.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    TS,
    core::item::{AnyItem, lookup_item_registration},
};

/// Wrapper that implements serde::Serialize by delegating through erased_serde.
///
/// Since `dyn AnyItem` implements `serde::Serialize` via `erased_serde::serialize_trait_object!`,
/// this wrapper enables serialization of type-erased items directly to any serde format
/// without going through serde_json::Value.
pub struct SerializableItem<'a>(pub &'a dyn AnyItem);

impl Serialize for SerializableItem<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        erased_serde::serialize(self.0, serializer)
    }
}

/// Wrapper for items in wire protocol responses.
/// Holds a type-erased AnyItem and serializes it directly to the output format.
#[derive(Clone)]
pub struct ErasedWrappedItem {
    pub item: Arc<dyn AnyItem>,
    pub item_type: Arc<str>,
}

impl std::fmt::Debug for ErasedWrappedItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedWrappedItem")
            .field("item_type", &self.item_type)
            .field("id", &self.item.id())
            .finish()
    }
}

impl Serialize for ErasedWrappedItem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeStruct};
        // Human-readable serializers (JSON) go through the typed shim emitted
        // by `myko_item`, which monomorphizes the inner `Serialize` and skips
        // erased_serde entirely. Non-human-readable serializers (CBOR) and
        // unregistered types (e.g. client-side `ValueItem`) fall back to the
        // erased path. Bench: JSON path is ~2.5× faster than erased_serde.
        let json_raw = if serializer.is_human_readable() {
            lookup_item_registration(self.item_type.as_ref())
                .map(|reg| (reg.serialize_json)(&*self.item).map_err(S::Error::custom))
                .transpose()?
        } else {
            None
        };

        let mut s = serializer.serialize_struct("WrappedItem", 2)?;
        match json_raw {
            Some(raw) => s.serialize_field("item", &raw)?,
            None => s.serialize_field("item", &*self.item as &dyn erased_serde::Serialize)?,
        }
        s.serialize_field("itemType", &self.item_type)?;
        s.end()
    }
}

/// JSON-valued item wrapper for deserialization and TS export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem {
    pub item: serde_json::Value,
    pub item_type: Arc<str>,
}
