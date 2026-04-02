//! Wire protocol types for items.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::core::item::AnyItem;

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
pub struct WrappedItem {
    pub item: Arc<dyn AnyItem>,
    pub item_type: Arc<str>,
}

impl std::fmt::Debug for WrappedItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrappedItem")
            .field("item_type", &self.item_type)
            .field("id", &self.item.id())
            .finish()
    }
}

impl Serialize for WrappedItem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("WrappedItem", 2)?;
        // Serialize the item directly via erased_serde — no intermediate Value
        s.serialize_field("item", &*self.item as &dyn erased_serde::Serialize)?;
        s.serialize_field("itemType", &self.item_type)?;
        s.end()
    }
}

/// Inbound wire type: a JSON-valued item wrapper for deserialization (e.g., import commands).
/// This preserves the original generic WrappedItem<Value> shape for client-to-server messages.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItemValue {
    pub item: serde_json::Value,
    pub item_type: Arc<str>,
}
