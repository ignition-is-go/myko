//! Wire protocol types for items.

use std::sync::Arc;

use crate::TS;
use serde::{Deserialize, Serialize};

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
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("WrappedItem", 2)?;
        // Serialize the item directly via erased_serde — no intermediate Value
        s.serialize_field("item", &*self.item as &dyn erased_serde::Serialize)?;
        s.serialize_field("itemType", &self.item_type)?;
        s.end()
    }
}

/// JSON-valued item wrapper for deserialization and TS export.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem {
    pub item: serde_json::Value,
    pub item_type: Arc<str>,
}
