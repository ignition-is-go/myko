//! Wire protocol types for items.

use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::encoded::EncodedJsonCache;

use crate::{
    TS,
    core::item::{AnyItem, lookup_item_registration},
};

/// Wrapper that implements `serde::Serialize` by delegating through `erased_serde`.
///
/// Since `dyn AnyItem` implements `serde::Serialize` via `erased_serde::serialize_trait_object!`,
/// this wrapper enables serialization of type-erased items directly to any serde format
/// without going through `serde_json::Value`.
pub struct SerializableItem<'a>(pub &'a dyn AnyItem);

impl Serialize for SerializableItem<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        erased_serde::serialize(self.0, serializer)
    }
}

/// Wrapper for items in wire protocol responses.
/// Holds a type-erased `AnyItem` and serializes it directly to the output format.
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
        // Human-readable serializers (JSON) splice the item's compact JSON,
        // encoded once per item allocation and shared by every frame and
        // subscriber that carries the same `Arc`. Non-human-readable
        // serializers (CBOR) serialize the item in place.
        let json_raw = if serializer.is_human_readable() {
            encoded_item_json(&self.item, &self.item_type)
        } else {
            None
        };

        let mut s = serializer.serialize_struct("WrappedItem", 2)?;
        if let Some(raw) = json_raw {
            s.serialize_field("item", &*raw)?;
        } else {
            let erased: &dyn erased_serde::Serialize = &*self.item;
            s.serialize_field("item", erased)?;
        }
        s.serialize_field("itemType", &self.item_type)?;
        s.end()
    }
}

fn item_json_cache() -> &'static EncodedJsonCache<dyn AnyItem> {
    static CACHE: OnceLock<EncodedJsonCache<dyn AnyItem>> = OnceLock::new();
    CACHE.get_or_init(EncodedJsonCache::default)
}

/// Compact JSON of `item`, encoded at most once per allocation.
///
/// Registered entity types go through the typed shim emitted by `myko_item`,
/// which monomorphizes the inner `Serialize` and skips `erased_serde`.
/// Registrations are indexed by unqualified entity name and distinct crates
/// can register the same name, so a shim that cannot downcast falls back to
/// the erased serializer, as do unregistered types such as view rows.
fn encoded_item_json(item: &Arc<dyn AnyItem>, item_type: &str) -> Option<Arc<RawValue>> {
    item_json_cache().get_or_encode(item, |item| {
        lookup_item_registration(item_type)
            .and_then(|reg| (reg.serialize_json)(item).ok())
            .or_else(|| serde_json::value::to_raw_value(&SerializableItem(item)).ok())
    })
}

/// JSON-valued item wrapper for deserialization and TS export.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem {
    pub item: serde_json::Value,
    pub item_type: Arc<str>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde::ser::SerializeStruct as _;

    use super::*;
    use crate::{
        common::with_id::WithId,
        entities::{client::Client, server::ServerId},
    };

    /// A row type with no item registration, like a view row.
    #[derive(Debug, Serialize)]
    struct Unregistered {
        id: Arc<str>,
        label: String,
        weight: f32,
    }

    impl WithId for Unregistered {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl AnyItem for Unregistered {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn entity_type(&self) -> &'static str {
            "Unregistered"
        }
        fn equals(&self, _other: &dyn AnyItem) -> bool {
            false
        }
    }

    /// The encoding before items were cached: the item serialized in place.
    struct InPlace<'a>(&'a ErasedWrappedItem);

    impl Serialize for InPlace<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut s = serializer.serialize_struct("WrappedItem", 2)?;
            let erased: &dyn erased_serde::Serialize = &*self.0.item;
            s.serialize_field("item", erased)?;
            s.serialize_field("itemType", &self.0.item_type)?;
            s.end()
        }
    }

    #[test]
    fn cached_item_json_matches_in_place_serialization() {
        let rows: [Arc<dyn AnyItem>; 2] = [
            Arc::new(Client {
                id: "client-\"1\"".into(),
                server_id: ServerId::from(Arc::<str>::from("server")),
                address: Some("10.0.0.1:9".into()),
                windback: None,
            }),
            Arc::new(Unregistered {
                id: "row\u{1}".into(),
                label: "ünïcode \\ tab\t".into(),
                weight: 0.1,
            }),
        ];
        for item in rows {
            let wrapped = ErasedWrappedItem {
                item_type: item.entity_type().into(),
                item,
            };
            let expected = serde_json::to_string(&InPlace(&wrapped)).unwrap();
            for _ in 0..2 {
                assert_eq!(serde_json::to_string(&wrapped).unwrap(), expected);
            }
        }
    }
}
