//! Item trait definitions and registration.

use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, value::RawValue};

// AHash on the lookup index — every wire-emit JSON serialize hits this.
type AMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

use crate::common::with_id::WithId;

// ─────────────────────────────────────────────────────────────────────────────
// AnyItem - Type-erased item trait
// ─────────────────────────────────────────────────────────────────────────────

pub trait AnyItem: WithId + erased_serde::Serialize + Any + Debug + Send + Sync + 'static {
    /// Returns self as &dyn Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns the entity type name (e.g., "Target", "Scene").
    fn entity_type(&self) -> &'static str;

    /// Typed equality across erased items.
    fn equals(&self, other: &dyn AnyItem) -> bool;

    /// Returns the current `#[server_owned]` field value, if this item has one.
    fn server_owner(&self) -> Option<&str> {
        None
    }

    /// Returns a clone of this item with the `#[server_owned]` field set to `server_id`.
    /// Returns `None` if this item has no `#[server_owned]` field.
    fn bake_server_owner(&self, server_id: &str) -> Option<Arc<dyn AnyItem>> {
        let _ = server_id;
        None
    }
}

// Generate `impl serde::Serialize for dyn AnyItem` via erased_serde.
// This enables direct serialization of type-erased items without going
// through serde_json::Value first.
erased_serde::serialize_trait_object!(AnyItem);

impl PartialEq for dyn AnyItem {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Eq for dyn AnyItem {}

// ─────────────────────────────────────────────────────────────────────────────
// Item Registration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(ItemRegistration);

/// Type alias for item parse function.
pub type ItemParseFn = fn(Value) -> Result<Arc<dyn AnyItem>, anyhow::Error>;

/// Type alias for direct-from-bytes parse. Skips the `serde_json::Value`
/// round-trip when raw bytes are available — bench: ~2.27× faster than
/// the bytes → Value → `from_value` path.
pub type ItemParseBytesFn = fn(&[u8]) -> Result<Arc<dyn AnyItem>, anyhow::Error>;

/// Type alias for typed JSON serialize function. Each registered entity emits
///
/// a monomorphized shim that downcasts to the concrete type and calls the
/// typed `serde_json` serializer, producing a `RawValue` that the outer
/// serializer embeds without going through `erased_serde`.
pub type ItemSerializeJsonFn = fn(&dyn AnyItem) -> Result<Box<RawValue>, serde_json::Error>;

/// Registration entry for an item type.
/// Collected via inventory for automatic discovery.
pub struct ItemRegistration {
    pub entity_type: &'static str,
    /// Crate where this entity is defined (for `type_gen` filtering)
    pub crate_name: &'static str,
    /// Parse function that deserializes JSON into the typed item
    pub parse: ItemParseFn,
    /// Direct-from-bytes parser. Available when the wire layer can hand off
    /// raw bytes; skips the `Value` allocation. Currently registered but only
    /// used opportunistically — the existing `parse(Value)` path stays as the
    /// universal entry point until callers can plumb bytes end-to-end.
    pub parse_bytes: ItemParseBytesFn,
    /// Typed JSON serialize shim. The macro emits `|any| {
    /// let typed = any.as_any().downcast_ref::<T>()?; serde_json::value::to_raw_value(typed) }`.
    /// Wire emit (`ErasedWrappedItem::serialize`) dispatches through this for
    /// human-readable serializers, sidestepping `erased_serde`'s vtable overhead.
    pub serialize_json: ItemSerializeJsonFn,
}

/// O(1) `entity_type` → registration index, lazily built from the inventory.
fn item_registry_index() -> &'static AMap<&'static str, &'static ItemRegistration> {
    static INDEX: OnceLock<AMap<&'static str, &'static ItemRegistration>> = OnceLock::new();
    INDEX.get_or_init(|| {
        inventory::iter::<ItemRegistration>
            .into_iter()
            .map(|reg| (reg.entity_type, reg))
            .collect()
    })
}

/// Look up the typed JSON serialize shim for an `entity_type`.
/// Returns `None` for unregistered types (e.g. the client-side `ValueItem`
/// wrapper), in which case callers should fall back to `erased_serde`.
#[must_use]
pub fn lookup_item_registration(entity_type: &str) -> Option<&'static ItemRegistration> {
    item_registry_index().get(entity_type).copied()
}

/// Opt-in ingest buffering policy for high-volume entity streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IngestBufferPolicy {
    #[default]
    None,
    TimeWindow {
        window_ms: u64,
    },
}

/// Registration entry for an entity type's ingest buffering policy.
pub struct IngestBufferRegistration {
    pub entity_type: &'static str,
    pub policy: IngestBufferPolicy,
}

inventory::collect!(IngestBufferRegistration);

// ─────────────────────────────────────────────────────────────────────────────
// Eventable - Trait for items that can be sent as events
// ─────────────────────────────────────────────────────────────────────────────

pub trait Eventable:
    AnyItem + Serialize + DeserializeOwned + Clone + PartialEq + Sized + Any
{
    /// Static entity type name (use `entity_type()` from `AnyItem` for instance access).
    const ENTITY_NAME_STATIC: &'static str;

    /// Back-compat helper for generic call sites that still expect a function.
    #[must_use]
    fn entity_name_static() -> &'static str {
        Self::ENTITY_NAME_STATIC
    }

    /// Opt-in ingest buffering policy for this entity type.
    #[must_use]
    fn ingest_buffer_policy() -> IngestBufferPolicy {
        IngestBufferPolicy::None
    }

    /// Parse JSON into this item type.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn parse(value: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_value::<Self>(value)?;
        Ok(Arc::new(item))
    }

    /// Parse JSON bytes into this item type. Skips the `Value` round-trip
    /// when callers can hand off raw bytes (~2.27× faster than the
    /// `Value → from_value` path on a typical entity payload).
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn parse_bytes(bytes: &[u8]) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_slice::<Self>(bytes)?;
        Ok(Arc::new(item))
    }
}
