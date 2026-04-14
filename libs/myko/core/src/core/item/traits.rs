//! Item trait definitions and registration.

use std::{any::Any, fmt::Debug, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

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

/// Registration entry for an item type.
/// Collected via inventory for automatic discovery.
pub struct ItemRegistration {
    pub entity_type: &'static str,
    /// Crate where this entity is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Parse function that deserializes JSON into the typed item
    pub parse: ItemParseFn,
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
    fn entity_name_static() -> &'static str {
        Self::ENTITY_NAME_STATIC
    }

    /// Opt-in ingest buffering policy for this entity type.
    fn ingest_buffer_policy() -> IngestBufferPolicy {
        IngestBufferPolicy::None
    }

    /// Parse JSON into this item type.
    fn parse(value: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_value::<Self>(value)?;
        Ok(Arc::new(item))
    }
}
