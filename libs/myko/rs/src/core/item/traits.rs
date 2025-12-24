//! Item trait definitions and registration.

use std::{any::Any, fmt::Debug, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::common::{to_value::ToValue, with_id::WithId};

// ─────────────────────────────────────────────────────────────────────────────
// AnyItem - Type-erased item trait
// ─────────────────────────────────────────────────────────────────────────────

pub trait AnyItem: WithId + ToValue + Any + Debug + Send + Sync + 'static {
    /// Returns self as &dyn Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns the entity type name (e.g., "Target", "Scene").
    fn entity_type(&self) -> &'static str;
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Eventable - Trait for items that can be sent as events
// ─────────────────────────────────────────────────────────────────────────────

pub trait Eventable: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    /// Static entity type name (use entity_type() from AnyItem for instance method).
    fn entity_name_static() -> &'static str;

    /// Parse JSON into this item type.
    fn parse(value: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_value::<Self>(value)?;
        Ok(Arc::new(item))
    }
}
