use crate::common::{to_value::ToValue, with_id::WithId};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::{any::Any, fmt::Debug, sync::Arc};

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
// Item Parser - Parses JSON into typed items
// ─────────────────────────────────────────────────────────────────────────────

pub trait MykoItemParser: Send + Sync + 'static {
    fn parse(&self, input: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error>;
}

pub struct CapturedItemParser<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T> Default for CapturedItemParser<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CapturedItemParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyItem> MykoItemParser for CapturedItemParser<T> {
    fn parse(&self, item: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_value::<T>(item)?;
        Ok(Arc::new(item))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Item Registration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(ItemRegistration);

/// Data needed to register an entity with the EventManager
pub struct RegisterItemData {
    pub entity_type: Arc<str>,
    pub parser: Arc<dyn MykoItemParser>,
}

/// Type alias for the item factory function pointer.
/// Returns the data needed to register an entity (parser).
pub type ItemFactoryFn = fn() -> RegisterItemData;

pub struct ItemRegistration {
    pub entity_type: &'static str,
    /// Crate where this entity is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Factory function that creates the registration data (parser)
    pub factory: ItemFactoryFn,
}

// ─────────────────────────────────────────────────────────────────────────────
// Eventable - Trait for items that can be sent as events
// ─────────────────────────────────────────────────────────────────────────────

pub trait Eventable: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    /// Static entity type name (use entity_type() from AnyItem for instance method).
    fn entity_name_static() -> &'static str;

    /// Create the registration data (parser) for this entity type.
    fn create_registration() -> RegisterItemData {
        RegisterItemData {
            entity_type: Self::entity_name_static().into(),
            parser: Arc::new(CapturedItemParser::<Self>::new()),
        }
    }
}
