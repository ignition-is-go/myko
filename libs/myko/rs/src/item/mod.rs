use crate::{
    parsers::item::{CapturedItemParser, MykoItemParser},
    prelude::AnyItem,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{any::Any, sync::Arc};
use ts_rs::TS;

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

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: Arc<str>,
}

pub trait Eventable: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    fn entity_name(&self) -> String;
    fn entity_name_static() -> String;

    /// Create the registration data (parser) for this entity type.
    fn create_registration() -> RegisterItemData {
        RegisterItemData {
            entity_type: Self::entity_name_static().into(),
            parser: Arc::new(CapturedItemParser::<Self>::new()),
        }
    }
}
