use crate::{
    actors::event::event_manager::EventManagerMsg,
    parsers::item::{CapturedItemParser, MykoItemParser},
    prelude::AnyItem,
    server::MykoServer,
};
use log::error;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{any::Any, sync::Arc};
use ts_rs::TS;

inventory::collect!(ItemRegistration);

/// Type alias for the registration function pointer
pub type RegisterFn = fn(&Arc<MykoServer>) -> Result<(), anyhow::Error>;

pub struct ItemRegistration {
    pub entity_type: &'static str,
    /// Crate where this entity is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Function pointer to register this entity with the server
    pub register_fn: RegisterFn,
}

/// Register all entities discovered via inventory with the server
///
/// This iterates through all `ItemRegistration`s collected via inventory
/// and calls their `register_fn`. Ensure dependent crates are linked first
/// by calling their `link()` function.
///
/// # Example
/// ```ignore
/// // Ensure entity crate is linked
/// rship_entities::link();
///
/// // Register all discovered entities
/// register_all_entities(&server)?;
/// ```
pub fn register_all_entities(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
    use log::{debug, info};

    let mut count = 0;
    for registration in inventory::iter::<ItemRegistration> {
        debug!("Registering entity: {}", registration.entity_type);
        (registration.register_fn)(server)?;
        count += 1;
    }
    info!("Registered {} entities", count);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: Arc<str>,
}

pub trait Eventable:
    AnyItem + MykoAutoQueries + MykoAutoReports + Serialize + DeserializeOwned + Clone + Sized + Any
{
    fn entity_name(&self) -> String;
    fn entity_name_static() -> String;
    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        let parser: Arc<dyn MykoItemParser> = Arc::new(CapturedItemParser::<Self>::new());

        // Direct send to EventManager (bypasses Server routing)
        if let Err(err) = server.event_manager.send_message(
            EventManagerMsg::RegisterRepo(Self::entity_name_static().into(), parser),
        ) {
            error!("Failed to register entity {}: {}", Self::entity_name_static(), err);
            return Err(anyhow::Error::msg(err));
        }

        // Register auto-generated queries
        <Self as MykoAutoQueries>::register_auto(server)?;

        // Register auto-generated reports
        <Self as MykoAutoReports>::register_auto(server)?;

        Ok(())
    }
}

pub trait MykoAutoQueries: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    fn register_auto(server: &Arc<MykoServer>) -> Result<(), anyhow::Error>;
}

pub trait MykoAutoReports: AnyItem + Serialize + DeserializeOwned + Clone + Sized + Any {
    fn register_auto(server: &Arc<MykoServer>) -> Result<(), anyhow::Error>;
}
