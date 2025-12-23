use std::sync::Arc;

use crate::{event::MEvent, prelude::AnyItem};

#[derive(Clone, Copy)]
pub enum PersistEvent {
    Persist,
    NoPersist,
}

#[derive(Clone)]
pub struct ProcessEventData {
    pub event: MEvent,
    pub persist: PersistEvent,
    /// Pre-parsed item for locally emitted events to avoid serialize/deserialize roundtrip.
    /// When present, EventHandler will use this instead of parsing from event.item.
    pub parsed_item: Option<Arc<dyn AnyItem>>,
    /// Client ID of the WebSocket connection that sent this event.
    /// Used to auto-populate `#[myko_client_id]` fields on entities.
    pub client_id: Option<Arc<str>>,
}
