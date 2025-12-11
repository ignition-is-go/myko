use std::sync::Arc;

use crate::{event::MEvent, prelude::AnyItem};

pub enum PersistEvent {
    Persist,
    NoPersist,
}

pub struct ProcessEventData {
    pub event: MEvent,
    pub persist: PersistEvent,
    /// Pre-parsed item for locally emitted events to avoid serialize/deserialize roundtrip.
    /// When present, EventHandler will use this instead of parsing from event.item.
    pub parsed_item: Option<Arc<dyn AnyItem>>,
}
