use crate::event::MEvent;

pub enum PersistEvent {
    Persist,
    NoPersist,
}

pub struct ProcessEventData {
    pub event: MEvent,
    pub persist: PersistEvent,
}
