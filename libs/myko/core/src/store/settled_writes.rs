//! Store writes that the settle carrying them applies.
//!
//! A store write made while an event set is being built changes the store at
//! once, and the store tells its subscribers what it holds when the set
//! settles. Several queued event sets that write one key are then told as
//! one change. That is wrong for an entity type declared with
//! `#[myko_sequential]`, whose every event must reach subscribers.
//!
//! A sequential type's write therefore goes through [`SettledWrites`]. The
//! write is queued in the event set and changes the store when the settle
//! that carries the set fires it. The store then tells its subscribers in
//! that same settle. Until then a read of the store does not see the write.

use std::sync::Arc;

use hyphae::{Cell, CellMutable, MapDiff, Signal, Watchable};

use super::EntityStore;
use crate::core::item::AnyItem;

/// One change to an entity store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreWrite {
    Insert(Arc<str>, Arc<dyn AnyItem>),
    Remove(Arc<str>),
    InsertMany(Vec<(Arc<str>, Arc<dyn AnyItem>)>),
    RemoveMany(Vec<Arc<str>>),
    Batch(Vec<MapDiff<Arc<str>, Arc<dyn AnyItem>>>),
}

impl StoreWrite {
    pub fn apply(self, store: &EntityStore) {
        match self {
            Self::Insert(id, item) => {
                store.insert(id, item);
            }
            Self::Remove(id) => {
                store.remove(&id);
            }
            Self::InsertMany(entries) => store.insert_many(entries),
            Self::RemoveMany(ids) => store.remove_many(ids),
            Self::Batch(changes) => store.apply_batch(changes),
        }
    }
}

/// Applies each write pushed to it when the settle that carries the write
/// fires.
pub struct SettledWrites {
    /// Written with `notify`, so writes that share a settle are all
    /// delivered, in order.
    queue: Cell<Option<StoreWrite>, CellMutable>,
}

impl SettledWrites {
    pub fn new(store: &EntityStore) -> Self {
        let queue = Cell::new(None::<StoreWrite>);
        let target = store.downgrade();
        // The store owns the subscription, which puts its cells above the
        // queue: what the store writes from this callback joins the settle
        // that fired the queue.
        store.own(queue.subscribe(move |signal| {
            if let Signal::Value(write) = signal
                && let Some(write) = write.as_ref()
                && let Some(store) = target.upgrade()
            {
                write.clone().apply(&store);
            }
        }));
        Self { queue }
    }

    pub fn push(&self, write: StoreWrite) {
        self.queue.notify(Signal::value(Some(write)));
    }
}
