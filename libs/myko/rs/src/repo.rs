use std::sync::Mutex;

use crate::{
    event::{MEvent, MEventType},
    item::Eventable,
};
use futures_signals::signal_map::MutableBTreeMap;
use uuid::Uuid;

pub struct Repo<T: Eventable<T> + Default> {
    items: Mutex<MutableBTreeMap<String, T>>,
}

impl<'a, T: Eventable<T> + Default + Clone> Repo<T> {
    pub fn new() -> Self {
        Repo {
            items: Mutex::new(MutableBTreeMap::new()),
        }
    }

    pub fn add(self, item: T) {}

    pub fn remove(&mut self, id: &str) -> Option<T> {
        todo!();
    }

    pub fn set(&mut self, _item: T) -> Result<(), ()> {
        self.items
            .lock()
            .and_then(|tree| {
                let mut lock = tree.lock_mut();
                lock.insert(_item.id().to_owned(), _item.clone());
                Ok(())
            })
            .map_err(|_| ())
    }

    pub fn process(&mut self, event: MEvent) -> Result<(), serde_json::Error> {
        let ent = T::from_json(event.item_json())?;

        match event.change_type() {
            MEventType::SET => Ok(()),
            MEventType::DEL => Ok(()),
        }
    }
}
