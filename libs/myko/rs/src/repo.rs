use crate::{
    event::{MEvent, MEventType},
    item::Eventable,
    subscription::{Publisher, Subscription},
    utils::filter_query,
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;

pub struct Repo<T: Eventable<T, PT>, PT: Clone> {
    subs: Vec<Box<Subscription<T, PT>>>,
    state: HashMap<String, T>,
}

impl<T: Eventable<T, PT> + PartialEq + DeserializeOwned, PT: Clone> Repo<T, PT> {
    pub fn new() -> Self {
        Repo {
            state: HashMap::new(),
            subs: Vec::new(),
        }
    }

    pub fn remove(&mut self, id: String) -> Option<T> {
        self.state.remove(&id)
    }

    pub fn set(&mut self, item: &T) {
        self.state.insert(item.id(), item.clone());
    }

    pub fn process(&mut self, event: MEvent) -> Result<(), serde_json::Error> {
        let ent = serde_json::from_value::<T>(event.item_json())?;

        match event.change_type() {
            MEventType::SET => {
                self.set(&ent);
            }
            MEventType::DEL => {
                self.remove(ent.id());
            }
        };

        self.subs
            .iter_mut()
            .for_each(|sub| sub.handle(&ent, event.change_type()));

        Ok(())
    }

    pub fn watch(&mut self, func: Box<dyn Fn(Vec<T>) -> ()>, query: PT) {
        let initial = filter_query(&self.state, &query);

        func(initial.values().cloned().collect());

        let sub = Subscription {
            state: self.state.clone(),
            func,
            query: Box::new(query),
        };

        self.subs.push(Box::new(sub));
    }
}
