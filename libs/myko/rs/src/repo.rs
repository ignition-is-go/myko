use std::collections::HashMap;

use serde::de::DeserializeOwned;

use crate::{
    event::{MEvent, MEventType},
    item::{matches, Eventable},
    subscription::{Publisher, Sub},
};

impl<T: Eventable<T, PT> + PartialEq, PT: Clone> Publisher<T, PT> for Sub<T, PT> {
    fn handle(&mut self, item: &T, event_type: MEventType) -> () {
        if matches(item, &self.query) {
            match event_type {
                MEventType::SET => {
                    self.state.insert(item.id(), item.clone());
                    self.publish();
                }
                MEventType::DEL => {
                    self.state.remove(&item.id().clone());
                    self.publish();
                }
            }
        }
    }

    fn publish(&self) -> () {
        (self.func)(self.state.values().cloned().collect());
    }
}

pub struct Repo<T: Eventable<T, PT>, PT: Clone> {
    subs: Vec<Box<Sub<T, PT>>>,
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

        let sub = Sub {
            state: self.state.clone(),
            func,
            query: Box::new(query),
        };

        self.subs.push(Box::new(sub));
    }
}

fn filter_query<T: Eventable<T, PT> + PartialEq, PT: Clone>(
    state: &HashMap<String, T>,
    query: &PT,
) -> HashMap<String, T> {
    state
        .iter()
        .filter(|(_, v)| matches(*v, query))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
