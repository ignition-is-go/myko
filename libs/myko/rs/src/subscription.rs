use std::collections::HashMap;

use crate::{event::MEventType, item::Eventable, utils::matches};

pub struct Subscription<T: Eventable<T, PT>, PT: Clone> {
    pub state: HashMap<String, T>,
    pub func: Box<dyn Fn(Vec<T>) -> ()>,
    pub query: Box<PT>,
}

pub trait Publisher<T: Eventable<T, PT>, PT: Clone> {
    fn publish(&self) -> ();
    fn handle(&mut self, item: &T, event_type: MEventType) -> ();
}

impl<T: Eventable<T, PT> + PartialEq, PT: Clone> Publisher<T, PT> for Subscription<T, PT> {
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
