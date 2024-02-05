use std::collections::HashMap;

use crate::{event::MEventType, item::Eventable};

pub struct Sub<T: Eventable<T, PT>, PT: Clone> {
    pub state: HashMap<String, T>,
    pub func: Box<dyn Fn(Vec<T>) -> ()>,
    pub query: Box<PT>,
}

pub trait Publisher<T: Eventable<T, PT>, PT: Clone> {
    fn publish(&self) -> ();
    fn handle(&mut self, item: &T, event_type: MEventType) -> ();
}
