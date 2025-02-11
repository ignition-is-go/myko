use std::collections::HashMap;

use tokio::sync::mpsc::error::TrySendError;

use crate::{event::MEventType, item::Eventable, utils::matches};

pub struct Subscription<T: Eventable<T, PT>, PT: Clone> {
    pub state: HashMap<String, T>,
    pub tx: tokio::sync::mpsc::Sender<Vec<T>>,
    pub query: Box<PT>,
}

pub trait Publisher<T: Eventable<T, PT>, PT: Clone> {
    fn publish(&self) -> Result<(), TrySendError<Vec<T>>>;
    fn handle(&mut self, item: &T, event_type: MEventType);
}

impl<T: Eventable<T, PT> + PartialEq, PT: Clone> Publisher<T, PT> for Subscription<T, PT> {
    fn handle(&mut self, item: &T, event_type: MEventType) {
        if matches(item, &self.query) {
            match event_type {
                MEventType::SET => {
                    self.state.insert(item.id(), item.clone());
                    self.publish().expect("publish failed");
                }
                MEventType::DEL => {
                    self.state.remove(&item.id().clone());
                    self.publish().expect("publish failed");
                }
            }
        }
    }

    fn publish(&self) -> Result<(), TrySendError<Vec<T>>> {
        self.tx
            .try_send(self.state.values().cloned().collect::<Vec<T>>())
    }
}
