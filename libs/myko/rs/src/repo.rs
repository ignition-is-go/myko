use crate::{
    event::{MEvent, MEventType},
    item::Eventable,
    subscription::{Publisher, Subscription},
    utils::filter_query,
};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub struct RepoStruct<T: Eventable<T, PT>, PT: Clone> {
    subs: Vec<Arc<Mutex<Subscription<T, PT>>>>,
    state: HashMap<String, T>,
}

pub trait Repo<T, PT> {
    fn watch(&mut self, func: Arc<dyn Fn(Vec<T>) -> ()>, query: PT);
}

impl<T: Eventable<T, PT> + PartialEq + DeserializeOwned, PT: Clone> RepoStruct<T, PT> {
    pub fn new() -> Self {
        RepoStruct {
            subs: vec![],
            state: HashMap::new(),
        }
    }
}

impl<T: Eventable<T, PT> + PartialEq, PT: Clone> RepoStruct<T, PT> {
    pub async fn listen(&mut self, mut rx: tokio::sync::broadcast::Receiver<MEvent>) {
        loop {
            let event = rx.recv().await.unwrap();
            self.process(event).await.unwrap();
        }
    }

    pub async fn process(&mut self, event: MEvent) -> Result<(), serde_json::Error> {
        let ent = serde_json::from_value::<T>(event.item_json())?;

        match event.change_type() {
            MEventType::SET => {
                self.set(&ent);
            }
            MEventType::DEL => {
                self.remove(ent.id());
            }
        };

        let event_type = event.change_type();
        for sub in self.subs.iter_mut() {
            sub.lock().await.handle(&ent, event_type);
        }

        Ok(())
    }

    fn remove(&mut self, id: String) -> Option<T> {
        self.state.remove(&id)
    }

    fn set(&mut self, item: &T) {
        self.state.insert(item.id(), item.clone());
    }
}

impl<T: Eventable<T, PT> + PartialEq + DeserializeOwned, PT: Clone> Repo<T, PT>
    for RepoStruct<T, PT>
{
    fn watch(&mut self, func: Arc<dyn Fn(Vec<T>) -> ()>, query: PT) {
        let initial = filter_query(&self.state, &query);

        func(initial.values().cloned().collect());

        let sub = Subscription {
            state: self.state.clone(),
            func: func.clone(),
            query: Box::new(query),
        };

        self.subs.push(Arc::new(Mutex::new(sub)));
    }
}
