use crate::{
    subscription::{Publisher, Subscription},
    utils::filter_query,
};
use myko_wasm::{
    event::{MEvent, MEventType},
    item::Eventable,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc::Receiver, Mutex};

pub struct RepoStruct<T: Eventable<T, PT>, PT: Clone> {
    subs: Vec<Arc<Mutex<Subscription<T, PT>>>>,
    state: HashMap<String, T>,
}

pub trait Repo<T, PT> {
    fn watch(&mut self, query: PT) -> Receiver<Vec<T>>
    where
        Self: Sized;
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
    pub async fn process(&mut self, event: MEvent) -> Result<(), serde_json::Error> {
        let mut item = event.item_json();

        let res = item.as_object_mut();

        if res.is_none() {
            return Err(serde::de::Error::custom("Invalid JSON"));
        }

        let mut json = res.unwrap().to_owned();

        let hash = json.get("hash");

        if hash.is_none() {
            let computed_hash = md5::compute(event.item_json().to_string());
            let hash_string = format!("{:x}", computed_hash);

            json.insert("hash".to_string(), Value::String(hash_string));
        }

        let ent = serde_json::from_value::<T>(Value::Object(json))?;

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
    fn watch(&mut self, query: PT) -> Receiver<Vec<T>> {
        let initial = filter_query(&self.state, &query);

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<T>>(1);

        tx.try_send(initial.values().cloned().collect()).unwrap();

        let sub = Subscription {
            state: self.state.clone(),
            tx,
            query: Box::new(query),
        };

        self.subs.push(Arc::new(Mutex::new(sub)));

        rx
    }
}
