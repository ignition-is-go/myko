use crate::parsers::item::AnyItem;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug)]
pub enum ProcessUpdateData {
    Set(Arc<dyn AnyItem>),
    Del(Arc<str>),
}

pub struct StartQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub query: Value,
}

/// Update sent via streaming query subscriptions.
/// Used for inter-actor query subscriptions without polling.
#[derive(Clone, Debug)]
pub enum QueryStreamUpdate {
    /// Initial snapshot of all matching items
    Initial(BTreeMap<Arc<str>, Arc<dyn AnyItem>>),
    /// An item was added or updated
    Upsert(Arc<str>, Arc<dyn AnyItem>),
    /// An item was removed
    Remove(Arc<str>),
}
