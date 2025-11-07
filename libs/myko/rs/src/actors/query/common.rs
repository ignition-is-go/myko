use crate::parsers::item::AnyItem;
use serde_json::Value;
use std::sync::Arc;

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
