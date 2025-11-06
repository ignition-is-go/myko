use std::{any::Any, sync::Arc};

use serde_json::Value;

#[derive(Debug, Clone)]
pub enum ProcessUpdateData {
    Set(Arc<dyn Any + Send + Sync>),
    Del(Arc<str>),
}

pub struct StartQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub query: Value,
}
