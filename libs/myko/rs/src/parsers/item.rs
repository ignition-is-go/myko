use crate::common::{to_value::ToValue, with_id::WithId};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{any::Any, fmt::Debug, sync::Arc};

pub trait AnyItem: WithId + ToValue + Any + Debug + Send + Sync + 'static {}

pub trait MykoItemParser: Send + Sync + 'static {
    fn parse(&self, input: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error>;
}

pub struct CapturedItemParser<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T> Default for CapturedItemParser<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CapturedItemParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyItem> MykoItemParser for CapturedItemParser<T> {
    fn parse(&self, item: Value) -> Result<Arc<dyn AnyItem>, anyhow::Error> {
        let item = serde_json::from_value::<T>(item)?;
        Ok(Arc::new(item))
    }
}
