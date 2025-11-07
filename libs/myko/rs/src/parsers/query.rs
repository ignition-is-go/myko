use crate::{
    prelude::WithTransaction,
    query::{Query, QueryId},
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{fmt::Debug, sync::Arc};

pub trait AnyQuery: WithTransaction + QueryId + Debug + Send + Sync + 'static {}

pub trait MykoQueryParser: Send + Sync + 'static {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;
}

pub struct CapturedQueryParser<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T: Query + Send + Sync> Default for CapturedQueryParser<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Query + Send + Sync> CapturedQueryParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyQuery> MykoQueryParser for CapturedQueryParser<T> {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let item = serde_json::from_value::<T>(value)?;
        Ok(Arc::new(item))
    }
}
