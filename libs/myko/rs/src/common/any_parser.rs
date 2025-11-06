use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{any::Any, sync::Arc};

pub struct CapturedTypeParser<T: Send + Sync> {
    phantom: std::marker::PhantomData<T>,
}
/// Provides an interface for parsing serde_json::Value into Arc<dyn Any, with type information captured at the declaration of the parser.
/// returns success if the value is successfully parsed into the captured type, otherwise returns an error.
pub trait MykoAnyParser: Send + Sync + 'static {
    fn parse(&self, value: Value) -> Result<Arc<dyn Any + Send + Sync>, anyhow::Error>;
}

impl<T: DeserializeOwned + Send + Sync> CapturedTypeParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Send + Sync + DeserializeOwned + 'static> MykoAnyParser for CapturedTypeParser<T> {
    fn parse(&self, value: Value) -> Result<Arc<dyn Any + Send + Sync>, anyhow::Error> {
        let item = serde_json::from_value::<T>(value);

        match item {
            Ok(item) => Ok(Arc::new(item)),
            Err(err) => Err(anyhow::anyhow!("Failed to parse JSON value: {}", err)),
        }
    }
}
