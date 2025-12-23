use crate::report::AnyReport;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;

/// Parser trait for deserializing reports from JSON.
pub trait MykoReportParser: Send + Sync + 'static {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error>;
}

/// Captured report parser that knows the concrete type.
pub struct CapturedReportParser<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T> Default for CapturedReportParser<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CapturedReportParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyReport> MykoReportParser for CapturedReportParser<T> {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyReport>, anyhow::Error> {
        let report = serde_json::from_value::<T>(value)?;
        Ok(Arc::new(report))
    }
}
