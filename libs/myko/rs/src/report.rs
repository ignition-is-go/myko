use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::MykoClient;

pub trait MykoReport<T> {
    fn watch(&self, client: &MykoClient) -> impl tokio_stream::Stream<Item = T>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportResponse {
    pub response: Value,

    pub tx: String,
}

impl ReportResponse {
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedReport {
    pub report: Value,
    pub report_id: String,
}
