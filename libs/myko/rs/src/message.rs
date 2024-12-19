use myko_wasm::event::MEvent;
use serde::{Deserialize, Serialize};

use crate::query::{QueryResponse, WrappedQuery};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", content = "data")]
pub enum MykoMessage<Commands> {
    #[serde(rename = "ws:m:query")]
    Query(WrappedQuery),
    #[serde(rename = "ws:m:query-response")]
    QueryResponse(QueryResponse),
    #[serde(rename = "ws:m:event")]
    Event(MEvent),
    #[serde(rename = "ws:m:command")]
    Command(Commands),
}
