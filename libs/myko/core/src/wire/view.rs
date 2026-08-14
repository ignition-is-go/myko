use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    query::{QueryResponse, QueryWindow},
    shared::value_with_tx,
};
use crate::{
    TS,
    core::view::{ViewId, ViewItemType},
};

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewWindowUpdate {
    pub tx: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedView {
    pub view: Value,
    pub view_id: Arc<str>,
    pub view_item_type: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewError {
    pub tx: String,
    pub view_id: String,
    pub message: String,
}

impl ViewError {
    pub fn new(
        tx: impl Into<String>,
        view_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            tx: tx.into(),
            view_id: view_id.into(),
            message: message.into(),
        }
    }
}

pub type ViewResponse = QueryResponse;

///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn wrap_view<V: ViewId + ViewItemType + Serialize + Clone>(
    tx: Arc<str>,
    view: &V,
) -> Result<WrappedView, serde_json::Error> {
    Ok(WrappedView {
        view: value_with_tx(tx, view)?,
        view_id: view.view_id(),
        view_item_type: view.view_item_type(),
        window: None,
    })
}
