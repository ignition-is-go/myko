//! Wire protocol types for items.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Wrapper for items in wire protocol responses.
/// Contains the serialized item and its type name.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedItem<T> {
    pub item: T,
    pub item_type: Arc<str>,
}
