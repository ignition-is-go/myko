//! ViewRequest wrapper type.

use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::traits::{
    AnyView, ViewBuildCellCtx, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams,
};
use crate::{TS, common::with_transaction::WithTransaction};

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ViewRequest<V> {
    pub tx: Arc<str>,
    #[serde(default = "default_created_at")]
    pub created_at: Arc<str>,
    #[serde(flatten)]
    pub view: V,
}

fn default_created_at() -> Arc<str> {
    Utc::now().to_rfc3339().into()
}

impl<V> ViewRequest<V> {
    pub fn new(view: V) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            created_at: default_created_at(),
            view,
        }
    }

    pub fn with_tx(view: V, tx: Arc<str>) -> Self {
        Self {
            tx,
            created_at: default_created_at(),
            view,
        }
    }
}

impl<V: Default> Default for ViewRequest<V> {
    fn default() -> Self {
        Self::new(V::default())
    }
}

impl<V: ViewParams> From<V> for ViewRequest<V> {
    fn from(view: V) -> Self {
        Self::new(view)
    }
}

impl<V: Clone> From<&ViewRequest<V>> for ViewRequest<V> {
    fn from(request: &ViewRequest<V>) -> Self {
        request.clone()
    }
}

impl<V: Send + Sync + 'static> WithTransaction for ViewRequest<V> {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone()
    }
}

impl<V: ViewId> ViewId for ViewRequest<V> {
    fn view_id(&self) -> Arc<str> {
        self.view.view_id()
    }
}

impl<V: ViewIdStatic> ViewIdStatic for ViewRequest<V> {
    fn view_id_static() -> Arc<str> {
        V::view_id_static()
    }
}

impl<V: ViewItemType> ViewItemType for ViewRequest<V> {
    type Item = V::Item;

    fn view_item_type(&self) -> Arc<str> {
        self.view.view_item_type()
    }

    fn view_item_type_static() -> Arc<str> {
        V::view_item_type_static()
    }
}

impl<V: ViewHandler + Clone + Send + Sync + 'static> ViewHandler for ViewRequest<V> {
    fn build_cell(ctx: ViewBuildCellCtx<Self>) -> impl hyphae::MapQuery<Arc<str>, Arc<Self::Item>> {
        // Materialize at the wrapper so the impl-trait return type infers
        // through the inner build_cell call uniformly.
        hyphae::MapQuery::materialize(V::build_cell(ViewBuildCellCtx {
            view: Arc::new(ctx.view.view.clone()),
            view_context: ctx.view_context,
        }))
    }
}

impl<V: ViewId + ViewItemType + Serialize + std::fmt::Debug + Send + Sync + 'static> AnyView
    for ViewRequest<V>
{
    fn view_item_type(&self) -> Arc<str> {
        ViewItemType::view_item_type(self)
    }

    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("ViewRequest should serialize to JSON")
    }
}
