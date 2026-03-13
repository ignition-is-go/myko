use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::context::ViewCellContext;
use crate::{cache::CacheKey, common::with_transaction::WithTransaction, wire::WrappedView};

pub trait ViewId {
    fn view_id(&self) -> Arc<str>;
}

pub trait ViewIdStatic {
    fn view_id_static() -> Arc<str>;
}

pub trait ViewItemType {
    type Item: hypha::traits::CellValue;
    fn view_item_type(&self) -> Arc<str>;
    fn view_item_type_static() -> Arc<str>;
}

pub struct ViewBuildCellCtx<TView: ViewItemType> {
    pub view: Arc<TView>,
    pub view_context: ViewCellContext,
}

pub trait ViewHandler: ViewItemType + Sized {
    fn build_cell(ctx: ViewBuildCellCtx<Self>) -> super::cell::TypedViewCellMap<Self::Item>
    where
        Self: Send + Sync + 'static;
}

pub trait AnyView: WithTransaction + ViewId + std::fmt::Debug + Send + Sync + 'static {
    fn view_item_type(&self) -> Arc<str>;
    fn to_value(&self) -> Value;
}

impl From<&dyn AnyView> for WrappedView {
    fn from(view: &dyn AnyView) -> Self {
        WrappedView {
            view: view.to_value(),
            view_id: view.view_id(),
            view_item_type: view.view_item_type(),
            window: None,
        }
    }
}

impl From<Arc<dyn AnyView>> for WrappedView {
    fn from(view: Arc<dyn AnyView>) -> Self {
        WrappedView::from(view.as_ref())
    }
}

impl From<&Arc<dyn AnyView>> for WrappedView {
    fn from(view: &Arc<dyn AnyView>) -> Self {
        WrappedView::from(view.as_ref())
    }
}

/// Trait bound bundle for view parameter types.
pub trait ViewParams:
    CacheKey
    + serde::Serialize
    + DeserializeOwned
    + Clone
    + Send
    + Sync
    + ViewId
    + ViewIdStatic
    + ViewItemType
    + ViewHandler
    + std::fmt::Debug
    + 'static
{
}

impl<T> ViewParams for T where
    T: serde::Serialize
        + CacheKey
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + ViewId
        + ViewIdStatic
        + ViewItemType
        + ViewHandler
        + std::fmt::Debug
        + 'static
{
}
