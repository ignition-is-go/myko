use std::sync::Arc;

use hyphae::MapQuery;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::context::ViewBuildContext;
use crate::{cache::CacheKey, common::with_transaction::WithTransaction, wire::WrappedView};

pub trait ViewId {
    fn view_id(&self) -> Arc<str>;
}

pub trait ViewIdStatic {
    fn view_id_static() -> Arc<str>;
}

pub trait ViewItemType {
    type Item: hyphae::traits::CellValue;
    fn view_item_type(&self) -> Arc<str>;
    fn view_item_type_static() -> Arc<str>;
}

pub struct ViewBuildArgs<TView: ViewItemType> {
    pub view: Arc<TView>,
    pub view_context: ViewBuildContext,
}

/// Build the reactive `CellMap` for a view.
///
/// # Ordering
///
/// Views are **sorted by their `CellMap` key** (the `id` field on each view item).
/// The wire protocol sorts items lexicographically by key before sending them
/// to clients. To control sort order, use a compound key like
/// `format!("{sort_field}\x1F{unique_id}")` where `\x1F` (Unit Separator) sorts
/// before all printable characters.
pub trait ViewHandler: ViewItemType + Sized {
    /// Build a reactive map plan for this view.
    ///
    /// Returns `impl MapQuery<Arc<str>, Arc<Self::Item>>` so impls can chain
    /// `inner_join`, `project_map`, `select_cell`, etc. without materializing
    /// intermediate `CellMap`s. The framework materializes once at the
    /// registration boundary. Concrete `TypedViewCellMap`/`CellMap` values
    /// still satisfy the bound via the blanket impl on `ReactiveMap`.
    #[cfg(not(target_arch = "wasm32"))]
    fn build_cell(ctx: ViewBuildArgs<Self>) -> impl MapQuery<Arc<str>, Arc<Self::Item>>
    where
        Self: Send + Sync + 'static;

    #[cfg(target_arch = "wasm32")]
    fn build_cell(_ctx: ViewBuildArgs<Self>) -> impl MapQuery<Arc<str>, Arc<Self::Item>>
    where
        Self: Send + Sync + 'static,
    {
        unreachable!("view handlers execute on the server")
            as super::cell::TypedViewCellMap<Self::Item>
    }
}

pub trait AnyView: WithTransaction + ViewId + std::fmt::Debug + Send + Sync + 'static {
    fn view_item_type(&self) -> Arc<str>;
    fn to_value(&self) -> Value;
}

impl From<&dyn AnyView> for WrappedView {
    fn from(view: &dyn AnyView) -> Self {
        Self {
            view: view.to_value(),
            view_id: view.view_id(),
            view_item_type: view.view_item_type(),
            window: None,
        }
    }
}

impl From<Arc<dyn AnyView>> for WrappedView {
    fn from(view: Arc<dyn AnyView>) -> Self {
        Self::from(view.as_ref())
    }
}

impl From<&Arc<dyn AnyView>> for WrappedView {
    fn from(view: &Arc<dyn AnyView>) -> Self {
        Self::from(view.as_ref())
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
