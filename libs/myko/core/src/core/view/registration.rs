//! View registration via inventory.

use std::{any::Any, sync::Arc};

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    cell::{FilteredViewCellMap, erase_typed_view_map},
    context::{ViewCellContext, ViewContext},
    request::ViewRequest,
    traits::{
        AnyView, ViewBuildCellCtx, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams,
    },
};
use crate::{
    common::with_id::WithId, item::Eventable, request::RequestContext, server::MykoServerCtx,
    store::StoreRegistry,
};

/// Type alias for view parse function.
pub type ViewParseFn = fn(Value) -> Result<Arc<dyn AnyView>, anyhow::Error>;

/// Type alias for view cell factory.
pub type ViewCellFactory = fn(
    Arc<dyn AnyView>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Arc<MykoServerCtx>,
) -> Result<FilteredViewCellMap, String>;

/// Registration entry for a view type.
/// Collected via inventory for automatic discovery.
pub struct ViewRegistration {
    /// View identifier (e.g., "GetTargetTreeByParentFiltered")
    pub view_id: &'static str,
    /// View output item type (e.g., "TargetTreeView")
    pub view_item_type: &'static str,
    /// Crate where this view is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing view params from JSON
    pub parse: ViewParseFn,
    /// Factory for creating reactive cell from view params
    pub cell_factory: ViewCellFactory,
    /// View struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// View struct's doc comment, if any.
    pub description: Option<&'static str>,
}

inventory::collect!(ViewRegistration);

/// Factory trait for creating view registration data.
pub trait ViewFactory: ViewParams {
    fn parse(value: Value) -> Result<Arc<dyn AnyView>, anyhow::Error>;

    fn cell_factory(
        view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerCtx>,
    ) -> Result<FilteredViewCellMap, String>;
}

impl<V> ViewFactory for V
where
    V: ViewParams
        + ViewId
        + ViewIdStatic
        + ViewItemType
        + ViewHandler
        + std::fmt::Debug
        + DeserializeOwned,
    <V as ViewItemType>::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn parse(value: Value) -> Result<Arc<dyn AnyView>, anyhow::Error> {
        tracing::trace!("ViewFactory::parse view_id={}", V::view_id_static());
        let view = serde_json::from_value::<ViewRequest<V>>(value)?;
        Ok(Arc::new(view))
    }

    fn cell_factory(
        any_view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerCtx>,
    ) -> Result<FilteredViewCellMap, String> {
        // Bounded cardinality (one span per view registration), matching
        // `myko.query`/`myko.command`.
        let _span =
            tracing::trace_span!("myko.view", view = V::view_id_static().as_ref()).entered();
        crate::server::dispatch_metrics::record_view(
            V::view_id_static().as_ref(),
            request_ctx.origin(),
        );
        tracing::trace!(
            "ViewFactory::cell_factory start view_id={}",
            V::view_id_static()
        );
        let any_ref: &dyn Any = any_view.as_ref();
        let request: ViewRequest<V> = any_ref
            .downcast_ref::<ViewRequest<V>>()
            .cloned()
            .ok_or_else(|| "Failed to downcast view payload".to_string())?;
        let view: Arc<V> = Arc::new(request.view);

        let view_ctx = Arc::new(ViewContext::new(
            request_ctx.clone(),
            registry.clone(),
            server_ctx.clone(),
        ));
        let view_cell_ctx =
            ViewCellContext::new(request_ctx, view_ctx, registry.clone(), server_ctx);

        let built = V::build_cell(ViewBuildCellCtx {
            view: view.clone(),
            view_context: view_cell_ctx.clone(),
        });
        tracing::trace!(
            "ViewFactory::cell_factory using build_cell view_id={}",
            V::view_id_static()
        );
        Ok(erase_typed_view_map(hyphae::MapQuery::materialize(built)))
    }
}
