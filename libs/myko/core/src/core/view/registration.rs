//! View registration via inventory.

use std::{any::Any, sync::Arc};

use hyphae::MapQuery;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    cell::{ErasedViewMap, FilteredViewCellMap, materialize_erased_view},
    context::{ViewBuildContext, ViewContext},
    request::ViewRequest,
    traits::{AnyView, ViewBuildArgs, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams},
};
use crate::{
    common::with_id::WithId, item::Eventable, request::RequestContext, server::MykoServerContext,
    store::StoreRegistry,
};

/// Type alias for view parse function.
pub type ViewParseFn = fn(Value) -> Result<Arc<dyn AnyView>, anyhow::Error>;

/// Type alias for view cell factory.
pub type ViewCellFactory = fn(
    Arc<dyn AnyView>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Arc<MykoServerContext>,
) -> Result<FilteredViewCellMap, String>;

/// Registration entry for a view type.
/// Collected via inventory for automatic discovery.
pub struct ViewRegistration {
    /// View identifier (e.g., "`GetTargetTreeByParentFiltered`")
    pub view_id: &'static str,
    /// View output item type (e.g., "`TargetTreeView`")
    pub view_item_type: &'static str,
    /// Crate where this view is defined (for `type_gen` filtering)
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

/// Type alias for a view subscription factory: the view's output as erased
/// rows read from its typed map, with no erased copy.
pub type ViewSubscribeFactory = fn(
    Arc<dyn AnyView>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Arc<MykoServerContext>,
) -> Result<ErasedViewMap, String>;

/// Subscription factory for a view, registered beside its
/// [`ViewRegistration`]. Views without one subscribe through `cell_factory`.
pub struct ViewSubscribeRegistration {
    pub view_id: &'static str,
    pub subscribe: ViewSubscribeFactory,
}

inventory::collect!(ViewSubscribeRegistration);

/// Factory trait for creating view registration data.
pub trait ViewFactory: ViewParams {
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn parse(value: Value) -> Result<Arc<dyn AnyView>, anyhow::Error>;

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn cell_factory(
        view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Result<FilteredViewCellMap, String>;

    /// Build the view for a live subscription.
    ///
    /// Erases per read from the view's typed map, so a view whose plan is
    /// already a map costs no extra scheduled write per changed row.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn subscribe_factory(
        view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Result<ErasedViewMap, String> {
        Self::cell_factory(view, registry, request_ctx, server_ctx).map(ErasedViewMap::from)
    }
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
        let view = crate::reflection::deserialize_operation::<ViewRequest<V>>(value)?;
        Ok(Arc::new(view))
    }

    fn cell_factory(
        any_view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Result<FilteredViewCellMap, String> {
        let _span =
            tracing::trace_span!("myko.view", view = V::view_id_static().as_ref()).entered();
        build_view::<V>(&any_view, registry, request_ctx, server_ctx).map(materialize_erased_view)
    }

    fn subscribe_factory(
        any_view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
    ) -> Result<ErasedViewMap, String> {
        let _span =
            tracing::trace_span!("myko.view", view = V::view_id_static().as_ref()).entered();
        let args = view_args::<V>(&any_view, registry, request_ctx.clone(), server_ctx.clone())?;
        if let Some(subscription) = V::subscribe(&args) {
            return Ok(subscription);
        }
        // A view merged across partitions has a graph per subscriber, which
        // the shared cache refuses to hold.
        if crate::server::partition::is_merged(&V::view_id_static()) {
            return Ok(ErasedViewMap::from_typed(V::build_cell(args).materialize()));
        }
        // Subscribers with the same parameters (and, for a `PerClient` view,
        // the same caller) read one cached graph, so N identical
        // subscriptions maintain one view, and the rows they are sent are the
        // same allocations, which lets the wire layer encode each row once.
        Ok(ErasedViewMap::from(
            server_ctx.view_map_untyped((*args.view).clone(), request_ctx),
        ))
    }
}

fn build_view<V>(
    any_view: &Arc<dyn AnyView>,
    registry: Arc<StoreRegistry>,
    request_ctx: Arc<RequestContext>,
    server_ctx: Arc<MykoServerContext>,
) -> Result<impl MapQuery<Key = Arc<str>, Value = Arc<<V as ViewItemType>::Item>>, String>
where
    V: ViewParams + ViewHandler + std::fmt::Debug + DeserializeOwned,
{
    view_args::<V>(any_view, registry, request_ctx, server_ctx).map(V::build_cell)
}

fn view_args<V>(
    any_view: &Arc<dyn AnyView>,
    registry: Arc<StoreRegistry>,
    request_ctx: Arc<RequestContext>,
    server_ctx: Arc<MykoServerContext>,
) -> Result<ViewBuildArgs<V>, String>
where
    V: ViewParams + ViewHandler + std::fmt::Debug + DeserializeOwned,
{
    // Bounded cardinality (one span per view registration), matching
    // `myko.query`/`myko.command`.
    crate::server::dispatch_metrics::record_view(
        V::view_id_static().as_ref(),
        request_ctx.origin(),
    );
    tracing::trace!("ViewFactory build start view_id={}", V::view_id_static());
    let any_ref: &dyn Any = any_view.as_ref();
    let request: ViewRequest<V> =
        crate::common::downcast::downcast_request(any_ref, "view payload")?;
    let view: Arc<V> = Arc::new(request.view);
    let view_ctx = Arc::new(ViewContext::new(request_ctx, registry, server_ctx));
    Ok(ViewBuildArgs {
        view,
        view_context: ViewBuildContext::new(view_ctx),
    })
}
