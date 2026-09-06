//! View registration via inventory.

use std::{any::Any, sync::Arc};

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    context::{ViewBuildContext, ViewContext},
    output::{RegisteredViewOutput, ViewBuildOutput},
    request::ViewRequest,
    traits::{AnyView, ViewBuildArgs, ViewHandler, ViewId, ViewIdStatic, ViewItemType, ViewParams},
};
use crate::{
    common::with_id::WithId, item::Eventable, request::RequestContext, server::MykoServerContext,
    store::StoreRegistry,
};

/// Type alias for view parse function.
pub type ViewParseFn = fn(Value) -> Result<Arc<dyn AnyView>, anyhow::Error>;

#[cfg(not(target_arch = "wasm32"))]
pub type ViewAuthorityFactory =
    fn(Value, myko_federation::NodeId) -> Result<crate::server::HandlerAuthority, String>;

/// Type alias for view cell factory.
pub type ViewCellFactory = fn(
    Arc<dyn AnyView>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Arc<MykoServerContext>,
    Option<crate::server::federated_source::FederatedRequest>,
) -> Result<RegisteredViewOutput, String>;

/// Registration entry for a view type.
/// Collected via inventory for automatic discovery.
pub struct ViewRegistration {
    /// View identifier (e.g., "`GetTargetTreeByParentFiltered`")
    pub view_id: &'static str,
    /// Typed service owner used by application activation.
    pub service_id: Option<crate::ServiceTypeId>,
    /// View output item type (e.g., "`TargetTreeView`")
    pub view_item_type: &'static str,
    /// Crate where this view is defined (for `type_gen` filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing view params from JSON
    pub parse: ViewParseFn,
    /// Factory for creating reactive cell from view params
    pub cell_factory: ViewCellFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: ViewAuthorityFactory,
    /// View struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// View struct's doc comment, if any.
    pub description: Option<&'static str>,
}

inventory::collect!(ViewRegistration);

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
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
    ) -> Result<RegisteredViewOutput, String>;

    #[cfg(not(target_arch = "wasm32"))]
    /// Resolve typed source, scope, claims, and capabilities before opening.
    ///
    /// # Errors
    ///
    /// Returns an error when the serialized view parameters are invalid.
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String>;
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
    #[cfg(not(target_arch = "wasm32"))]
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String> {
        let view: V = serde_json::from_value(value).map_err(|error| error.to_string())?;
        Ok(crate::server::HandlerAuthority {
            source_node: view.source_node(local_node),
            scope_id: view.scope_id(local_node),
            resource_claims: view.authority_claims(local_node),
            application_capabilities: view.required_capabilities(),
        })
    }

    fn parse(value: Value) -> Result<Arc<dyn AnyView>, anyhow::Error> {
        tracing::trace!("ViewFactory::parse view_id={}", V::view_id_static());
        let view = serde_json::from_value::<ViewRequest<V>>(value)?;
        Ok(Arc::new(view))
    }

    fn cell_factory(
        any_view: Arc<dyn AnyView>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
    ) -> Result<RegisteredViewOutput, String> {
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
        let request: ViewRequest<V> =
            crate::common::downcast::downcast_request(any_ref, "view payload")?;
        let view: Arc<V> = Arc::new(request.view);

        let view_ctx = Arc::new(ViewContext::new_routed(
            request_ctx,
            registry,
            server_ctx,
            #[cfg(not(target_arch = "wasm32"))]
            federated.clone(),
        ));
        let view_cell_ctx = ViewBuildContext::new(view_ctx);

        let built = V::build_cell(ViewBuildArgs {
            view,
            view_context: view_cell_ctx,
            #[cfg(not(target_arch = "wasm32"))]
            federated,
        });
        tracing::trace!(
            "ViewFactory::cell_factory using build_cell view_id={}",
            V::view_id_static()
        );
        Ok(built.into_registered())
    }
}
