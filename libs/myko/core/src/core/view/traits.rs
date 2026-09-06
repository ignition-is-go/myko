use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::context::ViewBuildContext;
use crate::{
    cache::CacheKey, common::with_transaction::WithTransaction, item::AnyItem, wire::WrappedView,
};

pub trait ViewId {
    fn view_id(&self) -> Arc<str>;
}

pub trait ViewIdStatic {
    fn view_id_static() -> Arc<str>;
}

pub trait ViewItemType {
    type Item: AnyItem + hyphae::traits::CellValue;
    fn view_item_type(&self) -> Arc<str>;
    fn view_item_type_static() -> Arc<str>;
}

pub struct ViewBuildArgs<TView: ViewItemType> {
    pub view: Arc<TView>,
    pub view_context: ViewBuildContext,
    #[cfg(not(target_arch = "wasm32"))]
    pub federated: Option<crate::server::federated_source::FederatedRequest>,
}

impl<TView: ViewItemType> ViewBuildArgs<TView> {
    /// Resolve one process-local resource installed by the application host.
    ///
    /// # Errors
    ///
    /// Returns an error when the resource is not installed or its registry is
    /// unavailable.
    pub fn resource<T>(&self) -> Result<Arc<T>, crate::AppError>
    where
        T: Send + Sync + 'static,
    {
        self.view_context
            .view_context
            .server_ctx
            .application_resource::<T>()
    }

    /// Open the request's durable item source through the retained map runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when this is not a federated request or the source
    /// projection cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn federated_items<T>(&self) -> Result<crate::query::FilteredCellMap, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        let request = self
            .federated
            .as_ref()
            .ok_or_else(|| "view was not opened from a federation request".to_owned())?;
        let runtime = self
            .view_context
            .view_context
            .server_ctx
            .federated()
            .ok_or_else(|| "server has no federation runtime".to_owned())?;
        runtime
            .items::<T>(request.source_node, request.scope_id.clone())
            .map(|source| source.rows())
    }

    /// Open this request's scope across every authoritative source while
    /// retaining source identity and revision metadata on every row.
    ///
    /// # Errors
    ///
    /// Returns an error when this is not a scoped federated request or the
    /// multi-source projection cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn federated_items_across_sources<T>(
        &self,
    ) -> Result<crate::server::SourcedItemMap<T>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        let request = self
            .federated
            .as_ref()
            .ok_or_else(|| "view was not opened from a federation request".to_owned())?;
        let scope_id = request
            .scope_id
            .clone()
            .ok_or_else(|| "multi-source view requires a concrete scope".to_owned())?;
        self.view_context
            .view_context
            .server_ctx
            .federated()
            .ok_or_else(|| "server has no federation runtime".to_owned())?
            .items_across_sources::<T>(scope_id)
    }

    /// Open an explicit exact scope or subtree across authoritative sources.
    ///
    /// # Errors
    ///
    /// Returns an error when this is not a federated request or the selected
    /// projection cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn federated_items_across_sources_selected<T>(
        &self,
        selection: myko_federation::ScopeSelection,
    ) -> Result<crate::server::SourcedItemMap<T>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        let _request = self
            .federated
            .as_ref()
            .ok_or_else(|| "view was not opened from a federation request".to_owned())?;
        self.view_context
            .view_context
            .server_ctx
            .federated()
            .ok_or_else(|| "server has no federation runtime".to_owned())?
            .items_across_sources_selected::<T>(selection)
    }

    /// Open canonical accepted-history snapshots for an exact scope or subtree.
    ///
    /// # Errors
    ///
    /// Returns an error when this is not a federated request or the selected
    /// projection snapshot cannot be established.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sourced_snapshots_selected<T>(
        &self,
        selection: myko_federation::ScopeSelection,
    ) -> Result<myko_federation::LiveSubscription<crate::server::SourcedItemSnapshot<T>>, String>
    where
        T: crate::MykoItem + crate::item::Eventable + crate::item::AnyItem,
    {
        let _request = self
            .federated
            .as_ref()
            .ok_or_else(|| "view was not opened from a federation request".to_owned())?;
        self.view_context
            .view_context
            .server_ctx
            .federated()
            .ok_or_else(|| "server has no federation runtime".to_owned())?
            .sourced_snapshots_selected::<T>(selection)
    }
}

/// Build a registered output for a view.
///
/// # Ordering
///
/// Local views are **sorted by their `CellMap` key** (the `id` field on each view item).
/// The wire protocol sorts items lexicographically by key before sending them
/// to clients. To control sort order, use a compound key like
/// `format!("{sort_field}\x1F{unique_id}")` where `\x1F` (Unit Separator) sorts
/// before all printable characters.
pub trait ViewHandler: ViewItemType + Sized {
    #[cfg(not(target_arch = "wasm32"))]
    fn source_node(&self, local_node: myko_federation::NodeId) -> Option<myko_federation::NodeId> {
        Some(local_node)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn scope_id(&self, _local_node: myko_federation::NodeId) -> Option<myko_federation::ScopeId> {
        None
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn authority_claims(
        &self,
        _local_node: myko_federation::NodeId,
    ) -> Vec<myko_federation::ResourceClaim> {
        Vec::new()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        Vec::new()
    }

    /// Build a local map plan or retained publication for this view.
    ///
    /// Local implementations return [`super::LocalView`] so impls can chain
    /// `inner_join`, `filter_map_entries`, `select_cell`, etc. before the
    /// registration boundary materializes the map. Retained implementations
    /// return [`super::RetainedView`] to preserve publication cursor, liveness,
    /// and sequence metadata.
    ///
    /// Keep recognized join/projection chains unmaterialized through this
    /// boundary so Hyphae can retain its specialized and adaptive join-region
    /// runtimes. Closures in the returned plan must be deterministic,
    /// externally side-effect-free, and nonblocking; Hyphae may invoke them
    /// repeatedly or concurrently, with no stable order, count, or thread.
    #[must_use]
    fn build_cell(ctx: ViewBuildArgs<Self>) -> impl super::ViewBuildOutput<Item = Self::Item>
    where
        Self: Send + Sync + 'static;
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
