use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use hypha::{Cell, CellImmutable, CellMap, MapDiff, MapExt};
#[cfg(target_arch = "wasm32")]
use hypha::{CellImmutable, CellMap, CellMutable};
#[cfg(not(target_arch = "wasm32"))]
use serde::de::DeserializeOwned;

#[cfg(target_arch = "wasm32")]
use crate::query::QueryParams;
#[cfg(not(target_arch = "wasm32"))]
use crate::view::ViewFactory;
use crate::{common::with_id::WithTypedId, request::RequestContext};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    cache::CacheKey,
    item::downcast_any_item_map_diff,
    query::{FilteredCellMap, QueryFactory, QueryHandler, QueryParams},
    report::{ReportHandler, ReportId},
    server::CellServerCtx,
    store::StoreRegistry,
};

#[derive(Clone)]
pub struct ViewContext {
    pub req: Arc<RequestContext>,
    #[cfg(not(target_arch = "wasm32"))]
    registry: Arc<StoreRegistry>,
    #[cfg(not(target_arch = "wasm32"))]
    server_ctx: Arc<CellServerCtx>,
}

#[derive(Clone)]
pub struct ViewCellContext {
    pub request_ctx: Arc<RequestContext>,
    pub view_context: Arc<ViewContext>,
    #[cfg(not(target_arch = "wasm32"))]
    registry: Arc<StoreRegistry>,
    #[cfg(not(target_arch = "wasm32"))]
    server_ctx: Arc<CellServerCtx>,
}

impl ViewCellContext {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        request_ctx: Arc<RequestContext>,
        view_context: Arc<ViewContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Arc<CellServerCtx>,
    ) -> Self {
        Self {
            request_ctx,
            view_context,
            registry,
            server_ctx,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn server_ctx(&self) -> Arc<CellServerCtx> {
        self.server_ctx.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_map_untyped<Q>(&self, query: Q) -> FilteredCellMap
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.query_map(query, self.request_ctx.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_map<Q>(
        &self,
        query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + WithTypedId + 'static,
    {
        crate::item::typed_map_from_any_item_with_typed_id(
            self.query_map_untyped(query),
            "ViewCellContext::query_map",
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn query_map<Q>(
        &self,
        _query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + Clone + Send + Sync + 'static,
        Q::Item: Clone + std::fmt::Debug + Send + Sync + WithTypedId + 'static,
    {
        CellMap::<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellMutable>::new().lock()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_diff<Q>(&self, query: Q) -> Cell<MapDiff<Arc<str>, Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.query_map_untyped(query)
            .diffs()
            .map(|diff| downcast_any_item_map_diff::<Q::Item>(diff, "ViewCellContext::query_diff"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + ReportId + CacheKey + Clone + serde::Serialize + 'static,
    {
        self.server_ctx.report(report, self.request_ctx.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn view_map_untyped<V>(&self, view: V) -> crate::view::FilteredViewCellMap
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx
            .view_map_untyped(view, self.request_ctx.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn view<V>(&self, view: V) -> crate::view::TypedViewCellMap<V::Item>
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.view(view, self.request_ctx.clone())
    }
}

impl ViewContext {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        req: Arc<RequestContext>,
        registry: Arc<StoreRegistry>,
        server_ctx: Arc<CellServerCtx>,
    ) -> Self {
        Self {
            req,
            registry,
            server_ctx,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn server_ctx(&self) -> Arc<CellServerCtx> {
        self.server_ctx.clone()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_map_untyped<Q>(&self, query: Q) -> FilteredCellMap
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.query_map(query, self.req.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_map<Q>(
        &self,
        query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + WithTypedId + 'static,
    {
        crate::item::typed_map_from_any_item_with_typed_id(
            self.query_map_untyped(query),
            "ViewContext::query_map",
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn query_map<Q>(
        &self,
        _query: Q,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + Clone + Send + Sync + 'static,
        Q::Item: Clone + std::fmt::Debug + Send + Sync + WithTypedId + 'static,
    {
        CellMap::<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellMutable>::new().lock()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_diff<Q>(&self, query: Q) -> Cell<MapDiff<Arc<str>, Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.query_map_untyped(query)
            .diffs()
            .map(|diff| downcast_any_item_map_diff::<Q::Item>(diff, "ViewContext::query_diff"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + ReportId + CacheKey + Clone + serde::Serialize + 'static,
    {
        self.server_ctx.report(report, self.req.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn view_map_untyped<V>(&self, view: V) -> crate::view::FilteredViewCellMap
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.view_map_untyped(view, self.req.clone())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn view<V>(&self, view: V) -> crate::view::TypedViewCellMap<V::Item>
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.server_ctx.view(view, self.req.clone())
    }
}
