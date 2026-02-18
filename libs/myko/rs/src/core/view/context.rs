use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use hypha::{Cell, CellImmutable, CellMap, CellMutable, MapDiff, MapExt};
#[cfg(not(target_arch = "wasm32"))]
use serde::de::DeserializeOwned;

#[cfg(not(target_arch = "wasm32"))]
use crate::{
    query::{FilteredCellMap, QueryFactory, QueryHandler, QueryParams},
    report::{ReportHandler, ReportId},
};
use crate::{request::RequestContext, server::CellServerCtx, store::StoreRegistry};

#[cfg(not(target_arch = "wasm32"))]
fn downcast_diff<T: Clone + 'static>(
    diff: &MapDiff<Arc<str>, Arc<dyn crate::core::item::AnyItem>>,
) -> MapDiff<Arc<str>, T> {
    match diff {
        MapDiff::Initial { entries } => MapDiff::Initial {
            entries: entries
                .iter()
                .map(|(k, v)| {
                    let typed = v
                        .as_any()
                        .downcast_ref::<T>()
                        .expect("query_map/query_diff type mismatch in Initial");
                    (k.clone(), typed.clone())
                })
                .collect(),
        },
        MapDiff::Insert { key, value } => {
            let typed = value
                .as_any()
                .downcast_ref::<T>()
                .expect("query_map/query_diff type mismatch in Insert");
            MapDiff::Insert {
                key: key.clone(),
                value: typed.clone(),
            }
        }
        MapDiff::Remove { key, old_value } => {
            let typed = old_value
                .as_any()
                .downcast_ref::<T>()
                .expect("query_map/query_diff type mismatch in Remove");
            MapDiff::Remove {
                key: key.clone(),
                old_value: typed.clone(),
            }
        }
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => {
            let old_typed = old_value
                .as_any()
                .downcast_ref::<T>()
                .expect("query_map/query_diff type mismatch in Update old_value");
            let new_typed = new_value
                .as_any()
                .downcast_ref::<T>()
                .expect("query_map/query_diff type mismatch in Update new_value");
            MapDiff::Update {
                key: key.clone(),
                old_value: old_typed.clone(),
                new_value: new_typed.clone(),
            }
        }
        MapDiff::Batch { changes } => MapDiff::Batch {
            changes: changes.iter().map(downcast_diff::<T>).collect(),
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn apply_diff<K, V>(output: &CellMap<K, V, CellMutable>, diff: &MapDiff<K, V>)
where
    K: std::hash::Hash + Eq + hypha::traits::CellValue,
    V: hypha::traits::CellValue,
{
    match diff {
        MapDiff::Initial { entries } => {
            let existing_keys: Vec<K> = output.snapshot().into_iter().map(|(k, _)| k).collect();
            output.remove_many(existing_keys);
            for (k, v) in entries {
                output.insert(k.clone(), v.clone());
            }
        }
        MapDiff::Insert { key, value } => {
            output.insert(key.clone(), value.clone());
        }
        MapDiff::Remove { key, .. } => {
            output.remove(key);
        }
        MapDiff::Update { key, new_value, .. } => {
            output.insert(key.clone(), new_value.clone());
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                apply_diff(output, change);
            }
        }
    }
}

#[derive(Clone)]
pub struct ViewContext {
    pub req: Arc<RequestContext>,
    registry: Arc<StoreRegistry>,
    server_ctx: Arc<CellServerCtx>,
}

#[derive(Clone)]
pub struct ViewCellContext {
    pub request_ctx: Arc<RequestContext>,
    pub view_context: Arc<ViewContext>,
    registry: Arc<StoreRegistry>,
    server_ctx: Arc<CellServerCtx>,
}

impl ViewCellContext {
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

    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

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
    pub fn query_map<Q>(&self, query: Q) -> CellMap<Arc<str>, Q::Item, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let typed = CellMap::<Arc<str>, Q::Item>::new();
        let typed_clone = typed.clone();
        let guard = self.query_map_untyped(query).subscribe_diffs(move |diff| {
            let typed_diff = downcast_diff::<Q::Item>(diff);
            apply_diff(&typed_clone, &typed_diff);
        });
        typed.own_guard(guard);
        typed.lock()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_diff<Q>(&self, query: Q) -> Cell<MapDiff<Arc<str>, Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.query_map_untyped(query)
            .diffs()
            .map(|diff| downcast_diff::<Q::Item>(diff))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + ReportId + Clone + 'static,
    {
        self.server_ctx.report(report, self.request_ctx.clone())
    }
}

impl ViewContext {
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

    pub fn registry(&self) -> Arc<StoreRegistry> {
        self.registry.clone()
    }

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
    pub fn query_map<Q>(&self, query: Q) -> CellMap<Arc<str>, Q::Item, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let typed = CellMap::<Arc<str>, Q::Item>::new();
        let typed_clone = typed.clone();
        let guard = self.query_map_untyped(query).subscribe_diffs(move |diff| {
            let typed_diff = downcast_diff::<Q::Item>(diff);
            apply_diff(&typed_clone, &typed_diff);
        });
        typed.own_guard(guard);
        typed.lock()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query_diff<Q>(&self, query: Q) -> Cell<MapDiff<Arc<str>, Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.query_map_untyped(query)
            .diffs()
            .map(|diff| downcast_diff::<Q::Item>(diff))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn report<R>(&self, report: R) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + ReportId + Clone + 'static,
    {
        self.server_ctx.report(report, self.req.clone())
    }
}
