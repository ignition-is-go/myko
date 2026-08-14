use std::{collections::HashMap, fmt::Debug, sync::Arc};

use gpui::{AnyElement, App, AppContext as _, Context, Entity, Render, Subscription};
use hyphae_gpui::MapEntry;
use myko::{
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, view::ViewParams},
    query::QueryParams,
    report::{ReportIdStatic, ReportParams},
};
use serde::de::DeserializeOwned;

use crate::{LoadState, QueryStore, Remote, live_query, live_query_store, live_report, live_view};

type BoundaryRenderer<T> = dyn Fn(&LoadState<T>) -> AnyElement + 'static;

/// Retained boundary that redraws itself for loading, error, and ready changes.
pub struct RemoteBoundary<T: Send + Sync + 'static> {
    remote: Entity<Remote<T>>,
    render: Box<BoundaryRenderer<T>>,
    _observation: Subscription,
}

impl<T: Send + Sync + 'static> Render for RemoteBoundary<T> {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut Context<Self>,
    ) -> impl gpui::IntoElement {
        (self.render)(self.remote.read(cx).state())
    }
}

/// Wrap a low-level remote in a self-observing, styling-agnostic component.
pub fn remote_boundary<T, F>(
    remote: Entity<Remote<T>>,
    render: F,
    cx: &mut App,
) -> Entity<RemoteBoundary<T>>
where
    T: Send + Sync + 'static,
    F: Fn(&LoadState<T>) -> AnyElement + 'static,
{
    cx.new(move |cx| {
        let observation = cx.observe(&remote, |_boundary, _remote, cx| cx.notify());
        RemoteBoundary {
            remote,
            render: Box::new(render),
            _observation: observation,
        }
    })
}

pub fn query_boundary<Q, F>(
    query: Q,
    render: F,
    cx: &mut App,
) -> Entity<RemoteBoundary<Vec<Arc<Q::Item>>>>
where
    Q: QueryParams + Clone + Send + Sync + 'static,
    Q::Item: Eventable + WithId + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
    F: Fn(&LoadState<Vec<Arc<Q::Item>>>) -> AnyElement + 'static,
{
    remote_boundary(live_query(query, cx), render, cx)
}

pub fn view_boundary<V, F>(view: V, render: F, cx: &mut App) -> Entity<RemoteBoundary<Vec<V::Item>>>
where
    V: ViewParams + Clone + Send + Sync + 'static,
    V::Item: Eventable + WithId + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
    F: Fn(&LoadState<Vec<V::Item>>) -> AnyElement + 'static,
{
    remote_boundary(live_view(view, cx), render, cx)
}

pub fn report_boundary<R, O, F>(report: R, render: F, cx: &mut App) -> Entity<RemoteBoundary<O>>
where
    R: ReportParams + ReportIdStatic + Clone + Send + Sync + 'static,
    O: DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static,
    F: Fn(&LoadState<O>) -> AnyElement + 'static,
{
    remote_boundary(live_report(report, cx), render, cx)
}

type RowFactory<T, R> = dyn Fn(Arc<str>, Entity<MapEntry<Arc<T>>>, &mut App) -> Entity<R> + 'static;
type ListRenderer<R> = dyn Fn(Vec<Entity<R>>) -> AnyElement + 'static;

/// Self-observing fine-grained query list with stable row entities.
pub struct FineQueryList<T, R>
where
    T: myko::hyphae::CellValue,
    R: Render + 'static,
{
    store: Entity<QueryStore<T>>,
    rows: HashMap<Arc<str>, Entity<R>>,
    row_factory: Box<RowFactory<T, R>>,
    loading: Box<dyn Fn() -> AnyElement>,
    error: Box<dyn Fn(&str) -> AnyElement>,
    empty: Box<dyn Fn() -> AnyElement>,
    list: Box<ListRenderer<R>>,
    _observations: Vec<Subscription>,
}

impl<T, R> FineQueryList<T, R>
where
    T: myko::hyphae::CellValue,
    R: Render + 'static,
{
    fn sync_rows(&mut self, cx: &mut Context<Self>) {
        let entries = self
            .store
            .read(cx)
            .keys()
            .iter()
            .filter_map(|key| {
                self.store
                    .read(cx)
                    .entry(key)
                    .map(|entry| (key.clone(), entry))
            })
            .collect::<Vec<_>>();
        self.rows
            .retain(|key, _| entries.iter().any(|(candidate, _)| candidate == key));
        for (key, entry) in entries {
            if !self.rows.contains_key(&key) {
                let row = (self.row_factory)(key.clone(), entry, cx);
                self.rows.insert(key, row);
            }
        }
    }
}

impl<T, R> Render for FineQueryList<T, R>
where
    T: myko::hyphae::CellValue,
    R: Render + 'static,
{
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut Context<Self>,
    ) -> impl gpui::IntoElement {
        let store = self.store.read(cx);
        match store.state() {
            LoadState::Error { message, .. } => return (self.error)(message),
            LoadState::Loading { .. } => return (self.loading)(),
            LoadState::Ready(_) => {}
        }
        if store.keys().is_empty() {
            return (self.empty)();
        }
        let mut keys = store.keys().to_vec();
        keys.sort();
        (self.list)(
            keys.iter()
                .filter_map(|key| self.rows.get(key).cloned())
                .collect(),
        )
    }
}

/// Construct a fine-grained list. The collection component observes only
/// membership/readiness; every returned row entity observes its own map entry.
pub fn fine_query_list<Q, R, RF, L, LE, E, EE, Empty, EmptyE, List, ListE>(
    query: Q,
    row_factory: RF,
    loading: L,
    error: E,
    empty: Empty,
    list: List,
    cx: &mut App,
) -> Entity<FineQueryList<Q::Item, R>>
where
    Q: QueryParams + Clone + Send + Sync + 'static,
    Q::Item: Eventable
        + WithTypedId
        + DeserializeOwned
        + Clone
        + Debug
        + PartialEq
        + Send
        + Sync
        + 'static,
    <Q::Item as WithTypedId>::Id: myko::hyphae::IdFor<Q::Item, MapKey = Arc<str>>,
    R: Render + 'static,
    RF: Fn(Entity<MapEntry<Arc<Q::Item>>>, &mut App) -> Entity<R> + 'static,
    L: Fn() -> LE + 'static,
    LE: gpui::IntoElement,
    E: Fn(&str) -> EE + 'static,
    EE: gpui::IntoElement,
    Empty: Fn() -> EmptyE + 'static,
    EmptyE: gpui::IntoElement,
    List: Fn(Vec<Entity<R>>) -> ListE + 'static,
    ListE: gpui::IntoElement,
{
    let store = live_query_store(query, cx);
    fine_query_list_from_store_with_key(
        store,
        move |_key, entry, cx| row_factory(entry, cx),
        loading,
        error,
        empty,
        list,
        cx,
    )
}

/// Construct a fine-grained list from an existing store and expose each row's
/// stable map key to the row factory. This is useful for keyed action state.
pub fn fine_query_list_from_store_with_key<T, R, RF, L, LE, E, EE, Empty, EmptyE, List, ListE>(
    store: Entity<QueryStore<T>>,
    row_factory: RF,
    loading: L,
    error: E,
    empty: Empty,
    list: List,
    cx: &mut App,
) -> Entity<FineQueryList<T, R>>
where
    T: myko::hyphae::CellValue,
    R: Render + 'static,
    RF: Fn(Arc<str>, Entity<MapEntry<Arc<T>>>, &mut App) -> Entity<R> + 'static,
    L: Fn() -> LE + 'static,
    LE: gpui::IntoElement,
    E: Fn(&str) -> EE + 'static,
    EE: gpui::IntoElement,
    Empty: Fn() -> EmptyE + 'static,
    EmptyE: gpui::IntoElement,
    List: Fn(Vec<Entity<R>>) -> ListE + 'static,
    ListE: gpui::IntoElement,
{
    cx.new(move |cx| {
        let store_observation = cx.observe(&store, |list: &mut FineQueryList<T, R>, _store, cx| {
            list.sync_rows(cx);
            cx.notify();
        });
        let mut component = FineQueryList {
            store,
            rows: HashMap::new(),
            row_factory: Box::new(row_factory),
            loading: Box::new(move || loading().into_any_element()),
            error: Box::new(move |message| error(message).into_any_element()),
            empty: Box::new(move || empty().into_any_element()),
            list: Box::new(move |rows| list(rows).into_any_element()),
            _observations: vec![store_observation],
        };
        component.sync_rows(cx);
        component
    })
}
