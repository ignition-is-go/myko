use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use gpui::{App, AppContext as _, Context, Entity, Subscription};
use hyphae_gpui::{
    CellEntity, CellEntityStatus, CellMapEntity, MapEntry, ToGpuiEntity as _, ToGpuiMapEntity as _,
};
use myko::{
    client::{ConnectionStatus, MykoClient},
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, view::ViewParams},
    hyphae::{Cell, CellImmutable, CellValue},
    query::QueryParams,
    report::{ReportIdStatic, ReportParams},
};
use serde::de::DeserializeOwned;

use crate::{client::myko, crud::CrudController};

/// Load state for a server-backed GPUI entity.
#[derive(Clone, Debug)]
pub enum LoadState<T> {
    /// Awaiting the first response. `stale` retains the last successful value
    /// during reconnects so callers may choose stale-while-revalidate rendering.
    Loading {
        stale: Option<Arc<T>>,
    },
    Ready(Arc<T>),
    Error {
        message: Arc<str>,
        stale: Option<Arc<T>>,
    },
}

impl<T> LoadState<T> {
    #[must_use]
    pub const fn value(&self) -> Option<&Arc<T>> {
        match self {
            Self::Ready(value) => Some(value),
            Self::Loading { stale } | Self::Error { stale, .. } => stale.as_ref(),
        }
    }

    #[must_use]
    pub const fn ready(&self) -> Option<&Arc<T>> {
        if let Self::Ready(value) = self {
            Some(value)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn is_loading(&self) -> bool {
        matches!(self, Self::Loading { .. })
    }
}

type CellMapper<S, T> = dyn Fn(&S) -> Option<T> + Send + Sync;

enum BridgeEvent<T> {
    Loading,
    Value(Arc<T>),
    Error(Arc<str>),
}

/// GPUI-owned, server-backed state.
///
/// The source cells are adapted by `hyphae-gpui`; this entity only observes
/// their GPUI notifications. Consequently all mutation and notification of
/// `Remote` happens on GPUI's foreground executor on native and Wasm.
pub struct Remote<T: Send + Sync + 'static> {
    state: LoadState<T>,
    _observations: Vec<Subscription>,
    _sources: Vec<Box<dyn Any + Send + Sync>>,
}

impl<T: Send + Sync + 'static> Remote<T> {
    #[must_use]
    pub const fn state(&self) -> &LoadState<T> {
        &self.state
    }

    #[must_use]
    pub fn value(&self) -> Option<&T> {
        self.state.value().map(AsRef::as_ref)
    }

    #[must_use]
    pub fn ready(&self) -> Option<&T> {
        self.state.ready().map(AsRef::as_ref)
    }

    #[must_use]
    pub const fn is_loading(&self) -> bool {
        self.state.is_loading()
    }

    fn apply(&mut self, event: BridgeEvent<T>) {
        apply_event(&mut self.state, event);
    }
}

fn apply_event<T>(state: &mut LoadState<T>, event: BridgeEvent<T>) {
    let previous_value = state.value().cloned();
    *state = match event {
        BridgeEvent::Loading => LoadState::Loading {
            stale: previous_value,
        },
        BridgeEvent::Value(value) => LoadState::Ready(value),
        BridgeEvent::Error(message) => LoadState::Error {
            message,
            stale: previous_value,
        },
    };
}

fn cell_event<S, T>(entity: &CellEntity<S>, map: &CellMapper<S, T>) -> Option<BridgeEvent<T>>
where
    S: CellValue,
    T: Send + Sync + 'static,
{
    match entity.status() {
        CellEntityStatus::Active => entity
            .value()
            .and_then(map)
            .map(|value| BridgeEvent::Value(Arc::new(value))),
        CellEntityStatus::Error(error) => Some(BridgeEvent::Error(error.clone().into())),
        CellEntityStatus::Complete => None,
    }
}

fn status_event<T>(entity: &CellEntity<ConnectionStatus>) -> Option<BridgeEvent<T>> {
    match entity.status() {
        CellEntityStatus::Error(error) => Some(BridgeEvent::Error(error.clone().into())),
        CellEntityStatus::Complete => None,
        CellEntityStatus::Active => match entity.value() {
            Some(ConnectionStatus::Connected(_)) | None => None,
            Some(ConnectionStatus::Disconnected) => {
                Some(BridgeEvent::Error("Myko connection disconnected".into()))
            }
            Some(
                ConnectionStatus::Idle
                | ConnectionStatus::Connecting(_)
                | ConnectionStatus::Reconnecting(_),
            ) => Some(BridgeEvent::Loading),
        },
    }
}

fn bridge_cell<S, T>(
    cell: &Cell<S, CellImmutable>,
    client: &MykoClient,
    skip_seed: bool,
    reset_on_disconnect: bool,
    map: impl Fn(&S) -> Option<T> + Send + Sync + 'static,
    cx: &mut App,
) -> Entity<Remote<T>>
where
    S: CellValue,
    T: Send + Sync + 'static,
{
    // `App` is exclusively borrowed for this whole construction. CellEntity
    // drivers can enqueue work meanwhile, but cannot apply/notify on GPUI's
    // foreground executor until after these observations are installed. Thus
    // the initial snapshot and observation registration have no lost-update gap.
    let value_entity = cell.to_gpui_entity(cx);
    let status_entity = reset_on_disconnect.then(|| client.connection_status().to_gpui_entity(cx));
    let map: Arc<CellMapper<S, T>> = Arc::new(map);

    cx.new(move |cx| {
        let value_map = Arc::clone(&map);
        let value_observation =
            cx.observe(&value_entity, move |remote: &mut Remote<T>, entity, cx| {
                let event = entity.read_with(cx, |cell, _| cell_event(cell, value_map.as_ref()));
                if let Some(event) = event {
                    remote.apply(event);
                    cx.notify();
                }
            });

        let status_observation = status_entity.as_ref().map(|status_entity| {
            cx.observe(status_entity, |remote: &mut Remote<T>, entity, cx| {
                let event = entity.read_with(cx, |cell, _| status_event(cell));
                if let Some(event) = event {
                    remote.apply(event);
                    cx.notify();
                }
            })
        });

        // Install observations before taking the construction snapshot. An
        // update racing this setup is therefore either reflected by these
        // reads or delivered by GPUI afterward; it cannot fall into a gap
        // between an initial read and observation registration.
        let mut initial_state = LoadState::Loading { stale: None };
        if !skip_seed && let Some(event) = cell_event(value_entity.read(cx), map.as_ref()) {
            apply_event(&mut initial_state, event);
        }
        if let Some(status_entity) = &status_entity
            && let Some(event) = status_event(status_entity.read(cx))
        {
            apply_event(&mut initial_state, event);
        }

        let mut sources: Vec<Box<dyn Any + Send + Sync>> = vec![Box::new(value_entity)];
        if let Some(status_entity) = status_entity {
            sources.push(Box::new(status_entity));
        }
        Remote {
            state: initial_state,
            _observations: status_observation
                .into_iter()
                .chain([value_observation])
                .collect(),
            _sources: sources,
        }
    })
}

/// Observe a remote entity from a GPUI owner and redraw the owner on updates.
///
/// Keep the returned subscription in the owner for as long as it renders the
/// remote state. A [`Remote`] notifies itself when Myko data changes; GPUI
/// owners reading it must observe it just like any other GPUI entity.
pub fn observe_remote<Owner, T>(remote: &Entity<Remote<T>>, cx: &mut Context<Owner>) -> Subscription
where
    Owner: 'static,
    T: Send + Sync + 'static,
{
    cx.observe(remote, |_owner, _remote, cx| cx.notify())
}

/// Live connection status, including the concrete transport state.
pub fn connection_status(cx: &mut App) -> Entity<Remote<ConnectionStatus>> {
    let client = myko(cx).client().clone();
    bridge_cell(
        &client.connection_status(),
        &client,
        false,
        false,
        |status| Some(status.clone()),
        cx,
    )
}

pub fn live_query<Q>(query: Q, cx: &mut App) -> Entity<Remote<Vec<Arc<Q::Item>>>>
where
    Q: QueryParams + Clone + Send + Sync + 'static,
    Q::Item: Eventable + WithId + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let client = myko(cx).client().clone();
    bridge_cell(
        &client.watch_query(query),
        &client,
        true,
        true,
        |items| Some(items.clone()),
        cx,
    )
}

/// GPUI query collection with stable, independently observable row entities.
///
/// This entity notifies only when membership or readiness changes. Observe a
/// [`MapEntry`] returned by [`entry`](Self::entry) to redraw for one item's
/// value updates without notifying the collection owner.
pub struct QueryStore<T: myko::hyphae::CellValue> {
    state: LoadState<()>,
    ready: bool,
    pending_ready: bool,
    keys: Vec<Arc<str>>,
    entries: HashMap<Arc<str>, Entity<MapEntry<Arc<T>>>>,
    _observations: Vec<Subscription>,
    pending_observations: Vec<Subscription>,
    _map: Entity<CellMapEntity<Arc<str>, Arc<T>>>,
    _ready: Entity<CellEntity<bool>>,
    _status: Entity<CellEntity<ConnectionStatus>>,
}

impl<T: myko::hyphae::CellValue> QueryStore<T> {
    /// Loading, ready, or connection-error state for the collection.
    #[must_use]
    pub const fn state(&self) -> &LoadState<()> {
        &self.state
    }

    /// Whether a valid response has been received from the server.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Current member keys. Query ordering is not implied.
    #[must_use]
    pub fn keys(&self) -> &[Arc<str>] {
        &self.keys
    }

    /// Stable fine-grained entity for a current row.
    #[must_use]
    pub fn entry(&self, key: &Arc<str>) -> Option<Entity<MapEntry<Arc<T>>>> {
        self.entries.get(key).cloned()
    }

    fn sync_membership(&mut self, map: &CellMapEntity<Arc<str>, Arc<T>>) {
        self.keys = map.keys().to_vec();
        self.entries = self
            .keys
            .iter()
            .filter_map(|key| map.entry(key).map(|entry| (key.clone(), entry)))
            .collect();
    }

    fn sync_status(&mut self, status: &CellEntity<ConnectionStatus>) {
        match status.status() {
            CellEntityStatus::Active => self.sync_state(status.value()),
            CellEntityStatus::Error(error) => {
                self.state = LoadState::Error {
                    message: error.clone().into(),
                    stale: self.state.value().cloned(),
                };
            }
            CellEntityStatus::Complete => {}
        }
    }

    fn sync_state(&mut self, status: Option<&ConnectionStatus>) {
        self.state = match status {
            Some(ConnectionStatus::Disconnected) => {
                self.ready = false;
                self.pending_ready = false;
                LoadState::Error {
                    message: "Myko connection disconnected".into(),
                    stale: self.state.value().cloned(),
                }
            }
            Some(
                ConnectionStatus::Idle
                | ConnectionStatus::Connecting(_)
                | ConnectionStatus::Reconnecting(_),
            ) => {
                self.ready = false;
                self.pending_ready = false;
                LoadState::Loading {
                    stale: self.state.value().cloned(),
                }
            }
            Some(ConnectionStatus::Connected(_)) | None if self.ready => {
                LoadState::Ready(Arc::new(()))
            }
            Some(ConnectionStatus::Connected(_)) | None => LoadState::Loading {
                stale: self.state.value().cloned(),
            },
        };
    }
}

/// Observe collection membership/readiness state and redraw the owner.
///
/// Row value changes do not notify this subscription; observe the returned
/// [`MapEntry`] for fine-grained updates to a particular item.
pub fn observe_query_store<Owner, T>(
    store: &Entity<QueryStore<T>>,
    cx: &mut Context<Owner>,
) -> Subscription
where
    Owner: 'static,
    T: myko::hyphae::CellValue,
{
    cx.observe(store, |_owner, _store, cx| cx.notify())
}

/// Keep a CRUD controller's per-row action slots aligned with query membership.
///
/// Existing members retain their stable action entities. Removed members are
/// pruned immediately and after every membership change, so reinsertion starts
/// with fresh command state.
pub fn observe_crud_store<Owner, StoreItem, CrudItem, CreateInput, RenameInput, CR, RR, DR>(
    controller: &Entity<CrudController<CrudItem, CreateInput, RenameInput, CR, RR, DR>>,
    store: &Entity<QueryStore<StoreItem>>,
    cx: &mut Context<Owner>,
) -> Subscription
where
    Owner: 'static,
    StoreItem: CellValue,
    CrudItem: 'static,
    CreateInput: 'static,
    RenameInput: 'static,
    CR: CellValue,
    RR: CellValue,
    DR: CellValue,
{
    let keys = store.read(cx).keys().to_vec();
    controller.update(cx, |controller, _cx| controller.retain_row_actions(&keys));

    let controller = controller.clone();
    cx.observe(store, move |_owner, store, cx| {
        let keys = store.read(cx).keys().to_vec();
        controller.update(cx, |controller, _cx| controller.retain_row_actions(&keys));
    })
}

/// Watch a query through one fine-grained map subscription.
///
/// The returned collection entity handles membership and first-response
/// readiness. Each value update is delivered only through its stable
/// [`MapEntry`] entity.
pub fn live_query_store<Q>(query: Q, cx: &mut App) -> Entity<QueryStore<Q::Item>>
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
{
    let watch = myko(cx).client().watch_query_map_state(query);
    build_store(watch.map(), watch.ready(), cx)
}

fn map_matches_source<T>(
    map: &CellMapEntity<Arc<str>, Arc<T>>,
    source: &myko::hyphae::CellMap<Arc<str>, Arc<T>, myko::hyphae::CellImmutable>,
    cx: &App,
) -> bool
where
    T: myko::hyphae::CellValue,
{
    let snapshot = source.snapshot();
    snapshot.len() == map.keys().len()
        && snapshot.iter().all(|(key, expected)| {
            map.entry(key).is_some_and(|entry| {
                entry
                    .read(cx)
                    .value()
                    .is_some_and(|actual| actual == expected)
            })
        })
}

fn observe_pending_entries<T>(
    store: &mut QueryStore<T>,
    map: &Entity<CellMapEntity<Arc<str>, Arc<T>>>,
    source: &myko::hyphae::CellMap<Arc<str>, Arc<T>, myko::hyphae::CellImmutable>,
    status: &Entity<CellEntity<ConnectionStatus>>,
    cx: &mut Context<QueryStore<T>>,
) where
    T: myko::hyphae::CellValue,
{
    store.pending_observations.clear();
    let entries = map.read_with(cx, |map, _| {
        map.keys()
            .iter()
            .filter_map(|key| map.entry(key))
            .collect::<Vec<_>>()
    });
    for entry in entries {
        let map = map.clone();
        let source = source.clone();
        let status = status.clone();
        store.pending_observations.push(cx.observe(
            &entry,
            move |store: &mut QueryStore<T>, _entry, cx| {
                if store.pending_ready && map_matches_source(map.read(cx), &source, cx) {
                    store.sync_membership(map.read(cx));
                    store.pending_ready = false;
                    store.ready = true;
                    store.sync_status(status.read(cx));
                    cx.notify();
                }
            },
        ));
    }
}

fn build_store<T>(
    source_map: &myko::hyphae::CellMap<Arc<str>, Arc<T>, myko::hyphae::CellImmutable>,
    source_ready: &myko::hyphae::Cell<bool, myko::hyphae::CellImmutable>,
    cx: &mut App,
) -> Entity<QueryStore<T>>
where
    T: myko::hyphae::CellValue,
{
    let status = myko(cx).client().connection_status();
    build_store_with_status(source_map, source_ready, &status, cx)
}

fn build_store_with_status<T>(
    source_map: &myko::hyphae::CellMap<Arc<str>, Arc<T>, myko::hyphae::CellImmutable>,
    source_ready: &myko::hyphae::Cell<bool, myko::hyphae::CellImmutable>,
    source_status: &myko::hyphae::Cell<ConnectionStatus, myko::hyphae::CellImmutable>,
    cx: &mut App,
) -> Entity<QueryStore<T>>
where
    T: myko::hyphae::CellValue,
{
    let map = source_map.to_gpui_map_entity(cx);
    let ready = source_ready.to_gpui_entity(cx);
    let status = source_status.to_gpui_entity(cx);

    let source_for_map = source_map.clone();
    let source_for_ready = source_map.clone();
    cx.new(move |cx| {
        let status_for_map = status.clone();
        let map_observation = cx.observe(&map, move |store: &mut QueryStore<T>, map, cx| {
            if store.pending_ready && map_matches_source(map.read(cx), &source_for_map, cx) {
                store.sync_membership(map.read(cx));
                store.pending_ready = false;
                store.ready = true;
                store.sync_status(status_for_map.read(cx));
                cx.notify();
            } else if store.pending_ready {
                observe_pending_entries(store, &map, &source_for_map, &status_for_map, cx);
            } else if store.ready {
                store.sync_membership(map.read(cx));
                cx.notify();
            }
        });

        let status_for_ready = status.clone();
        let map_for_ready = map.clone();
        let ready_observation = cx.observe(&ready, move |store: &mut QueryStore<T>, ready, cx| {
            let next = ready.read(cx).value().copied().unwrap_or(false);
            if !next {
                let changed = store.ready || store.pending_ready;
                store.ready = false;
                store.pending_ready = false;
                store.pending_observations.clear();
                store.sync_status(status_for_ready.read(cx));
                if changed {
                    cx.notify();
                }
                return;
            }

            let map = map_for_ready.read(cx);
            if map_matches_source(map, &source_for_ready, cx) {
                store.sync_membership(map);
                store.pending_ready = false;
                store.ready = true;
                store.sync_status(status_for_ready.read(cx));
                cx.notify();
            } else {
                // The source publishes readiness after replace_all, but the
                // independent GPUI map driver may still be behind. The next
                // map notification completes this hand-off without polling.
                store.ready = false;
                store.pending_ready = true;
                store.sync_status(status_for_ready.read(cx));
                observe_pending_entries(
                    store,
                    &map_for_ready,
                    &source_for_ready,
                    &status_for_ready,
                    cx,
                );
            }
        });
        let status_observation = cx.observe(&status, |store: &mut QueryStore<T>, status, cx| {
            store.sync_status(status.read(cx));
            cx.notify();
        });

        let map_is_current = map_matches_source(map.read(cx), source_map, cx);
        let source_is_ready = ready.read(cx).value().copied().unwrap_or(false);
        let mut store = QueryStore {
            state: LoadState::Loading { stale: None },
            ready: source_is_ready && map_is_current,
            pending_ready: source_is_ready && !map_is_current,
            keys: Vec::new(),
            entries: HashMap::new(),
            _observations: vec![map_observation, ready_observation, status_observation],
            pending_observations: Vec::new(),
            _map: map.clone(),
            _ready: ready.clone(),
            _status: status.clone(),
        };
        if store.ready {
            store.sync_membership(map.read(cx));
        } else if store.pending_ready {
            observe_pending_entries(&mut store, &map, source_map, &status, cx);
        }
        store.sync_status(status.read(cx));
        store
    })
}

/// Watch a server-side view through one fine-grained map subscription.
///
/// Collection notifications are limited to membership/readiness changes; each
/// joined output row retains its own stable [`MapEntry`] entity.
pub fn live_view_store<V>(view: V, cx: &mut App) -> Entity<QueryStore<V::Item>>
where
    V: ViewParams + Clone + Send + Sync + 'static,
    V::Item: Eventable
        + WithTypedId
        + DeserializeOwned
        + Clone
        + Debug
        + PartialEq
        + Send
        + Sync
        + 'static,
{
    let watch = myko(cx).client().watch_view_map_state(view);
    build_store(watch.map(), watch.ready(), cx)
}

pub fn live_view<V>(view: V, cx: &mut App) -> Entity<Remote<Vec<V::Item>>>
where
    V: ViewParams + Clone + Send + Sync + 'static,
    V::Item: Eventable + WithId + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let client = myko(cx).client().clone();
    bridge_cell(
        &client.watch_view(view),
        &client,
        true,
        true,
        |items| Some(items.clone()),
        cx,
    )
}

pub fn live_report<R, O>(report: R, cx: &mut App) -> Entity<Remote<O>>
where
    R: ReportParams + ReportIdStatic + Clone + Send + Sync + 'static,
    O: DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static,
{
    let client = myko(cx).client().clone();
    bridge_cell(
        &client.watch_report::<R, O>(report),
        &client,
        true,
        true,
        Clone::clone,
        cx,
    )
}

pub fn send_command<C, R>(command: &C, cx: &mut App) -> Entity<Remote<Result<R, String>>>
where
    C: serde::Serialize + Clone + myko::core::command::CommandId + Send + Sync + 'static,
    R: DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static,
{
    let client = myko(cx).client().clone();
    let cell = client.send_command::<C, R>(command);
    bridge_cell(&cell, &client, true, false, Clone::clone, cx)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use gpui::{AppContext as _, Subscription, TestAppContext};
    use myko::hyphae::{Cell, CellMap, Mutable as _};

    use super::{
        BridgeEvent, LoadState, Remote, build_store, build_store_with_status, observe_crud_store,
    };
    use crate::{CrudCommands, CrudController};
    use myko::client::ConnectionStatus;

    struct Probe {
        _observation: Subscription,
    }

    fn state(value: u32) -> Remote<u32> {
        Remote {
            state: LoadState::Ready(std::sync::Arc::new(value)),
            _observations: Vec::new(),
            _sources: Vec::new(),
        }
    }

    #[test]
    fn loading_and_errors_retain_the_last_ready_value() {
        let mut remote = state(42);

        remote.apply(BridgeEvent::Loading);
        assert_eq!(remote.value(), Some(&42));
        assert!(remote.is_loading());

        remote.apply(BridgeEvent::Error("offline".into()));
        assert_eq!(remote.value(), Some(&42));
        assert!(matches!(
            remote.state(),
            LoadState::Error { message, .. } if message.as_ref() == "offline"
        ));
    }

    #[test]
    fn a_new_value_replaces_stale_state() {
        let mut remote = state(1);
        remote.apply(BridgeEvent::Loading);
        remote.apply(BridgeEvent::Value(std::sync::Arc::new(2)));

        assert_eq!(remote.ready(), Some(&2));
        assert!(!remote.is_loading());
    }

    #[gpui::test]
    fn crud_store_observation_prunes_initial_and_removed_members(cx: &mut TestAppContext) {
        let source = CellMap::new();
        source.insert(Arc::<str>::from("one"), Arc::new(1_u32));
        source.insert(Arc::<str>::from("two"), Arc::new(2_u32));
        let source_read = source.lock();
        let ready = Cell::new(true).lock();
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store(&source_read, &ready, cx)
        });
        cx.run_until_parked();
        store.update(cx, |store, _cx| {
            store.keys = vec![Arc::from("one"), Arc::from("two")];
        });
        let controller =
            cx.new(|_| CrudController::<u32, (), (), bool, bool, bool>::new(CrudCommands::new()));

        let retained = controller.update(cx, |controller, cx| {
            controller.row_actions_for(Arc::from("one"), cx)
        });
        controller.update(cx, |controller, cx| {
            controller.row_actions_for(Arc::from("two"), cx);
            controller.row_actions_for(Arc::from("stale"), cx);
        });

        let _probe = cx.new({
            let controller = controller.clone();
            let store = store.clone();
            move |cx| Probe {
                _observation: observe_crud_store(&controller, &store, cx),
            }
        });
        assert!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("stale"))
                .is_none()
        );
        assert_eq!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("one"))
                .map(|actions| actions.entity_id()),
            Some(retained.entity_id())
        );

        store.update(cx, |store, cx| {
            store.keys.retain(|key| key.as_ref() != "one");
            cx.notify();
        });
        cx.run_until_parked();

        assert!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("one"))
                .is_none()
        );
        assert!(
            controller
                .read_with(cx, |controller, _| controller.row_actions("two"))
                .is_some()
        );
    }

    #[gpui::test]
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn source_readiness_waits_for_same_value_map_snapshot(cx: &mut TestAppContext) {
        let source = CellMap::new();
        source.insert(Arc::<str>::from("same-key"), Arc::new(7_u32));
        let source_read = source.lock();
        let ready_read = Cell::new(true).lock();
        let status = Cell::new(ConnectionStatus::Connected("test".into())).lock();

        // At construction the source is ready while the GPUI map driver has
        // not applied its initial snapshot yet. Equal keys alone are not
        // sufficient: the row value must also have arrived.
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store_with_status(&source_read, &ready_read, &status, cx)
        });
        assert!(!store.read_with(cx, |store, _| store.is_ready()));
        assert!(store.read_with(cx, |store, _| store.keys().is_empty()));

        cx.run_until_parked();

        assert!(store.read_with(cx, |store, _| store.is_ready()));
        let entry = store.read_with(cx, |store, _| store.entry(&Arc::from("same-key")));
        assert_eq!(
            entry.and_then(|entry| entry.read_with(cx, |entry, _| entry.value().cloned())),
            Some(Arc::new(7)),
        );
    }

    #[gpui::test]
    fn reconnect_snapshot_is_published_with_readiness(cx: &mut TestAppContext) {
        let source = CellMap::new();
        source.insert(Arc::<str>::from("task"), Arc::new(1_u32));
        let source_read = source.clone().lock();
        let ready = Cell::new(true);
        let ready_read = ready.clone().lock();
        let status = Cell::new(ConnectionStatus::Connected("test".into())).lock();
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store_with_status(&source_read, &ready_read, &status, cx)
        });
        cx.run_until_parked();

        let observed_ready_values: Arc<Mutex<Vec<Vec<u32>>>> = Arc::new(Mutex::new(Vec::new()));
        let _probe = cx.new({
            let store = store.clone();
            let observed_ready_values = observed_ready_values.clone();
            move |cx| Probe {
                _observation: cx.observe(&store, move |_probe, store, cx| {
                    let (is_ready, entries) = store.read_with(cx, |store, _| {
                        (
                            store.is_ready(),
                            store
                                .keys()
                                .iter()
                                .filter_map(|key| store.entry(key))
                                .collect::<Vec<_>>(),
                        )
                    });
                    if is_ready {
                        let values = entries
                            .into_iter()
                            .filter_map(|entry| {
                                entry.read_with(cx, |entry, _| entry.value().cloned())
                            })
                            .map(|value| *value)
                            .collect::<Vec<_>>();
                        observed_ready_values
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(values);
                    }
                }),
            }
        });

        ready.set(false);
        cx.run_until_parked();
        assert!(!store.read_with(cx, |store, _| store.is_ready()));
        // Stale membership remains available while revalidating.
        assert_eq!(store.read_with(cx, |store, _| store.keys().len()), 1);

        // Reuse the same key with a different value to ensure generation
        // gating cannot be fooled by membership equality.
        source.replace_all(vec![(Arc::<str>::from("task"), Arc::new(2_u32))]);
        ready.set(true);
        cx.run_until_parked();

        assert!(store.read_with(cx, |store, _| store.is_ready()));
        let ready_values = observed_ready_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert!(!ready_values.is_empty());
        assert!(ready_values.iter().all(|values| values == &[2]));
    }
}
