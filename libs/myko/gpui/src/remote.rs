use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, Mutex},
};

use gpui::{App, AppContext as _, Context, Entity, Subscription, Task};
use hyphae_gpui::{CellEntity, CellEntityStatus, ToGpuiEntity as _};
use myko::{
    client::{ConnectionStatus, MykoClient},
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, view::ViewParams},
    hyphae::{
        Cell, CellImmutable, CellMap, CellValue, MapDiff, Signal, SubscriptionGuard, Watchable as _,
    },
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

/// Live Myko round-trip time in milliseconds.
///
/// The remote returns to loading when the transport disconnects rather than
/// presenting the last successful ping as current.
pub fn ping_ms(cx: &mut App) -> Entity<Remote<u64>> {
    let client = myko(cx).client().clone();
    let ping = client.ping_ms().clone().lock();
    bridge_cell(&ping, &client, false, true, |ping| *ping, cx)
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

/// Fine-grained GPUI state for one current or previously present query row.
///
/// Existing observers see `None` when a row is removed. Reusing the same key
/// later reuses this entity, so keyed UI state remains stable across snapshots.
pub struct MapEntry<V: CellValue> {
    value: Option<V>,
}

impl<V: CellValue> MapEntry<V> {
    /// Current value, or `None` after removal.
    #[must_use]
    pub const fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }
}

enum QueryStoreEvent<T: CellValue> {
    Map(MapDiff<Arc<str>, Arc<T>>),
    Ready {
        ready: bool,
        snapshot: Vec<(Arc<str>, Arc<T>)>,
    },
    Status {
        status: ConnectionStatus,
        snapshot: Vec<(Arc<str>, Arc<T>)>,
    },
    ReadyError(Arc<str>),
    StatusError(Arc<str>),
}

/// GPUI query collection with stable, independently observable row entities.
///
/// Unlike the general-purpose Hyphae map adapter, this projection owns Myko's
/// response-readiness and connection semantics. A ready response is reconciled
/// from one authoritative map snapshot before the collection becomes ready.
/// This entity notifies only when membership or readiness changes. Observe a
/// [`MapEntry`] returned by [`entry`](Self::entry) to redraw for one item's
/// value updates without notifying the collection owner.
pub struct QueryStore<T: CellValue> {
    state: LoadState<()>,
    ready: bool,
    response_ready: bool,
    connection: Option<ConnectionStatus>,
    keys: Vec<Arc<str>>,
    entries: HashMap<Arc<str>, Entity<MapEntry<Arc<T>>>>,
    _subscriptions: Vec<SubscriptionGuard>,
    _driver: Task<()>,
}

impl<T: CellValue> QueryStore<T> {
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
        self.keys
            .contains(key)
            .then(|| self.entries.get(key).cloned())
            .flatten()
    }

    fn update_entry(&mut self, key: Arc<str>, value: Arc<T>, cx: &mut Context<Self>) -> bool {
        if let Some(entry) = self.entries.get(&key) {
            entry.update(cx, |entry, cx| {
                entry.value = Some(value);
                cx.notify();
            });
        } else {
            let entry = cx.new(|_| MapEntry { value: Some(value) });
            self.entries.insert(key.clone(), entry);
        }

        if self.keys.contains(&key) {
            false
        } else {
            self.keys.push(key);
            true
        }
    }

    fn remove_entry(&mut self, key: &Arc<str>, cx: &mut Context<Self>) -> bool {
        let previous_len = self.keys.len();
        self.keys.retain(|candidate| candidate != key);
        if let Some(entry) = self.entries.get(key) {
            entry.update(cx, |entry, cx| {
                entry.value = None;
                cx.notify();
            });
        }
        self.keys.len() != previous_len
    }

    fn reconcile_snapshot(
        &mut self,
        snapshot: Vec<(Arc<str>, Arc<T>)>,
        cx: &mut Context<Self>,
    ) -> bool {
        let next_keys = snapshot
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let next_members = next_keys.iter().cloned().collect::<HashSet<_>>();
        let removed = self
            .keys
            .iter()
            .filter(|key| !next_members.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in removed {
            if let Some(entry) = self.entries.get(&key) {
                entry.update(cx, |entry, cx| {
                    entry.value = None;
                    cx.notify();
                });
            }
        }

        let membership_changed = self.keys != next_keys;
        for (key, value) in snapshot {
            if let Some(entry) = self.entries.get(&key) {
                entry.update(cx, |entry, cx| {
                    entry.value = Some(value);
                    cx.notify();
                });
            } else {
                let entry = cx.new(|_| MapEntry { value: Some(value) });
                self.entries.insert(key, entry);
            }
        }
        self.keys = next_keys;
        membership_changed
    }

    fn apply_map_diff(&mut self, diff: MapDiff<Arc<str>, Arc<T>>, cx: &mut Context<Self>) -> bool {
        match diff {
            MapDiff::Initial { entries } => self.reconcile_snapshot(entries, cx),
            MapDiff::Insert { key, value } => self.update_entry(key, value, cx),
            MapDiff::Update { key, new_value, .. } => self.update_entry(key, new_value, cx),
            MapDiff::Remove { key, .. } => self.remove_entry(&key, cx),
            MapDiff::Batch { changes } => {
                let mut changed = false;
                for diff in changes {
                    changed |= self.apply_map_diff(diff, cx);
                }
                changed
            }
        }
    }

    fn sync_not_ready_state(&mut self) {
        self.ready = false;
        self.state = match self.connection.as_ref() {
            Some(ConnectionStatus::Disconnected) => LoadState::Error {
                message: "Myko connection disconnected".into(),
                stale: self.state.value().cloned(),
            },
            Some(
                ConnectionStatus::Idle
                | ConnectionStatus::Connecting(_)
                | ConnectionStatus::Reconnecting(_)
                | ConnectionStatus::Connected(_),
            )
            | None => LoadState::Loading {
                stale: self.state.value().cloned(),
            },
        };
    }

    fn publish_snapshot(&mut self, snapshot: Vec<(Arc<str>, Arc<T>)>, cx: &mut Context<Self>) {
        self.reconcile_snapshot(snapshot, cx);
        self.ready = true;
        self.state = LoadState::Ready(Arc::new(()));
        cx.notify();
    }

    fn apply(&mut self, event: QueryStoreEvent<T>, cx: &mut Context<Self>) {
        match event {
            QueryStoreEvent::Map(diff) => {
                if self.ready && self.apply_map_diff(diff, cx) {
                    cx.notify();
                }
            }
            QueryStoreEvent::Ready { ready, snapshot } => {
                self.response_ready = ready;
                if ready && matches!(self.connection, Some(ConnectionStatus::Connected(_))) {
                    self.publish_snapshot(snapshot, cx);
                } else {
                    self.sync_not_ready_state();
                    cx.notify();
                }
            }
            QueryStoreEvent::Status { status, snapshot } => {
                let connected = matches!(status, ConnectionStatus::Connected(_));
                if !connected {
                    // A later Connected status cannot bless rows from the
                    // previous response generation. Myko must publish a new
                    // ready boundary after reconnecting.
                    self.response_ready = false;
                }
                self.connection = Some(status);
                if self.response_ready && connected {
                    self.publish_snapshot(snapshot, cx);
                } else {
                    self.sync_not_ready_state();
                    cx.notify();
                }
            }
            QueryStoreEvent::ReadyError(message) => {
                self.ready = false;
                self.response_ready = false;
                self.state = LoadState::Error {
                    message,
                    stale: self.state.value().cloned(),
                };
                cx.notify();
            }
            QueryStoreEvent::StatusError(message) => {
                self.ready = false;
                self.response_ready = false;
                self.connection = None;
                self.state = LoadState::Error {
                    message,
                    stale: self.state.value().cloned(),
                };
                cx.notify();
            }
        }
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

fn build_store<T>(
    source_map: &CellMap<Arc<str>, Arc<T>, CellImmutable>,
    source_ready: &Cell<bool, CellImmutable>,
    cx: &mut App,
) -> Entity<QueryStore<T>>
where
    T: CellValue,
{
    let status = myko(cx).client().connection_status();
    build_store_with_status(source_map, source_ready, &status, cx)
}

fn build_store_with_status<T>(
    source_map: &CellMap<Arc<str>, Arc<T>, CellImmutable>,
    source_ready: &Cell<bool, CellImmutable>,
    source_status: &Cell<ConnectionStatus, CellImmutable>,
    cx: &mut App,
) -> Entity<QueryStore<T>>
where
    T: CellValue,
{
    let (sender, receiver) = flume::unbounded();
    // Hyphae callbacks may arrive on different producer threads. Serialize
    // callback snapshot capture and sends so their GPUI delivery cannot be
    // reordered. Myko's response handler completes `replace_all` and its
    // synchronous diff fanout before setting ready; this lock does not turn an
    // arbitrary concurrently-mutated CellMap snapshot into a transaction.
    let ingress = Arc::new(Mutex::new(()));

    let map_sender = sender.clone();
    let map_ingress = ingress.clone();
    let map_subscription = source_map.subscribe_diffs(move |diff| {
        let _serial = map_ingress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = map_sender.send(QueryStoreEvent::Map(diff.clone()));
    });

    let ready_sender = sender.clone();
    let ready_ingress = ingress.clone();
    let ready_map = source_map.clone();
    let ready_subscription = source_ready.subscribe(move |signal| {
        let _serial = ready_ingress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match signal {
            Signal::Value(value) => {
                let ready = **value;
                let snapshot = if ready {
                    ready_map.snapshot()
                } else {
                    Vec::new()
                };
                let _ = ready_sender.send(QueryStoreEvent::Ready { ready, snapshot });
            }
            Signal::Error(error) => {
                let _ = ready_sender.send(QueryStoreEvent::ReadyError(error.to_string().into()));
            }
            Signal::Complete => {}
        }
    });

    let status_sender = sender;
    let status_ingress = ingress;
    let status_map = source_map.clone();
    let status_subscription = source_status.subscribe(move |signal| {
        let _serial = status_ingress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match signal {
            Signal::Value(value) => {
                let status = (**value).clone();
                let snapshot = if matches!(status, ConnectionStatus::Connected(_)) {
                    status_map.snapshot()
                } else {
                    Vec::new()
                };
                let _ = status_sender.send(QueryStoreEvent::Status { status, snapshot });
            }
            Signal::Error(error) => {
                let _ = status_sender.send(QueryStoreEvent::StatusError(error.to_string().into()));
            }
            Signal::Complete => {}
        }
    });

    cx.new(move |cx| {
        let driver = cx.spawn(async move |entity, cx| {
            loop {
                // The background-completion hop is required for the same
                // native/Wasm wake behavior as the pure Hyphae GPUI adapters.
                let receive = receiver.clone();
                let event = cx
                    .background_executor()
                    .spawn(async move { receive.recv_async().await })
                    .await;
                let Ok(event) = event else {
                    break;
                };
                if entity
                    .update(cx, |store: &mut QueryStore<T>, cx| store.apply(event, cx))
                    .is_err()
                {
                    break;
                }
            }
        });

        QueryStore {
            state: LoadState::Loading { stale: None },
            ready: false,
            response_ready: false,
            connection: None,
            keys: Vec::new(),
            entries: HashMap::new(),
            _subscriptions: vec![map_subscription, ready_subscription, status_subscription],
            _driver: driver,
        }
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

    #[gpui::test]
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn empty_response_is_ready_after_the_authoritative_snapshot(cx: &mut TestAppContext) {
        let source = CellMap::<Arc<str>, Arc<u32>>::new();
        let source_read = source.lock();
        let ready = Cell::new(true).lock();
        let status = Cell::new(ConnectionStatus::Connected("test".into())).lock();
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store_with_status(&source_read, &ready, &status, cx)
        });

        assert!(!store.read_with(cx, |store, _| store.is_ready()));
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_ready()));
        assert!(store.read_with(cx, |store, _| store.keys().is_empty()));
    }

    #[gpui::test]
    fn row_updates_do_not_notify_the_collection_or_sibling_rows(cx: &mut TestAppContext) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let source = CellMap::new();
        let one = Arc::<str>::from("one");
        let two = Arc::<str>::from("two");
        source.insert(one.clone(), Arc::new(1_u32));
        source.insert(two.clone(), Arc::new(2_u32));
        let source_read = source.clone().lock();
        let ready = Cell::new(true).lock();
        let status = Cell::new(ConnectionStatus::Connected("test".into())).lock();
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store_with_status(&source_read, &ready, &status, cx)
        });
        cx.run_until_parked();

        let row_entries = store.read_with(cx, |store, _| {
            [store.entry(&one), store.entry(&two)]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        });
        let [one_entry, two_entry] = row_entries.as_slice() else {
            assert_eq!(row_entries.len(), 2);
            return;
        };
        let one_entry = one_entry.clone();
        let two_entry = two_entry.clone();
        let one_id = one_entry.entity_id();
        let store_notifications = Arc::new(AtomicUsize::new(0));
        let one_notifications = Arc::new(AtomicUsize::new(0));
        let two_notifications = Arc::new(AtomicUsize::new(0));

        let _store_probe = cx.new({
            let notifications = store_notifications.clone();
            let store = store.clone();
            move |cx| Probe {
                _observation: cx.observe(&store, move |_probe, _store, _cx| {
                    notifications.fetch_add(1, Ordering::SeqCst);
                }),
            }
        });
        let _one_probe = cx.new({
            let notifications = one_notifications.clone();
            let entry = one_entry;
            move |cx| Probe {
                _observation: cx.observe(&entry, move |_probe, _entry, _cx| {
                    notifications.fetch_add(1, Ordering::SeqCst);
                }),
            }
        });
        let _two_probe = cx.new({
            let notifications = two_notifications.clone();
            let entry = two_entry;
            move |cx| Probe {
                _observation: cx.observe(&entry, move |_probe, _entry, _cx| {
                    notifications.fetch_add(1, Ordering::SeqCst);
                }),
            }
        });

        source.insert(one.clone(), Arc::new(11));
        cx.run_until_parked();

        assert_eq!(store_notifications.load(Ordering::SeqCst), 0);
        assert_eq!(one_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(two_notifications.load(Ordering::SeqCst), 0);
        let current = store
            .read_with(cx, |store, _| store.entry(&one))
            .into_iter()
            .collect::<Vec<_>>();
        let [current] = current.as_slice() else {
            assert_eq!(current.len(), 1);
            return;
        };
        assert_eq!(current.entity_id(), one_id);
        assert_eq!(
            current.read_with(cx, |entry, _| entry.value().cloned()),
            Some(Arc::new(11)),
        );
    }

    #[gpui::test]
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn reconnect_requires_a_new_response_after_connected(cx: &mut TestAppContext) {
        let source = CellMap::new();
        let key = Arc::<str>::from("task");
        source.insert(key.clone(), Arc::new(1_u32));
        let source_read = source.clone().lock();
        let ready = Cell::new(true);
        let ready_read = ready.clone().lock();
        let status = Cell::new(ConnectionStatus::Connected("test".into()));
        let status_read = status.clone().lock();
        let store = cx.update(|cx| {
            crate::provide_myko("ws://127.0.0.1:1", cx);
            build_store_with_status(&source_read, &ready_read, &status_read, cx)
        });
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_ready()));

        status.set(ConnectionStatus::Reconnecting("test".into()));
        ready.set(false);
        source.replace_all(vec![(key.clone(), Arc::new(2))]);
        cx.run_until_parked();
        assert!(!store.read_with(cx, |store, _| store.is_ready()));

        status.set(ConnectionStatus::Connected("test".into()));
        cx.run_until_parked();
        assert!(!store.read_with(cx, |store, _| store.is_ready()));
        assert_eq!(
            store
                .read_with(cx, |store, _| store.entry(&key))
                .and_then(|entry| entry.read_with(cx, |entry, _| entry.value().cloned())),
            Some(Arc::new(1)),
        );

        ready.set(true);
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_ready()));
        assert_eq!(
            store
                .read_with(cx, |store, _| store.entry(&key))
                .and_then(|entry| entry.read_with(cx, |entry, _| entry.value().cloned())),
            Some(Arc::new(2)),
        );
    }
}
