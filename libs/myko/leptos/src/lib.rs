use std::sync::Arc;

use leptos::prelude::*;
use myko::{common::with_id::WithTypedId, hyphae::IdFor};

/// Initialize the myko-leptos bridge.
///
/// Creates a `MykoClient` with WASM WebSocket transport and stores it in Leptos context.
/// Call this once in your root `App` component.
///
/// `address` should be the myko server WebSocket address (e.g. `"localhost:5155"`).
pub fn provide_myko(address: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        use myko::client::MykoClient;
        let client = MykoClient::new();
        client.set_protocol(myko::client::MykoProtocol::JSON);
        client.set_address(Some(address.to_string()));
        provide_context(client);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = address;
}

/// Disconnect the Myko client (clears the address).
pub fn disconnect_myko() {
    #[cfg(target_arch = "wasm32")]
    {
        use myko::client::MykoClient;
        let client = expect_context::<MykoClient>();
        client.set_address(None);
    }
}

/// Returns a reactive signal tracking the Myko connection status.
pub fn use_connection_status() -> ReadSignal<bool> {
    let (read, write) = signal(false);

    #[cfg(target_arch = "wasm32")]
    {
        use myko::{
            client::{ConnectionStatus, MykoClient},
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.connection_status();
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(status) = signal {
                write.set(matches!(&**status, ConnectionStatus::Connected(_)));
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = write;

    read
}

/// Returns a reactive signal of query results that updates when the server pushes data.
///
/// `query` is a **reactive closure** — it is re-evaluated (and the underlying
/// watch re-subscribed) whenever any Leptos signal read inside the closure changes.
/// This lets callers pass filtered queries whose parameters come from signals:
///
/// ```ignore
/// let items = live_query(move || GetItemsByQuery(PartialItem {
///     owner_id: user_id.get(),
///     ..Default::default()
/// }));
/// ```
///
/// For static queries just wrap in a unit closure: `live_query(|| GetAllFoo {})`.
///
/// `Q` is the query type (e.g. `GetAllServers`).
pub fn live_query<Q>(query: impl Fn() -> Q + Send + Sync + 'static) -> ReadSignal<Vec<Arc<Q::Item>>>
where
    Q: myko::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko::core::item::Eventable
        + myko::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let (read, _loaded) = live_query_loaded(query);
    read
}

/// Like [`live_query`] but also returns a `loaded` flag.
///
/// `loaded` starts as `false`.  Each time the reactive query closure re-runs
/// (parameter change / re-subscribe) it resets to `false`.  It flips to `true`
/// on the **first emission** from the server — even if the returned vec is empty.
/// This lets callers distinguish "still waiting for a response" from "server
/// replied with zero items".
///
/// ```ignore
/// let (projects, loaded) = live_query_loaded(|| GetAllProjects {});
/// // loaded.get() == false  →  spinner
/// // loaded.get() == true && projects.get().is_empty()  →  "No items"
/// // loaded.get() == true && !projects.get().is_empty() →  render list
/// ```
#[allow(clippy::type_complexity)]
pub fn live_query_loaded<Q>(
    query: impl Fn() -> Q + Send + Sync + 'static,
) -> (ReadSignal<Vec<Arc<Q::Item>>>, ReadSignal<bool>)
where
    Q: myko::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko::core::item::Eventable
        + myko::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let (read, write) = signal(vec![]);
    let (loaded, set_loaded) = signal(false);

    #[cfg(target_arch = "wasm32")]
    {
        use myko::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();

        // `prev_cell` holds the previous Cell so it drops (and unsubscribes) on
        // each re-run of the effect, before the new subscription is created.
        let prev_cell: StoredValue<
            Option<myko::hyphae::Cell<Vec<Arc<Q::Item>>, myko::hyphae::CellImmutable>>,
        > = StoredValue::new(None);

        Effect::new(move || {
            let q = query(); // tracks signals read inside the closure
            // A new subscription is starting — mark as not-yet-loaded.
            set_loaded.set(false);
            let cell = client.watch_query(q);
            let guard = cell.subscribe(move |signal| {
                if let Signal::Value(items) = signal {
                    write.set((**items).to_vec());
                    // First emission (even an empty vec) means the query returned.
                    set_loaded.set(true);
                }
            });
            cell.own(guard);
            // Drop the previous cell (unsubscribes) and hold the new one.
            prev_cell.set_value(Some(cell));
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (query, write, set_loaded);
    }

    (read, loaded)
}

/// Returns a reactive signal of view rows that updates when the server pushes data.
///
/// Like [`live_query`] but for a `#[myko_view]` (a server-side reactive join).
/// Reach for this when the data you want is delivered through a view rather than
/// a direct query — e.g. `ProjectBootstrap`, whose server-side `query_map`s reach
/// store-resident data the client's own `watch_query` does not stream.
///
/// `view` is a **reactive closure** — re-evaluated (and the view re-subscribed)
/// whenever any Leptos signal read inside it changes.
pub fn live_view<V>(view: impl Fn() -> V + Send + Sync + 'static) -> ReadSignal<Vec<V::Item>>
where
    V: myko::core::view::ViewParams + Clone + Send + Sync + 'static,
    V::Item: myko::core::item::Eventable
        + myko::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let (read, _loaded) = live_view_loaded(view);
    read
}

/// Like [`live_view`] but also returns a `loaded` flag (see [`live_query_loaded`]).
pub fn live_view_loaded<V>(
    view: impl Fn() -> V + Send + Sync + 'static,
) -> (ReadSignal<Vec<V::Item>>, ReadSignal<bool>)
where
    V: myko::core::view::ViewParams + Clone + Send + Sync + 'static,
    V::Item: myko::core::item::Eventable
        + myko::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let (read, write) = signal(vec![]);
    let (loaded, set_loaded) = signal(false);

    #[cfg(target_arch = "wasm32")]
    {
        use myko::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();

        // Holds the previous Cell so it drops (and unsubscribes) on each re-run.
        let prev_cell: StoredValue<
            Option<myko::hyphae::Cell<Vec<V::Item>, myko::hyphae::CellImmutable>>,
        > = StoredValue::new(None);

        Effect::new(move || {
            let v = view(); // tracks signals read inside the closure
            set_loaded.set(false);
            let cell = client.watch_view(v);
            let guard = cell.subscribe(move |signal| {
                if let Signal::Value(items) = signal {
                    write.set((**items).to_vec());
                    set_loaded.set(true);
                }
            });
            cell.own(guard);
            prev_cell.set_value(Some(cell));
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (view, write, set_loaded);
    }

    (read, loaded)
}

/// Returns a reactive signal of a report's value that updates live whenever the
/// server pushes a recomputed response — online status, computed values, scene
/// state, etc. Starts as `None` and resolves to `Some(value)` on the first
/// response; the same persistent subscription then delivers every later change.
///
/// `report` is a reactive closure — it re-subscribes whenever a Leptos signal it
/// reads changes (e.g. a selected entity id):
///
/// ```ignore
/// let online = live_report::<_, bool>(move || TargetOnline { target_id: id.get() });
/// // online.get() == None  → loading; Some(true/false) → live status
/// ```
pub fn live_report<R, O>(report: impl Fn() -> R + Send + Sync + 'static) -> ReadSignal<Option<O>>
where
    R: myko::report::ReportParams + myko::report::ReportIdStatic + Clone + Send + Sync + 'static,
    O: serde::de::DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
{
    let (read, write) = signal::<Option<O>>(None);

    #[cfg(target_arch = "wasm32")]
    {
        use myko::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();

        // Holds the previous Cell so it drops (and unsubscribes) on each re-run.
        let prev_cell: StoredValue<
            Option<myko::hyphae::Cell<Option<O>, myko::hyphae::CellImmutable>>,
        > = StoredValue::new(None);

        Effect::new(move || {
            let r = report(); // tracks signals read inside the closure
            let cell = client.watch_report::<R, O>(r);
            let guard = cell.subscribe(move |signal| {
                if let Signal::Value(value) = signal {
                    write.set((**value).clone());
                }
            });
            cell.own(guard);
            prev_cell.set_value(Some(cell));
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (report, write);
    }

    read
}

/// Apply a hyphae MapDiff to per-entity Leptos signals.
///
/// Each entity has its own RwSignal — only subscribers to that specific entity
/// are notified on update. The `ids` signal tracks the full ID list for `<For>`
/// iteration and is updated once per diff (not per-entity).
#[allow(dead_code, clippy::type_complexity)]
fn apply_diff<T: Clone + Send + Sync + 'static>(
    signals: &StoredValue<std::collections::HashMap<Arc<str>, RwSignal<Option<Arc<T>>>>>,
    ids_write: &WriteSignal<Vec<Arc<str>>>,
    diff: &myko::hyphae::MapDiff<Arc<str>, Arc<T>>,
) {
    use myko::hyphae::MapDiff;
    match diff {
        MapDiff::Initial { entries } => {
            signals.update_value(|map| {
                for (key, value) in entries {
                    match map.entry(key.clone()) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            e.get().set(Some(value.clone()));
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(RwSignal::new(Some(value.clone())));
                        }
                    }
                }
            });
            ids_write.update(|ids| {
                *ids = entries.iter().map(|(k, _)| k.clone()).collect();
            });
        }
        MapDiff::Insert { key, value } => {
            signals.update_value(|map| match map.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    e.get().set(Some(value.clone()));
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(RwSignal::new(Some(value.clone())));
                }
            });
            ids_write.update(|ids| {
                if !ids.contains(key) {
                    ids.push(key.clone());
                }
            });
        }
        MapDiff::Update { key, new_value, .. } => {
            signals.update_value(|map| {
                if let Some(sig) = map.get(key) {
                    sig.set(Some(new_value.clone()));
                }
            });
            // No ids change on update — entity still exists
        }
        MapDiff::Remove { key, .. } => {
            // Update ids FIRST so <For> unmounts the component before
            // the signal goes None (avoids flash of empty state)
            ids_write.update(|ids| ids.retain(|i| i != key));
            signals.update_value(|map| {
                // Don't set to None — the component is being unmounted via ids.
                // Just clean up the signal from the map.
                map.remove(key);
            });
        }
        MapDiff::Batch { changes } => {
            // Collect ID mutations and apply once to avoid N notifications
            let mut ids_to_add: Vec<Arc<str>> = Vec::new();
            let mut ids_to_remove: Vec<Arc<str>> = Vec::new();

            for change in changes {
                match change {
                    MapDiff::Insert { key, value } => {
                        signals.update_value(|map| match map.entry(key.clone()) {
                            std::collections::hash_map::Entry::Occupied(e) => {
                                e.get().set(Some(value.clone()));
                            }
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert(RwSignal::new(Some(value.clone())));
                            }
                        });
                        ids_to_add.push(key.clone());
                    }
                    MapDiff::Update { key, new_value, .. } => {
                        signals.update_value(|map| {
                            if let Some(sig) = map.get(key) {
                                sig.set(Some(new_value.clone()));
                            }
                        });
                    }
                    MapDiff::Remove { key, .. } => {
                        signals.update_value(|map| {
                            map.remove(key);
                        });
                        ids_to_remove.push(key.clone());
                    }
                    // Nested batch or initial within a batch — recurse
                    other => apply_diff(signals, ids_write, other),
                }
            }

            // Single ids update for the whole batch
            if !ids_to_add.is_empty() || !ids_to_remove.is_empty() {
                ids_write.update(|ids| {
                    ids.retain(|i| !ids_to_remove.contains(i));
                    for id in ids_to_add {
                        if !ids.contains(&id) {
                            ids.push(id);
                        }
                    }
                });
            }
        }
    }
}

/// Fine-grained reactive query — per-entity Leptos signals.
///
/// Unlike `live_query` which returns `ReadSignal<Vec<Item>>` (all subscribers
/// re-render on any entity change), this uses `watch_query_map` to maintain
/// per-entity signals. Only components subscribed to a specific entity
/// re-render when that entity changes.
///
/// Returns a `LiveQueryMap` handle for looking up individual entities.
pub fn live_query_map<Q>(query: Q) -> LiveQueryMap<Q::Item>
where
    Q: myko::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko::core::item::Eventable
        + WithTypedId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + 'static,
    <Q::Item as WithTypedId>::Id: IdFor<Q::Item, MapKey = Arc<str>>,
{
    #[cfg(target_arch = "wasm32")]
    {
        use std::collections::HashMap;

        use myko::client::MykoClient;

        let client = expect_context::<MykoClient>();
        let cell_map = client.watch_query_map(query);

        // Per-entity Leptos signals, keyed by entity ID
        let signals: StoredValue<HashMap<Arc<str>, RwSignal<Option<Arc<Q::Item>>>>> =
            StoredValue::new(HashMap::new());

        // Track entity IDs for list rendering
        let (ids_read, ids_write) = signal(Vec::<Arc<str>>::new());

        // Subscribe to CellMap diffs and update per-entity Leptos signals
        let guard = cell_map.subscribe_diffs(move |diff| {
            apply_diff(&signals, &ids_write, diff);
        });

        // Keep both alive independently — no circular ownership.
        // Arena-managed: drops when the reactive owner (component) disposes.
        let _keepalive_map = StoredValue::new(cell_map);
        let _keepalive_guard = StoredValue::new(guard);

        LiveQueryMap {
            signals,
            ids: ids_read,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = query;
        LiveQueryMap {
            signals: StoredValue::new(std::collections::HashMap::new()),
            ids: signal(vec![]).0,
        }
    }
}

/// Handle for a fine-grained reactive query.
///
/// Provides per-entity signal access and a reactive list of entity IDs.
/// The inner CellMap is kept alive via StoredValue to maintain the diff subscription.
#[derive(Clone, Copy)]
pub struct LiveQueryMap<T: Clone + Send + Sync + 'static> {
    #[allow(clippy::type_complexity)]
    signals: StoredValue<std::collections::HashMap<Arc<str>, RwSignal<Option<Arc<T>>>>>,
    /// Reactive list of entity IDs (for `<For>` iteration)
    pub ids: ReadSignal<Vec<Arc<str>>>,
}

impl<T: Clone + Send + Sync + 'static> LiveQueryMap<T> {
    /// Get a reactive signal for a specific entity by ID.
    ///
    /// Returns `ReadSignal<Option<Arc<T>>>` — None if entity doesn't exist yet.
    /// Safe to call before data arrives — the signal will update when the entity
    /// is inserted via a diff.
    pub fn get(&self, id: &str) -> ReadSignal<Option<Arc<T>>> {
        let id: Arc<str> = id.into();
        // Ensure signal exists (subscribe-before-data pattern)
        self.signals.update_value(|map| {
            map.entry(id.clone()).or_insert_with(|| RwSignal::new(None));
        });
        let sig = self.signals.with_value(|map| map.get(&id).copied());
        sig.map(|s| s.read_only()).unwrap_or_else(|| signal(None).0)
    }
}

/// Send a command to the Myko server and return a reactive signal with the result.
///
/// The returned signal starts as `None` and resolves to `Some(Ok(response))` or
/// `Some(Err(message))` when the server responds.
pub fn send_command<C, R>(cmd: C) -> ReadSignal<Option<Result<R, String>>>
where
    C: serde::Serialize + Clone + myko::core::command::CommandId + 'static,
    R: serde::de::DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
{
    let (read, write) = signal(None);

    #[cfg(target_arch = "wasm32")]
    {
        use myko::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.send_command::<C, R>(&cmd);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(result) = signal {
                write.set((**result).clone());
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = (cmd, write);

    read
}

/// An owner-independent command sender that still observes command responses.
///
/// [`send_command`] reads the `MykoClient` from the reactive owner context lazily
/// (via `expect_context`) and allocates a Leptos signal — both of which **panic if
/// called outside a reactive owner**, e.g. from a `set_timeout` / debounced
/// callback or a detached event handler.
///
/// `CommandSink` captures the client up front (while the owner is alive) so the
/// send can happen later, anywhere. Crucially it is **not** fire-and-forget: it
/// keeps each in-flight command's result cell alive until the server responds, so
/// failures surface ([`CommandSink::send`] logs them) and the typed response is
/// available when a call site wants it ([`CommandSink::dispatch`]).
///
/// ```ignore
/// let sink = use_command_sink();                 // in component body (owner alive)
/// let debounced = use_debounce_fn_with_arg(move |t| {
///     sink.send(SetThing { /* … */ });           // runs in a timeout (no owner) — OK
/// }, 350.0);
///
/// // When the call site needs the outcome:
/// sink.dispatch::<_, ThingId>(MakeThing { /* … */ }, |res| match res {
///     Ok(id) => { /* … */ }
///     Err(e) => { /* … */ }
/// });
/// ```
#[derive(Clone)]
pub struct CommandSink {
    #[cfg(target_arch = "wasm32")]
    client: myko::client::MykoClient,
    // In-flight result cells kept alive until their response lands. Each entry's
    // flag flips to `true` once observed; resolved entries are pruned on the next
    // send (outside any subscription callback, so guards drop safely).
    #[cfg(target_arch = "wasm32")]
    #[allow(clippy::type_complexity)]
    inflight: std::sync::Arc<
        std::sync::Mutex<
            Vec<(
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                Box<dyn std::any::Any + Send>,
            )>,
        >,
    >,
}

/// Capture a [`CommandSink`] from the current reactive owner context.
///
/// Must be called where `provide_myko` is in scope (a component body / reactive
/// owner). The returned sink can then be used from deferred callbacks.
pub fn use_command_sink() -> CommandSink {
    #[cfg(target_arch = "wasm32")]
    {
        CommandSink {
            client: expect_context::<myko::client::MykoClient>(),
            inflight: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        CommandSink {}
    }
}

impl CommandSink {
    /// Send a command, surfacing failures (logged) and ignoring success.
    ///
    /// Owner-independent. Use [`dispatch`](CommandSink::dispatch) when the call
    /// site needs the typed response.
    pub fn send<C>(&self, cmd: C)
    where
        C: serde::Serialize + Clone + myko::core::command::CommandId + 'static,
    {
        let cmd_id = cmd.command_id();
        self.dispatch::<C, myko::serde_json::Value>(cmd, move |res| {
            if let Err(e) = res {
                leptos::logging::error!("[myko] command {cmd_id} failed: {e}");
            }
        });
    }

    /// Send a command and invoke `on_result` once with the typed response.
    ///
    /// Owner-independent: the result cell is kept alive internally until the
    /// server responds, so `on_result` fires even when called from a detached
    /// callback. The subscription is released afterward (pruned on the next send).
    pub fn dispatch<C, R>(
        &self,
        cmd: C,
        on_result: impl Fn(Result<R, String>) + Send + Sync + 'static,
    ) where
        C: serde::Serialize + Clone + myko::core::command::CommandId + 'static,
        R: serde::de::DeserializeOwned
            + Clone
            + std::fmt::Debug
            + PartialEq
            + Send
            + Sync
            + 'static,
    {
        #[cfg(target_arch = "wasm32")]
        {
            use myko::hyphae::{Signal, Watchable};

            // Drop already-resolved keepalives first (safe: not inside a callback).
            {
                let mut inflight = self.inflight.lock().unwrap();
                inflight.retain(|(done, _)| !done.load(std::sync::atomic::Ordering::Acquire));
            }

            let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let done_cb = done.clone();

            let cell = self.client.send_command::<C, R>(&cmd);
            let guard = cell.subscribe(move |signal| {
                if let Signal::Value(value) = signal {
                    if let Some(result) = &**value {
                        on_result(result.clone());
                        done_cb.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
            });
            cell.own(guard);

            self.inflight.lock().unwrap().push((done, Box::new(cell)));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (cmd, on_result);
        }
    }
}
