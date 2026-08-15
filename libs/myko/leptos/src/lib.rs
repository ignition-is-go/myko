use std::sync::Arc;

// Re-export the hyphae→Leptos bridge primitives so consumers get them from a
// single place (`myko_leptos`) without depending on `hyphae-leptos` directly.
pub use hyphae_leptos::{
    CellMapStore, CellMapStoreExt, MapGroup, NestedMapStore, NestedMapStoreExt, ToLeptosSignal,
};
use leptos::prelude::*;
use myko::{common::with_id::WithTypedId, hyphae::IdFor};

/// Initialize the myko-leptos bridge.
///
/// Creates a `MykoClient` with the platform-default WebSocket transport and
/// stores it in Leptos context. Call this once in your root `App` component.
///
/// `address` should be the myko server WebSocket address (e.g. `"localhost:5155"`).
pub fn provide_myko(address: &str) {
    use myko::client::MykoClient;
    let client = MykoClient::new();
    client.set_protocol(myko::client::MykoProtocol::CBOR);
    // NOTE(ts): The Leptos bridge consumes typed cells, never the raw-message
    // debug cell. Avoid a deep JSON/CBOR value clone for every incoming frame.
    client.set_last_message_capture(false);
    client.set_address(Some(address.to_string()));
    provide_context(client);
}

/// Disconnect the Myko client (clears the address).
pub fn disconnect_myko() {
    let client = expect_context::<myko::client::MykoClient>();
    client.set_address(None);
}

/// Returns a reactive signal tracking the Myko connection status.
///
/// The status cell is bridged to a Leptos signal via `hyphae-leptos`
/// ([`ToLeptosSignal`]) and projected to a `bool` (connected or not).
#[must_use]
pub fn use_connection_status() -> ReadSignal<bool> {
    use myko::client::{ConnectionStatus, MykoClient};

    let client = expect_context::<MykoClient>();
    let status = client.connection_status().to_leptos_signal();
    let (connected, set_connected) = signal(false);
    Effect::new(move || set_connected.set(matches!(status.get(), ConnectionStatus::Connected(_))));
    connected
}

/// Returns a reactive signal of query results that updates when the server pushes data.
///
/// `query` is a **reactive closure** — it is re-evaluated (and the underlying
/// watch re-subscribed) whenever any Leptos signal read inside the closure changes.
/// This lets callers pass filtered queries whose parameters come from signals:
///
/// ```ignore
/// let items = live_query(move || GetItemsByQuery(ItemQuery {
///     owner_id: Some(user_id.get().into()),
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
/// `loaded` starts as `false`. Each time the reactive query closure re-runs or
/// the client connection changes, it resets to `false`. It flips to `true` on
/// the **first response** from the server — even if the returned vec is empty.
/// This follows the core client's response-sequence readiness rather than
/// inferring state from the locally seeded empty collection, so callers can
/// reliably distinguish "still waiting" from "server replied with zero items",
/// including after reconnecting to a different server.
///
/// ```ignore
/// let (projects, loaded) = live_query_loaded(|| GetAllProjects {});
/// // loaded.get() == false  →  spinner
/// // loaded.get() == true && projects.get().is_empty()  →  "No items"
/// // loaded.get() == true && !projects.get().is_empty() →  render list
/// ```
pub type LiveQuerySignals<T> = (ReadSignal<Vec<Arc<T>>>, ReadSignal<bool>);
type QueryResultWatch<T> = myko::client::QueryWatch<T>;

pub fn live_query_loaded<Q>(
    query: impl Fn() -> Q + Send + Sync + 'static,
) -> LiveQuerySignals<Q::Item>
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
    use myko::{
        client::MykoClient,
        hyphae::{Signal, Watchable},
    };

    let (read, write) = signal(vec![]);
    let (loaded, set_loaded) = signal(false);

    let client = expect_context::<MykoClient>();

    // `prev_watch` holds the previous watch so it drops (and unsubscribes) on
    // each re-run of the effect, before the new subscription is created.
    let prev_watch: StoredValue<Option<QueryResultWatch<Q::Item>>> = StoredValue::new(None);

    Effect::new(move || {
        let q = query(); // tracks signals read inside the closure
        set_loaded.set(false);
        let watch = client.watch_query_state(q);
        let items_guard = watch.items().subscribe(move |signal| {
            if let Signal::Value(items) = signal {
                write.set((**items).clone());
            }
        });
        let ready_guard = watch.ready().subscribe(move |signal| {
            if let Signal::Value(ready) = signal {
                set_loaded.set(**ready);
            }
        });
        watch.items().own(items_guard);
        watch.items().own(ready_guard);

        // Drop the previous watch (unsubscribes) and hold the new one.
        prev_watch.set_value(Some(watch));
    });

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

/// Like [`live_view`] but also returns an authoritative `loaded` flag (see
/// [`live_query_loaded`]). Unlike the previous seed-based bridge, the initial
/// local empty collection does not mark the view loaded.
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
    use myko::{
        client::MykoClient,
        hyphae::{Signal, Watchable},
    };

    let (read, write) = signal(vec![]);
    let (loaded, set_loaded) = signal(false);

    let client = expect_context::<MykoClient>();

    // Holds the previous watch so it drops (and unsubscribes) on each re-run.
    let prev_watch: StoredValue<Option<myko::client::ViewWatch<V::Item>>> = StoredValue::new(None);

    Effect::new(move || {
        let v = view(); // tracks signals read inside the closure
        set_loaded.set(false);
        let watch = client.watch_view_state(v);
        let items_guard = watch.items().subscribe(move |signal| {
            if let Signal::Value(items) = signal {
                write.set((**items).clone());
            }
        });
        let ready_guard = watch.ready().subscribe(move |signal| {
            if let Signal::Value(ready) = signal {
                set_loaded.set(**ready);
            }
        });
        watch.items().own(items_guard);
        watch.items().own(ready_guard);
        prev_watch.set_value(Some(watch));
    });

    (read, loaded)
}

/// Returns a reactive signal of a report's value that updates live.
///
/// The server pushes recomputed responses for online status, computed values,
/// scene state, and similar data. The signal starts as `None`, resolves to
/// `Some(value)` on the first response, and then delivers every later change.
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
    use myko::{
        client::MykoClient,
        hyphae::{Signal, Watchable},
    };

    let (read, write) = signal::<Option<O>>(None);

    let client = expect_context::<MykoClient>();

    // Holds the previous Cell so it drops (and unsubscribes) on each re-run.
    let prev_cell: StoredValue<Option<myko::hyphae::Cell<Option<O>, myko::hyphae::CellImmutable>>> =
        StoredValue::new(None);

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

    read
}

/// Fine-grained reactive query — per-entity Leptos signals.
///
/// Unlike [`live_query`] which returns `ReadSignal<Vec<Item>>` (all subscribers
/// re-render on any entity change), this uses `watch_query_map` to maintain
/// per-entity signals: only components reading a specific entity re-render when
/// that entity changes.
///
/// Returns a [`CellMapStore`] (from `hyphae-leptos`) — the generic bridge from a
/// hyphae `CellMap` to fine-grained Leptos state. Drive a `<For>` from
/// [`keys()`](CellMapStore::keys) and read each entity via
/// [`value(&id)`](MapGroup::value), which returns a `ReadSignal<Option<Arc<Item>>>`
/// that is `None` until the entity arrives (subscribe-before-data):
///
/// ```ignore
/// let store = live_query_store(GetAllServers {});
/// view! {
///     <For each=move || store.keys().get() key=|id| id.clone() let:id>
///         {move || store.value(&id).get().map(|s| view! { <Row server=s/> })}
///     </For>
/// }
/// ```
pub fn live_query_store<Q>(query: Q) -> CellMapStore<Arc<str>, Arc<Q::Item>>
where
    Q: myko::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko::core::item::Eventable
        + WithTypedId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + 'static,
    <Q::Item as WithTypedId>::Id: IdFor<Q::Item, MapKey = Arc<str>>,
{
    use myko::client::MykoClient;

    let client = expect_context::<MykoClient>();
    // hyphae owns the diff-driven per-key cells; `to_leptos_store` bridges them
    // to per-entity Leptos signals tied to the current owner.
    client.watch_query_map(query).to_leptos_store()
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
    use myko::client::MykoClient;

    let client = expect_context::<MykoClient>();
    // The result cell is kept alive by the client's command-response handler
    // until the response lands; `to_leptos_signal` seeds it (`None`) and ties
    // the subscription to the reactive owner (released on unmount). It returns a
    // read-only signal — the response flows one way, server → UI.
    let signal = client.send_command::<C, R>(&cmd).to_leptos_signal();
    drop(cmd);
    signal
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
    client: myko::client::MykoClient,
    // In-flight result cells kept alive until their response lands. Each entry's
    // flag flips to `true` once observed; resolved entries are pruned on the next
    // send (outside any subscription callback, so guards drop safely).
    inflight: InflightCommands,
}

type InflightCommand = (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    Box<dyn std::any::Any + Send>,
);
type InflightCommands = std::sync::Arc<std::sync::Mutex<Vec<InflightCommand>>>;

/// Capture a [`CommandSink`] from the current reactive owner context.
///
/// Must be called where `provide_myko` is in scope (a component body / reactive
/// owner). The returned sink can then be used from deferred callbacks.
#[must_use]
pub fn use_command_sink() -> CommandSink {
    CommandSink {
        client: expect_context::<myko::client::MykoClient>(),
        inflight: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
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
        use myko::hyphae::{Signal, Watchable};

        // Drop already-resolved keepalives first (safe: not inside a callback).
        {
            let mut inflight = self
                .inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inflight.retain(|(done, _)| !done.load(std::sync::atomic::Ordering::Acquire));
        }

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_cb = done.clone();

        let cell = self.client.send_command::<C, R>(&cmd);
        drop(cmd);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(value) = signal
                && let Some(result) = &**value
            {
                on_result(result.clone());
                done_cb.store(true, std::sync::atomic::Ordering::Release);
            }
        });
        cell.own(guard);

        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((done, Box::new(cell)));
    }
}

// ── Server-mirrored local value ────────────────────────────────────────────

/// A locally-writable mirror of a live server value.
///
/// It provides an explicit commit/cancel lifecycle for editing a server-backed
/// value locally and persisting on drop, blur, or save.
///
/// [`value`](Self::value) is a writable signal you bind to inputs or hand to a
/// widget that mutates it. It re-seeds from `server` whenever the server value
/// changes — UNLESS an edit is in progress ([`editing`](Self::editing)) — so a
/// remote update can't clobber an in-flight local change. Persistence is
/// explicit and consumer-defined: [`commit`](Self::commit) runs the closure you
/// supplied; nothing is written to the server automatically.
///
/// Lifecycle:
/// - [`begin`](Self::begin)  — focus / drag-start: pause server re-seed.
/// - [`commit`](Self::commit) — blur / drag-end / save: persist the local value
///   and resume re-seed. Optimistic — the local value stays until the server
///   echoes it back (then re-seeds to the same value: a no-op).
/// - [`cancel`](Self::cancel) — Escape: discard the local edit, snap back to the
///   current server value, resume.
pub struct ServerMirror<T: Send + Sync + 'static> {
    /// Writable local value. Bind to inputs; hand to widgets that mutate it.
    pub value: RwSignal<T>,
    /// True while an edit/drag is in progress; pauses the server re-seed.
    pub editing: RwSignal<bool>,
    server: Signal<T>,
    persist: StoredValue<Arc<dyn Fn(T) + Send + Sync>>,
}

// Hand-typed Clone/Copy: all fields are `Copy` arena handles regardless of `T`,
// so the mirror is `Copy` even when `T` is not (a derive would wrongly add
// `T: Copy`).
impl<T: Send + Sync + 'static> Clone for ServerMirror<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Send + Sync + 'static> Copy for ServerMirror<T> {}

/// Create a [`ServerMirror`] over a reactive `server` value with a custom
/// `persist` closure (define your own command — e.g. `SetBindingNodeValue`).
///
/// Must be called inside a reactive owner scope (like the other hooks here).
pub fn use_server_mirror<T>(
    server: impl Into<Signal<T>>,
    persist: impl Fn(T) + Send + Sync + 'static,
) -> ServerMirror<T>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let server = server.into();
    let value = RwSignal::new(server.get_untracked());
    let editing = RwSignal::new(false);

    // Re-seed the local cache from the server. Depends ONLY on `server`
    // (untracked reads of `value`/`editing`): a remote change updates the cache,
    // but neither a paused edit nor the widget's own per-frame writes to `value`
    // (e.g. a drag) re-trigger it. The equality guard makes a server echo of a
    // just-committed value a no-op.
    Effect::new(move |_| {
        let next = server.get();
        if !editing.get_untracked() && value.get_untracked() != next {
            value.set(next);
        }
    });

    ServerMirror {
        value,
        editing,
        server,
        persist: StoredValue::new(Arc::new(persist)),
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> ServerMirror<T> {
    /// Pause server re-seed for an in-progress edit (focus / drag-start).
    pub fn begin(&self) {
        self.editing.set(true);
    }

    /// Persist the current local value (runs the supplied closure) and resume
    /// server re-seed. Optimistic: the local value is kept until the server
    /// echoes it back.
    pub fn commit(&self) {
        let v = self.value.get_untracked();
        self.persist.with_value(|f| f(v));
        self.editing.set(false);
    }

    /// Discard the local edit, snap `value` back to the current server value,
    /// and resume re-seed.
    pub fn cancel(&self) {
        self.value.set(self.server.get_untracked());
        self.editing.set(false);
    }
}

// ── Optimistic committer ────────────────────────────────────────────────────

/// A server-backed value with optimistic local commits and rollback.
///
/// It shows a discrete write immediately, then rolls back if the server rejects
/// it.
///
/// [`value`](Self::value) shows the optimistic override while a commit is
/// pending, otherwise the live `server` value. [`commit`](Self::commit)
/// optimistically sets the value and dispatches a command:
/// - on **success** it is a no-op — the override is kept until the server echoes
///   the same value, then dropped seamlessly (no flicker);
/// - on **failure** the override is dropped, snapping `value` back to the server.
pub struct Optimistic<T: Send + Sync + 'static> {
    /// Reactive display value: the optimistic override while pending, else `server`.
    pub value: Signal<T>,
    pending: RwSignal<Option<T>>,
    sink: StoredValue<CommandSink>,
}

// Hand-typed Clone/Copy: all fields are `Copy` arena handles regardless of `T`
// (a derive would wrongly require `T: Copy`).
impl<T: Send + Sync + 'static> Clone for Optimistic<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Send + Sync + 'static> Copy for Optimistic<T> {}

/// Create an [`Optimistic`] over a reactive `server` value.
///
/// Must be called inside a reactive owner scope (like the other hooks here).
pub fn use_optimistic<T>(server: impl Into<Signal<T>>) -> Optimistic<T>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let server = server.into();
    let pending: RwSignal<Option<T>> = RwSignal::new(None);

    // Display: the optimistic override while pending, otherwise the server value.
    let value = Signal::derive(move || pending.get().unwrap_or_else(|| server.get()));

    // Drop the override once the server catches up to it, so a successful commit is
    // a seamless no-op (the value never flinches). Depends only on `server`; reads
    // `pending` untracked so clearing it can't re-trigger this effect.
    Effect::new(move |_| {
        let s = server.get();
        if pending.with_untracked(|p| p.as_ref() == Some(&s)) {
            pending.set(None);
        }
    });

    Optimistic {
        value,
        pending,
        sink: StoredValue::new(use_command_sink()),
    }
}

impl<T: Clone + PartialEq + Send + Sync + 'static> Optimistic<T> {
    /// Optimistically show `next`, then dispatch `cmd`. Revert to the server value
    /// if the command fails; keep it on success (the server echo of `next` clears
    /// the override). The command's response is ignored — only success/failure.
    pub fn commit<C>(&self, next: T, cmd: C)
    where
        C: serde::Serialize + Clone + myko::core::command::CommandId + 'static,
    {
        self.pending.set(Some(next));
        let pending = self.pending;
        self.sink.with_value(|sink| {
            sink.dispatch::<C, myko::serde_json::Value>(cmd, move |res| {
                if res.is_err() {
                    pending.set(None);
                }
            });
        });
    }
}
