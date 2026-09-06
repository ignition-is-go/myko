use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

mod cbor_json;
#[cfg(not(target_arch = "wasm32"))]
mod durable_handler;
#[cfg(not(target_arch = "wasm32"))]
pub mod entity_sync;
mod map_response;
mod query_map;
mod view_map;

pub use autosocket::SocketConnectionStatus as ConnectionStatus;
use autosocket::{CallbackGuard, SocketTransport, WsFrame};
use dashmap::DashMap;
#[cfg(not(target_arch = "wasm32"))]
pub use durable_handler::{
    HandlerClientError, HandlerConnection, HandlerConnector, HandlerFrame, NodeHandlerSubscription,
    ReactiveHandlerSubscription, ReactiveViewSubscription,
};
use hyphae::{
    Cell, CellImmutable, CellMap, CellMutable, CellValue, Gettable, MapExt, Materialize, Mutable,
    SubscriptionGuard, Watchable, WeakCellMap,
};
pub use query_map::QueryMapWatch;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tracing::{debug, error, info, trace, warn};
use url::Url;
pub use view_map::ViewMapWatch;

use crate::{
    command::{CommandId, CommandRequest, WrappedCommand},
    common::with_id::WithId,
    core::item::Eventable,
    entities::server::{GetPeerServers, Server},
    query::{QueryParams, QueryRequest},
    report::{ReportIdStatic, ReportParams, ReportRequest},
    view::{ViewParams, ViewRequest},
    wire::{
        MEvent, MykoMessage, PingData, QueryCursorWindow, QueryCursorWindowUpdate, QueryWindow,
        QueryWindowUpdate, WrappedQuery, WrappedReport, WrappedView, wrap_command_request,
        wrap_view,
    },
};

const MAX_DISCONNECTED_SENDS: usize = 1_024;

fn enqueue_disconnected_frame(
    pending: &mut VecDeque<WsFrame>,
    frame: WsFrame,
) -> Result<usize, String> {
    if pending.len() >= MAX_DISCONNECTED_SENDS {
        return Err(format!(
            "disconnected send queue reached its {MAX_DISCONNECTED_SENDS}-frame limit"
        ));
    }
    pending.push_back(frame);
    Ok(pending.len())
}

/// Wire protocol for encoding messages.
/// Defaults to JSON; clients opt into CBOR by calling `set_protocol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::TS)]
#[ts(export)]
pub enum MykoProtocol {
    JSON = 0,
    CBOR = 1,
}

impl From<u8> for MykoProtocol {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::JSON,
            _ => Self::CBOR,
        }
    }
}

impl From<MykoProtocol> for u8 {
    fn from(protocol: MykoProtocol) -> Self {
        match protocol {
            MykoProtocol::JSON => 0,
            MykoProtocol::CBOR => 1,
        }
    }
}

/// A live collection and the authoritative readiness of its current epoch.
///
/// `ready` is false until a valid sequence-zero snapshot arrives, resets on
/// disconnect, and becomes true even when the authoritative result is empty.
/// Existing list-only APIs remain available through [`Self::into_items`].
#[derive(Clone)]
pub struct ListWatch<T: CellValue> {
    items: Cell<Vec<T>, CellImmutable>,
    ready: Cell<bool, CellImmutable>,
}

impl<T: CellValue> ListWatch<T> {
    #[must_use]
    pub const fn items(&self) -> &Cell<Vec<T>, CellImmutable> {
        &self.items
    }

    #[must_use]
    pub const fn ready(&self) -> &Cell<bool, CellImmutable> {
        &self.ready
    }

    #[must_use]
    pub fn into_items(self) -> Cell<Vec<T>, CellImmutable> {
        self.items
    }
}

/// A query list watch with authoritative response readiness.
pub type QueryWatch<T> = ListWatch<Arc<T>>;
/// A view list watch with authoritative response readiness.
pub type ViewWatch<T> = ListWatch<T>;
/// A keyset-paginated query watch. Cursor controls are available on
/// [`WindowedQueryWatch`] without introducing another subscription type.
pub type CursorQueryWatch<T> = WindowedQueryWatch<T>;

/// A coherent snapshot of a live windowed query for direct UI consumption.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct WindowedQueryState<T: CellValue> {
    pub items: Vec<Arc<T>>,
    pub ready: bool,
    pub total_count: Option<usize>,
    pub window: Option<QueryWindow>,
    /// Zero-based page index when the acknowledged window has a non-zero limit.
    pub page_index: Option<usize>,
    /// Total number of pages when both the count and a non-zero limit are known.
    pub page_count: Option<usize>,
    pub has_previous_page: bool,
    pub has_next_page: bool,
}

impl<T: CellValue> WindowedQueryState<T> {
    fn new(
        items: Vec<Arc<T>>,
        ready: bool,
        total_count: Option<usize>,
        window: Option<QueryWindow>,
    ) -> Self {
        let page_index = window
            .as_ref()
            .and_then(|window| window.offset.checked_div(window.limit));
        let page_count = total_count
            .zip(window.as_ref())
            .and_then(|(total, window)| (window.limit > 0).then(|| total.div_ceil(window.limit)));
        let has_previous_page = ready
            && window
                .as_ref()
                .is_some_and(|window| window.limit > 0 && window.offset > 0);
        let has_next_page = ready
            && total_count
                .zip(window.as_ref())
                .is_some_and(|(total, window)| {
                    window.limit > 0 && window.offset.saturating_add(window.limit) < total
                });
        Self {
            items,
            ready,
            total_count,
            window,
            page_index,
            page_count,
            has_previous_page,
            has_next_page,
        }
    }
}

/// A live ordered query page with authoritative pagination metadata.
#[derive(Clone)]
pub struct WindowedQueryWatch<T: CellValue> {
    items: Cell<Vec<Arc<T>>, CellImmutable>,
    ready: Cell<bool, CellImmutable>,
    total_count: Cell<Option<usize>, CellImmutable>,
    window: Cell<Option<QueryWindow>, CellImmutable>,
    state: Cell<WindowedQueryState<T>, CellImmutable>,
    cursor_window: Arc<std::sync::Mutex<Option<QueryCursorWindow>>>,
    tx: Arc<str>,
    client: MykoClient,
}

impl<T: CellValue> WindowedQueryWatch<T> {
    #[must_use]
    pub const fn items(&self) -> &Cell<Vec<Arc<T>>, CellImmutable> {
        &self.items
    }

    #[must_use]
    pub const fn ready(&self) -> &Cell<bool, CellImmutable> {
        &self.ready
    }

    #[must_use]
    pub const fn total_count(&self) -> &Cell<Option<usize>, CellImmutable> {
        &self.total_count
    }

    #[must_use]
    pub const fn window(&self) -> &Cell<Option<QueryWindow>, CellImmutable> {
        &self.window
    }

    /// One coherent reactive value for rendering the page and its controls.
    #[must_use]
    pub const fn state(&self) -> &Cell<WindowedQueryState<T>, CellImmutable> {
        &self.state
    }

    /// Move this live subscription to another window without resubscribing.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn set_window(&self, window: Option<QueryWindow>) -> Result<(), String> {
        let message = MykoMessage::QueryWindow(QueryWindowUpdate {
            tx: self.tx.to_string(),
            window,
        });
        let frame = self.client.encode_message(&message)?;
        *self
            .cursor_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.client.send_or_queue(frame)
    }

    /// Move this live subscription to an exclusive ID-keyset page.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid cursor or when encoding fails.
    pub fn set_cursor_window(&self, window: QueryCursorWindow) -> Result<(), String> {
        window.validate()?;
        let message = MykoMessage::QueryCursorWindow(QueryCursorWindowUpdate {
            tx: self.tx.to_string(),
            window: window.clone(),
        });
        let frame = self.client.encode_message(&message)?;
        *self
            .cursor_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(window);
        self.client.send_or_queue(frame)
    }

    /// Request the first keyset page.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding the control message fails.
    pub fn first_cursor_page(&self, limit: usize) -> Result<(), String> {
        self.set_cursor_window(QueryCursorWindow::first(limit))
    }

    /// Request the keyset page immediately after the last visible item.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding the control message fails.
    pub fn next_cursor_page(&self, limit: usize) -> Result<bool, String>
    where
        T: WithId,
    {
        let Some(cursor) = self.items.get().last().map(|item| item.id()) else {
            return Ok(false);
        };
        self.set_cursor_window(QueryCursorWindow::after(cursor, limit))?;
        Ok(true)
    }

    /// Request the keyset page immediately before the first visible item.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding the control message fails.
    pub fn previous_cursor_page(&self, limit: usize) -> Result<bool, String>
    where
        T: WithId,
    {
        let Some(cursor) = self.items.get().first().map(|item| item.id()) else {
            return Ok(false);
        };
        self.set_cursor_window(QueryCursorWindow::before(cursor, limit))?;
        Ok(true)
    }

    /// Select a page using an absolute offset and limit.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn set_page(&self, offset: usize, limit: usize) -> Result<(), String> {
        self.set_window(Some(QueryWindow { offset, limit }))
    }

    /// Request the next page when the acknowledged window has more rows.
    ///
    /// Returns `Ok(false)` when pagination metadata is not ready or the watch
    /// is already on its final page.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn next_page(&self) -> Result<bool, String> {
        let state = self.state.get();
        let Some(window) = state.window else {
            return Ok(false);
        };
        if !state.has_next_page {
            return Ok(false);
        }
        let next_offset = window.offset.saturating_add(window.limit);
        self.set_page(next_offset, window.limit)?;
        Ok(true)
    }

    /// Request the previous page when the acknowledged window is not first.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn previous_page(&self) -> Result<bool, String> {
        let state = self.state.get();
        let Some(window) = state.window else {
            return Ok(false);
        };
        if !state.has_previous_page {
            return Ok(false);
        }
        self.set_page(window.offset.saturating_sub(window.limit), window.limit)?;
        Ok(true)
    }

    /// Request the first page when the acknowledged window is not first.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn first_page(&self) -> Result<bool, String> {
        let state = self.state.get();
        let Some(window) = state.window else {
            return Ok(false);
        };
        if !state.has_previous_page {
            return Ok(false);
        }
        self.set_page(0, window.limit)?;
        Ok(true)
    }

    /// Request a zero-based page index when it is within the known result set.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn set_page_index(&self, page_index: usize) -> Result<bool, String> {
        let state = self.state.get();
        if !state.ready {
            return Ok(false);
        }
        let Some(window) = state.window.as_ref() else {
            return Ok(false);
        };
        let Some(page_count) = state.page_count else {
            return Ok(false);
        };
        if window.limit == 0 || page_index >= page_count || state.page_index == Some(page_index) {
            return Ok(false);
        }
        let Some(offset) = page_index.checked_mul(window.limit) else {
            return Ok(false);
        };
        self.set_page(offset, window.limit)?;
        Ok(true)
    }

    /// Request the final page when its position is known and not already active.
    ///
    /// # Errors
    ///
    /// Returns an error if the control message cannot be encoded.
    pub fn last_page(&self) -> Result<bool, String> {
        let state = self.state.get();
        let Some(page_count) = state.page_count else {
            return Ok(false);
        };
        let Some(last_page) = page_count.checked_sub(1) else {
            return Ok(false);
        };
        self.set_page_index(last_page)
    }
}

/// Response handler for incoming command responses (one-shot).
type CommandResponseHandler = Box<dyn FnOnce(Result<Value, String>) + Send>;

/// Handler for incoming query responses.
type QueryHandler = Box<dyn Fn(Value) + Send + Sync>;

/// Handler for incoming report responses.
type ReportHandler = Box<dyn Fn(Value) + Send + Sync>;

type QueryState<T> = Arc<Mutex<HashMap<Arc<str>, Arc<T>>>>;
type SharedMapWatchParts<T> = (
    CellMap<Arc<str>, Arc<T>, CellImmutable>,
    Cell<bool, CellImmutable>,
);

/// Handler for incoming command requests (from server).
type CommandRequestHandler = Box<dyn Fn(Value, CommandResponder) + Send + Sync>;

// ─────────────────────────────────────────────────────────────────────────────
// CommandResponder — allows sync command handlers to send responses
// ─────────────────────────────────────────────────────────────────────────────

/// Allows a command handler to send a response back to the server.
pub struct CommandResponder {
    socket: Arc<dyn SocketTransport>,
    protocol: Arc<AtomicU8>,
    tx: String,
    command_id: Arc<str>,
}

impl CommandResponder {
    /// Send a successful response.
    pub fn respond_ok(&self, response: Value) {
        let resp = crate::command::CommandResponse {
            tx: self.tx.clone(),
            response,
        };
        let msg = MykoMessage::CommandResponse(resp);
        if let Some(frame) = encode_protocol(&self.protocol, &msg) {
            let _ = self.socket.send(frame);
        }
    }

    /// Send an error response.
    pub fn respond_err(&self, message: String) {
        let err = crate::command::CommandError::new(
            self.tx.clone(),
            self.command_id.to_string(),
            message,
        );
        let msg = MykoMessage::CommandError(err);
        if let Some(frame) = encode_protocol(&self.protocol, &msg) {
            let _ = self.socket.send(frame);
        }
    }
}

fn encode_protocol(protocol: &AtomicU8, msg: &MykoMessage) -> Option<WsFrame> {
    match MykoProtocol::from(protocol.load(Ordering::SeqCst)) {
        MykoProtocol::JSON => serde_json::to_string(msg).ok().map(WsFrame::Text),
        MykoProtocol::CBOR => {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(msg, &mut bytes).ok()?;
            Some(WsFrame::Binary(bytes))
        }
    }
}

fn next_subscription_tx() -> Arc<str> {
    uuid::Uuid::new_v4().to_string().into()
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscription cancel helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Create a guard that sends a cancel message and removes the handler on drop.
fn query_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("query_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("query_cancel_guard: cancelling tx={}", tx);
        inner.query_handlers.remove(&tx);
        let msg = MykoMessage::QueryCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(()) => debug!("query_cancel_guard: sent QueryCancel tx={}", tx),
                Err(e) => warn!(
                    "query_cancel_guard: failed to send QueryCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

/// Cancel guard for views. View responses share the `query_handlers`
/// dispatch table on the client, but the server requires a `ViewCancel`
/// to release the view subscription (not a `QueryCancel`).
fn view_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("view_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("view_cancel_guard: cancelling tx={}", tx);
        inner.query_handlers.remove(&tx);
        let msg = MykoMessage::ViewCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(()) => debug!("view_cancel_guard: sent ViewCancel tx={}", tx),
                Err(e) => warn!(
                    "view_cancel_guard: failed to send ViewCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

fn report_cancel_guard(tx: Arc<str>, inner: Arc<MykoClientInner>) -> SubscriptionGuard {
    let tx_for_log = tx.clone();
    debug!("report_cancel_guard: created for tx={}", tx_for_log);
    SubscriptionGuard::from_callback(move || {
        info!("report_cancel_guard: cancelling tx={}", tx);
        inner.report_handlers.remove(&tx);
        let msg = MykoMessage::ReportCancel(crate::wire::CancelSubscription { tx: tx.to_string() });
        if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
            match inner.socket.send(frame) {
                Ok(()) => debug!("report_cancel_guard: sent ReportCancel tx={}", tx),
                Err(e) => warn!(
                    "report_cancel_guard: failed to send ReportCancel tx={}: {}",
                    tx, e
                ),
            }
        }
    })
}

fn list_watch_cache_guard(
    cache_key: String,
    tx: Arc<str>,
    inner: Arc<MykoClientInner>,
) -> SubscriptionGuard {
    SubscriptionGuard::from_callback(move || {
        let _gate = inner
            .list_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = inner
            .list_watch_cache
            .get(&cache_key)
            .is_some_and(|entry| entry.tx() == tx.as_ref());
        if remove {
            inner.list_watch_cache.remove(&cache_key);
        }
    })
}

fn map_watch_cache_guard(
    cache_key: String,
    tx: Arc<str>,
    inner: Arc<MykoClientInner>,
) -> SubscriptionGuard {
    SubscriptionGuard::from_callback(move || {
        let _gate = inner
            .map_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = inner
            .map_watch_cache
            .get(&cache_key)
            .is_some_and(|entry| entry.tx() == tx.as_ref());
        if remove {
            inner.map_watch_cache.remove(&cache_key);
        }
    })
}

fn retain_cell_guard<T: CellValue>(cell: Cell<T, CellImmutable>) -> SubscriptionGuard {
    SubscriptionGuard::from_callback(move || {
        let _ = &cell;
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Report cache — deduplicate identical report subscriptions over the wire
// ─────────────────────────────────────────────────────────────────────────────

trait ClientReportCacheEntryDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

struct ClientReportCacheEntry<T> {
    weak: hyphae::cell::WeakCell<T, CellImmutable>,
}

impl<T: Clone + Send + Sync + 'static> ClientReportCacheEntry<T> {
    fn new(cell: &Cell<T, CellImmutable>) -> Self {
        Self {
            weak: cell.downgrade(),
        }
    }

    fn get(&self) -> Option<Cell<T, CellImmutable>> {
        self.weak.upgrade()
    }
}

impl<T: Clone + Send + Sync + 'static> ClientReportCacheEntryDyn for ClientReportCacheEntry<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Query/view list watches cache both cells so readiness and data share one
// server subscription. Entries are weak and are removed by the final watch's
// ownership guard.
trait ClientListWatchCacheEntryDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn tx(&self) -> &str;
}

struct ClientListWatchCacheEntry<T: CellValue> {
    tx: Arc<str>,
    items: hyphae::cell::WeakCell<Vec<T>, CellImmutable>,
    ready: hyphae::cell::WeakCell<bool, CellImmutable>,
}

impl<T: CellValue> ClientListWatchCacheEntry<T> {
    fn new(tx: Arc<str>, watch: &ListWatch<T>) -> Self {
        Self {
            tx,
            items: watch.items.downgrade(),
            ready: watch.ready.downgrade(),
        }
    }

    fn get(&self) -> Option<ListWatch<T>> {
        Some(ListWatch {
            items: self.items.upgrade()?,
            ready: self.ready.upgrade()?,
        })
    }
}

impl<T: CellValue> ClientListWatchCacheEntryDyn for ClientListWatchCacheEntry<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn tx(&self) -> &str {
        &self.tx
    }
}

struct ClientWindowWatchCacheEntry<T: CellValue> {
    tx: Arc<str>,
    items: hyphae::cell::WeakCell<Vec<Arc<T>>, CellImmutable>,
    ready: hyphae::cell::WeakCell<bool, CellImmutable>,
    total_count: hyphae::cell::WeakCell<Option<usize>, CellImmutable>,
    window: hyphae::cell::WeakCell<Option<QueryWindow>, CellImmutable>,
    state: hyphae::cell::WeakCell<WindowedQueryState<T>, CellImmutable>,
    cursor_window: Arc<std::sync::Mutex<Option<QueryCursorWindow>>>,
}

impl<T: CellValue> ClientWindowWatchCacheEntry<T> {
    fn new(watch: &WindowedQueryWatch<T>) -> Self {
        Self {
            tx: watch.tx.clone(),
            items: watch.items.downgrade(),
            ready: watch.ready.downgrade(),
            total_count: watch.total_count.downgrade(),
            window: watch.window.downgrade(),
            state: watch.state.downgrade(),
            cursor_window: watch.cursor_window.clone(),
        }
    }

    fn get(&self, client: MykoClient) -> Option<WindowedQueryWatch<T>> {
        Some(WindowedQueryWatch {
            items: self.items.upgrade()?,
            ready: self.ready.upgrade()?,
            total_count: self.total_count.upgrade()?,
            window: self.window.upgrade()?,
            state: self.state.upgrade()?,
            cursor_window: self.cursor_window.clone(),
            tx: self.tx.clone(),
            client,
        })
    }
}

impl<T: CellValue> ClientListWatchCacheEntryDyn for ClientWindowWatchCacheEntry<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn tx(&self) -> &str {
        &self.tx
    }
}

trait ClientMapWatchCacheEntryDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn tx(&self) -> &str;
}

struct ClientMapWatchCacheEntry<T: CellValue> {
    tx: Arc<str>,
    map: WeakCellMap<Arc<str>, Arc<T>>,
    ready: hyphae::cell::WeakCell<bool, CellImmutable>,
}

impl<T: CellValue> ClientMapWatchCacheEntry<T> {
    fn new(
        tx: Arc<str>,
        map: &CellMap<Arc<str>, Arc<T>, CellImmutable>,
        ready: &Cell<bool, CellImmutable>,
    ) -> Self {
        Self {
            tx,
            map: map.downgrade(),
            ready: ready.downgrade(),
        }
    }

    fn get(&self) -> Option<SharedMapWatchParts<T>> {
        Some((self.map.upgrade()?.lock(), self.ready.upgrade()?))
    }
}

impl<T: CellValue> ClientMapWatchCacheEntryDyn for ClientMapWatchCacheEntry<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn tx(&self) -> &str {
        &self.tx
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MykoClient
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MykoClient {
    inner: Arc<MykoClientInner>,
    #[cfg(not(target_arch = "wasm32"))]
    handler_connector: Option<Arc<dyn HandlerConnector>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MykoClientOptions {
    pub auto_reconnect: bool,
    pub peer_failover: bool,
    pub app_ping: bool,
}

impl Default for MykoClientOptions {
    fn default() -> Self {
        Self {
            auto_reconnect: true,
            peer_failover: false,
            app_ping: true,
        }
    }
}

struct MykoClientInner {
    socket: Arc<dyn SocketTransport>,
    protocol: Arc<AtomicU8>,
    last_message: Cell<Option<Value>, CellMutable>,
    capture_last_message: AtomicBool,
    ping_ms: Cell<Option<u64>, CellMutable>,
    peer_failover_enabled: bool,
    known_servers: Mutex<Vec<String>>,
    current_address: Mutex<Option<String>>,
    peer_failover_status_guard: Mutex<Option<SubscriptionGuard>>,
    peer_discovery_guard: Mutex<Option<SubscriptionGuard>>,

    // Dispatch maps keyed by tx
    query_handlers: DashMap<Arc<str>, QueryHandler>,
    report_handlers: DashMap<Arc<str>, ReportHandler>,
    command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>>,
    command_request_handlers: DashMap<Arc<str>, CommandRequestHandler>,

    // Report subscription cache — keyed by report_id:params_hash
    report_cache: DashMap<String, Box<dyn ClientReportCacheEntryDyn>>,

    // Query/view list subscription cache — keyed by kind:id:item:params_hash.
    list_watch_cache: DashMap<String, Box<dyn ClientListWatchCacheEntryDyn>>,
    list_watch_cache_gate: Mutex<()>,

    // Fine-grained map watches use a separate representation/cache namespace.
    map_watch_cache: DashMap<String, Box<dyn ClientMapWatchCacheEntryDyn>>,
    map_watch_cache_gate: Mutex<()>,

    // Lossless frames queued while disconnected, capped at an explicit
    // admission boundary.
    pending_sends: Mutex<VecDeque<WsFrame>>,

    // One pending report value per subscription. A capacity-one wake channel
    // coalesces bursts without allowing response memory to grow per frame.
    report_dispatch_pending: Mutex<HashMap<Arc<str>, serde_json::Value>>,
    report_dispatch_tx: flume::Sender<()>,

    // Guards that keep subscriptions alive
    _read_guard: CallbackGuard,
    _report_dispatch_guard: CallbackGuard,
    _status_guard: SubscriptionGuard,
}

impl Default for MykoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoClient {
    fn try_register_query_handler(&self, tx: Arc<str>, handler: QueryHandler) -> bool {
        match self.inner.query_handlers.entry(tx) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(handler);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(_) => false,
        }
    }

    fn cached_list_watch<T: CellValue>(&self, cache_key: &str) -> Option<ListWatch<T>> {
        let existing = self.inner.list_watch_cache.get(cache_key)?;
        existing
            .as_any()
            .downcast_ref::<ClientListWatchCacheEntry<T>>()?
            .get()
    }

    fn cache_list_watch<T: CellValue>(
        &self,
        cache_key: String,
        tx: Arc<str>,
        watch: &ListWatch<T>,
    ) {
        self.inner.list_watch_cache.insert(
            cache_key,
            Box::new(ClientListWatchCacheEntry::new(tx, watch)),
        );
    }

    fn cached_window_watch<T: CellValue>(&self, cache_key: &str) -> Option<WindowedQueryWatch<T>> {
        let existing = self.inner.list_watch_cache.get(cache_key)?;
        existing
            .as_any()
            .downcast_ref::<ClientWindowWatchCacheEntry<T>>()?
            .get(self.clone())
    }

    fn cache_window_watch<T: CellValue>(&self, cache_key: String, watch: &WindowedQueryWatch<T>) {
        self.inner
            .list_watch_cache
            .insert(cache_key, Box::new(ClientWindowWatchCacheEntry::new(watch)));
    }

    fn cached_map_watch<T: CellValue>(&self, cache_key: &str) -> Option<SharedMapWatchParts<T>> {
        let existing = self.inner.map_watch_cache.get(cache_key)?;
        existing
            .as_any()
            .downcast_ref::<ClientMapWatchCacheEntry<T>>()?
            .get()
    }

    fn cache_map_watch<T: CellValue>(
        &self,
        cache_key: String,
        tx: Arc<str>,
        map: &CellMap<Arc<str>, Arc<T>, CellImmutable>,
        ready: &Cell<bool, CellImmutable>,
    ) {
        self.inner.map_watch_cache.insert(
            cache_key,
            Box::new(ClientMapWatchCacheEntry::new(tx, map, ready)),
        );
    }

    /// Create a new `MykoClient` with the platform-default transport.
    ///
    /// On native: uses `AutoReconnectSocket` (tokio-tungstenite).
    /// On WASM: uses `WasmSocket` (web-sys WebSocket).
    #[must_use]
    pub fn new() -> Self {
        Self::with_options(MykoClientOptions::default())
    }

    /// Create a client with peer failover enabled.
    #[must_use]
    pub fn with_failover() -> Self {
        Self::with_options(MykoClientOptions {
            auto_reconnect: true,
            peer_failover: true,
            app_ping: true,
        })
    }

    /// Create a new `MykoClient` with configurable transport auto-reconnect behavior.
    #[must_use]
    pub fn new_with_auto_reconnect(auto_reconnect: bool) -> Self {
        Self::with_options(MykoClientOptions {
            auto_reconnect,
            peer_failover: false,
            app_ping: true,
        })
    }

    /// Create a new `MykoClient` with explicit options.
    #[must_use]
    pub fn with_options(options: MykoClientOptions) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let socket: Arc<dyn SocketTransport> = Arc::new(
            autosocket::AutoReconnectSocket::with_auto_reconnect_and_limits(
                options.auto_reconnect,
                crate::WS_MAX_MESSAGE_SIZE_BYTES,
                crate::WS_MAX_FRAME_SIZE_BYTES,
            ),
        );

        #[cfg(target_arch = "wasm32")]
        let socket: Arc<dyn SocketTransport> = Arc::new(
            autosocket::WasmSocket::with_auto_reconnect(options.auto_reconnect),
        );

        Self::with_transport_and_options(&socket, options)
    }

    /// Create a `MykoClient` with a custom transport implementation.
    pub fn with_transport(transport: Arc<dyn SocketTransport>) -> Self {
        let client = Self::with_transport_and_options(&transport, MykoClientOptions::default());
        drop(transport);
        client
    }

    fn build_read_guard(
        weak: &std::sync::Weak<MykoClientInner>,
        transport: &Arc<dyn SocketTransport>,
    ) -> CallbackGuard {
        let weak = weak.clone();
        let rx = transport.read_rx();
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancelled = Arc::new(AtomicBool::new(false));
            let cancelled_for_thread = Arc::clone(&cancelled);
            let handle = std::thread::spawn(move || {
                loop {
                    if cancelled_for_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(frame) => {
                            let Some(inner) = weak.upgrade() else { break };
                            Self::handle_frame(&inner, &frame);
                        }
                        Err(flume::RecvTimeoutError::Timeout) => {}
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });
            CallbackGuard::new(move || {
                cancelled.store(true, Ordering::SeqCst);
                let _ = handle.join();
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let weak_for_callback = weak.clone();
            if let Some(guard) = transport.set_frame_callback(Arc::new(move |frame| {
                let Some(inner) = weak_for_callback.upgrade() else {
                    return;
                };
                Self::handle_frame(&inner, &frame);
            })) {
                return guard;
            }

            // Custom transports may only implement the channel API. Keep
            // draining it on WASM rather than silently dropping every frame.
            let cancelled = Arc::new(AtomicBool::new(false));
            let cancelled_for_task = Arc::clone(&cancelled);
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    while let Ok(frame) = rx.try_recv() {
                        let Some(inner) = weak.upgrade() else {
                            return;
                        };
                        Self::handle_frame(&inner, &frame);
                    }
                    if cancelled_for_task.load(Ordering::SeqCst) || rx.is_disconnected() {
                        break;
                    }
                    gloo_timers::future::TimeoutFuture::new(8).await;
                }
            });
            CallbackGuard::new(move || cancelled.store(true, Ordering::SeqCst))
        }
    }

    fn build_report_dispatch_guard(
        weak: &std::sync::Weak<MykoClientInner>,
        receiver: &flume::Receiver<()>,
    ) -> CallbackGuard {
        let weak = weak.clone();
        let rx = receiver.clone();
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancelled = Arc::new(AtomicBool::new(false));
            let cancelled_for_thread = Arc::clone(&cancelled);
            let handle = std::thread::spawn(move || {
                loop {
                    if cancelled_for_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(()) => {
                            let Some(inner) = weak.upgrade() else { break };
                            Self::dispatch_pending_reports(&inner);
                        }
                        Err(flume::RecvTimeoutError::Timeout) => {}
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });
            CallbackGuard::new(move || {
                cancelled.store(true, Ordering::SeqCst);
                let _ = handle.join();
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            wasm_bindgen_futures::spawn_local(async move {
                loop {
                    while let Ok(()) = rx.try_recv() {
                        let Some(inner) = weak.upgrade() else { return };
                        Self::dispatch_pending_reports(&inner);
                    }
                    if rx.is_disconnected() {
                        break;
                    }
                    gloo_timers::future::TimeoutFuture::new(8).await;
                }
            });
            CallbackGuard::noop()
        }
    }

    fn dispatch_pending_reports(inner: &MykoClientInner) {
        let pending = {
            let mut pending = inner
                .report_dispatch_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *pending)
        };
        for (tx, response) in pending {
            if let Some(handler) = inner.report_handlers.get(&tx) {
                handler(response);
            }
        }
    }

    fn with_transport_and_options(
        transport: &Arc<dyn SocketTransport>,
        options: MykoClientOptions,
    ) -> Self {
        let protocol = Arc::new(AtomicU8::new(MykoProtocol::JSON.into()));
        let last_message = Cell::new(None).with_name("last_message");
        let ping_ms = Cell::new(None).with_name("ping_ms");

        let query_handlers: DashMap<Arc<str>, QueryHandler> = DashMap::new();
        let report_handlers: DashMap<Arc<str>, ReportHandler> = DashMap::new();
        let command_response_handlers: Mutex<HashMap<String, CommandResponseHandler>> =
            Mutex::new(HashMap::new());
        let command_request_handlers: DashMap<Arc<str>, CommandRequestHandler> = DashMap::new();
        let pending_sends = Mutex::new(VecDeque::with_capacity(MAX_DISCONNECTED_SENDS));

        let report_dispatch_pending = Mutex::new(HashMap::new());
        let (report_dispatch_tx, report_dispatch_rx) = flume::bounded::<()>(1);

        // We need to set up the callbacks, but they reference the inner struct.
        // Use a two-step initialization: create with noop guards, then replace.
        let inner = Arc::new_cyclic(|weak| {
            let read_guard = Self::build_read_guard(weak, transport);

            // Dedicated worker for report-response payloads. Drains the
            // channel FIFO and calls each registered handler synchronously
            // — so the order of handler invocations is identical to the
            // order of incoming frames. The WS read thread enqueues and
            // returns immediately.
            let report_dispatch_guard =
                Self::build_report_dispatch_guard(weak, &report_dispatch_rx);

            let weak_for_status = weak.clone();
            let status_guard = transport
                .actual_connection_state()
                .subscribe(move |signal| {
                    let Some(inner) = weak_for_status.upgrade() else {
                        return;
                    };
                    let hyphae::Signal::Value(status) = signal else {
                        return;
                    };
                    let conn_status = (**status).clone();

                    if !matches!(conn_status, ConnectionStatus::Connected(_)) {
                        inner.ping_ms.set(None);
                    }

                    // Flush pending sends on connect
                    if let ConnectionStatus::Connected(_) = conn_status {
                        let mut pending = inner
                            .pending_sends
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        for frame in pending.drain(..) {
                            let _ = inner.socket.send(frame);
                        }
                    }
                });

            MykoClientInner {
                socket: transport.clone(),
                protocol: protocol.clone(),
                last_message,
                capture_last_message: AtomicBool::new(true),
                ping_ms,
                peer_failover_enabled: options.peer_failover,
                known_servers: Mutex::new(Vec::new()),
                current_address: Mutex::new(None),
                peer_failover_status_guard: Mutex::new(None),
                peer_discovery_guard: Mutex::new(None),
                query_handlers,
                report_handlers,
                command_response_handlers,
                command_request_handlers,
                report_cache: DashMap::new(),
                list_watch_cache: DashMap::new(),
                list_watch_cache_gate: Mutex::new(()),
                map_watch_cache: DashMap::new(),
                map_watch_cache_gate: Mutex::new(()),
                pending_sends,
                report_dispatch_pending,
                report_dispatch_tx,
                _read_guard: read_guard,
                _report_dispatch_guard: report_dispatch_guard,
                _status_guard: status_guard,
            }
        });

        let client = Self {
            inner,
            #[cfg(not(target_arch = "wasm32"))]
            handler_connector: None,
        };

        if options.app_ping {
            Self::spawn_ping_loop(Arc::downgrade(&client.inner));
        }

        if client.inner.peer_failover_enabled {
            client.setup_peer_failover();
        }

        client
    }

    fn setup_peer_failover(&self) {
        let this = self.clone();
        let status_guard = self
            .inner
            .socket
            .actual_connection_state()
            .subscribe(move |signal| {
                if let hyphae::Signal::Value(status) = signal
                    && matches!(&**status, ConnectionStatus::Disconnected)
                {
                    this.try_failover();
                }
            });
        *self
            .inner
            .peer_failover_status_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(status_guard);

        let discovery = self.watch_query(GetPeerServers {});
        let this = self.clone();
        let discovery_guard = discovery.subscribe(move |signal| {
            if let hyphae::Signal::Value(servers) = signal {
                this.update_known_servers_from_peers(servers.as_ref());
            }
        });
        *self
            .inner
            .peer_discovery_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(discovery_guard);
    }

    fn update_known_servers_from_peers(&self, peers: &[Arc<Server>]) {
        let current = self
            .inner
            .current_address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let use_wss = current.as_ref().is_some_and(|a| a.starts_with("wss://"));

        let mut next = Vec::new();
        if let Some(current) = current {
            next.push(current);
        }

        for server in peers {
            let addr = if use_wss {
                format!("wss://{}:{}/myko", server.address, server.port)
            } else {
                format!("ws://{}:{}/myko", server.address, server.port)
            };
            if !next.iter().any(|x| x == &addr) {
                next.push(addr);
            }
        }

        *self
            .inner
            .known_servers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    }

    fn try_failover(&self) {
        if !self.inner.peer_failover_enabled {
            return;
        }

        let current = self
            .inner
            .current_address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let servers = self
            .inner
            .known_servers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if servers.is_empty() {
            return;
        }

        let start_idx = current
            .as_ref()
            .and_then(|c| servers.iter().position(|s| s == c))
            .unwrap_or(0);

        for candidate in servers
            .iter()
            .cycle()
            .skip(start_idx.saturating_add(1))
            .take(servers.len())
            .cloned()
        {
            if current.as_ref() == Some(&candidate) {
                continue;
            }
            info!("MykoClient failover attempting {}", candidate);
            self.set_address(Some(candidate));
            return;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn_ping_loop(inner: std::sync::Weak<MykoClientInner>) {
        std::thread::spawn(move || {
            loop {
                let Some(inner) = inner.upgrade() else {
                    break;
                };

                if matches!(
                    inner.socket.actual_connection_state().get(),
                    ConnectionStatus::Connected(_)
                ) {
                    let msg = MykoMessage::Ping(PingData {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now().timestamp_millis(),
                    });
                    if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
                        let _ = inner.socket.send(frame);
                    }
                }

                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
    }

    #[cfg(target_arch = "wasm32")]
    fn spawn_ping_loop(inner: std::sync::Weak<MykoClientInner>) {
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                let Some(inner) = inner.upgrade() else {
                    break;
                };

                if matches!(
                    inner.socket.actual_connection_state().get(),
                    ConnectionStatus::Connected(_)
                ) {
                    let msg = MykoMessage::Ping(PingData {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now().timestamp_millis(),
                    });
                    if let Some(frame) = encode_protocol(&inner.protocol, &msg) {
                        let _ = inner.socket.send(frame);
                    }
                }

                gloo_timers::future::TimeoutFuture::new(1_000).await;
            }
        });
    }

    /// Handle an incoming WebSocket frame by dispatching to registered handlers.
    fn handle_frame(inner: &MykoClientInner, frame: &WsFrame) {
        let Some(mut value) = Self::decode_message(frame) else {
            return;
        };

        // NOTE(ts): Retaining the raw message requires a deep Value clone. High-rate
        // clients that do not inspect `messages()` can disable it and let dispatch
        // move the data payload instead.
        if inner.capture_last_message.load(Ordering::Relaxed) {
            inner.last_message.set(Some(value.clone()));
        }

        // Fast-path: extract the event tag and take data from the raw Value to avoid
        // both a deep clone and deserializing QueryResponse/ViewResponse through
        // MykoMessage (which would round-trip Arc<dyn AnyItem> through serde).
        let event_tag = value
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();

        let mut data = || {
            value
                .get_mut("data")
                .map_or(serde_json::Value::Null, std::mem::take)
        };

        match event_tag.as_str() {
            "ws:m:query-response" | "ws:m:view-response" => {
                let data_val = data();
                let tx_str = data_val.get("tx").and_then(|v| v.as_str()).unwrap_or("");
                let tx: Arc<str> = Arc::from(tx_str);
                if let Some(handler) = inner.query_handlers.get(&tx) {
                    handler(data_val);
                }
            }
            "ws:m:report-response" => {
                if let Ok(response) = serde_json::from_value::<crate::wire::ReportResponse>(data())
                {
                    let tx: Arc<str> = response.tx.clone().into();
                    if !inner.report_handlers.contains_key(&tx) {
                        return;
                    }
                    inner
                        .report_dispatch_pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(tx, response.response);
                    let _ = inner.report_dispatch_tx.try_send(());
                }
            }
            "ws:m:command-response" => {
                if let Ok(response) = serde_json::from_value::<crate::wire::CommandResponse>(data())
                {
                    // Release the lock BEFORE invoking the one-shot handler. The
                    // handler runs app callbacks synchronously (cell.set → hyphae
                    // subscribers), which may re-enter `send_command` and re-lock
                    // this same mutex → recursive-lock panic (WASM abort). `tx` is a
                    // unique per-request id, so remove-then-call loses no atomicity.
                    let handler = inner
                        .command_response_handlers
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&response.tx);
                    if let Some(handler) = handler {
                        handler(Ok(response.response));
                    }
                }
            }
            "ws:m:command-error" => {
                if let Ok(err) = serde_json::from_value::<crate::wire::CommandError>(data()) {
                    // Same as command-response: drop the lock before the callback so
                    // a command issued from it doesn't recursively re-lock.
                    let handler = inner
                        .command_response_handlers
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&err.tx);
                    if let Some(handler) = handler {
                        handler(Err(err.message));
                    }
                }
            }
            "ws:m:command" => {
                if let Ok(wrapped) = serde_json::from_value::<crate::wire::WrappedCommand>(data()) {
                    let command_id: Arc<str> = wrapped.command_id.clone().into();
                    if let Some(handler) = inner.command_request_handlers.get(&command_id) {
                        let tx = wrapped
                            .command
                            .get("tx")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        if tx.is_empty() {
                            return;
                        }
                        let responder = CommandResponder {
                            socket: inner.socket.clone(),
                            protocol: inner.protocol.clone(),
                            tx,
                            command_id: command_id.clone(),
                        };
                        handler(wrapped.command, responder);
                    }
                }
            }
            "ws:m:ping" => {
                if let Ok(ping) = serde_json::from_value::<PingData>(data()) {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let ping_ms = u64::try_from(now_ms.saturating_sub(ping.timestamp)).unwrap_or(0);
                    inner.ping_ms.set(Some(ping_ms));
                }
            }
            _ => {}
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Protocol and encoding
    // ─────────────────────────────────────────────────────────────────────────

    /// Set the wire protocol (JSON or CBOR). Default is JSON.
    pub fn set_protocol(&self, protocol: MykoProtocol) {
        self.inner.protocol.store(protocol.into(), Ordering::SeqCst);
    }

    /// Get the current wire protocol.
    #[must_use]
    pub fn protocol(&self) -> MykoProtocol {
        MykoProtocol::from(self.inner.protocol.load(Ordering::SeqCst))
    }

    /// Encode a message according to the current protocol.
    fn encode_message<T: Serialize>(&self, msg: &T) -> Result<WsFrame, String> {
        match self.protocol() {
            MykoProtocol::JSON => {
                let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
                Ok(WsFrame::Text(json))
            }
            MykoProtocol::CBOR => {
                let mut bytes = Vec::new();
                ciborium::ser::into_writer(msg, &mut bytes).map_err(|e| e.to_string())?;
                Ok(WsFrame::Binary(bytes))
            }
        }
    }

    /// Decode a WebSocket frame according to its type.
    fn decode_message(frame: &WsFrame) -> Option<Value> {
        match frame {
            WsFrame::Text(content) => serde_json::from_str::<Value>(content).ok(),
            WsFrame::Binary(bytes) => match cbor_json::from_slice(bytes) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("CBOR decode failed ({} bytes): {}", bytes.len(), e);
                    None
                }
            },
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Connection
    // ─────────────────────────────────────────────────────────────────────────

    /// Get a reactive cell of the connection status.
    #[must_use]
    pub fn connection_status(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.inner.socket.actual_connection_state()
    }

    /// Get the current connection status synchronously.
    #[must_use]
    pub fn connection_status_sync(&self) -> ConnectionStatus {
        self.inner.socket.actual_connection_state().get()
    }

    /// Get a reactive cell of live ping in milliseconds (None when unavailable).
    #[must_use]
    pub fn ping_ms(&self) -> &Cell<Option<u64>, CellMutable> {
        &self.inner.ping_ms
    }

    /// Get the latest raw incoming message.
    #[must_use]
    pub fn messages(&self) -> Cell<Option<Value>, CellImmutable> {
        self.inner.last_message.clone().lock()
    }

    /// Enable or disable retention of the latest raw incoming message.
    ///
    /// Retention is enabled by default for backwards compatibility. Disabling it
    /// avoids a deep [`serde_json::Value`] clone for every received frame and clears
    /// the previously retained message. Typed query, view, report, and command
    /// dispatch is unaffected.
    pub fn set_last_message_capture(&self, enabled: bool) {
        self.inner
            .capture_last_message
            .store(enabled, Ordering::Relaxed);
        if !enabled {
            self.inner.last_message.set(None);
        }
    }

    /// Whether the latest raw incoming message is retained.
    #[must_use]
    pub fn is_last_message_capture_enabled(&self) -> bool {
        self.inner.capture_last_message.load(Ordering::Relaxed)
    }

    /// Get the current ping synchronously.
    #[must_use]
    pub fn ping_ms_sync(&self) -> Option<u64> {
        self.inner.ping_ms.get()
    }

    pub fn set_address(&self, addr: Option<String>) {
        let Some(addr) = addr else {
            let current = self
                .inner
                .current_address
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if current.is_none() {
                debug!("set_address(None) ignored; already disconnected");
                return;
            }
            debug!("Setting address to None, disconnecting socket");
            self.inner.socket.set_addr(None);
            *self
                .inner
                .current_address
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return;
        };
        let parsed_addr = match normalize_myko_address(&addr) {
            Ok(address) => address,
            Err(error) => {
                warn!("Could not parse url: {error:?}");
                self.inner.socket.set_addr(None);
                return;
            }
        };
        let current = self
            .inner
            .current_address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if current.as_ref() == Some(&parsed_addr) {
            debug!("set_address({parsed_addr}) ignored; address unchanged");
            return;
        }

        info!("MykoClient connecting to {}", parsed_addr);
        self.inner.socket.set_addr(Some(parsed_addr.clone()));
        *self
            .inner
            .current_address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(parsed_addr.clone());
        if self.inner.peer_failover_enabled {
            let mut servers = self
                .inner
                .known_servers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pos) = servers.iter().position(|s| s == &parsed_addr) {
                servers.remove(pos);
            }
            servers.insert(0, parsed_addr);
        }
    }

    /// Disconnect the client and stop any reconnection attempts.
    pub fn disconnect(&self) {
        debug!("Disconnecting MykoClient");
        self.inner.socket.close();
    }

    /// Close the client and stop any reconnection attempts.
    pub fn close(&self) {
        debug!("Closing MykoClient");
        self.inner.socket.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Send a frame, or queue it if disconnected.
    fn send_or_queue(&self, frame: WsFrame) -> Result<(), String> {
        if let ConnectionStatus::Connected(_) = self.inner.socket.actual_connection_state().get() {
            self.inner.socket.send(frame)
        } else {
            let len = {
                let mut pending = self
                    .inner
                    .pending_sends
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                enqueue_disconnected_frame(&mut pending, frame)?
            };
            if len == 1 {
                warn!(
                    "MykoClient queued frame while disconnected; pending_sends={}",
                    len
                );
            }
            Ok(())
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn send_event(&self, event: MEvent) -> Result<(), String> {
        let myko_msg = MykoMessage::Event(event);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn send_event_batch(&self, events: Vec<MEvent>) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }
        let myko_msg = MykoMessage::EventBatch(events);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn send_query(&self, query: WrappedQuery) -> Result<(), String> {
        let myko_msg = MykoMessage::Query(query);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame)
    }

    /// Send a raw wrapped command (for federation forwarding)
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn send_command_raw(&self, command: crate::command::WrappedCommand) -> Result<(), String> {
        let myko_msg = MykoMessage::Command(command);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame)
    }

    /// Send a raw wrapped report (for federation forwarding)
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn send_report_raw(&self, report: crate::report::WrappedReport) -> Result<(), String> {
        let myko_msg = MykoMessage::Report(report);
        let frame = self.encode_message(&myko_msg)?;
        self.send_or_queue(frame)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch Query — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a query and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the current list of matching items.
    /// The Cell updates whenever the server pushes query diffs.
    /// On reconnect, the query is automatically re-subscribed.
    /// Identical query parameters share one decoded state and wire subscription.
    pub fn watch_query<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> Cell<Vec<Arc<Q::Item>>, CellImmutable>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        self.watch_query_state(query).into_items()
    }

    /// Watch a query together with authoritative initial-response readiness.
    ///
    /// Unlike observing the locally seeded empty list, `ready` distinguishes a
    /// pending subscription from a successful empty server response and resets
    /// when a reconnect starts a new response-sequence epoch.
    #[allow(clippy::too_many_lines)]
    pub fn watch_query_state<Q>(&self, query: impl Into<QueryRequest<Q>>) -> QueryWatch<Q::Item>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let supplied: QueryRequest<Q> = query.into();
        let query_id = supplied.query.query_id();
        let query_item_type = Q::query_item_type_static();
        let cache_key = format!(
            "query-list:{query_id}:{query_item_type}:{}:{:016x}",
            std::any::type_name::<Q::Item>(),
            supplied.query.cache_key_hash()
        );
        let _cache_gate = self
            .inner
            .list_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(shared) = self.cached_list_watch(&cache_key) {
            debug!("watch_query_state: cache hit for {cache_key}");
            return shared;
        }
        self.inner.list_watch_cache.remove(&cache_key);

        let query = QueryRequest::with_tx(supplied.query, next_subscription_tx());
        let tx: Arc<str> = query.tx.clone();

        let cell = Cell::new(vec![]).with_name(query_id.as_ref());
        let cell_weak = cell.downgrade();
        let ready =
            Cell::<bool, CellMutable>::new(false).with_name(format!("query_ready:{query_id}"));
        let ready_weak = ready.downgrade();
        let ready_read = ready.clone().lock();

        let Ok(query_value) = serde_json::to_value(&query) else {
            error!("Could not serialize query request for {query_id}");
            return QueryWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        };

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: query_id.clone(),
            query_item_type,
            window: None,
        };

        // State for accumulating query diffs
        let state: QueryState<Q::Item> = Arc::default();
        let sequences = Arc::new(map_response::MapSequence::new());
        let sequences_for_handler = Arc::clone(&sequences);

        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();

        // Register handler for query responses matching this tx
        let handler: QueryHandler = Box::new(move |response_value: Value| {
            let Some(cell_writer) = cell_weak.upgrade() else {
                warn!(
                    "watch_query: weak cell dead for query={} tx={}",
                    query_id_for_handler, tx_for_handler
                );
                return;
            };

            let response =
                match serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value) {
                    Ok(response) => response,
                    Err(error) => {
                        error!(
                            "Rejected query '{query_id_for_handler}' malformed response: {error}"
                        );
                        return;
                    }
                };

            if response.tx != tx_for_handler {
                return;
            }

            let upserts = match map_response::decode_map_upserts::<Q::Item, _>(
                response.upserts,
                WithId::id,
            ) {
                Ok(upserts) => upserts,
                Err(error) => {
                    error!(
                        "Rejected query '{}' response: invalid {} upsert: {}",
                        query_id_for_handler,
                        std::any::type_name::<Q::Item>(),
                        error
                    );
                    return;
                }
            };
            if !sequences_for_handler.accept(response.sequence) {
                error!(
                    "Rejected query '{}' out-of-order sequence {}",
                    query_id_for_handler, response.sequence
                );
                return;
            }

            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            let is_initial_response = response.sequence == 0;
            if is_initial_response {
                trace!("Sequence reset: Clearing {} state", query_id_for_handler);
                state.clear();
            }

            for del in response.deletes {
                state.remove(&del);
            }
            for (id, item) in upserts {
                state.insert(id, item);
            }

            cell_writer.set(state.values().cloned().collect());
            drop(state);
            if is_initial_response && let Some(ready_writer) = ready_weak.upgrade() {
                ready_writer.set(true);
            }
        });
        if !self.try_register_query_handler(tx.clone(), handler) {
            error!("Refusing duplicate query transaction {tx}");
            return QueryWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        }

        // Build the frame to send (and re-send on reconnect)
        let msg = MykoMessage::Query(wrapped);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode query request for {query_id}");
            self.inner.query_handlers.remove(&tx);
            return QueryWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        };

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let ready_for_status = ready.downgrade();
        let sequences_for_status = sequences;
        let status_cell = self.connection_status();
        let send_query_id = query_id;
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                sequences_for_status.reset_epoch();
                if let Some(ready_writer) = ready_for_status.upgrade() {
                    ready_writer.set(false);
                }
                if let ConnectionStatus::Connected(_) = &**status {
                    match socket.send(frame_to_send.clone()) {
                        Ok(()) => debug!("Watching query {send_query_id}"),
                        Err(e) => error!("Could not send query: {e:?}"),
                    }
                } else {
                    debug!("Query {send_query_id} disconnected");
                }
            }
        });

        cell.own(status_guard);
        cell.own(query_cancel_guard(tx.clone(), self.inner.clone()));
        cell.own(retain_cell_guard(ready_read.clone()));
        cell.own(list_watch_cache_guard(
            cache_key.clone(),
            tx.clone(),
            self.inner.clone(),
        ));

        let watch = QueryWatch {
            items: cell.lock(),
            ready: ready_read,
        };
        self.cache_list_watch(cache_key, tx, &watch);
        watch
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch Report — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a report and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the latest report value (None until first response).
    /// On reconnect, the report is automatically re-subscribed.
    pub fn watch_report<R, O>(
        &self,
        report: impl Into<ReportRequest<R>>,
    ) -> Cell<Option<O>, CellImmutable>
    where
        R: ReportParams + ReportIdStatic + Clone,
        O: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let report: ReportRequest<R> = report.into();
        let report_id: Arc<str> = R::report_id_static().into();

        // NOTE(ts): Cache key is report_id + params hash (excludes tx).
        // Identical report params share a single WS subscription.
        let cache_key = format!("{}:{:016x}", report_id, report.report.cache_key_hash());

        // Cache hit: if the cell is still alive (has subscribers), reuse it.
        if let Some(existing) = self.inner.report_cache.get(&cache_key) {
            if let Some(entry) = existing
                .value()
                .as_any()
                .downcast_ref::<ClientReportCacheEntry<Option<O>>>()
                && let Some(shared) = entry.get()
            {
                debug!("watch_report: cache hit for {cache_key}");
                return shared;
            }
            drop(existing);
            self.inner.report_cache.remove(&cache_key);
        }

        let tx: Arc<str> = report.tx.clone();
        let cell = Cell::new(None).with_name(report_id.as_ref());
        let cell_weak = cell.downgrade();

        let Ok(report_value) = serde_json::to_value(&report) else {
            error!("Could not serialize report request for {report_id}");
            return cell.lock();
        };
        let wrapped = WrappedReport {
            report: report_value,
            report_id: report_id.to_string(),
        };

        let msg = MykoMessage::Report(wrapped);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode report request for {report_id}");
            return cell.lock();
        };

        // Register handler for report responses matching this tx
        let report_id_for_handler = report_id.clone();
        let tx_for_handler = tx.clone();
        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(move |response: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    warn!(
                        "watch_report: weak cell dead for report={} tx={}",
                        report_id_for_handler, tx_for_handler
                    );
                    return;
                };
                match serde_json::from_value::<O>(response) {
                    Ok(data) => cell_writer.set(Some(data)),
                    Err(e) => error!("Could not parse report value: {e:?}"),
                }
            }),
        );

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let send_report_id = report_id;
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                match socket.send(frame_to_send.clone()) {
                    Ok(()) => debug!("Watching report {send_report_id}"),
                    Err(e) => error!("Could not send report: {e:?}"),
                }
            }
        });

        cell.own(status_guard);
        cell.own(report_cancel_guard(tx, self.inner.clone()));

        let locked = cell.lock();

        // Store weak ref in cache — dies when all subscribers drop.
        self.inner
            .report_cache
            .insert(cache_key, Box::new(ClientReportCacheEntry::new(&locked)));

        locked
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch View — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Watch a view and receive updates as a reactive Cell.
    ///
    /// Returns a Cell containing the current list of items in the view.
    /// The Cell updates whenever the server pushes view diffs.
    /// On reconnect, the view is automatically re-subscribed.
    /// Identical view parameters share one decoded state and wire subscription.
    pub fn watch_view<V>(
        &self,
        view: impl Into<ViewRequest<V>>,
    ) -> Cell<Vec<V::Item>, CellImmutable>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        self.watch_view_state(view).into_items()
    }

    /// Watch a view together with authoritative initial-response readiness.
    #[allow(clippy::too_many_lines)]
    pub fn watch_view_state<V>(&self, view: impl Into<ViewRequest<V>>) -> ViewWatch<V::Item>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let supplied: ViewRequest<V> = view.into();
        let view_id = supplied.view.view_id();
        let cache_key = format!(
            "view-list:{view_id}:{}:{:016x}",
            std::any::type_name::<V::Item>(),
            supplied.view.cache_key_hash()
        );
        let _cache_gate = self
            .inner
            .list_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(shared) = self.cached_list_watch(&cache_key) {
            debug!("watch_view_state: cache hit for {cache_key}");
            return shared;
        }
        self.inner.list_watch_cache.remove(&cache_key);

        let view = ViewRequest::with_tx(supplied.view, next_subscription_tx());
        let tx: Arc<str> = view.tx.clone();
        let cell = Cell::new(vec![]).with_name(view_id.as_ref());
        let cell_weak = cell.downgrade();
        let ready =
            Cell::<bool, CellMutable>::new(false).with_name(format!("view_ready:{view_id}"));
        let ready_weak = ready.downgrade();
        let ready_read = ready.clone().lock();

        let Ok(wrapped) = wrap_view(tx.clone(), &view.view) else {
            error!("Could not serialize view request for {view_id}");
            return ViewWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        };

        let msg = MykoMessage::View(wrapped);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode view request for {view_id}");
            return ViewWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        };

        let state: Arc<Mutex<HashMap<Arc<str>, V::Item>>> = Arc::default();
        let sequences = Arc::new(map_response::MapSequence::new());
        let sequences_for_handler = Arc::clone(&sequences);
        let tx_for_handler = tx.clone();
        let view_id_for_handler = view_id.clone();

        let handler: QueryHandler = Box::new(move |response_value: Value| {
            let Some(cell_writer) = cell_weak.upgrade() else {
                return;
            };

            let response =
                match serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value) {
                    Ok(response) => response,
                    Err(error) => {
                        error!("Rejected view '{view_id_for_handler}' malformed response: {error}");
                        return;
                    }
                };

            if response.tx != tx_for_handler {
                return;
            }

            let upserts = response
                .upserts
                .into_iter()
                .map(|wrapped| {
                    let item = serde_json::from_value::<V::Item>(wrapped.item)?;
                    Ok((item.id(), item))
                })
                .collect::<Result<Vec<_>, serde_json::Error>>();
            let upserts = match upserts {
                Ok(upserts) => upserts,
                Err(error) => {
                    error!(
                        "Rejected view '{}' response: invalid {} upsert: {}",
                        view_id_for_handler,
                        std::any::type_name::<V::Item>(),
                        error
                    );
                    return;
                }
            };
            if !sequences_for_handler.accept(response.sequence) {
                error!(
                    "Rejected view '{}' out-of-order sequence {}",
                    view_id_for_handler, response.sequence
                );
                return;
            }

            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            let is_initial_response = response.sequence == 0;
            if is_initial_response {
                trace!("Sequence reset: Clearing {} state", view_id_for_handler);
                state.clear();
            }

            for del in response.deletes {
                state.remove(&del);
            }
            for (id, item) in upserts {
                state.insert(id, item);
            }

            cell_writer.set(state.values().cloned().collect());
            drop(state);
            if is_initial_response && let Some(ready_writer) = ready_weak.upgrade() {
                ready_writer.set(true);
            }
        });
        if !self.try_register_query_handler(tx.clone(), handler) {
            error!("Refusing duplicate view transaction {tx}");
            return ViewWatch {
                items: cell.lock(),
                ready: ready_read,
            };
        }

        let socket = self.inner.socket.clone();
        let ready_for_status = ready.downgrade();
        let sequences_for_status = sequences;
        let status_cell = self.connection_status();
        let send_view_id = view_id;
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                sequences_for_status.reset_epoch();
                if let Some(ready_writer) = ready_for_status.upgrade() {
                    ready_writer.set(false);
                }
                if let ConnectionStatus::Connected(_) = &**status {
                    match socket.send(frame_to_send.clone()) {
                        Ok(()) => debug!("Watching view {send_view_id}"),
                        Err(e) => error!("Could not send view: {e:?}"),
                    }
                } else {
                    debug!("View {send_view_id} disconnected");
                }
            }
        });

        cell.own(status_guard);
        cell.own(view_cancel_guard(tx.clone(), self.inner.clone()));
        cell.own(retain_cell_guard(ready_read.clone()));
        cell.own(list_watch_cache_guard(
            cache_key.clone(),
            tx.clone(),
            self.inner.clone(),
        ));

        let watch = ViewWatch {
            items: cell.lock(),
            ready: ready_read,
        };
        self.cache_list_watch(cache_key, tx, &watch);
        watch
    }

    /// Watch a report and receive updates as a reactive Cell with an initial value.
    ///
    /// Like `watch_report`, but starts with a concrete initial value instead of None.
    pub fn watch_report_cell<R, O>(
        &self,
        report: impl Into<ReportRequest<R>>,
        initial: O,
    ) -> Cell<O, CellImmutable>
    where
        R: ReportParams + ReportIdStatic + Clone,
        O: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let cell = self.watch_report::<R, O>(report);
        // Map Option<O> -> O using the initial value as default
        cell.map(move |opt| opt.as_ref().map_or_else(|| initial.clone(), Clone::clone))
            .materialize()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send Command — Cell-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Send a command and get the result as a Cell.
    ///
    /// Returns a Cell<Option<Result<R, String>>> that starts as None
    /// and becomes Some when the server responds.
    pub fn send_command<C, R>(&self, command: &C) -> Cell<Option<Result<R, String>>, CellImmutable>
    where
        C: Serialize + Clone + CommandId,
        R: DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    {
        let request = CommandRequest::new(command.clone());
        let tx = request.tx.to_string();

        let wrapped = match wrap_command_request(&request) {
            Ok(w) => w,
            Err(e) => {
                let cell = Cell::new(Some(Err(e.to_string())));
                return cell.lock();
            }
        };

        let msg = MykoMessage::Command(wrapped);
        let frame = match self.encode_message(&msg) {
            Ok(f) => f,
            Err(e) => {
                let cell = Cell::new(Some(Err(e)));
                return cell.lock();
            }
        };

        let cell = Cell::new(None).with_name(format!("cmd:{}", command.command_id()).as_str());
        let cell_writer = cell.clone();

        // Register one-shot handler
        {
            let mut handlers = self
                .inner
                .command_response_handlers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handlers.insert(
                tx,
                Box::new(move |result: Result<Value, String>| {
                    let mapped = result.and_then(|value| {
                        serde_json::from_value::<R>(value).map_err(|e| e.to_string())
                    });
                    cell_writer.set(Some(mapped));
                }),
            );
        }

        if let Err(error) = self.send_or_queue(frame) {
            cell.set(Some(Err(error)));
        }

        cell.lock()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Handle Command — callback-based
    // ─────────────────────────────────────────────────────────────────────────

    /// Register a handler for incoming commands of a specific type.
    ///
    /// The handler receives the deserialized command and a `CommandResponder`
    /// that can be used to send back a response.
    ///
    /// Returns a `CallbackGuard` — drop it to unregister the handler.
    pub fn on_command<C, F>(&self, handler: F) -> CallbackGuard
    where
        C: DeserializeOwned
            + Clone
            + Send
            + crate::command::CommandId
            + crate::command::CommandIdStatic
            + 'static,
        F: Fn(C, CommandResponder) + Send + Sync + 'static,
    {
        let command_id: Arc<str> = C::COMMAND_ID.into();

        self.inner.command_request_handlers.insert(
            command_id.clone(),
            Box::new(move |value: Value, responder: CommandResponder| {
                if let Ok(cmd) = serde_json::from_value::<C>(value) {
                    handler(cmd, responder);
                }
            }),
        );

        let inner = self.inner.clone();
        let id = command_id;
        CallbackGuard::new(move || {
            inner.command_request_handlers.remove(&id);
        })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic Cell APIs
    // =========================================================================

    /// Dynamic/raw query watch for runtimes that only know wrapped query data.
    #[must_use]
    pub fn watch_query_raw(&self, mut query: WrappedQuery) -> Cell<Vec<Value>, CellImmutable> {
        let tx = next_subscription_tx();
        let Some(request) = query.query.as_object_mut() else {
            error!("Could not assign transaction ID to raw query request");
            return Cell::new(Vec::<Value>::new()).lock();
        };
        request.insert("tx".to_owned(), Value::String(tx.to_string()));

        let state: Arc<Mutex<HashMap<Arc<str>, Value>>> = Arc::default();
        let cell = Cell::new(Vec::<Value>::new()).with_name(query.query_id.as_ref());
        let cell_weak = cell.downgrade();
        let state_clone = state;
        let tx_clone = tx.clone();

        let msg = MykoMessage::Query(query);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode raw query request");
            return cell.lock();
        };

        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_clone {
                    return;
                }

                let items = {
                    let mut state = state_clone
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if response.sequence == 0 {
                        state.clear();
                    }
                    for wrapped_item in response.upserts {
                        if let Some(id) = wrapped_item.item.get("id").and_then(|v| v.as_str()) {
                            state.insert(id.into(), wrapped_item.item);
                        }
                    }
                    for id in response.deletes {
                        state.remove(&id);
                    }
                    state.values().cloned().collect::<Vec<Value>>()
                };
                cell_writer.set(items);
            }),
        );

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_to_send.clone());
            }
        });

        cell.own(status_guard);
        cell.own(query_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Dynamic/raw view watch for runtimes that only know wrapped view data.
    ///
    /// Mirrors [`watch_query_raw`](Self::watch_query_raw) but sends a
    /// `View` message and cancels with a `ViewCancel` on drop. View
    /// responses share the same `ws:m:view-response` event tag handling
    /// path as queries on the wire, so the same `query_handlers` slot
    /// stores the dispatch closure.
    #[must_use]
    pub fn watch_view_raw(&self, mut view: WrappedView) -> Cell<Vec<Value>, CellImmutable> {
        let tx = next_subscription_tx();
        let Some(request) = view.view.as_object_mut() else {
            error!("Could not assign transaction ID to raw view request");
            return Cell::new(Vec::<Value>::new()).lock();
        };
        request.insert("tx".to_owned(), Value::String(tx.to_string()));

        let state: Arc<Mutex<HashMap<Arc<str>, Value>>> = Arc::default();
        let cell = Cell::new(Vec::<Value>::new()).with_name(view.view_id.as_ref());
        let cell_weak = cell.downgrade();
        let state_clone = state;
        let tx_clone = tx.clone();

        let msg = MykoMessage::View(view);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode raw view request");
            return cell.lock();
        };

        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_clone {
                    return;
                }

                let items = {
                    let mut state = state_clone
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if response.sequence == 0 {
                        state.clear();
                    }
                    for wrapped_item in response.upserts {
                        if let Some(id) = wrapped_item.item.get("id").and_then(|v| v.as_str()) {
                            state.insert(id.into(), wrapped_item.item);
                        }
                    }
                    for id in response.deletes {
                        state.remove(&id);
                    }
                    state.values().cloned().collect::<Vec<Value>>()
                };
                cell_writer.set(items);
            }),
        );

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_to_send.clone());
            }
        });

        cell.own(status_guard);
        cell.own(view_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Dynamic/raw report watch for runtimes that only know wrapped report data.
    #[must_use]
    pub fn watch_report_raw(
        &self,
        report: crate::report::WrappedReport,
    ) -> Cell<Option<Value>, CellImmutable> {
        let tx: Arc<str> = report
            .report
            .get("tx")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into();

        let cell = Cell::new(None).with_name(report.report_id.as_str());
        let cell_weak = cell.downgrade();

        let msg = MykoMessage::Report(report);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode raw report request");
            return cell.lock();
        };

        self.inner.report_handlers.insert(
            tx.clone(),
            Box::new(move |response: Value| {
                let Some(cell_writer) = cell_weak.upgrade() else {
                    return;
                };
                cell_writer.set(Some(response));
            }),
        );

        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let ConnectionStatus::Connected(_) = &**status
            {
                let _ = socket.send(frame_to_send.clone());
            }
        });

        cell.own(status_guard);
        cell.own(report_cancel_guard(tx, self.inner.clone()));

        cell.lock()
    }

    /// Send a raw wrapped command and receive result as a reactive cell.
    pub fn send_command_raw_result(
        &self,
        command: WrappedCommand,
    ) -> Cell<Option<Result<Value, String>>, CellImmutable> {
        let tx = command
            .command
            .get("tx")
            .and_then(|v| v.as_str())
            .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_string);

        let cell = Cell::new(None).with_name(format!("cmd:{}", command.command_id).as_str());
        let cell_writer = cell.clone();

        {
            let mut handlers = self
                .inner
                .command_response_handlers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handlers.insert(
                tx,
                Box::new(move |result: Result<Value, String>| {
                    cell_writer.set(Some(result));
                }),
            );
        }

        if let Ok(frame) = self.encode_message(&MykoMessage::Command(command)) {
            if let Err(error) = self.send_or_queue(frame) {
                cell.set(Some(Err(error)));
            }
        } else {
            cell.set(Some(Err("Could not serialize command".to_string())));
        }

        cell.lock()
    }
}

fn normalize_myko_address(addr: &str) -> Result<String, url::ParseError> {
    let has_explicit_port = address_has_explicit_port(addr);
    let mut parsed = match Url::parse(addr) {
        Ok(url) if url.scheme() == "ws" || url.scheme() == "wss" => url,
        _ => Url::parse(&format!("ws://{addr}"))?,
    };

    if parsed.path() != "/myko" {
        parsed.set_path("/myko");
    }

    // `url::Url` canonicalizes explicit default ports (`:80` for ws and
    // `:443` for wss) to `None`. Preserve that standard-port intent instead of
    // replacing it with Myko's legacy 5155 default. Inputs that omit a port
    // entirely retain the established 5155 behavior.
    if parsed.port().is_none() && !has_explicit_port {
        let _ = parsed.set_port(Some(5155));
    }

    Ok(parsed.to_string())
}

fn address_has_explicit_port(addr: &str) -> bool {
    let authority = addr
        .split_once("://")
        .map_or(addr, |(_, remainder)| remainder)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();

    if let Some(remainder) = authority.strip_prefix('[')
        && let Some((_, suffix)) = remainder.split_once(']')
    {
        return suffix
            .strip_prefix(':')
            .is_some_and(|port| port.parse::<u16>().is_ok());
    }

    authority
        .rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
}

#[cfg(test)]
mod message_capture_tests {
    use std::{collections::VecDeque, time::Instant};

    use hyphae::{Gettable, Mutable};

    use super::{MAX_DISCONNECTED_SENDS, MykoClient, WsFrame, enqueue_disconnected_frame};
    use crate::wire::{MykoMessage, PingData};

    fn ping_frame(id: &str) -> Result<WsFrame, serde_json::Error> {
        let message = MykoMessage::Ping(PingData {
            id: id.to_owned(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        });
        serde_json::to_string(&message).map(WsFrame::Text)
    }

    fn wait_for_dispatch_state(client: &MykoClient, message_present: bool, ping_present: bool) {
        let started = Instant::now();
        while (
            client.messages().get().is_some(),
            client.ping_ms_sync().is_some(),
        ) != (message_present, ping_present)
            && started.elapsed() < std::time::Duration::from_secs(1)
        {
            std::thread::yield_now();
        }
    }

    #[test]
    fn raw_message_capture_can_be_disabled_without_affecting_dispatch() {
        let client = MykoClient::new_with_auto_reconnect(false);
        assert!(client.is_last_message_capture_enabled());

        let captured_frame = ping_frame("captured");
        assert!(captured_frame.is_ok());
        let Ok(captured_frame) = captured_frame else {
            return;
        };
        MykoClient::handle_frame(&client.inner, &captured_frame);
        wait_for_dispatch_state(&client, true, true);
        assert!(client.messages().get().is_some());
        assert!(client.ping_ms_sync().is_some());

        client.set_last_message_capture(false);
        client.inner.ping_ms.set(None);
        wait_for_dispatch_state(&client, false, false);
        assert!(!client.is_last_message_capture_enabled());
        assert!(client.messages().get().is_none());
        assert!(client.ping_ms_sync().is_none());

        let uncaptured_frame = ping_frame("not-captured");
        assert!(uncaptured_frame.is_ok());
        let Ok(uncaptured_frame) = uncaptured_frame else {
            return;
        };
        MykoClient::handle_frame(&client.inner, &uncaptured_frame);
        wait_for_dispatch_state(&client, false, true);
        assert!(client.messages().get().is_none());
        assert!(client.ping_ms_sync().is_some());
    }

    #[test]
    fn disconnected_frame_admission_is_bounded_and_lossless() {
        let mut pending = VecDeque::new();
        for index in 0..MAX_DISCONNECTED_SENDS {
            assert!(
                enqueue_disconnected_frame(&mut pending, WsFrame::Text(index.to_string())).is_ok()
            );
        }
        assert!(
            enqueue_disconnected_frame(&mut pending, WsFrame::Text("overflow".into())).is_err()
        );
        assert_eq!(pending.len(), MAX_DISCONNECTED_SENDS);
        assert!(matches!(pending.front(), Some(WsFrame::Text(value)) if value == "0"));
    }
}

#[cfg(test)]
mod address_tests {
    use super::normalize_myko_address;

    #[test]
    fn preserves_explicit_websocket_default_ports() {
        assert_eq!(
            normalize_myko_address("ws://agents.example:80/myko")
                .ok()
                .as_deref(),
            Some("ws://agents.example/myko")
        );
        assert_eq!(
            normalize_myko_address("wss://agents.example:443/myko")
                .ok()
                .as_deref(),
            Some("wss://agents.example/myko")
        );
        assert_eq!(
            normalize_myko_address("[::1]:80").ok().as_deref(),
            Some("ws://[::1]/myko")
        );
    }

    #[test]
    fn retains_legacy_default_when_port_is_omitted() {
        assert_eq!(
            normalize_myko_address("agents.example").ok().as_deref(),
            Some("ws://agents.example:5155/myko")
        );
        assert_eq!(
            normalize_myko_address("ws://agents.example")
                .ok()
                .as_deref(),
            Some("ws://agents.example:5155/myko")
        );
        assert_eq!(
            normalize_myko_address("[::1]").ok().as_deref(),
            Some("ws://[::1]:5155/myko")
        );
    }

    #[test]
    fn preserves_explicit_non_default_ports_and_normalizes_path() {
        assert_eq!(
            normalize_myko_address("ws://127.0.0.1:5174/ignored")
                .ok()
                .as_deref(),
            Some("ws://127.0.0.1:5174/myko")
        );
        assert_eq!(
            normalize_myko_address("agents.example:80").ok().as_deref(),
            Some("ws://agents.example/myko")
        );
    }
}
