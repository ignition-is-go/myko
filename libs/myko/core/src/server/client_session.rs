//! Transport-neutral client session and subscription management.
//!
//! Each authenticated connection gets a `ClientSession` that manages:
//! - Active subscriptions via `SubscriptionGuards`
//! - Message sending to the client
//! - Automatic cleanup on disconnect

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use hyphae::{Cell, CellImmutable, Signal, SubscriptionGuard, Watchable};

use crate::{
    client::MykoProtocol,
    core::item::AnyItem,
    query::{WindowedQuerySnapshot, WindowedQuerySource},
    report::AnyOutput,
    wire::{
        EncodedCommandMessage, ErasedWrappedItem, MykoMessage, QueryChange, QueryCursorWindow,
        QueryResponse, QueryWindow, ReportError, ReportResponse,
    },
};

/// Transport-neutral sink for delivering typed Myko messages to a client.
///
/// In-process, Iroh, WebSocket, and test adapters can all implement this
/// boundary without putting their framing protocol into session semantics.
pub trait SessionSink: Send + Sync + 'static {
    /// Send a message to the client.
    fn send(&self, msg: MykoMessage);

    /// Return the writer's preferred wire protocol for outbound messages.
    fn protocol(&self) -> MykoProtocol {
        MykoProtocol::JSON
    }

    /// Send a pre-serialized command payload while preserving command metadata.
    fn send_serialized_command(
        &self,
        tx: Arc<str>,
        command_id: String,
        payload: EncodedCommandMessage,
    );

    /// Send a report response while allowing implementations to defer
    /// expensive serialization/conversion work off the reactive callback path.
    fn send_report_response(&self, tx: Arc<str>, output: Arc<dyn AnyOutput>) {
        self.send(MykoMessage::ReportResponse(ReportResponse {
            response: output.to_value(),
            tx: tx.to_string(),
        }));
    }

    /// Send a query/view response while allowing implementations to defer
    /// expensive item-to-JSON conversion off the reactive callback path.
    fn send_query_response(&self, response: PendingQueryResponse, is_view: bool) {
        let wire = response.into_wire();
        if is_view {
            self.send(MykoMessage::ViewResponse(wire));
        } else {
            self.send(MykoMessage::QueryResponse(wire));
        }
    }

    /// Send one prepared node-protocol frame.
    ///
    /// Retained v6 WebSocket sinks can keep using [`Self::send`]. Native
    /// federation connectors override this method instead of owning a second
    /// session implementation.
    ///
    /// # Errors
    ///
    /// Returns an error when this sink cannot deliver native node frames.
    #[cfg(not(target_arch = "wasm32"))]
    fn send_node_frame(&self, _frame: myko_wire::NodeFrame) -> NodeFrameSend<'_> {
        Box::pin(async { Err("session sink does not support node frames".to_owned()) })
    }
}

pub type NodeFrameSend<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

#[cfg(not(target_arch = "wasm32"))]
static NEXT_HANDLER_EPOCH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct PendingQueryResponse {
    pub tx: Arc<str>,
    pub sequence: u64,
    pub upsert_items: Vec<Arc<dyn AnyItem>>,
    pub deletes: Vec<Arc<str>>,
    pub total_count: usize,
    pub window: Option<QueryWindow>,
    pub window_order_ids: Option<Vec<Arc<str>>>,
}

impl PendingQueryResponse {
    #[must_use]
    pub fn into_wire(self) -> QueryResponse {
        let upserts: Vec<ErasedWrappedItem> = self
            .upsert_items
            .iter()
            .map(|item| ErasedWrappedItem {
                item: item.clone(),
                item_type: crate::wire::intern_entity_type(item.entity_type()),
            })
            .collect();

        // `changes` carries ONLY the WindowOrder entry. Every client (TS,
        // Rust, and the other ports) reads item content from
        // `upserts`/`deletes` and consults `changes` solely to find
        // `windowOrder` — mirroring upserts/deletes into `changes` used to
        // serialize every item twice on the wire for nothing.
        let mut changes: Vec<QueryChange> =
            Vec::with_capacity(usize::from(self.window_order_ids.is_some()));
        if let Some(ids) = self.window_order_ids {
            changes.push(QueryChange::WindowOrder {
                ids,
                total_count: self.total_count,
                window: self.window.clone(),
            });
        }

        QueryResponse {
            tx: self.tx,
            sequence: self.sequence,
            changes,
            upserts,
            deletes: self.deletes,
            total_count: Some(self.total_count),
            window: self.window,
        }
    }
}

/// A connected client session that manages subscriptions.
///
/// When dropped, all subscription guards are dropped, automatically
/// cleaning up all reactive subscriptions.
pub struct ClientSession<W: SessionSink> {
    /// Unique client identifier
    pub client_id: Arc<str>,
    /// Transport sink for sending messages.
    writer: Arc<W>,
    /// Active subscriptions: tx -> entry
    subscriptions: HashMap<Arc<str>, SubscriptionEntry>,
}

enum SubscriptionEntry {
    Query(QuerySubscription),
    Guard {
        _guard: SubscriptionGuard,
    },
    #[cfg(not(target_arch = "wasm32"))]
    NativeMap {
        _task: NativeHandlerTask,
    },
    #[cfg(not(target_arch = "wasm32"))]
    NativeReport {
        _task: NativeHandlerTask,
        _guard: SubscriptionGuard,
    },
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeHandlerTask(tokio::task::JoinHandle<()>);

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeHandlerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct QuerySubscription {
    _guard: SubscriptionGuard,
    control: QueryWindowControl,
    kind: QuerySubscriptionKind,
}

enum QueryWindowControl {
    Materialized(Arc<Mutex<QuerySubscriptionState>>),
    Pushed(WindowedQuerySource),
}

#[derive(Clone, Copy)]
enum QuerySubscriptionKind {
    Query,
    View,
}

#[derive(Default)]
struct QuerySubscriptionState {
    sequence: u64,
    window: Option<QueryWindow>,
    cursor_window: Option<QueryCursorWindow>,
    all_items: HashMap<Arc<str>, Arc<dyn AnyItem>>,
    visible_items: HashMap<Arc<str>, Arc<dyn AnyItem>>,
}

#[derive(Default)]
struct PushedQuerySubscriptionState {
    sequence: u64,
    window: Option<QueryWindow>,
    visible_ids: Vec<Arc<str>>,
    visible_items: HashMap<Arc<str>, Arc<dyn AnyItem>>,
}

impl<W: SessionSink> ClientSession<W> {
    /// Create a new client session.
    pub fn new(client_id: Arc<str>, writer: W) -> Self {
        Self {
            client_id,
            writer: Arc::new(writer),
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a `CellMap` from a query cell factory.
    ///
    /// This is used by `WsHandler` when the query registration provides a cell factory.
    pub fn subscribe_query(
        &mut self,
        tx: Arc<str>,
        query_id: Arc<str>,
        cell: hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        let had_existing = self.subscriptions.contains_key(&tx);
        if had_existing {
            tracing::trace!(
                "ClientSession {} replacing existing query subscription tx={} (active_before={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
        }

        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let tx_for_log = tx_clone.clone();
        let query_id_for_diffs = query_id;
        let state = Arc::new(Mutex::new(QuerySubscriptionState {
            window,
            ..Default::default()
        }));
        let state_for_diffs = state.clone();

        // subscribe_diffs sends Initial first, then subsequent diffs
        let guard = cell.subscribe_diffs(move |diff| {
            let response = if let Ok(mut state) = state_for_diffs.lock() {
                state.apply_source_diff(diff, tx_clone.clone())
            } else {
                tracing::error!("Query subscription state poisoned for tx={}", tx_clone);
                return;
            };
            if let Some(response) = response {
                crate::server::dispatch_metrics::record_query_response(&query_id_for_diffs);
                writer.send_query_response(response, false);
            }
        });
        drop(cell);

        self.subscriptions.insert(
            tx,
            SubscriptionEntry::Query(QuerySubscription {
                _guard: guard,
                control: QueryWindowControl::Materialized(state),
                kind: QuerySubscriptionKind::Query,
            }),
        );

        let active = self.subscriptions.len();
        tracing::trace!(
            "ClientSession {} subscribed query tx={} active_subscriptions={}",
            self.client_id,
            tx_for_log,
            active
        );
        if active >= 100 && active.is_multiple_of(100) {
            tracing::trace!(
                "ClientSession {} high subscription count: {} (most recent tx={})",
                self.client_id,
                active,
                tx_for_log
            );
        }
    }

    /// Subscribe to an authoritative server-side bounded query source.
    pub fn subscribe_windowed_query(
        &mut self,
        tx: Arc<str>,
        query_id: Arc<str>,
        source: WindowedQuerySource,
    ) {
        let writer = self.writer.clone();
        let tx_for_diffs = tx.clone();
        let state = Arc::new(Mutex::new(PushedQuerySubscriptionState::default()));
        let state_for_diffs = state;
        let snapshots = source.snapshots().clone();
        let guard = snapshots.subscribe(move |signal| {
            let Signal::Value(snapshot) = signal else {
                return;
            };
            let response = if let Ok(mut state) = state_for_diffs.lock() {
                state.apply_snapshot(snapshot, tx_for_diffs.clone())
            } else {
                tracing::error!(
                    "Pushed query subscription state poisoned for tx={}",
                    tx_for_diffs
                );
                return;
            };
            crate::server::dispatch_metrics::record_query_response(&query_id);
            writer.send_query_response(response, false);
        });
        drop(snapshots);

        self.subscriptions.insert(
            tx,
            SubscriptionEntry::Query(QuerySubscription {
                _guard: guard,
                control: QueryWindowControl::Pushed(source),
                kind: QuerySubscriptionKind::Query,
            }),
        );
    }

    /// Subscribe to a `CellMap` from a view cell factory.
    pub fn subscribe_view(
        &mut self,
        tx: Arc<str>,
        cell: hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        self.subscribe_view_with_id(tx, "unknown".into(), cell, window);
    }

    /// Subscribe to a `CellMap` from a view cell factory with explicit view id for perf logging.
    pub fn subscribe_view_with_id(
        &mut self,
        tx: Arc<str>,
        view_id: Arc<str>,
        cell: hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let tx_for_log = tx_clone.clone();
        let client_id_for_log = self.client_id.clone();
        let view_id_for_log = view_id.clone();
        let view_id_for_metrics = view_id.clone();
        let subscribed_at = Instant::now();
        let state = Arc::new(Mutex::new(QuerySubscriptionState {
            window,
            ..Default::default()
        }));
        let state_for_diffs = state.clone();

        let guard = cell.subscribe_diffs(move |diff| {
            let response = if let Ok(mut state) = state_for_diffs.lock() { state.apply_source_diff(diff, tx_clone.clone()) } else {
                tracing::error!("View subscription state poisoned for tx={}", tx_clone);
                return;
            };
            let Some(response) = response else {
                return;
            };
            tracing::trace!(
                "ClientSession {} view tx={} seq={} upserts={} deletes={} changes={} window={:?} total_count={:?}",
                client_id_for_log,
                tx_clone,
                response.sequence,
                response.upsert_items.len(),
                response.deletes.len(),
                response
                    .upsert_items
                    .len()
                    .saturating_add(response.deletes.len())
                    .saturating_add(usize::from(response.window_order_ids.is_some())),
                response.window,
                response.total_count
            );
            if response.sequence == 0 {
                let first_emit_ms = subscribed_at.elapsed().as_millis();
                tracing::trace!(
                    target: "myko::server::view_perf",
                    "view_perf client={} view_id={} tx={} first_emit_ms={} initial_rows={} total_count={:?} window={:?}",
                    client_id_for_log,
                    view_id_for_log,
                    tx_clone,
                    first_emit_ms,
                    response.upsert_items.len(),
                    response.total_count,
                    response.window
                );
            }
            crate::server::dispatch_metrics::record_view_response(&view_id_for_metrics);
            writer.send_query_response(response, true);
        });
        drop(cell);

        self.subscriptions.insert(
            tx,
            SubscriptionEntry::Query(QuerySubscription {
                _guard: guard,
                control: QueryWindowControl::Materialized(state),
                kind: QuerySubscriptionKind::View,
            }),
        );

        tracing::trace!(
            "ClientSession {} subscribed view view_id={} tx={} active_subscriptions={}",
            self.client_id,
            view_id,
            tx_for_log,
            self.subscriptions.len()
        );
        drop(view_id);
    }

    /// Subscribe to a report cell.
    pub fn subscribe_report(
        &mut self,
        tx: Arc<str>,
        report_id: Arc<str>,
        cell: Cell<Arc<dyn AnyOutput>, CellImmutable>,
    ) {
        let had_existing = self.subscriptions.contains_key(&tx);
        if had_existing {
            tracing::trace!(
                "ClientSession {} replacing existing report subscription tx={} report_id={} (active_before={})",
                self.client_id,
                tx,
                report_id,
                self.subscriptions.len()
            );
        }

        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let tx_for_log = tx_clone.clone();
        let report_id_for_log = report_id.clone();
        let report_id_for_metrics = report_id.clone();

        let guard = cell.subscribe(move |signal| match &signal {
            Signal::Value(output) => {
                crate::server::dispatch_metrics::record_report_response(&report_id_for_metrics);
                writer.send_report_response(tx_clone.clone(), Arc::clone(output.as_ref()));
            }
            Signal::Complete => {}
            Signal::Error(e) => {
                writer.send(MykoMessage::ReportError(ReportError::new(
                    tx_clone.to_string(),
                    report_id.to_string(),
                    e.to_string(),
                )));
            }
        });
        drop(cell);

        self.subscriptions
            .insert(tx, SubscriptionEntry::Guard { _guard: guard });

        let active = self.subscriptions.len();
        tracing::trace!(
            "ClientSession {} subscribed report tx={} report_id={} active_subscriptions={}",
            self.client_id,
            tx_for_log,
            report_id_for_log,
            active
        );
        if active >= 100 && active.is_multiple_of(100) {
            tracing::trace!(
                "ClientSession {} high subscription count: {} (most recent report tx={}, id={})",
                self.client_id,
                active,
                tx_for_log,
                report_id_for_log
            );
        }
    }

    /// Subscribe a prepared node handler to the retained keyed-map runtime.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn subscribe_node_handler_map(
        &mut self,
        tx: Arc<str>,
        output: Arc<super::native_map::NativeMapOutput>,
        required_cut: Option<myko_federation::LogPosition>,
    ) -> Result<(), String> {
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("native handler requires an executor: {error}"))?;
        let writer = Arc::clone(&self.writer);
        let epoch = NEXT_HANDLER_EPOCH.fetch_add(1, Ordering::Relaxed);
        let retained = output.is_retained();
        let mut publications = output.watch();
        let task = executor.spawn(async move {
            let _owner = output;
            let mut state = NodeHandlerMapState::new(epoch);
            while let Ok(publication) = publications.recv_async().await {
                let waiting_for_cut = retained
                    && required_cut.is_some_and(|required| {
                        publication
                            .state
                            .through
                            .is_none_or(|through| through < required)
                    });
                if waiting_for_cut
                    && !matches!(
                        publication.state.liveness,
                        myko_federation::SubscriptionLiveness::Invalid { .. }
                    )
                {
                    continue;
                }
                match state.apply_snapshot(&publication.state) {
                    Ok(frame) => {
                        if let Err(error) = writer.send_node_frame(frame).await {
                            tracing::error!(%error, "node handler frame delivery failed");
                            return;
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "node handler frame serialization failed");
                        return;
                    }
                }
            }
        });
        self.subscriptions.insert(
            tx,
            SubscriptionEntry::NativeMap {
                _task: NativeHandlerTask(task),
            },
        );
        Ok(())
    }

    /// Subscribe a prepared scalar handler to the retained report runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when no Tokio executor is available to deliver frames.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_node_handler_report(
        &mut self,
        tx: Arc<str>,
        cell: Cell<Arc<dyn AnyOutput>, CellImmutable>,
    ) -> Result<(), String> {
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("native report requires an executor: {error}"))?;
        let writer = Arc::clone(&self.writer);
        let epoch = NEXT_HANDLER_EPOCH.fetch_add(1, Ordering::Relaxed);
        let (updates, mut latest) = tokio::sync::watch::channel(None::<Arc<dyn AnyOutput>>);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(output) = signal {
                updates.send_replace(Some(Arc::clone(output.as_ref())));
            }
        });
        let task = executor.spawn(async move {
            let mut sequence = 0_u64;
            while latest.changed().await.is_ok() {
                let output = latest.borrow_and_update().clone();
                let Some(output) = output else {
                    continue;
                };
                let frame = myko_wire::NodeFrame::HandlerState {
                    revision: myko_wire::HandlerStreamRevision { epoch, sequence },
                    state: Box::new(myko_wire::ErasedHandlerState {
                        value: Some(output.to_value()),
                        through: None,
                        liveness: myko_federation::SubscriptionLiveness::Current,
                        row_keys: None,
                    }),
                };
                if let Err(error) = writer.send_node_frame(frame).await {
                    tracing::error!(%error, "node report frame delivery failed");
                    return;
                }
                let Some(next) = sequence.checked_add(1) else {
                    return;
                };
                sequence = next;
            }
        });
        drop(cell);
        self.subscriptions.insert(
            tx,
            SubscriptionEntry::NativeReport {
                _task: NativeHandlerTask(task),
                _guard: guard,
            },
        );
        Ok(())
    }

    /// Update window for an active query subscription.
    pub fn update_query_window(&mut self, tx: &Arc<str>, window: Option<QueryWindow>) {
        if let Some((source, window)) = self.prepare_query_window_update(tx, window) {
            source.set_window(window);
        }
    }

    /// Apply an in-memory window update immediately, or return pushed-source
    /// work for the server runtime to dispatch on its blocking executor.
    ///
    /// Pushed sources may perform durable backend reads. Separating their
    /// callback from session bookkeeping lets async servers preserve request
    /// ordering without invoking blocking providers on runtime workers.
    #[doc(hidden)]
    #[must_use]
    pub fn prepare_query_window_update(
        &mut self,
        tx: &Arc<str>,
        window: Option<QueryWindow>,
    ) -> Option<(WindowedQuerySource, Option<QueryWindow>)> {
        let Some(SubscriptionEntry::Query(sub)) = self.subscriptions.get(tx) else {
            tracing::trace!(
                "ClientSession {} window update for unknown tx={} (active_subscriptions={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
            return None;
        };

        let response = match &sub.control {
            QueryWindowControl::Materialized(state) => {
                if let Ok(mut state) = state.lock() {
                    state.apply_window_update(window, tx.clone())
                } else {
                    tracing::error!(
                        "Query subscription state poisoned on window update for tx={}",
                        tx
                    );
                    return None;
                }
            }
            QueryWindowControl::Pushed(source) => {
                tracing::trace!(
                    "ClientSession {} pushed query window tx={} (active_subscriptions={})",
                    self.client_id,
                    tx,
                    self.subscriptions.len()
                );
                return Some((source.clone(), window));
            }
        };

        let Some(response) = response else {
            tracing::trace!(
                "ClientSession {} ignored no-op window update tx={} (active_subscriptions={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
            return None;
        };

        match sub.kind {
            QuerySubscriptionKind::Query => self.writer.send_query_response(response, false),
            QuerySubscriptionKind::View => self.writer.send_query_response(response, true),
        }
        tracing::trace!(
            "ClientSession {} updated query window tx={} (active_subscriptions={})",
            self.client_id,
            tx,
            self.subscriptions.len()
        );
        None
    }

    /// Move an active query to an exclusive ID-keyset page.
    pub fn update_query_cursor_window(&mut self, tx: &Arc<str>, window: QueryCursorWindow) {
        if window.validate().is_err() {
            tracing::warn!(tx = %tx, "rejected invalid query cursor window");
            return;
        }
        let Some(SubscriptionEntry::Query(sub)) = self.subscriptions.get(tx) else {
            return;
        };
        let response = match &sub.control {
            QueryWindowControl::Materialized(state) => {
                let Ok(mut state) = state.lock() else {
                    tracing::error!(tx = %tx, "query cursor state poisoned");
                    return;
                };
                state.window = None;
                state.apply_cursor_window_update(window, tx.clone())
            }
            QueryWindowControl::Pushed(source) => {
                source.set_cursor_window(window);
                return;
            }
        };
        let Some(response) = response else {
            return;
        };
        match sub.kind {
            QuerySubscriptionKind::Query => self.writer.send_query_response(response, false),
            QuerySubscriptionKind::View => self.writer.send_query_response(response, true),
        }
    }

    /// Update window for an active view subscription.
    pub fn update_view_window(&mut self, tx: &Arc<str>, window: Option<QueryWindow>) {
        tracing::trace!(
            "ClientSession {} requested view window update tx={} window={:?}",
            self.client_id,
            tx,
            window
        );
        self.update_query_window(tx, window);
    }

    /// Cancel a subscription by transaction ID.
    pub fn cancel(&mut self, tx: &Arc<str>) {
        let removed = self.subscriptions.remove(tx).is_some();
        tracing::trace!(
            "ClientSession {} cancel tx={} removed={} active_subscriptions={}",
            self.client_id,
            tx,
            removed,
            self.subscriptions.len()
        );
    }

    /// Cancel all subscriptions.
    pub fn cancel_all(&mut self) {
        let before = self.subscriptions.len();
        self.subscriptions.clear();
        tracing::trace!(
            "ClientSession {} cancel_all removed_subscriptions={}",
            self.client_id,
            before
        );
    }

    /// Get the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if a subscription exists.
    #[must_use]
    pub fn has_subscription(&self, tx: &Arc<str>) -> bool {
        self.subscriptions.contains_key(tx)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NodeHandlerMapState {
    epoch: u64,
    sequence: u64,
    initialized: bool,
    rows: BTreeMap<Arc<str>, Arc<dyn AnyItem>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NodeHandlerMapState {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            sequence: 0,
            initialized: false,
            rows: BTreeMap::new(),
        }
    }

    fn apply_snapshot(
        &mut self,
        snapshot: &super::native_map::MapSnapshot,
    ) -> Result<myko_wire::NodeFrame, serde_json::Error> {
        let through = snapshot.through.map(serde_json::to_value).transpose()?;
        let mut upserts = Vec::new();
        let next = snapshot.value.clone().unwrap_or_default();
        let deletes = self
            .rows
            .keys()
            .filter(|key| !next.contains_key(*key))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let membership_changed =
            !deletes.is_empty() || next.keys().any(|key| !self.rows.contains_key(key));
        for (key, value) in &next {
            if self.rows.get(key) != Some(value) {
                upserts.push(myko_wire::ErasedKeyedValue {
                    key: key.to_string(),
                    value: serde_json::to_value(value)?,
                });
            }
        }
        self.rows = next;
        let revision = myko_wire::HandlerStreamRevision {
            epoch: self.epoch,
            sequence: self.sequence,
        };
        self.sequence = self.sequence.saturating_add(1);
        if !self.initialized {
            self.initialized = true;
            let row_keys = self
                .rows
                .keys()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let values = self
                .rows
                .values()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(myko_wire::NodeFrame::HandlerState {
                revision,
                state: Box::new(myko_wire::ErasedHandlerState {
                    value: Some(serde_json::Value::Array(values)),
                    through,
                    liveness: snapshot.liveness.clone(),
                    row_keys: Some(row_keys),
                }),
            });
        }
        Ok(myko_wire::NodeFrame::HandlerViewDelta {
            revision,
            delta: Box::new(myko_wire::ErasedViewDelta {
                upserts,
                deletes,
                order: membership_changed
                    .then(|| self.rows.keys().map(ToString::to_string).collect()),
                through,
                liveness: snapshot.liveness.clone(),
            }),
        })
    }
}

impl PushedQuerySubscriptionState {
    fn apply_snapshot(
        &mut self,
        snapshot: &WindowedQuerySnapshot,
        tx: Arc<str>,
    ) -> PendingQueryResponse {
        if self.window.is_some() && snapshot.window.is_none() {
            self.sequence = 0;
        }

        let next_ids: Vec<_> = snapshot.entries.iter().map(|(id, _)| id.clone()).collect();
        let next_items: HashMap<_, _> = snapshot.entries.iter().cloned().collect();
        let mut deletes: Vec<_> = self
            .visible_ids
            .iter()
            .filter(|id| !next_items.contains_key(id.as_ref()))
            .cloned()
            .collect();
        deletes.sort_unstable();
        let upsert_items = snapshot
            .entries
            .iter()
            .filter(|(id, item)| {
                self.sequence == 0 || self.visible_items.get(id.as_ref()) != Some(item)
            })
            .map(|(_, item)| item.clone())
            .collect();
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        self.window.clone_from(&snapshot.window);
        self.visible_ids.clone_from(&next_ids);
        self.visible_items = next_items;

        PendingQueryResponse {
            tx,
            sequence,
            upsert_items,
            deletes,
            total_count: snapshot.total_count,
            window: snapshot.window.clone(),
            window_order_ids: snapshot.window.as_ref().map(|_| next_ids),
        }
    }
}

impl QuerySubscriptionState {
    fn apply_source_diff(
        &mut self,
        diff: &hyphae::MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        if self.window.is_none() && self.cursor_window.is_none() {
            return self.apply_source_diff_unwindowed(diff, tx);
        }

        let previous_total_count = self.all_items.len();
        let mut affected_ids = HashSet::new();
        let mut is_initial = false;
        self.apply_source_changes(diff, &mut affected_ids, &mut is_initial);

        // NOTE(ts): MapDiff::Initial = full state replacement — reset sequence
        // so the client performs replace_all instead of incremental update.
        if is_initial {
            self.sequence = 0;
        }

        self.compute_windowed_response(
            tx,
            &affected_ids,
            &affected_ids,
            previous_total_count,
            false,
        )
    }

    fn apply_source_changes(
        &mut self,
        diff: &hyphae::MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        affected_ids: &mut HashSet<Arc<str>>,
        is_initial: &mut bool,
    ) {
        match diff {
            hyphae::MapDiff::Initial { entries } => {
                *is_initial = true;
                affected_ids.extend(self.all_items.keys().cloned());
                self.all_items.clear();
                for (id, item) in entries {
                    affected_ids.insert(id.clone());
                    self.all_items.insert(id.clone(), item.clone());
                }
            }
            hyphae::MapDiff::Insert { key, value } => {
                affected_ids.insert(key.clone());
                self.all_items.insert(key.clone(), value.clone());
            }
            hyphae::MapDiff::Update { key, new_value, .. } => {
                affected_ids.insert(key.clone());
                self.all_items.insert(key.clone(), new_value.clone());
            }
            hyphae::MapDiff::Remove { key, .. } => {
                affected_ids.insert(key.clone());
                self.all_items.remove(key);
            }
            hyphae::MapDiff::Batch { changes } => {
                for change in changes {
                    self.apply_source_changes(change, affected_ids, is_initial);
                }
            }
        }
    }

    fn apply_source_diff_unwindowed(
        &mut self,
        diff: &hyphae::MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        let previous_total_count = self.all_items.len();
        let mut affected_ids = HashSet::new();
        let mut is_initial = false;
        self.apply_source_changes(diff, &mut affected_ids, &mut is_initial);

        // NOTE(ts): MapDiff::Initial means "here is the complete new state" —
        // reset sequence to 0 so the client performs a full replace_all instead
        // of an incremental update.
        if is_initial {
            self.sequence = 0;
        }

        let mut response_ids: Vec<_> = if is_initial {
            self.all_items.keys().cloned().collect()
        } else {
            affected_ids.into_iter().collect()
        };
        response_ids.sort_unstable();
        let mut upsert_items = Vec::new();
        let mut deletes = Vec::new();
        for id in response_ids {
            if let Some(item) = self.all_items.get(id.as_ref()) {
                upsert_items.push(item.clone());
            } else {
                deletes.push(id);
            }
        }

        let total_count = self.all_items.len();
        let total_count_changed = previous_total_count != total_count;
        let visible_changed = !upsert_items.is_empty() || !deletes.is_empty();
        let should_emit = self.sequence == 0 || visible_changed || total_count_changed;

        tracing::trace!(
            "ClientSession tx={} window_decision force_emit=false seq={} changed_ids={} upserts={} deletes={} visible_changed={} window_order_changed=false total_count_changed={} should_emit={} total_count={} window=None",
            tx,
            self.sequence,
            upsert_items.len().saturating_add(deletes.len()),
            upsert_items.len(),
            deletes.len(),
            visible_changed,
            total_count_changed,
            should_emit,
            total_count
        );

        if !should_emit {
            return None;
        }

        let seq = self.sequence;
        self.sequence = self.sequence.saturating_add(1);

        Some(PendingQueryResponse {
            tx,
            sequence: seq,
            upsert_items,
            deletes,
            total_count,
            window: None,
            window_order_ids: None,
        })
    }

    fn apply_window_update(
        &mut self,
        window: Option<QueryWindow>,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        let same_window = match (&self.window, &window) {
            (None, None) => true,
            (Some(current), Some(next)) => {
                current.offset == next.offset && current.limit == next.limit
            }
            _ => false,
        };
        if same_window {
            return None;
        }

        let was_windowed = self.window.is_some() || self.cursor_window.is_some();
        self.cursor_window = None;
        self.window = window;

        // Leaving windowed mode must replace the client's partial page with
        // the complete source state. Sequence zero gives every client the
        // same full-snapshot semantics as an Initial diff.
        if was_windowed && self.window.is_none() {
            self.sequence = 0;
        }

        // A changed window is observable even when it happens to select the
        // same IDs (for example, increasing a limit beyond the row count).
        self.compute_windowed_response(
            tx,
            &HashSet::new(),
            &HashSet::new(),
            self.all_items.len(),
            true,
        )
    }

    fn apply_cursor_window_update(
        &mut self,
        window: QueryCursorWindow,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        if self.cursor_window.as_ref() == Some(&window) {
            return None;
        }
        self.cursor_window = Some(window);
        self.compute_windowed_response(
            tx,
            &HashSet::new(),
            &HashSet::new(),
            self.all_items.len(),
            true,
        )
    }

    fn compute_windowed_response(
        &mut self,
        tx: Arc<str>,
        changed_ids: &HashSet<Arc<str>>,
        removed_ids: &HashSet<Arc<str>>,
        previous_total_count: usize,
        force_emit: bool,
    ) -> Option<PendingQueryResponse> {
        if self.window.is_none() && self.cursor_window.is_none() {
            return self.compute_unwindowed_response(
                tx,
                changed_ids,
                removed_ids,
                previous_total_count,
                force_emit,
            );
        }

        self.compute_bounded_window_response(tx, changed_ids, previous_total_count, force_emit)
    }

    fn compute_unwindowed_response(
        &mut self,
        tx: Arc<str>,
        changed_ids: &HashSet<Arc<str>>,
        removed_ids: &HashSet<Arc<str>>,
        previous_total_count: usize,
        force_emit: bool,
    ) -> Option<PendingQueryResponse> {
        if self.sequence == 0 {
            self.visible_items = self.all_items.clone();
        } else {
            for id in removed_ids {
                self.visible_items.remove(id);
            }
            for id in changed_ids {
                if let Some(item) = self.all_items.get(id.as_ref()) {
                    self.visible_items.insert(id.clone(), item.clone());
                }
            }
        }

        let mut deletes: Vec<Arc<str>> = removed_ids
            .iter()
            .filter(|id| !self.all_items.contains_key(id.as_ref()))
            .cloned()
            .collect();
        deletes.sort_unstable();
        let source_ids: Vec<Arc<str>> = if self.sequence == 0 {
            self.all_items.keys().cloned().collect()
        } else {
            changed_ids.iter().cloned().collect()
        };
        let mut source_ids = source_ids;
        source_ids.sort_unstable();
        let upsert_items: Vec<Arc<dyn AnyItem>> = source_ids
            .into_iter()
            .filter_map(|id| self.all_items.get(id.as_ref()).cloned())
            .collect();
        let total_count = self.all_items.len();
        let total_count_changed = previous_total_count != total_count;
        let visible_changed = !upsert_items.is_empty() || !deletes.is_empty();
        let should_emit =
            force_emit || self.sequence == 0 || visible_changed || total_count_changed;

        tracing::trace!(
            "ClientSession tx={} window_decision force_emit={} seq={} changed_ids={} upserts={} deletes={} visible_changed={} window_order_changed=false total_count_changed={} should_emit={} total_count={} window={:?}",
            tx,
            force_emit,
            self.sequence,
            changed_ids.len(),
            upsert_items.len(),
            deletes.len(),
            visible_changed,
            total_count_changed,
            should_emit,
            total_count,
            self.window
        );
        if !should_emit {
            return None;
        }
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        Some(PendingQueryResponse {
            tx,
            sequence,
            upsert_items,
            deletes,
            total_count,
            window: None,
            window_order_ids: None,
        })
    }

    fn compute_bounded_window_response(
        &mut self,
        tx: Arc<str>,
        changed_ids: &HashSet<Arc<str>>,
        previous_total_count: usize,
        force_emit: bool,
    ) -> Option<PendingQueryResponse> {
        let mut ordered_ids: Vec<Arc<str>> = self.all_items.keys().cloned().collect();
        ordered_ids.sort_unstable();

        let visible_ids: Vec<Arc<str>> = if let Some(cursor) = &self.cursor_window {
            let (start, end) = cursor.after.as_ref().map_or_else(
                || {
                    cursor.before.as_ref().map_or_else(
                        || (0, cursor.limit.min(ordered_ids.len())),
                        |before| {
                            let end = ordered_ids.partition_point(|id| id < before);
                            (end.saturating_sub(cursor.limit), end)
                        },
                    )
                },
                |after| {
                    let start = ordered_ids.partition_point(|id| id <= after);
                    (
                        start,
                        start.saturating_add(cursor.limit).min(ordered_ids.len()),
                    )
                },
            );
            ordered_ids.get(start..end).unwrap_or_default().to_vec()
        } else if let Some(window) = &self.window {
            if window.limit == 0 {
                Vec::new()
            } else {
                let start = window.offset.min(ordered_ids.len());
                let end = start.saturating_add(window.limit).min(ordered_ids.len());
                ordered_ids.get(start..end).unwrap_or_default().to_vec()
            }
        } else {
            ordered_ids
        };

        let previous_visible = self.visible_items.clone();
        let mut previous_visible_ids: Vec<Arc<str>> = previous_visible.keys().cloned().collect();
        previous_visible_ids.sort_unstable();
        let mut next_visible: HashMap<Arc<str>, Arc<dyn AnyItem>> = HashMap::new();

        for id in &visible_ids {
            if let Some(item) = self.all_items.get(id.as_ref()) {
                next_visible.insert(id.clone(), item.clone());
            }
        }

        let mut deletes: Vec<Arc<str>> = previous_visible
            .keys()
            .filter(|id| !next_visible.contains_key(*id))
            .cloned()
            .collect();
        deletes.sort_unstable();

        let mut upsert_items: Vec<Arc<dyn AnyItem>> = Vec::new();
        for id in &visible_ids {
            let is_new = !previous_visible.contains_key(id);
            let is_changed = changed_ids.contains(id);
            let should_emit = self.sequence == 0 || is_new || is_changed;

            if should_emit && let Some(item) = next_visible.get(id) {
                upsert_items.push(item.clone());
            }
        }

        let total_count = self.all_items.len();
        let window_order_changed = previous_visible_ids != visible_ids;
        let total_count_changed = previous_total_count != total_count;
        let visible_changed = !upsert_items.is_empty() || !deletes.is_empty();
        let should_emit = force_emit
            || self.sequence == 0
            || visible_changed
            || window_order_changed
            || total_count_changed;

        tracing::trace!(
            "ClientSession tx={} window_decision force_emit={} seq={} changed_ids={} upserts={} deletes={} visible_changed={} window_order_changed={} total_count_changed={} should_emit={} total_count={} window={:?}",
            tx,
            force_emit,
            self.sequence,
            changed_ids.len(),
            upsert_items.len(),
            deletes.len(),
            visible_changed,
            window_order_changed,
            total_count_changed,
            should_emit,
            total_count,
            self.window
        );

        self.visible_items = next_visible;

        if !should_emit {
            return None;
        }

        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);

        Some(PendingQueryResponse {
            tx,
            sequence,
            upsert_items,
            deletes,
            total_count,
            window: self.window.clone(),
            window_order_ids: (self.window.is_some() || self.cursor_window.is_some())
                .then_some(visible_ids),
        })
    }
}

impl<W: SessionSink> Drop for ClientSession<W> {
    fn drop(&mut self) {
        // All guards drop automatically
        tracing::trace!(
            "ClientSession dropped for client {}, cleaning up {} subscriptions",
            self.client_id,
            self.subscriptions.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use hyphae::{Mutable, SelectExt};

    use super::*;
    use crate::{common::with_id::WithId, store::StoreRegistry, test_util::scheduler_test_serial};

    // Mock writer that collects messages
    struct MockWriter {
        messages: Mutex<Vec<MykoMessage>>,
        #[cfg(not(target_arch = "wasm32"))]
        node_frames: Mutex<Vec<myko_wire::NodeFrame>>,
        #[cfg(not(target_arch = "wasm32"))]
        node_frame_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                messages: Mutex::new(Vec::new()),
                #[cfg(not(target_arch = "wasm32"))]
                node_frames: Mutex::new(Vec::new()),
                #[cfg(not(target_arch = "wasm32"))]
                node_frame_hook: Mutex::new(None),
            }
        }

        fn message_count(&self) -> usize {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }

        fn last_message(&self) -> Option<MykoMessage> {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last()
                .cloned()
        }

        fn wait_for_message_count(&self, expected: usize) {
            let deadline = std::time::Instant::now()
                .checked_add(std::time::Duration::from_secs(1))
                .unwrap_or_else(std::time::Instant::now);
            while self.message_count() < expected && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
        }

        fn messages(&self) -> Vec<MykoMessage> {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        #[cfg(not(target_arch = "wasm32"))]
        fn node_frames(&self) -> Vec<myko_wire::NodeFrame> {
            self.node_frames
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl SessionSink for MockWriter {
        fn send(&self, msg: MykoMessage) {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(msg);
        }

        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            payload: EncodedCommandMessage,
        ) {
            match payload {
                EncodedCommandMessage::Json(json) => {
                    if let Ok(message) = serde_json::from_str(&json) {
                        self.send(message);
                    }
                }
                EncodedCommandMessage::Cbor(bytes) => {
                    if let Ok(message) = ciborium::de::from_reader(bytes.as_slice()) {
                        self.send(message);
                    }
                }
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        fn send_node_frame(&self, frame: myko_wire::NodeFrame) -> NodeFrameSend<'_> {
            Box::pin(async move {
                self.node_frames
                    .lock()
                    .map_err(|_| "node frame sink is poisoned".to_owned())?
                    .push(frame);
                let hook = self
                    .node_frame_hook
                    .lock()
                    .map_err(|_| "node frame hook is poisoned".to_owned())?
                    .take();
                if let Some(hook) = hook {
                    hook();
                }
                Ok(())
            })
        }
    }

    // Need Arc wrapper for test
    struct ArcMockWriter(Arc<MockWriter>);

    impl SessionSink for ArcMockWriter {
        fn send(&self, msg: MykoMessage) {
            self.0.send(msg);
        }

        fn send_serialized_command(
            &self,
            tx: Arc<str>,
            command_id: String,
            payload: EncodedCommandMessage,
        ) {
            self.0.send_serialized_command(tx, command_id, payload);
        }

        #[cfg(not(target_arch = "wasm32"))]
        fn send_node_frame(&self, frame: myko_wire::NodeFrame) -> NodeFrameSend<'_> {
            self.0.send_node_frame(frame)
        }
    }

    // Test entity
    #[derive(Debug, Clone, PartialEq, serde::Serialize)]
    struct TestEntity {
        id: Arc<str>,
        name: String,
    }

    impl WithId for TestEntity {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl AnyItem for TestEntity {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestEntity"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|typed| self == typed)
        }
    }

    fn make_entity(id: &str, name: &str) -> Arc<dyn AnyItem> {
        Arc::new(TestEntity {
            id: id.into(),
            name: name.to_string(),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct BoundedNodeWriter {
        frames: flume::Sender<myko_wire::NodeFrame>,
        attempted: flume::Sender<()>,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl SessionSink for BoundedNodeWriter {
        fn send(&self, _msg: MykoMessage) {}

        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: EncodedCommandMessage,
        ) {
        }

        fn send_node_frame(&self, frame: myko_wire::NodeFrame) -> NodeFrameSend<'_> {
            Box::pin(async move {
                self.attempted.send(()).map_err(|error| error.to_string())?;
                self.frames
                    .send_async(frame)
                    .await
                    .map_err(|error| error.to_string())
            })
        }
    }

    #[tokio::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn dropping_native_map_cancels_a_backpressured_producer() -> anyhow::Result<()> {
        let _serial = scheduler_test_serial();
        let (frames, received) = flume::bounded(1);
        let (attempted, attempts) = flume::unbounded();
        let mut session = ClientSession::new(
            "bounded-map".into(),
            BoundedNodeWriter { frames, attempted },
        );
        let map = hyphae::CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
        let output = super::super::native_map::NativeMapOutput::new(map.clone().lock())
            .map_err(anyhow::Error::msg)?;
        session
            .subscribe_node_handler_map("map".into(), output, None)
            .map_err(anyhow::Error::msg)?;
        let deadline = std::time::Duration::from_secs(2);
        tokio::time::timeout(deadline, attempts.recv_async()).await??;
        map.insert("a".into(), make_entity("a", "Alice"));
        tokio::time::timeout(deadline, attempts.recv_async()).await??;
        anyhow::ensure!(
            received.len() == 1,
            "map producer did not fill the bounded queue"
        );
        drop(session);
        tokio::task::yield_now().await;
        anyhow::ensure!(
            matches!(
                received.recv_async().await?,
                myko_wire::NodeFrame::HandlerState { .. }
            ),
            "map did not deliver its initial state"
        );
        anyhow::ensure!(
            tokio::time::timeout(deadline, received.recv_async())
                .await?
                .is_err(),
            "map producer survived session drop"
        );
        Ok(())
    }

    #[tokio::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn native_reports_coalesce_backpressure_and_cancel_on_drop() -> anyhow::Result<()> {
        let _serial = scheduler_test_serial();
        let (frames, received) = flume::bounded(1);
        let (attempted, attempts) = flume::unbounded();
        let mut session = ClientSession::new(
            "bounded-report".into(),
            BoundedNodeWriter { frames, attempted },
        );
        let output = |value| -> Arc<dyn AnyOutput> { Arc::new(serde_json::json!(value)) };
        let cell = Cell::new(output(0));
        session
            .subscribe_node_handler_report("report".into(), cell.clone().lock())
            .map_err(anyhow::Error::msg)?;
        let deadline = std::time::Duration::from_secs(2);
        tokio::time::timeout(deadline, attempts.recv_async()).await??;
        cell.set(output(1));
        tokio::time::timeout(deadline, attempts.recv_async()).await??;
        for value in 2..=100 {
            cell.set(output(value));
        }
        for (sequence, expected) in [0, 1, 100].into_iter().enumerate() {
            let frame = tokio::time::timeout(deadline, received.recv_async()).await??;
            anyhow::ensure!(
                matches!(frame, myko_wire::NodeFrame::HandlerState { revision, state }
                if revision.sequence == u64::try_from(sequence)? && state.value == Some(serde_json::json!(expected))),
                "report lost its latest value or emitted a noncontiguous sequence"
            );
        }
        drop(session);
        anyhow::ensure!(
            tokio::time::timeout(deadline, received.recv_async())
                .await?
                .is_err(),
            "report producer survived session drop"
        );
        Ok(())
    }

    #[test]
    fn unwindowed_batch_remove_then_insert_same_key_emits_only_final_upsert() {
        let old_item = make_entity("task-1", "Old");
        let new_item = make_entity("task-1", "New");
        let expected_item = new_item.clone();
        let mut state = QuerySubscriptionState {
            sequence: 1,
            all_items: HashMap::from([("task-1".into(), old_item.clone())]),
            ..Default::default()
        };
        let diff = hyphae::MapDiff::Batch {
            changes: vec![
                hyphae::MapDiff::Remove {
                    key: "task-1".into(),
                    old_value: old_item,
                },
                hyphae::MapDiff::Insert {
                    key: "task-1".into(),
                    value: new_item,
                },
            ],
        };

        let response = state.apply_source_diff_unwindowed(&diff, "tx".into());
        assert!(response.is_some(), "expected a response");
        let Some(response) = response else {
            return;
        };
        assert!(response.deletes.is_empty());
        assert_eq!(response.upsert_items.len(), 1);
        assert!(
            response
                .upsert_items
                .first()
                .is_some_and(|item| Arc::ptr_eq(item, &expected_item)),
            "the final inserted item should be upserted"
        );
    }

    #[test]
    fn unwindowed_batch_insert_then_remove_same_key_emits_only_final_delete() {
        let item = make_entity("task-1", "Transient");
        let mut state = QuerySubscriptionState {
            sequence: 1,
            ..Default::default()
        };
        let diff = hyphae::MapDiff::Batch {
            changes: vec![
                hyphae::MapDiff::Insert {
                    key: "task-1".into(),
                    value: item.clone(),
                },
                hyphae::MapDiff::Remove {
                    key: "task-1".into(),
                    old_value: item,
                },
            ],
        };

        let response = state.apply_source_diff_unwindowed(&diff, "tx".into());
        assert!(response.is_some(), "expected a response");
        let Some(response) = response else {
            return;
        };
        assert!(response.upsert_items.is_empty());
        assert_eq!(response.deletes, vec![Arc::<str>::from("task-1")]);
    }

    #[test]
    fn nested_batch_initial_resets_sequence_and_coalesces_to_full_snapshot() {
        let stale_item = make_entity("stale", "Stale");
        let transient = make_entity("transient", "Transient");
        let fresh = make_entity("fresh", "Fresh");
        let later = make_entity("later", "Later");
        let mut state = QuerySubscriptionState {
            sequence: 7,
            all_items: HashMap::from([("stale".into(), stale_item)]),
            ..Default::default()
        };
        let diff = hyphae::MapDiff::Batch {
            changes: vec![
                hyphae::MapDiff::Insert {
                    key: "transient".into(),
                    value: transient,
                },
                hyphae::MapDiff::Batch {
                    changes: vec![
                        hyphae::MapDiff::Initial {
                            entries: vec![("fresh".into(), fresh)],
                        },
                        hyphae::MapDiff::Insert {
                            key: "later".into(),
                            value: later,
                        },
                    ],
                },
            ],
        };

        let response = state.apply_source_diff(&diff, "tx".into());
        assert!(response.is_some(), "nested Initial should emit a snapshot");
        let Some(response) = response else {
            return;
        };
        let mut ids: Vec<_> = response.upsert_items.iter().map(|item| item.id()).collect();
        ids.sort_unstable();

        assert_eq!(response.sequence, 0);
        assert_eq!(
            ids,
            vec![Arc::<str>::from("fresh"), Arc::<str>::from("later")]
        );
        assert!(response.deletes.is_empty());
        assert_eq!(response.total_count, 2);
    }

    #[test]
    fn bounded_to_unwindowed_emits_full_sequence_zero_snapshot() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);
        let tx: Arc<str> = "tx-1".into();
        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query(
            tx.clone(),
            "query-1".into(),
            cellmap,
            Some(QueryWindow {
                offset: 1,
                limit: 1,
            }),
        );
        let before = mock.message_count();

        session.update_query_window(&tx, None);

        assert_eq!(mock.message_count(), before + 1);
        let last_message = mock.last_message();
        assert!(
            matches!(last_message, Some(MykoMessage::QueryResponse(_))),
            "expected QueryResponse"
        );
        let Some(MykoMessage::QueryResponse(response)) = last_message else {
            return;
        };
        let mut ids: Vec<_> = response.upserts.iter().map(|item| item.item.id()).collect();
        ids.sort_unstable();
        assert_eq!(response.sequence, 0);
        assert_eq!(
            ids,
            vec![
                Arc::<str>::from("a"),
                Arc::<str>::from("b"),
                Arc::<str>::from("c")
            ]
        );
        assert!(response.deletes.is_empty());
        assert!(response.window.is_none());
        assert!(response.changes.is_empty());
        assert_eq!(response.total_count, Some(3));
    }

    #[test]
    fn pushed_window_source_emits_authoritative_pages_without_full_session_state() {
        let _serial = scheduler_test_serial();
        let first = make_entity("b", "Bob");
        let second = make_entity("c", "Charlie");
        let snapshots = Cell::new(Arc::new(WindowedQuerySnapshot {
            entries: vec![("b".into(), first)],
            total_count: 3,
            window: Some(QueryWindow {
                offset: 1,
                limit: 1,
            }),
        }));
        let snapshots_for_window = snapshots.clone();
        let second_for_window = second.clone();
        let source = WindowedQuerySource::new(snapshots.lock(), move |window| {
            snapshots_for_window.set(Arc::new(WindowedQuerySnapshot {
                entries: vec![("c".into(), second_for_window.clone())],
                total_count: 3,
                window,
            }));
        });

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);
        let tx: Arc<str> = "tx-pushed".into();
        session.subscribe_windowed_query(tx.clone(), "query-pushed".into(), source);

        let initial_message = mock.last_message();
        assert!(matches!(
            initial_message,
            Some(MykoMessage::QueryResponse(_))
        ));
        let Some(MykoMessage::QueryResponse(initial)) = initial_message else {
            return;
        };
        assert_eq!(initial.sequence, 0);
        assert_eq!(initial.total_count, Some(3));
        assert!(matches!(
            initial.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }] if ids == &[Arc::<str>::from("b")]
        ));

        session.update_query_window(
            &tx,
            Some(QueryWindow {
                offset: 2,
                limit: 1,
            }),
        );
        mock.wait_for_message_count(2);
        let next_message = mock.last_message();
        assert!(matches!(next_message, Some(MykoMessage::QueryResponse(_))));
        let Some(MykoMessage::QueryResponse(next)) = next_message else {
            return;
        };
        assert_eq!(next.sequence, 1);
        assert_eq!(next.deletes, vec![Arc::<str>::from("b")]);
        assert!(matches!(
            next.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }] if ids == &[Arc::<str>::from("c")]
        ));
        assert_eq!(next.total_count, Some(3));
    }

    #[test]
    fn unwindowed_to_bounded_updates_visible_page() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);
        let tx: Arc<str> = "tx-1".into();
        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query(tx.clone(), "query-1".into(), cellmap, None);

        session.update_query_window(
            &tx,
            Some(QueryWindow {
                offset: 1,
                limit: 1,
            }),
        );

        let last_message = mock.last_message();
        assert!(
            matches!(last_message, Some(MykoMessage::QueryResponse(_))),
            "expected QueryResponse"
        );
        let Some(MykoMessage::QueryResponse(response)) = last_message else {
            return;
        };
        assert_eq!(response.sequence, 1);
        assert_eq!(response.upserts.len(), 1);
        assert!(
            response
                .upserts
                .first()
                .is_some_and(|item| item.item.id().as_ref() == "b")
        );
        assert!(response.deletes.is_empty());
        assert!(matches!(
            response.window,
            Some(QueryWindow {
                offset: 1,
                limit: 1
            })
        ));
        assert!(matches!(
            response.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }] if ids == &[Arc::<str>::from("b")]
        ));
    }

    #[test]
    fn cursor_windows_are_exclusive_and_bidirectional() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        for id in ["a", "b", "c", "d"] {
            store.insert(id.into(), make_entity(id, id));
        }

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);
        let tx: Arc<str> = "tx-cursor".into();
        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query(
            tx.clone(),
            "query-cursor".into(),
            cellmap,
            Some(QueryWindow {
                offset: 0,
                limit: 2,
            }),
        );

        session.update_query_cursor_window(&tx, QueryCursorWindow::after("b", 2));
        let forward_message = mock.last_message();
        assert!(matches!(
            forward_message,
            Some(MykoMessage::QueryResponse(_))
        ));
        let Some(MykoMessage::QueryResponse(forward)) = forward_message else {
            return;
        };
        assert!(forward.window.is_none());
        assert!(matches!(
            forward.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }]
                if ids == &[Arc::<str>::from("c"), Arc::<str>::from("d")]
        ));

        session.update_query_cursor_window(&tx, QueryCursorWindow::before("c", 2));
        let backward_message = mock.last_message();
        assert!(matches!(
            backward_message,
            Some(MykoMessage::QueryResponse(_))
        ));
        let Some(MykoMessage::QueryResponse(backward)) = backward_message else {
            return;
        };
        assert!(matches!(
            backward.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }]
                if ids == &[Arc::<str>::from("a"), Arc::<str>::from("b")]
        ));
    }

    #[test]
    fn bounded_window_change_emits_even_when_selected_ids_are_unchanged() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);
        let tx: Arc<str> = "tx-1".into();
        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query(
            tx.clone(),
            "query-1".into(),
            cellmap,
            Some(QueryWindow {
                offset: 0,
                limit: 2,
            }),
        );
        let before = mock.message_count();

        session.update_query_window(
            &tx,
            Some(QueryWindow {
                offset: 0,
                limit: 20,
            }),
        );

        assert_eq!(mock.message_count(), before + 1);
        let last_message = mock.last_message();
        assert!(
            matches!(last_message, Some(MykoMessage::QueryResponse(_))),
            "expected QueryResponse"
        );
        let Some(MykoMessage::QueryResponse(response)) = last_message else {
            return;
        };
        assert_eq!(response.sequence, 1);
        assert!(response.upserts.is_empty());
        assert!(response.deletes.is_empty());
        assert!(matches!(
            response.window,
            Some(QueryWindow {
                offset: 0,
                limit: 20
            })
        ));
        assert!(matches!(
            response.changes.as_slice(),
            [QueryChange::WindowOrder { ids, .. }]
                if ids == &[Arc::<str>::from("a"), Arc::<str>::from("b")]
        ));
    }

    #[test]
    fn test_subscribe_query_cellmap() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query("tx-1".into(), "query-1".into(), cellmap, None);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Add an entity
        store.insert("c".into(), make_entity("c", "Charlie"));
        mock.wait_for_message_count(2);
        assert!(mock.message_count() >= 2);
    }

    #[tokio::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn node_handler_map_keeps_changes_made_while_delivering_its_initial_frame() {
        let _serial = scheduler_test_serial();
        let map = hyphae::CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
        map.insert("a".into(), make_entity("a", "Alice"));
        let mock = Arc::new(MockWriter::new());
        let during_initial = map.clone();
        *mock
            .node_frame_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
            during_initial.remove(&Arc::<str>::from("a"));
        }));
        let mut session =
            ClientSession::new("client-handoff".into(), ArcMockWriter(Arc::clone(&mock)));

        let output = super::super::native_map::NativeMapOutput::new(map.lock());
        assert!(output.is_ok());
        let Ok(output) = output else {
            return;
        };
        assert!(
            session
                .subscribe_node_handler_map("handler-handoff".into(), output, None)
                .is_ok()
        );
        wait_for_node_frames(&mock, 2).await;

        let frames = mock.node_frames();
        assert!(matches!(
            frames.first(),
            Some(myko_wire::NodeFrame::HandlerState { state, .. })
                if state.row_keys.as_deref() == Some(&["a".to_owned()])
        ));
        assert!(
            frames.iter().any(|frame| matches!(
                frame,
                myko_wire::NodeFrame::HandlerViewDelta { delta, .. }
                    if delta.deletes == ["a"]
            )),
            "the deletion during initial frame delivery was lost"
        );
    }

    #[tokio::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn node_handler_map_uses_the_retained_subscription_owner() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(Arc::clone(&mock));
        let mut session = ClientSession::new("client-1".into(), writer);
        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));

        let output = super::super::native_map::NativeMapOutput::new(cellmap);
        assert!(output.is_ok());
        let Ok(output) = output else {
            return;
        };
        assert!(
            session
                .subscribe_node_handler_map("handler-1".into(), output, None)
                .is_ok()
        );
        wait_for_node_frames(&mock, 1).await;
        store.remove(&"a".into());
        wait_for_node_frame(&mock, |frame| {
            matches!(
                frame,
                myko_wire::NodeFrame::HandlerViewDelta { delta, .. }
                    if delta.deletes == ["a"]
            )
        })
        .await;

        let frames = mock.node_frames();
        assert!(matches!(
            frames.first(),
            Some(myko_wire::NodeFrame::HandlerState { .. })
        ));
        assert!(frames.iter().any(|frame| matches!(
            frame,
            myko_wire::NodeFrame::HandlerViewDelta { delta, .. }
                if delta.deletes == ["a"]
        )));
        assert_eq!(session.subscription_count(), 1);
    }

    #[tokio::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn retained_handler_waits_for_target_cut_and_preserves_liveness() {
        let row = make_entity("a", "Alice");
        let value = BTreeMap::from([(Arc::<str>::from("a"), row)]);
        let (writer, live) =
            myko_federation::live_subscription(myko_federation::LiveSubscriptionState {
                value: Some(value.clone()),
                through: Some(myko_federation::LogPosition::new(1)),
                liveness: myko_federation::SubscriptionLiveness::Current,
            });
        let output = super::super::native_map::NativeMapOutput::from_retained(live);
        let mock = Arc::new(MockWriter::new());
        let mut session =
            ClientSession::new("retained-cut".into(), ArcMockWriter(Arc::clone(&mock)));
        assert!(
            session
                .subscribe_node_handler_map(
                    "retained-cut".into(),
                    output,
                    Some(myko_federation::LogPosition::new(2)),
                )
                .is_ok()
        );
        let stale_was_sent = tokio::time::timeout(std::time::Duration::from_millis(20), async {
            while mock.node_frames().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(stale_was_sent.is_err());
        assert!(mock.node_frames().is_empty());

        writer.replace(myko_federation::LiveSubscriptionState {
            value: Some(value),
            through: Some(myko_federation::LogPosition::new(2)),
            liveness: myko_federation::SubscriptionLiveness::Resynchronizing {
                reason: "selected history is incomplete".to_owned(),
            },
        });
        wait_for_node_frames(&mock, 1).await;
        let frames = mock.node_frames();
        assert!(matches!(
            frames.first(),
            Some(myko_wire::NodeFrame::HandlerState { state, .. })
                if state.through == Some(serde_json::json!(2))
                    && matches!(
                        state.liveness,
                        myko_federation::SubscriptionLiveness::Resynchronizing { .. }
                    )
                    && state.row_keys.as_deref() == Some(&["a".to_owned()])
        ));
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn wait_for_node_frames(mock: &MockWriter, count: usize) {
        let started = std::time::Instant::now();
        while mock.node_frames().len() < count {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(1),
                "native handler frames did not arrive"
            );
            tokio::task::yield_now().await;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn wait_for_node_frame(
        mock: &MockWriter,
        matches_frame: impl Fn(&myko_wire::NodeFrame) -> bool,
    ) {
        let started = std::time::Instant::now();
        while !mock.node_frames().iter().any(&matches_frame) {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(1),
                "native handler frame did not arrive"
            );
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn test_cancel_subscription() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock);
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query("tx-1".into(), "query-1".into(), cellmap, None);
        assert_eq!(session.subscription_count(), 1);

        session.cancel(&"tx-1".into());
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_session_drop_cleanup() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        {
            let mock = Arc::new(MockWriter::new());
            let writer = ArcMockWriter(mock);
            let mut session = ClientSession::new("client-1".into(), writer);

            let cellmap1 = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
            let cellmap2 = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
            session.subscribe_query("tx-1".into(), "query-1".into(), cellmap1, None);
            session.subscribe_query("tx-2".into(), "query-2".into(), cellmap2, None);

            // 2 subscriptions active
            assert_eq!(session.subscription_count(), 2);
        }
        // Session dropped - subscriptions should be cleaned up
    }

    #[test]
    fn test_subscribe_by_id() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let id: Arc<str> = "a".into();
        let cellmap =
            hyphae::MapQuery::materialize((*store).clone().select(move |item| *item.id() == *id));
        session.subscribe_query("tx-1".into(), "query-1".into(), cellmap, None);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Update the entity
        store.insert("a".into(), make_entity("a", "Alice Updated"));
        mock.wait_for_message_count(2);
        assert!(mock.message_count() >= 2);
    }

    #[test]
    fn test_delete_sends_deletes_not_upserts() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_query("tx-1".into(), "query-1".into(), cellmap, None);

        let initial_count = mock.message_count();

        // Delete an entity
        store.remove(&"a".into());

        // Should have received a message with deletes
        assert!(mock.message_count() > initial_count);

        // Find the delete message (it should be the last one)
        let last_msg = mock.last_message();
        assert!(last_msg.is_some(), "expected response message");
        let Some(last_msg) = last_msg else {
            return;
        };
        if let MykoMessage::QueryResponse(QueryResponse {
            deletes, upserts, ..
        }) = last_msg
        {
            // The delete message should have "a" in deletes and empty upserts
            assert!(
                deletes.iter().any(|id| id.as_ref() == "a"),
                "Delete should contain 'a'"
            );
            assert!(upserts.is_empty(), "Upserts should be empty for delete");
        } else {
            assert!(
                matches!(last_msg, MykoMessage::QueryResponse(_)),
                "Expected QueryResponse"
            );
        }
    }

    #[test]
    fn test_subscribe_view_respects_initial_window() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_view(
            "tx-view-1".into(),
            cellmap,
            Some(QueryWindow {
                offset: 0,
                limit: 1,
            }),
        );

        let msgs = mock.messages();
        let first = msgs.into_iter().find_map(|m| match m {
            MykoMessage::ViewResponse(r) => Some(r),
            _ => None,
        });
        assert!(first.is_some(), "expected at least one ViewResponse");
        let Some(resp) = first else {
            return;
        };

        assert_eq!(resp.upserts.len(), 1);
        assert_eq!(resp.deletes.len(), 0);
        assert_eq!(resp.total_count, Some(3));
        assert!(resp.window.is_some(), "expected window in response");
        let Some(window) = resp.window else {
            return;
        };
        assert_eq!(window.offset, 0);
        assert_eq!(window.limit, 1);
    }

    #[test]
    fn test_view_window_ignores_out_of_window_updates() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = hyphae::MapQuery::materialize((*store).clone().select(|_| true));
        session.subscribe_view(
            "tx-view-1".into(),
            cellmap,
            Some(QueryWindow {
                offset: 0,
                limit: 1,
            }),
        );

        // Initial window response
        let before = mock.message_count();
        assert!(before >= 1);

        // "c" is outside window [a] with sorted IDs.
        store.insert("c".into(), make_entity("c", "Charlie Updated"));

        // No visible/window/count change => no extra response.
        let after = mock.message_count();
        assert_eq!(after, before);
    }
}
