//! Client session management for WebSocket connections
//!
//! Each WebSocket connection gets a ClientSession that manages:
//! - Active subscriptions via SubscriptionGuards
//! - Message sending to the client
//! - Automatic cleanup on disconnect

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use hypha::{Cell, CellImmutable, Signal, SubscriptionGuard, Watchable};

use crate::{
    core::item::AnyItem,
    report::AnyOutput,
    wire::{
        MykoMessage, QueryChange, QueryResponse, QueryWindow, ReportError, ReportResponse,
        WrappedItem,
    },
};

/// Trait for sending WebSocket messages.
///
/// Implemented by the actual WebSocket writer to allow abstraction
/// and easier testing.
pub trait WsWriter: Send + Sync + 'static {
    /// Send a message to the client.
    fn send(&self, msg: MykoMessage);

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
}

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
    pub fn into_wire(self) -> QueryResponse {
        let upserts: Vec<WrappedItem<serde_json::Value>> = self
            .upsert_items
            .iter()
            .map(|item| WrappedItem {
                item: item.to_value(),
                item_type: item.entity_type().into(),
            })
            .collect();

        let mut changes: Vec<QueryChange> =
            Vec::with_capacity(upserts.len() + self.deletes.len() + usize::from(self.window_order_ids.is_some()));
        for item in &upserts {
            changes.push(QueryChange::Upsert { item: item.clone() });
        }
        for id in &self.deletes {
            changes.push(QueryChange::Delete { id: id.clone() });
        }
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

/// A WebSocket client session that manages subscriptions.
///
/// When dropped, all subscription guards are dropped, automatically
/// cleaning up all reactive subscriptions.
pub struct ClientSession<W: WsWriter> {
    /// Unique client identifier
    pub client_id: Arc<str>,
    /// WebSocket writer for sending messages
    writer: Arc<W>,
    /// Active subscriptions: tx -> entry
    subscriptions: HashMap<Arc<str>, SubscriptionEntry>,
}

enum SubscriptionEntry {
    Query(QuerySubscription),
    Guard { _guard: SubscriptionGuard },
}

struct QuerySubscription {
    _guard: SubscriptionGuard,
    state: Arc<Mutex<QuerySubscriptionState>>,
    kind: QuerySubscriptionKind,
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
    all_items: HashMap<Arc<str>, Arc<dyn AnyItem>>,
    visible_items: HashMap<Arc<str>, Arc<dyn AnyItem>>,
}

impl<W: WsWriter> ClientSession<W> {
    /// Create a new client session.
    pub fn new(client_id: Arc<str>, writer: W) -> Self {
        Self {
            client_id,
            writer: Arc::new(writer),
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a CellMap from a query cell factory.
    ///
    /// This is used by WsHandler when the query registration provides a cell factory.
    pub fn subscribe_query(
        &mut self,
        tx: Arc<str>,
        cell: hypha::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        let had_existing = self.subscriptions.contains_key(&tx);
        if had_existing {
            log::trace!(
                "ClientSession {} replacing existing query subscription tx={} (active_before={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
        }

        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let tx_for_log = tx_clone.clone();
        let state = Arc::new(Mutex::new(QuerySubscriptionState {
            window,
            ..Default::default()
        }));
        let state_for_diffs = state.clone();

        // subscribe_diffs sends Initial first, then subsequent diffs
        let guard = cell.subscribe_diffs(move |diff| {
            let response = match state_for_diffs.lock() {
                Ok(mut state) => state.apply_source_diff(diff, tx_clone.clone()),
                Err(_) => {
                    log::error!("Query subscription state poisoned for tx={}", tx_clone);
                    return;
                }
            };
            if let Some(response) = response {
                writer.send_query_response(response, false);
            }
        });

        self.subscriptions.insert(
            tx,
            SubscriptionEntry::Query(QuerySubscription {
                _guard: guard,
                state,
                kind: QuerySubscriptionKind::Query,
            }),
        );

        let active = self.subscriptions.len();
        log::trace!(
            "ClientSession {} subscribed query tx={} active_subscriptions={}",
            self.client_id,
            tx_for_log,
            active
        );
        if active >= 100 && active.is_multiple_of(100) {
            log::trace!(
                "ClientSession {} high subscription count: {} (most recent tx={})",
                self.client_id,
                active,
                tx_for_log
            );
        }
    }

    /// Subscribe to a CellMap from a view cell factory.
    pub fn subscribe_view(
        &mut self,
        tx: Arc<str>,
        cell: hypha::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        self.subscribe_view_with_id(tx, "unknown".into(), cell, window);
    }

    /// Subscribe to a CellMap from a view cell factory with explicit view id for perf logging.
    pub fn subscribe_view_with_id(
        &mut self,
        tx: Arc<str>,
        view_id: Arc<str>,
        cell: hypha::CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
        window: Option<QueryWindow>,
    ) {
        let writer = self.writer.clone();
        let tx_clone = tx.clone();
        let tx_for_log = tx_clone.clone();
        let client_id_for_log = self.client_id.clone();
        let view_id_for_log = view_id.clone();
        let subscribed_at = Instant::now();
        let state = Arc::new(Mutex::new(QuerySubscriptionState {
            window,
            ..Default::default()
        }));
        let state_for_diffs = state.clone();

        let guard = cell.subscribe_diffs(move |diff| {
            let response = match state_for_diffs.lock() {
                Ok(mut state) => state.apply_source_diff(diff, tx_clone.clone()),
                Err(_) => {
                    log::error!("View subscription state poisoned for tx={}", tx_clone);
                    return;
                }
            };
            let Some(response) = response else {
                return;
            };
            log::trace!(
                "ClientSession {} view tx={} seq={} upserts={} deletes={} changes={} window={:?} total_count={:?}",
                client_id_for_log,
                tx_clone,
                response.sequence,
                response.upsert_items.len(),
                response.deletes.len(),
                response.upsert_items.len()
                    + response.deletes.len()
                    + usize::from(response.window_order_ids.is_some()),
                response.window,
                response.total_count
            );
            if response.sequence == 0 {
                let first_emit_ms = subscribed_at.elapsed().as_millis();
                let payload_bytes =
                    serde_json::to_vec(&MykoMessage::ViewResponse(response.clone().into_wire()))
                    .map(|buf| buf.len())
                    .unwrap_or(0);
                log::trace!(
                    target: "myko_rs::server::view_perf",
                    "view_perf client={} view_id={} tx={} first_emit_ms={} initial_rows={} total_count={:?} payload_bytes={} window={:?}",
                    client_id_for_log,
                    view_id_for_log,
                    tx_clone,
                    first_emit_ms,
                    response.upsert_items.len(),
                    response.total_count,
                    payload_bytes,
                    response.window
                );
            }
            writer.send_query_response(response, true);
        });

        self.subscriptions.insert(
            tx,
            SubscriptionEntry::Query(QuerySubscription {
                _guard: guard,
                state,
                kind: QuerySubscriptionKind::View,
            }),
        );

        log::trace!(
            "ClientSession {} subscribed view view_id={} tx={} active_subscriptions={}",
            self.client_id,
            view_id,
            tx_for_log,
            self.subscriptions.len()
        );
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
            log::trace!(
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

        let guard = cell.subscribe(move |signal| match &signal {
            Signal::Value(output) => {
                writer.send_report_response(tx_clone.clone(), output.as_ref().clone());
            }
            Signal::Complete => {}
            Signal::Error(e) => {
                writer.send(MykoMessage::ReportError(ReportError {
                    tx: tx_clone.to_string(),
                    report_id: report_id.to_string(),
                    message: e.to_string(),
                }));
            }
        });

        self.subscriptions
            .insert(tx, SubscriptionEntry::Guard { _guard: guard });

        let active = self.subscriptions.len();
        log::trace!(
            "ClientSession {} subscribed report tx={} report_id={} active_subscriptions={}",
            self.client_id,
            tx_for_log,
            report_id_for_log,
            active
        );
        if active >= 100 && active.is_multiple_of(100) {
            log::trace!(
                "ClientSession {} high subscription count: {} (most recent report tx={}, id={})",
                self.client_id,
                active,
                tx_for_log,
                report_id_for_log
            );
        }
    }

    /// Update window for an active query subscription.
    pub fn update_query_window(&mut self, tx: &Arc<str>, window: Option<QueryWindow>) {
        let Some(SubscriptionEntry::Query(sub)) = self.subscriptions.get(tx) else {
            log::trace!(
                "ClientSession {} window update for unknown tx={} (active_subscriptions={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
            return;
        };

        let response = match sub.state.lock() {
            Ok(mut state) => state.apply_window_update(window, tx.clone()),
            Err(_) => {
                log::error!(
                    "Query subscription state poisoned on window update for tx={}",
                    tx
                );
                return;
            }
        };

        let Some(response) = response else {
            log::trace!(
                "ClientSession {} ignored no-op window update tx={} (active_subscriptions={})",
                self.client_id,
                tx,
                self.subscriptions.len()
            );
            return;
        };

        match sub.kind {
            QuerySubscriptionKind::Query => self.writer.send_query_response(response, false),
            QuerySubscriptionKind::View => self.writer.send_query_response(response, true),
        }
        log::trace!(
            "ClientSession {} updated query window tx={} (active_subscriptions={})",
            self.client_id,
            tx,
            self.subscriptions.len()
        );
    }

    /// Update window for an active view subscription.
    pub fn update_view_window(&mut self, tx: &Arc<str>, window: Option<QueryWindow>) {
        log::trace!(
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
        log::trace!(
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
        log::trace!(
            "ClientSession {} cancel_all removed_subscriptions={}",
            self.client_id,
            before
        );
    }

    /// Get the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if a subscription exists.
    pub fn has_subscription(&self, tx: &Arc<str>) -> bool {
        self.subscriptions.contains_key(tx)
    }
}

impl QuerySubscriptionState {
    fn apply_source_diff(
        &mut self,
        diff: &hypha::MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        if self.window.is_none() {
            return self.apply_source_diff_unwindowed(diff, tx);
        }

        let previous_total_count = self.all_items.len();
        let mut changed_ids: HashSet<Arc<str>> = HashSet::new();
        let mut removed_ids: HashSet<Arc<str>> = HashSet::new();

        match diff {
            hypha::MapDiff::Initial { entries } => {
                self.all_items.clear();
                for (id, item) in entries {
                    self.all_items.insert(id.clone(), item.clone());
                    changed_ids.insert(id.clone());
                }
            }
            hypha::MapDiff::Insert { key, value } => {
                self.all_items.insert(key.clone(), value.clone());
                changed_ids.insert(key.clone());
            }
            hypha::MapDiff::Update { key, new_value, .. } => {
                self.all_items.insert(key.clone(), new_value.clone());
                changed_ids.insert(key.clone());
            }
            hypha::MapDiff::Remove { key, .. } => {
                self.all_items.remove(key);
                removed_ids.insert(key.clone());
            }
            hypha::MapDiff::Batch { changes } => {
                let batch_size = changes.len();
                for change in changes {
                    match change {
                        hypha::MapDiff::Initial { entries } => {
                            self.all_items.clear();
                            for (id, item) in entries {
                                self.all_items.insert(id.clone(), item.clone());
                                changed_ids.insert(id.clone());
                            }
                        }
                        hypha::MapDiff::Insert { key, value } => {
                            self.all_items.insert(key.clone(), value.clone());
                            changed_ids.insert(key.clone());
                        }
                        hypha::MapDiff::Update { key, new_value, .. } => {
                            self.all_items.insert(key.clone(), new_value.clone());
                            changed_ids.insert(key.clone());
                        }
                        hypha::MapDiff::Remove { key, .. } => {
                            self.all_items.remove(key);
                            removed_ids.insert(key.clone());
                        }
                        hypha::MapDiff::Batch { .. } => {}
                    }
                }
                if batch_size >= 64 {
                    log::trace!(
                        "ClientSession tx={} apply_source_diff batch_size={} all_items={}",
                        tx,
                        batch_size,
                        self.all_items.len()
                    );
                }
            }
        }

        self.compute_windowed_response(tx, &changed_ids, &removed_ids, previous_total_count, false)
    }

    fn apply_source_diff_unwindowed(
        &mut self,
        diff: &hypha::MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
    ) -> Option<PendingQueryResponse> {
        let previous_total_count = self.all_items.len();
        let mut upsert_items: Vec<Arc<dyn AnyItem>> = Vec::new();
        let mut deletes: Vec<Arc<str>> = Vec::new();

        match diff {
            hypha::MapDiff::Initial { entries } => {
                self.all_items.clear();
                for (id, item) in entries {
                    self.all_items.insert(id.clone(), item.clone());
                    upsert_items.push(item.clone());
                }
            }
            hypha::MapDiff::Insert { key, value } => {
                self.all_items.insert(key.clone(), value.clone());
                upsert_items.push(value.clone());
            }
            hypha::MapDiff::Update { key, new_value, .. } => {
                self.all_items.insert(key.clone(), new_value.clone());
                upsert_items.push(new_value.clone());
            }
            hypha::MapDiff::Remove { key, .. } => {
                if self.all_items.remove(key).is_some() {
                    deletes.push(key.clone());
                }
            }
            hypha::MapDiff::Batch { changes } => {
                for change in changes {
                    match change {
                        hypha::MapDiff::Initial { entries } => {
                            self.all_items.clear();
                            for (id, item) in entries {
                                self.all_items.insert(id.clone(), item.clone());
                                upsert_items.push(item.clone());
                            }
                        }
                        hypha::MapDiff::Insert { key, value } => {
                            self.all_items.insert(key.clone(), value.clone());
                            upsert_items.push(value.clone());
                        }
                        hypha::MapDiff::Update { key, new_value, .. } => {
                            self.all_items.insert(key.clone(), new_value.clone());
                            upsert_items.push(new_value.clone());
                        }
                        hypha::MapDiff::Remove { key, .. } => {
                            if self.all_items.remove(key).is_some() {
                                deletes.push(key.clone());
                            }
                        }
                        hypha::MapDiff::Batch { .. } => {}
                    }
                }
            }
        }

        let total_count = self.all_items.len();
        let total_count_changed = previous_total_count != total_count;
        let visible_changed = !upsert_items.is_empty() || !deletes.is_empty();
        let should_emit = self.sequence == 0 || visible_changed || total_count_changed;

        log::trace!(
            "ClientSession tx={} window_decision force_emit=false seq={} changed_ids={} upserts={} deletes={} visible_changed={} window_order_changed=false total_count_changed={} should_emit={} total_count={} window=None",
            tx,
            self.sequence,
            upsert_items.len(),
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

        self.window = window;
        self.compute_windowed_response(
            tx,
            &HashSet::new(),
            &HashSet::new(),
            self.all_items.len(),
            false,
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
        if self.window.is_none() {
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

            let mut upsert_items: Vec<Arc<dyn AnyItem>> = Vec::new();
            if self.sequence == 0 {
                let mut ids: Vec<Arc<str>> = self.all_items.keys().cloned().collect();
                ids.sort_unstable();
                for id in ids {
                    if let Some(item) = self.all_items.get(id.as_ref()) {
                        upsert_items.push(item.clone());
                    }
                }
            } else {
                let mut ids: Vec<Arc<str>> = changed_ids.iter().cloned().collect();
                ids.sort_unstable();
                for id in ids {
                    if let Some(item) = self.all_items.get(id.as_ref()) {
                        upsert_items.push(item.clone());
                    }
                }
            }

            let total_count = self.all_items.len();
            let window_order_changed = false;
            let total_count_changed = previous_total_count != total_count;
            let visible_changed = !upsert_items.is_empty() || !deletes.is_empty();
            let should_emit = force_emit
                || self.sequence == 0
                || visible_changed
                || total_count_changed;

            log::trace!(
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

            if !should_emit {
                return None;
            }

            let seq = self.sequence;
            self.sequence = self.sequence.saturating_add(1);

            return Some(PendingQueryResponse {
                tx,
                sequence: seq,
                upsert_items,
                deletes,
                total_count,
                window: None,
                window_order_ids: None,
            });
        }

        let mut ordered_ids: Vec<Arc<str>> = self.all_items.keys().cloned().collect();
        ordered_ids.sort_unstable();

        let visible_ids: Vec<Arc<str>> = if let Some(window) = &self.window {
            if window.limit == 0 {
                Vec::new()
            } else {
                let start = window.offset.min(ordered_ids.len());
                let end = start.saturating_add(window.limit).min(ordered_ids.len());
                ordered_ids[start..end].to_vec()
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

        log::trace!(
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

        let seq = self.sequence;
        self.sequence = self.sequence.saturating_add(1);

        Some(PendingQueryResponse {
            tx,
            sequence: seq,
            upsert_items,
            deletes,
            total_count,
            window: self.window.clone(),
            window_order_ids: self.window.as_ref().map(|_| visible_ids),
        })
    }
}

impl<W: WsWriter> Drop for ClientSession<W> {
    fn drop(&mut self) {
        // All guards drop automatically
        log::trace!(
            "ClientSession dropped for client {}, cleaning up {} subscriptions",
            self.client_id,
            self.subscriptions.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use hypha::SelectExt;
    use serde_json::Value;

    use super::*;
    use crate::{
        common::{to_value::ToValue, with_id::WithId},
        store::StoreRegistry,
    };

    // Mock writer that collects messages
    struct MockWriter {
        messages: Mutex<Vec<MykoMessage>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                messages: Mutex::new(Vec::new()),
            }
        }

        fn message_count(&self) -> usize {
            self.messages.lock().unwrap().len()
        }

        fn last_message(&self) -> Option<MykoMessage> {
            self.messages.lock().unwrap().last().cloned()
        }

        fn messages(&self) -> Vec<MykoMessage> {
            self.messages.lock().unwrap().clone()
        }
    }

    impl WsWriter for MockWriter {
        fn send(&self, msg: MykoMessage) {
            self.messages.lock().unwrap().push(msg);
        }
    }

    // Need Arc wrapper for test
    struct ArcMockWriter(Arc<MockWriter>);

    impl WsWriter for ArcMockWriter {
        fn send(&self, msg: MykoMessage) {
            self.0.send(msg);
        }
    }

    // Test entity
    #[derive(Debug, Clone, PartialEq)]
    struct TestEntity {
        id: Arc<str>,
        name: String,
    }

    impl WithId for TestEntity {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl ToValue for TestEntity {
        fn to_value(&self) -> Value {
            serde_json::json!({
                "id": self.id.as_ref(),
                "name": self.name
            })
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
                .map(|typed| self == typed)
                .unwrap_or(false)
        }
    }

    fn make_entity(id: &str, name: &str) -> Arc<dyn AnyItem> {
        Arc::new(TestEntity {
            id: id.into(),
            name: name.to_string(),
        }) as Arc<dyn AnyItem>
    }

    #[test]
    fn test_subscribe_query_cellmap() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap, None);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Add an entity
        store.insert("c".into(), make_entity("c", "Charlie"));
        assert!(mock.message_count() >= 2);
    }

    #[test]
    fn test_cancel_subscription() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap, None);
        assert_eq!(session.subscription_count(), 1);

        session.cancel(&"tx-1".into());
        assert_eq!(session.subscription_count(), 0);
    }

    #[test]
    fn test_session_drop_cleanup() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        {
            let mock = Arc::new(MockWriter::new());
            let writer = ArcMockWriter(mock.clone());
            let mut session = ClientSession::new("client-1".into(), writer);

            let cellmap1 = store.select(|_| true);
            let cellmap2 = store.select(|_| true);
            session.subscribe_query("tx-1".into(), cellmap1, None);
            session.subscribe_query("tx-2".into(), cellmap2, None);

            // 2 subscriptions active
            assert_eq!(session.subscription_count(), 2);
        }
        // Session dropped - subscriptions should be cleaned up
    }

    #[test]
    fn test_subscribe_by_id() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let id: Arc<str> = "a".into();
        let cellmap = store.select(move |item| *item.id() == *id);
        session.subscribe_query("tx-1".into(), cellmap, None);

        // Should have received initial data
        assert!(mock.message_count() >= 1);

        // Update the entity
        store.insert("a".into(), make_entity("a", "Alice Updated"));
        assert!(mock.message_count() >= 2);
    }

    #[test]
    fn test_delete_sends_deletes_not_upserts() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
        session.subscribe_query("tx-1".into(), cellmap, None);

        let initial_count = mock.message_count();

        // Delete an entity
        store.remove(&"a".into());

        // Should have received a message with deletes
        assert!(mock.message_count() > initial_count);

        // Find the delete message (it should be the last one)
        let last_msg = mock.last_message().unwrap();
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
            panic!("Expected QueryResponse");
        }
    }

    #[test]
    fn test_subscribe_view_respects_initial_window() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
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
        let Some(resp) = first else {
            panic!("expected at least one ViewResponse");
        };

        assert_eq!(resp.upserts.len(), 1);
        assert_eq!(resp.deletes.len(), 0);
        assert_eq!(resp.total_count, Some(3));
        let Some(window) = resp.window else {
            panic!("expected window in response");
        };
        assert_eq!(window.offset, 0);
        assert_eq!(window.limit, 1);
    }

    #[test]
    fn test_view_window_ignores_out_of_window_updates() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Entity");
        store.insert("a".into(), make_entity("a", "Alice"));
        store.insert("b".into(), make_entity("b", "Bob"));
        store.insert("c".into(), make_entity("c", "Charlie"));

        let mock = Arc::new(MockWriter::new());
        let writer = ArcMockWriter(mock.clone());
        let mut session = ClientSession::new("client-1".into(), writer);

        let cellmap = store.select(|_| true);
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
