//! Opt-in per-connection delivery attribution with bounded payload inspection.
//! Never logs payload values.

use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use myko::wire::{MykoMessage, QueryChange, QueryResponse};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, UpDownCounter},
};
use serde_json::Value;

const MAX_INSPECTED_ID_BYTES: usize = 1024;
const MAX_PROPERTIES: usize = 4096;
const MAX_CLIENT_PROPERTIES: usize = 65_536;
const WINDOW: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Operation {
    kind: &'static str,
    id: String,
}

struct Subscription {
    operation: Operation,
    parameters: u64,
    ready: bool,
    requested: Instant,
    previous: Option<HashMap<u64, u64>>,
    last_sent: Option<Instant>,
    last_produced: Option<Instant>,
    items: HashMap<String, HashMap<u64, u64>>,
}

#[derive(Default)]
struct Counts {
    produced: u64,
    enqueue_failed: u64,
    messages: u64,
    bytes: u64,
    unchanged: u64,
    properties: u64,
    changed: u64,
    removed: u64,
    incomplete: u64,
    upserts: u64,
    deletes: u64,
    unique: HashSet<(u64, u64)>,
    max_produced_gap_ms: f64,
    max_gap_ms: f64,
}

struct State {
    subscriptions: HashMap<String, Subscription>,
    counts: HashMap<Operation, Counts>,
    retained_properties: usize,
    window: Instant,
    queue_peak: usize,
    queue_wait_ms: f64,
    materialize_ms: f64,
    serialize_ms: f64,
    send_ms: f64,
    failures: u64,
    untracked: u64,
}

/// Kept per connection, so cancellation/disconnect releases all retained fingerprints.
pub struct DeliveryDiagnostics {
    client_id: String,
    peer: SocketAddr,
    state: Option<Mutex<State>>,
}

struct Metrics {
    connections: UpDownCounter<i64>,
    opened: Counter<u64>,
    closed: Counter<u64>,
    pending: Gauge<u64>,
    untracked: Counter<u64>,
    failures: Counter<u64>,
    upserts: Counter<u64>,
    deletes: Counter<u64>,
    inspection: Histogram<f64>,
    active: Gauge<u64>,
    equivalent: Gauge<u64>,
    queue_peak: Gauge<u64>,
    produced: Counter<u64>,
    enqueue_failed: Counter<u64>,
    messages: Counter<u64>,
    bytes: Counter<u64>,
    unchanged: Counter<u64>,
    properties: Counter<u64>,
    changed: Counter<u64>,
    removed: Counter<u64>,
    incomplete: Counter<u64>,
    queue_wait: Histogram<f64>,
    materialize: Histogram<f64>,
    serialize: Histogram<f64>,
    send: Histogram<f64>,
    produced_gap: Histogram<f64>,
    gap: Histogram<f64>,
    setup: Histogram<f64>,
}

fn logs_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED
        .get_or_init(|| std::env::var("MYKO_WS_DIAGNOSTICS_LOGS").is_ok_and(|value| value == "1"))
}

fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = opentelemetry::global::meter("myko-server");
        Metrics {
            connections: meter
                .i64_up_down_counter("myko.ws.delivery.connections")
                .build(),
            opened: meter
                .u64_counter("myko.ws.delivery.connections.opened")
                .build(),
            closed: meter
                .u64_counter("myko.ws.delivery.connections.closed")
                .build(),
            pending: meter
                .u64_gauge("myko.ws.delivery.subscriptions.not_ready")
                .build(),
            untracked: meter.u64_counter("myko.ws.delivery.untracked").build(),
            failures: meter.u64_counter("myko.ws.delivery.failures").build(),
            upserts: meter.u64_counter("myko.ws.delivery.upserts").build(),
            deletes: meter.u64_counter("myko.ws.delivery.deletes").build(),
            inspection: meter
                .f64_histogram("myko.ws.delivery.inspection")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            active: meter.u64_gauge("myko.ws.delivery.subscriptions").build(),
            equivalent: meter
                .u64_gauge("myko.ws.delivery.subscriptions.equivalent")
                .build(),
            queue_peak: meter.u64_gauge("myko.ws.delivery.queue_peak").build(),
            produced: meter.u64_counter("myko.ws.delivery.produced").build(),
            enqueue_failed: meter.u64_counter("myko.ws.delivery.enqueue_failed").build(),
            messages: meter.u64_counter("myko.ws.delivery.messages").build(),
            bytes: meter.u64_counter("myko.ws.delivery.bytes").build(),
            unchanged: meter.u64_counter("myko.ws.delivery.unchanged").build(),
            properties: meter.u64_counter("myko.ws.delivery.properties").build(),
            changed: meter
                .u64_counter("myko.ws.delivery.properties.changed")
                .build(),
            removed: meter
                .u64_counter("myko.ws.delivery.properties.removed")
                .build(),
            incomplete: meter
                .u64_counter("myko.ws.delivery.inspection.incomplete")
                .build(),
            queue_wait: meter
                .f64_histogram("myko.ws.delivery.queue_wait")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            materialize: meter
                .f64_histogram("myko.ws.delivery.materialize")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            serialize: meter
                .f64_histogram("myko.ws.delivery.serialize")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            send: meter
                .f64_histogram("myko.ws.delivery.send")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            produced_gap: meter
                .f64_histogram("myko.ws.delivery.produced_gap")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            gap: meter
                .f64_histogram("myko.ws.delivery.gap")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
            setup: meter
                .f64_histogram("myko.ws.delivery.subscription_setup")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
                ])
                .build(),
        }
    })
}

impl DeliveryDiagnostics {
    pub fn new(client_id: &str, peer: SocketAddr) -> Self {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        let enabled = *ENABLED
            .get_or_init(|| std::env::var("MYKO_WS_DIAGNOSTICS").is_ok_and(|value| value == "1"));
        Self::with_enabled(client_id, peer, enabled)
    }

    fn with_enabled(client_id: &str, peer: SocketAddr, enabled: bool) -> Self {
        let diagnostics = Self {
            client_id: client_id.into(),
            peer,
            state: enabled.then(|| {
                Mutex::new(State {
                    subscriptions: HashMap::new(),
                    counts: HashMap::new(),
                    retained_properties: 0,
                    window: Instant::now(),
                    queue_peak: 0,
                    queue_wait_ms: 0.0,
                    materialize_ms: 0.0,
                    serialize_ms: 0.0,
                    send_ms: 0.0,
                    failures: 0,
                    untracked: 0,
                })
            }),
        };
        if enabled {
            metrics().connections.add(1, &diagnostics.attributes());
            metrics().opened.add(1, &diagnostics.attributes());
        }
        diagnostics
    }

    fn attributes(&self) -> [KeyValue; 2] {
        [
            KeyValue::new("client_id", self.client_id.clone()),
            KeyValue::new("peer", self.peer.ip().to_string()),
        ]
    }

    pub fn inbound(&self, message: &MykoMessage) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        let request = match message {
            MykoMessage::Report(w) => Some(("report", w.report_id.as_str(), &w.report)),
            MykoMessage::Query(w) => Some(("query", w.query_id.as_ref(), &w.query)),
            MykoMessage::View(w) => Some(("view", w.view_id.as_ref(), &w.view)),
            MykoMessage::ReportCancel(w)
            | MykoMessage::QueryCancel(w)
            | MykoMessage::ViewCancel(w) => {
                if let Some(subscription) = state.subscriptions.remove(&w.tx) {
                    state.retained_properties = state
                        .retained_properties
                        .saturating_sub(subscription.retained());
                }
                None
            }
            _ => None,
        };
        if let Some((kind, id, payload)) = request {
            let Some(tx) = payload.get("tx").and_then(Value::as_str) else {
                return;
            };
            if kind != "report" && state.subscriptions.contains_key(tx) {
                return;
            }
            // Ignore transaction identity when detecting equivalent subscriptions.
            let mut hasher = DefaultHasher::new();
            if let Some(parameters) = payload.as_object() {
                for (key, value) in parameters {
                    if key != "tx" {
                        key.hash(&mut hasher);
                        value.hash(&mut hasher);
                    }
                }
            }
            match message {
                MykoMessage::Query(w) => {
                    if let Ok(window) = serde_json::to_value(&w.window) {
                        window.hash(&mut hasher);
                    }
                    w.query_item_type.hash(&mut hasher);
                }
                MykoMessage::View(w) => {
                    if let Ok(window) = serde_json::to_value(&w.window) {
                        window.hash(&mut hasher);
                    }
                    if let Ok(rate) = serde_json::to_value(w.sample_rate) {
                        rate.hash(&mut hasher);
                    }
                    w.view_item_type.hash(&mut hasher);
                }
                _ => {}
            }
            if let Some(previous) = state.subscriptions.insert(
                tx.into(),
                Subscription {
                    operation: Operation {
                        kind,
                        id: id.into(),
                    },
                    parameters: hasher.finish(),
                    ready: false,
                    requested: Instant::now(),
                    previous: None,
                    last_sent: None,
                    last_produced: None,
                    items: HashMap::new(),
                },
            ) {
                state.retained_properties = state
                    .retained_properties
                    .saturating_sub(previous.retained());
            }
        }
    }

    pub fn ready(&self, tx: &str) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        if let Some(subscription) = state.subscriptions.get_mut(tx) {
            subscription.ready = true;
            let mut attrs = self.attributes().to_vec();
            attrs.push(KeyValue::new(
                "operation",
                subscription.operation.id.clone(),
            ));
            attrs.push(KeyValue::new("kind", subscription.operation.kind));
            metrics()
                .setup
                .record(subscription.requested.elapsed().as_secs_f64(), &attrs);
        }
    }

    pub fn queue(&self, depth: usize, wait: Option<(&str, Duration)>) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        state.queue_peak = state.queue_peak.max(depth);
        if let Some((tx, wait)) = wait {
            state.queue_wait_ms = state.queue_wait_ms.max(wait.as_secs_f64() * 1000.0);
            let mut attrs = self.attributes().to_vec();
            if let Some(subscription) = state.subscriptions.get(tx) {
                attrs.push(KeyValue::new("kind", subscription.operation.kind));
                attrs.push(KeyValue::new(
                    "operation",
                    subscription.operation.id.clone(),
                ));
            }
            metrics().queue_wait.record(wait.as_secs_f64(), &attrs);
        }
    }

    pub fn materialized(&self, tx: &str, duration: Duration) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        state.materialize_ms = state.materialize_ms.max(duration.as_secs_f64() * 1000.0);
        let mut attrs = self.attributes().to_vec();
        if let Some(subscription) = state.subscriptions.get(tx) {
            attrs.push(KeyValue::new(
                "operation",
                subscription.operation.id.clone(),
            ));
            attrs.push(KeyValue::new("kind", subscription.operation.kind));
        }
        metrics().materialize.record(duration.as_secs_f64(), &attrs);
    }

    pub fn produced(&self, tx: &str, depth: usize, success: bool) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        state.queue_peak = state.queue_peak.max(depth);
        let now = Instant::now();
        let Some((operation, gap)) = state.subscriptions.get_mut(tx).map(|s| {
            (
                s.operation.clone(),
                s.last_produced
                    .replace(now)
                    .map(|last| now.duration_since(last)),
            )
        }) else {
            state.untracked = state.untracked.saturating_add(1);
            return;
        };
        if let Some(gap) = gap {
            let mut attrs = self.attributes().to_vec();
            attrs.push(KeyValue::new("kind", operation.kind));
            attrs.push(KeyValue::new("operation", operation.id.clone()));
            metrics().produced_gap.record(gap.as_secs_f64(), &attrs);
        }
        let counts = state.counts.entry(operation).or_default();
        if let Some(gap) = gap {
            counts.max_produced_gap_ms = counts.max_produced_gap_ms.max(gap.as_secs_f64() * 1000.0);
        }
        counts.produced = counts.produced.saturating_add(1);
        counts.enqueue_failed = counts.enqueue_failed.saturating_add(u64::from(!success));
    }

    pub fn serialized_command(&self, command_id: &str, bytes: usize) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        let operation = Operation {
            kind: "Command",
            id: command_id.into(),
        };
        let counts = state.counts.entry(operation).or_default();
        counts.messages = counts.messages.saturating_add(1);
        counts.bytes = counts.bytes.saturating_add(count(bytes));
    }

    pub fn sent(&self, message: &MykoMessage, bytes: usize) {
        let Some(state) = &self.state else {
            return;
        };
        let inspection_started = Instant::now();
        let Ok(mut state) = state.lock() else {
            return;
        };
        let tx = match message {
            MykoMessage::ReportResponse(response) => Some(response.tx.as_str()),
            MykoMessage::QueryResponse(response) | MykoMessage::ViewResponse(response) => {
                Some(response.tx.as_ref())
            }
            _ => None,
        };
        let mut retained = state.retained_properties;
        let mut inspection = Inspection::default();
        let mut gap = None;
        let operation =
            if let Some(subscription) = tx.and_then(|tx| state.subscriptions.get_mut(tx)) {
                let previous_retained = subscription.retained();
                let now = Instant::now();
                gap = subscription
                    .last_sent
                    .replace(now)
                    .map(|last| now.duration_since(last));
                // Large frames still contribute traffic/timing counters, but never
                // trigger expensive property inspection or retain old fingerprints.
                if bytes <= 262_144 {
                    inspection = subscription.inspect(message);
                    if retained
                        .saturating_sub(previous_retained)
                        .saturating_add(subscription.retained())
                        > MAX_CLIENT_PROPERTIES
                    {
                        inspection.complete = false;
                    }
                }
                if !inspection.complete {
                    subscription.previous = None;
                    subscription.items.clear();
                }
                retained = retained
                    .saturating_sub(previous_retained)
                    .saturating_add(subscription.retained());
                subscription.operation.clone()
            } else {
                Operation {
                    kind: crate::ws_timing::message_kind(message),
                    id: "unattributed".into(),
                }
            };
        state.retained_properties = retained;
        let unique_remaining = MAX_CLIENT_PROPERTIES
            .saturating_sub(state.counts.values().map(|c| c.unique.len()).sum());
        let counts = state.counts.entry(operation.clone()).or_default();
        counts.messages = counts.messages.saturating_add(1);
        counts.bytes = counts.bytes.saturating_add(count(bytes));
        counts.upserts = counts.upserts.saturating_add(inspection.upserts);
        counts.deletes = counts.deletes.saturating_add(inspection.deletes);
        if inspection.complete {
            counts.properties = counts.properties.saturating_add(inspection.properties);
            counts.changed = counts.changed.saturating_add(inspection.changed);
            counts.removed = counts.removed.saturating_add(inspection.removed);
            counts.unchanged = counts
                .unchanged
                .saturating_add(u64::from(inspection.unchanged));
            let stream = path_hash(0, tx);
            let mut unique_remaining = unique_remaining;
            for key in inspection.keys {
                if counts.unique.contains(&(stream, key)) {
                    continue;
                }
                if unique_remaining > 0 {
                    unique_remaining = unique_remaining.saturating_sub(1);
                    counts.unique.insert((stream, key));
                } else {
                    counts.incomplete = counts.incomplete.saturating_add(1);
                    break;
                }
            }
        } else if tx.is_some() {
            counts.incomplete = counts.incomplete.saturating_add(1);
        }
        if let Some(gap) = gap {
            counts.max_gap_ms = counts.max_gap_ms.max(gap.as_secs_f64() * 1000.0);
            let mut attrs = self.attributes().to_vec();
            attrs.push(KeyValue::new("kind", operation.kind));
            attrs.push(KeyValue::new("operation", operation.id));
            metrics().gap.record(gap.as_secs_f64(), &attrs);
        }
        metrics().inspection.record(
            inspection_started.elapsed().as_secs_f64(),
            &self.attributes(),
        );
    }

    pub fn timing(
        &self,
        message: Option<&MykoMessage>,
        command_id: Option<&str>,
        serialize: Duration,
        send: Duration,
        success: bool,
    ) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        state.serialize_ms = state.serialize_ms.max(serialize.as_secs_f64() * 1000.0);
        state.send_ms = state.send_ms.max(send.as_secs_f64() * 1000.0);
        state.failures = state.failures.saturating_add(u64::from(!success));
        let mut attrs = self.attributes().to_vec();
        if let Some(message) = message {
            let tx = match message {
                MykoMessage::ReportResponse(response) => Some(response.tx.as_str()),
                MykoMessage::QueryResponse(response) | MykoMessage::ViewResponse(response) => {
                    Some(response.tx.as_ref())
                }
                _ => None,
            };
            if let Some(subscription) = tx.and_then(|tx| state.subscriptions.get(tx)) {
                attrs.push(KeyValue::new(
                    "operation",
                    subscription.operation.id.clone(),
                ));
                attrs.push(KeyValue::new("kind", subscription.operation.kind));
            }
        } else if let Some(command_id) = command_id {
            attrs.push(KeyValue::new("operation", command_id.to_string()));
            attrs.push(KeyValue::new("kind", "Command"));
        }
        metrics().serialize.record(serialize.as_secs_f64(), &attrs);
        metrics().send.record(send.as_secs_f64(), &attrs);
    }

    pub fn flush(&self, force: bool) {
        let Some(state) = &self.state else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            return;
        };
        let elapsed = state.window.elapsed();
        if !force && elapsed < WINDOW {
            return;
        }
        let mut equivalent = HashSet::new();
        let mut duplicates = 0_usize;
        let mut subscriptions_by_operation: HashMap<Operation, (usize, usize)> = HashMap::new();
        let mut active = 0_usize;
        for subscription in state.subscriptions.values().filter(|s| s.ready) {
            active = active.saturating_add(1);
            let counts = subscriptions_by_operation
                .entry(subscription.operation.clone())
                .or_default();
            counts.0 = counts.0.saturating_add(1);
            if !equivalent.insert((&subscription.operation, subscription.parameters)) {
                duplicates = duplicates.saturating_add(1);
                counts.1 = counts.1.saturating_add(1);
            }
        }
        if logs_enabled() {
            tracing::debug!(target: "myko_server::delivery", client_id = %self.client_id, peer = %self.peer,
            window_s = elapsed.as_secs_f64(), active_subscriptions = active,
            pending_subscriptions = state.subscriptions.len().saturating_sub(active),
            equivalent_subscriptions = duplicates, queue_peak = state.queue_peak,
            max_queue_wait_ms = state.queue_wait_ms, max_serialize_ms = state.serialize_ms,
            max_materialize_ms = state.materialize_ms, max_send_ms = state.send_ms, failures = state.failures, untracked = state.untracked,
            "client delivery summary");
        }
        let attrs = self.attributes();
        metrics().pending.record(
            count(state.subscriptions.len().saturating_sub(active)),
            &attrs,
        );
        metrics().untracked.add(state.untracked, &attrs);
        metrics().failures.add(state.failures, &attrs);
        metrics().active.record(count(active), &attrs);
        metrics().equivalent.record(count(duplicates), &attrs);
        metrics().queue_peak.record(count(state.queue_peak), &attrs);
        let mut operations: Vec<_> = state.counts.drain().collect();
        operations.sort_unstable_by_key(|(_, counts)| std::cmp::Reverse(counts.bytes));
        for (rank, (operation, counts)) in operations.into_iter().enumerate() {
            let subscriptions = subscriptions_by_operation
                .get(&operation)
                .copied()
                .unwrap_or_default();
            let mut attrs = self.attributes().to_vec();
            attrs.push(KeyValue::new("kind", operation.kind));
            attrs.push(KeyValue::new("operation", operation.id.clone()));
            let metrics = metrics();
            metrics.produced.add(counts.produced, &attrs);
            metrics.enqueue_failed.add(counts.enqueue_failed, &attrs);
            metrics.upserts.add(counts.upserts, &attrs);
            metrics.deletes.add(counts.deletes, &attrs);
            metrics.messages.add(counts.messages, &attrs);
            metrics.bytes.add(counts.bytes, &attrs);
            metrics.unchanged.add(counts.unchanged, &attrs);
            metrics.properties.add(counts.properties, &attrs);
            metrics.changed.add(counts.changed, &attrs);
            metrics.removed.add(counts.removed, &attrs);
            metrics.incomplete.add(counts.incomplete, &attrs);
            if logs_enabled() && rank < 5 {
                tracing::debug!(target: "myko_server::delivery", client_id = %self.client_id, peer = %self.peer,
                kind = operation.kind, operation = %operation.id, window_s = elapsed.as_secs_f64(),
                active_subscriptions = subscriptions.0, equivalent_subscriptions = subscriptions.1,
                produced = counts.produced, enqueue_failed = counts.enqueue_failed,
                messages = counts.messages, bytes = counts.bytes, unchanged = counts.unchanged,
                property_occurrences = counts.properties, changed_properties = counts.changed,
                removed_properties = counts.removed, unique_properties = counts.unique.len(),
                upserts = counts.upserts, deletes = counts.deletes,
                incomplete_inspections = counts.incomplete, max_produced_gap_ms = counts.max_produced_gap_ms, max_gap_ms = counts.max_gap_ms,
                "operation delivery summary");
            }
        }
        state.window = Instant::now();
        state.queue_peak = 0;
        state.queue_wait_ms = 0.0;
        state.materialize_ms = 0.0;
        state.serialize_ms = 0.0;
        state.send_ms = 0.0;
        state.failures = 0;
        state.untracked = 0;
    }
}

impl Drop for DeliveryDiagnostics {
    fn drop(&mut self) {
        if self.state.is_some() {
            self.flush(true);
            metrics().pending.record(0, &self.attributes());
            metrics().active.record(0, &self.attributes());
            metrics().equivalent.record(0, &self.attributes());
            metrics().queue_peak.record(0, &self.attributes());
            metrics().connections.add(-1, &self.attributes());
            metrics().closed.add(1, &self.attributes());
        }
    }
}

#[derive(Default)]
struct Inspection {
    complete: bool,
    unchanged: bool,
    properties: u64,
    changed: u64,
    removed: u64,
    upserts: u64,
    deletes: u64,
    keys: Vec<u64>,
}

impl Subscription {
    fn retained(&self) -> usize {
        self.previous
            .as_ref()
            .map_or(0, HashMap::len)
            .saturating_add(self.items.values().map(HashMap::len).sum::<usize>())
    }

    fn inspect(&mut self, message: &MykoMessage) -> Inspection {
        let mut inspection = Inspection {
            complete: true,
            unchanged: true,
            ..Inspection::default()
        };
        match message {
            MykoMessage::ReportResponse(response) => {
                let mut leaves = HashMap::new();
                inspection.complete = fingerprint_leaves(&response.response, 0, 0, &mut leaves);
                inspection.compare(self.previous.as_ref(), &leaves, 0);
                self.previous = Some(leaves);
            }
            MykoMessage::QueryResponse(response) | MykoMessage::ViewResponse(response) => {
                // Sequence zero is a fresh snapshot, even when a tx is reused.
                if response.sequence == 0 {
                    self.items.clear();
                    inspection.unchanged = false;
                }
                inspection.query(response, &mut self.items);
            }
            _ => inspection.complete = false,
        }
        inspection
    }
}

impl Inspection {
    fn compare(
        &mut self,
        previous: Option<&HashMap<u64, u64>>,
        current: &HashMap<u64, u64>,
        item: u64,
    ) {
        self.properties = self.properties.saturating_add(count(current.len()));
        self.changed = self.changed.saturating_add(count(
            current
                .iter()
                .filter(|(key, value)| previous.and_then(|p| p.get(key)) != Some(value))
                .count(),
        ));
        self.removed = self.removed.saturating_add(count(previous.map_or(0, |p| {
            p.keys().filter(|key| !current.contains_key(key)).count()
        })));
        self.unchanged &= previous.is_some() && previous == Some(current);
        self.keys
            .extend(current.keys().map(|key| path_hash(item, key)));
    }

    fn query(&mut self, response: &QueryResponse, items: &mut HashMap<String, HashMap<u64, u64>>) {
        // Ordered changes and legacy upserts/deletes represent the same data.
        // Inspect only the authoritative representation to avoid double counting.
        if response.changes.is_empty() {
            for item in &response.upserts {
                self.upsert(item, items);
                if !self.complete {
                    return;
                }
            }
            for id in &response.deletes {
                self.delete(id, items);
            }
        } else {
            for change in &response.changes {
                match change {
                    QueryChange::Upsert { item } => self.upsert(item, items),
                    QueryChange::Delete { id } => self.delete(id, items),
                    QueryChange::WindowOrder { .. } => {
                        self.unchanged = false;
                    }
                }
                if !self.complete {
                    return;
                }
            }
        }
    }

    fn upsert(
        &mut self,
        item: &myko::wire::ErasedWrappedItem,
        items: &mut HashMap<String, HashMap<u64, u64>>,
    ) {
        self.upserts = self.upserts.saturating_add(1);
        let id = item.item.id().to_string();
        if id.len() > MAX_INSPECTED_ID_BYTES {
            self.complete = false;
            return;
        }
        let Ok(value) = serde_json::to_value(&item.item) else {
            self.complete = false;
            return;
        };
        let mut leaves = HashMap::new();
        if !fingerprint_leaves(&value, 0, 0, &mut leaves)
            || self.keys.len().saturating_add(leaves.len()) > MAX_PROPERTIES
            || (items.len() >= MAX_PROPERTIES && !items.contains_key(&id))
        {
            self.complete = false;
            return;
        }
        self.compare(items.get(&id), &leaves, path_hash(0, &id));
        items.insert(id, leaves);
    }

    fn delete(&mut self, id: &str, items: &mut HashMap<String, HashMap<u64, u64>>) {
        self.deletes = self.deletes.saturating_add(1);
        if let Some(previous) = items.remove(id) {
            self.removed = self.removed.saturating_add(count(previous.len()));
            self.unchanged = false;
        }
    }
}

// Hash paths and values, retaining no field names or values. Bound depth and work
// as well as retained memory: telemetry must not become the delivery bottleneck.
fn fingerprint_leaves(
    value: &Value,
    path: u64,
    depth: usize,
    leaves: &mut HashMap<u64, u64>,
) -> bool {
    if depth > 64 || leaves.len() >= MAX_PROPERTIES {
        return false;
    }
    match value {
        Value::Object(object) if !object.is_empty() => {
            for (key, value) in object {
                if !fingerprint_leaves(value, path_hash(path, key), depth.saturating_add(1), leaves)
                {
                    return false;
                }
            }
        }
        Value::Array(array) if !array.is_empty() => {
            for (index, value) in array.iter().enumerate() {
                if !fingerprint_leaves(
                    value,
                    path_hash(path, index),
                    depth.saturating_add(1),
                    leaves,
                ) {
                    return false;
                }
            }
        }
        _ => {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            leaves.insert(path, hasher.finish());
        }
    }
    true
}

fn path_hash(parent: u64, key: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    parent.hash(&mut hasher);
    key.hash(&mut hasher);
    hasher.finish()
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(
    clippy::significant_drop_tightening,
    clippy::indexing_slicing,
    clippy::expect_used
)]
mod tests {
    use super::*;
    use myko::wire::{CancelSubscription, ReportResponse, WrappedReport};
    use serde_json::json;

    fn diagnostics() -> DeliveryDiagnostics {
        DeliveryDiagnostics::with_enabled(
            "client-a",
            "127.0.0.1:1234"
                .parse()
                .expect("valid diagnostic test fixture"),
            true,
        )
    }

    fn subscribe(diagnostics: &DeliveryDiagnostics, tx: &str) {
        diagnostics.inbound(&MykoMessage::Report(WrappedReport {
            report_id: "LiveValues".into(),
            report: json!({"tx": tx, "scene": "scene-a"}),
        }));
        diagnostics.ready(tx);
    }

    fn report(tx: &str, value: Value) -> MykoMessage {
        MykoMessage::ReportResponse(ReportResponse {
            tx: tx.into(),
            response: value,
        })
    }

    #[test]
    fn reports_count_changed_leaves_repeated_values_and_removals_per_subscription() {
        let diagnostics = diagnostics();
        subscribe(&diagnostics, "one");
        subscribe(&diagnostics, "two");
        diagnostics.sent(&report("one", json!({"x": 1, "nested": {"y": 2}})), 50);
        diagnostics.sent(&report("one", json!({"x": 1, "nested": {"y": 2}})), 50);
        diagnostics.sent(&report("one", json!({"x": 1, "nested": {"y": 3}})), 50);
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        diagnostics.sent(&report("two", json!({"x": 1})), 20);
        let state = diagnostics
            .state
            .as_ref()
            .expect("valid diagnostic test fixture")
            .lock()
            .expect("valid diagnostic test fixture");
        let counts = state
            .counts
            .values()
            .next()
            .expect("valid diagnostic test fixture");
        assert_eq!(counts.messages, 5);
        assert_eq!(counts.properties, 8);
        assert_eq!(counts.changed, 4);
        assert_eq!(counts.removed, 1);
        assert_eq!(counts.unchanged, 1);
        assert_eq!(counts.unique.len(), 3);
    }

    #[test]
    fn equivalent_requests_ignore_transaction_identity_and_cancel_clears_baseline() {
        let diagnostics = diagnostics();
        subscribe(&diagnostics, "one");
        subscribe(&diagnostics, "two");
        {
            let state = diagnostics
                .state
                .as_ref()
                .expect("valid diagnostic test fixture")
                .lock()
                .expect("valid diagnostic test fixture");
            assert_eq!(
                state.subscriptions["one"].parameters,
                state.subscriptions["two"].parameters
            );
        }
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        diagnostics.inbound(&MykoMessage::ReportCancel(CancelSubscription {
            tx: "one".into(),
        }));
        subscribe(&diagnostics, "one");
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        let state = diagnostics
            .state
            .as_ref()
            .expect("valid diagnostic test fixture")
            .lock()
            .expect("valid diagnostic test fixture");
        assert_eq!(
            state
                .counts
                .values()
                .next()
                .expect("valid diagnostic test fixture")
                .unchanged,
            0
        );
    }

    #[test]
    fn inspection_caps_are_explicit_and_do_not_claim_unchanged_after_a_gap() {
        let diagnostics = diagnostics();
        subscribe(&diagnostics, "one");
        diagnostics.sent(
            &report("one", json!(vec![1; MAX_PROPERTIES.saturating_add(1)])),
            20_000,
        );
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        diagnostics.sent(&report("one", json!({"x": 1})), 300_000);
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        let state = diagnostics
            .state
            .as_ref()
            .expect("valid diagnostic test fixture")
            .lock()
            .expect("valid diagnostic test fixture");
        let counts = state
            .counts
            .values()
            .next()
            .expect("valid diagnostic test fixture");
        assert_eq!(counts.messages, 4);
        assert_eq!(counts.incomplete, 2);
        assert_eq!(counts.unchanged, 0);
        assert_eq!(state.subscriptions["one"].retained(), 1);
    }

    fn query(sequence: u64, value: &Value) -> QueryResponse {
        // Use wire deserialization so this also exercises erased-item serialization.
        serde_json::from_value(json!({
            "tx": "query", "sequence": sequence, "deletes": [], "upserts": [],
            "changes": [{"kind": "upsert", "item": {"itemType": "DiagnosticFixture", "item": value}}]
        })).expect("valid diagnostic test fixture")
    }

    #[test]
    fn query_diffs_compare_item_state_and_do_not_count_legacy_mirrors_twice() {
        let mut subscription = Subscription {
            operation: Operation {
                kind: "view",
                id: "Properties".into(),
            },
            parameters: 0,
            ready: true,
            requested: Instant::now(),
            previous: None,
            last_sent: None,
            last_produced: None,
            items: HashMap::new(),
        };
        let mut first = query(0, &json!({"id": "a", "x": 1}));
        if let QueryChange::Upsert { item } = &first.changes[0] {
            first.upserts.push(item.clone());
        }
        let initial = subscription.inspect(&MykoMessage::ViewResponse(first));
        assert_eq!(initial.upserts, 1);
        assert_eq!(initial.properties, 2);
        assert_eq!(initial.changed, 2);
        let repeated = subscription.inspect(&MykoMessage::ViewResponse(query(
            1,
            &json!({"id": "a", "x": 1}),
        )));
        assert!(repeated.unchanged);
        let changed = subscription.inspect(&MykoMessage::ViewResponse(query(
            2,
            &json!({"id": "a", "x": 2}),
        )));
        assert_eq!(changed.changed, 1);
        let mut deletion = query(3, &json!({"id": "a", "x": 2}));
        deletion.changes = vec![QueryChange::Delete { id: "a".into() }];
        let deleted = subscription.inspect(&MykoMessage::ViewResponse(deletion));
        assert_eq!(deleted.deletes, 1);
        assert_eq!(deleted.removed, 2);
        assert!(!deleted.unchanged);
        assert!(subscription.items.is_empty());
    }

    #[test]
    fn produced_and_sent_are_distinct_and_disabled_mode_retains_nothing() {
        let diagnostics = diagnostics();
        subscribe(&diagnostics, "one");
        diagnostics.produced("one", 9, true);
        diagnostics.produced("one", 10, false);
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        {
            let state = diagnostics
                .state
                .as_ref()
                .expect("valid diagnostic test fixture")
                .lock()
                .expect("valid diagnostic test fixture");
            assert_eq!(state.queue_peak, 10);
            let counts = state
                .counts
                .values()
                .next()
                .expect("valid diagnostic test fixture");
            assert_eq!(
                (counts.produced, counts.enqueue_failed, counts.messages),
                (2, 1, 1)
            );
        }
        let disabled = DeliveryDiagnostics::with_enabled(
            "disabled",
            "127.0.0.1:1234"
                .parse()
                .expect("valid diagnostic test fixture"),
            false,
        );
        subscribe(&disabled, "one");
        disabled.sent(&report("one", json!({"x": 1})), 20);
        disabled.produced("one", 1, true);
        assert!(disabled.state.is_none());
    }

    #[test]
    fn flush_preserves_cross_window_baselines_but_resets_window_counters() {
        let diagnostics = diagnostics();
        subscribe(&diagnostics, "one");
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        diagnostics.flush(true);
        diagnostics.sent(&report("one", json!({"x": 1})), 20);
        let state = diagnostics
            .state
            .as_ref()
            .expect("valid diagnostic test fixture")
            .lock()
            .expect("valid diagnostic test fixture");
        let counts = state
            .counts
            .values()
            .next()
            .expect("valid diagnostic test fixture");
        assert_eq!(
            (counts.messages, counts.unchanged, counts.changed),
            (1, 1, 0)
        );
    }
    #[test]
    fn attribution_covers_all_live_subscriptions_and_operations() {
        let diagnostics = diagnostics();
        for index in 0..1024 {
            let tx = format!("stream-{index}");
            diagnostics.inbound(&MykoMessage::Report(WrappedReport {
                report_id: format!("Report{index}"),
                report: json!({"tx": tx}),
            }));
            diagnostics.ready(&tx);
            diagnostics.produced(&tx, 1, true);
            diagnostics.sent(&report(&tx, json!({"value": index})), 32);
        }
        {
            let state = diagnostics
                .state
                .as_ref()
                .expect("enabled")
                .lock()
                .expect("unpoisoned");
            assert_eq!(state.subscriptions.len(), 1024);
            assert_eq!(state.counts.len(), 1024);
            assert_eq!(state.untracked, 0);
            assert!(
                state
                    .counts
                    .iter()
                    .all(|(operation, counts)| operation.kind == "report"
                        && counts.messages == 1
                        && counts.produced == 1)
            );
        }
        for index in 0..1024 {
            diagnostics.inbound(&MykoMessage::ReportCancel(CancelSubscription {
                tx: format!("stream-{index}"),
            }));
        }
        diagnostics.flush(true);
        let state = diagnostics
            .state
            .as_ref()
            .expect("enabled")
            .lock()
            .expect("unpoisoned");
        assert!(state.subscriptions.is_empty());
        assert_eq!(state.retained_properties, 0);
        assert!(state.counts.is_empty());
    }

    #[test]
    fn client_memory_limits_remain_bounded_across_many_subscriptions() {
        let diagnostics = diagnostics();
        for index in 0..17 {
            let tx = format!("stream-{index}");
            subscribe(&diagnostics, &tx);
            diagnostics.sent(&report(&tx, json!(vec![1; MAX_PROPERTIES])), 20_000);
        }
        for index in 17..1024 {
            subscribe(&diagnostics, &format!("stream-{index}"));
        }
        let state = diagnostics
            .state
            .as_ref()
            .expect("enabled")
            .lock()
            .expect("unpoisoned");
        assert_eq!(state.subscriptions.len(), 1024);
        assert!(
            state
                .subscriptions
                .values()
                .map(Subscription::retained)
                .sum::<usize>()
                <= MAX_CLIENT_PROPERTIES
        );
        assert!(
            state.counts.values().map(|c| c.unique.len()).sum::<usize>() <= MAX_CLIENT_PROPERTIES
        );
        assert_eq!(state.untracked, 0);
        assert_eq!(
            state.retained_properties,
            state
                .subscriptions
                .values()
                .map(Subscription::retained)
                .sum::<usize>()
        );
        assert!(state.counts.values().any(|c| c.incomplete > 0));
    }
}
