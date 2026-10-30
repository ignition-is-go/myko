//! Connection-scoped WebSocket counters.
//!
//! Full operation detail bypasses SDK cardinality aggregation; closed clients
//! stay pending until backend confirmation.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak},
    time::{SystemTime, UNIX_EPOCH},
};

use num_traits::ToPrimitive;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Metric, NumberDataPoint, ScopeMetrics, Sum, metric,
        number_data_point,
    },
};

pub(crate) mod export;

const TOTAL: &str = "myko.ws.message.count";
const OPERATIONS: &str = "myko.ws.operation.message.count";
const BYTES: &str = "myko.ws.operation.message.bytes";

static GLOBAL: OnceLock<RwLock<Weak<Registry>>> = OnceLock::new();

fn global() -> Option<Arc<Registry>> {
    GLOBAL.get()?.read().ok()?.upgrade()
}

pub(crate) fn install(registry: &Arc<Registry>) {
    let global = GLOBAL.get_or_init(|| RwLock::new(Weak::new()));
    if let Ok(mut global) = global.write() {
        *global = Arc::downgrade(registry);
    }
}

/// Begin a connection's metric lifetime. Call after installing telemetry.
pub fn client_connected(client_id: &str) {
    if let Some(registry) = global() {
        registry.open(client_id);
    }
}

/// Close a connection after its writers have stopped.
///
/// Unconfirmed counts remain exportable; this does not discard measurements.
pub fn client_disconnected(client_id: &str) {
    if let Some(registry) = global() {
        registry.close(client_id);
    }
}

/// Associate a wire transaction with its stable query/report/view/command ID.
/// Payloads and request arguments are never retained.
pub fn bind_operation(client_id: &str, family: &'static str, tx: &str, operation: &str) {
    if let Some(registry) = global()
        && let Some(client) = registry.clients().get_mut(client_id)
    {
        client
            .bindings
            .insert((family, tx.into()), operation.into());
    }
}

/// Resolve an outgoing response's transaction to the original operation ID.
#[must_use]
pub fn operation(client_id: &str, family: &'static str, tx: &str) -> Option<String> {
    global()?
        .clients()
        .get(client_id)?
        .bindings
        .get(&(family, tx.into()))
        .cloned()
}

/// Record a message, optionally including successfully sent encoded bytes.
/// There is no cardinality cap, sampling, or overflow bucket in this registry.
pub fn record(
    client_id: &str,
    direction: &'static str,
    kind: &'static str,
    operation: Option<&str>,
    bytes: Option<u64>,
) {
    if let Some(registry) = global() {
        registry.record(client_id, direction, kind, operation, bytes);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Key {
    direction: &'static str,
    kind: &'static str,
    operation: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Counts {
    messages: u64,
    bytes: Option<u64>,
}

#[derive(Debug)]
struct Client {
    started: u64,
    closed: bool,
    counters: BTreeMap<Key, Counts>,
    bindings: BTreeMap<(&'static str, String), String>,
}

#[derive(Debug, Default)]
pub(crate) struct Registry {
    clients: Mutex<BTreeMap<String, Client>>,
}

impl Registry {
    fn clients(&self) -> MutexGuard<'_, BTreeMap<String, Client>> {
        self.clients.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open(&self, id: &str) {
        self.clients().entry(id.into()).or_insert_with(|| Client {
            started: now(),
            closed: false,
            counters: BTreeMap::new(),
            bindings: BTreeMap::new(),
        });
    }

    fn close(&self, id: &str) {
        let mut clients = self.clients();
        if let Some(client) = clients.get_mut(id) {
            client.closed = true;
            client.bindings.clear();
            // There is nothing to export for a completely idle connection.
            if client.counters.is_empty() {
                clients.remove(id);
            }
        }
    }

    fn record(
        &self,
        id: &str,
        direction: &'static str,
        kind: &'static str,
        operation: Option<&str>,
        bytes: Option<u64>,
    ) {
        if let Some(client) = self.clients().get_mut(id) {
            let counts = client
                .counters
                .entry(Key {
                    direction,
                    kind,
                    operation: operation.map(str::to_owned),
                })
                .or_default();
            counts.messages = counts.messages.saturating_add(1);
            if let Some(bytes) = bytes {
                counts.bytes = Some(counts.bytes.unwrap_or_default().saturating_add(bytes));
            }
        }
    }

    pub(crate) fn retained(&self) -> (u64, u64) {
        let clients = self.clients();
        let closed = clients.values().filter(|client| client.closed).count();
        (
            u64::try_from(clients.len().saturating_sub(closed)).unwrap_or(u64::MAX),
            u64::try_from(closed).unwrap_or(u64::MAX),
        )
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let clients = self
            .clients()
            .iter()
            .map(|(id, client)| ClientSnapshot {
                id: id.clone(),
                started: client.started,
                closed: client.closed,
                counters: client.counters.clone(),
            })
            .collect();
        Snapshot {
            clients,
            time: now(),
        }
    }

    pub(crate) fn acknowledge(&self, snapshot: &Snapshot, confirmed: &BTreeSet<String>) {
        let mut clients = self.clients();
        for exported in &snapshot.clients {
            if !exported.closed || !confirmed.contains(&exported.id) {
                continue;
            }
            if clients.get(&exported.id).is_some_and(|current| {
                current.closed
                    && current.started == exported.started
                    && current.counters == exported.counters
            }) {
                clients.remove(&exported.id);
            }
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Clone)]
pub(crate) struct ClientSnapshot {
    pub(crate) id: String,
    started: u64,
    pub(crate) closed: bool,
    counters: BTreeMap<Key, Counts>,
}

pub(crate) struct Snapshot {
    pub(crate) clients: Vec<ClientSnapshot>,
    time: u64,
}

pub(crate) struct Point {
    pub(crate) name: &'static str,
    pub(crate) labels: BTreeMap<&'static str, String>,
    pub(crate) value: u64,
}

impl ClientSnapshot {
    pub(crate) fn points(&self) -> Vec<Point> {
        let mut totals = BTreeMap::<_, u64>::new();
        let mut points = Vec::new();
        for (key, counts) in &self.counters {
            let total = totals.entry(key.direction).or_default();
            *total = total.saturating_add(counts.messages);
            let mut labels = BTreeMap::from([
                ("client_id", self.id.clone()),
                ("direction", key.direction.into()),
                ("kind", key.kind.into()),
            ]);
            if let Some(operation) = &key.operation {
                labels.insert("tag", operation.clone());
            }
            points.push(Point {
                name: OPERATIONS,
                labels: labels.clone(),
                value: counts.messages,
            });
            if let Some(bytes) = counts.bytes {
                points.push(Point {
                    name: BYTES,
                    labels,
                    value: bytes,
                });
            }
        }
        for (direction, value) in totals {
            points.push(Point {
                name: TOTAL,
                labels: BTreeMap::from([
                    ("client_id", self.id.clone()),
                    ("direction", direction.into()),
                ]),
                value,
            });
        }
        points
    }
}

impl Point {
    pub(crate) fn prometheus_name(&self) -> String {
        format!("{}_total", self.name.replace('.', "_"))
    }
}

impl Snapshot {
    /// Append raw OTLP sums after SDK aggregation, preserving all label sets.
    pub(crate) fn append(&self, request: &mut ExportMetricsServiceRequest) -> bool {
        let Some(resource) = request.resource_metrics.first_mut() else {
            return false;
        };
        let mut by_name = BTreeMap::<_, Vec<NumberDataPoint>>::new();
        for client in &self.clients {
            for point in client.points() {
                let attributes = point
                    .labels
                    .into_iter()
                    .map(|(key, value)| KeyValue {
                        key: key.into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(value)),
                        }),
                        ..Default::default()
                    })
                    .collect();
                by_name
                    .entry(point.name)
                    .or_default()
                    .push(NumberDataPoint {
                        attributes,
                        start_time_unix_nano: client.started,
                        time_unix_nano: self.time,
                        value: Some(i64::try_from(point.value).map_or_else(
                            |_| {
                                number_data_point::Value::AsDouble(
                                    point.value.to_f64().unwrap_or(f64::MAX),
                                )
                            },
                            number_data_point::Value::AsInt,
                        )),
                        ..Default::default()
                    });
            }
        }
        if by_name.is_empty() {
            return true;
        }
        let metrics = by_name
            .into_iter()
            .map(|(name, data_points)| Metric {
                name: name.into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points,
                    aggregation_temporality: AggregationTemporality::Cumulative.into(),
                    is_monotonic: true,
                })),
                ..Default::default()
            })
            .collect();
        resource.scope_metrics.push(ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "myko-server".into(),
                ..Default::default()
            }),
            metrics,
            ..Default::default()
        });
        true
    }
}

#[cfg(test)]
mod tests;
