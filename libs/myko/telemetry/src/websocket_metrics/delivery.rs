//! Delivery diagnostics share the wire counter's connection and export lifetime.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use num_traits::ToPrimitive;
use opentelemetry::KeyValue;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue as Attribute, any_value},
    metrics::v1::{
        AggregationTemporality, Gauge as OtlpGauge, Histogram as OtlpHistogram, HistogramDataPoint,
        Metric, NumberDataPoint, Sum, metric, number_data_point,
    },
};

use super::Registry;

macro_rules! instruments {
    ($name:ident { $($variant:ident => $suffix:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
        pub enum $name { $($variant),+ }
        impl $name {
            const fn name(self) -> &'static str {
                match self { $(Self::$variant => concat!("myko.ws.delivery.", $suffix)),+ }
            }
        }
    };
}

instruments!(Counter {
    Opened => "connections.opened", Closed => "connections.closed",
    Untracked => "untracked", Failures => "failures", Upserts => "upserts",
    Deletes => "deletes", Produced => "produced", EnqueueFailed => "enqueue_failed",
    Messages => "messages", Bytes => "bytes", Unchanged => "unchanged",
    Properties => "properties", Changed => "properties.changed",
    Removed => "properties.removed", Incomplete => "inspection.incomplete",
});
instruments!(Gauge {
    Connections => "connections", Pending => "subscriptions.not_ready",
    Active => "subscriptions", Equivalent => "subscriptions.equivalent",
    QueuePeak => "queue_peak",
});
instruments!(Histogram {
    Inspection => "inspection", QueueWait => "queue_wait", Materialize => "materialize",
    Serialize => "serialize", Send => "send", ProducedGap => "produced_gap",
    Gap => "gap", Setup => "subscription_setup",
});

const BOUNDS: [f64; 11] = [
    0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
];
type Labels = BTreeMap<String, String>;

#[derive(Clone, Debug, Default)]
struct Distribution {
    count: u64,
    sum: f64,
    buckets: [u64; BOUNDS.len() + 1],
    min: Option<f64>,
    max: Option<f64>,
}

impl Distribution {
    fn record(&mut self, duration: Duration) {
        let value = duration.as_secs_f64();
        self.count = self.count.saturating_add(1);
        self.sum += value;
        for (bucket, bound) in self
            .buckets
            .iter_mut()
            .zip(BOUNDS.into_iter().chain([f64::INFINITY]))
        {
            if value <= bound {
                *bucket = bucket.saturating_add(1);
                break;
            }
        }
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct Values {
    counters: BTreeMap<(Counter, Labels), u64>,
    gauges: BTreeMap<(Gauge, Labels), u64>,
    histograms: BTreeMap<(Histogram, Labels), Distribution>,
}

impl Values {
    pub(super) fn is_empty(&self) -> bool {
        self.counters.is_empty() && self.gauges.is_empty() && self.histograms.is_empty()
    }

    pub(super) fn append(&self, points: &mut Points, started: u64, time: u64) {
        for ((name, labels), value) in &self.counters {
            points
                .counters
                .entry(*name)
                .or_default()
                .push(number(*value, labels, started, time));
        }
        for ((name, labels), value) in &self.gauges {
            points
                .gauges
                .entry(*name)
                .or_default()
                .push(number(*value, labels, started, time));
        }
        for ((name, labels), value) in &self.histograms {
            points
                .histograms
                .entry(*name)
                .or_default()
                .push(HistogramDataPoint {
                    attributes: attributes(labels),
                    start_time_unix_nano: started,
                    time_unix_nano: time,
                    count: value.count,
                    sum: Some(value.sum),
                    bucket_counts: value.buckets.to_vec(),
                    explicit_bounds: BOUNDS.to_vec(),
                    min: value.min,
                    max: value.max,
                    ..Default::default()
                });
        }
    }
}

fn labels(attributes: &[KeyValue]) -> Labels {
    attributes
        .iter()
        .map(|attribute| (attribute.key.to_string(), attribute.value.to_string()))
        .collect()
}

fn attributes(labels: &Labels) -> Vec<Attribute> {
    labels
        .iter()
        .map(|(key, value)| Attribute {
            key: key.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.clone())),
            }),
            ..Default::default()
        })
        .collect()
}

fn number(value: u64, labels: &Labels, started: u64, time: u64) -> NumberDataPoint {
    NumberDataPoint {
        attributes: attributes(labels),
        start_time_unix_nano: started,
        time_unix_nano: time,
        value: Some(i64::try_from(value).map_or_else(
            |_| number_data_point::Value::AsDouble(value.to_f64().unwrap_or(f64::MAX)),
            number_data_point::Value::AsInt,
        )),
        ..Default::default()
    }
}

#[derive(Default)]
pub(super) struct Points {
    counters: BTreeMap<Counter, Vec<NumberDataPoint>>,
    gauges: BTreeMap<Gauge, Vec<NumberDataPoint>>,
    histograms: BTreeMap<Histogram, Vec<HistogramDataPoint>>,
}

impl Points {
    pub(super) fn metrics(self) -> impl Iterator<Item = Metric> {
        self.counters
            .into_iter()
            .map(|(name, data_points)| Metric {
                name: name.name().into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points,
                    aggregation_temporality: AggregationTemporality::Cumulative.into(),
                    is_monotonic: true,
                })),
                ..Default::default()
            })
            .chain(self.gauges.into_iter().map(|(name, data_points)| Metric {
                name: name.name().into(),
                // Keep the existing non-monotonic sum encoding of connections.
                data: Some(if name == Gauge::Connections {
                    metric::Data::Sum(Sum {
                        data_points,
                        aggregation_temporality: AggregationTemporality::Cumulative.into(),
                        is_monotonic: false,
                    })
                } else {
                    metric::Data::Gauge(OtlpGauge { data_points })
                }),
                ..Default::default()
            }))
            .chain(
                self.histograms
                    .into_iter()
                    .map(|(name, data_points)| Metric {
                        name: name.name().into(),
                        unit: "s".into(),
                        data: Some(metric::Data::Histogram(OtlpHistogram {
                            data_points,
                            aggregation_temporality: AggregationTemporality::Cumulative.into(),
                        })),
                        ..Default::default()
                    }),
            )
    }
}

/// Keeps a connection exportable until its final diagnostic writes have stopped.
/// Closing the socket alone cannot retire metrics while this owner is alive.
#[derive(Debug, Default)]
pub struct Connection {
    owner: Option<(Arc<Registry>, String, u64)>,
}

impl Connection {
    #[must_use]
    pub fn new(client_id: &str) -> Self {
        super::global().map_or_else(Self::default, |registry| Self::attach(registry, client_id))
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "Each allocated Connection owns one writer lease, so the count cannot overflow."
    )]
    pub(super) fn attach(registry: Arc<Registry>, client_id: &str) -> Self {
        let started = match registry.clients().get_mut(client_id) {
            Some(client) => {
                client.delivery_writers += 1;
                client.revision = client.revision.saturating_add(1);
                client.started
            }
            None => return Self::default(),
        };
        Self {
            owner: Some((registry, client_id.into(), started)),
        }
    }

    fn update(&self, write: impl FnOnce(&mut super::Client)) {
        let Some((registry, id, started)) = &self.owner else {
            return;
        };
        if let Some(client) = registry
            .clients()
            .get_mut(id)
            .filter(|client| client.started == *started)
        {
            client.revision = client.revision.saturating_add(1);
            write(client);
        }
    }

    pub fn counter(&self, name: Counter, value: u64, attributes: &[KeyValue]) {
        self.update(|client| {
            let count = client
                .delivery
                .counters
                .entry((name, labels(attributes)))
                .or_default();
            *count = count.saturating_add(value);
        });
    }

    pub fn gauge(&self, name: Gauge, value: u64, attributes: &[KeyValue]) {
        self.update(|client| {
            client
                .delivery
                .gauges
                .insert((name, labels(attributes)), value);
        });
    }

    pub fn histogram(&self, name: Histogram, value: Duration, attributes: &[KeyValue]) {
        self.update(|client| {
            client
                .delivery
                .histograms
                .entry((name, labels(attributes)))
                .or_default()
                .record(value);
        });
    }
}

impl Drop for Connection {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "A non-cloneable Connection releases exactly its own attached writer lease."
    )]
    fn drop(&mut self) {
        self.update(|client| {
            client.delivery_writers -= 1;
        });
    }
}

#[cfg(test)]
mod tests;
