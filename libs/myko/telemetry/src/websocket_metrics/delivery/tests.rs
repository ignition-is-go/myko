#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]
use super::*;
use crate::websocket_metrics::Snapshot;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::ResourceMetrics,
};

fn attrs(id: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("client_id", id.to_owned()),
        KeyValue::new("peer", "127.0.0.1"),
        KeyValue::new("kind", "Query"),
        KeyValue::new("operation", "Example"),
    ]
}

fn export(snapshot: &Snapshot) -> Vec<Metric> {
    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics::default()],
    };
    assert!(snapshot.append(&mut request));
    request
        .resource_metrics
        .into_iter()
        .flat_map(|r| r.scope_metrics)
        .flat_map(|s| s.metrics)
        .collect()
}

fn histogram(metrics: &[Metric]) -> &OtlpHistogram {
    let metric = metrics
        .iter()
        .find(|m| m.name == Histogram::Send.name())
        .expect("fixture value exists");
    assert_eq!(metric.unit, "s");
    let Some(metric::Data::Histogram(histogram)) = &metric.data else {
        panic!("histogram encoding")
    };
    histogram
}

#[test]
fn histogram_schema_and_cumulative_values_match_existing_instruments() {
    let registry = Arc::new(Registry::default());
    registry.open("client");
    let writer = Connection::attach(registry.clone(), "client");
    for micros in [0, 100, 200, 5_000_000, 6_000_000] {
        writer.histogram(
            Histogram::Send,
            Duration::from_micros(micros),
            &attrs("client"),
        );
    }
    let first = registry.snapshot();
    let metrics = export(&first);
    let hist = histogram(&metrics);
    assert_eq!(
        hist.aggregation_temporality,
        i32::from(AggregationTemporality::Cumulative)
    );
    let point = &hist.data_points[0];
    assert_eq!(point.count, 5);
    assert!((point.sum.expect("fixture value exists") - 11.0003).abs() < 1e-10);
    assert_eq!(point.min, Some(0.0));
    assert_eq!(point.max, Some(6.0));
    assert_eq!(point.explicit_bounds, BOUNDS);
    assert_eq!(point.bucket_counts, [2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1]);
    assert_eq!(point.start_time_unix_nano, first.clients[0].started);
    assert_eq!(point.time_unix_nano, first.time);
    assert_eq!(point.attributes, attributes(&labels(&attrs("client"))));
    let mut reordered = attrs("client");
    reordered.reverse();
    writer.histogram(Histogram::Send, Duration::from_secs(1), &reordered);
    let next = export(&registry.snapshot());
    assert_eq!(histogram(&next).data_points.len(), 1);
    assert_eq!(histogram(&next).data_points[0].count, 6);
    assert_eq!(histogram(&metrics).data_points[0].count, 5);
}

#[test]
fn socket_close_waits_for_final_diagnostic_writes_and_accepted_export() {
    let registry = Arc::new(Registry::default());
    registry.open("client");
    let writer = Connection::attach(registry.clone(), "client");
    writer.histogram(Histogram::Send, Duration::from_millis(1), &attrs("client"));
    registry.close("client");
    let in_flight = registry.snapshot();
    assert!(!in_flight.clients[0].closed);
    registry.acknowledge(&in_flight);
    assert_eq!(registry.retained(), (0, 1));
    writer.histogram(Histogram::Send, Duration::from_millis(2), &attrs("client"));
    writer.counter(Counter::Closed, 1, &attrs("client"));
    writer.gauge(Gauge::Connections, 0, &attrs("client"));
    drop(writer);
    registry.acknowledge(&in_flight);
    assert_eq!(registry.retained(), (0, 1));
    let final_export = registry.snapshot();
    assert!(final_export.clients[0].closed);
    let metrics = export(&final_export);
    assert_eq!(histogram(&metrics).data_points[0].count, 2);
    let connections = metrics
        .iter()
        .find(|m| m.name == Gauge::Connections.name())
        .expect("fixture value exists");
    let Some(metric::Data::Sum(sum)) = &connections.data else {
        panic!("connection sum")
    };
    assert!(!sum.is_monotonic);
    assert_eq!(
        sum.data_points[0].value,
        Some(number_data_point::Value::AsInt(0))
    );
    registry.acknowledge(&final_export);
    assert_eq!(registry.retained(), (0, 0));
    assert!(export(&registry.snapshot()).is_empty());
}

#[test]
fn diagnostic_owner_close_does_not_retire_an_open_socket() {
    let registry = Arc::new(Registry::default());
    registry.open("client");
    let writer = Connection::attach(registry.clone(), "client");
    writer.counter(Counter::Opened, 1, &attrs("client"));
    drop(writer);
    registry.acknowledge(&registry.snapshot());
    assert_eq!(registry.retained(), (1, 0));
    registry.close("client");
    registry.acknowledge(&registry.snapshot());
    assert_eq!(registry.retained(), (0, 0));
}

#[test]
fn new_writer_invalidates_an_in_flight_closed_snapshot() {
    let registry = Arc::new(Registry::default());
    registry.open("client");
    let writer = Connection::attach(registry.clone(), "client");
    writer.histogram(Histogram::Send, Duration::from_secs(1), &attrs("client"));
    drop(writer);
    registry.close("client");
    let old = registry.snapshot();
    assert!(old.clients[0].closed);
    let writer = Connection::attach(registry.clone(), "client");
    writer.histogram(Histogram::Send, Duration::from_secs(2), &attrs("client"));
    registry.acknowledge(&old);
    assert_eq!(registry.retained(), (0, 1));
    drop(writer);
    let current = registry.snapshot();
    assert_eq!(histogram(&export(&current)).data_points[0].count, 2);
    registry.acknowledge(&current);
    assert_eq!(registry.retained(), (0, 0));
}

#[test]
fn reconnect_churn_exports_full_detail_then_retires_all_closed_series() {
    let registry = Arc::new(Registry::default());
    registry.open("active");
    let active = Connection::attach(registry.clone(), "active");
    active.histogram(Histogram::Send, Duration::from_secs(1), &attrs("active"));
    for batch in 0..2 {
        for index in 0..2500 {
            let id = format!("{batch}-{index}");
            registry.open(&id);
            let writer = Connection::attach(registry.clone(), &id);
            writer.histogram(Histogram::Send, Duration::from_millis(1), &attrs(&id));
            registry.close(&id);
            drop(writer);
        }
        let snapshot = registry.snapshot();
        let metrics = export(&snapshot);
        assert_eq!(histogram(&metrics).data_points.len(), 2501);
        registry.acknowledge(&snapshot);
        assert_eq!(registry.retained(), (1, 0));
        let remaining = export(&registry.snapshot());
        let points = &histogram(&remaining).data_points;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].attributes, attributes(&labels(&attrs("active"))));
    }
}

#[test]
fn gauges_replace_values_and_counters_preserve_unsigned_totals() {
    let registry = Arc::new(Registry::default());
    registry.open("client");
    let writer = Connection::attach(registry.clone(), "client");
    writer.counter(Counter::Bytes, u64::MAX, &attrs("client"));
    writer.gauge(Gauge::QueuePeak, 8, &attrs("client"));
    writer.gauge(Gauge::QueuePeak, 2, &attrs("client"));
    let metrics = export(&registry.snapshot());
    let metric = metrics
        .iter()
        .find(|m| m.name == Gauge::QueuePeak.name())
        .expect("fixture value exists");
    let Some(metric::Data::Gauge(gauge)) = &metric.data else {
        panic!("gauge encoding")
    };
    assert_eq!(
        gauge.data_points[0].value,
        Some(number_data_point::Value::AsInt(2))
    );
    let metric = metrics
        .iter()
        .find(|m| m.name == Counter::Bytes.name())
        .expect("fixture value exists");
    let Some(metric::Data::Sum(sum)) = &metric.data else {
        panic!("counter encoding")
    };
    assert!(sum.is_monotonic);
    assert!(matches!(
        sum.data_points[0].value,
        Some(number_data_point::Value::AsDouble(_))
    ));
}

#[test]
fn idle_diagnostic_owner_releases_closed_connection_after_acknowledgement() {
    let registry = Arc::new(Registry::default());
    registry.open("idle");
    let writer = Connection::attach(registry.clone(), "idle");
    registry.close("idle");
    registry.acknowledge(&registry.snapshot());
    assert_eq!(registry.retained(), (0, 1));
    drop(writer);
    let final_export = registry.snapshot();
    assert!(export(&final_export).is_empty());
    registry.acknowledge(&final_export);
    assert_eq!(registry.retained(), (0, 0));
}
