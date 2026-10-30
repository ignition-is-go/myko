#![allow(clippy::panic_in_result_fn)]
use super::*;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;

#[test]
fn all_operation_series_survive_beyond_sdk_limit() {
    let registry = Registry::default();
    registry.open("client");
    for id in 0..5000 {
        registry.record(
            "client",
            "out",
            "QueryResponse",
            Some(&format!("query-{id}")),
            Some(17),
        );
    }
    let snapshot = registry.snapshot();
    let mut request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics::default()],
    };
    assert!(snapshot.append(&mut request));
    let points = request
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .filter_map(|metric| match metric.data.as_ref() {
            Some(metric::Data::Sum(sum)) => Some(sum.data_points.len()),
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(points, 10001); // Every operation's count and bytes, plus one client total.
}

#[test]
fn only_confirmed_final_closed_snapshot_can_retire() {
    let registry = Registry::default();
    registry.open("client");
    registry.record("client", "out", "ViewResponse", Some("view"), Some(10));
    let active = registry.snapshot();
    let confirmed = BTreeSet::from(["client".into()]);
    registry.acknowledge(&active, &confirmed);
    assert_eq!(registry.retained(), (1, 0));
    registry.close("client");
    let closed = registry.snapshot();
    registry.acknowledge(&closed, &BTreeSet::new());
    assert_eq!(registry.retained(), (0, 1));
    registry.record("client", "out", "ViewResponse", Some("view"), Some(10));
    registry.acknowledge(&closed, &confirmed); // Export raced with a final writer.
    assert_eq!(registry.retained(), (0, 1));
    registry.acknowledge(&registry.snapshot(), &confirmed);
    assert_eq!(registry.retained(), (0, 0));
}

#[test]
fn response_bindings_are_connection_and_family_scoped() {
    let registry = Arc::new(Registry::default());
    install(&registry);
    client_connected("one");
    client_connected("two");
    bind_operation("one", "query", "tx", "query-id");
    bind_operation("one", "view", "tx", "view-id");
    assert_eq!(operation("one", "query", "tx").as_deref(), Some("query-id"));
    assert_eq!(operation("one", "view", "tx").as_deref(), Some("view-id"));
    assert_eq!(operation("two", "query", "tx"), None);
    record("one", "in", "Query", Some("query-id"), None);
    client_disconnected("one");
    assert_eq!(operation("one", "query", "tx"), None);
    assert_eq!(registry.retained(), (1, 1));
    client_disconnected("two"); // Idle connections need no confirmation.
    assert_eq!(registry.retained(), (0, 1));
}
