//! Integration tests for the saga framework.
//!
//! These tests verify that sagas:
//! - Receive events from the event manager
//! - Can filter events with stream operators
//! - Can emit commands that get executed

#![cfg(feature = "bench")]

use futures::StreamExt;
use myko_rs::{
    bench_entities::BenchItem,
    event::{MEvent, MEventType},
    saga::SagaStreamExt,
};

fn make_bench_item_event(id: &str, name: &str) -> MEvent {
    let item = BenchItem {
        id: id.into(),
        hash: format!("hash-{}", id).into(),
        name: name.to_string(),
        category: "test".to_string(),
        value: 42,
    };

    MEvent::from_item(&item, MEventType::SET, format!("tx-{}", id))
}

fn make_other_event(item_type: &str, change_type: MEventType) -> MEvent {
    MEvent {
        tx: "test-tx".to_string(),
        item_type: item_type.to_string(),
        item: serde_json::json!({"id": "other-1", "hash": "h"}),
        change_type,
        created_at: chrono::Utc::now().to_rfc3339(),
        source_id: None,
    }
}

#[tokio::test]
async fn test_saga_pairwise_operator() {
    use futures::stream;

    let events = stream::iter(vec![
        make_bench_item_event("id-1", "First"),
        make_bench_item_event("id-2", "Second"),
        make_bench_item_event("id-3", "Third"),
    ]);

    let pairs: Vec<_> = events.pairwise().collect().await;

    assert_eq!(pairs.len(), 2);
    assert_eq!(
        pairs[0].0.item_json().get("id").and_then(|v| v.as_str()),
        Some("id-1")
    );
    assert_eq!(
        pairs[0].1.item_json().get("id").and_then(|v| v.as_str()),
        Some("id-2")
    );
}

#[tokio::test]
async fn test_saga_scan_operator() {
    use futures::stream;

    let events = stream::iter(vec![
        make_bench_item_event("id-1", "First"),
        make_bench_item_event("id-2", "Second"),
        make_bench_item_event("id-3", "Third"),
    ]);

    let counts: Vec<_> = events.accumulate(0, |count, _| *count + 1).collect().await;

    assert_eq!(counts, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_saga_filter_chain() {
    use futures::stream;

    let events = stream::iter(vec![
        make_bench_item_event("id-1", "First"),
        make_other_event("OtherItem", MEventType::SET),
        make_bench_item_event("id-2", "Second"),
        MEvent::from_item(
            &BenchItem {
                id: "id-3".into(),
                hash: "h".into(),
                name: "Third".to_string(),
                category: "test".to_string(),
                value: 0,
            },
            MEventType::DEL,
            "tx-3".to_string(),
        ),
    ]);

    // Filter to only BenchItem SET events
    let filtered: Vec<_> = events
        .of_item_type("BenchItem")
        .of_change_type(MEventType::SET)
        .collect()
        .await;

    assert_eq!(filtered.len(), 2);
    assert!(
        filtered
            .iter()
            .all(|e: &MEvent| e.item_type() == "BenchItem")
    );
    assert!(
        filtered
            .iter()
            .all(|e: &MEvent| e.change_type() == MEventType::SET)
    );
}

#[tokio::test]
async fn test_saga_of_item_type() {
    use futures::stream;

    let events = stream::iter(vec![
        make_bench_item_event("id-1", "First"),
        make_other_event("Scene", MEventType::SET),
        make_bench_item_event("id-2", "Second"),
    ]);

    let filtered: Vec<_> = events.of_item_type("BenchItem").collect().await;

    assert_eq!(filtered.len(), 2);
}

#[tokio::test]
async fn test_saga_of_change_type() {
    use futures::stream;

    let events = stream::iter(vec![
        make_bench_item_event("id-1", "First"),
        MEvent::from_item(
            &BenchItem {
                id: "id-2".into(),
                hash: "h".into(),
                name: "Second".to_string(),
                category: "test".to_string(),
                value: 0,
            },
            MEventType::DEL,
            "tx-2".to_string(),
        ),
        make_bench_item_event("id-3", "Third"),
    ]);

    let filtered: Vec<_> = events.of_change_type(MEventType::SET).collect().await;

    assert_eq!(filtered.len(), 2);
}
