//! Integration tests for entity equality and serialization.
//!
//! These tests verify that:
//! - Entities with identical fields are equal
//! - Different field values produce inequality
//! - Round-trip serialization preserves equality

#![cfg(feature = "bench")]

use myko::bench_entities::BenchItem;

fn make_item(id: &str, name: &str, category: &str, value: i64) -> BenchItem {
    BenchItem {
        id: id.into(),
        name: name.into(),
        category: category.into(),
        value,
    }
}

#[test]
fn identical_entities_are_equal() {
    let a = make_item("item-1", "Widget", "tools", 42);
    let b = make_item("item-1", "Widget", "tools", 42);
    assert_eq!(a, b);
}

#[test]
fn different_field_values_are_not_equal() {
    let a = make_item("item-1", "Widget", "tools", 42);
    let b = make_item("item-1", "Widget", "tools", 99);
    assert_ne!(a, b);
}

#[test]
fn round_trip_serialization_preserves_equality() {
    let original = make_item("item-1", "Widget", "tools", 42);
    let json = serde_json::to_value(&original).unwrap();
    let deserialized: BenchItem = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, original);
}

#[test]
fn id_is_wire_visible() {
    let item = make_item("item-1", "Widget", "tools", 42);
    let json: serde_json::Value = serde_json::to_value(&item).unwrap();
    assert_eq!(json["id"].as_str(), Some("item-1"));
}
