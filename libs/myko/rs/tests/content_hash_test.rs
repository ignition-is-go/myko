//! Integration tests for entity content hash feature.
//!
//! These tests verify that:
//! - Content hashing is deterministic for identical field values
//! - Different field values produce different hashes
//! - Deserialization recomputes the hash (preventing tampering)
//! - The hash field is visible on the wire (in serialized JSON)
//! - The AnyItem trait method matches the struct field

#![cfg(feature = "bench")]

use myko_rs::bench_entities::BenchItem;
use myko_rs::prelude::*;

fn make_item(id: &str, name: &str, category: &str, value: i64) -> BenchItem {
    let mut item = BenchItem {
        id: id.into(),
        name: name.into(),
        category: category.into(),
        value,
        hash: Default::default(),
    };
    item.rehash();
    item
}

#[test]
fn identical_entities_have_equal_hashes() {
    let a = make_item("item-1", "Widget", "tools", 42);
    let b = make_item("item-1", "Widget", "tools", 42);

    assert_eq!(a.hash, b.hash, "identical field values must produce equal hashes");
    assert_eq!(a, b, "items with equal hashes must be equal via PartialEq");
}

#[test]
fn different_field_values_produce_different_hashes() {
    let a = make_item("item-1", "Widget", "tools", 42);
    let b = make_item("item-1", "Widget", "tools", 99);

    assert_ne!(a.hash, b.hash, "differing field values must produce different hashes");
    assert_ne!(a, b, "items with different hashes must not be equal");
}

#[test]
fn deserialization_recomputes_hash() {
    let original = make_item("item-1", "Widget", "tools", 42);
    let mut json: serde_json::Value = serde_json::to_value(&original).unwrap();

    // Tamper with the hash in the serialized JSON
    json["hash"] = serde_json::Value::String("tampered-hash-value".into());

    let deserialized: BenchItem = serde_json::from_value(json).unwrap();

    assert_ne!(
        deserialized.hash.as_ref(),
        "tampered-hash-value",
        "deserialized hash must not retain the tampered value"
    );
    assert_eq!(
        deserialized.hash, original.hash,
        "deserialized hash must match the correctly computed hash"
    );
}

#[test]
fn hash_is_wire_visible() {
    let item = make_item("item-1", "Widget", "tools", 42);
    let json: serde_json::Value = serde_json::to_value(&item).unwrap();

    assert!(
        json.get("hash").is_some(),
        "the hash field must be present in serialized JSON"
    );
    assert!(
        json["hash"].is_string(),
        "the hash field must serialize as a string"
    );
    assert_eq!(
        json["hash"].as_str().unwrap(),
        item.hash.as_ref(),
        "the serialized hash must match the struct field"
    );
}

#[test]
fn content_hash_trait_method_matches_field() {
    let item = make_item("item-1", "Widget", "tools", 42);
    let any_item: &dyn AnyItem = &item;

    assert_eq!(
        any_item.content_hash(),
        &item.hash,
        "AnyItem::content_hash() must return the same value as the hash field"
    );
}
