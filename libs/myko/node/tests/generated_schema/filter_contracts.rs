use myko::{
    query::{EqFilter, IdFilter, NumericFilter, StringFilter},
    schemars::JsonSchema,
};
use myko_items::schema::TypeSchema;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{FacadeRecordQuery, FacadeValue, TestResult};

fn check<T: JsonSchema + DeserializeOwned + Serialize>(
    accepted: &[Value],
    rejected: &[Value],
) -> TestResult {
    let schema = TypeSchema::of::<T>();
    let input = jsonschema::validator_for(&serde_json::to_value(schema.deserialization)?)?;
    let output = jsonschema::validator_for(&serde_json::to_value(schema.serialization)?)?;
    for value in accepted {
        let decoded: T = serde_json::from_value(value.clone())
            .map_err(|error| format!("{} rejected {value}: {error}", std::any::type_name::<T>()))?;
        if !input.is_valid(value) {
            return Err(format!("input schema rejected {value}").into());
        }
        let encoded = serde_json::to_value(decoded)?;
        if !output.is_valid(&encoded) || !input.is_valid(&encoded) {
            return Err(format!("schema rejected emitted {encoded}").into());
        }
        serde_json::from_value::<T>(encoded)?;
    }
    for value in rejected {
        if input.is_valid(value) || serde_json::from_value::<T>(value.clone()).is_ok() {
            return Err(format!("schema or decoder accepted malformed {value}").into());
        }
    }
    Ok(())
}

#[test]
fn schemas_match_custom_filter_operators_instead_of_rust_enum_tags() -> TestResult {
    check::<IdFilter<String>>(
        &[
            json!("id"),
            json!({"$in": []}),
            json!({"$in": ["a", "a", "b"]}),
        ],
        &[
            json!({"Eq": "id"}),
            json!({"In": []}),
            json!({"$in": "a"}),
            json!({"$in": [], "extra": true}),
        ],
    )?;
    check::<StringFilter>(
        &[
            json!("text"),
            json!({"$in": ["a"]}),
            json!({"$contains": "a"}),
            json!({"$startsWith": "a"}),
        ],
        &[
            json!({"Contains": "a"}),
            json!({"$range": {"min": "a"}}),
            json!({"$contains": 1}),
        ],
    )?;
    check::<NumericFilter<u16>>(
        &[
            json!(12),
            json!({"$in": [1, 2, 2]}),
            json!({"$range": {}}),
            json!({"$range": {"min": 1}}),
            json!({"$range": {"min": null, "max": 3}}),
            json!({"$range": {"min": 1, "unknown": true}}),
        ],
        &[
            json!(-1),
            json!(65536),
            json!({"Range": {"min": 1}}),
            json!({"$range": {"min": "a"}}),
        ],
    )?;
    check::<EqFilter<FacadeValue>>(
        &[
            json!({"Text": "a"}),
            json!({"Count": 4}),
            json!({"$in": [{"Text": "a"}]}),
        ],
        &[
            json!({"Eq": {"Text": "a"}}),
            json!({"$in": [{"Count": "a"}]}),
        ],
    )?;
    Ok(())
}

#[test]
fn generated_item_filters_preserve_optional_and_unfilterable_fields() -> TestResult {
    check::<FacadeRecordQuery>(
        &[
            json!({}),
            json!({"id": "record"}),
            json!({"id": {"$in": ["a", "b"]}}),
            json!({"data": null}),
        ],
        &[json!({"id": 1}), json!({"data": {"count": 4}})],
    )
}

#[test]
fn framework_entity_reference_filters_keep_their_serialized_field_names() -> TestResult {
    check::<EqFilter<myko::graph::EntityRef>>(
        &[
            json!({"entityType": "Record", "id": "a"}),
            json!({"$in": [{"entityType": "Record", "id": "a"}]}),
        ],
        &[
            json!({"entity_type": "Record", "id": "a"}),
            json!({"entityType": "Record", "id": 1}),
        ],
    )
}

#[test]
fn numeric_bounds_accept_null_and_unknown_fields_but_do_not_emit_them() -> TestResult {
    let schema = TypeSchema::of::<NumericFilter<u16>>();
    let emitted = jsonschema::validator_for(&serde_json::to_value(schema.serialization)?)?;
    for value in [
        json!({"$range": {"min": null}}),
        json!({"$range": {"extra": 1}}),
    ] {
        let decoded: NumericFilter<u16> = serde_json::from_value(value.clone())?;
        if emitted.is_valid(&value) || serde_json::to_value(decoded)? != json!({"$range": {}}) {
            return Err(
                format!("output schema or encoder retained absent/unknown bound: {value}").into(),
            );
        }
    }
    Ok(())
}
