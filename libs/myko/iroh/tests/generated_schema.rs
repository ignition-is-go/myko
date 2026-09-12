#![cfg(feature = "schema")]

use std::error::Error;

use jsonschema::Validator;
use myko_iroh::NativeNodeDescriptor;
use myko_items::schema::TypeSchema;
use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn Error>>;

fn validators() -> Result<(Validator, Validator), Box<dyn Error>> {
    let schemas = TypeSchema::of::<NativeNodeDescriptor>();
    Ok((
        jsonschema::validator_for(&serde_json::to_value(schemas.serialization)?)?,
        jsonschema::validator_for(&serde_json::to_value(schemas.deserialization)?)?,
    ))
}

fn descriptor() -> Value {
    json!({
        "version": 1,
        "node_id": "00000000-0000-0000-0000-000000000001",
        "endpoint": {
            "id": "00".repeat(32),
            "addrs": [
                {"Relay": "https://relay.example.invalid/"},
                {"Ip": "127.0.0.1:1234"},
                {"Ip": "[::1]:1234"},
                {"Custom": {"id": 42, "data": [0, 1, 255]}}
            ]
        }
    })
}

#[test]
fn schemas_accept_actual_iroh_json_for_every_address_variant() -> TestResult {
    let (emitted, accepted) = validators()?;
    let value = descriptor();
    let decoded: NativeNodeDescriptor = serde_json::from_value(value.clone())?;
    let encoded = serde_json::to_value(decoded)?;
    if !accepted.is_valid(&value) || !emitted.is_valid(&encoded) || !accepted.is_valid(&encoded) {
        return Err("schema rejected Iroh's serialized address representation".into());
    }
    Ok(())
}

#[test]
fn schemas_preserve_asymmetric_key_and_set_contracts() -> TestResult {
    let (emitted, accepted) = validators()?;
    for key in ["a".repeat(52), "A".repeat(52)] {
        let mut value = descriptor();
        *value.pointer_mut("/endpoint/id").ok_or("missing id")? = json!(key);
        let decoded: NativeNodeDescriptor = serde_json::from_value(value.clone())?;
        if !accepted.is_valid(&value) || emitted.is_valid(&value) {
            return Err("base32 input was confused with emitted hex".into());
        }
        if !emitted.is_valid(&serde_json::to_value(decoded)?) {
            return Err("Iroh's canonical re-encoding did not match emitted schema".into());
        }
    }
    let mut value = descriptor();
    let addresses = value
        .pointer_mut("/endpoint/addrs")
        .ok_or("missing addrs")?;
    *addresses = json!([{"Ip": "127.0.0.1:1234"}, {"Ip": "127.0.0.1:1234"}]);
    let decoded: NativeNodeDescriptor = serde_json::from_value(value.clone())?;
    if !accepted.is_valid(&value) || emitted.is_valid(&value) || decoded.endpoint.addrs.len() != 1 {
        return Err("set schema lost duplicate-input/canonical-output distinction".into());
    }
    Ok(())
}

#[test]
fn malformed_address_shapes_fail_schema_and_real_decoder() -> TestResult {
    let (_, accepted) = validators()?;
    for addresses in [
        json!([{"Unsupported": "value"}]),
        json!([{"Custom": {"id": 42, "data": [256]}}]),
        json!([{"Custom": {"id": 42}}]),
        json!([{"Ip": 1234}]),
        json!([{"Ip": "127.0.0.1:1234", "Relay": "https://relay.example.invalid/"}]),
    ] {
        let mut value = descriptor();
        *value
            .pointer_mut("/endpoint/addrs")
            .ok_or("missing addrs")? = addresses;
        if accepted.is_valid(&value)
            || serde_json::from_value::<NativeNodeDescriptor>(value).is_ok()
        {
            return Err("malformed transport shape was accepted".into());
        }
    }
    Ok(())
}

#[test]
fn unknown_struct_fields_are_accepted_but_never_emitted() -> TestResult {
    let (emitted, accepted) = validators()?;
    let mut value = descriptor();
    value
        .pointer_mut("/endpoint")
        .and_then(Value::as_object_mut)
        .ok_or("missing endpoint")?
        .insert("future_field".to_owned(), json!(true));
    let decoded: NativeNodeDescriptor = serde_json::from_value(value.clone())?;
    if !accepted.is_valid(&value) || emitted.is_valid(&value) {
        return Err("unknown-field acceptance was confused with emitted structure".into());
    }
    if !emitted.is_valid(&serde_json::to_value(decoded)?) {
        return Err("canonical re-encoding failed its schema".into());
    }
    Ok(())
}

#[test]
fn custom_byte_strings_are_not_portable_between_json_decoder_paths() -> TestResult {
    let (emitted, accepted) = validators()?;
    let mut value = descriptor();
    *value
        .pointer_mut("/endpoint/addrs")
        .ok_or("missing addrs")? = json!([{"Custom": {"id": 42, "data": "abc"}}]);
    let wire = serde_json::to_vec(&value)?;
    let decoded: NativeNodeDescriptor = serde_json::from_slice(&wire)?;
    if serde_json::from_value::<NativeNodeDescriptor>(value.clone()).is_ok()
        || accepted.is_valid(&value)
    {
        return Err("decoder-specific byte strings were treated as portable inputs".into());
    }
    let canonical = serde_json::to_value(decoded)?;
    if !emitted.is_valid(&canonical) || !accepted.is_valid(&canonical) {
        return Err("canonical byte-array encoding failed the portable schema".into());
    }
    Ok(())
}
