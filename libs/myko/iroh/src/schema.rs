//! JSON schema adapters for external Iroh types without `JsonSchema` support.
//!
//! These describe the Iroh 1.1 address representation. Public-key curve validity
//! and URL/socket parsing still belong to Iroh's decoders, not schema comparison.
//! Custom bytes use arrays. Raw JSON's extra byte-string acceptance is omitted
//! because decoding a parsed JSON value rejects that representation.

use std::net::SocketAddr;

use schemars::{Schema, SchemaGenerator, json_schema};

// A dependency upgrade must not change the external wire type beneath this adapter.
const _: fn(iroh::EndpointAddr) -> iroh_base::EndpointAddr = |address| address;

/// Structural JSON key encoding, without cryptographic validity checks.
#[must_use]
pub fn endpoint_id_schema(generator: &mut SchemaGenerator) -> Schema {
    let pattern = if generator.contract().is_serialize() {
        "^[0-9a-f]{64}$"
    } else {
        "^([0-9a-f]{64}|[A-Za-z2-7]{51}[AaQq])$"
    };
    json_schema!({"type": "string", "pattern": pattern})
}

/// Structural JSON contract for `iroh::EndpointAddr`, including custom transports.
///
/// Binary Serde encodings are outside this JSON contract. Iroh emits lowercase
/// hex keys but also accepts unpadded base32. Its address set accepts duplicate
/// inputs even though serialization emits unique entries.
#[must_use]
pub fn endpoint_addr_schema(generator: &mut SchemaGenerator) -> Schema {
    let emitting = generator.contract().is_serialize();
    let mut custom = json_schema!({
        "type": "object",
        "properties": {
            "id": generator.subschema_for::<u64>(),
            "data": generator.subschema_for::<Vec<u8>>()
        },
        "required": ["id", "data"]
    });
    if emitting {
        custom.insert("additionalProperties".into(), false.into());
    }
    let mut addresses = json_schema!({
        "type": "array",
        "items": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {"Relay": {"type": "string", "format": "uri"}},
                    "required": ["Relay"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": {"Ip": generator.subschema_for::<SocketAddr>()},
                    "required": ["Ip"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": {"Custom": custom},
                    "required": ["Custom"],
                    "additionalProperties": false
                }
            ]
        }
    });
    if emitting {
        addresses.insert("uniqueItems".into(), true.into());
    }
    let mut address = json_schema!({
        "type": "object",
        "properties": {
            "id": endpoint_id_schema(generator),
            "addrs": addresses
        },
        "required": ["id", "addrs"]
    });
    if emitting {
        address.insert("additionalProperties".into(), false.into());
    }
    address
}
