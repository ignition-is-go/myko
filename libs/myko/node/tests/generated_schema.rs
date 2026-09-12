#![cfg(feature = "schema")]

use std::error::Error;

use myko_authority::{AuthorityRealm, AuthorityService};
use myko_items::{MykoService, schema::TypeSchema};
use myko_node::{FederationService, Peer};
use serde_json::json;

type TestResult = Result<(), Box<dyn Error>>;

#[path = "generated_schema/command_contracts.rs"]
mod command_contracts;
#[path = "generated_schema/command_namespaces.rs"]
mod command_namespaces;
#[path = "generated_schema/filter_contracts.rs"]
mod filter_contracts;
#[path = "generated_schema/handler_contracts.rs"]
mod handler_contracts;
#[path = "generated_schema/service_contracts.rs"]
mod service_contracts;

#[myko_items::myko_service(FacadeRecord)]
pub struct FacadeService;

#[myko::myko_subtype(derive(Eq))]
pub enum FacadeValue {
    Text(String),
    Count(u16),
}

#[myko::myko_item(service = FacadeService, scope_root)]
pub struct FacadeRecord {
    pub data: FacadeValue,
}

#[test]
fn runtime_facade_macros_generate_item_id_and_nested_subtype_schemas() -> TestResult {
    let record = FacadeRecord {
        id: FacadeRecordId::from("record"),
        data: FacadeValue::Count(12),
    };
    let items = FacadeService::item_schemas().ok_or("missing facade item schemas")?;
    let [item] = items.as_slice() else {
        return Err("facade service did not generate exactly one item schema".into());
    };
    let encoded = serde_json::to_value(record)?;
    let validator = jsonschema::validator_for(&serde_json::to_value(&item.value.serialization)?)?;
    if !validator.is_valid(&encoded) || validator.is_valid(&json!({"id": 7, "data": false})) {
        return Err("facade schema lost its typed fields".into());
    }
    Ok(())
}

#[test]
fn native_service_item_schemas_have_resolvable_transitive_contracts() -> TestResult {
    let authority = AuthorityService::item_schemas().ok_or("missing authority item schemas")?;
    let federation = FederationService::item_schemas().ok_or("missing federation item schemas")?;
    if authority.len() != 12 || federation.len() != 7 {
        return Err("native service schema omitted an item module".into());
    }
    for item in authority.into_iter().chain(federation) {
        for schema in [item.value.serialization, item.value.deserialization] {
            let value = serde_json::to_value(schema)?;
            jsonschema::validator_for(&value)?;
        }
    }
    Ok(())
}

#[test]
fn native_payloads_validate_without_replacing_framework_types() -> TestResult {
    let realm: AuthorityRealm = serde_json::from_value(json!({
        "id": "realm",
        "bootstrapPrincipal": {"id": "node:example", "kind": "node"},
        "bootstrappedAt": "2026-09-06T00:00:00Z"
    }))?;
    let peer: Peer = serde_json::from_value(json!({
        "id": "peer", "peerRosterId": "roster",
        "endpoint": {"id": "00".repeat(32), "addrs": []},
        "sourceNode": null, "replicationEnabled": true
    }))?;
    for (schema, value) in [
        (
            TypeSchema::of::<AuthorityRealm>(),
            serde_json::to_value(realm)?,
        ),
        (TypeSchema::of::<Peer>(), serde_json::to_value(peer)?),
    ] {
        for contract in [schema.serialization, schema.deserialization] {
            let validator = jsonschema::validator_for(&serde_json::to_value(contract)?)?;
            if !validator.is_valid(&value) {
                return Err("generated schema rejected an actual native payload".into());
            }
        }
    }
    Ok(())
}
