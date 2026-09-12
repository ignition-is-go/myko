use myko_wire::{NodeFrame, NodeRequest};
use serde_json::json;

#[test]
fn follow_handler_retains_the_observed_contract_precondition()
-> Result<(), Box<dyn std::error::Error>> {
    let request = json!({
        "type": "follow_handler",
        "request": {
            "kind": "report", "service_id": "records", "handler_id": "Count",
            "source_node": null, "scope_id": "records:one", "params": {}
        },
        "observed_contract": {
            "serving_node": "00000000-0000-0000-0000-000000000001",
            "service_id": "records", "kind": "report", "handler_id": "Count",
            "arguments": {"serialization": true, "deserialization": true},
            "result": {"kind": "value", "schema": {
                "serialization": {"type": "integer"}, "deserialization": true
            }}
        }
    });
    let decoded: NodeRequest = serde_json::from_value(request.clone())?;
    if serde_json::to_value(decoded)? != request {
        return Err("observed contract was discarded before handler admission".into());
    }
    Ok(())
}

#[test]
fn describe_handler_uses_the_canonical_request_and_response()
-> Result<(), Box<dyn std::error::Error>> {
    let request = json!({
        "type": "describe_handler",
        "request": {
            "kind": "report",
            "service_id": "records",
            "handler_id": "Count",
            "source_node": null,
            "scope_id": "records:one",
            "params": {}
        }
    });
    let decoded: NodeRequest = serde_json::from_value(request.clone())?;
    if decoded.kind() != "describe_handler" || serde_json::to_value(decoded)? != request {
        return Err("describe request changed across encoding".into());
    }

    let schema = json!({"serialization": {"type": "integer"}, "deserialization": true});
    let response = json!({
        "type": "handler_contract",
        "contract": {
            "serving_node": "00000000-0000-0000-0000-000000000001",
            "service_id": "records",
            "kind": "report",
            "handler_id": "Count",
            "arguments": {"serialization": {"type": "object"}, "deserialization": false},
            "result": {"kind": "value", "schema": schema}
        }
    });
    let decoded: NodeFrame = serde_json::from_value(response.clone())?;
    if decoded.kind() != "handler_contract" || serde_json::to_value(decoded)? != response {
        return Err("descriptor response changed across encoding".into());
    }
    Ok(())
}

#[test]
fn descriptor_validation_rejects_identity_and_result_substitution()
-> Result<(), Box<dyn std::error::Error>> {
    use myko_federation::{HandlerKind, NodeId, ServiceId};
    use myko_wire::{
        HandlerContract, HandlerRequest, HandlerResultContract, SchemaDocument, TypeSchemaPair,
    };

    let request = HandlerRequest {
        kind: HandlerKind::Report,
        service_id: Some(ServiceId::new("records")),
        handler_id: "Count".to_owned(),
        source_node: None,
        scope_id: None,
        params: json!({}),
    };
    let node = NodeId::new();
    let pair = TypeSchemaPair {
        serialization: SchemaDocument::Boolean(true),
        deserialization: SchemaDocument::Boolean(false),
    };
    let contract = HandlerContract {
        serving_node: node,
        service_id: request.service_id.clone(),
        kind: request.kind,
        handler_id: request.handler_id.clone(),
        arguments: pair.clone(),
        result: HandlerResultContract::Value(pair.clone()),
    };
    contract.validate_for(&request, Some(node))?;
    let variants = [
        HandlerContract {
            serving_node: NodeId::new(),
            ..contract.clone()
        },
        HandlerContract {
            service_id: None,
            ..contract.clone()
        },
        HandlerContract {
            kind: HandlerKind::View,
            ..contract.clone()
        },
        HandlerContract {
            handler_id: "Other".to_owned(),
            ..contract.clone()
        },
        HandlerContract {
            result: HandlerResultContract::Rows(pair),
            ..contract
        },
    ];
    for different in variants {
        if different.validate_for(&request, Some(node)).is_ok() {
            return Err("descriptor substitution was accepted".into());
        }
    }
    Ok(())
}

#[test]
fn schema_documents_reject_non_schema_roots() {
    use myko_wire::SchemaDocument;

    for value in [json!(null), json!(5), json!("schema"), json!([])] {
        assert!(SchemaDocument::try_from(value.clone()).is_err());
        assert!(serde_json::from_value::<SchemaDocument>(value).is_err());
    }
}
