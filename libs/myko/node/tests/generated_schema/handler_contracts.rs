use std::collections::BTreeSet;

use myko::server::HandlerRegistry;
use myko_items::{
    MykoService,
    schema::{HandlerResultSchema, TypeSchema},
};
use myko_node::{FederationService, NodeStatus, Peer, PeerReport, PeersView};
use serde_json::{Value, json};

use super::{FacadeRecord, FacadeRecordCount, FacadeService, GetFacadeRecordById, TestResult};

#[test]
fn activated_handlers_expose_typed_inputs_and_scalar_or_row_results() -> TestResult {
    let registry = HandlerRegistry::for_services(&BTreeSet::from([
        FacadeService::SERVICE_ID,
        FederationService::SERVICE_ID,
    ]));
    let query = registry
        .query(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetAllFacadeRecords",
        )
        .ok_or("missing query")?;
    let query_schema = query.payload_schema.ok_or("missing query schema")?();
    if query.service_id != Some(FacadeService::SERVICE_ID)
        || query_schema.result != HandlerResultSchema::Rows(TypeSchema::of::<FacadeRecord>())
    {
        return Err("query lost its service owner or typed row schema".into());
    }
    validate(&query_schema.arguments, &json!({}))?;

    let report = registry
        .report(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetFacadeRecordById",
        )
        .ok_or("missing report")?;
    let report_schema = report.payload_schema.ok_or("missing report schema")?();
    if report_schema.arguments != TypeSchema::of::<GetFacadeRecordById>()
        || report_schema.result
            != HandlerResultSchema::Value(TypeSchema::of::<Option<FacadeRecord>>())
    {
        return Err("report lost its typed input or optional scalar output".into());
    }
    validate(&report_schema.arguments, &json!({"id": "record"}))?;
    let count = registry
        .report(
            Some(FacadeService::SERVICE_ID.as_str()),
            "CountAllFacadeRecords",
        )
        .ok_or("missing count report")?;
    if count.payload_schema.ok_or("missing count schema")?().result
        != HandlerResultSchema::Value(TypeSchema::of::<FacadeRecordCount>())
    {
        return Err("generated count report lost its output schema".into());
    }

    let view = registry
        .view(Some(FederationService::SERVICE_ID.as_str()), "PeersView")
        .ok_or("missing view")?;
    let view_schema = view.payload_schema.ok_or("missing view schema")?();
    if view.service_id != Some(FederationService::SERVICE_ID)
        || view_schema.arguments != TypeSchema::of::<PeersView>()
        || view_schema.result != HandlerResultSchema::Rows(TypeSchema::of::<Peer>())
    {
        return Err("view lost its service owner or typed payload schema".into());
    }
    let native_report = registry
        .report(Some(FederationService::SERVICE_ID.as_str()), "PeerReport")
        .ok_or("missing native report")?;
    if native_report
        .payload_schema
        .ok_or("missing native report schema")?()
    .arguments
        != TypeSchema::of::<PeerReport>()
    {
        return Err("native report lost its argument schema".into());
    }
    let status = registry
        .view(
            Some(FederationService::SERVICE_ID.as_str()),
            "NodeStatusView",
        )
        .ok_or("missing status view")?;
    if status.payload_schema.ok_or("missing status schema")?().result
        != HandlerResultSchema::Rows(TypeSchema::of::<NodeStatus>())
    {
        return Err("generated view output lost its row schema".into());
    }
    Ok(())
}

#[test]
fn disabled_services_and_unowned_handlers_cannot_supply_execution_schema_evidence() -> TestResult {
    let registry = HandlerRegistry::for_services(&BTreeSet::new());
    if registry
        .query(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetAllFacadeRecords",
        )
        .is_some()
        || registry
            .query(Some(FederationService::SERVICE_ID.as_str()), "GetPeers")
            .is_some()
        || registry
            .report(
                Some(FacadeService::SERVICE_ID.as_str()),
                "GetFacadeRecordById",
            )
            .is_some()
        || registry
            .report(Some(FederationService::SERVICE_ID.as_str()), "PeerReport")
            .is_some()
        || registry
            .view(Some(FederationService::SERVICE_ID.as_str()), "PeersView")
            .is_some()
        || registry
            .view(
                Some(FederationService::SERVICE_ID.as_str()),
                "NodeStatusView",
            )
            .is_some()
    {
        return Err("inactive service exposed an executable handler".into());
    }
    // Legacy global handlers remain callable locally, but lack service-owned contracts.
    let global = registry
        .view(None, "ConnectedClients")
        .ok_or("missing global view")?;
    if global.service_id.is_some() || global.payload_schema.is_some() {
        return Err("unowned handler supplied service schema evidence".into());
    }
    Ok(())
}

#[test]
fn native_reactive_handler_schemas_resolve_all_nested_types() -> TestResult {
    let registry = HandlerRegistry::for_services(&BTreeSet::from([
        FederationService::SERVICE_ID,
        FacadeService::SERVICE_ID,
        myko_authority::AuthorityService::SERVICE_ID,
    ]));
    let schemas = registry
        .queries()
        .filter(|handler| handler.service_id.is_some())
        .map(|handler| handler.payload_schema)
        .chain(
            registry
                .reports()
                .filter(|handler| handler.service_id.is_some())
                .map(|handler| handler.payload_schema),
        )
        .chain(
            registry
                .views()
                .filter(|handler| handler.service_id.is_some())
                .map(|handler| handler.payload_schema),
        );
    for factory in schemas {
        let schema = factory.ok_or("activated service handler omitted its schema")?();
        let (HandlerResultSchema::Value(result) | HandlerResultSchema::Rows(result)) =
            schema.result;
        for ty in [schema.arguments, result] {
            jsonschema::validator_for(&serde_json::to_value(ty.serialization)?)?;
            jsonschema::validator_for(&serde_json::to_value(ty.deserialization)?)?;
        }
    }
    Ok(())
}

#[test]
fn registered_argument_schemas_match_typed_admission_parsing() -> TestResult {
    let registry = HandlerRegistry::for_services(&BTreeSet::from([
        FacadeService::SERVICE_ID,
        FederationService::SERVICE_ID,
    ]));
    let query = registry
        .query(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetFacadeRecordsByIds",
        )
        .ok_or("missing query")?;
    let report = registry
        .report(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetFacadeRecordById",
        )
        .ok_or("missing report")?;
    let view = registry
        .view(Some(FederationService::SERVICE_ID.as_str()), "PeersView")
        .ok_or("missing view")?;
    let node_json = json!("00000000-0000-0000-0000-000000000001");
    let local_node = serde_json::from_value(node_json.clone())?;
    for (factory, parse, valid, invalid) in [
        (
            query.payload_schema,
            query.authority,
            json!({"ids": ["record"]}),
            json!({"ids": [1]}),
        ),
        (
            report.payload_schema,
            report.authority,
            json!({"id": "record"}),
            json!({"id": 1}),
        ),
        (
            view.payload_schema,
            view.authority,
            json!({"sourceNode": node_json}),
            json!({"sourceNode": 1}),
        ),
    ] {
        let schema = factory.ok_or("missing argument schema")?().arguments;
        let validator = jsonschema::validator_for(&serde_json::to_value(schema.deserialization)?)?;
        if !validator.is_valid(&valid) {
            return Err(format!("schema rejected typed input {valid}").into());
        }
        parse(valid, local_node)?;
        if validator.is_valid(&invalid) || parse(invalid, local_node).is_ok() {
            return Err("schema or admission parser accepted an invalid argument".into());
        }
    }
    Ok(())
}

fn validate(schema: &TypeSchema, value: &Value) -> TestResult {
    for contract in [&schema.serialization, &schema.deserialization] {
        let validator = jsonschema::validator_for(&serde_json::to_value(contract)?)?;
        if !validator.is_valid(value) {
            return Err(format!("schema rejected {value}").into());
        }
    }
    Ok(())
}
