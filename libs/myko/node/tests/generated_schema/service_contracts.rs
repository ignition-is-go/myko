use myko::{
    MykoApplication,
    application::{ServiceContractError, ServiceHandlerKind},
};
use myko_authority::AuthorityService;
use myko_items::{
    MykoItem, MykoService, ServiceTypeId,
    schema::{HandlerResultSchema, ItemSchema, TypeSchema},
};
use myko_node::{FederationService, Peer};

use super::{FacadeRecord, FacadeService, FacadeValue, TestResult};

struct EmptyService;
impl MykoService for EmptyService {
    type Items = ();
    const SERVICE_ID: ServiceTypeId = ServiceTypeId::new("test:empty-service");

    fn item_schemas() -> Option<Vec<ItemSchema>> {
        Some(Vec::new())
    }
}

struct ManualService;
impl MykoService for ManualService {
    type Items = ();
    const SERVICE_ID: ServiceTypeId = ServiceTypeId::new("test:manual-service");
}

struct MissingAlias;
impl MykoService for MissingAlias {
    type Items = ();
    const SERVICE_ID: ServiceTypeId = FacadeService::SERVICE_ID;
}

struct ForeignItems;
impl MykoService for ForeignItems {
    type Items = (FacadeRecord,);
    const SERVICE_ID: ServiceTypeId = ServiceTypeId::new("test:foreign-items");

    fn item_schemas() -> Option<Vec<ItemSchema>> {
        FacadeService::item_schemas()
    }
}

struct DuplicateItems;
impl MykoService for DuplicateItems {
    type Items = (FacadeRecord, FacadeRecord);
    const SERVICE_ID: ServiceTypeId = FacadeService::SERVICE_ID;

    fn item_schemas() -> Option<Vec<ItemSchema>> {
        Some(vec![ItemSchema::of::<FacadeRecord>(); 2])
    }
}

#[test]
fn activated_contracts_combine_items_and_all_handler_kinds() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .service::<FederationService>()
        .service::<AuthorityService>()
        .build();
    let facade = app.service_contract(FacadeService::SERVICE_ID)?;
    if facade.service_id != FacadeService::SERVICE_ID
        || facade.items.get(FacadeRecord::ITEM_TYPE) != Some(&ItemSchema::of::<FacadeRecord>())
    {
        return Err("contract lost its generated item identity or payload schema".into());
    }
    for (kind, name, result) in [
        (
            ServiceHandlerKind::Query,
            "GetAllFacadeRecords",
            HandlerResultSchema::Rows(TypeSchema::of::<FacadeRecord>()),
        ),
        (
            ServiceHandlerKind::Report,
            "GetFacadeRecordById",
            HandlerResultSchema::Value(TypeSchema::of::<Option<FacadeRecord>>()),
        ),
        (
            ServiceHandlerKind::Command,
            "EchoValue",
            HandlerResultSchema::Value(TypeSchema::of::<FacadeValue>()),
        ),
    ] {
        let handler = facade
            .handlers
            .get(&(kind, name.into()))
            .ok_or("missing handler contract")?;
        if handler.result != result {
            return Err(format!("wrong output schema for {name}").into());
        }
    }
    let federation = app.service_contract(FederationService::SERVICE_ID)?;
    let view = federation
        .handlers
        .get(&(ServiceHandlerKind::View, "PeersView".into()))
        .ok_or("missing view contract")?;
    if view.result != HandlerResultSchema::Rows(TypeSchema::of::<Peer>()) {
        return Err("view contract lost its row type".into());
    }
    for service in app.services() {
        let contract = app.service_contract(service)?;
        if contract
            .handlers
            .keys()
            .any(|(_, id)| id.as_ref() == "ConnectedClients")
        {
            return Err("global handler leaked into a service contract".into());
        }
        for handler in contract.handlers.values() {
            let (HandlerResultSchema::Value(result) | HandlerResultSchema::Rows(result)) =
                &handler.result;
            for schema in [&handler.arguments, result] {
                jsonschema::validator_for(&serde_json::to_value(&schema.serialization)?)?;
                jsonschema::validator_for(&serde_json::to_value(&schema.deserialization)?)?;
            }
        }
    }
    Ok(())
}

#[test]
fn inactive_and_identity_only_services_cannot_borrow_inventory_schema_evidence() -> TestResult {
    let inactive = MykoApplication::new();
    if inactive.service_contract(FacadeService::SERVICE_ID)
        != Err(ServiceContractError::InactiveService {
            service_id: FacadeService::SERVICE_ID,
        })
    {
        return Err("inactive service borrowed linked schema evidence".into());
    }
    let identity_only = MykoApplication::builder()
        .service_id(FacadeService::SERVICE_ID)
        .build();
    if identity_only
        .handlers()
        .query(
            Some(FacadeService::SERVICE_ID.as_str()),
            "GetAllFacadeRecords",
        )
        .is_none()
    {
        return Err("fixture no longer exercises an activated raw identity".into());
    }
    if identity_only.service_contract(FacadeService::SERVICE_ID)
        != Err(ServiceContractError::MissingItems {
            service_id: FacadeService::SERVICE_ID,
        })
    {
        return Err("raw identity acquired typed activation evidence".into());
    }
    Ok(())
}

#[test]
fn missing_item_evidence_is_distinct_from_an_explicit_empty_contract() -> TestResult {
    let app = MykoApplication::builder()
        .service::<ManualService>()
        .service::<EmptyService>()
        .build();
    if app.service_contract(ManualService::SERVICE_ID)
        != Err(ServiceContractError::MissingItems {
            service_id: ManualService::SERVICE_ID,
        })
    {
        return Err("manual service's missing schemas became an empty contract".into());
    }
    let empty = app.service_contract(EmptyService::SERVICE_ID)?;
    if !empty.items.is_empty() || !empty.handlers.is_empty() {
        return Err("explicit empty service borrowed another service's handlers".into());
    }
    Ok(())
}

#[test]
fn an_uncontracted_type_cannot_inherit_another_types_schema_by_identity() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .service::<MissingAlias>()
        .build();
    if app.service_contract(MissingAlias::SERVICE_ID)
        != Err(ServiceContractError::MissingItems {
            service_id: MissingAlias::SERVICE_ID,
        })
    {
        return Err("uncontracted type reused earlier schema evidence for the same ID".into());
    }
    Ok(())
}

#[test]
fn service_contract_rejects_foreign_and_duplicate_item_metadata() -> TestResult {
    let foreign = MykoApplication::builder().service::<ForeignItems>().build();
    if foreign.service_contract(ForeignItems::SERVICE_ID)
        != Err(ServiceContractError::ForeignItem {
            service_id: ForeignItems::SERVICE_ID,
            item_type: FacadeRecord::ITEM_TYPE,
            owner: FacadeService::SERVICE_ID,
        })
    {
        return Err("foreign item metadata became a local service contract".into());
    }
    let duplicate = MykoApplication::builder()
        .service::<DuplicateItems>()
        .build();
    if duplicate.service_contract(DuplicateItems::SERVICE_ID)
        != Err(ServiceContractError::DuplicateItem {
            service_id: DuplicateItems::SERVICE_ID,
            item_type: FacadeRecord::ITEM_TYPE,
        })
    {
        return Err("duplicate item metadata silently replaced an item contract".into());
    }
    Ok(())
}

#[test]
fn adding_framework_services_preserves_application_contracts() -> TestResult {
    let app = MykoApplication::builder()
        .service::<FacadeService>()
        .build();
    let before = app.service_contract(FacadeService::SERVICE_ID)?;
    let extended = app.with_framework_service::<FederationService>();
    if extended.service_contract(FacadeService::SERVICE_ID)? != before {
        return Err("framework composition dropped or changed the application contract".into());
    }
    let direct = MykoApplication::builder()
        .service::<FederationService>()
        .build();
    if extended.service_contract(FederationService::SERVICE_ID)?
        != direct.service_contract(FederationService::SERVICE_ID)?
    {
        return Err("framework activation did not retain the same typed service contract".into());
    }
    let with_resource = extended.with_framework_resource_capability::<u32>(
        myko_federation::ApplicationCapability {
            id: myko_federation::CapabilityId::new("test:contract-resource"),
            description: "test resource".to_owned(),
            constraints: myko_federation::AuthorityConstraints::default(),
        },
    )?;
    if with_resource.service_contract(FacadeService::SERVICE_ID)? != before
        || with_resource.service_contract(FederationService::SERVICE_ID)?
            != direct.service_contract(FederationService::SERVICE_ID)?
    {
        return Err("resource composition dropped a typed service contract".into());
    }
    Ok(())
}
