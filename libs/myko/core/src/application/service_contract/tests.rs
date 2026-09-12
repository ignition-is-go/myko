use super::*;

fn empty_contract() -> ServiceContract {
    ServiceContract {
        service_id: ServiceTypeId::new("test:contracts"),
        items: BTreeMap::new(),
        handlers: BTreeMap::new(),
    }
}

#[test]
fn missing_handler_schema_is_an_error_not_an_empty_contract() {
    let mut contract = empty_contract();
    let id = Arc::from("Missing");
    assert_eq!(
        contract.insert_handler(ServiceHandlerKind::Report, Arc::clone(&id), None),
        Err(ServiceContractError::MissingHandler {
            service_id: contract.service_id,
            kind: ServiceHandlerKind::Report,
            id,
        })
    );
    assert!(contract.handlers.is_empty());
}

#[test]
fn result_shape_must_match_the_executable_handler_kind() {
    let scalar: fn() -> HandlerPayloadSchema = HandlerPayloadSchema::value::<(), u16>;
    let rows: fn() -> HandlerPayloadSchema = HandlerPayloadSchema::rows::<(), u16>;
    for (kind, provider) in [
        (ServiceHandlerKind::Query, scalar),
        (ServiceHandlerKind::View, scalar),
        (ServiceHandlerKind::Report, rows),
        (ServiceHandlerKind::Command, rows),
    ] {
        let mut contract = empty_contract();
        let id = Arc::from("WrongShape");
        assert_eq!(
            contract.insert_handler(kind, Arc::clone(&id), Some(provider)),
            Err(ServiceContractError::InvalidResult {
                service_id: contract.service_id,
                kind,
                id,
            })
        );
        assert!(contract.handlers.is_empty());
    }
}

#[test]
fn duplicate_handlers_fail_but_different_kinds_keep_their_own_contracts() {
    let mut contract = empty_contract();
    let id = Arc::from("SameName");
    let schema: Option<fn() -> HandlerPayloadSchema> = Some(HandlerPayloadSchema::value::<(), u16>);
    assert_eq!(
        contract.insert_handler(ServiceHandlerKind::Report, Arc::clone(&id), schema),
        Ok(())
    );
    assert_eq!(
        contract.insert_handler(ServiceHandlerKind::Command, Arc::clone(&id), schema),
        Ok(())
    );
    assert_eq!(contract.handlers.len(), 2);
    assert_eq!(
        contract.insert_handler(ServiceHandlerKind::Command, Arc::clone(&id), schema),
        Err(ServiceContractError::DuplicateHandler {
            service_id: contract.service_id,
            kind: ServiceHandlerKind::Command,
            id,
        })
    );
}
