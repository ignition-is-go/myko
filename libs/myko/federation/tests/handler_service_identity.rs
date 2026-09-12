use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityPresentation, AuthorizationBinding,
    HandlerAccess, HandlerKind, PrincipalId, ScopeId, ServiceId,
};

#[test]
fn service_identity_is_bound_even_without_a_primary_scope() {
    let principal = PrincipalId::new("reader");
    let mut request = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal),
        AccessOperation::FollowHandler,
        ScopeId::new("unused"),
    );
    request.resource_claims.clear();
    let mut bindings = Vec::new();
    for service in [
        None,
        Some(ServiceId::new("one")),
        Some(ServiceId::new("two")),
    ] {
        request.target = AccessTarget::Handler {
            access: HandlerAccess {
                kind: HandlerKind::Report,
                service_id: service.clone(),
                handler_id: "Summary".to_owned(),
            },
            source_node: None,
            scope_id: None,
        };
        let binding = AuthorizationBinding::from_request(&request);
        assert_eq!(binding.service_id, service);
        bindings.push(binding);
    }
    for pair in bindings.windows(2) {
        assert_ne!(pair.first(), pair.last());
    }
}

#[test]
fn unowned_handler_encoding_does_not_invent_historical_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = br#"{"kind":"view","handler_id":"Rows"}"#;
    let access: HandlerAccess = serde_json::from_slice(encoded)?;
    if access.service_id.is_some() || serde_json::to_vec(&access)? != encoded {
        return Err("unowned handler identity changed during replay".into());
    }
    Ok(())
}
