use myko_federation::{
    AccessAttempt, AccessOperation, AuthorityUnavailable, AuthorizationDecision, PrincipalId,
};

use super::*;

#[test]
fn handler_authorization_preserves_denial() {
    let principal = PrincipalId::new("reader");
    let request = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal),
        AccessOperation::FollowHandler,
        ScopeId::new("protected"),
    );
    let decision = AuthorizationDecision::from_rule(&request, Err("revoked".to_owned()));
    let mapped = iroh_handler_error(authorization_error(Box::new(decision.clone())));
    assert!(matches!(mapped, HandlerClientError::Authorization(actual) if *actual == decision));
}

#[test]
fn handler_authorization_preserves_unavailability() {
    let reason = AuthorityUnavailable::CoordinationUnavailable;
    let mapped = iroh_handler_error(IrohReplicationError::AuthorityUnavailable(reason));
    assert!(matches!(mapped, HandlerClientError::AuthorityUnavailable(actual) if actual == reason));
}
