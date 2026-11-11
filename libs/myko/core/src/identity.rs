//! Stable application user references supplied by a host authentication provider.
//! These IDs carry no grants and are distinct from permission principal IDs.
pub use myko_auth::identity::UserId;

impl crate::query::Filterable for UserId {
    type Filter = crate::query::EqFilter<Self>;
}

crate::register_typegen_type!(UserId);

#[cfg(test)]
mod tests {
    use super::UserId;
    fn accepts_filter<T: crate::query::Filterable>() {}
    #[test]
    fn user_reference_has_a_scalar_wire_shape_and_typed_filter() {
        let id = UserId::from("server-issued-user");
        let wire = serde_json::json!("server-issued-user");
        assert_eq!(serde_json::to_value(&id).ok(), Some(wire.clone()));
        assert_eq!(serde_json::from_value::<UserId>(wire).ok(), Some(id));
        accepts_filter::<UserId>();
    }
}

#[cfg(test)]
mod tests_context {
    use super::UserId;
    use myko_auth::{
        AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, Principal,
        PrincipalKind,
    };
    use std::{
        sync::Arc,
        time::{Duration, SystemTime},
    };
    struct Policy;
    impl Authorizer for Policy {
        fn authorize(
            &self,
            _: &AuthenticatedIdentity,
            _: AuthorizationRequest<'_>,
        ) -> Result<(), AuthError> {
            Ok(())
        }
    }
    #[test]
    #[allow(
        clippy::panic_in_result_fn,
        reason = "assertions fail the test; Result propagates setup errors"
    )]
    fn request_user_is_typed_shared_and_requires_live_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = AuthSession::new(
            AuthenticatedIdentity {
                user_id: Some(UserId::from("shared-user")),
                principal: Principal {
                    issuer: "issuer".into(),
                    subject: "provider-subject".into(),
                    kind: PrincipalKind::User,
                },
                client_id: None,
                scopes: vec![],
                delegation: vec![],
                claims: serde_json::Value::Null,
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_hours(1))
                    .ok_or("expiry overflow")?,
            },
            Arc::new(Policy),
        );
        let request = crate::request::RequestContext::from_client(
            "transaction".into(),
            "socket-id".into(),
            uuid::Uuid::nil(),
        );
        assert_eq!(request.user_id(), Err(AuthError::Unauthenticated));
        let request = request.with_auth(Some(session.clone()));
        assert_eq!(request.user_id().ok(), Some(&UserId::from("shared-user")));
        assert_eq!(
            request.child("nested").user_id().ok(),
            request.user_id().ok()
        );
        session.revoke();
        assert_eq!(request.user_id(), Err(AuthError::Revoked));
        Ok(())
    }
}
