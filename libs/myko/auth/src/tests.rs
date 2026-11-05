use super::*;
use std::{sync::atomic::AtomicU64, time::Duration};

struct Policy {
    revision: AtomicU64,
    allow: AtomicBool,
}
impl Authorizer for Policy {
    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        if self.allow.load(Ordering::Acquire) && identity.scopes.iter().any(|scope| scope == "read")
        {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
    fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
}
fn identity(expires_at: SystemTime) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        principal: Principal {
            issuer: "issuer".into(),
            subject: "service-1".into(),
            kind: PrincipalKind::Service,
        },
        client_id: Some("client".into()),
        scopes: vec!["read".into()],
        delegation: vec![],
        claims: Value::Null,
        expires_at,
    }
}
#[test]
fn policy_is_local_and_live_and_invalidation_is_permanent() {
    let policy = Arc::new(Policy {
        revision: AtomicU64::new(0),
        allow: AtomicBool::new(true),
    });
    let session = AuthSession::new(
        identity(SystemTime::now() + Duration::from_mins(1)),
        policy.clone(),
    );
    let request = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "GetThings",
    };
    assert_eq!(session.authorize(request), Ok(()));
    policy.allow.store(false, Ordering::Release);
    assert_eq!(session.authorize(request), Err(AuthError::Forbidden));
    policy.revision.store(1, Ordering::Release);
    assert_eq!(session.check(), Err(AuthError::Revoked));
    policy.revision.store(0, Ordering::Release);
    assert_eq!(session.check(), Err(AuthError::Revoked));
}
#[test]
fn expiry_and_explicit_revocation_fail_closed() {
    let policy = Arc::new(Policy {
        revision: AtomicU64::new(0),
        allow: AtomicBool::new(true),
    });
    let expired = AuthSession::new(identity(SystemTime::UNIX_EPOCH), policy.clone());
    assert_eq!(expired.check(), Err(AuthError::Expired));
    let active = AuthSession::new(identity(SystemTime::now() + Duration::from_mins(1)), policy);
    let clone = active.clone();
    active.revoke();
    assert_eq!(clone.check(), Err(AuthError::Revoked));
    assert_ne!(active.id(), expired.id());
}
