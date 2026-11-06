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

struct SessionAuthenticator {
    authenticated: AtomicU64,
    denied_session: std::sync::Mutex<Option<String>>,
    unavailable: AtomicBool,
}
impl Authenticator for SessionAuthenticator {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        self.authenticated.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            let session = credentials.bearer.ok_or(AuthError::Unauthenticated)?;
            let expires_at = SystemTime::now()
                .checked_add(Duration::from_mins(1))
                .ok_or(AuthError::Unavailable)?;
            let mut admitted = identity(expires_at);
            admitted.claims = serde_json::json!({ "session": session });
            Ok(admitted)
        })
    }
    fn validate_identity(&self, identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        if self.unavailable.load(Ordering::Acquire) {
            return Err(AuthError::Unavailable);
        }
        let denied = self
            .denied_session
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        if denied.as_deref().is_some_and(|session| {
            identity.claims.get("session").and_then(Value::as_str) == Some(session)
        }) {
            Err(AuthError::Revoked)
        } else {
            Ok(())
        }
    }
}
fn session_provider() -> (MykoAuthProvider, Arc<SessionAuthenticator>) {
    let authenticator = Arc::new(SessionAuthenticator {
        authenticated: AtomicU64::new(0),
        denied_session: std::sync::Mutex::new(None),
        unavailable: AtomicBool::new(false),
    });
    let policy = Arc::new(Policy {
        revision: AtomicU64::new(0),
        allow: AtomicBool::new(true),
    });
    (
        MykoAuthProvider::new(authenticator.clone(), policy),
        authenticator,
    )
}
fn admit(provider: &MykoAuthProvider, session: &str) -> Result<AuthSession, AuthError> {
    futures::executor::block_on(provider.authenticate(Credentials {
        bearer: Some(session.into()),
    }))
}
#[test]
#[allow(clippy::panic_in_result_fn)]
fn local_logout_revokes_only_one_credential_and_all_its_session_clones() -> Result<(), AuthError> {
    let (provider, authenticator) = session_provider();
    let first = admit(&provider, "first")?;
    let first_clone = first.clone();
    let second = admit(&provider, "second")?;
    assert_eq!(first.identity().principal, second.identity().principal);
    drop(provider);
    let request = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "GetThings",
    };
    assert_eq!(first.authorize(request), Ok(()));
    *authenticator
        .denied_session
        .lock()
        .map_err(|_| AuthError::Unavailable)? = Some("first".into());
    assert_eq!(first.authorize(request), Err(AuthError::Revoked));
    assert_eq!(first_clone.check(), Err(AuthError::Revoked));
    assert_eq!(second.authorize(request), Ok(()));
    *authenticator
        .denied_session
        .lock()
        .map_err(|_| AuthError::Unavailable)? = None;
    assert_eq!(first.check(), Err(AuthError::Revoked));
    assert_eq!(second.check(), Ok(()));
    assert_eq!(authenticator.authenticated.load(Ordering::Acquire), 2);
    Ok(())
}
#[test]
#[allow(clippy::panic_in_result_fn)]
fn local_liveness_denies_admission_and_fails_closed_during_outages() -> Result<(), AuthError> {
    let (provider, authenticator) = session_provider();
    *authenticator
        .denied_session
        .lock()
        .map_err(|_| AuthError::Unavailable)? = Some("logged-out".into());
    assert_eq!(
        admit(&provider, "logged-out").err(),
        Some(AuthError::Revoked)
    );
    let session = admit(&provider, "active")?;
    authenticator.unavailable.store(true, Ordering::Release);
    assert_eq!(session.check(), Err(AuthError::Unavailable));
    assert_eq!(
        session.authorize(AuthorizationRequest::Discover {
            kind: OperationKind::Query,
            operation: "GetThings",
        }),
        Err(AuthError::Unavailable)
    );
    authenticator.unavailable.store(false, Ordering::Release);
    assert_eq!(session.check(), Ok(()));
    Ok(())
}

#[test]
#[allow(clippy::panic_in_result_fn)]
fn identity_revocation_during_authorization_is_caught_before_success() -> Result<(), AuthError> {
    struct LogoutPolicy(Arc<SessionAuthenticator>);
    impl Authorizer for LogoutPolicy {
        fn authorize(
            &self,
            _: &AuthenticatedIdentity,
            _: AuthorizationRequest<'_>,
        ) -> Result<(), AuthError> {
            *self
                .0
                .denied_session
                .lock()
                .map_err(|_| AuthError::Unavailable)? = Some("active".into());
            Ok(())
        }
    }
    let (_, authenticator) = session_provider();
    let provider =
        MykoAuthProvider::new(authenticator.clone(), Arc::new(LogoutPolicy(authenticator)));
    let session = admit(&provider, "active")?;
    assert_eq!(
        session.authorize(AuthorizationRequest::Discover {
            kind: OperationKind::Query,
            operation: "GetThings",
        }),
        Err(AuthError::Revoked)
    );
    assert_eq!(session.check(), Err(AuthError::Revoked));
    Ok(())
}
