#![allow(
    clippy::panic_in_result_fn,
    reason = "assertions report test failures while setup errors propagate"
)]

use super::*;
use crate::{MemoryPermissionStorage, OperatorPrincipal, bootstrap, replace_as_operator};
use myko_auth::{Principal, ScopeDecision};
use std::time::{Duration, SystemTime};

fn identity(subject: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: None,
        principal: Principal {
            issuer: "https://login.example/".into(),
            subject: subject.into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec![],
        delegation: vec![],
        claims: serde_json::json!({"verified_email": "owner@example.com"}),
        expires_at: SystemTime::now()
            .checked_add(Duration::from_mins(1))
            .unwrap_or(SystemTime::UNIX_EPOCH),
    }
}

#[test]
fn only_explicit_verified_user_email_can_bootstrap() -> PermissionResult<()> {
    let storage = MemoryPermissionStorage::new();
    let config = AdminEmail::new(" OWNER@example.com ")?;
    let mut user = identity("owner");
    user.claims = serde_json::json!({"email": "owner@example.com", "email_verified": true});
    assert!(!config.ensure(&storage, &user)?);
    user.claims = serde_json::json!({"verified_email": "other@example.com"});
    assert!(!config.ensure(&storage, &user)?);
    user.claims = serde_json::json!({"verified_email": "owner@example.com"});
    user.principal.kind = PrincipalKind::Service;
    assert!(!config.ensure(&storage, &user)?);
    user.principal.kind = PrincipalKind::User;
    user.delegation.push(user.principal.clone());
    assert!(!config.ensure(&storage, &user)?);
    assert!(storage.load()?.is_none());
    user.delegation.clear();
    assert!(config.ensure(&storage, &user)?);
    let stored = storage.load()?.ok_or(PermissionError::Detached)?;
    let role = stored
        .document
        .roles
        .first()
        .ok_or(PermissionError::Detached)?;
    assert_eq!(role.name, "Admin");
    for scope in PermissionCatalog::registered().scopes() {
        assert!(role.rules.contains(&PermissionRule {
            scope: scope.to_string(),
            effect: PermissionEffect::Allow
        }));
        assert_eq!(
            crate::engine::explicit_decision(&stored.document, &user, scope),
            ScopeDecision::Allow
        );
    }
    assert!(config.ensure(&storage, &user)?);
    assert_eq!(storage.load()?.ok_or(PermissionError::Detached)?, stored);
    Ok(())
}

#[test]
fn assignment_is_restored_and_other_people_can_share_admin() -> PermissionResult<()> {
    let storage = MemoryPermissionStorage::new();
    let config = AdminEmail::new("owner@example.com")?;
    let owner = identity("password-owner");
    config.ensure(&storage, &owner)?;
    let first = storage.load()?.ok_or(PermissionError::Detached)?;
    // Another trusted login identity for the configured email receives the same role.
    config.ensure(&storage, &identity("otp-owner"))?;
    let second = storage.load()?.ok_or(PermissionError::Detached)?;
    assert_eq!(second.document.roles, first.document.roles);
    assert_eq!(second.document.bindings.len(), 2);
    let mut removed = second.document;
    let owner_id = first
        .document
        .principals
        .first()
        .ok_or(PermissionError::Detached)?
        .id
        .clone();
    removed
        .bindings
        .retain(|binding| binding.grantee != PermissionGrantee::Principal(owner_id.clone()));
    replace_as_operator(
        &storage,
        second.revision,
        removed,
        "test operator",
        "Remove configured owner assignment",
        false,
    )?;
    config.ensure(&storage, &owner)?;
    let restored = storage.load()?.ok_or(PermissionError::Detached)?;
    assert_eq!(restored.document.bindings.len(), 2);
    assert_eq!(restored.document.roles, first.document.roles);
    Ok(())
}

#[test]
fn legacy_role_is_upgraded_with_existing_assignments_preserved() -> PermissionResult<()> {
    let storage = MemoryPermissionStorage::new();
    let owner = identity("owner");
    bootstrap(
        &storage,
        OperatorPrincipal {
            issuer: owner.principal.issuer.clone(),
            subject: owner.principal.subject.clone(),
            kind: PermissionPrincipalKind::User,
            name: "owner".into(),
        },
        "test operator",
        "Legacy fixture",
    )?;
    let legacy = storage.load()?.ok_or(PermissionError::Detached)?;
    AdminEmail::new("owner@example.com")?.ensure(&storage, &owner)?;
    let migrated = storage.load()?.ok_or(PermissionError::Detached)?;
    assert_eq!(migrated.document.principals, legacy.document.principals);
    assert_eq!(migrated.document.bindings, legacy.document.bindings);
    assert_eq!(
        migrated.document.roles.first().map(|role| &role.id),
        legacy.document.roles.first().map(|role| &role.id)
    );
    assert_eq!(
        migrated
            .document
            .roles
            .first()
            .map(|role| role.name.as_str()),
        Some("Admin")
    );
    Ok(())
}

#[test]
fn configuration_is_validated() {
    for invalid in [
        "",
        " ",
        "not-email",
        "@example.com",
        "owner@",
        "owner@example@com",
        "owner\n@example.com",
    ] {
        assert!(AdminEmail::new(invalid).is_err());
    }
}

struct FixedAuthenticator {
    user: AuthenticatedIdentity,
    revoked: bool,
}
impl Authenticator for FixedAuthenticator {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedIdentity, AuthError>> + Send + '_>> {
        Box::pin(async { Ok(self.user.clone()) })
    }
    fn validate_identity(&self, _: &AuthenticatedIdentity) -> Result<(), AuthError> {
        if self.revoked {
            Err(AuthError::Revoked)
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn authenticator_has_no_default_admin_and_preserves_credential_liveness()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let storage = Arc::new(MemoryPermissionStorage::new());
    let permissions = Permissions::new(storage.clone(), crate::CredentialScopeMode::LocalGrants);
    let server = myko_server::MykoServer::builder().build();
    permissions.attach(Arc::new(server.ctx()))?;
    let user = identity("owner");
    let inner = Arc::new(FixedAuthenticator {
        user: user.clone(),
        revoked: false,
    });
    PermissionAuthenticator::new(inner.clone(), permissions.clone())
        .authenticate(Credentials::default())
        .await?;
    assert!(storage.load()?.is_none());
    let config = AdminEmail::new("owner@example.com")?;
    let revoked = PermissionAuthenticator::new(
        Arc::new(FixedAuthenticator {
            user: user.clone(),
            revoked: true,
        }),
        permissions.clone(),
    )
    .with_admin_email(config.clone());
    assert!(matches!(
        revoked.authenticate(Credentials::default()).await,
        Err(AuthError::Revoked)
    ));
    assert_eq!(revoked.validate_identity(&user), Err(AuthError::Revoked));
    assert!(storage.load()?.is_none());
    let mut expired_user = user;
    expired_user.expires_at = SystemTime::UNIX_EPOCH;
    let expired = PermissionAuthenticator::new(
        Arc::new(FixedAuthenticator {
            user: expired_user,
            revoked: false,
        }),
        permissions.clone(),
    )
    .with_admin_email(config.clone());
    assert!(matches!(
        expired.authenticate(Credentials::default()).await,
        Err(AuthError::Expired)
    ));
    assert!(storage.load()?.is_none());
    let authenticator =
        PermissionAuthenticator::new(inner, permissions.clone()).with_admin_email(config);
    authenticator.authenticate(Credentials::default()).await?;
    assert!(permissions.snapshot()?.is_some());
    assert_eq!(
        permissions.snapshot()?.map(|snapshot| snapshot.revision),
        storage.load()?.map(|snapshot| snapshot.revision)
    );
    let before = storage.load()?;
    PermissionAuthenticator::new(
        Arc::new(FixedAuthenticator {
            user: identity("owner"),
            revoked: false,
        }),
        permissions,
    )
    .authenticate(Credentials::default())
    .await?;
    assert_eq!(storage.load()?, before);
    Ok(())
}

#[test]
fn concurrent_matching_sign_ins_share_one_admin_role() -> PermissionResult<()> {
    let storage = MemoryPermissionStorage::new();
    let config = AdminEmail::new("owner@example.com")?;
    std::thread::scope(|scope| {
        let first = scope.spawn(|| config.ensure(&storage, &identity("password")));
        let second = scope.spawn(|| config.ensure(&storage, &identity("otp")));
        first.join().map_err(|_| PermissionError::Detached)??;
        second.join().map_err(|_| PermissionError::Detached)??;
        Ok::<_, PermissionError>(())
    })?;
    let stored = storage.load()?.ok_or(PermissionError::Detached)?;
    assert_eq!(stored.document.roles.len(), 1);
    assert_eq!(stored.document.principals.len(), 2);
    assert_eq!(stored.document.bindings.len(), 2);
    Ok(())
}
