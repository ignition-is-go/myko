//! Explicit host configuration for administrator admission and user discovery; no environment reads.
use std::{future::Future, pin::Pin, sync::Arc};

use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, Credentials, PermissionCatalog, PrincipalKind,
};

use crate::{
    AUTHORITY_ID, PermissionAuthorityId, PermissionChangeMode, PermissionDocument,
    PermissionEffect, PermissionError, PermissionGrantee, PermissionPrincipal,
    PermissionPrincipalKind, PermissionResult, PermissionRole, PermissionRoleBinding,
    PermissionRule, PermissionStorage, Permissions, operator::commit_operator,
};

/// A trusted host-selected email. Only the authenticator's `verified_email`
/// string claim can match it; unverified email claims never qualify.
#[derive(Clone, Debug)]
pub struct AdminEmail(String);
impl AdminEmail {
    /// # Errors
    /// Rejects blank, malformed, or oversized configuration.
    pub fn new(email: impl Into<String>) -> PermissionResult<Self> {
        let email = email.into().trim().to_owned();
        if email.len() > 4096
            || email.chars().any(char::is_whitespace)
            || email.chars().any(char::is_control)
            || !email.split_once('@').is_some_and(|(local, domain)| {
                !local.is_empty() && !domain.is_empty() && !domain.contains('@')
            })
        {
            return Err(PermissionError::Invalid(
                "administrator email must be an email address".into(),
            ));
        }
        Ok(Self(email))
    }

    fn matches(&self, identity: &AuthenticatedIdentity) -> bool {
        identity.principal.kind == PrincipalKind::User
            && identity.delegation.is_empty()
            && identity
                .claims
                .get("verified_email")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|email| email.eq_ignore_ascii_case(&self.0))
    }

    pub(crate) fn ensure(
        &self,
        storage: &dyn PermissionStorage,
        identity: &AuthenticatedIdentity,
    ) -> PermissionResult<bool> {
        if !self.matches(identity) {
            return Ok(false);
        }
        for _ in 0..5 {
            let current = storage.load()?;
            let mut document = current
                .as_ref()
                .map_or_else(PermissionDocument::default, |snapshot| {
                    snapshot.document.clone()
                });
            ensure_document(&mut document, identity, &self.0)?;
            if current
                .as_ref()
                .is_some_and(|snapshot| snapshot.document == document)
            {
                return Ok(true);
            }
            match commit_operator(
                storage,
                current.as_ref().map(|snapshot| snapshot.revision),
                document,
                "configured administrator email",
                "Ensure configured verified user has Admin",
                if current.is_none() {
                    PermissionChangeMode::Bootstrap
                } else {
                    PermissionChangeMode::Recovery
                },
                false,
            ) {
                Ok(_) => return Ok(true),
                Err(PermissionError::Conflict { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(PermissionError::Busy)
    }
}

fn ensure_document(
    document: &mut PermissionDocument,
    identity: &AuthenticatedIdentity,
    email: &str,
) -> PermissionResult<()> {
    let principal_id = if let Some(principal) = document.principals.iter_mut().find(|principal| {
        principal.issuer == identity.principal.issuer
            && principal.subject == identity.principal.subject
    }) {
        if principal.kind != PermissionPrincipalKind::User {
            return Err(PermissionError::Invalid(
                "configured administrator matches a non-user principal".into(),
            ));
        }
        principal.enabled = true;
        principal.id.clone()
    } else {
        let id: crate::PermissionPrincipalId = uuid::Uuid::new_v4().to_string().into();
        document.principals.push(PermissionPrincipal {
            id: id.clone(),
            authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
            issuer: identity.principal.issuer.clone(),
            subject: identity.principal.subject.clone(),
            kind: PermissionPrincipalKind::User,
            enabled: true,
            name: email.into(),
        });
        id
    };
    // NOTE(ts): retain legacy role IDs so existing assignments survive adoption.
    for role in &mut document.roles {
        if role.name == "Operator recovery administrator" {
            role.name = "Admin".into();
        }
    }
    let rules: Vec<_> = PermissionCatalog::registered()
        .scopes()
        .map(|scope| PermissionRule {
            scope: scope.to_string(),
            effect: PermissionEffect::Allow,
        })
        .collect();
    let role_id = if let Some(role) = document.roles.iter_mut().find(|role| role.name == "Admin") {
        role.enabled = true;
        role.rules = rules;
        role.id.clone()
    } else {
        let id: crate::PermissionRoleId = uuid::Uuid::new_v4().to_string().into();
        document.roles.push(PermissionRole {
            id: id.clone(),
            authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
            name: "Admin".into(),
            enabled: true,
            rules,
        });
        id
    };
    let grantee = PermissionGrantee::Principal(principal_id);
    if !document
        .bindings
        .iter()
        .any(|binding| binding.role_id == role_id && binding.grantee == grantee)
    {
        document.bindings.push(PermissionRoleBinding {
            id: uuid::Uuid::new_v4().to_string().into(),
            authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
            role_id,
            grantee,
        });
    }
    Ok(())
}

/// Adds configured administrator reconciliation and optional user discovery to an authenticator.
/// The wrapped authenticator must produce trusted, verified identity claims.
pub struct PermissionAuthenticator {
    inner: Arc<dyn Authenticator>,
    permissions: Permissions,
    admin_email: Option<AdminEmail>,
    discover_users: bool,
}
impl PermissionAuthenticator {
    pub fn new(inner: Arc<dyn Authenticator>, permissions: Permissions) -> Self {
        Self {
            inner,
            permissions,
            admin_email: None,
            discover_users: false,
        }
    }

    /// Record verified user identities in an initialized authority without granting
    /// roles or re-enabling existing principals. Services and delegated identities are skipped.
    #[must_use]
    pub const fn with_user_discovery(mut self) -> Self {
        self.discover_users = true;
        self
    }

    /// On every matching sign-in, persist an enabled Admin role and assignment.
    /// Removing this configuration stops reconciliation; persisted grants remain.
    #[must_use]
    pub fn with_admin_email(mut self, email: AdminEmail) -> Self {
        self.admin_email = Some(email);
        self
    }
}
impl Authenticator for PermissionAuthenticator {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedIdentity, AuthError>> + Send + '_>> {
        Box::pin(async move {
            let identity = self.inner.authenticate(credentials).await?;
            if identity.expires_at <= std::time::SystemTime::now() {
                return Err(AuthError::Expired);
            }
            self.inner.validate_identity(&identity)?;
            if let Some(email) = &self.admin_email {
                self.permissions
                    .ensure_admin_email(email, &identity)
                    .map_err(|_| AuthError::Unavailable)?;
            }
            if self.discover_users {
                self.permissions
                    .register_identity(&identity)
                    .map_err(|_| AuthError::Unavailable)?;
            }
            Ok(identity)
        })
    }
    fn validate_identity(&self, identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        self.inner.validate_identity(identity)
    }
}

#[cfg(test)]
mod tests;
