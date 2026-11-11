//! Permission-aware authentication provider composition.
use crate::{AdminEmail, PermissionAuthenticator, Permissions};
use myko_auth::{Authenticator, Authorizer, MykoAuthProvider, ScopedAuthorizer};
use std::sync::Arc;

/// Builds authentication with persisted permissions. Entity scopes govern all
/// operations unless an application fallback is explicitly selected.
pub struct PermissionAuthProviderBuilder {
    permissions: Permissions,
    authenticator: Arc<dyn Authenticator>,
    admin_email: Option<AdminEmail>,
    discover_users: bool,
    application: Option<Arc<dyn Authorizer>>,
    live_permissions: bool,
}
impl Permissions {
    #[must_use]
    pub fn auth_provider(
        &self,
        authenticator: Arc<dyn Authenticator>,
    ) -> PermissionAuthProviderBuilder {
        PermissionAuthProviderBuilder {
            permissions: self.clone(),
            authenticator,
            admin_email: None,
            discover_users: false,
            application: None,
            live_permissions: false,
        }
    }
}
impl PermissionAuthProviderBuilder {
    #[must_use]
    pub fn with_admin_email(mut self, email: Option<AdminEmail>) -> Self {
        self.admin_email = email;
        self
    }
    #[must_use]
    pub const fn with_user_discovery(mut self) -> Self {
        self.discover_users = true;
        self
    }
    /// Restrict persisted grants to administration, delegating application access
    /// to this explicitly selected policy.
    #[must_use]
    pub fn with_application_policy(mut self, application: Arc<dyn Authorizer>) -> Self {
        self.application = Some(application);
        self
    }
    #[must_use]
    pub const fn with_live_permissions(mut self) -> Self {
        self.live_permissions = true;
        self
    }
    #[must_use]
    pub fn build(self) -> MykoAuthProvider {
        let mut authenticator =
            PermissionAuthenticator::new(self.authenticator, self.permissions.clone());
        if let Some(email) = self.admin_email {
            authenticator = authenticator.with_admin_email(email);
        }
        if self.discover_users {
            authenticator = authenticator.with_user_discovery();
        }
        let authorizer: Arc<dyn Authorizer> = match self.application {
            Some(application) => self.permissions.administration_authorizer(application),
            None => Arc::new(ScopedAuthorizer::new(Arc::new(self.permissions.clone()))),
        };
        let provider = MykoAuthProvider::new(Arc::new(authenticator), authorizer);
        if self.live_permissions {
            provider.with_live_permissions()
        } else {
            provider
        }
    }
}
