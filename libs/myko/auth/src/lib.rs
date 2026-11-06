//! Verified identities and app-owned local authorization.
pub mod scopes;
use futures::future::BoxFuture;
pub use scopes::{
    EntityAction, EntityScope, EntityScopeRegistration, OperationScopeRegistration,
    PermissionCatalog, ScopeDecision, ScopeInheritanceRegistration, ScopePolicy, ScopedAuthorizer,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PrincipalKind {
    User,
    Service,
    Agent,
}

/// Stable identity is the issuer and subject together, never the socket ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    pub issuer: String,
    pub subject: String,
    pub kind: PrincipalKind,
}

/// Claims are verified by the authenticator. Local policy must honor their limits.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthenticatedIdentity {
    pub principal: Principal,
    pub client_id: Option<String>,
    pub scopes: Vec<String>,
    pub delegation: Vec<Principal>,
    pub claims: Value,
    pub expires_at: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OperationKind {
    Query,
    View,
    Report,
    Command,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventChange {
    Set,
    Delete,
}

#[derive(Clone, Copy, Debug)]
pub enum AuthorizationRequest<'a> {
    Discover {
        kind: OperationKind,
        operation: &'a str,
    },
    Execute {
        kind: OperationKind,
        operation: &'a str,
        arguments: &'a Value,
    },
    CommandResult {
        transaction: &'a str,
    },
    ClientEvent {
        entity_type: &'a str,
        change: EventChange,
        proposed: &'a Value,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthError {
    Unauthenticated,
    Forbidden,
    Expired,
    Revoked,
    Unavailable,
}
impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unauthenticated => "authentication required",
            Self::Forbidden => "permission denied",
            Self::Expired => "credential expired",
            Self::Revoked => "session revoked",
            Self::Unavailable => "authentication unavailable",
        })
    }
}
impl std::error::Error for AuthError {}

/// A transport adapter supplies credentials without storing them in request context.
#[derive(Clone, Default)]
pub struct Credentials {
    pub bearer: Option<String>,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("bearer", &self.bearer.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>>;

    /// Recheck local credential authority for an admitted identity.
    ///
    /// Stateless authenticators can rely on the session's expiry checks. Stateful
    /// authenticators use this hook for session-specific logout or revocation.
    /// This runs repeatedly on transport hot paths. It must be fast, idempotent,
    /// and local, with no network I/O.
    ///
    /// # Errors
    /// Returns an error when the identity's credential is no longer valid.
    /// `Revoked` permanently invalidates the session. Other errors deny the
    /// current check; `Unavailable` can recover when local state is available again.
    fn validate_identity(&self, _identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Decisions read current app state locally. Implementations must enforce credential
/// scopes and delegation limits as well as app grants. Discovery never grants execution.
pub trait Authorizer: Send + Sync + 'static {
    /// # Errors
    /// Returns a denial when the current grants or credential limits reject the request.
    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError>;
    /// Increment when a policy change can invalidate active subscriptions.
    fn revision(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
pub struct MykoAuthProvider {
    authenticator: Arc<dyn Authenticator>,
    authorizer: Arc<dyn Authorizer>,
}
impl MykoAuthProvider {
    pub fn new(authenticator: Arc<dyn Authenticator>, authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authenticator,
            authorizer,
        }
    }
    /// # Errors
    /// Returns an error when credentials are invalid, expired, or verification is unavailable.
    pub async fn authenticate(&self, credentials: Credentials) -> Result<AuthSession, AuthError> {
        let identity = self.authenticator.authenticate(credentials).await?;
        let session = AuthSession::from_identity(
            identity,
            self.authorizer.clone(),
            Some(self.authenticator.clone()),
        );
        session.check()?;
        Ok(session)
    }
}

#[derive(Clone)]
pub struct AuthSession(Arc<SessionInner>);
struct SessionInner {
    id: String,
    identity: AuthenticatedIdentity,
    authorizer: Arc<dyn Authorizer>,
    revision: u64,
    revoked: AtomicBool,
    authenticator: Option<Arc<dyn Authenticator>>,
}
impl fmt::Debug for AuthSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSession")
            .field("id", &self.0.id)
            .field("principal", &self.0.identity.principal)
            .finish_non_exhaustive()
    }
}
impl AuthSession {
    /// Creates a session from a trusted identity without authenticator liveness checks.
    /// Use `MykoAuthProvider::authenticate` to retain the credential authority.
    pub fn new(identity: AuthenticatedIdentity, authorizer: Arc<dyn Authorizer>) -> Self {
        Self::from_identity(identity, authorizer, None)
    }

    fn from_identity(
        identity: AuthenticatedIdentity,
        authorizer: Arc<dyn Authorizer>,
        authenticator: Option<Arc<dyn Authenticator>>,
    ) -> Self {
        let revision = authorizer.revision();
        Self(Arc::new(SessionInner {
            id: uuid::Uuid::new_v4().to_string(),
            identity,
            authorizer,
            revision,
            revoked: AtomicBool::new(false),
            authenticator,
        }))
    }
    #[must_use]
    pub fn id(&self) -> &str {
        &self.0.id
    }
    #[must_use]
    pub fn identity(&self) -> &AuthenticatedIdentity {
        &self.0.identity
    }
    pub fn revoke(&self) {
        self.0.revoked.store(true, Ordering::Release);
    }
    /// # Errors
    /// Returns an error when credentials expire or authority is invalidated.
    pub fn check(&self) -> Result<(), AuthError> {
        if self.0.revoked.load(Ordering::Acquire) || self.0.authorizer.revision() != self.0.revision
        {
            self.revoke();
            return Err(AuthError::Revoked);
        }
        if SystemTime::now() >= self.0.identity.expires_at {
            return Err(AuthError::Expired);
        }
        if let Some(authenticator) = &self.0.authenticator
            && let Err(error) = authenticator.validate_identity(&self.0.identity)
        {
            if error == AuthError::Revoked {
                self.revoke();
            }
            return Err(error);
        }
        Ok(())
    }
    /// # Errors
    /// Returns an error when the session or local policy rejects the request.
    pub fn authorize(&self, request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
        self.check()?;
        self.0.authorizer.authorize(&self.0.identity, request)?;
        self.check()
    }
}

/// Machine clients obtain a valid token at each connection attempt. Implementations
/// cache tokens until renewal is needed. Never include tokens in addresses or logs.
pub trait CredentialProvider: Send + Sync + 'static {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>>;
}

#[cfg(test)]
mod tests;
