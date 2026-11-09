//! Verified identities and app-owned local authorization.
pub mod client_events;
pub mod identity;
pub mod oauth;
pub mod public_access;
pub mod scopes;
use futures::future::BoxFuture;
pub use oauth::OAuthResource;
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
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// Stable application user supplied by the authenticator, distinct from the provider subject.
    #[serde(default)]
    pub user_id: Option<identity::UserId>,
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
    /// Authorize a caller without a verified identity. Explicit public metadata
    /// is allowed by default; applications may impose additional restrictions.
    /// # Errors
    /// Returns a denial for operations not exposed to anonymous callers.
    fn authorize_anonymous(&self, request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
        public_access::authorize(request)
    }
    /// Increment whenever local authorization decisions can change.
    /// Live transports use this to recheck active subscription permissions.
    fn revision(&self) -> u64 {
        0
    }
    /// Invalidation revision for this identity, or for an anonymous session.
    ///
    /// Defaults to global invalidation. Authorizers may isolate unrelated policy
    /// edits by returning a revision of this identity's effective access instead.
    /// It must change whenever existing subscriptions could lose authorization.
    /// `revision()` must also change whenever this value may change; sessions
    /// recompute it only after observing a new global policy revision.
    fn session_revision(&self, _identity: Option<&AuthenticatedIdentity>) -> u64 {
        self.revision()
    }
}

#[derive(Clone)]
pub struct MykoAuthProvider {
    authenticator: Arc<dyn Authenticator>,
    authorizer: Arc<dyn Authorizer>,
    anonymous_access: bool,
    live_permissions: bool,
    mcp_oauth: Option<OAuthResource>,
    oauth_server: Option<Arc<oauth::OAuthServer>>,
}
impl MykoAuthProvider {
    pub fn new(authenticator: Arc<dyn Authenticator>, authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authenticator,
            authorizer,
            anonymous_access: false,
            live_permissions: false,
            mcp_oauth: None,
            oauth_server: None,
        }
    }
    /// Require authenticated MCP admission and advertise its OAuth resource.
    /// Anonymous Myko WebSocket operations retain their configured policy.
    #[must_use]
    pub fn with_mcp_oauth(mut self, resource: OAuthResource) -> Self {
        self.mcp_oauth = Some(resource);
        self.oauth_server = None;
        self
    }

    #[must_use]
    pub const fn mcp_oauth(&self) -> Option<&OAuthResource> {
        self.mcp_oauth.as_ref()
    }

    /// Enable the framework OAuth handoff to a host-owned login/consent UI.
    #[must_use]
    pub fn with_oauth_server(mut self, server: Arc<oauth::OAuthServer>) -> Self {
        self.mcp_oauth = Some(server.resource().clone());
        self.oauth_server = Some(server);
        self
    }

    #[must_use]
    pub const fn oauth_server(&self) -> Option<&Arc<oauth::OAuthServer>> {
        self.oauth_server.as_ref()
    }

    fn session_authorizer(&self) -> Arc<dyn Authorizer> {
        if self.oauth_server.is_some() {
            Arc::new(OAuthFlowAuthorizer(self.authorizer.clone()))
        } else {
            self.authorizer.clone()
        }
    }

    /// Inspect or complete a browser authorization handoff. Approval always
    /// authenticates the submitted session credential before issuing a code.
    /// # Errors
    /// Rejects invalid login state or authority; no anonymous approval is possible.
    pub async fn oauth_flow(
        &self,
        operation: oauth::OAuthFlowOperation,
    ) -> Result<oauth::OAuthFlowReply, String> {
        use oauth::{OAuthFlowOperation, OAuthFlowReply};
        let server = self
            .oauth_server
            .as_ref()
            .ok_or("OAuth authorization is not configured")?;
        match operation {
            OAuthFlowOperation::Inspect { request } => Ok(OAuthFlowReply {
                info: Some(server.inspect(&request).map_err(|e| e.to_string())?),
                redirect: None,
            }),
            OAuthFlowOperation::Complete {
                request,
                credential,
                approve,
            } => {
                let authority = if approve {
                    let credential = credential
                        .filter(|v| !v.is_empty())
                        .ok_or("Sign in before approving access")?;
                    Some(
                        self.authenticate(Credentials {
                            bearer: Some(credential),
                        })
                        .await
                        .map_err(|e| e.to_string())?,
                    )
                } else {
                    None
                };
                Ok(OAuthFlowReply {
                    info: None,
                    redirect: Some(
                        server
                            .complete(&request, authority)
                            .map_err(|e| e.to_string())?,
                    ),
                })
            }
        }
    }

    /// Keep authenticated transports alive across permission edits.
    /// Transports must reauthorize subscriptions and queued data before delivery.
    /// Credential expiry, logout, and explicit session revocation still apply.
    #[must_use]
    pub const fn with_live_permissions(mut self) -> Self {
        self.live_permissions = true;
        self
    }

    /// Enable app-defined anonymous admission even without public decorators.
    /// The authorizer still decides access to each operation.
    #[must_use]
    pub const fn with_anonymous_access(mut self) -> Self {
        self.anonymous_access = true;
        self
    }

    /// Admit an authenticated caller or an explicitly enabled anonymous caller.
    /// Invalid supplied credentials never fall back to anonymous access.
    /// # Errors
    /// Returns an error if admission or credential verification fails.
    pub async fn admit(&self, credentials: Credentials) -> Result<AuthSession, AuthError> {
        if credentials.bearer.is_none()
            && (self.anonymous_access
                || public_access::has_registrations()
                || self.oauth_server.is_some())
        {
            return Ok(AuthSession::anonymous(
                self.session_authorizer(),
                self.live_permissions,
            ));
        }
        self.authenticate(credentials).await
    }

    /// # Errors
    /// Returns an error when credentials are invalid, expired, or verification is unavailable.
    pub async fn authenticate(&self, credentials: Credentials) -> Result<AuthSession, AuthError> {
        if let Some(token) = credentials
            .bearer
            .as_deref()
            .filter(|token| token.starts_with("myko_access_"))
        {
            return self
                .oauth_server
                .as_ref()
                .ok_or(AuthError::Unauthenticated)?
                .authenticate(token);
        }
        let identity = self.authenticator.authenticate(credentials).await?;
        let session = AuthSession::from_identity(
            identity,
            self.session_authorizer(),
            Some(self.authenticator.clone()),
            self.live_permissions,
        );
        session.check()?;
        Ok(session)
    }
}

#[derive(Clone)]
pub struct AuthSession(Arc<SessionInner>);
struct SessionInner {
    id: String,
    identity: Option<AuthenticatedIdentity>,
    authorizer: Arc<dyn Authorizer>,
    revision: u64,
    live_permissions: bool,
    observed_policy_revision: AtomicU64,
    revoked: AtomicBool,
    authenticator: Option<Arc<dyn Authenticator>>,
}
impl fmt::Debug for AuthSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSession")
            .field("id", &self.0.id)
            .field(
                "principal",
                &self.0.identity.as_ref().map(|identity| &identity.principal),
            )
            .finish_non_exhaustive()
    }
}
impl AuthSession {
    pub(crate) fn with_deadline(&self, deadline: SystemTime) -> Result<Self, AuthError> {
        self.check()?;
        let mut identity = self.identity().cloned().ok_or(AuthError::Unauthenticated)?;
        identity.expires_at = identity.expires_at.min(deadline);
        Ok(Self::from_identity(
            identity,
            self.0.authorizer.clone(),
            Some(Arc::new(ParentAuthority(self.clone()))),
            self.0.live_permissions,
        ))
    }
    /// Creates a session from a trusted identity without authenticator liveness checks.
    /// Use `MykoAuthProvider::authenticate` to retain the credential authority.
    pub fn new(identity: AuthenticatedIdentity, authorizer: Arc<dyn Authorizer>) -> Self {
        Self::from_identity(identity, authorizer, None, false)
    }

    fn from_identity(
        identity: AuthenticatedIdentity,
        authorizer: Arc<dyn Authorizer>,
        authenticator: Option<Arc<dyn Authenticator>>,
        live_permissions: bool,
    ) -> Self {
        let policy_revision = authorizer.revision();
        let revision = authorizer.session_revision(Some(&identity));
        Self(Arc::new(SessionInner {
            id: uuid::Uuid::new_v4().to_string(),
            identity: Some(identity),
            authorizer,
            revision,
            live_permissions,
            observed_policy_revision: AtomicU64::new(policy_revision),
            revoked: AtomicBool::new(false),
            authenticator,
        }))
    }
    fn anonymous(authorizer: Arc<dyn Authorizer>, live_permissions: bool) -> Self {
        let policy_revision = authorizer.revision();
        let revision = authorizer.session_revision(None);
        Self(Arc::new(SessionInner {
            id: uuid::Uuid::new_v4().to_string(),
            identity: None,
            authorizer,
            revision,
            live_permissions,
            observed_policy_revision: AtomicU64::new(policy_revision),
            revoked: AtomicBool::new(false),
            authenticator: None,
        }))
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.0.id
    }
    #[must_use]
    /// The verified identity, absent for an explicitly admitted anonymous caller.
    pub fn identity(&self) -> Option<&AuthenticatedIdentity> {
        self.0.identity.as_ref()
    }
    #[must_use]
    pub fn uses_live_permissions(&self) -> bool {
        self.0.live_permissions
    }

    /// Current local authorization policy version, independent of credential validity.
    #[must_use]
    pub fn policy_revision(&self) -> u64 {
        self.0.authorizer.revision()
    }

    pub fn revoke(&self) {
        self.0.revoked.store(true, Ordering::Release);
    }
    /// # Errors
    /// Returns an error when credentials expire or authority is invalidated.
    pub fn check(&self) -> Result<(), AuthError> {
        if self.0.revoked.load(Ordering::Acquire) {
            return Err(AuthError::Revoked);
        }
        if !self.0.live_permissions {
            self.check_policy_revision()?;
        }
        if let Some(identity) = &self.0.identity {
            if SystemTime::now() >= identity.expires_at {
                return Err(AuthError::Expired);
            }
            if let Some(authenticator) = &self.0.authenticator
                && let Err(error) = authenticator.validate_identity(identity)
            {
                if error == AuthError::Revoked {
                    self.revoke();
                }
                return Err(error);
            }
        }
        Ok(())
    }
    fn check_policy_revision(&self) -> Result<(), AuthError> {
        for _ in 0..3 {
            let policy_revision = self.0.authorizer.revision();
            if policy_revision == self.0.observed_policy_revision.load(Ordering::Acquire) {
                return Ok(());
            }
            let revision = self.0.authorizer.session_revision(self.0.identity.as_ref());
            // NOTE(ts): do not cache an effective revision assembled across
            // multiple policy snapshots while concurrent edits are publishing.
            if self.0.authorizer.revision() != policy_revision {
                continue;
            }
            if revision != self.0.revision {
                self.revoke();
                return Err(AuthError::Revoked);
            }
            self.0
                .observed_policy_revision
                .store(policy_revision, Ordering::Release);
            return Ok(());
        }
        Err(AuthError::Unavailable)
    }
    /// # Errors
    /// Returns an error when the session or local policy rejects the request.
    pub fn authorize(&self, request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
        if let AuthorizationRequest::ClientEvent { entity_type, .. } = request {
            self.check()?;
            client_events::authorize(entity_type)?;
        }
        for _ in 0..3 {
            self.check()?;
            let revision = self.policy_revision();
            let decision = self.0.identity.as_ref().map_or_else(
                || self.0.authorizer.authorize_anonymous(request),
                |identity| self.0.authorizer.authorize(identity, request),
            );
            self.check()?;
            if self.0.live_permissions && self.policy_revision() != revision {
                continue;
            }
            return decision;
        }
        Err(AuthError::Unavailable)
    }
}

struct ParentAuthority(AuthSession);
impl Authenticator for ParentAuthority {
    fn authenticate(
        &self,
        _credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async { Err(AuthError::Unauthenticated) })
    }
    fn validate_identity(&self, _identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        self.0.check()
    }
}

struct OAuthFlowAuthorizer(Arc<dyn Authorizer>);
fn is_oauth_flow(request: AuthorizationRequest<'_>) -> bool {
    matches!(
        request,
        AuthorizationRequest::Execute {
            kind: OperationKind::Command,
            operation: "MykoOAuthRequest",
            ..
        } | AuthorizationRequest::Discover {
            kind: OperationKind::Command,
            operation: "MykoOAuthRequest"
        }
    )
}
impl Authorizer for OAuthFlowAuthorizer {
    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        if is_oauth_flow(request) {
            Ok(())
        } else {
            self.0.authorize(identity, request)
        }
    }
    fn authorize_anonymous(&self, request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
        if is_oauth_flow(request) {
            Ok(())
        } else {
            self.0.authorize_anonymous(request)
        }
    }
    fn revision(&self) -> u64 {
        self.0.revision()
    }
    fn session_revision(&self, identity: Option<&AuthenticatedIdentity>) -> u64 {
        self.0.session_revision(identity)
    }
}

/// Machine clients obtain a valid token at each connection attempt. Implementations
/// cache tokens until renewal is needed. Never include tokens in addresses or logs.
pub trait CredentialProvider: Send + Sync + 'static {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>>;
}

#[cfg(test)]
mod tests;
