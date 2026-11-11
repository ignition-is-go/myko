//! Explicit broad authenticated access for applications adopting finer grants in stages.
use crate::{AuthError, AuthenticatedIdentity, AuthorizationRequest, Authorizer};

/// Allows every authenticated operation and explicitly public anonymous operations.
///
/// This policy intentionally does not enforce role grants or credential scopes.
/// Select it explicitly, or use it as a fallback outside a protected policy boundary.
pub struct AuthenticatedAccess;
impl Authorizer for AuthenticatedAccess {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
    fn authorize_anonymous(&self, request: AuthorizationRequest<'_>) -> Result<(), AuthError> {
        match request {
            // Transport dispatch independently validates outstanding command ownership.
            AuthorizationRequest::CommandResult { .. } => Ok(()),
            _ => crate::public_access::authorize(request),
        }
    }
}
