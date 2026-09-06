use std::{fmt::Debug, future::Future, pin::Pin};

use myko_federation::{AuthorityUnavailable, ScopeId};

/// Failure to refresh retained history, never an application authorization result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetainedEvidenceError {
    #[error(transparent)]
    Unavailable(AuthorityUnavailable),
    #[error("invalid retained evidence: {0}")]
    Invalid(String),
}

/// Completion of a retained-history refresh into the local store.
pub type RetainedEvidenceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), RetainedEvidenceError>> + Send + 'a>>;

/// Pulls authenticated retained history into the endpoint's own local store.
///
/// Refreshing does not establish custody or grant serving authority. The source
/// independently authorizes each requested scope through normal history access.
/// Success covers every requested scope. If a later scope fails, history already
/// retained for earlier scopes remains; callers must not treat partial refresh as
/// complete evidence.
pub trait ScopedRetainedEvidenceEndpoint: Debug + Send + Sync + 'static {
    fn refresh_scopes<'a>(&'a self, scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a>;
}
