use crate::{
    AccessAttempt, AuthorityChallenge, AuthorizationDecision, AuthorizationExplanation,
    AuthorizationReport, DenyDecision, PermitDecision, ResourceVisibility,
};

/// Authority could not evaluate the request. No permission decision was made.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityUnavailable {
    #[error("authority state is not current")]
    StateNotCurrent,
    #[error("authority history is unavailable")]
    HistoryUnavailable,
    #[error("authority persistence is unavailable")]
    PersistenceUnavailable,
    #[error("authority policy is unavailable")]
    PolicyUnavailable,
    #[error("authority coordination is unavailable")]
    CoordinationUnavailable,
}

/// A denied or challenged request, or an evaluation that could not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationFailure {
    Deny(Box<DenyDecision>),
    Challenge {
        challenge: Box<AuthorityChallenge>,
        report: Box<AuthorizationReport>,
    },
    Unavailable(AuthorityUnavailable),
}

impl From<AuthorityUnavailable> for AuthorizationFailure {
    fn from(reason: AuthorityUnavailable) -> Self {
        Self::Unavailable(reason)
    }
}

impl From<DenyDecision> for AuthorizationFailure {
    fn from(denied: DenyDecision) -> Self {
        Self::Deny(Box::new(denied))
    }
}

impl From<AuthorizationFailure> for crate::NodeError {
    fn from(failure: AuthorizationFailure) -> Self {
        match failure {
            AuthorizationFailure::Unavailable(reason) => Self::AuthorityUnavailable(reason),
            refusal => Self::AuthorizationDenied(refusal.public_message()),
        }
    }
}

impl AuthorizationFailure {
    /// Separates completed policy decisions from unavailable authority.
    ///
    /// # Errors
    /// Returns the unavailable reason when no decision was made.
    pub fn into_decision(self) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        match self {
            Self::Deny(denied) => Ok(AuthorizationDecision::Deny(*denied)),
            Self::Challenge { challenge, report } => Ok(AuthorizationDecision::Challenge {
                challenge: *challenge,
                report: *report,
            }),
            Self::Unavailable(reason) => Err(reason),
        }
    }

    #[must_use]
    pub fn public_message(&self) -> String {
        match self {
            Self::Deny(denied) => denied.report.explanations.last().map_or_else(
                || "access denied".to_owned(),
                |explanation| explanation.message.clone(),
            ),
            Self::Challenge { challenge, .. } => format!(
                "authorization challenge {} ({})",
                challenge.id, challenge.kind
            ),
            Self::Unavailable(reason) => reason.to_string(),
        }
    }
}

impl AuthorizationDecision {
    /// Converts an in-memory rule result into a decision. The error is a denial
    /// reason, never an inability to load or persist authority.
    #[must_use]
    pub fn from_rule(request: &AccessAttempt, rule: Result<(), String>) -> Self {
        let report = |code: &str, message: String| AuthorizationReport {
            evaluated_at: chrono::Utc::now(),
            principal: request.presentation.principal.clone(),
            executor: request.presentation.executor.clone(),
            operation: request.operation,
            explanations: vec![AuthorizationExplanation {
                code: code.to_owned(),
                message,
                grant_id: None,
                delegation_id: None,
                obligation_id: None,
                constraint: None,
            }],
        };
        match rule {
            Ok(()) => Self::Permit(PermitDecision {
                report: report(
                    "simple_policy_permit",
                    "authorized by the access policy".to_owned(),
                ),
                lease: None,
            }),
            Err(message) => Self::Deny(DenyDecision {
                report: report("simple_policy_deny", message),
                visibility: ResourceVisibility::Unauthorized,
            }),
        }
    }

    /// Extracts permission without confusing failure to evaluate with denial.
    ///
    /// # Errors
    /// Returns the completed denial or challenge.
    pub fn into_permit(self) -> Result<PermitDecision, AuthorizationFailure> {
        match self {
            Self::Permit(permit) => Ok(permit),
            Self::Deny(denied) => Err(AuthorizationFailure::Deny(Box::new(denied))),
            Self::Challenge { challenge, report } => Err(AuthorizationFailure::Challenge {
                challenge: Box::new(challenge),
                report: Box::new(report),
            }),
        }
    }
}
