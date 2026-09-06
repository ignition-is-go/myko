use std::sync::Arc;

use hyphae::{Cell, CellImmutable};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, ApplicationCapability, ApprovalDecision,
    AuthorityPresentation, AuthorityUnavailable, AuthorizationDecision, AuthorizationFailure,
    AuthorizationPhase, ChallengeId, PrincipalId, ReplicationSelection, ScopeTopology,
};

/// Defers effect authorization to the certified worker. Other operations use
/// the explicitly supplied policy and do not gain certified authority semantics.
#[derive(Debug)]
pub struct PreparedEffectPolicy {
    non_effect: Arc<dyn AccessPolicy>,
    notify: flume::Sender<()>,
}

impl PreparedEffectPolicy {
    pub(super) fn new(non_effect: Arc<dyn AccessPolicy>, notify: flume::Sender<()>) -> Self {
        Self { non_effect, notify }
    }
}

impl AccessPolicy for PreparedEffectPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        if request.authorization_phase != AuthorizationPhase::Effect {
            return self.non_effect.decide(request);
        }
        if request.operation != AccessOperation::SubmitCommand || request.command_id().is_none() {
            return Err(AuthorityUnavailable::PolicyUnavailable);
        }
        match self.notify.try_send(()) {
            Ok(()) | Err(flume::TrySendError::Full(())) => {
                Err(AuthorityUnavailable::CoordinationUnavailable)
            }
            Err(flume::TrySendError::Disconnected(())) => {
                Err(AuthorityUnavailable::PolicyUnavailable)
            }
        }
    }

    fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>> {
        self.non_effect.revision_cell()
    }

    fn constrain_replication(
        &self,
        request: &AccessAttempt,
        selection: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationFailure> {
        self.non_effect
            .constrain_replication(request, selection, topology)
    }

    fn approve(
        &self,
        executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        challenge_id: &ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, AuthorizationFailure> {
        self.non_effect
            .approve(executor, presentation, challenge_id, approved)
    }

    fn register_application_capability(
        &self,
        executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        capability: ApplicationCapability,
    ) -> Result<(), String> {
        self.non_effect
            .register_application_capability(executor, presentation, capability)
    }
}
