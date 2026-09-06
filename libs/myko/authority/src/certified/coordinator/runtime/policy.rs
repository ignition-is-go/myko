use std::sync::Arc;

use hyphae::{Cell, CellImmutable};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, ApplicationCapability, AuthorityPresentation,
    AuthorityUnavailable, AuthorizationFailure, AuthorizationPhase, ChallengeId, PrincipalId,
    ReplicationSelection, ScopeTopology,
};

/// Certifies initial scoped item reads and defers effects to the certified worker.
/// Other operations use the explicitly supplied policy without certified semantics.
#[derive(Debug)]
pub struct CertifiedRuntimePolicy {
    non_effect: Arc<dyn AccessPolicy>,
    notify: flume::Sender<()>,
    coordinator: Arc<super::AuthorityDecisionCoordinator>,
}

impl CertifiedRuntimePolicy {
    pub(super) fn new(
        non_effect: Arc<dyn AccessPolicy>,
        notify: flume::Sender<()>,
        coordinator: Arc<super::AuthorityDecisionCoordinator>,
    ) -> Self {
        Self {
            non_effect,
            notify,
            coordinator,
        }
    }
}

impl AccessPolicy for CertifiedRuntimePolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        if request.operation == AccessOperation::ReadItems {
            if !super::super::access::is_initial_item_read(request) {
                return Err(AuthorityUnavailable::PolicyUnavailable).into();
            }
            return myko_federation::PolicyDecision::coordinated(async move {
                self.coordinator.authorize_item_read(request.clone()).await
            });
        }
        if request.authorization_phase != AuthorizationPhase::Effect {
            return self.non_effect.decide(request);
        }
        if request.operation != AccessOperation::SubmitCommand || request.command_id().is_none() {
            return Err(AuthorityUnavailable::PolicyUnavailable).into();
        }
        (match self.notify.try_send(()) {
            Ok(()) | Err(flume::TrySendError::Full(())) => {
                Err(AuthorityUnavailable::CoordinationUnavailable)
            }
            Err(flume::TrySendError::Disconnected(())) => {
                Err(AuthorityUnavailable::PolicyUnavailable)
            }
        })
        .into()
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

    fn approve<'a>(
        &'a self,
        executor: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        challenge_id: &'a ChallengeId,
        approved: bool,
    ) -> myko_federation::AuthorityApprovalFuture<'a> {
        Box::pin(async move {
            if self.notify.is_disconnected() {
                return Err(AuthorityUnavailable::PolicyUnavailable.into());
            }
            let decision = self
                .coordinator
                .approve(executor, presentation, challenge_id, approved)
                .await?;
            match self.notify.try_send(()) {
                Ok(()) | Err(flume::TrySendError::Full(())) => Ok(decision),
                Err(flume::TrySendError::Disconnected(())) => {
                    Err(AuthorityUnavailable::PolicyUnavailable.into())
                }
            }
        })
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
