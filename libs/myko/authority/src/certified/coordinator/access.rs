use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityPresentation, AuthorityUnavailable,
    AuthorizationDecision, AuthorizationFailure, AuthorizationPhase, CommandId,
    control_quorum::{ControlHead, ControlValue},
};

use super::{
    AuthorityDecisionCoordinator, AuthorityDecisionRoot, AuthorityDecisionTransition,
    AuthorityHistory, AuthorityRequestSource, CertifiedAuthorityControlEndpoint,
    CertifiedAuthorityRequest, control_denial_for_message, runtime::next_counter,
};

pub(super) fn is_initial_scoped_access(request: &AccessAttempt) -> bool {
    request.authorization_phase == AuthorizationPhase::Admission
        && match (&request.operation, &request.target) {
            (
                AccessOperation::ReadItems,
                AccessTarget::Scope(_)
                | AccessTarget::ServiceScope { .. }
                | AccessTarget::Items { .. },
            ) => true,
            (AccessOperation::FollowItems, AccessTarget::Items { .. })
            | (
                AccessOperation::FollowHandler,
                AccessTarget::Handler {
                    scope_id: Some(_), ..
                },
            ) => request.admission_id.is_some(),
            _ => false,
        }
}

impl AuthorityDecisionCoordinator {
    pub(super) async fn authorize_scoped_access(
        &self,
        access: AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        self.synchronize()
            .await
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        let history = self
            .history_for_exact_snapshot()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        if access.authorization_phase == AuthorizationPhase::Continuation {
            return self.continue_scoped_access(&history, access).await;
        }
        let request_id = access.admission_id.unwrap_or_default();
        let request = AuthorityRequestSource::new(self.observer.clone())
            .current_request(access)
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let head = history
            .retained_head()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let counter =
            next_counter(&history, head).map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let chosen = self
            .decide(head, counter, CommandId::new(), request_id, request.clone())
            .await
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        if !chosen.decision().is_permit() {
            return Ok(chosen.decision().clone());
        }
        let history = self
            .history_for_exact_snapshot()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        self.revalidate_scoped_access(&history, request_id, request)
            .await
    }

    async fn continue_scoped_access(
        &self,
        history: &AuthorityHistory,
        mut access: AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let request_id = access
            .admission_id
            .ok_or(AuthorityUnavailable::PolicyUnavailable)?;
        let head = history
            .retained_head()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let root = AuthorityDecisionRoot::new(
            self.anchor.realm_id().clone(),
            request_id,
            AuthorizationPhase::Admission,
        )
        .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let original = history
            .decision_at(head, &root)
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?
            .ok_or(AuthorityUnavailable::HistoryUnavailable)?;
        let AuthorizationDecision::Permit(permit) = original.decision() else {
            return Err(AuthorityUnavailable::PolicyUnavailable);
        };
        let expected_lease = permit
            .lease
            .as_ref()
            .map(|lease| &lease.id)
            .or_else(|| original.request().presentation.active_lease.as_ref());
        if access.presentation.active_lease.as_ref() != expected_lease {
            return Err(AuthorityUnavailable::PolicyUnavailable);
        }
        access.authorization_phase = AuthorizationPhase::Admission;
        access
            .presentation
            .active_lease
            .clone_from(&original.request().presentation.active_lease);
        access.topology = Some(
            self.observer
                .scope_topology()
                .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?
                .proof_for(&access.scope_selections()),
        );
        if !is_initial_scoped_access(&access) || !original.matches_retained_request(access.clone())
        {
            return Err(AuthorityUnavailable::StateNotCurrent);
        }
        let request = AuthorityRequestSource::new(self.observer.clone())
            .current_request(access)
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        self.revalidate_scoped_access(history, request_id, request)
            .await
    }

    async fn revalidate_scoped_access(
        &self,
        history: &AuthorityHistory,
        request_id: CommandId,
        request: CertifiedAuthorityRequest,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let head = history
            .retained_head()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let counter =
            next_counter(history, head).map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        self.revalidate(head, counter, request_id, request)
            .await
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?
            .into_decision()
    }
}

impl CertifiedAuthorityControlEndpoint {
    pub(super) async fn planned_read_value(
        &self,
        presentation: &AuthorityPresentation,
        history: &AuthorityHistory,
        head: ControlHead,
        decision: &AuthorityDecisionTransition,
    ) -> Result<ControlValue, AuthorizationFailure> {
        let request = decision.request();
        if !is_initial_scoped_access(request)
            || decision.is_continuation()
            || request
                .admission_id
                .is_some_and(|id| id != decision.root().request_id())
        {
            return Err(control_denial_for_message(
                presentation,
                "certified access requires an initial scoped read or identified item or handler stream",
            ));
        }
        self.refresh_command_evidence(presentation, request).await?;
        let topology = self
            .node
            .scope_topology()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?
            .proof_for(&request.scope_selections());
        // Configured controllers forward authenticated read attempts. Each voter
        // supplies its own topology and evaluates the certified predecessor.
        history
            .plan_decision_at(
                head,
                decision.operation(),
                decision.root().request_id(),
                request.clone(),
                *decision.evaluated_at(),
                topology,
            )
            .and_then(|planned| planned.control_value())
            .map_err(|_| {
                control_denial_for_message(
                    presentation,
                    "authority read proposal differs from trusted evaluation",
                )
            })
    }
}
