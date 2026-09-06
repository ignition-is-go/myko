use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityPresentation, AuthorityUnavailable,
    AuthorizationDecision, AuthorizationFailure, AuthorizationPhase, CommandId,
    control_quorum::{ControlHead, ControlValue},
};

use super::{
    AuthorityDecisionCoordinator, AuthorityDecisionTransition, AuthorityHistory,
    AuthorityRequestSource, CertifiedAuthorityControlEndpoint, control_denial_for_message,
    runtime::next_counter,
};

pub(super) fn is_initial_item_read(request: &AccessAttempt) -> bool {
    request.authorization_phase == AuthorizationPhase::Admission
        && request.operation == AccessOperation::ReadItems
        && matches!(
            request.target,
            AccessTarget::Scope(_) | AccessTarget::ServiceScope { .. } | AccessTarget::Items { .. }
        )
}

impl AuthorityDecisionCoordinator {
    pub(super) async fn authorize_item_read(
        &self,
        access: AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        self.synchronize()
            .await
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        let request = AuthorityRequestSource::new(self.observer.clone())
            .current_request(access)
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let head = history
            .retained_head()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let counter =
            next_counter(&history, head).map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let request_id = CommandId::new();
        let chosen = self
            .decide(head, counter, CommandId::new(), request_id, request.clone())
            .await
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        if !chosen.decision().is_permit() {
            return Ok(chosen.decision().clone());
        }
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let head = history
            .retained_head()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let counter =
            next_counter(&history, head).map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
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
        if !is_initial_item_read(request) || decision.is_continuation() {
            return Err(control_denial_for_message(
                presentation,
                "certified access requires an initial scoped item read",
            ));
        }
        self.refresh_command_evidence(presentation, request).await?;
        let topology = self
            .node
            .scope_topology()
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
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
