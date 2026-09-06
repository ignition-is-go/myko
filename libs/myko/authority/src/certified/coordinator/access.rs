use myko_federation::{
    AccessOperation, AccessTarget, AuthorityPresentation, AuthorityUnavailable,
    AuthorizationFailure, AuthorizationPhase,
    control_quorum::{ControlHead, ControlValue},
};

use super::{
    AuthorityDecisionTransition, AuthorityHistory, CertifiedAuthorityControlEndpoint,
    control_denial_for_message,
};

impl CertifiedAuthorityControlEndpoint {
    pub(super) async fn planned_read_value(
        &self,
        presentation: &AuthorityPresentation,
        history: &AuthorityHistory,
        head: ControlHead,
        decision: &AuthorityDecisionTransition,
    ) -> Result<ControlValue, AuthorizationFailure> {
        let request = decision.request();
        if request.authorization_phase != AuthorizationPhase::Admission
            || request.operation != AccessOperation::ReadItems
            || !matches!(
                request.target,
                AccessTarget::Scope(_)
                    | AccessTarget::ServiceScope { .. }
                    | AccessTarget::Items { .. }
            )
            || decision.is_continuation()
        {
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
