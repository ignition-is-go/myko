use chrono::{DateTime, Utc};
use myko_federation::{AuthorizationDecision, CommandId, control_quorum::ControlHead};

use super::{AuthorityDecisionRoot, AuthorityDecisionTransition, AuthorityHistory, project_facts};
use crate::EvaluationState;

impl AuthorityHistory {
    /// Re-evaluate the exact challenged effect using approvals retained in the
    /// certified predecessor. A permitted or denied root cannot start another round.
    /// This plan must still be chosen by the controllers before use.
    ///
    /// # Errors
    /// Rejects absent challenges, missing fresh approval evidence, terminal roots,
    /// time reversal, or invalid certified history.
    pub fn plan_continuation_at(
        &self,
        head: ControlHead,
        operation: CommandId,
        root: &AuthorityDecisionRoot,
        evaluated_at: DateTime<Utc>,
    ) -> Result<AuthorityDecisionTransition, String> {
        let previous = self
            .decision_at(head, root)?
            .ok_or_else(|| "authority continuation has no previous decision".to_owned())?;
        let state = project_facts(
            &self.selected_at(head)?,
            self.realm_id(),
            previous.topology.clone(),
        )?;
        previous.continue_with_approvals(operation, evaluated_at, &state)
    }
}

impl AuthorityDecisionTransition {
    pub(super) fn continue_with_approvals(
        &self,
        operation: CommandId,
        evaluated_at: DateTime<Utc>,
        state: &EvaluationState,
    ) -> Result<Self, String> {
        let AuthorizationDecision::Challenge { challenge, .. } = &self.decision else {
            return Err("authority decision root is already terminal".to_owned());
        };
        if evaluated_at < self.evaluated_at {
            return Err("authority continuation precedes its challenge".to_owned());
        }
        let mut request = self.request.clone();
        let challenge_id = &challenge.id;
        let mut approvals = state
            .approvals
            .iter()
            .map(|record| &record.decision)
            .filter(|approval| {
                &approval.challenge_id == challenge_id
                    && approval.binding == self.binding
                    && approval.approved
                    && approval.decided_at <= evaluated_at
                    && evaluated_at < approval.expires_at
                    && !request.presentation.approvals.contains(&approval.id)
            })
            .map(|approval| approval.id.clone())
            .collect::<Vec<_>>();
        approvals.sort();
        if approvals.is_empty() {
            return Err("authority continuation has no new certified approval".to_owned());
        }
        request.presentation.approvals.extend(approvals);
        Self::plan_round(
            operation,
            self.root.clone(),
            request,
            evaluated_at,
            self.topology.clone(),
            state,
            Some(challenge.id.clone()),
        )
    }
}
