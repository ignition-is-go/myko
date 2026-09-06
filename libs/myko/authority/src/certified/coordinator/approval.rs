use chrono::Utc;
use myko_federation::{
    ApprovalDecision, AuthorityPresentation, AuthorityUnavailable, AuthorizationFailure,
    ChallengeId, CommandId, PrincipalId,
    control_quorum::{ControlBallot, ControlHead, ControlValue, SignedControlVote},
};

use super::{
    AuthorityApprovalTransition, AuthorityDecisionCoordinator, AuthorityDecisionTransition,
    AuthorityHistory, AuthorityRequestSource, CertifiedAuthorityControlEndpoint,
    control_denial_for_message, required_accepted_value, runtime::next_counter,
};

impl AuthorityDecisionCoordinator {
    /// Certify the next round of a saved effect using its retained approvals.
    /// The returned historical decision is not a live effect permit; release
    /// still requires fresh revalidation immediately before committing.
    ///
    /// # Errors
    /// Rejects missing or changed prepared effects, missing approval evidence,
    /// invalid history, and unavailable controller quorums.
    pub async fn continue_prepared(
        &self,
        command_id: CommandId,
    ) -> Result<AuthorityDecisionTransition, String> {
        let request = AuthorityRequestSource::new(self.observer.clone())
            .prepared_command_request(command_id)?;
        let root = request.root(self.anchor.realm_id(), command_id)?;
        let mut expected = request.request().clone();
        expected.topology = Some(request.topology().clone());
        let operation = CommandId::new();
        for _ in 0..self.max_rounds {
            self.synchronize().await?;
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            let head = history.retained_head()?;
            let previous = history
                .decision_at(head, &root)?
                .ok_or_else(|| "authority continuation has no previous challenge".to_owned())?;
            if !previous.matches_prepared_request(expected.clone()) {
                return Err("authority continuation differs from the prepared effect".to_owned());
            }
            if !matches!(
                previous.decision(),
                myko_federation::AuthorizationDecision::Challenge { .. }
            ) {
                return Ok(previous);
            }
            let planned = history.plan_continuation_at(head, operation, &root, Utc::now())?;
            let desired = planned.control_value()?;
            let ballot = ControlBallot {
                counter: next_counter(&history, head)?,
                proposer: self.proposer.controller,
            };
            let (chosen, evidence) = self
                .choose_value(&history, head, ballot, desired.clone())
                .await?;
            if evidence.proposal.message.value == desired {
                return AuthorityHistory::replay(&self.observer, self.anchor.clone())?
                    .decision_at(chosen, &root)?
                    .ok_or_else(|| "chosen authority continuation is not retained".to_owned());
            }
        }
        Err("authority continuation did not converge before the retry limit".to_owned())
    }

    /// Certify a directly authenticated approver's choice. The executor must
    /// come from the trusted session boundary, not a client-supplied identity.
    /// Retries recover the immutable choice and its original expiry.
    ///
    /// # Errors
    /// Rejects impersonation, contradictory retries, invalid approval requests,
    /// invalid history, and unavailable controller quorums.
    pub async fn approve(
        &self,
        authenticated_executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        challenge: &ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, String> {
        if authenticated_executor != &presentation.executor.id
            || presentation.principal != presentation.executor
            || !presentation.provenance.is_empty()
        {
            return Err("approval requires a directly authenticated approver".to_owned());
        }
        let operation = CommandId::new();
        for _ in 0..self.max_rounds {
            self.synchronize().await?;
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            let head = history.retained_head()?;
            if let Some(existing) = history.approval_at(head, challenge, &presentation.principal)? {
                if existing.approved != approved {
                    return Err(
                        "approval decision is immutable for this challenge and approver".to_owned(),
                    );
                }
                return Ok(existing);
            }
            let planned = history.plan_approval_at(
                head,
                operation,
                challenge,
                &presentation.principal,
                approved,
                Utc::now(),
            )?;
            let desired = planned.control_value()?;
            let ballot = ControlBallot {
                counter: next_counter(&history, head)?,
                proposer: self.proposer.controller,
            };
            let (chosen, evidence) = self
                .choose_value(&history, head, ballot, desired.clone())
                .await?;
            if evidence.proposal.message.value == desired {
                let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
                return history
                    .approval_at(chosen, challenge, &presentation.principal)?
                    .ok_or_else(|| "chosen authority approval is not retained".to_owned());
            }
        }
        Err("authority approval did not converge before the retry limit".to_owned())
    }
}

impl CertifiedAuthorityControlEndpoint {
    pub(super) fn validate_approval(
        &self,
        presentation: &AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        approval: &AuthorityApprovalTransition,
    ) -> Result<(), AuthorizationFailure> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        let verifier = history
            .context_at(head)
            .and_then(|context| context.verifier())
            .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
        verifier.verify_prepare(ballot, promises).map_err(|_| {
            control_denial_for_message(presentation, "authority approval prepare proof is invalid")
        })?;
        if let Some(required) = required_accepted_value(promises) {
            if required != value {
                return Err(control_denial_for_message(
                    presentation,
                    "authority approval differs from required accepted value",
                ));
            }
            return history.validate_transition_at(head, value).map_err(|_| {
                control_denial_for_message(presentation, "required authority approval is invalid")
            });
        }
        let decision = approval.decision();
        self.validate_evaluation_time(presentation, &decision.decided_at)?;
        let planned = history
            .plan_approval_at(
                head,
                approval.operation(),
                &decision.challenge_id,
                &decision.approver,
                decision.approved,
                decision.decided_at,
            )
            .and_then(|approval| approval.control_value())
            .map_err(|_| {
                control_denial_for_message(
                    presentation,
                    "authority approval is not valid against certified facts",
                )
            })?;
        if planned != *value {
            return Err(control_denial_for_message(
                presentation,
                "authority approval differs from trusted evaluation",
            ));
        }
        Ok(())
    }
}
