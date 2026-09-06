use chrono::Utc;
use myko_federation::{
    AuthorityUnavailable, AuthorizationDecision, CommandId,
    control_quorum::{ControlBallot, ControlHead},
};

use super::{
    AuthorityDecisionCoordinator, AuthorityDecisionRevalidation, AuthorityHistory,
    CertifiedAuthorityRequest, ChosenRoundEvidence,
};

/// A fresh coordinated recheck of one previously consumed access decision.
/// This is not a credential that may be cached or used for another request.
pub struct CoordinatedAuthorityRevalidation {
    head: ControlHead,
    revalidation: AuthorityDecisionRevalidation,
    evidence: ChosenRoundEvidence,
    history: AuthorityHistory,
}

impl CoordinatedAuthorityRevalidation {
    #[must_use]
    pub const fn head(&self) -> ControlHead {
        self.head
    }

    #[must_use]
    pub const fn revalidation(&self) -> &AuthorityDecisionRevalidation {
        &self.revalidation
    }

    #[must_use]
    pub const fn proposal(&self) -> &myko_federation::control_quorum::SignedControlProposal {
        &self.evidence.proposal
    }

    /// Consume the immediate result, rejecting authority that expired in transit.
    /// Callers must not cache this result as permission for subsequent access.
    ///
    /// # Errors
    /// Reports expiry as unavailable so that the next fresh check can decide.
    pub fn into_decision(self) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let decision = self.revalidation.decision();
        if decision.is_permit() {
            let current = self
                .history
                .plan_revalidation_at(
                    self.head,
                    self.revalidation.operation(),
                    self.revalidation.root(),
                    Utc::now(),
                )
                .map_err(|_| AuthorityUnavailable::HistoryUnavailable)?;
            if !current.decision().is_permit() {
                return Err(AuthorityUnavailable::StateNotCurrent);
            }
        }
        Ok(decision.clone())
    }
}

impl AuthorityDecisionCoordinator {
    /// Choose a new revalidation for a previously consumed, exact access request.
    /// Intervening accepted decisions are recovered before replanning. A fresh
    /// operation identity prevents an earlier revalidation from satisfying this call.
    ///
    /// # Errors
    /// Returns an error for mismatched requests, missing history, unavailable
    /// quorums, exhausted rounds, or a lease expiring during coordination.
    pub async fn revalidate(
        &self,
        head: ControlHead,
        counter: u64,
        request_id: CommandId,
        request: CertifiedAuthorityRequest,
    ) -> Result<CoordinatedAuthorityRevalidation, String> {
        let root = request.root(self.anchor.realm_id(), request_id)?;
        let mut expected = request.request.clone();
        expected.topology = Some(request.topology.clone());
        let operation = CommandId::new();
        let mut head = head;
        let mut counter = counter;
        for _ in 0..self.max_rounds {
            self.synchronize().await?;
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            let original = history
                .decision_at(head, &root)?
                .ok_or_else(|| "authority revalidation has no original decision".to_owned())?;
            if !original.matches_retained_request(expected.clone()) {
                return Err(
                    "authority revalidation request differs from original access".to_owned(),
                );
            }
            let revalidation = history.plan_revalidation_at(head, operation, &root, Utc::now())?;
            let desired = revalidation.control_value()?;
            let ballot = ControlBallot {
                counter,
                proposer: self.proposer.controller,
            };
            counter = counter
                .checked_add(1)
                .ok_or_else(|| "authority ballot counter overflowed".to_owned())?;
            let (chosen_head, evidence) = self
                .choose_value(&history, head, ballot, desired.clone())
                .await?;
            head = chosen_head;
            if evidence.proposal.message.value != desired {
                continue;
            }
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            history.context_at(head)?;
            return Ok(CoordinatedAuthorityRevalidation {
                head,
                revalidation,
                evidence,
                history,
            });
        }
        Err("authority revalidation did not converge before the retry limit".to_owned())
    }
}
