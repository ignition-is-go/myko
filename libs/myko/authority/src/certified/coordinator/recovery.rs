use myko_federation::control_quorum::ControlHead;

use super::{
    AuthorityDecisionRoot, AuthorityHistory, CertifiedAuthorityRequest,
    CoordinatedAuthorityDecision,
};

impl CoordinatedAuthorityDecision {
    pub(super) fn recover_at(
        history: &AuthorityHistory,
        head: ControlHead,
        root: &AuthorityDecisionRoot,
        request: &CertifiedAuthorityRequest,
    ) -> Result<Option<Self>, String> {
        let Some((transition, evidence)) = history.decision_evidence_at(head, root)? else {
            return Ok(None);
        };
        if transition.binding() != &request.binding() {
            return Err(
                "authority decision root was recovered for a different request binding".to_owned(),
            );
        }
        let proposal = evidence.proposal().clone();
        Ok(Some(Self {
            predecessor: proposal.message.slot.predecessor,
            head: evidence.head(),
            transition,
            promises: proposal.message.prepare_votes.clone(),
            proposal,
            accepts: evidence.accepts().to_vec(),
        }))
    }
}
