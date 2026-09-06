use ed25519_dalek::SigningKey;
use myko_federation::{
    Node,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, SignedControlProposal, SignedControlVote,
    },
};

use super::{AuthorityAnchor, AuthorityHistory};

/// Issues durable control evidence from a freshly validated historical context.
/// This does not establish network freshness, remote request authority or custody.
pub struct AuthorityController {
    node: Node,
    anchor: AuthorityAnchor,
}

impl AuthorityController {
    #[must_use]
    pub const fn new(node: Node, anchor: AuthorityAnchor) -> Self {
        Self { node, anchor }
    }

    /// Persist a promise using the electorate certified after `head`.
    ///
    /// # Errors
    /// Rejects incomplete authority history, invalid ballots, local history races,
    /// unavailable persistence and durable voter conflicts.
    pub fn prepare(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        let verifier = history.context_at(head)?.verifier()?;
        let request = verifier
            .prepare_request(ballot)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist a recovered proposal under the electorate certified after `head`.
    ///
    /// # Errors
    /// Rejects incomplete history, invalid prepare evidence, conflicting proposals,
    /// malformed authority payloads, local history races and persistence failures.
    pub fn propose(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        history.validate_transition_at(head, value)?;
        let verifier = history.context_at(head)?.verifier()?;
        let prepared = verifier
            .verify_prepare(ballot, promises)
            .map_err(|error| error.to_string())?;
        let request = prepared
            .proposal_request(value)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .propose_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist an acceptance only under the certified predecessor configuration.
    ///
    /// # Errors
    /// Rejects incomplete history, stale epochs, invalid proposal evidence, local
    /// history races, malformed authority payloads and persistence failures.
    pub fn accept(
        &self,
        head: ControlHead,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        history.validate_transition_at(head, &proposal.message.value)?;
        let verifier = history.context_at(head)?.verifier()?;
        let request = verifier
            .accept_request(proposal)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }
}
