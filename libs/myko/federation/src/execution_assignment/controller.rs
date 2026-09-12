use ed25519_dalek::SigningKey;

use super::{AssignmentPayload, ExecutionAssignmentObservation, ExecutionAssignmentsAtHead};
use crate::{
    CertifiedControlChain, ControlAnchor, ControlTransition, EventEnvelope, Node,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, SignedControlProposal, SignedControlVote,
    },
};

/// Local durable voting for assignments and observations in an independently anchored realm.
///
/// This framework controller does not load application modules. It uses the node's
/// existing journal and voter state, and returns signatures only after persistence.
/// The caller must authenticate remote requests before reaching this trusted API.
/// A vote or locally retained head is not a current execution or routing permit.
/// Each signing key belongs to one durable controller store; independent stores
/// must not share a key or roll back its voting history.
pub struct ExecutionAssignmentController {
    node: Node,
    anchor: ControlAnchor,
}

impl ExecutionAssignmentController {
    #[must_use]
    pub const fn new(node: Node, anchor: ControlAnchor) -> Self {
        Self { node, anchor }
    }

    /// Persist a promise under the electorate certified after `head`.
    ///
    /// # Errors
    /// Rejects invalid history, unknown heads, unavailable persistence, invalid
    /// ballots, voter conflicts, and changes to the locally checked history.
    pub fn prepare(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = self.history(head)?;
        let verifier = history.chain.context_at(head)?.verifier()?;
        let request = verifier
            .prepare_request(ballot)
            .map_err(|error| error.to_string())?
            .bind_history(&history.records);
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist an assignment or exact-predecessor observation consistent with the quorum.
    /// Previously accepted values must be recovered, not replaced by a new intent.
    ///
    /// # Errors
    /// Rejects foreign or malformed payloads, reused operations, invalid quorum
    /// evidence, history races, voter conflicts, and persistence failures.
    pub fn propose(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String> {
        let history = self.history(head)?;
        history.validate_transition(head, value)?;
        let verifier = history.chain.context_at(head)?.verifier()?;
        let prepared = verifier
            .verify_prepare(ballot, promises)
            .map_err(|error| error.to_string())?;
        let request = prepared
            .proposal_request(value)
            .map_err(|error| error.to_string())?
            .bind_history(&history.records);
        self.node
            .propose_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist acceptance of a verified assignment proposal.
    ///
    /// # Errors
    /// Rejects unsupported transitions, invalid signatures or prepare evidence,
    /// reused operations, history races, voter conflicts, and persistence failures.
    pub fn accept(
        &self,
        head: ControlHead,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = self.history(head)?;
        history.validate_transition(head, &proposal.message.value)?;
        let verifier = history.chain.context_at(head)?.verifier()?;
        let request = verifier
            .accept_request(proposal)
            .map_err(|error| error.to_string())?
            .bind_history(&history.records);
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }

    fn history(&self, head: ControlHead) -> Result<AssignmentHistory, String> {
        let records = self
            .node
            .events_after(None)
            .map_err(|error| error.to_string())?;
        let chain = CertifiedControlChain::replay(&records, self.anchor.clone())?;
        ExecutionAssignmentsAtHead::replay(&chain, head)?;
        Ok(AssignmentHistory { records, chain })
    }
}

struct AssignmentHistory {
    records: Vec<EventEnvelope>,
    chain: CertifiedControlChain,
}

impl AssignmentHistory {
    fn validate_transition(&self, head: ControlHead, value: &ControlValue) -> Result<(), String> {
        let transition = ControlTransition::from_control_value(value)?;
        let context = self.chain.context_at(head)?;
        if AssignmentPayload::from_transition(&transition, &context.slot().realm)?.is_none()
            && ExecutionAssignmentObservation::from_transition(
                &transition,
                &context.slot().realm,
                head,
            )?
            .is_none()
        {
            return Err(
                "execution controller requires an assignment or observation payload".to_owned(),
            );
        }
        if self
            .chain
            .transitions_to(head)?
            .iter()
            .any(|prior| prior.operation() == transition.operation())
        {
            return Err("execution assignment operation is already chosen".to_owned());
        }
        Ok(())
    }
}
