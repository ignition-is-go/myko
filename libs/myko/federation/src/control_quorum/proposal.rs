use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use super::{
    ControlBallot, ControlQuorumError, ControlQuorumVerifier, ControlSlot, ControlValue,
    ControllerId, SignedControlVote,
};
use crate::{EventEnvelope, FrameworkControlEvent, NodeEvent, signed_statement::signature_bytes};

/// Full proposer evidence, not an authorized authority transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlProposal {
    pub slot: ControlSlot,
    pub ballot: ControlBallot,
    pub value: ControlValue,
    pub prepare_votes: Vec<SignedControlVote>,
}

impl ControlProposal {
    /// Encode the complete proposal and proof under a versioned signing domain.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = b"myko/control-proposal/v1\0".to_vec();
        serde_json::to_writer(&mut bytes, self)?;
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedControlProposal {
    pub message: ControlProposal,
    #[serde(with = "signature_bytes")]
    pub signature: [u8; 64],
}

impl SignedControlProposal {
    /// Verify the proposer signature without trusting its electorate or proof.
    ///
    /// # Errors
    /// Rejects invalid signing keys, encodings, or signatures.
    pub fn verify_signature(&self) -> Result<(), ControlQuorumError> {
        let key = VerifyingKey::from_bytes(&self.message.ballot.proposer.0)
            .map_err(|_| ControlQuorumError::InvalidSignature)?;
        key.verify_strict(
            &self.message.signing_bytes()?,
            &Signature::from_bytes(&self.signature),
        )
        .map_err(|_| ControlQuorumError::InvalidSignature)
    }
}

/// Sealed request for a proposer to persist its one value and proof for a ballot.
pub struct ControlProposalRequest<'a> {
    verifier: &'a ControlQuorumVerifier,
    message: ControlProposal,
    history: Option<&'a [EventEnvelope]>,
}

impl<'a> ControlProposalRequest<'a> {
    pub(super) fn new(
        verifier: &'a ControlQuorumVerifier,
        ballot: ControlBallot,
        value: ControlValue,
        mut prepare_votes: Vec<SignedControlVote>,
    ) -> Self {
        prepare_votes.sort_unstable_by_key(|vote| vote.message.controller);
        Self {
            verifier,
            history: None,
            message: ControlProposal {
                slot: verifier.slot.clone(),
                ballot,
                value,
                prepare_votes,
            },
        }
    }

    /// Require the exact locally validated snapshot at the atomic signing boundary.
    /// This guards local races, not remote freshness or authority.
    #[must_use]
    pub const fn bind_history(mut self, history: &'a [EventEnvelope]) -> Self {
        self.history = Some(history);
        self
    }

    pub(crate) fn retained_response(
        &self,
        history: &[EventEnvelope],
        key: &SigningKey,
    ) -> Result<SignedControlProposal, ControlQuorumError> {
        if self.history.is_some_and(|validated| validated != history) {
            return Err(ControlQuorumError::HistoryChanged);
        }
        if ControllerId(key.verifying_key().to_bytes()) != self.message.ballot.proposer {
            return Err(ControlQuorumError::UnknownProposer);
        }
        let mut retained = None;
        for event in history {
            let NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)) =
                &event.event
            else {
                continue;
            };
            if proposal.message.slot != self.message.slot
                || proposal.message.ballot != self.message.ballot
            {
                continue;
            }
            self.verifier.accept_request(proposal)?;
            if proposal.message.value != self.message.value
                || retained.is_some_and(|previous: &SignedControlProposal| previous != proposal)
            {
                return Err(ControlQuorumError::ConflictingProposals);
            }
            retained = Some(proposal);
        }
        if let Some(proposal) = retained {
            return Ok(proposal.clone());
        }
        let signature = key.sign(&self.message.signing_bytes()?).to_bytes();
        Ok(SignedControlProposal {
            message: self.message.clone(),
            signature,
        })
    }
}
