//! Signed majority evidence for one crash-fault control decision.
//!
//! This module verifies ballots, not authority or durable storage. The caller
//! must obtain the expected slot and controller keys from an independent trust
//! anchor or validated epoch chain. Imported votes cannot establish that trust.
//! Controllers must persist promises and accepted values before signing replies.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{ScopeId, signed_statement::signature_bytes};

mod voter;
pub use voter::ControlVoteRequest;
mod proposal;
pub use proposal::{ControlProposal, ControlProposalRequest, SignedControlProposal};

/// An enrolled Ed25519 public key. Decoding does not enroll or validate it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ControllerId(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlEpochId(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlHead(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlSlot {
    pub realm: ScopeId,
    pub epoch: ControlEpochId,
    pub predecessor: ControlHead,
}

impl ControlSlot {
    /// Content-derived identifier, not proof that this value was chosen or authorized.
    ///
    /// # Errors
    /// Returns an error if the slot or value cannot be serialized.
    pub fn head_for(&self, value: &ControlValue) -> Result<ControlHead, serde_json::Error> {
        let mut bytes = b"myko/control-head/v1\0".to_vec();
        serde_json::to_writer(&mut bytes, &(self, value))?;
        Ok(ControlHead(Sha256::digest(bytes).into()))
    }
}

/// Counter followed by proposer key gives concurrent proposers a total order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlBallot {
    pub counter: u64,
    pub proposer: ControllerId,
}

/// Complete proposed bytes, not a hash requiring the proposer to remain online.
/// The authority layer must separately decode and authorize their meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlValue(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedControlValue {
    pub ballot: ControlBallot,
    pub value: ControlValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlVoteKind {
    Promise {
        accepted: Option<AcceptedControlValue>,
    },
    Accept {
        value: ControlValue,
    },
}

/// Unverified reply content. The variant is part of the signed message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlVote {
    pub slot: ControlSlot,
    pub ballot: ControlBallot,
    pub controller: ControllerId,
    pub vote: ControlVoteKind,
}

impl ControlVote {
    /// Encode the versioned, domain-separated message to sign.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = b"myko/control-vote/v1\0".to_vec();
        serde_json::to_writer(&mut bytes, self)?;
        Ok(bytes)
    }
}

/// A wire container, never proof that a controller persisted its vote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedControlVote {
    pub message: ControlVote,
    #[serde(with = "signature_bytes")]
    pub signature: [u8; 64],
}

impl SignedControlVote {
    /// Check the signed bytes without granting membership or authority.
    ///
    /// # Errors
    /// Rejects invalid keys, signatures, or a promise reporting a future acceptance.
    pub fn verify_signature(&self) -> Result<(), ControlQuorumError> {
        let key = VerifyingKey::from_bytes(&self.message.controller.0)
            .map_err(|_| ControlQuorumError::InvalidSignature)?;
        key.verify_strict(
            &self.message.signing_bytes()?,
            &Signature::from_bytes(&self.signature),
        )
        .map_err(|_| ControlQuorumError::InvalidSignature)?;
        if let ControlVoteKind::Promise {
            accepted: Some(accepted),
        } = &self.message.vote
            && accepted.ballot > self.message.ballot
        {
            return Err(ControlQuorumError::InvalidAcceptedBallot);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ControlQuorumError {
    #[error("controller configuration must be nonempty with distinct valid keys")]
    InvalidControllers,
    #[error("control ballot proposer is not enrolled")]
    UnknownProposer,
    #[error("control certificate does not contain a strict majority")]
    InsufficientQuorum,
    #[error("control certificate repeats a controller")]
    DuplicateController,
    #[error("control vote has an unexpected slot, ballot, or controller")]
    UnexpectedVote,
    #[error("control vote signature is invalid")]
    InvalidSignature,
    #[error("control vote has the wrong phase")]
    WrongPhase,
    #[error("promise reports an impossible accepted ballot")]
    InvalidAcceptedBallot,
    #[error("promises report conflicting values at one accepted ballot")]
    ConflictingAcceptedValues,
    #[error("proposal differs from the required accepted value")]
    WrongValue,
    #[error("control ballot is below an already retained promise")]
    SupersededBallot,
    #[error("controller ballot has conflicting retained proposals")]
    ConflictingProposals,
    #[error("control request history changed after validation")]
    HistoryChanged,
    #[error("control evidence encoding failed: {0}")]
    Encoding(String),
}

impl From<serde_json::Error> for ControlQuorumError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}

/// Verifies evidence against an externally established configuration for a slot.
/// Construction validates key shape and uniqueness, not configuration authority.
#[derive(Debug)]
pub struct ControlQuorumVerifier {
    slot: ControlSlot,
    controllers: BTreeMap<ControllerId, VerifyingKey>,
}

pub(crate) fn controller_keys(
    controllers: impl IntoIterator<Item = ControllerId>,
) -> Result<BTreeMap<ControllerId, VerifyingKey>, ControlQuorumError> {
    let mut keys = BTreeMap::new();
    for controller in controllers {
        let key = VerifyingKey::from_bytes(&controller.0)
            .map_err(|_| ControlQuorumError::InvalidControllers)?;
        if key.is_weak() || keys.insert(controller, key).is_some() {
            return Err(ControlQuorumError::InvalidControllers);
        }
    }
    if keys.is_empty() {
        return Err(ControlQuorumError::InvalidControllers);
    }
    Ok(keys)
}

impl ControlQuorumVerifier {
    /// Verify a proposer's signed value and complete prepare proof before accepting.
    ///
    /// # Errors
    /// Rejects wrong context, proposer, signatures, quorum, or recovered value.
    pub fn accept_request(
        &self,
        proposal: &SignedControlProposal,
    ) -> Result<ControlVoteRequest<'_>, ControlQuorumError> {
        let message = &proposal.message;
        if message.slot != self.slot {
            return Err(ControlQuorumError::UnexpectedVote);
        }
        if !self.controllers.contains_key(&message.ballot.proposer) {
            return Err(ControlQuorumError::UnknownProposer);
        }
        proposal.verify_signature()?;
        self.verify_prepare(message.ballot, &message.prepare_votes)?
            .check_value(&message.value)?;
        Ok(ControlVoteRequest::accept(
            self,
            message.ballot,
            message.value.clone(),
        ))
    }
    /// Build a prepare request for a controller to persist through its node.
    ///
    /// # Errors
    /// Rejects a proposer outside this independently established electorate.
    pub fn prepare_request(
        &self,
        ballot: ControlBallot,
    ) -> Result<ControlVoteRequest<'_>, ControlQuorumError> {
        if !self.controllers.contains_key(&ballot.proposer) {
            return Err(ControlQuorumError::UnknownProposer);
        }
        Ok(ControlVoteRequest::prepare(self, ballot))
    }
    /// Bind independently trusted keys to their expected decision slot.
    ///
    /// # Errors
    /// Rejects empty configurations, repeated keys, malformed keys, and weak keys.
    pub fn new(
        slot: ControlSlot,
        controllers: impl IntoIterator<Item = ControllerId>,
    ) -> Result<Self, ControlQuorumError> {
        Ok(Self {
            slot,
            controllers: controller_keys(controllers)?,
        })
    }

    /// Verify promises and determine which complete value recovery must adopt.
    ///
    /// # Errors
    /// Rejects invalid signatures, context, membership, quorum, phase, or
    /// contradictory accepted-value reports.
    pub fn verify_prepare(
        &self,
        ballot: ControlBallot,
        votes: &[SignedControlVote],
    ) -> Result<PreparedControlQuorum<'_>, ControlQuorumError> {
        self.verify_signers(ballot, votes)?;
        let mut accepted_values = BTreeMap::new();
        for signed in votes {
            let ControlVoteKind::Promise { accepted } = &signed.message.vote else {
                return Err(ControlQuorumError::WrongPhase);
            };
            if let Some(accepted) = accepted {
                if accepted.ballot > ballot
                    || !self.controllers.contains_key(&accepted.ballot.proposer)
                {
                    return Err(ControlQuorumError::InvalidAcceptedBallot);
                }
                if let Some(previous) = accepted_values.insert(accepted.ballot, &accepted.value)
                    && previous != &accepted.value
                {
                    return Err(ControlQuorumError::ConflictingAcceptedValues);
                }
            }
        }
        let required_value = accepted_values
            .last_key_value()
            .map(|(_, value)| (*value).clone());
        Ok(PreparedControlQuorum {
            verifier: self,
            ballot,
            required_value,
            votes: votes.to_vec(),
        })
    }

    fn verify_signers(
        &self,
        ballot: ControlBallot,
        votes: &[SignedControlVote],
    ) -> Result<(), ControlQuorumError> {
        if !self.controllers.contains_key(&ballot.proposer) {
            return Err(ControlQuorumError::UnknownProposer);
        }
        if votes.len() <= self.controllers.len().saturating_div(2) {
            return Err(ControlQuorumError::InsufficientQuorum);
        }
        let mut seen = BTreeSet::new();
        for signed in votes {
            let vote = &signed.message;
            if vote.slot != self.slot || vote.ballot != ballot {
                return Err(ControlQuorumError::UnexpectedVote);
            }
            let key = self
                .controllers
                .get(&vote.controller)
                .ok_or(ControlQuorumError::UnexpectedVote)?;
            if !seen.insert(vote.controller) {
                return Err(ControlQuorumError::DuplicateController);
            }
            key.verify_strict(
                &vote.signing_bytes()?,
                &Signature::from_bytes(&signed.signature),
            )
            .map_err(|_| ControlQuorumError::InvalidSignature)?;
        }
        Ok(())
    }
}

/// Verified promise quorum. Deserialization cannot construct this type.
#[derive(Debug, Clone)]
pub struct PreparedControlQuorum<'a> {
    verifier: &'a ControlQuorumVerifier,
    ballot: ControlBallot,
    required_value: Option<ControlValue>,
    votes: Vec<SignedControlVote>,
}

impl PreparedControlQuorum<'_> {
    /// Build a proposal for the proposer to bind durably to this ballot.
    ///
    /// # Errors
    /// Rejects a value that violates the quorum's recovery requirement.
    pub fn proposal_request(
        &self,
        value: &ControlValue,
    ) -> Result<ControlProposalRequest<'_>, ControlQuorumError> {
        self.check_value(value)?;
        Ok(ControlProposalRequest::new(
            self.verifier,
            self.ballot,
            value.clone(),
            self.votes.clone(),
        ))
    }
    #[must_use]
    pub const fn slot(&self) -> &ControlSlot {
        &self.verifier.slot
    }

    #[must_use]
    pub const fn ballot(&self) -> ControlBallot {
        self.ballot
    }

    /// Recover the highest accepted value, or use new bytes when none was accepted.
    #[must_use]
    pub fn select_value(&self, proposed: ControlValue) -> ControlValue {
        self.required_value.clone().unwrap_or(proposed)
    }

    /// Check an accept request against the value mandated by this quorum.
    ///
    /// # Errors
    /// Rejects any different value when the quorum reports an accepted value.
    pub fn check_value(&self, proposed: &ControlValue) -> Result<(), ControlQuorumError> {
        if self
            .required_value
            .as_ref()
            .is_some_and(|required| required != proposed)
        {
            return Err(ControlQuorumError::WrongValue);
        }
        Ok(())
    }

    /// Verify accepts against this prepare quorum's value, ballot, and electorate.
    ///
    /// This does not execute the value, prove durable votes, or activate an epoch.
    ///
    /// # Errors
    /// Rejects a violated recovery rule, invalid signatures, context, membership,
    /// quorum, phase, or value.
    pub fn verify_chosen(
        &self,
        value: &ControlValue,
        votes: &[SignedControlVote],
    ) -> Result<ChosenControlQuorum, ControlQuorumError> {
        self.check_value(value)?;
        self.verifier.verify_signers(self.ballot, votes)?;
        for signed in votes {
            let ControlVoteKind::Accept { value: accepted } = &signed.message.vote else {
                return Err(ControlQuorumError::WrongPhase);
            };
            if accepted != value {
                return Err(ControlQuorumError::WrongValue);
            }
        }
        Ok(ChosenControlQuorum {
            slot: self.verifier.slot.clone(),
            ballot: self.ballot,
            value: value.clone(),
        })
    }
}

/// Verified accept quorum, not an authorized or currently active authority head.
#[derive(Debug, Clone)]
pub struct ChosenControlQuorum {
    slot: ControlSlot,
    ballot: ControlBallot,
    value: ControlValue,
}

impl ChosenControlQuorum {
    #[must_use]
    pub const fn slot(&self) -> &ControlSlot {
        &self.slot
    }

    #[must_use]
    pub const fn ballot(&self) -> ControlBallot {
        self.ballot
    }

    #[must_use]
    pub const fn value(&self) -> &ControlValue {
        &self.value
    }

    /// Content-derived head independent of the observer, signers, and retry ballot.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn head(&self) -> Result<ControlHead, serde_json::Error> {
        self.slot.head_for(&self.value)
    }
}
