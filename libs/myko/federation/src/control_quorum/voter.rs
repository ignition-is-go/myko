use ed25519_dalek::{Signer as _, SigningKey};

use super::*;
use crate::{EventEnvelope, FrameworkControlEvent, NodeEvent};

enum RequestedVote {
    Prepare,
    Accept(ControlValue),
}

/// A request constructed from an independently bound verifier or verified prepare.
/// Only the durable node path can turn this request into a released vote.
pub struct ControlVoteRequest<'a> {
    verifier: &'a ControlQuorumVerifier,
    ballot: ControlBallot,
    requested: RequestedVote,
    history: Option<&'a [EventEnvelope]>,
}

impl<'a> ControlVoteRequest<'a> {
    pub(super) const fn prepare(
        verifier: &'a ControlQuorumVerifier,
        ballot: ControlBallot,
    ) -> Self {
        Self {
            verifier,
            ballot,
            requested: RequestedVote::Prepare,
            history: None,
        }
    }

    pub(super) const fn accept(
        verifier: &'a ControlQuorumVerifier,
        ballot: ControlBallot,
        value: ControlValue,
    ) -> Self {
        Self {
            verifier,
            ballot,
            requested: RequestedVote::Accept(value),
            history: None,
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
    ) -> Result<SignedControlVote, ControlQuorumError> {
        if self.history.is_some_and(|validated| validated != history) {
            return Err(ControlQuorumError::HistoryChanged);
        }
        let controller = ControllerId(key.verifying_key().to_bytes());
        let verifying_key = self
            .verifier
            .controllers
            .get(&controller)
            .ok_or(ControlQuorumError::UnexpectedVote)?;
        let mut promised = None;
        let mut accepted = BTreeMap::new();
        for event in history {
            let NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(signed)) =
                &event.event
            else {
                continue;
            };
            let message = &signed.message;
            if message.slot != self.verifier.slot || message.controller != controller {
                continue;
            }
            verifying_key
                .verify_strict(
                    &message.signing_bytes()?,
                    &Signature::from_bytes(&signed.signature),
                )
                .map_err(|_| ControlQuorumError::InvalidSignature)?;
            if !self
                .verifier
                .controllers
                .contains_key(&message.ballot.proposer)
            {
                return Err(ControlQuorumError::UnknownProposer);
            }
            promised = Some(promised.map_or(message.ballot, |previous: ControlBallot| {
                previous.max(message.ballot)
            }));
            let retained = match &message.vote {
                ControlVoteKind::Promise { accepted } => accepted.clone(),
                ControlVoteKind::Accept { value } => Some(AcceptedControlValue {
                    ballot: message.ballot,
                    value: value.clone(),
                }),
            };
            if let Some(retained) = retained {
                if retained.ballot > message.ballot
                    || !self
                        .verifier
                        .controllers
                        .contains_key(&retained.ballot.proposer)
                {
                    return Err(ControlQuorumError::InvalidAcceptedBallot);
                }
                if let Some(previous) = accepted.insert(retained.ballot, retained.value.clone())
                    && previous != retained.value
                {
                    return Err(ControlQuorumError::ConflictingAcceptedValues);
                }
            }
        }
        if promised.is_some_and(|promised| self.ballot < promised) {
            return Err(ControlQuorumError::SupersededBallot);
        }
        let vote = match &self.requested {
            RequestedVote::Prepare => ControlVoteKind::Promise {
                accepted: accepted
                    .pop_last()
                    .map(|(ballot, value)| AcceptedControlValue { ballot, value }),
            },
            RequestedVote::Accept(value) => {
                if accepted
                    .get(&self.ballot)
                    .is_some_and(|previous| previous != value)
                {
                    return Err(ControlQuorumError::ConflictingAcceptedValues);
                }
                ControlVoteKind::Accept {
                    value: value.clone(),
                }
            }
        };
        let message = ControlVote {
            slot: self.verifier.slot.clone(),
            ballot: self.ballot,
            controller,
            vote,
        };
        let signature = key.sign(&message.signing_bytes()?).to_bytes();
        Ok(SignedControlVote { message, signature })
    }
}
