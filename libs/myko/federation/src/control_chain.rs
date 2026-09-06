//! Historical controller succession from independently anchored quorum evidence.
//! A certified head is not proof of current authority, custody or durable signing.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    CommandId, EventEnvelope, EventId, FrameworkControlEvent, NodeEvent, ScopeId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlQuorumVerifier, ControlSlot,
        ControlValue, ControlVoteKind, ControllerId, SignedControlProposal, SignedControlVote,
        controller_keys,
    },
};

/// Independently provisioned root for a realm's control chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlAnchor {
    realm: ScopeId,
    initial_epoch: ControlEpochId,
    genesis: ControlHead,
    controllers: Vec<ControllerId>,
}

impl ControlAnchor {
    /// Bind the first trusted epoch for a realm.
    ///
    /// # Errors
    /// Rejects empty electorates, duplicate controllers, malformed keys and weak keys.
    pub fn new(
        realm: ScopeId,
        initial_epoch: ControlEpochId,
        genesis: ControlHead,
        controllers: Vec<ControllerId>,
    ) -> Result<Self, String> {
        validate_controllers(&controllers)?;
        Ok(Self {
            realm,
            initial_epoch,
            genesis,
            controllers,
        })
    }

    #[must_use]
    pub const fn realm(&self) -> &ScopeId {
        &self.realm
    }

    #[must_use]
    pub const fn genesis(&self) -> ControlHead {
        self.genesis
    }
}

/// Unverified control operation with an opaque domain payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlTransition {
    Retain {
        operation: CommandId,
        payload: ControlValue,
    },
    Rotate {
        operation: CommandId,
        successor: Vec<ControllerId>,
        payload: ControlValue,
    },
}

impl ControlTransition {
    #[must_use]
    pub const fn retain(operation: CommandId, payload: ControlValue) -> Self {
        Self::Retain { operation, payload }
    }

    /// Create a rotation transition with a canonical successor electorate.
    ///
    /// # Errors
    /// Rejects empty electorates, duplicate controllers, malformed keys and weak keys.
    pub fn rotate(
        operation: CommandId,
        successor: Vec<ControllerId>,
        payload: ControlValue,
    ) -> Result<Self, String> {
        validate_controllers(&successor)?;
        let mut successor = successor;
        successor.sort_unstable();
        Ok(Self::Rotate {
            operation,
            successor,
            payload,
        })
    }

    /// Encode this transition as the generic quorum value.
    ///
    /// # Errors
    /// Returns an error if serialization fails or the transition is not canonical.
    pub fn control_value(&self) -> Result<ControlValue, String> {
        self.validate()?;
        let mut bytes = b"myko/control-transition/v1\0".to_vec();
        serde_json::to_writer(&mut bytes, self).map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }

    /// Decode a generic quorum value into a control transition.
    ///
    /// # Errors
    /// Rejects malformed transition bytes or invalid successor electorates.
    pub fn from_control_value(value: &ControlValue) -> Result<Self, String> {
        let prefix = b"myko/control-transition/v1\0";
        let encoded = value
            .0
            .strip_prefix(prefix)
            .ok_or_else(|| "control transition value has the wrong domain".to_owned())?;
        let transition: Self =
            serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
        if transition.control_value()? != *value {
            return Err("control transition value is not canonical".to_owned());
        }
        Ok(transition)
    }

    #[must_use]
    pub const fn operation(&self) -> CommandId {
        match self {
            Self::Retain { operation, .. } | Self::Rotate { operation, .. } => *operation,
        }
    }

    #[must_use]
    pub const fn payload(&self) -> &ControlValue {
        match self {
            Self::Retain { payload, .. } | Self::Rotate { payload, .. } => payload,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if let Self::Rotate { successor, .. } = self {
            validate_controllers(successor)?;
            if !is_strictly_sorted(successor) {
                return Err("rotation successor controllers are not canonical".to_owned());
            }
        }
        Ok(())
    }
}

/// Certified historical control state for one realm.
#[derive(Debug, Clone)]
pub struct CertifiedControlChain {
    anchor: ControlAnchor,
    heads: BTreeMap<[u8; 32], CertifiedTransition>,
    failures: BTreeMap<[u8; 32], &'static str>,
}

impl CertifiedControlChain {
    /// Replay retained framework control evidence under one anchor.
    /// Unchosen invalid evidence is ignored. Invalid chosen heads remain errors
    /// when requested, without invalidating their historical predecessors.
    ///
    /// # Errors
    /// Rejects conflicting immutable origins and evidence encoding failures.
    pub fn replay(history: &[EventEnvelope], anchor: ControlAnchor) -> Result<Self, String> {
        validate_immutable_origins(history)?;
        let evidence = ControlEvidence::index(history, &anchor.realm)?;
        let mut chain = Self {
            anchor,
            heads: BTreeMap::new(),
            failures: BTreeMap::new(),
        };
        chain.certify_reachable(&evidence)?;
        Ok(chain)
    }

    /// Return the certified context after an exact historical head.
    ///
    /// # Errors
    /// Rejects unknown, invalid or conflicting certified heads.
    pub fn context_at(&self, head: ControlHead) -> Result<CertifiedControlContext, String> {
        self.reject_failed_head(head)?;
        let (epoch, controllers) = self.context_config(head)?;
        let slot = ControlSlot {
            realm: self.anchor.realm.clone(),
            epoch,
            predecessor: head,
        };
        Ok(CertifiedControlContext { slot, controllers })
    }

    /// Return the certified transitions from genesis through `head`.
    ///
    /// # Errors
    /// Rejects unknown or non-anchored heads.
    pub fn transitions_to(&self, head: ControlHead) -> Result<Vec<&ControlTransition>, String> {
        if head == self.anchor.genesis {
            return Ok(Vec::new());
        }
        self.reject_failed_head(head)?;
        let mut cursor = head;
        let mut transitions = Vec::new();
        while cursor != self.anchor.genesis {
            self.reject_failed_head(cursor)?;
            let certified = self
                .heads
                .get(&cursor.0)
                .ok_or_else(|| "control head is not certified by this chain".to_owned())?;
            transitions.push(&certified.transition);
            cursor = certified.predecessor;
        }
        transitions.reverse();
        Ok(transitions)
    }

    /// Return the last locally retained certified head, not proof of currentness.
    ///
    /// # Errors
    /// Rejects a chain stopped by conflicting or malformed chosen evidence.
    pub fn retained_head(&self) -> Result<ControlHead, String> {
        if let Some(reason) = self.failures.values().next() {
            return Err((*reason).to_owned());
        }
        let predecessors: BTreeSet<_> = self
            .heads
            .values()
            .map(|transition| transition.predecessor.0)
            .collect();
        Ok(self
            .heads
            .keys()
            .find(|head| !predecessors.contains(*head))
            .map_or(self.anchor.genesis, |head| ControlHead(*head)))
    }

    fn context_config(
        &self,
        head: ControlHead,
    ) -> Result<(ControlEpochId, Vec<ControllerId>), String> {
        if head == self.anchor.genesis {
            return Ok((self.anchor.initial_epoch, self.anchor.controllers.clone()));
        }
        self.reject_failed_head(head)?;
        let certified = self
            .heads
            .get(&head.0)
            .ok_or_else(|| "control head is not certified by this chain".to_owned())?;
        Ok((
            certified.context_epoch,
            certified.context_controllers.clone(),
        ))
    }

    fn certify_reachable(&mut self, evidence: &ControlEvidence<'_>) -> Result<(), String> {
        let mut predecessor = self.anchor.genesis;
        let mut operations = BTreeSet::new();
        loop {
            let Some(proposals) = evidence.proposals.get(&predecessor.0) else {
                return Ok(());
            };
            let mut candidates = BTreeMap::new();
            for proposal in proposals {
                let head = proposal
                    .message
                    .slot
                    .head_for(&proposal.message.value)
                    .map_err(|error| error.to_string())?;
                let votes = evidence
                    .accepts
                    .get(&(head.0, proposal.message.ballot))
                    .map_or(&[][..], Vec::as_slice);
                if let Ok(Some(candidate)) = self.certify_candidate(proposal, votes) {
                    candidates.insert(candidate.head().0, candidate);
                }
            }
            if candidates.len() > 1 {
                for head in candidates.keys() {
                    self.failures
                        .insert(*head, "distinct chosen successors share one predecessor");
                }
                return Ok(());
            }
            let Some((_, candidate)) = candidates.pop_first() else {
                return Ok(());
            };
            match candidate {
                CandidateEvidence::Invalid { head, reason } => {
                    self.failures.insert(head.0, reason);
                    return Ok(());
                }
                CandidateEvidence::Certified(candidate) => {
                    if !operations.insert(candidate.transition.operation()) {
                        self.failures.insert(
                            candidate.head.0,
                            "control operation was reused in the certified chain",
                        );
                        return Ok(());
                    }
                    predecessor = candidate.head;
                    self.heads.insert(candidate.head.0, candidate);
                }
            }
        }
    }

    fn certify_candidate(
        &self,
        proposal: &SignedControlProposal,
        votes: &[&SignedControlVote],
    ) -> Result<Option<CandidateEvidence>, String> {
        let slot = proposal.message.slot.clone();
        let (epoch, controllers) = self.context_config(slot.predecessor)?;
        if slot.epoch != epoch {
            return Ok(None);
        }
        let verifier = ControlQuorumVerifier::new(slot.clone(), controllers.clone())
            .map_err(|error| error.to_string())?;
        proposal
            .verify_signature()
            .map_err(|error| error.to_string())?;
        let accept_votes = valid_accept_votes(&slot, proposal, votes, &controllers);
        let prepared = verifier
            .verify_prepare(proposal.message.ballot, &proposal.message.prepare_votes)
            .map_err(|error| error.to_string())?;
        let chosen = prepared
            .verify_chosen(&proposal.message.value, &accept_votes)
            .map_err(|error| error.to_string())?;
        let head = chosen.head().map_err(|error| error.to_string())?;
        let Ok(transition) = ControlTransition::from_control_value(&proposal.message.value) else {
            return Ok(Some(CandidateEvidence::Invalid {
                head,
                reason: "chosen control transition value is malformed",
            }));
        };
        let (context_epoch, context_controllers) = match &transition {
            ControlTransition::Retain { .. } => self.context_config(slot.predecessor)?,
            ControlTransition::Rotate { successor, .. } => {
                (successor_epoch(head), successor.clone())
            }
        };
        Ok(Some(CandidateEvidence::Certified(CertifiedTransition {
            head,
            predecessor: slot.predecessor,
            transition,
            context_epoch,
            context_controllers,
        })))
    }

    fn reject_failed_head(&self, head: ControlHead) -> Result<(), String> {
        if let Some(reason) = self.failures.get(&head.0) {
            return Err((*reason).to_owned());
        }
        Ok(())
    }
}

/// Verified context for decisions after one exact predecessor head.
#[derive(Debug)]
pub struct CertifiedControlContext {
    slot: ControlSlot,
    controllers: Vec<ControllerId>,
}

impl CertifiedControlContext {
    #[must_use]
    pub const fn slot(&self) -> &ControlSlot {
        &self.slot
    }

    /// Build a verifier for the next decision after this certified head.
    ///
    /// # Errors
    /// Returns an error if the quorum verifier rejects the stored electorate.
    pub fn verifier(&self) -> Result<ControlQuorumVerifier, String> {
        ControlQuorumVerifier::new(self.slot.clone(), self.controllers.clone())
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone)]
struct CertifiedTransition {
    head: ControlHead,
    predecessor: ControlHead,
    transition: ControlTransition,
    context_epoch: ControlEpochId,
    context_controllers: Vec<ControllerId>,
}

#[derive(Debug, Clone)]
enum CandidateEvidence {
    Certified(CertifiedTransition),
    Invalid {
        head: ControlHead,
        reason: &'static str,
    },
}

impl CandidateEvidence {
    const fn head(&self) -> ControlHead {
        match self {
            Self::Certified(candidate) => candidate.head,
            Self::Invalid { head, .. } => *head,
        }
    }
}

struct ControlEvidence<'a> {
    proposals: BTreeMap<[u8; 32], Vec<&'a SignedControlProposal>>,
    accepts: BTreeMap<([u8; 32], ControlBallot), Vec<&'a SignedControlVote>>,
}

impl<'a> ControlEvidence<'a> {
    fn index(history: &'a [EventEnvelope], realm: &ScopeId) -> Result<Self, String> {
        let mut evidence = Self {
            proposals: BTreeMap::new(),
            accepts: BTreeMap::new(),
        };
        for event in history {
            match &event.event {
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal))
                    if &proposal.message.slot.realm == realm =>
                {
                    evidence
                        .proposals
                        .entry(proposal.message.slot.predecessor.0)
                        .or_default()
                        .push(proposal);
                }
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote))
                    if &vote.message.slot.realm == realm =>
                {
                    if let ControlVoteKind::Accept { value } = &vote.message.vote {
                        let head = vote
                            .message
                            .slot
                            .head_for(value)
                            .map_err(|error| error.to_string())?;
                        evidence
                            .accepts
                            .entry((head.0, vote.message.ballot))
                            .or_default()
                            .push(vote);
                    }
                }
                _ => {}
            }
        }
        Ok(evidence)
    }
}

fn validate_controllers(controllers: &[ControllerId]) -> Result<(), String> {
    controller_keys(controllers.iter().copied())
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn is_strictly_sorted(controllers: &[ControllerId]) -> bool {
    controllers
        .windows(2)
        .all(|pair| matches!(pair, [left, right] if left < right))
}

fn validate_immutable_origins(history: &[EventEnvelope]) -> Result<(), String> {
    let mut retained: HashMap<EventId, &EventEnvelope> = HashMap::new();
    for event in history {
        if let Some(previous) = retained.insert(event.origin, event)
            && (previous.recorded_at != event.recorded_at || previous.event != event.event)
        {
            return Err("control history reuses an event origin with different content".to_owned());
        }
    }
    Ok(())
}

fn successor_epoch(rotation_head: ControlHead) -> ControlEpochId {
    let mut digest = Sha256::new();
    digest.update(b"myko/control-successor-epoch/v1\0");
    digest.update(rotation_head.0);
    ControlEpochId(digest.finalize().into())
}

fn valid_accept_votes(
    slot: &ControlSlot,
    proposal: &SignedControlProposal,
    votes: &[&SignedControlVote],
    controllers: &[ControllerId],
) -> Vec<SignedControlVote> {
    let mut seen = BTreeSet::new();
    let mut accepted = Vec::new();
    for vote in votes {
        if vote.message.slot != *slot
            || vote.message.ballot != proposal.message.ballot
            || !matches!(
                &vote.message.vote,
                ControlVoteKind::Accept { value } if value == &proposal.message.value
            )
            || !controllers.contains(&vote.message.controller)
        {
            continue;
        }
        if vote.verify_signature().is_ok() && seen.insert(vote.message.controller) {
            accepted.push((*vote).clone());
        }
    }
    accepted
}
