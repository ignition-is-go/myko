//! Explicit execution configuration recovered from certified control history.
//! Historical configuration is not current authority or a ready serving route.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CertifiedControlChain, CommandId, ControlTransition, NodeId, ScopeId, ServiceId,
    control_quorum::{ControlHead, ControlValue},
};

const FAMILY: &[u8] = b"myko/execution-assignment/";
const DOMAIN: &[u8] = b"myko/execution-assignment/v1\0";

mod controller;
pub use controller::ExecutionAssignmentController;
mod observation;
pub use observation::ExecutionAssignmentObservation;

/// Proposed replacement of an exact scope's executor set for one service.
///
/// This is unchosen configuration, not a permission grant, storage placement,
/// schema compatibility proof, or assertion that any executor is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAssignment {
    payload: AssignmentPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentPayload {
    operation: CommandId,
    realm: ScopeId,
    scope: ScopeId,
    service: ServiceId,
    executors: BTreeSet<NodeId>,
}

impl ExecutionAssignment {
    #[must_use]
    pub const fn realm(&self) -> &ScopeId {
        &self.payload.realm
    }

    /// Propose a complete executor set; an empty set removes all exact executors.
    /// Repeated nodes are normalized because the proposal describes a set.
    #[must_use]
    pub fn new(
        operation: CommandId,
        realm: ScopeId,
        scope: ScopeId,
        service: ServiceId,
        executors: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self {
            payload: AssignmentPayload {
                operation,
                realm,
                scope,
                service,
                executors: executors.into_iter().collect(),
            },
        }
    }

    /// Encode the proposal for the existing control protocol to choose.
    /// This does not change controller membership.
    ///
    /// # Errors
    /// Returns an error if the payload cannot be encoded.
    pub fn transition(&self) -> Result<ControlTransition, String> {
        Ok(ControlTransition::retain(
            self.payload.operation,
            self.payload.encode()?,
        ))
    }
}

impl AssignmentPayload {
    fn encode(&self) -> Result<ControlValue, String> {
        let mut bytes = DOMAIN.to_vec();
        serde_json::to_writer(&mut bytes, self).map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }

    fn from_transition(
        transition: &ControlTransition,
        realm: &ScopeId,
    ) -> Result<Option<Self>, String> {
        let bytes = &transition.payload().0;
        if !bytes.starts_with(FAMILY) {
            return Ok(None);
        }
        let encoded = bytes
            .strip_prefix(DOMAIN)
            .ok_or_else(|| "unsupported execution assignment version".to_owned())?;
        let payload: Self = serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
        if payload.encode()? != *transition.payload() {
            return Err("execution assignment payload is not canonical".to_owned());
        }
        if &payload.realm != realm {
            return Err("execution assignment belongs to a different control realm".to_owned());
        }
        if payload.operation != transition.operation() {
            return Err("execution assignment operation does not match its transition".to_owned());
        }
        if !matches!(transition, ControlTransition::Retain { .. }) {
            return Err("execution assignment must not rotate controllers".to_owned());
        }
        Ok(Some(payload))
    }
}

/// Exact execution assignments after one certified historical control head.
///
/// Constructed only from anchored, chosen history. No currentness or serving
/// permit follows from this type. Nested effective assignment policy, storage
/// placement, service compatibility, and scope readiness are separate concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAssignmentsAtHead {
    realm: ScopeId,
    head: ControlHead,
    entries: BTreeMap<ScopeId, BTreeMap<ServiceId, BTreeSet<NodeId>>>,
}

impl ExecutionAssignmentsAtHead {
    /// Recover exact assignments through `head`, never a guessed latest head.
    /// Other control payload families do not alter execution assignments.
    ///
    /// # Errors
    /// Rejects unknown or invalid heads and malformed chosen assignment payloads,
    /// including unsupported versions and mismatched realm or operation bindings.
    pub fn replay(chain: &CertifiedControlChain, head: ControlHead) -> Result<Self, String> {
        let realm = chain.context_at(head)?.slot().realm.clone();
        let mut result = Self {
            realm,
            head,
            entries: BTreeMap::new(),
        };
        for transition in chain.transitions_to(head)? {
            if let Some(payload) = AssignmentPayload::from_transition(transition, &result.realm)? {
                result
                    .entries
                    .entry(payload.scope)
                    .or_default()
                    .insert(payload.service, payload.executors);
            }
        }
        Ok(result)
    }

    /// The independently anchored control realm used for this replay.
    #[must_use]
    pub const fn realm(&self) -> &ScopeId {
        &self.realm
    }

    /// The exact historical head, not proof that it is still current.
    #[must_use]
    pub const fn head(&self) -> ControlHead {
        self.head
    }

    /// Return only the exact recorded set, without an inheritance or route policy.
    /// `None` means no record; an empty set means an explicit empty assignment.
    #[must_use]
    pub fn exact(&self, scope: &ScopeId, service: &ServiceId) -> Option<&BTreeSet<NodeId>> {
        self.entries.get(scope)?.get(service)
    }
}
