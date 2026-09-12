use serde::{Deserialize, Serialize};

use crate::{
    CommandId, ControlTransition, ScopeId,
    control_quorum::{ControlHead, ControlValue},
};

const FAMILY: &[u8] = b"myko/execution-observation/";
const DOMAIN: &[u8] = b"myko/execution-observation/v1\0";

/// Proposed observation of assignments after an exact control predecessor.
///
/// Choosing it preserves the executor sets and controller membership.
/// Its identity must be new for each fresh observation, not reused as a permit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAssignmentObservation {
    payload: ObservationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationPayload {
    operation: CommandId,
    realm: ScopeId,
    predecessor: ControlHead,
}

impl ExecutionAssignmentObservation {
    #[must_use]
    pub const fn new(operation: CommandId, realm: ScopeId, predecessor: ControlHead) -> Self {
        Self {
            payload: ObservationPayload {
                operation,
                realm,
                predecessor,
            },
        }
    }

    /// Encode a no-change observation for the existing control quorum.
    ///
    /// # Errors
    /// Reports payload encoding failure.
    pub fn transition(&self) -> Result<ControlTransition, String> {
        Ok(ControlTransition::retain(
            self.payload.operation,
            self.encode()?,
        ))
    }

    pub(super) fn from_transition(
        transition: &ControlTransition,
        realm: &ScopeId,
        predecessor: ControlHead,
    ) -> Result<Option<Self>, String> {
        let bytes = &transition.payload().0;
        if !bytes.starts_with(FAMILY) {
            return Ok(None);
        }
        let encoded = bytes
            .strip_prefix(DOMAIN)
            .ok_or_else(|| "unsupported execution observation version".to_owned())?;
        let payload: ObservationPayload =
            serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
        let observation = Self { payload };
        if observation.encode()? != *transition.payload() {
            return Err("execution observation payload is not canonical".to_owned());
        }
        if observation.payload.operation != transition.operation()
            || &observation.payload.realm != realm
            || observation.payload.predecessor != predecessor
        {
            return Err(
                "execution observation differs from its operation, realm, or predecessor"
                    .to_owned(),
            );
        }
        if !matches!(transition, ControlTransition::Retain { .. }) {
            return Err("execution observation must not rotate controllers".to_owned());
        }
        Ok(Some(observation))
    }

    fn encode(&self) -> Result<ControlValue, String> {
        let mut bytes = DOMAIN.to_vec();
        serde_json::to_writer(&mut bytes, &self.payload).map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }
}
