use myko_federation::{
    CommandId, ControlTransition,
    control_quorum::{ControlValue, ControllerId},
};
use serde::{Deserialize, Serialize};

use crate::AuthorityRealmKey;

const ROTATION_DOMAIN: &[u8] = b"myko/certified-authority-rotation/v1\0";

/// Authority-domain payload for a certified generic controller rotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityRotation {
    operation: CommandId,
    realm: AuthorityRealmKey,
    transition: ControlTransition,
}

impl AuthorityRotation {
    /// Build an authority-scoped rotation transition.
    ///
    /// # Errors
    /// Rejects an empty or duplicate successor controller set.
    pub fn new(
        operation: CommandId,
        realm: AuthorityRealmKey,
        controllers: Vec<ControllerId>,
    ) -> Result<Self, String> {
        if realm.as_str().is_empty() {
            return Err("authority rotation realm is empty".to_owned());
        }
        let payload = payload_value(operation, &realm)?;
        let transition = ControlTransition::rotate(operation, controllers, payload)?;
        Ok(Self {
            operation,
            realm,
            transition,
        })
    }

    #[must_use]
    pub const fn operation(&self) -> CommandId {
        self.operation
    }

    #[must_use]
    pub const fn realm_id(&self) -> &AuthorityRealmKey {
        &self.realm
    }

    /// Encode this rotation as the generic quorum value.
    ///
    /// # Errors
    /// Returns an error if serialization fails or controllers are invalid.
    pub fn control_value(&self) -> Result<ControlValue, String> {
        self.transition.control_value()
    }

    pub(super) fn validate_transition(
        operation: CommandId,
        payload: &ControlValue,
        expected_realm: &AuthorityRealmKey,
    ) -> Result<(), String> {
        let wire = payload_wire(payload)?;
        if wire.operation != operation {
            return Err(
                "authority rotation operation does not match control transition".to_owned(),
            );
        }
        if &wire.realm != expected_realm {
            return Err("authority rotation names another realm".to_owned());
        }
        if payload_value(wire.operation, &wire.realm)? != payload.clone() {
            return Err("authority rotation is not canonical".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuthorityRotationWire {
    operation: CommandId,
    realm: AuthorityRealmKey,
}

fn payload_value(operation: CommandId, realm: &AuthorityRealmKey) -> Result<ControlValue, String> {
    let mut bytes = ROTATION_DOMAIN.to_vec();
    serde_json::to_writer(
        &mut bytes,
        &AuthorityRotationWire {
            operation,
            realm: realm.clone(),
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(ControlValue(bytes))
}

fn payload_wire(value: &ControlValue) -> Result<AuthorityRotationWire, String> {
    let encoded = value
        .0
        .strip_prefix(ROTATION_DOMAIN)
        .ok_or_else(|| "control value is not a certified authority rotation".to_owned())?;
    let wire: AuthorityRotationWire =
        serde_json::from_slice(encoded).map_err(|error| error.to_string())?;
    if wire.realm.as_str().is_empty() {
        return Err("authority rotation realm is empty".to_owned());
    }
    Ok(wire)
}
