use chrono::{DateTime, Utc};
use myko_federation::{
    AuthorizationDecision, CommandId, ControlTransition, control_quorum::ControlValue,
};
use serde::{Deserialize, Serialize};

use super::{AuthorityDecisionRoot, AuthorityDecisionTransition};
use crate::{
    EvaluationState,
    decision_records::DecisionRecord,
    evaluator::{deny, evaluate_seeded},
};

const DOMAIN: &[u8] = b"myko/certified-authority-revalidation/v1\0";

/// A historical recheck of a previously consumed effect, not a reusable permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityDecisionRevalidation {
    operation: CommandId,
    root: AuthorityDecisionRoot,
    evaluated_at: DateTime<Utc>,
    decision: AuthorizationDecision,
}

impl AuthorityDecisionRevalidation {
    #[must_use]
    pub const fn operation(&self) -> CommandId {
        self.operation
    }

    #[must_use]
    pub const fn root(&self) -> &AuthorityDecisionRoot {
        &self.root
    }

    #[must_use]
    pub const fn evaluated_at(&self) -> &DateTime<Utc> {
        &self.evaluated_at
    }

    #[must_use]
    pub const fn decision(&self) -> &AuthorizationDecision {
        &self.decision
    }

    /// Encode a revalidation for controller certification.
    ///
    /// # Errors
    /// Returns an error if the canonical payload cannot be serialized.
    pub fn control_value(&self) -> Result<ControlValue, String> {
        ControlTransition::retain(self.operation, self.payload_value()?).control_value()
    }

    fn payload_value(&self) -> Result<ControlValue, String> {
        let mut bytes = DOMAIN.to_vec();
        serde_json::to_writer(&mut bytes, self).map_err(|error| error.to_string())?;
        Ok(ControlValue(bytes))
    }

    pub(in crate::certified) fn from_retained_payload(
        value: &ControlValue,
    ) -> Result<Option<Self>, String> {
        let Some(bytes) = value.0.strip_prefix(DOMAIN) else {
            return Ok(None);
        };
        let revalidation: Self =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if revalidation.payload_value()? != *value {
            return Err("authority revalidation is not canonical".to_owned());
        }
        Ok(Some(revalidation))
    }

    pub(super) fn plan(
        operation: CommandId,
        original: &AuthorityDecisionTransition,
        evaluated_at: DateTime<Utc>,
        mut state: EvaluationState,
    ) -> Result<Self, String> {
        let AuthorizationDecision::Permit(original_permit) = &original.decision else {
            return Err("authority revalidation requires an original permit".to_owned());
        };
        if evaluated_at < original.evaluated_at {
            return Err("authority revalidation precedes its original decision".to_owned());
        }
        let mut request = original.request.clone();
        request.topology = Some(original.topology.clone());
        let decision = if original_permit
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at <= evaluated_at)
        {
            deny(
                &request,
                evaluated_at,
                "lease_expired",
                "the original effect lease has expired",
            )
            .decision
        } else {
            retain_original_contributors(&mut state, original);
            let mut outcome = evaluate_seeded(&state, &request, evaluated_at, original.seed);
            if let AuthorizationDecision::Permit(permit) = &mut outcome.decision {
                permit.lease.clone_from(&original_permit.lease);
            }
            outcome.decision
        };
        Ok(Self {
            operation,
            root: original.root.clone(),
            evaluated_at,
            decision,
        })
    }

    pub(super) fn validate_against(
        &self,
        original: &AuthorityDecisionTransition,
        state: EvaluationState,
    ) -> Result<(), String> {
        if Self::plan(self.operation, original, self.evaluated_at, state)? != *self {
            return Err("authority revalidation differs from predecessor evaluation".to_owned());
        }
        Ok(())
    }
}

fn retain_original_contributors(
    state: &mut EvaluationState,
    original: &AuthorityDecisionTransition,
) {
    state.grants.retain(|grant| {
        original.records.iter().any(|record| match record {
            DecisionRecord::GrantUse(usage) => usage.grant_id == grant.grant.id,
            _ => false,
        })
    });
    state.delegations.retain(|delegation| {
        original.records.iter().any(|record| match record {
            DecisionRecord::DelegationUse(usage) => usage.delegation_id == delegation.delegation.id,
            _ => false,
        })
    });
    state.approvals.retain(|approval| {
        original.records.iter().any(|record| match record {
            DecisionRecord::ApprovalUse(usage) => usage.approval_id == approval.decision.id,
            _ => false,
        })
    });
    state.grant_uses.retain(|usage| {
        !original
            .records
            .iter()
            .any(|record| matches!(record, DecisionRecord::GrantUse(consumed) if consumed == usage))
    });
    state.delegation_uses.retain(|usage| {
        !original.records.iter().any(
            |record| matches!(record, DecisionRecord::DelegationUse(consumed) if consumed == usage),
        )
    });
    state.approval_uses.retain(|usage| {
        !original.records.iter().any(
            |record| matches!(record, DecisionRecord::ApprovalUse(consumed) if consumed == usage),
        )
    });
}
