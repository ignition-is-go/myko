use chrono::{DateTime, Duration, Utc};
use myko_federation::{
    ApprovalDecision, ApprovalId, ChallengeId, CommandId, ControlTransition, Principal,
    ScopeTopology,
    control_quorum::{ControlHead, ControlValue},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{AuthorityHistory, project_facts};
use crate::{
    ApprovalRecord, ApprovalRecordId, AuthorityRealmKey, EvaluationState,
    decision_records::DecisionRecord, realm_item_id,
};

const DOMAIN: &[u8] = b"myko/certified-authority-approval/v1\0";

/// An approver's immutable choice for a certified challenge, not an effect permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityApprovalTransition {
    operation: CommandId,
    decision: ApprovalDecision,
}

impl AuthorityApprovalTransition {
    #[must_use]
    pub const fn operation(&self) -> CommandId {
        self.operation
    }

    #[must_use]
    pub const fn decision(&self) -> &ApprovalDecision {
        &self.decision
    }

    /// Encode the complete approval for controller certification.
    ///
    /// # Errors
    /// Returns an error when canonical serialization fails.
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
        let approval: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if approval.payload_value()? != *value {
            return Err("authority approval is not canonical".to_owned());
        }
        Ok(Some(approval))
    }

    pub(super) fn record(&self) -> DecisionRecord {
        DecisionRecord::Approval(ApprovalRecord {
            id: ApprovalRecordId::from(self.decision.id.as_str()),
            authority_realm_id: realm_item_id(&self.decision.realm_id),
            decision: self.decision.clone(),
        })
    }

    pub(super) fn validate_against(
        &self,
        realm: &AuthorityRealmKey,
        state: &EvaluationState,
    ) -> Result<(), String> {
        let planned = plan_approval(
            realm,
            &self.decision.challenge_id,
            &self.decision.approver,
            self.decision.approved,
            self.decision.decided_at,
            state,
        )?;
        if planned != self.decision {
            return Err("authority approval differs from predecessor facts".to_owned());
        }
        Ok(())
    }
}

impl AuthorityHistory {
    /// Recover an approver's already certified choice without renewing its expiry.
    ///
    /// # Errors
    /// Rejects invalid or incomplete certified history.
    pub fn approval_at(
        &self,
        head: ControlHead,
        challenge: &ChallengeId,
        approver: &Principal,
    ) -> Result<Option<ApprovalDecision>, String> {
        let state = project_facts(
            &self.selected_at(head)?,
            self.realm_id(),
            ScopeTopology::default(),
        )?;
        Ok(state
            .approvals
            .into_iter()
            .map(|record| record.decision)
            .find(|decision| &decision.challenge_id == challenge && &decision.approver == approver))
    }

    /// Plan a choice against one certified historical predecessor. The framework
    /// must authenticate the approver before requesting live coordination.
    ///
    /// # Errors
    /// Rejects duplicate choices, expired challenges, inactive obligations,
    /// unauthorized approvers, mismatched realms, and invalid expiry arithmetic.
    pub fn plan_approval_at(
        &self,
        head: ControlHead,
        operation: CommandId,
        challenge: &ChallengeId,
        approver: &Principal,
        accepted: bool,
        now: DateTime<Utc>,
    ) -> Result<AuthorityApprovalTransition, String> {
        let state = project_facts(
            &self.selected_at(head)?,
            self.realm_id(),
            ScopeTopology::default(),
        )?;
        Ok(AuthorityApprovalTransition {
            operation,
            decision: plan_approval(self.realm_id(), challenge, approver, accepted, now, &state)?,
        })
    }
}

fn plan_approval(
    realm: &AuthorityRealmKey,
    challenge_id: &ChallengeId,
    approver: &Principal,
    accepted: bool,
    now: DateTime<Utc>,
    state: &EvaluationState,
) -> Result<ApprovalDecision, String> {
    if state.approvals.iter().any(|record| {
        &record.decision.challenge_id == challenge_id && &record.decision.approver == approver
    }) {
        return Err("approval is already recorded for this challenge and approver".to_owned());
    }
    let challenge = &state
        .challenges
        .iter()
        .find(|record| &record.challenge.id == challenge_id)
        .ok_or_else(|| "challenge is not certified".to_owned())?
        .challenge;
    if &challenge.realm_id != realm || challenge.expires_at <= now || now < challenge.issued_at {
        return Err("challenge is expired or belongs to another realm".to_owned());
    }
    let obligation = &state
        .obligations
        .iter()
        .find(|record| {
            record.revoked_at.is_none() && record.obligation.id == challenge.obligation_id
        })
        .ok_or_else(|| "challenge obligation is not active".to_owned())?
        .obligation;
    if !obligation.approvers.contains(approver) {
        return Err("authenticated principal cannot approve challenge".to_owned());
    }
    let lifetime =
        i64::try_from(obligation.approval_lifetime_seconds).map_err(|error| error.to_string())?;
    let lifetime = Duration::try_seconds(lifetime)
        .ok_or_else(|| "approval lifetime exceeds supported duration".to_owned())?;
    let expires_at = now
        .checked_add_signed(lifetime)
        .ok_or_else(|| "approval expiry exceeds supported time".to_owned())?;
    let mut identity = Sha256::new();
    identity.update(DOMAIN);
    identity.update(
        serde_json::to_vec(&(realm, challenge_id, approver)).map_err(|error| error.to_string())?,
    );
    Ok(ApprovalDecision {
        id: ApprovalId::new(format!("approval/{:x}", identity.finalize())),
        realm_id: realm.clone(),
        challenge_id: challenge.id.clone(),
        obligation_id: challenge.obligation_id.clone(),
        approver: approver.clone(),
        binding: challenge.binding.clone(),
        approved: accepted,
        decided_at: now,
        expires_at,
        max_uses: obligation.approval_use_count,
    })
}
