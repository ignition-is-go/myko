use chrono::{DateTime, Utc};
use myko_federation::{
    AccessAttempt, AccessOperation, AuthorizationBinding, AuthorizationDecision,
    AuthorizationPhase, ItemMutation,
};
use myko_items::MykoItem;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    ApprovalRecord, ApprovalUse, ApprovalUseId, AuthorityRealmKey, ChallengeRecord,
    ChallengeRecordId, CommandContext, CommandError, DecisionAudit, DecisionAuditId, DelegationUse,
    DelegationUseId, EvaluationOutcome, EvaluationState, GrantUse, GrantUseId, LeaseRecord,
    LeaseRecordId, realm_item_id,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DecisionRecord {
    Approval(ApprovalRecord),
    GrantUse(GrantUse),
    DelegationUse(DelegationUse),
    ApprovalUse(ApprovalUse),
    Challenge(ChallengeRecord),
    Lease(LeaseRecord),
    Audit(Box<DecisionAudit>),
}

impl DecisionRecord {
    pub(super) fn emit(&self, context: &CommandContext) -> Result<(), CommandError> {
        match self {
            Self::Approval(record) => context.emit_set(record),
            Self::GrantUse(record) => context.emit_set(record),
            Self::DelegationUse(record) => context.emit_set(record),
            Self::ApprovalUse(record) => context.emit_set(record),
            Self::Challenge(record) => context.emit_set(record),
            Self::Lease(record) => context.emit_set(record),
            Self::Audit(record) => context.emit_set(record.as_ref()),
        }
    }

    pub(super) fn mutation(&self) -> Result<ItemMutation, String> {
        match self {
            Self::Approval(record) => ItemMutation::set(record),
            Self::GrantUse(record) => ItemMutation::set(record),
            Self::DelegationUse(record) => ItemMutation::set(record),
            Self::ApprovalUse(record) => ItemMutation::set(record),
            Self::Challenge(record) => ItemMutation::set(record),
            Self::Lease(record) => ItemMutation::set(record),
            Self::Audit(record) => ItemMutation::set(record.as_ref()),
        }
        .map_err(|error| error.to_string())
    }

    pub(super) const fn item_type(&self) -> &'static str {
        match self {
            Self::Approval(_) => ApprovalRecord::ITEM_TYPE,
            Self::GrantUse(_) => GrantUse::ITEM_TYPE,
            Self::DelegationUse(_) => DelegationUse::ITEM_TYPE,
            Self::ApprovalUse(_) => ApprovalUse::ITEM_TYPE,
            Self::Challenge(_) => ChallengeRecord::ITEM_TYPE,
            Self::Lease(_) => LeaseRecord::ITEM_TYPE,
            Self::Audit(_) => DecisionAudit::ITEM_TYPE,
        }
    }

    pub(super) fn item_id(&self) -> &str {
        match self {
            Self::Approval(record) => record.id.as_ref(),
            Self::GrantUse(record) => record.id.as_ref(),
            Self::DelegationUse(record) => record.id.as_ref(),
            Self::ApprovalUse(record) => record.id.as_ref(),
            Self::Challenge(record) => record.id.as_ref(),
            Self::Lease(record) => record.id.as_ref(),
            Self::Audit(record) => record.id.as_ref(),
        }
    }
}

fn use_id(decision: &str, kind: &str, contributor: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"myko/authority-use/v1\0");
    for part in [decision, kind, contributor] {
        digest.update(Sha256::digest(part.as_bytes()));
    }
    format!("{kind}/{:x}", digest.finalize())
}

pub fn decision_records(
    realm: &AuthorityRealmKey,
    request: &AccessAttempt,
    state: &EvaluationState,
    outcome: &EvaluationOutcome,
    decision_id: &str,
    now: DateTime<Utc>,
) -> Vec<DecisionRecord> {
    let realm_id = realm_item_id(realm);
    let mut records = Vec::new();
    let consume = request.authorization_phase == AuthorizationPhase::Effect
        || (request.authorization_phase == AuthorizationPhase::Admission
            && request.operation != AccessOperation::SubmitCommand);
    if consume {
        records.extend(outcome.grants.iter().map(|grant_id| {
            DecisionRecord::GrantUse(GrantUse {
                id: GrantUseId::from(use_id(decision_id, "grant-use", grant_id.as_str())),
                authority_realm_id: realm_id.clone(),
                grant_id: grant_id.clone(),
                decision_id: decision_id.to_owned(),
                used_at: now,
            })
        }));
        records.extend(outcome.delegations.iter().map(|delegation_id| {
            DecisionRecord::DelegationUse(DelegationUse {
                id: DelegationUseId::from(use_id(
                    decision_id,
                    "delegation-use",
                    delegation_id.as_str(),
                )),
                authority_realm_id: realm_id.clone(),
                delegation_id: delegation_id.clone(),
                decision_id: decision_id.to_owned(),
                used_at: now,
            })
        }));
        records.extend(outcome.approvals.iter().map(|approval_id| {
            DecisionRecord::ApprovalUse(ApprovalUse {
                id: ApprovalUseId::from(use_id(decision_id, "approval-use", approval_id.as_str())),
                authority_realm_id: realm_id.clone(),
                approval_id: approval_id.clone(),
                decision_id: decision_id.to_owned(),
                used_at: now,
            })
        }));
    }
    match &outcome.decision {
        AuthorizationDecision::Challenge { challenge, .. } => {
            if !state
                .challenges
                .iter()
                .any(|record| record.challenge.id == challenge.id)
            {
                records.push(DecisionRecord::Challenge(ChallengeRecord {
                    id: ChallengeRecordId::from(challenge.id.as_str()),
                    authority_realm_id: realm_id.clone(),
                    challenge: challenge.clone(),
                }));
            }
        }
        AuthorizationDecision::Permit(permit) => {
            if let Some(lease) = &permit.lease {
                records.push(DecisionRecord::Lease(LeaseRecord {
                    id: LeaseRecordId::from(lease.id.as_str()),
                    authority_realm_id: realm_id.clone(),
                    lease: lease.clone(),
                    binding: AuthorizationBinding::from_request(request),
                }));
            }
        }
        AuthorizationDecision::Deny(_) => {}
    }
    records.push(DecisionRecord::Audit(Box::new(DecisionAudit {
        id: DecisionAuditId::from(decision_id),
        authority_realm_id: realm_id,
        request: request.clone(),
        decision: outcome.decision.clone(),
        recorded_at: now,
    })));
    records
}

#[cfg(test)]
mod tests {
    use super::use_id;

    #[test]
    fn uses_bind_decision_kind_and_contributor_without_delimiter_aliases() {
        let original = use_id("decision-1", "grant-use", "grant-1");
        assert_eq!(
            original,
            "grant-use/769060eab913ccde326b75cc7cdce30a5896dd7a24da5bc4588a8fee66228881"
        );
        assert_eq!(original, use_id("decision-1", "grant-use", "grant-1"));
        assert_ne!(original, use_id("decision-2", "grant-use", "grant-1"));
        assert_ne!(original, use_id("decision-1", "approval-use", "grant-1"));
        assert_ne!(original, use_id("decision-1", "grant-use", "grant-2"));
        assert_ne!(use_id("ab", "c", "d"), use_id("a", "bc", "d"));
    }
}
