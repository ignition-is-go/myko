//! Authority reconstructed at a certified historical head, never live permission.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use myko_federation::{
    AccessAttempt, AuthorizationDecision, EventEnvelope, MutationOperation, NodeEvent,
    ScopeTopology, control_quorum::ControlHead,
};
use myko_items::{ItemProjection, MykoItem};

use crate::{
    ApprovalRecord, ApprovalUse, AuthorityRealm, AuthorityRealmKey, CapabilityRegistration,
    ChallengeRecord, DecisionAudit, DelegationRecord, DelegationUse, EvaluationState, GrantRecord,
    GrantUse, LeaseRecord, ObligationRecord, evaluate, requires_durable_evaluation,
};

mod history;
mod issuer;
mod rotation;
pub use history::{AuthorityAnchor, AuthorityHistory, AuthoritySelection};
pub use issuer::AuthorityController;
pub use rotation::AuthorityRotation;

/// An assessment of retained authority at one head and time, not an access permit.
pub struct HistoricalAuthorityAssessment {
    head: ControlHead,
    decision: AuthorizationDecision,
    requires_certified_effect: bool,
}

impl HistoricalAuthorityAssessment {
    #[must_use]
    pub const fn head(&self) -> ControlHead {
        self.head
    }

    /// Historical evaluator output. No current-head or consumption proof is implied.
    #[must_use]
    pub const fn decision_at_head(&self) -> &AuthorizationDecision {
        &self.decision
    }

    /// Whether this outcome would require a certified authority write before use.
    #[must_use]
    pub const fn requires_certified_effect(&self) -> bool {
        self.requires_certified_effect
    }
}

impl AuthorityHistory {
    /// Reconstruct and assess authority at an explicitly requested historical head.
    ///
    /// This does not implement `AccessPolicy`, establish currentness, or consume
    /// a grant, approval, or delegation. Topology is supplied for this assessment.
    ///
    /// # Errors
    /// Rejects unavailable or invalid certified history and malformed authority items.
    pub fn assess_at(
        &self,
        head: ControlHead,
        request: &AccessAttempt,
        at: DateTime<Utc>,
        topology: ScopeTopology,
    ) -> Result<HistoricalAuthorityAssessment, String> {
        let selected = self.selected_at(head)?;
        let state = project_facts(&selected, self.realm_id(), topology)?;
        let outcome = evaluate(&state, request, at);
        let requires_certified_effect = requires_durable_evaluation(&state, request, &outcome);
        Ok(HistoricalAuthorityAssessment {
            head,
            decision: outcome.decision,
            requires_certified_effect,
        })
    }
}

fn project<T: MykoItem>(
    events: &[EventEnvelope],
    realm: &AuthorityRealmKey,
) -> Result<Vec<T>, String> {
    let mut projection = ItemProjection::<T>::default();
    let immutable = [
        GrantUse::ITEM_TYPE,
        DelegationUse::ITEM_TYPE,
        ApprovalUse::ITEM_TYPE,
        DecisionAudit::ITEM_TYPE,
    ]
    .contains(&T::ITEM_TYPE);
    let mut seen = BTreeSet::new();
    for event in events {
        let NodeEvent::CommandCommitted { batch, .. } = &event.event else {
            continue;
        };
        for mutation in &batch.changes {
            if mutation.item_type == T::ITEM_TYPE {
                if immutable
                    && (mutation.operation != MutationOperation::Set
                        || !seen.insert(mutation.item_id.clone()))
                {
                    return Err(
                        "certified authority reused or removed an immutable use or audit"
                            .to_owned(),
                    );
                }
                if mutation.operation == MutationOperation::Set {
                    let item = mutation
                        .decode_set::<T>()
                        .map_err(|error| error.to_string())?;
                    if item.scope_id().as_ref() != realm.as_str() {
                        return Err(
                            "certified authority payload belongs to another realm".to_owned()
                        );
                    }
                }
            }
            projection
                .apply(mutation)
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(projection.values().cloned().collect())
}

fn project_facts(
    events: &[EventEnvelope],
    realm: &AuthorityRealmKey,
    topology: ScopeTopology,
) -> Result<EvaluationState, String> {
    let supported = [
        AuthorityRealm::ITEM_TYPE,
        CapabilityRegistration::ITEM_TYPE,
        GrantRecord::ITEM_TYPE,
        DelegationRecord::ITEM_TYPE,
        ObligationRecord::ITEM_TYPE,
        ChallengeRecord::ITEM_TYPE,
        ApprovalRecord::ITEM_TYPE,
        LeaseRecord::ITEM_TYPE,
        GrantUse::ITEM_TYPE,
        DelegationUse::ITEM_TYPE,
        ApprovalUse::ITEM_TYPE,
        DecisionAudit::ITEM_TYPE,
    ];
    for event in events {
        let NodeEvent::CommandCommitted { batch, .. } = &event.event else {
            continue;
        };
        if batch
            .changes
            .iter()
            .any(|change| !supported.contains(&change.item_type.as_str()))
        {
            return Err("certified authority record contains an unknown item type".to_owned());
        }
    }
    project::<DecisionAudit>(events, realm)?;
    let state = EvaluationState {
        realm: project::<AuthorityRealm>(events, realm)?.into_iter().next(),
        capabilities: project::<CapabilityRegistration>(events, realm)?,
        grants: project::<GrantRecord>(events, realm)?,
        delegations: project::<DelegationRecord>(events, realm)?,
        obligations: project::<ObligationRecord>(events, realm)?,
        challenges: project::<ChallengeRecord>(events, realm)?,
        approvals: project::<ApprovalRecord>(events, realm)?,
        grant_uses: project::<GrantUse>(events, realm)?,
        delegation_uses: project::<DelegationUse>(events, realm)?,
        approval_uses: project::<ApprovalUse>(events, realm)?,
        leases: project::<LeaseRecord>(events, realm)?,
        topology,
    };
    let mut fact_realms = state
        .grants
        .iter()
        .map(|record| &record.grant.realm_id)
        .chain(
            state
                .delegations
                .iter()
                .map(|record| &record.delegation.realm_id),
        )
        .chain(
            state
                .obligations
                .iter()
                .map(|record| &record.obligation.realm_id),
        )
        .chain(
            state
                .challenges
                .iter()
                .map(|record| &record.challenge.realm_id),
        )
        .chain(
            state
                .approvals
                .iter()
                .map(|record| &record.decision.realm_id),
        );
    if fact_realms.any(|fact_realm| fact_realm != realm) {
        return Err("certified authority fact names another realm".to_owned());
    }
    Ok(state)
}

fn validate_facts(events: &[EventEnvelope], realm: &AuthorityRealmKey) -> Result<(), String> {
    project_facts(events, realm, ScopeTopology::default()).map(|_| ())
}
