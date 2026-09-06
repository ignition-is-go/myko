//! Authority reconstructed at a certified historical head, never live permission.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use myko_federation::{
    AccessAttempt, AuthorizationDecision, EventEnvelope, MutationOperation, NodeEvent,
    ScopeTopology, control_quorum::ControlHead,
};
use myko_items::{ItemProjection, MykoItem};

use crate::decision_records::DecisionRecord;
use crate::{
    ApprovalRecord, ApprovalUse, AuthorityRealm, AuthorityRealmKey, CapabilityRegistration,
    ChallengeRecord, DecisionAudit, DelegationRecord, DelegationUse, EvaluationState, GrantRecord,
    GrantUse, LeaseRecord, ObligationRecord, evaluate, requires_durable_evaluation,
};

mod coordinator;
mod history;
mod issuer;
mod rotation;
pub use coordinator::{
    AuthorityControllerPrincipal, AuthorityCoordinatorPeer, AuthorityDecisionCoordinator,
    AuthorityRequestSource, CertifiedAuthorityControlEndpoint, CertifiedAuthorityRequest,
    CoordinatedAuthorityDecision, LocalAuthorityPeer,
};
pub use history::{
    AuthorityAnchor, AuthorityDecisionRevalidation, AuthorityDecisionRoot,
    AuthorityDecisionTransition, AuthorityHistory, AuthoritySelection,
};
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
        let facts = self.selected_at(head)?;
        let state = project_facts(&facts, self.realm_id(), topology)?;
        let outcome = evaluate(&state, request, at);
        let requires_certified_effect = requires_durable_evaluation(&state, request, &outcome);
        Ok(HistoricalAuthorityAssessment {
            head,
            decision: outcome.decision,
            requires_certified_effect,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum CertifiedAuthorityFact {
    Event(EventEnvelope),
    Decision(DecisionRecord),
}

fn project<T: MykoItem>(
    facts: &[CertifiedAuthorityFact],
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
    for fact in facts {
        match fact {
            CertifiedAuthorityFact::Event(event) => {
                let NodeEvent::CommandCommitted { batch, .. } = &event.event else {
                    continue;
                };
                for mutation in &batch.changes {
                    project_mutation::<T>(&mut projection, &mut seen, immutable, mutation, realm)?;
                }
            }
            CertifiedAuthorityFact::Decision(record) if record.item_type() == T::ITEM_TYPE => {
                let mutation = record.mutation()?;
                project_mutation::<T>(&mut projection, &mut seen, immutable, &mutation, realm)?;
            }
            CertifiedAuthorityFact::Decision(_) => {}
        }
    }
    Ok(projection.values().cloned().collect())
}

fn project_mutation<T: MykoItem>(
    projection: &mut ItemProjection<T>,
    seen: &mut BTreeSet<String>,
    immutable: bool,
    mutation: &myko_federation::ItemMutation,
    realm: &AuthorityRealmKey,
) -> Result<(), String> {
    if mutation.item_type == T::ITEM_TYPE {
        if immutable
            && (mutation.operation != MutationOperation::Set
                || !seen.insert(mutation.item_id.clone()))
        {
            return Err(
                "certified authority reused or removed an immutable use or audit".to_owned(),
            );
        }
        if mutation.operation == MutationOperation::Set {
            let item = mutation
                .decode_set::<T>()
                .map_err(|error| error.to_string())?;
            if item.scope_id().as_ref() != realm.as_str() {
                return Err("certified authority payload belongs to another realm".to_owned());
            }
        }
    }
    projection
        .apply(mutation)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn project_facts(
    facts: &[CertifiedAuthorityFact],
    realm: &AuthorityRealmKey,
    topology: ScopeTopology,
) -> Result<EvaluationState, String> {
    validate_supported_fact_types(facts)?;
    project::<DecisionAudit>(facts, realm)?;
    let state = EvaluationState {
        realm: project::<AuthorityRealm>(facts, realm)?.into_iter().next(),
        capabilities: project::<CapabilityRegistration>(facts, realm)?,
        grants: project::<GrantRecord>(facts, realm)?,
        delegations: project::<DelegationRecord>(facts, realm)?,
        obligations: project::<ObligationRecord>(facts, realm)?,
        challenges: project::<ChallengeRecord>(facts, realm)?,
        approvals: project::<ApprovalRecord>(facts, realm)?,
        grant_uses: project::<GrantUse>(facts, realm)?,
        delegation_uses: project::<DelegationUse>(facts, realm)?,
        approval_uses: project::<ApprovalUse>(facts, realm)?,
        leases: project::<LeaseRecord>(facts, realm)?,
        topology,
    };
    validate_fact_realms(&state, realm)?;
    Ok(state)
}

fn validate_supported_fact_types(facts: &[CertifiedAuthorityFact]) -> Result<(), String> {
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
    for fact in facts {
        match fact {
            CertifiedAuthorityFact::Event(event) => {
                let NodeEvent::CommandCommitted { batch, .. } = &event.event else {
                    continue;
                };
                if batch
                    .changes
                    .iter()
                    .any(|change| !supported.contains(&change.item_type.as_str()))
                {
                    return Err(
                        "certified authority record contains an unknown item type".to_owned()
                    );
                }
            }
            CertifiedAuthorityFact::Decision(record)
                if !supported.contains(&record.item_type()) =>
            {
                return Err(
                    "certified authority decision contains an unknown record type".to_owned(),
                );
            }
            CertifiedAuthorityFact::Decision(_) => {}
        }
    }
    Ok(())
}

fn validate_fact_realms(state: &EvaluationState, realm: &AuthorityRealmKey) -> Result<(), String> {
    let mut fact_realms = state
        .grants
        .iter()
        .map(|record| record.grant.realm_id.as_str())
        .chain(
            state
                .delegations
                .iter()
                .map(|record| record.delegation.realm_id.as_str()),
        )
        .chain(
            state
                .obligations
                .iter()
                .map(|record| record.obligation.realm_id.as_str()),
        )
        .chain(
            state
                .challenges
                .iter()
                .map(|record| record.challenge.realm_id.as_str()),
        )
        .chain(
            state
                .approvals
                .iter()
                .map(|record| record.decision.realm_id.as_str()),
        )
        .chain(
            state
                .grant_uses
                .iter()
                .map(|record| record.authority_realm_id.as_ref()),
        )
        .chain(
            state
                .delegation_uses
                .iter()
                .map(|record| record.authority_realm_id.as_ref()),
        )
        .chain(
            state
                .approval_uses
                .iter()
                .map(|record| record.authority_realm_id.as_ref()),
        )
        .chain(
            state
                .leases
                .iter()
                .map(|record| record.authority_realm_id.as_ref()),
        );
    if fact_realms.any(|fact_realm| fact_realm != realm.as_str()) {
        return Err("certified authority fact names another realm".to_owned());
    }
    Ok(())
}

fn validate_facts(
    facts: &[CertifiedAuthorityFact],
    realm: &AuthorityRealmKey,
) -> Result<(), String> {
    project_facts(facts, realm, ScopeTopology::default()).map(|_| ())
}
