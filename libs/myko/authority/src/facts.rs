use super::*;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct AuthorityFacts {
    realm: Option<AuthorityRealm>,
    capabilities: Vec<CapabilityRegistration>,
    grants: Vec<GrantRecord>,
    delegations: Vec<DelegationRecord>,
    obligations: Vec<ObligationRecord>,
    challenges: Vec<ChallengeRecord>,
    approvals: Vec<ApprovalRecord>,
    grant_uses: Vec<GrantUse>,
    delegation_uses: Vec<DelegationUse>,
    approval_uses: Vec<ApprovalUse>,
    leases: Vec<LeaseRecord>,
}

pub(super) struct EvaluationState {
    pub(super) realm: Option<AuthorityRealm>,
    pub(super) capabilities: Vec<CapabilityRegistration>,
    pub(super) grants: Vec<GrantRecord>,
    pub(super) delegations: Vec<DelegationRecord>,
    pub(super) obligations: Vec<ObligationRecord>,
    pub(super) challenges: Vec<ChallengeRecord>,
    pub(super) approvals: Vec<ApprovalRecord>,
    pub(super) grant_uses: Vec<GrantUse>,
    pub(super) delegation_uses: Vec<DelegationUse>,
    pub(super) approval_uses: Vec<ApprovalUse>,
    pub(super) leases: Vec<LeaseRecord>,
    pub(super) topology: ScopeTopology,
}

impl AuthorityFacts {
    pub(super) fn with_topology(self, topology: ScopeTopology) -> EvaluationState {
        EvaluationState {
            realm: self.realm,
            capabilities: self.capabilities,
            grants: self.grants,
            delegations: self.delegations,
            obligations: self.obligations,
            challenges: self.challenges,
            approvals: self.approvals,
            grant_uses: self.grant_uses,
            delegation_uses: self.delegation_uses,
            approval_uses: self.approval_uses,
            leases: self.leases,
            topology,
        }
    }
}

pub(super) struct AuthorityFactSources {
    realms: Arc<myko::server::federated_source::FederatedMapSource>,
    capabilities: Arc<myko::server::federated_source::FederatedMapSource>,
    grants: Arc<myko::server::federated_source::FederatedMapSource>,
    delegations: Arc<myko::server::federated_source::FederatedMapSource>,
    obligations: Arc<myko::server::federated_source::FederatedMapSource>,
    challenges: Arc<myko::server::federated_source::FederatedMapSource>,
    approvals: Arc<myko::server::federated_source::FederatedMapSource>,
    grant_uses: Arc<myko::server::federated_source::FederatedMapSource>,
    delegation_uses: Arc<myko::server::federated_source::FederatedMapSource>,
    approval_uses: Arc<myko::server::federated_source::FederatedMapSource>,
    leases: Arc<myko::server::federated_source::FederatedMapSource>,
}

impl AuthorityFactSources {
    pub(super) fn open(
        host: &ApplicationHost,
        source_node: myko_federation::NodeId,
        scope: &ScopeId,
    ) -> Result<Self, String> {
        macro_rules! watch {
            ($item:ty) => {
                host.item_source::<$item>(Some(source_node), Some(scope.clone()))?
            };
        }
        Ok(Self {
            realms: watch!(AuthorityRealm),
            capabilities: watch!(CapabilityRegistration),
            grants: watch!(GrantRecord),
            delegations: watch!(DelegationRecord),
            obligations: watch!(ObligationRecord),
            challenges: watch!(ChallengeRecord),
            approvals: watch!(ApprovalRecord),
            grant_uses: watch!(GrantUse),
            delegation_uses: watch!(DelegationUse),
            approval_uses: watch!(ApprovalUse),
            leases: watch!(LeaseRecord),
        })
    }

    pub(super) fn snapshot(&self, realm_id: &AuthorityRealmKey) -> AuthorityFacts {
        fn values<T>(source: &myko::server::federated_source::FederatedMapSource) -> Vec<T>
        where
            T: myko::item::AnyItem + Clone,
        {
            source
                .rows()
                .snapshot()
                .into_iter()
                .filter_map(|(_, value)| value.as_any().downcast_ref::<T>().cloned())
                .collect()
        }
        let realm_id = realm_item_id(realm_id);
        AuthorityFacts {
            realm: values::<AuthorityRealm>(&self.realms)
                .into_iter()
                .find(|realm| realm.id == realm_id),
            capabilities: values::<CapabilityRegistration>(&self.capabilities),
            grants: values::<GrantRecord>(&self.grants),
            delegations: values::<DelegationRecord>(&self.delegations),
            obligations: values::<ObligationRecord>(&self.obligations),
            challenges: values::<ChallengeRecord>(&self.challenges),
            approvals: values::<ApprovalRecord>(&self.approvals),
            grant_uses: values::<GrantUse>(&self.grant_uses),
            delegation_uses: values::<DelegationUse>(&self.delegation_uses),
            approval_uses: values::<ApprovalUse>(&self.approval_uses),
            leases: values::<LeaseRecord>(&self.leases),
        }
    }

    pub(super) fn subscribe_revision(
        &self,
        revision: &Arc<AtomicU64>,
        writer: &Cell<u64, CellMutable>,
    ) -> Vec<SubscriptionGuard> {
        fn watch(
            source: &myko::server::federated_source::FederatedMapSource,
            revision: Arc<AtomicU64>,
            writer: Cell<u64, CellMutable>,
        ) -> SubscriptionGuard {
            source.rows().subscribe_diffs(move |_| {
                let next = revision.fetch_add(1, Ordering::AcqRel).saturating_add(1);
                writer.set(next);
            })
        }
        vec![
            watch(&self.realms, Arc::clone(revision), writer.clone()),
            watch(&self.capabilities, Arc::clone(revision), writer.clone()),
            watch(&self.grants, Arc::clone(revision), writer.clone()),
            watch(&self.delegations, Arc::clone(revision), writer.clone()),
            watch(&self.obligations, Arc::clone(revision), writer.clone()),
            watch(&self.challenges, Arc::clone(revision), writer.clone()),
            watch(&self.approvals, Arc::clone(revision), writer.clone()),
            watch(&self.grant_uses, Arc::clone(revision), writer.clone()),
            watch(&self.delegation_uses, Arc::clone(revision), writer.clone()),
            watch(&self.approval_uses, Arc::clone(revision), writer.clone()),
            watch(&self.leases, Arc::clone(revision), writer.clone()),
        ]
    }
}

pub(super) fn load_state(
    node: &Node,
    realm_id: &AuthorityRealmKey,
) -> Result<EvaluationState, myko_federation::NodeError> {
    let scope = authority_realm_scope(realm_id);
    let source = node.node_id();
    Ok(AuthorityFacts {
        realm: node
            .query_items_in(
                source,
                &scope,
                GetAuthorityRealmById {
                    id: realm_item_id(realm_id),
                },
            )?
            .into_iter()
            .next(),
        capabilities: node.query_items_in(source, &scope, GetAllCapabilityRegistrations)?,
        grants: node.query_items_in(source, &scope, GetAllGrantRecords)?,
        delegations: node.query_items_in(source, &scope, GetAllDelegationRecords)?,
        obligations: node.query_items_in(source, &scope, GetAllObligationRecords)?,
        challenges: node.query_items_in(source, &scope, GetAllChallengeRecords)?,
        approvals: node.query_items_in(source, &scope, GetAllApprovalRecords)?,
        grant_uses: node.query_items_in(source, &scope, GetAllGrantUses)?,
        delegation_uses: node.query_items_in(source, &scope, GetAllDelegationUses)?,
        approval_uses: node.query_items_in(source, &scope, GetAllApprovalUses)?,
        leases: node.query_items_in(source, &scope, GetAllLeaseRecords)?,
    }
    .with_topology(node.scope_topology()?))
}

pub(super) struct EvaluationOutcome {
    pub(super) decision: AuthorizationDecision,
    pub(super) grants: BTreeSet<AuthorityGrantId>,
    pub(super) delegations: BTreeSet<DelegationId>,
    pub(super) approvals: BTreeSet<ApprovalId>,
}

pub(super) fn requires_durable_evaluation(
    state: &EvaluationState,
    request: &AccessAttempt,
    outcome: &EvaluationOutcome,
) -> bool {
    let permit = match &outcome.decision {
        AuthorizationDecision::Permit(permit) => permit,
        AuthorizationDecision::Challenge { .. } => return true,
        AuthorizationDecision::Deny(_) => return false,
    };
    if permit.lease.is_some() {
        return true;
    }
    let consumes_authority = matches!(
        request.authorization_phase,
        myko_federation::AuthorizationPhase::Effect
    ) || (matches!(
        request.authorization_phase,
        myko_federation::AuthorizationPhase::Admission
    ) && request.operation != AccessOperation::SubmitCommand);
    if !consumes_authority {
        return false;
    }
    outcome.grants.iter().any(|id| {
        state
            .grants
            .iter()
            .any(|record| record.grant.id == *id && record.grant.max_uses.is_some())
    }) || outcome.delegations.iter().any(|id| {
        state
            .delegations
            .iter()
            .any(|record| record.delegation.id == *id && record.delegation.max_uses.is_some())
    }) || !outcome.approvals.is_empty()
}

pub(super) const fn permission_for(operation: AccessOperation) -> Option<FederationPermission> {
    match operation {
        AccessOperation::ReadHistory | AccessOperation::FollowHistory => {
            Some(FederationPermission::ReadHistory)
        }
        AccessOperation::ReadItems
        | AccessOperation::FollowItems
        | AccessOperation::FollowHandler
        | AccessOperation::ReadCommand
        | AccessOperation::ReadCommands
        | AccessOperation::WatchCommand
        | AccessOperation::WatchCommands => Some(FederationPermission::ReadState),
        AccessOperation::SubmitCommand | AccessOperation::CancelCommand => {
            Some(FederationPermission::Write)
        }
        AccessOperation::AdministerAuthority => Some(FederationPermission::Admin),
        AccessOperation::DelegateAuthority => Some(FederationPermission::Reshare),
        AccessOperation::SubscribeLive | AccessOperation::ApproveAuthority => None,
    }
}

pub(super) const fn is_stream(operation: AccessOperation) -> bool {
    matches!(
        operation,
        AccessOperation::FollowItems
            | AccessOperation::FollowHandler
            | AccessOperation::FollowHistory
            | AccessOperation::WatchCommand
            | AccessOperation::WatchCommands
            | AccessOperation::SubscribeLive
    )
}
