use super::*;
use myko::common::with_id::WithId as _;

/// Durable authority entities. All records share one realm scope so evaluation
/// and consumption commit atomically within one Myko service.
#[myko_service(
    AuthorityRealm,
    CapabilityRegistration,
    GrantRecord,
    DelegationRecord,
    ObligationRecord,
    ChallengeRecord,
    ApprovalRecord,
    LeaseRecord,
    GrantUse,
    DelegationUse,
    ApprovalUse,
    DecisionAudit
)]
pub struct AuthorityService;

/// One node-local authorization realm and its immutable bootstrap identity.
#[myko_item(service = AuthorityService, scope_root)]
pub struct AuthorityRealm {
    pub bootstrap_principal: Principal,
    pub bootstrapped_at: DateTime<Utc>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct CapabilityRegistration {
    pub capability: ApplicationCapability,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct GrantRecord {
    pub grant: AuthorityGrant,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct DelegationRecord {
    pub delegation: AuthorityDelegation,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct ObligationRecord {
    pub obligation: Obligation,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct ChallengeRecord {
    pub challenge: AuthorityChallenge,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct ApprovalRecord {
    pub decision: ApprovalDecision,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct LeaseRecord {
    pub lease: AuthorityLease,
    pub binding: AuthorizationBinding,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct GrantUse {
    pub grant_id: AuthorityGrantId,
    pub decision_id: String,
    pub used_at: DateTime<Utc>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct DelegationUse {
    pub delegation_id: DelegationId,
    pub decision_id: String,
    pub used_at: DateTime<Utc>,
}

#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct ApprovalUse {
    pub approval_id: ApprovalId,
    pub decision_id: String,
    pub used_at: DateTime<Utc>,
}

/// Immutable explanation record. Consumers obtain live projections with the
/// generated `GetAllDecisionAudits` query or `Node::watch_items_in`.
#[myko_item(service = AuthorityService, scoped_by = AuthorityRealm)]
pub struct DecisionAudit {
    pub request: AccessAttempt,
    pub decision: AuthorizationDecision,
    pub recorded_at: DateTime<Utc>,
}

myko::register_federated_item!(AuthorityRealm);
myko::register_federated_item!(CapabilityRegistration);
myko::register_federated_item!(GrantRecord);
myko::register_federated_item!(DelegationRecord);
myko::register_federated_item!(ObligationRecord);
myko::register_federated_item!(ChallengeRecord);
myko::register_federated_item!(ApprovalRecord);
myko::register_federated_item!(LeaseRecord);
myko::register_federated_item!(GrantUse);
myko::register_federated_item!(DelegationUse);
myko::register_federated_item!(ApprovalUse);
myko::register_federated_item!(DecisionAudit);

/// Live grant state for one authority realm on one authoritative node.
#[myko_view(GrantRecord, item = GrantRecord)]
#[derive(PartialEq, Eq)]
pub struct AuthorityGrantsView {
    pub source_node: myko_federation::NodeId,
    pub realm_id: AuthorityRealmKey,
}

impl ViewHandler for AuthorityGrantsView {
    fn source_node(&self, _local_node: myko_federation::NodeId) -> Option<myko_federation::NodeId> {
        Some(self.source_node)
    }

    fn scope_id(&self, _local_node: myko_federation::NodeId) -> Option<ScopeId> {
        Some(authority_realm_scope(&self.realm_id))
    }

    #[allow(clippy::expect_used)]
    fn build_cell(
        context: ViewBuildArgs<Self>,
    ) -> impl myko::view::ViewBuildOutput<Item = Self::Item> {
        let source_node = context.view.source_node;
        let scope = ScopeSelection::Exact(authority_realm_scope(&context.view.realm_id));
        myko::view::RetainedView::new(
            context
                .sourced_snapshots_selected::<GrantRecord>(scope)
                .expect("validated authority-grant federation source")
                .map_value(move |rows| {
                    rows.iter()
                        .filter(|(key, _record)| key.source_node == source_node)
                        .map(|(_key, record)| {
                            let record = Arc::new(record.item.clone());
                            (record.id(), record)
                        })
                        .collect()
                }),
        )
    }
}

/// Returns the typed scope containing one authority realm's durable facts.
#[must_use]
pub fn authority_realm_scope(realm_id: &AuthorityRealmKey) -> ScopeId {
    ScopeId::for_item::<AuthorityRealm>(&AuthorityRealmId::from(realm_id.as_str()))
}

pub(super) fn realm_item_id(realm_id: &AuthorityRealmKey) -> AuthorityRealmId {
    AuthorityRealmId::from(realm_id.as_str())
}

pub(super) fn administration_claim(realm_id: &AuthorityRealmKey) -> ResourceClaim {
    let mut claim =
        ResourceClaim::scope(authority_realm_scope(realm_id), ResourceClaimKind::Primary);
    claim.service_id = Some(myko_federation::ServiceId::new(
        AuthorityService::SERVICE_ID,
    ));
    claim.required_permissions.push(FederationPermission::Admin);
    claim
        .required_permissions
        .push(FederationPermission::ReadState);
    claim
        .required_operations
        .push(AccessOperation::AdministerAuthority);
    claim.required_operations.push(AccessOperation::ReadItems);
    claim
}

fn authority_principal(node: &ApplicationHost) -> PrincipalId {
    PrincipalId::new(format!("service:myko-authority@{}", node.node_id()))
}

pub(super) fn authority_presentation(node: &ApplicationHost) -> AuthorityPresentation {
    AuthorityPresentation::direct(Principal::new(
        authority_principal(node),
        PrincipalKind::Service,
    ))
}

pub(super) fn record_id(prefix: &str) -> String {
    format!("{prefix}:{}", Uuid::new_v4())
}
