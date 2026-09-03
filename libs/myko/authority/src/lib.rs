//! Durable, fail-closed authority for Myko nodes.
//!
//! Every authoritative fact is an ordinary item in [`AuthorityService`]. The
//! evaluator reads only the local node's projection and records decisions and
//! bounded-use consumption as one service-atomic Myko command before returning.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Duration, Utc};
use myko_app::capability::{
    CommandQuerying as _, EventPublishing as _, NodeScoped as _, RequestScoped as _,
};
use myko_app::{
    AppError, ApplicationNode, CommandContext, CommandError, CommandHandler, MykoApplication,
};
use myko_federation::{
    AccessOperation, AccessPolicy, AccessRequest, AllowAllAccessPolicy, ApplicationCapability,
    ApprovalDecision, ApprovalId, AuthorityChallenge, AuthorityConstraints, AuthorityDelegation,
    AuthorityGrant, AuthorityGrantId, AuthorityLease, AuthorityLeaseRequest, AuthorityPresentation,
    AuthorityRealmId as AuthorityRealmKey, AuthorizationBinding, AuthorizationDecision,
    AuthorizationExplanation, AuthorizationReport, CapabilityId, ChallengeId, CommandState,
    DelegationId, DenyDecision, FederationPermission, LeaseId, MykoService as _, Node, NodeEvent,
    Obligation, PermitDecision, Principal, PrincipalId, PrincipalKind, ReplicationSelection,
    ResourceClaim, ResourceClaimKind, ResourceVisibility, ScopeId, ScopeSelection, ScopeTopology,
};
use myko_items::{myko_command, myko_item, myko_service};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    pub request: AccessRequest,
    pub decision: AuthorizationDecision,
    pub recorded_at: DateTime<Utc>,
}

fn realm_scope(realm_id: &AuthorityRealmKey) -> ScopeId {
    ScopeId::for_item::<AuthorityRealm>(&AuthorityRealmId::from(realm_id.as_str()))
}

fn realm_item_id(realm_id: &AuthorityRealmKey) -> AuthorityRealmId {
    AuthorityRealmId::from(realm_id.as_str())
}

fn administration_claim(realm_id: &AuthorityRealmKey) -> ResourceClaim {
    let mut claim = ResourceClaim::scope(realm_scope(realm_id), ResourceClaimKind::Primary);
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

fn authority_principal(node: &ApplicationNode) -> PrincipalId {
    PrincipalId::new(format!("service:myko-authority@{}", node.node_id()))
}

fn authority_presentation(node: &ApplicationNode) -> AuthorityPresentation {
    AuthorityPresentation::direct(Principal::new(
        authority_principal(node),
        PrincipalKind::Service,
    ))
}

fn is_internal_authority_request(application: &ApplicationNode, request: &AccessRequest) -> bool {
    let expected = authority_presentation(application);
    request.operation == AccessOperation::SubmitCommand
        && request
            .service_id
            .as_ref()
            .is_some_and(|service| service.as_str() == AuthorityService::SERVICE_ID.as_str())
        && request.principal_id == expected.executor.id
        && request.presentation == expected
        && request.command_principal_id.as_ref() == Some(&expected.principal.id)
}

fn record_id(prefix: &str) -> String {
    format!("{prefix}:{}", Uuid::new_v4())
}

#[myko_command(AuthorityRealm, service = AuthorityService, scope = AuthorityRealm)]
struct BootstrapRealm {
    realm_id: AuthorityRealmKey,
    principal: Principal,
    at: DateTime<Utc>,
}

impl CommandHandler for BootstrapRealm {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<AuthorityRealm, CommandError> {
        if context
            .exec_query(GetAuthorityRealmById {
                id: realm_item_id(&self.realm_id),
            })?
            .is_some()
        {
            return Err(CommandError::reject(
                "authority realm is already bootstrapped",
            ));
        }
        let realm = AuthorityRealm {
            id: realm_item_id(&self.realm_id),
            bootstrap_principal: self.principal.clone(),
            bootstrapped_at: self.at,
        };
        let grant = AuthorityGrant {
            id: AuthorityGrantId::new(format!("bootstrap:{}", self.realm_id)),
            realm_id: self.realm_id,
            grantor: self.principal.clone(),
            grantee: self.principal,
            selection: ScopeSelection::Exact(ScopeId::for_item::<AuthorityRealm>(&realm.id)),
            permissions: vec![
                FederationPermission::Admin,
                FederationPermission::ReadState,
                FederationPermission::Write,
                FederationPermission::Reshare,
            ],
            operations: vec![
                AccessOperation::AdministerAuthority,
                AccessOperation::ApproveAuthority,
                AccessOperation::ReadItems,
                AccessOperation::SubmitCommand,
                AccessOperation::DelegateAuthority,
            ],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: self.at,
            expires_at: None,
            max_uses: None,
        };
        context.emit_set(&realm)?;
        context.emit_set(&GrantRecord {
            id: GrantRecordId::from(grant.id.as_str()),
            authority_realm_id: realm.id.clone(),
            grant,
            revoked_at: None,
        })?;
        Ok(realm)
    }
}

#[myko_command((), service = AuthorityService, scope = AuthorityRealm)]
struct PutGrant {
    pub realm_id: AuthorityRealmKey,
    pub grant: AuthorityGrant,
}

impl CommandHandler for PutGrant {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<(), CommandError> {
        if self.grant.realm_id != self.realm_id {
            return Err(CommandError::reject(
                "grant belongs to another authority realm",
            ));
        }
        if self.grant.grantor != context.__request().authority.principal {
            return Err(CommandError::reject(
                "grantor does not match the authenticated authority principal",
            ));
        }
        if context
            .exec_query(GetGrantRecordById {
                id: GrantRecordId::from(self.grant.id.as_str()),
            })?
            .is_some()
        {
            return Err(CommandError::reject(
                "grant id is immutable and already exists",
            ));
        }
        context.emit_set(&GrantRecord {
            id: GrantRecordId::from(self.grant.id.as_str()),
            authority_realm_id: realm_item_id(&self.realm_id),
            grant: self.grant,
            revoked_at: None,
        })
    }
}

#[myko_command((), service = AuthorityService, scope = AuthorityRealm)]
struct PutDelegation {
    pub realm_id: AuthorityRealmKey,
    pub delegation: AuthorityDelegation,
    issuer: Principal,
}

impl CommandHandler for PutDelegation {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    #[allow(clippy::too_many_lines)] // Parent-chain validation is kept contiguous for auditability.
    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<(), CommandError> {
        if self.delegation.realm_id != self.realm_id {
            return Err(CommandError::reject(
                "delegation belongs to another authority realm",
            ));
        }
        if self.delegation.delegator != self.issuer {
            return Err(CommandError::reject(
                "delegator does not match the authenticated authority principal",
            ));
        }
        if context
            .exec_query(GetDelegationRecordById {
                id: DelegationRecordId::from(self.delegation.id.as_str()),
            })?
            .is_some()
        {
            return Err(CommandError::reject(
                "delegation id is immutable and already exists",
            ));
        }
        let topology = context
            .__node()
            .scope_topology()
            .map_err(|error| CommandError::retry(error.to_string()))?;
        let now = Utc::now();
        let grant_uses = context.exec_query(GetAllGrantUses)?.into_iter().fold(
            BTreeMap::<AuthorityGrantId, u64>::new(),
            |mut counts, usage| {
                counts
                    .entry(usage.grant_id)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
                counts
            },
        );
        let delegation_uses = context.exec_query(GetAllDelegationUses)?.into_iter().fold(
            BTreeMap::<DelegationId, u64>::new(),
            |mut counts, usage| {
                counts
                    .entry(usage.delegation_id)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
                counts
            },
        );
        let valid_parent = match &self.delegation.parent {
            myko_federation::DelegationParent::Grant(id) => context
                .exec_query(GetGrantRecordById {
                    id: GrantRecordId::from(id.as_str()),
                })?
                .filter(|record| record.revoked_at.is_none())
                .is_some_and(|record| {
                    let parent = record.grant;
                    self.delegation.delegator == parent.grantee
                        && parent.valid_from <= now
                        && parent.expires_at.is_none_or(|expiry| expiry > now)
                        && parent.max_uses.is_none_or(|maximum| {
                            grant_uses.get(&parent.id).copied().unwrap_or(0) < maximum
                        })
                        && self
                            .delegation
                            .selections
                            .iter()
                            .all(|selection| parent.selection.covers_in(selection, &topology))
                        && self
                            .delegation
                            .permissions
                            .iter()
                            .all(|permission| parent.permissions.contains(permission))
                        && self
                            .delegation
                            .operations
                            .iter()
                            .all(|operation| parent.operations.contains(operation))
                        && self
                            .delegation
                            .capabilities
                            .iter()
                            .all(|capability| parent.capabilities.contains(capability))
                        && self.delegation.constraints.attenuates(&parent.constraints)
                        && expiry_attenuates(self.delegation.expires_at, parent.expires_at)
                        && use_limit_attenuates(self.delegation.max_uses, parent.max_uses)
                }),
            myko_federation::DelegationParent::Delegation(id) => context
                .exec_query(GetDelegationRecordById {
                    id: DelegationRecordId::from(id.as_str()),
                })?
                .filter(|record| record.revoked_at.is_none())
                .is_some_and(|record| {
                    let parent = record.delegation;
                    self.delegation.delegator == parent.delegate
                        && parent.expires_at.is_none_or(|expiry| expiry > now)
                        && parent.max_uses.is_none_or(|maximum| {
                            delegation_uses.get(&parent.id).copied().unwrap_or(0) < maximum
                        })
                        && self.delegation.selections.iter().all(|selection| {
                            parent
                                .selections
                                .iter()
                                .any(|parent| parent.covers_in(selection, &topology))
                        })
                        && self
                            .delegation
                            .permissions
                            .iter()
                            .all(|permission| parent.permissions.contains(permission))
                        && self
                            .delegation
                            .operations
                            .iter()
                            .all(|operation| parent.operations.contains(operation))
                        && self
                            .delegation
                            .capabilities
                            .iter()
                            .all(|capability| parent.capabilities.contains(capability))
                        && self.delegation.constraints.attenuates(&parent.constraints)
                        && expiry_attenuates(self.delegation.expires_at, parent.expires_at)
                        && use_limit_attenuates(self.delegation.max_uses, parent.max_uses)
                }),
        };
        if !valid_parent {
            return Err(CommandError::reject(
                "delegation does not attenuate an active authoritative parent",
            ));
        }
        context.emit_set(&DelegationRecord {
            id: DelegationRecordId::from(self.delegation.id.as_str()),
            authority_realm_id: realm_item_id(&self.realm_id),
            delegation: self.delegation,
            revoked_at: None,
        })
    }
}

fn expiry_attenuates(child: Option<DateTime<Utc>>, parent: Option<DateTime<Utc>>) -> bool {
    match (child, parent) {
        (_, None) => true,
        (Some(child), Some(parent)) => child <= parent,
        (None, Some(_)) => false,
    }
}

const fn use_limit_attenuates(child: Option<u64>, parent: Option<u64>) -> bool {
    match (child, parent) {
        (_, None) => true,
        (Some(child), Some(parent)) => child <= parent,
        (None, Some(_)) => false,
    }
}

#[myko_command((), service = AuthorityService, scope = AuthorityRealm)]
struct PutObligation {
    pub realm_id: AuthorityRealmKey,
    pub obligation: Obligation,
}

impl CommandHandler for PutObligation {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<(), CommandError> {
        if self.obligation.realm_id != self.realm_id {
            return Err(CommandError::reject(
                "obligation belongs to another authority realm",
            ));
        }
        if context
            .exec_query(GetObligationRecordById {
                id: ObligationRecordId::from(self.obligation.id.as_str()),
            })?
            .is_some()
        {
            return Err(CommandError::reject(
                "obligation id is immutable and already exists",
            ));
        }
        context.emit_set(&ObligationRecord {
            id: ObligationRecordId::from(self.obligation.id.as_str()),
            authority_realm_id: realm_item_id(&self.realm_id),
            obligation: self.obligation,
            revoked_at: None,
        })
    }
}

#[myko_command((), service = AuthorityService, scope = AuthorityRealm)]
struct PutCapability {
    pub realm_id: AuthorityRealmKey,
    pub capability: ApplicationCapability,
}

impl CommandHandler for PutCapability {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<(), CommandError> {
        if context
            .exec_query(GetCapabilityRegistrationById {
                id: CapabilityRegistrationId::from(self.capability.id.as_str()),
            })?
            .is_some()
        {
            return Err(CommandError::reject(
                "capability id is immutable and already exists",
            ));
        }
        context.emit_set(&CapabilityRegistration {
            id: CapabilityRegistrationId::from(self.capability.id.as_str()),
            authority_realm_id: realm_item_id(&self.realm_id),
            capability: self.capability,
            revoked_at: None,
        })
    }
}

#[myko_command((), service = AuthorityService, scope = AuthorityRealm)]
struct RevokeAuthorityFact {
    pub realm_id: AuthorityRealmKey,
    pub kind: RevocationKind,
    pub id: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RevocationKind {
    Grant,
    Delegation,
    Obligation,
    Capability,
}

impl CommandHandler for RevokeAuthorityFact {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<(), CommandError> {
        match self.kind {
            RevocationKind::Grant => {
                let mut record = context
                    .exec_query(GetGrantRecordById {
                        id: GrantRecordId::from(self.id),
                    })?
                    .ok_or_else(|| CommandError::reject("grant is not authoritatively present"))?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Delegation => {
                let mut record = context
                    .exec_query(GetDelegationRecordById {
                        id: DelegationRecordId::from(self.id),
                    })?
                    .ok_or_else(|| {
                        CommandError::reject("delegation is not authoritatively present")
                    })?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Obligation => {
                let mut record = context
                    .exec_query(GetObligationRecordById {
                        id: ObligationRecordId::from(self.id),
                    })?
                    .ok_or_else(|| {
                        CommandError::reject("obligation is not authoritatively present")
                    })?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Capability => {
                let mut record = context
                    .exec_query(GetCapabilityRegistrationById {
                        id: CapabilityRegistrationId::from(self.id),
                    })?
                    .ok_or_else(|| {
                        CommandError::reject("capability is not authoritatively present")
                    })?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
        }
    }
}

#[myko_command(
    AuthorizationDecision,
    service = AuthorityService,
    scope = AuthorityRealm
)]
struct EvaluateAuthority {
    realm_id: AuthorityRealmKey,
    request: AccessRequest,
    now: DateTime<Utc>,
}

impl CommandHandler for EvaluateAuthority {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<AuthorizationDecision, CommandError> {
        let state = EvaluationState {
            realm: context.exec_query(GetAuthorityRealmById {
                id: realm_item_id(&self.realm_id),
            })?,
            capabilities: context.exec_query(GetAllCapabilityRegistrations)?,
            grants: context.exec_query(GetAllGrantRecords)?,
            delegations: context.exec_query(GetAllDelegationRecords)?,
            obligations: context.exec_query(GetAllObligationRecords)?,
            challenges: context.exec_query(GetAllChallengeRecords)?,
            approvals: context.exec_query(GetAllApprovalRecords)?,
            grant_uses: context.exec_query(GetAllGrantUses)?,
            delegation_uses: context.exec_query(GetAllDelegationUses)?,
            approval_uses: context.exec_query(GetAllApprovalUses)?,
            leases: context.exec_query(GetAllLeaseRecords)?,
            topology: context.__node().scope_topology().map_err(|error| {
                CommandError::retry(format!("authority topology unavailable: {error}"))
            })?,
        };
        let outcome = evaluate(&state, &self.request, self.now);
        let decision_id = record_id("decision");
        let realm_id = realm_item_id(&self.realm_id);

        let consume = matches!(
            self.request.authorization_phase,
            myko_federation::AuthorizationPhase::Effect
        ) || (matches!(
            self.request.authorization_phase,
            myko_federation::AuthorizationPhase::Admission
        ) && self.request.operation != AccessOperation::SubmitCommand);
        for grant_id in outcome.grants.iter().filter(|_| consume) {
            context.emit_set(&GrantUse {
                id: GrantUseId::from(record_id("grant-use")),
                authority_realm_id: realm_id.clone(),
                grant_id: grant_id.clone(),
                decision_id: decision_id.clone(),
                used_at: self.now,
            })?;
        }
        for delegation_id in outcome.delegations.iter().filter(|_| consume) {
            context.emit_set(&DelegationUse {
                id: DelegationUseId::from(record_id("delegation-use")),
                authority_realm_id: realm_id.clone(),
                delegation_id: delegation_id.clone(),
                decision_id: decision_id.clone(),
                used_at: self.now,
            })?;
        }
        for approval_id in outcome.approvals.iter().filter(|_| consume) {
            context.emit_set(&ApprovalUse {
                id: ApprovalUseId::from(record_id("approval-use")),
                authority_realm_id: realm_id.clone(),
                approval_id: approval_id.clone(),
                decision_id: decision_id.clone(),
                used_at: self.now,
            })?;
        }
        if let AuthorizationDecision::Challenge { challenge, .. } = &outcome.decision {
            let challenge_id = ChallengeRecordId::from(challenge.id.as_str());
            if context
                .exec_query(GetChallengeRecordById {
                    id: challenge_id.clone(),
                })?
                .is_none()
            {
                context.emit_set(&ChallengeRecord {
                    id: challenge_id,
                    authority_realm_id: realm_id.clone(),
                    challenge: challenge.clone(),
                })?;
            }
        }
        if let AuthorizationDecision::Permit(PermitDecision {
            lease: Some(lease), ..
        }) = &outcome.decision
        {
            context.emit_set(&LeaseRecord {
                id: LeaseRecordId::from(lease.id.as_str()),
                authority_realm_id: realm_id.clone(),
                lease: lease.clone(),
                binding: AuthorizationBinding::from_request(&self.request),
            })?;
        }
        context.emit_set(&DecisionAudit {
            id: DecisionAuditId::from(decision_id),
            authority_realm_id: realm_id,
            request: self.request,
            decision: outcome.decision.clone(),
            recorded_at: self.now,
        })?;
        Ok(outcome.decision)
    }
}

#[myko_command(
    ApprovalDecision,
    service = AuthorityService,
    scope = AuthorityRealm
)]
struct DecideChallenge {
    realm_id: AuthorityRealmKey,
    challenge_id: ChallengeId,
    approved: bool,
    approver: Principal,
    now: DateTime<Utc>,
}

impl CommandHandler for DecideChallenge {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(
        self,
        context: CommandContext<AuthorityService, AuthorityRealm>,
    ) -> Result<ApprovalDecision, CommandError> {
        let challenge = context
            .exec_query(GetChallengeRecordById {
                id: ChallengeRecordId::from(self.challenge_id.as_str()),
            })?
            .ok_or_else(|| CommandError::reject("challenge is not authoritatively present"))?
            .challenge;
        if let Some(existing) =
            context
                .exec_query(GetAllApprovalRecords)?
                .into_iter()
                .find(|record| {
                    record.decision.challenge_id == self.challenge_id
                        && record.decision.approver == self.approver
                })
        {
            if existing.decision.approved != self.approved {
                return Err(CommandError::reject(
                    "approval decision is immutable for this challenge and approver",
                ));
            }
            return Ok(existing.decision);
        }
        if challenge.expires_at <= self.now {
            return Err(CommandError::reject("challenge expired"));
        }
        let obligation = context
            .exec_query(GetObligationRecordById {
                id: ObligationRecordId::from(challenge.obligation_id.as_str()),
            })?
            .filter(|record| record.revoked_at.is_none())
            .ok_or_else(|| CommandError::reject("challenge obligation is not active"))?
            .obligation;
        if !obligation.approvers.contains(&self.approver) {
            return Err(CommandError::reject(
                "authenticated principal cannot approve challenge",
            ));
        }
        let lifetime = i64::try_from(obligation.approval_lifetime_seconds)
            .map_err(|error| CommandError::reject(format!("approval lifetime invalid: {error}")))?;
        let expires_at = self
            .now
            .checked_add_signed(Duration::seconds(lifetime))
            .ok_or_else(|| CommandError::reject("approval expiry exceeds supported time"))?;
        let decision = ApprovalDecision {
            id: ApprovalId::random(),
            realm_id: self.realm_id,
            challenge_id: challenge.id,
            obligation_id: challenge.obligation_id,
            approver: self.approver,
            binding: challenge.binding,
            approved: self.approved,
            decided_at: self.now,
            expires_at,
            max_uses: obligation.approval_use_count,
        };
        context.emit_set(&ApprovalRecord {
            id: ApprovalRecordId::from(decision.id.as_str()),
            authority_realm_id: realm_item_id(&decision.realm_id),
            decision: decision.clone(),
        })?;
        Ok(decision)
    }
}

struct EvaluationState {
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
    topology: ScopeTopology,
}

fn load_state(
    node: &Node,
    realm_id: &AuthorityRealmKey,
) -> Result<EvaluationState, myko_federation::NodeError> {
    let scope = realm_scope(realm_id);
    let source = node.node_id();
    Ok(EvaluationState {
        realm: node.query_items_in(
            source,
            &scope,
            GetAuthorityRealmById {
                id: realm_item_id(realm_id),
            },
        )?,
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
        topology: node.scope_topology()?,
    })
}

struct EvaluationOutcome {
    decision: AuthorizationDecision,
    grants: BTreeSet<AuthorityGrantId>,
    delegations: BTreeSet<DelegationId>,
    approvals: BTreeSet<ApprovalId>,
}

const fn permission_for(operation: AccessOperation) -> Option<FederationPermission> {
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

const fn is_stream(operation: AccessOperation) -> bool {
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

fn selection_covers(
    granted: &ScopeSelection,
    claimed: &ScopeSelection,
    topology: &ScopeTopology,
) -> bool {
    match (granted, claimed) {
        (ScopeSelection::Exact(grant), ScopeSelection::Exact(claim)) => grant == claim,
        (
            ScopeSelection::Subtree(grant),
            ScopeSelection::Exact(claim) | ScopeSelection::Subtree(claim),
        ) => grant == claim || topology.is_descendant_of(claim, grant),
        (ScopeSelection::Exact(_), ScopeSelection::Subtree(_)) => false,
    }
}

fn deny(
    request: &AccessRequest,
    now: DateTime<Utc>,
    code: &str,
    message: &str,
) -> EvaluationOutcome {
    EvaluationOutcome {
        decision: AuthorizationDecision::Deny(DenyDecision {
            report: AuthorizationReport {
                evaluated_at: now,
                principal: request.presentation.principal.clone(),
                executor: request.presentation.executor.clone(),
                operation: request.operation,
                explanations: vec![AuthorizationExplanation {
                    code: code.to_owned(),
                    message: message.to_owned(),
                    grant_id: None,
                    delegation_id: None,
                    obligation_id: None,
                    constraint: None,
                }],
            },
            visibility: ResourceVisibility::Unauthorized,
        }),
        grants: BTreeSet::new(),
        delegations: BTreeSet::new(),
        approvals: BTreeSet::new(),
    }
}

fn normalized_claims(request: &AccessRequest) -> Vec<ResourceClaim> {
    if !request.resource_claims.is_empty() {
        return request.resource_claims.clone();
    }
    if !request.scope_selections.is_empty() {
        return request
            .scope_selections
            .iter()
            .cloned()
            .map(|selection| ResourceClaim {
                selection,
                kind: ResourceClaimKind::Primary,
                source_node: None,
                service_id: request.service_id.clone(),
                item_type: None,
                item_id: None,
                required_permissions: Vec::new(),
                required_operations: Vec::new(),
                required_capabilities: Vec::new(),
            })
            .collect();
    }
    request
        .scope_id
        .clone()
        .map(|scope| vec![ResourceClaim::scope(scope, ResourceClaimKind::Primary)])
        .unwrap_or_default()
}

fn claim_requirements(
    request: &AccessRequest,
    claim: &ResourceClaim,
) -> (
    BTreeSet<FederationPermission>,
    BTreeSet<AccessOperation>,
    BTreeSet<CapabilityId>,
) {
    let mut permissions = claim
        .required_permissions
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if claim.kind == ResourceClaimKind::Primary {
        if let Some(permission) = permission_for(request.operation) {
            permissions.insert(permission);
        }
        if is_stream(request.operation) {
            permissions.insert(FederationPermission::Subscribe);
        }
    }
    let operations = claim
        .required_operations
        .iter()
        .copied()
        .chain((claim.kind == ResourceClaimKind::Primary).then_some(request.operation))
        .collect::<BTreeSet<_>>();
    let capabilities = claim
        .required_capabilities
        .iter()
        .cloned()
        .chain(
            (claim.kind == ResourceClaimKind::Primary)
                .then_some(request.application_capabilities.iter().cloned())
                .into_iter()
                .flatten(),
        )
        .collect::<BTreeSet<_>>();
    (permissions, operations, capabilities)
}

fn grant_independently_covers(
    grant: &AuthorityGrant,
    request: &AccessRequest,
    claims: &[ResourceClaim],
    topology: &ScopeTopology,
) -> bool {
    grant.constraints.permits(request)
        && claims.iter().all(|claim| {
            let (permissions, operations, capabilities) = claim_requirements(request, claim);
            selection_covers(&grant.selection, &claim.selection, topology)
                && permissions
                    .iter()
                    .all(|permission| grant.permissions.contains(permission))
                && operations
                    .iter()
                    .all(|operation| grant.operations.contains(operation))
                && capabilities
                    .iter()
                    .all(|capability| grant.capabilities.contains(capability))
        })
}

#[allow(clippy::too_many_lines, clippy::suspicious_operation_groupings)]
fn evaluate(
    state: &EvaluationState,
    request: &AccessRequest,
    now: DateTime<Utc>,
) -> EvaluationOutcome {
    if state.realm.is_none() {
        return deny(
            request,
            now,
            "realm_unbound",
            "authority realm is not bootstrapped",
        );
    }
    if request.principal_id != request.presentation.executor.id {
        return deny(
            request,
            now,
            "executor_mismatch",
            "authority executor does not match authenticated transport principal",
        );
    }
    let claims = normalized_claims(request);
    if claims.is_empty() {
        return deny(
            request,
            now,
            "claims_missing",
            "request declares no resource claims",
        );
    }
    let binding = AuthorizationBinding::from_request(request);
    if let Some(lease_id) = request.presentation.active_lease.as_ref() {
        let lease_record = state.leases.iter().find(|record| {
            &record.lease.id == lease_id
                && record.lease.expires_at > now
                && record.binding == binding
        });
        let Some(lease_record) = lease_record else {
            return deny(
                request,
                now,
                "lease_invalid",
                "the presented authority lease is absent, expired, or bound to another request",
            );
        };
        if request.authorization_phase == myko_federation::AuthorizationPhase::Admission
            && !lease_record.lease.offline
        {
            return deny(
                request,
                now,
                "lease_online_reconnect",
                "an online lease cannot authorize a new connection",
            );
        }
        if lease_record.lease.offline {
            return EvaluationOutcome {
                decision: AuthorizationDecision::Permit(PermitDecision {
                    report: AuthorizationReport {
                        evaluated_at: now,
                        principal: request.presentation.principal.clone(),
                        executor: request.presentation.executor.clone(),
                        operation: request.operation,
                        explanations: vec![AuthorizationExplanation {
                            code: "offline_lease".to_owned(),
                            message: "bounded cached authority is valid until lease expiry"
                                .to_owned(),
                            grant_id: None,
                            delegation_id: None,
                            obligation_id: None,
                            constraint: Some(
                                "offline leases intentionally defer live revocation until expiry"
                                    .to_owned(),
                            ),
                        }],
                    },
                    lease: Some(lease_record.lease.clone()),
                }),
                grants: BTreeSet::new(),
                delegations: BTreeSet::new(),
                approvals: BTreeSet::new(),
            };
        }
    } else if request.authorization_phase == myko_federation::AuthorizationPhase::Continuation
        && request.lease.is_some()
    {
        return deny(
            request,
            now,
            "lease_missing",
            "continuation requires the lease issued at admission",
        );
    }
    let mut required_capabilities = request
        .application_capabilities
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for claim in &claims {
        required_capabilities.extend(claim.required_capabilities.iter().cloned());
    }
    for capability_id in &required_capabilities {
        let registered = state
            .capabilities
            .iter()
            .find(|record| record.revoked_at.is_none() && &record.capability.id == capability_id);
        let Some(registered) = registered else {
            return deny(
                request,
                now,
                "capability_unregistered",
                "a required application capability is not registered",
            );
        };
        if !registered.capability.constraints.permits(request) {
            return deny(
                request,
                now,
                "capability_constraint",
                "application capability constraints reject the request",
            );
        }
    }

    let use_counts = state.grant_uses.iter().fold(
        BTreeMap::<AuthorityGrantId, u64>::new(),
        |mut counts, usage| {
            let entry = counts.entry(usage.grant_id.clone()).or_default();
            *entry = entry.saturating_add(1);
            counts
        },
    );
    let mut contributing = BTreeSet::new();
    let mut required_obligations = BTreeSet::new();
    for claim in &claims {
        let (permissions, operations, capabilities) = claim_requirements(request, claim);
        let candidates = state.grants.iter().filter(|record| {
            let grant = &record.grant;
            record.revoked_at.is_none()
                && grant.grantee == request.presentation.principal
                && grant.valid_from <= now
                && grant.expires_at.is_none_or(|expiry| expiry > now)
                && grant
                    .max_uses
                    .is_none_or(|maximum| use_counts.get(&grant.id).copied().unwrap_or(0) < maximum)
                && grant.constraints.permits(request)
                && selection_covers(&grant.selection, &claim.selection, &state.topology)
        });
        let mut missing_permissions = permissions;
        let mut missing_operations = operations;
        let mut missing_capabilities = capabilities;
        for record in candidates {
            let grant = &record.grant;
            let before = (
                missing_permissions.len(),
                missing_operations.len(),
                missing_capabilities.len(),
            );
            missing_permissions.retain(|permission| !grant.permissions.contains(permission));
            missing_operations.retain(|operation| !grant.operations.contains(operation));
            missing_capabilities.retain(|capability| !grant.capabilities.contains(capability));
            let after = (
                missing_permissions.len(),
                missing_operations.len(),
                missing_capabilities.len(),
            );
            if before != after {
                contributing.insert(grant.id.clone());
                required_obligations.extend(grant.obligations.iter().cloned());
            }
        }
        if !(missing_permissions.is_empty()
            && missing_operations.is_empty()
            && missing_capabilities.is_empty())
        {
            return deny(
                request,
                now,
                "grant_coverage",
                "active grants do not cover every required dimension of every resource claim",
            );
        }
    }

    let mut contributing_delegations = BTreeSet::new();
    let mut expected = request.presentation.principal.clone();
    let mut expected_parent: Option<DelegationId> = None;
    let delegation_counts = state.delegation_uses.iter().fold(
        BTreeMap::<DelegationId, u64>::new(),
        |mut counts, usage| {
            let entry = counts.entry(usage.delegation_id.clone()).or_default();
            *entry = entry.saturating_add(1);
            counts
        },
    );
    for hop in &request.presentation.provenance {
        if hop.delegator != expected {
            return deny(
                request,
                now,
                "provenance_chain",
                "provenance chain is discontinuous",
            );
        }
        let Some(record) = state.delegations.iter().find(|record| {
            record.revoked_at.is_none() && record.delegation.id == hop.delegation_id
        }) else {
            return deny(
                request,
                now,
                "delegation_missing",
                "delegation is not authoritative",
            );
        };
        let delegation = &record.delegation;
        let parent_is_live = match (&expected_parent, &delegation.parent) {
            (None, myko_federation::DelegationParent::Grant(grant_id)) => state
                .grants
                .iter()
                .find(|record| &record.grant.id == grant_id)
                .is_some_and(|record| {
                    let grant = &record.grant;
                    record.revoked_at.is_none()
                        && grant.grantee == request.presentation.principal
                        && grant.valid_from <= now
                        && grant.expires_at.is_none_or(|expiry| expiry > now)
                        && grant.max_uses.is_none_or(|maximum| {
                            use_counts.get(grant_id).copied().unwrap_or(0) < maximum
                        })
                        && grant_independently_covers(grant, request, &claims, &state.topology)
                }),
            (Some(expected), myko_federation::DelegationParent::Delegation(parent_id)) => {
                parent_id == expected
            }
            _ => false,
        };
        if delegation.delegator != hop.delegator
            || delegation.delegate != hop.delegate
            || delegation.provenance_operation != hop.operation
            || delegation.expires_at.is_some_and(|expiry| expiry <= now)
            || delegation.max_uses.is_some_and(|maximum| {
                delegation_counts.get(&delegation.id).copied().unwrap_or(0) >= maximum
            })
            || !delegation.constraints.permits(request)
            || !parent_is_live
            || claims.iter().any(|claim| {
                !delegation
                    .selections
                    .iter()
                    .any(|selection| selection_covers(selection, &claim.selection, &state.topology))
                    || {
                        let (permissions, operations, capabilities) =
                            claim_requirements(request, claim);
                        permissions
                            .iter()
                            .any(|permission| !delegation.permissions.contains(permission))
                            || operations
                                .iter()
                                .any(|operation| !delegation.operations.contains(operation))
                            || capabilities
                                .iter()
                                .any(|capability| !delegation.capabilities.contains(capability))
                    }
            })
        {
            return deny(
                request,
                now,
                "delegation_attenuation",
                "delegated authority does not cover the request",
            );
        }
        if let myko_federation::DelegationParent::Grant(grant_id) = &delegation.parent {
            contributing.clear();
            required_obligations.clear();
            contributing.insert(grant_id.clone());
            if let Some(parent) = state
                .grants
                .iter()
                .find(|record| &record.grant.id == grant_id)
            {
                required_obligations.extend(parent.grant.obligations.iter().cloned());
            }
        }
        contributing_delegations.insert(delegation.id.clone());
        expected_parent = Some(delegation.id.clone());
        expected = hop.delegate.clone();
    }
    if expected != request.presentation.executor {
        return deny(
            request,
            now,
            "provenance_executor",
            "provenance does not terminate at the authenticated executor",
        );
    }

    let approval_counts =
        state
            .approval_uses
            .iter()
            .fold(BTreeMap::<ApprovalId, u64>::new(), |mut counts, usage| {
                let entry = counts.entry(usage.approval_id.clone()).or_default();
                *entry = entry.saturating_add(1);
                counts
            });
    let mut used_approvals = BTreeSet::new();
    if request.operation == AccessOperation::SubmitCommand
        && request.authorization_phase != myko_federation::AuthorizationPhase::Effect
    {
        required_obligations.clear();
    }
    for obligation_id in required_obligations {
        let Some(obligation) = state
            .obligations
            .iter()
            .find(|record| record.revoked_at.is_none() && record.obligation.id == obligation_id)
        else {
            return deny(
                request,
                now,
                "obligation_missing",
                "required obligation is unavailable",
            );
        };
        let approval = state.approvals.iter().find(|record| {
            let decision = &record.decision;
            request.presentation.approvals.contains(&decision.id)
                && decision.obligation_id == obligation_id
                && decision.approved
                && decision.binding == binding
                && decision.expires_at > now
                && approval_counts.get(&decision.id).copied().unwrap_or(0) < decision.max_uses
        });
        if let Some(approval) = approval {
            used_approvals.insert(approval.decision.id.clone());
            continue;
        }
        if let Some(existing) = state.challenges.iter().find(|record| {
            record.challenge.obligation_id == obligation_id
                && record.challenge.binding == binding
                && record.challenge.expires_at > now
        }) {
            return EvaluationOutcome {
                decision: AuthorizationDecision::Challenge {
                    challenge: existing.challenge.clone(),
                    report: AuthorizationReport {
                        evaluated_at: now,
                        principal: request.presentation.principal.clone(),
                        executor: request.presentation.executor.clone(),
                        operation: request.operation,
                        explanations: vec![AuthorizationExplanation {
                            code: "obligation_challenge".to_owned(),
                            message: "approval is required".to_owned(),
                            grant_id: None,
                            delegation_id: None,
                            obligation_id: Some(obligation.obligation.id.clone()),
                            constraint: None,
                        }],
                    },
                },
                grants: BTreeSet::new(),
                delegations: BTreeSet::new(),
                approvals: BTreeSet::new(),
            };
        }
        let lifetime =
            i64::try_from(obligation.obligation.approval_lifetime_seconds).unwrap_or(i64::MAX);
        let expires_at = now
            .checked_add_signed(Duration::seconds(lifetime))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let challenge = AuthorityChallenge {
            id: ChallengeId::random(),
            realm_id: obligation.obligation.realm_id.clone(),
            obligation_id,
            kind: obligation.obligation.challenge_kind.clone(),
            prompt: obligation.obligation.prompt.clone(),
            binding,
            issued_at: now,
            expires_at,
        };
        return EvaluationOutcome {
            decision: AuthorizationDecision::Challenge {
                challenge,
                report: AuthorizationReport {
                    evaluated_at: now,
                    principal: request.presentation.principal.clone(),
                    executor: request.presentation.executor.clone(),
                    operation: request.operation,
                    explanations: vec![AuthorizationExplanation {
                        code: "obligation_challenge".to_owned(),
                        message: "approval is required".to_owned(),
                        grant_id: None,
                        delegation_id: None,
                        obligation_id: Some(obligation.obligation.id.clone()),
                        constraint: None,
                    }],
                },
            },
            grants: BTreeSet::new(),
            delegations: BTreeSet::new(),
            approvals: BTreeSet::new(),
        };
    }

    let lease = request
        .lease
        .and_then(|requested| make_lease(requested, now));
    EvaluationOutcome {
        decision: AuthorizationDecision::Permit(PermitDecision {
            report: AuthorizationReport {
                evaluated_at: now,
                principal: request.presentation.principal.clone(),
                executor: request.presentation.executor.clone(),
                operation: request.operation,
                explanations: contributing
                    .iter()
                    .map(|id| AuthorizationExplanation {
                        code: "grant_contributed".to_owned(),
                        message: "grant contributed required authority".to_owned(),
                        grant_id: Some(id.clone()),
                        delegation_id: None,
                        obligation_id: None,
                        constraint: None,
                    })
                    .collect(),
            },
            lease,
        }),
        grants: contributing,
        delegations: contributing_delegations,
        approvals: used_approvals,
    }
}

fn make_lease(request: AuthorityLeaseRequest, now: DateTime<Utc>) -> Option<AuthorityLease> {
    let seconds = i64::try_from(request.duration_seconds).ok()?;
    let expires_at = now.checked_add_signed(Duration::seconds(seconds))?;
    Some(AuthorityLease {
        id: LeaseId::random(),
        issued_at: now,
        expires_at,
        offline: request.offline,
    })
}

/// Policy backed exclusively by the local projection of [`AuthorityService`].
/// Replicated copies of authority entities are never consulted.
#[derive(Clone)]
pub struct AuthorityPolicy {
    application: ApplicationNode,
    realm_id: AuthorityRealmKey,
    revision: Arc<AtomicU64>,
}

impl fmt::Debug for AuthorityPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorityPolicy")
            .field("realm_id", &self.realm_id)
            .field("revision", &self.revision.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl AuthorityPolicy {
    #[must_use]
    pub fn new(application: ApplicationNode, realm_id: AuthorityRealmKey) -> Self {
        Self {
            application,
            realm_id,
            revision: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Adds the authority service to a composed application.
    ///
    /// # Errors
    /// Returns a registration conflict from the application builder.
    pub fn install(application: MykoApplication) -> Result<MykoApplication, AppError> {
        application.with_framework_service::<AuthorityService>()
    }

    /// The only unauthorised mutation: create one previously absent realm and
    /// its bounded administrator grant. The command rejects every replay.
    ///
    /// # Errors
    ///
    /// Returns an error when the realm exists or durable bootstrap fails.
    pub fn bootstrap(&self, principal: Principal) -> Result<AuthorityRealm, AppError> {
        let presentation = authority_presentation(&self.application);
        self.application.exec_authorized_command(
            presentation.executor.id.clone(),
            presentation,
            BootstrapRealm {
                realm_id: self.realm_id.clone(),
                principal,
                at: Utc::now(),
            },
        )
    }

    fn validate_authenticated_presentation(
        authenticated: &Principal,
        presentation: &AuthorityPresentation,
    ) -> Result<(), AppError> {
        if &presentation.executor != authenticated {
            return Err(AppError::State(
                "authority executor does not match the authenticated principal".to_owned(),
            ));
        }
        Ok(())
    }

    /// Issues an immutable grant through the authenticated administrator path.
    /// The grantor is always the original authenticated authority principal.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, administration authority, or the
    /// immutable durable write fails.
    pub fn issue_grant(
        &self,
        authenticated: Principal,
        presentation: AuthorityPresentation,
        grant: AuthorityGrant,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        if grant.grantor != presentation.principal {
            return Err(AppError::State(
                "grantor does not match the authenticated authority principal".to_owned(),
            ));
        }
        self.application.exec_authorized_command(
            authenticated.id,
            presentation,
            PutGrant {
                realm_id: self.realm_id.clone(),
                grant,
            },
        )
    }

    /// Creates a store-bound delegation only after the delegator proves
    /// `Reshare` authority over every attenuated selection.
    ///
    /// # Errors
    ///
    /// Returns an error when issuer binding, attenuation, or durable creation
    /// fails.
    #[allow(clippy::suspicious_operation_groupings)] // Realm and issuer are independent bindings.
    pub fn delegate(
        &self,
        authenticated: Principal,
        presentation: AuthorityPresentation,
        delegation: AuthorityDelegation,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        if delegation.realm_id != self.realm_id || delegation.delegator != presentation.principal {
            return Err(AppError::State(
                "delegation issuer or authority realm does not match authentication".to_owned(),
            ));
        }
        let mut request = AccessRequest::scoped(
            authenticated.id,
            presentation.clone(),
            AccessOperation::DelegateAuthority,
            delegation.selections.first().map_or_else(
                || realm_scope(&self.realm_id),
                |selection| selection.root().clone(),
            ),
        );
        request.scope_selections.clone_from(&delegation.selections);
        request.resource_claims = delegation
            .selections
            .iter()
            .cloned()
            .map(|selection| {
                let mut claim = ResourceClaim {
                    selection,
                    kind: ResourceClaimKind::Primary,
                    source_node: None,
                    service_id: None,
                    item_type: None,
                    item_id: None,
                    required_permissions: vec![FederationPermission::Reshare],
                    required_operations: vec![AccessOperation::DelegateAuthority],
                    required_capabilities: Vec::new(),
                };
                claim
                    .required_capabilities
                    .clone_from(&delegation.capabilities);
                claim
            })
            .collect();
        request.topology = self.application.node().scope_topology().ok();
        let decision = self.evaluate(request);
        if !decision.is_permit() {
            return Err(AppError::State(decision.public_message()));
        }
        let issuer = presentation.principal;
        let internal = authority_presentation(&self.application);
        self.application.exec_authorized_command(
            internal.executor.id.clone(),
            internal,
            PutDelegation {
                realm_id: self.realm_id.clone(),
                delegation,
                issuer,
            },
        )
    }

    /// Installs one immutable obligation through authenticated realm admin authority.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, administration authority, or the
    /// immutable durable write fails.
    pub fn issue_obligation(
        &self,
        authenticated: Principal,
        presentation: AuthorityPresentation,
        obligation: Obligation,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        self.application.exec_authorized_command(
            authenticated.id,
            presentation,
            PutObligation {
                realm_id: self.realm_id.clone(),
                obligation,
            },
        )
    }

    /// Revokes one durable authority fact through authenticated realm admin authority.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, administration authority, or the
    /// durable revocation fails.
    pub fn revoke(
        &self,
        authenticated: Principal,
        presentation: AuthorityPresentation,
        kind: RevocationKind,
        id: String,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        self.application.exec_authorized_command(
            authenticated.id,
            presentation,
            RevokeAuthorityFact {
                realm_id: self.realm_id.clone(),
                kind,
                id,
                at: Utc::now(),
            },
        )
    }

    fn register_capability(
        &self,
        authenticated_executor: PrincipalId,
        presentation: AuthorityPresentation,
        capability: ApplicationCapability,
    ) -> Result<(), AppError> {
        self.application.exec_authorized_command(
            authenticated_executor,
            presentation,
            PutCapability {
                realm_id: self.realm_id.clone(),
                capability,
            },
        )
    }

    /// Registers every capability declared by a composed application through
    /// the authenticated administrator path. Exact re-registration after a
    /// restart is idempotent; a conflicting definition is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, registration conflict checks, or
    /// a durable capability write fails.
    #[allow(clippy::needless_pass_by_value)] // Registration snapshots both authentication inputs.
    pub fn register_application_capabilities(
        &self,
        authenticated: Principal,
        presentation: AuthorityPresentation,
        application: &ApplicationNode,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        for capability in application.authority_capabilities().cloned() {
            let existing = self.application.node().query_items_in(
                self.application.node_id(),
                &realm_scope(&self.realm_id),
                GetCapabilityRegistrationById {
                    id: CapabilityRegistrationId::from(capability.id.as_str()),
                },
            )?;
            match existing {
                Some(existing) if existing.capability == capability => {}
                Some(_) => {
                    return Err(AppError::State(format!(
                        "capability {} is already registered with a different definition",
                        capability.id
                    )));
                }
                None => self.register_capability(
                    authenticated.id.clone(),
                    presentation.clone(),
                    capability,
                )?,
            }
        }
        Ok(())
    }

    fn evaluate(&self, request: AccessRequest) -> AuthorizationDecision {
        if request.authorization_phase == myko_federation::AuthorizationPhase::Continuation {
            return load_state(self.application.node(), &self.realm_id).map_or_else(
                |error| {
                    deny(
                        &request,
                        Utc::now(),
                        "continuation_projection_failed",
                        &format!("authoritative continuation projection failed: {error}"),
                    )
                    .decision
                },
                |state| evaluate(&state, &request, Utc::now()).decision,
            );
        }
        let request_for_error = request.clone();
        let presentation = authority_presentation(&self.application);
        self.application
            .exec_authorized_command(
                presentation.executor.id.clone(),
                presentation,
                EvaluateAuthority {
                    realm_id: self.realm_id.clone(),
                    request,
                    now: Utc::now(),
                },
            )
            .unwrap_or_else(|error| {
                deny(
                    &request_for_error,
                    Utc::now(),
                    "durable_evaluation_failed",
                    &format!("durable authority evaluation failed: {error}"),
                )
                .decision
            })
    }
}

impl AccessPolicy for AuthorityPolicy {
    fn authorize(&self, request: &AccessRequest) -> Result<(), String> {
        match self.decide(request) {
            AuthorizationDecision::Permit(_) => Ok(()),
            decision => Err(decision.public_message()),
        }
    }

    fn decide(&self, request: &AccessRequest) -> AuthorizationDecision {
        if is_internal_authority_request(&self.application, request) {
            return AllowAllAccessPolicy.decide(request);
        }
        self.evaluate(request.clone())
    }

    fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn subscribe_changes(&self) -> Option<flume::Receiver<u64>> {
        let history = self.application.node().events_after(None).ok()?;
        let cursor = history.last().map(|event| event.position);
        let mut events = self.application.node().subscribe(cursor).ok()?;
        let local_node = self.application.node_id();
        let revision = Arc::clone(&self.revision);
        let (tx, rx) = flume::unbounded();
        std::thread::Builder::new()
            .name("myko-authority-revision".to_owned())
            .spawn(move || {
                while let Ok(event) = events.recv() {
                    let NodeEvent::CommandCommitted { command, batch } = &event.event else {
                        continue;
                    };
                    if event.origin.node_id != local_node
                        || command.request.service_id.as_str()
                            != AuthorityService::SERVICE_ID.as_str()
                    {
                        continue;
                    }
                    let changes_authority = batch.changes.iter().any(|mutation| {
                        matches!(
                            mutation.item_type.as_str(),
                            "AuthorityRealm"
                                | "CapabilityRegistration"
                                | "GrantRecord"
                                | "DelegationRecord"
                                | "ObligationRecord"
                                | "ApprovalRecord"
                        )
                    });
                    if !changes_authority {
                        continue;
                    }
                    let next = revision.fetch_add(1, Ordering::AcqRel).saturating_add(1);
                    if tx.send(next).is_err() {
                        break;
                    }
                }
            })
            .ok()?;
        Some(rx)
    }

    #[allow(clippy::too_many_lines)] // Intersection and one-shot consumption form one audit unit.
    fn constrain_replication(
        &self,
        request: &AccessRequest,
        requested: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationDecision> {
        if request.lease.is_some() || request.presentation.active_lease.is_some() {
            return Err(deny(
                request,
                Utc::now(),
                "replication_lease_unsupported",
                "selected replication does not issue or accept offline leases",
            )
            .decision);
        }
        let requested_filter = match requested {
            ReplicationSelection::Intersection { requested, .. } => requested.as_ref(),
            requested => requested,
        };
        let mut selections = match requested_filter {
            ReplicationSelection::Scopes(selections) if selections.is_empty() => {
                return Err(deny(
                    request,
                    Utc::now(),
                    "replication_empty",
                    "requested replication selection is empty",
                )
                .decision);
            }
            ReplicationSelection::Scopes(selections) => selections
                .iter()
                .flat_map(|selection| match selection {
                    ScopeSelection::Exact(scope) => {
                        vec![ScopeSelection::Exact(scope.clone())]
                    }
                    ScopeSelection::Subtree(root) => std::iter::once(root.clone())
                        .chain(topology.descendants(root))
                        .map(ScopeSelection::Exact)
                        .collect(),
                })
                .collect(),
            ReplicationSelection::ServiceScope { scope_id, .. } => {
                vec![ScopeSelection::Exact(scope_id.clone())]
            }
            ReplicationSelection::Service(service) => self
                .application
                .node()
                .events_after(None)
                .map_err(|error| {
                    deny(
                        request,
                        Utc::now(),
                        "topology_unavailable",
                        &error.to_string(),
                    )
                    .decision
                })?
                .into_iter()
                .filter(|event| event.origin.node_id == self.application.node_id())
                .filter(|event| event.event.service_id() == service)
                .flat_map(|event| event.event.affected_scope_ids())
                .map(ScopeSelection::Exact)
                .collect(),
            ReplicationSelection::All => self
                .application
                .node()
                .events_after(None)
                .map_err(|error| {
                    deny(
                        request,
                        Utc::now(),
                        "topology_unavailable",
                        &error.to_string(),
                    )
                    .decision
                })?
                .into_iter()
                .filter(|event| event.origin.node_id == self.application.node_id())
                .flat_map(|event| event.event.affected_scope_ids())
                .map(ScopeSelection::Exact)
                .collect(),
            ReplicationSelection::Intersection { .. } => Vec::new(),
        };
        if let ReplicationSelection::Intersection { scopes, .. } = requested {
            selections.retain(|candidate| {
                scopes
                    .iter()
                    .any(|allowed| allowed.covers_in(candidate, topology))
            });
        }
        selections.sort_unstable_by(|left, right| left.root().as_str().cmp(right.root().as_str()));
        selections.dedup();
        let state = load_state(self.application.node(), &self.realm_id).map_err(|error| {
            deny(
                request,
                Utc::now(),
                "replication_projection_failed",
                &error.to_string(),
            )
            .decision
        })?;
        let now = Utc::now();
        let authorized = selections
            .iter()
            .filter(|&selection| {
                let mut candidate = request.clone();
                candidate.scope_id = Some(selection.root().clone());
                candidate.scope_selections = vec![selection.clone()];
                candidate.resource_claims = vec![ResourceClaim {
                    selection: selection.clone(),
                    kind: ResourceClaimKind::Primary,
                    source_node: request
                        .resource_claims
                        .first()
                        .and_then(|claim| claim.source_node),
                    service_id: request.service_id.clone(),
                    item_type: request
                        .resource_claims
                        .first()
                        .and_then(|claim| claim.item_type.clone()),
                    item_id: None,
                    required_permissions: request
                        .resource_claims
                        .first()
                        .map_or_else(Vec::new, |claim| claim.required_permissions.clone()),
                    required_operations: request
                        .resource_claims
                        .first()
                        .map_or_else(Vec::new, |claim| claim.required_operations.clone()),
                    required_capabilities: request
                        .resource_claims
                        .first()
                        .map_or_else(Vec::new, |claim| claim.required_capabilities.clone()),
                }];
                evaluate(&state, &candidate, now).decision.is_permit()
            })
            .cloned()
            .collect::<Vec<_>>();
        if authorized.is_empty() {
            let decision = self.evaluate(request.clone());
            return Err(decision);
        }
        let mut scoped = request.clone();
        scoped.scope_id = authorized.first().map(|selection| selection.root().clone());
        scoped.scope_selections.clone_from(&authorized);
        scoped.resource_claims = authorized
            .iter()
            .cloned()
            .map(|selection| ResourceClaim {
                selection,
                kind: ResourceClaimKind::Primary,
                source_node: request
                    .resource_claims
                    .first()
                    .and_then(|claim| claim.source_node),
                service_id: request.service_id.clone(),
                item_type: request
                    .resource_claims
                    .first()
                    .and_then(|claim| claim.item_type.clone()),
                item_id: None,
                required_permissions: request
                    .resource_claims
                    .first()
                    .map_or_else(Vec::new, |claim| claim.required_permissions.clone()),
                required_operations: request
                    .resource_claims
                    .first()
                    .map_or_else(Vec::new, |claim| claim.required_operations.clone()),
                required_capabilities: request
                    .resource_claims
                    .first()
                    .map_or_else(Vec::new, |claim| claim.required_capabilities.clone()),
            })
            .collect();
        let decision = self.evaluate(scoped);
        if decision.is_permit() {
            Ok(ReplicationSelection::Intersection {
                requested: Box::new(requested_filter.clone()),
                scopes: authorized,
            })
        } else {
            Err(decision)
        }
    }

    #[allow(clippy::too_many_lines)] // Approval binding and idempotent persistence are one operation.
    fn approve(
        &self,
        authenticated_executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        challenge_id: &ChallengeId,
        approved: bool,
    ) -> Result<ApprovalDecision, AuthorizationDecision> {
        if authenticated_executor != &presentation.executor.id
            || presentation.principal != presentation.executor
            || !presentation.provenance.is_empty()
        {
            return Err(deny(
                &AccessRequest::scoped(
                    authenticated_executor.clone(),
                    presentation.clone(),
                    AccessOperation::ApproveAuthority,
                    realm_scope(&self.realm_id),
                ),
                Utc::now(),
                "approval_executor_mismatch",
                "approval requires a directly authenticated approver",
            )
            .decision);
        }
        let internal = authority_presentation(&self.application);
        let decision = self
            .application
            .exec_authorized_command(
                internal.executor.id.clone(),
                internal,
                DecideChallenge {
                    realm_id: self.realm_id.clone(),
                    challenge_id: challenge_id.clone(),
                    approved,
                    approver: presentation.principal.clone(),
                    now: Utc::now(),
                },
            )
            .map_err(|error| {
                deny(
                    &AccessRequest::scoped(
                        authenticated_executor.clone(),
                        presentation.clone(),
                        AccessOperation::ApproveAuthority,
                        realm_scope(&self.realm_id),
                    ),
                    Utc::now(),
                    "approval_failed",
                    &error.to_string(),
                )
                .decision
            })?;
        if approved && let Some(command_id) = decision.binding.command_id {
            let binding = &decision.binding;
            let pending = self
                .application
                .node()
                .command(command_id)
                .map_err(|error| {
                    deny(
                        &AccessRequest::scoped(
                            authenticated_executor.clone(),
                            presentation.clone(),
                            AccessOperation::ApproveAuthority,
                            realm_scope(&self.realm_id),
                        ),
                        Utc::now(),
                        "approval_pending_command_failed",
                        &error.to_string(),
                    )
                    .decision
                })?
                .ok_or_else(|| {
                    deny(
                        &AccessRequest::scoped(
                            authenticated_executor.clone(),
                            presentation.clone(),
                            AccessOperation::ApproveAuthority,
                            realm_scope(&self.realm_id),
                        ),
                        Utc::now(),
                        "approval_pending_command_missing",
                        "the challenged command is not present",
                    )
                    .decision
                })?;
            let CommandState::AuthorizationPending {
                challenge_id: pending_challenge,
                approvals,
                ..
            } = &pending.state
            else {
                return Ok(decision);
            };
            if pending_challenge != &decision.challenge_id {
                return Ok(decision);
            }
            let mut command_presentation = pending.request.authority;
            for approval_id in approvals {
                if !command_presentation.approvals.contains(approval_id) {
                    command_presentation.approvals.push(approval_id.clone());
                }
            }
            if !command_presentation.approvals.contains(&decision.id) {
                command_presentation.approvals.push(decision.id.clone());
            }
            let effect_request = AccessRequest {
                principal_id: binding.executor.id.clone(),
                presentation: command_presentation,
                operation: binding.operation,
                service_id: binding.service_id.clone(),
                scope_id: binding
                    .resources
                    .first()
                    .map(|claim| claim.selection.root().clone()),
                command_id: binding.command_id,
                command_type: binding.command_type.clone(),
                command_principal_id: Some(binding.principal.id.clone()),
                scope_selections: binding
                    .resources
                    .iter()
                    .map(|claim| claim.selection.clone())
                    .collect(),
                resource_claims: binding.resources.clone(),
                application_capabilities: binding.capabilities.clone(),
                arguments_digest: binding.arguments_digest.clone(),
                effect_digest: binding.effect_digest.clone(),
                lease: None,
                authorization_phase: myko_federation::AuthorizationPhase::Effect,
                topology: self.application.node().scope_topology().ok(),
                live_topics: Vec::new(),
            };
            let next = self.evaluate(effect_request);
            let transition = match next {
                AuthorizationDecision::Permit(_) => self.application.node().resume_authorization(
                    command_id,
                    &decision.challenge_id,
                    decision.id.clone(),
                ),
                AuthorizationDecision::Challenge { challenge, .. } => {
                    self.application.node().advance_authorization(
                        command_id,
                        &decision.challenge_id,
                        challenge.id,
                        decision.id.clone(),
                    )
                }
                denied @ AuthorizationDecision::Deny(_) => return Err(denied),
            };
            transition.map_err(|error| {
                deny(
                    &AccessRequest::scoped(
                        authenticated_executor.clone(),
                        presentation.clone(),
                        AccessOperation::ApproveAuthority,
                        realm_scope(&self.realm_id),
                    ),
                    Utc::now(),
                    "approval_resume_failed",
                    &error.to_string(),
                )
                .decision
            })?;
        }
        Ok(decision)
    }

    fn register_application_capability(
        &self,
        authenticated_executor: &PrincipalId,
        presentation: &AuthorityPresentation,
        capability: ApplicationCapability,
    ) -> Result<(), String> {
        if authenticated_executor != &presentation.executor.id
            || presentation.principal != presentation.executor
            || !presentation.provenance.is_empty()
        {
            return Err(
                "capability registration requires a directly authenticated administrator"
                    .to_owned(),
            );
        }
        self.register_capability(
            authenticated_executor.clone(),
            presentation.clone(),
            capability,
        )
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::redundant_clone,
    clippy::too_many_lines,
    clippy::unwrap_used
)]
mod tests {
    use std::sync::Arc;

    use myko_federation::{
        AccessPolicy as _, AuthorityDelegation, DelegationParent, ObligationId, PrincipalKind,
        ProvenanceHop, ProvenanceOperation, ServiceId,
    };

    use super::*;

    fn node_principal(value: &str) -> Principal {
        Principal::new(PrincipalId::new(value), PrincipalKind::Node)
    }

    fn open(node: Node) -> Result<(ApplicationNode, Arc<AuthorityPolicy>, Principal), String> {
        let application =
            AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
        let application = ApplicationNode::new(node.clone(), application);
        let policy = Arc::new(AuthorityPolicy::new(
            application.clone(),
            AuthorityRealmKey::new("main"),
        ));
        let administrator = node_principal("node:administrator");
        let installed: Arc<dyn AccessPolicy> = policy.clone();
        node.set_command_access_policy(installed)
            .map_err(|error| error.to_string())?;
        policy
            .bootstrap(administrator.clone())
            .map_err(|error| error.to_string())?;
        Ok((application, policy, administrator))
    }

    fn request(principal: Principal, scope: &str, operation: AccessOperation) -> AccessRequest {
        let mut request = AccessRequest::scoped(
            principal.id.clone(),
            AuthorityPresentation::direct(principal),
            operation,
            ScopeId::new(scope),
        );
        request.service_id = Some(ServiceId::new("test.service"));
        request
    }

    fn grant(
        id: &str,
        principal: Principal,
        scope: &str,
        permissions: Vec<FederationPermission>,
        operations: Vec<AccessOperation>,
    ) -> AuthorityGrant {
        AuthorityGrant {
            id: AuthorityGrantId::new(id),
            realm_id: AuthorityRealmKey::new("main"),
            grantor: node_principal("node:administrator"),
            grantee: principal,
            selection: ScopeSelection::Exact(ScopeId::new(scope)),
            permissions,
            operations,
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: Utc::now() - Duration::seconds(1),
            expires_at: None,
            max_uses: None,
        }
    }

    fn put_grant(
        policy: &AuthorityPolicy,
        administrator: &Principal,
        grant: AuthorityGrant,
    ) -> Result<(), String> {
        policy
            .issue_grant(
                administrator.clone(),
                AuthorityPresentation::direct(administrator.clone()),
                grant,
            )
            .map_err(|error| error.to_string())
    }

    #[test]
    fn default_deny_and_bootstrap_is_one_time() -> Result<(), String> {
        let node = Node::in_memory();
        let application =
            AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
        let application = ApplicationNode::new(node.clone(), application);
        let policy = Arc::new(AuthorityPolicy::new(
            application,
            AuthorityRealmKey::new("main"),
        ));
        let user = node_principal("node:user");
        assert!(matches!(
            policy.decide(&request(user, "scope:a", AccessOperation::ReadItems)),
            AuthorizationDecision::Deny(_)
        ));
        let installed: Arc<dyn AccessPolicy> = policy.clone();
        node.set_command_access_policy(installed)
            .map_err(|error| error.to_string())?;
        let administrator = node_principal("node:administrator");
        policy
            .bootstrap(administrator.clone())
            .map_err(|error| error.to_string())?;
        assert!(policy.bootstrap(administrator).is_err());
        Ok(())
    }

    #[test]
    fn grants_compose_dimensions_but_claims_are_all_or_nothing() -> Result<(), String> {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let capability = ApplicationCapability {
            id: myko_federation::CapabilityId::new("test.export"),
            description: "export a document".to_owned(),
            constraints: AuthorityConstraints {
                services: vec![ServiceId::new("test.service")],
                ..AuthorityConstraints::default()
            },
        };
        let declared_application = MykoApplication::builder()
            .capability(capability.clone())
            .map_err(|error| error.to_string())?
            .build();
        let declared_application =
            ApplicationNode::new(application.node().clone(), declared_application);
        policy
            .register_application_capabilities(
                administrator.clone(),
                AuthorityPresentation::direct(administrator.clone()),
                &declared_application,
            )
            .map_err(|error| error.to_string())?;
        policy
            .register_application_capabilities(
                administrator.clone(),
                AuthorityPresentation::direct(administrator.clone()),
                &declared_application,
            )
            .map_err(|error| error.to_string())?;
        put_grant(
            &policy,
            &administrator,
            grant(
                "read",
                user.clone(),
                "scope:a",
                vec![FederationPermission::ReadState],
                Vec::new(),
            ),
        )?;
        let mut capability_grant = grant(
            "capability",
            user.clone(),
            "scope:a",
            Vec::new(),
            vec![AccessOperation::ReadItems],
        );
        capability_grant.capabilities.push(capability.id.clone());
        put_grant(&policy, &administrator, capability_grant)?;

        let mut authorized = request(user.clone(), "scope:a", AccessOperation::ReadItems);
        authorized
            .application_capabilities
            .push(capability.id.clone());
        let permit = match policy.decide(&authorized) {
            AuthorizationDecision::Permit(permit) => permit,
            decision => return Err(format!("expected composed permit, found {decision:?}")),
        };
        let contributing = permit
            .report
            .explanations
            .iter()
            .filter_map(|explanation| explanation.grant_id.as_ref())
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            contributing,
            BTreeSet::from(["capability".to_owned(), "read".to_owned()])
        );

        authorized.resource_claims.push(ResourceClaim::scope(
            ScopeId::new("scope:b"),
            ResourceClaimKind::Referenced,
        ));
        if let Some(claim) = authorized.resource_claims.last_mut() {
            claim
                .required_permissions
                .push(FederationPermission::ReadState);
            claim.required_operations.push(AccessOperation::ReadItems);
        }
        assert!(matches!(
            policy.decide(&authorized),
            AuthorizationDecision::Deny(_)
        ));
        Ok(())
    }

    #[test]
    fn immutable_fact_ids_cannot_reset_revocation() -> Result<(), String> {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let value = grant(
            "immutable",
            user,
            "scope:a",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        put_grant(&policy, &administrator, value.clone())?;
        application
            .exec_authenticated_command(
                administrator.id.clone(),
                RevokeAuthorityFact {
                    realm_id: AuthorityRealmKey::new("main"),
                    kind: RevocationKind::Grant,
                    id: value.id.to_string(),
                    at: Utc::now(),
                },
            )
            .map_err(|error| error.to_string())?;
        assert!(put_grant(&policy, &administrator, value).is_err());
        Ok(())
    }

    #[test]
    fn approval_is_bound_and_single_use() -> Result<(), String> {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let approver = node_principal("node:approver");
        let obligation = Obligation {
            id: myko_federation::ObligationId::new("human-review"),
            realm_id: AuthorityRealmKey::new("main"),
            challenge_kind: "confirm".to_owned(),
            prompt: "Approve exact read?".to_owned(),
            approvers: vec![approver.clone()],
            approval_lifetime_seconds: 60,
            approval_use_count: 1,
        };
        application
            .exec_authenticated_command(
                administrator.id.clone(),
                PutObligation {
                    realm_id: AuthorityRealmKey::new("main"),
                    obligation: obligation.clone(),
                },
            )
            .map_err(|error| error.to_string())?;
        let mut guarded = grant(
            "guarded",
            user.clone(),
            "scope:a",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        guarded.obligations.push(obligation.id);
        put_grant(&policy, &administrator, guarded)?;
        let mut access = request(user, "scope:a", AccessOperation::ReadItems);
        access.arguments_digest = Some("sha256:arguments".to_owned());
        let AuthorizationDecision::Challenge { challenge, .. } = policy.decide(&access) else {
            return Err("expected challenge".to_owned());
        };
        let approval = policy
            .approve(
                &approver.id,
                &AuthorityPresentation::direct(approver.clone()),
                &challenge.id,
                true,
            )
            .map_err(|decision| decision.public_message())?;
        access.presentation.approvals.push(approval.id.clone());
        assert!(policy.decide(&access).is_permit());
        assert!(matches!(
            policy.decide(&access),
            AuthorizationDecision::Challenge { .. }
        ));
        let mut rebound = access;
        rebound.arguments_digest = Some("sha256:different".to_owned());
        assert!(matches!(
            policy.decide(&rebound),
            AuthorizationDecision::Challenge { .. }
        ));
        Ok(())
    }

    #[test]
    fn approval_cannot_rebind_a_command_result_effect() -> Result<(), String> {
        let (_application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:effect-user");
        let obligation = Obligation {
            id: ObligationId::new("effect-review"),
            realm_id: AuthorityRealmKey::new("main"),
            challenge_kind: "confirm_effect".to_owned(),
            prompt: "approve the exact batch and result".to_owned(),
            approvers: vec![administrator.clone()],
            approval_lifetime_seconds: 60,
            approval_use_count: 1,
        };
        policy
            .issue_obligation(
                administrator.clone(),
                AuthorityPresentation::direct(administrator.clone()),
                obligation.clone(),
            )
            .map_err(|error| error.to_string())?;
        let mut guarded = grant(
            "effect-bound",
            user.clone(),
            "scope:effect",
            vec![FederationPermission::Write],
            vec![AccessOperation::SubmitCommand],
        );
        guarded.obligations.push(obligation.id);
        put_grant(&policy, &administrator, guarded)?;

        let mut exact = request(user, "scope:effect", AccessOperation::SubmitCommand);
        exact.authorization_phase = myko_federation::AuthorizationPhase::Effect;
        exact.effect_digest = Some("sha256:batch-and-result-a".to_owned());
        let AuthorizationDecision::Challenge { challenge, .. } = policy.decide(&exact) else {
            return Err("expected effect challenge".to_owned());
        };
        let approval = policy
            .approve(
                &administrator.id,
                &AuthorityPresentation::direct(administrator.clone()),
                &challenge.id,
                true,
            )
            .map_err(|decision| decision.public_message())?;
        exact.presentation.approvals.push(approval.id);
        let mut rebound_result = exact.clone();
        rebound_result.effect_digest = Some("sha256:batch-and-result-b".to_owned());
        assert!(matches!(
            policy.decide(&rebound_result),
            AuthorizationDecision::Challenge { .. }
        ));
        assert!(policy.decide(&exact).is_permit());
        Ok(())
    }

    #[test]
    fn provenance_is_store_bound_and_never_grants_ambient_authority() -> Result<(), String> {
        let (_application, policy, administrator) = open(Node::in_memory())?;
        let person = Principal::new(PrincipalId::new("person:one"), PrincipalKind::Person);
        let agent = Principal::new(PrincipalId::new("agent:one"), PrincipalKind::Agent);
        let parent = grant(
            "person-read",
            person.clone(),
            "scope:a",
            vec![
                FederationPermission::ReadState,
                FederationPermission::Reshare,
            ],
            vec![
                AccessOperation::ReadItems,
                AccessOperation::DelegateAuthority,
            ],
        );
        put_grant(&policy, &administrator, parent.clone())?;
        let delegation = AuthorityDelegation {
            id: DelegationId::new("delegate-read"),
            realm_id: AuthorityRealmKey::new("main"),
            parent: DelegationParent::Grant(parent.id),
            delegator: person.clone(),
            delegate: agent.clone(),
            provenance_operation: ProvenanceOperation::AgentInvocation {
                agent_id: "agent:one".to_owned(),
            },
            selections: vec![ScopeSelection::Exact(ScopeId::new("scope:a"))],
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            expires_at: None,
            max_uses: None,
        };
        policy
            .delegate(
                person.clone(),
                AuthorityPresentation::direct(person.clone()),
                delegation.clone(),
            )
            .map_err(|error| error.to_string())?;
        let hop = ProvenanceHop {
            delegation_id: delegation.id,
            delegator: person.clone(),
            delegate: agent.clone(),
            operation: delegation.provenance_operation,
        };
        let mut delegated = request(person, "scope:a", AccessOperation::ReadItems);
        delegated.principal_id = agent.id.clone();
        delegated.presentation = delegated.presentation.forward(hop.clone());
        assert!(policy.decide(&delegated).is_permit());
        let mut forged = delegated;
        forged.presentation.provenance[0].operation = ProvenanceOperation::TaskInvocation {
            task_id: "forged".to_owned(),
        };
        assert!(matches!(
            policy.decide(&forged),
            AuthorizationDecision::Deny(_)
        ));
        assert!(matches!(
            policy.decide(&request(agent, "scope:a", AccessOperation::ReadItems)),
            AuthorizationDecision::Deny(_)
        ));
        Ok(())
    }

    #[test]
    fn delegation_binds_issuer_and_exact_parent_obligations() -> Result<(), String> {
        let (_application, policy, administrator) = open(Node::in_memory())?;
        let person = Principal::new(PrincipalId::new("person:delegator"), PrincipalKind::Person);
        let agent = Principal::new(PrincipalId::new("agent:delegate"), PrincipalKind::Agent);
        let approver = node_principal("node:reviewer");
        let obligation = Obligation {
            id: ObligationId::new("delegated-review"),
            realm_id: AuthorityRealmKey::new("main"),
            challenge_kind: "human_review".to_owned(),
            prompt: "review delegated read".to_owned(),
            approvers: vec![approver],
            approval_lifetime_seconds: 60,
            approval_use_count: 1,
        };
        policy
            .issue_obligation(
                administrator.clone(),
                AuthorityPresentation::direct(administrator.clone()),
                obligation.clone(),
            )
            .map_err(|error| error.to_string())?;

        let overlapping = grant(
            "overlapping-unobligated",
            person.clone(),
            "scope:delegated",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        put_grant(&policy, &administrator, overlapping)?;
        let mut parent = grant(
            "exact-obligated-parent",
            person.clone(),
            "scope:delegated",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        parent.obligations.push(obligation.id.clone());
        put_grant(&policy, &administrator, parent.clone())?;
        put_grant(
            &policy,
            &administrator,
            grant(
                "separate-reshare",
                person.clone(),
                "scope:delegated",
                vec![FederationPermission::Reshare],
                vec![AccessOperation::DelegateAuthority],
            ),
        )?;

        let delegation = AuthorityDelegation {
            id: DelegationId::new("exact-parent-delegation"),
            realm_id: AuthorityRealmKey::new("main"),
            parent: DelegationParent::Grant(parent.id.clone()),
            delegator: person.clone(),
            delegate: agent.clone(),
            provenance_operation: ProvenanceOperation::AgentInvocation {
                agent_id: agent.id.to_string(),
            },
            selections: vec![ScopeSelection::Exact(ScopeId::new("scope:delegated"))],
            permissions: vec![FederationPermission::ReadState],
            operations: vec![AccessOperation::ReadItems],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            expires_at: None,
            max_uses: None,
        };
        policy
            .delegate(
                person.clone(),
                AuthorityPresentation::direct(person.clone()),
                delegation.clone(),
            )
            .map_err(|error| error.to_string())?;

        let mut forged = delegation.clone();
        forged.id = DelegationId::new("forged-delegator");
        forged.delegator = node_principal("node:someone-else");
        assert!(
            policy
                .delegate(
                    person.clone(),
                    AuthorityPresentation::direct(person.clone()),
                    forged,
                )
                .is_err()
        );
        let mut forged_grant = grant(
            "forged-grantor",
            agent.clone(),
            "scope:delegated",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        forged_grant.grantor = node_principal("node:someone-else");
        assert!(
            policy
                .issue_grant(
                    administrator.clone(),
                    AuthorityPresentation::direct(administrator.clone()),
                    forged_grant,
                )
                .is_err()
        );

        let hop = ProvenanceHop {
            delegation_id: delegation.id,
            delegator: person.clone(),
            delegate: agent.clone(),
            operation: delegation.provenance_operation,
        };
        let mut delegated = request(person, "scope:delegated", AccessOperation::ReadItems);
        delegated.principal_id = agent.id;
        delegated.presentation = delegated.presentation.forward(hop);
        assert!(matches!(
            policy.decide(&delegated),
            AuthorizationDecision::Challenge { challenge, report }
                if challenge.obligation_id == obligation.id
                    && report.explanations.iter().any(|explanation| {
                        explanation.obligation_id.as_ref() == Some(&obligation.id)
                    })
        ));
        policy
            .revoke(
                administrator.clone(),
                AuthorityPresentation::direct(administrator),
                RevocationKind::Grant,
                parent.id.to_string(),
            )
            .map_err(|error| error.to_string())?;
        assert!(matches!(
            policy.decide(&delegated),
            AuthorizationDecision::Deny(_)
        ));
        Ok(())
    }

    #[test]
    fn bounded_uses_survive_redb_restart() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("authority.redb");
        let user = node_principal("node:user");
        {
            let node =
                myko_redb::RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
            let (_application, policy, administrator) = open(node)?;
            let mut once = grant(
                "once",
                user.clone(),
                "scope:a",
                vec![FederationPermission::ReadState],
                vec![AccessOperation::ReadItems],
            );
            once.max_uses = Some(1);
            put_grant(&policy, &administrator, once)?;
            assert!(
                policy
                    .decide(&request(
                        user.clone(),
                        "scope:a",
                        AccessOperation::ReadItems
                    ))
                    .is_permit()
            );
        }
        let node = myko_redb::RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
        let application =
            AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
        let policy = AuthorityPolicy::new(
            ApplicationNode::new(node, application),
            AuthorityRealmKey::new("main"),
        );
        assert!(matches!(
            policy.decide(&request(user, "scope:a", AccessOperation::ReadItems)),
            AuthorizationDecision::Deny(_)
        ));
        Ok(())
    }

    #[test]
    fn replication_intersection_is_partial_and_consumes_once() -> Result<(), String> {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let mut read = grant(
            "replicate-a",
            user.clone(),
            "scope:a",
            vec![FederationPermission::ReadHistory],
            vec![AccessOperation::ReadHistory],
        );
        read.max_uses = Some(1);
        put_grant(&policy, &administrator, read)?;
        let access = request(user, "scope:a", AccessOperation::ReadHistory);
        let selection = policy
            .constrain_replication(
                &access,
                &ReplicationSelection::Scopes(vec![
                    ScopeSelection::Exact(ScopeId::new("scope:a")),
                    ScopeSelection::Exact(ScopeId::new("scope:b")),
                ]),
                &application
                    .node()
                    .scope_topology()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|decision| decision.public_message())?;
        assert_eq!(
            selection,
            ReplicationSelection::Intersection {
                requested: Box::new(ReplicationSelection::Scopes(vec![
                    ScopeSelection::Exact(ScopeId::new("scope:a")),
                    ScopeSelection::Exact(ScopeId::new("scope:b")),
                ])),
                scopes: vec![ScopeSelection::Exact(ScopeId::new("scope:a"))],
            }
        );
        assert!(
            policy
                .constrain_replication(
                    &request(
                        node_principal("node:user"),
                        "scope:a",
                        AccessOperation::ReadHistory,
                    ),
                    &ReplicationSelection::Scopes(vec![ScopeSelection::Exact(ScopeId::new(
                        "scope:a",
                    ))]),
                    &application
                        .node()
                        .scope_topology()
                        .map_err(|error| error.to_string())?,
                )
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn idle_stream_does_not_consume_and_revocation_closes_it() -> Result<(), String> {
        use myko_session::NodeSessionService;
        use myko_wire::{NodeFrame, NodeRequest, NodeRequestEnvelope};

        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let mut streaming = grant(
            "stream",
            user.clone(),
            "scope:a",
            vec![
                FederationPermission::ReadHistory,
                FederationPermission::Subscribe,
            ],
            vec![AccessOperation::FollowHistory],
        );
        streaming.max_uses = Some(1);
        put_grant(&policy, &administrator, streaming.clone())?;
        let access_policy: Arc<dyn AccessPolicy> = policy.clone();
        let session = NodeSessionService::for_application(application.clone(), access_policy);
        let mut stream = session
            .open(
                user.id.clone(),
                NodeRequestEnvelope::connected(NodeRequest::FollowScope {
                    scope_id: ScopeId::new("scope:a"),
                    after: None,
                }),
            )
            .await;
        let permit = tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .map_err(|error| error.to_string())?;
        assert!(matches!(
            permit,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Permit(_))
        ));
        let hello = tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .map_err(|error| error.to_string())?;
        assert!(matches!(hello, Some(NodeFrame::Hello { .. })));
        let before_idle = application
            .node()
            .events_after(None)
            .map_err(|error| error.to_string())?
            .len();
        tokio::time::sleep(std::time::Duration::from_millis(140)).await;
        let after_idle = application
            .node()
            .events_after(None)
            .map_err(|error| error.to_string())?
            .len();
        assert_eq!(before_idle, after_idle);
        application
            .exec_authenticated_command(
                administrator.id,
                RevokeAuthorityFact {
                    realm_id: AuthorityRealmKey::new("main"),
                    kind: RevocationKind::Grant,
                    id: streaming.id.to_string(),
                    at: Utc::now(),
                },
            )
            .map_err(|error| error.to_string())?;
        let closed = tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .map_err(|error| error.to_string())?;
        assert!(matches!(
            closed,
            Some(NodeFrame::Authorization { decision })
                if matches!(*decision, AuthorizationDecision::Deny(_))
        ));
        let finished = tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv())
            .await
            .map_err(|error| error.to_string())?;
        assert!(finished.is_none());
        Ok(())
    }

    #[test]
    fn leases_enforce_online_revocation_offline_expiry_and_exact_binding() -> Result<(), String> {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let online_user = node_principal("node:online-user");
        let mut online_grant = grant(
            "online-lease",
            online_user.clone(),
            "scope:lease",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        online_grant.constraints.max_lease_seconds = Some(1);
        put_grant(&policy, &administrator, online_grant.clone())?;

        let mut online_request = request(
            online_user.clone(),
            "scope:lease",
            AccessOperation::ReadItems,
        );
        online_request.lease = Some(AuthorityLeaseRequest {
            duration_seconds: 1,
            offline: false,
        });
        let online_lease = match policy.decide(&online_request) {
            AuthorizationDecision::Permit(PermitDecision {
                lease: Some(lease), ..
            }) => lease,
            decision => return Err(format!("expected online lease, found {decision:?}")),
        };
        let mut online_continuation = online_request.clone();
        online_continuation.authorization_phase = myko_federation::AuthorizationPhase::Continuation;
        online_continuation.lease = None;
        online_continuation.presentation =
            AuthorityPresentation::direct(online_user.clone()).with_lease(online_lease.id);
        assert!(policy.decide(&online_continuation).is_permit());
        application
            .exec_authenticated_command(
                administrator.id.clone(),
                RevokeAuthorityFact {
                    realm_id: AuthorityRealmKey::new("main"),
                    kind: RevocationKind::Grant,
                    id: online_grant.id.to_string(),
                    at: Utc::now(),
                },
            )
            .map_err(|error| error.to_string())?;
        assert!(matches!(
            policy.decide(&online_continuation),
            AuthorizationDecision::Deny(_)
        ));

        let offline_user = node_principal("node:offline-user");
        let mut offline_grant = grant(
            "offline-lease",
            offline_user.clone(),
            "scope:lease",
            vec![FederationPermission::ReadState],
            vec![AccessOperation::ReadItems],
        );
        offline_grant.constraints.max_lease_seconds = Some(1);
        offline_grant.constraints.allow_offline = true;
        put_grant(&policy, &administrator, offline_grant.clone())?;
        let mut offline_request = request(
            offline_user.clone(),
            "scope:lease",
            AccessOperation::ReadItems,
        );
        offline_request.lease = Some(AuthorityLeaseRequest {
            duration_seconds: 1,
            offline: true,
        });
        let offline_lease = match policy.decide(&offline_request) {
            AuthorizationDecision::Permit(PermitDecision {
                lease: Some(lease), ..
            }) => lease,
            decision => return Err(format!("expected offline lease, found {decision:?}")),
        };

        let wrong_user = node_principal("node:wrong-user");
        let mut wrong_binding = request(
            wrong_user.clone(),
            "scope:lease",
            AccessOperation::ReadItems,
        );
        wrong_binding.presentation =
            AuthorityPresentation::direct(wrong_user).with_lease(offline_lease.id.clone());
        assert!(matches!(
            policy.decide(&wrong_binding),
            AuthorizationDecision::Deny(_)
        ));
        let mut wrong_scope = request(
            offline_user.clone(),
            "scope:other",
            AccessOperation::ReadItems,
        );
        wrong_scope.presentation = AuthorityPresentation::direct(offline_user.clone())
            .with_lease(offline_lease.id.clone());
        assert!(matches!(
            policy.decide(&wrong_scope),
            AuthorizationDecision::Deny(_)
        ));

        application
            .exec_authenticated_command(
                administrator.id.clone(),
                RevokeAuthorityFact {
                    realm_id: AuthorityRealmKey::new("main"),
                    kind: RevocationKind::Grant,
                    id: offline_grant.id.to_string(),
                    at: Utc::now(),
                },
            )
            .map_err(|error| error.to_string())?;
        let mut reconnect = offline_request.clone();
        reconnect.lease = None;
        reconnect.presentation = AuthorityPresentation::direct(offline_user.clone())
            .with_lease(offline_lease.id.clone());
        let reconnect_permit = match policy.decide(&reconnect) {
            AuthorizationDecision::Permit(permit) => permit,
            decision => {
                return Err(format!(
                    "expected offline reconnect permit, found {decision:?}"
                ));
            }
        };
        assert!(
            reconnect_permit
                .report
                .explanations
                .iter()
                .any(|explanation| {
                    explanation.code == "offline_lease" && explanation.constraint.is_some()
                })
        );
        std::thread::sleep(std::time::Duration::from_millis(1_100));
        assert!(matches!(
            policy.decide(&reconnect),
            AuthorizationDecision::Deny(_)
        ));

        let mut forbidden_offline = request(online_user, "scope:lease", AccessOperation::ReadItems);
        forbidden_offline.lease = Some(AuthorityLeaseRequest {
            duration_seconds: 1,
            offline: true,
        });
        assert!(matches!(
            policy.decide(&forbidden_offline),
            AuthorizationDecision::Deny(_)
        ));
        let mut excessive = offline_request;
        excessive.lease = Some(AuthorityLeaseRequest {
            duration_seconds: 2,
            offline: true,
        });
        assert!(matches!(
            policy.decide(&excessive),
            AuthorizationDecision::Deny(_)
        ));
        Ok(())
    }

    #[test]
    fn effect_challenges_park_exact_batch_and_multiple_approvals_commit_once() -> Result<(), String>
    {
        let (application, policy, administrator) = open(Node::in_memory())?;
        let user = node_principal("node:user");
        let obligations = ["review-one", "review-two"]
            .into_iter()
            .map(|id| Obligation {
                id: ObligationId::new(id),
                realm_id: AuthorityRealmKey::new("main"),
                challenge_kind: "human_review".to_owned(),
                prompt: format!("approve {id}"),
                approvers: vec![administrator.clone()],
                approval_lifetime_seconds: 60,
                approval_use_count: 1,
            })
            .collect::<Vec<_>>();
        for obligation in &obligations {
            application
                .exec_authenticated_command(
                    administrator.id.clone(),
                    PutObligation {
                        realm_id: AuthorityRealmKey::new("main"),
                        obligation: obligation.clone(),
                    },
                )
                .map_err(|error| error.to_string())?;
        }
        let realm = realm_scope(&AuthorityRealmKey::new("main"));
        let mut delegated_admin = grant(
            "reviewed-admin",
            user.clone(),
            realm.as_str(),
            vec![
                FederationPermission::Admin,
                FederationPermission::ReadState,
                FederationPermission::Write,
            ],
            vec![
                AccessOperation::AdministerAuthority,
                AccessOperation::ReadItems,
                AccessOperation::SubmitCommand,
            ],
        );
        delegated_admin.obligations = obligations
            .iter()
            .map(|obligation| obligation.id.clone())
            .collect();
        put_grant(&policy, &administrator, delegated_admin)?;

        let command = PutCapability {
            realm_id: AuthorityRealmKey::new("main"),
            capability: ApplicationCapability {
                id: CapabilityId::new("reviewed.capability"),
                description: "requires two approvals".to_owned(),
                constraints: AuthorityConstraints::default(),
            },
        };
        let submitted = application
            .submit_authenticated_command(user.id.clone(), &command)
            .map_err(|error| error.to_string())?;
        let dispatched = application
            .dispatch_registered_command(submitted.request.id)
            .map_err(|error| format!("initial challenged dispatch: {error}"))?;
        let first_challenge = match dispatched.command.state {
            CommandState::AuthorizationPending { challenge_id, .. } => challenge_id,
            state => return Err(format!("expected parked command, found {state:?}")),
        };
        let first = policy
            .approve(
                &administrator.id,
                &AuthorityPresentation::direct(administrator.clone()),
                &first_challenge,
                true,
            )
            .map_err(|decision| format!("first approval: {}", decision.public_message()))?;
        let after_first = application
            .node()
            .command(submitted.request.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "challenged command disappeared".to_owned())?;
        let second_challenge = match after_first.state {
            CommandState::AuthorizationPending { challenge_id, .. } => challenge_id,
            state => return Err(format!("expected second challenge, found {state:?}")),
        };
        assert_ne!(first_challenge, second_challenge);
        let repeated = policy
            .approve(
                &administrator.id,
                &AuthorityPresentation::direct(administrator.clone()),
                &first_challenge,
                true,
            )
            .map_err(|decision| format!("repeated approval: {}", decision.public_message()))?;
        assert_eq!(first.id, repeated.id);
        policy
            .approve(
                &administrator.id,
                &AuthorityPresentation::direct(administrator.clone()),
                &second_challenge,
                true,
            )
            .map_err(|decision| format!("second approval: {}", decision.public_message()))?;
        let committed = application
            .node()
            .command(submitted.request.id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "approved command disappeared".to_owned())?;
        assert!(committed.state.is_committed());
        let capabilities = application
            .node()
            .query_items_in(application.node_id(), &realm, GetAllCapabilityRegistrations)
            .map_err(|error| error.to_string())?;
        assert_eq!(
            capabilities
                .iter()
                .filter(|record| record.capability.id.as_str() == "reviewed.capability")
                .count(),
            1
        );
        Ok(())
    }
}
