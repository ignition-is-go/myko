use super::{
    AccessAttempt, AccessOperation, ApplicationCapability, ApprovalDecision, ApprovalId,
    ApprovalRecord, ApprovalRecordId, ApprovalUse, ApprovalUseId, AuthorityConstraints,
    AuthorityDelegation, AuthorityGrant, AuthorityGrantId, AuthorityRealm, AuthorityRealmId,
    AuthorityRealmKey, AuthorityService, AuthorizationBinding, AuthorizationDecision, BTreeMap,
    CapabilityRegistration, CapabilityRegistrationId, ChallengeId, ChallengeRecord,
    ChallengeRecordId, CommandContext, CommandError, CommandHandler, DateTime, DecisionAudit,
    DecisionAuditId, DelegationId, DelegationRecord, DelegationRecordId, DelegationUse,
    DelegationUseId, Deserialize, Duration, EvaluationState, FederationPermission,
    GetAllApprovalRecords, GetAllApprovalUses, GetAllCapabilityRegistrations,
    GetAllChallengeRecords, GetAllDelegationRecords, GetAllDelegationUses, GetAllGrantRecords,
    GetAllGrantUses, GetAllLeaseRecords, GetAllObligationRecords, GetAuthorityRealmById,
    GetCapabilityRegistrationById, GetChallengeRecordById, GetDelegationRecordById,
    GetGrantRecordById, GetObligationRecordById, GrantRecord, GrantRecordId, GrantUse, GrantUseId,
    LeaseRecord, LeaseRecordId, Obligation, ObligationRecord, ObligationRecordId, PermitDecision,
    Principal, ResourceClaim, ScopeId, ScopeSelection, ScopeTopology, Serialize, Utc,
    administration_claim, evaluate, myko_command, realm_item_id, record_id,
};

#[myko_command(AuthorityRealm, service = AuthorityService, scope = AuthorityRealm)]
pub(super) struct BootstrapRealm {
    pub(super) realm_id: AuthorityRealmKey,
    pub(super) principal: Principal,
    pub(super) at: DateTime<Utc>,
}

impl CommandHandler for BootstrapRealm {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(self, context: CommandContext) -> Result<AuthorityRealm, CommandError> {
        if !context
            .exec_item_query(GetAuthorityRealmById {
                id: realm_item_id(&self.realm_id),
            })?
            .is_empty()
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
pub struct IssueAuthorityGrant {
    pub realm_id: AuthorityRealmKey,
    pub grant: AuthorityGrant,
}

impl CommandHandler for IssueAuthorityGrant {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
        if self.grant.realm_id != self.realm_id {
            return Err(CommandError::reject(
                "grant belongs to another authority realm",
            ));
        }
        if &self.grant.grantor != context.authority_principal() {
            return Err(CommandError::reject(
                "grantor does not match the authenticated authority principal",
            ));
        }
        if !context
            .exec_item_query(GetGrantRecordById {
                id: GrantRecordId::from(self.grant.id.as_str()),
            })?
            .is_empty()
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
pub(super) struct PutDelegation {
    pub realm_id: AuthorityRealmKey,
    pub delegation: AuthorityDelegation,
    pub(super) issuer: Principal,
}

impl CommandHandler for PutDelegation {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    #[allow(clippy::too_many_lines)] // Parent-chain validation is kept contiguous for auditability.
    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
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
        if !context
            .exec_item_query(GetDelegationRecordById {
                id: DelegationRecordId::from(self.delegation.id.as_str()),
            })?
            .is_empty()
        {
            return Err(CommandError::reject(
                "delegation id is immutable and already exists",
            ));
        }
        let topology = context
            .scope_topology()
            .map_err(|error| CommandError::retry(error.message))?;
        let now = Utc::now();
        let grant_uses = context.exec_item_query(GetAllGrantUses)?.into_iter().fold(
            BTreeMap::<AuthorityGrantId, u64>::new(),
            |mut counts, usage| {
                counts
                    .entry(usage.grant_id)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
                counts
            },
        );
        let delegation_uses = context
            .exec_item_query(GetAllDelegationUses)?
            .into_iter()
            .fold(BTreeMap::<DelegationId, u64>::new(), |mut counts, usage| {
                counts
                    .entry(usage.delegation_id)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
                counts
            });
        let valid_parent = match &self.delegation.parent {
            myko_federation::DelegationParent::Grant(id) => context
                .exec_item_query(GetGrantRecordById {
                    id: GrantRecordId::from(id.as_str()),
                })?
                .into_iter()
                .next()
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
                .exec_item_query(GetDelegationRecordById {
                    id: DelegationRecordId::from(id.as_str()),
                })?
                .into_iter()
                .next()
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
pub(super) struct PutObligation {
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

    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
        if self.obligation.realm_id != self.realm_id {
            return Err(CommandError::reject(
                "obligation belongs to another authority realm",
            ));
        }
        if !context
            .exec_item_query(GetObligationRecordById {
                id: ObligationRecordId::from(self.obligation.id.as_str()),
            })?
            .is_empty()
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
pub(super) struct PutCapability {
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

    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
        if !context
            .exec_item_query(GetCapabilityRegistrationById {
                id: CapabilityRegistrationId::from(self.capability.id.as_str()),
            })?
            .is_empty()
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
pub struct RevokeAuthorityFact {
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

    fn execute(self, context: CommandContext) -> Result<(), CommandError> {
        match self.kind {
            RevocationKind::Grant => {
                let mut record = context
                    .exec_item_query(GetGrantRecordById {
                        id: GrantRecordId::from(self.id),
                    })?
                    .into_iter()
                    .next()
                    .ok_or_else(|| CommandError::reject("grant is not authoritatively present"))?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Delegation => {
                let mut record = context
                    .exec_item_query(GetDelegationRecordById {
                        id: DelegationRecordId::from(self.id),
                    })?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        CommandError::reject("delegation is not authoritatively present")
                    })?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Obligation => {
                let mut record = context
                    .exec_item_query(GetObligationRecordById {
                        id: ObligationRecordId::from(self.id),
                    })?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        CommandError::reject("obligation is not authoritatively present")
                    })?;
                record.revoked_at = Some(self.at);
                context.emit_set(&record)
            }
            RevocationKind::Capability => {
                let mut record = context
                    .exec_item_query(GetCapabilityRegistrationById {
                        id: CapabilityRegistrationId::from(self.id),
                    })?
                    .into_iter()
                    .next()
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
pub(super) struct EvaluateAuthority {
    pub(super) realm_id: AuthorityRealmKey,
    pub(super) request: AccessAttempt,
    pub(super) topology_proof: ScopeTopology,
    pub(super) now: DateTime<Utc>,
}

impl CommandHandler for EvaluateAuthority {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(self, context: CommandContext) -> Result<AuthorizationDecision, CommandError> {
        let mut topology = context.scope_topology().map_err(|error| {
            CommandError::retry(format!("authority topology unavailable: {}", error.message))
        })?;
        topology
            .merge_proof(&self.topology_proof)
            .map_err(|error| CommandError::reject(error.to_string()))?;
        let mut request = self.request;
        request.topology = Some(topology.clone());
        let state = EvaluationState {
            realm: context
                .exec_item_query(GetAuthorityRealmById {
                    id: realm_item_id(&self.realm_id),
                })?
                .into_iter()
                .next(),
            capabilities: context.exec_item_query(GetAllCapabilityRegistrations)?,
            grants: context.exec_item_query(GetAllGrantRecords)?,
            delegations: context.exec_item_query(GetAllDelegationRecords)?,
            obligations: context.exec_item_query(GetAllObligationRecords)?,
            challenges: context.exec_item_query(GetAllChallengeRecords)?,
            approvals: context.exec_item_query(GetAllApprovalRecords)?,
            grant_uses: context.exec_item_query(GetAllGrantUses)?,
            delegation_uses: context.exec_item_query(GetAllDelegationUses)?,
            approval_uses: context.exec_item_query(GetAllApprovalUses)?,
            leases: context.exec_item_query(GetAllLeaseRecords)?,
            topology,
        };
        let outcome = evaluate(&state, &request, self.now);
        let decision_id = record_id("decision");
        let realm_id = realm_item_id(&self.realm_id);

        let consume = matches!(
            request.authorization_phase,
            myko_federation::AuthorizationPhase::Effect
        ) || (matches!(
            request.authorization_phase,
            myko_federation::AuthorizationPhase::Admission
        ) && request.operation != AccessOperation::SubmitCommand);
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
                .exec_item_query(GetChallengeRecordById {
                    id: challenge_id.clone(),
                })?
                .is_empty()
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
                binding: AuthorizationBinding::from_request(&request),
            })?;
        }
        context.emit_set(&DecisionAudit {
            id: DecisionAuditId::from(decision_id),
            authority_realm_id: realm_id,
            request,
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
pub(super) struct DecideChallenge {
    pub(super) realm_id: AuthorityRealmKey,
    pub(super) challenge_id: ChallengeId,
    pub(super) approved: bool,
    pub(super) approver: Principal,
    pub(super) now: DateTime<Utc>,
}

impl CommandHandler for DecideChallenge {
    fn scope(&self, _node_id: myko_federation::NodeId) -> AuthorityRealmId {
        realm_item_id(&self.realm_id)
    }

    fn authority_claims(&self, _node_id: myko_federation::NodeId) -> Vec<ResourceClaim> {
        vec![administration_claim(&self.realm_id)]
    }

    fn execute(self, context: CommandContext) -> Result<ApprovalDecision, CommandError> {
        let challenge = context
            .exec_item_query(GetChallengeRecordById {
                id: ChallengeRecordId::from(self.challenge_id.as_str()),
            })?
            .into_iter()
            .next()
            .ok_or_else(|| CommandError::reject("challenge is not authoritatively present"))?
            .challenge;
        if let Some(existing) = context
            .exec_item_query(GetAllApprovalRecords)?
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
            .exec_item_query(GetObligationRecordById {
                id: ObligationRecordId::from(challenge.obligation_id.as_str()),
            })?
            .into_iter()
            .next()
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
