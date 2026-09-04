use super::{
    AccessAttempt, AccessOperation, ApprovalId, AuthorityChallenge, AuthorityDelegation,
    AuthorityGrant, AuthorityGrantId, AuthorityLease, AuthorityLeaseRequest, AuthorizationBinding,
    AuthorizationDecision, AuthorizationExplanation, AuthorizationReport, BTreeMap, BTreeSet,
    CapabilityId, ChallengeId, DateTime, DelegationId, DenyDecision, Duration, EvaluationOutcome,
    EvaluationState, FederationPermission, LeaseId, ObligationRecord, PermitDecision,
    ResourceClaim, ResourceClaimKind, ResourceVisibility, ScopeSelection, ScopeTopology, Utc,
    is_stream, permission_for,
};

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

pub(super) fn deny(
    request: &AccessAttempt,
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

fn claim_requirements(
    request: &AccessAttempt,
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
    request: &AccessAttempt,
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

struct EvaluationContext<'a> {
    state: &'a EvaluationState,
    request: &'a AccessAttempt,
    now: DateTime<Utc>,
    claims: &'a [ResourceClaim],
    binding: AuthorizationBinding,
    grant_use_counts: BTreeMap<AuthorityGrantId, u64>,
}

type EvaluationResult<T> = Result<T, Box<EvaluationOutcome>>;

impl<'a> EvaluationContext<'a> {
    fn new(
        state: &'a EvaluationState,
        request: &'a AccessAttempt,
        now: DateTime<Utc>,
    ) -> EvaluationResult<Self> {
        if state.realm.is_none() {
            return Err(Box::new(deny(
                request,
                now,
                "realm_unbound",
                "authority realm is not bootstrapped",
            )));
        }
        if request.principal_id != request.presentation.executor.id {
            return Err(Box::new(deny(
                request,
                now,
                "executor_mismatch",
                "authority executor does not match authenticated transport principal",
            )));
        }
        if request.resource_claims.is_empty() {
            return Err(Box::new(deny(
                request,
                now,
                "claims_missing",
                "request declares no resource claims",
            )));
        }
        let grant_use_counts = state.grant_uses.iter().fold(
            BTreeMap::<AuthorityGrantId, u64>::new(),
            |mut counts, usage| {
                let entry = counts.entry(usage.grant_id.clone()).or_default();
                *entry = entry.saturating_add(1);
                counts
            },
        );
        Ok(Self {
            state,
            request,
            now,
            claims: &request.resource_claims,
            binding: AuthorizationBinding::from_request(request),
            grant_use_counts,
        })
    }

    fn deny(&self, code: &str, message: &str) -> EvaluationOutcome {
        deny(self.request, self.now, code, message)
    }

    fn failure(&self, code: &str, message: &str) -> Box<EvaluationOutcome> {
        Box::new(self.deny(code, message))
    }
}

struct GrantResolution {
    contributing: BTreeSet<AuthorityGrantId>,
    required_obligations: BTreeSet<myko_federation::ObligationId>,
}

fn validate_lease(context: &EvaluationContext<'_>) -> EvaluationResult<Option<EvaluationOutcome>> {
    let request = context.request;
    let Some(lease_id) = request.presentation.active_lease.as_ref() else {
        if request.authorization_phase == myko_federation::AuthorizationPhase::Continuation
            && request.lease.is_some()
        {
            return Err(context.failure(
                "lease_missing",
                "continuation requires the lease issued at admission",
            ));
        }
        return Ok(None);
    };
    let Some(lease_record) = context.state.leases.iter().find(|record| {
        &record.lease.id == lease_id
            && record.lease.expires_at > context.now
            && record.binding == context.binding
    }) else {
        return Err(context.failure(
            "lease_invalid",
            "the presented authority lease is absent, expired, or bound to another request",
        ));
    };
    if request.authorization_phase == myko_federation::AuthorizationPhase::Admission
        && !lease_record.lease.offline
    {
        return Err(context.failure(
            "lease_online_reconnect",
            "an online lease cannot authorize a new connection",
        ));
    }
    if !lease_record.lease.offline {
        return Ok(None);
    }
    Ok(Some(EvaluationOutcome {
        decision: AuthorizationDecision::Permit(PermitDecision {
            report: AuthorizationReport {
                evaluated_at: context.now,
                principal: request.presentation.principal.clone(),
                executor: request.presentation.executor.clone(),
                operation: request.operation,
                explanations: vec![AuthorizationExplanation {
                    code: "offline_lease".to_owned(),
                    message: "bounded cached authority is valid until lease expiry".to_owned(),
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
    }))
}

fn validate_capabilities(context: &EvaluationContext<'_>) -> EvaluationResult<()> {
    let mut required = context
        .request
        .application_capabilities
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for claim in context.claims {
        required.extend(claim.required_capabilities.iter().cloned());
    }
    for capability_id in required {
        let Some(registered) =
            context.state.capabilities.iter().find(|record| {
                record.revoked_at.is_none() && record.capability.id == capability_id
            })
        else {
            return Err(context.failure(
                "capability_unregistered",
                "a required application capability is not registered",
            ));
        };
        if !registered.capability.constraints.permits(context.request) {
            return Err(context.failure(
                "capability_constraint",
                "application capability constraints reject the request",
            ));
        }
    }
    Ok(())
}

fn resolve_grants(context: &EvaluationContext<'_>) -> EvaluationResult<GrantResolution> {
    let mut resolution = GrantResolution {
        contributing: BTreeSet::new(),
        required_obligations: BTreeSet::new(),
    };
    for claim in context.claims {
        if context.request.authorization_phase != myko_federation::AuthorizationPhase::Effect
            && claim.kind == ResourceClaimKind::Affected
        {
            continue;
        }
        let (permissions, operations, capabilities) = claim_requirements(context.request, claim);
        let candidates = context.state.grants.iter().filter(|record| {
            let grant = &record.grant;
            record.revoked_at.is_none()
                && grant.grantee == context.request.presentation.principal
                && grant.valid_from <= context.now
                && grant.expires_at.is_none_or(|expiry| expiry > context.now)
                && grant.max_uses.is_none_or(|maximum| {
                    context
                        .grant_use_counts
                        .get(&grant.id)
                        .copied()
                        .unwrap_or(0)
                        < maximum
                })
                && grant.constraints.permits(context.request)
                && selection_covers(&grant.selection, &claim.selection, &context.state.topology)
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
            if before
                != (
                    missing_permissions.len(),
                    missing_operations.len(),
                    missing_capabilities.len(),
                )
            {
                resolution.contributing.insert(grant.id.clone());
                resolution
                    .required_obligations
                    .extend(grant.obligations.iter().cloned());
            }
        }
        if !missing_permissions.is_empty()
            || !missing_operations.is_empty()
            || !missing_capabilities.is_empty()
        {
            let detail = format!(
                "active grants do not cover claim {:?} during {:?} for command {:?}; known ancestors {:?}; missing permissions {missing_permissions:?}, operations {missing_operations:?}, capabilities {missing_capabilities:?}",
                claim.selection,
                context.request.authorization_phase,
                context.request.command_type(),
                context.state.topology.ancestors(claim.selection.root()),
            );
            return Err(context.failure("grant_coverage", &detail));
        }
    }
    Ok(resolution)
}

fn delegation_use_counts(state: &EvaluationState) -> BTreeMap<DelegationId, u64> {
    state
        .delegation_uses
        .iter()
        .fold(BTreeMap::<DelegationId, u64>::new(), |mut counts, usage| {
            let entry = counts.entry(usage.delegation_id.clone()).or_default();
            *entry = entry.saturating_add(1);
            counts
        })
}

fn delegation_covers_claims(
    context: &EvaluationContext<'_>,
    delegation: &AuthorityDelegation,
) -> bool {
    context.claims.iter().all(|claim| {
        delegation
            .selections
            .iter()
            .any(|selection| selection_covers(selection, &claim.selection, &context.state.topology))
            && {
                let (permissions, operations, capabilities) =
                    claim_requirements(context.request, claim);
                permissions
                    .iter()
                    .all(|permission| delegation.permissions.contains(permission))
                    && operations
                        .iter()
                        .all(|operation| delegation.operations.contains(operation))
                    && capabilities
                        .iter()
                        .all(|capability| delegation.capabilities.contains(capability))
            }
    })
}

fn parent_is_live(
    context: &EvaluationContext<'_>,
    delegation: &AuthorityDelegation,
    expected_parent: Option<&DelegationId>,
) -> bool {
    match (expected_parent, &delegation.parent) {
        (None, myko_federation::DelegationParent::Grant(grant_id)) => context
            .state
            .grants
            .iter()
            .find(|record| &record.grant.id == grant_id)
            .is_some_and(|record| {
                let grant = &record.grant;
                record.revoked_at.is_none()
                    && grant.grantee == context.request.presentation.principal
                    && grant.valid_from <= context.now
                    && grant.expires_at.is_none_or(|expiry| expiry > context.now)
                    && grant.max_uses.is_none_or(|maximum| {
                        context.grant_use_counts.get(grant_id).copied().unwrap_or(0) < maximum
                    })
                    && grant_independently_covers(
                        grant,
                        context.request,
                        context.claims,
                        &context.state.topology,
                    )
            }),
        (Some(expected), myko_federation::DelegationParent::Delegation(parent_id)) => {
            parent_id == expected
        }
        _ => false,
    }
}

fn resolve_delegations(
    context: &EvaluationContext<'_>,
    grants: &mut GrantResolution,
) -> EvaluationResult<BTreeSet<DelegationId>> {
    let mut contributing = BTreeSet::new();
    let mut expected = context.request.presentation.principal.clone();
    let mut expected_parent = None;
    let use_counts = delegation_use_counts(context.state);
    for hop in &context.request.presentation.provenance {
        if hop.delegator != expected {
            return Err(context.failure("provenance_chain", "provenance chain is discontinuous"));
        }
        let Some(record) = context.state.delegations.iter().find(|record| {
            record.revoked_at.is_none() && record.delegation.id == hop.delegation_id
        }) else {
            return Err(context.failure("delegation_missing", "delegation is not authoritative"));
        };
        let delegation = &record.delegation;
        let identity_mismatch =
            delegation.delegator != hop.delegator || delegation.delegate != hop.delegate;
        let provenance_operation_mismatch = delegation.provenance_operation != hop.operation;
        let invalid = identity_mismatch
            || provenance_operation_mismatch
            || delegation
                .expires_at
                .is_some_and(|expiry| expiry <= context.now)
            || delegation.max_uses.is_some_and(|maximum| {
                use_counts.get(&delegation.id).copied().unwrap_or(0) >= maximum
            })
            || !delegation.constraints.permits(context.request)
            || !parent_is_live(context, delegation, expected_parent.as_ref())
            || !delegation_covers_claims(context, delegation);
        if invalid {
            return Err(context.failure(
                "delegation_attenuation",
                "delegated authority does not cover the request",
            ));
        }
        if let myko_federation::DelegationParent::Grant(grant_id) = &delegation.parent {
            grants.contributing.clear();
            grants.required_obligations.clear();
            grants.contributing.insert(grant_id.clone());
            if let Some(parent) = context
                .state
                .grants
                .iter()
                .find(|record| &record.grant.id == grant_id)
            {
                grants
                    .required_obligations
                    .extend(parent.grant.obligations.iter().cloned());
            }
        }
        contributing.insert(delegation.id.clone());
        expected_parent = Some(delegation.id.clone());
        expected = hop.delegate.clone();
    }
    if expected != context.request.presentation.executor {
        return Err(context.failure(
            "provenance_executor",
            "provenance does not terminate at the authenticated executor",
        ));
    }
    Ok(contributing)
}

fn challenge(
    context: &EvaluationContext<'_>,
    obligation: &ObligationRecord,
    challenge: AuthorityChallenge,
) -> EvaluationOutcome {
    EvaluationOutcome {
        decision: AuthorizationDecision::Challenge {
            challenge,
            report: AuthorizationReport {
                evaluated_at: context.now,
                principal: context.request.presentation.principal.clone(),
                executor: context.request.presentation.executor.clone(),
                operation: context.request.operation,
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
    }
}

fn resolve_obligations(
    context: &EvaluationContext<'_>,
    mut required: BTreeSet<myko_federation::ObligationId>,
) -> EvaluationResult<BTreeSet<ApprovalId>> {
    if context.request.operation == AccessOperation::SubmitCommand
        && context.request.authorization_phase != myko_federation::AuthorizationPhase::Effect
    {
        required.clear();
    }
    let approval_counts = context.state.approval_uses.iter().fold(
        BTreeMap::<ApprovalId, u64>::new(),
        |mut counts, usage| {
            let entry = counts.entry(usage.approval_id.clone()).or_default();
            *entry = entry.saturating_add(1);
            counts
        },
    );
    let mut used = BTreeSet::new();
    for obligation_id in required {
        let Some(obligation) =
            context.state.obligations.iter().find(|record| {
                record.revoked_at.is_none() && record.obligation.id == obligation_id
            })
        else {
            return Err(context.failure("obligation_missing", "required obligation is unavailable"));
        };
        let approval = context.state.approvals.iter().find(|record| {
            let decision = &record.decision;
            context
                .request
                .presentation
                .approvals
                .contains(&decision.id)
                && decision.obligation_id == obligation_id
                && decision.approved
                && decision.binding == context.binding
                && decision.expires_at > context.now
                && approval_counts.get(&decision.id).copied().unwrap_or(0) < decision.max_uses
        });
        if let Some(approval) = approval {
            used.insert(approval.decision.id.clone());
            continue;
        }
        if let Some(existing) = context.state.challenges.iter().find(|record| {
            record.challenge.obligation_id == obligation_id
                && record.challenge.binding == context.binding
                && record.challenge.expires_at > context.now
        }) {
            return Err(Box::new(challenge(
                context,
                obligation,
                existing.challenge.clone(),
            )));
        }
        let lifetime =
            i64::try_from(obligation.obligation.approval_lifetime_seconds).unwrap_or(i64::MAX);
        let expires_at = context
            .now
            .checked_add_signed(Duration::seconds(lifetime))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let pending = AuthorityChallenge {
            id: ChallengeId::random(),
            realm_id: obligation.obligation.realm_id.clone(),
            obligation_id,
            kind: obligation.obligation.challenge_kind.clone(),
            prompt: obligation.obligation.prompt.clone(),
            binding: context.binding.clone(),
            issued_at: context.now,
            expires_at,
        };
        return Err(Box::new(challenge(context, obligation, pending)));
    }
    Ok(used)
}

fn permit(
    context: &EvaluationContext<'_>,
    grants: GrantResolution,
    delegations: BTreeSet<DelegationId>,
    approvals: BTreeSet<ApprovalId>,
) -> EvaluationOutcome {
    let lease = context
        .request
        .lease
        .and_then(|requested| make_lease(requested, context.now));
    EvaluationOutcome {
        decision: AuthorizationDecision::Permit(PermitDecision {
            report: AuthorizationReport {
                evaluated_at: context.now,
                principal: context.request.presentation.principal.clone(),
                executor: context.request.presentation.executor.clone(),
                operation: context.request.operation,
                explanations: grants
                    .contributing
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
        grants: grants.contributing,
        delegations,
        approvals,
    }
}

fn evaluate_stages(context: &EvaluationContext<'_>) -> EvaluationResult<EvaluationOutcome> {
    if let Some(outcome) = validate_lease(context)? {
        return Ok(outcome);
    }
    validate_capabilities(context)?;
    let mut grants = resolve_grants(context)?;
    let delegations = resolve_delegations(context, &mut grants)?;
    let approvals = resolve_obligations(context, grants.required_obligations.clone())?;
    Ok(permit(context, grants, delegations, approvals))
}

pub(super) fn evaluate(
    state: &EvaluationState,
    request: &AccessAttempt,
    now: DateTime<Utc>,
) -> EvaluationOutcome {
    match EvaluationContext::new(state, request, now).and_then(|context| evaluate_stages(&context))
    {
        Ok(outcome) => outcome,
        Err(outcome) => *outcome,
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
