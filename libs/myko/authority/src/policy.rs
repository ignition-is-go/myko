use super::{
    AccessAttempt, AccessOperation, AccessPolicy, AccessTarget, AppError, ApplicationCapability,
    ApplicationHost, ApprovalDecision, Arc, AtomicU64, AuthorityDelegation, AuthorityFactSources,
    AuthorityGrant, AuthorityPresentation, AuthorityRealm, AuthorityRealmKey, AuthorityService,
    AuthorizationDecision, BootstrapRealm, CapabilityRegistrationId, Cell, CellImmutable,
    ChallengeId, CommandState, DecideChallenge, EvaluateAuthority, EvaluationState,
    FederationPermission, GetCapabilityRegistrationById, IssueAuthorityGrant, MykoApplication,
    Obligation, Ordering, Principal, PrincipalId, PutCapability, PutDelegation, PutObligation,
    ReplicationSelection, ResourceClaim, ResourceClaimKind, RevocationKind, RevokeAuthorityFact,
    ScopeId, ScopeSelection, ScopeTopology, SubscriptionGuard, Utc, authority_presentation,
    authority_realm_scope, deny, evaluate, fmt, load_state, requires_durable_evaluation,
};

/// Policy backed exclusively by the local projection of [`AuthorityService`].
/// Replicated copies of authority entities are never consulted.
#[derive(Clone)]
pub struct AuthorityPolicy {
    pub(super) application: ApplicationHost,
    pub(super) retained: Option<ApplicationHost>,
    pub(super) realm_id: AuthorityRealmKey,
    pub(super) facts: Option<Arc<AuthorityFactSources>>,
    revision: Arc<AtomicU64>,
    revision_cell: Cell<u64, CellImmutable>,
    _revision_guards: Option<Arc<Vec<SubscriptionGuard>>>,
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
    pub fn new(application: ApplicationHost, realm_id: AuthorityRealmKey) -> Self {
        let source_node = application.node_id();
        let scope = authority_realm_scope(&realm_id);
        let retained = Some(application.clone());
        let facts = retained
            .as_ref()
            .and_then(|retained| AuthorityFactSources::open(retained, source_node, &scope).ok())
            .map(Arc::new);
        let revision = Arc::new(AtomicU64::new(0));
        let revision_writer = Cell::new(0_u64).with_name("myko.authority.revision");
        let revision_cell = revision_writer.clone().lock();
        let revision_guards = facts
            .as_ref()
            .map(|facts| Arc::new(facts.subscribe_revision(&revision, &revision_writer)));
        Self {
            application,
            retained,
            realm_id,
            facts,
            revision,
            revision_cell,
            _revision_guards: revision_guards,
        }
    }

    /// Adds the authority service to a composed application.
    ///
    /// # Errors
    /// Returns a registration conflict from the application builder.
    pub fn install(application: MykoApplication) -> Result<MykoApplication, AppError> {
        Ok(application.with_framework_service::<AuthorityService>())
    }

    /// The only unauthorised mutation: create one previously absent realm and
    /// its bounded administrator grant. The command rejects every replay.
    ///
    /// # Errors
    ///
    /// Returns an error when the realm exists or durable bootstrap fails.
    pub fn bootstrap(&self, principal: Principal) -> Result<AuthorityRealm, AppError> {
        let presentation = authority_presentation(&self.application);
        self.application.exec_trusted_framework_command(
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
            IssueAuthorityGrant {
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
        let mut request = AccessAttempt::scoped(
            authenticated.id,
            presentation.clone(),
            AccessOperation::DelegateAuthority,
            delegation.selections.first().map_or_else(
                || authority_realm_scope(&self.realm_id),
                |selection| selection.root().clone(),
            ),
        );
        request.target = AccessTarget::ScopeSet(delegation.selections.clone());
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
        self.application.exec_trusted_framework_command(
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
        application: &ApplicationHost,
    ) -> Result<(), AppError> {
        Self::validate_authenticated_presentation(&authenticated, &presentation)?;
        for capability in application.authority_capabilities().cloned() {
            let existing = self.application.node().query_items_in(
                self.application.node_id(),
                &authority_realm_scope(&self.realm_id),
                GetCapabilityRegistrationById {
                    id: CapabilityRegistrationId::from(capability.id.as_str()),
                },
            )?;
            match existing.into_iter().next() {
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

    fn evaluate(&self, request: AccessAttempt) -> AuthorizationDecision {
        let now = Utc::now();
        let scope = authority_realm_scope(&self.realm_id);
        let authoritative_through = self
            .application
            .node()
            .authoritative_position_in::<AuthorityService>(&scope)
            .ok()
            .flatten();
        let state = self.current_state(&scope, authoritative_through);
        let Some(mut state) = state else {
            return deny(
                &request,
                now,
                "authority_projection_not_current",
                "authoritative authority projection is not current",
            )
            .decision;
        };
        if let Some(topology) = &request.topology {
            state.topology.clone_from(topology);
        }
        let outcome = evaluate(&state, &request, now);
        if request.authorization_phase == myko_federation::AuthorizationPhase::Continuation {
            return outcome.decision;
        }
        if !requires_durable_evaluation(&state, &request, &outcome) {
            return outcome.decision;
        }
        let request_for_error = request.clone();
        let topology_proof = request
            .topology
            .as_ref()
            .map_or_else(ScopeTopology::default, |topology| {
                topology.proof_for(&request.scope_selections())
            });
        let presentation = authority_presentation(&self.application);
        self.application
            .exec_trusted_framework_command(
                presentation,
                EvaluateAuthority {
                    realm_id: self.realm_id.clone(),
                    request,
                    topology_proof,
                    now,
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

    pub(super) fn current_state(
        &self,
        scope: &ScopeId,
        authoritative_through: Option<myko_federation::LogPosition>,
    ) -> Option<EvaluationState> {
        let retained = self.retained.as_ref()?;
        let facts = self.facts.as_ref()?;
        let deadline = std::time::Instant::now().checked_add(std::time::Duration::from_secs(2))?;
        while !retained.source_selection_is_current(
            Some(self.application.node_id()),
            scope,
            authoritative_through,
        ) {
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::yield_now();
        }
        let facts = facts.snapshot(&self.realm_id);
        self.application
            .node()
            .scope_topology()
            .ok()
            .map(|topology| facts.with_topology(topology))
    }
}

impl AccessPolicy for AuthorityPolicy {
    fn authorize(&self, request: &AccessAttempt) -> Result<(), String> {
        match self.decide(request) {
            AuthorizationDecision::Permit(_) => Ok(()),
            decision => Err(decision.public_message()),
        }
    }

    fn decide(&self, request: &AccessAttempt) -> AuthorizationDecision {
        self.evaluate(request.clone())
    }

    fn revision_cell(&self) -> Option<Cell<u64, CellImmutable>> {
        self.facts.as_ref().map(|_| self.revision_cell.clone())
    }

    #[allow(clippy::too_many_lines)] // Intersection and one-shot consumption form one audit unit.
    fn constrain_replication(
        &self,
        request: &AccessAttempt,
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
                    ScopeSelection::Subtree(root) => std::iter::once(selection.clone())
                        .chain(
                            std::iter::once(root.clone())
                                .chain(topology.descendants(root))
                                .map(ScopeSelection::Exact),
                        )
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
                .filter(|event| event.event.service_id() == Some(service))
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
                candidate.target = request.service_id().map_or_else(
                    || AccessTarget::Scope(selection.root().clone()),
                    |service_id| AccessTarget::ServiceScope {
                        service_id: service_id.clone(),
                        scope_id: selection.root().clone(),
                    },
                );
                candidate.resource_claims = vec![ResourceClaim {
                    selection: selection.clone(),
                    kind: ResourceClaimKind::Primary,
                    source_node: request
                        .resource_claims
                        .first()
                        .and_then(|claim| claim.source_node),
                    service_id: request.service_id().cloned(),
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
            return Err(deny(
                request,
                now,
                "replication_no_authorized_scopes",
                "no requested history scopes are authorized for this peer; pairing does not grant replication access",
            ).decision);
        }
        let mut scoped = request.clone();
        scoped.target = AccessTarget::ScopeSet(authorized.clone());
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
                service_id: request.service_id().cloned(),
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
                &AccessAttempt::scoped(
                    authenticated_executor.clone(),
                    presentation.clone(),
                    AccessOperation::ApproveAuthority,
                    authority_realm_scope(&self.realm_id),
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
            .exec_trusted_framework_command(
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
                    &AccessAttempt::scoped(
                        authenticated_executor.clone(),
                        presentation.clone(),
                        AccessOperation::ApproveAuthority,
                        authority_realm_scope(&self.realm_id),
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
                        &AccessAttempt::scoped(
                            authenticated_executor.clone(),
                            presentation.clone(),
                            AccessOperation::ApproveAuthority,
                            authority_realm_scope(&self.realm_id),
                        ),
                        Utc::now(),
                        "approval_pending_command_failed",
                        &error.to_string(),
                    )
                    .decision
                })?
                .ok_or_else(|| {
                    deny(
                        &AccessAttempt::scoped(
                            authenticated_executor.clone(),
                            presentation.clone(),
                            AccessOperation::ApproveAuthority,
                            authority_realm_scope(&self.realm_id),
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
            let command_target = AccessTarget::KnownCommand {
                command_id: pending.request.id,
                service_id: pending.request.service_id.clone(),
                scope_id: pending.request.scope_id.clone(),
                command_type: pending.request.command_type.clone(),
                principal_id: pending.request.principal_id.clone(),
            };
            let mut command_presentation = pending.request.authority;
            for approval_id in approvals {
                if !command_presentation.approvals.contains(approval_id) {
                    command_presentation.approvals.push(approval_id.clone());
                }
            }
            if !command_presentation.approvals.contains(&decision.id) {
                command_presentation.approvals.push(decision.id.clone());
            }
            let topology = self
                .application
                .node()
                .scope_topology()
                .and_then(|mut topology| {
                    topology.merge_proof(&binding.topology_proof)?;
                    Ok(topology)
                })
                .map_err(|error| {
                    deny(
                        &AccessAttempt::scoped(
                            authenticated_executor.clone(),
                            presentation.clone(),
                            AccessOperation::ApproveAuthority,
                            authority_realm_scope(&self.realm_id),
                        ),
                        Utc::now(),
                        "approval_topology_failed",
                        &error.to_string(),
                    )
                    .decision
                })?;
            let effect_request = AccessAttempt {
                principal_id: binding.executor.id.clone(),
                presentation: command_presentation,
                operation: binding.operation,
                target: command_target,
                resource_claims: binding.resources.clone(),
                application_capabilities: binding.capabilities.clone(),
                arguments_digest: binding.arguments_digest.clone(),
                effect_digest: binding.effect_digest.clone(),
                lease: None,
                authorization_phase: myko_federation::AuthorizationPhase::Effect,
                topology: Some(topology),
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
                    &AccessAttempt::scoped(
                        authenticated_executor.clone(),
                        presentation.clone(),
                        AccessOperation::ApproveAuthority,
                        authority_realm_scope(&self.realm_id),
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
