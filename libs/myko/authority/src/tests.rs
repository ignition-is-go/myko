use std::sync::Arc;

use myko_federation::{
    AccessPolicy as _, AuthorityDelegation, DelegationParent, ObligationId, PrincipalKind,
    ProvenanceHop, ProvenanceOperation, ServiceId,
};

use super::*;

fn node_principal(value: &str) -> Principal {
    Principal::new(PrincipalId::new(value), PrincipalKind::Node)
}

fn open(node: Node) -> Result<(ApplicationHost, Arc<AuthorityPolicy>, Principal), String> {
    let application =
        AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
    let application = ApplicationHost::new(node.clone(), application)?;
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

fn request(principal: Principal, scope: &str, operation: AccessOperation) -> AccessAttempt {
    let mut request = AccessAttempt::scoped(
        principal.id.clone(),
        AuthorityPresentation::direct(principal),
        operation,
        ScopeId::new(scope),
    );
    request.target = AccessTarget::ServiceScope {
        service_id: ServiceId::new("test.service"),
        scope_id: ScopeId::new(scope),
    };
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

fn wait_for_grant_record(
    view: &myko::view::TypedViewCellMap<GrantRecord>,
    grant_id: &str,
    revoked: bool,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if view.snapshot().iter().any(|(_, record)| {
            record.grant.id.as_str() == grant_id && record.revoked_at.is_some() == revoked
        }) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "grant view did not observe {grant_id} with revoked={revoked}"
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[test]
fn retained_authority_facts_open_at_the_authoritative_frontier() -> Result<(), String> {
    let node = Node::in_memory();
    let application =
        AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
    let application = ApplicationHost::new(node, application)?;
    let policy = Arc::new(AuthorityPolicy::new(
        application,
        AuthorityRealmKey::new("main"),
    ));
    assert!(policy.retained.is_some(), "retained host did not open");
    assert!(
        policy.facts.is_some(),
        "retained authority facts did not open"
    );
    assert!(
        policy
            .retained
            .as_ref()
            .is_some_and(|host| host.source_selection_is_current(
                Some(policy.application.node_id()),
                &authority_realm_scope(&policy.realm_id),
                None,
            ))
    );
    let installed: Arc<dyn AccessPolicy> = policy.clone();
    policy
        .application
        .node()
        .set_command_access_policy(installed)
        .map_err(|error| error.to_string())?;
    let administrator = node_principal("node:administrator");
    policy
        .bootstrap(administrator)
        .map_err(|error| error.to_string())?;
    let scope = authority_realm_scope(&policy.realm_id);
    let through = policy
        .application
        .node()
        .authoritative_position_in::<AuthorityService>(&scope)
        .map_err(|error| error.to_string())?;
    let state = policy
        .current_state(&scope, through)
        .ok_or_else(|| "retained authority state did not become current".to_owned())?;
    assert!(state.realm.is_some(), "retained authority realm is absent");
    Ok(())
}

#[test]
fn authority_grants_view_keeps_revoked_records_live() -> Result<(), String> {
    let (application, policy, administrator) = open(Node::in_memory())?;
    let retained = ApplicationHost::new(
        application.node().clone(),
        MykoApplication::builder()
            .service::<AuthorityService>()
            .build(),
    )?;
    let view = retained.watch_view_at(
        Some(application.node_id()),
        Some(authority_realm_scope(&AuthorityRealmKey::new("main"))),
        &AuthorityGrantsView {
            source_node: application.node_id(),
            realm_id: AuthorityRealmKey::new("main"),
        },
    )?;
    let issued = grant(
        "grant:native-view",
        node_principal("node:peer"),
        "scope:agents",
        vec![FederationPermission::ReadState],
        vec![AccessOperation::ReadItems],
    );
    put_grant(&policy, &administrator, issued)?;

    wait_for_grant_record(&view, "grant:native-view", false)?;

    policy
        .revoke(
            administrator.clone(),
            AuthorityPresentation::direct(administrator),
            RevocationKind::Grant,
            "grant:native-view".to_owned(),
        )
        .map_err(|error| error.to_string())?;
    wait_for_grant_record(&view, "grant:native-view", true)?;
    Ok(())
}

#[test]
fn default_deny_and_bootstrap_is_one_time() -> Result<(), String> {
    let node = Node::in_memory();
    let application =
        AuthorityPolicy::install(MykoApplication::new()).map_err(|error| error.to_string())?;
    let application = ApplicationHost::new(node.clone(), application)?;
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
        ApplicationHost::new(application.node().clone(), declared_application)?;
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
fn prospective_nested_scope_is_authorized_only_by_its_proven_parent() -> Result<(), String> {
    let (_application, policy, administrator) = open(Node::in_memory())?;
    let user = node_principal("node:nested-writer");
    let parent = ScopeId::new("scope:parent");
    let child = ScopeId::new("scope:child");
    let unrelated = ScopeId::new("scope:unrelated");
    let mut subtree_grant = grant(
        "nested-write",
        user.clone(),
        parent.as_str(),
        vec![FederationPermission::Write],
        vec![AccessOperation::SubmitCommand],
    );
    subtree_grant.selection = ScopeSelection::Subtree(parent.clone());
    put_grant(&policy, &administrator, subtree_grant)?;

    let mut primary = ResourceClaim::scope(parent.clone(), ResourceClaimKind::Primary);
    primary
        .required_permissions
        .push(FederationPermission::Write);
    primary
        .required_operations
        .push(AccessOperation::SubmitCommand);
    let mut affected = ResourceClaim::scope(child.clone(), ResourceClaimKind::Affected);
    affected
        .required_permissions
        .push(FederationPermission::Write);
    affected
        .required_operations
        .push(AccessOperation::SubmitCommand);

    let mut admission = request(user, parent.as_str(), AccessOperation::SubmitCommand);
    admission.resource_claims = vec![primary, affected];
    admission.target = AccessTarget::ScopeSet(vec![
        ScopeSelection::Exact(parent.clone()),
        ScopeSelection::Exact(child.clone()),
    ]);
    assert!(policy.decide(&admission).is_permit());

    let mut effect = admission;
    effect.authorization_phase = myko_federation::AuthorizationPhase::Effect;
    assert!(matches!(
        policy.decide(&effect),
        AuthorizationDecision::Deny(_)
    ));

    let proven_topology = serde_json::from_value::<ScopeTopology>(serde_json::json!({
        "parents": { (child.as_str()): parent.as_str() },
        "known": [parent.as_str(), child.as_str()]
    }))
    .map_err(|error| error.to_string())?;
    effect.topology = Some(proven_topology);
    assert!(policy.decide(&effect).is_permit());

    let unrelated_topology = serde_json::from_value::<ScopeTopology>(serde_json::json!({
        "parents": { (child.as_str()): unrelated.as_str() },
        "known": [parent.as_str(), child.as_str(), unrelated.as_str()]
    }))
    .map_err(|error| error.to_string())?;
    effect.topology = Some(unrelated_topology);
    assert!(matches!(
        policy.decide(&effect),
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
        let node = myko_redb::RedbJournal::open_node(&path).map_err(|error| error.to_string())?;
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
        ApplicationHost::new(node, application)?,
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
                &ReplicationSelection::Scopes(vec![ScopeSelection::Exact(
                    ScopeId::new("scope:a",)
                )]),
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
    use myko::server::FederatedSession;
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
    let session = FederatedSession::for_application(application.clone(), access_policy);
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
    wrong_scope.presentation =
        AuthorityPresentation::direct(offline_user.clone()).with_lease(offline_lease.id.clone());
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
    reconnect.presentation =
        AuthorityPresentation::direct(offline_user.clone()).with_lease(offline_lease.id.clone());
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
fn effect_challenges_park_exact_batch_and_multiple_approvals_commit_once() -> Result<(), String> {
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
    let realm = authority_realm_scope(&AuthorityRealmKey::new("main"));
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
