use std::{error::Error, sync::Arc};

use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use myko::{
    ApplicationHost, MykoApplication,
    server::{
        AuthorityControlEndpoint, AuthorityControlFuture, AuthorityControlProposeRequest,
        RetainedEvidenceError, RetainedEvidenceFuture, ScopedRetainedEvidenceEndpoint,
    },
};
use myko_authority::{
    AuthorityPolicy, authority_realm_scope,
    certified::{
        AuthorityAnchor, AuthorityController, AuthorityControllerPrincipal,
        AuthorityCoordinatorPeer, AuthorityDecisionCoordinator, AuthorityDecisionRoot,
        AuthorityHistory, AuthorityRequestSource, AuthoritySelection,
        CertifiedAuthorityControlEndpoint, CertifiedAuthorityRequest, CoordinatedAuthorityDecision,
    },
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, AccessTarget, AllowAllAccessPolicy,
    AuthorityConstraints, AuthorityGrant, AuthorityGrantId, AuthorityPresentation,
    AuthorityRealmId, AuthorityUnavailable, AuthorizationDecision, AuthorizationFailure,
    AuthorizationPhase, BatchId, ChangeBatch, CommandAdmission, CommandId, CommandRequest,
    EventEnvelope, FederationPermission, FrameworkControlEvent, MykoService as _, Node, NodeEvent,
    PreparedCommandEffect, Principal, PrincipalId, PrincipalKind, ReplicationSelection,
    ResourceClaim, ResourceClaimKind, ScopeId, ScopeSelection, ScopeTopology, ServiceId,
    control_quorum::{
        ControlBallot, ControlEpochId, ControlHead, ControlValue, ControllerId,
        SignedControlProposal, SignedControlVote,
    },
};
use myko_iroh::{EndpointAddr, IrohReplicator, IrohScopedEvidenceEndpoint, endpoint_principal_id};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn keys() -> [SigningKey; 2] {
    [1, 2].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn keys3() -> [SigningKey; 3] {
    [1, 2, 3].map(|seed| SigningKey::from_bytes(&[seed; 32]))
}

fn controller_id(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn realm() -> AuthorityRealmId {
    AuthorityRealmId::new("certified-coordinator")
}

fn anchor() -> Result<AuthorityAnchor, String> {
    let [a_key, b_key] = keys();
    anchor_for(vec![controller_id(&a_key), controller_id(&b_key)])
}

fn anchor3() -> Result<AuthorityAnchor, String> {
    let [a_key, b_key, c_key] = keys3();
    anchor_for(vec![
        controller_id(&a_key),
        controller_id(&b_key),
        controller_id(&c_key),
    ])
}

fn anchor_for(controllers: Vec<ControllerId>) -> Result<AuthorityAnchor, String> {
    AuthorityAnchor::new(
        realm(),
        ControlEpochId([8; 32]),
        ControlHead([9; 32]),
        controllers,
    )
}

fn authority_events(node: &Node) -> Result<Vec<EventEnvelope>, Box<dyn Error>> {
    let realm_scope = authority_realm_scope(&realm());
    let authority_service = ServiceId::new(myko_authority::AuthorityService::SERVICE_ID);
    Ok(node
        .events_after(None)?
        .into_iter()
        .filter(|event| match &event.event {
            NodeEvent::CommandLifecycle(command) | NodeEvent::CommandCommitted { command, .. } => {
                command.request.service_id == authority_service
                    && command.request.scope_id == realm_scope
            }
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)) => {
                proposal.message.slot.realm == realm_scope
            }
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote)) => {
                vote.message.slot.realm == realm_scope
            }
            NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(_)) => {
                false
            }
        })
        .collect())
}

fn sync_authority(a: &Node, b: &Node) -> TestResult {
    let a_events = authority_events(a)?;
    let b_events = authority_events(b)?;
    for event in a_events {
        b.ingest(event)?;
    }
    for event in b_events {
        a.ingest(event)?;
    }
    Ok(())
}

fn choose_selection_with_anchor(
    a: &Node,
    b: &Node,
    selected_anchor: AuthorityAnchor,
    a_key: &SigningKey,
    b_key: &SigningKey,
    predecessor: ControlHead,
    value: &ControlValue,
) -> Result<ControlHead, Box<dyn Error>> {
    sync_authority(a, b)?;
    let context = AuthorityHistory::replay(a, selected_anchor.clone())?.context_at(predecessor)?;
    let verifier = context.verifier()?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller_id(a_key),
    };
    let a_controller = AuthorityController::new(a.clone(), selected_anchor.clone());
    let b_controller = AuthorityController::new(b.clone(), selected_anchor);
    let promises = vec![
        a_controller.prepare(predecessor, ballot, a_key)?,
        b_controller.prepare(predecessor, ballot, b_key)?,
    ];
    let proposal = a_controller.propose(predecessor, ballot, &promises, value, a_key)?;
    let accepts = vec![
        a_controller.accept(predecessor, &proposal, a_key)?,
        b_controller.accept(predecessor, &proposal, b_key)?,
    ];
    let chosen = verifier
        .verify_prepare(ballot, &promises)?
        .verify_chosen(value, &accepts)?
        .head()?;
    sync_authority(a, b)?;
    Ok(chosen)
}

fn command_request(reader: Principal, scope: ScopeId, command_id: CommandId) -> CommandRequest {
    CommandRequest {
        id: command_id,
        service_id: ServiceId::new("test.service"),
        scope_id: scope.clone(),
        principal_id: reader.id.clone(),
        authority: AuthorityPresentation::direct(reader),
        resource_claims: vec![ResourceClaim::scope(scope, ResourceClaimKind::Primary)],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "certified-coordinator-command".to_owned(),
        payload: b"certified coordinator prepared effect".to_vec(),
    }
}

fn prepare_command_evidence(
    a: &Node,
    b: &Node,
    reader: Principal,
    scope: ScopeId,
    command_id: CommandId,
) -> Result<CertifiedAuthorityRequest, Box<dyn Error>> {
    let (request, events) = prepare_command_evidence_at(a, reader, scope, command_id)?;
    for event in events {
        b.ingest(event)?;
    }
    Ok(request)
}

fn prepare_command_evidence_at(
    a: &Node,
    reader: Principal,
    scope: ScopeId,
    command_id: CommandId,
) -> Result<(CertifiedAuthorityRequest, Vec<EventEnvelope>), Box<dyn Error>> {
    let policy = Arc::new(AllowAllAccessPolicy);
    a.set_command_access_policy(policy.clone())?;
    let before = a.local_history_cut()?;
    a.submit(command_request(reader, scope, command_id))?;
    let snapshot = match a.claim(command_id)? {
        CommandAdmission::Execute(snapshot) | CommandAdmission::Resume(snapshot) => snapshot,
    };
    let batch = ChangeBatch {
        id: BatchId::new(),
        command_id,
        service_id: snapshot.request.service_id.clone(),
        scope_id: snapshot.request.scope_id.clone(),
        causal_parents: vec![snapshot.updated_at],
        changes: Vec::new(),
    };
    let resource_claims = snapshot.request.resource_claims.clone();
    let application_capabilities = snapshot.request.application_capabilities;
    let effect = PreparedCommandEffect::new(
        snapshot.updated_at,
        batch,
        b"prepared effect result".to_vec(),
        resource_claims,
        application_capabilities,
        ScopeTopology::default(),
    )?;
    a.prepare_authorization(command_id, effect)?;
    let events = a.events_after(before)?;
    let source = AuthorityRequestSource::new(a.clone());
    let request = source.prepared_command_request(command_id)?;
    drop(policy);
    Ok((request, events))
}

fn install_grant(a: &Node, b: &Node) -> Result<(ControlHead, Principal, ScopeId), Box<dyn Error>> {
    let [a_key, b_key] = keys();
    install_grant_with_anchor(a, b, &anchor()?, &a_key, &b_key)
}

fn install_grant_with_anchor(
    a: &Node,
    b: &Node,
    selected_anchor: &AuthorityAnchor,
    a_key: &SigningKey,
    b_key: &SigningKey,
) -> Result<(ControlHead, Principal, ScopeId), Box<dyn Error>> {
    let app = AuthorityPolicy::install(MykoApplication::new())?;
    let policy = Arc::new(AuthorityPolicy::new(
        ApplicationHost::new(a.clone(), app)?,
        realm(),
    ));
    a.set_command_access_policy(policy.clone())?;
    let admin = Principal::new(PrincipalId::new("admin"), PrincipalKind::Node);
    let reader = Principal::new(PrincipalId::new("reader"), PrincipalKind::Node);
    let scope = ScopeId::new("coordinator:data");
    policy.bootstrap(admin.clone())?;
    policy.issue_grant(
        admin.clone(),
        AuthorityPresentation::direct(admin.clone()),
        AuthorityGrant {
            id: AuthorityGrantId::new("single-use"),
            realm_id: realm(),
            grantor: admin,
            grantee: reader.clone(),
            selection: ScopeSelection::Exact(scope.clone()),
            permissions: vec![FederationPermission::ReadState, FederationPermission::Write],
            operations: vec![AccessOperation::ReadItems, AccessOperation::SubmitCommand],
            capabilities: Vec::new(),
            constraints: AuthorityConstraints::default(),
            obligations: Vec::new(),
            valid_from: Utc::now()
                .checked_sub_signed(Duration::seconds(10))
                .ok_or("time underflow")?,
            expires_at: None,
            max_uses: Some(1),
        },
    )?;
    let selected = authority_events(a)?;
    let value = AuthoritySelection::new(CommandId::new(), &selected)?.control_value()?;
    let head = choose_selection_with_anchor(
        a,
        b,
        selected_anchor.clone(),
        a_key,
        b_key,
        selected_anchor.genesis(),
        &value,
    )?;
    drop(policy);
    Ok((head, reader, scope))
}

fn endpoint(
    node: Node,
    key: SigningKey,
    caller: AuthorityControllerPrincipal,
    max_skew: i64,
) -> Result<CertifiedAuthorityControlEndpoint, String> {
    CertifiedAuthorityControlEndpoint::new(node, anchor()?, key, vec![caller])
        .map(|endpoint| endpoint.with_max_evaluation_skew_seconds(max_skew))
}

#[derive(Debug)]
struct ScopedHistoryPolicy {
    principal_id: PrincipalId,
    scopes: Vec<ScopeId>,
}

impl ScopedHistoryPolicy {
    const fn new(principal_id: PrincipalId, scopes: Vec<ScopeId>) -> Self {
        Self {
            principal_id,
            scopes,
        }
    }

    fn permits(&self, request: &AccessAttempt) -> bool {
        request.principal_id == self.principal_id
            && request.operation == AccessOperation::ReadHistory
            && matches!(
                &request.target,
                AccessTarget::History(ReplicationSelection::Scopes(selections))
                    if selections.iter().all(|selection| self.covers(selection))
            )
    }

    fn covers(&self, selection: &ScopeSelection) -> bool {
        matches!(
            selection,
            ScopeSelection::Exact(scope) if self.scopes.iter().any(|allowed| allowed == scope)
        )
    }
}

impl AccessPolicy for ScopedHistoryPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let result = self
            .permits(request)
            .then_some(())
            .ok_or_else(|| "scope history is not granted to this peer".to_owned());
        Ok(AuthorizationDecision::from_rule(request, result))
    }
}

#[derive(Debug)]
struct InvalidEvidence;

impl ScopedRetainedEvidenceEndpoint for InvalidEvidence {
    fn refresh_scopes<'a>(&'a self, _scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async {
            Err(RetainedEvidenceError::Invalid(
                "retained evidence is malformed".to_owned(),
            ))
        })
    }
}

#[derive(Debug)]
struct UnavailableEvidence;

impl ScopedRetainedEvidenceEndpoint for UnavailableEvidence {
    fn refresh_scopes<'a>(&'a self, _scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async {
            Err(RetainedEvidenceError::Unavailable(
                AuthorityUnavailable::HistoryUnavailable,
            ))
        })
    }
}

#[derive(Debug)]
struct UnavailableControlEndpoint;

impl AuthorityControlEndpoint for UnavailableControlEndpoint {
    fn prepare<'a>(
        &'a self,
        _principal: &'a PrincipalId,
        _presentation: &'a AuthorityPresentation,
        _head: ControlHead,
        _ballot: ControlBallot,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async {
            Err(AuthorizationFailure::Unavailable(
                AuthorityUnavailable::CoordinationUnavailable,
            ))
        })
    }

    fn propose<'a>(
        &'a self,
        _principal: &'a PrincipalId,
        _presentation: &'a AuthorityPresentation,
        _request: AuthorityControlProposeRequest,
    ) -> AuthorityControlFuture<'a, SignedControlProposal> {
        Box::pin(async {
            Err(AuthorizationFailure::Unavailable(
                AuthorityUnavailable::CoordinationUnavailable,
            ))
        })
    }

    fn accept<'a>(
        &'a self,
        _principal: &'a PrincipalId,
        _presentation: &'a AuthorityPresentation,
        _head: ControlHead,
        _proposal: SignedControlProposal,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async {
            Err(AuthorizationFailure::Unavailable(
                AuthorityUnavailable::CoordinationUnavailable,
            ))
        })
    }
}

fn add_unrelated_canary(node: &Node, label: &str) -> Result<CommandId, Box<dyn Error>> {
    let command_id = CommandId::new();
    let policy = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    node.submit(command_request(
        Principal::node(PrincipalId::new(format!("canary:{label}"))),
        ScopeId::new(format!("coordinator:unrelated:{label}")),
        command_id,
    ))?;
    drop(policy);
    Ok(command_id)
}

fn coordinator(a: &Node, b: &Node) -> Result<AuthorityDecisionCoordinator, Box<dyn Error>> {
    let [a_key, b_key] = keys();
    let a_principal = Principal::node(PrincipalId::new("node:controller-a"));
    let a_binding = AuthorityControllerPrincipal::new(a_principal.clone(), controller_id(&a_key));
    let callers = vec![a_binding.clone()];
    let peers = vec![
        AuthorityCoordinatorPeer::local(
            a.clone(),
            anchor()?,
            a_key,
            a_principal.clone(),
            callers.clone(),
        )?,
        AuthorityCoordinatorPeer::local(b.clone(), anchor()?, b_key, a_principal, callers)?,
    ];
    Ok(AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        a_binding,
        peers,
    )?)
}

fn coordinator_with_unavailable_minority(
    a: &Node,
    b: &Node,
    evidence: Arc<dyn ScopedRetainedEvidenceEndpoint>,
) -> Result<AuthorityDecisionCoordinator, Box<dyn Error>> {
    let [a_key, b_key, c_key] = keys3();
    let selected_anchor = anchor3()?;
    let a_principal = Principal::node(PrincipalId::new("node:controller-a"));
    let a_binding = AuthorityControllerPrincipal::new(a_principal.clone(), controller_id(&a_key));
    let callers = vec![a_binding.clone()];
    let peers = vec![
        AuthorityCoordinatorPeer::local(
            a.clone(),
            selected_anchor.clone(),
            a_key,
            a_principal.clone(),
            callers.clone(),
        )?,
        AuthorityCoordinatorPeer::local(
            b.clone(),
            selected_anchor.clone(),
            b_key,
            a_principal.clone(),
            callers,
        )?,
        AuthorityCoordinatorPeer::new(
            Arc::new(UnavailableControlEndpoint),
            a_principal,
            controller_id(&c_key),
            realm(),
        )
        .with_observer_evidence_endpoint(evidence),
    ];
    Ok(AuthorityDecisionCoordinator::new(
        selected_anchor,
        a.clone(),
        a_binding,
        peers,
    )?)
}

fn coordinator_with_unavailable_majority(
    a: &Node,
) -> Result<AuthorityDecisionCoordinator, Box<dyn Error>> {
    let [a_key, b_key, c_key] = keys3();
    let selected_anchor = anchor3()?;
    let a_principal = Principal::node(PrincipalId::new("node:controller-a"));
    let a_binding = AuthorityControllerPrincipal::new(a_principal.clone(), controller_id(&a_key));
    let callers = vec![a_binding.clone()];
    let peers = vec![
        AuthorityCoordinatorPeer::local(
            a.clone(),
            selected_anchor.clone(),
            a_key,
            a_principal.clone(),
            callers,
        )?,
        AuthorityCoordinatorPeer::new(
            Arc::new(UnavailableControlEndpoint),
            a_principal.clone(),
            controller_id(&b_key),
            realm(),
        ),
        AuthorityCoordinatorPeer::new(
            Arc::new(UnavailableControlEndpoint),
            a_principal,
            controller_id(&c_key),
            realm(),
        ),
    ];
    Ok(AuthorityDecisionCoordinator::new(
        selected_anchor,
        a.clone(),
        a_binding,
        peers,
    )?)
}

struct NativeControlHarness {
    a_transport: IrohReplicator,
    b_transport: IrohReplicator,
    coordinator_transport: IrohReplicator,
    a_binding: AuthorityControllerPrincipal,
    a_principal: Principal,
    selected_anchor: AuthorityAnchor,
}

impl NativeControlHarness {
    async fn start(
        a: Node,
        b: Node,
        authority_scope: ScopeId,
        command_scope: ScopeId,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_with_anchor(a, b, authority_scope, command_scope, anchor()?).await
    }

    async fn start_with_anchor(
        a: Node,
        b: Node,
        authority_scope: ScopeId,
        command_scope: ScopeId,
        selected_anchor: AuthorityAnchor,
    ) -> Result<Self, Box<dyn Error>> {
        let a_transport = IrohReplicator::bind_loopback(a.clone()).await?;
        let b_transport = IrohReplicator::bind_loopback(b.clone()).await?;
        let coordinator_transport = IrohReplicator::bind_loopback(Node::in_memory()).await?;
        let a_transport_id = endpoint_principal_id(a_transport.address().id);
        let b_transport_id = endpoint_principal_id(b_transport.address().id);
        a_transport.set_access_policy(Arc::new(ScopedHistoryPolicy::new(
            b_transport_id,
            vec![authority_scope.clone(), command_scope],
        )))?;
        b_transport.set_access_policy(Arc::new(ScopedHistoryPolicy::new(
            a_transport_id,
            vec![authority_scope],
        )))?;
        let [a_key, b_key] = keys();
        let a_principal =
            Principal::node(endpoint_principal_id(coordinator_transport.address().id));
        let a_binding =
            AuthorityControllerPrincipal::new(a_principal.clone(), controller_id(&a_key));
        a_transport.sessions().set_authority_control(Some(Arc::new(
            CertifiedAuthorityControlEndpoint::new(
                a.clone(),
                selected_anchor.clone(),
                a_key,
                vec![a_binding.clone()],
            )?,
        )))?;
        b_transport.sessions().set_authority_control(Some(Arc::new(
            CertifiedAuthorityControlEndpoint::new(
                b.clone(),
                selected_anchor.clone(),
                b_key,
                vec![a_binding.clone()],
            )?
            .with_scoped_evidence_endpoint(Arc::new(IrohScopedEvidenceEndpoint::new(
                b_transport.clone(),
                a_transport.address(),
            ))),
        )))?;
        Ok(Self {
            a_transport,
            b_transport,
            coordinator_transport,
            a_binding,
            a_principal,
            selected_anchor,
        })
    }

    fn peers(&self) -> Vec<AuthorityCoordinatorPeer> {
        let [a_key, b_key] = keys();
        vec![
            AuthorityCoordinatorPeer::new(
                Arc::new(
                    self.coordinator_transport
                        .command_client(self.a_transport.address()),
                ),
                self.a_principal.clone(),
                controller_id(&a_key),
                realm(),
            ),
            AuthorityCoordinatorPeer::new(
                Arc::new(
                    self.coordinator_transport
                        .command_client(self.b_transport.address()),
                ),
                self.a_principal.clone(),
                controller_id(&b_key),
                realm(),
            )
            .with_observer_evidence_endpoint(Arc::new(
                IrohScopedEvidenceEndpoint::new(
                    self.a_transport.clone(),
                    self.b_transport.address(),
                ),
            )),
        ]
    }

    async fn decide(
        &self,
        observer: &Node,
        grant_head: ControlHead,
        counter: u64,
        operation: CommandId,
        command_id: CommandId,
        request: CertifiedAuthorityRequest,
    ) -> Result<CoordinatedAuthorityDecision, Box<dyn Error>> {
        Ok(AuthorityDecisionCoordinator::new(
            self.selected_anchor.clone(),
            observer.clone(),
            self.a_binding.clone(),
            self.peers(),
        )?
        .decide(grant_head, counter, operation, command_id, request)
        .await?)
    }

    fn coordinator_with_extra_peer(
        &self,
        observer: &Node,
        remote: EndpointAddr,
        controller: ControllerId,
    ) -> Result<AuthorityDecisionCoordinator, String> {
        let mut peers = self.peers();
        peers.push(
            AuthorityCoordinatorPeer::new(
                Arc::new(
                    self.coordinator_transport
                        .command_client(remote.clone())
                        .with_control_request_timeout(std::time::Duration::from_secs(1)),
                ),
                self.a_principal.clone(),
                controller,
                realm(),
            )
            .with_observer_evidence_endpoint(Arc::new(
                IrohScopedEvidenceEndpoint::new(self.a_transport.clone(), remote)
                    .with_request_timeout(std::time::Duration::from_secs(1)),
            )),
        );
        AuthorityDecisionCoordinator::new(
            self.selected_anchor.clone(),
            observer.clone(),
            self.a_binding.clone(),
            peers,
        )
    }

    async fn shutdown(self) -> TestResult {
        self.coordinator_transport.shutdown().await?;
        self.a_transport.shutdown().await?;
        self.b_transport.shutdown().await?;
        Ok(())
    }
}

#[tokio::test]
async fn coordinator_recovers_chosen_decision_from_old_predecessor() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (grant_head, reader, scope) = install_grant(&a, &b)?;
    let request_id = CommandId::new();
    let operation = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, request_id)?;
    let chosen = coordinator(&a, &b)?
        .decide(grant_head, 2, operation, request_id, request.clone())
        .await?;
    if !chosen.decision().is_permit() {
        return Err("coordinated authority decision did not permit first use".into());
    }
    let first_head = chosen.head();
    drop(a);
    drop(b);

    let reopened_a = RedbJournal::open_node(&a_path)?;
    let reopened_b = RedbJournal::open_node(&b_path)?;
    let retried = coordinator(&reopened_a, &reopened_b)?
        .decide(grant_head, 3, operation, request_id, request)
        .await?;
    if retried.head() != first_head {
        return Err("retry did not recover the originally chosen decision head".into());
    }
    let root = AuthorityDecisionRoot::new(realm(), request_id, AuthorizationPhase::Effect)?;
    if AuthorityHistory::replay(&reopened_a, anchor()?)?
        .decision_at(first_head, &root)?
        .is_none()
    {
        return Err("coordinated decision was not retained after reopen".into());
    }
    Ok(())
}

#[tokio::test]
async fn coordinator_decides_with_unavailable_minority_and_recovers_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("ha-a.redb");
    let b_path = directory.path().join("ha-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let [a_key, b_key, _c_key] = keys3();
    let selected_anchor = anchor3()?;
    let (grant_head, reader, scope) =
        install_grant_with_anchor(&a, &b, &selected_anchor, &a_key, &b_key)?;
    let request_id = CommandId::new();
    let operation = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, request_id)?;
    let coordinator = coordinator_with_unavailable_minority(&a, &b, Arc::new(UnavailableEvidence))?;
    let chosen = coordinator
        .decide(grant_head, 2, operation, request_id, request.clone())
        .await?;
    if !chosen.decision().is_permit() {
        return Err("healthy quorum did not permit the prepared single-use effect".into());
    }
    let first_head = chosen.head();
    let first_transition = chosen.transition().clone();
    drop(coordinator);
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let retried = coordinator_with_unavailable_minority(&a, &b, Arc::new(UnavailableEvidence))?
        .decide(grant_head, 4, operation, request_id, request)
        .await?;
    if retried.head() != first_head || retried.transition() != &first_transition {
        return Err("retry did not recover the original chosen decision".into());
    }
    let root = AuthorityDecisionRoot::new(realm(), request_id, AuthorizationPhase::Effect)?;
    if AuthorityHistory::replay(&a, anchor3()?)?
        .decision_at(first_head, &root)?
        .is_none()
    {
        return Err("healthy quorum decision was not retained durably".into());
    }
    drop(a);
    drop(b);
    drop(directory);
    Ok(())
}

#[tokio::test]
async fn coordinator_rejects_invalid_minority_evidence_before_voting() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("invalid-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("invalid-b.redb"))?;
    let [a_key, b_key, _c_key] = keys3();
    let (grant_head, reader, scope) =
        install_grant_with_anchor(&a, &b, &anchor3()?, &a_key, &b_key)?;
    let request_id = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, request_id)?;
    let before = a.events_after(None)?;
    let result = coordinator_with_unavailable_minority(&a, &b, Arc::new(InvalidEvidence))?
        .decide(grant_head, 2, CommandId::new(), request_id, request)
        .await;
    if result.is_ok() || a.events_after(None)? != before {
        return Err("invalid evidence must fail before the coordinator starts a vote".into());
    }
    Ok(())
}

#[tokio::test]
async fn coordinator_rejects_when_unavailable_controllers_leave_no_majority() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("insufficient-a.redb");
    let b_path = directory.path().join("insufficient-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let [a_key, b_key, _c_key] = keys3();
    let selected_anchor = anchor3()?;
    let (grant_head, reader, scope) =
        install_grant_with_anchor(&a, &b, &selected_anchor, &a_key, &b_key)?;
    let request_id = CommandId::new();
    let operation = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, request_id)?;
    let result = coordinator_with_unavailable_majority(&a)?
        .decide(grant_head, 2, operation, request_id, request)
        .await;
    if result.is_ok() {
        return Err("coordinator chose a decision without controller majority".into());
    }
    drop(a);
    drop(b);
    drop(directory);
    Ok(())
}

fn expired_value(
    a: &Node,
    grant_head: ControlHead,
    command_id: CommandId,
    request: &CertifiedAuthorityRequest,
) -> Result<ControlValue, Box<dyn Error>> {
    let old_time = Utc::now()
        .checked_sub_signed(Duration::seconds(2))
        .ok_or("time underflow")?;
    AuthorityHistory::replay(a, anchor()?)?
        .plan_decision_at(
            grant_head,
            CommandId::new(),
            command_id,
            request.request().clone(),
            old_time,
            request.topology().clone(),
        )?
        .control_value()
        .map_err(Into::into)
}

async fn endpoint_promises(
    a_endpoint: &CertifiedAuthorityControlEndpoint,
    b_endpoint: &CertifiedAuthorityControlEndpoint,
    a_principal: &Principal,
    presentation: &AuthorityPresentation,
    grant_head: ControlHead,
    ballot: ControlBallot,
) -> Result<Vec<SignedControlVote>, Box<dyn Error>> {
    Ok(vec![
        a_endpoint
            .prepare(&a_principal.id, presentation, grant_head, ballot)
            .await
            .map_err(|failure| format!("{failure:?}"))?,
        b_endpoint
            .prepare(&a_principal.id, presentation, grant_head, ballot)
            .await
            .map_err(|failure| format!("{failure:?}"))?,
    ])
}

struct EndpointQuorum<'a> {
    a_endpoint: &'a CertifiedAuthorityControlEndpoint,
    b_endpoint: &'a CertifiedAuthorityControlEndpoint,
    principal: &'a Principal,
    presentation: &'a AuthorityPresentation,
    grant_head: ControlHead,
}

async fn reject_new_expired_value(
    endpoint: &CertifiedAuthorityControlEndpoint,
    principal: &Principal,
    presentation: &AuthorityPresentation,
    grant_head: ControlHead,
    ballot: ControlBallot,
    promises: Vec<SignedControlVote>,
    value: ControlValue,
) -> TestResult {
    let result = endpoint
        .propose(
            &principal.id,
            presentation,
            AuthorityControlProposeRequest {
                head: grant_head,
                ballot,
                promises,
                value,
            },
        )
        .await;
    if !matches!(result, Err(AuthorizationFailure::Deny(_))) {
        return Err("endpoint allowed a new expired authority decision".into());
    }
    Ok(())
}

fn persist_raw_accepted_value(
    a: &Node,
    b: &Node,
    grant_head: ControlHead,
    value: &ControlValue,
    a_key: &SigningKey,
    b_key: &SigningKey,
) -> TestResult {
    let raw_ballot = ControlBallot {
        counter: 3,
        proposer: controller_id(a_key),
    };
    let a_controller = AuthorityController::new(a.clone(), anchor()?);
    let b_controller = AuthorityController::new(b.clone(), anchor()?);
    let raw_promises = vec![
        a_controller.prepare(grant_head, raw_ballot, a_key)?,
        b_controller.prepare(grant_head, raw_ballot, b_key)?,
    ];
    let raw_proposal = a_controller.propose(grant_head, raw_ballot, &raw_promises, value, a_key)?;
    a_controller.accept(grant_head, &raw_proposal, a_key)?;
    b_controller.accept(grant_head, &raw_proposal, b_key)?;
    Ok(())
}

async fn recover_accepted_value(
    quorum: &EndpointQuorum<'_>,
    ballot: ControlBallot,
    promises: Vec<SignedControlVote>,
    value: ControlValue,
) -> TestResult {
    let proposal = quorum
        .a_endpoint
        .propose(
            &quorum.principal.id,
            quorum.presentation,
            AuthorityControlProposeRequest {
                head: quorum.grant_head,
                ballot,
                promises,
                value,
            },
        )
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    quorum
        .b_endpoint
        .accept(
            &quorum.principal.id,
            quorum.presentation,
            quorum.grant_head,
            proposal,
        )
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    Ok(())
}

#[tokio::test]
async fn endpoint_rejects_new_expired_decision_but_recovers_accepted_one() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (grant_head, reader, scope) = install_grant(&a, &b)?;
    let command_id = CommandId::new();
    let request = prepare_command_evidence(&a, &b, reader, scope, command_id)?;
    let old_value = expired_value(&a, grant_head, command_id, &request)?;
    let [a_key, b_key] = keys();
    let a_principal = Principal::node(PrincipalId::new("node:controller-a"));
    let a_binding = AuthorityControllerPrincipal::new(a_principal.clone(), controller_id(&a_key));
    let a_endpoint = endpoint(a.clone(), a_key.clone(), a_binding.clone(), 0)?;
    let b_endpoint = endpoint(b.clone(), b_key.clone(), a_binding, 0)?;
    let presentation = AuthorityPresentation::direct(a_principal.clone());
    let fresh_ballot = ControlBallot {
        counter: 2,
        proposer: controller_id(&a_key),
    };
    let fresh_promises = endpoint_promises(
        &a_endpoint,
        &b_endpoint,
        &a_principal,
        &presentation,
        grant_head,
        fresh_ballot,
    )
    .await?;
    reject_new_expired_value(
        &a_endpoint,
        &a_principal,
        &presentation,
        grant_head,
        fresh_ballot,
        fresh_promises,
        old_value.clone(),
    )
    .await?;
    persist_raw_accepted_value(&a, &b, grant_head, &old_value, &a_key, &b_key)?;
    let recovery_ballot = ControlBallot {
        counter: 4,
        proposer: controller_id(&a_key),
    };
    let recovery_promises = endpoint_promises(
        &a_endpoint,
        &b_endpoint,
        &a_principal,
        &presentation,
        grant_head,
        recovery_ballot,
    )
    .await?;
    let quorum = EndpointQuorum {
        a_endpoint: &a_endpoint,
        b_endpoint: &b_endpoint,
        principal: &a_principal,
        presentation: &presentation,
        grant_head,
    };
    recover_accepted_value(&quorum, recovery_ballot, recovery_promises, old_value).await?;
    drop(a);
    drop(b);
    drop(directory);
    Ok(())
}

#[tokio::test]
async fn endpoint_reports_retained_evidence_failures_as_unavailable() -> TestResult {
    let [a_key, _b_key] = keys();
    let principal = Principal::node(PrincipalId::new("node:controller-a"));
    let presentation = AuthorityPresentation::direct(principal.clone());
    let binding = AuthorityControllerPrincipal::new(principal.clone(), controller_id(&a_key));
    let endpoint = CertifiedAuthorityControlEndpoint::new(
        Node::in_memory(),
        anchor()?,
        a_key.clone(),
        vec![binding],
    )?
    .with_scoped_evidence_endpoint(Arc::new(InvalidEvidence));
    let failure = endpoint
        .prepare(
            &principal.id,
            &presentation,
            anchor()?.genesis(),
            ControlBallot {
                counter: 1,
                proposer: controller_id(&a_key),
            },
        )
        .await
        .err()
        .ok_or("endpoint prepared despite invalid retained evidence")?;
    if failure != AuthorizationFailure::Unavailable(AuthorityUnavailable::HistoryUnavailable) {
        return Err("retained evidence failure was exposed as an authorization denial".into());
    }
    Ok(())
}

#[tokio::test]
async fn native_coordinator_survives_controller_shutdown_and_store_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("native-ha-a.redb");
    let b_path = directory.path().join("native-ha-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let c = RedbJournal::open_node(directory.path().join("native-ha-c.redb"))?;
    let [a_key, b_key, c_key] = keys3();
    let selected_anchor = anchor3()?;
    let (grant_head, reader, scope) =
        install_grant_with_anchor(&a, &b, &selected_anchor, &a_key, &b_key)?;
    for event in authority_events(&a)? {
        c.ingest(event)?;
    }
    let command_id = CommandId::new();
    let (request, _) = prepare_command_evidence_at(&a, reader, scope.clone(), command_id)?;
    let operation = CommandId::new();
    let authority_scope = authority_realm_scope(&realm());
    let harness = NativeControlHarness::start_with_anchor(
        a.clone(),
        b.clone(),
        authority_scope.clone(),
        scope.clone(),
        selected_anchor.clone(),
    )
    .await?;
    let c_transport = IrohReplicator::bind_loopback(c.clone()).await?;
    let c_address = c_transport.address();
    c_transport.sessions().set_authority_control(Some(Arc::new(
        CertifiedAuthorityControlEndpoint::new(
            c.clone(),
            selected_anchor.clone(),
            c_key.clone(),
            vec![harness.a_binding.clone()],
        )?,
    )))?;
    let c_client = harness
        .coordinator_transport
        .command_client(c_address.clone());
    let promise = c_client
        .prepare(
            &harness.a_principal.id,
            &AuthorityPresentation::direct(harness.a_principal.clone()),
            grant_head,
            ControlBallot {
                counter: 2,
                proposer: controller_id(&a_key),
            },
        )
        .await
        .map_err(|error| format!("third controller was not reachable: {error:?}"))?;
    if promise.message.controller != controller_id(&c_key) {
        return Err("third native controller returned the wrong signer".into());
    }
    c_transport.shutdown().await?;
    drop(c_client);
    drop(c);
    assert_native_peer_unavailable(&harness, c_address.clone(), grant_head).await?;
    let coordinator =
        harness.coordinator_with_extra_peer(&a, c_address.clone(), controller_id(&c_key))?;
    let chosen = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        coordinator.decide(grant_head, 3, operation, command_id, request),
    )
    .await??;
    if !chosen.decision().is_permit() || b.command(command_id)?.is_none() {
        return Err("native majority did not synchronize and certify the prepared effect".into());
    }
    let chosen_head = chosen.head();
    let chosen_transition = chosen.transition().clone();
    drop(coordinator);
    harness.shutdown().await?;
    drop(a);
    drop(b);

    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let request = AuthorityRequestSource::new(a.clone()).prepared_command_request(command_id)?;
    let harness = NativeControlHarness::start_with_anchor(
        a.clone(),
        b.clone(),
        authority_scope,
        scope,
        selected_anchor.clone(),
    )
    .await?;
    let coordinator = harness.coordinator_with_extra_peer(&a, c_address, controller_id(&c_key))?;
    let recovered = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        coordinator.decide(grant_head, 5, operation, command_id, request),
    )
    .await??;
    if recovered.head() != chosen_head || recovered.transition() != &chosen_transition {
        return Err("native minority-loss retry changed or spent the chosen decision".into());
    }
    drop(coordinator);
    harness.shutdown().await?;
    Ok(())
}

async fn assert_native_peer_unavailable(
    harness: &NativeControlHarness,
    remote: EndpointAddr,
    head: ControlHead,
) -> TestResult {
    let [a_key, _b_key] = keys();
    let client = harness
        .coordinator_transport
        .command_client(remote.clone())
        .with_control_request_timeout(std::time::Duration::from_millis(100));
    let result = client
        .prepare(
            &harness.a_principal.id,
            &AuthorityPresentation::direct(harness.a_principal.clone()),
            head,
            ControlBallot {
                counter: 3,
                proposer: controller_id(&a_key),
            },
        )
        .await;
    if !matches!(
        result,
        Err(AuthorizationFailure::Unavailable(
            AuthorityUnavailable::CoordinationUnavailable,
        ))
    ) {
        return Err("native control timeout did not report typed unavailability".into());
    }
    let evidence = IrohScopedEvidenceEndpoint::new(harness.a_transport.clone(), remote)
        .with_request_timeout(std::time::Duration::from_millis(100));
    let result = evidence
        .refresh_scopes(&[authority_realm_scope(&realm())])
        .await;
    if !matches!(
        result,
        Err(RetainedEvidenceError::Unavailable(
            AuthorityUnavailable::HistoryUnavailable,
        ))
    ) {
        return Err("native evidence timeout did not report typed unavailability".into());
    }
    Ok(())
}

#[tokio::test]
async fn native_coordinator_uses_iroh_control_and_scoped_evidence_replication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("native-a.redb");
    let b_path = directory.path().join("native-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (grant_head, reader, scope) = install_grant(&a, &b)?;
    let command_id = CommandId::new();
    let (request, prepared_events) =
        prepare_command_evidence_at(&a, reader, scope.clone(), command_id)?;
    let a_canary = add_unrelated_canary(&a, "a")?;
    let b_canary = add_unrelated_canary(&b, "b")?;
    if prepared_events.is_empty() || b.command(command_id)?.is_some() {
        return Err("prepared command evidence was not isolated on the source node".into());
    }
    let authority_scope = authority_realm_scope(&realm());
    let operation = CommandId::new();
    let harness =
        NativeControlHarness::start(a.clone(), b.clone(), authority_scope.clone(), scope.clone())
            .await?;
    let chosen = harness
        .decide(&a, grant_head, 2, operation, command_id, request)
        .await?;
    if !chosen.decision().is_permit() {
        return Err("native coordinated decision did not permit the prepared effect".into());
    }
    let chosen_head = chosen.head();
    let chosen_transition = chosen.transition().clone();
    if b.command(command_id)?.is_none() {
        return Err("native evidence sync did not replicate prepared command evidence".into());
    }
    if b.command(a_canary)?.is_some() || a.command(b_canary)?.is_some() {
        return Err("native evidence sync copied unrelated command history".into());
    }
    let [_a_key, b_key] = keys();
    if !a.events_after(None)?.iter().any(|event| {
        matches!(
            &event.event,
            NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote))
                if vote.message.controller == controller_id(&b_key)
                    && vote.message.ballot.counter >= 2
        )
    }) {
        return Err("native evidence sync did not replicate the remote control vote".into());
    }
    harness.shutdown().await?;
    drop(a);
    drop(b);

    let reopened_a = RedbJournal::open_node(&a_path)?;
    let reopened_b = RedbJournal::open_node(&b_path)?;
    let root = AuthorityDecisionRoot::new(realm(), command_id, AuthorizationPhase::Effect)?;
    let retained = AuthorityHistory::replay(&reopened_a, anchor()?)?
        .decision_at(chosen_head, &root)?
        .ok_or("native chosen decision was not retained after transport and store restart")?;
    if retained != chosen_transition {
        return Err("native chosen decision changed after restart".into());
    }
    let retry_request =
        AuthorityRequestSource::new(reopened_a.clone()).prepared_command_request(command_id)?;
    let recovery_harness = NativeControlHarness::start(
        reopened_a.clone(),
        reopened_b.clone(),
        authority_scope,
        scope,
    )
    .await?;
    let recovered = recovery_harness
        .decide(
            &reopened_a,
            grant_head,
            4,
            operation,
            command_id,
            retry_request,
        )
        .await?;
    if recovered.head() != chosen_head || recovered.transition() != &chosen_transition {
        return Err("native retry spent or changed the certified decision".into());
    }
    recovery_harness.shutdown().await?;
    drop(reopened_a);
    drop(reopened_b);
    drop(directory);
    Ok(())
}
