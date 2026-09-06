use std::{collections::BTreeMap, fmt, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::SigningKey;
use myko::server::{
    AuthorityControlEndpoint as MykoAuthorityControlEndpoint, AuthorityControlFuture,
    AuthorityControlProposeRequest, RetainedEvidenceError, ScopedRetainedEvidenceEndpoint,
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityPresentation, AuthorityUnavailable,
    AuthorizationBinding, AuthorizationDecision, AuthorizationExplanation, AuthorizationFailure,
    AuthorizationPhase, AuthorizationReport, CommandId, ControlTransition, EventEnvelope,
    FrameworkControlEvent, MykoService as _, Node, NodeEvent, Principal, PrincipalId,
    ResourceVisibility, ScopeId, ScopeSelection, ScopeTopology, ServiceId,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, ControlVoteKind, ControllerId,
        SignedControlProposal, SignedControlVote,
    },
};

use crate::{AuthorityRealmKey, AuthorityService, authority_realm_scope};

use super::{
    AuthorityAnchor, AuthorityController, AuthorityDecisionRoot, AuthorityDecisionTransition,
    AuthorityHistory,
};

const DEFAULT_MAX_COORDINATION_ROUNDS: usize = 8;
const DEFAULT_MAX_EVALUATION_SKEW_SECONDS: i64 = 300;

/// Framework-owned source for evidence used by certified coordination.
#[derive(Clone)]
pub struct AuthorityRequestSource {
    node: Node,
}

impl AuthorityRequestSource {
    #[must_use]
    pub const fn new(node: Node) -> Self {
        Self { node }
    }

    /// Build a request using topology derived from dependency-complete node history.
    ///
    /// This ignores `request.topology`. Transport input cannot authenticate
    /// topology merely by carrying bytes, even though serde skips that field.
    ///
    /// # Errors
    /// Returns an error when the node cannot derive trusted topology.
    pub fn current_request(
        &self,
        request: AccessAttempt,
    ) -> Result<CertifiedAuthorityRequest, String> {
        Ok(Self::trusted_request(
            request,
            self.node
                .scope_topology()
                .map_err(|error| error.to_string())?,
        ))
    }

    /// Build an effect request from a retained prepared command.
    ///
    /// # Errors
    /// Rejects unknown commands, unprepared commands, corrupt prepared effects
    /// or retained requests that are not in effect phase.
    pub fn prepared_command_request(
        &self,
        command_id: CommandId,
    ) -> Result<CertifiedAuthorityRequest, String> {
        let mut request = self
            .node
            .prepared_command_access(command_id)
            .map_err(|error| error.to_string())?;
        if request.authorization_phase != AuthorizationPhase::Effect {
            return Err("prepared command request must use effect phase".to_owned());
        }
        let topology = request
            .topology
            .take()
            .ok_or_else(|| "prepared command request is missing trusted topology".to_owned())?;
        Ok(Self::trusted_request(request, topology))
    }

    fn trusted_request(
        mut request: AccessAttempt,
        topology: ScopeTopology,
    ) -> CertifiedAuthorityRequest {
        request.topology = None;
        CertifiedAuthorityRequest {
            request,
            evaluated_at: Utc::now(),
            topology,
        }
    }
}

/// Request evidence captured before certified coordination begins.
///
/// This is still an input to certification, not a permit. The live caller must
/// verify the returned decision against the exact effect it will release and
/// any use-time lease or expiry rule.
#[derive(Debug, Clone)]
pub struct CertifiedAuthorityRequest {
    request: AccessAttempt,
    evaluated_at: DateTime<Utc>,
    topology: ScopeTopology,
}

impl CertifiedAuthorityRequest {
    #[must_use]
    pub const fn request(&self) -> &AccessAttempt {
        &self.request
    }

    #[must_use]
    pub const fn evaluated_at(&self) -> &DateTime<Utc> {
        &self.evaluated_at
    }

    #[must_use]
    pub const fn topology(&self) -> &ScopeTopology {
        &self.topology
    }

    fn root(
        &self,
        realm: &myko_federation::AuthorityRealmId,
        request_id: CommandId,
    ) -> Result<AuthorityDecisionRoot, String> {
        AuthorityDecisionRoot::new(realm.clone(), request_id, self.request.authorization_phase)
    }

    fn binding(&self) -> AuthorizationBinding {
        let mut request = self.request.clone();
        request.topology = Some(self.topology.clone());
        AuthorizationBinding::from_request(&request)
    }
}

/// Authenticated principal bound to one control key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityControllerPrincipal {
    principal: Principal,
    controller: ControllerId,
}

impl AuthorityControllerPrincipal {
    #[must_use]
    pub const fn new(principal: Principal, controller: ControllerId) -> Self {
        Self {
            principal,
            controller,
        }
    }
}

/// Concrete authority endpoint installed on controller sessions.
pub struct CertifiedAuthorityControlEndpoint {
    node: Node,
    anchor: AuthorityAnchor,
    controller: AuthorityController,
    key: SigningKey,
    controller_id: ControllerId,
    callers: BTreeMap<PrincipalId, AuthorityControllerPrincipal>,
    inbound_evidence: Option<Arc<dyn ScopedRetainedEvidenceEndpoint>>,
    max_evaluation_skew_seconds: i64,
}

impl CertifiedAuthorityControlEndpoint {
    /// Create an endpoint backed by one durable local controller key.
    ///
    /// # Errors
    /// Rejects an empty or duplicate authenticated-caller map.
    pub fn new(
        node: Node,
        anchor: AuthorityAnchor,
        key: SigningKey,
        callers: Vec<AuthorityControllerPrincipal>,
    ) -> Result<Self, String> {
        let mut indexed = BTreeMap::new();
        for caller in callers {
            let id = caller.principal.id.clone();
            if indexed.insert(id, caller).is_some() {
                return Err("authority control caller principal is duplicated".to_owned());
            }
        }
        if indexed.is_empty() {
            return Err("authority control endpoint requires callers".to_owned());
        }
        let controller_id = ControllerId(key.verifying_key().to_bytes());
        Ok(Self {
            node: node.clone(),
            anchor: anchor.clone(),
            controller: AuthorityController::new(node, anchor),
            key,
            controller_id,
            callers: indexed,
            inbound_evidence: None,
            max_evaluation_skew_seconds: DEFAULT_MAX_EVALUATION_SKEW_SECONDS,
        })
    }

    #[must_use]
    pub const fn with_max_evaluation_skew_seconds(mut self, seconds: i64) -> Self {
        self.max_evaluation_skew_seconds = seconds;
        self
    }

    /// Attach the local controller's authenticated scoped-history refresh path.
    #[must_use]
    pub fn with_scoped_evidence_endpoint(
        mut self,
        endpoint: Arc<dyn ScopedRetainedEvidenceEndpoint>,
    ) -> Self {
        self.inbound_evidence = Some(endpoint);
        self
    }

    fn authorize(
        &self,
        authenticated: &PrincipalId,
        presentation: &AuthorityPresentation,
        proposer: ControllerId,
    ) -> Result<(), AuthorizationFailure> {
        if &presentation.executor.id != authenticated {
            return Err(control_denial(
                presentation,
                "authority executor does not match authenticated controller".to_owned(),
            ));
        }
        if presentation.principal != presentation.executor || !presentation.provenance.is_empty() {
            return Err(control_denial(
                presentation,
                "authority control requests must use a direct controller principal".to_owned(),
            ));
        }
        let Some(caller) = self.callers.get(authenticated) else {
            return Err(control_denial(
                presentation,
                "authenticated principal is not an authority controller".to_owned(),
            ));
        };
        if caller.principal != presentation.executor {
            return Err(control_denial(
                presentation,
                "authenticated controller principal kind does not match binding".to_owned(),
            ));
        }
        if caller.controller != proposer {
            return Err(control_denial(
                presentation,
                "authenticated controller does not match proposal ballot".to_owned(),
            ));
        }
        Ok(())
    }

    async fn refresh_authority_evidence(
        &self,
        _presentation: &AuthorityPresentation,
    ) -> Result<(), AuthorizationFailure> {
        let Some(evidence) = &self.inbound_evidence else {
            return Ok(());
        };
        let scopes = [authority_realm_scope(self.anchor.realm_id())];
        evidence
            .refresh_scopes(&scopes)
            .await
            .map_err(|error| evidence_failure(&error))
    }

    async fn refresh_command_evidence(
        &self,
        _presentation: &AuthorityPresentation,
        request: &AccessAttempt,
    ) -> Result<(), AuthorizationFailure> {
        let Some(evidence) = &self.inbound_evidence else {
            return Ok(());
        };
        let scopes = command_scopes_from_attempt(request);
        if scopes.is_empty() {
            return Ok(());
        }
        evidence
            .refresh_scopes(&scopes)
            .await
            .map_err(|error| evidence_failure(&error))
    }

    async fn validate_proposed_value(
        &self,
        presentation: &AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
    ) -> Result<(), AuthorizationFailure> {
        let Some(decision) = decision_transition(presentation, value)? else {
            return Ok(());
        };
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        let verifier = history
            .context_at(head)
            .and_then(|context| context.verifier())
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        let prepared = verifier.verify_prepare(ballot, promises).map_err(|_| {
            control_denial_for_message(presentation, "authority proposal prepare proof is invalid")
        })?;
        if let Some(required) = required_accepted_value(promises) {
            if required != value {
                return Err(control_denial_for_message(
                    presentation,
                    "authority proposal differs from the required accepted value",
                ));
            }
            return history.validate_transition_at(head, value).map_err(|_| {
                control_denial_for_message(
                    presentation,
                    "required authority proposal is not valid certified history",
                )
            });
        }
        let planned = self
            .planned_live_value(presentation, &history, head, &decision)
            .await?;
        if prepared.select_value(planned) != *value {
            return Err(control_denial_for_message(
                presentation,
                "authority proposal does not match trusted prepared command evidence",
            ));
        }
        Ok(())
    }

    async fn planned_live_value(
        &self,
        presentation: &AuthorityPresentation,
        history: &AuthorityHistory,
        head: ControlHead,
        decision: &AuthorityDecisionTransition,
    ) -> Result<ControlValue, AuthorizationFailure> {
        self.validate_evaluation_time(presentation, decision.evaluated_at())?;
        self.refresh_command_evidence(presentation, decision.request())
            .await?;
        let mut request = self
            .node
            .prepared_command_access(decision.root().request_id())
            .map_err(|_| AuthorityUnavailable::CoordinationUnavailable)?;
        if request.authorization_phase != AuthorizationPhase::Effect {
            return Err(control_denial_for_message(
                presentation,
                "certified live decisions require prepared effect phase",
            ));
        }
        let topology = request.topology.take().ok_or_else(|| {
            control_denial_for_message(
                presentation,
                "prepared command evidence is missing trusted topology",
            )
        })?;
        history
            .plan_decision_at(
                head,
                decision.operation(),
                decision.root().request_id(),
                request,
                *decision.evaluated_at(),
                topology,
            )
            .and_then(|planned| planned.control_value())
            .map_err(|_| {
                control_denial_for_message(
                    presentation,
                    "authority proposal does not match prepared command evaluation",
                )
            })
    }

    fn validate_evaluation_time(
        &self,
        presentation: &AuthorityPresentation,
        evaluated_at: &DateTime<Utc>,
    ) -> Result<(), AuthorizationFailure> {
        let now = Utc::now();
        let earliest = now
            .checked_sub_signed(Duration::seconds(self.max_evaluation_skew_seconds))
            .ok_or(AuthorityUnavailable::CoordinationUnavailable)?;
        let latest = now
            .checked_add_signed(Duration::seconds(60))
            .ok_or(AuthorityUnavailable::CoordinationUnavailable)?;
        if evaluated_at < &earliest || evaluated_at > &latest {
            return Err(control_denial_for_message(
                presentation,
                "authority proposal evaluation time is outside the controller window",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for CertifiedAuthorityControlEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CertifiedAuthorityControlEndpoint")
            .field("controller_id", &self.controller_id)
            .field("callers", &self.callers)
            .field("has_inbound_evidence", &self.inbound_evidence.is_some())
            .finish_non_exhaustive()
    }
}

impl MykoAuthorityControlEndpoint for CertifiedAuthorityControlEndpoint {
    fn prepare<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            self.authorize(principal, presentation, ballot.proposer)?;
            self.refresh_authority_evidence(presentation).await?;
            self.controller
                .prepare(head, ballot, &self.key)
                .map_err(controller_failure)
        })
    }

    fn propose<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        request: AuthorityControlProposeRequest,
    ) -> AuthorityControlFuture<'a, SignedControlProposal> {
        Box::pin(async move {
            self.authorize(principal, presentation, request.ballot.proposer)?;
            self.refresh_authority_evidence(presentation).await?;
            self.validate_proposed_value(
                presentation,
                request.head,
                request.ballot,
                &request.promises,
                &request.value,
            )
            .await?;
            self.controller
                .propose(
                    request.head,
                    request.ballot,
                    &request.promises,
                    &request.value,
                    &self.key,
                )
                .map_err(controller_failure)
        })
    }

    fn accept<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        proposal: SignedControlProposal,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            self.authorize(principal, presentation, proposal.message.ballot.proposer)?;
            self.refresh_authority_evidence(presentation).await?;
            self.validate_proposed_value(
                presentation,
                head,
                proposal.message.ballot,
                &proposal.message.prepare_votes,
                &proposal.message.value,
            )
            .await?;
            self.controller
                .accept(head, &proposal, &self.key)
                .map_err(controller_failure)
        })
    }
}

fn control_denial(presentation: &AuthorityPresentation, message: String) -> AuthorizationFailure {
    AuthorizationFailure::Deny(Box::new(myko_federation::DenyDecision {
        report: AuthorizationReport {
            evaluated_at: Utc::now(),
            principal: presentation.principal.clone(),
            executor: presentation.executor.clone(),
            operation: AccessOperation::AdministerAuthority,
            explanations: vec![AuthorizationExplanation {
                code: "authority_control_rejected".to_owned(),
                message,
                grant_id: None,
                delegation_id: None,
                obligation_id: None,
                constraint: None,
            }],
        },
        visibility: ResourceVisibility::Unauthorized,
    }))
}

fn control_denial_for_message(
    presentation: &AuthorityPresentation,
    message: &str,
) -> AuthorizationFailure {
    control_denial(presentation, message.to_owned())
}

fn controller_failure(_error: String) -> AuthorizationFailure {
    AuthorizationFailure::Unavailable(AuthorityUnavailable::CoordinationUnavailable)
}

fn evidence_failure(error: &RetainedEvidenceError) -> AuthorizationFailure {
    match error {
        RetainedEvidenceError::Unavailable(reason) => (*reason).into(),
        RetainedEvidenceError::Invalid(_) => AuthorityUnavailable::HistoryUnavailable.into(),
    }
}

fn control_failure_message(failure: AuthorizationFailure) -> String {
    match failure {
        AuthorizationFailure::Deny(denial) => denial.report.explanations.last().map_or_else(
            || "authority control request was denied".to_owned(),
            |explanation| explanation.message.clone(),
        ),
        AuthorizationFailure::Challenge { .. } => {
            "authority control request cannot require a challenge".to_owned()
        }
        AuthorizationFailure::Unavailable(reason) => reason.to_string(),
    }
}

fn decision_transition(
    presentation: &AuthorityPresentation,
    value: &ControlValue,
) -> Result<Option<AuthorityDecisionTransition>, AuthorizationFailure> {
    let transition = ControlTransition::from_control_value(value).map_err(|_| {
        control_denial_for_message(presentation, "authority control value is malformed")
    })?;
    let ControlTransition::Retain { payload, .. } = transition else {
        return Ok(None);
    };
    AuthorityDecisionTransition::from_retained_payload(&payload).map_err(|_| {
        control_denial_for_message(presentation, "authority decision payload is malformed")
    })
}

fn required_accepted_value(promises: &[SignedControlVote]) -> Option<&ControlValue> {
    promises
        .iter()
        .filter_map(|signed| {
            let ControlVoteKind::Promise {
                accepted: Some(accepted),
            } = &signed.message.vote
            else {
                return None;
            };
            Some((accepted.ballot, &accepted.value))
        })
        .max_by_key(|(ballot, _)| *ballot)
        .map(|(_, value)| value)
}

fn certified_events(node: &Node, realm: &AuthorityRealmKey) -> Result<Vec<EventEnvelope>, String> {
    let realm_scope = authority_realm_scope(realm);
    let authority_service = ServiceId::new(AuthorityService::SERVICE_ID);
    Ok(node
        .events_after(None)
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|event| is_certified_authority_event(event, &realm_scope, &authority_service))
        .collect())
}

fn is_certified_authority_event(
    event: &EventEnvelope,
    realm_scope: &myko_federation::ScopeId,
    authority_service: &ServiceId,
) -> bool {
    match &event.event {
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal)) => {
            &proposal.message.slot.realm == realm_scope
        }
        NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote)) => {
            &vote.message.slot.realm == realm_scope
        }
        NodeEvent::CommandLifecycle(command) | NodeEvent::CommandCommitted { command, .. } => {
            command.request.service_id == *authority_service
                && command.request.scope_id == *realm_scope
        }
        NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(_)) => false,
    }
}

/// One async controller endpoint used by the request coordinator.
#[derive(Clone)]
pub struct AuthorityCoordinatorPeer {
    endpoint: Arc<dyn MykoAuthorityControlEndpoint>,
    principal: Principal,
    controller_id: ControllerId,
    retained_node: Option<Node>,
    realm: AuthorityRealmKey,
    evidence: Option<Arc<dyn ScopedRetainedEvidenceEndpoint>>,
}

impl fmt::Debug for AuthorityCoordinatorPeer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorityCoordinatorPeer")
            .field("principal", &self.principal)
            .field("controller_id", &self.controller_id)
            .field("has_retained_node", &self.retained_node.is_some())
            .field("has_evidence_synchronizer", &self.evidence.is_some())
            .finish_non_exhaustive()
    }
}

impl AuthorityCoordinatorPeer {
    /// Build a peer from an installed async authority-control endpoint.
    #[must_use]
    pub fn new(
        endpoint: Arc<dyn MykoAuthorityControlEndpoint>,
        principal: Principal,
        controller_id: ControllerId,
        realm: AuthorityRealmKey,
    ) -> Self {
        Self {
            endpoint,
            principal,
            controller_id,
            retained_node: None,
            realm,
            evidence: None,
        }
    }

    /// Attach a retained-history source for local integration tests.
    ///
    /// Remote integrations should supply the same scoped events through the
    /// normal replication path before requiring a recovered historical result.
    #[must_use]
    pub fn with_retained_node(mut self, node: Node) -> Self {
        self.retained_node = Some(node);
        self
    }

    /// Attach authenticated retained-evidence replication for this peer.
    #[must_use]
    pub fn with_observer_evidence_endpoint(
        mut self,
        evidence: Arc<dyn ScopedRetainedEvidenceEndpoint>,
    ) -> Self {
        self.evidence = Some(evidence);
        self
    }

    /// Build an in-process endpoint and peer for one durable controller.
    ///
    /// # Errors
    /// Rejects invalid endpoint caller configuration.
    pub fn local(
        node: Node,
        anchor: AuthorityAnchor,
        key: SigningKey,
        principal: Principal,
        callers: Vec<AuthorityControllerPrincipal>,
    ) -> Result<Self, String> {
        let controller_id = ControllerId(key.verifying_key().to_bytes());
        let realm = anchor.realm_id().clone();
        let endpoint = Arc::new(CertifiedAuthorityControlEndpoint::new(
            node.clone(),
            anchor,
            key,
            callers,
        )?);
        Ok(Self {
            endpoint,
            principal,
            controller_id,
            retained_node: Some(node),
            realm,
            evidence: None,
        })
    }

    async fn synchronize_evidence(
        &self,
        authority_scope: &ScopeId,
    ) -> Result<(), RetainedEvidenceError> {
        let Some(evidence) = &self.evidence else {
            return Ok(());
        };
        let scopes = [authority_scope.clone()];
        evidence.refresh_scopes(&scopes).await
    }

    #[must_use]
    pub const fn controller_id(&self) -> ControllerId {
        self.controller_id
    }

    async fn prepare(
        &self,
        caller: &Principal,
        head: ControlHead,
        ballot: ControlBallot,
    ) -> Result<SignedControlVote, AuthorizationFailure> {
        self.endpoint
            .prepare(
                &caller.id,
                &AuthorityPresentation::direct(caller.clone()),
                head,
                ballot,
            )
            .await
    }

    async fn propose(
        &self,
        caller: &Principal,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
    ) -> Result<SignedControlProposal, AuthorizationFailure> {
        self.endpoint
            .propose(
                &caller.id,
                &AuthorityPresentation::direct(caller.clone()),
                AuthorityControlProposeRequest {
                    head,
                    ballot,
                    promises: promises.to_vec(),
                    value: value.clone(),
                },
            )
            .await
    }

    async fn accept(
        &self,
        caller: &Principal,
        head: ControlHead,
        proposal: &SignedControlProposal,
    ) -> Result<SignedControlVote, AuthorizationFailure> {
        self.endpoint
            .accept(
                &caller.id,
                &AuthorityPresentation::direct(caller.clone()),
                head,
                proposal.clone(),
            )
            .await
    }

    fn retained_events(&self) -> Result<Vec<EventEnvelope>, String> {
        let Some(node) = &self.retained_node else {
            return Ok(Vec::new());
        };
        certified_events(node, &self.realm)
    }

    fn ingest(&self, event: EventEnvelope) -> Result<(), String> {
        let Some(node) = &self.retained_node else {
            return Ok(());
        };
        node.ingest(event)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub type LocalAuthorityPeer = AuthorityCoordinatorPeer;

/// Certified result of one coordinated authority decision.
#[derive(Debug, Clone)]
pub struct CoordinatedAuthorityDecision {
    predecessor: ControlHead,
    head: ControlHead,
    transition: AuthorityDecisionTransition,
    proposal: SignedControlProposal,
    promises: Vec<SignedControlVote>,
    accepts: Vec<SignedControlVote>,
}

impl CoordinatedAuthorityDecision {
    #[must_use]
    pub const fn predecessor(&self) -> ControlHead {
        self.predecessor
    }

    #[must_use]
    pub const fn head(&self) -> ControlHead {
        self.head
    }

    #[must_use]
    pub const fn transition(&self) -> &AuthorityDecisionTransition {
        &self.transition
    }

    #[must_use]
    pub const fn decision(&self) -> &AuthorizationDecision {
        self.transition.decision()
    }

    #[must_use]
    pub const fn proposal(&self) -> &SignedControlProposal {
        &self.proposal
    }

    #[must_use]
    pub fn promises(&self) -> &[SignedControlVote] {
        &self.promises
    }

    #[must_use]
    pub fn accepts(&self) -> &[SignedControlVote] {
        &self.accepts
    }
}

/// Coordinates one request-specific authority value through local controllers.
pub struct AuthorityDecisionCoordinator {
    anchor: AuthorityAnchor,
    observer: Node,
    proposer: AuthorityControllerPrincipal,
    peers: Vec<AuthorityCoordinatorPeer>,
    max_rounds: usize,
}

impl AuthorityDecisionCoordinator {
    /// Build a coordinator over durable local controller peers.
    ///
    /// # Errors
    /// Rejects an empty peer set.
    pub fn new(
        anchor: AuthorityAnchor,
        observer: Node,
        proposer: AuthorityControllerPrincipal,
        peers: Vec<AuthorityCoordinatorPeer>,
    ) -> Result<Self, String> {
        if peers.is_empty() {
            return Err("authority coordinator requires at least one peer".to_owned());
        }
        if peers
            .iter()
            .all(|peer| peer.controller_id() != proposer.controller)
        {
            return Err("authority coordinator proposer endpoint is not configured".to_owned());
        }
        Ok(Self {
            anchor,
            observer,
            proposer,
            peers,
            max_rounds: DEFAULT_MAX_COORDINATION_ROUNDS,
        })
    }

    #[must_use]
    pub const fn with_max_rounds(mut self, max_rounds: usize) -> Self {
        self.max_rounds = max_rounds;
        self
    }

    /// Choose or recover the request-specific authority decision after `head`.
    ///
    /// If prepare discovers a previously accepted value, the coordinator first
    /// chooses that value. If it belongs to another request, the coordinator
    /// advances to the chosen head and replans this request there.
    ///
    /// # Errors
    /// Returns a durable controller, verification, history, or coordination
    /// error. Returning an error does not grant or deny application access.
    pub async fn decide(
        &self,
        head: ControlHead,
        counter: u64,
        operation: CommandId,
        request_id: CommandId,
        request: CertifiedAuthorityRequest,
    ) -> Result<CoordinatedAuthorityDecision, String> {
        let root = request.root(self.anchor.realm_id(), request_id)?;
        let mut head = head;
        let mut counter = counter;
        for _ in 0..self.max_rounds {
            let Some(next_counter) = counter.checked_add(1) else {
                return Err("authority ballot counter overflowed".to_owned());
            };
            let ballot = ControlBallot {
                counter,
                proposer: self.proposer.controller,
            };
            let result = self
                .try_round(head, ballot, operation, &root, request.clone())
                .await?;
            counter = next_counter;
            match result {
                RoundResult::Decided(decision) => return Ok(*decision),
                RoundResult::Advanced(next_head) => head = next_head,
            }
        }
        Err("authority coordination did not converge before the retry limit".to_owned())
    }

    async fn try_round(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        operation: CommandId,
        root: &AuthorityDecisionRoot,
        request: CertifiedAuthorityRequest,
    ) -> Result<RoundResult, String> {
        self.synchronize().await?;
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
        let verifier = history.context_at(head)?.verifier()?;
        if self.proposer.controller != ballot.proposer {
            return Err("authority ballot proposer does not match coordinator identity".to_owned());
        }
        let Some(_proposer) = self.proposer(ballot.proposer) else {
            return Err("authority proposer peer is not configured".to_owned());
        };
        let promises = self
            .prepare_votes(&self.proposer.principal, head, ballot)
            .await?;
        let prepared = verifier
            .verify_prepare(ballot, &promises)
            .map_err(|error| error.to_string())?;
        let desired = Self::plan_value(&history, head, operation, root, request.clone())?;
        let value = prepared.select_value(desired);
        let proposal = self
            .propose_value(&self.proposer.principal, head, ballot, &promises, &value)
            .await?;
        let accepts = self
            .accept_votes(&self.proposer.principal, head, &proposal)
            .await?;
        let chosen = prepared
            .verify_chosen(&value, &accepts)
            .map_err(|error| error.to_string())?;
        let chosen_head = chosen.head().map_err(|error| error.to_string())?;
        self.synchronize().await?;
        self.recover_or_advance(
            head,
            chosen_head,
            root,
            &request,
            ChosenRoundEvidence {
                proposal,
                promises,
                accepts,
            },
        )
    }

    fn plan_value(
        history: &AuthorityHistory,
        head: ControlHead,
        operation: CommandId,
        root: &AuthorityDecisionRoot,
        request: CertifiedAuthorityRequest,
    ) -> Result<ControlValue, String> {
        history
            .plan_decision_at(
                head,
                operation,
                root.request_id(),
                request.request,
                request.evaluated_at,
                request.topology,
            )?
            .control_value()
    }

    fn recover_or_advance(
        &self,
        predecessor: ControlHead,
        chosen_head: ControlHead,
        root: &AuthorityDecisionRoot,
        request: &CertifiedAuthorityRequest,
        evidence: ChosenRoundEvidence,
    ) -> Result<RoundResult, String> {
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
        let Some(transition) = history.decision_at(chosen_head, root)? else {
            return Ok(RoundResult::Advanced(chosen_head));
        };
        if transition.binding() != &request.binding() {
            return Err(
                "authority decision root was recovered for a different request binding".to_owned(),
            );
        }
        Ok(RoundResult::Decided(Box::new(
            CoordinatedAuthorityDecision {
                predecessor,
                head: chosen_head,
                transition,
                proposal: evidence.proposal,
                promises: evidence.promises,
                accepts: evidence.accepts,
            },
        )))
    }

    fn proposer(&self, controller_id: ControllerId) -> Option<&AuthorityCoordinatorPeer> {
        self.peers
            .iter()
            .find(|peer| peer.controller_id() == controller_id)
    }

    async fn prepare_votes(
        &self,
        caller: &Principal,
        head: ControlHead,
        ballot: ControlBallot,
    ) -> Result<Vec<SignedControlVote>, String> {
        let mut votes = Vec::new();
        for peer in &self.peers {
            match peer.prepare(caller, head, ballot).await {
                Ok(vote) => votes.push(vote),
                Err(AuthorizationFailure::Deny(denial)) => {
                    return Err(denial.report.explanations.last().map_or_else(
                        || "authority control prepare was denied".to_owned(),
                        |explanation| explanation.message.clone(),
                    ));
                }
                Err(AuthorizationFailure::Challenge { .. }) => {
                    return Err("authority control prepare cannot require a challenge".to_owned());
                }
                Err(AuthorizationFailure::Unavailable(_)) => {}
            }
        }
        Ok(votes)
    }

    async fn propose_value(
        &self,
        caller: &Principal,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
    ) -> Result<SignedControlProposal, String> {
        let peer = self
            .peers
            .iter()
            .find(|peer| peer.controller_id() == ballot.proposer)
            .ok_or_else(|| "authority proposer peer is not configured".to_owned())?;
        peer.propose(caller, head, ballot, promises, value)
            .await
            .map_err(control_failure_message)
    }

    async fn accept_votes(
        &self,
        caller: &Principal,
        head: ControlHead,
        proposal: &SignedControlProposal,
    ) -> Result<Vec<SignedControlVote>, String> {
        let mut votes = Vec::new();
        for peer in &self.peers {
            match peer.accept(caller, head, proposal).await {
                Ok(vote) => votes.push(vote),
                Err(AuthorizationFailure::Deny(denial)) => {
                    return Err(denial.report.explanations.last().map_or_else(
                        || "authority control accept was denied".to_owned(),
                        |explanation| explanation.message.clone(),
                    ));
                }
                Err(AuthorizationFailure::Challenge { .. }) => {
                    return Err("authority control accept cannot require a challenge".to_owned());
                }
                Err(AuthorizationFailure::Unavailable(_)) => {}
            }
        }
        Ok(votes)
    }

    async fn synchronize(&self) -> Result<(), String> {
        let authority_scope = authority_realm_scope(self.anchor.realm_id());
        for peer in &self.peers {
            match peer.synchronize_evidence(&authority_scope).await {
                Ok(()) | Err(RetainedEvidenceError::Unavailable(_)) => {}
                Err(RetainedEvidenceError::Invalid(message)) => return Err(message),
            }
        }
        let mut events = certified_events(&self.observer, self.anchor.realm_id())?;
        for peer in &self.peers {
            events.extend(peer.retained_events()?);
        }
        for event in events {
            self.observer
                .ingest(event.clone())
                .map(|_| ())
                .map_err(|error| error.to_string())?;
            for peer in &self.peers {
                peer.ingest(event.clone())?;
            }
        }
        Ok(())
    }
}

struct ChosenRoundEvidence {
    proposal: SignedControlProposal,
    promises: Vec<SignedControlVote>,
    accepts: Vec<SignedControlVote>,
}

enum RoundResult {
    Decided(Box<CoordinatedAuthorityDecision>),
    Advanced(ControlHead),
}

fn command_scopes_from_attempt(request: &AccessAttempt) -> Vec<ScopeId> {
    let mut scopes = BTreeMap::new();
    match &request.target {
        AccessTarget::Scope(scope)
        | AccessTarget::ServiceScope {
            scope_id: scope, ..
        }
        | AccessTarget::Items {
            scope_id: scope, ..
        }
        | AccessTarget::KnownCommand {
            scope_id: scope, ..
        }
        | AccessTarget::CommandCatalog {
            scope_id: scope, ..
        }
        | AccessTarget::Handler {
            scope_id: Some(scope),
            ..
        } => {
            scopes.insert(scope.as_str().to_owned(), scope.clone());
        }
        AccessTarget::ScopeSet(selections)
        | AccessTarget::History(myko_federation::ReplicationSelection::Scopes(selections)) => {
            for selection in selections {
                insert_selection_scope(&mut scopes, selection);
            }
        }
        AccessTarget::History(_)
        | AccessTarget::Handler { scope_id: None, .. }
        | AccessTarget::NodeIdentity
        | AccessTarget::ScopeCatalog
        | AccessTarget::LiveTopics(_)
        | AccessTarget::Command(_)
        | AccessTarget::AuthorityApproval(_) => {}
    }
    for claim in &request.resource_claims {
        insert_selection_scope(&mut scopes, &claim.selection);
    }
    scopes.into_values().collect()
}

fn insert_selection_scope(scopes: &mut BTreeMap<String, ScopeId>, selection: &ScopeSelection) {
    let scope = selection.root();
    scopes.insert(scope.as_str().to_owned(), scope.clone());
}
