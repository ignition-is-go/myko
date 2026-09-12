use std::{collections::BTreeMap, fmt, sync::Arc};

use chrono::Utc;
use ed25519_dalek::SigningKey;
use myko_federation::{
    AccessOperation, AuthorityPresentation, AuthorityUnavailable, AuthorizationExplanation,
    AuthorizationFailure, AuthorizationReport, ControlAnchor, DenyDecision,
    ExecutionAssignmentController, Node, Principal, PrincipalId, ResourceVisibility, ScopeId,
    control_quorum::{
        ControlBallot, ControlHead, ControlTarget, ControllerId, SignedControlProposal,
        SignedControlVote,
    },
};

use super::{
    ControlEndpoint, ControlFuture, ControlProposeRequest, RetainedEvidenceError,
    ScopedRetainedEvidenceEndpoint,
};

/// Authenticated transport boundary for an explicitly configured assignment controller.
///
/// No application is loaded or queried. Caller bindings authorize controller
/// participation, not an operator's intent or permission to execute an application.
/// Each signing key must belong to exactly one durable controller store.
pub struct ExecutionControlEndpoint {
    realm: ScopeId,
    controller: ExecutionAssignmentController,
    key: SigningKey,
    callers: BTreeMap<PrincipalId, (Principal, ControllerId)>,
    inbound_evidence: BTreeMap<PrincipalId, Arc<dyn ScopedRetainedEvidenceEndpoint>>,
}

impl ExecutionControlEndpoint {
    /// Bind authenticated principals to the proposal keys they may use.
    ///
    /// # Errors
    /// Rejects an empty caller set or duplicate principal identities.
    pub fn new(
        node: Node,
        anchor: ControlAnchor,
        key: SigningKey,
        callers: Vec<(Principal, ControllerId)>,
    ) -> Result<Self, String> {
        let mut indexed = BTreeMap::new();
        for (principal, controller) in callers {
            if indexed
                .insert(principal.id.clone(), (principal, controller))
                .is_some()
            {
                return Err("execution control caller principal is duplicated".to_owned());
            }
        }
        if indexed.is_empty() {
            return Err("execution control endpoint requires callers".to_owned());
        }
        Ok(Self {
            realm: anchor.realm().clone(),
            controller: ExecutionAssignmentController::new(node, anchor),
            key,
            callers: indexed,
            inbound_evidence: BTreeMap::new(),
        })
    }

    /// Bind a registered caller to an authenticated history source.
    /// Only this controller's realm is refreshed, after caller authentication.
    /// Callers without a binding use locally retained evidence only.
    ///
    /// # Errors
    /// Rejects unknown callers and duplicate evidence bindings.
    pub fn with_scoped_evidence_endpoint(
        mut self,
        caller: PrincipalId,
        endpoint: Arc<dyn ScopedRetainedEvidenceEndpoint>,
    ) -> Result<Self, String> {
        if !self.callers.contains_key(&caller) {
            return Err("execution evidence source requires a registered caller".to_owned());
        }
        if self.inbound_evidence.insert(caller, endpoint).is_some() {
            return Err("execution evidence source is already bound for this caller".to_owned());
        }
        Ok(self)
    }

    async fn refresh_evidence(&self, caller: &PrincipalId) -> Result<(), AuthorizationFailure> {
        let Some(source) = self.inbound_evidence.get(caller) else {
            return Ok(());
        };
        source
            .refresh_scopes(std::slice::from_ref(&self.realm))
            .await
            .map_err(|error| match error {
                RetainedEvidenceError::Unavailable(reason) => reason.into(),
                RetainedEvidenceError::Invalid(_) => {
                    AuthorityUnavailable::HistoryUnavailable.into()
                }
            })
    }

    fn control_head(&self, target: &ControlTarget) -> Result<ControlHead, AuthorizationFailure> {
        (target.realm == self.realm)
            .then_some(target.head)
            .ok_or_else(|| AuthorityUnavailable::CoordinationUnavailable.into())
    }

    fn authorize(
        &self,
        authenticated: &PrincipalId,
        presentation: &AuthorityPresentation,
        proposer: ControllerId,
    ) -> Result<(), AuthorizationFailure> {
        let reason = if &presentation.executor.id != authenticated {
            Some("execution control executor differs from the authenticated principal")
        } else if presentation.principal != presentation.executor
            || !presentation.provenance.is_empty()
        {
            Some("execution control requires a direct controller principal")
        } else {
            match self.callers.get(authenticated) {
                None => Some("authenticated principal is not an execution controller"),
                Some((principal, _)) if principal != &presentation.executor => {
                    Some("execution controller principal kind differs from its binding")
                }
                Some((_, controller)) if *controller != proposer => {
                    Some("execution controller does not match the proposal ballot")
                }
                Some(_) => None,
            }
        };
        reason.map_or(Ok(()), |message| {
            Err(AuthorizationFailure::Deny(Box::new(DenyDecision {
                report: AuthorizationReport {
                    evaluated_at: Utc::now(),
                    principal: presentation.principal.clone(),
                    executor: presentation.executor.clone(),
                    operation: AccessOperation::AdministerExecution,
                    explanations: vec![AuthorizationExplanation {
                        code: "execution_control_rejected".to_owned(),
                        message: message.to_owned(),
                        grant_id: None,
                        delegation_id: None,
                        obligation_id: None,
                        constraint: None,
                    }],
                },
                visibility: ResourceVisibility::Unauthorized,
            })))
        })
    }
}

impl fmt::Debug for ExecutionControlEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionControlEndpoint")
            .field(
                "controller",
                &ControllerId(self.key.verifying_key().to_bytes()),
            )
            .field("callers", &self.callers)
            .field("inbound_evidence_callers", &self.inbound_evidence.keys())
            .finish_non_exhaustive()
    }
}

impl ControlEndpoint for ExecutionControlEndpoint {
    fn prepare<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        target: ControlTarget,
        ballot: ControlBallot,
    ) -> ControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            let head = self.control_head(&target)?;
            self.authorize(principal, presentation, ballot.proposer)?;
            self.refresh_evidence(principal).await?;
            self.controller
                .prepare(head, ballot, &self.key)
                .map_err(control_failure)
        })
    }

    fn propose<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        request: ControlProposeRequest,
    ) -> ControlFuture<'a, SignedControlProposal> {
        Box::pin(async move {
            let head = self.control_head(&request.target)?;
            self.authorize(principal, presentation, request.ballot.proposer)?;
            self.refresh_evidence(principal).await?;
            self.controller
                .propose(
                    head,
                    request.ballot,
                    &request.promises,
                    &request.value,
                    &self.key,
                )
                .map_err(control_failure)
        })
    }

    fn accept<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        target: ControlTarget,
        proposal: SignedControlProposal,
    ) -> ControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            let head = self.control_head(&target)?;
            self.authorize(principal, presentation, proposal.message.ballot.proposer)?;
            self.refresh_evidence(principal).await?;
            self.controller
                .accept(head, &proposal, &self.key)
                .map_err(control_failure)
        })
    }
}

fn control_failure(_error: String) -> AuthorizationFailure {
    AuthorityUnavailable::CoordinationUnavailable.into()
}
