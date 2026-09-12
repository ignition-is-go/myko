use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::future::join_all;
use myko_federation::{
    AuthorityPresentation, AuthorityUnavailable, AuthorizationFailure, CertifiedControlChain,
    CertifiedControlContext, CommandId, ControlAnchor, ControlTransition, EventEnvelope,
    ExecutionAssignment, ExecutionAssignmentObservation, ExecutionAssignmentsAtHead,
    FrameworkControlEvent, Node, NodeEvent, Principal,
    control_quorum::{ControlBallot, ControlHead, ControlValue, ControllerId, SignedControlVote},
};
use tokio::sync::Mutex;

use super::{
    ControlEndpoint, ControlFuture, ControlProposeRequest, RetainedEvidenceError,
    ScopedRetainedEvidenceEndpoint,
};

/// Configured controller transport and optional history source into the observer.
#[derive(Debug)]
pub struct ExecutionControllerPeer {
    controller: ControllerId,
    endpoint: Arc<dyn ControlEndpoint>,
    evidence: Option<Arc<dyn ScopedRetainedEvidenceEndpoint>>,
}

impl ExecutionControllerPeer {
    /// Without a source, the observer must already retain this peer's evidence.
    #[must_use]
    pub fn new(controller: ControllerId, endpoint: Arc<dyn ControlEndpoint>) -> Self {
        Self {
            controller,
            endpoint,
            evidence: None,
        }
    }

    #[must_use]
    pub fn with_observer_evidence_endpoint(
        mut self,
        evidence: Arc<dyn ScopedRetainedEvidenceEndpoint>,
    ) -> Self {
        self.evidence = Some(evidence);
        self
    }
}

/// A chosen historical assignment, never current authority or a ready route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAssignmentReceipt {
    assignment: ExecutionAssignment,
    head: ControlHead,
}

impl ExecutionAssignmentReceipt {
    #[must_use]
    pub const fn assignment(&self) -> &ExecutionAssignment {
        &self.assignment
    }

    #[must_use]
    pub const fn head(&self) -> ControlHead {
        self.head
    }
}

/// Chooses assignment operations through explicitly configured durable controllers.
///
/// The caller supplies trusted framework intent, not an application command.
/// Controller authentication does not authorize an operator's assignment changes.
/// No application modules, storage placement, or execution readiness are implied.
#[derive(Debug)]
pub struct ExecutionAssignmentCoordinator {
    observer: Node,
    anchor: ControlAnchor,
    caller: AuthorityPresentation,
    proposer: ControllerId,
    peers: BTreeMap<ControllerId, ExecutionControllerPeer>,
    request_timeout: Duration,
    proposal_turn: Mutex<()>,
}

impl ExecutionAssignmentCoordinator {
    /// # Errors
    /// Rejects duplicate controllers and a missing proposer endpoint.
    pub fn new(
        observer: Node,
        anchor: ControlAnchor,
        caller: Principal,
        proposer: ControllerId,
        peers: Vec<ExecutionControllerPeer>,
    ) -> Result<Self, String> {
        let mut indexed = BTreeMap::new();
        for peer in peers {
            if indexed.insert(peer.controller, peer).is_some() {
                return Err("execution coordinator repeats a controller".to_owned());
            }
        }
        if !indexed.contains_key(&proposer) {
            return Err("execution coordinator has no proposer endpoint".to_owned());
        }
        Ok(Self {
            observer,
            anchor,
            caller: AuthorityPresentation::direct(caller),
            proposer,
            peers: indexed,
            request_timeout: Duration::from_secs(10),
            proposal_turn: Mutex::new(()),
        })
    }

    /// Bound each controller call and evidence refresh, including connection setup.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Observe assignments through a fresh quorum operation.
    ///
    /// Each call chooses a new observation after recovering any accepted value.
    /// The result describes that point during this call. It is not a lease or
    /// reusable permission for subsequent execution, routing, or publication.
    ///
    /// # Errors
    /// Reports unavailable coordination, invalid evidence, or failure to converge
    /// within eight rounds. No cached observation satisfies a failed fresh check.
    pub async fn observe(&self) -> Result<ExecutionAssignmentsAtHead, String> {
        let _turn = self.proposal_turn.lock().await;
        for _ in 0..8 {
            self.refresh().await?;
            let history = self.history()?;
            let desired = ExecutionAssignmentObservation::new(
                CommandId::new(),
                self.anchor.realm().clone(),
                history.head,
            )
            .transition()?;
            let head = self.choose(&history, desired.control_value()?).await?;
            self.refresh().await?;
            let retained = self.history()?;
            retained.chain.context_at(head)?;
            if retained.recover(&desired)? == Some(head) {
                return ExecutionAssignmentsAtHead::replay(&retained.chain, head);
            }
        }
        Err("execution observation coordination exceeded eight rounds".to_owned())
    }

    /// Choose an assignment or recover its original historical receipt.
    ///
    /// A prior accepted value is recovered before another operation advances.
    /// Retrying an old operation never restores a replaced assignment.
    ///
    /// # Errors
    /// Rejects foreign realms, conflicting operation reuse, invalid evidence,
    /// failed quorum or persistence, and failure to converge within eight rounds.
    /// A failed call can leave durable votes. Retry explicitly with the same intent.
    pub async fn assign(
        &self,
        assignment: ExecutionAssignment,
    ) -> Result<ExecutionAssignmentReceipt, String> {
        if assignment.realm() != self.anchor.realm() {
            return Err("execution assignment belongs to another control realm".to_owned());
        }
        let _turn = self.proposal_turn.lock().await;
        let desired = assignment.transition()?;
        for _ in 0..8 {
            let retained = self.history()?;
            if let Some(head) = retained.recover(&desired)? {
                return Ok(ExecutionAssignmentReceipt { assignment, head });
            }
            self.refresh().await?;
            let history = self.history()?;
            if let Some(head) = history.recover(&desired)? {
                return Ok(ExecutionAssignmentReceipt { assignment, head });
            }
            let head = self.choose(&history, desired.control_value()?).await?;
            self.refresh().await?;
            // A reply certificate is insufficient: recovery needs retained records.
            let retained = self.history()?;
            retained.chain.context_at(head)?;
            if let Some(head) = retained.recover(&desired)? {
                return Ok(ExecutionAssignmentReceipt { assignment, head });
            }
        }
        Err("execution assignment coordination exceeded eight rounds".to_owned())
    }

    fn history(&self) -> Result<AssignmentHistory, String> {
        let records = self
            .observer
            .events_after(None)
            .map_err(|error| error.to_string())?;
        let chain = CertifiedControlChain::replay(&records, self.anchor.clone())?;
        let head = chain.retained_head()?;
        ExecutionAssignmentsAtHead::replay(&chain, head)?;
        Ok(AssignmentHistory {
            records,
            chain,
            head,
        })
    }

    async fn refresh(&self) -> Result<(), String> {
        let scopes = std::slice::from_ref(self.anchor.realm());
        let results = join_all(self.peers.values().filter_map(|peer| {
            peer.evidence.as_ref().map(|source| async move {
                tokio::time::timeout(self.request_timeout, source.refresh_scopes(scopes)).await
            })
        }))
        .await;
        for result in results {
            match result {
                Ok(Ok(()) | Err(RetainedEvidenceError::Unavailable(_))) | Err(_) => {}
                Ok(Err(RetainedEvidenceError::Invalid(message))) => return Err(message),
            }
        }
        Ok(())
    }

    async fn choose(
        &self,
        history: &AssignmentHistory,
        desired: ControlValue,
    ) -> Result<ControlHead, String> {
        let context = history.chain.context_at(history.head)?;
        let verifier = context.verifier()?;
        let ballot = history.next_ballot(&context, self.proposer)?;
        verifier
            .prepare_request(ballot)
            .map_err(|error| error.to_string())?;
        let peers: Vec<_> = self
            .peers
            .values()
            .filter(|peer| {
                verifier
                    .prepare_request(ControlBallot {
                        proposer: peer.controller,
                        ..ballot
                    })
                    .is_ok()
            })
            .collect();
        let target = self.anchor.target(history.head);
        let principal = &self.caller.executor.id;
        let promises = self
            .collect_votes(
                peers
                    .iter()
                    .map(|peer| {
                        peer.endpoint
                            .prepare(principal, &self.caller, target.clone(), ballot)
                    })
                    .collect(),
            )
            .await?;
        let prepared = verifier
            .verify_prepare(ballot, &promises)
            .map_err(|error| error.to_string())?;
        let value = prepared.select_value(desired);
        let proposer = self
            .peers
            .get(&self.proposer)
            .ok_or_else(|| "execution proposer endpoint is missing".to_owned())?;
        let proposal = bounded(
            self.request_timeout,
            proposer.endpoint.propose(
                principal,
                &self.caller,
                ControlProposeRequest {
                    target: target.clone(),
                    ballot,
                    promises: promises.clone(),
                    value: value.clone(),
                },
            ),
        )
        .await
        .map_err(control_error)?;
        verifier
            .accept_request(&proposal)
            .map_err(|error| error.to_string())?;
        if proposal.message.ballot != ballot || proposal.message.value != value {
            return Err("execution proposer returned another ballot or value".to_owned());
        }
        let accepts = self
            .collect_votes(
                peers
                    .iter()
                    .map(|peer| {
                        peer.endpoint.accept(
                            principal,
                            &self.caller,
                            target.clone(),
                            proposal.clone(),
                        )
                    })
                    .collect(),
            )
            .await?;
        prepared
            .verify_chosen(&value, &accepts)
            .map_err(|error| error.to_string())?
            .head()
            .map_err(|error| error.to_string())
    }

    async fn collect_votes(
        &self,
        calls: Vec<ControlFuture<'_, SignedControlVote>>,
    ) -> Result<Vec<SignedControlVote>, String> {
        let results = join_all(
            calls
                .into_iter()
                .map(|call| bounded(self.request_timeout, call)),
        )
        .await;
        let mut votes = Vec::new();
        for result in results {
            match result {
                Ok(vote) => votes.push(vote),
                Err(AuthorizationFailure::Unavailable(_)) => {}
                Err(error) => return Err(control_error(error)),
            }
        }
        Ok(votes)
    }
}

fn control_error(error: AuthorizationFailure) -> String {
    match error {
        AuthorizationFailure::Unavailable(reason) => reason.to_string(),
        AuthorizationFailure::Deny(denial) => denial.report.explanations.last().map_or_else(
            || "execution control request denied".to_owned(),
            |explanation| explanation.message.clone(),
        ),
        AuthorizationFailure::Challenge { .. } => {
            "execution control cannot require an application challenge".to_owned()
        }
    }
}

async fn bounded<T>(
    timeout: Duration,
    call: ControlFuture<'_, T>,
) -> Result<T, AuthorizationFailure> {
    tokio::time::timeout(timeout, call)
        .await
        .map_err(|_| AuthorizationFailure::from(AuthorityUnavailable::CoordinationUnavailable))?
}

struct AssignmentHistory {
    records: Vec<EventEnvelope>,
    chain: CertifiedControlChain,
    head: ControlHead,
}

impl AssignmentHistory {
    fn recover(&self, desired: &ControlTransition) -> Result<Option<ControlHead>, String> {
        let Some(evidence) = self
            .chain
            .operation_evidence_at(self.head, desired.operation())?
        else {
            return Ok(None);
        };
        if evidence.proposal().message.value != desired.control_value()? {
            return Err(
                "execution assignment operation was chosen with different content".to_owned(),
            );
        }
        Ok(Some(evidence.head()))
    }

    fn next_ballot(
        &self,
        context: &CertifiedControlContext,
        proposer: ControllerId,
    ) -> Result<ControlBallot, String> {
        let verifier = context.verifier()?;
        let maximum = self
            .records
            .iter()
            .filter_map(|event| match &event.event {
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote))
                    if vote.message.slot == *context.slot()
                        && vote.verify_signature().is_ok()
                        && verifier
                            .prepare_request(ControlBallot {
                                proposer: vote.message.controller,
                                ..vote.message.ballot
                            })
                            .is_ok() =>
                {
                    Some(vote.message.ballot.counter)
                }
                NodeEvent::FrameworkControl(FrameworkControlEvent::ControlProposal(proposal))
                    if verifier.accept_request(proposal).is_ok() =>
                {
                    Some(proposal.message.ballot.counter)
                }
                _ => None,
            })
            .max()
            .unwrap_or(0);
        let counter = maximum
            .checked_add(1)
            .ok_or_else(|| "execution assignment ballot counter exhausted".to_owned())?;
        Ok(ControlBallot { counter, proposer })
    }
}
