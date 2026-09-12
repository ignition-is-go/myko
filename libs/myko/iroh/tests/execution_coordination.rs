use std::{error::Error, path::Path, sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use myko::server::{
    ControlEndpoint, ExecutionAssignmentCoordinator, ExecutionControlEndpoint,
    ExecutionControllerPeer,
};
use myko_federation::{
    CertifiedControlChain, CommandId, ControlAnchor, ExecutionAssignment,
    ExecutionAssignmentController, ExecutionAssignmentsAtHead, FederationPermission, Node, NodeId,
    Principal, ScopeGrant, ScopeGrantCoverage, ScopeGrantPolicy, ScopeId, ServiceId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControllerId},
};
use myko_iroh::{IrohReplicator, IrohScopedEvidenceEndpoint, endpoint_principal_id};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

#[path = "execution_coordination/history_boundary.rs"]
mod history_boundary;
#[path = "execution_coordination/observations.rs"]
mod observations;

struct Peer {
    node: Node,
    key: SigningKey,
    transport: IrohReplicator,
}

impl Peer {
    fn id(&self) -> ControllerId {
        ControllerId(self.key.verifying_key().to_bytes())
    }

    fn principal(&self) -> Principal {
        Principal::node(endpoint_principal_id(self.transport.address().id))
    }
}

struct Mesh {
    peers: [Peer; 3],
    anchor: ControlAnchor,
}

impl Mesh {
    async fn open(directory: &Path) -> Result<Self, Box<dyn Error>> {
        let mut peers = Vec::new();
        for seed in [101, 102, 103] {
            let node = RedbJournal::open_node(directory.join(format!("{seed}.redb")))?;
            let transport = IrohReplicator::bind_loopback(node.clone()).await?;
            peers.push(Peer {
                node,
                transport,
                key: SigningKey::from_bytes(&[seed; 32]),
            });
        }
        let anchor = ControlAnchor::new(
            ScopeId::new("execution:coordination"),
            ControlEpochId([104; 32]),
            ControlHead([105; 32]),
            peers.iter().map(Peer::id).collect(),
        )?;
        let peers = peers.try_into().map_err(|_| "expected three controllers")?;
        let mesh = Self { peers, anchor };
        for peer in &mesh.peers {
            peer.transport
                .set_access_policy(Arc::new(ScopeGrantPolicy::new(
                    mesh.peers
                        .iter()
                        .map(|reader| ScopeGrant {
                            scope_id: mesh.anchor.realm().clone(),
                            coverage: ScopeGrantCoverage::Exact,
                            grantee: reader.principal().id,
                            permissions: vec![FederationPermission::ReadHistory],
                        })
                        .collect(),
                )))?;
            mesh.install(peer)?;
        }
        Ok(mesh)
    }

    fn endpoint(&self, peer: &Peer) -> Result<ExecutionControlEndpoint, String> {
        let mut endpoint = ExecutionControlEndpoint::new(
            peer.node.clone(),
            self.anchor.clone(),
            peer.key.clone(),
            self.peers
                .iter()
                .map(|caller| (caller.principal(), caller.id()))
                .collect(),
        )?;
        for caller in &self.peers {
            if caller.id() != peer.id() {
                endpoint = endpoint.with_scoped_evidence_endpoint(
                    caller.principal().id,
                    Arc::new(IrohScopedEvidenceEndpoint::new(
                        peer.transport.clone(),
                        caller.transport.address(),
                    )),
                )?;
            }
        }
        Ok(endpoint)
    }

    fn install(&self, peer: &Peer) -> TestResult {
        peer.transport.sessions().set_control_endpoint(
            self.anchor.realm().clone(),
            Some(Arc::new(self.endpoint(peer)?)),
        )?;
        Ok(())
    }

    fn coordinator(&self, caller: &Peer) -> Result<ExecutionAssignmentCoordinator, Box<dyn Error>> {
        let mut peers = Vec::new();
        for peer in &self.peers {
            let endpoint: Arc<dyn ControlEndpoint> = if caller.id() == peer.id() {
                Arc::new(self.endpoint(peer)?)
            } else {
                Arc::new(caller.transport.command_client(peer.transport.address()))
            };
            let mut configured = ExecutionControllerPeer::new(peer.id(), endpoint);
            if caller.id() != peer.id() {
                configured = configured.with_observer_evidence_endpoint(Arc::new(
                    IrohScopedEvidenceEndpoint::new(
                        caller.transport.clone(),
                        peer.transport.address(),
                    ),
                ));
            }
            peers.push(configured);
        }
        Ok(ExecutionAssignmentCoordinator::new(
            caller.node.clone(),
            self.anchor.clone(),
            caller.principal(),
            caller.id(),
            peers,
        )?
        .with_request_timeout(Duration::from_secs(1)))
    }

    fn assignment(&self, operation: CommandId, executor: NodeId) -> ExecutionAssignment {
        ExecutionAssignment::new(
            operation,
            self.anchor.realm().clone(),
            ScopeId::new("work"),
            ServiceId::new("records"),
            [executor],
        )
    }

    fn history(&self, peer: &Peer) -> Result<CertifiedControlChain, String> {
        CertifiedControlChain::replay(
            &peer
                .node
                .events_after(None)
                .map_err(|error| error.to_string())?,
            self.anchor.clone(),
        )
    }

    async fn shutdown(self) -> TestResult {
        for peer in self.peers {
            peer.transport.shutdown().await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn assignment_retry_after_reopen_never_restores_replaced_executors() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, _, _] = &mesh.peers;
    let coordinator = mesh.coordinator(a)?;
    let first = mesh.assignment(CommandId::new(), NodeId::new());
    let second_executor = NodeId::new();
    let second = mesh.assignment(CommandId::new(), second_executor);
    let (original, concurrent_retry) = tokio::join!(
        coordinator.assign(first.clone()),
        coordinator.assign(first.clone()),
    );
    let original = original?;
    if original != concurrent_retry? {
        return Err("concurrent retry did not recover the same receipt".into());
    }
    let replacement = coordinator.assign(second.clone()).await?;
    if original.head() == replacement.head() {
        return Err("replacement did not advance assignment history".into());
    }
    drop(coordinator);
    mesh.shutdown().await?;

    let reopened = Mesh::open(directory.path()).await?;
    let [a, b, c] = &reopened.peers;
    // The receipt is recoverable from disk even when no remote controller is running.
    b.transport.clone().shutdown().await?;
    c.transport.clone().shutdown().await?;
    let before = a.node.events_after(None)?;
    let coordinator = reopened.coordinator(a)?;
    if coordinator.assign(first.clone()).await? != original
        || coordinator.assign(second.clone()).await? != replacement
        || a.node.events_after(None)? != before
    {
        return Err("reopened retry issued new votes or lost the original receipt".into());
    }
    let conflicting = reopened.assignment(first.transition()?.operation(), NodeId::new());
    if coordinator.assign(conflicting).await.is_ok() || a.node.events_after(None)? != before {
        return Err("conflicting operation reuse was accepted or wrote history".into());
    }
    let chain = reopened.history(a)?;
    if chain.retained_head()? != replacement.head() {
        return Err("old retry restored a superseded assignment".into());
    }
    let expected = ExecutionAssignmentsAtHead::replay(&chain, replacement.head())?;
    if expected.exact(&ScopeId::new("work"), &ServiceId::new("records"))
        != Some(&[second_executor].into())
    {
        return Err("replacement lost its executor set".into());
    }
    a.transport.clone().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn minority_cannot_choose_and_explicit_retry_uses_retained_ballots() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, c] = &mesh.peers;
    for peer in [b, c] {
        peer.transport
            .sessions()
            .set_control_endpoint(mesh.anchor.realm().clone(), None)?;
    }
    let coordinator = mesh.coordinator(a)?;
    let desired = mesh.assignment(CommandId::new(), NodeId::new());
    if coordinator.assign(desired.clone()).await.is_ok()
        || mesh.history(a)?.retained_head()? != mesh.anchor.genesis()
    {
        return Err("minority established a chosen assignment".into());
    }
    mesh.install(b)?;
    let receipt = coordinator.assign(desired.clone()).await?;
    let chain = mesh.history(a)?;
    let evidence = chain
        .operation_evidence_at(receipt.head(), desired.transition()?.operation())?
        .ok_or("successful receipt is not backed by retained evidence")?;
    if evidence.proposal().message.ballot.counter <= 1 {
        return Err("retry reused the interrupted prepare ballot".into());
    }
    drop(coordinator);
    mesh.shutdown().await
}

#[tokio::test]
async fn surviving_proposer_recovers_accepted_value_before_its_own_assignment() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, c] = &mesh.peers;
    let interrupted = mesh.assignment(CommandId::new(), NodeId::new());
    let desired = mesh.assignment(CommandId::new(), NodeId::new());
    let local = ExecutionAssignmentController::new(a.node.clone(), mesh.anchor.clone());
    let client = a.transport.command_client(b.transport.address());
    let target = mesh.anchor.target(mesh.anchor.genesis());
    let ballot = ControlBallot {
        counter: 7,
        proposer: a.id(),
    };
    let promises = [
        local.prepare(target.head, ballot, &a.key)?,
        client.prepare_control(target.clone(), ballot).await?,
    ];
    let proposal = local.propose(
        target.head,
        ballot,
        &promises,
        &interrupted.transition()?.control_value()?,
        &a.key,
    )?;
    client.accept_control(target, proposal).await?;
    drop(client);
    a.transport.clone().shutdown().await?;
    if mesh.history(b)?.retained_head()? != mesh.anchor.genesis() {
        return Err("interruption fixture already had a chosen assignment".into());
    }

    let coordinator = mesh.coordinator(b)?;
    let receipt = coordinator.assign(desired.clone()).await?;
    let chain = mesh.history(b)?;
    let operations: Vec<_> = chain
        .transitions_to(receipt.head())?
        .iter()
        .map(|transition| transition.operation())
        .collect();
    if operations
        != [
            interrupted.transition()?.operation(),
            desired.transition()?.operation(),
        ]
    {
        return Err("new proposer replaced or skipped the accepted assignment".into());
    }
    if coordinator.assign(interrupted.clone()).await?.assignment() != &interrupted {
        return Err("recovered assignment lost its original content".into());
    }
    b.transport.clone().shutdown().await?;
    c.transport.clone().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reply_quorum_requires_retained_evidence_before_returning_a_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, _] = &mesh.peers;
    let no_import = ExecutionAssignmentCoordinator::new(
        a.node.clone(),
        mesh.anchor.clone(),
        a.principal(),
        a.id(),
        vec![
            ExecutionControllerPeer::new(a.id(), Arc::new(mesh.endpoint(a)?)),
            ExecutionControllerPeer::new(
                b.id(),
                Arc::new(a.transport.command_client(b.transport.address())),
            ),
        ],
    )?;
    let desired = mesh.assignment(CommandId::new(), NodeId::new());
    if no_import.assign(desired.clone()).await.is_ok() {
        return Err("reply quorum returned a receipt without retaining its evidence".into());
    }
    if mesh.history(a)?.retained_head()? != mesh.anchor.genesis() {
        return Err("fixture unexpectedly imported the remote acceptance".into());
    }
    let coordinator = mesh.coordinator(a)?;
    let receipt = coordinator.assign(desired.clone()).await?;
    let chain = mesh.history(a)?;
    let evidence = chain
        .operation_evidence_at(receipt.head(), desired.transition()?.operation())?
        .ok_or("refreshed receipt has no retained proof")?;
    if evidence.proposal().message.ballot.counter != 1 {
        return Err("retry created another proposal instead of recovering the reply quorum".into());
    }
    drop(coordinator);
    drop(no_import);
    mesh.shutdown().await
}

#[tokio::test]
async fn coordinator_rejects_ambiguous_configuration_and_foreign_intent() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, _] = &mesh.peers;
    let endpoint = Arc::new(mesh.endpoint(a)?);
    for peers in [
        Vec::new(),
        vec![ExecutionControllerPeer::new(b.id(), endpoint.clone())],
        vec![
            ExecutionControllerPeer::new(a.id(), endpoint.clone()),
            ExecutionControllerPeer::new(a.id(), endpoint.clone()),
        ],
    ] {
        if ExecutionAssignmentCoordinator::new(
            a.node.clone(),
            mesh.anchor.clone(),
            a.principal(),
            a.id(),
            peers,
        )
        .is_ok()
        {
            return Err("missing or duplicated proposer configuration was accepted".into());
        }
    }
    let before = mesh
        .peers
        .iter()
        .map(|peer| peer.node.events_after(None))
        .collect::<Result<Vec<_>, _>>()?;
    let foreign = ExecutionAssignment::new(
        CommandId::new(),
        ScopeId::new("foreign"),
        ScopeId::new("work"),
        ServiceId::new("records"),
        [NodeId::new()],
    );
    let coordinator = mesh.coordinator(a)?;
    if coordinator.assign(foreign).await.is_ok()
        || mesh
            .peers
            .iter()
            .map(|peer| peer.node.events_after(None))
            .collect::<Result<Vec<_>, _>>()?
            != before
    {
        return Err("foreign assignment caused a control request or history import".into());
    }
    drop(coordinator);
    drop(endpoint);
    mesh.shutdown().await
}
