use std::{error::Error, path::Path, sync::Arc};

use ed25519_dalek::SigningKey;
use myko::server::{ExecutionControlEndpoint, ScopedRetainedEvidenceEndpoint as _};
use myko_federation::{
    AuthorityPresentation, AuthorityUnavailable, CertifiedControlChain, CommandId, ControlAnchor,
    DenyAllAccessPolicy, ExecutionAssignment, ExecutionAssignmentController,
    ExecutionAssignmentsAtHead, FederationPermission, Node, NodeId, Principal, PrincipalId,
    ScopeGrant, ScopeGrantCoverage, ScopeGrantPolicy, ScopeId, ServiceId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControllerId},
};
use myko_iroh::{
    IrohReplicationError, IrohReplicator, IrohScopedEvidenceEndpoint, endpoint_principal_id,
};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

struct Controllers {
    a: Node,
    b: Node,
    a_transport: IrohReplicator,
    b_transport: IrohReplicator,
    a_key: SigningKey,
    b_key: SigningKey,
    anchor: ControlAnchor,
}

impl Controllers {
    async fn open(directory: &Path) -> Result<Self, Box<dyn Error>> {
        let a = RedbJournal::open_node(directory.join("a.redb"))?;
        let b = RedbJournal::open_node(directory.join("b.redb"))?;
        let a_key = SigningKey::from_bytes(&[91; 32]);
        let b_key = SigningKey::from_bytes(&[92; 32]);
        let anchor = ControlAnchor::new(
            ScopeId::new("execution:mesh"),
            ControlEpochId([93; 32]),
            ControlHead([94; 32]),
            vec![identity(&a_key), identity(&b_key)],
        )?;
        let a_transport = IrohReplicator::bind_loopback(a.clone()).await?;
        let b_transport = IrohReplicator::bind_loopback(b.clone()).await?;
        let pair = Self {
            a,
            b,
            a_transport,
            b_transport,
            a_key,
            b_key,
            anchor,
        };
        pair.allow_history()?;
        pair.b_transport.sessions().set_control_endpoint(
            pair.anchor.realm().clone(),
            Some(Arc::new(pair.b_controller()?)),
        )?;
        Ok(pair)
    }

    fn caller(&self) -> Principal {
        Principal::node(endpoint_principal_id(self.a_transport.address().id))
    }

    fn b_controller(&self) -> Result<ExecutionControlEndpoint, String> {
        ExecutionControlEndpoint::new(
            self.b.clone(),
            self.anchor.clone(),
            self.b_key.clone(),
            vec![(self.caller(), identity(&self.a_key))],
        )
    }

    fn source_for_b(&self) -> Arc<IrohScopedEvidenceEndpoint> {
        Arc::new(IrohScopedEvidenceEndpoint::new(
            self.b_transport.clone(),
            self.a_transport.address(),
        ))
    }

    fn allow_history(&self) -> Result<(), IrohReplicationError> {
        for (server, reader) in [
            (&self.a_transport, &self.b_transport),
            (&self.b_transport, &self.a_transport),
        ] {
            server.set_access_policy(Arc::new(ScopeGrantPolicy::new(vec![ScopeGrant {
                scope_id: self.anchor.realm().clone(),
                coverage: ScopeGrantCoverage::Exact,
                grantee: endpoint_principal_id(reader.address().id),
                permissions: vec![FederationPermission::ReadHistory],
            }])))?;
        }
        Ok(())
    }

    async fn choose_initial(&self, executor: NodeId) -> Result<ControlHead, Box<dyn Error>> {
        let local = ExecutionAssignmentController::new(self.a.clone(), self.anchor.clone());
        let remote = self.a_transport.command_client(self.b_transport.address());
        let head = self.anchor.genesis();
        let ballot = ControlBallot {
            counter: 1,
            proposer: identity(&self.a_key),
        };
        let promises = [
            local.prepare(head, ballot, &self.a_key)?,
            remote
                .prepare_control(self.anchor.target(head), ballot)
                .await?,
        ];
        let value = ExecutionAssignment::new(
            CommandId::new(),
            self.anchor.realm().clone(),
            ScopeId::new("work"),
            ServiceId::new("records"),
            [executor],
        )
        .transition()?
        .control_value()?;
        let proposal = local.propose(head, ballot, &promises, &value, &self.a_key)?;
        remote
            .accept_control(self.anchor.target(head), proposal.clone())
            .await?;
        local.accept(head, &proposal, &self.a_key)?;
        IrohScopedEvidenceEndpoint::new(self.a_transport.clone(), self.b_transport.address())
            .refresh_scopes(std::slice::from_ref(self.anchor.realm()))
            .await?;
        let head = retained_head(&self.a, &self.anchor)?;
        if head == self.anchor.genesis()
            || retained_head(&self.b, &self.anchor)? != self.anchor.genesis()
        {
            return Err(
                "fixture did not leave only the proposer with complete chosen evidence".into(),
            );
        }
        Ok(head)
    }

    async fn shutdown(self) -> TestResult {
        self.a_transport.shutdown().await?;
        self.b_transport.shutdown().await?;
        Ok(())
    }
}

fn identity(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn retained_head(node: &Node, anchor: &ControlAnchor) -> Result<ControlHead, Box<dyn Error>> {
    Ok(
        CertifiedControlChain::replay(&node.events_after(None)?, anchor.clone())?
            .retained_head()?,
    )
}

#[tokio::test]
async fn assignment_controller_pulls_authenticated_realm_evidence_before_advancing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let pair = Controllers::open(directory.path()).await?;
    let executor = NodeId::new();
    let head = pair.choose_initial(executor).await?;
    let client = pair.a_transport.command_client(pair.b_transport.address());
    let ballot = ControlBallot {
        counter: 1,
        proposer: identity(&pair.a_key),
    };
    let before = pair.b.events_after(None)?;
    let missing = client
        .prepare_control(pair.anchor.target(head), ballot)
        .await;
    if !matches!(
        missing,
        Err(IrohReplicationError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) || pair.b.events_after(None)? != before
    {
        return Err("controller without complete predecessor evidence advanced".into());
    }
    let endpoint = pair
        .b_controller()?
        .with_scoped_evidence_endpoint(pair.caller().id, pair.source_for_b())?;
    pair.b_transport
        .sessions()
        .set_control_endpoint(pair.anchor.realm().clone(), Some(Arc::new(endpoint)))?;
    let mut delegated = AuthorityPresentation::direct(pair.caller());
    delegated.principal = Principal::node(PrincipalId::new("intruder"));
    let rejected = client
        .clone()
        .with_authority(delegated)
        .prepare_control(pair.anchor.target(head), ballot)
        .await;
    if !matches!(rejected, Err(IrohReplicationError::Authorization { .. }))
        || pair.b.events_after(None)? != before
    {
        return Err("unauthorized caller caused an evidence pull or vote".into());
    }
    let foreign_anchor = ControlAnchor::new(
        ScopeId::new("unshared"),
        ControlEpochId([95; 32]),
        ControlHead([96; 32]),
        vec![identity(&pair.a_key)],
    )?;
    ExecutionAssignmentController::new(pair.a.clone(), foreign_anchor.clone()).prepare(
        foreign_anchor.genesis(),
        ballot,
        &pair.a_key,
    )?;
    let foreign = pair
        .a
        .events_after(None)?
        .last()
        .cloned()
        .ok_or("missing foreign record")?;
    let promise = client
        .prepare_control(pair.anchor.target(head), ballot)
        .await?;
    if promise.message.slot.predecessor != head || pair.b.events_after(None)?.contains(&foreign) {
        return Err("evidence refresh used the wrong predecessor or imported another realm".into());
    }
    let chain = CertifiedControlChain::replay(&pair.b.events_after(None)?, pair.anchor.clone())?;
    let assignments = ExecutionAssignmentsAtHead::replay(&chain, head)?;
    if assignments.exact(&ScopeId::new("work"), &ServiceId::new("records"))
        != Some(&[executor].into())
    {
        return Err("refreshed evidence did not preserve the chosen assignment".into());
    }
    pair.a_transport
        .set_access_policy(Arc::new(DenyAllAccessPolicy))?;
    let before = pair.b.events_after(None)?;
    let later = ControlBallot {
        counter: 2,
        ..ballot
    };
    let unavailable = client
        .prepare_control(pair.anchor.target(head), later)
        .await;
    if !matches!(
        unavailable,
        Err(IrohReplicationError::AuthorityUnavailable(
            AuthorityUnavailable::HistoryUnavailable
        ))
    ) || pair.b.events_after(None)? != before
    {
        return Err("failed configured refresh fell back to cached evidence".into());
    }
    pair.allow_history()?;
    client
        .prepare_control(pair.anchor.target(head), later)
        .await?;
    drop(client);
    pair.shutdown().await
}

#[tokio::test]
async fn execution_evidence_bindings_require_known_unique_callers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let pair = Controllers::open(directory.path()).await?;
    let source = pair.source_for_b();
    if pair
        .b_controller()?
        .with_scoped_evidence_endpoint(PrincipalId::new("unknown"), source.clone())
        .is_ok()
    {
        return Err("unknown caller acquired an evidence source".into());
    }
    let endpoint = pair
        .b_controller()?
        .with_scoped_evidence_endpoint(pair.caller().id, source.clone())?;
    if endpoint
        .with_scoped_evidence_endpoint(pair.caller().id, source)
        .is_ok()
    {
        return Err("duplicate caller evidence binding replaced the existing source".into());
    }
    pair.shutdown().await
}
