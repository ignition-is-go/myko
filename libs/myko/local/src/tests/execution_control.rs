use std::{error::Error, path::Path, sync::Arc};

use ed25519_dalek::SigningKey;
use myko::server::{ExecutionControlEndpoint, FederatedSession};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy as _, AuthorityPresentation, AuthorityUnavailable,
    CertifiedControlChain, CommandId, ControlAnchor, DelegationId, DenyAllAccessPolicy,
    EventEnvelope, ExecutionAssignment, ExecutionAssignmentController, ExecutionAssignmentsAtHead,
    FederationPermission, Node, NodeId, Principal, PrincipalId, PrincipalKind, ProvenanceHop,
    ProvenanceOperation, ScopeGrant, ScopeGrantCoverage, ScopeGrantPolicy, ScopeId, ServiceId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControlValue, ControllerId},
};
use myko_redb::RedbJournal;

use crate::{LocalCommandClient, LocalNodeServer, LocalPeerError};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn identity(key: &SigningKey) -> ControllerId {
    ControllerId(key.verifying_key().to_bytes())
}

fn caller() -> Principal {
    Principal::node(PrincipalId::new("controller:a"))
}

fn anchor(keys: &[SigningKey]) -> Result<ControlAnchor, String> {
    ControlAnchor::new(
        ScopeId::new("execution:mesh"),
        ControlEpochId([81; 32]),
        ControlHead([82; 32]),
        keys.iter().map(identity).collect(),
    )
}

fn value(anchor: &ControlAnchor, executor: NodeId) -> Result<ControlValue, String> {
    ExecutionAssignment::new(
        CommandId::new(),
        anchor.realm().clone(),
        ScopeId::new("work"),
        ServiceId::new("records"),
        [executor],
    )
    .transition()?
    .control_value()
}

fn assert_assignment(
    history: &[EventEnvelope],
    anchor: &ControlAnchor,
    executor: NodeId,
) -> TestResult {
    let chain = CertifiedControlChain::replay(history, anchor.clone())?;
    let chosen = ExecutionAssignmentsAtHead::replay(&chain, chain.retained_head()?)?;
    if chosen.head() == anchor.genesis()
        || chosen.exact(&ScopeId::new("work"), &ServiceId::new("records"))
            != Some(&[executor].into())
    {
        return Err("socket-issued majority did not establish the assignment".into());
    }
    Ok(())
}

async fn serve(
    node: &Node,
    socket: &Path,
    anchor: &ControlAnchor,
    key: SigningKey,
    proposer: ControllerId,
    authenticated: Principal,
) -> Result<LocalNodeServer, Box<dyn Error + Send + Sync>> {
    let endpoint = ExecutionControlEndpoint::new(
        node.clone(),
        anchor.clone(),
        key,
        vec![(caller(), proposer)],
    )?;
    let sessions = FederatedSession::new(node.clone(), Arc::new(DenyAllAccessPolicy));
    sessions.set_control_endpoint(anchor.realm().clone(), Some(Arc::new(endpoint)))?;
    Ok(LocalNodeServer::spawn_sessions_authenticated(socket, sessions, authenticated).await?)
}

#[test]
fn execution_controller_requires_unambiguous_caller_bindings() -> TestResult {
    let key = SigningKey::from_bytes(&[77; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    for callers in [
        vec![],
        vec![(caller(), identity(&key)), (caller(), identity(&key))],
        vec![
            (caller(), identity(&key)),
            (
                Principal::new(caller().id, PrincipalKind::Person),
                identity(&key),
            ),
        ],
    ] {
        if ExecutionControlEndpoint::new(Node::in_memory(), anchor.clone(), key.clone(), callers)
            .is_ok()
        {
            return Err("empty or ambiguous controller bindings were accepted".into());
        }
    }
    Ok(())
}

#[test]
fn application_scope_admin_does_not_grant_execution_control() -> TestResult {
    let scope = ScopeId::new("execution:mesh");
    let policy = ScopeGrantPolicy::new(vec![ScopeGrant {
        scope_id: scope.clone(),
        coverage: ScopeGrantCoverage::Subtree,
        grantee: caller().id,
        permissions: vec![FederationPermission::Admin],
    }]);
    let mut request = AccessAttempt::scoped(
        caller().id,
        AuthorityPresentation::direct(caller()),
        AccessOperation::AdministerAuthority,
        scope,
    );
    if policy
        .decide(&request)
        .into_immediate()?
        .into_permit()
        .is_err()
    {
        return Err("fixture did not grant application scope administration".into());
    }
    request.operation = AccessOperation::AdministerExecution;
    if policy
        .decide(&request)
        .into_immediate()?
        .into_permit()
        .is_ok()
    {
        return Err("application grant enrolled a controller".into());
    }
    Ok(())
}

#[tokio::test]
async fn socket_assignment_voting_requires_majority_and_survives_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let keys = [71, 72, 73].map(|seed| SigningKey::from_bytes(&[seed; 32]));
    let anchor = anchor(&keys)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: identity(&keys[0]),
    };
    let head = anchor.genesis();
    let target = anchor.target(head);
    let executor = NodeId::new();
    let value = value(&anchor, executor)?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b_path = directory.path().join("b.redb");
    let b = RedbJournal::open_node(&b_path)?;
    let a_socket = directory.path().join("a.sock");
    let b_socket = directory.path().join("b.sock");
    let a_server = serve(
        &a,
        &a_socket,
        &anchor,
        keys[0].clone(),
        ballot.proposer,
        caller(),
    )
    .await?;
    let b_server = serve(
        &b,
        &b_socket,
        &anchor,
        keys[1].clone(),
        ballot.proposer,
        caller(),
    )
    .await?;
    let a_client = LocalCommandClient::new(&a_socket);
    let b_client = LocalCommandClient::new(&b_socket);
    let a_promise = a_client.prepare_control(target.clone(), ballot).await?;
    let before = a.events_after(None)?;
    let minority = a_client
        .propose_control(
            target.clone(),
            ballot,
            vec![a_promise.clone()],
            value.clone(),
        )
        .await;
    if !matches!(
        minority,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) || a.events_after(None)? != before
    {
        return Err("minority proposal was not rejected without persistence".into());
    }
    let b_promise = b_client.prepare_control(target.clone(), ballot).await?;
    let proposal = a_client
        .propose_control(target.clone(), ballot, vec![a_promise, b_promise], value)
        .await?;
    a_client
        .accept_control(target.clone(), proposal.clone())
        .await?;
    if CertifiedControlChain::replay(&a.events_after(None)?, anchor.clone())?.retained_head()?
        != head
    {
        return Err("one of three acceptances established an assignment".into());
    }
    let acceptance = b_client
        .accept_control(target.clone(), proposal.clone())
        .await?;
    for event in a.events_after(None)? {
        b.ingest(event)?;
    }
    let history = b.events_after(None)?;
    assert_assignment(&history, &anchor, executor)?;
    a_server.shutdown().await?;
    b_server.shutdown().await?;
    drop(b);
    let reopened = RedbJournal::open_node(&b_path)?;
    let server = serve(
        &reopened,
        &b_socket,
        &anchor,
        keys[1].clone(),
        ballot.proposer,
        caller(),
    )
    .await?;
    let retried = LocalCommandClient::new(&b_socket)
        .accept_control(target.clone(), proposal)
        .await?;
    server.shutdown().await?;
    if retried != acceptance || reopened.events_after(None)? != history {
        return Err("socket acceptance did not survive reopen as an exact no-write retry".into());
    }
    assert_assignment(&reopened.events_after(None)?, &anchor, executor)?;
    Ok(())
}

#[tokio::test]
async fn socket_assignment_controller_denies_unbound_and_forged_callers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let key = SigningKey::from_bytes(&[74; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let node = RedbJournal::open_node(directory.path().join("controller.redb"))?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: identity(&key),
    };
    let head = anchor.genesis();
    let value = value(&anchor, NodeId::new())?;
    let controller = ExecutionAssignmentController::new(node.clone(), anchor.clone());
    let promise = controller.prepare(head, ballot, &key)?;
    let proposal =
        controller.propose(head, ballot, std::slice::from_ref(&promise), &value, &key)?;
    let intruder = Principal::node(PrincipalId::new("intruder"));
    let wrong_kind = Principal {
        id: caller().id,
        kind: PrincipalKind::Person,
    };
    let mut delegated = AuthorityPresentation::direct(caller());
    delegated.principal = intruder.clone();
    let mut forwarded = AuthorityPresentation::direct(caller());
    forwarded.provenance.push(ProvenanceHop {
        delegation_id: DelegationId::new("forwarded"),
        delegator: caller(),
        delegate: caller(),
        operation: ProvenanceOperation::NodeForward {
            node_id: "relay".to_owned(),
        },
    });
    let other = identity(&SigningKey::from_bytes(&[75; 32]));
    let cases = [
        (intruder.clone(), None, ballot.proposer),
        (wrong_kind, None, ballot.proposer),
        (
            caller(),
            Some(AuthorityPresentation::direct(intruder)),
            ballot.proposer,
        ),
        (caller(), Some(delegated), ballot.proposer),
        (caller(), Some(forwarded), ballot.proposer),
        (caller(), None, other),
    ];
    for (index, (authenticated, presentation, binding)) in cases.into_iter().enumerate() {
        let socket = directory.path().join(format!("denied-{index}.sock"));
        let server = serve(&node, &socket, &anchor, key.clone(), binding, authenticated).await?;
        let mut client = LocalCommandClient::new(&socket);
        if let Some(presentation) = presentation {
            client = client.with_authority(presentation);
        }
        let before = node.events_after(None)?;
        let prepare = client.prepare_control(anchor.target(head), ballot).await;
        let propose = client
            .propose_control(
                anchor.target(head),
                ballot,
                vec![promise.clone()],
                value.clone(),
            )
            .await;
        let accept = client
            .accept_control(anchor.target(head), proposal.clone())
            .await;
        server.shutdown().await?;
        if !matches!(prepare, Err(LocalPeerError::Authorization(_)))
            || !matches!(propose, Err(LocalPeerError::Authorization(_)))
            || !matches!(accept, Err(LocalPeerError::Authorization(_)))
            || node.events_after(None)? != before
        {
            return Err(format!("forged caller {index} was not denied before persistence").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn storage_participation_does_not_enable_control_voting() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("storage.redb"))?;
    let key = SigningKey::from_bytes(&[76; 32]);
    let anchor = anchor(std::slice::from_ref(&key))?;
    let socket = directory.path().join("storage.sock");
    let sessions = FederatedSession::new(node.clone(), Arc::new(DenyAllAccessPolicy));
    let server = LocalNodeServer::spawn_sessions_authenticated(&socket, sessions, caller()).await?;
    let result = LocalCommandClient::new(&socket)
        .prepare_control(
            anchor.target(anchor.genesis()),
            ControlBallot {
                counter: 1,
                proposer: identity(&key),
            },
        )
        .await;
    server.shutdown().await?;
    if !matches!(
        result,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) || !node.events_after(None)?.is_empty()
    {
        return Err("unconfigured storage node participated in control voting".into());
    }
    Ok(())
}
