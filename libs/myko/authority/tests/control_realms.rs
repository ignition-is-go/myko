use std::{error::Error, sync::Arc};

use ed25519_dalek::SigningKey;
use myko::server::{ExecutionControlEndpoint, FederatedSession};
use myko_authority::certified::{
    AuthorityAnchor, AuthorityControllerPrincipal, CertifiedAuthorityControlEndpoint,
};
use myko_federation::{
    AuthorityRealmId, AuthorityUnavailable, CertifiedControlChain, CommandId, ControlAnchor,
    DenyAllAccessPolicy, ExecutionAssignment, ExecutionAssignmentsAtHead, Node, NodeId, Principal,
    PrincipalId, ScopeId, ServiceId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControllerId},
};
use myko_local::{LocalCommandClient, LocalNodeServer, LocalPeerError};
use myko_redb::RedbJournal;

type TestResult = Result<(), Box<dyn Error>>;

fn principal() -> Principal {
    Principal::node(PrincipalId::new("controller"))
}

fn anchors(key: &SigningKey) -> Result<(AuthorityAnchor, ControlAnchor, ControlBallot), String> {
    let proposer = ControllerId(key.verifying_key().to_bytes());
    let epoch = ControlEpochId([81; 32]);
    let head = ControlHead([82; 32]);
    Ok((
        AuthorityAnchor::new(AuthorityRealmId::new("access"), epoch, head, vec![proposer])?,
        ControlAnchor::new(ScopeId::new("execution"), epoch, head, vec![proposer])?,
        ControlBallot {
            counter: 1,
            proposer,
        },
    ))
}

fn install(
    node: &Node,
    key: &SigningKey,
    authority: &AuthorityAnchor,
    execution: &ControlAnchor,
) -> Result<FederatedSession, String> {
    let sessions = FederatedSession::new(node.clone(), Arc::new(DenyAllAccessPolicy));
    let controller = ControllerId(key.verifying_key().to_bytes());
    sessions.set_control_endpoint(
        authority.target(authority.genesis()).realm,
        Some(Arc::new(CertifiedAuthorityControlEndpoint::new(
            node.clone(),
            authority.clone(),
            key.clone(),
            vec![AuthorityControllerPrincipal::new(principal(), controller)],
        )?)),
    )?;
    sessions.set_control_endpoint(
        execution.realm().clone(),
        Some(Arc::new(ExecutionControlEndpoint::new(
            node.clone(),
            execution.clone(),
            key.clone(),
            vec![(principal(), controller)],
        )?)),
    )?;
    Ok(sessions)
}

#[tokio::test]
async fn one_session_routes_colliding_heads_to_independent_control_realms() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("control.redb"))?;
    let key = SigningKey::from_bytes(&[83; 32]);
    let (authority, execution, ballot) = anchors(&key)?;
    let sessions = install(&node, &key, &authority, &execution)?;
    let socket = directory.path().join("control.sock");
    let server =
        LocalNodeServer::spawn_sessions_authenticated(&socket, sessions.clone(), principal())
            .await?;
    let client = LocalCommandClient::new(&socket);
    let authority_target = authority.target(authority.genesis());
    let execution_target = execution.target(execution.genesis());
    let authority_vote = client
        .prepare_control(authority_target.clone(), ballot)
        .await?;
    let execution_vote = client
        .prepare_control(execution_target.clone(), ballot)
        .await?;
    if authority_vote.message.slot.realm != authority_target.realm
        || execution_vote.message.slot.realm != execution_target.realm
        || authority_vote == execution_vote
        || node.events_after(None)?.len() != 2
    {
        return Err("colliding heads were not isolated by realm".into());
    }
    let executor = NodeId::new();
    let value = ExecutionAssignment::new(
        CommandId::new(),
        execution.realm().clone(),
        ScopeId::new("work"),
        ServiceId::new("records"),
        [executor],
    )
    .transition()?
    .control_value()?;
    let proposal = client
        .propose_control(
            execution_target.clone(),
            ballot,
            vec![execution_vote],
            value.clone(),
        )
        .await?;
    let before = node.events_after(None)?;
    if client
        .propose_control(
            authority_target.clone(),
            ballot,
            vec![authority_vote],
            value,
        )
        .await
        .is_ok()
        || client
            .accept_control(authority_target.clone(), proposal.clone())
            .await
            .is_ok()
        || node.events_after(None)? != before
    {
        return Err("execution proposal crossed into application authority history".into());
    }
    client
        .accept_control(execution_target.clone(), proposal.clone())
        .await?;
    let chain = CertifiedControlChain::replay(&node.events_after(None)?, execution.clone())?;
    let assignment = ExecutionAssignmentsAtHead::replay(&chain, chain.retained_head()?)?;
    if assignment.exact(&ScopeId::new("work"), &ServiceId::new("records"))
        != Some(&[executor].into())
    {
        return Err("selected execution controller did not establish its assignment".into());
    }
    let next_authority_vote = client
        .prepare_control(
            authority_target.clone(),
            ControlBallot {
                counter: 2,
                proposer: ballot.proposer,
            },
        )
        .await?;
    if next_authority_vote.message.slot.realm != authority_target.realm {
        return Err("chosen execution history changed authority realm routing".into());
    }
    sessions.set_control_endpoint(authority_target.realm.clone(), None)?;
    let before = node.events_after(None)?;
    let removed = client.prepare_control(authority_target, ballot).await;
    if !matches!(
        removed,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) {
        return Err("removed realm fell back to another installed controller".into());
    }
    client.accept_control(execution_target, proposal).await?;
    if node.events_after(None)? != before {
        return Err("realm removal or exact retry appended unexpected history".into());
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unknown_and_misregistered_realms_never_write_controller_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("control.redb"))?;
    let key = SigningKey::from_bytes(&[84; 32]);
    let (authority, execution, ballot) = anchors(&key)?;
    let sessions = install(&node, &key, &authority, &execution)?;
    let socket = directory.path().join("control.sock");
    let server =
        LocalNodeServer::spawn_sessions_authenticated(&socket, sessions.clone(), principal())
            .await?;
    let client = LocalCommandClient::new(&socket);
    let mut target = execution.target(execution.genesis());
    target.realm = ScopeId::new("alias");
    let unknown = client.prepare_control(target.clone(), ballot).await;
    if !matches!(
        unknown,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) {
        return Err("unknown realm used another controller".into());
    }
    sessions.set_control_endpoint(
        target.realm.clone(),
        Some(Arc::new(ExecutionControlEndpoint::new(
            node.clone(),
            execution,
            key.clone(),
            vec![(principal(), ballot.proposer)],
        )?)),
    )?;
    let misregistered = client.prepare_control(target.clone(), ballot).await;
    if !matches!(
        misregistered,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) {
        return Err("execution endpoint accepted an alias outside its anchor".into());
    }
    sessions.set_control_endpoint(
        target.realm.clone(),
        Some(Arc::new(CertifiedAuthorityControlEndpoint::new(
            node.clone(),
            authority,
            key,
            vec![AuthorityControllerPrincipal::new(
                principal(),
                ballot.proposer,
            )],
        )?)),
    )?;
    let misregistered = client.prepare_control(target, ballot).await;
    server.shutdown().await?;
    if !matches!(
        misregistered,
        Err(LocalPeerError::AuthorityUnavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) || !node.events_after(None)?.is_empty()
    {
        return Err(
            "authority endpoint accepted an alias outside its anchor or wrote history".into(),
        );
    }
    Ok(())
}
