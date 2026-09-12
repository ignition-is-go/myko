use myko_federation::{
    CommandId, ControlTransition, ExecutionAssignmentController, ExecutionAssignmentObservation,
    NodeId, ScopeId, ServiceId, control_quorum::ControlBallot,
};

use super::{Mesh, TestResult};

#[tokio::test]
async fn a_fresh_observation_never_reuses_a_retained_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, c] = &mesh.peers;
    let coordinator = mesh.coordinator(a)?;
    let executor = NodeId::new();
    let receipt = coordinator
        .assign(mesh.assignment(CommandId::new(), executor))
        .await?;
    let first = coordinator.observe().await?;
    let second = coordinator.observe().await?;
    if first.head() == receipt.head()
        || second.head() == first.head()
        || second.exact(&ScopeId::new("work"), &ServiceId::new("records"))
            != Some(&[executor].into())
    {
        return Err("fresh observation reused an old result or changed the executor set".into());
    }
    for peer in [b, c] {
        peer.transport
            .sessions()
            .set_control_endpoint(mesh.anchor.realm().clone(), None)?;
    }
    if coordinator.observe().await.is_ok() {
        return Err("cached assignments bypassed a missing observation quorum".into());
    }
    if mesh.history(a)?.retained_head()? != second.head() {
        return Err("failed observation changed the chosen assignment history".into());
    }
    drop(coordinator);
    mesh.shutdown().await
}

#[tokio::test]
async fn stale_observer_refreshes_replacement_before_certifying_its_observation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, c] = &mesh.peers;
    let coordinator = mesh.coordinator(a)?;
    coordinator
        .assign(mesh.assignment(CommandId::new(), NodeId::new()))
        .await?;
    c.transport
        .sessions()
        .set_control_endpoint(mesh.anchor.realm().clone(), None)?;
    let replacement_executor = NodeId::new();
    let replacement = mesh
        .coordinator(b)?
        .assign(mesh.assignment(CommandId::new(), replacement_executor))
        .await?;
    if mesh.history(c)?.retained_head()? == replacement.head() {
        return Err("fixture did not leave the observer behind the replacement".into());
    }
    let reader = mesh.coordinator(c)?;
    let observed = reader.observe().await?;
    if observed.head() == replacement.head()
        || observed.exact(&ScopeId::new("work"), &ServiceId::new("records"))
            != Some(&[replacement_executor].into())
    {
        return Err("observation failed to certify the replacement executor set".into());
    }
    let chain = mesh.history(c)?;
    if chain.transitions_to(observed.head())?.len() != 3 {
        return Err("observation did not follow both assignment operations".into());
    }
    drop(reader);
    drop(coordinator);
    mesh.shutdown().await
}

async fn accept_before_proposer_loss(mesh: &Mesh, transition: &ControlTransition) -> TestResult {
    let [a, b, _] = &mesh.peers;
    let controller = ExecutionAssignmentController::new(a.node.clone(), mesh.anchor.clone());
    let target = mesh.anchor.target(mesh.anchor.genesis());
    let ballot = ControlBallot {
        counter: 7,
        proposer: a.id(),
    };
    let client = a.transport.command_client(b.transport.address());
    let promises = [
        controller.prepare(target.head, ballot, &a.key)?,
        client.prepare_control(target.clone(), ballot).await?,
    ];
    let proposal = controller.propose(
        target.head,
        ballot,
        &promises,
        &transition.control_value()?,
        &a.key,
    )?;
    client.accept_control(target, proposal).await?;
    drop(client);
    a.transport.clone().shutdown().await?;
    if mesh.history(b)?.retained_head()? != mesh.anchor.genesis() {
        return Err("interruption fixture already contained a chosen operation".into());
    }
    Ok(())
}

#[tokio::test]
async fn observation_finishes_an_accepted_assignment_before_reading_the_set() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let executor = NodeId::new();
    let pending = mesh.assignment(CommandId::new(), executor).transition()?;
    accept_before_proposer_loss(&mesh, &pending).await?;
    let [_, b, c] = &mesh.peers;
    let observed = mesh.coordinator(b)?.observe().await?;
    if observed.exact(&ScopeId::new("work"), &ServiceId::new("records")) != Some(&[executor].into())
    {
        return Err("fresh observation ignored an earlier accepted assignment".into());
    }
    let chain = mesh.history(b)?;
    let operations = chain.transitions_to(observed.head())?;
    if operations.len() != 2
        || operations.first().map(|op| op.operation()) != Some(pending.operation())
    {
        return Err("observation was not ordered after the recovered assignment".into());
    }
    b.transport.clone().shutdown().await?;
    c.transport.clone().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn recovering_an_old_observation_does_not_satisfy_a_new_call() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let pending = ExecutionAssignmentObservation::new(
        CommandId::new(),
        mesh.anchor.realm().clone(),
        mesh.anchor.genesis(),
    )
    .transition()?;
    accept_before_proposer_loss(&mesh, &pending).await?;
    let [_, b, c] = &mesh.peers;
    let observed = mesh.coordinator(b)?.observe().await?;
    let chain = mesh.history(b)?;
    let old = chain
        .operation_evidence_at(observed.head(), pending.operation())?
        .ok_or("old observation was not recovered")?;
    if old.head() == observed.head() || chain.transitions_to(observed.head())?.len() != 2 {
        return Err("old observation satisfied a new invocation".into());
    }
    b.transport.clone().shutdown().await?;
    c.transport.clone().shutdown().await?;
    Ok(())
}
