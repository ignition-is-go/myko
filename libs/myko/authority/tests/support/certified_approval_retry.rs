use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration as StdDuration,
};

use super::*;

#[derive(Debug, Default)]
struct RecoveringEvidence(AtomicBool);

impl ScopedRetainedEvidenceEndpoint for RecoveringEvidence {
    fn refresh_scopes<'a>(&'a self, _scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async move {
            if self.0.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(RetainedEvidenceError::Unavailable(
                    AuthorityUnavailable::HistoryUnavailable,
                ))
            }
        })
    }
}

fn recovering_coordinator(
    a: &Node,
    b: &Node,
    evidence: Arc<RecoveringEvidence>,
) -> Result<AuthorityDecisionCoordinator, Box<dyn Error>> {
    let [a_key, b_key] = keys();
    let caller = Principal::node(PrincipalId::new("node:controller-a"));
    let binding = AuthorityControllerPrincipal::new(caller.clone(), controller_id(&a_key));
    let b_id = controller_id(&b_key);
    let endpoint =
        CertifiedAuthorityControlEndpoint::new(b.clone(), anchor()?, b_key, vec![binding.clone()])?
            .with_scoped_evidence_endpoint(evidence);
    let peers = vec![
        AuthorityCoordinatorPeer::local(
            a.clone(),
            anchor()?,
            a_key,
            caller.clone(),
            vec![binding.clone()],
        )?,
        AuthorityCoordinatorPeer::new(Arc::new(endpoint), caller, b_id, realm())
            .with_retained_node(b.clone()),
    ];
    Ok(AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        binding,
        peers,
    )?)
}

#[tokio::test]
async fn approved_command_recovers_quorum_without_another_wakeup() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let challenge = challenge(&a, &b, &approver).await?;
    let command_id = challenge.binding.command_id.ok_or("missing command")?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    a.await_prepared_authorization(command_id, saved.effect_digest(), challenge.id.clone())?;
    coordinator(&a, &b)?
        .approve(
            &approver.id,
            &AuthorityPresentation::direct(approver.clone()),
            &challenge.id,
            true,
        )
        .await
        .map_err(|failure| failure.public_message())?;
    let evidence = Arc::new(RecoveringEvidence::default());
    let (runtime, _policy) = myko_authority::certified::PreparedAuthorityRuntime::new(
        recovering_coordinator(&a, &b, evidence.clone())?,
        Arc::new(AllowAllAccessPolicy),
    );
    let (reported, reports) = flume::unbounded();
    let worker = tokio::spawn(runtime.run(move |result| {
        let _ = reported.send(result);
    }));
    let outcome = async {
        let first = tokio::time::timeout(StdDuration::from_secs(20), reports.recv_async())
            .await
            .map_err(|error| format!("no outage report: {error}"))??;
        require(
            first.is_err(),
            "unavailable controller unexpectedly allowed release",
        )?;
        require(
            !a.command(command_id)?
                .ok_or("missing command")?
                .state
                .is_committed(),
            "effect committed without quorum",
        )?;
        evidence.0.store(true, Ordering::SeqCst);
        tokio::time::timeout(StdDuration::from_secs(20), async {
            loop {
                if reports.recv_async().await??.state.is_committed() {
                    return Ok::<(), Box<dyn Error>>(());
                }
            }
        })
        .await
        .map_err(|error| format!("no recovery after quorum restored: {error}"))??;
        prepared_runtime::assert_exact_commit(&a, command_id, &saved)
    }
    .await;
    worker.abort();
    let _ = worker.await;
    outcome
}

#[tokio::test]
async fn waiting_for_approval_does_not_error_or_append_control_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let challenge = challenge(&a, &b, &approver).await?;
    let command_id = challenge.binding.command_id.ok_or("missing command")?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    let pending =
        a.await_prepared_authorization(command_id, saved.effect_digest(), challenge.id.clone())?;
    let coordinator = coordinator(&a, &b)?;
    let head = AuthorityHistory::replay(&a, anchor()?)?.retained_head()?;
    for _ in 0..2 {
        let current = coordinator.release_prepared(command_id).await?;
        require(current == pending, "waiting changed the parked command")?;
        require(
            AuthorityHistory::replay(&a, anchor()?)?.retained_head()? == head,
            "waiting appended an authority decision",
        )?;
    }
    Ok(())
}
