use std::time::Duration as StdDuration;

use myko_authority::certified::PreparedAuthorityRuntime;
use myko_federation::{ApprovalId, ChallengeId, CommandSnapshot, CommandState};

use super::*;

async fn challenges(
    a: &Node,
    b: &Node,
    approver: &Principal,
    names: &[&str],
) -> Result<AuthorityChallenge, Box<dyn Error>> {
    let [a_key, b_key] = keys();
    let obligations = names.iter().copied().map(|id| Obligation {
        id: ObligationId::new(id),
        realm_id: realm(),
        challenge_kind: "approval".to_owned(),
        prompt: id.to_owned(),
        approvers: vec![approver.clone()],
        approval_lifetime_seconds: 300,
        approval_use_count: 1,
    });
    let (head, reader, scope) = install_obligated_grant(
        a,
        b,
        &anchor()?,
        &a_key,
        &b_key,
        ScopeId::new("multiple:data"),
        obligations,
    )?;
    let command_id = CommandId::new();
    let request = prepare_command_evidence(a, b, reader, scope, command_id)?;
    let coordinator = coordinator(a, b)?;
    let chosen = coordinator
        .decide(head, 2, CommandId::new(), command_id, request)
        .await?;
    match chosen.decision() {
        AuthorizationDecision::Challenge { challenge, .. } => Ok(challenge.clone()),
        other => Err(format!("expected first challenge, got {other:?}").into()),
    }
}

fn next_challenge(
    command: &CommandSnapshot,
    previous: &ChallengeId,
    approval: &ApprovalId,
) -> Result<ChallengeId, Box<dyn Error>> {
    match &command.state {
        CommandState::AuthorizationPending {
            challenge_id,
            approvals,
            ..
        } if challenge_id != previous && approvals.contains(approval) => Ok(challenge_id.clone()),
        other => {
            Err(format!("expected next challenge with certified approval, got {other:?}").into())
        }
    }
}

async fn exercise_multiple(recover_chosen: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let first = challenges(&a, &b, &approver, &["first", "second"]).await?;
    let command_id = first.binding.command_id.ok_or("missing command")?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    a.await_prepared_authorization(command_id, saved.effect_digest(), first.id.clone())?;
    let initial_coordinator = coordinator(&a, &b)?;
    let approval = initial_coordinator
        .approve(
            &approver.id,
            &AuthorityPresentation::direct(approver.clone()),
            &first.id,
            true,
        )
        .await
        .map_err(|failure| failure.public_message())?;
    if recover_chosen {
        let chosen = initial_coordinator.continue_prepared(command_id).await?;
        require(
            matches!(chosen.decision(), AuthorizationDecision::Challenge { challenge, .. } if challenge.id != first.id),
            "continuation did not certify the second challenge",
        )?;
        require(
            matches!(a.command(command_id)?.ok_or("missing command")?.state,
            CommandState::AuthorizationPending { challenge_id, .. } if challenge_id == first.id),
            "certification unexpectedly advanced local command",
        )?;
    }
    drop(initial_coordinator);
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    let (reported, reports) = flume::unbounded();
    let worker = tokio::spawn(runtime.run(move |result| {
        let _ = reported.send(result);
    }));
    let outcome = async {
        let pending =
            tokio::time::timeout(StdDuration::from_secs(45), reports.recv_async()).await???;
        let second = next_challenge(&pending, &first.id, &approval.id)?;
        require(
            matches!(&pending.state, CommandState::AuthorizationPending { batch, result, .. }
                if batch.as_ref() == saved.batch() && result == saved.result()),
            "advancement changed saved effect",
        )?;
        policy
            .approve(
                &approver.id,
                &AuthorityPresentation::direct(approver.clone()),
                &second,
                true,
            )
            .await
            .map_err(|failure| failure.public_message())?;
        tokio::time::timeout(StdDuration::from_secs(45), async {
            loop {
                if reports.recv_async().await??.state.is_committed() {
                    return Ok::<(), Box<dyn Error>>(());
                }
            }
        })
        .await??;
        prepared_runtime::assert_exact_commit(&a, command_id, &saved)
    }
    .await
    .map_err(|error| error.to_string());
    worker.abort();
    let _ = worker.await;
    outcome.map_err(Into::into)
}

#[tokio::test]
async fn runtime_advances_multiple_approvals_before_committing() -> TestResult {
    exercise_multiple(false).await
}

#[tokio::test]
async fn runtime_recovers_chosen_challenge_after_reopen() -> TestResult {
    exercise_multiple(true).await
}

#[tokio::test]
async fn recovery_records_each_skipped_challenge_and_preserves_identity_guard() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let first = challenges(&a, &b, &approver, &["first", "second", "third"]).await?;
    let command_id = first.binding.command_id.ok_or("missing command")?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    let pending =
        a.await_prepared_authorization(command_id, saved.effect_digest(), first.id.clone())?;
    require(
        a.await_prepared_authorization(
            command_id,
            saved.effect_digest(),
            ChallengeId::new("unrelated"),
        )
        .is_err(),
        "initial parking API allowed a different challenge to replace the current one",
    )?;
    let mut current = first;
    let mut recorded_ids = Vec::new();
    for _ in 0..2 {
        let coordinator = coordinator(&a, &b)?;
        let approval = coordinator
            .approve(
                &approver.id,
                &AuthorityPresentation::direct(approver.clone()),
                &current.id,
                true,
            )
            .await
            .map_err(|failure| failure.public_message())?;
        recorded_ids.push(approval.id);
        let chosen = coordinator.continue_prepared(command_id).await?;
        current = match chosen.decision() {
            AuthorizationDecision::Challenge { challenge, .. } => challenge.clone(),
            other => return Err(format!("expected another challenge, got {other:?}").into()),
        };
    }
    require(
        a.command(command_id)?.as_ref() == Some(&pending),
        "certifying rounds unexpectedly advanced local pending state",
    )?;
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let coordinator = coordinator(&a, &b)?;
    let advanced = coordinator.release_prepared(command_id).await?;
    require(
        matches!(&advanced.state, CommandState::AuthorizationPending { challenge_id, batch, result, approvals }
        if challenge_id == &current.id && approvals == &recorded_ids && batch.as_ref() == saved.batch() && result == saved.result()),
        "recovery skipped approval evidence or changed the saved effect",
    )?;
    require(
        coordinator.release_prepared(command_id).await? == advanced,
        "retry appended duplicate challenge advancement",
    )?;
    coordinator
        .approve(
            &approver.id,
            &AuthorityPresentation::direct(approver.clone()),
            &current.id,
            true,
        )
        .await
        .map_err(|failure| failure.public_message())?;
    require(
        coordinator
            .release_prepared(command_id)
            .await?
            .state
            .is_committed(),
        "last approval did not release the saved effect",
    )?;
    prepared_runtime::assert_exact_commit(&a, command_id, &saved)
}
