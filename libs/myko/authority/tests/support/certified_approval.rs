use super::*;
use myko_federation::{AuthorityChallenge, Obligation, ObligationId};

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

async fn challenge(
    a: &Node,
    b: &Node,
    approver: &Principal,
) -> Result<AuthorityChallenge, Box<dyn Error>> {
    let [a_key, b_key] = keys();
    let (head, reader, scope) = install_obligated_grant(
        a,
        b,
        &anchor()?,
        &a_key,
        &b_key,
        ScopeId::new("coordinator:data"),
        Some(Obligation {
            id: ObligationId::new("approve-effect"),
            realm_id: realm(),
            challenge_kind: "approval".to_owned(),
            prompt: "Approve this effect?".to_owned(),
            approvers: vec![approver.clone()],
            approval_lifetime_seconds: 60,
            approval_use_count: 1,
        }),
    )?;
    let command_id = CommandId::new();
    let request = prepare_command_evidence(a, b, reader, scope, command_id)?;
    let coordinator = coordinator(a, b)?;
    let chosen = coordinator
        .decide(head, 2, CommandId::new(), command_id, request)
        .await?;
    match chosen.decision() {
        AuthorizationDecision::Challenge { challenge, .. } => Ok(challenge.clone()),
        other => Err(format!("expected certified challenge, got {other:?}").into()),
    }
}

fn reject_missing_or_expired_approval(
    a: &Node,
    challenge: &AuthorityChallenge,
    approver: &Principal,
) -> TestResult {
    let history = AuthorityHistory::replay(a, anchor()?)?;
    let head = history.retained_head()?;
    let root = AuthorityDecisionRoot::new(
        realm(),
        challenge.binding.command_id.ok_or("missing command")?,
        AuthorizationPhase::Effect,
    )?;
    require(
        history
            .plan_continuation_at(head, CommandId::new(), &root, Utc::now())
            .is_err(),
        "continued without approval",
    )?;
    require(
        history
            .plan_approval_at(
                head,
                CommandId::new(),
                &challenge.id,
                approver,
                true,
                challenge.expires_at,
            )
            .is_err(),
        "approved an expired challenge",
    )?;
    require(
        history
            .plan_approval_at(
                head,
                CommandId::new(),
                &challenge.id,
                approver,
                true,
                challenge
                    .issued_at
                    .checked_sub_signed(Duration::seconds(1))
                    .ok_or("time underflow")?,
            )
            .is_err(),
        "approved before challenge issuance",
    )
}

#[tokio::test]
async fn certified_approval_is_immutable_and_survives_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("a.redb");
    let b_path = directory.path().join("b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let challenge = challenge(&a, &b, &approver).await?;
    reject_missing_or_expired_approval(&a, &challenge, &approver)?;
    let presentation = AuthorityPresentation::direct(approver.clone());
    let command_id = challenge.binding.command_id.ok_or("missing command")?;
    let saved = prepared_runtime::saved_effect(&a, command_id)?;
    let digest = challenge
        .binding
        .effect_digest
        .as_deref()
        .ok_or("missing digest")?;
    a.await_prepared_authorization(command_id, digest, challenge.id.clone())?;
    let approval = coordinator(&a, &b)?
        .approve(&approver.id, &presentation, &challenge.id, true)
        .await?;
    require(
        approval.binding == challenge.binding && approval.max_uses == 1 && approval.approved,
        "approval changed binding or usage limit",
    )?;
    let root = AuthorityDecisionRoot::new(
        realm(),
        challenge.binding.command_id.ok_or("missing command")?,
        AuthorizationPhase::Effect,
    )?;
    let resumed = coordinator(&a, &b)?
        .continue_prepared(root.request_id())
        .await?;
    require(
        resumed.decision().is_permit() && resumed.binding() == &challenge.binding,
        "continuation did not permit the exact effect",
    )?;
    let history = AuthorityHistory::replay(&a, anchor()?)?;
    let resumed_head = history.retained_head()?;
    require(
        history.decision_at(resumed_head, &root)? == Some(resumed),
        "continuation was not retained",
    )?;
    require(
        history
            .plan_continuation_at(resumed_head, CommandId::new(), &root, Utc::now())
            .is_err(),
        "terminal root allowed another consuming round",
    )?;
    require(
        history
            .plan_revalidation_at(resumed_head, CommandId::new(), &root, Utc::now())?
            .decision()
            .is_permit(),
        "revalidation consumed the approval again",
    )?;
    drop(a);
    drop(b);
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let coordinator = coordinator(&a, &b)?;
    let before = a.local_history_cut()?;
    require(
        coordinator
            .approve(&approver.id, &presentation, &challenge.id, true)
            .await?
            == approval,
        "reopen changed approval or expiry",
    )?;
    require(
        coordinator
            .approve(&approver.id, &presentation, &challenge.id, false)
            .await
            .is_err(),
        "contradictory approval retry succeeded",
    )?;
    require(
        a.local_history_cut()? == before,
        "retry wrote another approval",
    )?;
    coordinator.release_prepared(root.request_id()).await?;
    prepared_runtime::assert_exact_commit(&a, command_id, &saved)?;
    require(
        coordinator
            .release_prepared(root.request_id())
            .await
            .is_err(),
        "committed effect was released again",
    )?;
    Ok(())
}

#[tokio::test]
async fn certified_approval_rejects_impersonation_and_unauthorized_approvers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let stranger = Principal::node(PrincipalId::new("stranger"));
    let challenge = challenge(&a, &b, &approver).await?;
    let coordinator = coordinator(&a, &b)?;
    let before = a.local_history_cut()?;
    require(
        coordinator
            .approve(
                &stranger.id,
                &AuthorityPresentation::direct(approver),
                &challenge.id,
                true,
            )
            .await
            .is_err(),
        "impersonation accepted",
    )?;
    require(
        coordinator
            .approve(
                &stranger.id,
                &AuthorityPresentation::direct(stranger.clone()),
                &challenge.id,
                true,
            )
            .await
            .is_err(),
        "unauthorized approver accepted",
    )?;
    require(
        a.local_history_cut()? == before,
        "invalid approval wrote history",
    )?;
    let history = AuthorityHistory::replay(&a, anchor()?)?;
    let head = history.retained_head()?;
    let valid = history.plan_approval_at(
        head,
        CommandId::new(),
        &challenge.id,
        &challenge.binding.principal,
        true,
        Utc::now(),
    );
    require(valid.is_err(), "planner accepted an unauthorized approver")?;
    Ok(())
}

#[tokio::test]
async fn certified_approval_controllers_reject_forged_binding() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let challenge = challenge(&a, &b, &approver).await?;
    let history = AuthorityHistory::replay(&a, anchor()?)?;
    let head = history.retained_head()?;
    let planned = history.plan_approval_at(
        head,
        CommandId::new(),
        &challenge.id,
        &approver,
        true,
        Utc::now(),
    )?;
    let mut payload = serde_json::to_value(&planned)?;
    *payload
        .pointer_mut("/decision/binding/effect_digest")
        .ok_or("missing digest field")? = serde_json::json!("different-effect");
    let forged: myko_authority::certified::AuthorityApprovalTransition =
        serde_json::from_value(payload)?;
    let [a_key, b_key] = keys();
    require(
        choose_selection_with_anchor(
            &a,
            &b,
            anchor()?,
            &a_key,
            &b_key,
            head,
            &forged.control_value()?,
        )
        .is_err(),
        "controllers certified a forged effect binding",
    )?;
    require(
        AuthorityHistory::replay(&a, anchor()?)?.retained_head()? == head,
        "forgery changed the certified head",
    )?;
    Ok(())
}

#[tokio::test]
async fn certified_approval_does_not_release_a_revoked_pending_effect() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let approver = Principal::node(PrincipalId::new("approver"));
    let challenge = challenge(&a, &b, &approver).await?;
    let command_id = challenge.binding.command_id.ok_or("missing command")?;
    let digest = challenge
        .binding
        .effect_digest
        .as_deref()
        .ok_or("missing digest")?;
    a.await_prepared_authorization(command_id, digest, challenge.id.clone())?;
    let coordinator = coordinator(&a, &b)?;
    coordinator
        .approve(
            &approver.id,
            &AuthorityPresentation::direct(approver.clone()),
            &challenge.id,
            true,
        )
        .await?;
    let head = AuthorityHistory::replay(&a, anchor()?)?.retained_head()?;
    certify_grant_revocation(&a, &b, head)?;
    let rejected = coordinator.release_prepared(command_id).await?;
    require(
        matches!(
            rejected.state,
            myko_federation::CommandState::Rejected { .. }
        ),
        "revoked pending effect was not rejected",
    )?;
    require(!a.events_after(None)?.iter().any(|event| matches!(&event.event, NodeEvent::CommandCommitted { command, .. } if command.request.id == command_id)), "revoked effect committed")
}
