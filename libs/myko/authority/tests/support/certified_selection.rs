use super::*;

fn record_revocation(node: &Node) -> Result<AuthoritySelection, Box<dyn Error>> {
    let policy = Arc::new(AuthorityPolicy::new(
        ApplicationHost::new(
            node.clone(),
            AuthorityPolicy::install(MykoApplication::new())?,
        )?,
        realm(),
    ));
    node.set_command_access_policy(policy.clone())?;
    let before = node.local_history_cut()?;
    let admin = Principal::node(PrincipalId::new("admin"));
    policy.revoke(
        admin.clone(),
        AuthorityPresentation::direct(admin),
        RevocationKind::Grant,
        "single-use".to_owned(),
    )?;
    Ok(AuthoritySelection::new(
        CommandId::new(),
        &node.events_after(before)?,
    )?)
}

fn read_attempt(reader: Principal, scope: ScopeId) -> AccessAttempt {
    AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    )
}

fn assert_historical_permission(
    node: &Node,
    head: ControlHead,
    request: &AccessAttempt,
    permitted: bool,
) -> TestResult {
    let assessment = AuthorityHistory::replay(node, anchor()?)?.assess_at(
        head,
        request,
        Utc::now(),
        ScopeTopology::default(),
    )?;
    if assessment.decision_at_head().is_permit() != permitted {
        return Err("certified selection did not produce the expected historical authority".into());
    }
    Ok(())
}

#[tokio::test]
async fn native_selection_preserves_records_and_recovers_after_outage_and_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("selection-a.redb");
    let b_path = directory.path().join("selection-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (reader, scope) = record_obligated_grant(&a, ScopeId::new("selection:data"), [])?;
    let original = authority_events(&a)?;
    let selection = AuthoritySelection::new(CommandId::new(), &original)?;
    let request = read_attempt(reader, scope.clone());
    let harness = NativeControlHarness::start(
        a.clone(),
        b.clone(),
        authority_realm_scope(&realm()),
        scope.clone(),
    )
    .await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let outcome = async {
        let chosen = coordinator.certify_selection(&selection).await?;
        assert_historical_permission(&a, chosen, &request, true)?;
        let before = a.events_after(None)?;
        if coordinator.certify_selection(&selection).await? != chosen
            || a.events_after(None)? != before
        {
            return Err("exact selection retry appended history or changed the chosen head".into());
        }
        let changed = AuthoritySelection::new(
            selection.operation(),
            &original.iter().take(1).cloned().collect::<Vec<_>>(),
        )?;
        if coordinator.certify_selection(&changed).await.is_ok() || a.events_after(None)? != before
        {
            return Err("selection operation identity accepted different records".into());
        }
        let revocation = record_revocation(&a)?;
        harness.b_transport.sessions().set_authority_control(None)?;
        if coordinator.certify_selection(&revocation).await.is_ok()
            || AuthorityHistory::replay(&a, anchor()?)?.retained_head()? != chosen
        {
            return Err("selection bypassed an unavailable native controller".into());
        }
        assert_historical_permission(&a, chosen, &request, true)?;
        Ok::<_, Box<dyn Error>>((chosen, revocation))
    }
    .await;
    drop(coordinator);
    harness.shutdown().await?;
    let (chosen, revocation) = outcome?;
    drop(a);
    drop(b);

    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let harness =
        NativeControlHarness::start(a.clone(), b, authority_realm_scope(&realm()), scope).await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let outcome = async {
        let revoked = coordinator.certify_selection(&revocation).await?;
        assert_historical_permission(&a, revoked, &request, false)?;
        let before = a.events_after(None)?;
        if revoked == chosen
            || coordinator.certify_selection(&selection).await? != chosen
            || coordinator.certify_selection(&revocation).await? != revoked
            || a.events_after(None)? != before
        {
            return Err(
                "reopened selection retry failed to recover original certified heads".into(),
            );
        }
        for event in &original {
            if before
                .iter()
                .find(|retained| retained.origin == event.origin)
                != Some(event)
            {
                return Err("certifying authority rewrote an original accepted record".into());
            }
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    drop(coordinator);
    harness.shutdown().await?;
    outcome
}

#[tokio::test]
async fn selection_rejects_missing_bodies_causal_gaps_and_wrong_realm_before_voting() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("invalid-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("invalid-b.redb"))?;
    record_obligated_grant(&a, ScopeId::new("selection:data"), [])?;
    let records = authority_events(&a)?;
    let selection = AuthoritySelection::new(CommandId::new(), &records)?;
    let missing = Node::in_memory();
    let other_missing = Node::in_memory();
    if coordinator(&missing, &other_missing)?
        .certify_selection(&selection)
        .await
        .is_ok()
        || !missing.events_after(None)?.is_empty()
        || !other_missing.events_after(None)?.is_empty()
    {
        return Err("missing selected bodies reached a controller vote".into());
    }
    let last = records
        .last()
        .ok_or("authority fixture has no records")?
        .clone();
    let incomplete = AuthoritySelection::new(CommandId::new(), &[last])?;
    let before = a.events_after(None)?;
    let mut altered = records.clone();
    let first = altered
        .first_mut()
        .ok_or("authority fixture has no records")?;
    first.recorded_at = first
        .recorded_at
        .checked_add_signed(Duration::seconds(1))
        .ok_or("recorded time overflowed")?;
    let altered = AuthoritySelection::new(CommandId::new(), &altered)?;
    if coordinator(&a, &b)?
        .certify_selection(&altered)
        .await
        .is_ok()
        || a.events_after(None)? != before
    {
        return Err("changed immutable record reached a controller vote".into());
    }
    if coordinator(&a, &b)?
        .certify_selection(&incomplete)
        .await
        .is_ok()
        || a.events_after(None)? != before
    {
        return Err("uncertified causal parents reached a controller vote".into());
    }
    let foreign = Node::in_memory();
    let policy = AuthorityPolicy::new(
        ApplicationHost::new(
            foreign.clone(),
            AuthorityPolicy::install(MykoApplication::new())?,
        )?,
        AuthorityRealmId::new("other-realm"),
    );
    policy.bootstrap(Principal::node(PrincipalId::new("admin")))?;
    let wrong_realm = AuthoritySelection::new(CommandId::new(), &foreign.events_after(None)?)?;
    if coordinator(&a, &b)?
        .certify_selection(&wrong_realm)
        .await
        .is_ok()
        || a.events_after(None)? != before
    {
        return Err("another realm reached a controller vote".into());
    }
    Ok(())
}

#[tokio::test]
async fn selection_recovers_an_earlier_accepted_value_before_its_own_choice() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("accepted-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("accepted-b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let request =
        AuthorityRequestSource::new(a.clone()).current_request(read_attempt(reader, scope))?;
    let request_id = CommandId::new();
    let decision = AuthorityHistory::replay(&a, anchor()?)?.plan_decision_at(
        head,
        CommandId::new(),
        request_id,
        request.request().clone(),
        *request.evaluated_at(),
        request.topology().clone(),
    )?;
    let [a_key, b_key] = keys();
    let ballot = ControlBallot {
        counter: 3,
        proposer: controller_id(&a_key),
    };
    let a_controller = AuthorityController::new(a.clone(), anchor()?);
    let b_controller = AuthorityController::new(b.clone(), anchor()?);
    let promises = vec![
        a_controller.prepare(head, ballot, &a_key)?,
        b_controller.prepare(head, ballot, &b_key)?,
    ];
    let proposal =
        a_controller.propose(head, ballot, &promises, &decision.control_value()?, &a_key)?;
    a_controller.accept(head, &proposal, &a_key)?;
    let selection = record_revocation(&a)?;
    let chosen = coordinator(&a, &b)?.certify_selection(&selection).await?;
    let root = AuthorityDecisionRoot::new(realm(), request_id, AuthorizationPhase::Admission)?;
    if AuthorityHistory::replay(&a, anchor()?)?.decision_at(chosen, &root)? != Some(decision) {
        return Err("selection bypassed the previously accepted authority decision".into());
    }
    assert_historical_permission(&a, chosen, request.request(), false)
}
