use super::*;

async fn wait_for_permission(node: &Node, request: &AccessAttempt, permitted: bool) -> TestResult {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let node = node.clone();
            let request = request.clone();
            let current = tokio::task::spawn_blocking(move || -> Result<bool, String> {
                let history = AuthorityHistory::replay(&node, anchor()?)?;
                let assessment = history.assess_at(
                    history.retained_head()?,
                    &request,
                    Utc::now(),
                    ScopeTopology::default(),
                )?;
                Ok(assessment.decision_at_head().is_permit())
            })
            .await??;
            if current == permitted {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| format!("worker did not publish permission={permitted}"))??;
    Ok(())
}

#[tokio::test]
async fn native_worker_publishes_bootstrap_then_idle_administration_changes() -> TestResult {
    check_worker_publication(false).await
}

#[tokio::test]
async fn native_worker_retries_publication_after_controller_recovery_without_commands() -> TestResult
{
    check_worker_publication(true).await
}

async fn check_worker_publication(interrupt_controller: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("publication-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("publication-b.redb"))?;
    let (reader, scope) = record_obligated_grant(&a, ScopeId::new("publication:data"), [])?;
    let request = read_attempt(reader, scope.clone());
    let harness =
        NativeControlHarness::start(a.clone(), b.clone(), authority_realm_scope(&realm()), scope)
            .await?;
    let starting = b.hold_startup();
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let (runtime, policy) = myko_authority::certified::PreparedAuthorityRuntime::new(
        coordinator,
        Arc::new(AllowAllAccessPolicy),
    );
    let (errors_tx, errors_rx) = flume::unbounded();
    let guard = runtime.start(move |result| {
        if let Err(error) = result {
            let _ = errors_tx.send(error);
        }
    })?;
    let outcome = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if AuthorityHistory::replay(&a, anchor()?)?.retained_head()? != anchor()?.genesis() {
            return Err("worker certified before required controller evidence was ready".into());
        }
        starting.ready();
        wait_for_permission(&a, &request, true).await?;
        if interrupt_controller {
            harness.b_transport.sessions().set_authority_control(None)?;
            for _ in errors_rx.drain() {}
        }
        record_revocation(&a)?;
        if interrupt_controller {
            tokio::time::timeout(std::time::Duration::from_secs(15), errors_rx.recv_async())
                .await??;
            let [_, b_key] = keys();
            harness
                .b_transport
                .sessions()
                .set_authority_control(Some(Arc::new(
                    CertifiedAuthorityControlEndpoint::new(
                        b.clone(),
                        anchor()?,
                        b_key,
                        vec![harness.a_binding.clone()],
                    )?
                    .with_scoped_evidence_endpoint(
                        harness.a_principal.id.clone(),
                        Arc::new(IrohScopedEvidenceEndpoint::new(
                            harness.b_transport.clone(),
                            harness.a_transport.address(),
                        )),
                    )?,
                )))?;
        }
        wait_for_permission(&a, &request, false).await?;
        Ok(())
    }
    .await
    .map_err(|error: Box<dyn Error>| error.to_string());
    guard.shutdown().await?;
    drop(policy);
    harness.shutdown().await?;
    outcome.map_err(|error| {
        format!(
            "{error}; worker errors: {:?}",
            errors_rx.drain().collect::<Vec<_>>()
        )
        .into()
    })
}

#[tokio::test]
async fn native_retained_selection_recovers_bootstrap_and_revocation_after_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("retained-a.redb");
    let b_path = directory.path().join("retained-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let (reader, scope) = record_obligated_grant(&a, ScopeId::new("retained:data"), [])?;
    let original = a.events_after(None)?;
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
        let head = coordinator.certify_local_authority().await?;
        assert_historical_permission(&a, head, &request, true)?;
        let before = a.events_after(None)?;
        if coordinator.certify_local_authority().await? != head || a.events_after(None)? != before {
            return Err("completed bootstrap certification was not idempotent".into());
        }
        record_revocation(&a)?;
        harness.b_transport.sessions().set_authority_control(None)?;
        if coordinator.certify_local_authority().await.is_ok() {
            return Err("unavailable controller did not stop revocation certification".into());
        }
        assert_historical_permission(&a, head, &request, true)?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    drop(coordinator);
    harness.shutdown().await?;
    outcome?;
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
        let head = coordinator.certify_local_authority().await?;
        assert_historical_permission(&a, head, &request, false)?;
        let before = a.events_after(None)?;
        if coordinator.certify_local_authority().await? != head || a.events_after(None)? != before {
            return Err("reopened revocation certification appended duplicate selection".into());
        }
        if !original.iter().all(|event| before.contains(event)) {
            return Err("startup certification rewrote accepted authority history".into());
        }
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    drop(coordinator);
    harness.shutdown().await?;
    outcome
}

#[tokio::test]
async fn retained_selection_recovers_an_accepted_prefix_before_selecting_the_rest() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("prefix-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("prefix-b.redb"))?;
    let (reader, scope) = record_obligated_grant(&a, ScopeId::new("prefix:data"), [])?;
    sync_authority(&a, &b)?;
    let events = authority_events(&a)?;
    let end = events
        .iter()
        .position(|event| matches!(event.event, NodeEvent::CommandCommitted { .. }))
        .ok_or("bootstrap has no commit")?;
    let prefix = events.iter().take(end + 1).cloned().collect::<Vec<_>>();
    let selection = AuthoritySelection::new(CommandId::new(), &prefix)?;
    let [a_key, b_key] = keys();
    let a_controller = AuthorityController::new(a.clone(), anchor()?);
    let b_controller = AuthorityController::new(b.clone(), anchor()?);
    let head = anchor()?.genesis();
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller_id(&a_key),
    };
    let promises = vec![
        a_controller.prepare(head, ballot, &a_key)?,
        b_controller.prepare(head, ballot, &b_key)?,
    ];
    let proposal =
        a_controller.propose(head, ballot, &promises, &selection.control_value()?, &a_key)?;
    a_controller.accept(head, &proposal, &a_key)?;
    let coordinator = coordinator(&a, &b)?;
    let head = coordinator.certify_local_authority().await?;
    assert_historical_permission(&a, head, &read_attempt(reader, scope), true)?;
    let before = a.events_after(None)?;
    if coordinator.certify_selection(&selection).await? == head {
        return Err("prefix was not chosen before the remaining records".into());
    }
    if coordinator.certify_local_authority().await? != head || a.events_after(None)? != before {
        return Err("prefix recovery reselected already chosen records".into());
    }
    Ok(())
}

#[tokio::test]
async fn local_certification_does_not_promote_foreign_raw_authority() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("local-a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("local-b.redb"))?;
    let foreign = RedbJournal::open_node(directory.path().join("foreign.redb"))?;
    let (reader, scope) = record_obligated_grant(&a, ScopeId::new("local:data"), [])?;
    record_obligated_grant(&foreign, ScopeId::new("foreign:data"), [])?;
    for event in foreign.events_after(None)? {
        a.ingest(event)?;
    }
    let coordinator = coordinator(&a, &b)?;
    let head = coordinator.certify_local_authority().await?;
    assert_historical_permission(&a, head, &read_attempt(reader.clone(), scope), true)?;
    assert_historical_permission(
        &a,
        head,
        &read_attempt(reader, ScopeId::new("foreign:data")),
        false,
    )?;
    let before = a.events_after(None)?;
    if coordinator.certify_local_authority().await? != head || a.events_after(None)? != before {
        return Err("foreign raw records kept triggering local certification".into());
    }
    Ok(())
}
