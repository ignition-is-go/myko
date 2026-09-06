use super::*;

#[tokio::test]
async fn installed_policy_certifies_item_reads_instead_of_using_the_local_fallback() -> TestResult {
    use myko::server::FederatedSession;
    use myko_authority::certified::PreparedAuthorityRuntime;
    use myko_federation::ItemStateRequest;
    use myko_wire::{NodeFrame, NodeRequest, NodeRequestEnvelope};

    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
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
    let (_runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator, Arc::new(AllowAllAccessPolicy));
    let session = FederatedSession::new(a.clone(), policy);
    let request = NodeRequestEnvelope::connected(NodeRequest::ItemState {
        request: ItemStateRequest {
            source_node: None,
            service_id: ServiceId::new("item-read"),
            scope_id: scope,
            item_type: "Record".to_owned(),
            schema_version: 1,
            snapshot_through: None,
            after_item_id: None,
            page_size: 1,
        },
    });
    let outcome = async {
        let mut first = session.open_authenticated(reader.clone(), request.clone()).await;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), first.recv()).await?;
        if !matches!(frame, Some(NodeFrame::Authorization { decision }) if decision.is_permit()) {
            return Err("certified item read was not permitted".into());
        }
        if !matches!(first.recv().await, Some(NodeFrame::ItemState { .. })) {
            return Err("certified item read did not serve its page".into());
        }
        if AuthorityHistory::replay(&a, anchor()?)?.retained_head()? == head {
            return Err("item read used local fallback without certified consumption".into());
        }
        let mut second = session.open_authenticated(reader.clone(), request.clone()).await;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), second.recv()).await?;
        if !matches!(frame, Some(NodeFrame::Authorization { decision }) if matches!(*decision, AuthorizationDecision::Deny(_))) {
            return Err("second item read bypassed the consumed grant through local fallback".into());
        }
        if second.recv().await.is_some() {
            return Err("denied item read leaked a page".into());
        }
        let before_outage = AuthorityHistory::replay(&a, anchor()?)?.retained_head()?;
        harness.b_transport.sessions().set_authority_control(None)?;
        let mut unavailable = session.open_authenticated(reader, request).await;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), unavailable.recv()).await?;
        if !matches!(frame, Some(NodeFrame::AuthorityUnavailable { reason: AuthorityUnavailable::CoordinationUnavailable })) {
            return Err("missing controller fell back to local item permission".into());
        }
        if unavailable.recv().await.is_some()
            || AuthorityHistory::replay(&a, anchor()?)?.retained_head()? != before_outage {
            return Err("unavailable read served data or advanced certified history".into());
        }
        Ok::<(), Box<dyn Error>>(())
    }.await;
    harness.shutdown().await?;
    outcome
}

#[test]
fn runtime_policy_rejects_unsupported_read_forms_without_fallback() -> TestResult {
    use myko_authority::certified::PreparedAuthorityRuntime;

    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (_, reader, scope) = install_grant(&a, &b)?;
    let (_runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    for phase in [AuthorizationPhase::Continuation, AuthorizationPhase::Effect] {
        let mut invalid = AccessAttempt::scoped(
            reader.id.clone(),
            AuthorityPresentation::direct(reader.clone()),
            AccessOperation::ReadItems,
            scope.clone(),
        );
        invalid.authorization_phase = phase;
        if policy.decide(&invalid).into_immediate() != Err(AuthorityUnavailable::PolicyUnavailable)
        {
            return Err("unsupported read phase used the fallback policy".into());
        }
    }
    let mut unscoped = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    );
    unscoped.target = AccessTarget::ScopeCatalog;
    if policy.decide(&unscoped).into_immediate() != Err(AuthorityUnavailable::PolicyUnavailable) {
        return Err("unscoped item read used the fallback policy".into());
    }
    Ok(())
}

#[tokio::test]
async fn controllers_certify_a_read_without_a_prepared_application_command() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let request = AuthorityRequestSource::new(a.clone()).current_request(AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    ))?;
    let coordinator = coordinator(&a, &b)?;
    let request_id = CommandId::new();
    let decision = coordinator
        .decide(head, 2, CommandId::new(), request_id, request.clone())
        .await?;
    if !decision.decision().is_permit() {
        return Err(format!("read decision was not permitted: {:?}", decision.decision()).into());
    }
    if a.command(request_id)?.is_some() || b.command(request_id)?.is_some() {
        return Err("read certification manufactured an application command".into());
    }
    let next = coordinator
        .decide(
            decision.head(),
            100,
            CommandId::new(),
            CommandId::new(),
            request.clone(),
        )
        .await
        .map_err(|error| format!("second read certification: {error}"))?;
    if next.decision().is_permit() {
        return Err("second item read reused a single-use grant".into());
    }
    let recovered = coordinator
        .decide(next.head(), 200, CommandId::new(), request_id, request)
        .await
        .map_err(|error| format!("recovering read from a later head: {error}"))?;
    if recovered.transition() != decision.transition()
        || recovered.head() != decision.head()
        || recovered.proposal() != decision.proposal()
        || recovered.accepts() != decision.accepts()
    {
        return Err("historical read retry changed the certified decision".into());
    }
    Ok(())
}

async fn propose_candidate(
    a: &Node,
    b: &Node,
    head: ControlHead,
    value: ControlValue,
    evidence: Option<Arc<dyn ScopedRetainedEvidenceEndpoint>>,
) -> Result<Result<SignedControlProposal, AuthorizationFailure>, Box<dyn Error>> {
    let [a_key, b_key] = keys();
    let principal = Principal::node(PrincipalId::new("node:controller-a"));
    let caller = AuthorityControllerPrincipal::new(principal.clone(), controller_id(&a_key));
    let presentation = AuthorityPresentation::direct(principal.clone());
    let mut a_endpoint = endpoint(a.clone(), a_key.clone(), caller.clone(), 300)?;
    let b_endpoint = endpoint(b.clone(), b_key, caller, 300)?;
    let ballot = ControlBallot {
        counter: 2,
        proposer: controller_id(&a_key),
    };
    let promises = endpoint_promises(
        &a_endpoint,
        &b_endpoint,
        &principal,
        &presentation,
        head,
        ballot,
    )
    .await?;
    if let Some(evidence) = evidence {
        a_endpoint = a_endpoint.with_scoped_evidence_endpoint(evidence);
    }
    Ok(a_endpoint
        .propose(
            &principal.id,
            &presentation,
            AuthorityControlProposeRequest {
                head,
                ballot,
                promises,
                value,
            },
        )
        .await)
}

#[tokio::test]
async fn item_read_certification_rejects_unsupported_operations_and_targets() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let history = AuthorityHistory::replay(&a, anchor()?)?;
    for operation in [
        AccessOperation::SubmitCommand,
        AccessOperation::CancelCommand,
        AccessOperation::AdministerAuthority,
        AccessOperation::DelegateAuthority,
        AccessOperation::FollowItems,
        AccessOperation::FollowHandler,
        AccessOperation::FollowHistory,
        AccessOperation::SubscribeLive,
        AccessOperation::ReadCommand,
        AccessOperation::ReadCommands,
        AccessOperation::WatchCommand,
        AccessOperation::WatchCommands,
        AccessOperation::ReadHistory,
        AccessOperation::ApproveAuthority,
    ] {
        let request = AccessAttempt::scoped(
            reader.id.clone(),
            AuthorityPresentation::direct(reader.clone()),
            operation,
            scope.clone(),
        );
        let value = history
            .plan_decision_at(
                head,
                CommandId::new(),
                CommandId::new(),
                request,
                Utc::now(),
                a.scope_topology()?,
            )?
            .control_value()?;
        if !matches!(
            propose_candidate(&a, &b, head, value, None).await?,
            Err(AuthorizationFailure::Deny(_))
        ) {
            return Err(format!("unsupported operation {operation:?} was certified").into());
        }
    }
    let mut request = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    );
    request.target = AccessTarget::ScopeCatalog;
    let value = history
        .plan_decision_at(
            head,
            CommandId::new(),
            CommandId::new(),
            request,
            Utc::now(),
            a.scope_topology()?,
        )?
        .control_value()?;
    if !matches!(
        propose_candidate(&a, &b, head, value, None).await?,
        Err(AuthorizationFailure::Deny(_))
    ) {
        return Err("unscoped read target was certified".into());
    }
    if AuthorityHistory::replay(&a, anchor()?)?.retained_head()? != head {
        return Err("rejected candidates changed the certified head".into());
    }
    Ok(())
}

#[tokio::test]
async fn item_read_controllers_reject_request_carried_topology() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let forged: ScopeTopology = serde_json::from_value(serde_json::json!({
        "parents": {}, "known": ["invented:scope"]
    }))?;
    let mut request = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    );
    request.topology = Some(forged.clone());
    let trusted = AuthorityRequestSource::new(a.clone()).current_request(request.clone())?;
    if trusted.topology() != &a.scope_topology()? {
        return Err("request source accepted caller topology".into());
    }
    let value = AuthorityHistory::replay(&a, anchor()?)?
        .plan_decision_at(
            head,
            CommandId::new(),
            CommandId::new(),
            request,
            Utc::now(),
            forged,
        )?
        .control_value()?;
    if !matches!(
        propose_candidate(&a, &b, head, value, None).await?,
        Err(AuthorizationFailure::Deny(_))
    ) {
        return Err("controller accepted request-carried topology".into());
    }
    Ok(())
}

#[derive(Debug)]
struct MissingReadEvidence;

impl ScopedRetainedEvidenceEndpoint for MissingReadEvidence {
    fn refresh_scopes<'a>(&'a self, scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async move {
            if scopes == [authority_realm_scope(&realm())] {
                Ok(())
            } else {
                Err(RetainedEvidenceError::Unavailable(
                    AuthorityUnavailable::HistoryUnavailable,
                ))
            }
        })
    }
}

#[tokio::test]
async fn item_read_without_scoped_evidence_is_unavailable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let request = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::ReadItems,
        scope,
    );
    let value = AuthorityHistory::replay(&a, anchor()?)?
        .plan_decision_at(
            head,
            CommandId::new(),
            CommandId::new(),
            request,
            Utc::now(),
            a.scope_topology()?,
        )?
        .control_value()?;
    if !matches!(
        propose_candidate(&a, &b, head, value, Some(Arc::new(MissingReadEvidence))).await?,
        Err(AuthorizationFailure::Unavailable(
            AuthorityUnavailable::HistoryUnavailable
        ))
    ) {
        return Err("missing read evidence did not fail unavailable".into());
    }
    Ok(())
}
