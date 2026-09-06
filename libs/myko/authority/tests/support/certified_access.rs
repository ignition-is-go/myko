use super::*;

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
