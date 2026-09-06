use myko::server::FederatedSession;
use myko_authority::certified::PreparedAuthorityRuntime;
use myko_federation::{ItemFollowRequest, ItemMutation, MutationOperation};
use myko_wire::{NodeFrame, NodeRequest, NodeRequestEnvelope};

use super::*;

#[tokio::test]
async fn installed_policy_certifies_item_streams() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (head, reader, scope) = install_grant(&a, &b)?;
    let producer = Node::in_memory();
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
    let request = NodeRequestEnvelope::connected(NodeRequest::FollowItems {
        request: ItemFollowRequest {
            serving_node: a.node_id(),
            source_node: producer.node_id(),
            service_id: ServiceId::new("test.service"),
            scope_id: scope.clone(),
            item_type: "Record".to_owned(),
            schema_version: 1,
            after: a.local_history_cut()?,
        },
    });
    let outcome = async {
        let mut frames = session.open_authenticated(reader.clone(), request.clone()).await;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), frames.recv()).await
            .map_err(|_| "stream admission timed out")?;
        if !matches!(frame, Some(NodeFrame::Authorization { decision }) if decision.is_permit()) {
            return Err("certified item stream was not permitted".into());
        }
        if !matches!(frames.recv().await, Some(NodeFrame::ItemFollowReady { .. })) {
            return Err("certified item stream did not become ready".into());
        }
        if AuthorityHistory::replay(&a, anchor()?)?.retained_head()? == head {
            return Err("item stream used raw fallback without certified admission".into());
        }
        for value in [1, 2] {
            append_item(&producer, &a, &reader, &scope, value)?;
            let frame = tokio::time::timeout(std::time::Duration::from_mins(1), frames.recv()).await
                .map_err(|_| format!("stream update {value} timed out"))?;
            if !matches!(&frame, Some(NodeFrame::ItemUpdate { update })
                if update.changes.len() == 1 && update.changes.first().is_some_and(|change| change.payload == Some(value.to_string().into_bytes()))) {
                return Err(format!("continued item stream lost update {value}: {frame:?}").into());
            }
        }
        let mut second = session.open_authenticated(reader, request).await;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), second.recv()).await
            .map_err(|_| "second stream admission timed out")?;
        if !matches!(&frame, Some(NodeFrame::Authorization { decision }) if matches!(**decision, AuthorizationDecision::Deny(_))) {
            return Err(format!("new stream did not independently deny its spent grant: {frame:?}").into());
        }
        if second.recv().await.is_some() {
            return Err("denied second stream emitted a frame".into());
        }
        harness.b_transport.sessions().set_authority_control(None)?;
        let frame = tokio::time::timeout(std::time::Duration::from_mins(1), frames.recv()).await
            .map_err(|_| "stream quorum-loss notification timed out")?;
        if !matches!(frame, Some(NodeFrame::AuthorityUnavailable { reason: AuthorityUnavailable::CoordinationUnavailable })) {
            return Err("controller loss did not end the certified stream as unavailable".into());
        }
        if frames.recv().await.is_some() {
            return Err("unavailable stream emitted another frame".into());
        }
        Ok::<(), Box<dyn Error>>(())
    }.await;
    harness.shutdown().await?;
    outcome
}

fn append_item(
    producer: &Node,
    target: &Node,
    reader: &Principal,
    scope: &ScopeId,
    value: u8,
) -> TestResult {
    let before = producer.local_history_cut()?;
    let request = command_request(reader.clone(), scope.clone(), CommandId::new());
    let id = request.id;
    let snapshot = producer.admit(request)?.snapshot().clone();
    producer.commit(
        id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: id,
            service_id: snapshot.request.service_id,
            scope_id: scope.clone(),
            causal_parents: vec![snapshot.updated_at],
            changes: vec![ItemMutation {
                service_id: "test.service".to_owned(),
                item_type: "Record".to_owned(),
                item_id: "record".to_owned(),
                schema_version: 1,
                roots_scope: false,
                belongs_to: None,
                scope_id: Some(scope.to_string()),
                operation: MutationOperation::Set,
                payload: Some(value.to_string().into_bytes()),
            }],
        },
        Vec::new(),
    )?;
    for event in producer.events_after(before)? {
        target.ingest(event)?;
    }
    Ok(())
}

#[tokio::test]
async fn continuation_binds_its_admission_and_rechecks_revocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let a = RedbJournal::open_node(directory.path().join("a.redb"))?;
    let b = RedbJournal::open_node(directory.path().join("b.redb"))?;
    let (_, reader, scope) = install_grant(&a, &b)?;
    let (_runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    let mut access = AccessAttempt::scoped(
        reader.id.clone(),
        AuthorityPresentation::direct(reader),
        AccessOperation::FollowItems,
        scope.clone(),
    );
    access.admission_id = Some(CommandId::new());
    access.target = AccessTarget::Items {
        source_node: None,
        service_id: ServiceId::new("test.service"),
        scope_id: scope,
        item_type: "Record".to_owned(),
    };
    if !policy.decide(&access).resolve().await?.is_permit() {
        return Err("stream admission was not certified".into());
    }
    access.authorization_phase = AuthorizationPhase::Continuation;
    for _ in 0..2 {
        if !policy.decide(&access).resolve().await?.is_permit() {
            return Err("continuation spent a new grant use".into());
        }
    }
    let head = AuthorityHistory::replay(&a, anchor()?)?.retained_head()?;
    let mut missing = access.clone();
    missing.admission_id = None;
    if policy.decide(&missing).resolve().await != Err(AuthorityUnavailable::PolicyUnavailable) {
        return Err("continuation without admission used fallback authority".into());
    }
    let mut unknown = access.clone();
    unknown.admission_id = Some(CommandId::new());
    if policy.decide(&unknown).resolve().await != Err(AuthorityUnavailable::HistoryUnavailable) {
        return Err("unknown admission was treated as a fresh stream".into());
    }
    let mut changed = access.clone();
    changed.arguments_digest = Some("changed handler arguments".to_owned());
    if policy.decide(&changed).resolve().await != Err(AuthorityUnavailable::StateNotCurrent) {
        return Err("continuation changed its bound request".into());
    }
    let mut lease = access.clone();
    lease.presentation.active_lease = Some(myko_federation::LeaseId::new("forged"));
    if policy.decide(&lease).resolve().await != Err(AuthorityUnavailable::PolicyUnavailable) {
        return Err("continuation accepted a different lease".into());
    }
    if AuthorityHistory::replay(&a, anchor()?)?.retained_head()? != head {
        return Err("invalid continuation appended certified history".into());
    }
    certify_grant_revocation(&a, &b, head)?;
    if !matches!(
        policy.decide(&access).resolve().await?,
        AuthorizationDecision::Deny(_)
    ) {
        return Err("continued stream ignored certified grant revocation".into());
    }
    Ok(())
}
