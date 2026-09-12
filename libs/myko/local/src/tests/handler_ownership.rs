use super::*;
use myko_federation::{HandlerKind, ScopeSelection};

#[myko::myko_report(u64)]
struct GlobalOwnershipReport;

impl myko::report::ReportHandler for GlobalOwnershipReport {
    type Output = u64;
}

#[tokio::test]
async fn forged_handler_service_is_rejected() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    commit_record(&node, ScopeId::new("local-scope"), "owned-record")?;
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let local = LocalClientSession::new(&socket);
    for (kind, id) in [
        (HandlerKind::Query, "AllLocalRecordHandlers"),
        (HandlerKind::View, "AllLocalRecordsView"),
        (HandlerKind::Report, "CountAllLocalRecords"),
    ] {
        for service in [Some("forged-owner"), None] {
            let request = serde_json::from_value(serde_json::json!({
                "kind": kind,
                "handler_id": id,
                "service_id": service,
                "source_node": node.node_id(),
                "scope_id": "local-scope",
                "params": {},
            }))?;
            let result = local.handler_connector().connect(request).await;
            assert!(
                matches!(result, Err(HandlerClientError::Authorization(decision))
                if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
                    && decision.public_message().contains("is not registered for the requested service"))
            );
        }
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn node_without_application_retains_history_but_rejects_handlers()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let source = Node::in_memory();
    commit_record(&source, ScopeId::new("local-scope"), "owned-record")?;
    let storage = Node::in_memory();
    let history = source.events_after(None)?;
    for event in &history {
        storage.ingest(event.clone())?;
    }
    let server = LocalNodeServer::spawn_sessions(
        &socket,
        FederatedSession::new(storage.clone(), Arc::new(AllowAllAccessPolicy)),
        PrincipalId::new("local:owner"),
    )
    .await?;
    let client = LocalClientSession::new(&socket)
        .handler_connector()
        .client();
    let result = client.follow_view(&AllLocalRecordsView {}).await;
    server.shutdown().await?;
    let error = result
        .err()
        .ok_or("node without application executed a typed view")?;
    assert!(
        matches!(&error, HandlerClientError::Authorization(decision)
        if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
            && decision.public_message().contains("this node does not expose a Myko application")),
        "{error}"
    );
    assert_eq!(storage.events_after(None)?, history);
    Ok(())
}

#[test]
fn activated_registry_retains_service_owners() -> Result<(), Box<dyn std::error::Error>> {
    let service = <LocalRecord as myko::MykoItem>::SERVICE_ID;
    assert_eq!(
        <AllLocalRecordHandlers as myko::query::QueryIdStatic>::SERVICE_ID,
        Some(service)
    );
    assert_eq!(
        <AllLocalRecordsView as myko::view::ViewIdStatic>::SERVICE_ID,
        Some(service)
    );
    assert_eq!(
        <CountAllLocalRecords as myko::report::ReportIdStatic>::SERVICE_ID,
        Some(service)
    );
    assert_eq!(
        <GlobalOwnershipReport as myko::report::ReportIdStatic>::SERVICE_ID,
        None
    );
    let registry = myko::server::HandlerRegistry::for_services(&[service].into());
    let inactive = myko::server::HandlerRegistry::for_services(&std::collections::BTreeSet::new());
    let node = NodeId::new();
    for (kind, id) in [
        (HandlerKind::Query, "AllLocalRecordHandlers"),
        (HandlerKind::View, "AllLocalRecordsView"),
        (HandlerKind::Report, "CountAllLocalRecords"),
    ] {
        for params in [
            serde_json::json!({}),
            serde_json::json!({"serviceId": "forged-owner"}),
        ] {
            let authority = registry.handler_authority(
                kind,
                Some(service.as_str()),
                id,
                params.clone(),
                node,
            )?;
            assert_eq!(authority.service_id, Some(service), "{id}");
            assert!(
                inactive
                    .handler_authority(kind, Some(service.as_str()), id, params, node)
                    .is_err()
            );
        }
    }
    for registry in [&registry, &inactive] {
        let authority = registry.handler_authority(
            HandlerKind::Report,
            None,
            "GlobalOwnershipReport",
            serde_json::Value::Null,
            node,
        )?;
        assert_eq!(authority.service_id, None);
    }
    Ok(())
}

#[derive(Debug)]
struct ServiceScopedPolicy;

impl AccessPolicy for ServiceScopedPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let service = ServiceId::new(<LocalService as myko_federation::MykoService>::SERVICE_ID);
        let rule = if request.operation == AccessOperation::FollowHandler
            && (request.service_id() != Some(&service)
                || !request.resource_claims.iter().any(|claim| {
                    claim.kind == ResourceClaimKind::Primary
                        && claim.selection == ScopeSelection::Exact(ScopeId::new("local-scope"))
                        && claim.service_id.as_ref() == Some(&service)
                })) {
            Err("subscription is missing its registered service owner".to_owned())
        } else {
            Ok(())
        };
        Ok(AuthorizationDecision::from_rule(request, rule)).into()
    }
}

async fn open_service_scoped_handler(
    view: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("myko.sock");
    let node = Node::in_memory();
    let scope = ScopeId::new("local-scope");
    let record = commit_record(&node, scope.clone(), "owned-record")?;
    let server = LocalNodeServer::spawn_application(
        &socket,
        local_record_application(node.clone())?,
        PrincipalId::new("local:owner"),
        Arc::new(ServiceScopedPolicy),
    )
    .await?;
    let local = LocalClientSession::new(&socket);
    let client = local.handler_connector().client();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        if view {
            let handle = client.follow_view(&AllLocalRecordsView {}).await?;
            assert_eq!(
                handle.current().value.as_deref(),
                Some(std::slice::from_ref(&record))
            );
        } else {
            let handle = client
                .follow_query(Some(node.node_id()), scope, &AllLocalRecordHandlers {})
                .await?;
            assert_eq!(
                handle.current().value.as_deref(),
                Some(std::slice::from_ref(&record))
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    server.shutdown().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn query_subscription_preserves_registered_service_owner()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_service_scoped_handler(false).await
}

#[tokio::test]
async fn view_subscription_preserves_registered_service_owner()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_service_scoped_handler(true).await
}
