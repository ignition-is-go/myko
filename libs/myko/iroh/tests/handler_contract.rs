use std::{error::Error, sync::Arc, time::Duration};

use myko::{ApplicationHost, MykoApplication, MykoService as _, prelude::*};
use myko_federation::{
    AccessAttempt, AccessOperation, AuthorizationDecision, AuthorizationPhase, DenyAllAccessPolicy,
};
use myko_federation::{
    AccessPolicy, AllowAllAccessPolicy, HandlerKind, Node, NodeId, ScopeId, ServiceId,
};
use myko_iroh::IrohReplicator;
use myko_wire::HandlerRequest;

type TestResult = Result<(), Box<dyn Error>>;

#[myko_service(Record)]
pub struct ContractService;

#[myko_item(service = ContractService)]
pub struct Record {
    label: String,
}

#[myko_report(u64, item = Record)]
pub struct DescribeCount {}

impl ReportHandler for DescribeCount {
    type Output = u64;

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new("contract:scope"))
    }

    fn required_capabilities(&self) -> Vec<myko_federation::CapabilityId> {
        vec![myko_federation::CapabilityId::new("test.describe")]
    }
    // The inherited compute panics. Description must never invoke it.
}

#[myko_view(Record, item = Record)]
pub struct RecordView {}

impl ViewHandler for RecordView {
    fn build_cell(
        _ctx: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = Record>, String> {
        VIEW_EXECUTED.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(myko::view::LocalView::new(hyphae::CellMap::<
            Arc<str>,
            Arc<Record>,
        >::new()))
    }
}

static VIEW_EXECUTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[myko_query(Record, item = Record)]
#[derive(PartialEq, Eq)]
pub struct DescribeQuery {}

impl QueryHandler for DescribeQuery {}

fn request(node: NodeId, kind: HandlerKind, handler: &str) -> HandlerRequest {
    HandlerRequest {
        kind,
        service_id: Some(ServiceId::new(ContractService::SERVICE_ID)),
        handler_id: handler.to_owned(),
        source_node: Some(node),
        scope_id: (kind == HandlerKind::Report).then(|| ScopeId::new("contract:scope")),
        params: serde_json::json!({}),
    }
}

async fn application(policy: Arc<dyn AccessPolicy>) -> Result<IrohReplicator, Box<dyn Error>> {
    let host = ApplicationHost::new(
        Node::in_memory(),
        MykoApplication::builder()
            .service::<ContractService>()
            .build(),
    )?;
    Ok(IrohReplicator::bind_loopback_application_with_policy(host, policy).await?)
}

async fn describe(
    client: &IrohReplicator,
    server: &IrohReplicator,
    request: HandlerRequest,
) -> Result<myko_wire::HandlerContract, myko::client::HandlerClientError> {
    tokio::time::timeout(
        Duration::from_secs(5),
        client.handler_connector(server.address()).describe(request),
    )
    .await
    .map_err(|_| {
        myko::client::HandlerClientError::Transport("descriptor request timed out".to_owned())
    })?
}

#[cfg(feature = "schema")]
#[tokio::test]
async fn native_descriptions_match_generated_schemas_without_running_handlers() -> TestResult {
    use myko::schema::{HandlerPayloadSchema, HandlerResultSchema};
    use myko_wire::HandlerResultContract;

    let server = application(Arc::new(AllowAllAccessPolicy)).await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let server_id = server.sessions().node().node_id();
    for (kind, id, generated) in [
        (
            HandlerKind::Report,
            "DescribeCount",
            HandlerPayloadSchema::value::<DescribeCount, u64>(),
        ),
        (
            HandlerKind::Query,
            "DescribeQuery",
            HandlerPayloadSchema::rows::<DescribeQuery, Record>(),
        ),
        (
            HandlerKind::View,
            "RecordView",
            HandlerPayloadSchema::rows::<RecordView, Record>(),
        ),
    ] {
        let request = request(server_id, kind, id);
        let contract = describe(&client, &server, request.clone()).await?;
        contract.validate_for(&request, Some(server_id))?;
        if serde_json::to_value(contract.arguments.serialization)?
            != serde_json::to_value(generated.arguments.serialization)?
            || serde_json::to_value(contract.arguments.deserialization)?
                != serde_json::to_value(generated.arguments.deserialization)?
        {
            return Err("argument schemas changed across native transport".into());
        }
        let ((HandlerResultContract::Value(wire), HandlerResultSchema::Value(generated))
        | (HandlerResultContract::Rows(wire), HandlerResultSchema::Rows(generated))) =
            (contract.result, generated.result)
        else {
            return Err("result envelope changed across native transport".into());
        };
        if serde_json::to_value(wire.serialization)?
            != serde_json::to_value(generated.serialization)?
            || serde_json::to_value(wire.deserialization)?
                != serde_json::to_value(generated.deserialization)?
        {
            return Err("result schemas changed across native transport".into());
        }
    }
    if VIEW_EXECUTED.load(std::sync::atomic::Ordering::SeqCst) {
        return Err("description executed the view builder".into());
    }
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn storage_and_inactive_applications_cannot_describe_handlers() -> TestResult {
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let storage = IrohReplicator::bind_loopback_with_policy(
        Node::in_memory(),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let empty = ApplicationHost::new(Node::in_memory(), MykoApplication::builder().build())?;
    let inactive = IrohReplicator::bind_loopback_application_with_policy(
        empty,
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    for (server, expected) in [
        (&storage, "does not expose a Myko application"),
        (&inactive, "not registered for the requested service"),
    ] {
        let result = describe(
            &client,
            server,
            request(
                server.sessions().node().node_id(),
                HandlerKind::Report,
                "DescribeCount",
            ),
        )
        .await;
        if !matches!(result, Err(myko::client::HandlerClientError::Authorization(decision))
            if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
                && decision.public_message().contains(expected))
        {
            return Err(format!("expected descriptor rejection: {expected}").into());
        }
    }
    inactive.shutdown().await?;
    storage.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn descriptor_requests_reject_forged_owners_scopes_and_malformed_parameters() -> TestResult {
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let server = application(Arc::new(AllowAllAccessPolicy)).await?;
    let base = request(
        server.sessions().node().node_id(),
        HandlerKind::Report,
        "DescribeCount",
    );
    for (request, message) in [
        (
            HandlerRequest {
                service_id: None,
                ..base.clone()
            },
            "not registered for the requested service",
        ),
        (
            HandlerRequest {
                service_id: Some(ServiceId::new("foreign")),
                ..base.clone()
            },
            "not registered for the requested service",
        ),
        (
            HandlerRequest {
                handler_id: "Unknown".to_owned(),
                ..base.clone()
            },
            "not registered for the requested service",
        ),
        (
            HandlerRequest {
                scope_id: None,
                ..base.clone()
            },
            "source or scope does not match",
        ),
        (
            HandlerRequest {
                source_node: None,
                ..base.clone()
            },
            "source or scope does not match",
        ),
        (
            HandlerRequest {
                params: serde_json::json!(null),
                ..base
            },
            "invalid type",
        ),
    ] {
        let result = describe(&client, &server, request).await;
        if !matches!(result, Err(myko::client::HandlerClientError::Authorization(decision))
            if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
                && decision.public_message().contains(message))
        {
            return Err(format!("descriptor request did not fail with {message}").into());
        }
    }
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[cfg(feature = "schema")]
#[derive(Debug)]
struct DeclaredReportAccess {
    executor: myko_federation::PrincipalId,
}

#[cfg(feature = "schema")]
impl AccessPolicy for DeclaredReportAccess {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        use myko_federation::{
            CapabilityId, FederationPermission, ResourceClaimKind, ScopeSelection,
        };
        let permitted = request.operation == AccessOperation::FollowHandler
            && request.presentation.executor.id == self.executor
            && request
                .application_capabilities
                .contains(&CapabilityId::new("test.describe"))
            && request.resource_claims.iter().any(|claim| {
                claim.kind == ResourceClaimKind::Primary
                    && claim.selection == ScopeSelection::Exact(ScopeId::new("contract:scope"))
                    && claim.service_id == Some(ServiceId::new(ContractService::SERVICE_ID))
                    && claim
                        .required_permissions
                        .contains(&FederationPermission::ReadState)
            });
        Ok(AuthorizationDecision::from_rule(
            request,
            if permitted {
                Ok(())
            } else {
                Err("typed descriptor authority was not preserved".to_owned())
            },
        ))
        .into()
    }
}

#[cfg(feature = "schema")]
#[tokio::test]
async fn native_description_uses_authenticated_typed_handler_claims() -> TestResult {
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let policy = DeclaredReportAccess {
        executor: myko_iroh::endpoint_principal_id(client.address().id),
    };
    let server = application(Arc::new(policy)).await?;
    let contract = describe(
        &client,
        &server,
        request(
            server.sessions().node().node_id(),
            HandlerKind::Report,
            "DescribeCount",
        ),
    )
    .await?;
    if contract.handler_id != "DescribeCount" {
        return Err("typed descriptor did not arrive".into());
    }
    server.shutdown().await?;
    client.shutdown().await?;
    Ok(())
}

#[derive(Debug)]
struct AdmissionOnly;

impl AccessPolicy for AdmissionOnly {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let permitted = request.operation == AccessOperation::FollowHandler
            && request.authorization_phase == AuthorizationPhase::Admission;
        Ok(AuthorizationDecision::from_rule(
            request,
            if permitted {
                Ok(())
            } else {
                Err("descriptor access revoked".to_owned())
            },
        ))
        .into()
    }
}

#[tokio::test]
async fn native_description_requires_admission_and_release_authority() -> TestResult {
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let policies: [(Arc<dyn AccessPolicy>, &str); 2] = [
        (
            Arc::new(DenyAllAccessPolicy),
            "does not serve application or federation data",
        ),
        (Arc::new(AdmissionOnly), "descriptor access revoked"),
    ];
    for (policy, expected) in policies {
        let server = application(policy).await?;
        let result = describe(
            &client,
            &server,
            request(
                server.sessions().node().node_id(),
                HandlerKind::Report,
                "DescribeCount",
            ),
        )
        .await;
        match result {
            Err(myko::client::HandlerClientError::Authorization(decision))
                if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
                    && decision.public_message().contains(expected) => {}
            other => {
                return Err(format!("expected authorization denial, got {other:?}").into());
            }
        }
        server.shutdown().await?;
    }
    client.shutdown().await?;
    Ok(())
}

#[cfg(not(feature = "schema"))]
#[tokio::test]
async fn build_without_schema_rejects_instead_of_advertising_empty_contract() -> TestResult {
    let server = application(Arc::new(AllowAllAccessPolicy)).await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let result = describe(
        &client,
        &server,
        request(
            server.sessions().node().node_id(),
            HandlerKind::Report,
            "DescribeCount",
        ),
    )
    .await;
    if !matches!(result, Err(myko::client::HandlerClientError::Protocol(message)) if message.contains("no generated handler schema evidence"))
    {
        return Err("missing schema did not fail explicitly".into());
    }
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}
