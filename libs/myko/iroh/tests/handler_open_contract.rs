#![cfg(feature = "schema")]

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use myko::{ApplicationHost, MykoApplication, MykoService as _, prelude::*};
use myko_federation::{
    AccessAttempt, AccessPolicy, AllowAllAccessPolicy, AuthorizationDecision, AuthorizationPhase,
    HandlerKind, Node, ServiceId,
};
use myko_iroh::IrohReplicator;
use myko_wire::{HandlerRequest, SchemaDocument};

type TestResult = Result<(), Box<dyn Error>>;

#[myko_service(Record)]
pub struct ContractService;

#[myko_item(service = ContractService)]
pub struct Record {
    label: String,
}

#[myko_view(Record, item = Record)]
pub struct GuardedRecords {}

static HANDLER_RUNS: AtomicUsize = AtomicUsize::new(0);

impl ViewHandler for GuardedRecords {
    fn build_cell(
        _ctx: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = Record>, String> {
        HANDLER_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(myko::view::LocalView::new(hyphae::CellMap::<
            Arc<str>,
            Arc<Record>,
        >::new()))
    }
}

#[myko_view(Record, item = Record)]
pub struct StableRecords {}

impl ViewHandler for StableRecords {
    fn build_cell(
        _ctx: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = Record>, String> {
        Ok(myko::view::LocalView::new(hyphae::CellMap::<
            Arc<str>,
            Arc<Record>,
        >::new()))
    }
}

fn application(node: Node) -> Result<ApplicationHost, String> {
    ApplicationHost::new(
        node,
        MykoApplication::builder()
            .service::<ContractService>()
            .build(),
    )
}

async fn server() -> Result<IrohReplicator, Box<dyn Error>> {
    Ok(IrohReplicator::bind_loopback_application_with_policy(
        application(Node::in_memory())?,
        Arc::new(AllowAllAccessPolicy),
    )
    .await?)
}

fn request(server: &IrohReplicator, handler: &str) -> HandlerRequest {
    HandlerRequest {
        kind: HandlerKind::View,
        service_id: Some(ServiceId::new(ContractService::SERVICE_ID)),
        handler_id: handler.to_owned(),
        source_node: Some(server.sessions().node().node_id()),
        scope_id: None,
        params: serde_json::json!({}),
    }
}

#[tokio::test]
async fn changed_observed_contract_is_rejected_before_running_the_handler() -> TestResult {
    let server = server().await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let connector = client.handler_connector(server.address());
    let request = request(&server, "GuardedRecords");
    let observed = connector.describe(request.clone()).await?;
    let mut changed = observed.clone();
    changed.arguments.deserialization = SchemaDocument::Boolean(false);
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        connector.connect_described(request.clone(), changed),
    )
    .await?;
    if !matches!(opened, Err(myko::client::HandlerClientError::Protocol(message)) if message.contains("observed handler contract changed"))
    {
        return Err("server ignored the observed contract precondition".into());
    }
    if HANDLER_RUNS.load(Ordering::SeqCst) != 0 {
        return Err("mismatched handler executed before precondition rejection".into());
    }
    let (initial, connection) = tokio::time::timeout(
        Duration::from_secs(5),
        connector.connect_described(request, observed),
    )
    .await??;
    if !matches!(initial, myko::client::HandlerFrame::State { .. })
        || HANDLER_RUNS.load(Ordering::SeqCst) != 1
    {
        return Err("unchanged contract did not open its actual handler".into());
    }
    drop(connection);
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn replacing_an_application_ends_its_inspected_stream() -> TestResult {
    let server = server().await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let connector = client.handler_connector(server.address());
    let request = request(&server, "StableRecords");
    let observed = connector.describe(request.clone()).await?;
    let (_, mut connection) = connector
        .connect_described(request.clone(), observed)
        .await?;
    server
        .sessions()
        .set_application(application(server.sessions().node().clone())?)?;
    let next = tokio::time::timeout(Duration::from_secs(5), connection.recv()).await?;
    if !matches!(next, Err(myko::client::HandlerClientError::Protocol(message)) if message.contains("serving application changed"))
    {
        return Err("inspected stream survived application replacement".into());
    }
    let refreshed = connector.describe(request.clone()).await?;
    let (_, replacement) = connector.connect_described(request, refreshed).await?;
    drop(connection);
    drop(replacement);
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[derive(Debug)]
struct AdmissionOnly;

impl AccessPolicy for AdmissionOnly {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        Ok(AuthorizationDecision::from_rule(
            request,
            if request.authorization_phase == AuthorizationPhase::Admission {
                Ok(())
            } else {
                Err("inspected handler access revoked".to_owned())
            },
        ))
        .into()
    }
}

#[tokio::test]
async fn continuation_denial_hides_a_changed_contract() -> TestResult {
    let server = server().await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let connector = client.handler_connector(server.address());
    let request = request(&server, "StableRecords");
    let mut observed = connector.describe(request.clone()).await?;
    observed.arguments.deserialization = SchemaDocument::Boolean(false);
    server.set_access_policy(Arc::new(AdmissionOnly))?;
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        connector.connect_described(request, observed),
    )
    .await?;
    if !matches!(opened, Err(myko::client::HandlerClientError::Authorization(decision))
        if matches!(decision.as_ref(), AuthorizationDecision::Deny(_))
            && decision.public_message().contains("inspected handler access revoked"))
    {
        return Err("contract mismatch escaped the continuation denial".into());
    }
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}
