use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use myko::{ApplicationHost, MykoApplication, prelude::*};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessPolicy, AllowAllAccessPolicy, AuthorityPresentation,
    AuthorityUnavailable, AuthorizationDecision, BatchId, ChangeBatch, CommandId, CommandRequest,
    ItemMutation, Node, NodeId, PrincipalId, ScopeId, ServiceId,
};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const SCOPE: &str = "native-authorization";

#[myko_service(Record)]
pub struct RecordService;

#[myko_item(service = RecordService, scope_root)]
pub struct Record {
    pub value: String,
}

#[myko_query(Record, item = Record)]
#[derive(PartialEq, Eq)]
pub struct RecordsQuery {}

impl QueryHandler for RecordsQuery {
    fn scope_id(&self, _node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new(SCOPE))
    }

    fn build_view(
        context: myko::query::QueryBuildArgs<Self>,
    ) -> Result<Option<impl myko::query::QueryBuildOutput>, String> {
        Ok(Some(myko::query::RetainedQuery::new(
            context.federated_items::<Record>()?,
        )))
    }
}

#[myko_view(Record, item = Record)]
pub struct RecordsView {}

impl ViewHandler for RecordsView {
    fn scope_id(&self, _node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new(SCOPE))
    }

    fn build_cell(
        context: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = Record>, String> {
        Ok(myko::view::RetainedView::new(
            context.federated_items::<Record>()?,
        ))
    }
}

#[myko_command(bool, item = Record)]
pub struct SetRecord {
    pub id: RecordId,
    pub value: String,
}

impl CommandHandler for SetRecord {
    fn scope(&self, _node: NodeId) -> RecordId {
        RecordId::from(SCOPE)
    }

    fn execute(self, context: CommandContext) -> Result<bool, CommandError> {
        context.emit_set(&Record {
            id: self.id,
            value: self.value,
        })?;
        Ok(true)
    }
}

pub fn application(node: Node) -> TestResult<ApplicationHost> {
    Ok(ApplicationHost::new(
        node,
        MykoApplication::builder()
            .service::<RecordService>()
            .build(),
    )?)
}

pub fn commit_record(node: &Node, id: &str) -> TestResult {
    let principal = PrincipalId::new("node:test");
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(RecordService::SERVICE_ID),
        scope_id: ScopeId::new(SCOPE),
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "record.fixture".to_owned(),
        payload: Vec::new(),
    };
    let admission = node.admit(request.clone())?;
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![ItemMutation::set(&Record {
                id: RecordId::from(id),
                value: id.to_owned(),
            })?],
        },
        Vec::new(),
    )?;
    Ok(())
}

#[derive(Debug)]
pub struct ReadPolicy {
    pub allowed: AtomicBool,
    pub available: AtomicBool,
    pub unavailable: flume::Sender<()>,
}

impl ReadPolicy {
    pub fn new() -> (Arc<Self>, flume::Receiver<()>) {
        let (unavailable, receiver) = flume::unbounded();
        (
            Arc::new(Self {
                allowed: AtomicBool::new(true),
                available: AtomicBool::new(true),
                unavailable,
            }),
            receiver,
        )
    }
}

impl AccessPolicy for ReadPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        let protected = matches!(
            request.operation,
            AccessOperation::FollowHandler
                | AccessOperation::ReadItems
                | AccessOperation::FollowItems
        );
        if protected && !self.available.load(Ordering::SeqCst) {
            let _ignored = self.unavailable.send(());
            return Err(AuthorityUnavailable::CoordinationUnavailable).into();
        }
        if protected && !self.allowed.load(Ordering::SeqCst) {
            return Ok(AuthorizationDecision::from_rule(
                request,
                Err("native read grant revoked".to_owned()),
            ))
            .into();
        }
        AllowAllAccessPolicy.decide(request)
    }
}
