use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use myko::{CommandContext, CommandError, CommandHandler, MykoApplication};
use myko_federation::{
    AccessAttempt, AccessOperation, AllowAllAccessPolicy, ApprovalDecision, ApprovalId,
    AuthorityPresentation, AuthorityRealmId, AuthorityUnavailable, AuthorizationBinding,
    AuthorizationDecision, AuthorizationFailure, BatchId, ChallengeId, ChangeBatch, CommandId,
    CommandRequest, CommandState, DelegationId, FederationPermission, ItemMutation, MykoItem as _,
    MykoService as _, ObligationId, Principal, PrincipalId, PrincipalKind, ProjectionCoverage,
    ProvenanceHop, ProvenanceOperation, ResourceClaim, ResourceClaimKind, ResourceVisibility,
    ScopeId, ScopeSelection, ScopeTopology, ServiceId,
};
use myko_items::{myko_command, myko_item, myko_service};
use myko_redb::RedbJournal;

#[test]
fn all_replication_uses_the_authority_constrained_handshake() {
    let selection = FollowSelection::replication(ReplicationSelection::All);

    assert!(matches!(
        selection,
        FollowSelection::Selected(ReplicationSelection::All)
    ));
}

async fn bind_allow_all(node: Node) -> Result<IrohReplicator, IrohReplicationError> {
    IrohReplicator::bind_loopback_with_policy(node, Arc::new(AllowAllAccessPolicy)).await
}

async fn bind_with_secret_allow_all(
    node: Node,
    secret_key: SecretKey,
) -> Result<IrohReplicator, IrohReplicationError> {
    IrohReplicator::bind_loopback_with_secret_and_policy(
        node,
        secret_key,
        Arc::new(AllowAllAccessPolicy),
    )
    .await
}

#[derive(Debug)]
struct ApprovalPolicy;

impl AccessPolicy for ApprovalPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        Ok(AuthorizationDecision::from_rule(request, Ok(())))
    }

    fn approve<'a>(
        &'a self,
        authenticated_executor: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        challenge_id: &'a ChallengeId,
        approved: bool,
    ) -> myko_federation::AuthorityApprovalFuture<'a> {
        Box::pin(async move {
            let now = Utc::now();
            let request = AccessAttempt::scoped(
                authenticated_executor.clone(),
                presentation.clone(),
                AccessOperation::ApproveAuthority,
                ScopeId::new("authority:test"),
            );
            Ok(ApprovalDecision {
                id: ApprovalId::new("iroh-approval"),
                realm_id: AuthorityRealmId::new("test"),
                challenge_id: challenge_id.clone(),
                obligation_id: ObligationId::new("test-review"),
                approver: presentation.principal.clone(),
                binding: AuthorizationBinding::from_request(&request),
                approved,
                decided_at: now,
                expires_at: now + ChronoDuration::minutes(1),
                max_uses: 1,
            })
        })
    }
}

#[derive(Debug)]
struct PresentationPolicy {
    expected: AuthorityPresentation,
    operations: Arc<Mutex<Vec<AccessOperation>>>,
}

impl AccessPolicy for PresentationPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let rule = if request.presentation == self.expected {
            self.operations
                .lock()
                .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?
                .push(request.operation);
            Ok(())
        } else {
            Err("authority presentation was not preserved".to_owned())
        };
        Ok(AuthorizationDecision::from_rule(request, rule))
    }
}

#[tokio::test]
async fn iroh_client_submits_and_decodes_authenticated_approval() -> Result<(), String> {
    let server =
        IrohReplicator::bind_loopback_with_policy(Node::in_memory(), Arc::new(ApprovalPolicy))
            .await
            .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let decision = client
        .command_client(server.address())
        .approve_authority(ChallengeId::new("iroh-challenge"), true)
        .await
        .map_err(|error| error.to_string())?;
    assert!(decision.approved);
    assert_eq!(decision.challenge_id.as_str(), "iroh-challenge");
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())?;
    Ok(())
}

#[myko_service(RemoteRecord)]
pub struct RemoteService;

#[myko_item(service = RemoteService, scope_root)]
pub struct RemoteRecord {
    pub value: String,
}

#[myko_service(SelectedProject, SelectedScene, SelectedElement)]
pub struct SelectedService;

#[myko_item(service = SelectedService, scope_root)]
pub struct SelectedProject {
    pub name: String,
}

#[myko_item(service = SelectedService, scope_root, scoped_by = SelectedProject)]
pub struct SelectedScene {
    pub name: String,
}

#[myko_item(service = SelectedService, scoped_by = SelectedScene)]
pub struct SelectedElement {
    pub name: String,
}

#[derive(Debug)]
struct SelectedIntersectionPolicy {
    allowed: Vec<ScopeSelection>,
}

impl AccessPolicy for SelectedIntersectionPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        Ok(AuthorizationDecision::from_rule(request, Ok(())))
    }

    fn constrain_replication(
        &self,
        request: &AccessAttempt,
        selection: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationFailure> {
        let requested_selections = request.scope_selections();
        let requested = requested_selections.first();
        let scopes = self
            .allowed
            .iter()
            .filter(|candidate| {
                requested.is_some_and(|requested| requested.covers_in(candidate, topology))
            })
            .cloned()
            .collect();
        Ok(ReplicationSelection::Intersection {
            requested: Box::new(selection.clone()),
            scopes,
        })
    }
}

#[myko_command(bool, item = RemoteRecord)]
struct SetRemoteRecord {
    id: RemoteRecordId,
    scope: String,
    value: String,
}

impl CommandHandler for SetRemoteRecord {
    fn scope(&self, _node_id: NodeId) -> RemoteRecordId {
        RemoteRecordId::from(self.scope.clone())
    }

    fn execute(self, context: CommandContext) -> Result<bool, CommandError> {
        context.emit_set(&RemoteRecord {
            id: self.id,
            value: self.value,
        })?;
        Ok(true)
    }
}

#[tokio::test]
async fn iroh_item_application_and_live_clients_preserve_authority_presentations()
-> Result<(), String> {
    let source = Node::in_memory();
    let _record = commit_remote_record(
        &source,
        ScopeId::new("application-handler"),
        "presented",
        "authority",
    )?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let original = Principal::new(PrincipalId::new("person:owner"), PrincipalKind::Person);
    let executor = Principal::node(endpoint_principal_id(client.address().id));
    let presentation = AuthorityPresentation::direct(original.clone()).forward(ProvenanceHop {
        delegation_id: DelegationId::new("iroh-delegation"),
        delegator: original,
        delegate: executor,
        operation: ProvenanceOperation::AgentInvocation {
            agent_id: "remote-client".to_owned(),
        },
    });
    let operations = Arc::new(Mutex::new(Vec::new()));
    let policy: Arc<dyn AccessPolicy> = Arc::new(PresentationPolicy {
        expected: presentation.clone(),
        operations: Arc::clone(&operations),
    });
    let application = MykoApplication::builder()
        .service::<RemoteService>()
        .build();
    let server = IrohReplicator::bind_loopback_application_with_policy(
        ApplicationHost::new(source.clone(), application)?,
        policy,
    )
    .await
    .map_err(|error| error.to_string())?;

    let (_initial, items) = client
        .item_client(server.address())
        .with_authority(presentation.clone())
        .watch_serving_items(ScopeId::new("application-handler"), GetAllRemoteRecords)
        .await
        .map_err(|error| error.to_string())?;
    items.close();
    let live = client
        .subscribe_live_remote_with_authority(
            server.address(),
            vec!["presented-topic".to_owned()],
            Some(presentation),
        )
        .await
        .map_err(|error| error.to_string())?;
    live.close();

    {
        let observed = operations
            .lock()
            .map_err(|_| "presentation-policy lock is poisoned".to_owned())?;
        for expected in [
            AccessOperation::ReadItems,
            AccessOperation::FollowItems,
            AccessOperation::SubscribeLive,
        ] {
            if !observed.contains(&expected) {
                return Err(format!(
                    "authority presentation was not exercised for {expected:?}"
                ));
            }
        }
    }
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[derive(Debug)]
struct ReadOnlyScopePolicy {
    scope_id: ScopeId,
}

impl AccessPolicy for ReadOnlyScopePolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let is_read = matches!(
            request.operation,
            AccessOperation::ReadHistory
                | AccessOperation::ReadItems
                | AccessOperation::FollowItems
                | AccessOperation::FollowHistory
                | AccessOperation::ReadCommand
                | AccessOperation::ReadCommands
                | AccessOperation::WatchCommand
                | AccessOperation::WatchCommands
        );
        let rule = if is_read && request.scope_id() == Some(&self.scope_id) {
            Ok(())
        } else {
            Err("peer has read-only access to one scope".to_owned())
        };
        Ok(AuthorizationDecision::from_rule(request, rule))
    }
}

#[derive(Debug)]
struct ReadScopeSetPolicy {
    scope_ids: Vec<ScopeId>,
}

impl AccessPolicy for ReadScopeSetPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        let permitted = request.operation == AccessOperation::ReadHistory
            && request
                .scope_id()
                .is_some_and(|scope_id| self.scope_ids.contains(scope_id));
        let rule = if permitted {
            Ok(())
        } else {
            Err("peer cannot read this scope".to_owned())
        };
        Ok(AuthorizationDecision::from_rule(request, rule))
    }
}

#[derive(Debug)]
struct DenyAllPolicy;

impl AccessPolicy for DenyAllPolicy {
    fn decide(
        &self,
        request: &AccessAttempt,
    ) -> Result<AuthorizationDecision, AuthorityUnavailable> {
        Ok(AuthorizationDecision::from_rule(
            request,
            Err("test policy revoked access".to_owned()),
        ))
    }
}

fn commit_test_command(node: &Node, command_type: &str) -> Result<CommandRequest, String> {
    commit_test_command_in_scope(node, command_type, ScopeId::new("durable-cursor"))
}

fn commit_test_command_in_scope(
    node: &Node,
    command_type: &str,
    scope_id: ScopeId,
) -> Result<CommandRequest, String> {
    commit_test_command_for_service(node, command_type, ServiceId::new("test"), scope_id)
}

fn commit_test_command_for_service(
    node: &Node,
    command_type: &str,
    service_id: ServiceId,
    scope_id: ScopeId,
) -> Result<CommandRequest, String> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id,
        scope_id,
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: command_type.to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id.clone(),
            scope_id: request.scope_id.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: Vec::new(),
        },
        Vec::new(),
    )
    .map_err(|error| error.to_string())?;
    Ok(request)
}

fn commit_remote_record(
    node: &Node,
    scope_id: ScopeId,
    id: &str,
    value: &str,
) -> Result<RemoteRecord, String> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(RemoteService::SERVICE_ID),
        scope_id,
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "records.put".to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let record = RemoteRecord {
        id: RemoteRecordId::from(id),
        value: value.to_owned(),
    };
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![ItemMutation::set(&record).map_err(|error| error.to_string())?],
        },
        Vec::new(),
    )
    .map_err(|error| error.to_string())?;
    Ok(record)
}

fn commit_selected_project(
    node: &Node,
    project_id: &SelectedProjectId,
) -> Result<CommandRequest, String> {
    let project_scope = ScopeId::for_item::<SelectedProject>(project_id);
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(SelectedService::SERVICE_ID),
        scope_id: project_scope.clone(),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: vec![ResourceClaim {
            selection: ScopeSelection::Exact(project_scope.clone()),
            kind: ResourceClaimKind::Primary,
            source_node: None,
            service_id: Some(ServiceId::new(SelectedService::SERVICE_ID)),
            item_type: Some(SelectedProject::ITEM_TYPE.to_owned()),
            item_id: Some(project_id.as_ref().to_owned()),
            required_permissions: vec![FederationPermission::Write],
            required_operations: vec![AccessOperation::SubmitCommand],
            required_capabilities: Vec::new(),
        }],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "selected.project".to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let mut mutation = ItemMutation::set(&SelectedProject {
        id: project_id.clone(),
        name: "project".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    mutation.scope_id = Some(project_scope.as_str().to_owned());
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id.clone(),
            scope_id: request.scope_id.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![mutation],
        },
        Vec::new(),
    )
    .map_err(|error| error.to_string())?;
    Ok(request)
}

fn commit_selected_scene(
    node: &Node,
    project_id: &SelectedProjectId,
    scene_id: &SelectedSceneId,
    element_id: &SelectedElementId,
) -> Result<CommandRequest, String> {
    let project_scope = ScopeId::for_item::<SelectedProject>(project_id);
    let scene_scope = ScopeId::for_item::<SelectedScene>(scene_id);
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(SelectedService::SERVICE_ID),
        scope_id: project_scope.clone(),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: vec![
            ResourceClaim {
                selection: ScopeSelection::Exact(project_scope),
                kind: ResourceClaimKind::Primary,
                source_node: None,
                service_id: Some(ServiceId::new(SelectedService::SERVICE_ID)),
                item_type: Some(SelectedProject::ITEM_TYPE.to_owned()),
                item_id: Some(project_id.as_ref().to_owned()),
                required_permissions: vec![FederationPermission::Write],
                required_operations: vec![AccessOperation::SubmitCommand],
                required_capabilities: Vec::new(),
            },
            ResourceClaim {
                selection: ScopeSelection::Exact(scene_scope.clone()),
                kind: ResourceClaimKind::Affected,
                source_node: None,
                service_id: Some(ServiceId::new(SelectedService::SERVICE_ID)),
                item_type: None,
                item_id: None,
                required_permissions: vec![FederationPermission::Write],
                required_operations: Vec::new(),
                required_capabilities: Vec::new(),
            },
        ],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "selected.scene".to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let mut scene = ItemMutation::set(&SelectedScene {
        id: scene_id.clone(),
        selected_project_id: project_id.clone(),
        name: "scene".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    scene.scope_id = Some(scene_scope.as_str().to_owned());
    let mut element = ItemMutation::set(&SelectedElement {
        id: element_id.clone(),
        selected_scene_id: scene_id.clone(),
        name: "element".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    element.scope_id = Some(scene_scope.as_str().to_owned());
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id.clone(),
            scope_id: request.scope_id.clone(),
            causal_parents: vec![admission.snapshot().updated_at],
            changes: vec![scene, element],
        },
        Vec::new(),
    )
    .map_err(|error| error.to_string())?;
    Ok(request)
}

async fn wait_for_committed(node: &Node, command_id: CommandId) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if node
                .command(command_id)
                .map_err(|error| error.to_string())?
                .is_some_and(|command| command.state.is_committed())
            {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "peer follower did not deliver a committed command".to_owned())?
}

async fn wait_for_cursor(follower: &PeerSync) -> Result<LogPosition, String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(cursor) = follower.status().map_err(|error| error.to_string())?.cursor {
                return Ok::<LogPosition, String>(cursor);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "peer follower did not persist a cursor".to_owned())?
}

async fn wait_for_connection(follower: &PeerSync) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if follower
                .status()
                .map_err(|error| error.to_string())?
                .connected
            {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "peer follower did not establish its live stream".to_owned())?
}

#[tokio::test]
async fn remote_item_client_executes_the_same_typed_query_contract() -> Result<(), String> {
    let source = Node::in_memory();
    let service_id = ServiceId::new(RemoteService::SERVICE_ID);
    let scope_id = ScopeId::new("session:records");
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: service_id.clone(),
        scope_id: scope_id.clone(),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "records.put".to_owned(),
        payload: Vec::new(),
    };
    let admission = source
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let record = RemoteRecord {
        id: RemoteRecordId::from("record-1"),
        value: "remote".to_owned(),
    };
    let second = RemoteRecord {
        id: RemoteRecordId::from("record-2"),
        value: "second page".to_owned(),
    };
    source
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: service_id.clone(),
                scope_id: scope_id.clone(),
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![
                    ItemMutation::set(&record).map_err(|error| error.to_string())?,
                    ItemMutation::set(&second).map_err(|error| error.to_string())?,
                ],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;

    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let state = client
        .item_client(server.address())
        .item_state(ItemStateRequest::for_serving_item::<RemoteRecord>(scope_id).with_page_size(1))
        .await
        .map_err(|error| error.to_string())?;
    let snapshot = state
        .query(GetAllRemoteRecords)
        .map_err(|error| error.to_string())?;
    if snapshot.value != [record, second] || snapshot.through.is_none() {
        return Err("remote typed item query returned the wrong state".to_owned());
    }
    let typed = client
        .item_client(server.address())
        .query_serving_items(ScopeId::new("session:records"), GetAllRemoteRecords)
        .await
        .map_err(|error| error.to_string())?;
    if typed.value != snapshot.value || typed.through != snapshot.through {
        return Err("typed item client facade diverged from its collected state".to_owned());
    }
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn every_item_state_page_is_authorized_independently() -> Result<(), String> {
    let source = Node::in_memory();
    let service_id = ServiceId::new(RemoteService::SERVICE_ID);
    let scope_id = ScopeId::new("session:paged-policy");
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: service_id.clone(),
        scope_id: scope_id.clone(),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "records.put".to_owned(),
        payload: Vec::new(),
    };
    let admission = source
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let first = RemoteRecord {
        id: RemoteRecordId::from("record-1"),
        value: "first".to_owned(),
    };
    let second = RemoteRecord {
        id: RemoteRecordId::from("record-2"),
        value: "second".to_owned(),
    };
    source
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: service_id.clone(),
                scope_id: scope_id.clone(),
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![
                    ItemMutation::set(&first).map_err(|error| error.to_string())?,
                    ItemMutation::set(&second).map_err(|error| error.to_string())?,
                ],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;

    let server = bind_allow_all(source)
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: scope_id.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let item_client = client.item_client(server.address());
    let first_page = item_client
        .item_state_page(
            ItemStateRequest::for_serving_item::<RemoteRecord>(scope_id).with_page_size(1),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut continuation = first_page.request;
    continuation.after_item_id = first_page.next_after_item_id;
    if continuation.after_item_id.is_none() {
        return Err("first item-state page did not expose a continuation".to_owned());
    }

    server
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    if item_client.item_state_page(continuation).await.is_ok() {
        return Err("policy revocation did not deny the next item-state page".to_owned());
    }
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_typed_item_stream_filters_history_and_observes_revocation() -> Result<(), String> {
    let source = Node::in_memory();
    let scope_id = ScopeId::new("session:typed-stream");
    let initial = commit_remote_record(&source, scope_id.clone(), "record-1", "initial")?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: scope_id.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let (snapshot, mut subscription) = client
        .item_client(server.address())
        .watch_serving_items(scope_id.clone(), GetAllRemoteRecords)
        .await
        .map_err(|error| error.to_string())?;
    if snapshot.value != [initial.clone()]
        || subscription.current() != snapshot.value
        || subscription.source_node() != source.node_id()
    {
        return Err("typed item stream did not retain its initial snapshot".to_owned());
    }

    let _hidden = commit_remote_record(
        &source,
        ScopeId::new("session:hidden-stream"),
        "record-hidden",
        "must not cross the stream",
    )?;
    let second = commit_remote_record(&source, scope_id.clone(), "record-2", "live")?;
    let update = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "typed item stream did not deliver a matching commit".to_owned())?
        .map_err(|error| error.to_string())?;
    if update.value != [initial, second] || subscription.current() != update.value {
        return Err("typed item stream exposed unrelated or incomplete state".to_owned());
    }

    server
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    let revoked = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "typed item stream did not observe policy revocation".to_owned())?;
    if !matches!(revoked, Err(ref error) if error.to_string().contains("access denied")) {
        return Err(format!(
            "typed item stream returned the wrong revocation result: {revoked:?}"
        ));
    }
    subscription.close();
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_typed_item_stream_drives_hyphae_lifecycle_state() -> Result<(), String> {
    use hyphae::{Signal, Watchable as _};

    let source = Node::in_memory();
    let scope_id = ScopeId::new("session:reactive-stream");
    let initial = commit_remote_record(&source, scope_id.clone(), "record-1", "initial")?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: scope_id.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let reactive = client
        .item_client(server.address())
        .watch_serving_items_reactive(scope_id.clone(), GetAllRemoteRecords)
        .await
        .map_err(|error| error.to_string())?;
    let (updates_tx, updates_rx) = flume::bounded(16);
    let _guard = reactive.live().state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _ignored = updates_tx.send(state.clone());
        }
    });
    let _initial_notification = updates_rx.try_recv();

    let second = commit_remote_record(&source, scope_id.clone(), "record-2", "live")?;
    let update = tokio::time::timeout(Duration::from_secs(10), updates_rx.recv_async())
        .await
        .map_err(|_| "reactive native item stream did not update".to_owned())?
        .map_err(|error| error.to_string())?;
    if update.value != Some(vec![initial.clone(), second.clone()])
        || update.liveness != SubscriptionLiveness::Current
    {
        return Err(format!("unexpected reactive native state: {update:?}"));
    }

    server
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    let invalid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let update = updates_rx
                .recv_async()
                .await
                .map_err(|error| error.to_string())?;
            if matches!(update.liveness, SubscriptionLiveness::Invalid { .. }) {
                return Ok::<_, String>(update);
            }
        }
    })
    .await
    .map_err(|_| "reactive native item stream did not expose revocation".to_owned())??;
    if !matches!(
        invalid.liveness,
        SubscriptionLiveness::Invalid { ref reason }
            if reason.contains("access denied")
    ) {
        return Err(format!("unexpected invalid state: {invalid:?}"));
    }

    drop(reactive);
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

async fn assert_remote_cancellation(
    client: &IrohReplicator,
    server: &IrohReplicator,
    command_id: CommandId,
) -> Result<(), String> {
    let cancelled = client
        .cancel_remote(
            server.address(),
            command_id,
            "operator interrupted".to_owned(),
        )
        .await
        .map_err(|error| error.to_string())?;
    if !cancelled.command.as_ref().is_some_and(|command| {
        matches!(
            command.state,
            CommandState::Cancelled { ref reason } if reason == "operator interrupted"
        )
    }) {
        return Err(format!(
            "native cancellation did not become durable: {cancelled:?}"
        ));
    }
    let repeated = client
        .cancel_remote(
            server.address(),
            command_id,
            "different retry text".to_owned(),
        )
        .await
        .map_err(|error| error.to_string())?;
    if repeated.command != cancelled.command {
        return Err("native cancellation retry changed terminal state".to_owned());
    }
    Ok(())
}

#[tokio::test]
async fn two_iroh_endpoints_exchange_immutable_history() -> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("test"),
        scope_id: ScopeId::new("test"),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "test".to_owned(),
        payload: Vec::new(),
    };
    let admission = source
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    source
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: Vec::<ItemMutation>::new(),
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;

    let source_transport = bind_allow_all(source)
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let report = target_transport
        .pull(source_transport.address(), None)
        .await
        .map_err(|error| error.to_string())?;
    if report.applied != 2 {
        return Err(format!("unexpected Iroh replication report: {report:?}"));
    }
    let command = target
        .command(request.id)
        .map_err(|error| error.to_string())?;
    if !command.is_some_and(|command| command.state.is_committed()) {
        return Err("target did not ingest the committed command".to_owned());
    }
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn native_descriptor_verifies_transport_and_myko_identities() -> Result<(), String> {
    let source = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    source
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let descriptor = source.descriptor();
    let encoded = serde_json::to_vec(&descriptor).map_err(|error| error.to_string())?;
    let decoded: NativePeerReference =
        serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
    if decoded.descriptor() != &descriptor {
        return Err("native descriptor did not decode as a pinned peer".to_owned());
    }
    let endpoint_only =
        serde_json::to_vec(&descriptor.endpoint).map_err(|error| error.to_string())?;
    if serde_json::from_slice::<NativePeerReference>(&endpoint_only).is_ok() {
        return Err("an endpoint-only peer reference bypassed identity pinning".to_owned());
    }
    client
        .verify_descriptor(&descriptor)
        .await
        .map_err(|error| error.to_string())?;

    let wrong = NativeNodeDescriptor::new(NodeId::new(), descriptor.endpoint.clone());
    let error = match client.verify_descriptor(&wrong).await {
        Ok(()) => return Err("descriptor with another Myko identity was accepted".to_owned()),
        Err(error) => error,
    };
    if !error.to_string().contains("advertised Myko source") {
        return Err(format!(
            "descriptor mismatch returned the wrong error: {error}"
        ));
    }

    client.shutdown().await.map_err(|error| error.to_string())?;
    source.shutdown().await.map_err(|error| error.to_string())
}

fn assert_pairing_ttl_bounds(server: &IrohReplicator) -> Result<(), String> {
    let excessive = Duration::from_hours(24)
        .checked_add(Duration::from_millis(1))
        .ok_or_else(|| "test pairing TTL overflowed".to_owned())?;
    for invalid_ttl in [Duration::ZERO, Duration::from_nanos(1), excessive] {
        if server.issue_pairing_invitation(invalid_ttl).is_ok() {
            return Err(format!(
                "invalid pairing invitation TTL was accepted: {invalid_ttl:?}"
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn pairing_is_identity_bound_one_use_expiring_and_operator_visible() -> Result<(), String> {
    let server = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let mut receipts = server.subscribe_pairing_receipts();
    assert_pairing_ttl_bounds(&server)?;
    let invitation = server
        .issue_pairing_invitation(Duration::from_mins(1))
        .map_err(|error| error.to_string())?;
    let encoded = serde_json::to_vec(&invitation).map_err(|error| error.to_string())?;
    let mut encoded_value: serde_json::Value =
        serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
    let bearer = encoded_value
        .get("secret_hex")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "pairing invitation omitted its encoded bearer".to_owned())?
        .to_owned();
    let debug = format!("{invitation:?}");
    if debug.contains(&bearer) || !debug.contains("[redacted]") {
        return Err("pairing invitation debug output exposed its bearer".to_owned());
    }

    let secret = encoded_value
        .as_object_mut()
        .and_then(|object| object.get_mut("secret_hex"))
        .ok_or_else(|| "pairing invitation omitted its encoded bearer".to_owned())?;
    *secret = serde_json::Value::String("01".repeat(32));
    let tampered: PairingInvitation =
        serde_json::from_value(encoded_value).map_err(|error| error.to_string())?;
    let error = client
        .redeem_pairing(&tampered)
        .await
        .err()
        .ok_or_else(|| "tampered pairing bearer was accepted".to_owned())?;
    if !error.to_string().contains("proof did not verify") {
        return Err(format!("tampered pairing returned wrong error: {error}"));
    }

    let forged_client =
        NativeNodeDescriptor::new(client.node.node_id(), server.descriptor().endpoint);
    let mismatch = pairing::redeem_pairing(client.router.endpoint(), forged_client, &invitation)
        .await
        .err()
        .ok_or_else(|| "pairing accepted a descriptor for another endpoint".to_owned())?;
    if !mismatch
        .to_string()
        .contains("does not match authenticated endpoint")
    {
        return Err(format!(
            "identity-mismatched pairing returned wrong error: {mismatch}"
        ));
    }

    let receipt = client
        .redeem_pairing(&invitation)
        .await
        .map_err(|error| error.to_string())?;
    if receipt.server != server.descriptor()
        || receipt.client != client.descriptor()
        || receipt.comparison_code.len() != 6
    {
        return Err(format!(
            "pairing receipt lost identity binding: {receipt:?}"
        ));
    }
    let observed = tokio::time::timeout(Duration::from_secs(5), receipts.recv())
        .await
        .map_err(|_| "server did not observe redeemed pairing".to_owned())?
        .map_err(|error| error.to_string())?;
    if observed != [receipt.clone()] {
        return Err(format!(
            "server observed wrong pairing receipt: {observed:?}"
        ));
    }
    let replay = client
        .redeem_pairing(&invitation)
        .await
        .err()
        .ok_or_else(|| "one-use pairing invitation was replayed".to_owned())?;
    if !replay.to_string().contains("already used") {
        return Err(format!("pairing replay returned wrong error: {replay}"));
    }

    let expired = server
        .issue_pairing_invitation(Duration::from_millis(1))
        .map_err(|error| error.to_string())?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let expiry = client
        .redeem_pairing(&expired)
        .await
        .err()
        .ok_or_else(|| "expired pairing invitation was accepted".to_owned())?;
    if !expiry.to_string().contains("expired") {
        return Err(format!("pairing expiry returned wrong error: {expiry}"));
    }

    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn pairing_offer_delivers_one_receipt_to_both_nodes() -> Result<(), String> {
    let initiator = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let recipient = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let mut initiator_receipts = initiator.subscribe_pairing_receipts();
    let mut recipient_receipts = recipient.subscribe_pairing_receipts();

    let receipt = initiator
        .offer_pairing(&recipient.descriptor(), Duration::from_mins(1))
        .await
        .map_err(|error| error.to_string())?;
    if receipt.server != initiator.descriptor() || receipt.client != recipient.descriptor() {
        return Err(format!(
            "pairing offer lost its pinned identities: {receipt:?}"
        ));
    }
    let observed_by_initiator =
        tokio::time::timeout(Duration::from_secs(5), initiator_receipts.recv())
            .await
            .map_err(|_| "initiator did not observe its pairing offer".to_owned())?
            .map_err(|error| error.to_string())?;
    let observed_by_recipient =
        tokio::time::timeout(Duration::from_secs(5), recipient_receipts.recv())
            .await
            .map_err(|_| "recipient did not observe its pairing offer".to_owned())?
            .map_err(|error| error.to_string())?;
    if observed_by_initiator != [receipt.clone()] || observed_by_recipient != [receipt] {
        return Err("pairing offer produced different operator receipts".to_owned());
    }

    recipient
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    initiator
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[test]
fn persistent_secret_key_restores_the_same_transport_identity() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let path = directory.path().join("client").join("iroh-secret.json");
    let first = load_or_create_secret_key(&path).map_err(|error| error.to_string())?;
    let second = load_or_create_secret_key(&path).map_err(|error| error.to_string())?;

    if first.public() != second.public() {
        return Err("persistent key changed its Iroh identity".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = std::fs::metadata(path)
            .map_err(|error| error.to_string())?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o600 {
            return Err(format!("persistent key permissions were {mode:o}"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_scope_catalog_is_paginated_and_policy_filtered() -> Result<(), String> {
    let source = Node::in_memory();
    let first = ScopeId::new("session:alpha");
    let second = ScopeId::new("session:bravo");
    let secret = ScopeId::new("session:secret");
    commit_test_command_in_scope(&source, "first", first.clone())?;
    commit_test_command_in_scope(&source, "secret", secret)?;
    commit_test_command_in_scope(&source, "second", second.clone())?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadScopeSetPolicy {
            scope_ids: vec![first.clone(), second.clone()],
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;

    let first_page = client
        .list_scopes_remote(server.address(), None, NonZeroUsize::MIN)
        .await
        .map_err(|error| error.to_string())?;
    if first_page.source_node != source.node_id()
        || first_page.scopes != vec![first.clone()]
        || first_page.next_after != Some(first.clone())
    {
        return Err(format!("unexpected first catalog page: {first_page:?}"));
    }
    let second_page = client
        .list_scopes_remote(server.address(), first_page.next_after, NonZeroUsize::MIN)
        .await
        .map_err(|error| error.to_string())?;
    if second_page.source_node != source.node_id()
        || second_page.scopes != vec![second]
        || second_page.next_after.is_some()
    {
        return Err(format!("unexpected second catalog page: {second_page:?}"));
    }

    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_command_watch_is_gap_free_and_observes_revocation() -> Result<(), String> {
    let source = Node::in_memory();
    let command_policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    source
        .set_command_access_policy(command_policy.clone())
        .map_err(|error| error.to_string())?;
    let scope_id = ScopeId::new("session:command-watch");
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("test"),
        scope_id: scope_id.clone(),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "test.watch".to_owned(),
        payload: Vec::new(),
    };
    source
        .submit(request.clone())
        .map_err(|error| error.to_string())?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy { scope_id }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let (initial, mut subscription) = client
        .command_client(server.address())
        .watch_command(request.id)
        .await
        .map_err(|error| error.to_string())?;
    if !initial.command.is_some_and(|command| {
        command.request == request && command.state == CommandState::Submitted
    }) || subscription.current().state != CommandState::Submitted
    {
        return Err("native command watch returned the wrong initial state".to_owned());
    }

    source
        .claim(request.id)
        .map_err(|error| error.to_string())?;
    let executing = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "native command watch did not receive execution".to_owned())?
        .map_err(|error| error.to_string())?;
    if executing.state != CommandState::Executing {
        return Err(format!(
            "native command watch returned the wrong transition: {executing:?}"
        ));
    }

    server
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    if tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "revoked command watch did not close".to_owned())?
        .is_ok()
    {
        return Err("policy revocation did not close command watch".to_owned());
    }
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_scoped_pull_omits_unrelated_history_and_advances_cursor() -> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let wanted_scope = ScopeId::new("session:wanted");
    let wanted = commit_test_command_in_scope(&source, "wanted", wanted_scope.clone())?;
    let hidden = commit_test_command_in_scope(&source, "hidden", ScopeId::new("session:hidden"))?;
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;

    let first = target_transport
        .pull_scope(source_transport.address(), wanted_scope.clone(), None)
        .await
        .map_err(|error| error.to_string())?;
    if first.applied != 2
        || first.through != Some(LogPosition::new(4))
        || !target
            .command(wanted.id)
            .map_err(|error| error.to_string())?
            .is_some_and(|command| command.state.is_committed())
        || target
            .command(hidden.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!(
            "scoped native pull leaked or lost history: {first:?}"
        ));
    }

    let hidden_later =
        commit_test_command_in_scope(&source, "hidden-later", ScopeId::new("session:hidden"))?;
    let second = target_transport
        .pull_scope(
            source_transport.address(),
            wanted_scope,
            Some(first.checkpoint()),
        )
        .await
        .map_err(|error| error.to_string())?;
    if second.applied != 0
        || second.through != Some(LogPosition::new(6))
        || target
            .command(hidden_later.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!(
            "scoped cursor did not skip hidden history: {second:?}"
        ));
    }

    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_selected_pull_filters_service_and_service_scope() -> Result<(), String> {
    let source = Node::in_memory();
    let service_target = Node::in_memory();
    let scope_target = Node::in_memory();
    let wanted_scope = ScopeId::new("session:wanted");
    let wanted = commit_test_command_in_scope(&source, "wanted", wanted_scope.clone())?;
    let other_scope =
        commit_test_command_in_scope(&source, "other-scope", ScopeId::new("session:other"))?;
    let other_service = commit_test_command_for_service(
        &source,
        "other-service",
        ServiceId::new("other"),
        wanted_scope.clone(),
    )?;
    let source_transport = bind_allow_all(source)
        .await
        .map_err(|error| error.to_string())?;
    let service_transport = bind_allow_all(service_target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let scope_transport = bind_allow_all(scope_target.clone())
        .await
        .map_err(|error| error.to_string())?;

    let service_report = service_transport
        .pull_selected(
            source_transport.address(),
            ReplicationSelection::Service(ServiceId::new("test")),
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    if service_report.applied != 4
        || service_target
            .command(wanted.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || service_target
            .command(other_scope.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || service_target
            .command(other_service.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!(
            "service selection leaked or lost history: {service_report:?}"
        ));
    }

    let scope_report = scope_transport
        .pull_selected(
            source_transport.address(),
            ReplicationSelection::ServiceScope {
                service_id: ServiceId::new("test"),
                scope_id: wanted_scope,
            },
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    if scope_report.applied != 2
        || scope_target
            .command(wanted.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || scope_target
            .command(other_scope.id)
            .map_err(|error| error.to_string())?
            .is_some()
        || scope_target
            .command(other_service.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!(
            "service-scope selection leaked or lost history: {scope_report:?}"
        ));
    }

    scope_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    service_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn subtree_intersection_crosses_iroh_and_revalidates_complete_checkpoint()
-> Result<(), String> {
    let source = Node::in_memory();
    let project_id = SelectedProjectId::from("project-1");
    let scene_one = SelectedSceneId::from("scene-1");
    let scene_two = SelectedSceneId::from("scene-2");
    let scene_three = SelectedSceneId::from("scene-3");
    let project = commit_selected_project(&source, &project_id)?;
    let first = commit_selected_scene(
        &source,
        &project_id,
        &scene_one,
        &SelectedElementId::from("element-1"),
    )?;
    let second = commit_selected_scene(
        &source,
        &project_id,
        &scene_two,
        &SelectedElementId::from("element-2"),
    )?;
    let hidden = commit_selected_scene(
        &source,
        &project_id,
        &scene_three,
        &SelectedElementId::from("element-3"),
    )?;
    let project_scope = ScopeId::for_item::<SelectedProject>(&project_id);
    let scene_one_scope = ScopeId::for_item::<SelectedScene>(&scene_one);
    let scene_two_scope = ScopeId::for_item::<SelectedScene>(&scene_two);
    let scene_three_scope = ScopeId::for_item::<SelectedScene>(&scene_three);
    let requested_scope = ScopeSelection::Subtree(project_scope.clone());
    let requested = ReplicationSelection::Scopes(vec![requested_scope.clone()]);
    let allowed = vec![
        ScopeSelection::Exact(project_scope),
        ScopeSelection::Subtree(scene_one_scope.clone()),
        ScopeSelection::Subtree(scene_two_scope.clone()),
    ];
    let expected_effective = ReplicationSelection::Intersection {
        requested: Box::new(requested.clone()),
        scopes: allowed.clone(),
    };
    let source_policy: Arc<dyn AccessPolicy> = Arc::new(SelectedIntersectionPolicy {
        allowed: allowed.clone(),
    });
    let source_transport = IrohReplicator::bind_loopback_with_policy(source.clone(), source_policy)
        .await
        .map_err(|error| error.to_string())?;

    let pull_target = Node::in_memory();
    let pull_transport = bind_allow_all(pull_target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let pulled = pull_transport
        .pull_selected(source_transport.address(), requested.clone(), None)
        .await
        .map_err(|error| error.to_string())?;
    if pulled.selection != expected_effective
        || pull_target
            .command(project.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || pull_target
            .command(first.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || pull_target
            .command(second.id)
            .map_err(|error| error.to_string())?
            .is_none()
        || pull_target
            .command(hidden.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!("unsafe selected pull intersection: {pulled:?}"));
    }
    pull_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;

    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let database = directory.path().join("selected-follow.redb");
    let (target, journal) =
        RedbJournal::open_node_with_journal(&database).map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower = target_transport
        .follow_persisted_selected(
            source_transport.address(),
            requested.clone(),
            journal.clone(),
            Duration::from_millis(20),
        )
        .map_err(|error| error.to_string())?;
    if let Err(error) = wait_for_committed(&target, second.id).await {
        return Err(format!(
            "{error}; follower status: {:?}",
            follower.status().map_err(|error| error.to_string())?
        ));
    }
    let _cursor = wait_for_cursor(&follower).await?;
    if target
        .command(hidden.id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("selected follower leaked scene-3".to_owned());
    }
    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    drop(target);
    drop(journal);

    let (reopened, reopened_journal) =
        RedbJournal::open_node_with_journal(&database).map_err(|error| error.to_string())?;
    let restored_policy: Arc<dyn AccessPolicy> = Arc::new(SelectedIntersectionPolicy {
        allowed: allowed.clone(),
    });
    let reopened_transport =
        IrohReplicator::bind_loopback_with_policy(reopened.clone(), restored_policy)
            .await
            .map_err(|error| error.to_string())?;
    let reader = PrincipalId::new("node:selected-reader");
    let before_reconnect = reopened
        .query_items_selected(
            reader.clone(),
            AuthorityPresentation::direct_node(reader.clone()),
            source.node_id(),
            &requested_scope,
            GetAllSelectedElements,
        )
        .map_err(|error| error.to_string())?;
    if before_reconnect.complete
        || before_reconnect.visibility == ResourceVisibility::AuthoritativelyAbsent
        || before_reconnect.coverage == ProjectionCoverage::ReplicatedComplete
    {
        return Err(format!(
            "persisted cursor was incorrectly treated as current completeness: {before_reconnect:?}"
        ));
    }
    let resumed = reopened_transport
        .follow_persisted_selected(
            source_transport.address(),
            requested,
            reopened_journal,
            Duration::from_millis(20),
        )
        .map_err(|error| error.to_string())?;
    let projection = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let projection = reopened
                .query_items_selected(
                    reader.clone(),
                    AuthorityPresentation::direct_node(reader.clone()),
                    source.node_id(),
                    &requested_scope,
                    GetAllSelectedElements,
                )
                .map_err(|error| error.to_string())?;
            if projection.coverage == ProjectionCoverage::ReplicatedComplete {
                break Ok::<_, String>(projection);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "timed out revalidating selected checkpoint; status={:?}",
            resumed.status()
        )
    })??;
    if projection.coverage != ProjectionCoverage::ReplicatedComplete
        || projection.visibility != ResourceVisibility::Present
        || !projection.complete
        || projection.requested_fully_authorized
        || !matches!(projection.value, Some(ref elements) if elements.len() == 2)
        || projection.included_scopes.contains(&scene_three_scope)
    {
        return Err(format!(
            "durable effective selection did not restore completeness: {projection:?}"
        ));
    }

    resumed
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    reopened_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn persisted_selected_follower_skips_other_services() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let source = Node::in_memory();
    let hidden = commit_test_command_for_service(
        &source,
        "hidden",
        ServiceId::new("other"),
        ScopeId::new("session:hidden"),
    )?;
    let wanted = commit_test_command(&source, "wanted")?;
    let source_node = source.node_id();
    let source_transport = bind_allow_all(source)
        .await
        .map_err(|error| error.to_string())?;
    let (target, journal) =
        RedbJournal::open_node_with_journal(directory.path().join("selected-target.redb"))
            .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower = target_transport
        .follow_persisted_source_selected(
            source_transport.address(),
            source_node,
            ReplicationSelection::Service(ServiceId::new("test")),
            journal,
            Duration::from_millis(20),
        )
        .map_err(|error| error.to_string())?;

    wait_for_committed(&target, wanted.id).await?;
    if target
        .command(hidden.id)
        .map_err(|error| error.to_string())?
        .is_some()
        || follower.status().map_err(|error| error.to_string())?.cursor != Some(LogPosition::new(4))
    {
        return Err("selected follower leaked history or failed to advance its cursor".to_owned());
    }

    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn scoped_pull_resets_on_replacement_history_and_rejects_cross_scope_cursor()
-> Result<(), String> {
    let first_source = Node::in_memory();
    let replacement_source = Node::in_memory();
    let target = Node::in_memory();
    let scope = ScopeId::new("session:source-aware");
    commit_test_command_in_scope(&first_source, "first-a", scope.clone())?;
    commit_test_command_in_scope(&first_source, "first-b", scope.clone())?;
    let replacement =
        commit_test_command_in_scope(&replacement_source, "replacement", scope.clone())?;
    let first_transport = bind_allow_all(first_source)
        .await
        .map_err(|error| error.to_string())?;
    let replacement_transport = bind_allow_all(replacement_source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;

    let first = target_transport
        .pull_scope(first_transport.address(), scope.clone(), None)
        .await
        .map_err(|error| error.to_string())?;
    if first.through != Some(LogPosition::new(4)) {
        return Err(format!("unexpected initial scoped cursor: {first:?}"));
    }
    let replacement_report = target_transport
        .pull_scope(
            replacement_transport.address(),
            scope.clone(),
            Some(first.checkpoint()),
        )
        .await
        .map_err(|error| error.to_string())?;
    if replacement_report.source_node != replacement_source.node_id()
        || replacement_report.through != Some(LogPosition::new(2))
        || replacement_report.applied != 2
        || !target
            .command(replacement.id)
            .map_err(|error| error.to_string())?
            .is_some_and(|command| command.state.is_committed())
    {
        return Err(format!(
            "replacement scope was not replayed from its beginning: {replacement_report:?}"
        ));
    }
    let wrong_scope = target_transport
        .pull_scope(
            replacement_transport.address(),
            ScopeId::new("session:other"),
            Some(replacement_report.checkpoint()),
        )
        .await;
    if !matches!(wrong_scope, Err(IrohReplicationError::Cursor(_))) {
        return Err(format!("cross-scope cursor was accepted: {wrong_scope:?}"));
    }

    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    replacement_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    first_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn scoped_follower_replays_then_tracks_only_one_scope() -> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let wanted_scope = ScopeId::new("session:wanted-follow");
    let hidden = commit_test_command_in_scope(
        &source,
        "hidden-before-follow",
        ScopeId::new("session:hidden-follow"),
    )?;
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower = target_transport
        .follow_scope(
            source_transport.address(),
            wanted_scope.clone(),
            None,
            Duration::from_millis(10),
        )
        .map_err(|error| error.to_string())?;
    wait_for_connection(&follower).await?;

    let wanted = commit_test_command_in_scope(&source, "wanted-live", wanted_scope)?;
    wait_for_committed(&target, wanted.id).await?;
    if target
        .command(hidden.id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("scoped follower imported hidden replay history".to_owned());
    }
    let status = follower.status().map_err(|error| error.to_string())?;
    if status.cursor != Some(LogPosition::new(4)) || status.successful_batches < 4 {
        return Err(format!(
            "scoped follower did not advance globally: {status:?}"
        ));
    }

    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn embedded_command_client_uses_the_same_unclaimed_contract() -> Result<(), String> {
    let node = Node::in_memory();
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())
        .map_err(|error| error.to_string())?;
    let application = MykoApplication::builder()
        .service::<RemoteService>()
        .build();
    let application = ApplicationHost::new(node.clone(), application)?;
    let submitted = application
        .submit_typed_command(SetRemoteRecord {
            id: RemoteRecordId::from("embedded-command"),
            scope: "session:test".to_owned(),
            value: "hello in process".to_owned(),
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some(snapshot) = submitted.command.as_ref() else {
        return Err("embedded command returned no state".to_owned());
    };
    if submitted.source_node != node.node_id() || snapshot.state != CommandState::Submitted {
        return Err(format!("unexpected embedded response: {submitted:?}"));
    }
    let queried = myko_federation::CommandClient::command_state(&application, snapshot.request.id)
        .await
        .map_err(|error| error.to_string())?;
    if queried != submitted {
        return Err("embedded command facade changed the command projection".to_owned());
    }
    Ok(())
}

#[tokio::test]
async fn native_command_catalog_collects_authorized_pages() -> Result<(), String> {
    let source = Node::in_memory();
    let scope_id = ScopeId::new("session:catalog");
    let first = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
    let second = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: scope_id.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;

    let catalog = client
        .command_client(server.address())
        .command_states(CommandStateRequest {
            source_node: None,
            service_id: ServiceId::new("test"),
            scope_id,
            command_type: "prompt".to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: 1,
        })
        .await
        .map_err(|error| error.to_string())?;
    if catalog.serving_node != source.node_id()
        || catalog.commands.len() != 2
        || !catalog
            .commands
            .iter()
            .all(|entry| entry.command.state.is_committed())
    {
        return Err(format!("unexpected remote command catalog: {catalog:?}"));
    }
    let mut command_ids = catalog
        .commands
        .iter()
        .map(|entry| entry.command.request.id)
        .collect::<Vec<_>>();
    command_ids.sort_unstable_by_key(|id| id.as_uuid());
    let mut expected = vec![first.id, second.id];
    expected.sort_unstable_by_key(|id| id.as_uuid());
    if command_ids != expected {
        return Err("remote command catalog returned the wrong commands".to_owned());
    }

    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn native_command_catalog_watches_new_and_changed_commands() -> Result<(), String> {
    let source = Node::in_memory();
    let scope_id = ScopeId::new("session:catalog-follow");
    let first = commit_test_command_in_scope(&source, "prompt", scope_id.clone())?;
    let server = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    server
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: scope_id.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let client = bind_allow_all(Node::in_memory())
        .await
        .map_err(|error| error.to_string())?;
    let remote = client.command_client(server.address());
    let (initial, mut subscription) = remote
        .watch_commands(CommandStateRequest {
            source_node: None,
            service_id: ServiceId::new("test"),
            scope_id: scope_id.clone(),
            command_type: "prompt".to_owned(),
            snapshot_through: None,
            after_command_id: None,
            page_size: 10,
        })
        .await
        .map_err(|error| error.to_string())?;
    if initial
        .commands
        .first()
        .is_none_or(|entry| entry.command.request.id != first.id)
        || initial.commands.len() != 1
    {
        return Err("command watch returned the wrong initial catalog".to_owned());
    }
    let mut wrong_server = initial.clone();
    wrong_server.serving_node = Node::in_memory().node_id();
    match remote.watch_command_states(&wrong_server).await {
        Err(IrohReplicationError::Stream(message)) if message.contains("another serving node") => {}
        Err(error) => {
            return Err(format!(
                "command watch returned the wrong serving-node error: {error}"
            ));
        }
        Ok(unexpected) => {
            unexpected.close();
            return Err("command watch accepted another server's cursor".to_owned());
        }
    }

    let second = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("test"),
        scope_id,
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "prompt".to_owned(),
        payload: Vec::new(),
    };
    source
        .admit(second.clone())
        .map_err(|error| error.to_string())?;
    let submitted = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "command catalog did not receive submission".to_owned())?
        .map_err(|error| error.to_string())?;
    if !submitted
        .commands
        .iter()
        .any(|entry| entry.command.request.id == second.id)
    {
        return Err("command catalog did not materialize the new submission".to_owned());
    }
    source
        .retry(second.id, "test transition")
        .map_err(|error| error.to_string())?;
    let retrying = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "command catalog did not receive retry transition".to_owned())?
        .map_err(|error| error.to_string())?;
    if !retrying.commands.iter().any(|entry| {
        entry.command.request.id == second.id
            && matches!(entry.command.state, CommandState::Retrying { .. })
    }) {
        return Err("command catalog did not advance the command lifecycle".to_owned());
    }

    server
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    let revoked = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
        .await
        .map_err(|_| "command catalog access revocation was not enforced".to_owned())?;
    if !matches!(revoked, Err(ref error) if error.to_string().contains("access denied")) {
        return Err(format!(
            "command catalog remained open after revocation: {revoked:?}"
        ));
    }
    subscription.close();
    client.shutdown().await.map_err(|error| error.to_string())?;
    server.shutdown().await.map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn native_remote_submission_is_idempotent_and_never_claims_execution() -> Result<(), String> {
    let server = Node::in_memory();
    let client = Node::in_memory();
    let application = MykoApplication::builder()
        .service::<RemoteService>()
        .build();
    let server_transport = IrohReplicator::bind_loopback_application_with_policy(
        ApplicationHost::new(server.clone(), application)?,
        Arc::new(AllowAllAccessPolicy),
    )
    .await
    .map_err(|error| error.to_string())?;
    let client_transport = bind_allow_all(client)
        .await
        .map_err(|error| error.to_string())?;
    let server_address = server_transport.address();
    let command = CommandSubmission::for_command(&SetRemoteRecord {
        id: RemoteRecordId::from("remote-command"),
        scope: "session:test".to_owned(),
        value: "hello over native iroh".to_owned(),
    })
    .map_err(|error| error.to_string())?;

    let submitted = client_transport
        .submit_remote(server_address.clone(), command.clone())
        .await
        .map_err(|error| error.to_string())?;
    let Some(snapshot) = submitted.command else {
        return Err("remote submission returned no command".to_owned());
    };
    if submitted.source_node != server.node_id()
        || snapshot.request.id != command.id
        || snapshot.state != CommandState::Submitted
    {
        return Err(format!("unexpected remote submission: {snapshot:?}"));
    }

    let repeated = client_transport
        .submit_remote(server_address.clone(), command.clone())
        .await
        .map_err(|error| error.to_string())?;
    if repeated.command.as_ref() != Some(&snapshot) {
        return Err("stable remote submission was not idempotent".to_owned());
    }
    let mut conflicting = command.clone();
    conflicting.payload = serde_json::to_vec(&SetRemoteRecord {
        id: RemoteRecordId::from("remote-command"),
        scope: "session:test".to_owned(),
        value: "conflicting reuse".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    let conflict = client_transport
        .submit_remote(server_address.clone(), conflicting)
        .await;
    if !matches!(
        conflict,
        Err(IrohReplicationError::Stream(ref message))
            if message.contains("remote command failed")
    ) {
        return Err(format!(
            "conflicting remote command was accepted: {conflict:?}"
        ));
    }
    let queried = client_transport
        .command_client(server_address)
        .command_state(command.id)
        .await
        .map_err(|error| error.to_string())?;
    if queried.command.as_ref() != Some(&snapshot) {
        return Err("native command query did not return submitted state".to_owned());
    }
    if !server
        .claim(command.id)
        .map_err(|error| error.to_string())?
        .should_execute()
    {
        return Err("remote client claimed execution before the local handler".to_owned());
    }
    assert_remote_cancellation(&client_transport, &server_transport, command.id).await?;

    client_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    server_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn native_policy_limits_history_and_commands_before_exposure_or_mutation()
-> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let granted_scope = ScopeId::new("session:granted");
    let granted = commit_test_command_in_scope(&source, "granted", granted_scope.clone())?;
    let hidden =
        commit_test_command_in_scope(&source, "hidden", ScopeId::new("session:hidden-policy"))?;
    let application = MykoApplication::builder()
        .service::<RemoteService>()
        .build();
    let source_transport = IrohReplicator::bind_loopback_application_with_policy(
        ApplicationHost::new(source.clone(), application)?,
        Arc::new(AllowAllAccessPolicy),
    )
    .await
    .map_err(|error| error.to_string())?;
    source_transport
        .set_access_policy(Arc::new(ReadOnlyScopePolicy {
            scope_id: granted_scope.clone(),
        }))
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let address = source_transport.address();

    let report = target_transport
        .pull_scope(address.clone(), granted_scope.clone(), None)
        .await
        .map_err(|error| error.to_string())?;
    if report.applied != 2
        || target
            .command(hidden.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err(format!("granted scoped read leaked history: {report:?}"));
    }
    if target_transport.pull(address.clone(), None).await.is_ok() {
        return Err("policy allowed unscoped history replication".to_owned());
    }
    let queried = target_transport
        .command_remote(address.clone(), granted.id)
        .await
        .map_err(|error| error.to_string())?;
    if !queried
        .command
        .is_some_and(|command| command.state.is_committed())
    {
        return Err("policy-blocked an allowed command read".to_owned());
    }
    if target_transport
        .cancel_remote(address.clone(), granted.id, "denied".to_owned())
        .await
        .is_ok()
    {
        return Err("read-only policy allowed command cancellation".to_owned());
    }
    let denied = CommandSubmission::for_command(&SetRemoteRecord {
        id: RemoteRecordId::from("denied-submit"),
        scope: granted_scope.to_string(),
        value: "denied".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    if target_transport
        .submit_remote(address.clone(), denied.clone())
        .await
        .is_ok()
        || source
            .command(denied.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("read-only policy allowed or persisted submission".to_owned());
    }
    if target_transport
        .subscribe_live_remote(address, vec!["session:granted".to_owned()])
        .await
        .is_ok()
    {
        return Err("read-only policy allowed an ungranted live topic".to_owned());
    }

    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn replacing_policy_revokes_open_history_and_live_streams() -> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let address = source_transport.address();
    let follower = target_transport.follow(address.clone(), None, Duration::from_millis(20));
    wait_for_connection(&follower).await?;
    let mut live = target_transport
        .subscribe_live_remote(address, vec!["session:revoked".to_owned()])
        .await
        .map_err(|error| error.to_string())?;

    source_transport
        .set_access_policy(Arc::new(DenyAllPolicy))
        .map_err(|error| error.to_string())?;
    let live_error = tokio::time::timeout(Duration::from_secs(10), live.recv())
        .await
        .map_err(|_| "open live stream did not observe policy revocation".to_owned())?;
    if !matches!(live_error, Err(ref error) if error.to_string().contains("access denied")) {
        return Err(format!(
            "open live stream returned the wrong revocation result: {live_error:?}"
        ));
    }
    live.close();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = follower.status().map_err(|error| error.to_string())?;
            if !status.connected
                && status
                    .last_error
                    .as_ref()
                    .is_some_and(|message| message.contains("access denied"))
            {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "open history stream did not observe policy revocation".to_owned())??;

    let withheld = commit_test_command(&source, "withheld-while-revoked")?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    if target
        .command(withheld.id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("revoked history stream continued importing events".to_owned());
    }

    source_transport
        .set_access_policy(Arc::new(AllowAllAccessPolicy))
        .map_err(|error| error.to_string())?;
    wait_for_committed(&target, withheld.id).await?;

    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn native_live_stream_filters_best_effort_events_without_durable_history()
-> Result<(), String> {
    let source = Node::in_memory();
    let client = Node::in_memory();
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let client_transport = bind_allow_all(client)
        .await
        .map_err(|error| error.to_string())?;
    let mut live = client_transport
        .subscribe_live_remote(source_transport.address(), vec!["session:a".to_owned()])
        .await
        .map_err(|error| error.to_string())?;
    if live.source_node() != source.node_id() {
        return Err("live stream advertised the wrong Myko source".to_owned());
    }

    let unrelated = source_transport
        .publish_live("session:b", b"ignore".to_vec())
        .map_err(|error| error.to_string())?;
    if unrelated.delivered != 0 || unrelated.dropped != 0 {
        return Err(format!(
            "topic filter received an unrelated event: {unrelated:?}"
        ));
    }
    let published = source_transport
        .publish_live("session:a", b"delta".to_vec())
        .map_err(|error| error.to_string())?;
    if published.delivered != 1 || published.dropped != 0 {
        return Err(format!("live event was not delivered: {published:?}"));
    }
    let event = tokio::time::timeout(Duration::from_secs(10), live.recv())
        .await
        .map_err(|_| "timed out waiting for native live event".to_owned())?
        .map_err(|error| error.to_string())?;
    if event.source_node != source.node_id()
        || event.sequence != 1
        || event.topic != "session:a"
        || event.payload != b"delta"
    {
        return Err(format!("unexpected native live event: {event:?}"));
    }
    if !source
        .events_after(None)
        .map_err(|error| error.to_string())?
        .is_empty()
    {
        return Err("best-effort live event entered durable history".to_owned());
    }

    live.close();
    client_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn follower_delivers_changes_committed_after_it_starts() -> Result<(), String> {
    let source = Node::in_memory();
    let target = Node::in_memory();
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower =
        target_transport.follow(source_transport.address(), None, Duration::from_mins(1));
    let mut target_events = target.subscribe(None).map_err(|error| error.to_string())?;
    wait_for_connection(&follower).await?;

    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("test"),
        scope_id: ScopeId::new("live"),
        principal_id: PrincipalId::new("node:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("node:test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "after-follow".to_owned(),
        payload: Vec::new(),
    };
    let admission = source
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    source
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id,
                scope_id: request.scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: Vec::new(),
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let envelope = target_events
                .recv_async()
                .await
                .map_err(|error| error.to_string())?;
            if matches!(
                envelope.event,
                myko_federation::NodeEvent::CommandCommitted { ref command, .. }
                    if command.request.id == request.id
            ) {
                return Ok::<(), String>(());
            }
        }
    })
    .await
    .map_err(|_| "peer follower did not deliver the live commit".to_owned())??;

    let status = follower.status().map_err(|error| error.to_string())?;
    if !status.connected
        || status.successful_connections != 1
        || status.successful_batches == 0
        || status.cursor.is_none()
        || status.last_error.is_some()
    {
        return Err(format!("unexpected peer follower status: {status:?}"));
    }
    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn peer_supervisor_multiplexes_and_removes_independent_followers() -> Result<(), String> {
    let first_source = Node::in_memory();
    let second_source = Node::in_memory();
    let target = Node::in_memory();
    let first_initial = commit_test_command(&first_source, "first-initial")?;
    let second_initial = commit_test_command(&second_source, "second-initial")?;

    let first_transport = bind_allow_all(first_source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let second_transport = bind_allow_all(second_source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let first_address = first_transport.address();
    let second_address = second_transport.address();
    let supervisor = PeerSupervisor::new(target_transport.clone());

    if supervisor
        .upsert(first_address.clone(), None, Duration::from_mins(1))
        .await
        .map_err(|error| error.to_string())?
        || supervisor
            .upsert(second_address.clone(), None, Duration::from_mins(1))
            .await
            .map_err(|error| error.to_string())?
    {
        return Err("new peer unexpectedly replaced a follower".to_owned());
    }
    wait_for_committed(&target, first_initial.id).await?;
    wait_for_committed(&target, second_initial.id).await?;
    if supervisor
        .statuses()
        .map_err(|error| error.to_string())?
        .len()
        != 2
    {
        return Err("supervisor did not retain both peer followers".to_owned());
    }

    if !supervisor
        .remove(first_address.id)
        .await
        .map_err(|error| error.to_string())?
    {
        return Err("supervisor did not remove the first peer".to_owned());
    }
    let first_after_removal = commit_test_command(&first_source, "first-after-removal")?;
    let second_after_removal = commit_test_command(&second_source, "second-after-removal")?;
    wait_for_committed(&target, second_after_removal.id).await?;
    if target
        .command(first_after_removal.id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("removed peer continued delivering history".to_owned());
    }
    let statuses = supervisor.statuses().map_err(|error| error.to_string())?;
    if statuses.len() != 1
        || statuses
            .first()
            .is_none_or(|status| status.peer.id != second_address.id)
    {
        return Err(format!("unexpected remaining peer set: {statuses:?}"));
    }

    supervisor
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    second_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    first_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn follower_resumes_from_a_redb_cursor_after_restart() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let path = directory.path().join("target.redb");
    let source = Node::in_memory();
    let first = commit_test_command(&source, "before-restart")?;
    let source_transport = bind_allow_all(source.clone())
        .await
        .map_err(|error| error.to_string())?;
    let source_address = source_transport.address();

    let (target, journal) =
        RedbJournal::open_node_with_journal(&path).map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower = target_transport
        .follow_persisted(
            source_address.clone(),
            journal.clone(),
            Duration::from_millis(20),
        )
        .map_err(|error| error.to_string())?;
    wait_for_committed(&target, first.id).await?;
    let cursor_before_restart = wait_for_cursor(&follower).await?;
    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    drop(target);
    drop(journal);

    let (reopened, reopened_journal) =
        RedbJournal::open_node_with_journal(&path).map_err(|error| error.to_string())?;
    let reopened_transport = bind_allow_all(reopened.clone())
        .await
        .map_err(|error| error.to_string())?;
    let resumed = reopened_transport
        .follow_persisted(source_address, reopened_journal, Duration::from_millis(20))
        .map_err(|error| error.to_string())?;
    if resumed.status().map_err(|error| error.to_string())?.cursor != Some(cursor_before_restart) {
        return Err("restarted follower did not load its durable cursor".to_owned());
    }

    let second = commit_test_command(&source, "after-restart")?;
    wait_for_committed(&reopened, second.id).await?;
    resumed
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    reopened_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    source_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::test]
async fn persisted_follower_resets_when_transport_peer_has_a_new_myko_history() -> Result<(), String>
{
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let target_path = directory.path().join("target.redb");
    let transport_secret = SecretKey::generate();

    let first_source = Node::in_memory();
    let first_source_id = first_source.node_id();
    let first_command = commit_test_command(&first_source, "first-history")?;
    let first_transport = bind_with_secret_allow_all(first_source, transport_secret.clone())
        .await
        .map_err(|error| error.to_string())?;
    let first_address = first_transport.address();

    let (target, journal) =
        RedbJournal::open_node_with_journal(&target_path).map_err(|error| error.to_string())?;
    let target_transport = bind_allow_all(target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let follower = target_transport
        .follow_persisted(
            first_address.clone(),
            journal.clone(),
            Duration::from_millis(20),
        )
        .map_err(|error| error.to_string())?;
    wait_for_committed(&target, first_command.id).await?;
    let first_cursor = wait_for_cursor(&follower).await?;
    follower
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    target_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    first_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    drop(target);
    drop(journal);

    let replacement_source = Node::in_memory();
    let replacement_source_id = replacement_source.node_id();
    if replacement_source_id == first_source_id {
        return Err("fresh source unexpectedly reused its Myko node identity".to_owned());
    }
    let replacement_command = commit_test_command(&replacement_source, "new-history")?;
    let replacement_transport = bind_with_secret_allow_all(replacement_source, transport_secret)
        .await
        .map_err(|error| error.to_string())?;
    let replacement_address = replacement_transport.address();
    if replacement_address.id != first_address.id {
        return Err("test transport identity was not preserved".to_owned());
    }

    let (reopened_target, reopened_journal) =
        RedbJournal::open_node_with_journal(&target_path).map_err(|error| error.to_string())?;
    let reopened_transport = bind_allow_all(reopened_target.clone())
        .await
        .map_err(|error| error.to_string())?;
    let resumed = reopened_transport
        .follow_persisted(
            replacement_address,
            reopened_journal.clone(),
            Duration::from_mins(1),
        )
        .map_err(|error| error.to_string())?;
    if resumed.status().map_err(|error| error.to_string())?.cursor != Some(first_cursor) {
        return Err("test did not begin from the first source's cursor".to_owned());
    }
    wait_for_committed(&reopened_target, replacement_command.id).await?;

    let status = resumed.status().map_err(|error| error.to_string())?;
    if status.source_node != Some(replacement_source_id) || status.successful_connections < 2 {
        return Err(format!(
            "follower did not identify and restart the replacement history: {status:?}"
        ));
    }
    let key = replication_cursor_key(first_address.id, &ReplicationSelection::All);
    let checkpoint = reopened_journal
        .load_checkpoint(&key)
        .map_err(|error| error.to_string())?;
    if !checkpoint.as_ref().is_some_and(|checkpoint| {
        checkpoint.source_node == replacement_source_id && checkpoint.position.is_some()
    }) {
        return Err(format!(
            "replacement source checkpoint was not durable: {checkpoint:?}"
        ));
    }

    resumed
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    reopened_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    replacement_transport
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}
