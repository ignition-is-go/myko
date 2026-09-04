use super::*;
use myko_items::{myko_item, myko_service};

fn install_allow_all(node: &Node) -> Arc<dyn AccessPolicy> {
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone()).unwrap();
    policy
}

fn allow_all_node() -> Node {
    static POLICY: std::sync::OnceLock<Arc<dyn AccessPolicy>> = std::sync::OnceLock::new();
    let node = Node::in_memory();
    let policy = POLICY
        .get_or_init(|| Arc::new(AllowAllAccessPolicy))
        .clone();
    node.set_command_access_policy(policy).unwrap();
    node
}

#[derive(Debug)]
struct SelectedScopesPolicy {
    allowed: Vec<ScopeSelection>,
}

impl AccessPolicy for SelectedScopesPolicy {
    fn authorize(&self, _request: &AccessAttempt) -> Result<(), String> {
        Ok(())
    }

    fn constrain_replication(
        &self,
        request: &AccessAttempt,
        selection: &ReplicationSelection,
        topology: &ScopeTopology,
    ) -> Result<ReplicationSelection, AuthorizationDecision> {
        let requested_selections = request.scope_selections();
        let requested = requested_selections.first();
        let scopes = self
            .allowed
            .iter()
            .filter(|candidate| {
                requested
                    .is_some_and(|requested| requested.contains_scope(candidate.root(), topology))
            })
            .cloned()
            .collect::<Vec<_>>();
        Ok(ReplicationSelection::Intersection {
            requested: Box::new(selection.clone()),
            scopes,
        })
    }
}

#[test]
fn startup_gates_are_shared_by_every_node_clone() {
    let node = allow_all_node();
    let peer_handle = node.clone();
    assert!(node.is_ready());

    let startup = node.hold_startup();
    assert!(!node.is_ready());
    assert!(!peer_handle.is_ready());

    startup.ready();
    assert!(node.is_ready());
    assert!(peer_handle.is_ready());
}

#[test]
fn scope_grants_are_directional_and_require_subscription_explicitly() {
    let scope_id = ScopeId::new("project:pine");
    let principal = PrincipalId::new("iroh:node-b");
    let policy = ScopeGrantPolicy::new(vec![ScopeGrant {
        scope_id: scope_id.clone(),
        coverage: ScopeGrantCoverage::Exact,
        grantee: principal.clone(),
        permissions: vec![FederationPermission::ReadState],
    }]);
    let read = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal),
        AccessOperation::ReadItems,
        scope_id,
    );
    assert!(policy.authorize(&read).is_ok());

    let mut follow = read.clone();
    follow.operation = AccessOperation::FollowItems;
    assert!(policy.authorize(&follow).is_err());
    let mut wrong_principal = read;
    wrong_principal.principal_id = PrincipalId::new("iroh:node-c");
    assert!(policy.authorize(&wrong_principal).is_err());
}

#[test]
fn exact_single_scope_history_exposes_its_typed_scope() {
    let principal = PrincipalId::new("iroh:node-b");
    let scope_id = ScopeId::new("project:pine");
    let mut request = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal),
        AccessOperation::ReadHistory,
        scope_id.clone(),
    );
    request.target =
        AccessTarget::History(ReplicationSelection::Scopes(vec![ScopeSelection::Exact(
            scope_id.clone(),
        )]));

    assert_eq!(request.scope_id(), Some(&scope_id));

    request.target = AccessTarget::History(ReplicationSelection::Scopes(vec![
        ScopeSelection::Exact(scope_id),
        ScopeSelection::Exact(ScopeId::new("project:cedar")),
    ]));
    assert_eq!(request.scope_id(), None);
}

#[test]
fn composite_scope_access_requires_coverage_for_every_selection() {
    let principal = PrincipalId::new("iroh:node-b");
    let project = ScopeId::new("project:pine");
    let first_scene = ScopeId::new("scene:first");
    let second_scene = ScopeId::new("scene:second");
    let mut request = AccessAttempt::scoped(
        principal.clone(),
        AuthorityPresentation::direct_node(principal.clone()),
        AccessOperation::FollowHistory,
        project.clone(),
    );
    request.resource_claims.clear();
    request.target = AccessTarget::History(ReplicationSelection::Scopes(vec![
        ScopeSelection::Exact(project.clone()),
        ScopeSelection::Subtree(first_scene.clone()),
        ScopeSelection::Subtree(second_scene.clone()),
    ]));
    let permissions = vec![
        FederationPermission::ReadHistory,
        FederationPermission::Subscribe,
    ];
    let mut grants = vec![
        ScopeGrant {
            scope_id: project,
            coverage: ScopeGrantCoverage::Exact,
            grantee: principal.clone(),
            permissions: permissions.clone(),
        },
        ScopeGrant {
            scope_id: first_scene,
            coverage: ScopeGrantCoverage::Subtree,
            grantee: principal.clone(),
            permissions: permissions.clone(),
        },
    ];
    assert!(
        ScopeGrantPolicy::new(grants.clone())
            .authorize(&request)
            .is_err()
    );

    grants.push(ScopeGrant {
        scope_id: second_scene,
        coverage: ScopeGrantCoverage::Subtree,
        grantee: principal,
        permissions,
    });
    assert!(ScopeGrantPolicy::new(grants).authorize(&request).is_ok());
}

#[test]
fn new_nested_scope_must_be_created_from_its_parent_scope() {
    let node = allow_all_node();
    let project_id = FederationProjectId::from("project-1");
    let scene_id = FederationSceneId::from("scene-1");
    let scene_scope = ScopeId::for_item::<FederationScene>(&scene_id);
    let mut command = request(CommandId::new());
    command.scope_id = ScopeId::new("unrelated:scope");
    let executing = node.admit(command.clone()).unwrap().snapshot().clone();
    let scene = FederationScene {
        id: scene_id,
        federation_project_id: project_id,
        name: "scene".to_owned(),
    };
    let result = node.commit(
        command.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: command.id,
            service_id: command.service_id,
            scope_id: command.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![mutation_in(&scene_scope, &scene)],
        },
        Vec::new(),
    );
    assert!(matches!(
        result,
        Err(NodeError::InvalidItemMutation(reason))
            if reason.contains("must be created in a batch that also covers parent scope")
    ));
}

#[test]
fn nested_scope_parent_is_immutable() {
    let node = allow_all_node();
    let first_project_id = FederationProjectId::from("project-1");
    let second_project_id = FederationProjectId::from("project-2");
    let scene_id = FederationSceneId::from("scene-1");
    commit_nested_scene(
        &node,
        &first_project_id,
        &scene_id,
        &FederationSceneElementId::from("element-1"),
    );

    let scene_scope = ScopeId::for_item::<FederationScene>(&scene_id);
    let mut command = request(CommandId::new());
    command.scope_id = ScopeId::for_item::<FederationProject>(&second_project_id);
    let executing = node.admit(command.clone()).unwrap().snapshot().clone();
    let moved = FederationScene {
        id: scene_id,
        federation_project_id: second_project_id,
        name: "moved".to_owned(),
    };
    let result = node.commit(
        command.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: command.id,
            service_id: command.service_id,
            scope_id: command.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![mutation_in(&scene_scope, &moved)],
        },
        Vec::new(),
    );
    assert!(matches!(
        result,
        Err(NodeError::InvalidItemMutation(reason))
            if reason.contains("cannot move from parent")
    ));
}

#[myko_service(
    TestRecord,
    TestMarker,
    FederationProject,
    FederationScene,
    FederationSceneElement
)]
pub struct TestService;

#[myko_item(service = TestService, scope_root)]
pub struct TestRecord {
    pub value: String,
}

#[myko_item(service = TestService, scope_root)]
pub struct TestMarker {
    pub value: String,
}

#[myko_item(service = TestService, scope_root)]
pub struct FederationProject {
    pub name: String,
}

#[myko_item(
    service = TestService,
    scope_root,
    scoped_by = FederationProject
)]
pub struct FederationScene {
    pub name: String,
}

#[myko_item(service = TestService, scoped_by = FederationScene)]
pub struct FederationSceneElement {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutRecord {
    pub id: String,
    pub value: String,
}

impl MykoOperation for PutRecord {
    const OPERATION_ID: &'static str = stringify!(PutRecord);
}

impl MykoCommandContract for PutRecord {
    type Output = bool;
    type Service = TestService;
    type Scope = TestRecord;
}

impl MykoCommand for PutRecord {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtherCommand {
    pub value: String,
}

#[myko_service(OtherRecord)]
pub struct OtherService;

#[myko_item(service = OtherService)]
pub struct OtherRecord {
    pub value: String,
}

impl MykoOperation for OtherCommand {
    const OPERATION_ID: &'static str = stringify!(OtherCommand);
}

impl MykoCommandContract for OtherCommand {
    type Output = ();
    type Service = OtherService;
    type Scope = OtherRecord;
}

impl MykoCommand for OtherCommand {}

struct FailingJournal {
    node_id: NodeId,
}

impl EventJournal for FailingJournal {
    fn node_id(&self) -> Result<NodeId, NodeError> {
        Ok(self.node_id)
    }

    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
        Ok(Vec::new())
    }

    fn append(&self, _event: &EventEnvelope) -> Result<(), NodeError> {
        Err(NodeError::Backend("injected append failure".to_owned()))
    }
}

fn request(id: CommandId) -> CommandRequest {
    let principal_id = PrincipalId::new("human:test");
    let scope_id = ScopeId::new("session:test");
    CommandRequest {
        id,
        service_id: ServiceId::new(TestService::SERVICE_ID),
        scope_id: scope_id.clone(),
        principal_id: principal_id.clone(),
        authority: AuthorityPresentation::direct_node(principal_id),
        resource_claims: vec![ResourceClaim {
            selection: ScopeSelection::Exact(scope_id),
            kind: ResourceClaimKind::Primary,
            source_node: None,
            service_id: Some(ServiceId::new(TestService::SERVICE_ID)),
            item_type: None,
            item_id: None,
            required_permissions: vec![
                FederationPermission::ReadState,
                FederationPermission::Write,
            ],
            required_operations: vec![AccessOperation::ReadItems, AccessOperation::SubmitCommand],
            required_capabilities: Vec::new(),
        }],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "prompt".to_owned(),
        payload: b"hello".to_vec(),
    }
}

fn batch(command: &CommandRequest) -> ChangeBatch {
    ChangeBatch {
        id: BatchId::new(),
        command_id: command.id,
        service_id: command.service_id.clone(),
        scope_id: command.scope_id.clone(),
        causal_parents: Vec::new(),
        changes: vec![ItemMutation {
            service_id: command.service_id.as_str().to_owned(),
            item_type: "message".to_owned(),
            item_id: "message:1".to_owned(),
            schema_version: 1,
            roots_scope: false,
            belongs_to: None,
            scope_id: None,
            operation: MutationOperation::Set,
            payload: Some(b"hello".to_vec()),
        }],
    }
}

fn commit_test_record(node: &Node, id: &str, value: &str) -> TestRecord {
    commit_test_record_in(node, ScopeId::new("session:test"), id, value)
}

fn commit_test_record_in(node: &Node, scope_id: ScopeId, id: &str, value: &str) -> TestRecord {
    let mut request = request(CommandId::new());
    request.scope_id = scope_id;
    let executing = node.admit(request.clone()).unwrap().snapshot().clone();
    let record = TestRecord {
        id: TestRecordId::from(id),
        value: value.to_owned(),
    };
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![ItemMutation::set(&record).unwrap()],
        },
        Vec::new(),
    )
    .unwrap();
    record
}

fn commit_test_marker(node: &Node, id: &str, value: &str) {
    let request = request(CommandId::new());
    let executing = node.admit(request.clone()).unwrap().snapshot().clone();
    let marker = TestMarker {
        id: TestMarkerId::from(id),
        value: value.to_owned(),
    };
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![ItemMutation::set(&marker).unwrap()],
        },
        Vec::new(),
    )
    .unwrap();
}

#[test]
fn authoritative_service_scope_frontier_ignores_replicated_commits() {
    let local = allow_all_node();
    let remote = allow_all_node();
    let scope = ScopeId::new("session:test");
    commit_test_record_in(&local, scope.clone(), "local", "one");
    let local_frontier = local
        .authoritative_position_in::<TestService>(&scope)
        .unwrap();

    commit_test_record_in(&remote, scope.clone(), "remote", "two");
    for event in remote.events_after(None).unwrap() {
        local.ingest(event).unwrap();
    }

    assert_eq!(
        local
            .authoritative_position_in::<TestService>(&scope)
            .unwrap(),
        local_frontier
    );
}

fn mutation_in<T: MykoItem>(scope_id: &ScopeId, item: &T) -> ItemMutation {
    let mut mutation = ItemMutation::set(item).unwrap();
    mutation.scope_id = Some(scope_id.as_str().to_owned());
    mutation
}

fn commit_nested_scene(
    node: &Node,
    project_id: &FederationProjectId,
    scene_id: &FederationSceneId,
    element_id: &FederationSceneElementId,
) {
    let project_scope = ScopeId::for_item::<FederationProject>(project_id);
    let scene_scope = ScopeId::for_item::<FederationScene>(scene_id);
    let mut request = request(CommandId::new());
    request.scope_id = project_scope.clone();
    request.resource_claims.first_mut().unwrap().selection = ScopeSelection::Exact(project_scope);
    let executing = node.admit(request.clone()).unwrap().snapshot().clone();
    let scene = FederationScene {
        id: scene_id.clone(),
        federation_project_id: project_id.clone(),
        name: "scene".to_owned(),
    };
    let element = FederationSceneElement {
        id: element_id.clone(),
        federation_scene_id: scene_id.clone(),
        name: "element".to_owned(),
    };
    node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![
                mutation_in(&scene_scope, &scene),
                mutation_in(&scene_scope, &element),
            ],
        },
        Vec::new(),
    )
    .unwrap();
}

#[test]
fn stable_command_id_executes_once_and_resumes_after_commit() {
    let node = allow_all_node();
    let request = request(CommandId::new());

    assert!(node.admit(request.clone()).unwrap().should_execute());
    assert!(!node.admit(request.clone()).unwrap().should_execute());
    node.commit(request.id, batch(&request), b"done".to_vec())
        .unwrap();

    let resumed = node.admit(request).unwrap().snapshot().clone();
    assert!(resumed.state.is_committed());
    assert_eq!(resumed.result.as_deref(), Some(b"done".as_slice()));
}

#[test]
fn command_watch_starts_current_then_updates_without_a_gap() {
    let node = allow_all_node();
    let request = request(CommandId::new());
    node.submit(request.clone()).unwrap();
    let (initial, mut watch) = node.watch_command(request.id).unwrap();
    assert!(initial.command.is_some_and(|command| {
        command.request == request && command.state == CommandState::Submitted
    }));

    node.claim(request.id).unwrap();
    assert_eq!(watch.recv().unwrap().state, CommandState::Executing);
    node.cancel(request.id, "stopped").unwrap();
    assert_eq!(
        watch.recv().unwrap().state,
        CommandState::Cancelled {
            reason: "stopped".to_owned()
        }
    );
    assert!(matches!(
        node.watch_command(CommandId::new()),
        Err(NodeError::UnknownCommand(_))
    ));
}

#[test]
fn command_catalog_pages_hold_the_first_log_ceiling() {
    let node = allow_all_node();
    let scope_id = ScopeId::new("session:test");
    let principal_id = PrincipalId::new("human:test");
    let first = DeclaredCommand::new(
        CommandId::from_uuid(Uuid::from_u128(1)),
        scope_id.clone(),
        principal_id.clone(),
        PutRecord {
            id: "record-1".to_owned(),
            value: "first".to_owned(),
        },
    );
    let third = DeclaredCommand::new(
        CommandId::from_uuid(Uuid::from_u128(3)),
        scope_id.clone(),
        principal_id.clone(),
        PutRecord {
            id: "record-3".to_owned(),
            value: "third".to_owned(),
        },
    );
    node.submit(first.request().unwrap()).unwrap();
    node.submit(third.request().unwrap()).unwrap();

    let first_page = node
        .command_state_page(
            CommandStateRequest::for_serving_declared::<PutRecord>(scope_id.clone())
                .with_page_size(1),
        )
        .unwrap();
    assert_eq!(first_page.commands.len(), 1);
    assert_eq!(
        first_page
            .commands
            .first()
            .map(|entry| entry.command.request.id),
        Some(first.id)
    );
    let through = first_page.through;
    let (mut snapshot, next) = CommandStateSnapshot::from_first_page(first_page).unwrap();

    let concurrent = DeclaredCommand::new(
        CommandId::from_uuid(Uuid::from_u128(2)),
        scope_id.clone(),
        principal_id,
        PutRecord {
            id: "record-2".to_owned(),
            value: "too late for this snapshot".to_owned(),
        },
    );
    node.submit(concurrent.request().unwrap()).unwrap();
    let next = next.unwrap();
    assert_eq!(next.snapshot_through, through);
    let second_page = node.command_state_page(next.clone()).unwrap();
    assert_eq!(second_page.through, through);
    assert!(snapshot.append_page(&next, second_page).unwrap().is_none());
    assert_eq!(
        snapshot
            .typed::<PutRecord>()
            .unwrap()
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        vec![first.id, third.id]
    );
    assert_eq!(
        node.command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
            scope_id
        ))
        .unwrap()
        .commands
        .len(),
        3
    );
}

#[test]
fn typed_command_catalog_decodes_body_result_and_admission_order() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let scope_id = ScopeId::new("session:test");
    let principal_id = PrincipalId::new("human:test");
    let first = DeclaredCommand::new(
        CommandId::new(),
        scope_id.clone(),
        principal_id.clone(),
        PutRecord {
            id: "record-1".to_owned(),
            value: "first".to_owned(),
        },
    );
    node.submit(first.request().unwrap()).unwrap();
    let DeclaredCommandAdmission::Execute(context) =
        node.begin_declared_command::<PutRecord>(first.id).unwrap()
    else {
        return;
    };
    context.commit(&true).unwrap();
    let second = DeclaredCommand::new(
        CommandId::new(),
        scope_id.clone(),
        principal_id,
        PutRecord {
            id: "record-2".to_owned(),
            value: "second".to_owned(),
        },
    );
    node.submit(second.request().unwrap()).unwrap();

    let catalog = node
        .command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
            scope_id,
        ))
        .unwrap()
        .typed::<PutRecord>()
        .unwrap();
    assert_eq!(
        catalog.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    if let [first_state, second_state] = catalog.as_slice() {
        assert_eq!(first_state.command.id, first.body.id);
        assert_eq!(first_state.command.value, first.body.value);
        assert_eq!(first_state.result, Some(true));
        assert!(first_state.state.is_committed());
        assert_eq!(second_state.command.id, second.body.id);
        assert_eq!(second_state.command.value, second.body.value);
        assert_eq!(second_state.result, None);
        assert_eq!(second_state.state, CommandState::Submitted);
    } else {
        assert_eq!(catalog.len(), 2);
    }
}

#[test]
fn command_catalog_ignores_stale_lifecycle_events() {
    let source = allow_all_node();
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "cancel me".to_owned(),
        },
    );
    source.submit(command.request().unwrap()).unwrap();
    source.claim(command.id).unwrap();
    source.cancel(command.id, "stopped").unwrap();

    let replica = allow_all_node();
    for event in source.events_after(None).unwrap().into_iter().rev() {
        replica.ingest(event).unwrap();
    }
    let catalog = replica
        .command_states(CommandStateRequest::for_declared::<PutRecord>(
            source.node_id(),
            command.scope_id,
        ))
        .unwrap();
    if let [entry] = catalog.commands.as_slice() {
        assert_eq!(entry.admitted_at, LogPosition::FIRST);
        assert_eq!(entry.last_changed_at, LogPosition::FIRST);
        assert!(matches!(
            entry.command.state,
            CommandState::Cancelled { ref reason } if reason == "stopped"
        ));
    } else {
        assert_eq!(catalog.commands.len(), 1);
    }
}

#[test]
fn command_catalog_stream_adds_and_advances_matching_commands() {
    let node = allow_all_node();
    let scope_id = ScopeId::new("session:test");
    let first = DeclaredCommand::new(
        CommandId::new(),
        scope_id.clone(),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "first".to_owned(),
        },
    );
    node.submit(first.request().unwrap()).unwrap();
    let snapshot = node
        .command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
            scope_id.clone(),
        ))
        .unwrap();
    let mut stream = CommandStateStream::from_snapshot(&snapshot).unwrap();
    let second = DeclaredCommand::new(
        CommandId::new(),
        scope_id,
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-2".to_owned(),
            value: "second".to_owned(),
        },
    );
    node.submit(second.request().unwrap()).unwrap();
    node.claim(second.id).unwrap();

    let events = node.events_after(snapshot.through).unwrap();
    for event in &events {
        let Some(update) = stream.request().update_from_envelope(event) else {
            assert!(stream.request().update_from_envelope(event).is_some());
            return;
        };
        let _current = stream.apply(&update).unwrap();
    }
    let current = stream.current().typed::<PutRecord>().unwrap();
    assert_eq!(
        current.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    if let [first_state, second_state] = current.as_slice() {
        assert_eq!(first_state.state, CommandState::Submitted);
        assert_eq!(second_state.state, CommandState::Executing);
    } else {
        assert_eq!(current.len(), 2);
    }
    let Some(first_event) = events.first() else {
        assert!(!events.is_empty());
        return;
    };
    let mut foreign = first_event.clone();
    foreign.origin.node_id = NodeId::new();
    assert!(stream.request().update_from_envelope(&foreign).is_none());
}

#[test]
fn typed_command_context_owns_atomic_item_batch_and_result_encoding() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let request = request(CommandId::new());
    node.submit(request.clone()).unwrap();
    let admission = node.begin_command(request.id).unwrap();
    assert!(matches!(&admission, TypedCommandAdmission::Execute(_)));
    let TypedCommandAdmission::Execute(context) = admission else {
        return;
    };
    assert!(context.query(GetAllTestRecords).unwrap().is_empty());
    let record = TestRecord {
        id: TestRecordId::from("record-1"),
        value: "owned by Myko".to_owned(),
    };
    context.emit_set(&record).unwrap();
    assert_eq!(context.change_count(), Ok(1));
    let committed = context.commit(&true).unwrap();
    assert_eq!(committed.result.as_deref(), Some(b"true".as_slice()));
    assert_eq!(
        node.query_items_in(node.node_id(), &request.scope_id, GetAllTestRecords,)
            .unwrap(),
        vec![record]
    );
    assert!(matches!(
        node.begin_command(request.id).unwrap(),
        TypedCommandAdmission::Resume(_)
    ));
}

#[test]
fn command_effects_fail_closed_without_an_installed_policy() {
    let node = Node::in_memory();
    let request = request(CommandId::new());
    assert!(matches!(
        node.submit(request.clone()),
        Err(NodeError::AuthorizationDenied(reason))
            if reason.contains("does not serve application or federation data")
    ));
    assert!(node.command(request.id).unwrap().is_none());
    assert!(
        node.query_items_in(node.node_id(), &request.scope_id, GetAllTestRecords)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn command_context_rejects_cross_service_writes_but_allows_declared_reads() {
    let node = allow_all_node();
    let mut request = request(CommandId::new());
    request.resource_claims.push(ResourceClaim {
        selection: ScopeSelection::Exact(request.scope_id.clone()),
        kind: ResourceClaimKind::Referenced,
        source_node: Some(node.node_id()),
        service_id: Some(ServiceId::new(OtherService::SERVICE_ID)),
        item_type: Some(OtherRecord::ITEM_TYPE.to_owned()),
        item_id: None,
        required_permissions: vec![FederationPermission::ReadState],
        required_operations: vec![AccessOperation::ReadItems],
        required_capabilities: Vec::new(),
    });
    let _submitted = node.submit(request.clone()).unwrap();
    let context = match node.begin_command(request.id).unwrap() {
        TypedCommandAdmission::Execute(context) => context,
        TypedCommandAdmission::Resume(_) => return,
    };
    let record = OtherRecord {
        id: OtherRecordId::from("other-1"),
        value: "must not cross services".to_owned(),
    };
    assert!(matches!(
        context.emit_set(&record),
        Err(NodeError::ItemServiceMismatch {
            item_service,
            ..
        }) if item_service == OtherService::SERVICE_ID.as_str()
    ));
    assert!(matches!(
        context.emit_delete::<OtherRecord>(&record.id),
        Err(NodeError::ItemServiceMismatch {
            item_service,
            ..
        }) if item_service == OtherService::SERVICE_ID.as_str()
    ));
    assert_eq!(context.query(GetAllOtherRecords).unwrap(), Vec::new());
    assert_eq!(context.change_count(), Ok(0));
    let _rejected = context.reject("test complete").unwrap();
}

#[test]
fn raw_batch_rejects_a_forged_item_service() {
    let node = allow_all_node();
    let request = request(CommandId::new());
    let executing = node.admit(request.clone()).unwrap().snapshot().clone();
    let record = TestRecord {
        id: TestRecordId::from("record-1"),
        value: "forged ownership".to_owned(),
    };
    let mut mutation = ItemMutation::set(&record).unwrap();
    mutation.service_id = "other".to_owned();
    let result = node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![mutation],
        },
        Vec::new(),
    );
    assert!(matches!(result, Err(NodeError::InvalidItemMutation(_))));
}

#[test]
fn typed_command_context_refuses_replicated_execution() {
    let source = allow_all_node();
    let request = request(CommandId::new());
    source.submit(request.clone()).unwrap();
    let replica = allow_all_node();
    for event in source.events_after(None).unwrap() {
        let _status = replica.ingest(event).unwrap();
    }
    assert!(matches!(
        replica.begin_command(request.id),
        Err(NodeError::ForeignCommand { .. })
    ));
}

#[test]
fn declared_command_owns_submission_decoding_items_and_typed_result() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "declared command".to_owned(),
        },
    );
    node.submit(command.request().unwrap()).unwrap();
    let admission = node
        .begin_declared_command::<PutRecord>(command.id)
        .unwrap();
    assert!(matches!(&admission, DeclaredCommandAdmission::Execute(_)));
    let DeclaredCommandAdmission::Execute(mut context) = admission else {
        return;
    };
    let record = TestRecord {
        id: TestRecordId::from(context.body().id.clone()),
        value: context.body().value.clone(),
    };
    context.emit_set(&record).unwrap();
    let committed = context.commit(&true).unwrap();
    assert_eq!(committed.result.as_deref(), Some(b"true".as_slice()));
    assert_eq!(
        node.query_items_in(node.node_id(), &command.scope_id, GetAllTestRecords,)
            .unwrap(),
        vec![record]
    );
    assert!(matches!(
        node.begin_declared_command::<PutRecord>(command.id)
            .unwrap(),
        DeclaredCommandAdmission::Resume(_)
    ));
}

#[test]
fn concurrent_declared_dispatch_resumes_only_after_the_owner_commits() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "owned execution".to_owned(),
        },
    );
    node.submit(command.request().unwrap()).unwrap();

    let (owner_started_tx, owner_started_rx) = std::sync::mpsc::channel();
    let (release_owner_tx, release_owner_rx) = std::sync::mpsc::channel();
    let owner_node = node.clone();
    let command_id = command.id;
    let owner_thread = std::thread::spawn(move || {
        owner_node.dispatch_declared_command::<PutRecord, _>(command_id, |_| {
            owner_started_tx.send(()).unwrap();
            release_owner_rx.recv().unwrap();
            Ok(true)
        })
    });
    owner_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (contender_started_tx, contender_started_rx) = std::sync::mpsc::channel();
    let (contender_result_tx, contender_result_rx) = std::sync::mpsc::channel();
    let contender_node = node;
    let contender = std::thread::spawn(move || {
        contender_started_tx.send(()).unwrap();
        let result =
            contender_node.dispatch_declared_command::<PutRecord, _>(command_id, |_| Ok(false));
        contender_result_tx.send(result).unwrap();
    });
    contender_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(
        contender_result_rx
            .recv_timeout(Duration::from_millis(25))
            .is_err()
    );

    release_owner_tx.send(()).unwrap();
    let owner_result = owner_thread.join().unwrap().unwrap();
    let resumed = contender_result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    contender.join().unwrap();

    assert_eq!(
        owner_result.disposition,
        CommandDispatchDisposition::Committed
    );
    assert_eq!(resumed.disposition, CommandDispatchDisposition::Resumed);
    assert_eq!(
        resumed.command.typed_completion::<PutRecord>().unwrap(),
        Some(true)
    );
}

#[test]
fn declared_command_schema_mismatch_does_not_claim_execution() {
    let node = allow_all_node();
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "not other".to_owned(),
        },
    );
    node.submit(command.request().unwrap()).unwrap();
    assert!(matches!(
        node.begin_declared_command::<OtherCommand>(command.id),
        Err(NodeError::CommandSchemaMismatch { .. })
    ));
    let snapshot = node.command(command.id).unwrap().unwrap();
    assert!(matches!(snapshot.state, CommandState::Submitted));
}

#[test]
fn declared_dispatch_rejects_malformed_work_and_continues_in_order() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let first = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "first".to_owned(),
        },
    );
    node.submit(first.request().unwrap()).unwrap();
    let malformed_id = CommandId::new();
    node.submit(CommandRequest {
        id: malformed_id,
        service_id: ServiceId::new(PutRecord::SERVICE_ID),
        scope_id: ScopeId::new("session:test"),
        principal_id: PrincipalId::new("human:test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("human:test")),
        resource_claims: vec![ResourceClaim::scope(
            ScopeId::new("session:test"),
            ResourceClaimKind::Primary,
        )],
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: PutRecord::COMMAND_TYPE.to_owned(),
        payload: b"not json".to_vec(),
    })
    .unwrap();
    let second = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-2".to_owned(),
            value: "second".to_owned(),
        },
    );
    node.submit(second.request().unwrap()).unwrap();

    let dispatched = node
        .dispatch_declared::<PutRecord, _>(|context| {
            let record = TestRecord {
                id: TestRecordId::from(context.body().id.clone()),
                value: context.body().value.clone(),
            };
            context
                .emit_set(&record)
                .map_err(|error| error.to_string())?;
            Ok(true)
        })
        .unwrap();
    assert_eq!(
        dispatched
            .iter()
            .map(|result| result.disposition)
            .collect::<Vec<_>>(),
        vec![
            CommandDispatchDisposition::Committed,
            CommandDispatchDisposition::Rejected,
            CommandDispatchDisposition::Committed,
        ]
    );
    assert!(matches!(
        node.command(malformed_id).unwrap().unwrap().state,
        CommandState::Rejected { .. }
    ));
    assert_eq!(
        node.query_items_in(
            node.node_id(),
            &ScopeId::new("session:test"),
            GetAllTestRecords,
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn local_pending_discovery_never_executes_a_replicated_submission() {
    let source = allow_all_node();
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "foreign".to_owned(),
        },
    );
    source.submit(command.request().unwrap()).unwrap();
    let replica = allow_all_node();
    for event in source.events_after(None).unwrap() {
        let _status = replica.ingest(event).unwrap();
    }
    assert!(
        replica
            .pending_local_commands(PutRecord::SERVICE_ID.as_str(), PutRecord::COMMAND_TYPE)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn declared_pending_watch_replays_current_then_follows_without_polling() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let completed = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "completed".to_owned(),
            value: "old".to_owned(),
        },
    );
    node.submit(completed.request().unwrap()).unwrap();
    node.dispatch_declared_command::<PutRecord, _>(completed.id, |_| Ok(true))
        .unwrap();
    let queued = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "queued".to_owned(),
            value: "restart catch-up".to_owned(),
        },
    );
    node.submit(queued.request().unwrap()).unwrap();

    let mut pending = node.watch_pending_typed::<PutRecord>().unwrap();
    assert_eq!(
        pending.service_id().map(ServiceId::as_str),
        Some(PutRecord::SERVICE_ID.as_str())
    );
    assert_eq!(pending.command_type(), Some(PutRecord::COMMAND_TYPE));
    assert_eq!(pending.recv().unwrap().request.id, queued.id);

    let unrelated = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        OtherCommand {
            value: "ignore me".to_owned(),
        },
    );
    node.submit(unrelated.request().unwrap()).unwrap();
    assert!(
        pending
            .recv_timeout(Duration::from_millis(5))
            .unwrap()
            .is_none()
    );

    let live = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "live".to_owned(),
            value: "event driven".to_owned(),
        },
    );
    node.submit(live.request().unwrap()).unwrap();
    assert_eq!(
        pending
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .request
            .id,
        live.id
    );
}

#[test]
fn service_pending_watch_preserves_admission_order_and_omits_foreign_work() {
    let node = allow_all_node();
    let first = request(CommandId::new());
    node.submit(first.clone()).unwrap();
    let second = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "second".to_owned(),
            value: "same service".to_owned(),
        },
    );
    node.submit(second.request().unwrap()).unwrap();

    let source = allow_all_node();
    let foreign = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:remote"),
        PutRecord {
            id: "foreign".to_owned(),
            value: "projection only".to_owned(),
        },
    );
    source.submit(foreign.request().unwrap()).unwrap();
    for event in source.events_after(None).unwrap() {
        let _status = node.ingest(event).unwrap();
    }

    let mut pending = node
        .watch_pending_local_service_commands(TestService::SERVICE_ID.as_str())
        .unwrap();
    assert_eq!(pending.command_type(), None);
    assert_eq!(pending.recv().unwrap().request.id, first.id);
    assert_eq!(pending.recv().unwrap().request.id, second.id);
    assert!(
        pending
            .recv_timeout(Duration::from_millis(5))
            .unwrap()
            .is_none()
    );
}

#[test]
fn application_pending_watch_preserves_order_across_services() {
    let node = allow_all_node();
    let first = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        OtherCommand {
            value: "first service".to_owned(),
        },
    );
    node.submit(first.request().unwrap()).unwrap();
    let second = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "second".to_owned(),
            value: "second service".to_owned(),
        },
    );
    node.submit(second.request().unwrap()).unwrap();

    assert_eq!(
        node.pending_local_application_commands()
            .unwrap()
            .into_iter()
            .map(|command| command.request.id)
            .collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    let mut pending = node.watch_pending_local_application_commands().unwrap();
    assert_eq!(pending.service_id(), None);
    assert_eq!(pending.command_type(), None);
    assert_eq!(pending.recv().unwrap().request.id, first.id);
    assert_eq!(pending.recv().unwrap().request.id, second.id);
}

#[test]
fn declared_dispatch_durably_retries_transient_handler_failures() {
    let node = allow_all_node();
    let _policy = install_allow_all(&node);
    let command = DeclaredCommand::new(
        CommandId::new(),
        ScopeId::new("session:test"),
        PrincipalId::new("human:test"),
        PutRecord {
            id: "record-1".to_owned(),
            value: "retry me".to_owned(),
        },
    );
    node.submit(command.request().unwrap()).unwrap();
    let mut item_changes = node.subscribe_item_changes_from_now().unwrap();
    let retrying = node
        .dispatch_declared_command::<PutRecord, _>(command.id, |_| {
            Err(CommandHandlerError::retry("workspace registry unavailable"))
        })
        .unwrap();
    assert_eq!(retrying.disposition, CommandDispatchDisposition::Retrying);
    assert!(matches!(
        retrying.command.state,
        CommandState::Retrying { .. }
    ));
    assert_eq!(
        node.pending_local_commands(PutRecord::SERVICE_ID.as_str(), PutRecord::COMMAND_TYPE)
            .unwrap()
            .len(),
        1
    );
    assert!(item_changes.try_recv().is_none());

    let committed = node
        .dispatch_declared_command::<PutRecord, _>(command.id, |context| {
            let record = TestRecord {
                id: TestRecordId::from(context.body().id.clone()),
                value: context.body().value.clone(),
            };
            context
                .emit_set(&record)
                .map_err(|error| CommandHandlerError::retry(error.to_string()))?;
            Ok(true)
        })
        .unwrap();
    assert_eq!(committed.disposition, CommandDispatchDisposition::Committed);
    assert!(committed.command.state.is_committed());
    assert!(item_changes.try_recv().is_some());
}

#[test]
fn typed_query_materializes_replicated_service_scope_state() {
    let source = allow_all_node();
    let request = request(CommandId::new());
    let executing = source.admit(request.clone()).unwrap().snapshot().clone();
    let record = TestRecord {
        id: TestRecordId::from("record-1"),
        value: "federated".to_owned(),
    };
    source
        .commit(
            request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: request.id,
                service_id: request.service_id.clone(),
                scope_id: request.scope_id.clone(),
                causal_parents: vec![executing.updated_at],
                changes: vec![ItemMutation::set(&record).unwrap()],
            },
            Vec::new(),
        )
        .unwrap();

    let replica = allow_all_node();
    for event in source.events_after(None).unwrap() {
        let _status = replica.ingest(event).unwrap();
    }
    let projected = replica
        .query_items_in(source.node_id(), &request.scope_id, GetAllTestRecords)
        .unwrap();
    assert_eq!(projected, vec![record]);
}

#[test]
fn current_item_state_is_bounded_and_rehydrates_a_typed_query() {
    let node = allow_all_node();
    let first = commit_test_record(&node, "record-1", "first");
    let second = commit_test_record(&node, "record-2", "second");
    let request =
        ItemStateRequest::for_item::<TestRecord>(node.node_id(), ScopeId::new("session:test"));
    let snapshot = node.item_state_snapshot(request).unwrap();
    assert_eq!(snapshot.serving_node, node.node_id());
    assert!(snapshot.through.is_some());
    assert_eq!(snapshot.items.len(), 2);
    assert_eq!(
        snapshot.query(GetAllTestRecords).unwrap().value,
        vec![first, second]
    );
}

#[test]
fn raw_item_snapshot_rejects_unqualified_scope_payloads() {
    let node = Node::in_memory();
    let project_id = FederationProjectId::from("project-1");
    let scene_id = FederationSceneId::from("scene-1");
    let current_parent_scope = ScopeId::for_item::<FederationProject>(&project_id);
    let legacy_parent_scope =
        ScopeId::new(current_parent_scope.as_str().split_once('/').unwrap().1);
    let current_scene_scope = ScopeId::for_item::<FederationScene>(&scene_id);
    let commit_legacy_scene = |name: &str| {
        let mut command = request(CommandId::new());
        command.scope_id = legacy_parent_scope.clone();
        let executing = node.admit(command.clone()).unwrap().snapshot().clone();
        node.commit(
            command.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: command.id,
                service_id: command.service_id,
                scope_id: command.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![ItemMutation {
                    service_id: TestService::SERVICE_ID.as_str().to_owned(),
                    item_type: FederationScene::ITEM_TYPE.to_owned(),
                    item_id: scene_id.as_ref().to_owned(),
                    schema_version: FederationScene::SCHEMA_VERSION,
                    roots_scope: false,
                    belongs_to: None,
                    scope_id: None,
                    operation: MutationOperation::Set,
                    payload: Some(
                        serde_json::to_vec(&serde_json::json!({
                            "id": scene_id.as_ref(),
                            "name": name,
                        }))
                        .unwrap(),
                    ),
                }],
            },
            Vec::new(),
        )
        .unwrap();
    };

    commit_legacy_scene("legacy snapshot");
    let snapshot = node
        .item_state_snapshot(ItemStateRequest::for_serving_item::<FederationScene>(
            current_scene_scope,
        ))
        .unwrap();
    let (initial, _stream) =
        ItemQueryStream::from_snapshot(&snapshot, GetAllFederationScenes).unwrap();
    assert!(initial.value.is_empty());
}

#[test]
fn item_state_pages_hold_the_first_log_ceiling_during_concurrent_commits() {
    let node = allow_all_node();
    let first = commit_test_record(&node, "record-1", "first");
    let third = commit_test_record(&node, "record-3", "third");
    let request = ItemStateRequest::for_serving_item::<TestRecord>(ScopeId::new("session:test"))
        .with_page_size(1);
    let first_page = node.item_state_page(request).unwrap();
    assert_eq!(first_page.items.len(), 1);
    assert!(first_page.next_after_item_id.is_some());
    let through = first_page.through;
    let (mut snapshot, next) = ItemStateSnapshot::from_first_page(first_page).unwrap();

    let concurrent = commit_test_record(&node, "record-2", "too late for this snapshot");
    let next = next.unwrap();
    assert_eq!(next.snapshot_through, through);
    let second_page = node.item_state_page(next.clone()).unwrap();
    assert_eq!(second_page.through, through);
    assert!(snapshot.append_page(&next, second_page).unwrap().is_none());
    assert_eq!(
        snapshot.query(GetAllTestRecords).unwrap().value,
        vec![first, third]
    );
    assert_eq!(
        node.query_items(GetAllTestRecords).unwrap(),
        vec![
            TestRecord {
                id: TestRecordId::from("record-1"),
                value: "first".to_owned(),
            },
            concurrent,
            TestRecord {
                id: TestRecordId::from("record-3"),
                value: "third".to_owned(),
            },
        ]
    );
}

#[test]
fn typed_item_stream_applies_each_atomic_update_or_none_of_it() {
    let node = allow_all_node();
    let first = commit_test_record(&node, "record-1", "initial");
    let snapshot = node
        .item_state_snapshot(ItemStateRequest::for_serving_item::<TestRecord>(
            ScopeId::new("session:test"),
        ))
        .unwrap();
    let (initial, mut stream) =
        ItemQueryStream::from_snapshot(&snapshot, GetAllTestRecords).unwrap();
    assert_eq!(initial.value, vec![first.clone()]);

    let second = commit_test_record(&node, "record-2", "live");
    let follow = snapshot.follow_request().unwrap();
    let update = node
        .events_after(snapshot.through)
        .unwrap()
        .iter()
        .find_map(|envelope| follow.update_from_envelope(envelope).transpose())
        .transpose()
        .unwrap()
        .unwrap();
    assert_eq!(stream.apply(&update).unwrap().value, vec![first, second]);

    let before_invalid = stream.current();
    let mut invalid = update;
    invalid.changes.push(ItemMutation {
        service_id: TestRecord::SERVICE_ID.as_str().to_owned(),
        item_type: TestRecord::ITEM_TYPE.to_owned(),
        item_id: "broken".to_owned(),
        schema_version: TestRecord::SCHEMA_VERSION,
        roots_scope: false,
        belongs_to: None,
        scope_id: None,
        operation: MutationOperation::Set,
        payload: Some(b"not-json".to_vec()),
    });
    invalid.through = LogPosition::new(invalid.through.get().saturating_add(1));
    assert!(stream.apply(&invalid).is_err());
    assert_eq!(stream.current(), before_invalid);
}

#[test]
fn typed_query_watch_replays_then_tracks_replicated_batches_without_a_gap() {
    let source = allow_all_node();
    let first = commit_test_record(&source, "record-1", "initial");
    let replica = allow_all_node();
    for event in source.events_after(None).unwrap() {
        let _status = replica.ingest(event).unwrap();
    }
    let (snapshot, mut watch) = replica
        .watch_items_in(
            source.node_id(),
            ScopeId::new("session:test"),
            GetAllTestRecords,
        )
        .unwrap();
    assert_eq!(snapshot.value, vec![first.clone()]);
    assert!(
        watch
            .recv_timeout(Duration::from_millis(5))
            .unwrap()
            .is_none()
    );

    let second = commit_test_record(&source, "record-2", "live");
    for event in source.events_after(None).unwrap() {
        let _status = replica.ingest(event).unwrap();
    }
    let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
    assert_eq!(update.value, vec![first, second]);
    assert!(watch.try_recv().unwrap().is_none());
}

#[test]
fn item_projection_reports_lifecycle_changes_when_the_value_is_unchanged() {
    let node = allow_all_node();
    let record = commit_test_record(&node, "record-1", "stable");
    let (snapshot, mut watch) = node
        .watch_item_projection::<TestRecord>(
            Some(node.node_id()),
            Some(ScopeId::new("session:test")),
        )
        .unwrap();
    let initial = snapshot.projection.states().next().unwrap().clone();

    assert_eq!(commit_test_record(&node, "record-1", "stable"), record);

    let update = node
        .events_after(snapshot.through)
        .unwrap()
        .iter()
        .find_map(|envelope| watch.apply(envelope).transpose())
        .transpose()
        .unwrap()
        .unwrap();
    assert!(matches!(
        update.diff,
        Some(MapDiff::Update {
            key,
            old_value,
            new_value,
        }) if key == record.id
            && old_value.value() == new_value.value()
            && old_value.first_changed_at() == initial.first_changed_at()
            && new_value.first_changed_at() == initial.first_changed_at()
            && new_value.last_changed_at() > old_value.last_changed_at()
            && new_value.change_index() == 0
    ));
}

#[test]
fn typed_query_watch_from_tracks_every_scope_owned_by_one_source() {
    let node = allow_all_node();
    let first = commit_test_record_in(&node, ScopeId::new("session:first"), "record-1", "first");
    let (snapshot, mut watch) = node
        .watch_items_from(node.node_id(), GetAllTestRecords)
        .unwrap();
    assert_eq!(snapshot.value, vec![first.clone()]);

    let second = commit_test_record_in(&node, ScopeId::new("session:second"), "record-2", "second");
    let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
    assert_eq!(update.value, vec![first, second]);
    assert!(watch.try_recv().unwrap().is_none());
}

#[test]
fn typed_query_watch_advances_across_other_item_types_in_the_same_scope() {
    let node = allow_all_node();
    let record = commit_test_record(&node, "record-1", "stable");
    let (snapshot, mut watch) = node
        .watch_items_in(
            node.node_id(),
            ScopeId::new("session:test"),
            GetAllTestRecords,
        )
        .unwrap();

    commit_test_marker(&node, "marker-1", "same atomic cursor stream");
    let update = watch.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();

    assert_eq!(update.value, vec![record]);
    assert!(
        snapshot
            .through
            .is_none_or(|through| update.position > through)
    );
}

#[test]
fn malformed_item_mutation_is_rejected_before_commit() {
    let node = allow_all_node();
    let request = request(CommandId::new());
    let executing = node.admit(request.clone()).unwrap().snapshot().clone();
    let invalid = ItemMutation {
        service_id: TestRecord::SERVICE_ID.as_str().to_owned(),
        item_type: TestRecord::ITEM_TYPE.to_owned(),
        item_id: "record-1".to_owned(),
        schema_version: TestRecord::SCHEMA_VERSION,
        roots_scope: false,
        belongs_to: None,
        scope_id: None,
        operation: MutationOperation::Delete,
        payload: Some(Vec::new()),
    };
    let result = node.commit(
        request.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: request.id,
            service_id: request.service_id,
            scope_id: request.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![invalid],
        },
        Vec::new(),
    );
    assert!(matches!(result, Err(NodeError::InvalidItemMutation(_))));
    assert!(
        !node
            .command(request.id)
            .unwrap()
            .unwrap()
            .state
            .is_committed()
    );
}

#[test]
fn scoped_replication_omits_other_scopes_and_advances_its_watermark() {
    let source = allow_all_node();
    let target = allow_all_node();
    let wanted = request(CommandId::new());
    let mut hidden = request(CommandId::new());
    hidden.scope_id = ScopeId::new("session:hidden");

    source.submit(wanted.clone()).unwrap();
    source.submit(hidden.clone()).unwrap();
    source.claim(wanted.id).unwrap();
    source
        .commit(wanted.id, batch(&wanted), b"done".to_vec())
        .unwrap();

    let scoped = source.export_scope(wanted.scope_id.clone(), None).unwrap();
    assert_eq!(scoped.events.len(), 3);
    assert_eq!(
        scoped
            .events
            .iter()
            .map(|event| event.position)
            .collect::<Vec<_>>(),
        vec![
            LogPosition::new(1),
            LogPosition::new(3),
            LogPosition::new(4)
        ]
    );
    assert_eq!(scoped.through, Some(LogPosition::new(4)));
    let report = target.ingest_scoped_batch(scoped).unwrap();
    assert_eq!(report.applied, 3);
    assert!(
        target
            .command(wanted.id)
            .unwrap()
            .is_some_and(|command| command.state.is_committed())
    );
    assert!(target.command(hidden.id).unwrap().is_none());

    source.cancel(hidden.id, "hidden cancellation").unwrap();
    let advanced = source
        .export_scope(wanted.scope_id, report.through)
        .unwrap();
    assert!(advanced.events.is_empty());
    assert_eq!(advanced.after, Some(LogPosition::new(4)));
    assert_eq!(advanced.through, Some(LogPosition::new(5)));
    let advanced_report = target.ingest_scoped_batch(advanced).unwrap();
    assert_eq!(advanced_report.applied, 0);
    assert_eq!(advanced_report.through, Some(LogPosition::new(5)));
}

#[test]
fn selected_replication_filters_by_service_and_service_scope() {
    let source = allow_all_node();
    let target = allow_all_node();
    let first = request(CommandId::new());
    let mut other_service = request(CommandId::new());
    other_service.service_id = ServiceId::new(OtherService::SERVICE_ID);
    let mut second_scope = request(CommandId::new());
    second_scope.scope_id = ScopeId::new("session:second");

    source.submit(first.clone()).unwrap();
    source.submit(other_service.clone()).unwrap();
    source.submit(second_scope.clone()).unwrap();

    let service = source
        .export_selected(
            ReplicationSelection::Service(ServiceId::new(TestService::SERVICE_ID)),
            None,
        )
        .unwrap();
    assert_eq!(
        service
            .events
            .iter()
            .map(|event| event.position)
            .collect::<Vec<_>>(),
        vec![LogPosition::new(1), LogPosition::new(3)]
    );
    assert_eq!(service.through, Some(LogPosition::new(3)));
    let report = target.ingest_selected_batch(service).unwrap();
    assert_eq!(report.applied, 2);
    assert!(target.command(first.id).unwrap().is_some());
    assert!(target.command(second_scope.id).unwrap().is_some());
    assert!(target.command(other_service.id).unwrap().is_none());

    let service_scope = source
        .export_selected(
            ReplicationSelection::ServiceScope {
                service_id: ServiceId::new(TestService::SERVICE_ID),
                scope_id: first.scope_id,
            },
            None,
        )
        .unwrap();
    assert_eq!(service_scope.events.len(), 1);
    assert_eq!(
        service_scope.events.first().map(|event| event.position),
        Some(LogPosition::new(1))
    );
    assert_eq!(service_scope.through, Some(LogPosition::new(3)));
}

#[test]
fn selected_replication_composes_exact_scopes_and_nested_subtrees() {
    let source = allow_all_node();
    let target = allow_all_node();
    let project_id = FederationProjectId::from("project-1");
    let project_scope = ScopeId::for_item::<FederationProject>(&project_id);
    let project = FederationProject {
        id: project_id.clone(),
        name: "project".to_owned(),
    };
    let mut project_request = request(CommandId::new());
    project_request.scope_id = project_scope.clone();
    project_request
        .resource_claims
        .first_mut()
        .unwrap()
        .selection = ScopeSelection::Exact(project_scope.clone());
    let executing = source
        .admit(project_request.clone())
        .unwrap()
        .snapshot()
        .clone();
    source
        .commit(
            project_request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: project_request.id,
                service_id: project_request.service_id,
                scope_id: project_request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![mutation_in(&project_scope, &project)],
            },
            Vec::new(),
        )
        .unwrap();

    let first_scene_id = FederationSceneId::from("scene-1");
    let second_scene_id = FederationSceneId::from("scene-2");
    let hidden_scene_id = FederationSceneId::from("scene-3");
    commit_nested_scene(
        &source,
        &project_id,
        &first_scene_id,
        &FederationSceneElementId::from("element-1"),
    );
    commit_nested_scene(
        &source,
        &project_id,
        &second_scene_id,
        &FederationSceneElementId::from("element-2"),
    );
    commit_nested_scene(
        &source,
        &project_id,
        &hidden_scene_id,
        &FederationSceneElementId::from("element-3"),
    );

    let first_scene_scope = ScopeId::for_item::<FederationScene>(&first_scene_id);
    let second_scene_scope = ScopeId::for_item::<FederationScene>(&second_scene_id);
    let hidden_scene_scope = ScopeId::for_item::<FederationScene>(&hidden_scene_id);
    let selection = ReplicationSelection::Scopes(vec![
        ScopeSelection::Exact(project_scope.clone()),
        ScopeSelection::Subtree(first_scene_scope.clone()),
        ScopeSelection::Subtree(second_scene_scope.clone()),
    ]);
    let batch = source.export_selected(selection, None).unwrap();
    assert!(batch.events.iter().all(|envelope| {
        !envelope
            .event
            .affected_scope_ids()
            .contains(&hidden_scene_scope)
    }));
    target.ingest_selected_batch(batch).unwrap();

    let topology = target.scope_topology().unwrap();
    assert_eq!(topology.parent(&first_scene_scope), Some(&project_scope));
    assert_eq!(topology.parent(&second_scene_scope), Some(&project_scope));
    assert_eq!(topology.parent(&hidden_scene_scope), None);
    assert!(matches!(
        target.query_items_in(
            source.node_id(),
            &first_scene_scope,
            GetAllFederationSceneElements,
        ),
        Ok(items) if items.len() == 1
    ));
    assert!(matches!(
        target.query_items_in(
            source.node_id(),
            &second_scene_scope,
            GetAllFederationSceneElements,
        ),
        Ok(items) if items.len() == 1
    ));
    assert!(matches!(
        target.query_items_in(
            source.node_id(),
            &hidden_scene_scope,
            GetAllFederationSceneElements,
        ),
        Ok(items) if items.is_empty()
    ));
}

#[test]
#[allow(clippy::too_many_lines)] // Exercises every fail-closed visibility branch together.
fn selected_queries_use_authorized_view_and_node_owned_completeness() {
    let source = allow_all_node();
    let project_id = FederationProjectId::from("project-selected");
    let project_scope = ScopeId::for_item::<FederationProject>(&project_id);
    let project = FederationProject {
        id: project_id.clone(),
        name: "project".to_owned(),
    };
    let mut project_request = request(CommandId::new());
    project_request.scope_id = project_scope.clone();
    let executing = source
        .admit(project_request.clone())
        .unwrap()
        .snapshot()
        .clone();
    source
        .commit(
            project_request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: project_request.id,
                service_id: project_request.service_id,
                scope_id: project_request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![mutation_in(&project_scope, &project)],
            },
            Vec::new(),
        )
        .unwrap();

    let first_scene = FederationSceneId::from("scene-visible");
    let empty_scene = FederationSceneId::from("scene-empty");
    let hidden_scene = FederationSceneId::from("scene-hidden");
    let first_element = FederationSceneElementId::from("element-visible");
    let deleted_element = FederationSceneElementId::from("element-deleted");
    let hidden_element = FederationSceneElementId::from("element-hidden");
    commit_nested_scene(&source, &project_id, &first_scene, &first_element);
    commit_nested_scene(&source, &project_id, &empty_scene, &deleted_element);
    commit_nested_scene(&source, &project_id, &hidden_scene, &hidden_element);

    let first_scope = ScopeId::for_item::<FederationScene>(&first_scene);
    let empty_scope = ScopeId::for_item::<FederationScene>(&empty_scene);
    let hidden_scope = ScopeId::for_item::<FederationScene>(&hidden_scene);
    let mut delete_request = request(CommandId::new());
    delete_request.scope_id = empty_scope.clone();
    delete_request
        .resource_claims
        .first_mut()
        .unwrap()
        .selection = ScopeSelection::Exact(empty_scope.clone());
    let executing = source
        .admit(delete_request.clone())
        .unwrap()
        .snapshot()
        .clone();
    let mut deletion = ItemMutation::delete::<FederationSceneElement>(&deleted_element);
    deletion.scope_id = Some(empty_scope.as_str().to_owned());
    source
        .commit(
            delete_request.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: delete_request.id,
                service_id: delete_request.service_id,
                scope_id: delete_request.scope_id,
                causal_parents: vec![executing.updated_at],
                changes: vec![deletion],
            },
            Vec::new(),
        )
        .unwrap();

    let requested = ScopeSelection::Subtree(project_scope.clone());
    let allowed = vec![
        ScopeSelection::Exact(project_scope),
        ScopeSelection::Exact(first_scope),
        ScopeSelection::Exact(empty_scope.clone()),
    ];
    let principal = PrincipalId::new("node:selected-reader");
    let presentation = AuthorityPresentation::direct_node(principal.clone());

    let unbound = Node::in_memory()
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(unbound.visibility, ResourceVisibility::Unbound);
    assert!(unbound.value.is_none());
    assert!(!unbound.complete);

    let policy: Arc<dyn AccessPolicy> = Arc::new(SelectedScopesPolicy {
        allowed: allowed.clone(),
    });
    source.set_command_access_policy(policy.clone()).unwrap();
    let local = source
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(local.coverage, ProjectionCoverage::LocalAuthoritative);
    assert!(local.complete);
    assert!(!local.requested_fully_authorized);
    assert_eq!(local.visibility, ResourceVisibility::Present);
    assert_eq!(
        local.included_scopes,
        allowed.iter().map(|s| s.root().clone()).collect::<Vec<_>>()
    );
    assert!(
        matches!(local.value, Some(ref items) if items == &vec![FederationSceneElement {
            id: first_element,
            federation_scene_id: first_scene,
            name: "element".to_owned(),
        }]),
        "unexpected selected value: {:?}",
        local.value
    );
    assert!(!local.included_scopes.contains(&hidden_scope));

    let exact_empty = ScopeSelection::Exact(empty_scope);
    let absent = source
        .query_item_selected::<FederationSceneElement>(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &exact_empty,
            GetFederationSceneElementById {
                id: deleted_element,
            },
        )
        .unwrap();
    assert_eq!(absent.value, Some(None));
    assert_eq!(absent.visibility, ResourceVisibility::AuthoritativelyAbsent);

    let replica = allow_all_node();
    for event in source.events_after(None).unwrap() {
        replica.ingest(event).unwrap();
    }
    let replica_policy: Arc<dyn AccessPolicy> = Arc::new(SelectedScopesPolicy {
        allowed: allowed.clone(),
    });
    replica
        .set_command_access_policy(replica_policy.clone())
        .unwrap();
    let incomplete = replica
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(
        incomplete.coverage,
        ProjectionCoverage::ReplicatedIncomplete
    );
    assert_eq!(incomplete.visibility, ResourceVisibility::NotReplicated);
    assert!(!incomplete.complete);

    let undiscoverable = allow_all_node();
    undiscoverable
        .set_command_access_policy(replica_policy.clone())
        .unwrap();
    let missing_source = undiscoverable
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(missing_source.coverage, ProjectionCoverage::Undiscoverable);
    assert_eq!(
        missing_source.visibility,
        ResourceVisibility::Undiscoverable
    );
    undiscoverable
        .mark_replication_source_unreachable(source.node_id())
        .unwrap();
    let unreachable = undiscoverable
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(unreachable.coverage, ProjectionCoverage::Unreachable);
    assert_eq!(unreachable.visibility, ResourceVisibility::Unreachable);

    let unknown_root = ScopeId::new("project:topology-unknown");
    let topology_policy: Arc<dyn AccessPolicy> = Arc::new(SelectedScopesPolicy {
        allowed: vec![ScopeSelection::Subtree(unknown_root.clone())],
    });
    source
        .set_command_access_policy(topology_policy.clone())
        .unwrap();
    let topology_incomplete = source
        .query_items_selected(
            principal.clone(),
            presentation.clone(),
            source.node_id(),
            &ScopeSelection::Subtree(unknown_root),
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(
        topology_incomplete.visibility,
        ResourceVisibility::TopologyIncomplete
    );
    assert!(!topology_incomplete.complete);
    source.set_command_access_policy(policy).unwrap();

    let effective = ReplicationSelection::Intersection {
        requested: Box::new(ReplicationSelection::Scopes(vec![requested.clone()])),
        scopes: allowed,
    };
    replica
        .ingest_selected_batch(source.export_selected(effective, None).unwrap())
        .unwrap();
    let complete = replica
        .query_items_selected(
            principal,
            presentation,
            source.node_id(),
            &requested,
            GetAllFederationSceneElements,
        )
        .unwrap();
    assert_eq!(complete.coverage, ProjectionCoverage::ReplicatedComplete);
    assert!(complete.complete);
    assert!(!complete.requested_fully_authorized);
    assert!(matches!(complete.value, Some(items) if items.len() == 1));
}

#[test]
fn selected_query_keeps_in_scope_mutations_from_commands_with_out_of_scope_claims() {
    let node = allow_all_node();
    let selected_scope = ScopeId::new("session:selected");
    let mut command = request(CommandId::new());
    command.scope_id = selected_scope.clone();
    assert_eq!(command.resource_claims.len(), 1);
    let Some(primary_claim) = command.resource_claims.first_mut() else {
        return;
    };
    primary_claim.selection = ScopeSelection::Exact(selected_scope.clone());
    command.resource_claims.push(ResourceClaim {
        selection: ScopeSelection::Exact(ScopeId::new("catalog:referenced")),
        kind: ResourceClaimKind::Referenced,
        source_node: Some(node.node_id()),
        service_id: Some(ServiceId::new(TestService::SERVICE_ID)),
        item_type: Some(TestMarker::ITEM_TYPE.to_owned()),
        item_id: Some("referenced".to_owned()),
        required_permissions: vec![FederationPermission::ReadState],
        required_operations: vec![AccessOperation::ReadItems],
        required_capabilities: Vec::new(),
    });
    let executing = node.admit(command.clone()).unwrap().snapshot().clone();
    let record = TestRecord {
        id: TestRecordId::from("selected"),
        value: "visible".to_owned(),
    };
    node.commit(
        command.id,
        ChangeBatch {
            id: BatchId::new(),
            command_id: command.id,
            service_id: command.service_id,
            scope_id: command.scope_id,
            causal_parents: vec![executing.updated_at],
            changes: vec![mutation_in(&selected_scope, &record)],
        },
        Vec::new(),
    )
    .unwrap();

    let principal = PrincipalId::new("human:selected-reader");
    let result = node
        .query_items_selected(
            principal.clone(),
            AuthorityPresentation::direct_node(principal),
            node.node_id(),
            &ScopeSelection::Exact(selected_scope),
            GetAllTestRecords,
        )
        .unwrap();

    assert_eq!(result.value, Some(vec![record]));
}

#[test]
fn selected_replication_rejects_an_event_outside_its_selection() {
    let source = allow_all_node();
    let command = request(CommandId::new());
    source.submit(command).unwrap();
    let mut selected = source
        .export_selected(ReplicationSelection::All, None)
        .unwrap();
    selected.selection = ReplicationSelection::Service(ServiceId::new(OtherService::SERVICE_ID));

    assert!(matches!(
        allow_all_node().ingest_selected_batch(selected),
        Err(NodeError::InvalidReplicationBatch(_))
    ));
}

#[test]
fn short_lived_client_submits_and_only_a_handler_claims_execution() {
    let node = allow_all_node();
    let request = request(CommandId::new());

    let submitted = node.submit(request.clone()).unwrap();
    assert!(matches!(submitted.state, CommandState::Submitted));
    assert!(matches!(
        node.claim(request.id).unwrap(),
        CommandAdmission::Execute(_)
    ));
    assert!(matches!(
        node.claim(request.id).unwrap(),
        CommandAdmission::Resume(CommandSnapshot {
            state: CommandState::Executing,
            ..
        })
    ));
}

#[test]
fn cancellation_is_terminal_idempotent_and_blocks_execution() {
    let node = allow_all_node();
    let queued = request(CommandId::new());
    node.submit(queued.clone()).unwrap();

    let cancelled = node.cancel(queued.id, "operator stopped it").unwrap();
    assert!(matches!(
        cancelled.state,
        CommandState::Cancelled { ref reason } if reason == "operator stopped it"
    ));
    assert!(cancelled.state.is_terminal_locally());
    assert!(!cancelled.state.is_committed());
    assert!(!node.claim(queued.id).unwrap().should_execute());
    assert_eq!(
        node.cancel(queued.id, "different retry reason").unwrap(),
        cancelled
    );

    let running = request(CommandId::new());
    node.admit(running.clone()).unwrap();
    node.cancel(running.id, "cancel running work").unwrap();
    assert_eq!(
        node.commit(running.id, batch(&running), Vec::new()),
        Err(NodeError::CommandNotExecuting(running.id))
    );
}

#[test]
fn stale_lifecycle_events_cannot_resurrect_a_cancelled_command() {
    let source = allow_all_node();
    let target = allow_all_node();
    let command = request(CommandId::new());
    source.submit(command.clone()).unwrap();
    source.claim(command.id).unwrap();
    source.cancel(command.id, "stop").unwrap();

    for event in source.events_after(None).unwrap().into_iter().rev() {
        target.ingest(event).unwrap();
    }
    assert!(matches!(
        target.command(command.id).unwrap().map(|value| value.state),
        Some(CommandState::Cancelled { reason }) if reason == "stop"
    ));
}

#[test]
fn failed_durable_append_never_changes_visible_state() {
    let node = Node::from_journal(Arc::new(FailingJournal {
        node_id: NodeId::new(),
    }))
    .unwrap();
    let _policy = install_allow_all(&node);
    let command = request(CommandId::new());
    let mut events = node.subscribe(None).unwrap();

    assert!(matches!(
        node.submit(command.clone()),
        Err(NodeError::Backend(_))
    ));
    assert!(node.command(command.id).unwrap().is_none());
    assert!(node.events_after(None).unwrap().is_empty());
    assert!(events.try_recv().is_none());
}

#[test]
fn subscription_replays_then_continues_without_a_cursor_gap() {
    let node = allow_all_node();
    let first = request(CommandId::new());
    node.admit(first).unwrap();

    let mut events = node.subscribe(None).unwrap();
    assert_eq!(events.recv().unwrap().position, LogPosition::new(1));

    let second = request(CommandId::new());
    let second_id = second.id;
    node.admit(second).unwrap();
    assert_eq!(events.recv().unwrap().position, LogPosition::new(2));
    assert!(node.command(second_id).unwrap().is_some());
}

#[test]
fn subscription_from_now_omits_existing_history_and_follows_new_events() {
    let node = allow_all_node();
    for _ in 0..=DURABLE_EVENT_PAGE_SIZE {
        node.admit(request(CommandId::new())).unwrap();
    }

    let mut events = node.subscribe_from_now().unwrap();
    assert!(events.replay.as_ref().is_some_and(|replay| {
        replay.after
            == Some(LogPosition::new(
                u64::try_from(DURABLE_EVENT_PAGE_SIZE + 1).unwrap(),
            ))
            && replay.buffered.is_empty()
    }));
    assert!(events.try_recv().is_none());

    node.admit(request(CommandId::new())).unwrap();
    assert_eq!(
        events.recv().unwrap().position,
        LogPosition::new(u64::try_from(DURABLE_EVENT_PAGE_SIZE + 2).unwrap())
    );
}

#[test]
fn scope_catalog_reads_stable_pages_from_the_backend_index() {
    let node = allow_all_node();
    for ordinal in 0..25 {
        let scope_id = ScopeId::new(format!("session:{ordinal:04}"));
        let mut command = request(CommandId::new());
        command.scope_id = scope_id.clone();
        command.resource_claims.first_mut().unwrap().selection = ScopeSelection::Exact(scope_id);
        node.submit(command).unwrap();
    }
    let limit = NonZeroUsize::new(10).unwrap();

    let first = node.scope_ids_page(None, limit).unwrap();
    assert_eq!(first.len(), 10);
    assert_eq!(first.first().map(ScopeId::as_str), Some("session:0000"));
    assert_eq!(first.last().map(ScopeId::as_str), Some("session:0009"));

    let second = node.scope_ids_page(first.last(), limit).unwrap();
    assert_eq!(second.len(), 10);
    assert_eq!(second.first().map(ScopeId::as_str), Some("session:0010"));
    assert_eq!(second.last().map(ScopeId::as_str), Some("session:0019"));
}

#[test]
fn durable_subscription_applies_lossless_backpressure_at_its_bound() {
    let backend = Arc::new(InMemoryBackend::new(NodeId::new()));
    let mut events = backend.subscribe(None).unwrap();
    assert!(events.try_recv().is_none());
    assert_eq!(
        events.live.as_ref().and_then(flume::Receiver::capacity),
        Some(DURABLE_EVENT_SUBSCRIPTION_CAPACITY)
    );

    for _ in 0..DURABLE_EVENT_SUBSCRIPTION_CAPACITY {
        backend.submit(request(CommandId::new())).unwrap();
    }
    assert_eq!(
        events.live.as_ref().map(flume::Receiver::len),
        Some(DURABLE_EVENT_SUBSCRIPTION_CAPACITY)
    );

    let (completed_send, completed_recv) = std::sync::mpsc::channel();
    let blocked_backend = Arc::clone(&backend);
    let publisher = std::thread::spawn(move || {
        let result = blocked_backend.submit(request(CommandId::new()));
        completed_send.send(result).unwrap();
    });
    assert!(
        completed_recv
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );

    assert_eq!(events.recv().unwrap().position, LogPosition::new(1));
    completed_recv
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    publisher.join().unwrap();

    for expected in 2..=DURABLE_EVENT_SUBSCRIPTION_CAPACITY + 1 {
        assert_eq!(
            events.recv().unwrap().position,
            LogPosition::new(u64::try_from(expected).unwrap())
        );
    }
}

#[test]
fn durable_exports_page_history_without_an_unbounded_batch() {
    let node = allow_all_node();
    for _ in 0..=DURABLE_EVENT_PAGE_SIZE {
        node.submit(request(CommandId::new())).unwrap();
    }

    let first = node.export(None).unwrap();
    assert_eq!(first.events.len(), DURABLE_EVENT_PAGE_SIZE);
    assert_eq!(
        first.through,
        Some(LogPosition::new(
            u64::try_from(DURABLE_EVENT_PAGE_SIZE).unwrap()
        ))
    );

    let second = node.export(first.through).unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(
        second.through,
        Some(LogPosition::new(
            u64::try_from(DURABLE_EVENT_PAGE_SIZE + 1).unwrap()
        ))
    );
}

#[test]
fn durable_subscription_pages_replay_then_hands_off_to_live_without_a_gap() {
    let backend = Arc::new(InMemoryBackend::new(NodeId::new()));
    for _ in 0..=DURABLE_EVENT_PAGE_SIZE {
        backend.submit(request(CommandId::new())).unwrap();
    }

    let mut events = backend.subscribe(None).unwrap();
    assert_eq!(events.recv().unwrap().position, LogPosition::new(1));
    assert!(
        events
            .replay
            .as_ref()
            .is_some_and(|replay| replay.buffered.len() < DURABLE_EVENT_PAGE_SIZE)
    );

    backend.submit(request(CommandId::new())).unwrap();
    for expected in 2..=DURABLE_EVENT_PAGE_SIZE + 2 {
        assert_eq!(
            events.recv().unwrap().position,
            LogPosition::new(u64::try_from(expected).unwrap())
        );
    }
    assert!(events.try_recv().is_none());

    backend.submit(request(CommandId::new())).unwrap();
    assert_eq!(
        events.recv().unwrap().position,
        LogPosition::new(u64::try_from(DURABLE_EVENT_PAGE_SIZE + 3).unwrap())
    );
}

#[test]
fn scope_catalog_is_sorted_and_deduplicated() {
    let node = allow_all_node();
    let mut second = request(CommandId::new());
    second.scope_id = ScopeId::new("session:zulu");
    second.resource_claims.first_mut().unwrap().selection =
        ScopeSelection::Exact(second.scope_id.clone());
    node.admit(second).unwrap();
    let mut first = request(CommandId::new());
    first.scope_id = ScopeId::new("session:alpha");
    first.resource_claims.first_mut().unwrap().selection =
        ScopeSelection::Exact(first.scope_id.clone());
    node.admit(first).unwrap();
    let mut duplicate = request(CommandId::new());
    duplicate.scope_id = ScopeId::new("session:zulu");
    duplicate.resource_claims.first_mut().unwrap().selection =
        ScopeSelection::Exact(duplicate.scope_id.clone());
    node.admit(duplicate).unwrap();

    assert_eq!(
        node.scope_ids().unwrap(),
        vec![ScopeId::new("session:alpha"), ScopeId::new("session:zulu")]
    );
}

#[test]
fn commit_rejects_a_batch_from_another_scope() {
    let node = allow_all_node();
    let request = request(CommandId::new());
    node.admit(request.clone()).unwrap();
    let mut wrong = batch(&request);
    wrong.scope_id = ScopeId::new("session:other");

    assert_eq!(
        node.commit(request.id, wrong, Vec::new()),
        Err(NodeError::BatchMismatch(request.id))
    );
    assert!(matches!(
        node.command(request.id)
            .unwrap()
            .map(|snapshot| snapshot.state),
        Some(CommandState::Executing)
    ));
}

#[test]
fn another_node_ingests_origin_events_exactly_once() {
    let source = allow_all_node();
    let target = allow_all_node();
    let request = request(CommandId::new());
    source.admit(request.clone()).unwrap();
    source
        .commit(request.id, batch(&request), b"done".to_vec())
        .unwrap();
    let source_events = source.events_after(None).unwrap();

    for event in &source_events {
        assert!(matches!(
            target.ingest(event.clone()).unwrap(),
            IngestStatus::Applied { .. }
        ));
    }
    let first_source_event = source_events.first().cloned().unwrap();
    assert_eq!(
        target.ingest(first_source_event.clone()).unwrap(),
        IngestStatus::Duplicate
    );
    assert!(
        target
            .command(request.id)
            .unwrap()
            .is_some_and(|command| command.state.is_committed())
    );

    let target_events = target.events_after(None).unwrap();
    assert_eq!(target_events.len(), source_events.len());
    let first_target_event = target_events.first().unwrap();
    assert_eq!(first_target_event.position, LogPosition::new(1));
    assert_eq!(first_target_event.origin, first_source_event.origin);
}

#[test]
fn command_origin_survives_replication_without_becoming_the_replica() {
    let source = allow_all_node();
    let target = allow_all_node();
    let source_command = request(CommandId::new());
    source.submit(source_command.clone()).unwrap();

    assert_eq!(
        source.command_origin(source_command.id).unwrap(),
        Some(source.node_id())
    );
    assert_eq!(target.command_origin(source_command.id).unwrap(), None);
    target.ingest_batch(source.export(None).unwrap()).unwrap();
    assert_eq!(
        target.command_origin(source_command.id).unwrap(),
        Some(source.node_id())
    );
    assert_ne!(source.node_id(), target.node_id());

    let target_command = request(CommandId::new());
    target.submit(target_command.clone()).unwrap();
    assert_eq!(
        target.command_origin(target_command.id).unwrap(),
        Some(target.node_id())
    );
}

#[test]
fn invalid_batch_cursor_is_rejected_before_any_event_is_ingested() {
    let source = allow_all_node();
    let target = allow_all_node();
    let command = request(CommandId::new());
    source.admit(command).unwrap();
    let mut batch = source.export(None).unwrap();
    batch.through = Some(LogPosition::new(99));

    assert!(matches!(
        target.ingest_batch(batch),
        Err(NodeError::InvalidReplicationBatch(_))
    ));
    assert!(target.events_after(None).unwrap().is_empty());
}

#[test]
fn live_events_are_filtered_and_drop_only_for_a_slow_subscriber() {
    let node_id = NodeId::new();
    let hub = LiveEventHub::new(node_id);
    let capacity = NonZeroUsize::new(1).unwrap();
    let mut all = hub.subscribe(Vec::new(), capacity).unwrap();
    let mut selected = hub
        .subscribe(vec!["session:a".to_owned()], capacity)
        .unwrap();

    let first = hub.publish("session:a", b"one".to_vec()).unwrap();
    assert_eq!(first.delivered, 2);
    assert_eq!(first.dropped, 0);
    let second = hub.publish("session:a", b"two".to_vec()).unwrap();
    assert_eq!(second.delivered, 0);
    assert_eq!(second.dropped, 2);

    let all_first = all.recv().unwrap();
    let selected_first = selected.recv().unwrap();
    assert_eq!(all_first, selected_first);
    assert_eq!(all_first.source_node, node_id);
    assert_eq!(all_first.sequence, 1);

    let unrelated = hub.publish("session:b", b"other".to_vec()).unwrap();
    assert_eq!(unrelated.delivered, 1);
    assert_eq!(unrelated.dropped, 0);
    assert_eq!(all.recv().unwrap().topic, "session:b");
    assert!(selected.try_recv().is_none());

    drop(all);
    let resumed = hub.publish("session:a", b"three".to_vec()).unwrap();
    assert_eq!(resumed.sequence, 3);
    assert_eq!(resumed.delivered, 1);
    assert_eq!(resumed.dropped, 0);
    assert_eq!(selected.recv().unwrap().payload, b"three");
}

#[test]
fn command_snapshot_requires_canonical_scope_spelling() {
    let node = Node::in_memory();
    let _policy = install_allow_all(&node);
    let record_id = TestRecordId::from("record-1");
    let current_scope = ScopeId::for_item::<TestRecord>(&record_id);
    let legacy_scope = ScopeId::new(current_scope.as_str().split_once('/').unwrap().1);
    let command = DeclaredCommand::new(
        CommandId::new(),
        legacy_scope,
        PrincipalId::new("human:test"),
        PutRecord {
            id: record_id.as_ref().to_owned(),
            value: "legacy scope".to_owned(),
        },
    );
    node.submit(command.request().unwrap()).unwrap();

    let snapshot = node
        .command_states(CommandStateRequest::for_serving_declared::<PutRecord>(
            current_scope,
        ))
        .unwrap();
    assert!(snapshot.commands.is_empty());
}
