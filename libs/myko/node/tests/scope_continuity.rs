use std::{path::Path, sync::Arc, time::Duration};

use hyphae::Gettable as _;
use myko::{MykoApplication, MykoService as _, myko_item};
use myko_federation::{
    AccessOperation, AllowAllAccessPolicy, AuthorityPresentation, BatchId, ChangeBatch,
    CommandClient as _, CommandId, CommandRequest, CommandSubmission, CommandWatchingClient,
    EventJournal as _, FederationPermission, NodeId, PrincipalId, ResourceClaim, ResourceClaimKind,
    ScopeId, ScopeSelection, SelectedHistoryManifest, SelectedHistorySnapshot, ServiceId,
};
use myko_items::{ItemMutation, myko_service};
use myko_node::Node;

#[myko_service(ContinuityRecord)]
pub struct ContinuityService;

#[myko_item(service = ContinuityService, scope_root)]
pub struct ContinuityRecord {
    pub value: String,
}

#[myko::myko_command(ContinuityRecord, item = ContinuityRecord)]
pub struct UpdateContinuityRecord {
    record_id: ContinuityRecordId,
    expected_value: String,
    value: String,
}

impl myko::CommandHandler for UpdateContinuityRecord {
    fn scope(&self, _local_node: NodeId) -> ContinuityRecordId {
        self.record_id.clone()
    }

    fn authority_claims(&self, _local_node: NodeId) -> Vec<ResourceClaim> {
        let mut claim = ResourceClaim::scope(
            ScopeId::for_item::<ContinuityRecord>(&self.record_id),
            ResourceClaimKind::Primary,
        );
        claim
            .required_permissions
            .push(FederationPermission::ReadState);
        claim.required_operations.push(AccessOperation::ReadItems);
        vec![claim]
    }

    fn execute(self, context: myko::CommandContext) -> Result<Self::Result, myko::CommandError> {
        let mut record = context
            .exec_item_query(GetContinuityRecordsByIds {
                ids: vec![self.record_id],
            })?
            .into_iter()
            .next()
            .ok_or_else(|| myko::CommandError::reject("replicated record is missing"))?;
        if record.value != self.expected_value {
            return Err(myko::CommandError::reject(
                "replacement read the wrong predecessor value",
            ));
        }
        record.value = self.value;
        context.emit_set(&record)?;
        Ok(record)
    }
}

#[myko::myko_query(ContinuityRecord, item = ContinuityRecord)]
#[derive(PartialEq, Eq)]
struct ContinuityRecords {
    origin: Option<NodeId>,
    scope_id: ScopeId,
}

impl myko::query::QueryHandler for ContinuityRecords {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        self.origin
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(self.scope_id.clone())
    }

    fn build_view(
        context: myko::query::QueryBuildArgs<Self>,
    ) -> Option<impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<dyn myko::item::AnyItem>>> {
        let source = context.federated_items::<ContinuityRecord>();
        assert!(
            source.is_ok(),
            "continuity query requires its declared durable source"
        );
        source.ok()
    }
}

fn continuity_application() -> MykoApplication {
    MykoApplication::builder()
        .service::<ContinuityService>()
        .build()
}

async fn open_continuity_node(data_dir: &Path) -> Result<Node, String> {
    Node::open_loopback_application_with_policy(
        data_dir,
        Duration::from_millis(20),
        continuity_application(),
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await
    .map_err(|error| error.to_string())
}

fn commit_record(node: &Node, scope_id: ScopeId, value: &str) -> Result<CommandId, String> {
    let request = CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new(ContinuityService::SERVICE_ID),
        scope_id,
        principal_id: PrincipalId::new("node:scope-continuity-test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new(
            "node:scope-continuity-test",
        )),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "scope_continuity.create".to_owned(),
        payload: Vec::new(),
    };
    let admission = node
        .node()
        .admit(request.clone())
        .map_err(|error| error.to_string())?;
    let record = ContinuityRecord {
        id: ContinuityRecordId::from("founding-record"),
        value: value.to_owned(),
    };
    node.node()
        .commit(
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
    Ok(request.id)
}

async fn update_scope(
    reader: &Node,
    server: &Node,
    previous: &ContinuityRecord,
) -> Result<CommandId, String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = reader.replicator().command_client(server.address());
        let submission = CommandSubmission::for_command(&UpdateContinuityRecord {
            record_id: previous.id.clone(),
            expected_value: previous.value.clone(),
            value: "accepted on C".to_owned(),
        })
        .map_err(|error| error.to_string())?;
        let command_id = submission.id;
        let response = client
            .submit_submission(submission)
            .await
            .map_err(|error| error.to_string())?;
        if response.source_node != server.node().node_id() {
            return Err("replacement command was admitted on another node".to_owned());
        }
        let mut watch = CommandWatchingClient::watch_command(&client, command_id)
            .await
            .map_err(|error| error.to_string())?;
        loop {
            if let Some(record) = watch
                .current()
                .typed_completion::<UpdateContinuityRecord>()
                .map_err(|error| error.to_string())?
            {
                if record.id != previous.id || record.value != "accepted on C" {
                    return Err("replacement command returned the wrong durable result".to_owned());
                }
                return Ok(command_id);
            }
            let _update = watch.recv().await.map_err(|error| error.to_string())?;
        }
    })
    .await
    .map_err(|error| format!("remote scope update timed out: {error}"))?
}

async fn remote_records(
    reader: &Node,
    server: &Node,
    origin: Option<NodeId>,
    scope_id: ScopeId,
) -> Result<Option<Vec<ContinuityRecord>>, String> {
    let client = reader.replicator().application_client(server.address());
    tokio::time::timeout(Duration::from_secs(5), async {
        let subscription = client
            .follow_query(
                origin,
                scope_id.clone(),
                &ContinuityRecords { origin, scope_id },
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(subscription.current().value.clone())
    })
    .await
    .map_err(|error| format!("remote scope read timed out: {error}"))?
}

fn materialized_records(
    node: &Node,
    source_node: NodeId,
    scope_id: ScopeId,
) -> Result<Option<Vec<ContinuityRecord>>, String> {
    let watch = node
        .watch_items_reactive_in(source_node, scope_id, GetAllContinuityRecords {})
        .map_err(|error| error.to_string())?;
    Ok(watch.live().state().get().value)
}

fn retained_scope_history(node: &Node, scope: &ScopeId) -> Result<SelectedHistoryManifest, String> {
    let manifest = SelectedHistorySnapshot::current(node.node())
        .map_err(|error| error.to_string())?
        .retained_manifest(&ScopeSelection::Exact(scope.clone()))
        .map_err(|error| error.to_string())?;
    if manifest.events().is_empty() {
        return Err("continuity fixture has no retained scope history".to_owned());
    }
    node.journal()
        .verify_retained_history(manifest.events())
        .map_err(|error| error.to_string())?;
    Ok(manifest)
}

async fn replicate_record(
    source: &Node,
    replica: &Node,
    origin: NodeId,
    scope: &ScopeId,
    record: &ContinuityRecord,
    command: CommandId,
) -> Result<(), String> {
    let required = retained_scope_history(source, scope)?;
    let copied = replica
        .replicator()
        .pull(source.address(), None)
        .await
        .map_err(|error| error.to_string())?;
    replica
        .journal()
        .verify_retained_history(required.events())
        .map_err(|error| error.to_string())?;
    let persisted = replica
        .node()
        .command(command)
        .map_err(|error| error.to_string())?;
    let rows = materialized_records(replica, origin, scope.clone())?;
    if copied.applied == 0
        || persisted.is_none()
        || rows.as_deref() != Some(std::slice::from_ref(record))
    {
        return Err(format!(
            "replica {} did not persist and materialize origin {origin}'s record",
            replica.node().node_id()
        ));
    }
    Ok(())
}

fn require_non_replica(reader: &Node, commands: &[CommandId]) -> Result<(), String> {
    for command in commands {
        if reader
            .node()
            .command(*command)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err(format!(
                "the request client unexpectedly replicated command {command}"
            ));
        }
    }
    Ok(())
}

fn require_recovered_command(
    node: &Node,
    command: CommandId,
    expected: &ContinuityRecord,
) -> Result<(), String> {
    let result = node
        .node()
        .command(command)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "reopened C lost the replacement command".to_owned())?
        .typed_completion::<UpdateContinuityRecord>()
        .map_err(|error| error.to_string())?;
    if result.as_ref() != Some(expected) {
        return Err("reopened C lost the replacement command's typed result".to_owned());
    }
    Ok(())
}

async fn require_remote_records(
    reader: &Node,
    server: &Node,
    scope: &ScopeId,
    expectations: &[(Option<NodeId>, Vec<ContinuityRecord>)],
) -> Result<(), String> {
    for (origin, expected) in expectations {
        let actual = remote_records(reader, server, *origin, scope.clone()).await?;
        if actual.as_ref() != Some(expected) {
            return Err(format!(
                "endpoint {} returned {actual:?} for origin {origin:?}; expected {expected:?}",
                server.node().node_id(),
            ));
        }
    }
    Ok(())
}

/// Native retained-history baseline for C01/C02, not a custody proof.
/// Serving endpoints and immutable event origins are deliberately independent.
/// The request client never pulls history or acts as a custodian.
#[tokio::test]
async fn replacement_node_materializes_scope_after_founder_and_relay_leave() -> Result<(), String> {
    let directories = tempfile::tempdir().map_err(|error| error.to_string())?;
    let record_id = ContinuityRecordId::from("founding-record");
    let scope_id = ScopeId::for_item::<ContinuityRecord>(&record_id);
    let expected = ContinuityRecord {
        id: record_id,
        value: "accepted on A".to_owned(),
    };

    let founder = open_continuity_node(&directories.path().join("a")).await?;
    let relay = open_continuity_node(&directories.path().join("b")).await?;
    let founder_node_id = founder.node().node_id();
    let command_id = commit_record(&founder, scope_id.clone(), "accepted on A")?;
    if materialized_records(&founder, founder_node_id, scope_id.clone())?
        != Some(vec![expected.clone()])
    {
        return Err("founder did not materialize its accepted scope state".to_owned());
    }

    replicate_record(
        &founder,
        &relay,
        founder_node_id,
        &scope_id,
        &expected,
        command_id,
    )
    .await?;

    founder
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;

    let replacement = open_continuity_node(&directories.path().join("c")).await?;
    replicate_record(
        &relay,
        &replacement,
        founder_node_id,
        &scope_id,
        &expected,
        command_id,
    )
    .await?;

    relay.shutdown().await.map_err(|error| error.to_string())?;

    let reader = open_continuity_node(&directories.path().join("reader")).await?;
    require_remote_records(
        &reader,
        &replacement,
        &scope_id,
        &[
            (None, vec![expected.clone()]),
            (Some(founder_node_id), vec![expected.clone()]),
            (Some(replacement.node().node_id()), Vec::new()),
        ],
    )
    .await?;
    let replacement_command = update_scope(&reader, &replacement, &expected).await?;
    let replacement_id = replacement.node().node_id();
    let updated = ContinuityRecord {
        value: "accepted on C".to_owned(),
        ..expected.clone()
    };
    let origin_rows = [
        (None, vec![updated.clone()]),
        (Some(founder_node_id), vec![expected]),
        (Some(replacement_id), vec![updated.clone()]),
    ];
    require_remote_records(&reader, &replacement, &scope_id, &origin_rows).await?;
    let required = retained_scope_history(&replacement, &scope_id)?;
    replacement
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;

    let reopened = open_continuity_node(&directories.path().join("c")).await?;
    reopened
        .journal()
        .verify_retained_history(required.events())
        .map_err(|error| error.to_string())?;
    require_recovered_command(&reopened, replacement_command, &updated)?;
    if reopened.node().node_id() != replacement_id {
        return Err("reopening C changed its node identity".to_owned());
    }
    require_remote_records(&reader, &reopened, &scope_id, &origin_rows).await?;
    require_non_replica(&reader, &[command_id, replacement_command])?;
    reopened
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    reader.shutdown().await.map_err(|error| error.to_string())?;
    Ok(())
}
