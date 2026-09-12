use std::{sync::Arc, time::Duration};

use myko::{ApplicationHost, MykoApplication, MykoService as _, prelude::*};
use myko_federation::{
    CommandId, CommandState, ExecutionAssignment, FederationPermission, NodeId, ScopeGrant,
    ScopeGrantCoverage, ScopeGrantPolicy, ScopeId, ScopeSelection, SelectedHistorySnapshot,
    ServiceId, SubscriptionLiveness,
};
use myko_redb::RedbJournal;

use super::{Mesh, Peer, TestResult};

#[myko_service(BoundaryRecord)]
pub struct BoundaryService;

#[myko_item(service = BoundaryService, scope_root)]
pub struct BoundaryRecord {
    value: String,
}

#[myko::myko_command(BoundaryRecord, item = BoundaryRecord)]
pub struct SetBoundaryRecord {
    record_id: BoundaryRecordId,
    value: String,
}

impl CommandHandler for SetBoundaryRecord {
    fn scope(&self, _local_node: NodeId) -> BoundaryRecordId {
        self.record_id.clone()
    }

    fn execute(self, context: CommandContext) -> Result<Self::Result, CommandError> {
        let record = BoundaryRecord {
            id: self.record_id,
            value: self.value,
        };
        context.emit_set(&record)?;
        Ok(record)
    }
}

#[myko::myko_query(BoundaryRecord, item = BoundaryRecord)]
#[derive(PartialEq, Eq)]
struct BoundaryRecords {
    scope_id: ScopeId,
}

impl myko::query::QueryHandler for BoundaryRecords {
    fn source_node(&self, _local_node: NodeId) -> Option<NodeId> {
        None
    }

    fn scope_id(&self, _local_node: NodeId) -> Option<ScopeId> {
        Some(self.scope_id.clone())
    }

    fn build_view(
        context: myko::query::QueryBuildArgs<Self>,
    ) -> Result<Option<impl myko::query::QueryBuildOutput>, String> {
        Ok(Some(myko::query::RetainedQuery::new(
            context.federated_items::<BoundaryRecord>()?,
        )))
    }
}

fn install_application(
    peer: &Peer,
    grants: &[ScopeGrant],
) -> Result<ApplicationHost, Box<dyn std::error::Error>> {
    let policy: Arc<dyn myko_federation::AccessPolicy> =
        Arc::new(ScopeGrantPolicy::new(grants.to_vec()));
    let application = ApplicationHost::new(
        peer.node.clone(),
        MykoApplication::builder()
            .service::<BoundaryService>()
            .build(),
    )?
    .with_access_policy(Arc::clone(&policy))?;
    peer.transport.set_access_policy(policy)?;
    peer.transport
        .sessions()
        .set_application(application.clone())?;
    Ok(application)
}

async fn current_records(
    reader: &Peer,
    server: &Peer,
    scope: &ScopeId,
) -> Result<Vec<BoundaryRecord>, Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let client = reader
            .transport
            .application_client(server.transport.address());
        let mut subscription = client
            .follow_query(
                None,
                scope.clone(),
                &BoundaryRecords {
                    scope_id: scope.clone(),
                },
            )
            .await?;
        while subscription.current().liveness != SubscriptionLiveness::Current {
            subscription.recv().await?;
        }
        subscription
            .current()
            .value
            .clone()
            .ok_or_else(|| "Current query did not supply a value".into())
    })
    .await?
}

fn scope_grants(mesh: &Mesh, scope: &ScopeId) -> Vec<ScopeGrant> {
    let [a, b, c] = &mesh.peers;
    let mut grants: Vec<_> = [a, b, c]
        .into_iter()
        .map(|reader| ScopeGrant {
            scope_id: mesh.anchor.realm().clone(),
            coverage: ScopeGrantCoverage::Exact,
            grantee: reader.principal().id,
            permissions: vec![FederationPermission::ReadHistory],
        })
        .collect();
    grants.extend([
        ScopeGrant {
            scope_id: scope.clone(),
            coverage: ScopeGrantCoverage::Exact,
            grantee: a.principal().id,
            permissions: vec![FederationPermission::Write],
        },
        ScopeGrant {
            scope_id: scope.clone(),
            coverage: ScopeGrantCoverage::Exact,
            grantee: c.principal().id,
            permissions: vec![
                FederationPermission::ReadState,
                FederationPermission::Subscribe,
            ],
        },
    ]);
    grants
}

#[tokio::test]
async fn fresh_assignment_quorum_does_not_certify_application_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mesh = Mesh::open(directory.path()).await?;
    let [a, b, c] = &mesh.peers;
    let record_id = BoundaryRecordId::from("work");
    let scope = ScopeId::for_item::<BoundaryRecord>(&record_id);
    let service = ServiceId::new(BoundaryService::SERVICE_ID);
    let selection = ScopeSelection::Exact(scope.clone());
    let assigned = mesh
        .coordinator(a)?
        .assign(ExecutionAssignment::new(
            CommandId::new(),
            mesh.anchor.realm().clone(),
            scope.clone(),
            service.clone(),
            [a.node.node_id(), b.node.node_id()],
        ))
        .await?;

    let grants = scope_grants(&mesh, &scope);
    let application = install_application(a, &grants)?;
    let replacement = install_application(b, &grants)?;
    let record = BoundaryRecord {
        id: record_id.clone(),
        value: "committed only on A".to_owned(),
    };
    let accepted = application.submit_authenticated_command(
        a.principal().id,
        &SetBoundaryRecord {
            record_id,
            value: record.value.clone(),
        },
    )?;
    if accepted.state != CommandState::Submitted {
        return Err("fixture did not acknowledge a locally submitted command".into());
    }
    let committed = application
        .dispatch_registered_command(accepted.request.id)?
        .command;
    if !matches!(committed.state, CommandState::CommittedLocally { .. })
        || committed.typed_completion::<SetBoundaryRecord>()? != Some(record.clone())
    {
        return Err("typed command did not commit its record and result locally".into());
    }
    if current_records(c, a, &scope).await? != vec![record] {
        return Err("source query did not publish the committed record as Current".into());
    }
    let required = SelectedHistorySnapshot::current(&a.node)?
        .retained_manifest(&selection)?
        .commitment()?;
    if required.event_count() < 3 {
        return Err("fixture did not retain its command lifecycle".into());
    }

    application.shutdown().await;
    drop(application);
    a.transport.clone().shutdown().await?;
    let observed = mesh.coordinator(b)?.observe().await?;
    if observed.head() == assigned.head()
        || observed.exact(&scope, &service) != Some(&[a.node.node_id(), b.node.node_id()].into())
    {
        return Err("surviving controllers did not produce a fresh assignment observation".into());
    }
    let mut copies = Vec::new();
    for peer in [b, c] {
        if peer.node.command(accepted.request.id)?.is_some() {
            return Err("fixture unexpectedly replicated the application command".into());
        }
        let readable = SelectedHistorySnapshot::current(&peer.node)?
            .retained_manifest(&selection)?
            .commitment()?;
        if readable.event_count() != 0 || readable == required {
            return Err(
                "fixture did not distinguish closed local history from required history".into(),
            );
        }
        copies.push(readable);
    }
    if copies.first() != copies.last() {
        return Err("surviving controllers should agree on the same incomplete history".into());
    }

    // This diagnoses the missing serving gate; an empty Current is not safe failover.
    if !current_records(c, b, &scope).await?.is_empty() {
        return Err("replacement no longer exposes the incomplete-history counterexample".into());
    }

    replacement.shutdown().await;
    drop(replacement);
    b.transport.clone().shutdown().await?;
    c.transport.clone().shutdown().await?;
    drop(mesh);
    let reopened = RedbJournal::open_node(directory.path().join("101.redb"))?;
    if reopened.command(accepted.request.id)? != Some(committed) {
        return Err("the source commit was not durably retained across reopen".into());
    }
    println!(
        "A published its typed commit as Current; after A stopped, fresh assignment quorum succeeded and B published empty Current without that durable history"
    );
    Ok(())
}
