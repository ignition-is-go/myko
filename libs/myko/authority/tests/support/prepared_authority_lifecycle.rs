use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration as StdDuration,
};

use myko_authority::certified::PreparedAuthorityRuntime;
use myko_federation::CommandState;

use super::*;

static EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

#[myko::myko_service(LifecycleRoot)]
pub struct LifecycleService;

#[myko::myko_item(service = LifecycleService, scope_root)]
pub struct LifecycleRoot {
    label: String,
}

#[myko::myko_command(String, item = LifecycleRoot)]
pub struct LifecycleCommand {
    root: LifecycleRootId,
    label: String,
}

impl myko::CommandHandler for LifecycleCommand {
    fn scope(&self, _node_id: myko_federation::NodeId) -> LifecycleRootId {
        self.root.clone()
    }

    fn execute(self, context: myko::CommandContext) -> Result<String, myko::CommandError> {
        EXECUTIONS.fetch_add(1, Ordering::SeqCst);
        context.emit_set(&LifecycleRoot {
            id: self.root,
            label: self.label.clone(),
        })?;
        Ok(self.label)
    }
}

fn host(node: &Node) -> Result<ApplicationHost, String> {
    ApplicationHost::new(
        node.clone(),
        MykoApplication::builder()
            .service::<LifecycleService>()
            .build(),
    )
}

#[derive(Debug)]
struct HeldEvidence {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

impl ScopedRetainedEvidenceEndpoint for HeldEvidence {
    fn refresh_scopes<'a>(&'a self, _scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async move {
            self.entered
                .send(())
                .map_err(|error| RetainedEvidenceError::Invalid(error.to_string()))?;
            self.release
                .recv_async()
                .await
                .map_err(|error| RetainedEvidenceError::Invalid(error.to_string()))?;
            Ok(())
        })
    }
}

async fn wait_committed(node: &Node, command_id: CommandId) -> TestResult {
    tokio::time::timeout(StdDuration::from_secs(30), async {
        loop {
            if node
                .command(command_id)?
                .is_some_and(|command| command.state.is_committed())
            {
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn installed_runtime_joins_inflight_coordination_and_recovers_item_effect_once() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let a_path = directory.path().join("lifecycle-a.redb");
    let b_path = directory.path().join("lifecycle-b.redb");
    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let application = host(&a)?.with_access_policy(Arc::new(AllowAllAccessPolicy))?;
    let command = application.submit_authenticated_command(
        PrincipalId::new("reader"),
        &LifecycleCommand {
            root: LifecycleRootId::from("installed"),
            label: "retained item effect".to_owned(),
        },
    )?;
    let command_id = command.request.id;
    let scope = command.request.scope_id;
    let [a_key, b_key] = keys();
    install_scoped_grant(&a, &b, &anchor()?, &a_key, &b_key, scope.clone())?;
    let harness = NativeControlHarness::start(
        a.clone(),
        b.clone(),
        authority_realm_scope(&realm()),
        scope.clone(),
    )
    .await?;
    let (entered_tx, entered_rx) = flume::unbounded();
    let (_release_tx, release_rx) = flume::unbounded();
    let held = Arc::new(HeldEvidence {
        entered: entered_tx,
        release: release_rx,
    });
    let peers = harness
        .peers()
        .into_iter()
        .map(|peer| peer.with_observer_evidence_endpoint(held.clone()))
        .collect();
    let coordinator =
        AuthorityDecisionCoordinator::new(anchor()?, a.clone(), harness.a_binding.clone(), peers)?;
    let (application, guard) = PreparedAuthorityRuntime::install(
        application,
        coordinator,
        Arc::new(AllowAllAccessPolicy),
        |_| {},
    )?;
    tokio::time::timeout(StdDuration::from_secs(5), entered_rx.recv_async()).await??;
    let saved = super::prepared_runtime::saved_effect(&a, command_id)?;
    if saved.batch().changes.is_empty() || EXECUTIONS.load(Ordering::SeqCst) != 1 {
        return Err("installed dispatch did not run the item-writing handler exactly once".into());
    }
    tokio::time::timeout(StdDuration::from_secs(5), guard.shutdown()).await??;
    if a.command(command_id)?
        .is_none_or(|command| !matches!(command.state, CommandState::AuthorizationPrepared { .. }))
    {
        return Err("shutdown released or lost the blocked effect".into());
    }
    application.shutdown().await;
    harness.shutdown().await?;
    drop(application);
    drop(a);
    drop(b);

    let a = RedbJournal::open_node(&a_path)?;
    let b = RedbJournal::open_node(&b_path)?;
    let harness =
        NativeControlHarness::start(a.clone(), b.clone(), authority_realm_scope(&realm()), scope)
            .await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        a.clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let (application, guard) = PreparedAuthorityRuntime::install(
        host(&a)?,
        coordinator,
        Arc::new(AllowAllAccessPolicy),
        |_| {},
    )?;
    wait_committed(&a, command_id).await?;
    super::prepared_runtime::assert_exact_commit(&a, command_id, &saved)?;
    if EXECUTIONS.load(Ordering::SeqCst) != 1 || guard.failure().is_some() {
        return Err("restart reran the handler or stopped an installed worker".into());
    }
    guard.shutdown().await?;
    application.shutdown().await;
    harness.shutdown().await?;
    Ok(())
}

#[test]
fn installation_without_an_executor_does_not_replace_the_existing_policy() -> TestResult {
    let a = Node::in_memory();
    let b = Node::in_memory();
    let application = host(&a)?.with_access_policy(Arc::new(AllowAllAccessPolicy))?;
    let retained = application.clone();
    if PreparedAuthorityRuntime::install(
        application,
        coordinator(&a, &b)?,
        Arc::new(AllowAllAccessPolicy),
        |_| {},
    )
    .is_ok()
    {
        return Err("installed async authority without an executor".into());
    }
    retained.submit_authenticated_command(
        PrincipalId::new("reader"),
        &LifecycleCommand {
            root: LifecycleRootId::from("preflight"),
            label: "not dispatched".to_owned(),
        },
    )?;
    Ok(())
}
