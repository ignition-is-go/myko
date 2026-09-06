use std::time::Duration as StdDuration;

use myko_authority::certified::PreparedAuthorityRuntime;

use super::*;

#[myko::myko_service(NativeRoot)]
pub struct NativeService;

#[myko::myko_item(service = NativeService, scope_root)]
pub struct NativeRoot {
    label: String,
}

#[myko::myko_command(String, item = NativeRoot)]
pub struct NativeCommand {
    root: NativeRootId,
    label: String,
}

impl myko::CommandHandler for NativeCommand {
    fn scope(&self, _node_id: myko_federation::NodeId) -> NativeRootId {
        self.root.clone()
    }

    fn execute(self, context: myko::CommandContext) -> Result<String, myko::CommandError> {
        context.emit_set(&NativeRoot {
            id: self.root,
            label: self.label.clone(),
        })?;
        Ok(self.label)
    }
}

async fn wait_prepared(node: &Node, id: CommandId) -> TestResult {
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if node.command(id)?.is_some_and(|command| {
                matches!(
                    command.state,
                    myko_federation::CommandState::AuthorizationPrepared { .. }
                )
            }) {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| format!("command {id} did not prepare: {:?}", node.command(id)))??;
    Ok(())
}

#[tokio::test]
async fn authority_worker_leaves_native_dispatch_owned_by_the_node() -> TestResult {
    let directory = tempfile::tempdir()?;
    let b = RedbJournal::open_node(directory.path().join("controller.redb"))?;
    let native = myko_node::Node::open_loopback_application_with_policy(
        directory.path().join("native"),
        StdDuration::from_secs(1),
        AuthorityPolicy::install(
            MykoApplication::builder()
                .service::<NativeService>()
                .build(),
        )?,
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await?;
    let command = NativeCommand {
        root: NativeRootId::from("native"),
        label: "first".to_owned(),
    };
    let request = native.application().authenticate_command_submission(
        PrincipalId::new("reader"),
        myko_federation::CommandSubmission::for_command(&command)?,
    )?;
    let [a_key, b_key] = keys();
    install_scoped_grant(
        native.node(),
        &b,
        &anchor()?,
        &a_key,
        &b_key,
        request.scope_id.clone(),
    )?;
    let harness = NativeControlHarness::start(
        native.node().clone(),
        b,
        authority_realm_scope(&realm()),
        request.scope_id,
    )
    .await?;
    let coordinator = AuthorityDecisionCoordinator::new(
        anchor()?,
        native.node().clone(),
        harness.a_binding.clone(),
        harness.peers(),
    )?;
    let (runtime, policy) =
        PreparedAuthorityRuntime::new(coordinator, Arc::new(AllowAllAccessPolicy));
    native.set_access_policy(policy.clone())?;
    let command = native
        .application()
        .submit_authenticated_command(PrincipalId::new("reader"), &command)?;
    wait_prepared(native.node(), command.request.id).await?;
    let saved = super::prepared_runtime::saved_effect(native.node(), command.request.id)?;
    let (result_tx, result_rx) = flume::unbounded();
    let guard = runtime.start(move |result| {
        let _ = result_tx.send(result);
    })?;
    let completed = tokio::time::timeout(StdDuration::from_secs(30), result_rx.recv_async())
        .await
        .map_err(|_| {
            format!(
                "worker did not report: {:?}",
                native.node().command(command.request.id)
            )
        })???;
    if completed.request.id != command.request.id || !completed.state.is_committed() {
        return Err("worker did not release the native dispatcher effect".into());
    }
    super::prepared_runtime::assert_exact_commit(native.node(), command.request.id, &saved)?;
    guard.shutdown().await?;
    let later = native.application().submit_authenticated_command(
        PrincipalId::new("reader"),
        &NativeCommand {
            root: NativeRootId::from("native"),
            label: "after worker shutdown".to_owned(),
        },
    )?;
    wait_prepared(native.node(), later.request.id).await?;
    let attempt = native.node().prepared_command_access(later.request.id)?;
    if policy.decide(&attempt).into_immediate() != Err(AuthorityUnavailable::PolicyUnavailable) {
        return Err("stopped worker left native effects available".into());
    }
    native.shutdown().await?;
    harness.shutdown().await?;
    Ok(())
}

#[test]
fn worker_start_without_an_executor_returns_an_error() -> TestResult {
    let a = Node::in_memory();
    let b = Node::in_memory();
    let (runtime, _policy) =
        PreparedAuthorityRuntime::new(coordinator(&a, &b)?, Arc::new(AllowAllAccessPolicy));
    if runtime.start(|_| {}).is_ok() {
        return Err("started the authority worker without an executor".into());
    }
    Ok(())
}
