use myko_node::{AuthorityControllerAddress, AuthorityRuntimeConfig};

use super::*;

#[derive(Debug)]
struct AssemblyPolicy(ScopedHistoryPolicy);

impl AccessPolicy for AssemblyPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> myko_federation::PolicyDecision<'a> {
        if request.operation == AccessOperation::ReadHistory {
            self.0.decide(request)
        } else {
            AllowAllAccessPolicy.decide(request)
        }
    }
}

async fn open_node(path: &std::path::Path) -> Result<myko_node::Node, Box<dyn Error>> {
    Ok(myko_node::Node::open_loopback_application_with_policy(
        path,
        StdDuration::from_secs(1),
        AuthorityPolicy::install(
            MykoApplication::builder()
                .service::<NativeService>()
                .build(),
        )?,
        |_| Ok(Arc::new(AllowAllAccessPolicy)),
    )
    .await?)
}

#[tokio::test]
async fn native_nodes_own_certification_without_auxiliary_transports() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut a = open_node(&directory.path().join("a")).await?;
    let mut b = open_node(&directory.path().join("b")).await?;
    let a_startup = a.node().hold_startup();
    let b_startup = b.node().hold_startup();
    let command = NativeCommand {
        root: NativeRootId::from("configured"),
        label: "native configuration".to_owned(),
    };
    let request = a.application().authenticate_command_submission(
        PrincipalId::new("reader"),
        myko_federation::CommandSubmission::for_command(&command)?,
    )?;
    record_obligated_grant_in(a.application().clone(), request.scope_id.clone(), [])?;
    let [a_key, b_key] = keys();
    let config = AuthorityRuntimeConfig {
        realm: realm(),
        initial_epoch: ControlEpochId([8; 32]),
        genesis: anchor()?.genesis(),
        initial_controllers: vec![controller_id(&a_key), controller_id(&b_key)],
        controllers: vec![
            AuthorityControllerAddress {
                controller: controller_id(&a_key),
                endpoint: a.address(),
            },
            AuthorityControllerAddress {
                controller: controller_id(&b_key),
                endpoint: b.address(),
            },
        ],
    };
    let scopes = vec![authority_realm_scope(&realm()), request.scope_id];
    let a_policy = Arc::new(AssemblyPolicy(ScopedHistoryPolicy::new(
        endpoint_principal_id(b.address().id),
        scopes.clone(),
    )));
    let b_policy = Arc::new(AssemblyPolicy(ScopedHistoryPolicy::new(
        endpoint_principal_id(a.address().id),
        scopes,
    )));
    let before = a.node().events_after(None)?;
    if a.install_certified_authority(&config, b_key.clone(), a_policy.clone(), |_| {})
        .is_ok()
        || a.node().events_after(None)? != before
    {
        return Err("invalid local key changed the native node".into());
    }
    let (errors_tx, errors_rx) = flume::unbounded();
    let report = move |result| {
        if let Err(error) = result {
            let _ = errors_tx.send(error);
        }
    };
    a.install_certified_authority(&config, a_key, a_policy, report)?;
    b.install_certified_authority(&config, b_key, b_policy, |_| {})?;
    a_startup.ready();
    b_startup.ready();
    let submitted = a
        .application()
        .submit_authenticated_command(PrincipalId::new("reader"), &command)?;
    let id = submitted.request.id;
    let outcome = tokio::time::timeout(StdDuration::from_secs(30), async {
        loop {
            if let Some(snapshot) = a.node().command(id)?
                && let Some(result) = snapshot.typed_completion::<NativeCommand>()?
            {
                if result != command.label {
                    return Err("native command returned a different typed result".into());
                }
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "native command did not commit: {:?}; {:?}",
            a.node().command(id),
            errors_rx.drain().collect::<Vec<_>>()
        )
    })?
    .map_err(|error| error.to_string());
    if a.certified_authority_failure().is_some() || b.certified_authority_failure().is_some() {
        return Err("native authority worker stopped".into());
    }
    a.shutdown().await?;
    b.shutdown().await?;
    outcome?;
    let reopened = open_node(&directory.path().join("a")).await?;
    let retained = reopened
        .node()
        .command(id)?
        .ok_or("committed command disappeared on reopen")?;
    let result = retained.typed_completion::<NativeCommand>()?;
    reopened.shutdown().await?;
    if result != Some(command.label) {
        return Err("reopened native node lost the certified typed result".into());
    }
    Ok(())
}
