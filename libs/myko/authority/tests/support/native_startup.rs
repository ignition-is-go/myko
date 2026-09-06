use std::time::Duration;

use super::*;

#[tokio::test]
async fn native_controller_votes_before_application_startup_completes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("starting.redb"))?;
    let startup = node.hold_startup();
    let server = IrohReplicator::bind_loopback(node.clone()).await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let [a_key, b_key] = keys();
    let principal = Principal::node(endpoint_principal_id(client.address().id));
    let binding = AuthorityControllerPrincipal::new(principal.clone(), controller_id(&a_key));
    server.sessions().set_authority_control(Some(Arc::new(
        CertifiedAuthorityControlEndpoint::new(node.clone(), anchor()?, b_key, vec![binding])?,
    )))?;
    let endpoint = client.command_client(server.address());
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller_id(&a_key),
    };
    let outcome = async {
        let vote = tokio::time::timeout(
            Duration::from_secs(5),
            endpoint.prepare(
                &principal.id,
                &AuthorityPresentation::direct(principal.clone()),
                anchor()?.genesis(),
                ballot,
            ),
        )
        .await
        .map_err(|_| "controller vote waited for application startup")?
        .map_err(|error| format!("startup controller vote failed: {error:?}"))?;
        if vote.message.ballot != ballot || node.is_ready() {
            return Err("controller vote released the application startup barrier".into());
        }
        let before = node.events_after(None)?;
        let wrong_proposer = endpoint
            .prepare(
                &principal.id,
                &AuthorityPresentation::direct(principal.clone()),
                anchor()?.genesis(),
                ControlBallot {
                    counter: 2,
                    proposer: vote.message.controller,
                },
            )
            .await;
        if wrong_proposer.is_ok() || before != node.events_after(None)? {
            return Err("startup bypass ignored the caller's controller binding".into());
        }
        let forged = Principal::node(PrincipalId::new("unbound"));
        let rejected = tokio::time::timeout(
            Duration::from_secs(5),
            endpoint.prepare(
                &forged.id,
                &AuthorityPresentation::direct(forged.clone()),
                anchor()?.genesis(),
                ControlBallot {
                    counter: 2,
                    ..ballot
                },
            ),
        )
        .await
        .map_err(|_| "forged controller request waited for application startup")?;
        if rejected.is_ok() || before != node.events_after(None)? {
            return Err("startup bypass admitted a forged controller".into());
        }
        let sessions = server.sessions();
        let request = myko_wire::NodeRequestEnvelope::connected(myko_wire::NodeRequest::Identify);
        let opening = sessions.open(principal.id.clone(), request);
        tokio::pin!(opening);
        if tokio::time::timeout(Duration::from_millis(100), &mut opening)
            .await
            .is_ok()
        {
            return Err("ordinary session escaped the application startup barrier".into());
        }
        startup.ready();
        let _frames = tokio::time::timeout(Duration::from_secs(5), opening).await?;
        Ok(())
    }
    .await;
    client.shutdown().await?;
    server.shutdown().await?;
    outcome
}
