use super::*;

fn principal(transport: &IrohReplicator) -> Principal {
    Principal::node(endpoint_principal_id(transport.address().id))
}

fn retains(history: &[EventEnvelope], expected: &EventEnvelope) -> bool {
    history.iter().any(|event| {
        event.origin == expected.origin
            && event.recorded_at == expected.recorded_at
            && event.event == expected.event
    })
}

fn controller_endpoint(
    nodes: &[Node; 3],
    transports: &[IrohReplicator; 3],
    keys: &[SigningKey; 3],
) -> Result<CertifiedAuthorityControlEndpoint, Box<dyn Error>> {
    let callers =
        [(&transports[0], &keys[0]), (&transports[1], &keys[1])].map(|(transport, key)| {
            AuthorityControllerPrincipal::new(principal(transport), controller_id(key))
        });
    let mut endpoint = CertifiedAuthorityControlEndpoint::new(
        nodes[2].clone(),
        anchor3()?,
        keys[2].clone(),
        callers.to_vec(),
    )?;
    for transport in &transports[..2] {
        endpoint = endpoint.with_scoped_evidence_endpoint(
            principal(transport).id,
            Arc::new(IrohScopedEvidenceEndpoint::new(
                transports[2].clone(),
                transport.address(),
            )),
        )?;
    }
    Ok(endpoint)
}

async fn check_proposer_routes(
    nodes: &[Node; 3],
    transports: &[IrohReplicator; 3],
    keys: &[SigningKey; 3],
) -> TestResult {
    let genesis = anchor3()?.genesis();
    let (a_markers, b_markers) = {
        let markers = [(&nodes[0], &keys[0]), (&nodes[1], &keys[1])].map(
            |(node, key)| -> Result<_, Box<dyn Error>> {
                let before = node.local_history_cut()?;
                AuthorityController::new(node.clone(), anchor3()?).prepare(
                    genesis,
                    ControlBallot {
                        counter: 1,
                        proposer: controller_id(key),
                    },
                    key,
                )?;
                Ok(node.events_after(before)?)
            },
        );
        let [a_markers, b_markers] = markers;
        (a_markers?, b_markers?)
    };
    let a = principal(&transports[0]);
    let b = principal(&transports[1]);
    let client_a = transports[0].command_client(transports[2].address());
    let client_b = transports[1].command_client(transports[2].address());
    client_a
        .prepare(
            &a.id,
            &AuthorityPresentation::direct(a.clone()),
            genesis,
            ControlBallot {
                counter: 2,
                proposer: controller_id(&keys[0]),
            },
        )
        .await
        .map_err(|error| format!("first proposer failed: {error:?}"))?;
    let after_a = nodes[2].events_after(None)?;
    if a_markers.is_empty()
        || !a_markers.iter().all(|event| retains(&after_a, event))
        || b_markers.iter().any(|event| retains(&after_a, event))
    {
        return Err("first proposer did not select only its configured evidence source".into());
    }
    let forged = client_a
        .prepare(
            &b.id,
            &AuthorityPresentation::direct(b.clone()),
            genesis,
            ControlBallot {
                counter: 3,
                proposer: controller_id(&keys[1]),
            },
        )
        .await;
    if forged.is_ok() || nodes[2].events_after(None)? != after_a {
        return Err("forged proposer changed evidence or controller history".into());
    }
    client_b
        .prepare(
            &b.id,
            &AuthorityPresentation::direct(b.clone()),
            genesis,
            ControlBallot {
                counter: 3,
                proposer: controller_id(&keys[1]),
            },
        )
        .await
        .map_err(|error| format!("second proposer failed: {error:?}"))?;
    let after_b = nodes[2].events_after(None)?;
    if b_markers.is_empty() || !b_markers.iter().all(|event| retains(&after_b, event)) {
        return Err("second proposer used the first proposer's evidence source".into());
    }
    Ok(())
}

#[tokio::test]
async fn native_controller_selects_evidence_by_authenticated_proposer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let nodes = ["a", "b", "c"]
        .map(|name| RedbJournal::open_node(directory.path().join(format!("{name}.redb"))));
    let [a, b, c] = nodes;
    let nodes = [a?, b?, c?];
    let transports = [
        IrohReplicator::bind_loopback(nodes[0].clone()).await?,
        IrohReplicator::bind_loopback(nodes[1].clone()).await?,
        IrohReplicator::bind_loopback(nodes[2].clone()).await?,
    ];
    for transport in &transports[..2] {
        transport.set_access_policy(Arc::new(ScopedHistoryPolicy::new(
            principal(&transports[2]).id,
            vec![authority_realm_scope(&realm())],
        )))?;
    }
    let keys = keys3();
    transports[2]
        .sessions()
        .set_authority_control(Some(Arc::new(controller_endpoint(
            &nodes,
            &transports,
            &keys,
        )?)))?;
    let outcome = check_proposer_routes(&nodes, &transports, &keys).await;
    for transport in transports {
        transport.shutdown().await?;
    }
    outcome
}

#[test]
fn evidence_bindings_reject_unknown_and_duplicate_callers() -> TestResult {
    let [key, _] = keys();
    let caller = Principal::node(PrincipalId::new("bound"));
    let endpoint = || {
        CertifiedAuthorityControlEndpoint::new(
            Node::in_memory(),
            anchor()?,
            key.clone(),
            vec![AuthorityControllerPrincipal::new(
                caller.clone(),
                controller_id(&key),
            )],
        )
    };
    if endpoint()?
        .with_scoped_evidence_endpoint(PrincipalId::new("unknown"), Arc::new(InvalidEvidence))
        .is_ok()
    {
        return Err("unknown caller acquired an evidence source".into());
    }
    if endpoint()?
        .with_scoped_evidence_endpoint(caller.id.clone(), Arc::new(InvalidEvidence))?
        .with_scoped_evidence_endpoint(caller.id, Arc::new(InvalidEvidence))
        .is_ok()
    {
        return Err("duplicate caller silently replaced its evidence source".into());
    }
    Ok(())
}
