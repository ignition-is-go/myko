use std::{error::Error, path::Path, sync::Arc};

use ed25519_dalek::SigningKey;
use myko::server::{
    AuthorityControlEndpoint as _, FederatedSession, ScopedRetainedEvidenceEndpoint as _,
};
use myko_authority::certified::{
    AuthorityAnchor, AuthorityControllerPrincipal, CertifiedAuthorityControlEndpoint,
};
use myko_federation::{
    AuthorityPresentation, AuthorityRealmId, AuthorityUnavailable, AuthorizationFailure,
    DenyAllAccessPolicy, FederationPermission, Node, Principal, PrincipalId, ScopeGrant,
    ScopeGrantCoverage, ScopeGrantPolicy, ScopeId,
    control_quorum::{ControlBallot, ControlEpochId, ControlHead, ControllerId},
};
use myko_iroh::{IrohReplicator, IrohScopedEvidenceEndpoint, endpoint_principal_id};
use myko_local::{LocalCommandClient, LocalNodeServer, LocalPeerError};
use myko_redb::RedbJournal;

fn controller() -> ControllerId {
    ControllerId(SigningKey::from_bytes(&[1; 32]).verifying_key().to_bytes())
}

fn caller() -> Principal {
    Principal::node(PrincipalId::new("controller:a"))
}

fn anchor() -> Result<AuthorityAnchor, String> {
    AuthorityAnchor::new(
        AuthorityRealmId::new("endpoint-test"),
        ControlEpochId([8; 32]),
        ControlHead([9; 32]),
        vec![controller()],
    )
}

async fn serve(
    node: &Node,
    socket: &Path,
    authenticated: Principal,
) -> Result<LocalNodeServer, Box<dyn Error>> {
    let endpoint = CertifiedAuthorityControlEndpoint::new(
        node.clone(),
        anchor()?,
        SigningKey::from_bytes(&[1; 32]),
        vec![AuthorityControllerPrincipal::new(caller(), controller())],
    )?;
    let sessions = FederatedSession::new(node.clone(), Arc::new(DenyAllAccessPolicy));
    sessions.set_authority_control(Some(Arc::new(endpoint)))?;
    Ok(LocalNodeServer::spawn_sessions_authenticated(socket, sessions, authenticated).await?)
}

#[tokio::test]
async fn unauthorized_socket_caller_cannot_write_a_controller_promise() -> Result<(), Box<dyn Error>>
{
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("controller.redb"))?;
    let socket = directory.path().join("controller.sock");
    let server = serve(
        &node,
        &socket,
        Principal::node(PrincipalId::new("intruder")),
    )
    .await?;
    let result = LocalCommandClient::new(&socket)
        .prepare_control(
            anchor()?.genesis(),
            ControlBallot {
                counter: 1,
                proposer: controller(),
            },
        )
        .await;
    if !matches!(result, Err(LocalPeerError::Authorization(_))) {
        return Err(format!("unauthorized controller request was not denied: {result:?}").into());
    }
    if !node.events_after(None)?.is_empty() {
        return Err("unauthorized caller wrote controller history".into());
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn authenticated_socket_promise_survives_reopen_and_rejects_impersonation()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("controller.redb");
    let retained = {
        let node = RedbJournal::open_node(&path)?;
        let socket = directory.path().join("controller.sock");
        let server = serve(&node, &socket, caller()).await?;
        let client = LocalCommandClient::new(&socket);
        let result = client
            .prepare_control(
                anchor()?.genesis(),
                ControlBallot {
                    counter: 1,
                    proposer: ControllerId(
                        SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes(),
                    ),
                },
            )
            .await;
        if !matches!(result, Err(LocalPeerError::Authorization(_))) {
            return Err(format!("caller impersonated another proposer: {result:?}").into());
        }
        if !node.events_after(None)?.is_empty() {
            return Err("mismatched proposer wrote controller history".into());
        }
        let vote = client
            .prepare_control(
                anchor()?.genesis(),
                ControlBallot {
                    counter: 1,
                    proposer: controller(),
                },
            )
            .await?;
        let retained = node.events_after(None)?;
        if retained.len() != 1 {
            return Err(format!("expected one durable promise, got {}", retained.len()).into());
        }
        server.shutdown().await?;
        (retained, vote)
    };
    let reopened = RedbJournal::open_node(&path)?;
    if reopened.events_after(None)? != retained.0 {
        return Err("socket-issued promise did not survive journal reopen".into());
    }
    let socket = directory.path().join("reopened.sock");
    let server = serve(&reopened, &socket, caller()).await?;
    let vote = LocalCommandClient::new(&socket)
        .prepare_control(
            anchor()?.genesis(),
            ControlBallot {
                counter: 1,
                proposer: controller(),
            },
        )
        .await?;
    if vote != retained.1 || reopened.events_after(None)? != retained.0 {
        return Err("exact promise retry changed durable evidence".into());
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn native_controller_adapter_preserves_identity_and_typed_unavailability()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let node = RedbJournal::open_node(directory.path().join("native.redb"))?;
    let server = IrohReplicator::bind_loopback(node.clone()).await?;
    let observer = Node::in_memory();
    let sender = IrohReplicator::bind_loopback(observer.clone()).await?;
    let principal = Principal::node(endpoint_principal_id(sender.address().id));
    let presentation = AuthorityPresentation::direct(principal.clone());
    let client = sender.command_client(server.address());
    let ballot = ControlBallot {
        counter: 1,
        proposer: controller(),
    };
    let result = client
        .prepare(&principal.id, &presentation, anchor()?.genesis(), ballot)
        .await;
    if !matches!(
        result,
        Err(AuthorizationFailure::Unavailable(
            AuthorityUnavailable::CoordinationUnavailable
        ))
    ) {
        return Err(
            format!("missing native controller did not report unavailable: {result:?}").into(),
        );
    }
    server.sessions().set_authority_control(Some(Arc::new(
        CertifiedAuthorityControlEndpoint::new(
            node.clone(),
            anchor()?,
            SigningKey::from_bytes(&[1; 32]),
            vec![AuthorityControllerPrincipal::new(
                principal.clone(),
                controller(),
            )],
        )?,
    )))?;
    let spoofed = caller();
    let result = client
        .prepare(
            &spoofed.id,
            &AuthorityPresentation::direct(spoofed.clone()),
            anchor()?.genesis(),
            ballot,
        )
        .await;
    if !matches!(result, Err(AuthorizationFailure::Deny(_))) {
        return Err(format!("native adapter allowed caller impersonation: {result:?}").into());
    }
    if !node.events_after(None)?.is_empty() {
        return Err("unavailable or impersonated request wrote a controller promise".into());
    }
    let vote = client
        .prepare(&principal.id, &presentation, anchor()?.genesis(), ballot)
        .await;
    if let Err(error) = vote {
        return Err(format!("native authenticated controller prepare failed: {error:?}").into());
    }
    if node.events_after(None)?.len() != 1 {
        return Err("native controller did not persist exactly one promise".into());
    }
    assert_scoped_evidence_refresh(&server, &sender, &node, &observer).await?;
    drop(client);
    sender.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

async fn assert_scoped_evidence_refresh(
    server: &IrohReplicator,
    sender: &IrohReplicator,
    source: &Node,
    observer: &Node,
) -> Result<(), Box<dyn Error>> {
    let evidence = IrohScopedEvidenceEndpoint::new(sender.clone(), server.address());
    let scope = myko_authority::authority_realm_scope(anchor()?.realm_id());
    let requested = [scope.clone()];
    if evidence.refresh_scopes(&requested).await.is_ok() {
        return Err("controller access implicitly granted history replication".into());
    }
    if !observer.events_after(None)?.is_empty() {
        return Err("denied evidence refresh copied history".into());
    }
    server.set_access_policy(Arc::new(ScopeGrantPolicy::new(vec![ScopeGrant {
        scope_id: scope,
        coverage: ScopeGrantCoverage::Exact,
        grantee: endpoint_principal_id(sender.address().id),
        permissions: vec![FederationPermission::ReadHistory],
    }])))?;
    evidence.refresh_scopes(&requested).await?;
    let retained = observer.events_after(None)?;
    let original = source.events_after(None)?;
    if retained.len() != original.len()
        || retained.iter().zip(&original).any(|(copy, accepted)| {
            copy.origin != accepted.origin
                || copy.recorded_at != accepted.recorded_at
                || copy.event != accepted.event
        })
    {
        return Err("authorized native refresh did not retain exact control evidence".into());
    }
    let unrelated = [ScopeId::new("unrelated-private-scope")];
    if evidence.refresh_scopes(&unrelated).await.is_ok() {
        return Err("evidence refresh escaped its authorized scope".into());
    }
    evidence.refresh_scopes(&requested).await?;
    if observer.events_after(None)? != retained {
        return Err("repeated evidence refresh changed accepted history".into());
    }
    Ok(())
}
