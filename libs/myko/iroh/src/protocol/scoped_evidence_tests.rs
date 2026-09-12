#![allow(clippy::panic_in_result_fn)]

use super::*;
use myko::server::ScopedRetainedEvidenceEndpoint as _;
use myko_federation::{AllowAllAccessPolicy, CommandRequest, ServiceId};
use tokio::sync::Mutex;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Clone)]
struct ObservedProtocol {
    sessions: Arc<Mutex<FederatedSession>>,
    batches: flume::Sender<ScopedReplicationBatch>,
    held_scope: Arc<Mutex<Option<ScopeId>>>,
    opened: flume::Sender<ScopeId>,
    release: Arc<tokio::sync::Semaphore>,
}

impl ProtocolHandler for ObservedProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut receive) = connection.accept_bi().await?;
        let request = read_request(&mut receive).await?;
        if let ReplicationRequest::PullScope { scope_id, .. } = &request.request {
            let held = self.held_scope.lock().await.as_ref() == Some(scope_id);
            if held {
                self.opened
                    .send(scope_id.clone())
                    .map_err(AcceptError::from_err)?;
                self.release
                    .acquire()
                    .await
                    .map_err(AcceptError::from_err)?
                    .forget();
            }
        }
        let sessions = self.sessions.lock().await.clone();
        let mut frames = sessions
            .open(endpoint_principal_id(connection.remote_id()), request)
            .await;
        while let Some(frame) = frames.recv().await {
            if let ReplicationFrame::ScopedBatch { batch } = &frame {
                self.batches
                    .send((**batch).clone())
                    .map_err(AcceptError::from_err)?;
            }
            write_frame(&mut send, &frame)
                .await
                .map_err(AcceptError::from_err)?;
        }
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

struct Peer {
    router: Router,
    sessions: Arc<Mutex<FederatedSession>>,
    batches: flume::Receiver<ScopedReplicationBatch>,
    held_scope: Arc<Mutex<Option<ScopeId>>>,
    opened: flume::Receiver<ScopeId>,
    release: Arc<tokio::sync::Semaphore>,
}

impl Peer {
    async fn open(node: Node) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr_with_opts(
                "127.0.0.1:0",
                BindOpts::default()
                    .set_prefix_len(8)
                    .set_is_default_route(false),
            )?
            .bind()
            .await?;
        let sessions = Arc::new(Mutex::new(FederatedSession::new(
            node,
            Arc::new(AllowAllAccessPolicy),
        )));
        let (sender, batches) = flume::unbounded();
        let held_scope = Arc::new(Mutex::new(None));
        let (opened_sender, opened) = flume::unbounded();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let router = Router::builder(endpoint)
            .accept(
                MYKO_REPLICATION_ALPN,
                ObservedProtocol {
                    sessions: sessions.clone(),
                    batches: sender,
                    held_scope: held_scope.clone(),
                    opened: opened_sender,
                    release: release.clone(),
                },
            )
            .spawn();
        Ok(Self {
            router,
            sessions,
            batches,
            held_scope,
            opened,
            release,
        })
    }
}

fn admit(node: &Node, scope: &ScopeId) -> Result<(), myko_federation::NodeError> {
    node.admit(CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("evidence-test"),
        scope_id: scope.clone(),
        principal_id: PrincipalId::new("test"),
        authority: AuthorityPresentation::direct_node(PrincipalId::new("test")),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "record".to_owned(),
        payload: Vec::new(),
    })?;
    Ok(())
}

#[tokio::test]
async fn cloned_evidence_refresh_transfers_only_new_exact_scope_history() -> TestResult {
    let source = Node::in_memory();
    let a = ScopeId::new("a");
    let b = ScopeId::new("b");
    admit(&source, &a)?;
    admit(&source, &b)?;
    let peer = Peer::open(source.clone()).await?;
    let sink = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let evidence = IrohScopedEvidenceEndpoint::new(sink.clone(), peer.router.endpoint().addr());
    evidence.refresh_scopes(std::slice::from_ref(&a)).await?;
    let first = peer.batches.recv_async().await?;
    assert_eq!(first.after, None);
    assert_eq!(first.events.len(), 1);
    evidence
        .clone()
        .refresh_scopes(&[a.clone(), b.clone()])
        .await?;
    let repeat = peer.batches.recv_async().await?;
    assert_eq!(
        repeat.after, first.through,
        "clone must resume the retained checkpoint"
    );
    assert!(
        repeat.events.is_empty(),
        "unchanged scope must not replay history"
    );
    let other = peer.batches.recv_async().await?;
    assert_eq!(other.scope_id, b);
    assert_eq!(other.after, None, "a different scope starts independently");
    assert_eq!(other.events.len(), 1);
    admit(&source, &b)?;
    let left = evidence.clone();
    let right = evidence.clone();
    let scopes = [a];
    let (left, right) = tokio::join!(left.refresh_scopes(&scopes), right.refresh_scopes(&scopes));
    left?;
    right?;
    let advanced = peer.batches.recv_async().await?;
    let concurrent = peer.batches.recv_async().await?;
    assert_eq!(advanced.after, first.through);
    assert_ne!(advanced.through, first.through);
    assert_eq!(concurrent.after, advanced.through);
    assert!(advanced.events.is_empty());
    assert!(concurrent.events.is_empty());
    assert_eq!(sink.node.events_after(None)?.len(), 2);
    sink.shutdown().await?;
    peer.router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn evidence_refresh_rechecks_permission_without_advancing_on_denial() -> TestResult {
    let source = Node::in_memory();
    let scope = ScopeId::new("protected");
    admit(&source, &scope)?;
    let peer = Peer::open(source.clone()).await?;
    let sink = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let evidence = IrohScopedEvidenceEndpoint::new(sink.clone(), peer.router.endpoint().addr());
    let scopes = [scope.clone()];
    evidence.refresh_scopes(&scopes).await?;
    let first = peer.batches.recv_async().await?;
    admit(&source, &scope)?;
    peer.sessions
        .lock()
        .await
        .set_access_policy(Arc::new(DenyAllAccessPolicy))?;
    assert!(evidence.refresh_scopes(&scopes).await.is_err());
    assert_eq!(sink.node.events_after(None)?.len(), 1);
    peer.sessions
        .lock()
        .await
        .set_access_policy(Arc::new(AllowAllAccessPolicy))?;
    evidence.refresh_scopes(&scopes).await?;
    let restored = peer.batches.recv_async().await?;
    assert_eq!(restored.after, first.through);
    assert_eq!(restored.events.len(), 1);
    assert_eq!(sink.node.events_after(None)?.len(), 2);
    sink.shutdown().await?;
    peer.router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn evidence_refresh_resets_checkpoint_when_peer_replaces_source_history() -> TestResult {
    let first_source = Node::in_memory();
    let scope = ScopeId::new("replaced");
    for _ in 0..3 {
        admit(&first_source, &scope)?;
    }
    let peer = Peer::open(first_source).await?;
    let sink = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let evidence = IrohScopedEvidenceEndpoint::new(sink.clone(), peer.router.endpoint().addr());
    let scopes = [scope.clone()];
    evidence.refresh_scopes(&scopes).await?;
    let first = peer.batches.recv_async().await?;
    let replacement = Node::in_memory();
    admit(&replacement, &scope)?;
    *peer.sessions.lock().await =
        FederatedSession::new(replacement, Arc::new(AllowAllAccessPolicy));
    evidence.refresh_scopes(&scopes).await?;
    let stale = peer.batches.recv_async().await?;
    assert_eq!(stale.after, first.through);
    assert_ne!(stale.source_node, first.source_node);
    let replay = peer.batches.recv_async().await?;
    assert_eq!(replay.after, None);
    assert_eq!(replay.events.len(), 1);
    evidence.refresh_scopes(&scopes).await?;
    let resumed = peer.batches.recv_async().await?;
    assert_eq!(resumed.after, replay.through);
    assert!(resumed.events.is_empty());
    assert_eq!(sink.node.events_after(None)?.len(), 4);
    sink.shutdown().await?;
    peer.router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn evidence_refresh_bounds_scope_lock_wait_and_cancellation() -> TestResult {
    let source = Node::in_memory();
    let a = ScopeId::new("blocked");
    let b = ScopeId::new("independent");
    admit(&source, &a)?;
    admit(&source, &b)?;
    let peer = Peer::open(source).await?;
    let sink = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let evidence = IrohScopedEvidenceEndpoint::new(sink.clone(), peer.router.endpoint().addr());
    *peer.held_scope.lock().await = Some(a.clone());
    let first = evidence.clone();
    let first_scopes = [a.clone()];
    let first = tokio::spawn(async move { first.refresh_scopes(&first_scopes).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer.opened.recv_async()).await??,
        a
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        evidence.refresh_scopes(std::slice::from_ref(&b)),
    )
    .await??;
    assert_eq!(peer.batches.recv_async().await?.scope_id, b);
    let impatient = evidence
        .clone()
        .with_request_timeout(Duration::from_millis(50));
    let waiting = impatient.refresh_scopes(std::slice::from_ref(&a)).await;
    assert!(matches!(
        waiting,
        Err(myko::server::RetainedEvidenceError::Unavailable(
            myko_federation::AuthorityUnavailable::HistoryUnavailable
        ))
    ));
    assert!(
        peer.opened.try_recv().is_err(),
        "a waiting clone must not start a duplicate transfer"
    );
    assert_eq!(sink.node.events_after(None)?.len(), 1);
    first.abort();
    assert!(matches!(first.await, Err(error) if error.is_cancelled()));
    *peer.held_scope.lock().await = None;
    peer.release.add_permits(1);
    evidence.refresh_scopes(std::slice::from_ref(&a)).await?;
    let retries = peer.batches.try_iter().collect::<Vec<_>>();
    assert!(!retries.is_empty());
    for retry in retries {
        assert_eq!(retry.scope_id, a);
        assert_eq!(
            retry.after, None,
            "cancelled refresh did not retain a checkpoint"
        );
        assert_eq!(retry.events.len(), 1);
    }
    assert_eq!(sink.node.events_after(None)?.len(), 2);
    sink.shutdown().await?;
    peer.router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn evidence_refresh_keeps_last_checkpoint_after_partial_ingest_failure() -> TestResult {
    let source = Node::in_memory();
    let scope = ScopeId::new("conflict");
    admit(&source, &scope)?;
    let peer = Peer::open(source.clone()).await?;
    let sink = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let evidence = IrohScopedEvidenceEndpoint::new(sink.clone(), peer.router.endpoint().addr());
    evidence
        .refresh_scopes(std::slice::from_ref(&scope))
        .await?;
    let first = peer.batches.recv_async().await?;
    admit(&source, &scope)?;
    admit(&source, &scope)?;
    let mut conflicting = source
        .events_after(None)?
        .pop()
        .ok_or("missing test event")?;
    conflicting.recorded_at = conflicting
        .recorded_at
        .checked_add_signed(chrono::Duration::seconds(1))
        .ok_or("test timestamp overflow")?;
    sink.node.ingest(conflicting)?;
    let scopes = [scope, ScopeId::new("not-attempted")];
    for _ in 0..2 {
        assert!(evidence.refresh_scopes(&scopes).await.is_err());
        let failed = peer.batches.recv_async().await?;
        assert_eq!(
            failed.after, first.through,
            "failed ingestion must retain the prior cursor"
        );
        assert_eq!(failed.events.len(), 2);
        assert_eq!(
            sink.node.events_after(None)?.len(),
            3,
            "valid prefix remains retained"
        );
        assert!(
            peer.batches.try_recv().is_err(),
            "later scopes must not run after an error"
        );
    }
    sink.shutdown().await?;
    peer.router.shutdown().await?;
    Ok(())
}
