use std::sync::atomic::{AtomicBool, Ordering};

use hyphae::Watchable as _;
use myko_authority::{IssueAuthorityGrant, RevokeAuthorityFact};
use myko_federation::{
    AuthorizationBlock, CommandClient, LiveCollectionHandle as _, LiveSubscription,
    LiveSubscriptionHandle as _, LiveSubscriptionState, NodeError, ReconnectPolicy,
    SubscriptionLiveness,
};

use super::*;

const ROOT: &str = "live-grants";
// Four concurrent certified streams share a coordinator. This is a debug-test
// completion budget, not an acceptable application latency target.
const PHASE_TIMEOUT: StdDuration = StdDuration::from_mins(2);
type States = [(&'static str, LiveSubscription<Vec<String>>); 4];

fn scope() -> ScopeId {
    ScopeId::for_item::<NativeRoot>(&NativeRootId::from(ROOT))
}

#[myko::myko_query(NativeRoot, item = NativeRoot)]
#[derive(PartialEq, Eq)]
pub struct GrantedRootsQuery {}

impl myko::query::QueryHandler for GrantedRootsQuery {
    fn scope_id(&self, _node_id: myko_federation::NodeId) -> Option<ScopeId> {
        Some(scope())
    }

    fn build_view(
        context: myko::query::QueryBuildArgs<Self>,
    ) -> Result<Option<impl myko::query::QueryBuildOutput>, String> {
        Ok(Some(myko::query::RetainedQuery::new(
            context.federated_items::<NativeRoot>()?,
        )))
    }
}

#[myko::myko_report_output]
pub struct GrantedLabels {
    labels: Vec<String>,
}

#[myko::myko_report(GrantedLabels, item = NativeRoot)]
pub struct GrantedRootsReport {}

impl myko::report::ReportHandler for GrantedRootsReport {
    type Output = GrantedLabels;

    fn scope_id(&self, _node_id: myko_federation::NodeId) -> Option<ScopeId> {
        Some(scope())
    }

    fn compute(
        &self,
        context: myko::report::ReportContext,
    ) -> Result<impl myko::report::ReportBuildOutput<Self::Output>, String> {
        Ok(myko::report::RetainedReport::new(
            context.federated_items::<NativeRoot>()?.map_value(|rows| {
                Arc::new(GrantedLabels {
                    labels: rows.values().map(|row| row.label.clone()).collect(),
                })
            }),
        ))
    }
}

#[myko::myko_view(NativeRoot, item = NativeRoot)]
pub struct GrantedRoots {}

impl myko::view::ViewHandler for GrantedRoots {
    fn scope_id(&self, _node_id: myko_federation::NodeId) -> Option<ScopeId> {
        Some(scope())
    }

    fn build_cell(
        context: myko::view::ViewBuildArgs<Self>,
    ) -> Result<impl myko::view::ViewBuildOutput<Item = NativeRoot>, String> {
        Ok(myko::view::RetainedView::new(
            context.federated_items::<NativeRoot>()?,
        ))
    }
}

fn read_grant(admin: &Principal, reader: &Principal, id: &str) -> Result<AuthorityGrant, String> {
    Ok(AuthorityGrant {
        id: AuthorityGrantId::new(id),
        realm_id: realm(),
        grantor: admin.clone(),
        grantee: reader.clone(),
        selection: ScopeSelection::Exact(scope()),
        permissions: vec![
            FederationPermission::ReadState,
            FederationPermission::Subscribe,
            FederationPermission::Write,
        ],
        operations: vec![
            AccessOperation::ReadItems,
            AccessOperation::FollowItems,
            AccessOperation::FollowHandler,
            AccessOperation::SubmitCommand,
        ],
        capabilities: Vec::new(),
        constraints: AuthorityConstraints::default(),
        obligations: Vec::new(),
        valid_from: Utc::now()
            .checked_sub_signed(Duration::seconds(1))
            .ok_or("grant timestamp underflow")?,
        expires_at: None,
        max_uses: None,
    })
}

fn bootstrap_reader(a: &myko_node::Node, reader: &Principal) -> Result<Principal, Box<dyn Error>> {
    let policy = Arc::new(AuthorityPolicy::new(a.application().clone(), realm()));
    a.node().set_command_access_policy(policy.clone())?;
    let admin = Principal::node(PrincipalId::new("admin"));
    policy.bootstrap(admin.clone())?;
    policy.issue_grant(
        admin.clone(),
        AuthorityPresentation::direct(admin.clone()),
        read_grant(&admin, reader, "initial-reader")?,
    )?;
    Ok(admin)
}

fn install_pair(a: &mut myko_node::Node, b: &mut myko_node::Node) -> TestResult {
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
    let scopes = vec![authority_realm_scope(&realm()), scope()];
    let a_policy = Arc::new(AssemblyPolicy(ScopedHistoryPolicy::new(
        endpoint_principal_id(b.address().id),
        scopes.clone(),
    )));
    let b_policy = Arc::new(AssemblyPolicy(ScopedHistoryPolicy::new(
        endpoint_principal_id(a.address().id),
        scopes,
    )));
    a.install_certified_authority(&config, a_key, a_policy, |_| {})?;
    b.install_certified_authority(&config, b_key, b_policy, |_| {})?;
    Ok(())
}

#[tokio::test]
async fn certified_grant_revocation_recovers_all_owned_outputs() -> TestResult {
    if std::env::var_os("MYKO_AUTHORITY_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()?)
            .try_init()
            .map_err(|error| format!("could not install test tracing: {error}"))?;
    }
    let directory = tempfile::tempdir()?;
    let mut a = open_node(&directory.path().join("a")).await?;
    let mut b = open_node(&directory.path().join("b")).await?;
    let client = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let stranger = IrohReplicator::bind_loopback(Node::in_memory()).await?;
    let reader = Principal::node(endpoint_principal_id(client.address().id));
    let a_startup = a.node().hold_startup();
    let b_startup = b.node().hold_startup();
    let admin = bootstrap_reader(&a, &reader)?;
    install_pair(&mut a, &mut b)?;
    a_startup.ready();
    b_startup.ready();
    let outcome = exercise_grant_lifecycle(&a, &client, &stranger, &admin, &reader).await;
    let shutdown_started = std::time::Instant::now();
    stranger.shutdown().await?;
    client.shutdown().await?;
    a.shutdown().await?;
    b.shutdown().await?;
    eprintln!(
        "native grant lifecycle cleanup: {:?}",
        shutdown_started.elapsed()
    );
    let committed = outcome?;
    assert_reopened_grants(&directory.path().join("b"), committed).await
}

async fn exercise_grant_lifecycle(
    server: &myko_node::Node,
    client: &IrohReplicator,
    stranger: &IrohReplicator,
    admin: &Principal,
    reader: &Principal,
) -> Result<[CommandId; 2], Box<dyn Error>> {
    let initial = server.application().submit_authenticated_command(
        reader.id.clone(),
        &NativeCommand {
            root: NativeRootId::from(ROOT),
            label: "protected".to_owned(),
        },
    )?;
    wait_command(server.node(), initial.request.id).await?;
    let retry = ReconnectPolicy::new(StdDuration::from_millis(20), StdDuration::from_millis(50))?;
    let items = tokio::time::timeout(
        PHASE_TIMEOUT,
        client
            .item_client(server.address())
            .with_reconnect_policy(retry)
            .watch_serving_items_reactive(scope(), GetAllNativeRoots {}),
    )
    .await??;
    let handlers = client
        .handler_connector(server.address())
        .with_reconnect_policy(retry)
        .client();
    let query = handlers.follow_query_reactive(
        Some(server.node().node_id()),
        scope(),
        &GrantedRootsQuery {},
    )?;
    let report = handlers.follow_report_reactive(&GrantedRootsReport {})?;
    let view = handlers.follow_view_reactive(&GrantedRoots {})?;
    let states = [
        (
            "items",
            items
                .live()
                .map_value(|rows| rows.iter().map(|row| row.label.clone()).collect()),
        ),
        (
            "query",
            query
                .live_collection()
                .as_subscription()
                .map_value(|rows| rows.iter().map(|row| row.label.clone()).collect()),
        ),
        (
            "report",
            report
                .live_subscription()
                .map_value(|output| output.labels.clone()),
        ),
        (
            "view",
            view.live_collection()
                .as_subscription()
                .map_value(|rows| rows.iter().map(|row| row.label.clone()).collect()),
        ),
    ];
    let (_guards, receiver) = observe(&states);
    wait_outputs(&states, &receiver, is_protected).await?;
    assert_stranger_denied(server, stranger).await?;
    let revoked = server.application().submit_authenticated_command(
        admin.id.clone(),
        &RevokeAuthorityFact {
            realm_id: realm(),
            kind: RevocationKind::Grant,
            id: "initial-reader".to_owned(),
            at: Utc::now(),
        },
    )?;
    wait_command(server.node(), revoked.request.id).await?;
    wait_outputs(&states, &receiver, is_denied).await?;
    if !query.live_collection().rows().snapshot().is_empty()
        || !view.live_collection().rows().snapshot().is_empty()
        || report.live_subscription().current().value.is_some()
        || items.live().current().value.is_some()
    {
        return Err("revoked subscription retained protected output".into());
    }
    for (name, live) in &states {
        assert_denied_command(name, live, &client.command_client(server.address())).await;
    }
    let restored = server.application().submit_authenticated_command(
        admin.id.clone(),
        &IssueAuthorityGrant {
            realm_id: realm(),
            grant: read_grant(admin, reader, "replacement-reader")?,
        },
    )?;
    wait_command(server.node(), restored.request.id).await?;
    wait_outputs(&states, &receiver, is_protected).await?;
    assert_stranger_denied(server, stranger).await?;
    if server.certified_authority_failure().is_some() {
        return Err("certified authority worker stopped".into());
    }
    drop((items, query, report, view));
    Ok([revoked.request.id, restored.request.id])
}

async fn assert_reopened_grants(path: &std::path::Path, commands: [CommandId; 2]) -> TestResult {
    let reopened = open_node(path).await?;
    let outcome = (|| {
        let history = AuthorityHistory::replay(reopened.node(), anchor()?)?;
        if history.retained_head()? == anchor()?.genesis() {
            return Err("second controller lost certified authority history on reopen".into());
        }
        for id in commands {
            if reopened.node().command(id)?.is_none_or(|command| {
                !command.state.is_committed()
                    || command.request.scope_id != authority_realm_scope(&realm())
                    || command.request.service_id
                        != ServiceId::new(myko_authority::AuthorityService::SERVICE_ID)
            }) {
                return Err(
                    format!("second controller lost authority command {id} on reopen").into(),
                );
            }
        }
        Ok::<_, Box<dyn Error>>(())
    })()
    .map_err(|error| error.to_string());
    reopened.shutdown().await?;
    outcome?;
    Ok(())
}

async fn assert_stranger_denied(server: &myko_node::Node, stranger: &IrohReplicator) -> TestResult {
    let started = std::time::Instant::now();
    let denied = tokio::time::timeout(
        PHASE_TIMEOUT,
        stranger
            .handler_connector(server.address())
            .client()
            .follow_view(&GrantedRoots {}),
    )
    .await?;
    if !matches!(denied, Err(myko::client::HandlerClientError::Authorization(decision))
        if matches!(decision.as_ref(), AuthorizationDecision::Deny(_)))
    {
        return Err("ungranted transport principal was not denied".into());
    }
    eprintln!("native ungranted client denial: {:?}", started.elapsed());
    Ok(())
}

async fn wait_command(node: &Node, id: CommandId) -> TestResult {
    let started = std::time::Instant::now();
    let mut events = node.subscribe_from_now()?;
    tokio::time::timeout(PHASE_TIMEOUT, async {
        while !node
            .command(id)?
            .is_some_and(|command| command.state.is_committed())
        {
            events.recv_async().await?;
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await
    .map_err(|_| format!("command {id} did not commit: {:?}", node.command(id)))??;
    eprintln!(
        "native command {id} committed after {:?}",
        started.elapsed()
    );
    Ok(())
}

fn observe(states: &States) -> (Vec<hyphae::SubscriptionGuard>, flume::Receiver<()>) {
    let (updates, receiver) = flume::unbounded();
    let guards = states
        .iter()
        .map(|(_, live)| {
            let updates = updates.clone();
            live.state().subscribe(move |_| {
                let _ignored = updates.send(());
            })
        })
        .collect();
    (guards, receiver)
}

async fn wait_outputs(
    states: &States,
    updates: &flume::Receiver<()>,
    predicate: impl Fn(&LiveSubscriptionState<Vec<String>>) -> bool + Send + Sync,
) -> TestResult {
    let started = std::time::Instant::now();
    tokio::time::timeout(PHASE_TIMEOUT, async {
        while !states.iter().all(|(_, live)| predicate(&live.current())) {
            updates.recv_async().await?;
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await
    .map_err(|_| {
        format!(
            "outputs did not reach expected state: {:?}",
            states
                .iter()
                .map(|(name, live)| (name, live.current()))
                .collect::<Vec<_>>()
        )
    })??;
    for (name, live) in states {
        eprintln!(
            "native {name} reached {:?} after {:?}",
            live.current().liveness,
            started.elapsed()
        );
    }
    Ok(())
}

fn is_protected(state: &LiveSubscriptionState<Vec<String>>) -> bool {
    state.liveness == SubscriptionLiveness::Current
        && state.through.is_some()
        && state.value.as_deref() == Some(["protected".to_owned()].as_slice())
}

const fn is_denied(state: &LiveSubscriptionState<Vec<String>>) -> bool {
    matches!(
        state.liveness,
        SubscriptionLiveness::AuthorizationBlocked {
            block: AuthorizationBlock::Denied(_)
        }
    ) && state.value.is_none()
        && state.through.is_none()
}

async fn assert_denied_command(
    name: &str,
    live: &LiveSubscription<Vec<String>>,
    client: &myko_iroh::IrohCommandClient,
) {
    let built = AtomicBool::new(false);
    let result = client
        .submit_from(live, |_| {
            built.store(true, Ordering::SeqCst);
            NativeCommand {
                root: NativeRootId::from(ROOT),
                label: "must-not-run".to_owned(),
            }
        })
        .await;
    assert!(
        matches!(
            result,
            Err(myko_iroh::IrohReplicationError::Ingest(
                NodeError::CommandDependencyNotCurrent(
                    SubscriptionLiveness::AuthorizationBlocked { .. }
                )
            ))
        ),
        "{name} did not reject a denied command dependency: {result:?}"
    );
    assert!(
        !built.load(Ordering::SeqCst),
        "{name} invoked the denied command builder"
    );
}
