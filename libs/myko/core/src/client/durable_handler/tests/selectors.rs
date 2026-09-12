use super::*;

type TestOpen = Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError>;

#[crate::myko_report(u64)]
struct TargetBoundReport;

impl crate::report::ReportHandler for TargetBoundReport {
    type Output = u64;

    fn source_node(&self, local_node: NodeId) -> Option<NodeId> {
        Some(local_node)
    }

    fn scope_id(&self, local_node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new(format!("target:{local_node}")))
    }
}

#[crate::myko_service(TargetBoundRow)]
pub struct TargetBoundService;

#[crate::myko_item(service = TargetBoundService, scope_root)]
pub struct TargetBoundRow {
    value: u64,
}

#[crate::myko_view(TargetBoundRow)]
struct TargetBoundView;

impl crate::view::ViewHandler for TargetBoundView {
    fn source_node(&self, local_node: NodeId) -> Option<NodeId> {
        Some(local_node)
    }

    fn scope_id(&self, local_node: NodeId) -> Option<ScopeId> {
        Some(ScopeId::new(format!("target:{local_node}")))
    }

    fn build_cell(
        _context: crate::view::ViewBuildArgs<Self>,
    ) -> Result<impl crate::view::ViewBuildOutput<Item = Self::Item>, String> {
        Err::<crate::view::RetainedView<TargetBoundRow>, _>(
            "test handler is never registered".to_owned(),
        )
    }
}

struct SelectorConnector {
    _serial: crate::test_util::SchedulerTestPermit,
    targets: Mutex<VecDeque<NodeId>>,
    opens: Mutex<VecDeque<TestOpen>>,
    requests: Mutex<Vec<HandlerRequest>>,
}

impl SelectorConnector {
    fn requests(&self) -> Vec<HandlerRequest> {
        self.requests.lock().expect("test request lock").clone()
    }
}

#[async_trait::async_trait]
impl HandlerConnector for SelectorConnector {
    async fn target_node(&self) -> Result<NodeId, HandlerClientError> {
        self.targets
            .lock()
            .expect("test target lock")
            .pop_front()
            .ok_or_else(|| HandlerClientError::Transport("no target available".to_owned()))
    }

    async fn connect(
        &self,
        request: HandlerRequest,
    ) -> Result<(HandlerFrame, Box<dyn HandlerConnection>), HandlerClientError> {
        self.requests
            .lock()
            .expect("test request lock")
            .push(request);
        self.opens
            .lock()
            .expect("test open lock")
            .pop_front()
            .unwrap_or_else(|| Err(HandlerClientError::Transport("no open queued".to_owned())))
    }

    fn at(&self, _destination: NodeId) -> Arc<dyn HandlerConnector> {
        panic!("test connector does not route")
    }

    fn reconnect_policy(&self) -> ReconnectPolicy {
        ReconnectPolicy::new(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        )
        .expect("valid immediate retry policy")
    }
}

fn scalar_state(value: u64) -> HandlerFrame {
    HandlerFrame::State {
        revision: HandlerStreamRevision {
            epoch: 1,
            sequence: 0,
        },
        state: ErasedHandlerState {
            value: Some(serde_json::json!(value)),
            through: None,
            liveness: SubscriptionLiveness::Current,
            row_keys: None,
        },
    }
}

fn empty_view_state(epoch: u64) -> HandlerFrame {
    HandlerFrame::State {
        revision: HandlerStreamRevision { epoch, sequence: 0 },
        state: ErasedHandlerState {
            value: Some(serde_json::json!([])),
            through: None,
            liveness: SubscriptionLiveness::Current,
            row_keys: Some(Vec::new()),
        },
    }
}

fn target_bound_connector(
    first_target: NodeId,
    next_target: NodeId,
    opens: Vec<TestOpen>,
    serial: crate::test_util::SchedulerTestPermit,
) -> Arc<SelectorConnector> {
    Arc::new(SelectorConnector {
        _serial: serial,
        targets: Mutex::new(VecDeque::from([first_target, next_target])),
        opens: Mutex::new(opens.into()),
        requests: Mutex::new(Vec::new()),
    })
}

async fn wait_for_requests(connector: &SelectorConnector, count: usize) -> Vec<HandlerRequest> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let requests = connector.requests();
            if requests.len() >= count {
                return requests;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler opens should complete")
}

#[tokio::test]
async fn reactive_report_retains_the_first_resolved_target_across_initial_and_reconnect_opens() {
    let serial = crate::test_util::scheduler_test_serial();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let first_target = NodeId::new();
        let next_target = NodeId::new();
        let (sender, receiver) = flume::unbounded();
        let (_next_sender, next_receiver) = flume::unbounded();
        let connector = target_bound_connector(
            first_target,
            next_target,
            vec![
                Err(HandlerClientError::Transport(
                    "first executor unavailable".to_owned(),
                )),
                Ok((scalar_state(1), Box::new(ChannelConnection(receiver)))),
                Ok((scalar_state(2), Box::new(ChannelConnection(next_receiver)))),
            ],
            serial,
        );
        let client = MykoClient::with_handler_connector(connector.clone());
        let owner = client
            .follow_report_reactive(&TargetBoundReport)
            .expect("create reactive report");
        let mut publications = owner.live_subscription().watch_publications();
        let requests = wait_for_requests(&connector, 2).await;
        let [original, retry, ..] = requests.as_slice() else {
            panic!("expected two initial report open attempts");
        };
        assert_eq!(original, retry);
        assert_eq!(original.source_node, Some(first_target));
        assert_eq!(
            original.scope_id,
            Some(ScopeId::new(format!("target:{first_target}")))
        );
        loop {
            if publications
                .recv_async()
                .await
                .expect("report snapshot")
                .state
                .liveness
                == SubscriptionLiveness::Current
            {
                break;
            }
        }
        drop(sender);
        let requests = wait_for_requests(&connector, 3).await;
        assert_eq!(Some(original), requests.get(2));
    })
    .await
    .expect("request identity survives initial failure and reconnect");
}

#[tokio::test]
async fn reactive_view_retains_the_first_resolved_target_across_initial_and_reconnect_opens() {
    let serial = crate::test_util::scheduler_test_serial();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let first_target = NodeId::new();
        let next_target = NodeId::new();
        let (sender, receiver) = flume::unbounded();
        let (_next_sender, next_receiver) = flume::unbounded();
        let connector = target_bound_connector(
            first_target,
            next_target,
            vec![
                Err(HandlerClientError::Transport(
                    "first executor unavailable".to_owned(),
                )),
                Ok((empty_view_state(1), Box::new(ChannelConnection(receiver)))),
                Ok((
                    empty_view_state(2),
                    Box::new(ChannelConnection(next_receiver)),
                )),
            ],
            serial,
        );
        let client = MykoClient::with_handler_connector(connector.clone());
        let owner = client
            .follow_view_reactive(&TargetBoundView)
            .expect("create reactive view");
        let revisions = owner.live_collection().subscribe_revisions();
        let requests = wait_for_requests(&connector, 2).await;
        let [original, retry, ..] = requests.as_slice() else {
            panic!("expected two initial view open attempts");
        };
        assert_eq!(original, retry);
        assert_eq!(original.source_node, Some(first_target));
        assert_eq!(
            original.scope_id,
            Some(ScopeId::new(format!("target:{first_target}")))
        );
        loop {
            if revisions
                .receiver()
                .recv_async()
                .await
                .expect("view snapshot")
                .state
                .liveness
                == SubscriptionLiveness::Current
            {
                break;
            }
        }
        drop(sender);
        let requests = wait_for_requests(&connector, 3).await;
        assert_eq!(Some(original), requests.get(2));
    })
    .await
    .expect("request identity survives initial failure and reconnect");
}
