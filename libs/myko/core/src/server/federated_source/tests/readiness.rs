use super::*;

async fn native_map_open_preserves_pending_history(
    kind: myko_federation::HandlerKind,
    handler_id: &str,
) -> Result<(), String> {
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let host = ApplicationHost::new(
        node.clone(),
        MykoApplication::builder()
            .service::<ProjectionService>()
            .build(),
    )?;
    let frames = Arc::new(Mutex::new(Vec::new()));
    let mut session = crate::server::ClientSession::new(
        Arc::from("map-readiness"),
        NodeFrameSink(Arc::clone(&frames)),
    );
    host.open_handler(
        &mut session,
        Arc::from("map-readiness"),
        myko_wire::HandlerRequest {
            kind,
            service_id: Some(ServiceId::new(ProjectionService::SERVICE_ID)),
            handler_id: handler_id.to_owned(),
            source_node: Some(node.node_id()),
            scope_id: Some(scope()),
            params: serde_json::json!({}),
        },
    )?;
    wait_for_frames(&frames, 1).await?;
    let initial = frames
        .lock()
        .map_err(|_| "frame sink poisoned".to_owned())?
        .first()
        .cloned()
        .ok_or_else(|| "initial map frame missing".to_owned())?;
    let myko_wire::NodeFrame::HandlerState {
        state,
        revision: initial_revision,
    } = initial
    else {
        return Err("map did not open with a snapshot".to_owned());
    };
    if !matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. }) {
        return Err(format!(
            "{handler_id} labeled pending history {:?}",
            state.liveness
        ));
    }
    assert_eq!(
        state.value,
        Some(serde_json::json!([{"id": "record", "value": 1}]))
    );
    assert!(state.through.is_some());
    node.ingest(dependency).map_err(|error| error.to_string())?;
    wait_for_frames(&frames, 2).await?;
    let recovered = frames
        .lock()
        .map_err(|_| "frame sink poisoned".to_owned())?
        .last()
        .cloned()
        .ok_or_else(|| "recovered map frame missing".to_owned())?;
    let myko_wire::NodeFrame::HandlerViewDelta { delta, revision } = recovered else {
        return Err("map did not recover with a keyed update".to_owned());
    };
    assert_eq!(revision.epoch, initial_revision.epoch);
    assert_eq!(
        revision.sequence,
        initial_revision
            .sequence
            .checked_add(1)
            .ok_or_else(|| { "initial map publication sequence exhausted".to_owned() })?
    );
    assert_eq!(delta.liveness, SubscriptionLiveness::Current);
    assert_eq!(
        delta.upserts,
        vec![myko_wire::ErasedKeyedValue {
            key: "record".to_owned(),
            value: serde_json::json!({"id": "record", "value": 2}),
        }]
    );
    assert!(delta.deletes.is_empty());
    assert!(delta.through.is_some());
    Ok(())
}

#[tokio::test]
async fn ordinary_query_open_preserves_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    native_map_open_preserves_pending_history(
        myko_federation::HandlerKind::Query,
        "ProjectionRecords",
    )
    .await
}

#[tokio::test]
async fn ordinary_view_open_preserves_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    native_map_open_preserves_pending_history(
        myko_federation::HandlerKind::View,
        "ProjectionRecordView",
    )
    .await
}

#[tokio::test]
async fn nested_query_open_preserves_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    native_map_open_preserves_pending_history(
        myko_federation::HandlerKind::Query,
        "NestedProjectionRecords",
    )
    .await
}

#[tokio::test]
async fn nested_view_open_preserves_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    native_map_open_preserves_pending_history(
        myko_federation::HandlerKind::View,
        "NestedProjectionRecordView",
    )
    .await
}

#[tokio::test]
async fn cached_query_retains_lifecycle_and_rejects_raw_map_conversion() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let server = retained_context(node.clone())?;
    let request = Arc::new(RequestContext::internal(
        "query-cache".into(),
        server.host_id,
        "test",
    ));
    let open = || {
        server.query_value_routed(
            ProjectionRecords,
            request.clone(),
            Some(FederatedRequest {
                source_node: Some(node.node_id()),
                scope_id: Some(scope()),
            }),
        )
    };
    let first = open()?;
    let second = open()?;
    assert!(first.clone().into_local_map().is_err());
    let raw = server.query_map_untyped_routed(
        ProjectionRecords,
        request.clone(),
        Some(FederatedRequest {
            source_node: Some(node.node_id()),
            scope_id: Some(scope()),
        }),
    );
    assert!(matches!(raw, Err(error) if error.contains("retained query output")));
    let live = first.clone().into_retained()?;
    assert!(live.shares_state_with(&second.clone().into_retained()?));
    let mut publications = live.watch_publications();
    publications.recv().map_err(|error| error.to_string())?;
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let pending = next_publication(&mut publications, &node).await?;
    assert!(matches!(
        pending.state.liveness,
        SubscriptionLiveness::Resynchronizing { .. }
    ));
    assert!(first.read_current().is_err());
    assert!(second.read_current().is_err());
    assert!(open()?.read_current().is_err());
    node.ingest(dependency).map_err(|error| error.to_string())?;
    let recovered = next_publication(&mut publications, &node).await?;
    assert_eq!(recovered.state.liveness, SubscriptionLiveness::Current);
    assert_eq!(first.read_current()?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cached_origin_query_opens_after_another_origin_advances_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let host = ApplicationHost::new(
        node.clone(),
        MykoApplication::builder()
            .service::<ProjectionService>()
            .build(),
    )?;
    let request = myko_wire::HandlerRequest {
        kind: myko_federation::HandlerKind::Query,
        service_id: Some(ServiceId::new(ProjectionService::SERVICE_ID)),
        handler_id: "ProjectionRecords".to_owned(),
        source_node: Some(node.node_id()),
        scope_id: Some(scope()),
        params: serde_json::json!({}),
    };
    let priming_frames = Arc::new(Mutex::new(Vec::new()));
    let mut priming_session = crate::server::ClientSession::new(
        Arc::from("origin-prime"),
        NodeFrameSink(Arc::clone(&priming_frames)),
    );
    host.open_handler(
        &mut priming_session,
        Arc::from("origin-prime"),
        request.clone(),
    )?;
    wait_for_frames(&priming_frames, 1).await?;

    let other = Node::in_memory();
    commit(&other, Some(&record(2)))?;
    for event in other
        .events_after(None)
        .map_err(|error| error.to_string())?
    {
        node.ingest(event).map_err(|error| error.to_string())?;
    }
    let required_cut = node
        .local_history_cut()
        .map_err(|error| error.to_string())?;
    let frames = Arc::new(Mutex::new(Vec::new()));
    let mut session = crate::server::ClientSession::new(
        Arc::from("origin-reopen"),
        NodeFrameSink(Arc::clone(&frames)),
    );
    host.open_handler(&mut session, Arc::from("origin-reopen"), request)?;
    wait_for_frames(&frames, 1).await?;
    assert!(matches!(
        frames.lock().map_err(|_| "test sink poisoned")?.first(),
        Some(myko_wire::NodeFrame::HandlerState { state, .. })
            if state.liveness == SubscriptionLiveness::Current
                && state.through == required_cut.map(|cut| serde_json::json!(cut.get()))
                && state.value == Some(serde_json::json!([{"id": "record", "value": 1}]))
                && state.row_keys.as_deref() == Some(&["record".to_owned()])
    ));
    session.cancel_all();
    priming_session.cancel_all();
    host.shutdown().await;
    Ok(())
}

#[test]
fn handler_setup_errors_reach_callers_without_empty_results() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let server = retained_context(Node::in_memory())?;
    let request = Arc::new(RequestContext::internal(
        "missing-federated-route".into(),
        server.host_id,
        "test",
    ));
    let query = server.query_map_untyped(ProjectionRecords, request.clone());
    assert!(matches!(query, Err(error) if error.contains("federation request")));
    let view = server.view_value_routed(ProjectionRecordView, request.clone(), None);
    assert!(matches!(view, Err(error) if error.contains("federation request")));
    let report = server.report_routed(FederatedProjectionCountReport {}, request, None);
    assert!(matches!(report, Err(error) if error.contains("federation request")));
    Ok(())
}

#[tokio::test]
async fn cached_and_composed_reports_preserve_dependency_lifecycle() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let server = retained_context(node.clone())?;
    let request = Arc::new(crate::request::RequestContext::internal(
        Arc::from("cached-readiness"),
        server.host_id,
        "report-readiness",
    ));
    let route = FederatedRequest {
        source_node: Some(node.node_id()),
        scope_id: Some(scope()),
    };
    let open = || {
        server.report_routed(
            FederatedProjectionCountReport {},
            request.clone(),
            Some(route.clone()),
        )
    };
    let first = open()?;
    let second = open()?;
    match (&first, &second) {
        (
            crate::report::ReportValue::RetainedPublication(a),
            crate::report::ReportValue::RetainedPublication(b),
        ) => assert!(a.shares_state_with(b)),
        _ => return Err("registered report cache lost retained lifecycle".to_owned()),
    }
    let composed = first.map_value(|count| Arc::new(count.count));
    let mut publications = composed.clone().into_live().watch_publications();
    let initial = publications.recv().map_err(|e| e.to_string())?;
    assert_eq!(initial.state.value, Some(Arc::new(1)));
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let pending = next_publication(&mut publications, &node).await?;
    assert!(matches!(
        pending.state.liveness,
        SubscriptionLiveness::Resynchronizing { .. }
    ));
    assert!(pending.sequence > initial.sequence);
    assert!(first.read_current().is_err());
    assert!(second.read_current().is_err());
    assert!(composed.read_current().is_err());
    assert!(open()?.read_current().is_err());
    node.ingest(dependency).map_err(|e| e.to_string())?;
    let recovered = next_publication(&mut publications, &node).await?;
    assert_eq!(recovered.state.liveness, SubscriptionLiveness::Current);
    assert!(recovered.sequence > pending.sequence);
    assert_eq!(*composed.read_current()?, 1);
    Ok(())
}

#[tokio::test]
async fn native_report_preserves_readiness_when_its_value_does_not_change() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let host = ApplicationHost::new(
        node.clone(),
        MykoApplication::builder()
            .service::<ProjectionService>()
            .build(),
    )?;
    let frames = Arc::new(Mutex::new(Vec::new()));
    let mut session = crate::server::ClientSession::new(
        Arc::from("report-readiness"),
        NodeFrameSink(Arc::clone(&frames)),
    );
    host.open_handler(
        &mut session,
        Arc::from("report-readiness"),
        myko_wire::HandlerRequest {
            kind: myko_federation::HandlerKind::Report,
            service_id: Some(ServiceId::new(ProjectionService::SERVICE_ID)),
            handler_id: "FederatedProjectionCountReport".to_owned(),
            source_node: Some(node.node_id()),
            scope_id: Some(scope()),
            params: serde_json::json!({}),
        },
    )?;
    wait_for_frames(&frames, 1).await?;
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let pending = next_report_frame(&frames, &node).await?;
    if !matches!(pending, myko_wire::NodeFrame::HandlerState { state, .. }
        if matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. })
            && state.value == Some(serde_json::json!({"count": 1})))
    {
        return Err("report lost dependency desync or changed its coherent count".to_owned());
    }
    node.ingest(dependency).map_err(|error| error.to_string())?;
    let recovered = next_report_frame(&frames, &node).await?;
    if !matches!(recovered, myko_wire::NodeFrame::HandlerState { state, .. }
        if state.liveness == SubscriptionLiveness::Current
            && state.value == Some(serde_json::json!({"count": 1})))
    {
        return Err("report did not recover without a count change".to_owned());
    }
    Ok(())
}

fn record(value: u64) -> ProjectionRecord {
    ProjectionRecord {
        id: ProjectionRecordId::from("record"),
        value,
    }
}

fn scope() -> ScopeId {
    ScopeId::for_item::<ProjectionRecord>(&ProjectionRecordId::from("record"))
}

fn value(source: &FederatedMapSource) -> Result<u64, String> {
    source
        .rows()
        .get_value(&Arc::from("record"))
        .and_then(|row| {
            row.as_any()
                .downcast_ref::<ProjectionRecord>()
                .map(|row| row.value)
        })
        .ok_or_else(|| "coherent projection row is missing".to_owned())
}

async fn initial_pending_projection(source_filtered: bool) -> Result<(), String> {
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let source = FederatedMapSource::start::<ProjectionRecord>(
        &node,
        source_filtered.then(|| node.node_id()),
        Some(scope()),
        &tokio::runtime::Handle::current(),
    )?;
    if value(&source)? != 1 {
        return Err("initial projection exposed a causally incomplete write".to_owned());
    }
    if !matches!(
        source.revision().get().liveness,
        SubscriptionLiveness::Resynchronizing { .. }
    ) {
        return Err("initial projection labeled pending history Current".to_owned());
    }
    source.shutdown().await;
    Ok(())
}

async fn live_pending_projection(source_filtered: bool) -> Result<(), String> {
    let node = Node::in_memory();
    commit(&node, Some(&record(1)))?;
    let source = FederatedMapSource::start::<ProjectionRecord>(
        &node,
        source_filtered.then(|| node.node_id()),
        Some(scope()),
        &tokio::runtime::Handle::current(),
    )?;
    let (sender, receiver) = flume::unbounded();
    let _guard = source.revision().subscribe(move |signal| {
        if let Signal::Value(revision) = signal {
            let _delivered = sender.send(revision.clone());
        }
    });
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    let pending_cut = node
        .local_history_cut()
        .map_err(|error| error.to_string())?;
    let pending = next_revision(&receiver, pending_cut).await?;
    if !matches!(
        pending.liveness,
        SubscriptionLiveness::Resynchronizing { .. }
    ) {
        return Err("live projection labeled pending history Current".to_owned());
    }
    if value(&source)? != 1 {
        return Err("live projection exposed a causally incomplete write".to_owned());
    }
    node.ingest(dependency).map_err(|error| error.to_string())?;
    let released_cut = node
        .local_history_cut()
        .map_err(|error| error.to_string())?;
    let released = next_revision(&receiver, released_cut).await?;
    if released.liveness != SubscriptionLiveness::Current || value(&source)? != 2 {
        return Err("unrelated-origin parent did not release the ready projection".to_owned());
    }
    source.shutdown().await;
    Ok(())
}

async fn next_revision(
    receiver: &flume::Receiver<Arc<MapRevision>>,
    through: Option<LogPosition>,
) -> Result<Arc<MapRevision>, String> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let revision = receiver
                .recv_async()
                .await
                .map_err(|error| error.to_string())?;
            if revision.frontier == through {
                return Ok(revision);
            }
        }
    })
    .await
    .map_err(|_| "projection did not publish its consumed history cut".to_owned())?
}

async fn next_publication<T, C>(
    publications: &mut myko_federation::LivePublicationStream<
        myko_federation::LiveSubscriptionState<T, C>,
    >,
    node: &Node,
) -> Result<myko_federation::LivePublication<myko_federation::LiveSubscriptionState<T, C>>, String>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue + serde::Serialize,
{
    let cut = node
        .local_history_cut()
        .map_err(|error| error.to_string())?;
    let through = serde_json::to_value(cut).map_err(|error| error.to_string())?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let publication = publications
                .recv_async()
                .await
                .map_err(|error| error.to_string())?;
            if serde_json::to_value(&publication.state.through)
                .map_err(|error| error.to_string())?
                == through
            {
                return Ok(publication);
            }
        }
    })
    .await
    .map_err(|_| "subscription did not publish the required history cut".to_owned())?
}

async fn next_report_frame(
    frames: &Mutex<Vec<myko_wire::NodeFrame>>,
    node: &Node,
) -> Result<myko_wire::NodeFrame, String> {
    let through = node
        .local_history_cut()
        .map_err(|error| error.to_string())?
        .map(|cut| serde_json::json!(cut.get()));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let frame = frames
                .lock()
                .map_err(|_| "frame sink poisoned".to_owned())?
                .iter()
                .find(|frame| {
                    matches!(frame,
                    myko_wire::NodeFrame::HandlerState { state, .. } if state.through == through)
                })
                .cloned();
            if let Some(frame) = frame {
                return Ok(frame);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "report did not publish the required history cut".to_owned())?
}

#[tokio::test]
async fn all_source_initial_projection_reports_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    initial_pending_projection(false).await
}

#[tokio::test]
async fn source_filtered_initial_projection_withholds_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    initial_pending_projection(true).await
}

#[tokio::test]
async fn all_source_live_projection_reports_and_recovers_pending_history() -> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    live_pending_projection(false).await
}

#[tokio::test]
async fn source_filtered_live_projection_withholds_and_recovers_pending_history()
-> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    live_pending_projection(true).await
}

#[tokio::test]
async fn pending_history_is_scoped_but_unscoped_projections_remain_desynchronized()
-> Result<(), String> {
    let _serial = crate::test_util::scheduler_test_serial();
    let node = Node::in_memory();
    let dependency = unrelated_dependency_event()?;
    commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
    for source in [None, Some(node.node_id())] {
        let (unrelated, _) = node
            .watch_item_projection::<ProjectionRecord>(
                source,
                Some(ScopeId::new("unrelated:scope")),
            )
            .map_err(|error| error.to_string())?;
        if unrelated.liveness != SubscriptionLiveness::Current {
            return Err("an unrelated scope inherited the pending scope's desync".to_owned());
        }
        let (unscoped, _) = node
            .watch_item_projection::<ProjectionRecord>(source, None)
            .map_err(|error| error.to_string())?;
        if !matches!(
            unscoped.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) {
            return Err("unscoped projection ignored pending history".to_owned());
        }
    }
    Ok(())
}

#[tokio::test]
async fn readiness_recovers_even_when_the_released_event_does_not_change_rows() -> Result<(), String>
{
    let _serial = crate::test_util::scheduler_test_serial();
    for source_filtered in [false, true] {
        let node = Node::in_memory();
        let source = FederatedMapSource::start::<ProjectionRecord>(
            &node,
            source_filtered.then(|| node.node_id()),
            Some(scope()),
            &tokio::runtime::Handle::current(),
        )?;
        let (sender, receiver) = flume::unbounded();
        let _guard = source.revision().subscribe(move |signal| {
            if let Signal::Value(revision) = signal {
                let _delivered = sender.send(revision.clone());
            }
        });
        let dependency = unrelated_dependency_event()?;
        commit_after(&node, None, vec![dependency.origin])?;
        let pending_cut = node
            .local_history_cut()
            .map_err(|error| error.to_string())?;
        let pending = next_revision(&receiver, pending_cut).await?;
        if !matches!(
            pending.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) || pending.diff.is_some()
        {
            return Err("row-free pending delete did not publish desync".to_owned());
        }
        node.ingest(dependency).map_err(|error| error.to_string())?;
        let released_cut = node
            .local_history_cut()
            .map_err(|error| error.to_string())?;
        let released = next_revision(&receiver, released_cut).await?;
        if released.liveness != SubscriptionLiveness::Current || released.diff.is_some() {
            return Err("row-free dependency release did not publish Current".to_owned());
        }
        if !source.rows().snapshot().is_empty() {
            return Err("deleting an absent item produced a row".to_owned());
        }
        source.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn queued_projection_updates_do_not_borrow_readiness_from_a_later_cut() -> Result<(), String>
{
    let _serial = crate::test_util::scheduler_test_serial();
    for source in [false, true] {
        let node = Node::in_memory();
        commit(&node, Some(&record(1)))?;
        let (_, mut watch) = node
            .watch_item_projection::<ProjectionRecord>(
                source.then(|| node.node_id()),
                Some(scope()),
            )
            .map_err(|error| error.to_string())?;
        let dependency = unrelated_dependency_event()?;
        commit_after(&node, Some(&record(2)), vec![dependency.origin])?;
        let pending_cut = node
            .local_history_cut()
            .map_err(|error| error.to_string())?;
        node.ingest(dependency).map_err(|error| error.to_string())?;
        let admitted = tokio::time::timeout(std::time::Duration::from_secs(1), watch.recv_async())
            .await
            .map_err(|_| "admission projection update was not delivered".to_owned())?
            .map_err(|error| error.to_string())?;
        if Some(admitted.position) >= pending_cut
            || admitted.liveness != SubscriptionLiveness::Current
            || admitted.diff.is_some()
            || admitted
                .projection
                .values()
                .map(|row| row.value)
                .collect::<Vec<_>>()
                != [1]
        {
            return Err("admission projection borrowed history from its future".to_owned());
        }
        let pending = tokio::time::timeout(std::time::Duration::from_secs(1), watch.recv_async())
            .await
            .map_err(|_| "pending projection update was not delivered".to_owned())?
            .map_err(|error| error.to_string())?;
        if Some(pending.position) != pending_cut
            || !matches!(
                pending.liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            )
            || pending.projection.values().next() != Some(&record(1))
        {
            return Err("queued projection borrowed history from its future".to_owned());
        }
        let released = tokio::time::timeout(std::time::Duration::from_secs(1), watch.recv_async())
            .await
            .map_err(|_| "released projection update was not delivered".to_owned())?
            .map_err(|error| error.to_string())?;
        if released.liveness != SubscriptionLiveness::Current
            || released.projection.values().next() != Some(&record(2))
        {
            return Err("queued projection did not recover at its release cut".to_owned());
        }
    }
    Ok(())
}
