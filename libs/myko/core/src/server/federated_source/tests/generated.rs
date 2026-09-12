use super::*;

async fn generated_handler_retains_history(
    kind: myko_federation::HandlerKind,
    handler_id: &str,
    params: serde_json::Value,
    before: serde_json::Value,
    after: serde_json::Value,
) -> Result<(), String> {
    let node = Node::in_memory();
    let record = |value| ProjectionRecord {
        id: ProjectionRecordId::from("record"),
        value,
    };
    commit(&node, Some(&record(1)))?;
    let other_origin = Node::in_memory();
    commit(&other_origin, Some(&record(99)))?;
    for event in other_origin.events_after(None).map_err(|e| e.to_string())? {
        node.ingest(event).map_err(|e| e.to_string())?;
    }
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
        Arc::from("generated-readiness"),
        NodeFrameSink(Arc::clone(&frames)),
    );
    host.open_handler(
        &mut session,
        Arc::from("generated-readiness"),
        myko_wire::HandlerRequest {
            kind,
            service_id: Some(ServiceId::new(ProjectionService::SERVICE_ID)),
            handler_id: handler_id.to_owned(),
            source_node: Some(node.node_id()),
            scope_id: Some(ScopeId::for_item::<ProjectionRecord>(
                &ProjectionRecordId::from("record"),
            )),
            params,
        },
    )?;
    wait_for_frames(&frames, 1).await?;
    let initial = frames.lock().map_err(|e| e.to_string())?.first().cloned();
    let Some(myko_wire::NodeFrame::HandlerState { state, revision }) = initial else {
        return Err(format!("{handler_id} did not publish an initial snapshot"));
    };
    assert!(
        matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. }),
        "{handler_id} labeled incomplete history {:?}",
        state.liveness,
    );
    assert_eq!(state.value, Some(before), "{handler_id} lost ready history");
    assert!(state.through.is_some());

    node.ingest(dependency).map_err(|e| e.to_string())?;
    wait_for_frames(&frames, 2).await?;
    let recovered = frames.lock().map_err(|e| e.to_string())?.last().cloned();
    let (next_revision, liveness, through, value) = match recovered {
        Some(myko_wire::NodeFrame::HandlerState { state, revision }) => {
            (revision, state.liveness, state.through, state.value)
        }
        Some(myko_wire::NodeFrame::HandlerViewDelta { delta, revision }) => {
            assert!(delta.deletes.is_empty());
            let values: Vec<_> = delta.upserts.into_iter().map(|row| row.value).collect();
            (
                revision,
                delta.liveness,
                delta.through,
                Some(serde_json::json!(values)),
            )
        }
        other => return Err(format!("{handler_id} failed to recover: {other:?}")),
    };
    assert_eq!(next_revision.epoch, revision.epoch);
    assert_eq!(
        Some(next_revision.sequence),
        revision.sequence.checked_add(1)
    );
    assert_eq!(liveness, SubscriptionLiveness::Current);
    let initial_cut: Option<LogPosition> = state
        .through
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| e.to_string())?;
    let recovered_cut: Option<LogPosition> = through
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| e.to_string())?;
    assert!(recovered_cut > initial_cut);
    assert_eq!(value, Some(after));
    Ok(())
}

macro_rules! generated_case {
    ($test:ident, $kind:ident, $handler:literal, $params:expr, $before:expr, $after:expr) => {
        #[tokio::test]
        async fn $test() -> Result<(), String> {
            let _serial = crate::test_util::scheduler_test_serial();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                generated_handler_retains_history(
                    myko_federation::HandlerKind::$kind,
                    $handler,
                    $params,
                    $before,
                    $after,
                ),
            )
            .await
            .map_err(|e| e.to_string())?
        }
    };
}

generated_case!(
    generated_all_query_retains_history,
    Query,
    "GetAllProjectionRecords",
    serde_json::json!({}),
    serde_json::json!([{"id":"record", "value":1}]),
    serde_json::json!([{"id":"record", "value":2}])
);
generated_case!(
    generated_ids_query_retains_history,
    Query,
    "GetProjectionRecordsByIds",
    serde_json::json!({"ids":["record", "absent", "record"]}),
    serde_json::json!([{"id":"record", "value":1}]),
    serde_json::json!([{"id":"record", "value":2}])
);
generated_case!(
    generated_filter_query_retains_history,
    Query,
    "GetProjectionRecordsByQuery",
    serde_json::json!({"value":2}),
    serde_json::json!([]),
    serde_json::json!([{"id":"record", "value":2}])
);
generated_case!(
    generated_by_id_report_retains_history,
    Report,
    "GetProjectionRecordById",
    serde_json::json!({"id":"record"}),
    serde_json::json!({"id":"record", "value":1}),
    serde_json::json!({"id":"record", "value":2})
);
generated_case!(
    generated_count_all_report_retains_history,
    Report,
    "CountAllProjectionRecords",
    serde_json::json!({}),
    serde_json::json!({"count":1}),
    serde_json::json!({"count":1})
);
generated_case!(
    generated_count_report_retains_history,
    Report,
    "CountProjectionRecords",
    serde_json::json!({"value":2}),
    serde_json::json!({"count":0}),
    serde_json::json!({"count":1})
);
generated_case!(
    generated_empty_ids_query_retains_history,
    Query,
    "GetProjectionRecordsByIds",
    serde_json::json!({"ids":[]}),
    serde_json::json!([]),
    serde_json::json!([])
);
generated_case!(
    generated_nonmatching_filter_retains_history,
    Query,
    "GetProjectionRecordsByQuery",
    serde_json::json!({"id":"absent"}),
    serde_json::json!([]),
    serde_json::json!([])
);
generated_case!(
    generated_missing_id_report_retains_history,
    Report,
    "GetProjectionRecordById",
    serde_json::json!({"id":"absent"}),
    serde_json::Value::Null,
    serde_json::Value::Null
);
