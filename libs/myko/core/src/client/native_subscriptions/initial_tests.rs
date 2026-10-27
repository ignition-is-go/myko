use futures::FutureExt as _;

use super::*;

fn server(id: &str) -> Value {
    serde_json::json!({"itemType":"Server", "item":{
        "id":id,"version":"fixture","address":"","port":0,"startedAt":""
    }})
}

#[test]
fn complete_populated_and_empty_snapshots_do_not_depend_on_cell_readiness() {
    let fixture = setup(2);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let populated = native.try_query_initial(GetPeerServers {});
    let empty = native.try_query_initial(GetPeerServers {});
    assert!(populated.is_ok() && empty.is_ok());
    let (Ok(populated), Ok(empty)) = (populated, empty) else {
        return;
    };
    let frames = transport.frames();
    assert_eq!(frames.len(), 2);
    let mut frames = frames.iter();
    let Some(first) = frames.next() else {
        return;
    };
    let Some(second) = frames.next() else {
        return;
    };
    hyphae::batch(|| {
        publish_item(&client, tx(first), &server("complete"));
        initial(&client, tx(second));
        let result = populated.recv().now_or_never();
        assert!(result.as_ref().is_some_and(Result::is_ok));
        if let Some(Ok(items)) = result {
            assert_eq!(
                items
                    .iter()
                    .map(|item| item.id.as_ref())
                    .collect::<Vec<_>>(),
                vec!["complete"]
            );
        }
        let result = empty.recv().now_or_never();
        assert!(matches!(result, Some(Ok(items)) if items.is_empty()));
    });
    assert!(populated.try_snapshot().is_none());
    assert!(empty.try_snapshot().is_none());
    drop(populated);
    drop(empty);
    assert_eq!(native.subscription_count(), 0);
}

#[test]
fn duplicate_ids_keep_the_last_upsert_from_the_complete_snapshot() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let handler = client
        .inner
        .query_handlers
        .get(transaction.as_str())
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    let Some(handler) = handler else {
        return;
    };
    let first = server("same");
    let mut last = server("same");
    if let Some(version) = last.pointer_mut("/item/version") {
        *version = Value::String("last".into());
    }
    handler(serde_json::json!({"tx":transaction,"sequence":0,"upserts":[first,last],"deletes":[]}));
    assert!(result.try_snapshot().is_some_and(
        |items| items.len() == 1 && items.first().is_some_and(|item| item.version == "last")
    ));
}

#[test]
fn reconnect_can_publish_while_the_old_response_wake_is_paused() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    let old_tx = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let prepared = result.lease.prepare_response(
        &old_tx,
        serde_json::json!({
            "tx":old_tx,"sequence":0,"upserts":[server("retired")],"deletes":[]
        }),
    );
    assert!(prepared.is_some());
    let Some(prepared) = prepared else {
        return;
    };
    assert!(prepared.completion.is_some());
    result.lease.connection(false);
    result.lease.connection(true);
    let fresh_tx = transport
        .frames()
        .last()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    publish_item(&client, &fresh_tx, &server("fresh"));
    if let Some(completion) = prepared.completion {
        completion();
    }
    let received = result.recv().now_or_never();
    assert!(
        matches!(received, Some(Ok(items)) if items.first().is_some_and(|item| item.id.as_ref() == "fresh"))
    );
    drop(result);
    assert_eq!(native.subscription_count(), 0);
}

#[test]
fn pending_receive_retains_the_client_and_cancellation_obeys_initial_permits() {
    use std::{
        future::Future as _,
        task::{Context, Poll},
    };

    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    assert_eq!(transport.frames().len(), 1);
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let weak_client = Arc::downgrade(&client.inner);
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(result.recv());
    assert!(pending.as_mut().poll(&mut context).is_pending());
    drop(client);
    drop(native);
    let retained = weak_client.upgrade();
    assert!(retained.is_some());
    let Some(inner) = retained else {
        return;
    };
    let retained = MykoClient { inner };
    let native = retained.native_subscriptions(limits(1));
    assert!(native.is_ok());
    let Ok(native) = native else {
        return;
    };
    result.cancel();
    assert!(matches!(
        pending.as_mut().poll(&mut context),
        Poll::Ready(Err(SubscriptionAdmissionError::ClientClosed))
    ));
    assert_eq!(native.subscription_count(), 1);
    initial(&retained, &transaction);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
    drop(pending);
    drop(result);
    drop(retained);
    assert!(weak_client.upgrade().is_none());
}

#[test]
fn reconnect_rejects_old_tx_and_replaces_queued_old_snapshot() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    let old_tx = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let old_handler = client
        .inner
        .query_handlers
        .get(old_tx.as_str())
        .map(|handler| handler.clone());
    assert!(old_handler.is_some());
    publish_item(&client, &old_tx, &server("retired"));
    transport.connected(false);
    assert!(result.try_snapshot().is_none());
    transport.connected(true);
    let new_tx = transport
        .frames()
        .last()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    assert_ne!(old_tx, new_tx);
    publish_item(&client, &new_tx, &server("fresh"));
    if let Some(old_handler) = old_handler {
        old_handler(
            serde_json::json!({"tx":old_tx,"sequence":0,"upserts":[server("stale")],"deletes":[]}),
        );
    }
    let snapshot = result.recv().now_or_never();
    assert!(
        matches!(snapshot, Some(Ok(items)) if items.len() == 1 && items.first().is_some_and(|item| item.id.as_ref() == "fresh"))
    );
    assert!(result.try_snapshot().is_none());
}

#[test]
fn a_full_wake_channel_cannot_hide_a_new_complete_snapshot() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    publish_item(&client, &transaction, &server("first"));
    publish_item(&client, &transaction, &server("replacement"));
    let snapshot = result.recv().now_or_never();
    assert!(
        matches!(snapshot, Some(Ok(items)) if items.first().is_some_and(|item| item.id.as_ref() == "replacement"))
    );
    initial(&client, &transaction);
    let snapshot = result.recv().now_or_never();
    assert!(matches!(snapshot, Some(Ok(items)) if items.is_empty()));
}

#[test]
fn full_start_and_cancel_retain_only_their_admitted_permits() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    transport.full.store(true, Ordering::SeqCst);
    let rejected = native.try_query_initial(GetPeerServers {});
    assert!(rejected.is_ok());
    assert_eq!(native.subscription_count(), 1);
    assert!(matches!(
        native.try_query_initial(GetPeerServers {}),
        Err(SubscriptionAdmissionError::CapacityExceeded)
    ));
    drop(rejected);
    assert_eq!(native.subscription_count(), 0);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    transport.full.store(false, Ordering::SeqCst);
    assert_eq!(native.retry_pending_subscriptions(1), 1);
    let transaction = transport
        .frames()
        .last()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    drop(result);
    assert_eq!(native.subscription_count(), 1);
    transport.full.store(true, Ordering::SeqCst);
    initial(&client, &transaction);
    assert_eq!(native.subscription_count(), 1);
    transport.full.store(false, Ordering::SeqCst);
    assert_eq!(native.retry_pending_subscriptions(1), 1);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
}

#[test]
fn partial_or_malformed_responses_do_not_complete_an_initial_result() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let result = native.try_query_initial(GetPeerServers {});
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let handler = client
        .inner
        .query_handlers
        .get(transaction.as_str())
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    let Some(handler) = handler else {
        return;
    };
    handler(
        serde_json::json!({"tx":transaction,"sequence":1,"upserts":[server("partial")],"deletes":[]}),
    );
    assert!(result.try_snapshot().is_none());
    handler(
        serde_json::json!({"tx":transaction,"sequence":0,"upserts":[server("valid"),{"itemType":"Server","item":{}}],"deletes":[]}),
    );
    assert!(result.try_snapshot().is_none());
    initial(&client, &transaction);
    assert!(result.try_snapshot().is_some_and(|items| items.is_empty()));
}
