use std::sync::atomic::{AtomicBool, Ordering};

use autosocket::{SocketTransport, WsFrame};
use hyphae::{Cell, CellImmutable, CellMutable, Gettable as _, Materialize as _, Mutable as _};

use super::*;
use crate::{
    client::{MykoClientOptions, SendAdmission},
    entities::{client::ConnectedClients, server::GetPeerServers},
};

type SendHook = Box<dyn FnOnce() + Send + Sync>;

struct Transport {
    status: Cell<ConnectionStatus, CellMutable>,
    full: AtomicBool,
    sent: Mutex<Vec<Value>>,
    incoming: flume::Receiver<WsFrame>,
    _sender: flume::Sender<WsFrame>,
    on_send: Mutex<Option<SendHook>>,
}

impl Transport {
    fn new() -> Arc<Self> {
        let (sender, incoming) = flume::bounded(1);
        Arc::new(Self {
            status: Cell::new(ConnectionStatus::Disconnected),
            full: AtomicBool::new(false),
            sent: Mutex::new(Vec::new()),
            incoming,
            _sender: sender,
            on_send: Mutex::new(None),
        })
    }

    fn connected(&self, connected: bool) {
        self.status.set(if connected {
            ConnectionStatus::Connected("fixture".into())
        } else {
            ConnectionStatus::Disconnected
        });
    }

    fn frames(&self) -> Vec<Value> {
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl SocketTransport for Transport {
    fn set_addr(&self, _addr: Option<String>) {}
    fn close(&self) {
        self.connected(false);
    }
    fn intended_connection_state(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.status.clone().lock()
    }
    fn actual_connection_state(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.status.clone().lock()
    }
    fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming.clone()
    }
    fn send(&self, frame: WsFrame) -> Result<(), String> {
        let hook = self
            .on_send
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook();
        }
        if !matches!(self.status.get(), ConnectionStatus::Connected(_)) {
            return Err("disconnected".into());
        }
        if self.full.load(Ordering::SeqCst) {
            return Err("full".into());
        }
        let WsFrame::Text(text) = frame else {
            return Err("fixture requires JSON".into());
        };
        let value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(value);
        Ok(())
    }
}

fn limits(entries: usize) -> NativeSubscriptionLimits {
    NativeSubscriptionLimits {
        max_subscriptions: entries,
        max_retained_request_bytes: 16_384,
        max_request_bytes: 4_096,
    }
}

fn setup(
    entries: usize,
) -> Result<(Arc<Transport>, MykoClient, NativeSubscriptions), SubscriptionAdmissionError> {
    let transport = Transport::new();
    let socket: Arc<dyn SocketTransport> = transport.clone();
    let client = MykoClient::with_transport_and_options(
        &socket,
        MykoClientOptions {
            app_ping: false,
            ..MykoClientOptions::default()
        },
        SendAdmission::Transport,
    );
    let capability = client.native_subscriptions(limits(entries))?;
    Ok((transport, client, capability))
}

fn tx(frame: &Value) -> &str {
    frame
        .pointer("/data/query/tx")
        .or_else(|| frame.pointer("/data/view/tx"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn initial(client: &MykoClient, tx: &str) {
    let handler = client
        .inner
        .query_handlers
        .get(tx)
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    if let Some(handler) = handler {
        handler(serde_json::json!({ "tx": tx, "sequence": 0, "upserts": [], "deletes": [] }));
    }
}

#[test]
fn clones_and_reconfiguration_share_one_budget_and_cache() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((_, client, native)) = fixture else {
        return;
    };
    let second = client.native_subscriptions(limits(1));
    assert!(second.is_ok());
    let Ok(second) = second else {
        return;
    };
    let first = native.try_watch_query_state(GetPeerServers {});
    let duplicate = second.try_watch_query_state(GetPeerServers {});
    assert!(first.is_ok() && duplicate.is_ok());
    assert_eq!(native.subscription_count(), 1);
    assert!(matches!(
        second.try_watch_view_state(ConnectedClients {}),
        Err(SubscriptionAdmissionError::CapacityExceeded)
    ));
    assert!(matches!(
        client.native_subscriptions(limits(2)),
        Err(SubscriptionAdmissionError::ConfigurationConflict)
    ));
    drop(first);
    assert_eq!(native.subscription_count(), 1);
    drop(duplicate);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
}

#[test]
fn full_start_retries_without_reconnect_and_empty_initial_is_authoritative() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    transport.full.store(true, Ordering::SeqCst);
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    assert!(!watch.ready().get());
    assert!(transport.frames().is_empty());
    assert_eq!(native.retry_pending_subscriptions(3), 3);
    transport.full.store(false, Ordering::SeqCst);
    assert_eq!(native.retry_pending_subscriptions(1), 1);
    let frames = transport.frames();
    assert_eq!(frames.len(), 1);
    let Some(frame) = frames.first() else {
        return;
    };
    initial(&client, tx(frame));
    assert!(watch.ready().get());
    assert!(watch.items().get().is_empty());
    drop(watch);
    assert_eq!(native.subscription_count(), 0);
}

#[test]
fn dropped_startup_waits_for_initial_then_retries_cancel_while_full() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_view_state(ConnectedClients {});
    assert!(watch.is_ok());
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let transaction = tx(frame).to_owned();
    drop(watch);
    assert_eq!(transport.frames().len(), 1);
    assert_eq!(native.subscription_count(), 1);
    assert!(matches!(
        native.try_watch_query_state(GetPeerServers {}),
        Err(SubscriptionAdmissionError::CapacityExceeded)
    ));
    transport.full.store(true, Ordering::SeqCst);
    initial(&client, &transaction);
    assert_eq!(native.subscription_count(), 1);
    assert_eq!(native.retry_pending_subscriptions(2), 2);
    transport.full.store(false, Ordering::SeqCst);
    assert_eq!(native.retry_pending_subscriptions(1), 1);
    assert_eq!(native.subscription_count(), 0);
    let frames = transport.frames();
    assert_eq!(frames.len(), 2);
    assert!(
        frames
            .last()
            .is_some_and(|frame| frame.get("event").and_then(Value::as_str)
                == Some("ws:m:view-cancel")
                && frame.pointer("/data/tx").and_then(Value::as_str) == Some(transaction.as_str()))
    );
    assert!(client.inner.query_handlers.is_empty());
}

#[test]
fn retired_connection_frees_hung_watch_and_stale_dequeued_initial_is_rejected() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_view_map_state(ConnectedClients {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let old_tx = tx(frame).to_owned();
    let old_handler = client
        .inner
        .query_handlers
        .get(old_tx.as_str())
        .map(|handler| handler.clone());
    transport.connected(false);
    transport.connected(true);
    let frames = transport.frames();
    let Some(frame) = frames.last() else {
        return;
    };
    let new_tx = tx(frame);
    assert_ne!(old_tx, new_tx);
    if let Some(handler) = old_handler {
        handler(serde_json::json!({"tx": old_tx, "sequence": 0, "upserts": [], "deletes": []}));
    }
    assert!(!watch.ready().get());
    initial(&client, new_tx);
    assert!(watch.ready().get());
    transport.connected(false);
    transport.connected(true);
    drop(watch);
    assert_eq!(native.subscription_count(), 1);
    transport.connected(false);
    assert_eq!(native.subscription_count(), 0);
    assert!(client.inner.query_handlers.is_empty());
    assert_eq!(native.pending_subscription_count(), 0);
}

#[test]
fn oversized_request_does_not_publish_a_handler() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, _)) = fixture else {
        return;
    };
    let another = MykoClient::with_transport_owned_admission(transport);
    let native = another.native_subscriptions(NativeSubscriptionLimits {
        max_subscriptions: 1,
        max_retained_request_bytes: 1,
        max_request_bytes: 1,
    });
    assert!(native.is_ok());
    let Ok(native) = native else {
        return;
    };
    assert!(matches!(
        native.try_watch_query_state(GetPeerServers {}),
        Err(SubscriptionAdmissionError::RequestTooLarge)
    ));
    assert_eq!(native.subscription_count(), 0);
    assert!(another.inner.query_handlers.is_empty());
    assert!(client.inner.query_handlers.is_empty());
}

#[test]
fn rejected_start_churn_does_not_retain_dirty_entries_or_bytes() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    transport.full.store(true, Ordering::SeqCst);
    for _ in 0..1_000 {
        let watch = native.try_watch_query_state(GetPeerServers {});
        assert!(watch.is_ok());
        assert_eq!(native.pending_subscription_count(), 1);
        drop(watch);
        assert_eq!(native.subscription_count(), 0);
        assert_eq!(native.retained_request_bytes(), 0);
        assert_eq!(native.pending_subscription_count(), 0);
        assert!(client.inner.query_handlers.is_empty());
    }
    assert!(transport.frames().is_empty());
}

#[test]
fn total_byte_limit_is_independent_of_entry_limit() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, first_client, first)) = fixture else {
        return;
    };
    let client = MykoClient::with_transport_owned_admission(transport);
    let first_watch = first.try_watch_query_state(GetPeerServers {});
    assert!(first_watch.is_ok());
    let request_bytes = first.retained_request_bytes().saturating_add(32);
    assert!(matches!(
        first_client.connection_status_sync(),
        ConnectionStatus::Disconnected
    ));
    let native = client.native_subscriptions(NativeSubscriptionLimits {
        max_subscriptions: 100,
        max_request_bytes: request_bytes,
        max_retained_request_bytes: request_bytes,
    });
    assert!(native.is_ok());
    let Ok(native) = native else {
        return;
    };
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    assert!(matches!(
        native.try_watch_view_state(ConnectedClients {}),
        Err(SubscriptionAdmissionError::CapacityExceeded)
    ));
    assert_eq!(native.subscription_count(), 1);
    drop(watch);
    assert_eq!(native.retained_request_bytes(), 0);
}

#[test]
fn final_owner_after_client_close_releases_awaiting_initial_capacity() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    drop(client);
    assert_eq!(native.subscription_count(), 1);
    drop(watch);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
}

#[test]
fn final_owner_dropped_after_connection_changes_during_admission_releases_capacity() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let callback_owner = Arc::new(Mutex::new(Some(watch)));
    let callback_transport = transport.clone();
    *transport
        .on_send
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
        callback_transport.connected(false);
        callback_transport.connected(true);
        let watch = callback_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(watch);
    }));
    transport.connected(true);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
    assert_eq!(native.pending_subscription_count(), 0);
    assert!(client.inner.query_handlers.is_empty());
}

#[test]
fn retired_connection_keeps_inflight_reservation_until_send_completes() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let callback_owner = Arc::new(Mutex::new(Some(watch)));
    let callback_transport = transport.clone();
    let callback_native = native.clone();
    *transport
        .on_send
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
        let watch = callback_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(watch);
        callback_transport.connected(false);
        assert_eq!(callback_native.subscription_count(), 1);
        assert!(callback_native.retained_request_bytes() > 0);
    }));
    transport.connected(true);
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
    assert_eq!(native.pending_subscription_count(), 0);
    assert!(client.inner.query_handlers.is_empty());
}

#[test]
fn startup_send_can_reenter_the_same_watch_cache() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let reentrant = native.clone();
    let (completed, completion) = std::sync::mpsc::channel();
    *transport
        .on_send
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
        let duplicate = reentrant.try_watch_query_state(GetPeerServers {});
        assert!(duplicate.is_ok());
        drop(duplicate);
        assert!(completed.send(()).is_ok());
    }));
    let worker_native = native.clone();
    let worker = std::thread::spawn(move || worker_native.try_watch_query_state(GetPeerServers {}));
    let result = completion.recv_timeout(std::time::Duration::from_secs(2));
    assert!(
        result.is_ok(),
        "startup send must run outside the watch cache gate"
    );
    let result = worker.join();
    assert!(result.is_ok());
    let Ok(watch) = result else {
        return;
    };
    assert!(watch.is_ok());
    assert_eq!(native.subscription_count(), 1);
    drop(watch);
    transport.connected(false);
    assert_eq!(native.subscription_count(), 0);
    assert!(client.inner.query_handlers.is_empty());
}

fn publish_item(client: &MykoClient, tx: &str, item: &Value) {
    let handler = client
        .inner
        .query_handlers
        .get(tx)
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    if let Some(handler) = handler {
        handler(serde_json::json!({"tx": tx, "sequence": 0, "upserts": [item], "deletes": []}));
    }
}

#[test]
fn list_publication_reentry_cannot_mark_replacement_attempt_ready() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let transaction = tx(frame).to_owned();
    let callback_transport = transport.clone();
    let once = AtomicBool::new(true);
    let guard = watch.items().subscribe(move |signal| {
        if let hyphae::Signal::Value(items) = signal
            && !items.is_empty()
            && once.swap(false, Ordering::SeqCst)
        {
            callback_transport.connected(false);
            callback_transport.connected(true);
        }
    });
    publish_item(
        &client,
        &transaction,
        &serde_json::json!({"itemType":"Server", "item":{"id":"published", "version":"fixture", "address":"", "port":0, "startedAt":""}}),
    );
    assert!(
        !watch.ready().get(),
        "old initial publication must not mark replacement attempt ready"
    );
    let frames = transport.frames();
    let Some(frame) = frames.last() else {
        return;
    };
    let replacement = tx(frame);
    assert_ne!(transaction, replacement);
    initial(&client, replacement);
    assert!(watch.ready().get());
    drop(guard);
}

#[test]
fn map_publication_reentry_cannot_mark_replacement_attempt_ready() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_view_map_state(ConnectedClients {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let transaction = tx(frame).to_owned();
    let callback_transport = transport.clone();
    let once = AtomicBool::new(true);
    let guard = watch.map().subscribe_diffs(move |diff| {
        let populated = !matches!(diff, hyphae::MapDiff::Initial { entries } if entries.is_empty());
        if populated && once.swap(false, Ordering::SeqCst) {
            callback_transport.connected(false);
            callback_transport.connected(true);
        }
    });
    publish_item(
        &client,
        &transaction,
        &serde_json::json!({"itemType":"Client", "item":{"id":"published", "serverId":"server", "address":null, "windback":null}}),
    );
    assert!(
        !watch.ready().get(),
        "old map publication must not mark replacement attempt ready"
    );
    let frames = transport.frames();
    let Some(frame) = frames.last() else {
        return;
    };
    initial(&client, tx(frame));
    assert!(watch.ready().get());
    drop(guard);
}

struct ReleaseDelivery(Option<std::sync::mpsc::Sender<()>>);

impl Drop for ReleaseDelivery {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn concurrent_connection_change_waits_for_validated_publication() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let release = ReleaseDelivery(Some(release));
    let released = Mutex::new(released);
    let delivered = Arc::new(AtomicBool::new(false));
    let callback_delivered = delivered.clone();
    let request = Request::Query(WrappedQuery {
        query: serde_json::json!({"tx":"placeholder"}),
        query_id: "GetPeerServers".into(),
        query_item_type: "Server".into(),
        window: None,
    });
    let delivery: Delivery = Arc::new(move |_epoch, _value| {
        assert!(entered.send(()).is_ok());
        assert!(
            released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok()
        );
        callback_delivered.store(true, Ordering::SeqCst);
    });
    let lease = native.lease(request, delivery, Arc::new(|| {}));
    assert!(lease.is_ok());
    let Ok(lease) = lease else {
        return;
    };
    lease.start();
    transport.connected(true);
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let transaction = tx(frame).to_owned();
    let handler = client
        .inner
        .query_handlers
        .get(transaction.as_str())
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    let Some(handler) = handler else {
        return;
    };
    let publisher = std::thread::spawn(move || {
        handler(serde_json::json!({"tx":transaction, "sequence":0, "upserts":[], "deletes":[]}));
    });
    assert!(
        entry
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok()
    );
    let (changed, change) = std::sync::mpsc::channel();
    let (changing, starting) = std::sync::mpsc::channel();
    let transition_lease = lease.clone();
    let transition = std::thread::spawn(move || {
        assert!(changing.send(()).is_ok());
        transition_lease.connection(false);
        assert!(changed.send(()).is_ok());
    });
    assert!(
        starting
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok()
    );
    assert!(
        change
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "connection retirement must serialize with already validated publication"
    );
    drop(release);
    assert!(publisher.join().is_ok());
    assert!(transition.join().is_ok());
    assert!(delivered.load(Ordering::SeqCst));
    lease.close();
    assert_eq!(native.subscription_count(), 0);
}

#[test]
fn final_owner_dropped_during_initial_publication_retires_ready_and_capacity() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let frames = transport.frames();
    let Some(frame) = frames.first() else {
        return;
    };
    let transaction = tx(frame).to_owned();
    let items = watch.items().clone();
    let ready = watch.ready().clone();
    let owner = Arc::new(Mutex::new(Some(watch)));
    let callback_owner = owner.clone();
    let guard = items.subscribe(move |signal| {
        if let hyphae::Signal::Value(items) = signal
            && !items.is_empty()
        {
            let watch = callback_owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(watch);
        }
    });
    drop(items);
    publish_item(
        &client,
        &transaction,
        &serde_json::json!({"itemType":"Server", "item":{"id":"published", "version":"fixture", "address":"", "port":0, "startedAt":""}}),
    );
    drop(guard);
    assert!(
        owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    );
    assert!(!ready.get());
    assert_eq!(native.subscription_count(), 0);
    assert_eq!(native.retained_request_bytes(), 0);
    assert!(client.inner.query_handlers.is_empty());
}

#[test]
fn other_watch_publication_can_finish_while_an_observer_waits_to_reconnect() {
    let fixture = setup(2);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let first = native.try_watch_query_state(GetPeerServers {});
    assert!(first.is_ok());
    let Ok(first) = first else {
        return;
    };
    let second = native.try_watch_view_state(ConnectedClients {});
    assert!(second.is_ok());
    let Ok(second) = second else {
        return;
    };
    let frames = transport.frames();
    let [first_frame, second_frame] = frames.as_slice() else {
        assert_eq!(frames.len(), 2);
        return;
    };
    let first_tx = tx(first_frame).to_owned();
    let second_tx = tx(second_frame).to_owned();
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let release = ReleaseDelivery(Some(release));
    let released = Mutex::new(released);
    let callback_transport = transport.clone();
    let guard = first.items().subscribe(move |signal| {
        if let hyphae::Signal::Value(items) = signal
            && !items.is_empty()
        {
            assert!(entered.send(()).is_ok());
            assert!(
                released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .is_ok()
            );
            callback_transport.connected(false);
            callback_transport.connected(true);
        }
    });
    let first_handler = client
        .inner
        .query_handlers
        .get(first_tx.as_str())
        .map(|h| h.clone());
    let second_handler = client
        .inner
        .query_handlers
        .get(second_tx.as_str())
        .map(|h| h.clone());
    assert!(first_handler.is_some() && second_handler.is_some());
    let (Some(first_handler), Some(second_handler)) = (first_handler, second_handler) else {
        return;
    };
    let publisher = std::thread::spawn(move || {
        first_handler(serde_json::json!({
        "tx":first_tx,"sequence":0,"deletes":[],"upserts":[{"itemType":"Server", "item":{
            "id":"published","version":"fixture","address":"","port":0,"startedAt":""}}]}));
    });
    assert!(
        entry
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok()
    );
    let (finished, finish) = std::sync::mpsc::channel();
    let other = std::thread::spawn(move || {
        second_handler(serde_json::json!({"tx":second_tx,"sequence":0,"deletes":[],"upserts":[]}));
        assert!(finished.send(()).is_ok());
    });
    assert!(
        finish
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok(),
        "observer fanout must not hold the native dispatch gate"
    );
    drop(release);
    assert!(publisher.join().is_ok());
    assert!(other.join().is_ok());
    assert!(!first.ready().get() && !second.ready().get());
    drop(guard);
    drop(first);
    drop(second);
    transport.connected(false);
    assert_eq!(native.subscription_count(), 0);
}
fn map_client_row(id: usize, address: &str) -> Value {
    serde_json::json!({"itemType":"Client", "item":{"id":id.to_string(), "serverId":"server", "address":address, "windback":null}})
}

fn per_key_reconnect_preserves_new_initial(row_count: usize) {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_view_map_state(ConnectedClients {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let once = Arc::new(AtomicBool::new(true));
    let cells = (0..row_count)
        .map(|id| {
            watch
                .map()
                .get(&Arc::<str>::from(id.to_string()))
                .materialize()
        })
        .collect::<Vec<_>>();
    let guards = cells
        .iter()
        .map(|cell| {
            let callback_transport = transport.clone();
            let callback_once = once.clone();
            cell.subscribe(move |signal| {
                if let hyphae::Signal::Value(item) = signal
                    && item.is_some()
                    && callback_once.swap(false, Ordering::SeqCst)
                {
                    callback_transport.connected(false);
                    callback_transport.connected(true);
                }
            })
        })
        .collect::<Vec<_>>();
    let callback_client = Arc::downgrade(&client.inner);
    let new_rows = (0..row_count)
        .map(|id| map_client_row(id, "new"))
        .collect::<Vec<_>>();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let callback_ran = hook_ran.clone();
    *transport
        .on_send
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(move || {
        let Some(inner) = callback_client.upgrade() else {
            return;
        };
        let handler = inner
            .query_handlers
            .iter()
            .next()
            .map(|entry| (entry.key().clone(), entry.value().clone()));
        let Some((tx, handler)) = handler else {
            return;
        };
        callback_ran.store(true, Ordering::SeqCst);
        handler(serde_json::json!({"tx":tx, "sequence":0, "upserts":new_rows, "deletes":[]}));
    }));
    let old_rows = (0..row_count)
        .map(|id| map_client_row(id, "old"))
        .collect::<Vec<_>>();
    let handler = client
        .inner
        .query_handlers
        .get(transaction.as_str())
        .map(|handler| handler.clone());
    assert!(handler.is_some());
    if let Some(handler) = handler {
        handler(
            serde_json::json!({"tx":transaction, "sequence":0, "upserts":old_rows, "deletes":[]}),
        );
    }
    assert!(!once.load(Ordering::SeqCst));
    assert!(hook_ran.load(Ordering::SeqCst));
    for id in 0..row_count {
        assert!(
            watch
                .map()
                .get_value(&Arc::<str>::from(id.to_string()))
                .is_some_and(|item| item.address.as_deref() == Some("new")),
            "old replacement resumed over a newer initial map"
        );
    }
    assert!(watch.ready().get());
    drop(guards);
    drop(cells);
    drop(watch);
    transport.connected(false);
    assert_eq!(native.subscription_count(), 0);
}

#[test]
fn per_key_reconnect_does_not_overwrite_newer_initial_map() {
    per_key_reconnect_preserves_new_initial(2);
}

#[test]
fn wide_per_key_reconnect_preserves_new_map_without_fanout_deadlock() {
    per_key_reconnect_preserves_new_initial(80);
}

#[test]
fn ready_observer_disconnect_leaves_current_readiness_false() {
    let fixture = setup(1);
    assert!(fixture.is_ok());
    let Ok((transport, client, native)) = fixture else {
        return;
    };
    transport.connected(true);
    let watch = native.try_watch_query_state(GetPeerServers {});
    assert!(watch.is_ok());
    let Ok(watch) = watch else {
        return;
    };
    let transaction = transport
        .frames()
        .first()
        .map(tx)
        .unwrap_or_default()
        .to_owned();
    let callback_transport = transport.clone();
    let once = AtomicBool::new(true);
    let guard = watch.ready().subscribe(move |signal| {
        if let hyphae::Signal::Value(ready) = signal
            && **ready
            && once.swap(false, Ordering::SeqCst)
        {
            callback_transport.connected(false);
        }
    });
    initial(&client, &transaction);
    assert!(!watch.ready().get());
    assert!(matches!(
        transport.status.get(),
        ConnectionStatus::Disconnected
    ));
    drop(guard);
}
