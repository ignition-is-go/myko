//! Global client registry for sending messages to connected WebSocket clients.
//!
//! Provides a thread-safe mapping from `client_id` to `WsWriter`,
//! enabling any part of the server to send messages to specific clients.

use std::{
    fmt,
    sync::{Arc, OnceLock, Weak},
};

use hyphae::{Cell, CellImmutable, CellMap, MapExt, Materialize};
#[cfg(not(target_arch = "wasm32"))]
use hyphae::{MapEntriesExt, MapQuery};
use parking_lot::Mutex;
use serde::Serialize;

use super::{CommandValueKey, WsWriter};
use crate::{
    command::{CommandId, CommandRequest},
    wire::{MykoMessage, encode_command_message},
};

/// Thread-safe registry mapping client IDs to their WebSocket writers.
pub struct ClientRegistry {
    writers: CellMap<Arc<str>, RegisteredWriter>,
}

#[derive(Clone)]
struct RegisteredWriter {
    slot: Arc<Mutex<Option<Arc<dyn WsWriter>>>>,
    identity: Weak<dyn WsWriter>,
}

impl RegisteredWriter {
    fn new(writer: Arc<dyn WsWriter>) -> Self {
        Self {
            identity: Arc::downgrade(&writer),
            slot: Arc::new(Mutex::new(Some(writer))),
        }
    }

    fn acquire(&self) -> Option<Arc<dyn WsWriter>> {
        let slot = self.slot.lock();
        let writer = slot.clone();
        drop(slot);
        writer
    }

    fn revoke(&self) -> Option<Arc<dyn WsWriter>> {
        let mut slot = self.slot.lock();
        let writer = slot.take();
        drop(slot);
        writer
    }
}

impl fmt::Debug for RegisteredWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RegisteredWriter").finish()
    }
}

impl PartialEq for RegisteredWriter {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.identity, &other.identity)
    }
}

impl ClientRegistry {
    fn new() -> Self {
        Self {
            writers: CellMap::new(),
        }
    }

    /// Register a client's writer.
    pub fn register(&self, client_id: Arc<str>, writer: Arc<dyn WsWriter>) {
        let previous = self
            .writers
            .insert(client_id, RegisteredWriter::new(writer));
        if let Some(previous) = previous {
            let retired = previous.revoke();
            drop(retired);
        }
    }

    /// Unregister a client's writer.
    pub fn unregister(&self, client_id: &str) {
        if let Some(removed) = self.writers.remove(&Arc::<str>::from(client_id)) {
            let retired = removed.revoke();
            drop(retired);
        }
    }

    /// Reactively track whether a client has a live WebSocket writer.
    ///
    /// This derives directly from the registry's per-key `CellMap` observation;
    /// persisted `Client` entities are deliberately not part of liveness.
    #[must_use]
    pub(crate) fn watch_connected(&self, client_id: &Arc<str>) -> Cell<bool, CellImmutable> {
        self.writers
            .get(client_id)
            .map(Option::is_some)
            .materialize()
    }

    /// Reactively project the ids that currently have live WebSocket writers.
    ///
    /// Writer values stay private to the registry; consumers can compose this
    /// membership plan with entity maps without exposing transport handles.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn connected_ids(&self) -> impl MapQuery<Key = Arc<str>, Value = ()> + use<> {
        self.writers
            .clone()
            .map_entries(|client_id, _| (client_id.clone(), ()))
    }

    /// Send a message to a specific client.
    ///
    /// Returns `true` if the client was found and the message was sent.
    #[must_use]
    pub fn send_to(&self, client_id: &str, msg: MykoMessage) -> bool {
        self.writers
            .get_value(&Arc::<str>::from(client_id))
            .and_then(|writer| writer.acquire())
            .is_some_and(|writer| {
                writer.send(msg);
                true
            })
    }

    /// Queue an event command for a client.
    ///
    /// Returns `false` when the client is not connected, the command cannot
    /// be encoded, or the client's queue is already closed; the command was
    /// not queued and will not be delivered.
    pub fn send_command_request_to<C>(&self, client_id: &str, request: &CommandRequest<C>) -> bool
    where
        C: CommandId + Serialize,
    {
        self.send_command(client_id, None, request)
    }

    /// Send a *value* command: while it is still queued for the client, a
    /// later value command with the same `key` replaces it in place.
    ///
    /// Use this only when the newest command for `key` makes every older one
    /// redundant. Commands whose every instance must arrive (events) go
    /// through [`Self::send_command_request_to`], which never coalesces.
    pub fn send_value_command_request_to<C>(
        &self,
        client_id: &str,
        key: CommandValueKey,
        request: &CommandRequest<C>,
    ) -> bool
    where
        C: CommandId + Serialize,
    {
        self.send_command(client_id, Some(key), request)
    }

    fn send_command<C>(
        &self,
        client_id: &str,
        key: Option<CommandValueKey>,
        request: &CommandRequest<C>,
    ) -> bool
    where
        C: CommandId + Serialize,
    {
        let Some(writer) = self
            .writers
            .get_value(&Arc::<str>::from(client_id))
            .and_then(|writer| writer.acquire())
        else {
            return false;
        };

        let command_id = request.command_id().to_string();
        let protocol = writer.protocol();

        match encode_command_message(protocol, request) {
            Ok(payload) => {
                let tx = request.tx.clone();
                match key {
                    Some(key) => {
                        writer.try_send_serialized_value_command(key, tx, command_id, payload)
                    }
                    None => writer.try_send_serialized_command(tx, command_id, payload),
                }
            }
            Err(err) => {
                tracing::error!(
                    "Failed to serialize command {} for client {}: {}",
                    request.command_id(),
                    client_id,
                    err
                );
                false
            }
        }
    }

    /// Number of currently connected clients.
    #[must_use]
    pub fn len(&self) -> usize {
        self.writers.keys_snapshot().len()
    }

    /// Returns true when there are no connected clients.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Global accessor (same pattern as sync_client)
// ─────────────────────────────────────────────────────────────────────────────

static CLIENT_REGISTRY: OnceLock<Arc<ClientRegistry>> = OnceLock::new();

/// Initialize the global client registry.
///
/// Safe to call multiple times — only the first call has effect.
pub fn init_client_registry() {
    let _ = CLIENT_REGISTRY.set(Arc::new(ClientRegistry::new()));
}

/// Get the global client registry.
pub fn client_registry() -> Arc<ClientRegistry> {
    CLIENT_REGISTRY
        .get_or_init(|| Arc::new(ClientRegistry::new()))
        .clone()
}

/// Try to get the global client registry.
///
/// Returns None if `init_client_registry()` has not been called.
pub fn try_client_registry() -> Option<Arc<ClientRegistry>> {
    CLIENT_REGISTRY.get().cloned()
}

#[cfg(test)]
mod liveness_tests {
    use hyphae::{Gettable, Signal, Watchable};

    use super::*;
    use crate::{server::client_session::WsWriter, wire::message::MykoMessage};

    struct NullWriter;

    struct CallbackWriter {
        send: Box<dyn Fn() + Send + Sync>,
        on_drop: Box<dyn Fn() + Send + Sync>,
    }

    impl WsWriter for CallbackWriter {
        fn send(&self, _msg: MykoMessage) {
            (self.send)();
        }

        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: crate::wire::command::EncodedCommandMessage,
        ) {
            (self.send)();
        }
    }

    impl Drop for CallbackWriter {
        fn drop(&mut self) {
            (self.on_drop)();
        }
    }

    fn message() -> MykoMessage {
        MykoMessage::Benchmark(serde_json::Value::Null)
    }
    impl WsWriter for NullWriter {
        fn send(&self, _msg: MykoMessage) {}
        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: crate::wire::command::EncodedCommandMessage,
        ) {
        }
    }

    #[test]
    fn unregister_releases_the_last_writer_without_another_mutation() {
        let registry = ClientRegistry::new();
        let writer = Arc::new(NullWriter);
        let weak = Arc::downgrade(&writer);
        registry.register("a".into(), writer);
        assert!(weak.upgrade().is_some());

        registry.unregister("a");

        assert!(registry.is_empty());
        assert!(weak.upgrade().is_none(), "removed writer remains owned");
    }

    #[test]
    fn replacing_a_writer_releases_the_previous_writer() {
        let registry = ClientRegistry::new();
        let previous = Arc::new(NullWriter);
        let weak_previous = Arc::downgrade(&previous);
        registry.register("a".into(), previous);
        let current = Arc::new(NullWriter);
        let weak_current = Arc::downgrade(&current);

        registry.register("a".into(), current);

        assert_eq!(registry.len(), 1);
        assert!(weak_current.upgrade().is_some());
        assert!(
            weak_previous.upgrade().is_none(),
            "replaced writer remains owned"
        );
    }

    #[test]
    fn same_arc_registration_preserves_observed_membership_and_routing() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let registry = ClientRegistry::new();
        let id = Arc::<str>::from("a");
        let connected = registry.watch_connected(&id);
        let ids = registry.connected_ids().materialize();
        let observed_writer = registry.writers.get(&id).materialize();
        let notifications = Arc::new(AtomicUsize::new(0));
        let _guard = observed_writer.subscribe({
            let notifications = notifications.clone();
            move |_| {
                notifications.fetch_add(1, Ordering::SeqCst);
            }
        });
        let writer = Arc::new(NullWriter);
        let weak = Arc::downgrade(&writer);
        registry.register(id.clone(), writer.clone());
        let before = notifications.load(Ordering::SeqCst);
        registry.register(id.clone(), writer);
        assert_eq!(notifications.load(Ordering::SeqCst), before);
        assert!(connected.get());
        assert!(ids.get_value(&id).is_some());
        assert!(registry.send_to(&id, message()));

        registry.unregister(&id);
        registry.unregister(&id);

        assert!(!connected.get());
        assert!(ids.get_value(&id).is_none());
        assert!(!registry.send_to(&id, message()));
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn revoked_lookup_cannot_acquire_or_redirect_to_a_replacement() {
        let registry = ClientRegistry::new();
        let id = Arc::<str>::from("a");
        let previous = Arc::new(NullWriter);
        let weak = Arc::downgrade(&previous);
        registry.register(id.clone(), previous);
        let looked_up = registry.writers.get_value(&id);
        assert!(looked_up.is_some());
        registry.register(id.clone(), Arc::new(NullWriter));

        assert!(matches!(looked_up, Some(writer) if writer.acquire().is_none()));
        assert!(weak.upgrade().is_none());
        assert!(registry.send_to(&id, message()));
        registry.unregister(&id);
    }

    #[test]
    fn acquired_writer_finishes_once_after_concurrent_replacement() {
        use std::sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        };

        let registry = Arc::new(ClientRegistry::new());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(CallbackWriter {
            send: Box::new({
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.wait();
                    release.wait();
                }
            }),
            on_drop: Box::new(|| {}),
        });
        let weak = Arc::downgrade(&writer);
        registry.register("a".into(), writer);
        let sending = {
            let registry = registry.clone();
            std::thread::spawn(move || registry.send_to("a", message()))
        };
        entered.wait();
        let replacement_calls = Arc::new(AtomicUsize::new(0));
        registry.register(
            "a".into(),
            Arc::new(CallbackWriter {
                send: Box::new({
                    let replacement_calls = replacement_calls.clone();
                    move || {
                        replacement_calls.fetch_add(1, Ordering::SeqCst);
                    }
                }),
                on_drop: Box::new(|| {}),
            }),
        );
        assert!(weak.upgrade().is_some());
        release.wait();
        assert!(matches!(sending.join(), Ok(true)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(replacement_calls.load(Ordering::SeqCst), 0);
        assert!(weak.upgrade().is_none());
        registry.unregister("a");
    }

    #[test]
    fn writer_send_and_destructor_can_reenter_the_registry() {
        let registry = Arc::new(ClientRegistry::new());
        let writer = Arc::new(CallbackWriter {
            send: Box::new({
                let registry = registry.clone();
                move || registry.unregister("a")
            }),
            on_drop: Box::new({
                let registry = registry.clone();
                move || registry.register("from-drop".into(), Arc::new(NullWriter))
            }),
        });
        let weak = Arc::downgrade(&writer);
        registry.register("a".into(), writer);

        assert!(registry.send_to("a", message()));

        assert!(weak.upgrade().is_none());
        assert!(!registry.send_to("a", message()));
        assert!(registry.send_to("from-drop", message()));
        registry.unregister("from-drop");
    }

    #[test]
    fn synchronous_membership_observer_can_mutate_another_registration() {
        let registry = Arc::new(ClientRegistry::new());
        let connected = registry.watch_connected(&Arc::from("a"));
        let _guard = connected.subscribe({
            let registry = registry.clone();
            move |signal| {
                if matches!(signal, Signal::Value(value) if **value) {
                    registry.register("observer".into(), Arc::new(NullWriter));
                    assert!(registry.send_to("observer", message()));
                    registry.unregister("observer");
                }
            }
        });
        registry.register("a".into(), Arc::new(NullWriter));
        assert!(connected.get());
        assert_eq!(registry.len(), 1);
        registry.unregister("a");
        assert!(!connected.get());
        assert!(registry.is_empty());
    }

    #[test]
    fn same_key_observer_replacement_retires_only_displaced_slots() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let registry = Arc::new(ClientRegistry::new());
        let id = Arc::<str>::from("a");
        let first = Arc::new(NullWriter);
        let second = Arc::new(NullWriter);
        let final_writer = Arc::new(NullWriter);
        let first_weak = Arc::downgrade(&first);
        let second_weak = Arc::downgrade(&second);
        let final_weak = Arc::downgrade(&final_writer);
        registry.register(id.clone(), first);
        let armed = Arc::new(AtomicBool::new(false));
        let observed_writer = registry.writers.get(&id).materialize();
        let _guard = observed_writer.subscribe({
            let registry = registry.clone();
            let armed = armed.clone();
            let final_writer = Mutex::new(Some(final_writer));
            move |_| {
                if armed.swap(false, Ordering::SeqCst) {
                    let writer = { final_writer.lock().take() };
                    assert!(writer.is_some());
                    if let Some(writer) = writer {
                        registry.register("a".into(), writer);
                    }
                }
            }
        });
        armed.store(true, Ordering::SeqCst);

        registry.register(id.clone(), second);

        assert!(first_weak.upgrade().is_none());
        assert!(second_weak.upgrade().is_none());
        assert!(final_weak.upgrade().is_some());
        assert_eq!(registry.len(), 1);
        assert!(registry.send_to(&id, message()));
        registry.unregister(&id);
        assert!(final_weak.upgrade().is_none());
    }

    #[test]
    fn typed_dispatch_allows_protocol_and_serializer_reentry_without_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use serde::ser::SerializeMap;

        struct Writer {
            registry: Arc<ClientRegistry>,
            calls: Arc<AtomicUsize>,
        }
        impl WsWriter for Writer {
            fn send(&self, _msg: MykoMessage) {}
            fn protocol(&self) -> crate::client::MykoProtocol {
                self.registry
                    .register("protocol".into(), Arc::new(NullWriter));
                crate::client::MykoProtocol::JSON
            }
            fn send_serialized_command(
                &self,
                _tx: Arc<str>,
                _command_id: String,
                _payload: crate::wire::EncodedCommandMessage,
            ) {
                assert!(self.registry.send_to("protocol", message()));
                self.registry.unregister("protocol");
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Command(Arc<ClientRegistry>);
        impl CommandId for Command {
            fn command_id(&self) -> Arc<str> {
                "registry-test".into()
            }
        }
        impl Serialize for Command {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.unregister("a");
                serializer.serialize_map(Some(0))?.end()
            }
        }

        let registry = Arc::new(ClientRegistry::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(Writer {
            registry: registry.clone(),
            calls: calls.clone(),
        });
        let weak = Arc::downgrade(&writer);
        registry.register("a".into(), writer);
        let request = CommandRequest::new(Command(registry.clone()));

        assert!(registry.send_command_request_to("a", &request));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(registry.is_empty());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn cellmap_liveness_reflects_registered_writers() {
        let registry = ClientRegistry::new();
        let client_id = Arc::<str>::from("a");
        let connected = registry.watch_connected(&client_id);
        assert!(!connected.get());
        registry.register("a".into(), Arc::new(NullWriter));
        assert!(connected.get());
        registry.unregister("a");
        assert!(!connected.get());
    }
}
