//! Client-wide observation of outgoing command lifecycles.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// One step in the life of a command sent by this client.
///
/// Every observed command produces exactly one `Sent` followed by exactly one
/// `Resolved`, unless the command never resolves. Commands are not failed on
/// disconnect: frames sent while offline are queued and flushed on reconnect,
/// and their response handlers stay registered until the server answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandLifecycle {
    /// The command was registered and handed to the transport (or its queue).
    Sent { tx: Arc<str>, command_id: Arc<str> },
    /// The server answered, or the client failed the command locally.
    Resolved {
        tx: Arc<str>,
        command_id: Arc<str>,
        result: Result<(), Arc<str>>,
    },
}

type Observer = Arc<dyn Fn(&CommandLifecycle) + Send + Sync>;

/// Registered lifecycle observers.
///
/// `count` lets the per-command path skip all lifecycle work with one atomic
/// load when nothing is registered.
#[derive(Default)]
pub(super) struct CommandObservers {
    count: AtomicUsize,
    next_id: AtomicU64,
    observers: Mutex<Vec<(u64, Observer)>>,
}

impl CommandObservers {
    pub(super) fn is_observed(&self) -> bool {
        self.count.load(Ordering::Acquire) > 0
    }

    pub(super) fn add(&self, observer: Observer) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut observers = self
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observers.push((id, observer));
        self.count.store(observers.len(), Ordering::Release);
        id
    }

    pub(super) fn remove(&self, id: u64) {
        let mut observers = self
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observers.retain(|(observer_id, _)| *observer_id != id);
        self.count.store(observers.len(), Ordering::Release);
    }

    /// Deliver `event` to a snapshot of the observers, outside the lock, so an
    /// observer may send commands or register and drop observers.
    pub(super) fn emit(&self, event: &CommandLifecycle) {
        let snapshot: Vec<Observer> = self
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(_, observer)| Arc::clone(observer))
            .collect();
        for observer in snapshot {
            observer(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use autosocket::WsFrame;
    use hyphae::Gettable as _;
    use serde::{Serialize, Serializer};

    use super::CommandLifecycle;
    use crate::{
        client::MykoClient,
        core::command::CommandId,
        wire::{CommandError, CommandResponse, MykoMessage},
    };

    #[derive(Clone, Serialize)]
    struct Ping {
        value: u32,
    }

    impl CommandId for Ping {
        fn command_id(&self) -> Arc<str> {
            "Ping".into()
        }
    }

    /// A command whose serialization always fails, driving the local-failure path.
    #[derive(Clone)]
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("refused"))
        }
    }

    impl CommandId for Unserializable {
        fn command_id(&self) -> Arc<str> {
            "Unserializable".into()
        }
    }

    type Log = Arc<Mutex<Vec<CommandLifecycle>>>;

    fn record(client: &MykoClient) -> (Log, autosocket::CallbackGuard) {
        let log: Log = Arc::default();
        let sink = Arc::clone(&log);
        let guard = client.on_command_lifecycle(move |event| {
            sink.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event.clone());
        });
        (log, guard)
    }

    fn events(log: &Log) -> Vec<CommandLifecycle> {
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn deliver(client: &MykoClient, message: &MykoMessage) {
        let text = serde_json::to_string(message).unwrap_or_default();
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(text));
    }

    fn sent_tx(log: &Log) -> Option<Arc<str>> {
        match events(log).first() {
            Some(CommandLifecycle::Sent { tx, .. }) => Some(Arc::clone(tx)),
            _ => None,
        }
    }

    #[test]
    fn success_emits_sent_then_resolved_ok() {
        let client = MykoClient::new_with_auto_reconnect(false);
        let (log, _guard) = record(&client);
        let result = client.send_command::<_, u32>(&Ping { value: 1 });
        let tx = sent_tx(&log).unwrap_or_default();
        deliver(
            &client,
            &MykoMessage::CommandResponse(CommandResponse {
                response: serde_json::json!(7),
                tx: tx.to_string(),
            }),
        );
        assert_eq!(result.get(), Some(Ok(7)));
        assert_eq!(
            events(&log),
            vec![
                CommandLifecycle::Sent {
                    tx: Arc::clone(&tx),
                    command_id: "Ping".into()
                },
                CommandLifecycle::Resolved {
                    tx,
                    command_id: "Ping".into(),
                    result: Ok(())
                },
            ]
        );
    }

    #[test]
    fn server_error_emits_resolved_err() {
        let client = MykoClient::new_with_auto_reconnect(false);
        let (log, _guard) = record(&client);
        let _result = client.send_command::<_, u32>(&Ping { value: 1 });
        let tx = sent_tx(&log).unwrap_or_default();
        deliver(
            &client,
            &MykoMessage::CommandError(CommandError {
                tx: tx.to_string(),
                command_id: "Ping".into(),
                message: "denied".into(),
            }),
        );
        assert_eq!(
            events(&log).get(1),
            Some(&CommandLifecycle::Resolved {
                tx,
                command_id: "Ping".into(),
                result: Err("denied".into()),
            })
        );
    }

    #[test]
    fn encode_failure_emits_sent_then_resolved_err() {
        let client = MykoClient::new_with_auto_reconnect(false);
        let (log, _guard) = record(&client);
        let result = client.send_command::<_, u32>(&Unserializable);
        let events = events(&log);
        assert_eq!(events.len(), 2);
        assert!(
            matches!(events.first(), Some(CommandLifecycle::Sent { command_id, .. }) if &**command_id == "Unserializable")
        );
        assert!(matches!(
            events.get(1),
            Some(CommandLifecycle::Resolved { result: Err(_), .. })
        ));
        assert!(matches!(result.get(), Some(Err(_))));
    }

    #[test]
    fn unobserved_commands_capture_nothing_and_still_resolve() {
        let client = MykoClient::new_with_auto_reconnect(false);
        assert!(!client.inner.command_observers.is_observed());
        let result = client.send_command::<_, u32>(&Ping { value: 1 });
        let pending = client
            .inner
            .command_response_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(tx, (command_id, _))| (tx.clone(), command_id.clone()))
            .next();
        assert!(
            pending.is_some(),
            "send_command registers a response handler"
        );
        let (tx, command_id) = pending.unwrap_or_default();
        assert_eq!(
            command_id, None,
            "no command id is captured without observers"
        );
        // An observer registered after send does not see a stray Resolved.
        let (log, _guard) = record(&client);
        deliver(
            &client,
            &MykoMessage::CommandResponse(CommandResponse {
                response: serde_json::json!(3),
                tx,
            }),
        );
        assert_eq!(result.get(), Some(Ok(3)));
        assert!(events(&log).is_empty());
    }

    #[test]
    fn dropping_guard_unsubscribes() {
        let client = MykoClient::new_with_auto_reconnect(false);
        let (log, guard) = record(&client);
        drop(guard);
        assert!(!client.inner.command_observers.is_observed());
        let _result = client.send_command::<_, u32>(&Ping { value: 1 });
        assert!(events(&log).is_empty());
    }

    #[test]
    fn observer_may_send_commands_reentrantly() {
        let client = MykoClient::new_with_auto_reconnect(false);
        let inner_client = client.clone();
        let count = Arc::new(Mutex::new(0usize));
        let seen = Arc::clone(&count);
        let _guard = client.on_command_lifecycle(move |event| {
            let mut seen = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *seen += 1;
            if *seen == 1 && matches!(event, CommandLifecycle::Sent { .. }) {
                drop(seen);
                let _nested = inner_client.send_command::<_, u32>(&Ping { value: 2 });
            }
        });
        let _result = client.send_command::<_, u32>(&Ping { value: 1 });
        assert_eq!(
            *count
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            2
        );
    }
}
