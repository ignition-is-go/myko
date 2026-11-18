//! Local coordination for conflict-checked history operations.
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::server::OperationProvenance;

thread_local! {
    static HELD: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

#[derive(Default)]
pub struct TransactionCoordinator {
    gate: RwLock<()>,
    history_enabled: AtomicBool,
    revision: Arc<AtomicU64>,
    active: Arc<Mutex<HashMap<Arc<str>, Activity>>>,
}

#[derive(Default)]
struct Activity {
    commands: usize,
    deliveries: usize,
    provenance: Option<OperationProvenance>,
    completion: Option<(Arc<dyn crate::server::Persister>, crate::identity::UserId)>,
    invalid: bool,
    actor: Option<crate::identity::UserId>,
}

/// Keeps a causal command alive while it crosses an asynchronous saga stream.
pub struct CommandCausation {
    pub(crate) provenance: Option<OperationProvenance>,
    _activity: Arc<CommandActivity>,
}

pub struct CommandActivity {
    states: Arc<Mutex<HashMap<Arc<str>, Activity>>>,
    tx: Arc<str>,
    revision: Arc<AtomicU64>,
}

impl Drop for CommandActivity {
    fn drop(&mut self) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut completed = false;
        if let Some(state) = states.get_mut(&self.tx) {
            state.commands = state.commands.saturating_sub(1);
            if state.commands == 0
                && state.deliveries == 0
                && let Some(state) = states.remove(&self.tx)
                && !state.invalid
                && let Some((backend, user)) = state.completion
            {
                if let Err(error) = backend.complete_transaction(&self.tx, &user) {
                    tracing::error!(%error, "transaction completion could not be persisted");
                }
                completed = true;
            }
        }
        // Completion must be enqueued before a new context can reuse this tx.
        drop(states);
        if completed {
            self.revision.fetch_add(1, Ordering::Release);
        }
    }
}

struct Held(usize);
impl Drop for Held {
    fn drop(&mut self) {
        HELD.with(|held| {
            held.borrow_mut().retain(|key| *key != self.0);
        });
    }
}

impl TransactionCoordinator {
    pub(crate) fn history_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    pub(crate) fn enable_history(&self) {
        self.history_enabled.store(true, Ordering::Release);
    }

    pub(crate) fn coordinate<T>(&self, exclusive: bool, action: impl FnOnce() -> T) -> T {
        if !exclusive && !self.history_enabled.load(Ordering::Acquire) {
            return action();
        }
        let key = std::ptr::from_ref(self).addr();
        if HELD.with(|held| held.borrow().contains(&key)) {
            return action();
        }
        // Release authority before the reactive batch drains callbacks.
        hyphae::batch(|| {
            if exclusive {
                let _guard = self
                    .gate
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                HELD.with(|held| held.borrow_mut().push(key));
                let _held = Held(key);
                action()
            } else {
                let _guard = self
                    .gate
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                HELD.with(|held| held.borrow_mut().push(key));
                let _held = Held(key);
                action()
            }
        })
    }

    pub(crate) fn begin(
        &self,
        tx: Arc<str>,
        actor: Option<crate::identity::UserId>,
    ) -> Arc<CommandActivity> {
        {
            let mut states = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = states.entry(tx.clone()).or_default();
            if let Some(actor) = actor {
                if state
                    .actor
                    .as_ref()
                    .is_some_and(|previous| *previous != actor)
                {
                    state.invalid = true;
                } else {
                    state.actor = Some(actor);
                }
            }
            state.commands = state.commands.saturating_add(1);
            drop(states);
        }
        Arc::new(CommandActivity {
            states: self.active.clone(),
            tx,
            revision: self.revision.clone(),
        })
    }

    pub(crate) fn busy(&self, tx: &str) -> bool {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tx)
            .is_some_and(|state| state.commands > 0 || state.deliveries > 0)
    }

    pub(crate) fn busy_user(&self, user: &crate::identity::UserId, except: &str) -> bool {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|(tx, state)| {
                tx.as_ref() != except
                    && state.actor.as_ref() == Some(user)
                    && (state.commands > 0 || state.deliveries > 0)
            })
    }

    pub(crate) fn recorded(
        &self,
        record: &crate::server::EventRecord,
        backend: Arc<dyn crate::server::Persister>,
    ) {
        if self.history_enabled.load(Ordering::Acquire) && backend.supports_provenance() {
            self.revision.fetch_add(1, Ordering::Release);
        }
        if record.provenance.is_none() {
            return;
        }
        let mut states = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = states.get_mut(&record.event.tx) {
            let user = record
                .provenance
                .as_ref()
                .and_then(|p| p.actor_user_id.as_ref());
            if let Some(user) = user.filter(|_| backend.supports_provenance()) {
                if state.actor.is_none() {
                    state.actor = Some(user.clone());
                }
                if state.completion.as_ref().is_some_and(|(previous, actor)| {
                    !Arc::ptr_eq(previous, &backend) || actor != user
                }) {
                    state.invalid = true;
                } else if state.completion.is_none() {
                    state.completion = Some((backend, user.clone()));
                }
            } else {
                state.invalid = true;
            }
        }
        drop(states);
    }

    pub(crate) fn queue(
        &self,
        tx: Arc<str>,
        provenance: Option<OperationProvenance>,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        let mut states = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = states.entry(tx).or_default();
        if state.deliveries > 0
            && state
                .provenance
                .as_ref()
                .and_then(|p| p.actor_user_id.as_ref())
                != provenance.as_ref().and_then(|p| p.actor_user_id.as_ref())
        {
            state.invalid = true;
            state.provenance = None;
        } else if !state.invalid {
            state.provenance = provenance;
        }
        state.deliveries = state.deliveries.saturating_add(count);
        drop(states);
    }

    pub(crate) fn receive(&self, tx: &Arc<str>) -> CommandCausation {
        let activity = self.begin(tx.clone(), None);
        let mut states = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let provenance = states.get_mut(tx).and_then(|state| {
            state.deliveries = state.deliveries.saturating_sub(1);
            state.provenance.clone()
        });
        drop(states);
        CommandCausation {
            provenance,
            _activity: activity,
        }
    }
}
