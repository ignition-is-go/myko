//! A connection's ordered outbound queue with latest-value coalescing.
//!
//! Frames leave in the order they were queued. A frame pushed with a
//! [`CommandValueKey`] is a *value*: while an earlier value with the same key
//! is still queued and no *barrier* frame (an event) has been queued since,
//! the new value replaces that one's payload where it stands. Barriers and
//! plain frames are never replaced. Popping is the hand-off to the socket
//! writer; from then on a frame is no longer replaceable.
//!
//! The barrier rule keeps the delivered sequence a subsequence of the sent
//! one: a value is only ever dropped in favour of a newer value for the same
//! key with no event between them, so no value moves ahead of an event it
//! followed, nor behind one it preceded. An event may carry state for the
//! same destination as a value (a press that also writes the current value),
//! and letting a value cross it would leave the receiver with the older one.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use myko::server::CommandValueKey;
use tokio::sync::Notify;

enum Entry<T> {
    Plain(T),
    /// A barrier frame (an event command).
    Event(T),
    /// Index into `Inner::slots`, whose payload a newer value may replace.
    Value(usize),
}

struct Slot<T> {
    key: CommandValueKey,
    item: T,
    /// Barriers queued before this slot; a newer barrier seals it.
    epoch: u64,
}

struct Inner<T> {
    fifo: VecDeque<Entry<T>>,
    slots: Vec<Option<Slot<T>>>,
    free: Vec<usize>,
    queued_values: HashMap<CommandValueKey, usize>,
    barriers: u64,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    ready: Notify,
    senders: AtomicUsize,
    receiver_alive: AtomicBool,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// Commands still queued when the receiver closed, never handed off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Discarded {
    pub events: u64,
    pub values: u64,
}

/// Whether a push queued a new frame or replaced a queued value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    Superseded,
    Closed,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            fifo: VecDeque::new(),
            slots: Vec::new(),
            free: Vec::new(),
            queued_values: HashMap::new(),
            barriers: 0,
        }),
        ready: Notify::new(),
        senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

impl<T> Shared<T> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<T>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T> Sender<T> {
    pub fn is_closed(&self) -> bool {
        !self.shared.receiver_alive.load(Ordering::Acquire)
    }

    /// Queue a frame that values may coalesce around (a state message).
    pub fn push(&self, item: T) -> Pushed {
        self.push_plain(item, false)
    }

    /// Queue an event: never coalesced, and no value queued before it is
    /// replaced by one queued after it.
    pub fn push_barrier(&self, item: T) -> Pushed {
        self.push_plain(item, true)
    }

    fn push_plain(&self, item: T, barrier: bool) -> Pushed {
        if self.is_closed() {
            return Pushed::Closed;
        }
        let mut inner = self.shared.lock();
        // Re-checked under the lock: `Receiver::close` holds it while it
        // flips the flag, so nothing lands after the discard count is taken.
        if self.is_closed() {
            return Pushed::Closed;
        }
        inner.barriers = inner.barriers.wrapping_add(u64::from(barrier));
        inner.fifo.push_back(if barrier {
            Entry::Event(item)
        } else {
            Entry::Plain(item)
        });
        drop(inner);
        self.shared.ready.notify_one();
        Pushed::Queued
    }

    pub fn push_value(&self, key: CommandValueKey, item: T) -> Pushed {
        if self.is_closed() {
            return Pushed::Closed;
        }
        let mut inner = self.shared.lock();
        if self.is_closed() {
            return Pushed::Closed;
        }
        let epoch = inner.barriers;
        if let Some(&index) = inner.queued_values.get(&key)
            && let Some(Some(slot)) = inner.slots.get_mut(index)
            && slot.epoch == epoch
        {
            slot.item = item;
            return Pushed::Superseded;
        }
        let slot = Some(Slot {
            key: key.clone(),
            item,
            epoch,
        });
        let index = if let Some(index) = inner.free.pop() {
            if let Some(free) = inner.slots.get_mut(index) {
                *free = slot;
            }
            index
        } else {
            inner.slots.push(slot);
            inner.slots.len().saturating_sub(1)
        };
        inner.queued_values.insert(key, index);
        inner.fifo.push_back(Entry::Value(index));
        drop(inner);
        self.shared.ready.notify_one();
        Pushed::Queued
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.ready.notify_one();
        }
    }
}

impl<T> Receiver<T> {
    /// Hand the oldest frame to the caller. `None` once every sender is gone
    /// and the queue is empty. Cancel-safe: nothing is taken until it returns.
    pub async fn recv(&self) -> Option<T> {
        loop {
            if let Some(item) = self.try_recv() {
                return Some(item);
            }
            if self.shared.senders.load(Ordering::Acquire) == 0 {
                return self.try_recv();
            }
            self.shared.ready.notified().await;
        }
    }

    pub fn try_recv(&self) -> Option<T> {
        Self::pop(&mut self.shared.lock())
    }

    fn pop(inner: &mut Inner<T>) -> Option<T> {
        match inner.fifo.pop_front()? {
            Entry::Plain(item) | Entry::Event(item) => Some(item),
            Entry::Value(index) => {
                let slot = inner.slots.get_mut(index)?.take()?;
                inner.free.push(index);
                // A sealed slot's key may already name a newer slot.
                if inner.queued_values.get(&slot.key) == Some(&index) {
                    inner.queued_values.remove(&slot.key);
                }
                Some(slot.item)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.shared.lock().fifo.len()
    }

    /// Refuse further pushes and discard what is queued, reporting the
    /// commands among it. Later calls report nothing.
    pub fn close(&self) -> Discarded {
        let mut inner = self.shared.lock();
        self.shared.receiver_alive.store(false, Ordering::Release);
        let mut discarded = Discarded::default();
        for entry in inner.fifo.drain(..) {
            match entry {
                Entry::Plain(_) => {}
                Entry::Event(_) => discarded.events = discarded.events.saturating_add(1),
                Entry::Value(_) => discarded.values = discarded.values.saturating_add(1),
            }
        }
        inner.slots.clear();
        inner.free.clear();
        inner.queued_values.clear();
        discarded
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> CommandValueKey {
        CommandValueKey::new(name)
    }

    fn drain(rx: &Receiver<&'static str>) -> Vec<&'static str> {
        std::iter::from_fn(|| rx.try_recv()).collect()
    }

    #[test]
    fn replacement_keeps_the_first_unsent_position() {
        let (tx, rx) = channel();
        tx.push_value(key("a"), "a1");
        tx.push("m1");
        tx.push_value(key("b"), "b1");
        assert_eq!(tx.push_value(key("a"), "a2"), Pushed::Superseded);
        assert_eq!(drain(&rx), ["a2", "m1", "b1"]);
    }

    #[test]
    fn unkeyed_frames_are_never_coalesced_or_reordered() {
        let (tx, rx) = channel();
        for event in ["e1", "e1", "e2", "e1"] {
            assert_eq!(tx.push_barrier(event), Pushed::Queued);
        }
        assert_eq!(drain(&rx), ["e1", "e1", "e2", "e1"]);
    }

    #[test]
    fn values_never_cross_an_event() {
        let (tx, rx) = channel();
        tx.push_barrier("e1");
        tx.push_value(key("v"), "v1");
        assert_eq!(tx.push_value(key("v"), "v1b"), Pushed::Superseded);
        tx.push_barrier("e2");
        assert_eq!(tx.push_value(key("v"), "v2"), Pushed::Queued);
        assert_eq!(tx.push_value(key("v"), "v2b"), Pushed::Superseded);
        tx.push_barrier("e3");
        assert_eq!(drain(&rx), ["e1", "v1b", "e2", "v2b", "e3"]);
    }

    #[test]
    fn popping_a_sealed_value_keeps_the_newer_one_replaceable() {
        let (tx, rx) = channel();
        tx.push_value(key("v"), "v1");
        tx.push_barrier("e1");
        tx.push_value(key("v"), "v2");
        assert_eq!(rx.try_recv(), Some("v1"));
        assert_eq!(tx.push_value(key("v"), "v3"), Pushed::Superseded);
        assert_eq!(drain(&rx), ["e1", "v3"]);
    }

    #[test]
    fn a_handed_off_value_is_not_replaced() {
        let (tx, rx) = channel();
        tx.push_value(key("v"), "v1");
        assert_eq!(rx.try_recv(), Some("v1"));
        assert_eq!(tx.push_value(key("v"), "v2"), Pushed::Queued);
        assert_eq!(tx.push_value(key("v"), "v3"), Pushed::Superseded);
        assert_eq!(drain(&rx), ["v3"]);
    }

    #[test]
    fn many_values_to_few_keys_stay_bounded_by_keys() {
        let (tx, rx) = channel::<(usize, usize)>();
        let keys: Vec<_> = (0..10).map(|k| key(&k.to_string())).collect();
        for (i, (k, key)) in (0..100_000).zip(keys.iter().enumerate().cycle()) {
            tx.push_value(key.clone(), (k, i));
        }
        assert_eq!(rx.len(), 10);
        let latest: Vec<_> = std::iter::from_fn(|| rx.try_recv()).collect();
        assert_eq!(latest, (0..10).map(|k| (k, 99_990 + k)).collect::<Vec<_>>());
    }

    #[test]
    fn slots_are_reused_after_hand_off() {
        let (tx, rx) = channel();
        for round in 0..1000 {
            tx.push_value(key("a"), round);
            tx.push_value(key("b"), round);
            assert_eq!(rx.try_recv(), Some(round));
            assert_eq!(rx.try_recv(), Some(round));
        }
        assert!(rx.shared.lock().slots.len() <= 2);
    }

    #[tokio::test]
    async fn recv_ends_when_senders_drop_and_queue_drains() {
        let (tx, rx) = channel();
        let other = tx.clone();
        tx.push("e1");
        drop(tx);
        other.push_value(key("v"), "v1");
        drop(other);
        assert_eq!(rx.recv().await, Some("e1"));
        assert_eq!(rx.recv().await, Some("v1"));
        assert_eq!(rx.recv().await, None);
    }

    #[test]
    fn close_counts_undelivered_events_and_values_but_not_frames() {
        let (tx, rx) = channel();
        tx.push("frame");
        tx.push_barrier("event");
        tx.push_value(key("a"), "a1");
        tx.push_value(key("a"), "a2");
        tx.push_value(key("b"), "b1");
        tx.push_barrier("event 2");
        assert_eq!(rx.try_recv(), Some("frame"));
        assert_eq!(
            rx.close(),
            Discarded {
                events: 2,
                values: 2
            }
        );
        assert_eq!(tx.push_barrier("late"), Pushed::Closed);
        assert_eq!(tx.push_value(key("a"), "late"), Pushed::Closed);
        assert_eq!(rx.close(), Discarded::default());
    }

    #[test]
    fn push_after_receiver_drop_reports_closed() {
        let (tx, rx) = channel::<&str>();
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.push("e"), Pushed::Closed);
        assert_eq!(tx.push_value(key("v"), "v"), Pushed::Closed);
    }
}
