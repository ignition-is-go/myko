//! Writes a writer made but did not wait for.
//!
//! A write routed into a partition is admitted (validated and recorded) on
//! the writer's path, and applied in admission order behind the writes sent
//! there before it. A writer normally waits for those, so that once its
//! write returns, everything it reads next observes it. Waiting means waiting
//! for the partition's whole backlog, other writers' work included.
//!
//! [`PendingWrites::capture`] lets a writer skip that wait: the writes it
//! makes inside are applied behind the backlog on the partition's worker,
//! and it gets back a token per write instead. Whatever must observe those
//! writes waits for the tokens, not for the partition.

use std::{cell::RefCell, sync::Arc};

use parking_lot::{Condvar, Mutex};

type Waiter = Box<dyn FnOnce() + Send>;

thread_local! {
    static CAPTURED: RefCell<Option<Vec<Arc<Latch>>>> = const { RefCell::new(None) };
}

/// Released once its write has been applied, or discarded with its
/// partition's queue.
#[derive(Default)]
struct Latch {
    /// `None` once released.
    waiters: Mutex<Option<Vec<Waiter>>>,
    released: Condvar,
}

impl Latch {
    fn release(&self) {
        let waiters = self.waiters.lock().take();
        self.released.notify_all();
        for waiter in waiters.into_iter().flatten() {
            waiter();
        }
    }
}

/// Held by a queued write; releases its latch when the write has run or was
/// dropped unrun.
pub struct Applied(Arc<Latch>);

impl Drop for Applied {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Whether this thread is capturing its pending writes.
pub fn capturing() -> bool {
    CAPTURED.with(|captured| captured.borrow().is_some())
}

/// A token for a write about to be queued, when this thread is capturing.
pub fn record() -> Option<Applied> {
    CAPTURED.with(|captured| {
        let mut captured = captured.borrow_mut();
        let latches = captured.as_mut()?;
        let latch = Arc::new(Latch {
            waiters: Mutex::new(Some(Vec::new())),
            released: Condvar::new(),
        });
        latches.push(Arc::clone(&latch));
        Some(Applied(latch))
    })
}

/// Writes made by one writer and not yet known to be applied.
#[derive(Clone, Default)]
pub struct PendingWrites {
    latches: Vec<Arc<Latch>>,
}

impl PendingWrites {
    /// Run `write` without waiting for partitions' backlogs, and return the
    /// writes it left queued.
    pub fn capture<R>(write: impl FnOnce() -> R) -> (R, Self) {
        /// Puts back the enclosing capture, which also owns these writes.
        struct Restore(Option<Vec<Arc<Latch>>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let outer = self.0.take();
                CAPTURED.with(|captured| {
                    let mut captured = captured.borrow_mut();
                    let inner = std::mem::replace(&mut *captured, outer);
                    if let (Some(outer), Some(inner)) = (captured.as_mut(), inner) {
                        outer.extend(inner);
                    }
                });
            }
        }
        let restore = Restore(CAPTURED.with(|captured| captured.borrow_mut().replace(Vec::new())));
        let result = write();
        let latches = CAPTURED.with(|captured| captured.borrow().clone().unwrap_or_default());
        drop(restore);
        let mut pending = Self { latches };
        pending.prune();
        (result, pending)
    }

    fn prune(&mut self) {
        self.latches.retain(|latch| latch.waiters.lock().is_some());
    }

    /// Whether every write is known to be applied.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.latches
            .iter()
            .all(|latch| latch.waiters.lock().is_none())
    }

    /// Add `other`'s writes, forgetting those already applied.
    pub fn extend(&mut self, other: Self) {
        self.latches.extend(other.latches);
        self.prune();
    }

    /// Call `then` once every write is applied: now, or on the thread that
    /// applies the last of them.
    pub fn when_applied(&self, then: impl FnOnce() + Send + 'static) {
        let then: Waiter = Box::new(then);
        for latch in &self.latches {
            let mut waiters = latch.waiters.lock();
            if let Some(waiters) = waiters.as_mut() {
                let rest = self.clone();
                waiters.push(Box::new(move || rest.when_applied(then)));
                return;
            }
        }
        then();
    }

    /// Block until every write is applied.
    pub fn wait(&self) {
        for latch in &self.latches {
            let mut waiters = latch.waiters.lock();
            while waiters.is_some() {
                latch.released.wait(&mut waiters);
            }
        }
    }
}
