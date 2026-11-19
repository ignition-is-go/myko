//! Writes made while holding a lock that a subscriber callback also takes.
//!
//! A hyphae write returns once it has settled, and another thread may be the
//! one that settles it. If the writer holds a lock that a subscriber callback
//! on that thread needs, the two wait on each other. The writes inside
//! [`locked_writes`] are only queued, in the order the lock was held, and are
//! settled after the outermost call returns and its lock is released.

use std::cell::Cell;

thread_local! {
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

struct Exit;

impl Drop for Exit {
    fn drop(&mut self) {
        DEPTH.set(DEPTH.get().saturating_sub(1));
    }
}

/// Run `f`, which takes the lock and releases it before returning, with every
/// hyphae write it makes queued as one event set.
///
/// The outermost call on a thread waits for those writes to settle after `f`
/// returns. A nested call does not wait, because the outer call may still
/// hold its lock.
pub fn locked_writes<R>(f: impl FnOnce() -> R) -> R {
    locked_writes_under((), f)
}

/// [`locked_writes`] for a lock the caller already holds, passed as `lock`.
///
/// `lock` is released after the writes of `f` are queued and before they are
/// waited on. Two threads therefore queue their writes in the order they held
/// the lock, and a subscriber is told of the changes in the order they were
/// made. Releasing inside `f` would let the second thread queue first.
///
/// A call nested in an event set that is still being built cannot keep that
/// order: its writes are queued when the outer set closes, after `lock` is
/// released.
pub fn locked_writes_under<L, R>(lock: L, f: impl FnOnce() -> R) -> R {
    DEPTH.set(DEPTH.get().saturating_add(1));
    let result = {
        let _exit = Exit;
        hyphae::enqueue(&[], None, f)
    };
    drop(lock);
    if DEPTH.get() == 0 {
        hyphae::flush();
    }
    result
}
