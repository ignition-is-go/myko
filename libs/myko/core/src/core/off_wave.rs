//! Graph construction and teardown kept off the propagating thread.
//!
//! Hyphae runs one propagation wave at a time for the whole process. Building
//! or dropping a reactive graph from inside a wave (in a selector callback, a
//! `map`/`project_cell` closure, a subscriber) holds that wave open for as long
//! as the work takes, and every unrelated write in the process queues behind
//! it. Work handed to [`off_wave`] runs on a shared builder pool instead, so a
//! wave only records that something must be built.

use std::sync::{Arc, OnceLock};

use hyphae::{Cell, CellImmutable, CellValue, Mutable, Signal, SubscriptionGuard, Watchable};

type Job = Box<dyn FnOnce() + Send>;

/// Upper bound on builder threads. Builds are mostly independent graph
/// construction; more threads than this mostly contend on shared stores.
const MAX_BUILDERS: usize = 4;

fn builders() -> Option<&'static flume::Sender<Job>> {
    static POOL: OnceLock<Option<flume::Sender<Job>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let (sender, receiver) = flume::unbounded::<Job>();
        let count = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .min(MAX_BUILDERS);
        let mut started = false;
        for index in 0..count {
            let receiver = receiver.clone();
            started |= std::thread::Builder::new()
                .name(format!("myko-off-wave-{index}"))
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        job();
                    }
                })
                .is_ok();
        }
        started.then_some(sender)
    })
    .as_ref()
}

/// Run `job` on the shared builder pool, outside any propagation wave.
///
/// Jobs run in no particular order relative to each other, and a job must not
/// wait for another job. When no builder thread could be started the job runs
/// on the calling thread, so it is never lost.
pub fn off_wave(job: impl FnOnce() + Send + 'static) {
    let job: Job = Box::new(job);
    match builders() {
        Some(sender) => {
            if let Err(flume::SendError(job)) = sender.send(job) {
                job();
            }
        }
        None => job(),
    }
}

/// A cell that holds `placeholder` until `build` has run off the wave, then
/// follows the cell `build` returned.
///
/// Use it where a cell is created inside a propagation and creating it builds
/// a graph. Subscribers see `placeholder` first, then the built cell's current
/// value and each later value; they never miss the built cell's latest value.
/// Dropping every handle to the returned cell releases the built cell on the
/// builder pool as well, so its teardown is also kept out of the wave.
pub fn deferred_cell<T>(
    placeholder: T,
    build: impl FnOnce() -> Cell<T, CellImmutable> + Send + 'static,
) -> Cell<T, CellImmutable>
where
    T: CellValue,
{
    let out = Cell::new(placeholder);
    let follower = out.downgrade();
    let state = Arc::new(parking_lot::Mutex::new(Built::Pending));
    let slot = state.clone();
    off_wave(move || {
        if follower.upgrade().is_none() {
            return;
        }
        let source = build();
        let guard = source.subscribe(move |signal| {
            if let (Signal::Value(value), Some(out)) = (signal, follower.upgrade()) {
                out.set(value.as_ref().clone());
            }
        });
        let mut slot = slot.lock();
        if matches!(*slot, Built::Released) {
            drop(slot);
            drop(guard);
        } else {
            *slot = Built::Following(guard);
        }
    });
    out.own(SubscriptionGuard::from_callback(move || {
        let previous = std::mem::replace(&mut *state.lock(), Built::Released);
        if let Built::Following(guard) = previous {
            off_wave(move || drop(guard));
        }
    }));
    out.lock()
}

enum Built {
    Pending,
    Following(SubscriptionGuard),
    Released,
}

#[cfg(test)]
#[path = "off_wave_tests.rs"]
mod tests;
