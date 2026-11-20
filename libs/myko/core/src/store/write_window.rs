//! One write window at a time, process-wide.
//!
//! Store and query cells carry diffs, and hyphae's scheduler keeps one
//! pending operation per cell, last write wins. A cell must therefore fire at
//! most once between two drains. One thread keeps that rule by itself. Two
//! threads do not: a batch that closes while another thread is draining
//! returns before its own operations have run, and the next write then
//! replaces the ones still queued. The subscriber never sees the replaced
//! diff, although the store holds the row.
//!
//! Holding this lock from the first store write until the drain has finished
//! means the queue is empty whenever the next write starts.
//!
//! The lock is process-wide because hyphae's queue is. It is reentrant
//! because a subscriber may write from inside a drain.

use parking_lot::{ReentrantMutex, ReentrantMutexGuard};

static WINDOW: ReentrantMutex<()> = ReentrantMutex::new(());

thread_local! {
    /// hyphae runs a wide wave on its own pool while the draining thread
    /// waits for it. That thread holds the window, so a write issued from a
    /// pool thread is already inside it and must not wait for it.
    static INSIDE_A_DRAIN: bool = std::thread::current()
        .name()
        .is_some_and(|name| name.starts_with("hyphae-worker-"));
}

/// Held for the length of one write and its propagation.
#[must_use = "the window closes when this is dropped"]
pub struct WriteWindow(#[allow(dead_code)] Option<ReentrantMutexGuard<'static, ()>>);

/// Opens the write window, waiting for any other thread's write to settle.
///
/// Take it before `hyphae::batch` in any code that writes stores from its own
/// batch. A batch opened first keeps the scheduler from draining to
/// completion for the writer that holds the window.
pub fn write_window() -> WriteWindow {
    WriteWindow((!INSIDE_A_DRAIN.with(|inside| *inside)).then(|| WINDOW.lock()))
}
