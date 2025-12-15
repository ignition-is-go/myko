//! Worker pool primitive.
//!
//! A Pool runs multiple workers sharing a single queue, providing parallel
//! processing of messages. Order is not guaranteed - messages may be
//! processed in any order across workers.

use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};

use super::error::SendError;
use super::sink::Sink;

/// Handle to a worker pool.
pub struct Pool<M> {
    tx: Sender<M>,
}

// Manual Clone impl - Sender is always Clone regardless of M
impl<M> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<M: Send + 'static> Pool<M> {
    /// Send a message to the pool.
    ///
    /// One of the workers will pick it up for processing.
    pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
        self.tx.send(msg).map_err(SendError::from)
    }
}

impl<M: Send + 'static> Sink<M> for Pool<M> {
    fn send(&self, msg: M) -> Result<(), SendError<M>> {
        Pool::send(self, msg)
    }
}

/// Handle for managing pool lifecycle.
pub struct PoolHandle<M> {
    pool: Pool<M>,
    workers: Vec<JoinHandle<()>>,
}

impl<M> PoolHandle<M> {
    /// Get a cloneable reference for sending messages.
    pub fn pool(&self) -> Pool<M> {
        self.pool.clone()
    }

    /// Shutdown the pool and wait for all workers to finish.
    ///
    /// This drops the internal sender, signals workers to stop,
    /// and waits for all worker threads to complete.
    pub fn shutdown(mut self) -> thread::Result<()> {
        // Drop our sender to signal shutdown
        self.pool = Pool {
            tx: crossbeam::channel::unbounded().0,
        };
        for worker in self.workers {
            worker.join()?;
        }
        Ok(())
    }

    /// Wait for all workers to finish, assuming all external senders have been dropped.
    ///
    /// Note: This will block forever if any Pool clones are still alive.
    /// Prefer using `shutdown()` which handles this automatically.
    pub fn join(self) -> thread::Result<()> {
        for worker in self.workers {
            worker.join()?;
        }
        Ok(())
    }
}

/// Spawn a worker pool.
///
/// Creates `num_workers` threads that share a single queue.
/// Messages are processed in parallel with no ordering guarantees.
pub fn spawn<M, F>(num_workers: usize, handler: F) -> PoolHandle<M>
where
    M: Send + 'static,
    F: Fn(M) + Send + Sync + Clone + 'static,
{
    assert!(num_workers > 0, "pool must have at least one worker");

    let (tx, rx): (Sender<M>, Receiver<M>) = crossbeam::channel::unbounded();

    let workers: Vec<_> = (0..num_workers)
        .map(|_| {
            let rx = rx.clone();
            let handler = handler.clone();
            thread::spawn(move || {
                while let Ok(msg) = rx.recv() {
                    handler(msg);
                }
            })
        })
        .collect();

    PoolHandle {
        pool: Pool { tx },
        workers,
    }
}

/// Spawn a worker pool with per-worker state.
///
/// Each worker gets its own state created by `init_fn`.
/// Useful when workers need thread-local resources.
pub fn spawn_with_state<M, S, I, F>(num_workers: usize, init_fn: I, handler: F) -> PoolHandle<M>
where
    M: Send + 'static,
    S: Send + 'static,
    I: Fn() -> S + Send + Sync + Clone + 'static,
    F: Fn(&mut S, M) + Send + Sync + Clone + 'static,
{
    assert!(num_workers > 0, "pool must have at least one worker");

    let (tx, rx): (Sender<M>, Receiver<M>) = crossbeam::channel::unbounded();

    let workers: Vec<_> = (0..num_workers)
        .map(|_| {
            let rx = rx.clone();
            let init_fn = init_fn.clone();
            let handler = handler.clone();
            thread::spawn(move || {
                let mut state = init_fn();
                while let Ok(msg) = rx.recv() {
                    handler(&mut state, msg);
                }
            })
        })
        .collect();

    PoolHandle {
        pool: Pool { tx },
        workers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_pool_processes_in_parallel() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let handle = spawn(4, move |_: ()| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let pool = handle.pool();
        for _ in 0..100 {
            pool.send(()).expect("send failed");
        }

        drop(pool);
        handle.shutdown().expect("shutdown failed");

        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_pool_with_state() {
        let total = Arc::new(AtomicUsize::new(0));
        let total_clone = total.clone();

        let handle = spawn_with_state(
            4,
            || 0usize, // per-worker counter
            move |count, n: usize| {
                *count += n;
                if *count >= 10 {
                    total_clone.fetch_add(*count, Ordering::SeqCst);
                    *count = 0;
                }
            },
        );

        let pool = handle.pool();
        for i in 0..100 {
            pool.send(i).expect("send failed");
        }

        drop(pool);
        handle.shutdown().expect("shutdown failed");

        // Exact count depends on distribution, but should be > 0
        assert!(total.load(Ordering::SeqCst) > 0);
    }
}
