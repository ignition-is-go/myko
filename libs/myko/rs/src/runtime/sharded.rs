//! Sharded actor primitive.
//!
//! A Sharded actor runs multiple workers, each owning a partition of the key space.
//! Messages with the same key always go to the same worker, guaranteeing order.
//! Messages with different keys may be processed in parallel.

use std::hash::{Hash, Hasher};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};

use super::error::SendError;
use super::sink::KeyedSink;

/// Handle to a sharded actor.
pub struct Sharded<M> {
    shards: Vec<Sender<M>>,
}

// Manual Clone impl - Sender is always Clone regardless of M
impl<M> Clone for Sharded<M> {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
        }
    }
}

impl<M: Send + 'static> Sharded<M> {
    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Send a message with a routing key.
    ///
    /// Messages with the same key are guaranteed to be processed in order
    /// by the same worker.
    pub fn send_keyed<K: Hash>(&self, key: &K, msg: M) -> Result<(), SendError<M>> {
        let shard = self.shard_for_key(key);
        self.shards[shard].send(msg).map_err(SendError::from)
    }

    /// Send to a specific shard by index.
    ///
    /// Useful when you've already computed the shard.
    pub fn send_to_shard(&self, shard: usize, msg: M) -> Result<(), SendError<M>> {
        self.shards[shard % self.shards.len()]
            .send(msg)
            .map_err(SendError::from)
    }

    /// Compute which shard a key maps to.
    pub fn shard_for_key<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }
}

impl<K: Hash, M: Send + 'static> KeyedSink<K, M> for Sharded<M> {
    fn send_keyed(&self, key: &K, msg: M) -> Result<(), SendError<M>> {
        Sharded::send_keyed(self, key, msg)
    }
}

/// Handle for managing sharded actor lifecycle.
pub struct ShardedHandle<M> {
    sharded: Sharded<M>,
    workers: Vec<JoinHandle<()>>,
}

impl<M> ShardedHandle<M> {
    /// Get a cloneable reference for sending messages.
    pub fn sharded(&self) -> Sharded<M> {
        self.sharded.clone()
    }

    /// Shutdown all shards and wait for workers to finish.
    ///
    /// This drops the internal senders, signals workers to stop,
    /// and waits for all worker threads to complete.
    pub fn shutdown(mut self) -> thread::Result<()> {
        // Drop our senders to signal shutdown
        self.sharded = Sharded { shards: Vec::new() };
        for worker in self.workers {
            worker.join()?;
        }
        Ok(())
    }

    /// Wait for all workers to finish, assuming all external senders have been dropped.
    ///
    /// Note: This will block forever if any Sharded clones are still alive.
    /// Prefer using `shutdown()` which handles this automatically.
    pub fn join(self) -> thread::Result<()> {
        for worker in self.workers {
            worker.join()?;
        }
        Ok(())
    }
}

/// Spawn a sharded actor.
///
/// Creates `num_shards` workers, each with its own queue.
/// Messages are routed by key hash to ensure per-key ordering.
pub fn spawn<M, F>(num_shards: usize, handler: F) -> ShardedHandle<M>
where
    M: Send + 'static,
    F: Fn(M) + Send + Sync + Clone + 'static,
{
    assert!(num_shards > 0, "must have at least one shard");

    let mut senders = Vec::with_capacity(num_shards);
    let mut workers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (tx, rx): (Sender<M>, Receiver<M>) = crossbeam::channel::unbounded();
        let handler = handler.clone();

        let worker = thread::spawn(move || {
            while let Ok(msg) = rx.recv() {
                handler(msg);
            }
        });

        senders.push(tx);
        workers.push(worker);
    }

    ShardedHandle {
        sharded: Sharded { shards: senders },
        workers,
    }
}

/// Spawn a sharded actor with per-shard state.
///
/// Each shard gets its own state created by `init_fn`.
pub fn spawn_with_state<M, S, I, F>(num_shards: usize, init_fn: I, handler: F) -> ShardedHandle<M>
where
    M: Send + 'static,
    S: Send + 'static,
    I: Fn(usize) -> S + Send + Sync + Clone + 'static,
    F: Fn(&mut S, M) + Send + Sync + Clone + 'static,
{
    assert!(num_shards > 0, "must have at least one shard");

    let mut senders = Vec::with_capacity(num_shards);
    let mut workers = Vec::with_capacity(num_shards);

    for shard_idx in 0..num_shards {
        let (tx, rx): (Sender<M>, Receiver<M>) = crossbeam::channel::unbounded();
        let init_fn = init_fn.clone();
        let handler = handler.clone();

        let worker = thread::spawn(move || {
            let mut state = init_fn(shard_idx);
            while let Ok(msg) = rx.recv() {
                handler(&mut state, msg);
            }
        });

        senders.push(tx);
        workers.push(worker);
    }

    ShardedHandle {
        sharded: Sharded { shards: senders },
        workers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_sharded_preserves_key_order() {
        // Track the order of messages per key
        let orders: Arc<Mutex<HashMap<String, Vec<usize>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let orders_clone = orders.clone();

        let handle = spawn(4, move |(key, seq): (String, usize)| {
            let mut orders = orders_clone.lock().expect("lock poisoned");
            orders.entry(key).or_default().push(seq);
        });

        let sharded = handle.sharded();

        // Send messages for multiple keys, each with a sequence number
        for key in ["a", "b", "c", "d"] {
            for seq in 0..10 {
                sharded
                    .send_keyed(&key, (key.to_string(), seq))
                    .expect("send failed");
            }
        }

        drop(sharded);
        handle.shutdown().expect("shutdown failed");

        // Verify each key's messages arrived in order
        let orders = orders.lock().expect("lock poisoned");
        for (key, seqs) in orders.iter() {
            let expected: Vec<usize> = (0..10).collect();
            assert_eq!(seqs, &expected, "key {key} out of order");
        }
    }

    #[test]
    fn test_sharded_distributes_across_shards() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let shard_counts: Arc<Vec<AtomicUsize>> =
            Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let shard_counts_clone = shard_counts.clone();

        let handle = spawn_with_state(
            4,
            |shard_idx| shard_idx,
            move |shard_idx, _msg: ()| {
                shard_counts_clone[*shard_idx].fetch_add(1, Ordering::SeqCst);
            },
        );

        let sharded = handle.sharded();

        // Send messages with different keys
        for i in 0..1000 {
            sharded.send_keyed(&i, ()).expect("send failed");
        }

        drop(sharded);
        handle.shutdown().expect("shutdown failed");

        // All shards should have received some messages
        let counts: Vec<_> = shard_counts
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect();

        assert!(
            counts.iter().all(|&c| c > 0),
            "some shards got no messages: {counts:?}"
        );
        assert_eq!(counts.iter().sum::<usize>(), 1000);
    }
}
