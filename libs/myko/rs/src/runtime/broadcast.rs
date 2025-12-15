//! Broadcast primitive for fan-out.
//!
//! A Broadcast sends messages to multiple subscribers.
//! Each subscriber receives a clone of every message.
//!
//! # Performance Considerations
//!
//! The cost of broadcasting is O(n × clone_cost) where n is the number of subscribers.
//! For large messages, wrap them in `Arc` to reduce cloning overhead:
//!
//! ```ignore
//! // Expensive: clones entire Event for each subscriber
//! let broadcast: Broadcast<Event> = ...;
//! broadcast.send(large_event);  // O(n × sizeof(Event))
//!
//! // Cheap: only clones Arc pointer for each subscriber
//! let broadcast: Broadcast<Arc<Event>> = ...;
//! broadcast.send(Arc::new(large_event));  // O(n × 16 bytes)
//! ```
//!
//! # Scaling Characteristics
//!
//! - **Small subscriber counts (< 10)**: Linear scaling is fine
//! - **Large subscriber counts**: Consider Arc-wrapped messages and parallel fan-out
//! - **EventBus pattern**: Fixed small subscriber count (~5), Arc<Event> recommended

use std::sync::Arc;

use super::error::SendError;
use super::sink::Sink;

/// Handle to a broadcast channel.
///
/// Cloneable and thread-safe. All clones share the same subscriber list.
#[derive(Clone)]
pub struct Broadcast<M> {
    subscribers: Arc<Vec<Arc<dyn Sink<M> + Send + Sync>>>,
}

impl<M: Clone + Send + 'static> Broadcast<M> {
    /// Create a new broadcast with no subscribers.
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Vec::new()),
        }
    }

    /// Create a broadcast with initial subscribers.
    pub fn with_subscribers<S: Sink<M> + Send + Sync + 'static>(subscribers: Vec<S>) -> Self {
        Self {
            subscribers: Arc::new(
                subscribers
                    .into_iter()
                    .map(|s| Arc::new(s) as Arc<dyn Sink<M> + Send + Sync>)
                    .collect(),
            ),
        }
    }

    /// Number of subscribers.
    pub fn num_subscribers(&self) -> usize {
        self.subscribers.len()
    }

    /// Send a message to all subscribers.
    ///
    /// Returns Ok if at least one subscriber received the message.
    /// Returns Err only if there are no subscribers or all failed.
    pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
        if self.subscribers.is_empty() {
            return Err(SendError::Disconnected(msg));
        }

        let mut any_success = false;

        for subscriber in self.subscribers.iter() {
            if subscriber.send(msg.clone()).is_ok() {
                any_success = true;
            }
        }

        if any_success {
            Ok(())
        } else {
            Err(SendError::Disconnected(msg))
        }
    }
}

impl<M: Clone + Send + 'static> Default for Broadcast<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Clone + Send + 'static> Sink<M> for Broadcast<M> {
    fn send(&self, msg: M) -> Result<(), SendError<M>> {
        Broadcast::send(self, msg)
    }
}

/// Builder for constructing a Broadcast with subscribers.
pub struct BroadcastBuilder<M> {
    subscribers: Vec<Arc<dyn Sink<M> + Send + Sync>>,
}

impl<M: Clone + Send + 'static> BroadcastBuilder<M> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
        }
    }

    /// Add a subscriber.
    pub fn subscribe<S: Sink<M> + Send + Sync + 'static>(mut self, sink: S) -> Self {
        self.subscribers.push(Arc::new(sink));
        self
    }

    /// Build the broadcast.
    pub fn build(self) -> Broadcast<M> {
        Broadcast {
            subscribers: Arc::new(self.subscribers),
        }
    }
}

impl<M: Clone + Send + 'static> Default for BroadcastBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::pool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_broadcast_to_multiple_pools() {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        let c1 = counter1.clone();
        let c2 = counter2.clone();

        let pool1_handle = pool::spawn(2, move |_: i32| {
            c1.fetch_add(1, Ordering::SeqCst);
        });

        let pool2_handle = pool::spawn(2, move |_: i32| {
            c2.fetch_add(1, Ordering::SeqCst);
        });

        let pool1 = pool1_handle.pool();
        let pool2 = pool2_handle.pool();

        let broadcast = BroadcastBuilder::new()
            .subscribe(pool1.clone())
            .subscribe(pool2.clone())
            .build();

        for i in 0..10 {
            broadcast.send(i).expect("send failed");
        }

        // Drop broadcast and external pool refs
        drop(pool1);
        drop(pool2);
        drop(broadcast);

        // Shutdown pools (drops internal refs and waits)
        pool1_handle.shutdown().expect("shutdown failed");
        pool2_handle.shutdown().expect("shutdown failed");

        assert_eq!(counter1.load(Ordering::SeqCst), 10);
        assert_eq!(counter2.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_broadcast_empty() {
        let broadcast: Broadcast<i32> = Broadcast::new();
        assert!(broadcast.send(42).is_err());
    }
}
