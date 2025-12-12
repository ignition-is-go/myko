//! Shared event broadcast channel for high-throughput event distribution.
//!
//! This module provides a lock-free broadcast channel for distributing events
//! to multiple subscribers (saga runners, etc.) concurrently.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use log::debug;
use tokio::sync::broadcast;

use crate::event::MEvent;

/// Default capacity for the broadcast channel.
/// Should be large enough to handle burst traffic without dropping events.
const DEFAULT_CHANNEL_CAPACITY: usize = 16384;

/// Shared event bus for broadcasting events to multiple subscribers.
/// Uses tokio::sync::broadcast internally for lock-free, concurrent distribution.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Arc<MEvent>>,
}

impl EventBus {
    /// Create a new event bus with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// Create a new event bus with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event to all subscribers.
    /// Uses Arc to avoid cloning the event for each subscriber.
    /// Skips publishing if there are no subscribers (performance optimization).
    #[inline]
    pub fn publish(&self, event: MEvent) {
        // Fast path: skip if no subscribers
        if self.sender.receiver_count() == 0 {
            return;
        }
        let event = Arc::new(event);
        // Ignore send errors (subscriber dropped between check and send is fine)
        let _ = self.sender.send(event);
    }

    /// Publish a pre-wrapped Arc event (avoids extra Arc allocation).
    #[inline]
    pub fn publish_arc(&self, event: Arc<MEvent>) {
        if self.sender.receiver_count() == 0 {
            return;
        }
        let _ = self.sender.send(event);
    }

    /// Create a new subscriber to this event bus.
    /// Returns a receiver that will receive all events published after subscription.
    pub fn subscribe(&self) -> EventBusSubscriber {
        EventBusSubscriber {
            receiver: self.sender.subscribe(),
        }
    }

    /// Get the current number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// A subscriber to the event bus.
pub struct EventBusSubscriber {
    receiver: broadcast::Receiver<Arc<MEvent>>,
}

impl EventBusSubscriber {
    /// Receive the next event from the bus.
    /// Returns None if the bus is closed, or an error if events were missed.
    pub async fn recv(&mut self) -> Option<Arc<MEvent>> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    // Log but continue - saga streams handle missing events gracefully
                    debug!("Event bus subscriber lagged, skipped {} events", skipped);
                    continue;
                }
            }
        }
    }

    /// Try to receive an event without blocking.
    pub fn try_recv(&mut self) -> Option<Arc<MEvent>> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Closed) => return None,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    debug!("Event bus subscriber lagged, skipped {} events", skipped);
                    continue;
                }
            }
        }
    }

    /// Convert this subscriber into a Stream of MEvents.
    /// This avoids an intermediate channel by implementing Stream directly.
    pub fn into_stream(self) -> EventBusStream {
        EventBusStream {
            receiver: self.receiver,
        }
    }
}

/// A Stream adapter for EventBusSubscriber.
/// Yields MEvent values directly without intermediate channels.
pub struct EventBusStream {
    receiver: broadcast::Receiver<Arc<MEvent>>,
}

impl Stream for EventBusStream {
    type Item = MEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        use std::pin::pin;

        loop {
            // Create a pinned future for the recv operation
            let recv_future = self.receiver.recv();
            let mut pinned = pin!(recv_future);

            match pinned.as_mut().poll(cx) {
                Poll::Ready(Ok(event)) => {
                    // Clone the event from Arc - this is the hot path
                    return Poll::Ready(Some((*event).clone()));
                }
                Poll::Ready(Err(broadcast::error::RecvError::Closed)) => {
                    return Poll::Ready(None);
                }
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                    debug!("Event bus stream lagged, skipped {} events", skipped);
                    // Continue polling - we'll try again
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
