//! Core Sink trait for message destinations.

use super::SendError;

/// A destination for messages.
///
/// All runtime primitives implement this trait, allowing them to be used
/// interchangeably. Note that this trait is dyn-compatible.
pub trait Sink<M>: Send + Sync {
    /// Send a message to this sink.
    ///
    /// Returns an error if the sink has been shut down.
    fn send(&self, msg: M) -> Result<(), SendError<M>>;
}

/// A sink that can route messages by key.
///
/// Used for maintaining per-key ordering while allowing parallelism
/// across different keys.
pub trait KeyedSink<K, M>: Send + Sync {
    /// Send a message with a routing key.
    ///
    /// Messages with the same key are guaranteed to be processed in order.
    /// Messages with different keys may be processed in parallel.
    fn send_keyed(&self, key: &K, msg: M) -> Result<(), SendError<M>>;
}
