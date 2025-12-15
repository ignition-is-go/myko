//! Error types for the runtime.

use std::fmt;

/// Error returned when sending a message fails.
#[derive(Debug)]
pub enum SendError<M> {
    /// The receiving actor/pool has been shut down.
    Disconnected(M),
}

impl<M> SendError<M> {
    /// Retrieve the message that failed to send.
    pub fn into_inner(self) -> M {
        match self {
            SendError::Disconnected(m) => m,
        }
    }
}

impl<M: fmt::Debug> fmt::Display for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Disconnected(_) => write!(f, "channel disconnected"),
        }
    }
}

impl<M: fmt::Debug> std::error::Error for SendError<M> {}

impl<M> From<crossbeam::channel::SendError<M>> for SendError<M> {
    fn from(err: crossbeam::channel::SendError<M>) -> Self {
        SendError::Disconnected(err.into_inner())
    }
}
