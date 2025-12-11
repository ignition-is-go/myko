//! Utilities for converting futures-signals types to async streams.

use futures::Stream;
use futures_signals::signal_map::{MapDiff, SignalMap};
use std::{pin::Pin, task::Poll};

/// Wrapper to convert a SignalMap into a Stream of MapDiff.
///
/// This allows using SignalMap with async stream combinators like `StreamExt::next()`.
///
/// # Example
/// ```ignore
/// use myko_rs::utils::signal_stream::SignalMapStream;
/// use futures::StreamExt;
///
/// let signal_map = query_manager.start_query(query).await?;
/// let mut stream = SignalMapStream::new(signal_map);
///
/// while let Some(diff) = stream.next().await {
///     match diff {
///         MapDiff::Replace { entries } => { /* initial state */ }
///         MapDiff::Insert { key, value } => { /* new entry */ }
///         MapDiff::Update { key, value } => { /* updated entry */ }
///         MapDiff::Remove { key } => { /* removed entry */ }
///         MapDiff::Clear {} => { /* all cleared */ }
///     }
/// }
/// ```
pub struct SignalMapStream<S> {
    signal: S,
}

impl<S> SignalMapStream<S> {
    /// Create a new SignalMapStream from a SignalMap.
    pub fn new(signal: S) -> Self {
        Self { signal }
    }
}

impl<S: SignalMap + Unpin> Stream for SignalMapStream<S> {
    type Item = MapDiff<S::Key, S::Value>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.signal).poll_map_change(cx)
    }
}
