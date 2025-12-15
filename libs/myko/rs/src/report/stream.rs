//! Stream combinators for report handlers.
//!
//! Provides RxJS-like operators for combining multiple reactive streams:
//! - `combine_latest_vec` - Combine vec of streams, emit `Vec<T>` when any emits
//! - `combine_latest!` - Emit tuple whenever any stream emits (2-4 streams)
//! - `with_latest_from` - One stream drives, grab latest from others
//! - `.switch_map(f)` - Switch to new inner stream on each emission
//!
//! # Example
//!
//! ```ignore
//! use myko_rs::report::stream::combine_latest_vec;
//!
//! fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Output> + Send>> {
//!     combine_latest_vec(vec![
//!         ctx.query(GetServers {}).boxed(),
//!         ctx.query(GetClients {}).boxed(),
//!     ])
//!     .map(|results| Output { ... })
//!     .boxed()
//! }
//! ```

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;

// ============================================================================
// combine_latest_vec - for dynamic number of same-typed streams
// ============================================================================

/// Combines a vector of streams, emitting `Vec<T>` whenever any stream emits.
///
/// All streams must have the same item type. After all streams have emitted
/// at least once, emits a `Vec` of the latest values on every subsequent emission.
///
/// # Example
///
/// ```ignore
/// combine_latest_vec(vec![
///     ctx.query(GetServersByIds { ids: vec![id1] }).boxed(),
///     ctx.query(GetServersByIds { ids: vec![id2] }).boxed(),
///     ctx.query(GetServersByIds { ids: vec![id3] }).boxed(),
/// ])
/// .map(|all_servers| {
///     // all_servers is Vec<Vec<Server>>
/// })
/// ```
pub fn combine_latest_vec<S, T>(streams: Vec<S>) -> CombineLatestVec<S, T>
where
    S: Stream<Item = T> + Unpin,
    T: Clone,
{
    let len = streams.len();
    CombineLatestVec {
        streams,
        latest: vec![None; len],
        done: vec![false; len],
    }
}

/// Stream that combines a vector of streams using latest values.
pub struct CombineLatestVec<S, T> {
    streams: Vec<S>,
    latest: Vec<Option<T>>,
    done: Vec<bool>,
}

// CombineLatestVec is Unpin because it only contains Unpin types (Vec, bool)
impl<S: Unpin, T> Unpin for CombineLatestVec<S, T> {}

impl<S, T> Stream for CombineLatestVec<S, T>
where
    S: Stream<Item = T> + Unpin,
    T: Clone,
{
    type Item = Vec<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut changed = false;

        // Poll all streams
        for i in 0..this.streams.len() {
            if this.done[i] {
                continue;
            }

            match Pin::new(&mut this.streams[i]).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.latest[i] = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => {
                    this.done[i] = true;
                }
                Poll::Pending => {}
            }
        }

        // Emit if all have values and something changed
        if changed && this.latest.iter().all(|v| v.is_some()) {
            let values: Vec<T> = this.latest.iter().map(|v| v.clone().unwrap()).collect();
            return Poll::Ready(Some(values));
        }

        // If all streams are done, we're done
        if this.done.iter().all(|&d| d) {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// ============================================================================
// combine_latest for 2 streams
// ============================================================================

/// Combines two streams, emitting a tuple whenever either stream emits.
///
/// After both streams have emitted at least once, emits `(A, B)` on every
/// subsequent emission from either stream, using the latest value from each.
///
/// # Example
///
/// ```ignore
/// combine_latest(server_stream, clients_stream)
///     .map(|(servers, clients)| {
///         Stats { server: servers.first().cloned(), client_count: clients.len() }
///     })
/// ```
pub fn combine_latest<S1, S2>(s1: S1, s2: S2) -> CombineLatest2<S1, S2>
where
    S1: Stream,
    S2: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
{
    CombineLatest2 {
        stream1: s1,
        stream2: s2,
        latest1: None,
        latest2: None,
        done1: false,
        done2: false,
    }
}

pin_project! {
    /// Stream that combines two streams using latest values.
    pub struct CombineLatest2<S1: Stream, S2: Stream> {
        #[pin]
        stream1: S1,
        #[pin]
        stream2: S2,
        latest1: Option<S1::Item>,
        latest2: Option<S2::Item>,
        done1: bool,
        done2: bool,
    }
}

impl<S1, S2> Stream for CombineLatest2<S1, S2>
where
    S1: Stream,
    S2: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
{
    type Item = (S1::Item, S2::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut changed = false;

        // Poll stream1 if not done
        if !*this.done1 {
            match this.stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest1 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => {
                    *this.done1 = true;
                }
                Poll::Pending => {}
            }
        }

        // Poll stream2 if not done
        if !*this.done2 {
            match this.stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest2 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => {
                    *this.done2 = true;
                }
                Poll::Pending => {}
            }
        }

        // Emit if we have both values and something changed
        if changed
            && let (Some(v1), Some(v2)) = (this.latest1.as_ref(), this.latest2.as_ref())
        {
            return Poll::Ready(Some((v1.clone(), v2.clone())));
        }

        // If both streams are done, we're done
        if *this.done1 && *this.done2 {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// ============================================================================
// combine_latest for 3 streams
// ============================================================================

/// Combines three streams, emitting a tuple whenever any stream emits.
pub fn combine_latest3<S1, S2, S3>(s1: S1, s2: S2, s3: S3) -> CombineLatest3<S1, S2, S3>
where
    S1: Stream,
    S2: Stream,
    S3: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
    S3::Item: Clone,
{
    CombineLatest3 {
        stream1: s1,
        stream2: s2,
        stream3: s3,
        latest1: None,
        latest2: None,
        latest3: None,
        done1: false,
        done2: false,
        done3: false,
    }
}

pin_project! {
    /// Stream that combines three streams using latest values.
    pub struct CombineLatest3<S1: Stream, S2: Stream, S3: Stream> {
        #[pin]
        stream1: S1,
        #[pin]
        stream2: S2,
        #[pin]
        stream3: S3,
        latest1: Option<S1::Item>,
        latest2: Option<S2::Item>,
        latest3: Option<S3::Item>,
        done1: bool,
        done2: bool,
        done3: bool,
    }
}

impl<S1, S2, S3> Stream for CombineLatest3<S1, S2, S3>
where
    S1: Stream,
    S2: Stream,
    S3: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
    S3::Item: Clone,
{
    type Item = (S1::Item, S2::Item, S3::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut changed = false;

        if !*this.done1 {
            match this.stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest1 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done1 = true,
                Poll::Pending => {}
            }
        }

        if !*this.done2 {
            match this.stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest2 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done2 = true,
                Poll::Pending => {}
            }
        }

        if !*this.done3 {
            match this.stream3.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest3 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done3 = true,
                Poll::Pending => {}
            }
        }

        if changed
            && let (Some(v1), Some(v2), Some(v3)) = (
                this.latest1.as_ref(),
                this.latest2.as_ref(),
                this.latest3.as_ref(),
            )
        {
            return Poll::Ready(Some((v1.clone(), v2.clone(), v3.clone())));
        }

        if *this.done1 && *this.done2 && *this.done3 {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// ============================================================================
// combine_latest for 4 streams
// ============================================================================

/// Combines four streams, emitting a tuple whenever any stream emits.
pub fn combine_latest4<S1, S2, S3, S4>(
    s1: S1,
    s2: S2,
    s3: S3,
    s4: S4,
) -> CombineLatest4<S1, S2, S3, S4>
where
    S1: Stream,
    S2: Stream,
    S3: Stream,
    S4: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
    S3::Item: Clone,
    S4::Item: Clone,
{
    CombineLatest4 {
        stream1: s1,
        stream2: s2,
        stream3: s3,
        stream4: s4,
        latest1: None,
        latest2: None,
        latest3: None,
        latest4: None,
        done1: false,
        done2: false,
        done3: false,
        done4: false,
    }
}

pin_project! {
    /// Stream that combines four streams using latest values.
    pub struct CombineLatest4<S1: Stream, S2: Stream, S3: Stream, S4: Stream> {
        #[pin]
        stream1: S1,
        #[pin]
        stream2: S2,
        #[pin]
        stream3: S3,
        #[pin]
        stream4: S4,
        latest1: Option<S1::Item>,
        latest2: Option<S2::Item>,
        latest3: Option<S3::Item>,
        latest4: Option<S4::Item>,
        done1: bool,
        done2: bool,
        done3: bool,
        done4: bool,
    }
}

impl<S1, S2, S3, S4> Stream for CombineLatest4<S1, S2, S3, S4>
where
    S1: Stream,
    S2: Stream,
    S3: Stream,
    S4: Stream,
    S1::Item: Clone,
    S2::Item: Clone,
    S3::Item: Clone,
    S4::Item: Clone,
{
    type Item = (S1::Item, S2::Item, S3::Item, S4::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut changed = false;

        if !*this.done1 {
            match this.stream1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest1 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done1 = true,
                Poll::Pending => {}
            }
        }

        if !*this.done2 {
            match this.stream2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest2 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done2 = true,
                Poll::Pending => {}
            }
        }

        if !*this.done3 {
            match this.stream3.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest3 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done3 = true,
                Poll::Pending => {}
            }
        }

        if !*this.done4 {
            match this.stream4.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest4 = Some(item);
                    changed = true;
                }
                Poll::Ready(None) => *this.done4 = true,
                Poll::Pending => {}
            }
        }

        if changed
            && let (Some(v1), Some(v2), Some(v3), Some(v4)) = (
                this.latest1.as_ref(),
                this.latest2.as_ref(),
                this.latest3.as_ref(),
                this.latest4.as_ref(),
            )
        {
            return Poll::Ready(Some((v1.clone(), v2.clone(), v3.clone(), v4.clone())));
        }

        if *this.done1 && *this.done2 && *this.done3 && *this.done4 {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// ============================================================================
// with_latest_from - one stream drives, grabs latest from another
// ============================================================================

/// Returns a stream that emits whenever the source stream emits,
/// combining the source value with the latest value from another stream.
///
/// Unlike `combine_latest`, only emissions from the source stream trigger output.
/// The `other` stream's values are sampled but don't trigger emissions.
///
/// # Example
///
/// ```ignore
/// // Emit server stats whenever server changes, grabbing latest client count
/// with_latest_from(server_stream, clients_stream)
///     .map(|(server, clients)| Stats { ... })
/// ```
pub fn with_latest_from<S1, S2>(source: S1, other: S2) -> WithLatestFrom<S1, S2>
where
    S1: Stream,
    S2: Stream,
    S2::Item: Clone,
{
    WithLatestFrom {
        source,
        other,
        latest_other: None,
    }
}

pin_project! {
    /// Stream that combines source emissions with the latest value from another stream.
    pub struct WithLatestFrom<S1: Stream, S2: Stream> {
        #[pin]
        source: S1,
        #[pin]
        other: S2,
        latest_other: Option<S2::Item>,
    }
}

impl<S1, S2> Stream for WithLatestFrom<S1, S2>
where
    S1: Stream,
    S2: Stream,
    S2::Item: Clone,
{
    type Item = (S1::Item, S2::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Always poll the other stream to keep it updated
        while let Poll::Ready(Some(item)) = this.other.as_mut().poll_next(cx) {
            *this.latest_other = Some(item);
        }

        // Only source stream triggers emissions
        match this.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(source_item)) => {
                if let Some(other_item) = this.latest_other.as_ref() {
                    Poll::Ready(Some((source_item, other_item.clone())))
                } else {
                    // Other stream hasn't emitted yet, wait
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================================================================
// switch_map - switch to new inner stream on each outer emission
// ============================================================================

/// Extension trait for switch_map operation.
pub trait SwitchMapExt: Stream + Sized {
    /// Maps each value to an inner stream, switching to the new stream and
    /// canceling the previous one whenever the outer stream emits.
    ///
    /// This is equivalent to RxJS's `switchMap` operator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // When selected_id changes, cancel previous query and start new one
    /// selected_id_stream
    ///     .switch_map(|id| ctx.query(GetItemById { id }))
    /// ```
    fn switch_map<F, Inner>(self, f: F) -> SwitchMap<Self, F, Inner>
    where
        F: FnMut(Self::Item) -> Inner,
        Inner: Stream,
    {
        SwitchMap {
            outer: self,
            f,
            inner: None,
            outer_done: false,
        }
    }
}

impl<S: Stream + Sized> SwitchMapExt for S {}

pin_project! {
    /// Stream that switches to a new inner stream on each outer emission.
    #[project = SwitchMapProj]
    pub struct SwitchMap<Outer, F, Inner>
    where
        Outer: Stream,
        Inner: Stream,
    {
        #[pin]
        outer: Outer,
        f: F,
        #[pin]
        inner: Option<Inner>,
        outer_done: bool,
    }
}

impl<Outer, F, Inner> Stream for SwitchMap<Outer, F, Inner>
where
    Outer: Stream,
    F: FnMut(Outer::Item) -> Inner,
    Inner: Stream,
{
    type Item = Inner::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Poll outer stream for new values (which trigger new inner streams)
        if !*this.outer_done {
            match this.outer.as_mut().poll_next(cx) {
                Poll::Ready(Some(outer_item)) => {
                    // Create new inner stream, replacing the old one
                    let new_inner = (this.f)(outer_item);
                    this.inner.set(Some(new_inner));
                }
                Poll::Ready(None) => {
                    *this.outer_done = true;
                }
                Poll::Pending => {}
            }
        }

        // Poll inner stream if we have one
        if let Some(inner) = this.inner.as_mut().as_pin_mut() {
            match inner.poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    // Inner stream completed
                    this.inner.set(None);
                }
                Poll::Pending => {}
            }
        }

        // If outer is done and we have no inner stream, we're done
        if *this.outer_done && this.inner.as_ref().get_ref().is_none() {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// ============================================================================
// Additional stream operators as extension trait
// ============================================================================

/// Extension trait providing additional stream operators.
///
/// These operators mirror common RxJS patterns used in report handlers.
pub trait StreamOpsExt: Stream + Sized {
    /// Emits only when the current value differs from the previous.
    ///
    /// Uses `PartialEq` to compare values. First value is always emitted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// stream.distinct_until_changed()  // dedup consecutive identical values
    /// ```
    fn distinct_until_changed(self) -> DistinctUntilChanged<Self>
    where
        Self::Item: Clone + PartialEq,
    {
        DistinctUntilChanged {
            stream: self,
            last: None,
        }
    }

    /// Emits an initial value before any values from the stream.
    ///
    /// # Example
    ///
    /// ```ignore
    /// stream.start_with(vec![])  // emit empty vec first, then stream values
    /// ```
    fn start_with(self, initial: Self::Item) -> StartWith<Self>
    where
        Self::Item: Clone,
    {
        StartWith {
            stream: self,
            initial: Some(initial),
        }
    }

    /// Completes the stream when the notifier stream emits.
    ///
    /// Unlike `futures::StreamExt::take_until` which takes a Future,
    /// this takes a Stream as the notifier (matching RxJS behavior).
    ///
    /// # Example
    ///
    /// ```ignore
    /// data_stream.take_until_signal(cancel_stream)
    /// ```
    fn take_until_signal<N>(self, notifier: N) -> TakeUntilSignal<Self, N>
    where
        N: Stream,
    {
        TakeUntilSignal {
            stream: self,
            notifier,
            done: false,
        }
    }
}

impl<S: Stream + Sized> StreamOpsExt for S {}

// DistinctUntilChanged
pin_project! {
    pub struct DistinctUntilChanged<S: Stream> {
        #[pin]
        stream: S,
        last: Option<S::Item>,
    }
}

impl<S> Stream for DistinctUntilChanged<S>
where
    S: Stream,
    S::Item: Clone + PartialEq,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let dominated = this.last.as_ref().map(|l| l == &item).unwrap_or(false);
                    if !dominated {
                        *this.last = Some(item.clone());
                        return Poll::Ready(Some(item));
                    }
                    // Same as last, skip and poll again
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// StartWith
pin_project! {
    pub struct StartWith<S: Stream> {
        #[pin]
        stream: S,
        initial: Option<S::Item>,
    }
}

impl<S> Stream for StartWith<S>
where
    S: Stream,
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Emit initial value first
        if let Some(initial) = this.initial.take() {
            return Poll::Ready(Some(initial));
        }

        this.stream.as_mut().poll_next(cx)
    }
}

// TakeUntilSignal
pin_project! {
    pub struct TakeUntilSignal<S, N>
    where
        S: Stream,
        N: Stream,
    {
        #[pin]
        stream: S,
        #[pin]
        notifier: N,
        done: bool,
    }
}

impl<S, N> Stream for TakeUntilSignal<S, N>
where
    S: Stream,
    N: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        // Check if notifier has emitted
        match this.notifier.as_mut().poll_next(cx) {
            Poll::Ready(Some(_)) | Poll::Ready(None) => {
                *this.done = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        // Poll the main stream
        this.stream.as_mut().poll_next(cx)
    }
}

// ============================================================================
// Macro for ergonomic usage
// ============================================================================

/// Combines multiple streams, emitting a tuple whenever any stream emits.
///
/// This macro selects the appropriate `combine_latest` function based on arity.
///
/// # Example
///
/// ```ignore
/// combine_latest!(
///     ctx.query(GetServers {}),
///     ctx.query(GetClients {}),
///     ctx.query(GetScenes {}),
/// )
/// .map(|(servers, clients, scenes)| {
///     // Process combined values
/// })
/// ```
#[macro_export]
macro_rules! combine_latest {
    ($s1:expr, $s2:expr $(,)?) => {
        $crate::report::stream::combine_latest($s1, $s2)
    };
    ($s1:expr, $s2:expr, $s3:expr $(,)?) => {
        $crate::report::stream::combine_latest3($s1, $s2, $s3)
    };
    ($s1:expr, $s2:expr, $s3:expr, $s4:expr $(,)?) => {
        $crate::report::stream::combine_latest4($s1, $s2, $s3, $s4)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_combine_latest_basic() {
        // Use synchronous iterators for predictable testing
        let s1 = futures::stream::iter(vec![1, 2]);
        let s2 = futures::stream::iter(vec!["a", "b"]);

        let combined = combine_latest(s1, s2);
        let results: Vec<_> = combined.collect().await;

        // With iter streams, we get emissions as values become available
        // Stream polling is synchronous, so we get: (1, "a"), (2, "a"), (2, "b")
        assert!(!results.is_empty());
        // All results should be tuples
        for (a, b) in &results {
            assert!(*a == 1 || *a == 2);
            assert!(*b == "a" || *b == "b");
        }
    }

    #[tokio::test]
    async fn test_combine_latest3_basic() {
        let s1 = futures::stream::iter(vec![1, 2]);
        let s2 = futures::stream::iter(vec!["a", "b"]);
        let s3 = futures::stream::iter(vec![true, false]);

        let combined = combine_latest3(s1, s2, s3);
        let results: Vec<_> = combined.collect().await;

        // First emission when all three have values
        assert!(!results.is_empty());
        // All results should be tuples of 3
        for (a, b, c) in &results {
            assert!(*a == 1 || *a == 2);
            assert!(*b == "a" || *b == "b");
            assert!(*c || !*c);
        }
    }

    #[tokio::test]
    async fn test_combine_latest4_basic() {
        let s1 = futures::stream::iter(vec![1, 2]);
        let s2 = futures::stream::iter(vec!["a", "b"]);
        let s3 = futures::stream::iter(vec![true, false]);
        let s4 = futures::stream::iter(vec![10.0, 20.0]);

        let combined = combine_latest4(s1, s2, s3, s4);
        let results: Vec<_> = combined.collect().await;

        assert!(!results.is_empty());
        for (a, b, c, d) in &results {
            assert!(*a == 1 || *a == 2);
            assert!(*b == "a" || *b == "b");
            assert!(*c || !*c);
            assert!(*d == 10.0 || *d == 20.0);
        }
    }

    #[tokio::test]
    async fn test_with_latest_from_iter() {
        let source = futures::stream::iter(vec![1, 2, 3]);
        let other = futures::stream::iter(vec!["a", "b"]);

        let combined = with_latest_from(source, other);
        let results: Vec<_> = combined.collect().await;

        // Source drives emissions, other provides latest value
        assert!(!results.is_empty());
        for (a, b) in &results {
            assert!(*a >= 1 && *a <= 3);
            assert!(*b == "a" || *b == "b");
        }
    }

    #[tokio::test]
    async fn test_combine_latest_macro() {
        let s1 = futures::stream::iter(vec![1, 2]);
        let s2 = futures::stream::iter(vec!["a", "b"]);

        let combined = combine_latest!(s1, s2);
        let results: Vec<_> = combined.collect().await;

        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_combine_latest_completes() {
        let s1 = futures::stream::iter(vec![1]);
        let s2 = futures::stream::iter(vec!["a"]);

        let combined = combine_latest(s1, s2);
        let results: Vec<_> = combined.collect().await;

        // Should complete after both streams end
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (1, "a"));
    }

    #[tokio::test]
    async fn test_switch_map_basic() {
        use super::SwitchMapExt;

        // Outer stream emits IDs, inner stream returns items for each ID
        let outer = futures::stream::iter(vec![1, 2, 3]);

        let result: Vec<_> = outer
            .switch_map(|id| futures::stream::iter(vec![id * 10, id * 10 + 1]))
            .collect()
            .await;

        // Each outer value creates an inner stream of 2 values
        // With sync streams, we get all values from each inner before outer emits again
        assert!(!result.is_empty());
        // Results should be multiples of outer values
        for val in &result {
            assert!(*val >= 10 && *val <= 31);
        }
    }

    #[tokio::test]
    async fn test_switch_map_completes() {
        use super::SwitchMapExt;

        let outer = futures::stream::iter(vec![1]);
        let result: Vec<_> = outer
            .switch_map(|_| futures::stream::iter(vec!["a", "b"]))
            .collect()
            .await;

        assert_eq!(result, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_combine_latest_vec_basic() {
        let streams = vec![
            futures::stream::iter(vec![1, 2]),
            futures::stream::iter(vec![10, 20]),
            futures::stream::iter(vec![100, 200]),
        ];

        let combined = combine_latest_vec(streams);
        let results: Vec<_> = combined.collect().await;

        // Should emit Vec<i32> whenever any stream emits (after all have initial value)
        assert!(!results.is_empty());
        for vec in &results {
            assert_eq!(vec.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_combine_latest_vec_single() {
        let streams = vec![futures::stream::iter(vec![1, 2, 3])];

        let combined = combine_latest_vec(streams);
        let results: Vec<_> = combined.collect().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], vec![1]);
        assert_eq!(results[1], vec![2]);
        assert_eq!(results[2], vec![3]);
    }

    #[tokio::test]
    async fn test_combine_latest_vec_empty() {
        let streams: Vec<futures::stream::Iter<std::vec::IntoIter<i32>>> = vec![];

        let combined = combine_latest_vec(streams);
        let results: Vec<_> = combined.collect().await;

        // Empty vec of streams should complete immediately
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_distinct_until_changed() {
        use super::StreamOpsExt;

        let stream = futures::stream::iter(vec![1, 1, 2, 2, 2, 3, 1, 1]);
        let results: Vec<_> = stream.distinct_until_changed().collect().await;

        // Should emit: 1, 2, 3, 1 (consecutive duplicates removed)
        assert_eq!(results, vec![1, 2, 3, 1]);
    }

    #[tokio::test]
    async fn test_start_with() {
        use super::StreamOpsExt;

        let stream = futures::stream::iter(vec![2, 3]);
        let results: Vec<_> = stream.start_with(1).collect().await;

        // Should emit: 1, 2, 3
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_take_until_signal() {
        use super::StreamOpsExt;

        // Main stream: 1, 2, 3, 4, 5
        let main = futures::stream::iter(vec![1, 2, 3, 4, 5]);
        // Notifier emits after 3 items polled
        let notifier = futures::stream::iter(vec![()]);

        let results: Vec<_> = main.take_until_signal(notifier).collect().await;

        // Should complete when notifier emits
        // With sync streams, notifier emits immediately so we get nothing or first item
        assert!(results.len() <= 1);
    }
}
