//! Stream operators for saga event processing.
//!
//! Provides RxJS-like operators for filtering and transforming event streams:
//! - `of_items::<T>()` - Filter by item type
//! - `of_type(ChangeType)` - Filter by SET/DEL
//! - `pairwise()` - Compare prev/current for transitions
//! - And more...

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project_lite::pin_project;

use crate::event::{MEvent, MEventType};

/// Extension trait for event streams with saga-specific operators.
///
/// These operators mirror the RxJS patterns used in the TypeScript codebase,
/// making it easy to translate saga logic between languages.
pub trait SagaStreamExt: Stream<Item = MEvent> + Sized {
    /// Filter events by item type name.
    ///
    /// # Example
    /// ```ignore
    /// events.of_item_type("Target")
    /// ```
    fn of_item_type(self, item_type: &'static str) -> OfItemType<Self> {
        OfItemType {
            stream: self,
            item_type,
        }
    }

    /// Filter events by change type (SET or DEL).
    ///
    /// # Example
    /// ```ignore
    /// events.of_change_type(MEventType::SET)
    /// ```
    fn of_change_type(self, change_type: MEventType) -> OfChangeType<Self> {
        OfChangeType {
            stream: self,
            change_type,
        }
    }

    /// Emit pairs of (previous, current) events for detecting state transitions.
    ///
    /// The first event is buffered and not emitted until the second event arrives.
    ///
    /// # Example
    /// ```ignore
    /// events
    ///     .of_item_type("Scene")
    ///     .pairwise()
    ///     .filter_map(|(prev, curr)| {
    ///         if prev.item["status"] != curr.item["status"] {
    ///             Some(StatusChanged { ... })
    ///         } else {
    ///             None
    ///         }
    ///     })
    /// ```
    fn pairwise(self) -> Pairwise<Self> {
        Pairwise {
            stream: self,
            previous: None,
        }
    }

    /// Accumulate state across events using a fold-like operation.
    ///
    /// Similar to RxJS `scan` - emits the accumulated state after each event.
    /// Named `accumulate` to avoid conflict with `futures::StreamExt::scan`.
    ///
    /// # Example
    /// ```ignore
    /// events
    ///     .accumulate(0, |count, _event| count + 1)
    ///     .filter(|count| count % 10 == 0)
    /// ```
    fn accumulate<S, F>(self, initial: S, f: F) -> Scan<Self, S, F>
    where
        S: Clone,
        F: FnMut(&mut S, MEvent) -> S,
    {
        Scan {
            stream: self,
            state: initial,
            f,
        }
    }
}

// Implement SagaStreamExt for all streams of MEvent
impl<T: Stream<Item = MEvent> + Sized> SagaStreamExt for T {}

// ============================================================================
// OfItemType - Filter by item type
// ============================================================================

pin_project! {
    /// Stream adapter that filters events by item type.
    pub struct OfItemType<S> {
        #[pin]
        stream: S,
        item_type: &'static str,
    }
}

impl<S: Stream<Item = MEvent>> Stream for OfItemType<S> {
    type Item = MEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    if event.item_type() == *this.item_type {
                        return Poll::Ready(Some(event));
                    }
                    // Continue to next event
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ============================================================================
// OfChangeType - Filter by change type
// ============================================================================

pin_project! {
    /// Stream adapter that filters events by change type (SET/DEL).
    pub struct OfChangeType<S> {
        #[pin]
        stream: S,
        change_type: MEventType,
    }
}

impl<S: Stream<Item = MEvent>> Stream for OfChangeType<S> {
    type Item = MEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    if event.change_type() == *this.change_type {
                        return Poll::Ready(Some(event));
                    }
                    // Continue to next event
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ============================================================================
// Pairwise - Emit (prev, current) pairs
// ============================================================================

pin_project! {
    /// Stream adapter that emits pairs of consecutive events.
    pub struct Pairwise<S: Stream<Item = MEvent>> {
        #[pin]
        stream: S,
        previous: Option<MEvent>,
    }
}

impl<S: Stream<Item = MEvent>> Stream for Pairwise<S> {
    type Item = (MEvent, MEvent);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    if let Some(prev) = this.previous.take() {
                        *this.previous = Some(event.clone());
                        return Poll::Ready(Some((prev, event)));
                    } else {
                        // First event, buffer it
                        *this.previous = Some(event);
                        // Continue to wait for next event
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ============================================================================
// Scan - Accumulate state
// ============================================================================

pin_project! {
    /// Stream adapter that accumulates state across events.
    pub struct Scan<S, State, F> {
        #[pin]
        stream: S,
        state: State,
        f: F,
    }
}

impl<S, State, F> Stream for Scan<S, State, F>
where
    S: Stream<Item = MEvent>,
    State: Clone,
    F: FnMut(&mut State, MEvent) -> State,
{
    type Item = State;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Ready(Some(event)) => {
                let new_state = (this.f)(this.state, event);
                *this.state = new_state.clone();
                Poll::Ready(Some(new_state))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Create a filter predicate for item type
pub fn is_item_type(item_type: &'static str) -> impl Fn(&MEvent) -> bool {
    move |event| event.item_type() == item_type
}

/// Create a filter predicate for change type
pub fn is_change_type(change_type: MEventType) -> impl Fn(&MEvent) -> bool {
    move |event| event.change_type() == change_type
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, executor::block_on, stream};
    use serde_json::json;

    use super::*;

    fn make_event(item_type: &str, change_type: MEventType) -> MEvent {
        MEvent {
            tx: "test-tx".to_string(),
            item_type: item_type.to_string(),
            item: json!({"id": "test-id", "hash": "test-hash"}),
            change_type,
            created_at: chrono::Utc::now().to_rfc3339(),
            source_id: None,
            options: None,
        }
    }

    #[test]
    fn test_of_item_type() {
        let events = stream::iter(vec![
            make_event("Target", MEventType::SET),
            make_event("Scene", MEventType::SET),
            make_event("Target", MEventType::DEL),
        ]);

        let filtered: Vec<_> = block_on(events.of_item_type("Target").collect());
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|e: &MEvent| e.item_type() == "Target"));
    }

    #[test]
    fn test_of_change_type() {
        let events = stream::iter(vec![
            make_event("Target", MEventType::SET),
            make_event("Target", MEventType::DEL),
            make_event("Target", MEventType::SET),
        ]);

        let filtered: Vec<_> = block_on(events.of_change_type(MEventType::SET).collect());
        assert_eq!(filtered.len(), 2);
        assert!(
            filtered
                .iter()
                .all(|e: &MEvent| e.change_type() == MEventType::SET)
        );
    }

    #[test]
    fn test_pairwise() {
        let events = stream::iter(vec![
            make_event("Target", MEventType::SET),
            make_event("Target", MEventType::SET),
            make_event("Target", MEventType::SET),
        ]);

        let pairs: Vec<_> = block_on(events.pairwise().collect());
        assert_eq!(pairs.len(), 2); // 3 events -> 2 pairs
    }

    #[test]
    fn test_scan() {
        let events = stream::iter(vec![
            make_event("Target", MEventType::SET),
            make_event("Target", MEventType::SET),
            make_event("Target", MEventType::SET),
        ]);

        let counts: Vec<_> = block_on(events.accumulate(0, |count, _| *count + 1).collect());
        assert_eq!(counts, vec![1, 2, 3]);
    }

    #[test]
    fn test_chained_operators() {
        let events = stream::iter(vec![
            make_event("Target", MEventType::SET),
            make_event("Scene", MEventType::SET),
            make_event("Target", MEventType::DEL),
            make_event("Target", MEventType::SET),
        ]);

        let filtered: Vec<_> = block_on(
            events
                .of_item_type("Target")
                .of_change_type(MEventType::SET)
                .collect(),
        );

        assert_eq!(filtered.len(), 2);
    }
}
