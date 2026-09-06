use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use hyphae::{
    Cell, CellImmutable, CellMutable, Mutable as _, Signal, SubscriptionGuard, Watchable as _,
};

/// One immutable accepted publication in source-defined order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LivePublication<T>
where
    T: hyphae::CellValue,
{
    pub sequence: u64,
    pub state: T,
}

/// A bounded stream of the latest immutable snapshot publications.
///
/// A slow reader may skip intermediate sequences, including the initial snapshot
/// if superseded before it is read. This is not an event or delta stream.
/// The stream owns its reactive subscription. Completion or failure of the
/// underlying cell closes the stream; callers must not treat its last value as
/// current after that terminal condition.
/// A queued snapshot is delivered before the receive operation reports closure.
pub struct LivePublicationStream<T>
where
    T: hyphae::CellValue,
{
    receiver: flume::Receiver<LivePublication<T>>,
    _guard: SubscriptionGuard,
}

struct PublicationReceiver<T>
where
    T: hyphae::CellValue,
{
    last: Option<u64>,
    sender: Option<flume::Sender<LivePublication<T>>>,
    queued: flume::Receiver<LivePublication<T>>,
}

impl<T> PublicationReceiver<T>
where
    T: hyphae::CellValue,
{
    fn new() -> (Self, flume::Receiver<LivePublication<T>>) {
        let (sender, receiver) = flume::bounded(1);
        (
            Self {
                last: None,
                sender: Some(sender),
                queued: receiver.clone(),
            },
            receiver,
        )
    }

    fn publish(&mut self, publication: LivePublication<T>) {
        if self.last.is_some_and(|last| publication.sequence <= last) {
            return;
        }
        self.last = Some(publication.sequence);
        let Some(sender) = &self.sender else {
            return;
        };
        let mut pending = publication;
        loop {
            match sender.try_send(pending) {
                Ok(()) | Err(flume::TrySendError::Disconnected(_)) => return,
                Err(flume::TrySendError::Full(publication)) => {
                    let _superseded = self.queued.try_recv();
                    pending = publication;
                }
            }
        }
    }
}

impl<T> LivePublicationStream<T>
where
    T: hyphae::CellValue,
{
    pub(crate) fn from_cell(cell: &Cell<LivePublication<T>, CellImmutable>) -> Self {
        let (delivery, receiver) = PublicationReceiver::new();
        let delivery = Mutex::new(delivery);
        let guard = cell.subscribe(move |signal| {
            let mut delivery = delivery
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match signal {
                Signal::Value(publication) => {
                    delivery.publish((**publication).clone());
                }
                Signal::Complete | Signal::Error(_) => {
                    delivery.sender.take();
                }
            }
        });
        Self {
            receiver,
            _guard: guard,
        }
    }

    /// Receives the next publication, waiting if none is queued.
    ///
    /// # Errors
    /// Returns an error after the underlying publication cell terminates.
    pub fn recv(&mut self) -> Result<LivePublication<T>, flume::RecvError> {
        self.receiver.recv()
    }

    /// Receives the next publication without blocking the executor.
    ///
    /// # Errors
    /// Returns an error after the underlying publication cell terminates.
    pub async fn recv_async(&mut self) -> Result<LivePublication<T>, flume::RecvError> {
        self.receiver.recv_async().await
    }

    /// Receives a queued publication without waiting.
    ///
    /// # Errors
    /// Returns an error when the queue is empty or the cell has terminated.
    pub fn try_recv(&mut self) -> Result<LivePublication<T>, flume::TryRecvError> {
        self.receiver.try_recv()
    }
}

/// The publication sequence cannot represent another accepted state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationSequenceExhausted;

impl std::fmt::Display for PublicationSequenceExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("live publication sequence is exhausted")
    }
}

impl std::error::Error for PublicationSequenceExhausted {}

/// Shared writer for one ordered immutable publication cell.
#[derive(Clone)]
pub struct PublicationSource<T>
where
    T: hyphae::CellValue,
{
    inner: Arc<PublicationInner<T>>,
}

struct PublicationInner<T>
where
    T: hyphae::CellValue,
{
    accepted: Mutex<AcceptedPublications<T>>,
    publication: Cell<LivePublication<T>, CellMutable>,
}

struct AcceptedPublications<T>
where
    T: hyphae::CellValue,
{
    latest: LivePublication<T>,
    queued: VecDeque<LivePublication<T>>,
    draining: bool,
}

struct DrainReset<'a, T>
where
    T: hyphae::CellValue,
{
    source: &'a PublicationSource<T>,
    armed: bool,
}

impl<T> Drop for DrainReset<'_, T>
where
    T: hyphae::CellValue,
{
    fn drop(&mut self) {
        if self.armed {
            self.source
                .inner
                .accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .draining = false;
        }
    }
}

impl<T> PublicationSource<T>
where
    T: hyphae::CellValue,
{
    pub(crate) fn new(initial: T) -> Self {
        let latest = LivePublication {
            sequence: 0,
            state: initial,
        };
        Self {
            inner: Arc::new(PublicationInner {
                accepted: Mutex::new(AcceptedPublications {
                    latest: latest.clone(),
                    queued: VecDeque::new(),
                    draining: false,
                }),
                publication: hyphae::scheduler::no_coalesce(|| Cell::new(latest))
                    .with_name("myko.live_publication"),
            }),
        }
    }

    /// Returns the latest accepted state, which may be newer than the reactive cell while draining.
    #[cfg(test)]
    pub(crate) fn current(&self) -> LivePublication<T> {
        self.inner
            .accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .latest
            .clone()
    }

    pub(crate) fn publication(&self) -> Cell<LivePublication<T>, CellImmutable> {
        self.inner.publication.clone().lock()
    }

    pub(crate) fn fail(&self, error: PublicationSequenceExhausted) {
        self.inner.publication.fail(error);
    }

    /// Accepts an update against the latest accepted state and publishes it in acceptance order.
    ///
    /// This returns an error rather than wrapping after sequence `u64::MAX`.
    pub(crate) fn update(
        &self,
        update: impl FnOnce(&T) -> T,
    ) -> Result<LivePublication<T>, PublicationSequenceExhausted> {
        let (accepted, owns_drain) = {
            let mut state = self
                .inner
                .accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let sequence = state
                .latest
                .sequence
                .checked_add(1)
                .ok_or(PublicationSequenceExhausted)?;
            let accepted = LivePublication {
                sequence,
                state: update(&state.latest.state),
            };
            state.latest = accepted.clone();
            state.queued.push_back(accepted.clone());
            let owns_drain = !state.draining;
            if owns_drain {
                state.draining = true;
            }
            drop(state);
            (accepted, owns_drain)
        };
        if owns_drain {
            self.drain();
        }
        Ok(accepted)
    }

    fn drain(&self) {
        let mut reset = DrainReset {
            source: self,
            armed: true,
        };
        loop {
            let next = {
                let mut state = self
                    .inner
                    .accepted
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let next = state.queued.pop_front();
                if next.is_none() {
                    state.draining = false;
                }
                next
            };
            let Some(next) = next else {
                reset.armed = false;
                return;
            };
            self.inner.publication.set(next);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hyphae::{Signal, Watchable as _};

    use super::*;
    use crate::{LiveSubscriptionState, SubscriptionLiveness};

    fn wait_until(mut ready: impl FnMut() -> bool) {
        let started = std::time::Instant::now();
        while !ready() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(1),
                "publication did not arrive"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn sequence_exhaustion_rejects_the_update_and_can_terminate_the_stream() {
        let source = PublicationSource::new("last accepted");
        source
            .inner
            .accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .latest
            .sequence = u64::MAX;
        let stream = LivePublicationStream::from_cell(&source.publication());
        let deadline = std::time::Duration::from_secs(1);
        assert_eq!(
            stream.receiver.recv_timeout(deadline).map(|p| p.state),
            Ok("last accepted")
        );
        let update = source.update(|_| "must not be accepted");
        assert_eq!(update, Err(PublicationSequenceExhausted));
        assert_eq!(source.current().state, "last accepted");
        if let Err(error) = update {
            source.fail(error);
        }
        assert_eq!(
            stream.receiver.recv_timeout(deadline),
            Err(flume::RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn publication_mailbox_is_bounded_and_discards_older_and_duplicate_versions() {
        let (mut delivery, receiver) = PublicationReceiver::new();
        for sequence in 0..10_000 {
            delivery.publish(LivePublication {
                sequence,
                state: sequence,
            });
            assert_eq!(receiver.len(), 1);
        }
        delivery.publish(LivePublication {
            sequence: 3,
            state: 3,
        });
        delivery.publish(LivePublication {
            sequence: 9_999,
            state: 0,
        });
        assert_eq!(
            receiver.try_recv(),
            Ok(LivePublication {
                sequence: 9_999,
                state: 9_999
            })
        );
        assert_eq!(receiver.try_recv(), Err(flume::TryRecvError::Empty));
    }

    #[test]
    fn slow_publication_stream_retains_the_latest_complete_snapshot() {
        let source = PublicationSource::new(0_u64);
        let mut stream = LivePublicationStream::from_cell(&source.publication());
        hyphae::batch(|| {
            for value in 1..=1_000 {
                assert!(source.update(|_| value).is_ok());
                assert!(stream.receiver.len() <= 1);
            }
        });
        let mut latest = None;
        wait_until(|| {
            if let Ok(publication) = stream.try_recv() {
                assert_eq!(publication.sequence, publication.state);
                latest = Some(publication);
            }
            latest
                .as_ref()
                .is_some_and(|publication| publication.sequence == 1_000)
        });
        assert_eq!(
            latest,
            Some(LivePublication {
                sequence: 1_000,
                state: 1_000
            })
        );
        assert_eq!(stream.try_recv(), Err(flume::TryRecvError::Empty));
    }

    #[test]
    fn publication_stream_delivers_a_newer_snapshot_after_its_seed() {
        let cell = Cell::new(LivePublication {
            sequence: 4,
            state: "initial",
        });
        let mut stream = LivePublicationStream::from_cell(&cell.clone().lock());
        let deadline = std::time::Duration::from_secs(1);
        assert_eq!(
            stream.receiver.recv_timeout(deadline).map(|p| p.state),
            Ok("initial")
        );
        cell.set(LivePublication {
            sequence: 2,
            state: "older",
        });
        cell.set(LivePublication {
            sequence: 4,
            state: "duplicate",
        });
        cell.set(LivePublication {
            sequence: 5,
            state: "newer",
        });

        assert_eq!(
            stream.receiver.recv_timeout(deadline).map(|p| p.state),
            Ok("newer")
        );
        assert_eq!(stream.try_recv(), Err(flume::TryRecvError::Empty));
    }

    #[test]
    fn publication_stream_is_installed_before_initial_delivery() {
        let source = PublicationSource::new("initial");
        let stream = LivePublicationStream::from_cell(&source.publication());
        let deadline = std::time::Duration::from_secs(1);
        assert_eq!(
            stream.receiver.recv_timeout(deadline),
            Ok(LivePublication {
                sequence: 0,
                state: "initial"
            })
        );
        assert!(source.update(|_| "changed during delivery").is_ok());
        assert_eq!(
            stream.receiver.recv_timeout(deadline),
            Ok(LivePublication {
                sequence: 1,
                state: "changed during delivery"
            })
        );
    }

    #[test]
    fn publication_stream_owns_the_source_until_dropped() {
        let cell = Cell::new(LivePublication {
            sequence: 0,
            state: 1_u64,
        });
        let weak = cell.downgrade();
        let stream = LivePublicationStream::from_cell(&cell.clone().lock());
        drop(cell);
        assert!(weak.upgrade().is_some());
        drop(stream);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn publication_stream_drains_queued_snapshot_before_terminal() {
        for fail in [false, true] {
            let cell = Cell::new(LivePublication {
                sequence: 0,
                state: "last value",
            });
            let stream = LivePublicationStream::from_cell(&cell.clone().lock());
            if fail {
                cell.fail(std::io::Error::other("publication failed"));
            } else {
                cell.complete();
            }
            let deadline = std::time::Duration::from_secs(1);
            assert_eq!(
                stream.receiver.recv_timeout(deadline),
                Ok(LivePublication {
                    sequence: 0,
                    state: "last value"
                })
            );
            assert_eq!(
                stream.receiver.recv_timeout(deadline),
                Err(flume::RecvTimeoutError::Disconnected)
            );
        }
    }

    #[test]
    fn publication_stream_closes_on_completion_or_failure() {
        for fail in [false, true] {
            let cell = Cell::new(LivePublication {
                sequence: 0,
                state: "last value",
            });
            let stream = LivePublicationStream::from_cell(&cell.clone().lock());
            let deadline = std::time::Duration::from_secs(1);
            assert_eq!(
                stream.receiver.recv_timeout(deadline).map(|p| p.state),
                Ok("last value")
            );
            if fail {
                cell.fail(std::io::Error::other("publication failed"));
            } else {
                cell.complete();
            }
            assert_eq!(
                stream.receiver.recv_timeout(deadline),
                Err(flume::RecvTimeoutError::Disconnected)
            );
        }
    }

    fn observed_sequences(
        source: &PublicationSource<u64>,
    ) -> (Arc<Mutex<Vec<u64>>>, hyphae::SubscriptionGuard) {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        let guard = source.publication().subscribe(move |signal| {
            if let Signal::Value(publication) = signal {
                callback_observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(publication.sequence);
            }
        });
        (observed, guard)
    }

    #[test]
    fn reentrant_update_is_drained_after_the_publication_that_triggered_it() {
        let source = PublicationSource::new(0_u64);
        let callback_source = source.clone();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        let _guard = source.publication().subscribe(move |signal| {
            let Signal::Value(publication) = signal else {
                return;
            };
            callback_observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(publication.sequence);
            if publication.sequence == 1 {
                let _accepted = callback_source.update(|value| value + 1);
            }
        });
        assert!(source.update(|value| value + 1).is_ok());
        wait_until(|| {
            source.current().state == 2
                && observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    == 3
        });
        assert_eq!(source.current().state, 2);
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[0, 1, 2]
        );
    }

    #[test]
    fn concurrent_updates_publish_every_accepted_sequence_in_order() {
        let source = PublicationSource::new(Vec::<u64>::new());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_observed = Arc::clone(&observed);
        let _guard = source.publication().subscribe(move |signal| {
            if let Signal::Value(publication) = signal {
                callback_observed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(publication.sequence);
            }
        });
        let handles = (0..8)
            .map(|value| {
                let source = source.clone();
                std::thread::spawn(move || {
                    source.update(|values| {
                        let mut values = values.clone();
                        values.push(value);
                        values
                    })
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            assert!(matches!(handle.join(), Ok(Ok(_))));
        }
        wait_until(|| {
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                == 9
        });
        assert_eq!(source.current().sequence, 8);
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn lifecycle_only_update_preserves_the_latest_accepted_payload() {
        let source = PublicationSource::new(LiveSubscriptionState::<String, u64> {
            value: None,
            through: None,
            liveness: SubscriptionLiveness::Connecting,
        });
        assert!(
            source
                .update(|_| LiveSubscriptionState {
                    value: Some("committed".to_owned()),
                    through: Some(4),
                    liveness: SubscriptionLiveness::Current,
                })
                .is_ok()
        );
        assert!(
            source
                .update(|state| LiveSubscriptionState {
                    value: state.value.clone(),
                    through: Some(5),
                    liveness: SubscriptionLiveness::Resynchronizing {
                        reason: "handoff".to_owned(),
                    },
                })
                .is_ok()
        );
        let current = source.current();
        assert_eq!(current.sequence, 2);
        assert_eq!(current.state.value.as_deref(), Some("committed"));
        assert_eq!(current.state.through, Some(5));
    }

    #[test]
    fn outer_batch_publishes_every_accepted_sequence() {
        let source = PublicationSource::new(0_u64);
        let (observed, _guard) = observed_sequences(&source);
        hyphae::batch(|| {
            for _ in 0..3 {
                assert!(source.update(|value| value + 1).is_ok());
            }
        });
        wait_until(|| {
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                == 4
        });
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[0, 1, 2, 3]
        );
    }
}
