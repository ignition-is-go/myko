//! Per-connection view delivery. Only pending deltas are retained; source maps
//! and their reactive caches remain owned by the native subscriptions.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use myko::{item::AnyItem, server::PendingQueryResponse, wire::ViewSampleRate};

pub enum ViewDeliveryControl {
    Subscribe {
        tx: Arc<str>,
        rate: Option<ViewSampleRate>,
    },
    SetRate {
        tx: Arc<str>,
        rate: Option<ViewSampleRate>,
    },
    Cancel(Arc<str>),
}

struct Subscription {
    rate: Option<ViewSampleRate>,
    next_sequence: u64,
    last_emit: Instant,
    /// Changes for the frame due at `due`.
    pending: Option<(Instant, PendingDelta)>,
    /// Changes produced at or after `pending`'s frame instant, while that
    /// frame was not yet sent: they belong to the frame after it.
    late: Option<(Instant, PendingDelta)>,
}

struct PendingDelta {
    latest: PendingQueryResponse,
    changes: BTreeMap<Arc<str>, Option<Arc<dyn AnyItem>>>,
}

impl PendingDelta {
    fn new(mut response: PendingQueryResponse) -> Self {
        let mut changes = BTreeMap::new();
        Self::take_changes(&mut changes, &mut response);
        Self {
            latest: response,
            changes,
        }
    }

    fn take_changes(
        changes: &mut BTreeMap<Arc<str>, Option<Arc<dyn AnyItem>>>,
        response: &mut PendingQueryResponse,
    ) {
        for id in response.deletes.drain(..) {
            changes.insert(id, None);
        }
        for item in response.upsert_items.drain(..) {
            changes.insert(item.id(), Some(item));
        }
    }

    fn merge(&mut self, mut response: PendingQueryResponse) {
        Self::take_changes(&mut self.changes, &mut response);
        // An update without a window change must preserve a previously queued
        // order change in this interval.
        if response.window_order_ids.is_none() {
            response.window_order_ids = self.latest.window_order_ids.take();
        }
        self.latest = response;
    }

    fn finish(mut self, sequence: u64) -> PendingQueryResponse {
        self.latest.sequence = sequence;
        for (id, item) in self.changes {
            match item {
                Some(item) => self.latest.upsert_items.push(item),
                None => self.latest.deletes.push(id),
            }
        }
        self.latest
    }
}

#[derive(Default)]
pub struct ViewDelivery {
    subscriptions: HashMap<Arc<str>, Subscription>,
}

impl ViewDelivery {
    pub(crate) fn control(&mut self, control: ViewDeliveryControl, now: Instant) {
        match control {
            ViewDeliveryControl::Subscribe { tx, rate } => {
                self.subscriptions.insert(
                    tx,
                    Subscription {
                        rate,
                        next_sequence: 0,
                        last_emit: now,
                        pending: None,
                        late: None,
                    },
                );
            }
            ViewDeliveryControl::SetRate { tx, rate } => {
                if let Some(subscription) = self.subscriptions.get_mut(&tx) {
                    subscription.rate = rate;
                    // Queued frames follow the new rate: due at its next grid
                    // instant, or at once without a rate.
                    let due = next_frame(now, rate);
                    for (queued_due, _) in [&mut subscription.pending, &mut subscription.late]
                        .into_iter()
                        .flatten()
                    {
                        *queued_due = (*queued_due).min(due);
                    }
                }
            }
            ViewDeliveryControl::Cancel(tx) => {
                self.subscriptions.remove(&tx);
            }
        }
    }

    /// Queue `response`, produced at `produced`. A sampled view's frame is cut
    /// at its grid instant: a change produced at or after the instant of the
    /// frame still pending goes to the frame after it, so every subscription
    /// puts one change in the same frame however late its connection wakes.
    pub(crate) fn push(
        &mut self,
        mut response: PendingQueryResponse,
        produced: Instant,
    ) -> Option<PendingQueryResponse> {
        let subscription = self.subscriptions.get_mut(&response.tx)?;
        if response.sequence == 0 {
            subscription.pending = None;
            subscription.late = None;
            subscription.next_sequence = 1;
            subscription.last_emit = produced;
            return Some(response);
        }
        if subscription.rate.is_none() && subscription.pending.is_none() {
            response.sequence = subscription.next_sequence;
            subscription.next_sequence = subscription.next_sequence.saturating_add(1);
            subscription.last_emit = produced;
            return Some(response);
        }
        let due = next_frame(produced, subscription.rate);
        match &mut subscription.pending {
            Some((pending_due, _)) if produced >= *pending_due && subscription.rate.is_some() => {
                match &mut subscription.late {
                    Some((_, late)) => late.merge(response),
                    None => subscription.late = Some((due, PendingDelta::new(response))),
                }
            }
            Some((_, pending)) => pending.merge(response),
            None => subscription.pending = Some((due, PendingDelta::new(response))),
        }
        None
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.subscriptions
            .values()
            .filter_map(|subscription| subscription.pending.as_ref().map(|(due, _)| *due))
            .min()
    }

    pub(crate) fn take_due(&mut self, now: Instant) -> Option<PendingQueryResponse> {
        let subscription = self.subscriptions.values_mut().find(|subscription| {
            subscription
                .pending
                .as_ref()
                .is_some_and(|(due, _)| *due <= now)
        })?;
        let (_, pending) = subscription.pending.take()?;
        subscription.pending = subscription.late.take();
        let response = pending.finish(subscription.next_sequence);
        subscription.next_sequence = subscription.next_sequence.saturating_add(1);
        subscription.last_emit = now;
        Some(response)
    }
}

/// The process-wide origin of the frame grid.
static FRAME_EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

/// When the frame after one sent at `last_emit` is due: the next point of a
/// grid of `rate`'s interval shared by the whole process. Every subscription at
/// one rate sends at the same instants, whenever it subscribed, and a late send
/// does not shift later frames. Without a rate a frame is due at once.
fn next_frame(last_emit: Instant, rate: Option<ViewSampleRate>) -> Instant {
    let Some(interval) = rate.map(ViewSampleRate::interval) else {
        return last_emit;
    };
    let epoch = *FRAME_EPOCH;
    let step = interval.as_nanos().max(1);
    let since = last_emit.saturating_duration_since(epoch).as_nanos();
    let slot = since.checked_div(step).unwrap_or(0);
    let next = slot.saturating_add(1).saturating_mul(step);
    let next = u64::try_from(next).unwrap_or(u64::MAX);
    epoch
        .checked_add(std::time::Duration::from_nanos(next))
        .unwrap_or(last_emit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use myko::common::with_id::WithId;
    use std::time::Duration;

    #[derive(Debug, serde::Serialize, PartialEq)]
    struct Row {
        id: Arc<str>,
        value: u32,
    }
    impl WithId for Row {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }
    impl AnyItem for Row {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn entity_type(&self) -> &'static str {
            "Row"
        }
        fn equals(&self, other: &dyn AnyItem) -> bool {
            other.as_any().downcast_ref::<Self>() == Some(self)
        }
    }

    fn response(sequence: u64, rows: &[(&str, u32)], deletes: &[&str]) -> PendingQueryResponse {
        PendingQueryResponse {
            tx: "view".into(),
            sequence,
            upsert_items: rows
                .iter()
                .map(|(id, value)| -> Arc<dyn AnyItem> {
                    Arc::new(Row {
                        id: (*id).into(),
                        value: *value,
                    })
                })
                .collect(),
            deletes: deletes.iter().map(|id| (*id).into()).collect(),
            total_count: 2,
            window: None,
            window_order_ids: None,
        }
    }

    fn rate(fps: f64) -> Option<ViewSampleRate> {
        let rate = ViewSampleRate::try_from(fps).ok();
        assert!(rate.is_some(), "invalid test sample rate");
        rate
    }

    /// A frame instant of `fps`'s process-wide grid. Other tests start the
    /// grid, so a bare `Instant::now()` falls anywhere inside a frame.
    fn on_grid(fps: f64) -> Instant {
        next_frame(Instant::now(), rate(fps))
    }

    fn sampled(now: Instant) -> ViewDelivery {
        let mut delivery = ViewDelivery::default();
        delivery.control(
            ViewDeliveryControl::Subscribe {
                tx: "view".into(),
                rate: rate(30.0),
            },
            now,
        );
        assert_eq!(
            delivery
                .push(response(0, &[("a", 0)], &[]), now)
                .map(|frame| frame.sequence),
            Some(0)
        );
        delivery
    }

    #[test]
    fn coalesces_latest_values_and_deletes_into_one_contiguous_payload() {
        let now = on_grid(30.0);
        let mut delivery = sampled(now);
        assert!(
            delivery
                .push(response(1, &[("a", 1), ("b", 1)], &[]), now)
                .is_none()
        );
        assert!(
            delivery
                .push(response(2, &[("a", 2)], &["b"]), now)
                .is_none()
        );
        assert!(
            delivery
                .push(response(3, &[("c", 3)], &["a"]), now)
                .is_none()
        );
        assert!(delivery.push(response(4, &[("a", 4)], &[]), now).is_none());
        assert!(delivery.take_due(now + Duration::from_millis(30)).is_none());
        let frame = delivery.take_due(now + Duration::from_millis(34));
        assert_eq!(frame.as_ref().map(|frame| frame.sequence), Some(1));
        assert_eq!(
            frame.as_ref().map(|frame| frame.deletes.as_slice()),
            Some([Arc::<str>::from("b")].as_slice())
        );
        let values: Option<Vec<_>> = frame.as_ref().and_then(|frame| {
            frame
                .upsert_items
                .iter()
                .map(|row| {
                    row.as_any()
                        .downcast_ref::<Row>()
                        .map(|row| (row.id.as_ref(), row.value))
                })
                .collect()
        });
        assert_eq!(values, Some(vec![("a", 4), ("c", 3)]));
        assert!(delivery.next_deadline().is_none());
        delivery.push(
            response(5, &[("a", 5)], &[]),
            now + Duration::from_millis(35),
        );
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(68))
                .map(|frame| frame.sequence),
            Some(2)
        );
    }

    #[test]
    fn rate_change_flushes_pending_without_replacing_subscription() {
        let now = on_grid(60.0);
        let mut delivery = sampled(now);
        delivery.push(response(1, &[("a", 1)], &[]), now);
        delivery.control(
            ViewDeliveryControl::SetRate {
                tx: "view".into(),
                rate: None,
            },
            now,
        );
        assert_eq!(delivery.take_due(now).map(|frame| frame.sequence), Some(1));
        assert_eq!(
            delivery
                .push(response(2, &[("a", 2)], &[]), now)
                .map(|frame| frame.sequence),
            Some(2)
        );
        delivery.control(
            ViewDeliveryControl::SetRate {
                tx: "view".into(),
                rate: rate(60.0),
            },
            now,
        );
        delivery.push(response(3, &[("a", 3)], &[]), now);
        assert!(delivery.take_due(now + Duration::from_millis(16)).is_none());
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(17))
                .map(|frame| frame.sequence),
            Some(3)
        );
    }

    #[test]
    fn reset_and_cancel_discard_pending_changes() {
        let now = Instant::now();
        let mut delivery = sampled(now);
        delivery.push(response(1, &[("old", 1)], &[]), now);
        assert_eq!(
            delivery
                .push(response(0, &[("new", 1)], &[]), now)
                .map(|frame| frame.sequence),
            Some(0)
        );
        assert!(delivery.next_deadline().is_none());
        delivery.push(response(1, &[("new", 2)], &[]), now);
        delivery.control(ViewDeliveryControl::Cancel("view".into()), now);
        assert!(delivery.take_due(now + Duration::from_secs(1)).is_none());
        assert!(
            delivery
                .push(response(2, &[("late", 1)], &[]), now)
                .is_none()
        );
    }

    #[test]
    fn retains_latest_window_order_when_later_update_only_changes_a_value() {
        let now = on_grid(30.0);
        let mut delivery = sampled(now);
        let mut ordered = response(1, &[("a", 1)], &[]);
        ordered.window_order_ids = Some(vec!["a".into(), "b".into()]);
        delivery.push(ordered, now);
        delivery.push(response(2, &[("a", 2)], &[]), now);
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(34))
                .and_then(|frame| frame.window_order_ids),
            Some(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn late_sends_do_not_stretch_the_sample_interval() {
        let start = on_grid(60.0);
        let mut delivery = ViewDelivery::default();
        delivery.control(
            ViewDeliveryControl::Subscribe {
                tx: "view".into(),
                rate: rate(60.0),
            },
            start,
        );
        delivery.push(response(0, &[("a", 0)], &[]), start);
        let interval = Duration::from_secs_f64(1.0 / 60.0);
        let late = Duration::from_millis(1);
        let mut now = start;
        for frame in 1..=60_u32 {
            delivery.push(response(u64::from(frame), &[("a", frame)], &[]), now);
            let due = delivery.next_deadline();
            assert!(due.is_some(), "frame {frame} is pending");
            let due = due.unwrap_or(now);
            now = due + late;
            assert!(delivery.take_due(now).is_some(), "frame {frame} is due");
        }
        // Sixty frames at 60 Hz, each sent 1 ms late: the schedule stays on
        // its grid (about 1 s), instead of drifting by 1 ms per frame.
        let elapsed = now.duration_since(start);
        assert!(
            elapsed < Duration::from_millis(1_005) && elapsed >= interval * 60,
            "60 frames took {elapsed:?}"
        );
    }

    #[test]
    fn subscriptions_at_one_rate_share_frame_instants() {
        let start = Instant::now();
        let subscribe = |tx: &str, at: Instant, delivery: &mut ViewDelivery| {
            delivery.control(
                ViewDeliveryControl::Subscribe {
                    tx: tx.into(),
                    rate: rate(60.0),
                },
                at,
            );
        };
        let mut first = ViewDelivery::default();
        let mut second = ViewDelivery::default();
        subscribe("view", start, &mut first);
        // The second subscribes 7 ms later, out of phase with the first.
        let later = start + Duration::from_millis(7);
        subscribe("view", later, &mut second);
        first.push(response(0, &[("a", 0)], &[]), start);
        second.push(response(0, &[("a", 0)], &[]), later);
        let now = later + Duration::from_millis(1);
        first.push(response(1, &[("a", 1)], &[]), now);
        second.push(response(1, &[("a", 1)], &[]), now);
        let (a, b) = (first.next_deadline(), second.next_deadline());
        assert!(a.is_some() && b.is_some());
        assert_eq!(a, b, "both send their next frame at the same instant");
        // A frame is never more than one interval away, so a subscription
        // that fell behind gets no burst of catch-up frames.
        let due = a.unwrap_or(now);
        assert!(due.saturating_duration_since(now) <= Duration::from_secs_f64(1.0 / 60.0));
    }

    #[test]
    fn a_late_connection_cuts_its_frame_at_the_grid_instant() {
        let start = on_grid(60.0);
        let subscribe = |delivery: &mut ViewDelivery| {
            delivery.control(
                ViewDeliveryControl::Subscribe {
                    tx: "view".into(),
                    rate: rate(60.0),
                },
                start,
            );
            delivery.push(response(0, &[("a", 0)], &[]), start);
        };
        let (mut prompt, mut late) = (ViewDelivery::default(), ViewDelivery::default());
        subscribe(&mut prompt);
        subscribe(&mut late);
        // One change just before a frame instant, one just after it.
        let before = start + Duration::from_millis(1);
        prompt.push(response(1, &[("a", 1)], &[]), before);
        late.push(response(1, &[("a", 1)], &[]), before);
        let instant = prompt.next_deadline();
        assert!(instant.is_some() && instant == late.next_deadline());
        let instant = instant.unwrap_or(start);
        let after = instant + Duration::from_micros(200);
        // The prompt connection sends at the instant, before the second change
        // reaches it; the late one wakes 5 ms later with both queued.
        let first = prompt.take_due(instant);
        prompt.push(response(2, &[("a", 2)], &[]), after);
        late.push(response(2, &[("a", 2)], &[]), after);
        let late_first = late.take_due(instant + Duration::from_millis(5));
        let values =
            |frame: Option<PendingQueryResponse>| frame.map(|frame| frame.upsert_items.len());
        assert_eq!(values(first), Some(1));
        assert_eq!(
            values(late_first),
            Some(1),
            "the later change waits for the next frame"
        );
        // Both send the second change at the following instant, together.
        assert_eq!(prompt.next_deadline(), late.next_deadline());
    }
}
