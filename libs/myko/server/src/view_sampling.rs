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
    pending: Option<PendingDelta>,
}

struct PendingDelta {
    latest: PendingQueryResponse,
    changes: BTreeMap<Arc<str>, Option<Arc<dyn AnyItem>>>,
}

impl PendingDelta {
    fn new(response: PendingQueryResponse) -> Self {
        let mut pending = Self {
            latest: response.clone(),
            changes: BTreeMap::new(),
        };
        pending.merge(response);
        pending
    }

    fn merge(&mut self, mut response: PendingQueryResponse) {
        for id in response.deletes.drain(..) {
            self.changes.insert(id, None);
        }
        for item in response.upsert_items.drain(..) {
            self.changes.insert(item.id(), Some(item));
        }
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
                    },
                );
            }
            ViewDeliveryControl::SetRate { tx, rate } => {
                if let Some(subscription) = self.subscriptions.get_mut(&tx) {
                    subscription.rate = rate;
                }
            }
            ViewDeliveryControl::Cancel(tx) => {
                self.subscriptions.remove(&tx);
            }
        }
    }

    pub(crate) fn push(
        &mut self,
        mut response: PendingQueryResponse,
        now: Instant,
    ) -> Option<PendingQueryResponse> {
        let subscription = self.subscriptions.get_mut(&response.tx)?;
        if response.sequence == 0 {
            subscription.pending = None;
            subscription.next_sequence = 1;
            subscription.last_emit = now;
            return Some(response);
        }
        if subscription.rate.is_none() && subscription.pending.is_none() {
            response.sequence = subscription.next_sequence;
            subscription.next_sequence = subscription.next_sequence.saturating_add(1);
            subscription.last_emit = now;
            return Some(response);
        }
        match &mut subscription.pending {
            Some(pending) => pending.merge(response),
            None => subscription.pending = Some(PendingDelta::new(response)),
        }
        None
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.subscriptions
            .values()
            .filter_map(|subscription| {
                subscription.pending.as_ref()?;
                subscription.last_emit.checked_add(
                    subscription
                        .rate
                        .map_or(std::time::Duration::ZERO, ViewSampleRate::interval),
                )
            })
            .min()
    }

    pub(crate) fn take_due(&mut self, now: Instant) -> Option<PendingQueryResponse> {
        let subscription = self.subscriptions.values_mut().find(|subscription| {
            subscription.pending.is_some()
                && subscription
                    .last_emit
                    .checked_add(
                        subscription
                            .rate
                            .map_or(std::time::Duration::ZERO, ViewSampleRate::interval),
                    )
                    .is_some_and(|deadline| deadline <= now)
        })?;
        let pending = subscription.pending.take()?;
        let response = pending.finish(subscription.next_sequence);
        subscription.next_sequence = subscription.next_sequence.saturating_add(1);
        subscription.last_emit = now;
        Some(response)
    }
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
                .map(|(id, value)| {
                    Arc::new(Row {
                        id: (*id).into(),
                        value: *value,
                    }) as Arc<dyn AnyItem>
                })
                .collect(),
            deletes: deletes.iter().map(|id| (*id).into()).collect(),
            total_count: 2,
            window: None,
            window_order_ids: None,
        }
    }

    fn sampled(now: Instant) -> ViewDelivery {
        let mut delivery = ViewDelivery::default();
        delivery.control(
            ViewDeliveryControl::Subscribe {
                tx: "view".into(),
                rate: Some(ViewSampleRate::try_from(30.0).unwrap()),
            },
            now,
        );
        assert_eq!(
            delivery
                .push(response(0, &[("a", 0)], &[]), now)
                .unwrap()
                .sequence,
            0
        );
        delivery
    }

    #[test]
    fn coalesces_latest_values_and_deletes_into_one_contiguous_payload() {
        let now = Instant::now();
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
        let frame = delivery.take_due(now + Duration::from_millis(34)).unwrap();
        assert_eq!(frame.sequence, 1);
        assert_eq!(frame.deletes, vec![Arc::<str>::from("b")]);
        let values: Vec<_> = frame
            .upsert_items
            .iter()
            .map(|r| {
                let r = r.as_any().downcast_ref::<Row>().unwrap();
                (r.id.as_ref(), r.value)
            })
            .collect();
        assert_eq!(values, vec![("a", 4), ("c", 3)]);
        assert!(delivery.next_deadline().is_none());
        delivery.push(
            response(5, &[("a", 5)], &[]),
            now + Duration::from_millis(35),
        );
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(68))
                .unwrap()
                .sequence,
            2
        );
    }

    #[test]
    fn rate_change_flushes_pending_without_replacing_subscription() {
        let now = Instant::now();
        let mut delivery = sampled(now);
        delivery.push(response(1, &[("a", 1)], &[]), now);
        delivery.control(
            ViewDeliveryControl::SetRate {
                tx: "view".into(),
                rate: None,
            },
            now,
        );
        assert_eq!(delivery.take_due(now).unwrap().sequence, 1);
        assert_eq!(
            delivery
                .push(response(2, &[("a", 2)], &[]), now)
                .unwrap()
                .sequence,
            2
        );
        delivery.control(
            ViewDeliveryControl::SetRate {
                tx: "view".into(),
                rate: Some(ViewSampleRate::try_from(60.0).unwrap()),
            },
            now,
        );
        delivery.push(response(3, &[("a", 3)], &[]), now);
        assert!(delivery.take_due(now + Duration::from_millis(16)).is_none());
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(17))
                .unwrap()
                .sequence,
            3
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
                .unwrap()
                .sequence,
            0
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
        let now = Instant::now();
        let mut delivery = sampled(now);
        let mut ordered = response(1, &[("a", 1)], &[]);
        ordered.window_order_ids = Some(vec!["a".into(), "b".into()]);
        delivery.push(ordered, now);
        delivery.push(response(2, &[("a", 2)], &[]), now);
        assert_eq!(
            delivery
                .take_due(now + Duration::from_millis(34))
                .unwrap()
                .window_order_ids,
            Some(vec!["a".into(), "b".into()])
        );
    }
}
