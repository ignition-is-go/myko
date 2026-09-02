//! Process-local reactive sources for framework-owned views.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use myko_federation::{
    LiveSubscription, LiveSubscriptionState, LiveSubscriptionWriter, SubscriptionLiveness,
};

#[derive(Clone)]
pub struct RuntimeFeed<T>
where
    T: hyphae::CellValue,
{
    revision: Arc<AtomicU64>,
    writer: LiveSubscriptionWriter<Vec<T>, u64>,
    pub live: LiveSubscription<Vec<T>, u64>,
}

impl<T> RuntimeFeed<T>
where
    T: hyphae::CellValue,
{
    #[must_use]
    pub fn new(initial: Vec<T>) -> Self {
        let (writer, live) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(initial),
            through: Some(0),
            liveness: SubscriptionLiveness::Current,
        });
        Self {
            revision: Arc::new(AtomicU64::new(0)),
            writer,
            live,
        }
    }

    pub fn publish(&self, value: Vec<T>) {
        let revision = self.revision.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        self.writer.publish(value, Some(revision));
    }

    pub fn invalidate(&self, reason: impl Into<String>) {
        self.writer.invalidate(reason.into());
    }
}
