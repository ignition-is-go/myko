use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use hyphae::{Gettable as _, Signal, Watchable as _};
use myko_federation::{LiveSubscriptionState, SubscriptionLiveness, live_subscription};

// A separate test binary keeps the deliberately panicking callback off another
// test's thread when Hyphae's process-global scheduler has an active drain.
#[test]
fn observer_panic_releases_drain_ownership_for_the_next_update() {
    let (writer, live) = live_subscription(LiveSubscriptionState::<u64, u64> {
        value: Some(0),
        through: Some(0),
        liveness: SubscriptionLiveness::Current,
    });
    let panic_once = Arc::new(AtomicBool::new(true));
    let callback_panic_once = Arc::clone(&panic_once);
    let _guard = live.publication().subscribe(move |signal| {
        if let Signal::Value(publication) = signal
            && publication.sequence == 1
            && callback_panic_once.swap(false, Ordering::SeqCst)
        {
            std::panic::resume_unwind(Box::new("injected publication observer panic"));
        }
    });
    let first =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| writer.publish(1, Some(1))));
    assert!(first.is_err());
    writer.publish(2, Some(2));
    let publication = live.publication().get();
    assert_eq!(publication.sequence, 2);
    assert_eq!(publication.state.value, Some(2));
}
