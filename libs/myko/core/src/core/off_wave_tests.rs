//! What a holder of a [`deferred_cell`] observes, and that its build never
//! holds a propagation open.
#![allow(clippy::expect_used)]

use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use hyphae::{Cell, Gettable, Mutable, Signal, Watchable};

use super::deferred_cell;

const PATIENCE: Duration = Duration::from_secs(10);

fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while !done() {
        if started.elapsed() > PATIENCE {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

#[test]
fn holds_the_placeholder_then_follows_the_built_cell() {
    let source = Cell::new(1_u32);
    let built = source.clone();
    let (subscribed, gate) = mpsc::channel::<()>();
    let deferred = deferred_cell(0, move || {
        let _ = gate.recv_timeout(PATIENCE);
        built.lock()
    });
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let log = seen.clone();
    let _guard = deferred.subscribe(move |signal| {
        if let Signal::Value(value) = signal {
            log.lock().push(**value);
        }
    });
    subscribed.send(()).expect("gate");
    assert!(
        eventually(|| deferred.get() == 1),
        "follows the built value"
    );
    source.set(2);
    assert!(eventually(|| deferred.get() == 2), "follows later values");
    let seen = seen.lock().clone();
    assert_eq!(seen.first(), Some(&0), "placeholder first: {seen:?}");
    assert_eq!(seen.last(), Some(&2), "latest last: {seen:?}");
}

/// A build started from inside a propagation that waits for an unrelated
/// write to propagate would never finish if it ran inside that propagation.
#[test]
fn a_build_started_inside_a_propagation_does_not_hold_it() {
    let trigger = Cell::new(0_u32);
    let unrelated = Cell::new(0_u32);
    let (propagated_tx, propagated_rx) = mpsc::channel::<()>();
    let _unrelated_guard = unrelated.subscribe(move |signal| {
        if matches!(signal, Signal::Value(value) if **value > 0) {
            let _ = propagated_tx.send(());
        }
    });
    let (built_tx, built_rx) = mpsc::channel::<bool>();
    let holder = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let created = holder.clone();
    let propagated = Arc::new(parking_lot::Mutex::new(Some(propagated_rx)));
    let _trigger_guard = trigger.subscribe(move |signal| {
        if !matches!(signal, Signal::Value(value) if **value > 0) {
            return;
        }
        let (propagated_rx, built_tx) = (propagated.lock().take(), built_tx.clone());
        created.lock().push(deferred_cell(0, move || {
            let saw = propagated_rx.is_some_and(|rx| rx.recv_timeout(PATIENCE).is_ok());
            let _ = built_tx.send(saw);
            Cell::new(1).lock()
        }));
    });
    hyphae::batch(|| trigger.set(1));
    let writer = std::thread::spawn(move || unrelated.set(1));
    let saw = built_rx.recv_timeout(PATIENCE * 2).expect("build ran");
    writer.join().expect("writer");
    assert!(saw, "an unrelated write propagated while the build ran");
    let deferred = holder.lock().first().cloned().expect("deferred cell");
    assert!(eventually(|| deferred.get() == 1), "built value arrives");
}

#[test]
fn dropping_the_deferred_cell_releases_the_built_cell() {
    let source = Cell::new(1_u32);
    let weak = source.downgrade();
    let deferred = deferred_cell(0, move || source.lock());
    assert!(eventually(|| deferred.get() == 1), "built");
    drop(deferred);
    assert!(
        eventually(|| weak.upgrade().is_none()),
        "built cell released"
    );
}
