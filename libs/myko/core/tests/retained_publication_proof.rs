//! Scheduler experiments comparing a rejected split-cell design with an atomic
//! publication. Production handler wiring is covered by
//! `registered_retained_view_gates_cached_output_and_preserves_publications`.

use std::sync::{Arc, Barrier, Mutex, MutexGuard, OnceLock};

use hyphae::{
    Cell, Gettable as _, JoinExt as _, MapExt as _, Materialize as _, Mutable as _, Signal,
    SwitchMapExt as _, Watchable as _, batch,
};

type ObservedPublications = Arc<Mutex<Vec<(u64, u64)>>>;
type RecordedSubscription = (ObservedPublications, hyphae::SubscriptionGuard);

fn record_publications(
    publication: &Cell<(u64, u64), hyphae::CellImmutable>,
) -> RecordedSubscription {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let callback_observed = Arc::clone(&observed);
    let guard = publication.subscribe(move |signal| {
        if let Signal::Value(value) = signal {
            callback_observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(**value);
        }
    });
    (observed, guard)
}

fn isolate_scheduler() -> MutexGuard<'static, ()> {
    static TEST_GATE: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_GATE
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn joined_evidence_never_names_a_version_the_final_output_has_not_settled() {
    let _scheduler_guard = isolate_scheduler();
    let source = Cell::new(0_u64);
    let evidence = Cell::new(0_u64);
    let output = source.clone().map(|value| value / 2).materialize();
    let publication = output
        .join(evidence.clone())
        .map(|pair| (pair.0, pair.1))
        .materialize();
    let (observed, _guard) = record_publications(&publication);

    for version in 1..=20 {
        batch(|| {
            source.set(version);
            evidence.set(version);
        });
    }

    assert_eq!(publication.get(), (10, 20));
    assert!(
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|(value, version)| *value == *version / 2)
    );
}

#[test]
fn joined_evidence_tracks_a_deeper_dependency_selected_in_the_same_tick() {
    let _scheduler_guard = isolate_scheduler();
    let select_deep = Cell::new(false);
    let deep_source = Cell::new(0_u64);
    let evidence = Cell::new(0_u64);
    let selected_source = deep_source.clone();
    let output = select_deep
        .clone()
        .switch_map(move |deep| {
            if *deep {
                selected_source
                    .clone()
                    .map(|value| value * 10)
                    .materialize()
            } else {
                Cell::new(0_u64).lock()
            }
        })
        .materialize();
    let publication = output
        .clone()
        .join(evidence.clone())
        .map(|pair| (pair.0, pair.1))
        .materialize();
    let (observed, _guard) = record_publications(&publication);

    batch(|| {
        deep_source.set(7);
        select_deep.set(true);
        evidence.set(7);
    });

    assert!(select_deep.get());
    assert_eq!(deep_source.get(), 7);
    assert_eq!(evidence.get(), 7);
    assert_eq!(output.get(), 70);
    assert_eq!(publication.get(), (70, 7));
    assert!(
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|(value, version)| (*version == 0 && *value == 0) || *value == *version * 10)
    );
}

#[test]
fn split_cells_can_publish_crossed_source_and_evidence_versions() {
    let _scheduler_guard = isolate_scheduler();
    let source = Cell::new(0_u64);
    let evidence = Cell::new(0_u64);
    let publication = source
        .clone()
        .join(evidence.clone())
        .map(|pair| (pair.0, pair.1))
        .materialize();
    let (observed, _guard) = record_publications(&publication);
    let first_payload_written = Arc::new(Barrier::new(2));
    let second_pair_written = Arc::new(Barrier::new(2));

    let handles = [1_u64, 2_u64].map(|version| {
        let thread_source = source.clone();
        let thread_evidence = evidence.clone();
        let first_payload_written = Arc::clone(&first_payload_written);
        let second_pair_written = Arc::clone(&second_pair_written);
        std::thread::spawn(move || {
            batch(|| {
                if version == 1 {
                    thread_source.set(version);
                    first_payload_written.wait();
                    second_pair_written.wait();
                    thread_evidence.set(version);
                } else {
                    first_payload_written.wait();
                    thread_source.set(version);
                    thread_evidence.set(version);
                    second_pair_written.wait();
                }
            });
        })
    });
    for handle in handles {
        assert!(handle.join().is_ok(), "publication worker must finish");
    }

    assert_eq!(publication.get(), (2, 1));
    assert!(
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&(2, 1))
    );
}

#[test]
fn immutable_payload_and_evidence_stay_paired_under_the_same_interleaving() {
    let _scheduler_guard = isolate_scheduler();
    let source = Cell::new((0_u64, 0_u64));
    let publication = source
        .clone()
        .map(|observed| (observed.0 / 2, observed.1))
        .materialize();
    let (observed, _guard) = record_publications(&publication);
    let first_payload_prepared = Arc::new(Barrier::new(2));
    let second_pair_written = Arc::new(Barrier::new(2));

    let handles = [1_u64, 2_u64].map(|version| {
        let source = source.clone();
        let first_payload_prepared = Arc::clone(&first_payload_prepared);
        let second_pair_written = Arc::clone(&second_pair_written);
        std::thread::spawn(move || {
            batch(|| {
                let payload = version;
                first_payload_prepared.wait();
                if version == 1 {
                    second_pair_written.wait();
                    source.set((payload, version));
                } else {
                    source.set((payload, version));
                    second_pair_written.wait();
                }
            });
        })
    });
    for handle in handles {
        assert!(handle.join().is_ok(), "publication worker must finish");
    }

    assert_eq!(publication.get(), (0, 1));
    assert_eq!(
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last(),
        Some(&(0, 1))
    );
    assert!(
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|(value, version)| *value == *version / 2)
    );
}
