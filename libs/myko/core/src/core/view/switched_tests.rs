//! What a subscriber of `ErasedViewMap::switched` receives.
//!
//! A subscriber mirrors its diffs the way the websocket session does:
//! `Initial` replaces the mirror, the rest apply in order.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    entities::{client::Client, server::ServerId},
    hyphae::{Cell, CellMap, CellMutable, MapDiff, SubscriptionGuard},
    prelude::*,
    view::ErasedViewMap,
};

type Rows = BTreeMap<Arc<str>, Arc<dyn AnyItem>>;
type Source = CellMap<Arc<str>, Arc<dyn AnyItem>, CellMutable>;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn row(id: &str, version: usize) -> Arc<dyn AnyItem> {
    Arc::new(Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from("s")),
        address: Some(Arc::from(format!("v{version}"))),
        windback: None,
    })
}

fn put(source: &Source, id: &str, version: usize) {
    source.insert(Arc::from(id), row(id, version));
}

#[derive(Default)]
struct Log {
    mirror: Rows,
    diffs: Vec<String>,
    initials: usize,
    started: bool,
    violation: Option<String>,
}

fn apply(log: &mut Log, diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>) {
    match diff {
        MapDiff::Initial { entries } => {
            log.initials += 1;
            log.mirror = entries.iter().cloned().collect();
            log.diffs.push(format!("initial {}", entries.len()));
        }
        MapDiff::Insert { key, value } => {
            log.mirror.insert(key.clone(), value.clone());
            log.diffs.push(format!("insert {key}"));
        }
        MapDiff::Update { key, new_value, .. } => {
            log.mirror.insert(key.clone(), new_value.clone());
            log.diffs.push(format!("update {key}"));
        }
        MapDiff::Remove { key, .. } => {
            log.mirror.remove(key);
            log.diffs.push(format!("remove {key}"));
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                apply(log, change);
            }
        }
    }
}

struct Subscriber {
    log: Arc<Mutex<Log>>,
    _guard: SubscriptionGuard,
}

impl Subscriber {
    fn new(map: &ErasedViewMap) -> Self {
        let log = Arc::new(Mutex::new(Log::default()));
        let sink = log.clone();
        let guard = map.subscribe_diffs(move |diff| {
            let mut log = sink.lock().expect("log");
            if !log.started && !matches!(diff, MapDiff::Initial { .. }) {
                log.violation = Some("first delivery was not Initial".into());
            }
            log.started = true;
            apply(&mut log, diff);
            // Each source holds only keys with its own prefix, so a mirror
            // that mixes prefixes holds rows of a map no longer selected.
            let mut prefixes = log.mirror.keys().map(|key| &key[..1]);
            if let Some(first) = prefixes.next()
                && prefixes.any(|prefix| prefix != first)
                && log.violation.is_none()
            {
                log.violation = Some(format!("mixed sources: {:?}", log.mirror.keys()));
            }
        });
        Self { log, _guard: guard }
    }

    fn rows(&self) -> Rows {
        self.log.lock().expect("log").mirror.clone()
    }

    fn take_diffs(&self) -> Vec<String> {
        std::mem::take(&mut self.log.lock().expect("log").diffs)
    }
}

fn rows_of(map: &ErasedViewMap) -> Rows {
    map.snapshot().into_iter().collect()
}

fn source_rows(source: &Source) -> Rows {
    source.snapshot().into_iter().collect()
}

fn assert_same(actual: &Rows, expected: &Rows) {
    let ids = |rows: &Rows| rows.keys().cloned().collect::<Vec<_>>();
    assert_eq!(ids(actual), ids(expected));
    for (id, row) in actual {
        assert!(row.equals(expected[id].as_ref()), "row {id} differs");
    }
}

/// Two sources and a selector over them, `None` meaning closed.
struct Fixture {
    a: Source,
    b: Source,
    selector: Cell<Option<char>, CellMutable>,
    map: ErasedViewMap,
}

fn fixture(selected: Option<char>) -> Fixture {
    let a: Source = CellMap::new();
    let b: Source = CellMap::new();
    let selector = Cell::new(selected);
    let (open_a, open_b) = (a.clone().lock(), b.clone().lock());
    let map = ErasedViewMap::switched(selector.clone().lock(), move |key| match key {
        'a' => open_a.clone().into(),
        _ => open_b.clone().into(),
    });
    Fixture {
        a,
        b,
        selector,
        map,
    }
}

#[test]
fn a_closed_gate_delivers_an_empty_initial_and_nothing_after() {
    let _serial = serial();
    let f = fixture(None);
    put(&f.a, "a1", 0);
    let sub = Subscriber::new(&f.map);
    assert_eq!(sub.take_diffs(), ["initial 0"]);
    put(&f.a, "a2", 0);
    put(&f.b, "b1", 0);
    assert!(
        sub.take_diffs().is_empty(),
        "a closed gate delivers nothing"
    );
    assert!(f.map.snapshot().is_empty(), "a closed gate snapshots empty");
}

#[test]
fn an_open_gate_starts_with_the_source_and_passes_its_diffs_in_order() {
    let _serial = serial();
    let f = fixture(Some('a'));
    put(&f.a, "a1", 0);
    put(&f.a, "a2", 0);
    let sub = Subscriber::new(&f.map);
    assert_eq!(sub.take_diffs(), ["initial 2"]);
    put(&f.a, "a3", 0);
    put(&f.a, "a1", 1);
    f.a.remove(&Arc::from("a2"));
    crate::hyphae::batch(|| {
        put(&f.a, "a4", 0);
        put(&f.a, "a3", 1);
    });
    let diffs = sub.take_diffs();
    assert_eq!(&diffs[..3], ["insert a3", "update a1", "remove a2"]);
    assert_eq!(diffs.len(), 5);
    assert_same(&sub.rows(), &source_rows(&f.a));
    assert_same(&rows_of(&f.map), &source_rows(&f.a));
    put(&f.b, "b1", 0);
    assert!(
        sub.take_diffs().is_empty(),
        "an unselected source is not read"
    );
}

#[test]
fn closing_empties_the_subscriber_and_reopening_restores_the_current_source() {
    let _serial = serial();
    let f = fixture(Some('a'));
    put(&f.a, "a1", 0);
    put(&f.a, "a2", 0);
    let sub = Subscriber::new(&f.map);
    sub.take_diffs();
    f.selector.set(None);
    assert_eq!(sub.take_diffs(), ["initial 0"]);
    assert!(sub.rows().is_empty());
    assert!(f.map.snapshot().is_empty());
    put(&f.a, "a3", 0);
    f.a.remove(&Arc::from("a1"));
    put(&f.a, "a2", 7);
    assert!(
        sub.take_diffs().is_empty(),
        "writes while closed are not delivered"
    );
    f.selector.set(Some('a'));
    assert_eq!(sub.take_diffs(), ["initial 2"]);
    assert_same(&sub.rows(), &source_rows(&f.a));
    assert_same(&rows_of(&f.map), &source_rows(&f.a));
    put(&f.a, "a4", 0);
    assert_eq!(sub.take_diffs(), ["insert a4"]);
}

#[test]
fn switching_sources_replaces_the_rows_and_stops_reading_the_old_source() {
    let _serial = serial();
    let f = fixture(Some('a'));
    put(&f.a, "a1", 0);
    put(&f.b, "b1", 0);
    put(&f.b, "b2", 0);
    let sub = Subscriber::new(&f.map);
    sub.take_diffs();
    f.selector.set(Some('b'));
    assert_eq!(sub.take_diffs(), ["initial 2"]);
    assert_same(&sub.rows(), &source_rows(&f.b));
    put(&f.a, "a2", 0);
    put(&f.b, "b3", 0);
    assert_eq!(sub.take_diffs(), ["insert b3"]);
    assert_same(&rows_of(&f.map), &source_rows(&f.b));
}

#[test]
fn reselecting_the_same_source_delivers_nothing() {
    let _serial = serial();
    let f = fixture(Some('a'));
    put(&f.a, "a1", 0);
    let sub = Subscriber::new(&f.map);
    sub.take_diffs();
    f.selector.set(Some('a'));
    assert!(sub.take_diffs().is_empty());
    put(&f.a, "a1", 1);
    assert_eq!(sub.take_diffs(), ["update a1"]);
}

#[test]
fn a_gate_change_and_a_source_write_in_one_batch_settle_exactly() {
    let _serial = serial();
    for (from, to) in [(Some('a'), None), (None, Some('a')), (Some('a'), Some('b'))] {
        let f = fixture(from);
        put(&f.a, "a1", 0);
        put(&f.b, "b1", 0);
        let sub = Subscriber::new(&f.map);
        crate::hyphae::batch(|| {
            put(&f.a, "a1", 1);
            put(&f.a, "a2", 0);
            f.selector.set(to);
            put(&f.b, "b2", 0);
            put(&f.a, "a3", 0);
        });
        let expected = match to {
            Some('a') => source_rows(&f.a),
            Some(_) => source_rows(&f.b),
            None => Rows::new(),
        };
        assert_same(&sub.rows(), &expected);
        assert_same(&rows_of(&f.map), &expected);
        assert_eq!(sub.log.lock().expect("log").violation, None);
    }
}

#[test]
fn dropping_the_subscription_stops_delivery() {
    let _serial = serial();
    let f = fixture(Some('a'));
    let sub = Subscriber::new(&f.map);
    let log = sub.log.clone();
    drop(sub);
    put(&f.a, "a1", 0);
    f.selector.set(Some('b'));
    put(&f.b, "b1", 0);
    assert_eq!(log.lock().expect("log").diffs, ["initial 0"]);
}

/// Gate flips race writes to both sources from other threads. Every
/// delivery must leave the subscriber holding rows of one source only, an
/// empty `Initial` (a close: both sources always hold a row) must be followed
/// by nothing but another `Initial`, and once the threads stop the subscriber
/// holds exactly what the final selection holds.
#[test]
fn concurrent_gate_flips_against_source_writes_settle_exactly() {
    let _serial = serial();
    const ROUNDS: usize = 5000;
    for round in 0..ROUNDS {
        let f = Arc::new(fixture(Some('a')));
        put(&f.a, "a-anchor", 0);
        put(&f.b, "b-anchor", 0);
        let sub = Subscriber::new(&f.map);
        let closed = Arc::new(AtomicBool::new(false));
        let after_close = closed.clone();
        let check = f.map.subscribe_diffs(move |diff| match diff {
            MapDiff::Initial { entries } => after_close.store(entries.is_empty(), Ordering::SeqCst),
            _ => assert!(
                !after_close.load(Ordering::SeqCst),
                "a diff was delivered while closed: {diff:?}"
            ),
        });
        let writer = |source: Source, prefix: char, seed: usize| {
            std::thread::spawn(move || {
                for i in 0..40 {
                    let id = format!("{prefix}{}", (i * 7 + seed) % 5);
                    match (i + seed) % 3 {
                        0 => {
                            source.remove(&Arc::from(id.as_str()));
                        }
                        1 if i % 4 == 0 => crate::hyphae::batch(|| {
                            put(&source, &id, i);
                            put(&source, &format!("{prefix}x"), i);
                        }),
                        _ => put(&source, &id, i),
                    }
                }
            })
        };
        let writers = [
            writer(f.a.clone(), 'a', round),
            writer(f.b.clone(), 'b', round + 1),
        ];
        let flipper = {
            let selector = f.selector.clone();
            std::thread::spawn(move || {
                for i in 0..30 {
                    selector.set(match (i + round) % 3 {
                        0 => None,
                        1 => Some('a'),
                        _ => Some('b'),
                    });
                }
            })
        };
        for handle in writers {
            handle.join().expect("writer");
        }
        flipper.join().expect("flipper");
        let expected = match f.selector.get() {
            Some('a') => source_rows(&f.a),
            Some(_) => source_rows(&f.b),
            None => Rows::new(),
        };
        let log = sub.log.lock().expect("log");
        assert_eq!(log.violation, None, "round {round}");
        assert!(log.initials >= 1);
        drop(log);
        assert_same(&sub.rows(), &expected);
        assert_same(&rows_of(&f.map), &expected);
        drop(check);
    }
}
