//! One view read across several partitions, merged for a reader that is in
//! none of them.
//!
//! Each part runs in its own partition. Its diffs are only queued from that
//! partition's propagation; the merge runs on the shared builder pool,
//! outside every domain's propagation, and builds no cell. So a part never
//! waits on another part's merge, and nothing in any domain's graph can read
//! the merged rows.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use hyphae::{MapDiff, SubscriptionGuard};
use parking_lot::Mutex;

use super::cell::{ErasedCallback, ErasedSource, ErasedViewDiff, ErasedViewMap};
use crate::core::item::AnyItem;

type Row = Arc<dyn AnyItem>;

pub(super) struct Merged {
    pub(super) parts: Vec<ErasedViewMap>,
}

impl ErasedSource for Merged {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        let (queue, received) = flume::unbounded::<(usize, ErasedViewDiff)>();
        let merge = Arc::new(Merge {
            received,
            scheduled: AtomicBool::new(false),
            state: Mutex::new(MergeState {
                callback,
                rows: HashMap::new(),
                waiting: self.parts.len(),
                started: vec![false; self.parts.len()],
                closed: false,
            }),
        });
        let mut guards: Vec<SubscriptionGuard> = Vec::with_capacity(self.parts.len());
        for (index, part) in self.parts.iter().enumerate() {
            let queue = queue.clone();
            let merge = Arc::downgrade(&merge);
            guards.push(part.subscribe_diffs(move |diff| {
                if queue.send((index, diff.clone())).is_ok()
                    && let Some(merge) = merge.upgrade()
                {
                    merge.schedule();
                }
            }));
        }
        if self.parts.is_empty() {
            merge.state.lock().start();
        }
        let mut held = Some((guards, merge));
        SubscriptionGuard::from_callback(move || {
            if let Some((guards, merge)) = held.take() {
                drop(guards);
                merge.state.lock().closed = true;
            }
        })
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Row)> {
        let mut rows: HashMap<Arc<str>, BTreeMap<usize, Row>> = HashMap::new();
        for (index, part) in self.parts.iter().enumerate() {
            for (key, row) in part.snapshot() {
                rows.entry(key).or_default().insert(index, row);
            }
        }
        rows.into_iter()
            .filter_map(|(key, held)| Some((key, visible(&held)?.clone())))
            .collect()
    }
}

struct Merge {
    received: flume::Receiver<(usize, ErasedViewDiff)>,
    scheduled: AtomicBool,
    state: Mutex<MergeState>,
}

impl Merge {
    fn schedule(self: Arc<Self>) {
        if !self.scheduled.swap(true, Ordering::AcqRel) {
            crate::core::off_wave::off_wave(move || self.drain());
        }
    }

    fn drain(&self) {
        self.scheduled.store(false, Ordering::Release);
        let mut state = self.state.lock();
        while let Ok((part, diff)) = self.received.try_recv() {
            state.apply(part, diff);
        }
        drop(state);
    }
}

struct MergeState {
    callback: ErasedCallback,
    /// Each key's row in every part that holds it, by part.
    rows: HashMap<Arc<str>, BTreeMap<usize, Row>>,
    /// Parts whose first snapshot has not arrived yet.
    waiting: usize,
    started: Vec<bool>,
    closed: bool,
}

/// The row a reader sees for a key: the first part's that holds it, so two
/// parts disagreeing on a key resolve the same way every time.
fn visible(held: &BTreeMap<usize, Row>) -> Option<&Row> {
    held.values().next()
}

impl MergeState {
    fn apply(&mut self, part: usize, diff: ErasedViewDiff) {
        let mut out = Vec::new();
        self.fold(part, diff, &mut out);
        if self.closed || self.waiting > 0 {
            return;
        }
        match out.len() {
            0 => {}
            1 => (self.callback)(&out.remove(0)),
            _ => (self.callback)(&MapDiff::Batch { changes: out }),
        }
    }

    fn start(&self) {
        let entries = self
            .rows
            .iter()
            .filter_map(|(key, held)| Some((key.clone(), visible(held)?.clone())))
            .collect();
        if !self.closed {
            (self.callback)(&MapDiff::Initial { entries });
        }
    }

    fn fold(&mut self, part: usize, diff: ErasedViewDiff, out: &mut Vec<ErasedViewDiff>) {
        match diff {
            MapDiff::Initial { entries } => {
                let held: Vec<Arc<str>> = self
                    .rows
                    .iter()
                    .filter(|(_, held)| held.contains_key(&part))
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in held {
                    self.set(part, key, None, out);
                }
                for (key, row) in entries {
                    self.set(part, key, Some(row), out);
                }
                if let Some(started) = self.started.get_mut(part)
                    && !*started
                {
                    *started = true;
                    self.waiting = self.waiting.saturating_sub(1);
                    if self.waiting == 0 {
                        out.clear();
                        self.start();
                    }
                }
            }
            MapDiff::Insert { key, value: row }
            | MapDiff::Update {
                key,
                new_value: row,
                ..
            } => self.set(part, key, Some(row), out),
            MapDiff::Remove { key, .. } => self.set(part, key, None, out),
            MapDiff::Batch { changes } => {
                for change in changes {
                    self.fold(part, change, out);
                }
            }
        }
    }

    /// Set `part`'s row for `key`, recording what a reader of the merge sees
    /// change.
    fn set(&mut self, part: usize, key: Arc<str>, row: Option<Row>, out: &mut Vec<ErasedViewDiff>) {
        let held = self.rows.entry(key.clone()).or_default();
        let before = visible(held).cloned();
        match row {
            Some(row) => drop(held.insert(part, row)),
            None => drop(held.remove(&part)),
        }
        let after = visible(held).cloned();
        if held.is_empty() {
            self.rows.remove(&key);
        }
        match (before, after) {
            (None, Some(value)) => out.push(MapDiff::Insert { key, value }),
            (Some(old_value), None) => out.push(MapDiff::Remove { key, old_value }),
            (Some(old_value), Some(new_value)) if !Arc::ptr_eq(&old_value, &new_value) => {
                out.push(MapDiff::Update {
                    key,
                    old_value,
                    new_value,
                });
            }
            _ => {}
        }
    }
}
