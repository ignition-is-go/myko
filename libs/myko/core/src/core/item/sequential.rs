//! Sequential keys: which wire events must settle one after another.

use std::hash::{DefaultHasher, Hash, Hasher};

use hyphae::SettleKey;
use serde_json::Value;

use crate::wire::MEvent;

/// Reads an item's sequential key from its wire form.
pub type SequentialKeyFn = fn(&Value) -> SettleKey;

/// Registration entry for an entity type declared with `#[myko_sequential]`.
pub struct SequentialRegistration {
    pub entity_type: &'static str,
    pub key: SequentialKeyFn,
}

inventory::collect!(SequentialRegistration);

/// The settle key for one event's marked value.
///
/// Two events with equal values get the same exclusive key, whatever their
/// entity types. An event with no value gets [`SettleKey::Barrier`]: nothing
/// says which events it may settle beside, so it settles alone.
pub fn sequential_key<K: Hash>(value: Option<K>) -> SettleKey {
    value.map_or(SettleKey::Barrier, |value| {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        SettleKey::Exclusive(hasher.finish())
    })
}

/// Events that settle together, and the keys their event set carries.
#[derive(Debug)]
pub struct SequentialSet {
    pub keys: Vec<SettleKey>,
    pub events: Vec<MEvent>,
}

impl SequentialSet {
    /// Splits `events` into the fewest sets that keep arrival order and never
    /// put two events with one exclusive key, or anything beside a barrier
    /// event, in the same set.
    ///
    /// A set's keys are the distinct keys of its events, so two sets with no
    /// key in common are free to settle at the same time.
    pub fn split(events: Vec<MEvent>, key_of: impl Fn(&MEvent) -> SettleKey) -> Vec<Self> {
        let mut sets: Vec<Self> = Vec::new();
        for event in events {
            let key = key_of(&event);
            let fits = sets.last().is_some_and(|set| {
                key != SettleKey::Barrier
                    && !set.keys.contains(&key)
                    && !set.keys.contains(&SettleKey::Barrier)
            });
            if !fits {
                sets.push(Self {
                    keys: Vec::new(),
                    events: Vec::new(),
                });
            }
            if let Some(set) = sets.last_mut() {
                if key != SettleKey::Shared {
                    set.keys.push(key);
                }
                set.events.push(event);
            }
        }
        sets
    }
}
