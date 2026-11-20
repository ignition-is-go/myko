//! Sequential keys: which wire events must settle one after another.

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
/// An event with a value is [`SettleKey::Shared`]: every ticket is delivered
/// on its own and a sequential type's store writes go through
/// the store's settled write queue, so every
/// event of one key reaches subscribers, in order, whatever it settles
/// beside. An event with no value gets [`SettleKey::Barrier`]: nothing says
/// which events it may settle beside, so it settles alone.
pub fn sequential_key<K>(value: Option<K>) -> SettleKey {
    value.map_or(SettleKey::Barrier, |_| SettleKey::Shared)
}

/// Events that settle together, and the keys their event set carries.
#[derive(Debug)]
pub struct SequentialSet {
    pub keys: Vec<SettleKey>,
    pub events: Vec<MEvent>,
}

impl SequentialSet {
    /// Splits `events` into the fewest sets that keep arrival order and put
    /// each barrier event in a set of its own.
    pub fn split(events: Vec<MEvent>, key_of: impl Fn(&MEvent) -> SettleKey) -> Vec<Self> {
        let mut sets: Vec<Self> = Vec::new();
        for event in events {
            let barrier = key_of(&event) == SettleKey::Barrier;
            let fits = !barrier
                && sets
                    .last()
                    .is_some_and(|set| !set.keys.contains(&SettleKey::Barrier));
            if !fits {
                sets.push(Self {
                    keys: if barrier { vec![SettleKey::Barrier] } else { Vec::new() },
                    events: Vec::new(),
                });
            }
            if let Some(set) = sets.last_mut() {
                set.events.push(event);
            }
        }
        sets
    }
}
