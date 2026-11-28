//! Encode-once JSON for immutable shared values.
//!
//! Rows and report outputs are immutable once wrapped in an `Arc`, and the same
//! allocation is commonly sent to many subscribers and in many frames. An
//! [`EncodedJsonCache`] keeps each allocation's compact JSON beside a `Weak`
//! to it, so every later frame that carries the same `Arc` splices the stored
//! bytes instead of serializing again.
//!
//! Lifetime: the cache never keeps a value alive. A `Weak` pins only the
//! allocation's address, which is what makes the pointer a sound key: no other
//! value can be allocated there while the entry exists. Entries whose value has
//! been dropped are removed by a sweep that runs whenever a shard has doubled
//! since its last sweep, so dead entries never outnumber live ones.

use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::{Arc, Mutex, PoisonError, Weak},
};

use serde_json::value::RawValue;

const SHARDS: usize = 64;
const MIN_SWEEP_LEN: usize = 256;

struct Entry<T: ?Sized> {
    owner: Weak<T>,
    json: Arc<RawValue>,
}

/// Keys are allocation addresses, already unique; one multiply spreads them.
#[derive(Default)]
struct AddressHasher(u64);

impl Hasher for AddressHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(*byte)).wrapping_mul(SPREAD);
        }
    }

    fn write_usize(&mut self, value: usize) {
        self.0 = u64::try_from(value)
            .unwrap_or_default()
            .wrapping_mul(SPREAD);
    }
}

const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

struct Shard<T: ?Sized> {
    entries: HashMap<usize, Entry<T>, BuildHasherDefault<AddressHasher>>,
    sweep_at: usize,
}

impl<T: ?Sized> Default for Shard<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::default(),
            sweep_at: MIN_SWEEP_LEN,
        }
    }
}

/// Compact JSON per shared allocation, released with the allocation.
pub struct EncodedJsonCache<T: ?Sized> {
    shards: [Mutex<Shard<T>>; SHARDS],
}

impl<T: ?Sized> Default for EncodedJsonCache<T> {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(Shard::default())),
        }
    }
}

/// The data address of an `Arc`, ignoring any vtable half of a fat pointer.
fn address<T: ?Sized>(value: &Arc<T>) -> usize {
    Arc::as_ptr(value).cast::<()>().addr()
}

impl<T: ?Sized> EncodedJsonCache<T> {
    fn shard(&self, key: usize) -> &Mutex<Shard<T>> {
        // Allocations are at least 8-byte aligned; the bits above that pick
        // the shard, and the map hashes the whole address.
        let [first, ..] = &self.shards;
        self.shards.get((key >> 4) % SHARDS).unwrap_or(first)
    }

    /// The JSON of `value`, encoded by `encode` the first time this allocation
    /// is asked for and shared afterwards. `None` when `encode` fails; failures
    /// are not remembered.
    pub fn get_or_encode(
        &self,
        value: &Arc<T>,
        encode: impl FnOnce(&T) -> Option<Box<RawValue>>,
    ) -> Option<Arc<RawValue>> {
        let key = address(value);
        let shard = self.shard(key);
        // A hit is always this value: the entry's `Weak` keeps the address
        // from being reused, and the caller holds a strong reference to it.
        if let Some(entry) = shard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .get(&key)
        {
            return Some(entry.json.clone());
        }
        let json: Arc<RawValue> = Arc::from(encode(value)?);
        let mut shard = shard.lock().unwrap_or_else(PoisonError::into_inner);
        let json = shard
            .entries
            .entry(key)
            .or_insert_with(|| Entry {
                owner: Arc::downgrade(value),
                json,
            })
            .json
            .clone();
        if shard.entries.len() >= shard.sweep_at {
            shard
                .entries
                .retain(|_, entry| entry.owner.strong_count() > 0);
            shard.sweep_at = shard.entries.len().saturating_mul(2).max(MIN_SWEEP_LEN);
        }
        drop(shard);
        Some(json)
    }

    /// Entries currently held, live or awaiting a sweep.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entries
                    .len()
            })
            .sum()
    }

    /// True when no entry is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn encodes_each_allocation_once_and_distinguishes_equal_values() {
        let cache = EncodedJsonCache::<String>::default();
        let encodes = AtomicUsize::new(0);
        let encode = |value: &String| {
            encodes.fetch_add(1, Ordering::Relaxed);
            serde_json::value::to_raw_value(value).ok()
        };
        let first = Arc::new("a".to_owned());
        let twin = Arc::new("a".to_owned());
        for _ in 0..3 {
            assert_eq!(cache.get_or_encode(&first, encode).unwrap().get(), "\"a\"");
        }
        assert_eq!(encodes.load(Ordering::Relaxed), 1);
        cache.get_or_encode(&twin, encode).unwrap();
        assert_eq!(encodes.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn dropped_values_are_swept_and_never_answer_for_a_new_value() {
        let cache = EncodedJsonCache::<String>::default();
        let encode = |value: &String| serde_json::value::to_raw_value(value).ok();
        for round in 0..(MIN_SWEEP_LEN * SHARDS * 4) {
            let value = Arc::new(format!("value-{round}"));
            let json = cache.get_or_encode(&value, encode).unwrap();
            assert_eq!(json.get(), format!("\"value-{round}\""));
        }
        // Every value above was dropped right after use; only entries added
        // since each shard's last sweep can remain.
        assert!(cache.len() <= MIN_SWEEP_LEN * SHARDS);
    }
}
