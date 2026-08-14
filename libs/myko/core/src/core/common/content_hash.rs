use std::sync::atomic::{AtomicU64, Ordering};

/// Lazy content hash for equality comparison.
///
/// Wraps an `AtomicU64` that caches the hash on first access.
/// `Clone` resets the cache to 0, forcing recomputation.
#[derive(Debug, Default)]
pub struct ContentHash(AtomicU64);

impl ContentHash {
    /// Return the cached hash, or compute and cache it.
    pub fn get_or_compute(&self, compute: impl FnOnce() -> u64) -> u64 {
        let cached = self.0.load(Ordering::Relaxed);
        if cached != 0 {
            return cached;
        }
        let computed = compute();
        let to_store = if computed == 0 { 1 } else { computed };
        self.0.store(to_store, Ordering::Relaxed);
        to_store
    }
}

impl Clone for ContentHash {
    fn clone(&self) -> Self {
        Self(AtomicU64::new(0))
    }
}

impl PartialEq for ContentHash {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
