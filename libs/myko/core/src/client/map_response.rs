use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;

use crate::wire::item::WrappedItem;

/// Validates response sequences within one socket connection epoch.
/// A reconnect starts a new epoch that must begin with a sequence-zero snapshot.
pub(super) struct MapSequence {
    state: Mutex<SequenceState>,
}

struct SequenceState {
    awaiting_snapshot: bool,
    next: u64,
}

impl MapSequence {
    pub(super) const fn new() -> Self {
        Self {
            state: Mutex::new(SequenceState {
                awaiting_snapshot: true,
                next: 0,
            }),
        }
    }

    pub(super) fn reset_epoch(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .awaiting_snapshot = true;
    }

    pub(super) fn accept(&self, sequence: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.awaiting_snapshot {
            if sequence != 0 {
                return false;
            }
            state.awaiting_snapshot = false;
            state.next = 1;
            return true;
        }
        if sequence != state.next {
            return false;
        }
        let Some(next) = state.next.checked_add(1) else {
            return false;
        };
        state.next = next;
        true
    }
}

pub(super) type DecodedMapUpserts<T> = Vec<(Arc<str>, Arc<T>)>;

pub(super) fn decode_map_upserts<T, F>(
    upserts: Vec<WrappedItem>,
    id: F,
) -> Result<DecodedMapUpserts<T>, serde_json::Error>
where
    T: DeserializeOwned,
    F: Fn(&T) -> Arc<str>,
{
    upserts
        .into_iter()
        .map(|wrapped| {
            let item = Arc::new(serde_json::from_value::<T>(wrapped.item)?);
            let item_id = id(&item);
            Ok((item_id, item))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::MapSequence;

    #[test]
    fn reconnect_rejects_delayed_old_epoch_and_duplicate_sequences() {
        let sequences = MapSequence::new();
        assert!(sequences.accept(0));
        assert!(sequences.accept(1));
        assert!(!sequences.accept(1));
        sequences.reset_epoch();
        assert!(!sequences.accept(2));
        assert!(sequences.accept(0));
        assert!(!sequences.accept(0));
        assert!(!sequences.accept(2));
        assert!(sequences.accept(1));
    }
}
