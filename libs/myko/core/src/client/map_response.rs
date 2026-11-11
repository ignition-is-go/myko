use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;

use crate::wire::item::WrappedItem;

/// Validates response sequences for a map subscription.
///
/// Sequence zero is an authoritative snapshot. It starts a socket connection
/// epoch, but may also reset an existing subscription when the server rebuilds
/// its query runtime and emits a new initial snapshot.
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
        if sequence == 0 {
            state.awaiting_snapshot = false;
            state.next = 1;
            return true;
        }
        if state.awaiting_snapshot {
            return false;
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
    fn authoritative_snapshot_resets_sequence_with_or_without_reconnect() {
        let sequences = MapSequence::new();
        assert!(sequences.accept(0));
        assert!(sequences.accept(1));
        assert!(!sequences.accept(1));

        // A live query runtime can emit a fresh authoritative snapshot without
        // reconnecting. It resets the expected incremental sequence.
        assert!(sequences.accept(0));
        assert!(!sequences.accept(2));
        assert!(sequences.accept(1));

        sequences.reset_epoch();
        assert!(!sequences.accept(2));
        assert!(sequences.accept(0));
        assert!(!sequences.accept(2));
        assert!(sequences.accept(1));
    }
}
