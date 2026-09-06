//! Gap-free observation of local maps and retained native publications.
//!
//! Raw maps report local observations with no frontier. Retained publications
//! preserve their supplied history frontier and liveness without upgrading
//! either into replica coverage or custody evidence.

use std::{collections::BTreeMap, sync::Arc};

use hyphae::{Materialize as _, Signal, SubscriptionGuard, Watchable as _};
use myko_federation::{
    LivePublicationStream, LiveSubscription, LiveSubscriptionState, LiveSubscriptionWriter,
    SubscriptionLiveness, live_subscription,
};
use parking_lot::Mutex;

use crate::{item::AnyItem, query::FilteredCellMap};

pub type NativeMap = BTreeMap<Arc<str>, Arc<dyn AnyItem>>;
pub type MapSnapshot = LiveSubscriptionState<NativeMap>;
type NativeMapPair = (
    LiveSubscriptionWriter<NativeMap>,
    LiveSubscription<NativeMap>,
);

/// Retains a native map and publishes coherent full snapshots for its lifetime.
pub struct NativeMapOutput {
    live: LiveSubscription<NativeMap>,
    source: NativeMapSource,
}

enum NativeMapSource {
    Local { _guard: SubscriptionGuard },
    Retained,
}

impl NativeMapOutput {
    /// Installs the wakeup subscriber before accepting its initial snapshot.
    pub fn new(map: FilteredCellMap) -> Result<Arc<Self>, String> {
        let initialized = Arc::new(Mutex::new(None::<NativeMapPair>));
        let terminal = Arc::new(Mutex::new(None::<String>));
        let callback_slot = Arc::clone(&initialized);
        let callback_terminal = Arc::clone(&terminal);
        let diffs = map.diffs().materialize();
        let callback_map = map;
        let raw_guard = diffs.subscribe(move |signal| match signal {
            Signal::Value(_) => {
                let mut slot = callback_slot.lock();
                if slot.is_none() {
                    let (writer, live) = live_subscription(snapshot_state(&callback_map));
                    *slot = Some((writer, live));
                    return;
                }
                let writer = slot.as_ref().map(|(writer, _)| writer.clone());
                drop(slot);
                if let Some(writer) = writer {
                    writer.replace_with(|| snapshot_state(&callback_map));
                }
            }
            Signal::Complete => terminate(&callback_slot, &callback_terminal, "map completed"),
            Signal::Error(error) => terminate(
                &callback_slot,
                &callback_terminal,
                format!("map failed: {error}"),
            ),
        });

        let live = initialized
            .lock()
            .as_ref()
            .map(|(_, live)| live.clone())
            .ok_or_else(|| {
                terminal
                    .lock()
                    .clone()
                    .unwrap_or_else(|| "map closed before its initial value".to_owned())
            })?;
        Ok(Arc::new(Self {
            live,
            source: NativeMapSource::Local { _guard: raw_guard },
        }))
    }

    /// Retain an existing authoritative publication without rebuilding it.
    #[must_use]
    pub fn from_retained(live: LiveSubscription<NativeMap>) -> Arc<Self> {
        Arc::new(Self {
            live,
            source: NativeMapSource::Retained,
        })
    }

    #[must_use]
    pub const fn is_retained(&self) -> bool {
        matches!(&self.source, NativeMapSource::Retained)
    }

    pub fn watch(&self) -> LivePublicationStream<MapSnapshot> {
        self.live.watch_publications()
    }
}

fn snapshot_state(map: &FilteredCellMap) -> MapSnapshot {
    LiveSubscriptionState {
        value: Some(map.snapshot().into_iter().collect()),
        through: None,
        liveness: SubscriptionLiveness::Current,
    }
}

fn terminate(
    slot: &Mutex<Option<NativeMapPair>>,
    terminal: &Mutex<Option<String>>,
    reason: impl Into<String>,
) {
    let reason = reason.into();
    let writer = slot.lock().as_ref().map(|(writer, _)| writer.clone());
    if let Some(writer) = writer {
        writer.invalidate(reason);
    } else {
        *terminal.lock() = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, time::Duration};

    use hyphae::CellMap;
    use myko_federation::LivePublicationStream;

    use super::*;
    use crate::common::with_id::WithId;

    #[derive(Clone, Debug, PartialEq, serde::Serialize)]
    struct TestItem {
        id: Arc<str>,
    }

    impl WithId for TestItem {
        fn id(&self) -> Arc<str> {
            Arc::clone(&self.id)
        }
    }

    impl AnyItem for TestItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "NativeMapTestItem"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other.as_any().downcast_ref::<Self>() == Some(self)
        }
    }

    fn item(id: &str) -> Arc<dyn AnyItem> {
        Arc::new(TestItem { id: id.into() })
    }

    fn receive_state(
        stream: &mut LivePublicationStream<MapSnapshot>,
    ) -> Result<MapSnapshot, String> {
        let started = std::time::Instant::now();
        loop {
            match stream.try_recv() {
                Ok(publication) => return Ok(publication.state),
                Err(flume::TryRecvError::Empty) if started.elapsed() < Duration::from_secs(1) => {
                    std::thread::yield_now();
                }
                Err(error) => return Err(format!("publication was not delivered: {error}")),
            }
        }
    }

    #[test]
    fn publishes_real_seed_then_full_snapshot_after_delete() {
        let map = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
        map.insert("a".into(), item("a"));
        map.insert("b".into(), item("b"));
        let output = NativeMapOutput::new(map.clone().lock());
        assert!(output.is_ok());
        let Ok(output) = output else {
            return;
        };
        let mut stream = output.watch();

        let initial = receive_state(&mut stream);
        assert!(initial.is_ok());
        let Ok(initial) = initial else {
            return;
        };
        assert!(initial.value.is_some());
        let Some(initial) = initial.value else {
            return;
        };
        assert_eq!(
            initial.keys().map(AsRef::as_ref).collect::<Vec<_>>(),
            ["a", "b"]
        );

        map.remove(&Arc::<str>::from("a"));
        let changed = receive_state(&mut stream);
        assert!(changed.is_ok());
        let Ok(changed) = changed else {
            return;
        };
        assert!(changed.value.is_some());
        let Some(changed) = changed.value else {
            return;
        };
        assert_eq!(changed.keys().map(AsRef::as_ref).collect::<Vec<_>>(), ["b"]);
    }

    #[test]
    fn terminal_signal_invalidates_and_retains_the_snapshot() {
        let initial = LiveSubscriptionState {
            value: Some(NativeMap::from([("a".into(), item("a"))])),
            through: None,
            liveness: SubscriptionLiveness::Current,
        };
        let (writer, live) = live_subscription(initial.clone());
        let mut stream = live.watch_publications();
        let seeded = receive_state(&mut stream);
        assert_eq!(seeded, Ok(initial.clone()));
        let slot = Mutex::new(Some((writer, live.clone())));
        let terminal = Mutex::new(None);

        terminate(&slot, &terminal, "raw map ended");

        let invalid = receive_state(&mut stream);
        assert!(invalid.is_ok());
        let Ok(invalid) = invalid else {
            return;
        };
        assert_eq!(invalid.value, initial.value);
        assert_eq!(
            invalid.liveness,
            SubscriptionLiveness::Invalid {
                reason: "raw map ended".to_owned(),
            }
        );
    }

    #[test]
    fn last_output_owner_releases_the_retained_map() {
        let map = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
        let weak = map.downgrade();
        let output = NativeMapOutput::new(map.clone().lock());
        assert!(output.is_ok());
        let Ok(output) = output else {
            return;
        };
        drop(map);
        assert!(weak.upgrade().is_some());

        drop(output);
        assert!(weak.upgrade().is_none());
    }
}
