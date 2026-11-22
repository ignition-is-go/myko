use std::sync::Arc;

use hyphae::{
    Cell, CellImmutable, CellMap, CellValue, Gettable, MapDiff, MapQuery, MapValuesExt, Signal,
    SubscriptionGuard, Watchable,
};
use parking_lot::Mutex;

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, Arc<T>, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

/// A diff of a type-erased view.
pub type ErasedViewDiff = MapDiff<Arc<str>, Arc<dyn AnyItem>>;

/// Materialize a view plan straight into its type-erased map.
///
/// The erasure is a `map_values` stage fused into the plan, so each changed
/// row costs one scheduled write to the output map. Materializing typed and
/// then forwarding into a second erased map cost two.
pub fn materialize_erased_view<T, Q>(plan: Q) -> FilteredViewCellMap
where
    T: AnyItem + PartialEq + Send + Sync + 'static,
    Q: MapQuery<Key = Arc<str>, Value = Arc<T>>,
{
    plan.map_values(|_, value| -> Arc<dyn AnyItem> { value.clone() })
        .materialize()
}

/// A view's output read as type-erased rows, without a second map.
///
/// Wraps the view's typed map and erases each value as a reader takes it, so
/// erasure costs no scheduled write. Subscribers see exactly the typed map's
/// diffs, in its order.
#[derive(Clone)]
pub struct ErasedViewMap {
    source: Arc<dyn ErasedSource>,
}

impl ErasedViewMap {
    /// Read a typed view map as erased rows.
    #[must_use]
    pub fn from_typed<T>(map: TypedViewCellMap<T>) -> Self
    where
        T: AnyItem + PartialEq + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(map),
        }
    }

    /// Subscribe to erased diffs, starting with `MapDiff::Initial`.
    ///
    /// The returned guard keeps the underlying map alive; dropping it cancels
    /// the subscription.
    pub fn subscribe_diffs<F>(&self, callback: F) -> SubscriptionGuard
    where
        F: Fn(&ErasedViewDiff) + Send + Sync + 'static,
    {
        self.source.subscribe_erased(Box::new(callback))
    }

    /// Point-in-time erased rows.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.source.snapshot_erased()
    }

    /// Read the map `selector` currently selects, and no rows while it
    /// selects none.
    ///
    /// Nothing is materialized per subscription: a subscriber reads the
    /// selected map's own diffs. Each time the selection changes the
    /// subscriber receives one `MapDiff::Initial` holding the full state of
    /// the new selection (empty when none), which replaces whatever it held;
    /// then that map's diffs in their order. A diff from a map that is no
    /// longer selected is never delivered. `open` is called once per
    /// selection change per subscription, so it should return an existing
    /// shared map rather than build one.
    ///
    /// The subscriber callback runs under this subscription's lock and must
    /// not subscribe to or change `selector` synchronously.
    #[must_use]
    pub fn switched<K, F>(selector: Cell<Option<K>, CellImmutable>, open: F) -> Self
    where
        K: CellValue,
        F: Fn(&K) -> Self + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(Switched {
                selector,
                open: Arc::new(open),
            }),
        }
    }
}

impl From<FilteredViewCellMap> for ErasedViewMap {
    fn from(map: FilteredViewCellMap) -> Self {
        Self {
            source: Arc::new(map),
        }
    }
}

type ErasedCallback = Box<dyn Fn(&ErasedViewDiff) + Send + Sync>;

trait ErasedSource: Send + Sync {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard;
    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)>;
}

impl<T> ErasedSource for TypedViewCellMap<T>
where
    T: AnyItem + PartialEq + Send + Sync + 'static,
{
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        self.subscribe_diffs(move |diff| callback(&erase_diff(diff)))
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.snapshot()
            .into_iter()
            .map(|(key, value)| (key, erase(&value)))
            .collect()
    }
}

impl ErasedSource for FilteredViewCellMap {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        self.subscribe_diffs(callback)
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.snapshot()
    }
}

type OpenSource<K> = Arc<dyn Fn(&K) -> ErasedViewMap + Send + Sync>;

struct Switched<K: CellValue> {
    selector: Cell<Option<K>, CellImmutable>,
    open: OpenSource<K>,
}

impl<K: CellValue> ErasedSource for Switched<K> {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        let subscription = Arc::new(SwitchedSubscription {
            selector: self.selector.clone(),
            open: self.open.clone(),
            callback,
            state: Mutex::new(SwitchState {
                epoch: 0,
                selected: None,
                followed: false,
                source: None,
            }),
        });
        let weak = Arc::downgrade(&subscription);
        let selector_guard = self.selector.subscribe(move |signal| {
            if let (Signal::Value(_), Some(subscription)) = (signal, weak.upgrade()) {
                subscription.follow_selector();
            }
        });
        let mut held = Some((selector_guard, subscription));
        SubscriptionGuard::from_callback(move || {
            if let Some((selector_guard, subscription)) = held.take() {
                drop(selector_guard);
                let source = subscription.state.lock().source.take();
                drop(source);
            }
        })
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.selector
            .get()
            .map_or_else(Vec::new, |key| (self.open)(&key).snapshot())
    }
}

struct SwitchedSubscription<K: CellValue> {
    selector: Cell<Option<K>, CellImmutable>,
    open: OpenSource<K>,
    callback: ErasedCallback,
    state: Mutex<SwitchState<K>>,
}

struct SwitchState<K> {
    /// Bumped on every selection change. Only diffs of the subscription
    /// opened at the current epoch reach the subscriber.
    epoch: u64,
    /// The selection the subscriber is on; meaningless until `followed`.
    selected: Option<K>,
    followed: bool,
    source: Option<SubscriptionGuard>,
}

impl<K: CellValue> SwitchedSubscription<K> {
    /// Move the subscriber to the selector's current value.
    ///
    /// Reads the selector rather than the signal, so however selector
    /// notifications interleave, the last one to run leaves the subscriber on
    /// the latest selection.
    fn follow_selector(self: &Arc<Self>) {
        let (epoch, selected, replaced) = {
            let mut state = self.state.lock();
            let selected = self.selector.get();
            if state.followed && state.selected == selected {
                return;
            }
            state.followed = true;
            state.epoch = state.epoch.wrapping_add(1);
            state.selected.clone_from(&selected);
            if selected.is_none() {
                (self.callback)(&MapDiff::Initial {
                    entries: Vec::new(),
                });
            }
            (state.epoch, selected, state.source.take())
        };
        drop(replaced);
        let Some(key) = selected else { return };
        // Subscribed outside the lock: the source delivers its Initial
        // synchronously into the callback below, which takes the lock.
        let weak = Arc::downgrade(self);
        let guard = (self.open)(&key).subscribe_diffs(move |diff| {
            let Some(subscription) = weak.upgrade() else {
                return;
            };
            let state = subscription.state.lock();
            if state.epoch == epoch {
                (subscription.callback)(diff);
            }
        });
        let mut state = self.state.lock();
        if state.epoch == epoch {
            state.source = Some(guard);
        } else {
            drop(state);
            drop(guard);
        }
    }
}

fn erase<T: AnyItem + 'static>(value: &Arc<T>) -> Arc<dyn AnyItem> {
    value.clone()
}

fn erase_diff<T: AnyItem + 'static>(diff: &MapDiff<Arc<str>, Arc<T>>) -> ErasedViewDiff {
    match diff {
        MapDiff::Initial { entries } => MapDiff::Initial {
            entries: entries.iter().map(|(k, v)| (k.clone(), erase(v))).collect(),
        },
        MapDiff::Insert { key, value } => MapDiff::Insert {
            key: key.clone(),
            value: erase(value),
        },
        MapDiff::Remove { key, old_value } => MapDiff::Remove {
            key: key.clone(),
            old_value: erase(old_value),
        },
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => MapDiff::Update {
            key: key.clone(),
            old_value: erase(old_value),
            new_value: erase(new_value),
        },
        MapDiff::Batch { changes } => MapDiff::Batch {
            changes: changes.iter().map(erase_diff).collect(),
        },
    }
}
