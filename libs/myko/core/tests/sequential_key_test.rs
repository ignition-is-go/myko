//! `#[myko_sequential]`: the key each wire event carries, how a wire batch
//! splits on it, and what a store subscriber then sees.

use std::sync::{Arc, Mutex, PoisonError};

pub use myko::*;
use myko::{
    hyphae::{Materialize, SettleKey, Signal, Watchable},
    item::{SequentialSet, sequential_key},
    prelude::*,
    search::SearchIndex,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, RelationshipManager,
        persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

mod lane {
    use myko::prelude::*;
    #[myko_item]
    pub struct LaneReading {
        #[myko_sequential]
        pub lane: String,
        pub value: i64,
    }
}

mod other_lane {
    use myko::prelude::*;
    #[myko_item]
    pub struct OtherLaneReading {
        #[myko_sequential]
        pub lane: String,
    }
}

mod channel {
    use myko::prelude::*;
    #[myko_item]
    pub struct ChannelReading {
        #[myko_sequential]
        pub lane: String,
        #[myko_sequential]
        pub channel: Option<u32>,
    }
}

mod parity {
    use myko::prelude::*;

    fn parity(reading: &ParityReading) -> Option<bool> {
        (reading.value >= 0).then_some(reading.value % 2 == 0)
    }

    #[myko_item]
    #[myko_sequential(with = parity)]
    pub struct ParityReading {
        pub value: i64,
    }
}

mod plain {
    use myko::prelude::*;
    #[myko_item]
    pub struct PlainReading {
        pub value: i64,
    }
}

use channel::ChannelReading;
use lane::LaneReading;
use other_lane::OtherLaneReading;
use parity::ParityReading;
use plain::PlainReading;

fn set(item: &impl Eventable) -> MEvent {
    MEvent::from_item(item, MEventType::SET, "test")
}

fn lane(id: &str, lane: &str, value: i64) -> MEvent {
    set(&LaneReading {
        id: id.into(),
        lane: lane.into(),
        value,
    })
}

fn channel(lane: &str, channel: Option<u32>) -> MEvent {
    set(&ChannelReading {
        id: "reading".into(),
        lane: lane.into(),
        channel,
    })
}

fn parity_reading(value: i64) -> MEvent {
    set(&ParityReading {
        id: "reading".into(),
        value,
    })
}

fn plain(value: i64) -> MEvent {
    set(&PlainReading {
        id: "reading".into(),
        value,
    })
}

#[test]
fn a_marked_field_keys_the_event_by_its_value() {
    let registry = HandlerRegistry::new();
    let a = registry.settle_key(&lane("one", "a", 1));

    assert_eq!(a, sequential_key(Some("a".to_string())));
    assert_eq!(a, registry.settle_key(&lane("two", "a", 2)));
    assert_ne!(a, registry.settle_key(&lane("one", "b", 1)));
    let other_type = set(&OtherLaneReading {
        id: "one".into(),
        lane: "a".into(),
    });
    assert_eq!(a, registry.settle_key(&other_type));
}

#[test]
fn several_marked_fields_key_the_event_by_their_tuple() {
    let registry = HandlerRegistry::new();

    assert_eq!(
        registry.settle_key(&channel("a", Some(1))),
        sequential_key(Some(("a".to_string(), 1_u32)))
    );
    assert_ne!(
        registry.settle_key(&channel("a", Some(1))),
        registry.settle_key(&channel("a", Some(2)))
    );
}

#[test]
fn an_event_without_its_key_value_is_a_barrier() {
    let registry = HandlerRegistry::new();
    let mut unreadable = lane("one", "a", 1);
    unreadable.item = serde_json::Value::Null;

    assert_eq!(registry.settle_key(&channel("a", None)), SettleKey::Barrier);
    assert_eq!(registry.settle_key(&parity_reading(-1)), SettleKey::Barrier);
    assert_eq!(registry.settle_key(&unreadable), SettleKey::Barrier);
}

#[test]
fn a_struct_level_function_keys_the_event() {
    let registry = HandlerRegistry::new();

    assert_eq!(
        registry.settle_key(&parity_reading(4)),
        sequential_key(Some(true))
    );
    assert_eq!(
        registry.settle_key(&parity_reading(4)),
        registry.settle_key(&parity_reading(6))
    );
    assert_ne!(
        registry.settle_key(&parity_reading(4)),
        registry.settle_key(&parity_reading(5))
    );
}

#[test]
fn an_unmarked_item_is_shared() {
    assert_eq!(
        HandlerRegistry::new().settle_key(&plain(1)),
        SettleKey::Shared
    );
}

fn lanes(sets: &[SequentialSet]) -> Vec<(Vec<SettleKey>, Vec<i64>)> {
    sets.iter()
        .map(|set| {
            let values = set
                .events
                .iter()
                .filter_map(|event| event.item.get("value")?.as_i64())
                .collect();
            (set.keys.clone(), values)
        })
        .collect()
}

#[test]
fn a_batch_splits_where_a_key_repeats_and_around_a_barrier() {
    let registry = HandlerRegistry::new();
    let a = sequential_key(Some("a".to_string()));
    let b = sequential_key(Some("b".to_string()));

    let sets = registry.sequential_sets(vec![
        plain(1),
        lane("one", "a", 2),
        lane("two", "b", 3),
        lane("one", "a", 4),
        plain(5),
        lane("one", "a", 6),
        parity_reading(-7),
        plain(8),
    ]);

    assert_eq!(
        lanes(&sets),
        vec![
            (vec![a, b], vec![1, 2, 3]),
            (vec![a], vec![4, 5]),
            (vec![a], vec![6]),
            (vec![SettleKey::Barrier], vec![-7]),
            (vec![], vec![8]),
        ]
    );
}

#[test]
fn a_batch_without_sequential_items_stays_one_set() {
    let sets = HandlerRegistry::new().sequential_sets(vec![plain(1), plain(2), plain(3)]);

    assert_eq!(lanes(&sets), vec![(vec![], vec![1, 2, 3])]);
}

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
}

#[test]
fn a_subscriber_sees_every_event_of_one_key_from_a_single_batch() {
    let ctx = make_ctx();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let cell = ctx
        .registry
        .get_or_create("LaneReading")
        .get(&Arc::from("one"))
        .materialize();
    let _guard = cell.subscribe(move |signal| {
        if let Signal::Value(item) = signal
            && let Some(item) = item.as_ref()
            && let Some(reading) = item.as_any().downcast_ref::<LaneReading>()
        {
            sink.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(reading.value);
        }
    });

    let batch = vec![
        lane("one", "a", 1),
        lane("one", "a", 2),
        lane("one", "a", 3),
    ];
    // Queued before any of them settles, as a burst is when a settle is
    // already running.
    for set in ctx.handler_registry.sequential_sets(batch) {
        let applied = hyphae::enqueue(&set.keys, None, || ctx.apply_event_batch(set.events));
        assert_eq!(applied.ok(), Some(1));
    }
    hyphae::flush();

    assert_eq!(
        *seen.lock().unwrap_or_else(PoisonError::into_inner),
        vec![1, 2, 3]
    );
}
