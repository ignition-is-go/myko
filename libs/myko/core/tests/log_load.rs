//! Loading a persisted log through the one entry point a backend uses.
//!
//! The context places every row: straight into root's store when no
//! partition schema has a say in its type, in its partition otherwise, and
//! held when no open partition takes it yet. A server without partitions is
//! the case with none.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

pub use myko::*;
use myko::{
    prelude::Eventable,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey, PartitionSchema,
        RelationshipManager, persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use parking_lot::Mutex;
use uuid::Uuid;

/// Root's tick is shared by the process, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

pub mod space {
    use myko::prelude::*;

    /// Shared. Setting a space opens the partition named by its id.
    #[myko_item]
    pub struct LlSpace {}
}

pub mod row {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by the space it names.
    #[myko_item]
    pub struct LlRow {
        #[myko_partition_key]
        pub space: Arc<str>,
        pub value: i64,
    }
}

pub mod note {
    use myko::prelude::*;

    /// Shared.
    #[myko_item]
    pub struct LlNote {
        pub value: i64,
    }
}

use note::LlNote;
use row::LlRow;
use space::LlSpace;

fn context(host: Uuid) -> MykoServerContext {
    MykoServerContext::new(
        host,
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
}

fn partitioned(host: Uuid) -> MykoServerContext {
    context(host).with_partitions(
        PartitionSchema::new()
            .lifecycle::<LlSpace>()
            .partition_by::<LlRow>(|row| PartitionKey::new(row.space.clone())),
    )
}

fn event(item: &impl Eventable) -> MEvent {
    MEvent::from_item(item, MEventType::SET, "log")
}

fn note(index: usize) -> LlNote {
    LlNote {
        id: format!("note-{index}").into(),
        value: i64::try_from(index).unwrap_or_default(),
    }
}

fn row(space: &str, id: &str) -> LlRow {
    LlRow {
        id: id.into(),
        space: space.into(),
        value: 1,
    }
}

fn store_len(ctx: &MykoServerContext, entity_type: &str) -> usize {
    ctx.registry.get_or_create(entity_type).snapshot().len()
}

#[test]
fn with_no_partitions_every_row_lands_in_root_across_batches() {
    let _serial = SERIAL.lock();
    let host = Uuid::new_v4();
    let ctx = context(host);
    let mut load = ctx.load_log();
    let rows = 512 * 2 + 3;
    for index in 0..rows {
        let logged = event(&note(index));
        load.push(&logged);
        let mut own = logged;
        own.source_id = Some(host.to_string().into());
        load.push(&own);
    }
    let summary = load.finish().expect("load");
    assert_eq!(
        summary.loaded, rows,
        "every row is loaded, the tail batch too"
    );
    assert_eq!(summary.own, rows, "this host's own rows are skipped");
    assert!(summary.held.is_empty() && summary.unreadable.is_empty());
    assert_eq!(store_len(&ctx, LlNote::ENTITY_NAME_STATIC), rows);
}

#[test]
fn with_partitions_rows_land_in_their_partitions_and_shared_rows_in_root() {
    let _serial = SERIAL.lock();
    let ctx = partitioned(Uuid::new_v4());
    let mut load = ctx.load_log();
    for logged in [
        event(&row("a", "r-a")),
        event(&note(1)),
        event(&LlSpace { id: "a".into() }),
        event(&LlSpace { id: "b".into() }),
        event(&row("b", "r-b")),
    ] {
        load.push(&logged);
    }
    let summary = load.finish().expect("load");
    assert_eq!(summary.loaded, 5);
    assert!(summary.held.is_empty(), "{summary}");
    let a = ctx.partition(&PartitionKey::new("a")).expect("a opened");
    let b = ctx.partition(&PartitionKey::new("b")).expect("b opened");
    a.domain().flush();
    b.domain().flush();
    assert_eq!(store_len(&a, LlRow::ENTITY_NAME_STATIC), 1);
    assert_eq!(store_len(&b, LlRow::ENTITY_NAME_STATIC), 1);
    assert_eq!(store_len(&ctx, LlNote::ENTITY_NAME_STATIC), 1);
}

#[test]
fn a_row_no_partition_takes_is_held_reported_and_lands_when_one_opens() {
    let _serial = SERIAL.lock();
    let ctx = partitioned(Uuid::new_v4());
    let mut load = ctx.load_log();
    load.push(&event(&row("later", "waiting")));
    let summary = load.finish().expect("load");
    assert_eq!(summary.loaded, 0);
    assert_eq!(
        summary.held.get(LlRow::ENTITY_NAME_STATIC).copied(),
        Some(1),
        "{summary}"
    );
    assert!(summary.to_string().contains("held"), "{summary}");

    ctx.set(&LlSpace { id: "later".into() })
        .expect("open later");
    let later = ctx
        .partition(&PartitionKey::new("later"))
        .expect("later opened");
    later.domain().flush();
    assert_eq!(
        store_len(&later, LlRow::ENTITY_NAME_STATIC),
        1,
        "the held row lands in the partition once it opens"
    );
}

#[test]
fn a_row_that_does_not_parse_is_counted_once_and_left_out() {
    let _serial = SERIAL.lock();
    for ctx in [context(Uuid::new_v4()), partitioned(Uuid::new_v4())] {
        let mut load = ctx.load_log();
        let mut stale = event(&row("a", "old"));
        if let Some(fields) = stale.item.as_object_mut() {
            fields.remove("space");
        }
        load.push(&stale);
        load.push(&event(&note(1)));
        let summary = load.finish().expect("load");
        assert_eq!(
            summary.unreadable.get(LlRow::ENTITY_NAME_STATIC).copied(),
            Some(1),
            "{summary}"
        );
        assert_eq!(summary.loaded, 1);
        assert!(summary.to_string().contains("unreadable"), "{summary}");
    }
}

#[test]
fn a_tailed_event_from_another_host_applies_and_this_hosts_own_is_skipped() {
    let _serial = SERIAL.lock();
    let host = Uuid::new_v4();
    for ctx in [context(host), partitioned(host)] {
        ctx.apply_logged(&event(&note(7)));
        let mut own = event(&note(8));
        own.source_id = Some(host.to_string().into());
        ctx.apply_logged(&own);
        let notes = ctx.registry.get_or_create(LlNote::ENTITY_NAME_STATIC);
        assert!(notes.get_value(&Arc::from("note-7")).is_some());
        assert!(
            notes.get_value(&Arc::from("note-8")).is_none(),
            "this host's own event is skipped"
        );
        ctx.apply_logged(&MEvent::from_item(&note(7), MEventType::DEL, "log"));
        assert!(notes.get_value(&Arc::from("note-7")).is_none());
    }
}
