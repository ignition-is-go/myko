//! `PostgreSQL` implementation of the backend-neutral history subscription.

use std::{collections::BTreeSet, time::Duration};

use myko::server::{
    HistoryEvent, HistoryFeedRow, HistoryFilter, HistoryResetReason, HistorySubscription,
    HistoryUpdate,
};
use postgres::{Client, fallible_iterator::FallibleIterator};

use super::{PostgresConfig, connect_pg_client, qi};

const PAGE_SIZE: i64 = 1000;
const RECENT_ROWS: usize = 10_000;

pub(super) struct PostgresHistorySubscription {
    config: PostgresConfig,
    types: Vec<String>,
    key_types: Vec<String>,
    key_ids: Vec<String>,
    connections: Option<(Client, Client)>,
    started: bool,
    replaying: bool,
    announce_caught_up: bool,
    cursor: i64,
    recent: BTreeSet<i64>,
    pending: BTreeSet<i64>,
}

impl PostgresHistorySubscription {
    pub(super) fn new(config: PostgresConfig, filter: HistoryFilter) -> Self {
        let (key_types, key_ids) = filter
            .entities
            .into_iter()
            .map(|key| (key.item_type.to_string(), key.item_id.to_string()))
            .unzip();
        Self {
            config,
            types: filter
                .item_types
                .into_iter()
                .map(|item_type| item_type.to_string())
                .collect(),
            key_types,
            key_ids,
            connections: None,
            started: false,
            replaying: true,
            announce_caught_up: true,
            cursor: 0,
            recent: BTreeSet::new(),
            pending: BTreeSet::new(),
        }
    }

    fn reset(&mut self, reason: HistoryResetReason) -> HistoryUpdate {
        self.cursor = 0;
        self.recent.clear();
        self.pending.clear();
        self.replaying = true;
        self.announce_caught_up = true;
        HistoryUpdate::Reset(reason)
    }

    fn remember(&mut self, rows: &[HistoryFeedRow]) {
        for row in rows {
            self.cursor = self.cursor.max(row.id);
            self.recent.insert(row.id);
            if self.recent.len() > RECENT_ROWS {
                self.recent.pop_first();
            }
        }
    }

    fn connect(&mut self) -> Result<HistoryUpdate, String> {
        // NOTE(ts): Register LISTEN before replay so commits behind the replay
        // position can still be recovered by exact ID, including late commits.
        let mut listener = connect_pg_client(&self.config, "history-feed-listener")?;
        listener
            .batch_execute(&format!(
                "SET application_name = 'myko-history-feed-listener'; LISTEN {}",
                qi(&self.config.channel),
            ))
            .map_err(|error| error.to_string())?;
        let mut reader = connect_pg_client(&self.config, "history-feed-reader")?;
        reader
            .batch_execute(
                "SET application_name = 'myko-history-feed-reader'; SET statement_timeout = '10s'",
            )
            .map_err(|error| error.to_string())?;
        self.connections = Some((reader, listener));
        let reason = if self.started {
            HistoryResetReason::Reconnected
        } else {
            HistoryResetReason::Initial
        };
        self.started = true;
        Ok(self.reset(reason))
    }

    fn read_rows(&mut self, ids: Option<&Vec<i64>>) -> Result<Vec<HistoryFeedRow>, String> {
        let table = qi(&self.config.table);
        let selection = "((cardinality($1::text[]) = 0 AND cardinality($2::text[]) = 0) \
            OR item_type = ANY($1) \
            OR (item_type, item_id) IN (SELECT * FROM unnest($2::text[], $3::text[])))";
        let Some((reader, _)) = self.connections.as_mut() else {
            return Err("History subscription is disconnected".to_string());
        };
        let rows = if let Some(ids) = ids {
            reader.query(
                &format!(
                    "SELECT id, created_at, event::text FROM {table} \
                 WHERE {selection} AND id = ANY($4) ORDER BY id ASC"
                ),
                &[&self.types, &self.key_types, &self.key_ids, ids],
            )
        } else {
            reader.query(
                &format!(
                    "SELECT id, created_at, event::text FROM {table} \
                 WHERE {selection} AND id > $4 ORDER BY id ASC LIMIT $5"
                ),
                &[
                    &self.types,
                    &self.key_types,
                    &self.key_ids,
                    &self.cursor,
                    &PAGE_SIZE,
                ],
            )
        }
        .map_err(|error| format!("History feed read failed: {error}"))?;
        rows.into_iter()
            .map(|row| {
                let id = row.try_get(0).map_err(|error| error.to_string())?;
                let event = (|| {
                    let created_at = row.try_get(1).map_err(|error| error.to_string())?;
                    let json: String = row.try_get(2).map_err(|error| error.to_string())?;
                    let event = myko::wire::MEvent::from_str_trim(&json)
                        .map_err(|error| error.to_string())?;
                    Ok(HistoryEvent {
                        id,
                        created_at,
                        event,
                    })
                })();
                Ok(HistoryFeedRow { id, event })
            })
            .collect()
    }

    fn notifications(&mut self, timeout: Duration) -> Result<bool, String> {
        let Some((_, listener)) = self.connections.as_mut() else {
            return Err("History subscription is disconnected".to_string());
        };
        let mut notifications = listener.notifications();
        for index in 0..PAGE_SIZE {
            // Wait once, then drain only notifications that are already available.
            let wait = if index == 0 { timeout } else { Duration::ZERO };
            let Some(notification) = notifications
                .timeout_iter(wait)
                .next()
                .map_err(|error| error.to_string())?
            else {
                return Ok(true);
            };
            if let Ok(id) = notification.payload().parse::<i64>() {
                self.pending.insert(id);
            }
        }
        Ok(false)
    }

    fn poll_connected(&mut self, timeout: Duration) -> Result<HistoryUpdate, String> {
        if self.connections.is_none() {
            return self.connect();
        }
        if self.replaying {
            let rows = self.read_rows(None)?;
            self.replaying = rows.len() == usize::try_from(PAGE_SIZE).unwrap_or_default();
            self.remember(&rows);
            if !rows.is_empty() {
                return Ok(HistoryUpdate::Rows(rows));
            }
        }
        let drained =
            self.notifications(if self.announce_caught_up || !self.pending.is_empty() {
                Duration::from_millis(1)
            } else {
                timeout
            })?;
        self.pending.retain(|id| !self.recent.contains(id));
        if !self.pending.is_empty() {
            let ids: Vec<_> = self
                .pending
                .iter()
                .take(usize::try_from(PAGE_SIZE).unwrap_or_default())
                .copied()
                .collect();
            let rows = self.read_rows(Some(&ids))?;
            // NOTE(ts): Sequence allocation does not order commits. Replaying
            // gives all consumers ordered history without domain-specific repair.
            if rows.iter().any(|row| row.id <= self.cursor) {
                return Ok(self.reset(HistoryResetReason::LateCommit));
            }
            for id in ids {
                self.pending.remove(&id);
            }
            self.remember(&rows);
            if !rows.is_empty() {
                return Ok(HistoryUpdate::Rows(rows));
            }
        }
        if self.announce_caught_up && self.pending.is_empty() && drained {
            self.announce_caught_up = false;
            return Ok(HistoryUpdate::CaughtUp {
                last_id: self.cursor,
            });
        }
        Ok(HistoryUpdate::Idle)
    }
}

impl HistorySubscription for PostgresHistorySubscription {
    fn poll(&mut self, timeout: Duration) -> Result<HistoryUpdate, String> {
        let result = self.poll_connected(timeout);
        if result.is_err() {
            self.connections = None;
        }
        result
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    reason = "dedicated temporary PostgreSQL fixture"
)]
mod tests {
    use std::time::Instant;

    use myko::{
        entities::{client::Client as Entity, server::ServerId},
        item::Eventable,
        server::{HistoryEntityKey, HistoryReplayProvider},
        wire::{MEvent, MEventType},
    };

    use super::*;
    use crate::postgres::{PostgresHistoryReplayProvider, ensure_schema};

    struct Fixture {
        config: PostgresConfig,
        client: Client,
    }

    impl Fixture {
        fn new() -> Self {
            let url = std::env::var("MYKO_HISTORY_TEST_URL").expect("dedicated test URL");
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let config = PostgresConfig {
                url,
                table: format!("history_feed_{suffix}"),
                channel: format!("history_feed_{suffix}"),
            };
            let mut client = connect_pg_client(&config, "fixture").expect("fixture connection");
            ensure_schema(&mut client, &config).expect("fixture schema");
            Self { config, client }
        }

        fn insert(&mut self, id: i64, item_id: &str, kind: MEventType, item_type: &str) {
            let item = Entity {
                id: item_id.into(),
                server_id: ServerId::from("fixture"),
                address: None,
                windback: None,
            };
            let event = MEvent::from_item(&item, kind, "fixture");
            let payload = serde_json::to_string(&event).expect("typed event");
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, item_type, item_id, change_type, tx, event) \
                 VALUES ($1, $2, $3, 'SET', 'fixture', ($4::text)::jsonb)",
                        qi(&self.config.table)
                    ),
                    &[&id, &item_type, &item_id, &payload],
                )
                .expect("insert event");
        }

        fn subscribe(&self, filter: HistoryFilter) -> Box<dyn HistorySubscription> {
            PostgresHistoryReplayProvider::new(self.config.clone())
                .subscribe_history(filter)
                .expect("subscribe")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.client.batch_execute(&format!(
                "DROP TABLE IF EXISTS {} CASCADE; DROP FUNCTION IF EXISTS {}()",
                qi(&self.config.table),
                qi(&format!("{}_notify_insert_fn", self.config.table)),
            ));
        }
    }

    fn replay(
        feed: &mut dyn HistorySubscription,
        reason: HistoryResetReason,
    ) -> Vec<HistoryFeedRow> {
        assert_eq!(
            feed.poll(Duration::ZERO).expect("reset"),
            HistoryUpdate::Reset(reason)
        );
        let started = Instant::now();
        let mut result = Vec::new();
        loop {
            assert!(started.elapsed() < Duration::from_secs(5));
            match feed.poll(Duration::from_millis(10)).expect("replay") {
                HistoryUpdate::Rows(rows) => {
                    assert!(rows.len() <= usize::try_from(PAGE_SIZE).expect("page size"));
                    result.extend(rows);
                }
                HistoryUpdate::CaughtUp { .. } => return result,
                HistoryUpdate::Idle => {}
                other @ HistoryUpdate::Reset(_) => panic!("unexpected replay update: {other:?}"),
            }
        }
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn postgres_notification_wait_is_bounded_under_continuous_traffic() {
        let fixture = Fixture::new();
        let mut feed =
            PostgresHistorySubscription::new(fixture.config.clone(), HistoryFilter::default());
        assert_eq!(
            feed.connect().expect("connect"),
            HistoryUpdate::Reset(HistoryResetReason::Initial)
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let config = fixture.config.clone();
        std::thread::scope(|scope| {
            let writer = scope.spawn(move || {
                let mut client = connect_pg_client(&config, "notification writer").expect("writer");
                for id in 0..50 {
                    client
                        .execute(
                            "SELECT pg_notify($1, $2)",
                            &[&config.channel, &id.to_string()],
                        )
                        .expect("notify");
                    if id == 0 {
                        started_tx.send(()).expect("ready");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            started_rx.recv().expect("notification started");
            let started = Instant::now();
            feed.notifications(Duration::from_millis(100))
                .expect("bounded drain");
            assert!(
                started.elapsed() < Duration::from_millis(300),
                "one poll must not keep waiting for subsequent notifications"
            );
            writer.join().expect("writer finished");
        });
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn postgres_filtered_replay_live_and_late_commit() {
        let mut fixture = Fixture::new();
        fixture.insert(20, "node", MEventType::SET, Entity::ENTITY_NAME_STATIC);
        fixture.insert(21, "deleted", MEventType::DEL, Entity::ENTITY_NAME_STATIC);
        fixture.insert(22, "runtime", MEventType::SET, "ExcludedRuntime");
        let mut feed = fixture.subscribe(HistoryFilter {
            item_types: vec![Entity::ENTITY_NAME_STATIC.into()],
            entities: Vec::new(),
        });
        let rows = replay(feed.as_mut(), HistoryResetReason::Initial);
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![20, 21]
        );
        assert_eq!(
            rows.last()
                .expect("delete row")
                .event
                .as_ref()
                .expect("decoded")
                .event
                .change_type,
            MEventType::DEL
        );

        let mut early =
            connect_pg_client(&fixture.config, "early transaction").expect("connection");
        let mut tx = early.transaction().expect("transaction");
        tx.execute(&format!(
            "INSERT INTO {} SELECT 10, item_type, item_id, change_type, created_at, tx, source_id, event FROM {} WHERE id = 20",
            qi(&fixture.config.table), qi(&fixture.config.table),
        ), &[]).expect("uncommitted lower ID");
        fixture.insert(30, "live", MEventType::SET, Entity::ENTITY_NAME_STATIC);
        let update = feed.poll(Duration::from_secs(1)).expect("live event");
        let HistoryUpdate::Rows(rows) = update else {
            panic!("expected live rows: {update:?}");
        };
        assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![30]);
        fixture
            .client
            .execute("SELECT pg_notify($1, '30')", &[&fixture.config.channel])
            .expect("duplicate notify");
        assert_eq!(
            feed.poll(Duration::from_millis(20)).expect("dedup"),
            HistoryUpdate::Idle
        );
        tx.commit().expect("late commit");
        let rows = replay(feed.as_mut(), HistoryResetReason::LateCommit);
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![10, 20, 21, 30]
        );
        drop(feed);
        let mut union = fixture.subscribe(HistoryFilter {
            item_types: vec!["ExcludedRuntime".into()],
            entities: vec![HistoryEntityKey::new(Entity::ENTITY_NAME_STATIC, "node")],
        });
        assert_eq!(
            replay(union.as_mut(), HistoryResetReason::Initial)
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![10, 20, 22]
        );
        drop(union);
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn postgres_bounded_pages_exact_keys_malformed_rows_and_reconnect() {
        let mut fixture = Fixture::new();
        fixture.insert(1, "selected", MEventType::SET, Entity::ENTITY_NAME_STATIC);
        fixture.insert(2, "other", MEventType::SET, Entity::ENTITY_NAME_STATIC);
        fixture.client.execute(&format!(
            "INSERT INTO {} SELECT g, item_type, item_id, change_type, created_at, tx, source_id, event FROM {} CROSS JOIN generate_series(3, 1006) g WHERE id = 1",
            qi(&fixture.config.table), qi(&fixture.config.table),
        ), &[]).expect("multi-page fixture");
        fixture
            .client
            .execute(
                &format!(
                    "UPDATE {} SET event = '{{}}'::jsonb WHERE id = 4",
                    qi(&fixture.config.table)
                ),
                &[],
            )
            .expect("malformed payload");
        let filter = HistoryFilter {
            item_types: Vec::new(),
            entities: vec![HistoryEntityKey::new(
                Entity::ENTITY_NAME_STATIC,
                "selected",
            )],
        };
        let mut feed = PostgresHistorySubscription::new(fixture.config.clone(), filter);
        let rows = replay(&mut feed, HistoryResetReason::Initial);
        assert_eq!(rows.len(), 1005);
        assert!(rows.iter().all(|row| row.id != 2));
        assert_eq!(rows.iter().filter(|row| row.event.is_err()).count(), 1);
        let listener_pid: i32 = feed
            .connections
            .as_mut()
            .expect("connected")
            .1
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("listener pid")
            .get(0);
        fixture
            .client
            .execute("SELECT pg_terminate_backend($1)", &[&listener_pid])
            .expect("disconnect listener");
        assert!(feed.poll(Duration::from_millis(50)).is_err());
        // A lower ID written without a listener must be recovered by replay.
        fixture
            .client
            .execute(
                &format!("DELETE FROM {} WHERE id = 1", qi(&fixture.config.table)),
                &[],
            )
            .expect("prune old event");
        fixture.insert(1, "selected", MEventType::DEL, Entity::ENTITY_NAME_STATIC);
        let rows = replay(&mut feed, HistoryResetReason::Reconnected);
        assert_eq!(
            rows.first()
                .expect("replayed row")
                .event
                .as_ref()
                .expect("valid event")
                .event
                .change_type,
            MEventType::DEL
        );
        let reader_pid: i32 = feed
            .connections
            .as_mut()
            .expect("reconnected")
            .0
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("current reader pid")
            .get(0);
        let listener_pid: i32 = feed
            .connections
            .as_mut()
            .expect("reconnected")
            .1
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("current listener pid")
            .get(0);
        drop(feed);
        let live: i64 = fixture
            .client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 OR pid = $2",
                &[&reader_pid, &listener_pid],
            )
            .expect("connection cleanup")
            .get(0);
        assert_eq!(live, 0);
        let mut all = fixture.subscribe(HistoryFilter::default());
        assert_eq!(
            replay(all.as_mut(), HistoryResetReason::Initial).len(),
            1006
        );
        drop(all);
    }
    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn postgres_caught_up_waits_for_the_notification_backlog() {
        let mut fixture = Fixture::new();
        let mut feed = fixture.subscribe(HistoryFilter::default());
        assert_eq!(
            feed.poll(Duration::ZERO).expect("initial reset"),
            HistoryUpdate::Reset(HistoryResetReason::Initial)
        );
        fixture
            .client
            .query(
                "SELECT pg_notify($1, g::text) FROM generate_series(2000, 3001) g",
                &[&fixture.config.channel],
            )
            .expect("queued notifications for pruned or excluded rows");
        assert_eq!(
            feed.poll(Duration::ZERO)
                .expect("bounded notification drain"),
            HistoryUpdate::Idle
        );
        assert_eq!(
            feed.poll(Duration::ZERO).expect("remaining notifications"),
            HistoryUpdate::CaughtUp { last_id: 0 }
        );
        drop(feed);
    }
}
