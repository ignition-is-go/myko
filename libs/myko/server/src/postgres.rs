//! `PostgreSQL` producer and consumer for the cell-based server.
//!
//! Architecture:
//! - Producer persists `MEvent` rows into a durable table.
//! - Consumer replays table rows (catch-up), then follows new inserts via LISTEN/NOTIFY.

use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ::postgres::{Client, Config as PgClientConfig, NoTls};
use chrono::{DateTime, Utc};
use confique::Config as _;
use myko::{
    event::{MEvent, MEventType},
    hyphae::{Cell, CellImmutable, CellMutable, Mutable},
    item::AnyItem,
    server::{
        CommittedHistoryEvent, EventRecord, HandlerRegistry, HistoryEntityKey, HistoryEvent,
        HistoryPage, MykoServerContext, PersistError, PersistHealth, Persister,
    },
    store::StoreRegistry,
    wire::QueryWindow,
};
use postgres::fallible_iterator::FallibleIterator;
use tracing::{error, info, trace, warn};
use uuid::Uuid;

mod history_feed;
mod schema;
mod transactions;
mod undo_list;

pub use schema::{REQUIRED_SCHEMA_VERSION, SCHEMA_VERSION, SchemaError, migrate};
use schema::{check_server_settings, ensure_schema};

const PG_CONNECT_TIMEOUT_SECS: u64 = 10;
const PG_CATCH_UP_TIMEOUT_SECS_DEFAULT: u64 = 300;
const PG_CATCH_UP_TIMEOUT_ENV: &str = "MYKO_POSTGRES_CATCH_UP_TIMEOUT_SECS";
const PG_KEEPALIVE_IDLE_SECS: u64 = 30;
const PG_KEEPALIVE_INTERVAL_SECS: u64 = 10;
const PG_KEEPALIVE_RETRIES: u32 = 3;
/// `TCP_USER_TIMEOUT`: tear down a connection whose sent data stays unacked for
/// this long. Unlike keepalives — which only probe an *idle* socket — this
/// fires on a peer that died mid-write, so a consumer killed mid-snapshot (OOM)
/// has its Postgres backend reaped promptly instead of lingering in `ClientWrite`
/// holding the events-table lock and stalling every subsequent boot's snapshot
/// until the catch-up timeout (the lv-8c2f "boot convoy").
const PG_TCP_USER_TIMEOUT_SECS: u64 = 30;
const PG_PRODUCER_MAX_BATCH: usize = 256;
const PG_SNAPSHOT_STORE_BATCH: usize = 512;
const PG_FETCH_BATCH: i64 = 1000;

type SnapshotEntries = HashMap<Arc<str>, Vec<(Arc<str>, Arc<dyn AnyItem>)>>;

/// Where rows read back from the log go.
#[derive(Clone)]
pub(crate) enum Landing {
    /// Straight into the stores. A server without partitions has one store
    /// per type, so a row's place is its type's store.
    Stores(Arc<StoreRegistry>),
    /// Through the context's replay, which applies shared rows first and
    /// places each partitioned row in its partition.
    Context(MykoServerContext),
}

impl Landing {
    fn loading(&self) -> Loading {
        match self {
            Self::Stores(registry) => Loading::Stores(Arc::clone(registry), SnapshotEntries::new()),
            Self::Context(ctx) => Loading::Context(ctx.clone(), Vec::new()),
        }
    }

    /// Apply one event another host wrote, as the consumer tails the log.
    pub(crate) fn apply_remote(
        &self,
        event: &MEvent,
        host_id: &str,
        handler_registry: &Arc<HandlerRegistry>,
    ) {
        match self {
            Self::Stores(registry) => {
                apply_remote_event(event, host_id, handler_registry, registry)
            }
            Self::Context(ctx) => {
                if event.source_id.as_deref().is_some_and(|id| id == host_id) {
                    return;
                }
                if let Err(error) = ctx.replay([event.clone()]) {
                    error!("Failed to place {} from the log: {error}", event.item_type);
                }
            }
        }
    }
}

/// A snapshot being loaded: rows are gathered, then land at once.
enum Loading {
    Stores(Arc<StoreRegistry>, SnapshotEntries),
    /// Replay orders the rows, shared first, so every row is gathered.
    Context(MykoServerContext, Vec<MEvent>),
}

impl Loading {
    fn push(&mut self, event: &MEvent, host_id: &str, handler_registry: &HandlerRegistry) {
        match self {
            Self::Stores(registry, pending) => {
                queue_snapshot_event(event, host_id, handler_registry, registry, pending);
            }
            Self::Context(_, events) => {
                if snapshot_item(event, host_id, handler_registry).is_some() {
                    events.push(event.clone());
                }
            }
        }
    }

    fn finish(self) -> Result<(), String> {
        match self {
            Self::Stores(registry, pending) => {
                flush_snapshot_entries(&registry, pending);
                Ok(())
            }
            Self::Context(ctx, events) => ctx
                .replay(events)
                .map(drop)
                .map_err(|error| format!("Failed to place the snapshot: {error}")),
        }
    }
}

/// `PostgreSQL` configuration.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// `PostgreSQL` connection URL.
    pub url: String,
    /// Events table name.
    pub table: String,
    /// LISTEN/NOTIFY channel name.
    pub channel: String,
}

/// Persisted event row fetched from Postgres.
#[derive(Debug, Clone)]
pub struct PersistedEvent {
    pub id: i64,
    pub created_at: String,
    pub event: MEvent,
}

impl PostgresConfig {
    /// Load the `PostgreSQL` settings from the process environment.
    ///
    /// The URL is optional because Myko can still be used with an in-memory
    /// persister. When it is present, all values are parsed before the config
    /// reaches the server runtime.
    ///
    /// # Errors
    ///
    /// Returns the configuration error if an environment value cannot be
    /// loaded or parsed.
    pub fn try_from_env() -> Result<Option<Self>, confique::Error> {
        let values = PostgresEnvironment::builder().env().load()?;
        let Some(url) = values.url else {
            return Ok(None);
        };

        Ok(Some(Self {
            url,
            table: values.table,
            channel: values.channel,
        }))
    }

    /// Create config from environment.
    ///
    /// - `MYKO_POSTGRES_URL` (required)
    /// - `MYKO_POSTGRES_TABLE` (optional, default `myko_events`)
    /// - `MYKO_POSTGRES_CHANNEL` (optional, default `myko_events_notify`)
    #[must_use]
    pub fn from_env() -> Option<Self> {
        Self::try_from_env().ok().flatten()
    }
}

#[derive(confique::Config)]
struct PostgresEnvironment {
    #[config(env = "MYKO_POSTGRES_URL")]
    url: Option<String>,
    #[config(env = "MYKO_POSTGRES_TABLE", default = "myko_events")]
    table: String,
    #[config(env = "MYKO_POSTGRES_CHANNEL", default = "myko_events_notify")]
    channel: String,
}

pub(crate) fn postgres_catch_up_timeout_from_env() -> Result<Option<Duration>, String> {
    match std::env::var(PG_CATCH_UP_TIMEOUT_ENV) {
        Ok(value) => parse_postgres_catch_up_timeout(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse_postgres_catch_up_timeout(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "{PG_CATCH_UP_TIMEOUT_ENV} must be a non-negative integer number of seconds"
        )),
    }
}

fn parse_postgres_catch_up_timeout(value: Option<&str>) -> Result<Option<Duration>, String> {
    let seconds = value.map_or(Ok(PG_CATCH_UP_TIMEOUT_SECS_DEFAULT), |raw| {
        raw.parse::<u64>().map_err(|_| {
            format!("{PG_CATCH_UP_TIMEOUT_ENV} must be a non-negative integer number of seconds")
        })
    })?;
    Ok((seconds != 0).then(|| Duration::from_secs(seconds)))
}

/// Read API for durable event history (windback/replay providers).
pub struct PostgresHistoryStore {
    config: PostgresConfig,
}

impl PostgresHistoryStore {
    /// Create a history store and ensure schema exists.
    ///
    /// # Errors
    ///
    /// Returns an error when configured identifiers are invalid.
    pub fn new(config: PostgresConfig) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;
        Ok(Self { config })
    }

    /// Read durable envelopes without discarding provenance. Legacy rows return
    /// None; metadata is correlation evidence, never verified user identity.
    ///
    /// # Errors
    /// Returns an error when `PostgreSQL` cannot be queried or a row is invalid.
    pub fn load_records_after_id(
        &self,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, EventRecord)>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_records_after_id)")?;
        let sql = format!(
            "SELECT id, event::text FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
            qi(&self.config.table)
        );
        client
            .query(&sql, &[&after_id, &limit])
            .map_err(|e| format!("history query failed: {e}"))?
            .into_iter()
            .map(|row| {
                let id: i64 = row.get(0);
                let json: String = row.get(1);
                serde_json::from_str(&json)
                    .map(|record| (id, record))
                    .map_err(|e| format!("invalid history record id={id}: {e}"))
            })
            .collect()
    }

    /// Read events with `id > after_id`, ascending.
    ///
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` cannot be queried or a row is invalid.
    pub fn load_after_id(&self, after_id: i64, limit: i64) -> Result<Vec<PersistedEvent>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_after_id)")?;
        let sql = format!(
            "SELECT id, created_at, event::text FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
            qi(&self.config.table)
        );
        let rows = client
            .query(&sql, &[&after_id, &limit])
            .map_err(|e| format!("history query failed: {e}"))?;
        rows.into_iter()
            .map(row_to_persisted_event)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Read events with `id > after_id` and `created_at <= until`, ascending.
    ///
    /// # Errors
    ///
    /// Returns an error when the timestamp is invalid or `PostgreSQL` cannot be queried.
    pub fn load_until(
        &self,
        after_id: i64,
        until: &str,
        limit: i64,
    ) -> Result<Vec<PersistedEvent>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_until)")?;
        // NOTE(ts): Validate timestamp format to prevent SQL injection
        if !until
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-.:+TZ ".contains(c))
        {
            return Err(format!("Invalid timestamp format: {until}"));
        }
        let sql = format!(
            "SELECT id, created_at, event::text FROM {} WHERE id > {} AND created_at <= '{}'::timestamptz ORDER BY id ASC LIMIT {}",
            qi(&self.config.table),
            after_id,
            until,
            limit
        );
        let rows = client
            .query(&sql, &[])
            .map_err(|e| format!("history query failed: {e}"))?;
        rows.into_iter()
            .map(row_to_persisted_event)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Read events in a time window, ascending.
    ///
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` cannot be queried or a row is invalid.
    pub fn load_between(
        &self,
        from_iso: &str,
        to_iso: &str,
        limit: i64,
    ) -> Result<Vec<PersistedEvent>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_between)")?;
        let sql = format!(
            "SELECT id, created_at, event::text FROM {} WHERE created_at >= $1::timestamptz AND created_at <= $2::timestamptz ORDER BY id ASC LIMIT $3",
            qi(&self.config.table)
        );
        let rows = client
            .query(&sql, &[&from_iso, &to_iso, &limit])
            .map_err(|e| format!("history query failed: {e}"))?;
        rows.into_iter()
            .map(row_to_persisted_event)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Read durable events for one entity, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` cannot count, read, or decode the
    /// requested history page.
    pub fn load_entity_history_page(
        &self,
        item_type: &str,
        item_id: &str,
        window: &QueryWindow,
    ) -> Result<HistoryPage, String> {
        let item_types = myko::server::history_storage_types(item_type);
        let mut client = connect_pg_client(&self.config, "history(entity)")?;
        let count_sql = format!(
            "SELECT COUNT(*)::bigint FROM {} WHERE item_type = ANY($1) AND item_id = $2",
            qi(&self.config.table)
        );
        let total_count: i64 = client
            .query_one(&count_sql, &[&item_types, &item_id])
            .map_err(|e| format!("history count query failed: {e}"))?
            .get(0);
        let total_count = usize::try_from(total_count)
            .map_err(|_| "history row count does not fit usize".to_string())?;
        let sql = format!(
            "SELECT id, created_at, event::text FROM {} WHERE item_type = ANY($1) AND item_id = $2 ORDER BY id DESC LIMIT $3 OFFSET $4",
            qi(&self.config.table)
        );
        let limit = i64::try_from(window.limit)
            .map_err(|_| "history page size is too large".to_string())?;
        let offset = i64::try_from(window.offset)
            .map_err(|_| "history page offset is too large".to_string())?;
        let rows = client
            .query(&sql, &[&item_types, &item_id, &limit, &offset])
            .map_err(|e| format!("history query failed: {e}"))?;
        let events = rows
            .into_iter()
            .map(|row| {
                let id: i64 = row.get(0);
                let created_at: DateTime<Utc> = row.get(1);
                let event_json: String = row.get(2);
                let event = MEvent::from_str_trim(&event_json)
                    .map_err(|e| format!("invalid history event payload for id={id}: {e}"))?;
                Ok(HistoryEvent {
                    id,
                    created_at,
                    event,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(HistoryPage {
            events,
            total_count,
        })
    }
}

/// `HistoryReplayProvider` backed by `PostgresHistoryStore`.
pub struct PostgresHistoryReplayProvider {
    undo_list: Arc<undo_list::Source>,
    config: PostgresConfig,
    committed: Cell<Option<Arc<CommittedHistoryEvent>>, CellMutable>,
}

impl PostgresHistoryReplayProvider {
    #[must_use]
    pub fn new(config: PostgresConfig) -> Self {
        let undo_list = undo_list::Source::shared(&config);
        Self {
            undo_list,
            config,
            committed: Cell::new(None),
        }
    }

    fn observe_committed(&self, key: HistoryEntityKey, row_id: i64) {
        self.committed
            .set(Some(Arc::new(CommittedHistoryEvent { key, row_id })));
    }
}

impl myko::server::HistoryReplayProvider for PostgresHistoryReplayProvider {
    fn undo_list_source(
        &self,
        user: &myko::identity::UserId,
    ) -> Result<Cell<Arc<myko::entities::undo::UndoLogSnapshot>, CellImmutable>, String> {
        Ok(self.undo_list.watch(user.as_ref()))
    }
    fn undo_boundary_exists(&self, id: &str) -> Result<bool, String> {
        PostgresHistoryStore::new(self.config.clone())?.undo_boundary_exists(id)
    }

    fn history_identity(&self) -> Option<String> {
        Some(format!(
            "postgres:{}:{}",
            self.config.url, self.config.table
        ))
    }

    fn transaction_exists(&self, tx: &str) -> Result<bool, String> {
        PostgresHistoryStore::new(self.config.clone())?.transaction_exists(tx)
    }

    fn transaction_history(&self, tx: &str) -> Result<myko::server::TransactionHistory, String> {
        PostgresHistoryStore::new(self.config.clone())?.transaction_history(tx)
    }

    fn transaction_history_excluding(
        &self,
        tx: &str,
        ignored: &[Arc<str>],
    ) -> Result<myko::server::TransactionHistory, String> {
        PostgresHistoryStore::new(self.config.clone())?.transaction_history_excluding(tx, ignored)
    }

    fn latest_user_transaction(
        &self,
        user: &myko::identity::UserId,
    ) -> Result<Option<Arc<str>>, String> {
        PostgresHistoryStore::new(self.config.clone())?.latest_user_transaction(user)
    }

    fn user_transaction_page(
        &self,
        user: &myko::identity::UserId,
        before_first_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<myko::server::UserTransaction>, String> {
        PostgresHistoryStore::new(self.config.clone())?.user_transaction_page(
            user,
            before_first_id,
            limit,
        )
    }

    fn entity_event_at(
        &self,
        key: &myko::server::HistoryEntityKey,
        row_id: i64,
    ) -> Result<Option<MEvent>, String> {
        PostgresHistoryStore::new(self.config.clone())?.entity_event_at(key, row_id)
    }

    fn subscribe_history(
        &self,
        filter: myko::server::HistoryFilter,
    ) -> Result<Box<dyn myko::server::HistorySubscription>, String> {
        validate_ident(&self.config.table)?;
        validate_ident(&self.config.channel)?;
        Ok(Box::new(history_feed::PostgresHistorySubscription::new(
            self.config.clone(),
            filter,
        )))
    }

    fn committed_history_event(&self) -> Cell<Option<Arc<CommittedHistoryEvent>>, CellImmutable> {
        self.committed.clone().lock()
    }

    fn entity_history_page(
        &self,
        key: &HistoryEntityKey,
        window: &QueryWindow,
    ) -> Result<HistoryPage, String> {
        PostgresHistoryStore::new(self.config.clone())?.load_entity_history_page(
            &key.item_type,
            &key.item_id,
            window,
        )
    }

    fn replay_to_store(
        &self,
        until: &str,
        handler_registry: &HandlerRegistry,
    ) -> Result<Arc<StoreRegistry>, String> {
        eprintln!(
            "[HistoryReplay] loading snapshot as of {} from {}",
            until, self.config.url
        );

        // NOTE(ts): Validate timestamp format to prevent SQL injection
        if !until
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-.:+TZ ".contains(c))
        {
            return Err(format!("Invalid timestamp format: {until}"));
        }

        let mut client = connect_pg_client(&self.config, "history_replay")?;
        let table = qi(&self.config.table);
        let registry = StoreRegistry::new();

        // NOTE(ts): Use DISTINCT ON to get the latest event per entity as of the
        // timestamp, same approach as the bootstrap snapshot but time-bounded.
        // Only include entities whose latest event is a SET (not deleted).
        let sql = format!(
            "
            WITH latest AS (
                SELECT DISTINCT ON (item_type, item_id)
                    id, change_type
                FROM {table}
                WHERE created_at <= '{until}'::timestamptz
                ORDER BY item_type, item_id, id DESC
            )
            SELECT e.event::text
            FROM latest
            JOIN {table} e ON e.id = latest.id
            WHERE latest.change_type = 'SET'
            ORDER BY e.id ASC
            "
        );

        let rows = client
            .query(&sql, &[])
            .map_err(|e| format!("history snapshot query failed: {e}"))?;

        let mut count = 0usize;
        for row in &rows {
            let event_json: String = row.get(0);
            match MEvent::from_str_trim(&event_json) {
                Ok(event) => {
                    if let Some(parse) = handler_registry.item_parser(&event.item_type)
                        && let Ok(item) = parse(event.item.clone())
                    {
                        let store = registry.get_or_create(&event.item_type);
                        store.insert(item.id(), item);
                        count = count.saturating_add(1);
                    }
                }
                Err(err) => {
                    eprintln!("[HistoryReplay] invalid event row: {err}");
                }
            }
        }

        eprintln!(
            "[HistoryReplay] loaded {} entities from {} rows as of {}",
            count,
            rows.len(),
            until
        );

        Ok(Arc::new(registry))
    }
}

enum ProducerRequest {
    Event(
        EventRecord,
        Option<Arc<myko::entities::undo::UndoPreviewInput>>,
    ),
    Barrier(flume::Sender<()>),
    Complete {
        tx: String,
        user: String,
    },
}

/// Handle to the `PostgreSQL` producer.
#[derive(Clone)]
pub struct PostgresProducerHandle {
    sender: flume::Sender<ProducerRequest>,
    host_id: Uuid,
    config: PostgresConfig,
    health: Arc<PersistHealth>,
}

impl PostgresProducerHandle {
    /// Persist an event. Enqueues to the background producer thread and
    /// returns immediately. Returns Err only if the channel is full (backpressure).
    ///
    /// # Errors
    ///
    /// Returns an error when the producer thread is unavailable.
    pub fn produce(&self, event: MEvent) -> Result<(), PersistError> {
        self.produce_record(EventRecord {
            event,
            provenance: None,
        })
    }

    fn produce_record(&self, record: EventRecord) -> Result<(), PersistError> {
        self.produce_record_with_preview(record, None)
    }
    fn produce_record_with_preview(
        &self,
        mut record: EventRecord,
        preview: Option<Arc<myko::entities::undo::UndoPreviewInput>>,
    ) -> Result<(), PersistError> {
        // Caller labels are untrusted. PostgreSQL JSONB cannot encode NUL,
        // and retrying such a sidecar would poison an otherwise valid batch.
        if record.provenance.as_ref().is_some_and(|provenance| {
            provenance.request_tx.contains('\0')
                || provenance
                    .actor_user_id
                    .as_ref()
                    .is_some_and(|user| user.as_ref().contains('\0'))
                || provenance
                    .undo_of_tx
                    .as_ref()
                    .is_some_and(|tx| tx.contains('\0'))
                || provenance.command_id.contains('\0')
                || provenance
                    .unverified_client_id
                    .as_ref()
                    .is_some_and(|id| id.contains('\0'))
                || provenance.lineage.iter().any(|entry| entry.contains('\0'))
        }) {
            let message = "operation provenance contains unsupported null bytes".to_string();
            self.health.record_dropped(message.clone());
            return Err(PersistError {
                entity_type: record.event.item_type,
                message,
            });
        }
        let event = &mut record.event;
        if event.source_id.is_none() {
            event.source_id = Some(std::sync::Arc::from(self.host_id.to_string()));
        }
        let entity_type = event.item_type.clone();
        if self
            .sender
            .send(ProducerRequest::Event(record, preview))
            .is_ok()
        {
            self.health.record_enqueue();
            Ok(())
        } else {
            let msg = "Postgres producer thread not running".to_string();
            self.health.record_dropped(msg.clone());
            Err(PersistError {
                entity_type,
                message: msg,
            })
        }
    }
}

impl Persister for PostgresProducerHandle {
    fn wants_undo_preview(&self) -> bool {
        true
    }
    fn persist_record_with_preview(
        &self,
        record: EventRecord,
        preview: Option<Arc<myko::entities::undo::UndoPreviewInput>>,
    ) -> Result<(), PersistError> {
        self.produce_record_with_preview(record, preview)
    }

    fn history_identity(&self) -> Option<String> {
        Some(format!(
            "postgres:{}:{}",
            self.config.url, self.config.table
        ))
    }

    fn complete_transaction(&self, tx: &str, user: &myko::identity::UserId) -> Result<(), String> {
        self.sender
            .send(ProducerRequest::Complete {
                tx: tx.to_owned(),
                user: user.to_string(),
            })
            .map_err(|_| "Postgres producer stopped".to_owned())
    }

    fn flush(&self) -> Result<(), String> {
        let (tx, rx) = flume::bounded(1);
        self.sender
            .send(ProducerRequest::Barrier(tx))
            .map_err(|_| "Postgres producer stopped".to_owned())?;
        rx.recv_timeout(Duration::from_secs(10))
            .map_err(|_| "timed out waiting for durable PostgreSQL persistence".to_owned())
    }

    fn persist_record(&self, record: EventRecord) -> Result<(), PersistError> {
        self.produce_record(record)
    }

    fn supports_provenance(&self) -> bool {
        true
    }

    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        self.produce(event)
    }

    fn health(&self) -> Arc<PersistHealth> {
        self.health.clone()
    }

    fn startup_healthcheck(&self) -> Result<(), String> {
        validate_ident(&self.config.table)?;
        validate_ident(&self.config.channel)?;
        Ok(())
    }
}

/// `PostgreSQL` producer — background thread with fail-fast error propagation.
pub struct PostgresProducer {
    handle: PostgresProducerHandle,
}

impl PostgresProducer {
    /// Create a new `PostgreSQL` producer.
    ///
    /// # Errors
    ///
    /// Returns an error when configured identifiers are invalid.
    pub fn new(config: &PostgresConfig, host_id: Uuid) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;

        let health = Arc::new(PersistHealth::default());
        let (tx, rx) = flume::unbounded::<ProducerRequest>();
        let cfg = config.clone();
        let thread_health = health.clone();
        std::thread::spawn(move || run_producer_loop(&cfg, &rx, &thread_health));

        Ok(Self {
            handle: PostgresProducerHandle {
                sender: tx,
                host_id,
                config: config.clone(),
                health,
            },
        })
    }

    /// Get a shareable persister handle.
    #[must_use]
    pub fn handle(&self) -> PostgresProducerHandle {
        self.handle.clone()
    }
}

fn run_producer_loop(
    config: &PostgresConfig,
    rx: &flume::Receiver<ProducerRequest>,
    health: &Arc<PersistHealth>,
) {
    let undo_list = undo_list::Source::shared(config);
    let mut client: Option<Client> = None;
    let mut batch = Vec::<EventRecord>::new();
    let mut previews = Vec::new();
    let mut pending: Option<ProducerRequest> = None;
    loop {
        if batch.is_empty() && pending.is_none() {
            match rx.recv() {
                Ok(ProducerRequest::Event(record, preview)) => {
                    batch.push(record);
                    previews.push(preview);
                }
                Ok(request) => pending = Some(request),
                Err(_) => break,
            }
        }
        while pending.is_none() && batch.len() < PG_PRODUCER_MAX_BATCH {
            match rx.try_recv() {
                Ok(ProducerRequest::Event(record, preview)) => {
                    batch.push(record);
                    previews.push(preview);
                }
                Ok(request) => pending = Some(request),
                Err(_) => break,
            }
        }
        if client.is_none() {
            client = connect_producer_client(config);
        }
        let mut sequence = undo_list
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = if let Some(client) = client.as_mut() {
            insert_event_batch(client, config, &batch).and_then(|mut changes| {
                for (change, preview) in changes.iter_mut().zip(&previews) { change.preview.clone_from(preview); }
                undo_list.records(&mut sequence, changes);
                if let Some(ProducerRequest::Complete { tx, user }) = &pending {
                    let completions = qi(&format!("{}_transactions", config.table));
                    let table = qi(&config.table);
                    let completed = client.query_opt(&format!("INSERT INTO {completions} (tx, user_id, first_id, last_id)
                        SELECT $1, $2, MIN(id), MAX(id) FROM {table} WHERE tx = $1 HAVING MAX(id) IS NOT NULL
                        ON CONFLICT (tx) DO UPDATE SET user_id = EXCLUDED.user_id, first_id = EXCLUDED.first_id, last_id = EXCLUDED.last_id RETURNING first_id"), &[tx, user])?;
                    if let Some(row) = completed { undo_list.complete(&mut sequence, tx, user, row.get(0)); }
                }
                Ok(())
            })
        } else {
            health.record_error_no_dequeue("Postgres producer connection failed".into());
            std::thread::sleep(Duration::from_secs(1));
            continue;
        };
        drop(sequence);
        match result {
            Ok(()) => {
                health.record_success_batch(u64::try_from(batch.len()).unwrap_or(u64::MAX));
                batch.clear();
                previews.clear();
                if let Some(ProducerRequest::Barrier(reply)) = pending.take() {
                    let _ = reply.send(());
                }
            }
            Err(error) => {
                health.record_error_no_dequeue(format_pg_error(
                    "persist transaction history",
                    Some(&config.url),
                    &error,
                ));
                client = None;
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn connect_producer_client(config: &PostgresConfig) -> Option<Client> {
    match connect_pg_client(config, "producer") {
        Ok(mut c) => match ensure_schema(&mut c, config) {
            Ok(()) => Some(c),
            Err(err) => {
                error!("{}", err.describe("ensure_schema(producer)", &config.url));
                None
            }
        },
        Err(err) => {
            error!("{err}");
            None
        }
    }
}

fn invalidate_transaction_completions(
    client: &mut Client,
    config: &PostgresConfig,
    events: &[EventRecord],
) -> Result<(), ::postgres::Error> {
    let completions = qi(&format!("{}_transactions", config.table));
    let transactions: Vec<_> = events
        .iter()
        .map(|record| record.event.tx.to_string())
        .collect();
    client.execute(
        &format!("DELETE FROM {completions} WHERE tx = ANY($1)"),
        &[&transactions],
    )?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn insert_event_batch(
    client: &mut Client,
    config: &PostgresConfig,
    events: &[EventRecord],
) -> Result<Vec<myko::entities::undo::UndoListChange>, ::postgres::Error> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let table = qi(&config.table);
    invalidate_transaction_completions(client, config, events)?;

    // NOTE(ts): Single event — skip transaction overhead.
    if events.len() == 1 {
        let Some(record) = events.first() else {
            return Ok(Vec::new());
        };
        let event = &record.event;
        let sql = format!(
            "INSERT INTO {table} (item_type, item_id, change_type, created_at, tx, source_id, event) VALUES ($1, $2, $3, ($4::text)::timestamptz, $5, ($6::text), ($7::text)::jsonb) RETURNING id"
        );
        let item_id = event
            .item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let event_json = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());
        let change_type = match event.change_type {
            MEventType::SET => "SET",
            MEventType::DEL => "DEL",
        };
        let item_type: &str = &event.item_type;
        let created_at: &str = &event.created_at;
        let tx: &str = &event.tx;
        let source_id: Option<&str> = event.source_id.as_deref();
        let row = client.query_one(
            &sql,
            &[
                &item_type,
                &item_id,
                &change_type,
                &created_at,
                &tx,
                &source_id,
                &event_json,
            ],
        )?;
        return Ok(vec![myko::entities::undo::UndoListChange::from_record(
            row.get(0),
            record,
        )]);
    }

    // NOTE(ts): Build a multi-row INSERT for the batch within a single transaction.
    let mut sql = format!(
        "INSERT INTO {table} (item_type, item_id, change_type, created_at, tx, source_id, event) VALUES "
    );
    let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> =
        Vec::with_capacity(events.len().saturating_mul(7));
    for (i, record) in events.iter().enumerate() {
        let event = &record.event;
        if i > 0 {
            sql.push_str(", ");
        }
        let base = i.saturating_mul(7);
        let _ = write!(
            sql,
            "(${}, ${}, ${}, (${}::text)::timestamptz, ${}, (${}::text), (${}::text)::jsonb)",
            base.saturating_add(1),
            base.saturating_add(2),
            base.saturating_add(3),
            base.saturating_add(4),
            base.saturating_add(5),
            base.saturating_add(6),
            base.saturating_add(7)
        );
        let item_id = event
            .item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let event_json = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());
        let change_type = match event.change_type {
            MEventType::SET => "SET",
            MEventType::DEL => "DEL",
        };
        params.push(Box::new(event.item_type.to_string()));
        params.push(Box::new(item_id));
        params.push(Box::new(change_type.to_string()));
        params.push(Box::new(event.created_at.to_string()));
        params.push(Box::new(event.tx.to_string()));
        params.push(Box::new(
            event
                .source_id
                .as_ref()
                .map(std::string::ToString::to_string),
        ));
        params.push(Box::new(event_json));
    }

    let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
        params.iter().map(std::convert::AsRef::as_ref).collect();

    sql.push_str(" RETURNING id");
    let mut txn = client.transaction()?;
    let rows = txn.query(&sql, &param_refs)?;
    txn.commit()?;

    Ok(rows
        .iter()
        .zip(events)
        .map(|(row, record)| myko::entities::undo::UndoListChange::from_record(row.get(0), record))
        .collect())
}

/// Shared status for startup catch-up.
#[derive(Debug)]
pub struct CatchUpStatus {
    caught_up: AtomicBool,
    failed: AtomicBool,
    failure_reason: std::sync::RwLock<Option<String>>,
}

impl CatchUpStatus {
    const fn new() -> Self {
        Self {
            caught_up: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            failure_reason: std::sync::RwLock::new(None),
        }
    }

    /// Check if startup catch-up completed.
    pub fn is_caught_up(&self) -> bool {
        self.caught_up.load(Ordering::SeqCst)
    }

    /// Check if catch-up has failed.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn fail(&self, reason: impl Into<String>) {
        let reason = reason.into();
        *self
            .failure_reason
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reason);
        self.failed.store(true, Ordering::SeqCst);
        self.caught_up.store(false, Ordering::SeqCst);
    }

    /// Block until caught up or timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when catch-up fails or the timeout expires.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> Result<(), String> {
        self.wait_until_caught_up_configured(Some(timeout))
    }

    fn wait_until_caught_up_configured(&self, timeout: Option<Duration>) -> Result<(), String> {
        let start = std::time::Instant::now();
        while !self.is_caught_up() {
            if self.is_failed() {
                return Err(self
                    .failure_reason
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .unwrap_or_else(|| "Postgres catch-up failed".to_string()));
            }
            if let Some(timeout) = timeout.filter(|timeout| start.elapsed() >= *timeout) {
                return Err(format!(
                    "Postgres catch-up timed out after {}s",
                    timeout.as_secs()
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }
}

/// `PostgreSQL` consumer that replays + tails events.
pub struct PostgresConsumer {
    catch_up_status: Arc<CatchUpStatus>,
    _handle: std::thread::JoinHandle<()>,
}

impl PostgresConsumer {
    /// Start a `PostgreSQL` event consumer thread.
    ///
    /// # Errors
    ///
    /// Returns an error when configured identifiers are invalid.
    pub fn start(
        config: &PostgresConfig,
        host_id: Uuid,
        handler_registry: Arc<HandlerRegistry>,
        landing: Landing,
        history_replay: Arc<PostgresHistoryReplayProvider>,
    ) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;

        let catch_up_status = Arc::new(CatchUpStatus::new());
        let status = catch_up_status.clone();
        let cfg = config.clone();
        let host_id_string = host_id.to_string();

        let handle = std::thread::spawn(move || {
            if let Err(err) = run_consumer_loop(
                &cfg,
                &host_id_string,
                &handler_registry,
                &landing,
                &status,
                &history_replay,
            ) {
                let reason = format!("Postgres consumer failed: {err}");
                error!("{reason}");
                status.fail(reason);
            }
        });

        Ok(Self {
            catch_up_status,
            _handle: handle,
        })
    }

    /// Check if startup catch-up is complete.
    #[must_use]
    pub fn is_caught_up(&self) -> bool {
        self.catch_up_status.is_caught_up()
    }

    /// Wait for startup catch-up with timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when catch-up fails or the timeout expires.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> Result<(), String> {
        self.catch_up_status.wait_until_caught_up(timeout)
    }

    pub(crate) fn wait_until_caught_up_without_timeout(&self) -> Result<(), String> {
        self.catch_up_status.wait_until_caught_up_configured(None)
    }
}

fn load_consumer_snapshot(
    reader: &mut Client,
    config: &PostgresConfig,
    host_id: &str,
    handler_registry: &Arc<HandlerRegistry>,
    landing: &Landing,
) -> Result<(i64, usize), String> {
    info!(
        "PostgresConsumer started (table={}, channel={})",
        config.table, config.channel
    );
    let pg_error = |role: &str| {
        let url = config.url.clone();
        let role = role.to_owned();
        move |error: postgres::Error| format_pg_error(&role, Some(&url), &error)
    };
    // NOTE(ts): One snapshot for the high-water mark and the state read, so the state is
    // exactly the log up to high_water and catch-up resumes after it without a gap.
    let mut snapshot = reader
        .build_transaction()
        .isolation_level(postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .map_err(pg_error("begin(snapshot)"))?;
    let high_water: i64 = snapshot
        .query_one(
            &format!("SELECT COALESCE(MAX(id), 0) FROM {}", qi(&config.table)),
            &[],
        )
        .map_err(pg_error("query(high_water)"))?
        .get(0);
    let source = if schema::latest_table_ready(&mut snapshot, &config.table)
        .map_err(pg_error("query(schema version)"))?
    {
        SnapshotSource::LatestTable
    } else {
        SnapshotSource::EventsSkipScan
    };
    info!("Postgres snapshot source: {source:?}");
    let snapshot_types = snapshot_item_types(handler_registry);
    let params: [&(dyn postgres::types::ToSql + Sync); 2] = [&high_water, &snapshot_types];
    let mut rows = snapshot
        .query_raw(&snapshot_sql(&config.table, source), params)
        .map_err(pg_error("query(snapshot latest events)"))?;
    let mut count = 0_usize;
    let mut loading = landing.loading();
    while let Some(row) = rows
        .next()
        .map_err(pg_error("stream(snapshot latest events)"))?
    {
        let id: i64 = row.get(0);
        let event_json: String = row.get(1);
        match MEvent::from_str_trim(&event_json) {
            Ok(event) => {
                loading.push(&event, host_id, handler_registry);
            }
            Err(error) => error!("Invalid postgres snapshot row id={id}: {error}"),
        }
        count = count.saturating_add(1);
    }
    drop(rows);
    snapshot.commit().map_err(pg_error("commit(snapshot)"))?;
    loading.finish()?;
    Ok((high_water, count))
}

/// Types whose events this server can parse; the snapshot only hydrates those.
fn snapshot_item_types(handler_registry: &HandlerRegistry) -> Vec<String> {
    let mut types: Vec<String> = handler_registry
        .entity_types()
        .map(ToString::to_string)
        .collect();
    types.sort();
    types
}

/// Parseable types plus their history storage aliases, which tailing observes for history feeds.
fn tailed_item_types(handler_registry: &HandlerRegistry) -> Vec<String> {
    let mut types: Vec<String> = handler_registry
        .entity_types()
        .flat_map(|kind| myko::server::history_storage_types(kind))
        .collect();
    types.sort();
    types.dedup();
    types
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotSource {
    /// `{table}_latest`, once its backfill migration is recorded.
    LatestTable,
    /// Loose index scan over `item_latest_idx`: each step jumps to the next item of the
    /// same type, so cost scales with this server's item count rather than the log length.
    EventsSkipScan,
}

/// Latest event at or below `$1` for every item of the types in `$2`, keeping SET rows.
fn snapshot_sql(table: &str, source: SnapshotSource) -> String {
    let events = qi(table);
    match source {
        SnapshotSource::LatestTable => format!(
            "SELECT last_id, event::text FROM {} \
             WHERE change_type = 'SET' AND item_type = ANY($2) AND last_id <= $1 ORDER BY last_id ASC",
            qi(&format!("{table}_latest"))
        ),
        // NOTE(ts): Select each item's latest event, deletes included, before filtering on
        // change_type; filtering first would resurrect an item from its last SET.
        SnapshotSource::EventsSkipScan => format!(
            "
            WITH RECURSIVE latest AS (
                SELECT first.item_type, first.item_id, first.id
                FROM unnest($2::text[]) AS kinds(item_type)
                CROSS JOIN LATERAL (
                    SELECT m.item_type, m.item_id, m.id FROM {events} m
                    WHERE m.item_type = kinds.item_type AND m.id <= $1
                    ORDER BY m.item_id, m.id DESC
                    LIMIT 1
                ) first
                UNION ALL
                SELECT next.item_type, next.item_id, next.id
                FROM latest
                CROSS JOIN LATERAL (
                    SELECT m.item_type, m.item_id, m.id FROM {events} m
                    WHERE m.item_type = latest.item_type AND m.item_id > latest.item_id AND m.id <= $1
                    ORDER BY m.item_id, m.id DESC
                    LIMIT 1
                ) next
            )
            SELECT e.id, e.event::text
            FROM latest
            JOIN {events} e ON e.id = latest.id
            WHERE e.change_type = 'SET'
            ORDER BY e.id ASC
            "
        ),
    }
}

/// Loads the SET events of a snapshot into the stores, as the startup load
/// does. Returns the number of events read.
pub(crate) fn hydrate_stores(
    events: impl IntoIterator<Item = MEvent>,
    host_id: &str,
    handler_registry: &HandlerRegistry,
    landing: &Landing,
) -> Result<usize, String> {
    let mut loading = landing.loading();
    let mut count = 0_usize;
    for event in events {
        if event.change_type == MEventType::SET {
            loading.push(&event, host_id, handler_registry);
            count = count.saturating_add(1);
        }
    }
    loading.finish()?;
    Ok(count)
}

fn flush_snapshot_entries(registry: &StoreRegistry, pending: SnapshotEntries) {
    for (entity_type, entries) in pending {
        registry.get_or_create(&entity_type).insert_many(entries);
    }
}

fn queue_snapshot_event(
    event: &MEvent,
    host_id: &str,
    handler_registry: &HandlerRegistry,
    registry: &StoreRegistry,
    pending: &mut SnapshotEntries,
) {
    let Some(item) = snapshot_item(event, host_id, handler_registry) else {
        return;
    };
    let entity_type: Arc<str> = item.entity_type().into();
    let entries = pending.entry(entity_type.clone()).or_default();
    entries.push((item.id(), item));
    if entries.len() >= PG_SNAPSHOT_STORE_BATCH {
        registry
            .get_or_create(&entity_type)
            .insert_many(std::mem::take(entries));
    }
}

/// The item a snapshot row loads, or `None` for a row this host wrote or
/// cannot parse.
fn snapshot_item(
    event: &MEvent,
    host_id: &str,
    handler_registry: &HandlerRegistry,
) -> Option<Arc<dyn AnyItem>> {
    if event.source_id.as_deref().is_some_and(|id| id == host_id) {
        return None;
    }
    let Some(parse) = handler_registry.item_parser(&event.item_type) else {
        warn!("No parser for entity type: {}", event.item_type);
        return None;
    };
    match parse(event.item.clone()) {
        Ok(item) => Some(item),
        Err(error) => {
            let message = error.to_string();
            let short = message
                .find(", expected one of")
                .and_then(|position| message.get(..position).map(str::to_string))
                .unwrap_or(message);
            error!("Failed to parse {}: {short}", event.item_type);
            None
        }
    }
}

fn run_consumer_loop(
    config: &PostgresConfig,
    host_id: &str,
    handler_registry: &Arc<HandlerRegistry>,
    landing: &Landing,
    status: &Arc<CatchUpStatus>,
    history_replay: &PostgresHistoryReplayProvider,
) -> Result<(), String> {
    let mut reader = connect_consumer_client("reader", config)?;
    let mut listener = connect_listener_client(config)?;
    check_server_settings(&mut reader);
    let (high_water, snapshot_count) =
        load_consumer_snapshot(&mut reader, config, host_id, handler_registry, landing)?;
    let table = qi(&config.table);
    let high_water_sql = format!("SELECT COALESCE(MAX(id), 0) FROM {table}");
    let fetch_sql = format!(
        "SELECT id, item_type, item_id, event::text FROM {table} \
         WHERE id > $1 AND id <= $2 AND item_type = ANY($3) ORDER BY id ASC LIMIT $4"
    );
    let tailed_types = tailed_item_types(handler_registry);
    info!(
        "Postgres snapshot loaded latest state rows={} high_water={}",
        snapshot_count, high_water
    );

    let mut last_seen_id: i64 = high_water;
    let mut initial_done = false;

    loop {
        // NOTE(ts): Bound each pass by the current max id so rows of foreign types advance
        // last_seen_id too; otherwise every poll would rescan another app's tail.
        let fetched = reader.query_one(&high_water_sql, &[]).and_then(|row| {
            let bound: i64 = row.get(0);
            reader
                .query(
                    &fetch_sql,
                    &[&last_seen_id, &bound, &tailed_types, &PG_FETCH_BATCH],
                )
                .map(|rows| (bound, rows))
        });
        let rows = match fetched {
            Ok((bound, rows)) => {
                let full = i64::try_from(rows.len()).is_ok_and(|len| len >= PG_FETCH_BATCH);
                if !full {
                    last_seen_id = last_seen_id.max(bound);
                }
                rows
            }
            Err(err) => {
                warn!(
                    "{}",
                    format_pg_error("query(fetch events)", Some(&config.url), &err)
                );
                std::thread::sleep(Duration::from_millis(500));
                reader = connect_consumer_client("reader", config)?;
                continue;
            }
        };
        if !rows.is_empty() {
            for row in rows {
                let id: i64 = row.get(0);
                let item_type: String = row.get(1);
                let item_id: String = row.get(2);
                let event_json: String = row.get(3);
                last_seen_id = last_seen_id.max(id);

                match MEvent::from_str_trim(&event_json) {
                    Ok(event) => {
                        landing.apply_remote(&event, host_id, handler_registry);
                        history_replay
                            .observe_committed(HistoryEntityKey::new(item_type, item_id), id);
                    }
                    Err(err) => {
                        error!("Invalid postgres event row id={id}: {err}");
                    }
                }
            }
            continue;
        }

        if !initial_done {
            initial_done = true;
            status.caught_up.store(true, Ordering::SeqCst);
            info!("Postgres consumer caught up at event_id={last_seen_id}");
        }

        let mut notified = false;
        let mut reconnect_listener = false;
        {
            let mut notifications = listener.notifications();
            let mut iter = notifications.timeout_iter(Duration::from_millis(500));
            match iter.next() {
                Ok(Some(_n)) => {
                    notified = true;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!("Postgres LISTEN error: {err}; reconnecting listener");
                    reconnect_listener = true;
                }
            }
        }
        if reconnect_listener {
            std::thread::sleep(Duration::from_millis(500));
            listener = connect_listener_client(config)?;
            // Listener dropped; run an immediate catch-up query pass before waiting again.
            continue;
        }

        if !notified {
            trace!("Postgres consumer poll tick (no notification)");
        }
    }
}

fn connect_consumer_client(role: &str, config: &PostgresConfig) -> Result<Client, String> {
    let mut backoff_ms = 250u64;
    loop {
        match connect_pg_client(config, role) {
            Ok(mut client) => {
                match ensure_schema(&mut client, config) {
                    Ok(()) => {}
                    Err(err @ SchemaError::Outdated { .. }) => return Err(err.to_string()),
                    Err(err) => warn!(
                        "{}",
                        err.describe(&format!("ensure_schema({role})"), &config.url)
                    ),
                }
                return Ok(client);
            }
            Err(err) => {
                warn!("{err}");
                std::thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms = backoff_ms.saturating_mul(2).min(5_000);
            }
        }
    }
}

fn connect_listener_client(config: &PostgresConfig) -> Result<Client, String> {
    let mut backoff_ms = 250u64;
    loop {
        let mut client = connect_consumer_client("listener", config)?;
        match client.batch_execute(&format!("LISTEN {};", qi(&config.channel))) {
            Ok(()) => return Ok(client),
            Err(err) => {
                warn!(
                    "{}",
                    format_pg_error("LISTEN(register)", Some(&config.url), &err)
                );
                std::thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms = backoff_ms.saturating_mul(2).min(5_000);
            }
        }
    }
}

fn apply_remote_event(
    event: &MEvent,
    host_id: &str,
    handler_registry: &Arc<HandlerRegistry>,
    registry: &Arc<StoreRegistry>,
) {
    registry.with_mutation(|| {
        apply_remote_event_coordinated(event, host_id, handler_registry, registry);
    });
}

fn apply_remote_event_coordinated(
    event: &MEvent,
    host_id: &str,
    handler_registry: &Arc<HandlerRegistry>,
    registry: &Arc<StoreRegistry>,
) {
    let is_my_event = event.source_id.as_deref().is_some_and(|id| id == host_id);
    if is_my_event {
        return;
    }

    match event.change_type {
        MEventType::SET => {
            if let Some(parse) = handler_registry.item_parser(&event.item_type) {
                match parse(event.item.clone()) {
                    Ok(item) => {
                        let store = registry.get_or_create(item.entity_type());
                        store.insert(item.id(), item);
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let short = msg
                            .find(", expected one of")
                            .and_then(|pos| msg.get(..pos).map(str::to_string))
                            .unwrap_or(msg);
                        error!("Failed to parse {}: {short}", event.item_type);
                    }
                }
            } else {
                warn!("No parser for entity type: {}", event.item_type);
            }
        }
        MEventType::DEL => {
            if let Some(id) = event.item.get("id").and_then(|v| v.as_str()) {
                let store = registry.get_or_create(&event.item_type);
                store.remove(&id.into());
            } else {
                error!("DEL event missing id field: {:?}", event.item);
            }
        }
    }
}

fn validate_ident(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("identifier cannot be empty".to_string());
    }
    let mut chars = name.chars();
    let first = chars
        .next()
        .ok_or_else(|| "identifier cannot be empty".to_string())?;
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(format!(
            "invalid identifier `{name}`: must start with letter or underscore"
        ));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(format!(
            "invalid identifier `{name}`: only letters, numbers, underscore are allowed"
        ));
    }
    Ok(())
}

fn qi(name: &str) -> String {
    format!("\"{name}\"")
}

fn row_to_persisted_event(row: ::postgres::Row) -> Result<PersistedEvent, String> {
    let id: i64 = row.get(0);
    let created_at: DateTime<Utc> = row.get(1);
    let event_json: String = row.get(2);
    let event = MEvent::from_str_trim(&event_json)
        .map_err(|e| format!("invalid history event payload for id={id}: {e}"))?;
    drop(row);
    Ok(PersistedEvent {
        id,
        created_at: history_timestamp_to_wire(created_at),
        event,
    })
}

fn history_timestamp_to_wire(created_at: DateTime<Utc>) -> String {
    created_at.to_rfc3339()
}

fn redact_pg_url(url: &str) -> String {
    url.rfind('@').map_or_else(
        || url.to_string(),
        |at| {
            let after_scheme = url.find("://").map_or(0, |idx| idx.saturating_add(3));
            format!(
                "{}***{}",
                url.get(..after_scheme).unwrap_or_default(),
                url.get(at..).unwrap_or_default()
            )
        },
    )
}

fn format_pg_connect_error(role: &str, url: &str, err: &postgres::Error) -> String {
    format_pg_error(role, Some(url), err)
}

fn format_pg_error(role: &str, url: Option<&str>, err: &postgres::Error) -> String {
    let mut msg = url.map_or_else(
        || format!("{role} failed: {err}"),
        |url| format!("{role} failed (dsn={}): {}", redact_pg_url(url), err),
    );
    if let Some(db) = err.as_db_error() {
        let _ = write!(
            msg,
            " [code={} severity={} message={}]",
            db.code().code(),
            db.severity(),
            db.message()
        );
        if let Some(detail) = db.detail() {
            let _ = write!(msg, " [detail={detail}]");
        }
        if let Some(hint) = db.hint() {
            let _ = write!(msg, " [hint={hint}]");
        }
    }
    msg
}

fn connect_pg_client(config: &PostgresConfig, role: &str) -> Result<Client, String> {
    let mut client_config = parse_pg_client_config(config, role)?;

    // Avoid default 2h keepalive-idle so long-lived idle sockets are detected quickly.
    client_config.connect_timeout(Duration::from_secs(PG_CONNECT_TIMEOUT_SECS));
    client_config.keepalives(true);
    client_config.keepalives_idle(Duration::from_secs(PG_KEEPALIVE_IDLE_SECS));
    client_config.keepalives_interval(Duration::from_secs(PG_KEEPALIVE_INTERVAL_SECS));
    client_config.keepalives_retries(PG_KEEPALIVE_RETRIES);
    // Reap a peer that died mid-write, which keepalives (idle-only) miss — see
    // PG_TCP_USER_TIMEOUT_SECS. Applies to every role; on a live connection the
    // data is acked normally so it never fires.
    client_config.tcp_user_timeout(Duration::from_secs(PG_TCP_USER_TIMEOUT_SECS));

    client_config
        .connect(NoTls)
        .map_err(|err| format_pg_connect_error(role, &config.url, &err))
}

fn parse_pg_client_config(config: &PostgresConfig, role: &str) -> Result<PgClientConfig, String> {
    config.url.parse::<PgClientConfig>().map_err(|err| {
        format!(
            "postgres config parse failed ({role}, dsn={}): {err}",
            redact_pg_url(&config.url)
        )
    })
}

#[cfg(test)]
mod tests {
    use myko::{hyphae::Gettable, server::HistoryReplayProvider};

    use super::*;

    #[test]
    fn snapshot_batches_preserve_all_entities_and_flush_the_tail() {
        use myko::{
            common::with_id::WithId,
            entities::{client::Client, server::ServerId},
            item::AnyItem,
        };

        let registry = StoreRegistry::new();
        let handlers = HandlerRegistry::new();
        let mut pending = SnapshotEntries::new();
        let mut expected = Vec::new();
        let mut entity_type = String::new();
        for index in 0..PG_SNAPSHOT_STORE_BATCH.saturating_add(3) {
            let client = Client {
                id: format!("snapshot-{index}").into(),
                server_id: ServerId::from(Arc::<str>::from("snapshot-server")),
                address: None,
                windback: None,
            };
            expected.push(client.id());
            entity_type = client.entity_type().to_string();
            let event = MEvent::from_item(&client, MEventType::SET, "snapshot-test");
            queue_snapshot_event(&event, "local-host", &handlers, &registry, &mut pending);
            let mut echoed_event = event;
            echoed_event.source_id = Some("local-host".into());
            queue_snapshot_event(
                &echoed_event,
                "local-host",
                &handlers,
                &registry,
                &mut pending,
            );
        }
        let store = registry.get_or_create(&entity_type);
        assert_eq!(store.items_snapshot().len(), PG_SNAPSHOT_STORE_BATCH);
        assert_eq!(pending.values().map(Vec::len).sum::<usize>(), 3);
        flush_snapshot_entries(&registry, pending);
        let mut actual: Vec<_> = store
            .items_snapshot()
            .iter()
            .map(|item| item.id())
            .collect();
        expected.sort();
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn committed_rows_flow_through_the_history_notification_cell() {
        let provider = PostgresHistoryReplayProvider::new(PostgresConfig {
            url: "postgres://test".to_string(),
            table: "events".to_string(),
            channel: "events_notify".to_string(),
        });
        let committed = provider.committed_history_event();
        let key = HistoryEntityKey::new("TestItem", "item-1");

        assert_eq!(committed.get(), None);
        provider.observe_committed(key.clone(), 42);
        assert_eq!(
            committed.get(),
            Some(Arc::new(CommittedHistoryEvent { key, row_id: 42 }))
        );
    }

    #[test]
    fn postgres_timestamps_leave_the_adapter_as_rfc3339() {
        let timestamp = DateTime::from_timestamp(1_777_777_777, 123_456_000).unwrap_or_default();
        let wire_timestamp = history_timestamp_to_wire(timestamp);

        assert!(DateTime::parse_from_rfc3339(&wire_timestamp).is_ok());
        assert_eq!(wire_timestamp, "2026-05-03T03:09:37.123456+00:00");
    }

    #[test]
    fn postgres_catch_up_timeout_defaults_to_five_minutes() {
        assert_eq!(
            parse_postgres_catch_up_timeout(None),
            Ok(Some(Duration::from_mins(5)))
        );
    }

    #[test]
    fn postgres_catch_up_timeout_accepts_positive_seconds() {
        assert_eq!(
            parse_postgres_catch_up_timeout(Some("900")),
            Ok(Some(Duration::from_mins(15)))
        );
    }

    #[test]
    fn postgres_catch_up_timeout_accepts_zero_as_no_limit() {
        assert_eq!(parse_postgres_catch_up_timeout(Some("0")), Ok(None));
    }

    #[test]
    fn postgres_catch_up_timeout_rejects_negative_and_invalid_values() {
        assert!(parse_postgres_catch_up_timeout(Some("-1")).is_err());
        assert!(parse_postgres_catch_up_timeout(Some("forever")).is_err());
    }
}

#[cfg(test)]
mod provenance_tests {
    use super::*;
    use myko::server::OperationProvenance;

    #[test]
    #[ignore = "requires an isolated MYKO_PROVENANCE_TEST_POSTGRES database"]
    #[allow(clippy::too_many_lines)]
    fn command_transaction_survives_cascades_and_durable_history() -> anyhow::Result<()> {
        use myko::{
            command::CommandContext,
            entities::{
                client::Client as ConnectedClient,
                server::{Server, ServerId},
            },
            prelude::EventPublishing,
            request::RequestContext,
            search::SearchIndex,
            server::{
                MykoServerContext, MykoServerRuntime, RelationshipManager,
                persister::PersisterRouter,
            },
        };

        let config = PostgresConfig {
            url: std::env::var("MYKO_PROVENANCE_TEST_POSTGRES")?,
            table: "transaction_test_events".into(),
            channel: "transaction_test_events".into(),
        };
        let mut client = Client::connect(&config.url, NoTls)?;
        ensure_schema(&mut client, &config)?;
        client.batch_execute("TRUNCATE transaction_test_events RESTART IDENTITY")?;
        let host_id = Uuid::new_v4();
        let producer = PostgresProducer::new(&config, host_id).map_err(anyhow::Error::msg)?;
        let mut router = PersisterRouter::default();
        router.set_default(Some(Arc::new(producer.handle())));
        let server = Arc::new(MykoServerContext::new(
            host_id,
            Arc::new(StoreRegistry::new()),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(router),
            Arc::new(SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink: None,
                history_replay: None,
            },
        ));
        let request = Arc::new(RequestContext::from_client(
            "originating-command-tx".into(),
            "connection".into(),
            host_id,
        ));
        let ctx = CommandContext::new("EditTree".into(), request, server);
        let parent = Server {
            id: "parent".into(),
            version: "test".into(),
            address: "test".into(),
            port: 0,
            started_at: "test".into(),
        };
        ctx.emit_set(&parent)
            .map_err(|error| anyhow::anyhow!(error.message))?;
        ctx.emit_set(&ConnectedClient {
            id: "child".into(),
            server_id: ServerId::from(Arc::<str>::from("parent")),
            address: None,
            windback: None,
        })
        .map_err(|error| anyhow::anyhow!(error.message))?;
        ctx.emit_del(&parent)
            .map_err(|error| anyhow::anyhow!(error.message))?;

        let history = PostgresHistoryStore::new(config).map_err(anyhow::Error::msg)?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let records = loop {
            let records = history
                .load_records_after_id(0, 10)
                .map_err(anyhow::Error::msg)?;
            if records.len() == 4 {
                break records;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "command events were not persisted"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        anyhow::ensure!(
            records
                .iter()
                .all(|(_, record)| record.event.tx.as_ref() == "originating-command-tx")
        );
        anyhow::ensure!(
            records
                .iter()
                .any(|(_, record)| record.event.change_type == MEventType::DEL
                    && record.event.item_type.as_ref() == "Client")
        );
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM transaction_test_events WHERE tx = $1",
                &[&"originating-command-tx"],
            )?
            .get(0);
        anyhow::ensure!(
            count == 4,
            "the indexed transaction must group every command and cascade event"
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires an isolated MYKO_PROVENANCE_TEST_POSTGRES database"]
    fn durable_envelopes_survive_single_batch_and_background_producer() -> anyhow::Result<()> {
        let url = std::env::var("MYKO_PROVENANCE_TEST_POSTGRES")?;
        let config = PostgresConfig {
            url: url.clone(),
            table: "provenance_test_events".into(),
            channel: "provenance_test_events".into(),
        };
        let mut client = Client::connect(&url, NoTls)?;
        ensure_schema(&mut client, &config)?;
        client.batch_execute("TRUNCATE provenance_test_events RESTART IDENTITY")?;
        let make = |id: &str| -> anyhow::Result<EventRecord> {
            Ok(EventRecord {
                event: serde_json::from_value(serde_json::json!({
                    "item": {"id": id}, "itemType": "TestEntity", "changeType": "SET",
                    "createdAt": "2026-11-05T00:00:00Z", "tx": "legacy-event-tx", "sourceId": null
                }))?,
                provenance: Some(OperationProvenance {
                    request_tx: "untrusted-request-label".into(),
                    actor_user_id: None,
                    undo_of_tx: None,
                    unverified_client_id: Some("connection-only".into()),
                    command_id: "EditTree".into(),
                    lineage: vec!["client".into()],
                }),
            })
        };
        let single = make("single")?;
        insert_event_batch(&mut client, &config, std::slice::from_ref(&single))?;
        let batch = vec![
            make("batch-one")?,
            EventRecord {
                event: make("legacy")?.event,
                provenance: None,
            },
        ];
        insert_event_batch(&mut client, &config, &batch)?;
        let producer =
            PostgresProducer::new(&config, Uuid::new_v4()).map_err(anyhow::Error::msg)?;
        let handle = producer.handle();
        anyhow::ensure!(handle.supports_provenance());
        let mut invalid = make("invalid")?;
        if let Some(provenance) = &mut invalid.provenance {
            provenance.request_tx = "untrusted\0label".into();
        }
        anyhow::ensure!(
            handle.persist_record(invalid).is_err(),
            "invalid metadata must fail before enqueue, without poisoning subsequent writes"
        );
        handle.persist_record(make("worker")?)?;
        let history = PostgresHistoryStore::new(config.clone()).map_err(anyhow::Error::msg)?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let rows = loop {
            let rows = history
                .load_records_after_id(0, 10)
                .map_err(anyhow::Error::msg)?;
            if rows.len() == 4 {
                break rows;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "producer did not persist the record"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        anyhow::ensure!(rows.first().map(|(_, record)| record) == Some(&single));
        anyhow::ensure!(rows.iter().any(|(_, record)| {
            record
                .event
                .item
                .get("id")
                .and_then(serde_json::Value::as_str)
                == Some("legacy")
                && record.provenance.is_none()
        }));
        anyhow::ensure!(rows.iter().any(|(_, record)| {
            record
                .event
                .item
                .get("id")
                .and_then(serde_json::Value::as_str)
                == Some("worker")
                && record.provenance == single.provenance
                && record.event.source_id.is_some()
        }));
        let old_rows = history.load_after_id(0, 10).map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            old_rows.len() == rows.len(),
            "legacy history reader still accepts envelopes"
        );
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM provenance_test_events WHERE tx = 'legacy-event-tx'",
                &[],
            )?
            .get(0);
        anyhow::ensure!(count == 4, "indexed tx column agrees with envelope");
        Ok(())
    }
}
