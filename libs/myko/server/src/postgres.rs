//! PostgreSQL producer and consumer for the cell-based server.
//!
//! Architecture:
//! - Producer persists `MEvent` rows into a durable table.
//! - Consumer replays table rows (catch-up), then follows new inserts via LISTEN/NOTIFY.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ::postgres::{Client, Config as PgClientConfig, NoTls};
use myko::{
    event::{MEvent, MEventType},
    server::{HandlerRegistry, PersistError, PersistHealth, Persister},
    store::StoreRegistry,
};
use postgres::fallible_iterator::FallibleIterator;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

const PG_CONNECT_TIMEOUT_SECS: u64 = 10;
const PG_KEEPALIVE_IDLE_SECS: u64 = 30;
const PG_KEEPALIVE_INTERVAL_SECS: u64 = 10;
const PG_KEEPALIVE_RETRIES: u32 = 3;
/// TCP_USER_TIMEOUT: tear down a connection whose sent data stays unacked for
/// this long. Unlike keepalives — which only probe an *idle* socket — this
/// fires on a peer that died mid-write, so a consumer killed mid-snapshot (OOM)
/// has its Postgres backend reaped promptly instead of lingering in ClientWrite
/// holding the events-table lock and stalling every subsequent boot's snapshot
/// until the catch-up timeout (the lv-8c2f "boot convoy").
const PG_TCP_USER_TIMEOUT_SECS: u64 = 30;
const PG_PRODUCER_MAX_BATCH: usize = 256;

/// PostgreSQL configuration.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL.
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
    /// Create config from environment.
    ///
    /// - `MYKO_POSTGRES_URL` (required)
    /// - `MYKO_POSTGRES_TABLE` (optional, default `myko_events`)
    /// - `MYKO_POSTGRES_CHANNEL` (optional, default `myko_events_notify`)
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("MYKO_POSTGRES_URL").ok()?;
        let table = std::env::var("MYKO_POSTGRES_TABLE").unwrap_or_else(|_| "myko_events".into());
        let channel =
            std::env::var("MYKO_POSTGRES_CHANNEL").unwrap_or_else(|_| "myko_events_notify".into());
        Some(Self {
            url,
            table,
            channel,
        })
    }
}

/// Read API for durable event history (windback/replay providers).
pub struct PostgresHistoryStore {
    config: PostgresConfig,
}

impl PostgresHistoryStore {
    /// Create a history store and ensure schema exists.
    pub fn new(config: PostgresConfig) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;
        Ok(Self { config })
    }

    /// Read events with `id > after_id`, ascending.
    pub fn load_after_id(&self, after_id: i64, limit: i64) -> Result<Vec<PersistedEvent>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_after_id)")?;
        let sql = format!(
            "SELECT id, created_at::text, event::text FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
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
            return Err(format!("Invalid timestamp format: {}", until));
        }
        let sql = format!(
            "SELECT id, created_at::text, event::text FROM {} WHERE id > {} AND created_at <= '{}'::timestamptz ORDER BY id ASC LIMIT {}",
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
    pub fn load_between(
        &self,
        from_iso: &str,
        to_iso: &str,
        limit: i64,
    ) -> Result<Vec<PersistedEvent>, String> {
        let mut client = connect_pg_client(&self.config, "history(load_between)")?;
        let sql = format!(
            "SELECT id, created_at::text, event::text FROM {} WHERE created_at >= $1::timestamptz AND created_at <= $2::timestamptz ORDER BY id ASC LIMIT $3",
            qi(&self.config.table)
        );
        let rows = client
            .query(&sql, &[&from_iso, &to_iso, &limit])
            .map_err(|e| format!("history query failed: {e}"))?;
        rows.into_iter()
            .map(row_to_persisted_event)
            .collect::<Result<Vec<_>, _>>()
    }
}

/// HistoryReplayProvider backed by PostgresHistoryStore.
pub struct PostgresHistoryReplayProvider {
    config: PostgresConfig,
}

impl PostgresHistoryReplayProvider {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl myko::server::HistoryReplayProvider for PostgresHistoryReplayProvider {
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
            return Err(format!("Invalid timestamp format: {}", until));
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
                        count += 1;
                    }
                }
                Err(err) => {
                    eprintln!("[HistoryReplay] invalid event row: {}", err);
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

type ProducerRequest = MEvent;

/// Handle to the PostgreSQL producer.
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
    pub fn produce(&self, mut event: MEvent) -> Result<(), PersistError> {
        if event.source_id.is_none() {
            event.source_id = Some(std::sync::Arc::from(self.host_id.to_string()));
        }
        let entity_type = event.item_type.clone();

        match self.sender.send(event) {
            Ok(()) => {
                self.health.record_enqueue();
                Ok(())
            }
            Err(_) => {
                let msg = "Postgres producer thread not running".to_string();
                self.health.record_dropped(msg.clone());
                Err(PersistError {
                    entity_type,
                    message: msg,
                })
            }
        }
    }
}

impl Persister for PostgresProducerHandle {
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

/// PostgreSQL producer — background thread with fail-fast error propagation.
pub struct PostgresProducer {
    handle: PostgresProducerHandle,
}

impl PostgresProducer {
    /// Create a new PostgreSQL producer.
    pub fn new(config: &PostgresConfig, host_id: Uuid) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;

        let health = Arc::new(PersistHealth::default());
        let (tx, rx) = flume::unbounded::<ProducerRequest>();
        let cfg = config.clone();
        let thread_health = health.clone();
        std::thread::spawn(move || run_producer_loop(cfg, rx, thread_health));

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
    pub fn handle(&self) -> PostgresProducerHandle {
        self.handle.clone()
    }
}

fn run_producer_loop(
    config: PostgresConfig,
    rx: flume::Receiver<ProducerRequest>,
    health: Arc<PersistHealth>,
) {
    let mut client: Option<Client> = None;
    let mut retry_batch: Vec<MEvent> = Vec::new();

    loop {
        // NOTE(ts): Collect a batch — start with any retry events, then drain the channel.
        let mut batch: Vec<MEvent> = Vec::new();
        if !retry_batch.is_empty() {
            std::mem::swap(&mut batch, &mut retry_batch);
        } else {
            match rx.recv() {
                Ok(ev) => batch.push(ev),
                Err(_) => break, // channel closed
            }
        }
        // NOTE(ts): Drain additional ready events up to the batch limit.
        while batch.len() < PG_PRODUCER_MAX_BATCH {
            match rx.try_recv() {
                Ok(ev) => batch.push(ev),
                Err(_) => break,
            }
        }

        if client.is_none() {
            client = connect_producer_client(&config);
        }

        let batch_len = batch.len();
        let pending = rx.len();
        if pending > 1000 {
            trace!(
                "Postgres producer backlog: {} pending events in channel",
                pending
            );
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            let mut counts: std::collections::BTreeMap<(&str, &str), usize> =
                std::collections::BTreeMap::new();
            for ev in &batch {
                let kind = match ev.change_type {
                    MEventType::SET => "SET",
                    MEventType::DEL => "DEL",
                };
                *counts.entry((&*ev.item_type, kind)).or_insert(0) += 1;
            }
            let summary: Vec<String> = counts
                .iter()
                .map(|((t, k), n)| format!("{}:{}={}", t, k, n))
                .collect();
            debug!(
                "[pg-producer] batch_len={} pending_after={} kinds=[{}]",
                batch_len,
                pending,
                summary.join(", ")
            );
        }
        if let Some(c) = client.as_mut() {
            match insert_event_batch(c, &config, &batch) {
                Ok(()) => {
                    health.record_success_batch(batch_len as u64);
                }
                Err(err) => {
                    let msg =
                        format_pg_error("insert_event_batch(producer)", Some(&config.url), &err);
                    error!("{}", msg);
                    health.record_error_no_dequeue(msg);
                    client = None;
                    retry_batch = batch;
                    // NOTE(ts): Back off before retrying to avoid tight-looping on persistent failures.
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        } else {
            let msg = format!(
                "Postgres producer connection failed (url: {})",
                redact_pg_url(&config.url),
            );
            error!("{}", msg);
            health.record_error_no_dequeue(msg);
            retry_batch = batch;
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

fn connect_producer_client(config: &PostgresConfig) -> Option<Client> {
    match connect_pg_client(config, "producer") {
        Ok(mut c) => match ensure_schema(&mut c, config) {
            Ok(()) => Some(c),
            Err(err) => {
                error!(
                    "{}",
                    format_pg_error("ensure_schema(producer)", Some(&config.url), &err)
                );
                None
            }
        },
        Err(err) => {
            error!("{err}");
            None
        }
    }
}

fn insert_event_batch(
    client: &mut Client,
    config: &PostgresConfig,
    events: &[MEvent],
) -> Result<(), ::postgres::Error> {
    if events.is_empty() {
        return Ok(());
    }

    let table = qi(&config.table);

    // NOTE(ts): Single event — skip transaction overhead.
    if events.len() == 1 {
        let event = &events[0];
        let sql = format!(
            "INSERT INTO {table} (item_type, item_id, change_type, created_at, tx, source_id, event) VALUES ($1, $2, $3, ($4::text)::timestamptz, $5, ($6::text), ($7::text)::jsonb)"
        );
        let item_id = event
            .item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let event_json = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
        let change_type = match event.change_type {
            MEventType::SET => "SET",
            MEventType::DEL => "DEL",
        };
        let item_type: &str = &event.item_type;
        let created_at: &str = &event.created_at;
        let tx: &str = &event.tx;
        let source_id: Option<&str> = event.source_id.as_deref();
        client.execute(
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
        return Ok(());
    }

    // NOTE(ts): Build a multi-row INSERT for the batch within a single transaction.
    let mut sql = format!(
        "INSERT INTO {table} (item_type, item_id, change_type, created_at, tx, source_id, event) VALUES "
    );
    let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> =
        Vec::with_capacity(events.len() * 7);
    for (i, event) in events.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let base = i * 7;
        sql.push_str(&format!(
            "(${}, ${}, ${}, (${}::text)::timestamptz, ${}, (${}::text), (${}::text)::jsonb)",
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
            base + 6,
            base + 7
        ));
        let item_id = event
            .item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let event_json = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
        let change_type = match event.change_type {
            MEventType::SET => "SET",
            MEventType::DEL => "DEL",
        };
        params.push(Box::new(event.item_type.to_string()));
        params.push(Box::new(item_id));
        params.push(Box::new(change_type.to_string()));
        params.push(Box::new(event.created_at.to_string()));
        params.push(Box::new(event.tx.to_string()));
        params.push(Box::new(event.source_id.as_ref().map(|s| s.to_string())));
        params.push(Box::new(event_json));
    }

    let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
        params.iter().map(|p| p.as_ref()).collect();

    let mut txn = client.transaction()?;
    txn.execute(&sql, &param_refs)?;
    txn.commit()?;

    Ok(())
}

/// Shared status for startup catch-up.
#[derive(Debug)]
pub struct CatchUpStatus {
    caught_up: AtomicBool,
    failed: AtomicBool,
    failure_reason: std::sync::RwLock<Option<String>>,
}

impl CatchUpStatus {
    fn new() -> Self {
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
        *self.failure_reason.write().unwrap() = Some(reason.clone());
        self.failed.store(true, Ordering::SeqCst);
        self.caught_up.store(false, Ordering::SeqCst);
    }

    /// Block until caught up or timeout.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> Result<(), String> {
        let start = std::time::Instant::now();
        while !self.is_caught_up() {
            if self.is_failed() {
                return Err(self
                    .failure_reason
                    .read()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "Postgres catch-up failed".to_string()));
            }
            if start.elapsed() >= timeout {
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

/// PostgreSQL consumer that replays + tails events.
pub struct PostgresConsumer {
    catch_up_status: Arc<CatchUpStatus>,
    _handle: std::thread::JoinHandle<()>,
}

impl PostgresConsumer {
    /// Start a PostgreSQL event consumer thread.
    pub fn start(
        config: &PostgresConfig,
        host_id: Uuid,
        handler_registry: Arc<HandlerRegistry>,
        registry: Arc<StoreRegistry>,
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
                handler_registry,
                registry,
                status.clone(),
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
    pub fn is_caught_up(&self) -> bool {
        self.catch_up_status.is_caught_up()
    }

    /// Wait for startup catch-up with timeout.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> Result<(), String> {
        self.catch_up_status.wait_until_caught_up(timeout)
    }
}

fn run_consumer_loop(
    config: &PostgresConfig,
    host_id: &str,
    handler_registry: Arc<HandlerRegistry>,
    registry: Arc<StoreRegistry>,
    status: Arc<CatchUpStatus>,
) -> Result<(), String> {
    let mut reader = connect_consumer_client("reader", config)?;
    let mut listener = connect_listener_client(config)?;

    info!(
        "PostgresConsumer started (table={}, channel={})",
        config.table, config.channel
    );

    let table = qi(&config.table);
    let high_water_sql = format!("SELECT COALESCE(MAX(id), 0) FROM {table}");
    let high_water_row = reader
        .query_one(&high_water_sql, &[])
        .map_err(|e| format_pg_error("query(high_water)", Some(&config.url), &e))?;
    let high_water: i64 = high_water_row.get(0);
    // `latest` is the newest event per item; the outer `change_type = 'SET'`
    // then drops items whose latest event is a DEL — i.e. deleted items, whose
    // tombstone row would otherwise be fetched and applied only to no-op in
    // apply_remote_event (~48% of rows on a long-lived table). The filter MUST
    // be on the outer query, after DISTINCT ON has picked each item's latest
    // row: filtering DEL inside the CTE would instead take a deleted item's
    // latest *non-DEL* row and wrongly resurrect it. Mirrors the history-replay
    // snapshot query above.
    let snapshot_sql = format!(
        "
        WITH latest AS (
            SELECT DISTINCT ON (item_type, item_id)
                id, change_type
            FROM {table}
            WHERE id <= $1
            ORDER BY item_type, item_id, id DESC
        )
        SELECT e.id, e.event::text
        FROM latest
        JOIN {table} e ON e.id = latest.id
        WHERE latest.change_type = 'SET'
        ORDER BY e.id ASC
        "
    );
    // Stream the snapshot with `query_raw` (server-side portal, fetched in
    // batches) and apply each event as it arrives, rather than collecting the
    // entire latest-per-item result into one `Vec<Row>` first — boot memory was
    // proportional to (distinct live items) x (event JSON size), a multi-GiB
    // spike on large tables. The RowIter borrows `reader` for the duration;
    // apply_remote_event only touches the registries, never `reader`.
    // Scoped so the RowIter's mutable borrow of `reader` is released before the
    // tail catch-up loop below reuses (and reconnects) `reader`.
    let snapshot_count: usize = {
        let mut snapshot_rows = reader
            .query_raw(
                &snapshot_sql,
                [&high_water as &(dyn postgres::types::ToSql + Sync)],
            )
            .map_err(|e| format_pg_error("query(snapshot latest events)", Some(&config.url), &e))?;
        let mut count: usize = 0;
        while let Some(row) = snapshot_rows
            .next()
            .map_err(|e| format_pg_error("stream(snapshot latest events)", Some(&config.url), &e))?
        {
            let id: i64 = row.get(0);
            let event_json: String = row.get(1);
            match MEvent::from_str_trim(&event_json) {
                Ok(event) => {
                    apply_remote_event(event, host_id, &handler_registry, &registry);
                }
                Err(err) => {
                    error!("Invalid postgres snapshot row id={id}: {err}");
                }
            }
            count += 1;
        }
        count
    };
    info!(
        "Postgres snapshot loaded latest state rows={} high_water={}",
        snapshot_count, high_water
    );

    let fetch_sql =
        format!("SELECT id, event::text FROM {table} WHERE id > $1 ORDER BY id ASC LIMIT $2");
    let mut last_seen_id: i64 = high_water;
    let mut initial_done = false;

    loop {
        let rows = match reader.query(&fetch_sql, &[&last_seen_id, &1000_i64]) {
            Ok(rows) => rows,
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
                let event_json: String = row.get(1);
                last_seen_id = id;

                match MEvent::from_str_trim(&event_json) {
                    Ok(event) => {
                        apply_remote_event(event, host_id, &handler_registry, &registry);
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
                if let Err(err) = ensure_schema(&mut client, config) {
                    warn!(
                        "{}",
                        format_pg_error(&format!("ensure_schema({role})"), Some(&config.url), &err)
                    );
                }
                return Ok(client);
            }
            Err(err) => {
                warn!("{err}");
                std::thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms = (backoff_ms * 2).min(5_000);
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
                backoff_ms = (backoff_ms * 2).min(5_000);
            }
        }
    }
}

fn apply_remote_event(
    event: MEvent,
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
                            .map(|pos| msg[..pos].to_string())
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

fn ensure_schema(client: &mut Client, config: &PostgresConfig) -> Result<(), ::postgres::Error> {
    let table = qi(&config.table);
    let idx_tx = qi(&format!("{}_tx_idx", config.table));
    let idx_item = qi(&format!("{}_item_type_item_id_idx", config.table));
    let idx_item_latest = qi(&format!("{}_item_latest_idx", config.table));
    let idx_created = qi(&format!("{}_created_at_idx", config.table));
    let trigger_fn = qi(&format!("{}_notify_insert_fn", config.table));
    let trigger_name = qi(&format!("{}_notify_insert_trigger", config.table));

    client.batch_execute(&format!(
        "
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            item_type TEXT NOT NULL,
            item_id TEXT NOT NULL,
            change_type TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            tx TEXT NOT NULL,
            source_id TEXT,
            event JSONB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {idx_tx} ON {table} (tx);
        CREATE INDEX IF NOT EXISTS {idx_item} ON {table} (item_type, item_id);
        CREATE INDEX IF NOT EXISTS {idx_item_latest} ON {table} (item_type, item_id, id DESC);
        CREATE INDEX IF NOT EXISTS {idx_created} ON {table} (created_at);
        CREATE OR REPLACE FUNCTION {trigger_fn}() RETURNS trigger AS $$
        BEGIN
            PERFORM pg_notify('{channel}', NEW.id::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        DROP TRIGGER IF EXISTS {trigger_name} ON {table};
        CREATE TRIGGER {trigger_name}
            AFTER INSERT ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION {trigger_fn}();
        ",
        channel = config.channel
    ))?;

    Ok(())
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
    let created_at: String = row.get(1);
    let event_json: String = row.get(2);
    let event = MEvent::from_str_trim(&event_json)
        .map_err(|e| format!("invalid history event payload for id={id}: {e}"))?;
    Ok(PersistedEvent {
        id,
        created_at,
        event,
    })
}

fn redact_pg_url(url: &str) -> String {
    match url.rfind('@') {
        Some(at) => {
            let after_scheme = url.find("://").map(|idx| idx + 3).unwrap_or(0);
            format!("{}***{}", &url[..after_scheme], &url[at..])
        }
        None => url.to_string(),
    }
}

fn format_pg_connect_error(role: &str, url: &str, err: &postgres::Error) -> String {
    format_pg_error(role, Some(url), err)
}

fn format_pg_error(role: &str, url: Option<&str>, err: &postgres::Error) -> String {
    let mut msg = match url {
        Some(url) => format!("{role} failed (dsn={}): {}", redact_pg_url(url), err),
        None => format!("{role} failed: {err}"),
    };
    if let Some(db) = err.as_db_error() {
        msg.push_str(&format!(
            " [code={} severity={} message={}]",
            db.code().code(),
            db.severity(),
            db.message()
        ));
        if let Some(detail) = db.detail() {
            msg.push_str(&format!(" [detail={}]", detail));
        }
        if let Some(hint) = db.hint() {
            msg.push_str(&format!(" [hint={}]", hint));
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
