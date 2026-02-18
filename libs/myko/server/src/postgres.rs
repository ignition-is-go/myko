//! PostgreSQL producer and consumer for the cell-based server.
//!
//! Architecture mirrors Kafka:
//! - Producer persists `MEvent` rows into a durable table.
//! - Consumer replays table rows (catch-up), then follows new inserts via LISTEN/NOTIFY.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, TrySendError},
    },
    time::Duration,
};

use ::postgres::{Client, NoTls};
use log::{error, info, trace, warn};
use myko_rs::{
    event::{MEvent, MEventType},
    server::{HandlerRegistry, Persister},
    store::StoreRegistry,
};
use postgres::fallible_iterator::FallibleIterator;
use uuid::Uuid;

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
        let mut client = Client::connect(&self.config.url, NoTls)
            .map_err(|e| format!("postgres connect failed: {e}"))?;
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

    /// Read events in a time window, ascending.
    pub fn load_between(
        &self,
        from_iso: &str,
        to_iso: &str,
        limit: i64,
    ) -> Result<Vec<PersistedEvent>, String> {
        let mut client = Client::connect(&self.config.url, NoTls)
            .map_err(|e| format!("postgres connect failed: {e}"))?;
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

/// Handle to the PostgreSQL producer.
#[derive(Clone)]
pub struct PostgresProducerHandle {
    sender: mpsc::SyncSender<MEvent>,
    host_id: Uuid,
    config: PostgresConfig,
}

impl PostgresProducerHandle {
    /// Persist an event row.
    pub fn produce(&self, mut event: MEvent) {
        if event.source_id.is_none() {
            event.source_id = Some(self.host_id.to_string());
        }
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                error!("Postgres producer buffer full (50k), dropping event");
            }
            Err(TrySendError::Disconnected(_)) => {
                error!(
                    "Postgres producer channel closed, dropping event (producer thread not running)"
                );
            }
        }
    }
}

impl Persister for PostgresProducerHandle {
    fn persist(&self, event: MEvent) {
        self.produce(event);
    }

    fn startup_healthcheck(&self) -> Result<(), String> {
        validate_ident(&self.config.table)?;
        validate_ident(&self.config.channel)?;
        Ok(())
    }
}

/// PostgreSQL producer owning the background thread.
pub struct CellPostgresProducer {
    handle: PostgresProducerHandle,
}

impl CellPostgresProducer {
    /// Create a new PostgreSQL producer.
    pub fn new(config: &PostgresConfig, host_id: Uuid) -> Result<Self, String> {
        validate_ident(&config.table)?;
        validate_ident(&config.channel)?;

        let (tx, rx) = mpsc::sync_channel::<MEvent>(50_000);
        let cfg = config.clone();
        let cfg_for_handle = cfg.clone();
        std::thread::spawn(move || run_producer_loop(cfg, rx));

        Ok(Self {
            handle: PostgresProducerHandle {
                sender: tx,
                host_id,
                config: cfg_for_handle,
            },
        })
    }

    /// Get a shareable persister handle.
    pub fn handle(&self) -> PostgresProducerHandle {
        self.handle.clone()
    }
}

fn run_producer_loop(config: PostgresConfig, rx: mpsc::Receiver<MEvent>) {
    let mut client: Option<Client> = None;

    while let Ok(event) = rx.recv() {
        if client.is_none() {
            client = connect_producer_client(&config);
            if client.is_none() {
                error!("Postgres producer unavailable; dropping event");
                continue;
            }
        }

        if let Some(c) = client.as_mut() {
            if let Err(err) = insert_event(c, &config, event.clone()) {
                error!(
                    "{}",
                    format_pg_error("insert_event(producer)", Some(&config.url), &err)
                );
                client = None;
            } else {
                continue;
            }
        }

        if client.is_none() {
            client = connect_producer_client(&config);
            if let Some(c) = client.as_mut() {
                if let Err(err) = insert_event(c, &config, event) {
                    error!(
                        "{}",
                        format_pg_error("insert_event(producer-retry)", Some(&config.url), &err)
                    );
                    client = None;
                }
            } else {
                error!("Postgres producer reconnect failed; dropping event");
            }
        }
    }
}

fn connect_producer_client(config: &PostgresConfig) -> Option<Client> {
    match Client::connect(&config.url, NoTls) {
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
            log_pg_connect_error("producer", &config.url, &err);
            None
        }
    }
}

fn insert_event(
    client: &mut Client,
    config: &PostgresConfig,
    event: MEvent,
) -> Result<(), ::postgres::Error> {
    let table = qi(&config.table);
    let sql = format!(
        "INSERT INTO {table} (item_type, item_id, change_type, created_at, tx, source_id, event) VALUES ($1, $2, $3, ($4::text)::timestamptz, $5, ($6::text), ($7::text)::jsonb)"
    );
    let item_id = event
        .item
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let event_json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
    let change_type = match event.change_type {
        MEventType::SET => "SET",
        MEventType::DEL => "DEL",
    };

    client.execute(
        &sql,
        &[
            &event.item_type,
            &item_id,
            &change_type,
            &event.created_at,
            &event.tx,
            &event.source_id,
            &event_json,
        ],
    )?;
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
pub struct CellPostgresConsumer {
    catch_up_status: Arc<CatchUpStatus>,
    _handle: std::thread::JoinHandle<()>,
}

impl CellPostgresConsumer {
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
    let mut reader = Client::connect(&config.url, NoTls)
        .map_err(|e| format_pg_connect_error("reader", &config.url, &e))?;
    ensure_schema(&mut reader, config)
        .map_err(|e| format_pg_error("ensure_schema(reader)", Some(&config.url), &e))?;

    let mut listener = Client::connect(&config.url, NoTls)
        .map_err(|e| format_pg_connect_error("listener", &config.url, &e))?;
    ensure_schema(&mut listener, config)
        .map_err(|e| format_pg_error("ensure_schema(listener)", Some(&config.url), &e))?;
    listener
        .batch_execute(&format!("LISTEN {};", qi(&config.channel)))
        .map_err(|e| format!("LISTEN failed: {e}"))?;

    info!(
        "CellPostgresConsumer started (table={}, channel={})",
        config.table, config.channel
    );

    let table = qi(&config.table);
    let fetch_sql =
        format!("SELECT id, event::text FROM {table} WHERE id > $1 ORDER BY id ASC LIMIT $2");
    let mut last_seen_id: i64 = 0;
    let mut initial_done = false;

    loop {
        let rows = reader
            .query(&fetch_sql, &[&last_seen_id, &1000_i64])
            .map_err(|e| format_pg_error("query(fetch events)", Some(&config.url), &e))?;
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
        let mut notifications = listener.notifications();
        let mut iter = notifications.timeout_iter(Duration::from_millis(500));
        match iter.next() {
            Ok(Some(_n)) => {
                notified = true;
            }
            Ok(None) => {}
            Err(err) => {
                warn!("Postgres LISTEN error: {err}");
            }
        }

        if !notified {
            trace!("Postgres consumer poll tick (no notification)");
        }
    }
}

fn apply_remote_event(
    event: MEvent,
    host_id: &str,
    handler_registry: &Arc<HandlerRegistry>,
    registry: &Arc<StoreRegistry>,
) {
    let is_my_event = event.source_id.as_ref().is_some_and(|id| id == host_id);
    if is_my_event {
        return;
    }

    match event.change_type {
        MEventType::SET => {
            if let Some(parse) = handler_registry.get_item_parser(&event.item_type) {
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

fn log_pg_connect_error(role: &str, url: &str, err: &postgres::Error) {
    error!("{}", format_pg_connect_error(role, url, err));
}
