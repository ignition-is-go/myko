//! Versioned, forward-only schema for the events table.
//!
//! Many servers, of different versions, share one events table. A booting server
//! therefore never changes shared schema on its own initiative: it verifies the
//! recorded migration version and fails when that is older than it needs. Only an
//! explicit runner (`MYKO_POSTGRES_MIGRATE=true` or [`migrate`]) changes schema, and
//! the single exception is bootstrapping a database where the events table does
//! not exist yet.

use std::{
    fmt,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

use ::postgres::{Client, GenericClient};
use tracing::{info, warn};

use super::{PostgresConfig, connect_pg_client, format_pg_error, qi, validate_ident};

/// Highest migration this binary knows about.
pub const SCHEMA_VERSION: i32 = 10;
/// Lowest recorded version this binary can run against.
///
/// A higher recorded version is fine: a newer server already migrated the shared table. Only the
/// baseline is required; migrations 2..=10 are performance work that can take hours on a large
/// log, so servers start while a runner is still applying them. Until migration 6 (the latest-table backfill) is recorded,
/// startup snapshots fall back to scanning the events table.
pub const REQUIRED_SCHEMA_VERSION: i32 = 1;
const LATEST_TABLE_BACKFILLED: i32 = 6;
const BACKFILL_BATCH_ITEMS: i32 = 10_000;

const APPLIED_BY: &str = concat!("myko-server ", env!("CARGO_PKG_VERSION"));

/// Why the events schema could not be prepared.
#[derive(Debug)]
pub enum SchemaError {
    Postgres(::postgres::Error),
    Outdated { found: i32, required: i32 },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postgres(error) => write!(f, "{error}"),
            Self::Outdated { found, required } => write!(
                f,
                "events schema is at migration version {found} but this server requires at least \
                 {required}; run the migrations (MYKO_POSTGRES_MIGRATE=true, or \
                 myko_server::postgres::migrate) before starting it"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

impl From<::postgres::Error> for SchemaError {
    fn from(error: ::postgres::Error) -> Self {
        Self::Postgres(error)
    }
}

impl SchemaError {
    pub(crate) fn describe(&self, role: &str, url: &str) -> String {
        match self {
            Self::Postgres(error) => format_pg_error(role, Some(url), error),
            Self::Outdated { .. } => format!("{role}: {self}"),
        }
    }
}

struct Names {
    table: String,
    completions: String,
    migrations: String,
    latest: String,
    channel: String,
    raw_table: String,
}

impl Names {
    fn new(config: &PostgresConfig) -> Self {
        Self {
            table: qi(&config.table),
            completions: qi(&format!("{}_transactions", config.table)),
            migrations: qi(&format!("{}_schema_migrations", config.table)),
            latest: qi(&format!("{}_latest", config.table)),
            channel: config.channel.clone(),
            raw_table: config.table.clone(),
        }
    }

    fn index(&self, suffix: &str) -> String {
        format!("{}_{suffix}", self.raw_table)
    }
}

enum Step {
    Sql(String),
    /// Built with `CREATE INDEX CONCURRENTLY` so inserts keep flowing.
    Index {
        name: String,
        columns: String,
    },
    DropIndex(String),
    BackfillLatest,
}

struct Migration {
    version: i32,
    name: &'static str,
    steps: Vec<Step>,
}

#[allow(clippy::too_many_lines)]
fn migrations(n: &Names) -> Vec<Migration> {
    let Names {
        table,
        completions,
        channel,
        ..
    } = n;
    let completed_user = qi(&n.index("completed_user_idx"));
    let trigger_fn = qi(&n.index("notify_insert_fn"));
    let trigger_name = n.index("notify_insert_trigger");
    let create_trigger = create_trigger_if_missing(table, &trigger_name, &trigger_fn);
    let latest = &n.latest;
    let latest_fn = qi(&n.index("latest_upsert_fn"));
    let create_latest_trigger =
        create_trigger_if_missing(table, &n.index("latest_upsert_trigger"), &latest_fn);
    let upsert = LATEST_UPSERT;
    vec![
        Migration {
            version: 1,
            name: "baseline tables and notify trigger",
            steps: vec![Step::Sql(format!(
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
                CREATE TABLE IF NOT EXISTS {completions} (
                    tx TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    first_id BIGINT NOT NULL,
                    last_id BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {completed_user} ON {completions} (user_id, first_id DESC);
                CREATE OR REPLACE FUNCTION {trigger_fn}() RETURNS trigger AS $$
                BEGIN
                    PERFORM pg_notify('{channel}', NEW.id::text);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                {create_trigger}
                "
            ))],
        },
        Migration {
            version: 2,
            name: "latest-event-per-item index",
            steps: vec![Step::Index {
                name: n.index("item_latest_idx"),
                columns: "(item_type, item_id, id DESC)".into(),
            }],
        },
        Migration {
            version: 3,
            name: "transaction index",
            steps: vec![Step::Index {
                name: n.index("tx_idx"),
                columns: "(tx)".into(),
            }],
        },
        Migration {
            version: 4,
            name: "append-only autovacuum settings",
            // NOTE(ts): Scale factors of 0 make vacuum/analyze trigger on a fixed row
            // count; the defaults scale with table size and never fire on a huge log.
            steps: vec![Step::Sql(format!(
                "ALTER TABLE {table} SET (
                    autovacuum_vacuum_insert_scale_factor = 0,
                    autovacuum_vacuum_insert_threshold = 1000000,
                    autovacuum_analyze_scale_factor = 0,
                    autovacuum_analyze_threshold = 1000000,
                    autovacuum_freeze_min_age = 0,
                    autovacuum_vacuum_cost_limit = 2000
                )"
            ))],
        },
        Migration {
            version: 5,
            name: "latest-state table maintained by trigger",
            // NOTE(ts): A trigger, not producer code, keeps this current, so servers built
            // before the table existed still maintain it when they append to a shared log.
            steps: vec![Step::Sql(format!(
                "
                CREATE TABLE IF NOT EXISTS {latest} (
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    last_id BIGINT NOT NULL,
                    change_type TEXT NOT NULL,
                    event JSONB NOT NULL,
                    PRIMARY KEY (item_type, item_id)
                );
                CREATE OR REPLACE FUNCTION {latest_fn}() RETURNS trigger
                SET search_path FROM CURRENT AS $$
                BEGIN
                    INSERT INTO {latest} AS l (item_type, item_id, last_id, change_type, event)
                    VALUES (NEW.item_type, NEW.item_id, NEW.id, NEW.change_type, NEW.event)
                    {upsert};
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                {create_latest_trigger}
                "
            ))],
        },
        Migration {
            version: LATEST_TABLE_BACKFILLED,
            name: "backfill latest-state table",
            steps: vec![Step::BackfillLatest],
        },
        Migration {
            version: 7,
            name: "undo history index",
            steps: vec![Step::Index {
                name: n.index("undo_history_idx"),
                columns: "((event->'item'->>'transactionId')) \
                          WHERE item_type = 'TransactionUndo' AND change_type = 'SET'"
                    .into(),
            }],
        },
        Migration {
            version: 8,
            name: "created_at BRIN index",
            steps: vec![Step::Index {
                name: n.index("created_at_brin_idx"),
                columns: "USING brin (created_at)".into(),
            }],
        },
        Migration {
            version: 9,
            name: "drop created_at btree superseded by BRIN",
            steps: vec![Step::DropIndex(n.index("created_at_idx"))],
        },
        Migration {
            version: 10,
            name: "drop item_type,item_id index covered by item_latest_idx",
            steps: vec![Step::DropIndex(n.index("item_type_item_id_idx"))],
        },
    ]
}

fn create_trigger_if_missing(table: &str, name: &str, function: &str) -> String {
    format!(
        "DO $do$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '{table}'::regclass AND tgname = '{name}') THEN
                CREATE TRIGGER {} AFTER INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {function}();
            END IF;
        END $do$;",
        qi(name)
    )
}

/// Conflict clause shared by the trigger and the backfill: the highest event id wins,
/// whatever order the writers commit in.
const LATEST_UPSERT: &str = "ON CONFLICT (item_type, item_id) DO UPDATE
    SET last_id = EXCLUDED.last_id, change_type = EXCLUDED.change_type, event = EXCLUDED.event
    WHERE EXCLUDED.last_id > l.last_id";

/// Bring the events schema to [`SCHEMA_VERSION`], connecting with `config`.
///
/// Index migrations run `CONCURRENTLY` and can take hours on a large table.
/// Concurrent runners serialize on an advisory lock; a waiting runner re-reads the
/// version and skips migrations the other runner applied.
///
/// # Errors
///
/// Returns an error when the configuration is invalid, `PostgreSQL` cannot be
/// reached, or a migration fails.
pub fn migrate(config: &PostgresConfig) -> Result<Vec<i32>, String> {
    validate_ident(&config.table)?;
    validate_ident(&config.channel)?;
    let mut client = connect_pg_client(config, "migrate")?;
    run_migrations(&mut client, config).map_err(|error| error.describe("migrate", &config.url))
}

/// Prepare a connection for use: migrate when asked, bootstrap an empty
/// database, otherwise verify the recorded version.
pub(super) fn ensure_schema(
    client: &mut Client,
    config: &PostgresConfig,
) -> Result<(), SchemaError> {
    if config.migrate || !relation_exists(client, &config.table)? {
        run_migrations(client, config)?;
        return Ok(());
    }
    let found = recorded_version(client, &config.table)?;
    if found < REQUIRED_SCHEMA_VERSION {
        return Err(SchemaError::Outdated {
            found,
            required: REQUIRED_SCHEMA_VERSION,
        });
    }
    Ok(())
}

fn run_migrations(client: &mut Client, config: &PostgresConfig) -> Result<Vec<i32>, SchemaError> {
    let lock_key = format!("myko_schema:{}", config.table);
    let acquired: bool = client
        .query_one("SELECT pg_try_advisory_lock(hashtext($1))", &[&lock_key])?
        .get(0);
    if !acquired {
        info!(
            "Waiting for another runner to finish {} migrations",
            config.table
        );
        client.execute("SELECT pg_advisory_lock(hashtext($1))", &[&lock_key])?;
    }
    let result = apply_pending(client, config);
    let unlocked = client.execute("SELECT pg_advisory_unlock(hashtext($1))", &[&lock_key]);
    let applied = result?;
    unlocked?;
    Ok(applied)
}

fn apply_pending(client: &mut Client, config: &PostgresConfig) -> Result<Vec<i32>, SchemaError> {
    let names = Names::new(config);
    client.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            applied_by TEXT NOT NULL
        )",
        names.migrations
    ))?;
    let current = recorded_version(client, &config.table)?;
    let mut applied = Vec::new();
    for migration in migrations(&names)
        .into_iter()
        .filter(|m| m.version > current)
    {
        let started = Instant::now();
        info!(
            "Applying {} migration {} ({})",
            config.table, migration.version, migration.name
        );
        for step in &migration.steps {
            apply_step(client, &names, step)?;
        }
        client.execute(
            &format!(
                "INSERT INTO {} (version, name, applied_by) VALUES ($1, $2, $3) ON CONFLICT (version) DO NOTHING",
                names.migrations
            ),
            &[&migration.version, &migration.name, &APPLIED_BY],
        )?;
        info!(
            "Applied {} migration {} in {:.1}s",
            config.table,
            migration.version,
            started.elapsed().as_secs_f64()
        );
        applied.push(migration.version);
    }
    Ok(applied)
}

// Each statement goes out on its own: several statements in one simple query form
// an implicit transaction, which CONCURRENTLY refuses.
fn apply_step(client: &mut Client, names: &Names, step: &Step) -> Result<(), ::postgres::Error> {
    match step {
        Step::Sql(sql) => client.batch_execute(sql),
        Step::BackfillLatest => backfill_latest(client, names, BACKFILL_BATCH_ITEMS),
        Step::DropIndex(name) => {
            client.batch_execute(&format!("DROP INDEX CONCURRENTLY IF EXISTS {}", qi(name)))
        }
        Step::Index { name, columns } => {
            match index_validity(client, name)? {
                Some(true) => return Ok(()),
                Some(false) => {
                    warn!("Dropping invalid index {name} left by an interrupted build");
                    client.batch_execute(&format!(
                        "DROP INDEX CONCURRENTLY IF EXISTS {}",
                        qi(name)
                    ))?;
                }
                None => {}
            }
            info!("Building index {name}; progress is visible in pg_stat_progress_create_index");
            client.batch_execute(&format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} {columns}",
                qi(name),
                names.table
            ))
        }
    }
}

// NOTE(ts): Postgres truncates identifiers to 63 bytes in DDL; lookups truncate the same way.
const IN_CURRENT_SCHEMA: &str =
    "c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())";

fn relation_exists(client: &mut impl GenericClient, name: &str) -> Result<bool, ::postgres::Error> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = left($1::text, 63) AND {IN_CURRENT_SCHEMA})"
    );
    Ok(client.query_one(&sql, &[&name])?.get(0))
}

fn index_validity(client: &mut Client, name: &str) -> Result<Option<bool>, ::postgres::Error> {
    let sql = format!(
        "SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relname = left($1::text, 63) AND {IN_CURRENT_SCHEMA}"
    );
    Ok(client.query_opt(&sql, &[&name])?.map(|row| row.get(0)))
}

fn recorded_version(
    client: &mut impl GenericClient,
    table: &str,
) -> Result<i32, ::postgres::Error> {
    let migrations = format!("{table}_schema_migrations");
    if !relation_exists(client, &migrations)? {
        return Ok(0);
    }
    let sql = format!("SELECT COALESCE(MAX(version), 0) FROM {}", qi(&migrations));
    Ok(client.query_one(&sql, &[])?.get(0))
}

/// Whether startup snapshots can read `{table}_latest` instead of scanning the log.
pub(super) fn latest_table_ready(
    client: &mut impl GenericClient,
    table: &str,
) -> Result<bool, ::postgres::Error> {
    Ok(recorded_version(client, table)? >= LATEST_TABLE_BACKFILLED)
}

/// Copy each item's latest event into `{table}_latest`, in batches that commit on their
/// own while inserts continue. The guarded upsert makes a rerun after interruption safe.
fn backfill_latest(
    client: &mut Client,
    names: &Names,
    batch_items: i32,
) -> Result<(), ::postgres::Error> {
    let Names { table, latest, .. } = names;
    let upsert = LATEST_UPSERT;
    let batch = |anchor: &str| {
        format!(
            "
            WITH RECURSIVE keys AS (
                (SELECT item_type, item_id, id, 1 AS n FROM {table} {anchor}
                 ORDER BY item_type, item_id, id DESC LIMIT 1)
                UNION ALL
                SELECT next.item_type, next.item_id, next.id, keys.n + 1
                FROM keys
                CROSS JOIN LATERAL (
                    SELECT m.item_type, m.item_id, m.id FROM {table} m
                    WHERE (m.item_type, m.item_id) > (keys.item_type, keys.item_id)
                    ORDER BY m.item_type, m.item_id, m.id DESC
                    LIMIT 1
                ) next
                WHERE keys.n < $1
            ), upserted AS (
                INSERT INTO {latest} AS l (item_type, item_id, last_id, change_type, event)
                SELECT e.item_type, e.item_id, e.id, e.change_type, e.event
                FROM keys JOIN {table} e ON e.id = keys.id
                {upsert}
            )
            SELECT item_type, item_id, n FROM keys ORDER BY n DESC LIMIT 1
            "
        )
    };
    let first = batch("");
    let rest = batch("WHERE (item_type, item_id) > ($2, $3)");
    let mut cursor: Option<(String, String)> = None;
    let mut items = 0_i64;
    loop {
        let row = match &cursor {
            None => client.query_opt(&first, &[&batch_items])?,
            Some((kind, id)) => client.query_opt(&rest, &[&batch_items, kind, id])?,
        };
        let Some(row) = row else { break };
        let n: i32 = row.get(2);
        items = items.saturating_add(i64::from(n));
        cursor = Some((row.get(0), row.get(1)));
        info!("Backfilled {items} items into {latest}");
        if n < batch_items {
            break;
        }
    }
    Ok(())
}

static SETTINGS_CHECKED: AtomicBool = AtomicBool::new(false);

/// Warn once per process about server settings that suit an append-only log
/// poorly. Read-only: the database node's sizing belongs to its deployment.
pub(super) fn check_server_settings(client: &mut Client) {
    if SETTINGS_CHECKED.swap(true, Ordering::SeqCst) {
        return;
    }
    let names: Vec<&str> = SETTING_CHECKS.iter().map(|check| check.name).collect();
    let rows = match client.query(
        "SELECT name, setting, COALESCE(unit, '') FROM pg_settings WHERE name = ANY($1)",
        &[&names],
    ) {
        Ok(rows) => rows,
        Err(error) => {
            warn!("Could not read pg_settings: {error}");
            return;
        }
    };
    let settings: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    let findings = settings_findings(&settings);
    if !findings.is_empty() {
        warn!(
            "Postgres settings are far from append-only event log recommendations (see the myko README): {}",
            findings.join("; ")
        );
    }
}

enum Expect {
    MinBytes(u64),
    MaxNumber(f64),
    MinNumber(f64),
    Not(&'static str),
}

struct SettingCheck {
    name: &'static str,
    expect: Expect,
    advice: &'static str,
}

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

const SETTING_CHECKS: &[SettingCheck] = &[
    SettingCheck {
        name: "shared_buffers",
        expect: Expect::MinBytes(GIB),
        advice: "about 25% of node RAM",
    },
    SettingCheck {
        name: "maintenance_work_mem",
        expect: Expect::MinBytes(256 * MIB),
        advice: "1-2GB",
    },
    SettingCheck {
        name: "max_wal_size",
        expect: Expect::MinBytes(2 * GIB),
        advice: "8GB or more",
    },
    SettingCheck {
        name: "random_page_cost",
        expect: Expect::MaxNumber(2.0),
        advice: "1.1 on SSD",
    },
    SettingCheck {
        name: "effective_io_concurrency",
        expect: Expect::MinNumber(16.0),
        advice: "200 on SSD",
    },
    SettingCheck {
        name: "wal_compression",
        expect: Expect::Not("off"),
        advice: "lz4",
    },
    SettingCheck {
        name: "autovacuum",
        expect: Expect::Not("off"),
        advice: "on",
    },
];

/// `(name, setting, unit)` rows from `pg_settings` to human-readable findings.
fn settings_findings(settings: &[(String, String, String)]) -> Vec<String> {
    let mut findings = Vec::new();
    for check in SETTING_CHECKS {
        let Some((_, setting, unit)) = settings.iter().find(|(name, ..)| name == check.name) else {
            continue;
        };
        let number = setting.parse::<f64>().ok();
        let off = match check.expect {
            Expect::MinBytes(min) => setting
                .parse::<u64>()
                .ok()
                .zip(unit_bytes(unit))
                .and_then(|(value, unit)| value.checked_mul(unit))
                .is_some_and(|bytes| bytes < min),
            Expect::MaxNumber(max) => number.is_some_and(|value| value > max),
            Expect::MinNumber(min) => number.is_some_and(|value| value < min),
            Expect::Not(bad) => setting.eq_ignore_ascii_case(bad),
        };
        if off {
            let shown = if unit.is_empty() {
                setting.clone()
            } else {
                format!("{setting}x{unit}")
            };
            findings.push(format!(
                "{}={shown} (recommend {})",
                check.name, check.advice
            ));
        }
    }
    findings
}

/// Bytes per `pg_settings.unit`, such as `8kB` or `MB`.
fn unit_bytes(unit: &str) -> Option<u64> {
    let split = unit
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(unit.len());
    let (count, suffix) = unit.split_at(split);
    let count: u64 = if count.is_empty() {
        1
    } else {
        count.parse().ok()?
    };
    let scale = match suffix {
        "B" => 1,
        "kB" => 1024,
        "MB" => MIB,
        "GB" => GIB,
        _ => return None,
    };
    count.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, setting: &str, unit: &str) -> (String, String, String) {
        (name.into(), setting.into(), unit.into())
    }

    #[test]
    fn migrations_are_numbered_contiguously_up_to_the_schema_version() {
        let config = PostgresConfig {
            url: String::new(),
            table: "t".into(),
            channel: "c".into(),
            migrate: false,
        };
        let versions: Vec<i32> = migrations(&Names::new(&config))
            .iter()
            .map(|m| m.version)
            .collect();
        assert_eq!(versions, (1..=SCHEMA_VERSION).collect::<Vec<_>>());
    }

    #[test]
    fn stock_defaults_are_flagged_and_tuned_values_pass() {
        let stock = [
            row("shared_buffers", "16384", "8kB"),
            row("maintenance_work_mem", "65536", "kB"),
            row("max_wal_size", "1024", "MB"),
            row("random_page_cost", "4", ""),
            row("effective_io_concurrency", "1", ""),
            row("wal_compression", "off", ""),
            row("autovacuum", "on", ""),
        ];
        let flagged: Vec<String> = settings_findings(&stock)
            .iter()
            .map(|f| f.split('=').next().unwrap_or_default().to_owned())
            .collect();
        assert_eq!(
            flagged,
            [
                "shared_buffers",
                "maintenance_work_mem",
                "max_wal_size",
                "random_page_cost",
                "effective_io_concurrency",
                "wal_compression"
            ]
        );
        let tuned = [
            row("shared_buffers", "2097152", "8kB"),
            row("maintenance_work_mem", "2097152", "kB"),
            row("max_wal_size", "8192", "MB"),
            row("random_page_cost", "1.1", ""),
            row("effective_io_concurrency", "200", ""),
            row("wal_compression", "lz4", ""),
            row("autovacuum", "on", ""),
        ];
        assert!(settings_findings(&tuned).is_empty());
    }

    #[test]
    fn pg_setting_units_resolve_to_bytes() {
        assert_eq!(unit_bytes("8kB"), Some(8192));
        assert_eq!(unit_bytes("MB"), Some(MIB));
        assert_eq!(unit_bytes("B"), Some(1));
        assert_eq!(unit_bytes("ms"), None);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod db_tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::postgres::{SnapshotSource, snapshot_sql};

    const THIS: &str = "Ours";
    const FOREIGN: &str = "Theirs";

    struct Fixture {
        config: PostgresConfig,
        client: Client,
    }

    impl Fixture {
        fn new() -> Self {
            let url = std::env::var("MYKO_HISTORY_TEST_URL").expect("dedicated test URL");
            let table = format!("schema_{}", uuid::Uuid::new_v4().simple());
            let config = PostgresConfig {
                url,
                channel: table.clone(),
                table,
                migrate: false,
            };
            let mut client = connect_pg_client(&config, "fixture").expect("fixture connection");
            ensure_schema(&mut client, &config).expect("bootstrap");
            Self { config, client }
        }

        fn names(&self) -> Names {
            Names::new(&self.config)
        }

        fn insert(&mut self, id: Option<i64>, kind: &str, item: &str, change: &str) {
            let table = qi(&self.config.table);
            let sql = format!(
                "INSERT INTO {table} (id, item_type, item_id, change_type, tx, event)
                 VALUES (COALESCE($1, nextval(pg_get_serial_sequence('{table}', 'id'))), $2, $3, $4, 'tx', '{{}}')"
            );
            self.client
                .execute(&sql, &[&id, &kind, &item, &change])
                .expect("insert");
        }

        fn snapshot(&mut self, source: SnapshotSource, high_water: i64) -> BTreeSet<i64> {
            let types = vec![THIS.to_owned()];
            self.client
                .query(
                    &snapshot_sql(&self.config.table, source),
                    &[&high_water, &types],
                )
                .expect("snapshot")
                .iter()
                .map(|row| row.get(0))
                .collect()
        }

        /// The query this module replaced, restricted to this server's types.
        fn distinct_on(&mut self, high_water: i64) -> BTreeSet<i64> {
            let table = qi(&self.config.table);
            let sql = format!(
                "WITH latest AS (SELECT DISTINCT ON (item_type, item_id) id FROM {table}
                   WHERE id <= $1 AND item_type = $2 ORDER BY item_type, item_id, id DESC)
                 SELECT e.id FROM latest JOIN {table} e ON e.id = latest.id WHERE e.change_type = 'SET'"
            );
            self.client
                .query(&sql, &[&high_water, &THIS])
                .expect("distinct on")
                .iter()
                .map(|row| row.get(0))
                .collect()
        }

        fn latest_rows(&mut self) -> BTreeSet<(String, String, i64, String)> {
            let sql = format!(
                "SELECT item_type, item_id, last_id, change_type FROM {}",
                self.names().latest
            );
            self.client
                .query(&sql, &[])
                .expect("latest")
                .iter()
                .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
                .collect()
        }

        fn expected_latest(&mut self) -> BTreeSet<(String, String, i64, String)> {
            let table = qi(&self.config.table);
            let sql = format!(
                "SELECT DISTINCT ON (item_type, item_id) item_type, item_id, id, change_type FROM {table}
                 ORDER BY item_type, item_id, id DESC"
            );
            self.client
                .query(&sql, &[])
                .expect("expected latest")
                .iter()
                .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
                .collect()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let t = &self.config.table;
            let _ = self.client.batch_execute(&format!(
                "DROP TABLE IF EXISTS {}, {}, {}, {} CASCADE",
                qi(t),
                qi(&format!("{t}_latest")),
                qi(&format!("{t}_transactions")),
                qi(&format!("{t}_schema_migrations"))
            ));
        }
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn snapshot_sources_match_the_distinct_on_query() {
        let mut f = Fixture::new();
        f.insert(None, THIS, "kept", "SET");
        f.insert(None, THIS, "kept", "SET");
        f.insert(None, THIS, "deleted", "SET");
        f.insert(None, THIS, "deleted", "DEL");
        f.insert(None, FOREIGN, "kept", "SET");
        f.insert(None, THIS, "resurrected", "DEL");
        f.insert(None, THIS, "resurrected", "SET");
        f.insert(None, THIS, "deleted_after_high_water", "SET");
        let high_water: i64 = f
            .client
            .query_one(&format!("SELECT MAX(id) FROM {}", qi(&f.config.table)), &[])
            .unwrap()
            .get(0);
        f.insert(None, THIS, "deleted_after_high_water", "DEL");
        f.insert(None, THIS, "only_after_high_water", "SET");
        f.insert(None, THIS, "kept", "SET");

        let expected = f.distinct_on(high_water);
        assert_eq!(expected, BTreeSet::from([2, 7, 8]));
        assert_eq!(
            f.snapshot(SnapshotSource::EventsSkipScan, high_water),
            expected
        );
        assert_eq!(
            f.snapshot(SnapshotSource::LatestTable, i64::MAX),
            BTreeSet::from([7, 10, 11])
        );
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn trigger_keeps_the_highest_id_per_item_including_deletes() {
        let mut f = Fixture::new();
        f.insert(Some(10), THIS, "a", "SET");
        f.insert(Some(5), THIS, "a", "DEL");
        f.insert(Some(20), THIS, "b", "SET");
        f.insert(Some(30), THIS, "b", "DEL");
        let s = |v: &str| v.to_owned();
        assert_eq!(
            f.latest_rows(),
            BTreeSet::from([
                (s(THIS), s("a"), 10, s("SET")),
                (s(THIS), s("b"), 30, s("DEL"))
            ])
        );
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn backfill_converges_with_concurrent_inserts() {
        let mut f = Fixture::new();
        for item in 0..500 {
            f.insert(None, THIS, &format!("i{item:03}"), "SET");
            f.insert(
                None,
                FOREIGN,
                &format!("i{item:03}"),
                if item % 3 == 0 { "DEL" } else { "SET" },
            );
        }
        f.client
            .batch_execute(&format!("TRUNCATE {}", f.names().latest))
            .unwrap();
        let config = f.config.clone();
        let writer = std::thread::spawn(move || {
            let mut client = connect_pg_client(&config, "writer").unwrap();
            let table = qi(&config.table);
            for round in 0..300 {
                let change = if round % 2 == 0 { "DEL" } else { "SET" };
                client
                    .execute(
                        &format!("INSERT INTO {table} (item_type, item_id, change_type, tx, event) VALUES ($1, $2, $3, 'tx', '{{}}')"),
                        &[&THIS, &format!("i{:03}", (round * 7) % 600), &change],
                    )
                    .unwrap();
            }
        });
        let names = f.names();
        backfill_latest(&mut f.client, &names, 37).expect("backfill");
        writer.join().unwrap();
        assert_eq!(f.latest_rows(), f.expected_latest());
    }

    #[test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    fn verify_only_startup_rejects_an_outdated_schema_and_migrate_repairs_it() {
        let mut f = Fixture::new();
        let names = f.names();
        assert_eq!(
            recorded_version(&mut f.client, &f.config.table).unwrap(),
            SCHEMA_VERSION
        );
        let tx_idx = names.index("tx_idx");
        f.client
            .batch_execute(&format!(
                "DELETE FROM {} WHERE version > 0;
                 UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{}'::regclass",
                names.migrations,
                qi(&tx_idx)
            ))
            .unwrap();
        let error = ensure_schema(&mut f.client, &f.config).expect_err("outdated");
        assert!(
            matches!(error, SchemaError::Outdated { found: 0, .. }),
            "{error}"
        );

        let applied = migrate(&f.config).expect("migrate");
        assert_eq!(applied, (1..=SCHEMA_VERSION).collect::<Vec<_>>());
        assert_eq!(index_validity(&mut f.client, &tx_idx).unwrap(), Some(true));
        assert!(
            index_validity(&mut f.client, &names.index("created_at_idx"))
                .unwrap()
                .is_none()
        );
        ensure_schema(&mut f.client, &f.config).expect("current schema verifies");
    }
}
